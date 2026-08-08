//! TtsManager — synthesize response text into TTS chunk events.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;
use xtalk_bus::EventBus;
use xtalk_events::{Event, EventMeta};
use xtalk_models::Tts;

use crate::Manager;

struct TtsState {
    /// Pending sentence texts (FIFO). New [`Event::ResponseUpdate`] enqueues;
    /// they are not cancelled by later updates in the same generation.
    queue: VecDeque<String>,
    /// Cancel token for the in-flight worker session. Replaced on stop so a
    /// later enqueue can start a fresh session.
    cancel: CancellationToken,
    worker_running: bool,
}

enum SynthOutcome {
    Finished,
    Cancelled,
    Error,
}

/// Subscribes to [`Event::ResponseUpdate`], runs TTS, and emits
/// started / chunk / finished (or stopped on cancel).
pub struct TtsManager {
    session_id: String,
    tts: Arc<dyn Tts>,
    state: Mutex<TtsState>,
}

impl TtsManager {
    pub fn new(session_id: impl Into<String>, tts: Arc<dyn Tts>) -> Self {
        Self {
            session_id: session_id.into(),
            tts,
            state: Mutex::new(TtsState {
                queue: VecDeque::new(),
                cancel: CancellationToken::new(),
                worker_running: false,
            }),
        }
    }

    fn meta(&self) -> EventMeta {
        EventMeta::new(self.session_id.clone())
    }

    fn on_response_update(self: &Arc<Self>, bus: Arc<EventBus>, text: String) {
        if text.trim().is_empty() {
            return;
        }

        {
            let mut state = self.state.lock().expect("tts state poisoned");
            state.queue.push_back(text);
        }
        self.spawn_worker_if_needed(bus);
    }

    fn spawn_worker_if_needed(self: &Arc<Self>, bus: Arc<EventBus>) {
        let should_spawn = {
            let mut state = self.state.lock().expect("tts state poisoned");
            if state.worker_running || state.queue.is_empty() {
                false
            } else {
                if state.cancel.is_cancelled() {
                    state.cancel = CancellationToken::new();
                }
                state.worker_running = true;
                true
            }
        };

        if should_spawn {
            let this = Arc::clone(self);
            tokio::spawn(async move {
                this.run_worker(bus).await;
            });
        }
    }

    async fn run_worker(self: Arc<Self>, bus: Arc<EventBus>) {
        // Session token for this worker. `on_stop` cancels it and installs a
        // fresh token for any post-stop enqueue / respawn.
        let session_token = {
            let state = self.state.lock().expect("tts state poisoned");
            state.cancel.clone()
        };

        bus.publish(Event::TtsStarted { meta: self.meta() }).await;

        let mut terminal_stopped = false;
        loop {
            let text = {
                let mut state = self.state.lock().expect("tts state poisoned");
                // Check cancel *before* dequeue so post-stop enqueued texts
                // are left for the next worker (on_stop already cleared the
                // pre-stop queue).
                if session_token.is_cancelled() {
                    state.worker_running = false;
                    terminal_stopped = true;
                    break;
                }
                match state.queue.pop_front() {
                    Some(text) => text,
                    None => {
                        state.worker_running = false;
                        break;
                    }
                }
            };

            match self.synthesize_one(&bus, &text, &session_token).await {
                SynthOutcome::Finished => {}
                SynthOutcome::Cancelled => {
                    terminal_stopped = true;
                    let mut state = self.state.lock().expect("tts state poisoned");
                    state.worker_running = false;
                    // Do NOT queue.clear() — on_stop already cleared; texts
                    // enqueued after stop must survive for respawn.
                    break;
                }
                SynthOutcome::Error => {
                    // ErrorOccurred already published inside synthesize_one.
                    // Emit a terminal TTS event so TurnTaking depth decrements.
                    terminal_stopped = true;
                    let mut state = self.state.lock().expect("tts state poisoned");
                    state.worker_running = false;
                    break;
                }
            }
        }

        if terminal_stopped {
            bus.publish(Event::TtsStopped { meta: self.meta() }).await;
        } else {
            bus.publish(Event::TtsFinished { meta: self.meta() }).await;
        }

        // Work may have been enqueued while this session was winding down
        // (including after stop replaced the cancel token).
        self.spawn_worker_if_needed(bus);
    }

    async fn synthesize_one(
        &self,
        bus: &EventBus,
        text: &str,
        token: &CancellationToken,
    ) -> SynthOutcome {
        if token.is_cancelled() {
            return SynthOutcome::Cancelled;
        }

        tokio::select! {
            _ = token.cancelled() => SynthOutcome::Cancelled,
            result = self.tts.synthesize_stream(text) => {
                if token.is_cancelled() {
                    return SynthOutcome::Cancelled;
                }
                match result {
                    Ok(chunks) => {
                        let sample_rate = self.tts.sample_rate();
                        for audio_chunk in chunks {
                            if token.is_cancelled() {
                                return SynthOutcome::Cancelled;
                            }
                            bus.publish(Event::TtsChunkReady {
                                meta: self.meta(),
                                audio_chunk,
                                sample_rate,
                            })
                            .await;
                        }
                        if token.is_cancelled() {
                            SynthOutcome::Cancelled
                        } else {
                            SynthOutcome::Finished
                        }
                    }
                    Err(err) => {
                        tracing::error!(error = %err, "TTS synthesize failed");
                        bus.publish(Event::ErrorOccurred {
                            meta: self.meta(),
                            error_message: err.to_string(),
                        })
                        .await;
                        SynthOutcome::Error
                    }
                }
            }
        }
    }

    fn on_stop(&self) {
        let mut state = self.state.lock().expect("tts state poisoned");
        state.queue.clear();
        state.cancel.cancel();
        // Fresh token for any subsequent ResponseUpdate session. The in-flight
        // worker retains a clone of the cancelled token.
        state.cancel = CancellationToken::new();
    }
}

impl Manager for TtsManager {
    fn name(&self) -> &'static str {
        "tts_manager"
    }

    fn register(self: Arc<Self>, bus: Arc<EventBus>) {
        {
            let this = Arc::clone(&self);
            let bus_h = Arc::clone(&bus);
            bus.subscribe(
                "response.update",
                20,
                Arc::new(move |ev| {
                    let this = Arc::clone(&this);
                    let bus_h = Arc::clone(&bus_h);
                    Box::pin(async move {
                        if let Event::ResponseUpdate { text, .. } = ev {
                            this.on_response_update(bus_h, text);
                        }
                    })
                }),
            );
        }
        {
            let this = Arc::clone(&self);
            bus.subscribe(
                "turn.tts_stop_requested",
                95,
                Arc::new(move |_ev| {
                    let this = Arc::clone(&this);
                    Box::pin(async move {
                        this.on_stop();
                    })
                }),
            );
        }
    }
}
