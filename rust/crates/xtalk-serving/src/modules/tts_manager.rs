//! TtsManager — synthesize response text into TTS chunk events.

use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;
use xtalk_bus::EventBus;
use xtalk_events::{Event, EventMeta};
use xtalk_models::Tts;

use crate::Manager;

/// Subscribes to [`Event::ResponseUpdate`], runs TTS, and emits
/// started / chunk / finished (or stopped on cancel).
pub struct TtsManager {
    session_id: String,
    tts: Arc<dyn Tts>,
    cancel: Mutex<Option<CancellationToken>>,
}

impl TtsManager {
    pub fn new(session_id: impl Into<String>, tts: Arc<dyn Tts>) -> Self {
        Self {
            session_id: session_id.into(),
            tts,
            cancel: Mutex::new(None),
        }
    }

    fn meta(&self) -> EventMeta {
        EventMeta::new(self.session_id.clone())
    }

    fn on_response_update(self: &Arc<Self>, bus: Arc<EventBus>, text: String) {
        if text.trim().is_empty() {
            return;
        }

        let token = CancellationToken::new();
        {
            let mut guard = self.cancel.lock().expect("tts cancel poisoned");
            if let Some(prev) = guard.take() {
                prev.cancel();
            }
            *guard = Some(token.clone());
        }

        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.run_synthesis(bus, text, token).await;
        });
    }

    async fn run_synthesis(&self, bus: Arc<EventBus>, text: String, token: CancellationToken) {
        bus.publish(Event::TtsStarted { meta: self.meta() })
            .await;

        if token.is_cancelled() {
            bus.publish(Event::TtsStopped { meta: self.meta() })
                .await;
            return;
        }

        tokio::select! {
            _ = token.cancelled() => {
                bus.publish(Event::TtsStopped { meta: self.meta() }).await;
            }
            result = self.tts.synthesize_stream(&text) => {
                if token.is_cancelled() {
                    bus.publish(Event::TtsStopped { meta: self.meta() }).await;
                    return;
                }
                match result {
                    Ok(chunks) => {
                        let sample_rate = self.tts.sample_rate();
                        for audio_chunk in chunks {
                            if token.is_cancelled() {
                                bus.publish(Event::TtsStopped { meta: self.meta() }).await;
                                return;
                            }
                            bus.publish(Event::TtsChunkReady {
                                meta: self.meta(),
                                audio_chunk,
                                sample_rate,
                            })
                            .await;
                        }
                        if token.is_cancelled() {
                            bus.publish(Event::TtsStopped { meta: self.meta() }).await;
                            return;
                        }
                        bus.publish(Event::TtsFinished { meta: self.meta() })
                            .await;
                    }
                    Err(err) => {
                        tracing::error!(error = %err, "TTS synthesize failed");
                        bus.publish(Event::ErrorOccurred {
                            meta: self.meta(),
                            error_message: err.to_string(),
                        })
                        .await;
                    }
                }
            }
        }
    }

    fn on_stop(&self) {
        if let Some(token) = self.cancel.lock().expect("tts cancel poisoned").take() {
            token.cancel();
        }
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
