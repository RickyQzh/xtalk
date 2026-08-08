//! TurnTakingManager — barge-in: stop TTS + LLM when client speaks during TTS.

use std::sync::{Arc, Mutex};

use xtalk_bus::EventBus;
use xtalk_events::{Event, EventMeta};

use crate::Manager;

struct TurnState {
    /// Nested TTS session depth (unmatched `tts.started` count).
    ///
    /// Using a depth/refcount instead of a bool so a superseded session's
    /// `tts.stopped` cannot clear activity while a newer session is already
    /// active (Started A → Started B → Stopped A must leave barge-in armed).
    tts_active_depth: u32,
}

/// Tracks TTS activity and interrupts generation on client VAD speech start.
pub struct TurnTakingManager {
    session_id: String,
    state: Mutex<TurnState>,
}

impl TurnTakingManager {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            state: Mutex::new(TurnState {
                tts_active_depth: 0,
            }),
        }
    }

    fn meta(&self) -> EventMeta {
        EventMeta::new(self.session_id.clone())
    }

    fn on_tts_started(&self) {
        self.state
            .lock()
            .expect("turn taking state poisoned")
            .tts_active_depth += 1;
    }

    fn on_tts_ended(&self) {
        let mut state = self.state.lock().expect("turn taking state poisoned");
        state.tts_active_depth = state.tts_active_depth.saturating_sub(1);
    }

    fn tts_active(&self) -> bool {
        self.state
            .lock()
            .expect("turn taking state poisoned")
            .tts_active_depth
            > 0
    }

    async fn on_vad_speech_start(&self, bus: &EventBus, origin: &str) {
        if origin != "client" {
            return;
        }
        if !self.tts_active() {
            return;
        }

        bus.publish(Event::TurnTtsStopRequested { meta: self.meta() })
            .await;
        bus.publish(Event::TurnLlmAgentStopRequested { meta: self.meta() })
            .await;
    }
}

impl Manager for TurnTakingManager {
    fn name(&self) -> &'static str {
        "turn_taking_manager"
    }

    fn register(self: Arc<Self>, bus: Arc<EventBus>) {
        {
            let this = Arc::clone(&self);
            let bus_h = Arc::clone(&bus);
            bus.subscribe(
                "vad.speech_start",
                90,
                Arc::new(move |ev| {
                    let this = Arc::clone(&this);
                    let bus_h = Arc::clone(&bus_h);
                    Box::pin(async move {
                        if let Event::VadSpeechStart { origin, .. } = ev {
                            this.on_vad_speech_start(&bus_h, &origin).await;
                        }
                    })
                }),
            );
        }
        {
            let this = Arc::clone(&self);
            bus.subscribe(
                "tts.started",
                50,
                Arc::new(move |_ev| {
                    let this = Arc::clone(&this);
                    Box::pin(async move {
                        this.on_tts_started();
                    })
                }),
            );
        }
        for type_name in ["tts.finished", "tts.stopped"] {
            let this = Arc::clone(&self);
            bus.subscribe(
                type_name,
                50,
                Arc::new(move |_ev| {
                    let this = Arc::clone(&this);
                    Box::pin(async move {
                        this.on_tts_ended();
                    })
                }),
            );
        }
    }
}
