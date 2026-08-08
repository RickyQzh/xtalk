//! VadManager — optional server-side VAD (Phase 1: simple edge detection).

use std::sync::{Arc, Mutex};

use xtalk_bus::EventBus;
use xtalk_events::{Event, EventMeta};
use xtalk_models::Vad;

use crate::Manager;

/// Optional backend VAD. When no model is configured, client VAD drives events.
pub struct VadManager {
    session_id: String,
    vad: Option<Arc<dyn Vad>>,
    in_speech: Mutex<bool>,
}

impl VadManager {
    pub fn new(session_id: impl Into<String>, vad: Option<Arc<dyn Vad>>) -> Self {
        Self {
            session_id: session_id.into(),
            vad,
            in_speech: Mutex::new(false),
        }
    }

    async fn on_audio_frame(&self, bus: &EventBus, audio_data: &[u8]) {
        let Some(vad) = self.vad.as_ref() else {
            return;
        };
        if audio_data.is_empty() {
            return;
        }

        let is_speech = match vad.is_speech(audio_data).await {
            Ok(v) => v,
            Err(err) => {
                tracing::error!(error = %err, "server VAD failed");
                false
            }
        };

        let transition = {
            let mut in_speech = self.in_speech.lock().expect("vad manager state poisoned");
            if is_speech && !*in_speech {
                *in_speech = true;
                Some(true)
            } else if !is_speech && *in_speech {
                *in_speech = false;
                Some(false)
            } else {
                None
            }
        };

        match transition {
            Some(true) => {
                bus.publish(Event::VadSpeechStart {
                    meta: EventMeta::new(self.session_id.clone()),
                    origin: "server".into(),
                })
                .await;
            }
            Some(false) => {
                bus.publish(Event::VadSpeechEnd {
                    meta: EventMeta::new(self.session_id.clone()),
                    origin: "server".into(),
                })
                .await;
            }
            None => {}
        }
    }
}

impl Manager for VadManager {
    fn name(&self) -> &'static str {
        "vad_manager"
    }

    fn register(self: Arc<Self>, bus: Arc<EventBus>) {
        if self.vad.is_none() {
            return;
        }
        let this = Arc::clone(&self);
        let bus_h = Arc::clone(&bus);
        bus.subscribe(
            "audio.frame_received",
            100,
            Arc::new(move |ev| {
                let this = Arc::clone(&this);
                let bus_h = Arc::clone(&bus_h);
                Box::pin(async move {
                    if let Event::AudioFrameReceived { audio_data, .. } = ev {
                        this.on_audio_frame(&bus_h, &audio_data).await;
                    }
                })
            }),
        );
    }
}
