//! AsrManager — preroll-buffered streaming ASR driven by VAD / turn events.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use xtalk_bus::EventBus;
use xtalk_events::{Event, EventMeta};
use xtalk_models::Asr;

use crate::Manager;

/// Number of recent audio frames retained as preroll padding before ASR start.
pub const PRE_ROLL_FRAMES: usize = 20;

struct AsrState {
    listening: bool,
    preroll: VecDeque<Vec<u8>>,
}

/// Consumes audio + VAD/turn start/end events; emits ASR partial/final results.
pub struct AsrManager {
    session_id: String,
    asr: Arc<dyn Asr>,
    state: Mutex<AsrState>,
}

impl AsrManager {
    pub fn new(session_id: impl Into<String>, asr: Arc<dyn Asr>) -> Self {
        Self {
            session_id: session_id.into(),
            asr,
            state: Mutex::new(AsrState {
                listening: false,
                preroll: VecDeque::with_capacity(PRE_ROLL_FRAMES),
            }),
        }
    }

    fn meta(&self) -> EventMeta {
        EventMeta::new(self.session_id.clone())
    }

    async fn on_audio_frame(&self, bus: &EventBus, audio_data: Vec<u8>) {
        let feed = {
            let mut st = self.state.lock().expect("asr manager state poisoned");
            st.preroll.push_back(audio_data.clone());
            while st.preroll.len() > PRE_ROLL_FRAMES {
                st.preroll.pop_front();
            }
            st.listening.then_some(audio_data)
        };

        if let Some(audio) = feed {
            self.recognize_partial(bus, &audio).await;
        }
    }

    async fn on_start(&self, bus: &EventBus) {
        let preroll = {
            let mut st = self.state.lock().expect("asr manager state poisoned");
            if st.listening {
                return;
            }
            st.listening = true;
            st.preroll.iter().flatten().copied().collect::<Vec<u8>>()
        };

        if !preroll.is_empty() {
            self.recognize_partial(bus, &preroll).await;
        }
    }

    async fn on_end(&self, bus: &EventBus) {
        {
            let mut st = self.state.lock().expect("asr manager state poisoned");
            if !st.listening {
                return;
            }
            st.listening = false;
        }

        match self.asr.recognize_stream(&[], true).await {
            Ok(text) => {
                if !text.trim().is_empty() {
                    bus.publish(Event::AsrResultFinal {
                        meta: self.meta(),
                        text: text.clone(),
                        display_text: text,
                        speech_pause: false,
                    })
                    .await;
                }
            }
            Err(err) => {
                tracing::error!(error = %err, "ASR finalize failed");
            }
        }

        self.asr.reset();
    }

    async fn recognize_partial(&self, bus: &EventBus, audio: &[u8]) {
        match self.asr.recognize_stream(audio, false).await {
            Ok(text) => {
                if text.trim().is_empty() {
                    return;
                }
                bus.publish(Event::AsrResultPartial {
                    meta: self.meta(),
                    text: text.clone(),
                    display_text: text,
                    speech_pause: false,
                })
                .await;
            }
            Err(err) => {
                tracing::error!(error = %err, "ASR partial failed");
            }
        }
    }
}

impl Manager for AsrManager {
    fn name(&self) -> &'static str {
        "asr_manager"
    }

    fn register(self: Arc<Self>, bus: Arc<EventBus>) {
        {
            let this = Arc::clone(&self);
            let bus_h = Arc::clone(&bus);
            bus.subscribe(
                "audio.frame_received",
                10,
                Arc::new(move |ev| {
                    let this = Arc::clone(&this);
                    let bus_h = Arc::clone(&bus_h);
                    Box::pin(async move {
                        if let Event::AudioFrameReceived { audio_data, .. } = ev {
                            this.on_audio_frame(&bus_h, audio_data).await;
                        }
                    })
                }),
            );
        }
        for type_name in ["vad.speech_start", "turn.asr_start_requested"] {
            let this = Arc::clone(&self);
            let bus_h = Arc::clone(&bus);
            bus.subscribe(
                type_name,
                10,
                Arc::new(move |_ev| {
                    let this = Arc::clone(&this);
                    let bus_h = Arc::clone(&bus_h);
                    Box::pin(async move {
                        this.on_start(&bus_h).await;
                    })
                }),
            );
        }
        for type_name in ["vad.speech_end", "turn.asr_end_requested"] {
            let this = Arc::clone(&self);
            let bus_h = Arc::clone(&bus);
            bus.subscribe(
                type_name,
                10,
                Arc::new(move |_ev| {
                    let this = Arc::clone(&this);
                    let bus_h = Arc::clone(&bus_h);
                    Box::pin(async move {
                        this.on_end(&bus_h).await;
                    })
                }),
            );
        }
    }
}
