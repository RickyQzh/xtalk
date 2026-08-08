//! OutputGateway — bus events → WebSocket outbound frames.

use std::sync::Arc;

use bytes::Bytes;
use xtalk_bus::EventBus;
use xtalk_events::Event;
use xtalk_protocol::{
    outbound_action, ERROR, FINISH_ASR, FINISH_RESP, SESSION_ATTACHED, START_TTS, STOP_TTS,
    TTS_FINISHED, UPDATE_ASR, UPDATE_RESP,
};

use crate::modules::WsSink;
use crate::Manager;

/// Forwards selected session events to the client via [`WsSink`].
pub struct OutputGateway {
    session_id: String,
    sink: Arc<dyn WsSink>,
}

impl OutputGateway {
    pub fn new(session_id: String, sink: Arc<dyn WsSink>) -> Self {
        Self { session_id, sink }
    }

    /// Notify the client that the session is ready (called by Service later).
    pub async fn send_session_attached(&self) {
        // Always include `session_id` (frontend session.ts reads this key).
        let msg = outbound_action(
            SESSION_ATTACHED,
            serde_json::json!({ "session_id": self.session_id }),
        );
        self.sink.send_text(msg).await;
    }

    async fn forward_event(&self, event: Event) {
        match event {
            Event::AsrResultPartial {
                text, display_text, ..
            } => {
                let msg = outbound_action(UPDATE_ASR, text_data(asr_display(text, display_text)));
                self.sink.send_text(msg).await;
            }
            Event::AsrResultFinal {
                text, display_text, ..
            } => {
                let msg = outbound_action(FINISH_ASR, text_data(asr_display(text, display_text)));
                self.sink.send_text(msg).await;
            }
            Event::ResponseUpdate { text, .. } => {
                // Frontend messages.ts requires `data.text`.
                let msg = outbound_action(UPDATE_RESP, text_data(text));
                self.sink.send_text(msg).await;
            }
            Event::ResponseFinish { text, .. } => {
                let msg = outbound_action(FINISH_RESP, text_data(text));
                self.sink.send_text(msg).await;
            }
            Event::TtsStarted { .. } => {
                let msg = outbound_action(START_TTS, "");
                self.sink.send_text(msg).await;
            }
            Event::TtsStopped { .. } => {
                let msg = outbound_action(STOP_TTS, "");
                self.sink.send_text(msg).await;
            }
            Event::TtsFinished { .. } => {
                let msg = outbound_action(TTS_FINISHED, serde_json::json!({}));
                self.sink.send_text(msg).await;
            }
            Event::TtsChunkReady { audio_chunk, .. } => {
                if !audio_chunk.is_empty() {
                    self.sink.send_bytes(Bytes::from(audio_chunk)).await;
                }
            }
            Event::ErrorOccurred { error_message, .. } => {
                let msg = outbound_action(ERROR, error_message);
                self.sink.send_text(msg).await;
            }
            // Intentionally omit `latency_metrics`: Phase 1 has no LatencyManager.
            // Do not emit a partial object (frontend expects all *_ms fields).
            _ => {}
        }
    }
}

/// Prefer non-empty `display_text`; otherwise fall back to `text` (may be empty).
fn asr_display(text: String, display_text: String) -> String {
    if display_text.is_empty() {
        text
    } else {
        display_text
    }
}

/// Outbound payload with a guaranteed `text` key (never omit the field).
fn text_data(text: impl Into<String>) -> serde_json::Value {
    serde_json::json!({ "text": text.into() })
}

impl Manager for OutputGateway {
    fn name(&self) -> &'static str {
        "output_gateway"
    }

    fn register(self: Arc<Self>, bus: Arc<EventBus>) {
        const TYPES: &[&str] = &[
            "asr.result_partial",
            "asr.result_final",
            "response.update",
            "response.finish",
            "tts.started",
            "tts.stopped",
            "tts.finished",
            "tts.chunk_ready",
            "error.occurred",
        ];
        for type_name in TYPES {
            let this = Arc::clone(&self);
            bus.subscribe(
                type_name,
                5,
                Arc::new(move |ev| {
                    let this = Arc::clone(&this);
                    Box::pin(async move {
                        this.forward_event(ev).await;
                    })
                }),
            );
        }
    }
}
