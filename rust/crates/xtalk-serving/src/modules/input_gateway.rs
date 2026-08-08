//! InputGateway — WebSocket inbound frames → bus events.

use std::sync::Arc;

use bytes::Bytes;
use xtalk_bus::EventBus;
use xtalk_events::{Event, EventMeta};
use xtalk_protocol::{outbound_action, parse_inbound_text, InboundMessage, ProtocolError, PONG};

use crate::modules::WsSink;
use crate::Manager;

/// Maps client WebSocket text/binary frames onto session bus events.
pub struct InputGateway {
    session_id: String,
    bus: Arc<EventBus>,
    sink: Arc<dyn WsSink>,
}

impl InputGateway {
    pub fn new(session_id: String, bus: Arc<EventBus>, sink: Arc<dyn WsSink>) -> Self {
        Self {
            session_id,
            bus,
            sink,
        }
    }

    /// Parse and handle a text control frame.
    pub async fn handle_text(&self, text: &str) -> Result<(), ProtocolError> {
        match parse_inbound_text(text)? {
            InboundMessage::Ping { timestamp } => {
                let msg =
                    outbound_action(PONG, serde_json::json!({ "client_timestamp": timestamp }));
                self.sink.send_text(msg).await;
            }
            InboundMessage::VadSpeechStart => {
                self.bus
                    .publish(Event::VadSpeechStart {
                        meta: EventMeta::new(self.session_id.clone()),
                        origin: "client".into(),
                    })
                    .await;
            }
            InboundMessage::VadSpeechEnd => {
                self.bus
                    .publish(Event::VadSpeechEnd {
                        meta: EventMeta::new(self.session_id.clone()),
                        origin: "client".into(),
                    })
                    .await;
            }
            InboundMessage::TtsPlaybackFinished => {
                self.bus
                    .publish(Event::TtsPlaybackFinished {
                        meta: EventMeta::new(self.session_id.clone()),
                    })
                    .await;
            }
            // Minimal set: other inbound actions are accepted but ignored for now.
            _ => {}
        }
        Ok(())
    }

    /// Publish raw PCM as [`Event::AudioFrameReceived`].
    pub async fn handle_binary(&self, data: Bytes, sample_rate: u32) {
        self.bus
            .publish(Event::AudioFrameReceived {
                meta: EventMeta::new(self.session_id.clone()),
                audio_data: data.to_vec(),
                sample_rate,
            })
            .await;
    }
}

impl Manager for InputGateway {
    fn name(&self) -> &'static str {
        "input_gateway"
    }

    fn register(self: Arc<Self>, _bus: Arc<EventBus>) {
        // InputGateway publishes; subscriptions are not required for the minimal set.
    }
}
