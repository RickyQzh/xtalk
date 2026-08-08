//! Serving modules: WebSocket gateways and (later) ASR/TTS/VAD managers.

mod input_gateway;
mod output_gateway;

pub use input_gateway::InputGateway;
pub use output_gateway::OutputGateway;

use async_trait::async_trait;
use bytes::Bytes;

/// Abstraction over an outbound WebSocket connection (text + binary frames).
#[async_trait]
pub trait WsSink: Send + Sync {
    async fn send_text(&self, text: String);
    async fn send_bytes(&self, data: Bytes);
}
