//! Serving modules: WebSocket gateways and ASR/TTS/VAD managers.

mod asr_manager;
mod input_gateway;
mod output_gateway;
mod vad_manager;

pub use asr_manager::{AsrManager, PRE_ROLL_FRAMES};
pub use input_gateway::InputGateway;
pub use output_gateway::OutputGateway;
pub use vad_manager::VadManager;

use async_trait::async_trait;
use bytes::Bytes;

/// Abstraction over an outbound WebSocket connection (text + binary frames).
#[async_trait]
pub trait WsSink: Send + Sync {
    async fn send_text(&self, text: String);
    async fn send_bytes(&self, data: Bytes);
}
