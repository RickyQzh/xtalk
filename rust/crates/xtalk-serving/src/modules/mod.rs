//! Serving modules: WebSocket gateways and ASR/TTS/VAD managers.

mod asr_manager;
mod input_gateway;
mod llm_agent_context_manager;
mod llm_agent_generation_manager;
mod output_gateway;
mod tts_manager;
mod turn_taking_manager;
mod vad_manager;

pub use asr_manager::{AsrManager, PRE_ROLL_FRAMES};
pub use input_gateway::InputGateway;
pub use llm_agent_context_manager::LlmAgentContextManager;
pub use llm_agent_generation_manager::LlmAgentGenerationManager;
pub use output_gateway::OutputGateway;
pub use tts_manager::TtsManager;
pub use turn_taking_manager::TurnTakingManager;
pub use vad_manager::VadManager;

use async_trait::async_trait;
use bytes::Bytes;

/// Abstraction over an outbound WebSocket connection (text + binary frames).
#[async_trait]
pub trait WsSink: Send + Sync {
    async fn send_text(&self, text: String);
    async fn send_bytes(&self, data: Bytes);
}
