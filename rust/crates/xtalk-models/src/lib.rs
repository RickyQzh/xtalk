//! xtalk-models — ASR, TTS, VAD, and Agent interfaces for the Rust runtime.

mod agent;
mod asr;
mod error;
mod http_agent;
mod tts;
mod vad;

pub use agent::{Agent, AgentContext, DummyAgent};
pub use asr::{Asr, DummyAsr};
pub use error::ModelError;
pub use http_agent::HttpChatAgent;
pub use tts::{DummyTts, Tts};
pub use vad::{DummyVad, Vad};
