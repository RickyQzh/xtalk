//! Model error types.

use thiserror::Error;

/// Errors returned by ASR / TTS / VAD / Agent implementations.
#[derive(Debug, Error)]
pub enum ModelError {
    #[error("model error: {0}")]
    Message(String),

    #[error("cancelled")]
    Cancelled,

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl ModelError {
    /// Convenience constructor for a simple message error.
    pub fn message(msg: impl Into<String>) -> Self {
        Self::Message(msg.into())
    }
}
