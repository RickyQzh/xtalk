//! VAD trait and DummyVad.

use crate::error::ModelError;
use async_trait::async_trait;

/// Voice activity detection interface.
#[async_trait]
pub trait Vad: Send + Sync {
    async fn is_speech(&self, frame: &[u8]) -> Result<bool, ModelError>;
    fn clone_box(&self) -> Box<dyn Vad>;
}

/// Stub VAD: treats frames of at least 320 bytes (10ms @ 16 kHz s16le mono) as speech.
pub struct DummyVad;

impl DummyVad {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DummyVad {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Vad for DummyVad {
    async fn is_speech(&self, frame: &[u8]) -> Result<bool, ModelError> {
        Ok(frame.len() >= 320)
    }

    fn clone_box(&self) -> Box<dyn Vad> {
        Box::new(DummyVad::new())
    }
}
