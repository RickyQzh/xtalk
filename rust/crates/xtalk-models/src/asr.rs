//! ASR trait and DummyAsr.

use crate::error::ModelError;
use async_trait::async_trait;
use std::sync::Mutex;

/// Streaming / offline speech recognition interface.
#[async_trait]
pub trait Asr: Send + Sync {
    async fn recognize_stream(&self, audio: &[u8], is_final: bool) -> Result<String, ModelError>;
    fn reset(&self);
    fn clone_box(&self) -> Box<dyn Asr>;
}

/// Stub ASR that returns a fixed string on stream finals (and when audio arrives).
pub struct DummyAsr {
    default_text: String,
    stream_text: Mutex<String>,
}

impl DummyAsr {
    pub fn new(default_text: impl Into<String>) -> Self {
        Self {
            default_text: default_text.into(),
            stream_text: Mutex::new(String::new()),
        }
    }
}

#[async_trait]
impl Asr for DummyAsr {
    async fn recognize_stream(&self, audio: &[u8], is_final: bool) -> Result<String, ModelError> {
        let mut stream_text = self
            .stream_text
            .lock()
            .map_err(|_| ModelError::message("DummyAsr stream_text lock poisoned"))?;
        if !audio.is_empty() || is_final {
            *stream_text = self.default_text.clone();
        }
        Ok(stream_text.clone())
    }

    fn reset(&self) {
        if let Ok(mut stream_text) = self.stream_text.lock() {
            stream_text.clear();
        }
    }

    fn clone_box(&self) -> Box<dyn Asr> {
        Box::new(DummyAsr::new(self.default_text.clone()))
    }
}
