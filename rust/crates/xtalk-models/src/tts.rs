//! TTS trait and DummyTts.

use crate::error::ModelError;
use async_trait::async_trait;

/// Text-to-speech interface producing PCM chunks.
#[async_trait]
pub trait Tts: Send + Sync {
    async fn synthesize_stream(&self, text: &str) -> Result<Vec<Vec<u8>>, ModelError>;
    fn sample_rate(&self) -> u32;
    fn clone_box(&self) -> Box<dyn Tts>;
}

/// Stub TTS that emits raw PCM s16le mono silence (no WAV header).
pub struct DummyTts {
    sample_rate: u32,
}

impl DummyTts {
    pub fn new(sample_rate: u32) -> Self {
        Self { sample_rate }
    }
}

#[async_trait]
impl Tts for DummyTts {
    async fn synthesize_stream(&self, text: &str) -> Result<Vec<Vec<u8>>, ModelError> {
        let _ = text;
        // 100ms silence @ sample_rate, s16le mono
        let bytes = (self.sample_rate as usize) / 10 * 2;
        Ok(vec![vec![0u8; bytes]])
    }

    fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    fn clone_box(&self) -> Box<dyn Tts> {
        Box::new(DummyTts::new(self.sample_rate))
    }
}
