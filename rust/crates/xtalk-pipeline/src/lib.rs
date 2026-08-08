//! xtalk-pipeline — dialogue pipeline orchestration for the Rust runtime.

use xtalk_models::{Agent, Asr, Tts, Vad};

/// Session pipeline holding optional model slots.
pub trait Pipeline: Send + Sync {
    fn clone_session(&self) -> Box<dyn Pipeline>;
    fn asr(&self) -> Option<&dyn Asr>;
    fn tts(&self) -> Option<&dyn Tts>;
    fn vad(&self) -> Option<&dyn Vad>;
    fn agent(&self) -> Option<&dyn Agent>;
}

/// Default pipeline with ASR / TTS / VAD / Agent slots.
pub struct DefaultPipeline {
    asr: Option<Box<dyn Asr>>,
    tts: Option<Box<dyn Tts>>,
    vad: Option<Box<dyn Vad>>,
    agent: Option<Box<dyn Agent>>,
}

impl DefaultPipeline {
    pub fn builder() -> DefaultPipelineBuilder {
        DefaultPipelineBuilder::default()
    }
}

impl Pipeline for DefaultPipeline {
    fn clone_session(&self) -> Box<dyn Pipeline> {
        Box::new(DefaultPipeline {
            asr: self.asr.as_ref().map(|m| m.clone_box()),
            tts: self.tts.as_ref().map(|m| m.clone_box()),
            vad: self.vad.as_ref().map(|m| m.clone_box()),
            agent: self.agent.as_ref().map(|m| m.clone_box()),
        })
    }

    fn asr(&self) -> Option<&dyn Asr> {
        self.asr.as_deref()
    }

    fn tts(&self) -> Option<&dyn Tts> {
        self.tts.as_deref()
    }

    fn vad(&self) -> Option<&dyn Vad> {
        self.vad.as_deref()
    }

    fn agent(&self) -> Option<&dyn Agent> {
        self.agent.as_deref()
    }
}

/// Builder for [`DefaultPipeline`].
#[derive(Default)]
pub struct DefaultPipelineBuilder {
    asr: Option<Box<dyn Asr>>,
    tts: Option<Box<dyn Tts>>,
    vad: Option<Box<dyn Vad>>,
    agent: Option<Box<dyn Agent>>,
}

impl DefaultPipelineBuilder {
    pub fn asr(mut self, asr: Box<dyn Asr>) -> Self {
        self.asr = Some(asr);
        self
    }

    pub fn tts(mut self, tts: Box<dyn Tts>) -> Self {
        self.tts = Some(tts);
        self
    }

    pub fn vad(mut self, vad: Box<dyn Vad>) -> Self {
        self.vad = Some(vad);
        self
    }

    pub fn agent(mut self, agent: Box<dyn Agent>) -> Self {
        self.agent = Some(agent);
        self
    }

    pub fn build(self) -> DefaultPipeline {
        DefaultPipeline {
            asr: self.asr,
            tts: self.tts,
            vad: self.vad,
            agent: self.agent,
        }
    }
}
