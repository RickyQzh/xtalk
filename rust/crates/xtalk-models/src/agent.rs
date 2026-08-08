//! Agent trait, context, and DummyAgent.

use crate::error::ModelError;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// Context payload forwarded into an agent turn.
#[derive(Debug, Clone)]
pub struct AgentContext {
    pub context_type: String,
    pub text: String,
}

/// LLM / dialogue agent interface.
#[async_trait]
pub trait Agent: Send + Sync {
    async fn accept(
        &self,
        ctx: AgentContext,
        cancel: CancellationToken,
    ) -> Result<Vec<String>, ModelError>;
    fn clone_box(&self) -> Box<dyn Agent>;
}

/// Stub agent that replies with a fixed string for `asr_final` / `embedding` contexts.
pub struct DummyAgent {
    default_response: String,
}

impl DummyAgent {
    pub fn new(default_response: impl Into<String>) -> Self {
        Self {
            default_response: default_response.into(),
        }
    }
}

#[async_trait]
impl Agent for DummyAgent {
    async fn accept(
        &self,
        ctx: AgentContext,
        cancel: CancellationToken,
    ) -> Result<Vec<String>, ModelError> {
        if cancel.is_cancelled() {
            return Err(ModelError::Cancelled);
        }
        match ctx.context_type.as_str() {
            "asr_final" | "embedding" => Ok(vec![self.default_response.clone()]),
            _ => Ok(Vec::new()),
        }
    }

    fn clone_box(&self) -> Box<dyn Agent> {
        Box::new(DummyAgent::new(self.default_response.clone()))
    }
}
