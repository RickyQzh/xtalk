//! LlmAgentGenerationManager — run agent.accept and emit response events.

use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;
use xtalk_bus::EventBus;
use xtalk_events::{Event, EventMeta};
use xtalk_models::{Agent, AgentContext, ModelError};

use crate::Manager;

/// Consumes [`Event::LlmAgentConsumeGenerationRequested`], calls the agent,
/// and publishes [`Event::ResponseUpdate`] / [`Event::ResponseFinish`].
pub struct LlmAgentGenerationManager {
    session_id: String,
    agent: Arc<dyn Agent>,
    cancel: Mutex<Option<CancellationToken>>,
}

impl LlmAgentGenerationManager {
    pub fn new(session_id: impl Into<String>, agent: Arc<dyn Agent>) -> Self {
        Self {
            session_id: session_id.into(),
            agent,
            cancel: Mutex::new(None),
        }
    }

    fn meta(&self) -> EventMeta {
        EventMeta::new(self.session_id.clone())
    }

    async fn on_consume(&self, bus: &EventBus, context_type: String, text: String) {
        let token = CancellationToken::new();
        {
            let mut guard = self.cancel.lock().expect("generation cancel poisoned");
            if let Some(prev) = guard.take() {
                prev.cancel();
            }
            *guard = Some(token.clone());
        }

        let ctx = AgentContext {
            context_type,
            text,
        };

        match self.agent.accept(ctx, token.clone()).await {
            Ok(parts) => {
                if token.is_cancelled() {
                    return;
                }
                let mut accumulated = String::new();
                for part in parts {
                    if token.is_cancelled() {
                        return;
                    }
                    accumulated.push_str(&part);
                    bus.publish(Event::ResponseUpdate {
                        meta: self.meta(),
                        text: part,
                    })
                    .await;
                }
                if token.is_cancelled() {
                    return;
                }
                bus.publish(Event::ResponseFinish {
                    meta: self.meta(),
                    text: accumulated,
                })
                .await;
            }
            Err(ModelError::Cancelled) => {}
            Err(err) => {
                tracing::error!(error = %err, "LLM agent accept failed");
                bus.publish(Event::ErrorOccurred {
                    meta: self.meta(),
                    error_message: err.to_string(),
                })
                .await;
            }
        }
    }

    fn on_stop(&self) {
        if let Some(token) = self
            .cancel
            .lock()
            .expect("generation cancel poisoned")
            .take()
        {
            token.cancel();
        }
    }
}

impl Manager for LlmAgentGenerationManager {
    fn name(&self) -> &'static str {
        "llm_agent_generation_manager"
    }

    fn register(self: Arc<Self>, bus: Arc<EventBus>) {
        {
            let this = Arc::clone(&self);
            let bus_h = Arc::clone(&bus);
            bus.subscribe(
                "llm_agent.consume_generation_requested",
                20,
                Arc::new(move |ev| {
                    let this = Arc::clone(&this);
                    let bus_h = Arc::clone(&bus_h);
                    Box::pin(async move {
                        if let Event::LlmAgentConsumeGenerationRequested {
                            context_type,
                            text,
                            ..
                        } = ev
                        {
                            this.on_consume(&bus_h, context_type, text).await;
                        }
                    })
                }),
            );
        }
        {
            let this = Arc::clone(&self);
            bus.subscribe(
                "turn.llm_agent_stop_requested",
                95,
                Arc::new(move |_ev| {
                    let this = Arc::clone(&this);
                    Box::pin(async move {
                        this.on_stop();
                    })
                }),
            );
        }
    }
}
