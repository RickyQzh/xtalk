//! LlmAgentContextManager — bridge ASR finals into LLM consume requests.

use std::sync::Arc;

use xtalk_bus::EventBus;
use xtalk_events::{Event, EventMeta};

use crate::Manager;

/// Forwards [`Event::AsrResultFinal`] into
/// [`Event::LlmAgentConsumeGenerationRequested`].
pub struct LlmAgentContextManager {
    session_id: String,
}

impl LlmAgentContextManager {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
        }
    }

    fn meta(&self) -> EventMeta {
        EventMeta::new(self.session_id.clone())
    }
}

impl Manager for LlmAgentContextManager {
    fn name(&self) -> &'static str {
        "llm_agent_context_manager"
    }

    fn register(self: Arc<Self>, bus: Arc<EventBus>) {
        let this = Arc::clone(&self);
        let bus_h = Arc::clone(&bus);
        bus.subscribe(
            "asr.result_final",
            20,
            Arc::new(move |ev| {
                let this = Arc::clone(&this);
                let bus_h = Arc::clone(&bus_h);
                Box::pin(async move {
                    if let Event::AsrResultFinal { text, .. } = ev {
                        bus_h
                            .publish(Event::LlmAgentConsumeGenerationRequested {
                                meta: this.meta(),
                                context_type: "asr_final".into(),
                                text,
                            })
                            .await;
                    }
                })
            }),
        );
    }
}
