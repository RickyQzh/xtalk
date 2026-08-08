//! Session [`Service`] that owns the event bus and registered managers.

use std::sync::Arc;

use xtalk_bus::EventBus;
use xtalk_pipeline::Pipeline;

use crate::Manager;

/// One dialogue session: event bus, pipeline, and registered managers.
pub struct Service {
    pub session_id: String,
    bus: Arc<EventBus>,
    #[allow(dead_code)]
    pipeline: Box<dyn Pipeline>,
    /// Kept alive so manager state (and handler captures) remain valid.
    _managers: Vec<Arc<dyn Manager>>,
}

impl Service {
    /// Create a session service: builds an [`EventBus`] and registers each manager.
    pub fn new(
        session_id: impl Into<String>,
        pipeline: Box<dyn Pipeline>,
        managers: Vec<Arc<dyn Manager>>,
    ) -> Arc<Self> {
        let bus = Arc::new(EventBus::new());
        for manager in &managers {
            Arc::clone(manager).register(Arc::clone(&bus));
        }
        Arc::new(Self {
            session_id: session_id.into(),
            bus,
            pipeline,
            _managers: managers,
        })
    }

    /// Shared handle to this session's event bus.
    pub fn bus(&self) -> Arc<EventBus> {
        Arc::clone(&self.bus)
    }

    /// Clear bus subscriptions (and error-rate state).
    pub async fn shutdown(&self) {
        self.bus.shutdown().await;
    }
}
