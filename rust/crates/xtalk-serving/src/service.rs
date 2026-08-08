//! Session [`Service`] that owns the event bus and registered managers.

use std::sync::Arc;

use xtalk_bus::EventBus;
use xtalk_pipeline::Pipeline;

use crate::modules::{InputGateway, OutputGateway};
use crate::Manager;

/// Managers + gateways produced after the session bus exists.
pub struct ManagerBundle {
    pub managers: Vec<Arc<dyn Manager>>,
    pub input_gateway: Arc<InputGateway>,
    pub output_gateway: Arc<OutputGateway>,
}

/// One dialogue session: event bus, pipeline, and registered managers.
pub struct Service {
    pub session_id: String,
    bus: Arc<EventBus>,
    #[allow(dead_code)]
    pipeline: Box<dyn Pipeline>,
    /// Kept alive so manager state (and handler captures) remain valid.
    _managers: Vec<Arc<dyn Manager>>,
    input_gateway: Option<Arc<InputGateway>>,
    output_gateway: Option<Arc<OutputGateway>>,
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
            input_gateway: None,
            output_gateway: None,
        })
    }

    /// Create a session, building managers after the bus exists (needed by gateways).
    pub fn build(
        session_id: impl Into<String>,
        pipeline: Box<dyn Pipeline>,
        build_managers: impl FnOnce(&str, &dyn Pipeline, Arc<EventBus>) -> ManagerBundle,
    ) -> Arc<Self> {
        let session_id = session_id.into();
        let bus = Arc::new(EventBus::new());
        let bundle = build_managers(&session_id, pipeline.as_ref(), Arc::clone(&bus));
        for manager in &bundle.managers {
            Arc::clone(manager).register(Arc::clone(&bus));
        }
        Arc::new(Self {
            session_id,
            bus,
            pipeline,
            _managers: bundle.managers,
            input_gateway: Some(bundle.input_gateway),
            output_gateway: Some(bundle.output_gateway),
        })
    }

    /// Shared handle to this session's event bus.
    pub fn bus(&self) -> Arc<EventBus> {
        Arc::clone(&self.bus)
    }

    /// Inbound WebSocket gateway (present when built via [`Service::build`]).
    pub fn input_gateway(&self) -> Option<Arc<InputGateway>> {
        self.input_gateway.as_ref().map(Arc::clone)
    }

    /// Outbound WebSocket gateway (present when built via [`Service::build`]).
    pub fn output_gateway(&self) -> Option<Arc<OutputGateway>> {
        self.output_gateway.as_ref().map(Arc::clone)
    }

    /// Notify the client that the session is ready.
    pub async fn send_session_attached(&self) {
        if let Some(output) = &self.output_gateway {
            output.send_session_attached().await;
        }
    }

    /// Clear bus subscriptions (and error-rate state).
    pub async fn shutdown(&self) {
        self.bus.shutdown().await;
    }
}
