//! Manager trait for components that register event handlers on the bus.

use std::sync::Arc;

use xtalk_bus::EventBus;

/// A serving-layer component that registers handlers on an [`EventBus`].
///
/// Prefer injecting model handles at construction time rather than looking them
/// up from the pipeline later.
pub trait Manager: Send + Sync {
    fn name(&self) -> &'static str;

    /// Subscribe handlers on `bus`. Called once during [`crate::Service::new`].
    fn register(self: Arc<Self>, bus: Arc<EventBus>);
}
