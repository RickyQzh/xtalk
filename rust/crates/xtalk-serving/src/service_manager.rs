//! Active session table with concurrency limiting.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use uuid::Uuid;
use xtalk_bus::EventBus;
use xtalk_pipeline::Pipeline;

use crate::modules::WsSink;
use crate::service::{ManagerBundle, Service};
use crate::session_limiter::{LimitError, SessionLimiter, SessionPermit};

/// Factory that produces a fresh pipeline per session.
pub type PipelineFactory = Arc<dyn Fn() -> Box<dyn Pipeline> + Send + Sync>;

/// Builds managers/gateways once the session bus and WebSocket sink exist.
pub type ManagerFactory = Arc<
    dyn Fn(&str, &dyn Pipeline, Arc<EventBus>, Arc<dyn WsSink>) -> ManagerBundle + Send + Sync,
>;

struct SessionEntry {
    service: Arc<Service>,
    /// Held until disconnect so the limiter slot stays reserved.
    _permit: SessionPermit,
}

/// Creates and tracks [`Service`] sessions under a [`SessionLimiter`].
pub struct ServiceManager {
    limiter: SessionLimiter,
    pipeline_factory: PipelineFactory,
    manager_factory: Option<ManagerFactory>,
    sessions: Mutex<HashMap<String, SessionEntry>>,
}

impl ServiceManager {
    /// Build a manager with `max_sessions` concurrent slots and a pipeline factory.
    pub fn new(max_sessions: usize, pipeline_factory: PipelineFactory) -> Self {
        Self {
            limiter: SessionLimiter::new(max_sessions),
            pipeline_factory,
            manager_factory: None,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Attach a manager/gateway factory used when `connect` receives a [`WsSink`].
    pub fn with_manager_factory(mut self, factory: ManagerFactory) -> Self {
        self.manager_factory = Some(factory);
        self
    }

    /// Create a session, acquire a limiter permit, and track it.
    ///
    /// When both `sink` and a [`ManagerFactory`] are present, gateways and model
    /// managers are registered on the session bus. Otherwise the service is
    /// created with an empty managers list (useful for limiter unit tests).
    ///
    /// `user_id` is reserved for persistence/auth wiring later.
    pub async fn connect(
        &self,
        sink: Option<Arc<dyn WsSink>>,
        _user_id: Option<&str>,
    ) -> Result<Arc<Service>, LimitError> {
        let permit = self.limiter.acquire().await?;
        let session_id = Uuid::new_v4().to_string();
        let pipeline = (self.pipeline_factory)();

        let service = match (sink, self.manager_factory.as_ref()) {
            (Some(sink), Some(factory)) => {
                let factory = Arc::clone(factory);
                Service::build(session_id.clone(), pipeline, move |sid, pipe, bus| {
                    factory(sid, pipe, bus, sink)
                })
            }
            _ => Service::new(session_id.clone(), pipeline, Vec::new()),
        };

        self.sessions.lock().await.insert(
            session_id,
            SessionEntry {
                service: Arc::clone(&service),
                _permit: permit,
            },
        );
        Ok(service)
    }

    /// Shut down and remove a tracked session, releasing its limiter slot.
    pub async fn disconnect(&self, session_id: &str) {
        let entry = self.sessions.lock().await.remove(session_id);
        if let Some(entry) = entry {
            entry.service.shutdown().await;
            // SessionPermit dropped here → frees limiter slot.
        }
    }

    /// Look up an active session by id.
    pub async fn get(&self, session_id: &str) -> Option<Arc<Service>> {
        self.sessions
            .lock()
            .await
            .get(session_id)
            .map(|e| Arc::clone(&e.service))
    }

    /// Number of currently tracked sessions.
    pub fn session_count(&self) -> usize {
        // Prefer limiter count (consistent even if maps diverge); use try_lock
        // fallback via blocking would be awkward — use active_sessions.
        self.limiter.active_sessions()
    }
}
