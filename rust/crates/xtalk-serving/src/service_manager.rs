//! Active session table with concurrency limiting.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use uuid::Uuid;

use xtalk_pipeline::Pipeline;

use crate::modules::WsSink;
use crate::session_limiter::{LimitError, SessionLimiter, SessionPermit};
use crate::service::Service;
use crate::Manager;

/// Factory that produces a fresh pipeline per session.
pub type PipelineFactory = Arc<dyn Fn() -> Box<dyn Pipeline> + Send + Sync>;

struct SessionEntry {
    service: Arc<Service>,
    /// Held until disconnect so the limiter slot stays reserved.
    _permit: SessionPermit,
}

/// Creates and tracks [`Service`] sessions under a [`SessionLimiter`].
pub struct ServiceManager {
    limiter: SessionLimiter,
    pipeline_factory: PipelineFactory,
    sessions: Mutex<HashMap<String, SessionEntry>>,
}

impl ServiceManager {
    /// Build a manager with `max_sessions` concurrent slots and a pipeline factory.
    pub fn new(max_sessions: usize, pipeline_factory: PipelineFactory) -> Self {
        Self {
            limiter: SessionLimiter::new(max_sessions),
            pipeline_factory,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Create a session, acquire a limiter permit, and track it.
    ///
    /// `sink` is accepted for forward compatibility with Task 12 (WebSocket
    /// gateways need a [`WsSink`]). For Task 11 the service is created with an
    /// empty managers list — gateways are not registered yet.
    ///
    /// `user_id` is reserved for persistence/auth wiring later.
    pub async fn connect(
        &self,
        _sink: Option<Arc<dyn WsSink>>,
        _user_id: Option<&str>,
    ) -> Result<Arc<Service>, LimitError> {
        let permit = self.limiter.acquire().await?;
        let session_id = Uuid::new_v4().to_string();
        let pipeline = (self.pipeline_factory)();
        let managers: Vec<Arc<dyn Manager>> = Vec::new();
        let service = Service::new(session_id.clone(), pipeline, managers);

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
