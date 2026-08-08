//! xtalk-bus — async priority EventBus for the Rust runtime.

use futures::future::BoxFuture;
use futures::FutureExt;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex;
use xtalk_events::{now_ts, Event, EventMeta};

/// Max recursion depth for `error.occurred` dispatch.
pub const MAX_ERROR_EVENT_DEPTH: u32 = 3;
/// Minimum seconds between accepted `error.occurred` publishes.
pub const ERROR_EVENT_COOLDOWN: f64 = 1.0;
/// Max `error.occurred` events accepted per trailing 1-second window.
pub const ERROR_EVENT_RATE_LIMIT: usize = 10;

/// Async event handler: receives a cloned [`Event`] and returns a boxed future.
pub type Handler = Arc<dyn Fn(Event) -> BoxFuture<'static, ()> + Send + Sync>;

#[derive(Clone)]
struct Sub {
    priority: i32,
    handler: Handler,
}

struct ErrorLimitState {
    depth: u32,
    last_error_event_time: f64,
    error_event_times: Vec<f64>,
}

impl ErrorLimitState {
    fn new() -> Self {
        Self {
            depth: 0,
            last_error_event_time: 0.0,
            error_event_times: Vec::new(),
        }
    }

    fn reset(&mut self) {
        self.depth = 0;
        self.last_error_event_time = 0.0;
        self.error_event_times.clear();
    }
}

/// Priority-ordered async publish/subscribe bus.
///
/// Subscription map uses [`StdMutex`] so sync [`Self::subscribe`] works from any
/// runtime flavor (including `#[tokio::test]` current-thread). Error-limit state
/// uses [`tokio::sync::Mutex`] and is only touched from async paths.
pub struct EventBus {
    subs: StdMutex<HashMap<String, Vec<Sub>>>,
    error_state: Mutex<ErrorLimitState>,
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            subs: StdMutex::new(HashMap::new()),
            error_state: Mutex::new(ErrorLimitState::new()),
        }
    }

    /// Subscribe `handler` to `type_name`. Higher `priority` runs earlier.
    pub fn subscribe(&self, type_name: &str, priority: i32, handler: Handler) {
        let mut guard = self.subs.lock().expect("event bus subscription lock poisoned");
        let list = guard.entry(type_name.to_string()).or_default();
        list.push(Sub { priority, handler });
        list.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Publish `event` to matching handlers (priority descending, sequential await).
    pub async fn publish(&self, event: Event) {
        if matches!(event, Event::ErrorOccurred { .. }) {
            if !self.begin_error_event().await {
                return;
            }
            self.dispatch(event).await;
            self.end_error_event().await;
            return;
        }

        self.dispatch(event).await;
    }

    /// Clear subscriptions and reset error-rate tracking.
    pub async fn shutdown(&self) {
        self.subs
            .lock()
            .expect("event bus subscription lock poisoned")
            .clear();
        self.error_state.lock().await.reset();
    }

    async fn dispatch(&self, event: Event) {
        let key = event.type_name().to_string();
        let handlers = {
            let guard = self
                .subs
                .lock()
                .expect("event bus subscription lock poisoned");
            guard.get(&key).cloned().unwrap_or_default()
        };

        for sub in handlers {
            let fut = (sub.handler)(event.clone());
            match AssertUnwindSafe(fut).catch_unwind().await {
                Ok(()) => {}
                Err(_) => {
                    tracing::error!(
                        event_type = key.as_str(),
                        "Event handler panicked; continuing dispatch"
                    );
                    if key != "error.occurred" {
                        let session_id = event.meta().session_id.clone();
                        let error_event = Event::ErrorOccurred {
                            meta: EventMeta::new(session_id),
                            error_message: "event_handler_panic".into(),
                        };
                        // Box to allow recursive async publish without infinitely sized futures.
                        Box::pin(self.publish(error_event)).await;
                    }
                }
            }
        }
    }

    /// Returns true if the error event should be dispatched (and depth is reserved).
    async fn begin_error_event(&self) -> bool {
        let mut state = self.error_state.lock().await;
        if state.depth >= MAX_ERROR_EVENT_DEPTH {
            tracing::error!(
                depth = MAX_ERROR_EVENT_DEPTH,
                "Max error event depth reached, dropping error event"
            );
            return false;
        }

        let current_time = now_ts();
        if current_time - state.last_error_event_time < ERROR_EVENT_COOLDOWN {
            tracing::debug!("Error event cooldown active, dropping error event");
            return false;
        }

        state
            .error_event_times
            .retain(|t| current_time - *t < 1.0);
        if state.error_event_times.len() >= ERROR_EVENT_RATE_LIMIT {
            tracing::warn!(
                limit = ERROR_EVENT_RATE_LIMIT,
                "Error event rate limit exceeded, dropping error event"
            );
            return false;
        }

        state.depth += 1;
        state.last_error_event_time = current_time;
        state.error_event_times.push(current_time);
        true
    }

    async fn end_error_event(&self) {
        let mut state = self.error_state.lock().await;
        if state.depth > 0 {
            state.depth -= 1;
        }
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}
