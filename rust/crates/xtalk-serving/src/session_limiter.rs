//! Concurrent session slot limiter.

use std::sync::{Arc, Mutex};

use thiserror::Error;

/// Error when no session slot is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum LimitError {
    #[error("session limit reached")]
    Full,
}

/// RAII permit for one active session slot. Released on drop.
pub struct SessionPermit {
    slots: Arc<Mutex<usize>>,
}

impl Drop for SessionPermit {
    fn drop(&mut self) {
        let mut active = self.slots.lock().expect("session limiter lock poisoned");
        *active = active.saturating_sub(1);
    }
}

/// Limits the number of concurrent sessions.
pub struct SessionLimiter {
    max_sessions: usize,
    /// Count of currently held permits.
    active: Arc<Mutex<usize>>,
}

impl SessionLimiter {
    /// Create a limiter with at most `max_sessions` concurrent permits.
    ///
    /// `max_sessions == 0` means unlimited (acquire always succeeds).
    pub fn new(max_sessions: usize) -> Self {
        Self {
            max_sessions,
            active: Arc::new(Mutex::new(0)),
        }
    }

    /// Acquire a session slot asynchronously.
    ///
    /// Returns [`LimitError::Full`] when the limit is already reached.
    pub async fn acquire(&self) -> Result<SessionPermit, LimitError> {
        self.try_acquire()
    }

    /// Non-blocking acquire. Same semantics as [`Self::acquire`].
    pub fn try_acquire(&self) -> Result<SessionPermit, LimitError> {
        let mut active = self.active.lock().expect("session limiter lock poisoned");
        if self.max_sessions > 0 && *active >= self.max_sessions {
            return Err(LimitError::Full);
        }
        *active += 1;
        Ok(SessionPermit {
            slots: Arc::clone(&self.active),
        })
    }

    /// Current number of held permits.
    pub fn active_sessions(&self) -> usize {
        *self.active.lock().expect("session limiter lock poisoned")
    }
}
