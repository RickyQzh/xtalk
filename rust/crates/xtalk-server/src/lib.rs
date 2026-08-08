//! xtalk-server — HTTP/WebSocket runtime for the X-Talk Rust port.

pub mod app;
pub mod auth;
pub mod config;

pub use app::{build_app, build_session_managers, serve, AppState};
pub use auth::{AuthError, AuthMode, WsAuthQuery};
pub use config::{build_pipeline, ConfigError, ModelSpec, ServerConfig};
