//! xtalk-serving — serving layer (gateways and managers) for the Rust runtime.

mod manager;
mod modules;
mod service;
mod service_manager;
mod session_limiter;

pub use manager::Manager;
pub use modules::{
    AsrManager, InputGateway, LlmAgentContextManager, LlmAgentGenerationManager, OutputGateway,
    TtsManager, TurnTakingManager, VadManager, WsSink, PRE_ROLL_FRAMES,
};
pub use service::{ManagerBundle, Service};
pub use service_manager::{ManagerFactory, PipelineFactory, ServiceManager};
pub use session_limiter::{LimitError, SessionLimiter, SessionPermit};

pub fn crate_name() -> &'static str {
    "xtalk-serving"
}
