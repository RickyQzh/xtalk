//! xtalk-serving — serving layer (gateways and managers) for the Rust runtime.

mod manager;
mod modules;
mod service;

pub use manager::Manager;
pub use modules::{AsrManager, InputGateway, OutputGateway, VadManager, WsSink, PRE_ROLL_FRAMES};
pub use service::Service;

pub fn crate_name() -> &'static str {
    "xtalk-serving"
}
