//! xtalk-serving — serving layer (gateways and managers) for the Rust runtime.

mod manager;
mod service;

pub use manager::Manager;
pub use service::Service;

pub fn crate_name() -> &'static str {
    "xtalk-serving"
}
