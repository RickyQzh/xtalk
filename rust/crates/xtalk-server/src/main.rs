//! xtalk-server binary entrypoint.

use clap::Parser;
use tracing_subscriber::EnvFilter;
use xtalk_server::{serve, ServerConfig};

#[derive(Debug, Parser)]
#[command(name = "xtalk-server", about = "X-Talk Rust runtime server")]
struct Args {
    /// Path to JSON server config.
    #[arg(long, default_value = "examples/dummy_server/config.dummy.json")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();
    let cfg = ServerConfig::from_path(&args.config)?;
    serve(cfg).await
}
