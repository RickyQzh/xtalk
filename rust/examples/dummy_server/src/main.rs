//! Dummy-model example server (config-driven xtalk-server).

use clap::Parser;
use tracing_subscriber::EnvFilter;
use xtalk_server::{serve, ServerConfig};

#[derive(Debug, Parser)]
#[command(name = "dummy_server", about = "X-Talk Rust dummy pipeline demo")]
struct Args {
    /// Path to JSON server config (defaults to bundled dummy config).
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
    tracing::info!(listen = %cfg.listen, "starting dummy_server");
    serve(cfg).await
}
