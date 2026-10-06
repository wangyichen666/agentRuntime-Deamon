//! 进程组合：所有业务 owner 与入口实现均在 workspace crate。
mod bootstrap;
use anyhow::Result;
use tracing_subscriber::EnvFilter;
fn main() -> Result<()> {
    bootstrap::load_persisted_environment();
    init_tracing();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(agent_cli::command::run(&bootstrap::BootstrapHost))
}
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}
