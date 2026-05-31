use super::*;
use hardy_bpv7::eid::NodeId;
use std::path::PathBuf;

mod exec;
mod output;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Verbosity {
    /// Most verbose, all internal details
    #[value(name = "trace")]
    Trace,

    /// Debug information
    #[value(name = "debug")]
    Debug,

    /// Informational messages
    #[value(name = "info")]
    Info,

    /// Warnings only
    #[value(name = "warn")]
    Warn,

    /// Errors only
    #[value(name = "error")]
    Error,
}

impl From<Verbosity> for tracing::Level {
    fn from(value: Verbosity) -> Self {
        match value {
            Verbosity::Trace => tracing::Level::TRACE,
            Verbosity::Debug => tracing::Level::DEBUG,
            Verbosity::Info => tracing::Level::INFO,
            Verbosity::Warn => tracing::Level::WARN,
            Verbosity::Error => tracing::Level::ERROR,
        }
    }
}

/// Receive bundles destined for a service endpoint and save payloads to files.
///
/// Connects to a running Hardy BPA via TCPCLv4 as a peer node, registers a
/// service endpoint, and writes received bundle payloads to files or stdout.
/// Press Ctrl+C to stop.
#[derive(Parser, Debug)]
#[command(about, long_about = None)]
pub struct Command {
    /// Peer address (host:port) of the BPA's TCPCLv4 listener
    #[arg(long)]
    peer: String,

    /// This tool's Node_ID (e.g., ipn:99.0). Random if omitted.
    #[arg(long)]
    node_id: Option<NodeId>,

    /// Service number to listen on (e.g., 1 for ipn:N.1)
    #[arg(long, default_value = "1")]
    service: u64,

    /// Output directory for received payloads (stdout if omitted)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Maximum number of bundles to receive before exiting
    #[arg(short, long)]
    count: Option<u32>,

    /// Suppress non-error output
    #[arg(short, long)]
    quiet: bool,

    /// Verbose output level [trace, debug, info, warn, error]
    #[arg(short, long, num_args = 0..=1, require_equals = true, default_missing_value = "info")]
    verbose: Option<Verbosity>,

    /// Accept self-signed TLS certificates
    #[arg(long = "tls-insecure")]
    tls_insecure: bool,

    /// CA bundle directory for TLS verification
    #[arg(long = "tls-ca")]
    tls_ca: Option<PathBuf>,
}

impl Command {
    pub fn exec(self) -> ! {
        if let Some(level) = self.verbose.map(tracing::Level::from) {
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(level)
                .with_target(level > tracing::Level::INFO)
                .finish();
            if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
                eprintln!("Failed to set global default subscriber: {e}");
                std::process::exit(2);
            }
        }

        exec::exec(self)
    }
}
