use super::*;
use hardy_bpv7::eid::{Eid, NodeId};
use std::path::PathBuf;

mod bundle;
mod exec;

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

/// Send a file (or stdin) as a BPv7 bundle to a destination endpoint.
///
/// Connects to a running Hardy BPA via TCPCLv4 as a peer node, constructs
/// a BPv7 bundle containing the file payload, and transfers it to the BPA
/// for forwarding to the destination.
#[derive(Parser, Debug)]
#[command(about, long_about = None)]
pub struct Command {
    /// Destination EID (e.g., ipn:2.1)
    destination: Eid,

    /// File to send (reads from stdin if omitted and stdin is not a terminal)
    file: Option<PathBuf>,

    /// Peer address (host:port) of the BPA's TCPCLv4 listener
    #[arg(long)]
    peer: String,

    /// This tool's Node_ID (e.g., ipn:99.0). Random if omitted.
    #[arg(long)]
    node_id: Option<NodeId>,

    /// Bundle lifetime in seconds
    #[arg(long, default_value = "3600")]
    lifetime: u64,

    /// Do not allow the bundle to be fragmented
    #[arg(long)]
    no_fragment: bool,

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
        exec::exec(self)
    }
}
