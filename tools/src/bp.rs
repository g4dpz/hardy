use clap::{Parser, Subcommand, ValueEnum};

pub mod exit_code;
mod ping;
mod recv;
mod send;
pub mod session;

/// Bundle Protocol diagnostic and testing tools.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Send ping bundles and measure round-trip time
    Ping(ping::Command),
    /// Send a file (or stdin) as a BPv7 bundle to a destination endpoint
    Send(send::Command),
    /// Receive bundles and save payloads to files
    Recv(recv::Command),
}

fn main() {
    // Match on the parsed subcommand and call the appropriate handler function.
    // This is the core of the dispatch logic.
    match Cli::parse().command {
        Commands::Ping(args) => args.exec(),
        Commands::Send(args) => args.exec(),
        Commands::Recv(args) => args.exec(),
    }
}
