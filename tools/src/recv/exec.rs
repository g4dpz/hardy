use super::output::{extract_payload, write_payload};
use super::Command;
use crate::exit_code::ExitCode;
use crate::session;
use hardy_bpa::async_trait;
use hardy_bpa::bpa::BpaRegistration;
use hardy_bpv7::eid::{Eid, IpnNodeId, NodeId, Service as EidService};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// A service that receives bundles and writes their payloads to files or stdout.
///
/// Implements the `hardy_bpa::services::Service` trait to receive raw bundle bytes,
/// extract the payload using `extract_payload`, and write it using `write_payload`.
///
/// The TCPCLv4 layer automatically sends XFER_ACK when a bundle is successfully
/// delivered to this service (handled by the BPA/CLA infrastructure).
pub struct RecvService {
    /// The sink for communicating back to the BPA (stored on registration).
    sink: std::sync::OnceLock<Box<dyn hardy_bpa::services::ServiceSink>>,
    /// Output directory for received payloads (None = stdout).
    output_dir: Option<PathBuf>,
    /// Whether to suppress non-error output.
    quiet: bool,
    /// Number of bundles received so far.
    received: AtomicU32,
    /// Optional count limit — when reached, signals completion.
    count_limit: Option<u32>,
    /// Semaphore used to signal when count limit is reached.
    semaphore: Option<Arc<tokio::sync::Semaphore>>,
}

impl RecvService {
    /// Create a new `RecvService` with the given configuration.
    pub fn new(
        output_dir: Option<PathBuf>,
        quiet: bool,
        count_limit: Option<u32>,
    ) -> (Self, Option<Arc<tokio::sync::Semaphore>>) {
        let semaphore = count_limit.map(|_| Arc::new(tokio::sync::Semaphore::new(0)));
        let sema_clone = semaphore.clone();
        (
            Self {
                sink: std::sync::OnceLock::new(),
                output_dir,
                quiet,
                received: AtomicU32::new(0),
                count_limit,
                semaphore,
            },
            sema_clone,
        )
    }

    /// Get the number of bundles received so far.
    pub fn received_count(&self) -> u32 {
        self.received.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl hardy_bpa::services::Service for RecvService {
    async fn on_register(
        &self,
        _endpoint: &Eid,
        sink: Box<dyn hardy_bpa::services::ServiceSink>,
    ) {
        self.sink.get_or_init(|| sink);
    }

    async fn on_unregister(&self) {
        // Nothing to do on unregister
    }

    async fn on_receive(&self, data: hardy_bpa::Bytes, _expiry: time::OffsetDateTime) {
        // Extract payload from the raw bundle bytes
        match extract_payload(&data) {
            Ok(received) => {
                // Write payload to file or stdout
                match write_payload(
                    &received.payload,
                    &received.source,
                    &received.creation_timestamp,
                    self.output_dir.as_deref(),
                ) {
                    Ok(Some(path)) => {
                        if !self.quiet {
                            eprintln!(
                                "Received bundle from {} → {}",
                                received.source,
                                path.display()
                            );
                        }
                    }
                    Ok(None) => {
                        // Written to stdout, no message needed unless verbose
                        if !self.quiet {
                            eprintln!(
                                "Received bundle from {} ({} bytes)",
                                received.source,
                                received.payload.len()
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to write payload: {e}");
                        return;
                    }
                }

                // Increment received count
                let count = self.received.fetch_add(1, Ordering::Relaxed) + 1;

                // Signal completion if count limit reached
                if let Some(limit) = self.count_limit {
                    if count >= limit {
                        if let Some(semaphore) = &self.semaphore {
                            semaphore.add_permits(1);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to process received bundle: {e}");
            }
        }
    }

    async fn on_status_notify(
        &self,
        _bundle_id: &hardy_bpv7::bundle::Id,
        _from: &Eid,
        _kind: hardy_bpa::services::StatusNotify,
        _reason: hardy_bpv7::status_report::ReasonCode,
        _timestamp: Option<time::OffsetDateTime>,
    ) {
        // Not used for recv — we don't send bundles, so no status reports expected
    }
}

async fn exec_async(args: &Command) -> anyhow::Result<ExitCode> {
    if !args.quiet {
        eprintln!("Listening for bundles on service {}...", args.service);
    }

    // 1. Determine node ID (provided or generate random)
    let node_id = args.node_id.clone().unwrap_or_else(|| {
        use rand::RngExt;
        let mut rng = rand::rng();
        NodeId::Ipn(IpnNodeId {
            allocator_id: rng.random_range(0x40000000..0x80000000),
            node_number: rng.random_range(1..u32::MAX),
        })
    });

    // 2. Establish TCPCLv4 session with the remote BPA
    let bpa = session::establish_session(
        &args.peer,
        Some(node_id),
        args.tls_insecure,
        args.tls_ca.as_deref(),
    )
    .await?;

    // 3. Create and register the RecvService
    let (service, semaphore) =
        RecvService::new(args.output.clone(), args.quiet, args.count);
    let service = Arc::new(service);

    let service_id = EidService::Ipn(args.service as u32);
    bpa.register_service(service_id, service.clone())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to register service: {e}"))?;

    // 4. Set up signal handler for graceful shutdown
    let cancel_token = session::setup_signal_handler();

    // 5. Wait for bundles (until count limit or cancellation)
    if let Some(sema) = semaphore {
        // Wait for count limit or cancellation
        tokio::select! {
            _ = cancel_token.cancelled() => {
                if !args.quiet {
                    eprintln!("\nShutting down...");
                }
            }
            result = sema.acquire() => {
                let _ = result;
                if !args.quiet {
                    eprintln!(
                        "Received {} bundle(s), exiting.",
                        service.received_count()
                    );
                }
            }
        }
    } else {
        // No count limit — wait until cancelled
        cancel_token.cancelled().await;
        if !args.quiet {
            eprintln!("\nShutting down...");
        }
    }

    // 6. Graceful shutdown
    session::shutdown(bpa).await;

    Ok(ExitCode::Success)
}

pub fn exec(args: Command) -> ! {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Failed to build tokio runtime: {e}");
            std::process::exit(ExitCode::Error as i32);
        }
    };

    match runtime.block_on(exec_async(&args)) {
        Ok(exit_code) => std::process::exit(exit_code as i32),
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(ExitCode::Error as i32);
        }
    }
}
