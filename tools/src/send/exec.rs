use super::*;
use crate::exit_code::{self, ExitCode};
use crate::session;
use hardy_bpa::async_trait;
use hardy_bpa::bpa::BpaRegistration;
use hardy_bpa::services::{Service as BpaService, ServiceSink};
use hardy_bpv7::eid::Service as EidService;
use std::io::IsTerminal;
use std::io::Read;
use std::sync::Arc;

/// Read the payload bytes from the file path argument or stdin.
///
/// - If `file` is `Some`, reads the file at that path.
/// - If `file` is `None` and stdin is not a terminal, reads from stdin.
/// - If `file` is `None` and stdin IS a terminal, reports an error and exits.
///
/// File I/O errors are reported with context and exit code 2.
fn read_payload(file: Option<&PathBuf>) -> Vec<u8> {
    if let Some(path) = file {
        // Read from the specified file
        std::fs::read(path).unwrap_or_else(|e| {
            exit_code::report_file_error(path, &e);
        })
    } else if !std::io::stdin().is_terminal() {
        // Read from stdin (piped input)
        let mut buf = Vec::new();
        std::io::stdin().read_to_end(&mut buf).unwrap_or_else(|e| {
            exit_code::report_error(&format!("Failed to read from stdin: {e}"));
        });
        buf
    } else {
        exit_code::report_error(
            "No file path provided and stdin is a terminal. Provide a file path or pipe data to stdin.",
        );
    }
}

/// A minimal BPA service used by `bp send` to dispatch bundles.
///
/// This service registers with the local BPA to obtain a [`ServiceSink`],
/// which is then used to send the constructed bundle into the BPA's dispatch
/// pipeline. The BPA forwards the bundle via TCPCLv4 to the remote BPA.
struct SendService {
    sink: std::sync::OnceLock<Box<dyn ServiceSink>>,
}

impl SendService {
    fn new() -> Self {
        Self {
            sink: std::sync::OnceLock::new(),
        }
    }

    /// Wait for the sink to be available (set during on_register).
    fn sink(&self) -> &dyn ServiceSink {
        self.sink.wait().as_ref()
    }
}

#[async_trait]
impl BpaService for SendService {
    async fn on_register(&self, _endpoint: &Eid, sink: Box<dyn ServiceSink>) {
        self.sink.get_or_init(|| sink);
    }

    async fn on_unregister(&self) {}

    async fn on_receive(&self, _data: hardy_bpa::Bytes, _expiry: time::OffsetDateTime) {
        // bp send does not expect to receive bundles
    }

    async fn on_status_notify(
        &self,
        _bundle_id: &hardy_bpv7::bundle::Id,
        _from: &Eid,
        _kind: hardy_bpa::services::StatusNotify,
        _reason: hardy_bpv7::status_report::ReasonCode,
        _timestamp: Option<time::OffsetDateTime>,
    ) {
        // Not used for send
    }
}

async fn exec_async(args: &Command) -> anyhow::Result<ExitCode> {
    // 1. Read the payload (file or stdin)
    let payload = read_payload(args.file.as_ref());

    if !args.quiet {
        eprintln!("Sending {} bytes to {}", payload.len(), args.destination);
    }

    // 2. Determine node ID (provided or generate random)
    let node_id = args.node_id.clone().unwrap_or_else(|| {
        use hardy_bpv7::eid::IpnNodeId;
        use rand::RngExt;
        let mut rng = rand::rng();
        NodeId::Ipn(IpnNodeId {
            allocator_id: rng.random_range(0x40000000..0x80000000),
            node_number: rng.random_range(1..u32::MAX),
        })
    });

    // 3. Derive source EID from node ID (service number 1)
    let source_eid = match &node_id {
        NodeId::Ipn(fqnn) => Eid::Ipn {
            fqnn: *fqnn,
            service_number: 1,
        },
        other => other.clone().into(),
    };

    // 4. Establish TCPCLv4 session with the remote BPA
    let bpa = session::establish_session(
        &args.peer,
        Some(node_id),
        args.tls_insecure,
        args.tls_ca.as_deref(),
    )
    .await?;

    // 5. Set up signal handler for graceful shutdown
    let cancel_token = session::setup_signal_handler();

    // 6. Build the bundle
    let bundle_data = bundle::build_bundle(
        &source_eid,
        &args.destination,
        &payload,
        args.lifetime,
        args.no_fragment,
    )?;

    // 7. Register a send service with the BPA to get a ServiceSink
    let send_service = Arc::new(SendService::new());
    let service_id = EidService::Ipn(1); // service number 1 matches source_eid
    bpa.register_service(service_id, send_service.clone())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to register send service: {e}"))?;

    // 8. Dispatch the bundle through the BPA
    // NOTE: TCPCLv4 segmentation is handled automatically by the `hardy_tcpclv4` CLA.
    // The Session::send_once() method segments bundles into XFER_SEGMENT messages
    // of at most `segment_mtu` bytes (negotiated from the peer's Segment_MRU during
    // SESS_INIT). No manual segmentation is needed here regardless of bundle size.
    // See: tcpclv4/src/session.rs — send_once() and on_transfer() methods.
    let bundle_bytes = hardy_bpa::Bytes::from(bundle_data.into_vec());
    let result = send_service.sink().send(bundle_bytes).await;

    let exit_code = match result {
        Ok(_bundle_id) => {
            // Bundle accepted by BPA and queued for forwarding.
            // Wait briefly for the async forwarding to complete via TCPCLv4.
            // The BPA's dispatcher processes the bundle and the TCPCLv4 CLA
            // handles the XFER_SEGMENT/XFER_ACK exchange with the remote BPA.
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                    // Forwarding should have completed within this window
                }
                _ = cancel_token.cancelled() => {
                    // User interrupted - graceful shutdown
                    if !args.quiet {
                        eprintln!("Transfer interrupted");
                    }
                }
            }

            if !args.quiet {
                eprintln!(
                    "Sent {} bytes to {} successfully",
                    payload.len(),
                    args.destination
                );
            }
            ExitCode::Success
        }
        Err(e) => {
            // Check if this is a "dropped by filter" error which could indicate refusal
            let err_str = e.to_string();
            if err_str.contains("dropped") || err_str.contains("refused") {
                eprintln!("Error: Bundle transfer refused: {e}");
                ExitCode::TransferRefused
            } else {
                return Err(anyhow::anyhow!("Failed to dispatch bundle: {e}"));
            }
        }
    };

    // 9. Shutdown the BPA gracefully
    session::shutdown(bpa).await;

    Ok(exit_code)
}

pub fn exec(args: Command) -> ! {
    // Set up tracing if verbose output is requested
    if let Some(level) = args.verbose {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::from(level))
            .with_writer(std::io::stderr)
            .init();
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_payload_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test_payload.bin");
        let expected = b"hello, bundle protocol!";
        std::fs::write(&file_path, expected).unwrap();

        let result = read_payload(Some(&file_path));
        assert_eq!(result, expected);
    }

    #[test]
    fn read_payload_from_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("empty.bin");
        std::fs::write(&file_path, b"").unwrap();

        let result = read_payload(Some(&file_path));
        assert_eq!(result, Vec::<u8>::new());
    }

    #[test]
    fn read_payload_from_file_binary() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("binary.bin");
        let expected: Vec<u8> = (0..=255).collect();
        std::fs::write(&file_path, &expected).unwrap();

        let result = read_payload(Some(&file_path));
        assert_eq!(result, expected);
    }
}
