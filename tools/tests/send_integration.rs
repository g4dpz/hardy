//! End-to-end integration tests for `bp send`.
//!
//! These tests spawn an in-process BPA with a TCPCLv4 listener on localhost,
//! then run `bp send` as a subprocess to verify bundles are correctly
//! transferred and received.
//!
//! **Validates: Requirements 2.1, 2.4, 2.5**

use hardy_bpa::bpa::{Bpa, BpaRegistration};
use hardy_bpa::services::{Application, ApplicationSink, StatusNotify};
use hardy_bpa::{Bytes, async_trait};
use hardy_bpv7::eid::{Eid, IpnNodeId, NodeId, Service as EidService};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Test Application — captures received bundle payloads via a channel
// ---------------------------------------------------------------------------

struct TestRecvApp {
    sink: std::sync::OnceLock<Box<dyn ApplicationSink>>,
    received_tx: mpsc::Sender<(Eid, Vec<u8>)>,
}

impl TestRecvApp {
    fn new() -> (Arc<Self>, mpsc::Receiver<(Eid, Vec<u8>)>) {
        let (tx, rx) = mpsc::channel(16);
        (
            Arc::new(Self {
                sink: std::sync::OnceLock::new(),
                received_tx: tx,
            }),
            rx,
        )
    }
}

#[async_trait]
impl Application for TestRecvApp {
    async fn on_register(&self, _source: &Eid, sink: Box<dyn ApplicationSink>) {
        self.sink.get_or_init(|| sink);
    }

    async fn on_unregister(&self) {}

    async fn on_receive(
        &self,
        source: Eid,
        _expiry: time::OffsetDateTime,
        _ack_requested: bool,
        payload: Bytes,
    ) {
        let _ = self.received_tx.send((source, payload.to_vec())).await;
    }

    async fn on_status_notify(
        &self,
        _bundle_id: &hardy_bpv7::bundle::Id,
        _from: &Eid,
        _kind: StatusNotify,
        _reason: hardy_bpv7::status_report::ReasonCode,
        _timestamp: Option<time::OffsetDateTime>,
    ) {
    }
}

// ---------------------------------------------------------------------------
// Helper: Find a free TCP port on localhost
// ---------------------------------------------------------------------------

/// Binds to port 0 on localhost to find a free port, then releases it.
/// There's a small race window, but it's acceptable for tests.
fn find_free_port() -> u16 {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind to find free port");
    listener.local_addr().unwrap().port()
}

// ---------------------------------------------------------------------------
// Helper: Build a BPA with TCPCLv4 listener on a specific port
// ---------------------------------------------------------------------------

/// Creates a BPA with a TCPCLv4 listener on localhost at the given port.
/// Returns the BPA and the listener address.
async fn setup_bpa_with_tcpclv4() -> (Arc<Bpa>, SocketAddr) {
    let node_id = IpnNodeId {
        allocator_id: 0,
        node_number: 1,
    };
    let node_ids =
        hardy_bpa::node_ids::NodeIds::try_from([NodeId::Ipn(node_id)].as_slice()).unwrap();

    let port = find_free_port();
    let listen_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let tcpclv4_config = hardy_tcpclv4::config::Config {
        address: Some(listen_addr),
        session_defaults: hardy_tcpclv4::config::SessionConfig {
            require_tls: false,
            ..Default::default()
        },
        ..Default::default()
    };

    let cla = Arc::new(
        hardy_tcpclv4::Cla::new(&tcpclv4_config).expect("Failed to create TCPCLv4 CLA"),
    );

    let bpa = Arc::new(
        Bpa::builder()
            .status_reports(false)
            .node_ids(node_ids)
            .cla("tcpclv4".to_string(), cla.clone(), None)
            .build()
            .await
            .expect("Failed to build BPA"),
    );

    bpa.start(false);

    // Wait briefly for the listener to bind and start accepting connections
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    (bpa, listen_addr)
}

// ---------------------------------------------------------------------------
// Helper: Get the path to the `bp` binary
// ---------------------------------------------------------------------------

fn bp_binary_path() -> std::path::PathBuf {
    // When running integration tests, cargo puts the test binary in the same
    // directory as other binaries built by the workspace.
    let mut path = std::env::current_exe()
        .expect("Failed to get current exe path")
        .parent()
        .expect("Failed to get parent directory")
        .parent()
        .expect("Failed to get grandparent directory")
        .to_path_buf();
    path.push("bp");
    path
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test sending an empty file (0 bytes) via `bp send`.
///
/// Validates: Requirements 2.1, 2.4, 2.5
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_empty_file() {
    let (bpa, listen_addr) = setup_bpa_with_tcpclv4().await;

    // Register a test application on service 1 of the BPA's node
    let (app, mut rx) = TestRecvApp::new();
    bpa.register_application(EidService::Ipn(1), app)
        .await
        .expect("Failed to register test app");

    // Create an empty temp file
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("empty.bin");
    std::fs::write(&file_path, b"").unwrap();

    // Run `bp send` as a subprocess
    let bp = bp_binary_path();
    let output = tokio::process::Command::new(&bp)
        .args([
            "send",
            "ipn:0.1.1", // destination: BPA's own node, service 1
            file_path.to_str().unwrap(),
            "--peer",
            &listen_addr.to_string(),
            "--quiet",
        ])
        .output()
        .await
        .expect("Failed to run bp send");

    assert!(
        output.status.success(),
        "bp send failed with exit code {:?}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    // Wait for the bundle to arrive
    let result = tokio::time::timeout(tokio::time::Duration::from_secs(10), rx.recv()).await;

    match result {
        Ok(Some((_source, payload))) => {
            assert_eq!(payload, b"", "Empty file should produce empty payload");
        }
        Ok(None) => panic!("Channel closed without receiving bundle"),
        Err(_) => panic!("Timeout waiting for bundle delivery"),
    }

    bpa.shutdown().await;
}

/// Test sending a small file via `bp send`.
///
/// Validates: Requirements 2.1, 2.4, 2.5
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_small_file() {
    let (bpa, listen_addr) = setup_bpa_with_tcpclv4().await;

    // Register a test application on service 1
    let (app, mut rx) = TestRecvApp::new();
    bpa.register_application(EidService::Ipn(1), app)
        .await
        .expect("Failed to register test app");

    // Create a small test file
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("small.txt");
    let content = b"Hello, Bundle Protocol! This is a small test payload.";
    std::fs::write(&file_path, content).unwrap();

    // Run `bp send`
    let bp = bp_binary_path();
    let output = tokio::process::Command::new(&bp)
        .args([
            "send",
            "ipn:0.1.1",
            file_path.to_str().unwrap(),
            "--peer",
            &listen_addr.to_string(),
            "--quiet",
        ])
        .output()
        .await
        .expect("Failed to run bp send");

    assert!(
        output.status.success(),
        "bp send failed with exit code {:?}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    // Wait for the bundle to arrive
    let result = tokio::time::timeout(tokio::time::Duration::from_secs(10), rx.recv()).await;

    match result {
        Ok(Some((_source, payload))) => {
            assert_eq!(
                payload, content,
                "Received payload should match sent content"
            );
        }
        Ok(None) => panic!("Channel closed without receiving bundle"),
        Err(_) => panic!("Timeout waiting for bundle delivery"),
    }

    bpa.shutdown().await;
}

/// Test sending a large file (64 KB) via `bp send`.
/// This exercises TCPCLv4 segmentation since the default segment MRU is 16 KB.
///
/// Validates: Requirements 2.1, 2.4, 2.5
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_large_file() {
    let (bpa, listen_addr) = setup_bpa_with_tcpclv4().await;

    // Register a test application on service 1
    let (app, mut rx) = TestRecvApp::new();
    bpa.register_application(EidService::Ipn(1), app)
        .await
        .expect("Failed to register test app");

    // Create a large test file (64 KB of patterned data)
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("large.bin");
    let content: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();
    std::fs::write(&file_path, &content).unwrap();

    // Run `bp send`
    let bp = bp_binary_path();
    let output = tokio::process::Command::new(&bp)
        .args([
            "send",
            "ipn:0.1.1",
            file_path.to_str().unwrap(),
            "--peer",
            &listen_addr.to_string(),
            "--quiet",
        ])
        .output()
        .await
        .expect("Failed to run bp send");

    assert!(
        output.status.success(),
        "bp send failed with exit code {:?}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    // Wait for the bundle to arrive (longer timeout for large transfer)
    let result = tokio::time::timeout(tokio::time::Duration::from_secs(15), rx.recv()).await;

    match result {
        Ok(Some((_source, payload))) => {
            assert_eq!(
                payload.len(),
                content.len(),
                "Received payload length should match sent content length"
            );
            assert_eq!(
                payload, content,
                "Received payload should match sent content byte-for-byte"
            );
        }
        Ok(None) => panic!("Channel closed without receiving bundle"),
        Err(_) => panic!("Timeout waiting for bundle delivery"),
    }

    bpa.shutdown().await;
}
