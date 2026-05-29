//! Full Hardy BPA-to-BPA LunarLink test.
//!
//! Creates two real Hardy BPA instances (Ground Station and Spacecraft),
//! each with an LTP CLA, and exchanges bundles between them using the
//! full BPA routing and dispatch pipeline.
//!
//! Architecture:
//! ```text
//! Ground Station BPA (ipn:2.0)  ←── LTP/UDP ──→  Spacecraft BPA (ipn:3.0)
//!   port 11001                                      port 11002
//! ```
//!
//! Run with: `cargo test -p hardy-ltp-cla --test lunar_link_bpa_test -- --nocapture`

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hardy_bpa::async_trait;
use hardy_bpa::builder::BpaBuilder;
use hardy_bpa::node_ids::NodeIds;
use hardy_bpa::services::{Service, ServiceSink, StatusNotify};
use hardy_bpv7::eid::{Eid, IpnNodeId, NodeId, Service as EidService};
use hardy_ltp_cla::cla::LtpCla;
use hardy_ltp_cla::config::{Config, SpanConfig};
use tokio::sync::Notify;

// Fixed ports for the test (avoids chicken-and-egg problem).
const GROUND_LTP_PORT: u16 = 11001;
const SPACECRAFT_LTP_PORT: u16 = 11002;

// Service number for our capture service.
const CAPTURE_SERVICE_NUM: u32 = 42;

// ---------------------------------------------------------------------------
// CaptureService — receives bundles delivered by the BPA dispatcher
// ---------------------------------------------------------------------------

/// A low-level Service that captures received bundles for test assertion.
/// Stores the ServiceSink for sending bundles back through the BPA.
struct CaptureService {
    bundles: tokio::sync::Mutex<Vec<Bytes>>,
    notify: Notify,
    sink: tokio::sync::Mutex<Option<Box<dyn ServiceSink>>>,
}

impl CaptureService {
    fn new() -> Self {
        Self {
            bundles: tokio::sync::Mutex::new(Vec::new()),
            notify: Notify::new(),
            sink: tokio::sync::Mutex::new(None),
        }
    }

    /// Wait for at least one bundle to arrive, with a timeout.
    async fn wait_for_bundle(&self, timeout: Duration) -> Option<Bytes> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            {
                let bundles = self.bundles.lock().await;
                if let Some(b) = bundles.last() {
                    return Some(b.clone());
                }
            }
            tokio::select! {
                _ = self.notify.notified() => {
                    let bundles = self.bundles.lock().await;
                    if let Some(b) = bundles.last() {
                        return Some(b.clone());
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return None;
                }
            }
        }
    }

    /// Send a pre-built bundle through the BPA via the ServiceSink.
    async fn send_bundle(&self, bundle_bytes: Bytes) {
        let sink = self.sink.lock().await;
        if let Some(ref s) = *sink {
            s.send(bundle_bytes)
                .await
                .expect("Failed to send bundle via ServiceSink");
        } else {
            panic!("CaptureService: sink not available (service not registered)");
        }
    }
}

#[async_trait]
impl Service for CaptureService {
    async fn on_register(&self, _endpoint: &Eid, sink: Box<dyn ServiceSink>) {
        *self.sink.lock().await = Some(sink);
    }

    async fn on_unregister(&self) {
        *self.sink.lock().await = None;
    }

    async fn on_receive(&self, data: Bytes, _expiry: time::OffsetDateTime) {
        self.bundles.lock().await.push(data);
        self.notify.notify_waiters();
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
// Helper: build a BPv7 bundle as raw CBOR bytes
// ---------------------------------------------------------------------------

fn build_bundle(source: &Eid, destination: &Eid, payload: &[u8]) -> Bytes {
    let (_, data) = hardy_bpv7::builder::Builder::new(source.clone(), destination.clone())
        .with_payload(std::borrow::Cow::Borrowed(payload))
        .build(hardy_bpv7::creation_timestamp::CreationTimestamp::now())
        .expect("Failed to build bundle");
    Bytes::from(data)
}

// ---------------------------------------------------------------------------
// Test: Command from Ground Station to Spacecraft via full BPA + LTP
// ---------------------------------------------------------------------------

/// Sends a command bundle from Ground Station (ipn:2.42) to Spacecraft (ipn:3.42)
/// through the full BPA routing pipeline and LTP transport layer.
///
/// Flow:
/// 1. Ground CaptureService sends bundle via ServiceSink
/// 2. Ground BPA routes to ipn:3.0 → finds LTP CLA peer (engine 3)
/// 3. LTP CLA aggregates and transmits via UDP to port 11002
/// 4. Spacecraft LTP CLA receives, reassembles, dispatches to BPA
/// 5. Spacecraft BPA delivers to service 42 → CaptureService receives
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lunar_link_bpa_command_to_spacecraft() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hardy_ltp_cla=debug,hardy_bpa=info")
        .with_test_writer()
        .try_init();

    eprintln!();
    eprintln!("=== LunarLink Full-BPA Test ===");
    eprintln!("    Ground Station: ipn:2.0 (LTP port {})", GROUND_LTP_PORT);
    eprintln!("    Spacecraft: ipn:3.0 (LTP port {})", SPACECRAFT_LTP_PORT);
    eprintln!();

    // --- Build Ground Station BPA (ipn:2.0) ---
    let ground_ltp_config = Config {
        bind: format!("0.0.0.0:{}", GROUND_LTP_PORT).parse().unwrap(),
        engine_id: Some(2),
        client_service_id: 1,
        spans: vec![SpanConfig {
            engine_id: 3,
            address: format!("127.0.0.1:{}", SPACECRAFT_LTP_PORT)
                .parse()
                .unwrap(),
            node_ids: vec!["ipn:3.0".to_string()],
            max_segment_size: 1400,
            max_retransmissions: 5,
            retransmit_cycle_secs: 10,
            aggr_size_limit: 65536,
            aggr_time_limit_secs: 0,         // immediate flush
            one_way_light_time_ms: Some(50), // 50ms for fast test
            one_way_margin_time_ms: 20,
            ..Default::default()
        }],
    };

    let ground_capture = Arc::new(CaptureService::new());
    let ground_node_ids = NodeIds::try_from(
        [NodeId::Ipn(IpnNodeId {
            allocator_id: 0,
            node_number: 2,
        })]
        .as_slice(),
    )
    .unwrap();

    let ground_bpa = BpaBuilder::new()
        .node_ids(ground_node_ids)
        .cla("ltp0", Arc::new(LtpCla::new(ground_ltp_config)), None)
        .service(ground_capture.clone(), EidService::Ipn(CAPTURE_SERVICE_NUM))
        .build()
        .await
        .expect("Failed to build Ground Station BPA");

    ground_bpa.start(false);

    // --- Build Spacecraft BPA (ipn:3.0) ---
    let spacecraft_ltp_config = Config {
        bind: format!("0.0.0.0:{}", SPACECRAFT_LTP_PORT).parse().unwrap(),
        engine_id: Some(3),
        client_service_id: 1,
        spans: vec![SpanConfig {
            engine_id: 2,
            address: format!("127.0.0.1:{}", GROUND_LTP_PORT).parse().unwrap(),
            node_ids: vec!["ipn:2.0".to_string()],
            max_segment_size: 1400,
            max_retransmissions: 5,
            retransmit_cycle_secs: 10,
            aggr_size_limit: 65536,
            aggr_time_limit_secs: 0,
            one_way_light_time_ms: Some(50),
            one_way_margin_time_ms: 20,
            ..Default::default()
        }],
    };

    let spacecraft_capture = Arc::new(CaptureService::new());
    let spacecraft_node_ids = NodeIds::try_from(
        [NodeId::Ipn(IpnNodeId {
            allocator_id: 0,
            node_number: 3,
        })]
        .as_slice(),
    )
    .unwrap();

    let spacecraft_bpa = BpaBuilder::new()
        .node_ids(spacecraft_node_ids)
        .cla("ltp0", Arc::new(LtpCla::new(spacecraft_ltp_config)), None)
        .service(
            spacecraft_capture.clone(),
            EidService::Ipn(CAPTURE_SERVICE_NUM),
        )
        .build()
        .await
        .expect("Failed to build Spacecraft BPA");

    spacecraft_bpa.start(false);

    // Allow both BPAs to fully initialize (CLA registration, peer setup).
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- Send a command from Ground Station to Spacecraft ---
    eprintln!("[Ground Station] Sending command to spacecraft (ipn:3.42)...");

    let source: Eid = "ipn:2.42".parse().unwrap();
    let destination: Eid = "ipn:3.42".parse().unwrap();
    let command_payload = b"LUNAR_CMD: Activate science instrument Alpha";
    let bundle_bytes = build_bundle(&source, &destination, command_payload);

    // Send through the ground station's service sink → BPA routes → LTP → spacecraft
    ground_capture.send_bundle(bundle_bytes).await;

    // --- Wait for delivery at the spacecraft ---
    let delivered = spacecraft_capture
        .wait_for_bundle(Duration::from_secs(10))
        .await;

    if let Some(bundle) = delivered {
        eprintln!(
            "[Spacecraft] Received bundle ({} bytes) via LTP",
            bundle.len()
        );
        eprintln!("=== Command delivered successfully via full BPA + LTP pipeline ===");
    } else {
        // Shutdown before panicking for clean resource release
        ground_bpa.shutdown().await;
        spacecraft_bpa.shutdown().await;
        panic!("Command was NOT delivered to spacecraft within 10 seconds");
    }

    // --- Cleanup ---
    ground_bpa.shutdown().await;
    spacecraft_bpa.shutdown().await;

    eprintln!();
}

// ---------------------------------------------------------------------------
// Test: Telemetry from Spacecraft to Ground Station via full BPA + LTP
// ---------------------------------------------------------------------------

/// Sends a telemetry bundle from Spacecraft (ipn:3.42) to Ground Station (ipn:2.42)
/// through the full BPA routing pipeline and LTP transport layer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lunar_link_bpa_telemetry_to_ground() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hardy_ltp_cla=debug,hardy_bpa=info")
        .with_test_writer()
        .try_init();

    eprintln!();
    eprintln!("=== LunarLink Full-BPA Telemetry Test ===");
    eprintln!(
        "    Ground Station: ipn:2.0 (LTP port {})",
        GROUND_LTP_PORT + 10
    );
    eprintln!(
        "    Spacecraft: ipn:3.0 (LTP port {})",
        SPACECRAFT_LTP_PORT + 10
    );
    eprintln!();

    // Use offset ports to avoid conflict with the other test running in parallel.
    let ground_port = GROUND_LTP_PORT + 10;
    let spacecraft_port = SPACECRAFT_LTP_PORT + 10;

    // --- Build Ground Station BPA ---
    let ground_ltp_config = Config {
        bind: format!("0.0.0.0:{}", ground_port).parse().unwrap(),
        engine_id: Some(2),
        client_service_id: 1,
        spans: vec![SpanConfig {
            engine_id: 3,
            address: format!("127.0.0.1:{}", spacecraft_port).parse().unwrap(),
            node_ids: vec!["ipn:3.0".to_string()],
            max_segment_size: 1400,
            max_retransmissions: 5,
            retransmit_cycle_secs: 10,
            aggr_size_limit: 65536,
            aggr_time_limit_secs: 0,
            one_way_light_time_ms: Some(50),
            one_way_margin_time_ms: 20,
            ..Default::default()
        }],
    };

    let ground_capture = Arc::new(CaptureService::new());
    let ground_node_ids = NodeIds::try_from(
        [NodeId::Ipn(IpnNodeId {
            allocator_id: 0,
            node_number: 2,
        })]
        .as_slice(),
    )
    .unwrap();

    let ground_bpa = BpaBuilder::new()
        .node_ids(ground_node_ids)
        .cla("ltp0", Arc::new(LtpCla::new(ground_ltp_config)), None)
        .service(ground_capture.clone(), EidService::Ipn(CAPTURE_SERVICE_NUM))
        .build()
        .await
        .expect("Failed to build Ground Station BPA");

    ground_bpa.start(false);

    // --- Build Spacecraft BPA ---
    let spacecraft_ltp_config = Config {
        bind: format!("0.0.0.0:{}", spacecraft_port).parse().unwrap(),
        engine_id: Some(3),
        client_service_id: 1,
        spans: vec![SpanConfig {
            engine_id: 2,
            address: format!("127.0.0.1:{}", ground_port).parse().unwrap(),
            node_ids: vec!["ipn:2.0".to_string()],
            max_segment_size: 1400,
            max_retransmissions: 5,
            retransmit_cycle_secs: 10,
            aggr_size_limit: 65536,
            aggr_time_limit_secs: 0,
            one_way_light_time_ms: Some(50),
            one_way_margin_time_ms: 20,
            ..Default::default()
        }],
    };

    let spacecraft_capture = Arc::new(CaptureService::new());
    let spacecraft_node_ids = NodeIds::try_from(
        [NodeId::Ipn(IpnNodeId {
            allocator_id: 0,
            node_number: 3,
        })]
        .as_slice(),
    )
    .unwrap();

    let spacecraft_bpa = BpaBuilder::new()
        .node_ids(spacecraft_node_ids)
        .cla("ltp0", Arc::new(LtpCla::new(spacecraft_ltp_config)), None)
        .service(
            spacecraft_capture.clone(),
            EidService::Ipn(CAPTURE_SERVICE_NUM),
        )
        .build()
        .await
        .expect("Failed to build Spacecraft BPA");

    spacecraft_bpa.start(false);

    // Allow initialization.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- Send telemetry from Spacecraft to Ground Station ---
    eprintln!("[Spacecraft] Sending telemetry to ground station (ipn:2.42)...");

    let source: Eid = "ipn:3.42".parse().unwrap();
    let destination: Eid = "ipn:2.42".parse().unwrap();

    // 10 KB telemetry payload (multi-segment LTP transfer).
    let telemetry_payload: Vec<u8> = (0..10 * 1024)
        .map(|i| ((i * 7 + 0xAB) % 256) as u8)
        .collect();
    let bundle_bytes = build_bundle(&source, &destination, &telemetry_payload);

    spacecraft_capture.send_bundle(bundle_bytes).await;

    // --- Wait for delivery at the ground station ---
    let delivered = ground_capture
        .wait_for_bundle(Duration::from_secs(10))
        .await;

    if let Some(bundle) = delivered {
        eprintln!(
            "[Ground Station] Received telemetry bundle ({} bytes) via LTP",
            bundle.len()
        );
        eprintln!("=== Telemetry delivered successfully via full BPA + LTP pipeline ===");
    } else {
        ground_bpa.shutdown().await;
        spacecraft_bpa.shutdown().await;
        panic!("Telemetry was NOT delivered to ground station within 10 seconds");
    }

    // --- Cleanup ---
    ground_bpa.shutdown().await;
    spacecraft_bpa.shutdown().await;

    eprintln!();
}
