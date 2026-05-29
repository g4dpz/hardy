//! Full-stack integration test: BPv7 Bundle ↔ LTP ↔ BPv7 Bundle
//!
//! Demonstrates that the LTP stack correctly transports real BPv7-encoded
//! bundles between two endpoints. Creates two LTP Span instances on localhost,
//! constructs a valid BPv7 bundle using `hardy_bpv7::builder::Builder`, sends
//! it through the full LTP pipeline (aggregation → export session → UDP →
//! receive loop → import session → block delivery), and verifies the bundle
//! arrives intact and can be decoded as a valid BPv7 bundle.
//!
//! Run with: `cargo test -p hardy-ltp-cla full_stack -- --nocapture`

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hardy_bpa::async_trait;
use hardy_bpa::cla::{ClaAddress, Sink};
use hardy_bpv7::bpsec;
use hardy_bpv7::builder::Builder;
use hardy_bpv7::bundle::ParsedBundle;
use hardy_bpv7::creation_timestamp::CreationTimestamp;
use hardy_bpv7::eid::Eid;
use hardy_ltp::segment::{self, Segment};
use hardy_ltp_cla::config::SpanConfig;
use hardy_ltp_cla::span::Span;
use tokio::net::UdpSocket;
use tokio::sync::Notify;

// ---------------------------------------------------------------------------
// Capture Sink — records delivered bundles for assertion
// ---------------------------------------------------------------------------

struct CaptureSink {
    bundles: tokio::sync::Mutex<Vec<Bytes>>,
    notify: Notify,
}

impl CaptureSink {
    fn new() -> Self {
        Self {
            bundles: tokio::sync::Mutex::new(Vec::new()),
            notify: Notify::new(),
        }
    }

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
}

#[async_trait]
impl Sink for CaptureSink {
    async fn unregister(&self) {}

    async fn dispatch(
        &self,
        bundle: Bytes,
        _peer_node: Option<&hardy_bpv7::eid::NodeId>,
        _peer_addr: Option<&ClaAddress>,
    ) -> hardy_bpa::cla::Result<()> {
        self.bundles.lock().await.push(bundle);
        self.notify.notify_waiters();
        Ok(())
    }

    async fn add_peer(
        &self,
        _addr: ClaAddress,
        _node_ids: &[hardy_bpv7::eid::NodeId],
    ) -> hardy_bpa::cla::Result<bool> {
        Ok(true)
    }

    async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// Receive loop (routes LTP segments to the correct span)
// ---------------------------------------------------------------------------

async fn test_receive_loop(socket: Arc<UdpSocket>, spans: HashMap<u64, Arc<Span>>) {
    let mut buf = vec![0u8; 65536];
    loop {
        let (len, _src) = match socket.recv_from(&mut buf).await {
            Ok(r) => r,
            Err(_) => break,
        };

        let datagram = Bytes::copy_from_slice(&buf[..len]);
        let mut cursor = datagram;
        let seg = match segment::decode(&mut cursor) {
            Ok(s) => s,
            Err(_) => continue,
        };

        match seg {
            Segment::Data {
                session_id,
                segment_type,
                client_service_id,
                offset,
                data,
                checkpoint,
            } => {
                if let Some(span) = spans.get(&session_id.engine_id) {
                    span.on_import_data_segment(
                        session_id.session_number,
                        segment_type,
                        client_service_id,
                        offset,
                        &data,
                        checkpoint,
                    )
                    .await;
                }
            }
            Segment::Report {
                session_id,
                report_serial,
                checkpoint_serial,
                upper_bound,
                lower_bound,
                claims,
            } => {
                if let Some(span) = spans.get(&session_id.engine_id) {
                    span.on_export_report(
                        session_id.session_number,
                        report_serial,
                        checkpoint_serial,
                        upper_bound,
                        lower_bound,
                        &claims,
                    )
                    .await;
                }
            }
            Segment::ReportAck {
                session_id,
                report_serial,
            } => {
                if let Some(span) = spans.get(&session_id.engine_id) {
                    span.on_import_report_ack(session_id.session_number, report_serial)
                        .await;
                }
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: build a valid BPv7 bundle as raw bytes
// ---------------------------------------------------------------------------

fn build_bpv7_bundle(source: &Eid, destination: &Eid, payload: &[u8]) -> Bytes {
    let (_, data) = Builder::new(source.clone(), destination.clone())
        .with_payload(std::borrow::Cow::Borrowed(payload))
        .build(CreationTimestamp::now())
        .expect("Failed to build BPv7 bundle");
    Bytes::from(data)
}

// ---------------------------------------------------------------------------
// Helper: create a connected span pair with receive loops
// ---------------------------------------------------------------------------

async fn create_span_pair() -> (
    Arc<Span>,
    Arc<CaptureSink>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let socket1 = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let socket2 = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let addr1 = socket1.local_addr().unwrap();
    let addr2 = socket2.local_addr().unwrap();

    let sink1: Arc<dyn Sink> = Arc::new(CaptureSink::new());
    let capture_sink = Arc::new(CaptureSink::new());
    let sink2: Arc<dyn Sink> = capture_sink.clone();

    let config1 = SpanConfig {
        engine_id: 2,
        address: addr2,
        max_segment_size: 1400,
        max_retransmissions: 5,
        retransmit_cycle_secs: 5,
        aggr_size_limit: 65536,
        aggr_time_limit_secs: 0, // immediate flush
        max_import_sessions: 100,
        max_export_sessions: 100,
        ..Default::default()
    };
    let span1 = Arc::new(Span::new(config1, 1, socket1.clone(), sink1));

    let config2 = SpanConfig {
        engine_id: 1,
        address: addr1,
        max_segment_size: 1400,
        max_retransmissions: 5,
        retransmit_cycle_secs: 5,
        aggr_size_limit: 65536,
        aggr_time_limit_secs: 0,
        max_import_sessions: 100,
        max_export_sessions: 100,
        ..Default::default()
    };
    let span2 = Arc::new(Span::new(config2, 2, socket2.clone(), sink2));

    let mut spans_for_socket2: HashMap<u64, Arc<Span>> = HashMap::new();
    spans_for_socket2.insert(1, span2.clone());

    let mut spans_for_socket1: HashMap<u64, Arc<Span>> = HashMap::new();
    spans_for_socket1.insert(1, span1.clone());

    let recv1 = tokio::spawn(test_receive_loop(socket1, spans_for_socket1));
    let recv2 = tokio::spawn(test_receive_loop(socket2, spans_for_socket2));

    (span1, capture_sink, recv1, recv2)
}

// ---------------------------------------------------------------------------
// Full-Stack Tests
// ---------------------------------------------------------------------------

/// Send a real BPv7 bundle through the LTP stack and verify it arrives intact
/// and can be decoded as a valid bundle with correct source, destination, and payload.
#[tokio::test]
async fn full_stack_bpv7_bundle_over_ltp() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hardy_ltp_cla=debug,hardy_ltp=debug")
        .with_test_writer()
        .try_init();

    let (span1, capture_sink, recv1, recv2) = create_span_pair().await;

    // Build a real BPv7 bundle.
    let source: Eid = "ipn:1.1".parse().unwrap();
    let destination: Eid = "ipn:2.7".parse().unwrap();
    let payload = b"Hello from LTP full-stack test!";
    let bundle_bytes = build_bpv7_bundle(&source, &destination, payload);

    eprintln!(
        "--- Sending BPv7 bundle ({} bytes) over LTP ---",
        bundle_bytes.len()
    );
    eprintln!("    Source: {source}");
    eprintln!("    Destination: {destination}");

    // Verify the bundle is valid before sending.
    let pre_check = ParsedBundle::parse(&bundle_bytes, bpsec::no_keys)
        .expect("Pre-send: bundle should be valid BPv7");
    assert_eq!(pre_check.bundle.id.source, source);
    assert_eq!(pre_check.bundle.destination, destination);

    // Inject into LTP span and flush.
    let block = {
        let mut agg = span1.aggregation.lock().unwrap();
        agg.append(&bundle_bytes);
        agg.flush().expect("buffer should have data after append")
    };

    // Create export session — sends data segments via UDP.
    span1.create_export_session(block).await;

    // Wait for delivery.
    let delivered = capture_sink
        .wait_for_bundle(Duration::from_secs(5))
        .await
        .expect("BPv7 bundle should be delivered within 5 seconds");

    recv1.abort();
    recv2.abort();

    // Verify the delivered bytes are identical to what was sent.
    assert_eq!(
        delivered, bundle_bytes,
        "Delivered bytes should match the original bundle"
    );

    // Parse the delivered bundle and verify its structure.
    let parsed = ParsedBundle::parse(&delivered, bpsec::no_keys)
        .expect("Delivered data should be a valid BPv7 bundle");

    assert_eq!(parsed.bundle.id.source, source, "Source EID mismatch");
    assert_eq!(
        parsed.bundle.destination, destination,
        "Destination EID mismatch"
    );

    // Extract and verify the payload from the bundle bytes using the block's data range.
    // The data range includes the CBOR byte-string encoding of the payload.
    let payload_block = parsed
        .bundle
        .blocks
        .values()
        .find(|b| b.block_type == hardy_bpv7::block::Type::Payload)
        .expect("Bundle should have a payload block");
    // Verify the payload block data range is non-empty (payload exists).
    assert!(
        !payload_block.data.is_empty(),
        "Payload block data range should be non-empty"
    );

    eprintln!("--- BPv7 bundle delivered and verified successfully over LTP ---");
}

/// Send multiple BPv7 bundles aggregated into a single LTP block.
/// Verifies that each bundle is individually delivered and decodable.
#[tokio::test]
async fn full_stack_multiple_bpv7_bundles_aggregated() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hardy_ltp_cla=debug")
        .with_test_writer()
        .try_init();

    let (span1, capture_sink, recv1, recv2) = create_span_pair().await;

    // Build several BPv7 bundles with different payloads.
    let source: Eid = "ipn:1.1".parse().unwrap();
    let bundles: Vec<(Eid, Bytes)> = (0..5)
        .map(|i| {
            let dest: Eid = format!("ipn:2.{}", 10 + i).parse().unwrap();
            let payload = format!("Bundle payload #{i} via LTP aggregation");
            let bundle_bytes = build_bpv7_bundle(&source, &dest, payload.as_bytes());
            (dest, bundle_bytes)
        })
        .collect();

    eprintln!(
        "--- Sending {} BPv7 bundles aggregated into one LTP block ---",
        bundles.len()
    );

    // Aggregate all bundles into one block.
    let block = {
        let mut agg = span1.aggregation.lock().unwrap();
        for (_, bundle_bytes) in &bundles {
            let flushed = agg.append(bundle_bytes);
            assert!(flushed.is_none(), "should not flush mid-aggregation");
        }
        agg.flush().expect("buffer should have data")
    };

    span1.create_export_session(block).await;

    // Wait for all bundles to be delivered.
    tokio::time::sleep(Duration::from_secs(3)).await;

    recv1.abort();
    recv2.abort();

    let delivered_bundles = capture_sink.bundles.lock().await;
    assert_eq!(
        delivered_bundles.len(),
        bundles.len(),
        "Should deliver all {} bundles",
        bundles.len()
    );

    // Verify each delivered bundle is valid BPv7 with correct metadata.
    for (i, (delivered, (expected_dest, original_bytes))) in
        delivered_bundles.iter().zip(bundles.iter()).enumerate()
    {
        assert_eq!(
            delivered, original_bytes,
            "Bundle {i}: delivered bytes should match original"
        );

        let parsed = ParsedBundle::parse(delivered, bpsec::no_keys)
            .unwrap_or_else(|e| panic!("Bundle {i}: should be valid BPv7: {e}"));

        assert_eq!(
            parsed.bundle.id.source, source,
            "Bundle {i}: source mismatch"
        );
        assert_eq!(
            parsed.bundle.destination, *expected_dest,
            "Bundle {i}: destination mismatch"
        );

        let payload_block = parsed
            .bundle
            .blocks
            .values()
            .find(|b| b.block_type == hardy_bpv7::block::Type::Payload)
            .unwrap_or_else(|| panic!("Bundle {i}: should have payload block"));
        assert!(
            !payload_block.data.is_empty(),
            "Bundle {i}: payload block data range should be non-empty"
        );
    }

    eprintln!(
        "--- All {} BPv7 bundles delivered and verified ---",
        bundles.len()
    );
}

/// Send a large BPv7 bundle (64 KB payload) that requires multi-segment LTP
/// transmission. Verifies the reassembled bundle is valid BPv7.
#[tokio::test]
async fn full_stack_large_bpv7_bundle_multi_segment() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hardy_ltp_cla=debug")
        .with_test_writer()
        .try_init();

    let (span1, capture_sink, recv1, recv2) = create_span_pair().await;

    // Build a bundle with a 64 KB payload — will require ~47 LTP segments.
    let source: Eid = "ipn:1.1".parse().unwrap();
    let destination: Eid = "ipn:2.7".parse().unwrap();
    let large_payload: Vec<u8> = (0..65536u32).map(|i| (i % 256) as u8).collect();
    let bundle_bytes = build_bpv7_bundle(&source, &destination, &large_payload);

    eprintln!(
        "--- Sending large BPv7 bundle ({} bytes, ~{} LTP segments) ---",
        bundle_bytes.len(),
        bundle_bytes.len().div_ceil(1400_usize)
    );

    let block = {
        let mut agg = span1.aggregation.lock().unwrap();
        agg.append(&bundle_bytes);
        agg.flush().expect("buffer should have data")
    };

    span1.create_export_session(block).await;

    let delivered = capture_sink
        .wait_for_bundle(Duration::from_secs(10))
        .await
        .expect("Large BPv7 bundle should be delivered within 10 seconds");

    recv1.abort();
    recv2.abort();

    // Verify byte-for-byte integrity.
    assert_eq!(
        delivered, bundle_bytes,
        "Delivered bytes should match the original large bundle"
    );

    // Parse and verify the bundle structure.
    let parsed = ParsedBundle::parse(&delivered, bpsec::no_keys)
        .expect("Delivered large bundle should be valid BPv7");

    assert_eq!(parsed.bundle.id.source, source);
    assert_eq!(parsed.bundle.destination, destination);

    let payload_block = parsed
        .bundle
        .blocks
        .values()
        .find(|b| b.block_type == hardy_bpv7::block::Type::Payload)
        .expect("Large bundle should have a payload block");
    assert!(
        !payload_block.data.is_empty(),
        "Payload block data range should be non-empty"
    );

    eprintln!(
        "--- Large BPv7 bundle ({} bytes) delivered and verified ---",
        bundle_bytes.len()
    );
}
