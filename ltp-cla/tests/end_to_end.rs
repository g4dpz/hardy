// Copyright 2026 David Johnson, G4DPZ, AMSAT-UK
// SPDX-License-Identifier: Apache-2.0

//! End-to-end integration test for LTP CLA.
//!
//! Creates two Span instances on localhost UDP ports, sends a bundle
//! from one to the other via the full LTP stack (aggregation → export
//! session → UDP → receive loop → import session → block delivery),
//! and verifies the bundle arrives intact.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hardy_bpa::async_trait;
use hardy_bpa::cla::{ClaAddress, Sink};
use hardy_ltp::segment::{self, Segment};
use hardy_ltp_cla::config::SpanConfig;
use hardy_ltp_cla::span::Span;
use tokio::net::UdpSocket;
use tokio::sync::Notify;

// ---------------------------------------------------------------------------
// Capture Sink — records delivered bundles for assertion
// ---------------------------------------------------------------------------

/// A mock Sink that captures all dispatched bundles and notifies waiters.
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

    /// Wait until at least one bundle has been delivered, with a timeout.
    async fn wait_for_bundle(&self, timeout: Duration) -> Option<Bytes> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // Check if we already have a bundle.
            {
                let bundles = self.bundles.lock().await;
                if let Some(b) = bundles.last() {
                    return Some(b.clone());
                }
            }
            // Wait for notification or timeout.
            tokio::select! {
                _ = self.notify.notified() => {
                    let bundles = self.bundles.lock().await;
                    if let Some(b) = bundles.last() {
                        return Some(b.clone());
                    }
                    // Spurious wake — loop again.
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
// Minimal receive loop for the test
// ---------------------------------------------------------------------------

/// A minimal receive loop that reads UDP datagrams, decodes LTP segments,
/// and routes them to the appropriate span handler.
///
/// The `spans` map is keyed by the engine_id that appears in incoming segments'
/// session_id field. For data segments this is the sender's engine ID; for
/// reports/report-acks it's the original session creator's engine ID.
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
            // Cancel segments are not expected in this happy-path test.
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Integration Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ltp_end_to_end_bundle_delivery() {
    // Initialize tracing so we can see the LTP exchange in test output.
    // Run with: cargo test -p hardy-ltp-cla end_to_end -- --nocapture
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hardy_ltp_cla=debug,hardy_ltp=debug")
        .with_test_writer()
        .try_init();

    // Bind two UDP sockets on random localhost ports.
    let socket1 = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let socket2 = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let addr1 = socket1.local_addr().unwrap();
    let addr2 = socket2.local_addr().unwrap();

    // Create sinks. Span 2's sink captures delivered bundles.
    let sink1: Arc<dyn Sink> = Arc::new(CaptureSink::new());
    let capture_sink = Arc::new(CaptureSink::new());
    let sink2: Arc<dyn Sink> = capture_sink.clone();

    // Span 1 (local engine 1) → sends to Span 2 (remote engine 2).
    let config1 = SpanConfig {
        engine_id: 2, // remote engine ID that Span 1 talks to
        address: addr2,
        max_segment_size: 1400,
        max_retransmissions: 3,
        retransmit_cycle_secs: 5,
        aggr_size_limit: 65536,
        aggr_time_limit_secs: 0, // immediate flush on forward
        max_import_sessions: 100,
        max_export_sessions: 100,
        ..Default::default()
    };
    let span1 = Arc::new(Span::new(config1, 1, socket1.clone(), sink1));

    // Span 2 (local engine 2) → receives from Span 1 (remote engine 1).
    let config2 = SpanConfig {
        engine_id: 1, // remote engine ID that Span 2 talks to
        address: addr1,
        max_segment_size: 1400,
        max_retransmissions: 3,
        retransmit_cycle_secs: 5,
        aggr_size_limit: 65536,
        aggr_time_limit_secs: 0,
        max_import_sessions: 100,
        max_export_sessions: 100,
        ..Default::default()
    };
    let span2 = Arc::new(Span::new(config2, 2, socket2.clone(), sink2));

    // Build span maps for the receive loops.
    //
    // Span 1 creates export sessions with session_id.engine_id = 1 (its local engine).
    // Data segments sent to socket2 have session_id.engine_id = 1.
    // Reports sent back to socket1 also have session_id.engine_id = 1.
    //
    // Socket 2's receive loop: route engine_id=1 → span2 (handles imports from engine 1).
    let mut spans_for_socket2: HashMap<u64, Arc<Span>> = HashMap::new();
    spans_for_socket2.insert(1, span2.clone());

    // Socket 1's receive loop: route engine_id=1 → span1 (owns the export session).
    let mut spans_for_socket1: HashMap<u64, Arc<Span>> = HashMap::new();
    spans_for_socket1.insert(1, span1.clone());

    // Spawn receive loops.
    let recv1_handle = tokio::spawn(test_receive_loop(socket1.clone(), spans_for_socket1));
    let recv2_handle = tokio::spawn(test_receive_loop(socket2.clone(), spans_for_socket2));

    // Prepare a test bundle payload.
    let test_bundle = Bytes::from(vec![
        0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
    ]);

    // Inject the bundle into Span 1's aggregation buffer and flush immediately.
    let block = {
        let mut agg = span1.aggregation.lock().unwrap();
        agg.append(&test_bundle);
        agg.flush().expect("buffer should have data after append")
    };

    // Create an export session — this sends the data segments via UDP to socket2.
    span1.create_export_session(block).await;

    // Wait for the bundle to be delivered to Span 2's capture sink.
    let delivered = capture_sink.wait_for_bundle(Duration::from_secs(5)).await;

    // Clean up: abort receive loops.
    recv1_handle.abort();
    recv2_handle.abort();

    // Assert the bundle was delivered and matches the original.
    let delivered = delivered.expect("bundle should have been delivered within 5 seconds");
    assert_eq!(
        delivered, test_bundle,
        "delivered bundle should match the original"
    );
}

/// Helper: create a pair of connected spans with receive loops for testing.
/// Returns (span1, capture_sink, recv_handles) where span1 is the sender
/// and capture_sink collects bundles delivered to span2.
async fn create_test_pair(
    max_segment_size: usize,
) -> (
    Arc<Span>,
    Arc<CaptureSink>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hardy_ltp_cla=debug")
        .with_test_writer()
        .try_init();

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
        max_segment_size,
        max_retransmissions: 5,
        retransmit_cycle_secs: 5,
        aggr_size_limit: 1_048_576, // 1 MB aggregation limit
        aggr_time_limit_secs: 0,
        max_import_sessions: 100,
        max_export_sessions: 100,
        ..Default::default()
    };
    let span1 = Arc::new(Span::new(config1, 1, socket1.clone(), sink1));

    let config2 = SpanConfig {
        engine_id: 1,
        address: addr1,
        max_segment_size,
        max_retransmissions: 5,
        retransmit_cycle_secs: 5,
        aggr_size_limit: 1_048_576,
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

/// Simulate bpsendfile: send a large payload (like a file) over LTP and verify
/// it arrives intact at the receiver. This exercises multi-segment transmission,
/// report generation, and block reassembly for payloads larger than one segment.
#[tokio::test]
async fn test_ltp_send_file_100kb() {
    let (span1, capture_sink, recv1, recv2) = create_test_pair(1400).await;

    // Create a 100 KB "file" payload with a recognizable pattern.
    let file_size: usize = 100 * 1024;
    let file_data: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
    let file_bundle = Bytes::from(file_data.clone());

    eprintln!(
        "--- Sending {} byte payload over LTP (segment size 1400) ---",
        file_size
    );
    eprintln!(
        "--- Expected segments: ~{} ---",
        (file_size + 4).div_ceil(1400_usize)
    );

    // Inject into aggregation buffer and flush.
    let block = {
        let mut agg = span1.aggregation.lock().unwrap();
        agg.append(&file_bundle);
        agg.flush().expect("buffer should have data")
    };

    // Create export session — sends all segments.
    span1.create_export_session(block).await;

    // Wait for delivery (up to 10 seconds for a large transfer).
    let delivered = capture_sink.wait_for_bundle(Duration::from_secs(10)).await;

    recv1.abort();
    recv2.abort();

    let delivered = delivered.expect("100 KB file should be delivered within 10 seconds");
    assert_eq!(delivered.len(), file_size, "delivered size mismatch");
    assert_eq!(&delivered[..], &file_data[..], "delivered content mismatch");

    eprintln!("--- 100 KB file delivered successfully over LTP ---");
}

/// Send a 1 MB payload — exercises many segments and verifies the full
/// reassembly pipeline handles large blocks correctly.
#[tokio::test]
async fn test_ltp_send_file_1mb() {
    let (span1, capture_sink, recv1, recv2) = create_test_pair(1400).await;

    // Create a 1 MB payload.
    let file_size: usize = 1024 * 1024;
    let file_data: Vec<u8> = (0..file_size).map(|i| ((i * 7 + 13) % 256) as u8).collect();
    let file_bundle = Bytes::from(file_data.clone());

    eprintln!("--- Sending {} byte (1 MB) payload over LTP ---", file_size);
    eprintln!(
        "--- Expected segments: ~{} ---",
        (file_size + 4).div_ceil(1400_usize)
    );

    let block = {
        let mut agg = span1.aggregation.lock().unwrap();
        agg.append(&file_bundle);
        agg.flush().expect("buffer should have data")
    };

    span1.create_export_session(block).await;

    let delivered = capture_sink.wait_for_bundle(Duration::from_secs(30)).await;

    recv1.abort();
    recv2.abort();

    let delivered = delivered.expect("1 MB file should be delivered within 30 seconds");
    assert_eq!(delivered.len(), file_size, "delivered size mismatch");
    assert_eq!(&delivered[..], &file_data[..], "delivered content mismatch");

    eprintln!("--- 1 MB file delivered successfully over LTP ---");
}

/// Send multiple bundles in a single aggregated block — simulates the common
/// case where several small bundles are batched together.
#[tokio::test]
async fn test_ltp_send_multiple_bundles_aggregated() {
    let (span1, capture_sink, recv1, recv2) = create_test_pair(1400).await;

    // Create 10 bundles of varying sizes.
    let bundles: Vec<Vec<u8>> = (0..10)
        .map(|i| {
            let size = 100 + i * 50; // 100, 150, 200, ..., 550 bytes
            (0..size).map(|j| ((i + j) % 256) as u8).collect()
        })
        .collect();

    eprintln!("--- Sending 10 bundles aggregated into one LTP block ---");

    // Aggregate all bundles into one block.
    let block = {
        let mut agg = span1.aggregation.lock().unwrap();
        for bundle in &bundles {
            let flushed = agg.append(bundle);
            assert!(flushed.is_none(), "should not flush mid-aggregation");
        }
        agg.flush().expect("buffer should have data")
    };

    span1.create_export_session(block).await;

    // Wait and collect all delivered bundles.
    tokio::time::sleep(Duration::from_secs(3)).await;

    recv1.abort();
    recv2.abort();

    let delivered_bundles = capture_sink.bundles.lock().await;
    assert_eq!(
        delivered_bundles.len(),
        bundles.len(),
        "should deliver all {} bundles",
        bundles.len()
    );

    for (i, (delivered, original)) in delivered_bundles.iter().zip(bundles.iter()).enumerate() {
        assert_eq!(
            &delivered[..],
            &original[..],
            "bundle {} content mismatch",
            i
        );
    }

    eprintln!("--- All 10 aggregated bundles delivered successfully ---");
}
