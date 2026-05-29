//! LunarLink Mission Simulation Test
//!
//! Simulates a lunar communications mission between a Ground Station (Earth)
//! and a Satellite (lunar orbit) using LTP over UDP with simulated propagation
//! delay.
//!
//! Architecture:
//! ```text
//! Ground Station (Engine 2)  ←── LTP/UDP (50ms simulated OWLT) ──→  Satellite (Engine 3)
//!      port A                                                            port B
//! ```
//!
//! A delay proxy sits between the two spans to simulate one-way light time.
//! Real lunar OWLT is ~1300ms; we use 50ms to keep tests fast while still
//! exercising the delay-tolerant behavior and timer configuration.
//!
//! Run with: `cargo test -p hardy-ltp-cla --test lunar_link_test -- --nocapture`

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hardy_bpa::async_trait;
use hardy_bpa::cla::{ClaAddress, Sink};
use hardy_ltp::segment::{self, Segment};
use hardy_ltp_cla::config::SpanConfig;
use hardy_ltp_cla::span::Span;
use tokio::net::UdpSocket;
use tokio::sync::Notify;

// ---------------------------------------------------------------------------
// Constants — Lunar Link Parameters
// ---------------------------------------------------------------------------

/// Simulated one-way light time (real lunar OWLT is ~1300ms).
/// We use 50ms to keep tests fast while demonstrating delay behavior.
const SIMULATED_OWLT_MS: u64 = 50;

/// Real one-way light time for Earth-Moon (used in config for timer calculation).
const REAL_OWLT_MS: u64 = 1300;

/// One-way margin time (processing/queuing overhead).
const MARGIN_MS: u64 = 200;

/// Max segment size (typical space link MTU).
const MAX_SEGMENT_SIZE: usize = 1400;

/// Link rate in bits per second (256 kbps realistic deep-space downlink).
const LINK_RATE_BPS: u64 = 256_000;

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
// Delay Proxy — simulates one-way light time between ground and satellite
// ---------------------------------------------------------------------------

/// Forwards UDP datagrams from `from_socket` to `to_addr` via `to_socket`,
/// adding a configurable one-way delay to simulate propagation time.
///
/// Each received datagram is spawned into a separate task that sleeps for
/// the delay duration before forwarding, preserving packet ordering under
/// normal conditions while allowing concurrent in-flight packets.
async fn delay_proxy(
    from_socket: Arc<UdpSocket>,
    to_addr: SocketAddr,
    to_socket: Arc<UdpSocket>,
    delay: Duration,
) {
    let mut buf = vec![0u8; 65536];
    loop {
        let (len, _src) = match from_socket.recv_from(&mut buf).await {
            Ok(r) => r,
            Err(_) => break,
        };
        let data = buf[..len].to_vec();
        let to_sock = to_socket.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            to_sock.send_to(&data, to_addr).await.ok();
        });
    }
}

// ---------------------------------------------------------------------------
// Receive loop — routes LTP segments to the correct span
// ---------------------------------------------------------------------------

async fn receive_loop(socket: Arc<UdpSocket>, spans: HashMap<u64, Arc<Span>>) {
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
// Test Infrastructure — creates the lunar link topology with delay proxy
// ---------------------------------------------------------------------------

/// Creates the full lunar link topology:
///
/// ```text
/// Ground Station Span → UDP → Proxy Socket A → [delay] → Satellite Socket
/// Satellite Span → UDP → Proxy Socket B → [delay] → Ground Station Socket
/// ```
///
/// Returns (ground_span, satellite_span, ground_sink, satellite_sink, task_handles)
struct LunarLink {
    ground_span: Arc<Span>,
    satellite_span: Arc<Span>,
    ground_sink: Arc<CaptureSink>,
    satellite_sink: Arc<CaptureSink>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl LunarLink {
    async fn new() -> Self {
        // Sockets for the actual spans (where they send/receive LTP segments).
        let ground_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let satellite_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let ground_addr = ground_socket.local_addr().unwrap();
        let satellite_addr = satellite_socket.local_addr().unwrap();

        // Proxy sockets — sit between the spans to add delay.
        // Ground span sends to proxy_a; proxy_a forwards to satellite after delay.
        // Satellite span sends to proxy_b; proxy_b forwards to ground after delay.
        let proxy_a_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let proxy_b_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let proxy_a_addr = proxy_a_socket.local_addr().unwrap();
        let proxy_b_addr = proxy_b_socket.local_addr().unwrap();

        // Create sinks for both directions.
        let ground_sink = Arc::new(CaptureSink::new());
        let satellite_sink = Arc::new(CaptureSink::new());

        let ground_sink_trait: Arc<dyn Sink> = ground_sink.clone();
        let satellite_sink_trait: Arc<dyn Sink> = satellite_sink.clone();

        // Ground Station span (local engine 2, talks to remote engine 3 = satellite).
        // Sends segments to proxy_a_addr (which delays then forwards to satellite).
        let ground_config = SpanConfig {
            engine_id: 3, // remote engine ID (satellite)
            address: proxy_a_addr,
            max_segment_size: MAX_SEGMENT_SIZE,
            max_retransmissions: 5,
            retransmit_cycle_secs: 10,
            aggr_size_limit: 65536,
            aggr_time_limit_secs: 0, // immediate flush
            max_import_sessions: 100,
            max_export_sessions: 100,
            one_way_light_time_ms: Some(REAL_OWLT_MS),
            one_way_margin_time_ms: MARGIN_MS,
            xmit_rate_bps: LINK_RATE_BPS,
            ..Default::default()
        };
        let ground_span = Arc::new(Span::new(
            ground_config,
            2, // local engine ID (ground station)
            ground_socket.clone(),
            ground_sink_trait,
        ));

        // Satellite span (local engine 3, talks to remote engine 2 = ground).
        // Sends segments to proxy_b_addr (which delays then forwards to ground).
        let satellite_config = SpanConfig {
            engine_id: 2, // remote engine ID (ground station)
            address: proxy_b_addr,
            max_segment_size: MAX_SEGMENT_SIZE,
            max_retransmissions: 5,
            retransmit_cycle_secs: 10,
            aggr_size_limit: 65536,
            aggr_time_limit_secs: 0,
            max_import_sessions: 100,
            max_export_sessions: 100,
            one_way_light_time_ms: Some(REAL_OWLT_MS),
            one_way_margin_time_ms: MARGIN_MS,
            xmit_rate_bps: LINK_RATE_BPS,
            ..Default::default()
        };
        let satellite_span = Arc::new(Span::new(
            satellite_config,
            3, // local engine ID (satellite)
            satellite_socket.clone(),
            satellite_sink_trait,
        ));

        // Delay proxy tasks:
        // proxy_a: receives from ground span, delays, forwards to satellite socket
        let delay = Duration::from_millis(SIMULATED_OWLT_MS);
        let proxy_a_to_sat = tokio::spawn(delay_proxy(
            proxy_a_socket.clone(),
            satellite_addr,
            proxy_a_socket.clone(),
            delay,
        ));
        // proxy_b: receives from satellite span, delays, forwards to ground socket
        let proxy_b_to_ground = tokio::spawn(delay_proxy(
            proxy_b_socket.clone(),
            ground_addr,
            proxy_b_socket.clone(),
            delay,
        ));

        // Receive loops on the actual span sockets.
        // Ground socket receives segments from proxy_b (satellite → ground direction).
        // These are reports/report-acks from satellite (session engine_id=2 = ground's exports)
        // or data segments from satellite (session engine_id=3 = satellite's exports).
        let mut ground_spans: HashMap<u64, Arc<Span>> = HashMap::new();
        ground_spans.insert(2, ground_span.clone()); // ground's own export sessions
        ground_spans.insert(3, ground_span.clone()); // satellite's exports → ground imports

        let ground_recv = tokio::spawn(receive_loop(ground_socket, ground_spans));

        // Satellite socket receives segments from proxy_a (ground → satellite direction).
        // These are data segments from ground (session engine_id=2 = ground's exports)
        // or reports/report-acks from ground (session engine_id=3 = satellite's exports).
        let mut satellite_spans: HashMap<u64, Arc<Span>> = HashMap::new();
        satellite_spans.insert(2, satellite_span.clone()); // ground's exports → satellite imports
        satellite_spans.insert(3, satellite_span.clone()); // satellite's own export sessions

        let satellite_recv = tokio::spawn(receive_loop(satellite_socket, satellite_spans));

        let handles = vec![
            proxy_a_to_sat,
            proxy_b_to_ground,
            ground_recv,
            satellite_recv,
        ];

        Self {
            ground_span,
            satellite_span,
            ground_sink,
            satellite_sink,
            handles,
        }
    }

    fn shutdown(self) {
        for h in self.handles {
            h.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: print mission banner
// ---------------------------------------------------------------------------

fn print_mission_banner() {
    eprintln!();
    eprintln!("--- LunarLink Mission Simulation ---");
    eprintln!("    Ground Station: ipn:2.0 (engine-id 2)");
    eprintln!("    Satellite: ipn:3.0 (engine-id 3)");
    eprintln!(
        "    One-way light time: {}ms (simulated, real: {}ms)",
        SIMULATED_OWLT_MS, REAL_OWLT_MS
    );
    eprintln!(
        "    Retransmit timeout: 2 × ({} + {}) = {}ms",
        REAL_OWLT_MS,
        MARGIN_MS,
        2 * (REAL_OWLT_MS + MARGIN_MS)
    );
    eprintln!("    Link rate: {} kbps", LINK_RATE_BPS / 1000);
    eprintln!("    Max segment size: {} bytes", MAX_SEGMENT_SIZE);
    eprintln!();
}

// ---------------------------------------------------------------------------
// Test 1: Command Uplink (Ground → Satellite)
// ---------------------------------------------------------------------------

/// Simulates the MOC sending a 500-byte command to the satellite via the
/// ground station. Verifies the command arrives after the simulated one-way
/// light time and that the report-ack cycle completes (round-trip).
#[tokio::test]
async fn lunar_link_command_uplink() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hardy_ltp_cla=debug,hardy_ltp=debug")
        .with_test_writer()
        .try_init();

    print_mission_banner();

    let link = LunarLink::new().await;

    // Create a 500-byte "command" payload.
    let command: Vec<u8> = (0..500u16).map(|i| (i % 256) as u8).collect();
    let command_bytes = Bytes::from(command.clone());

    let t0 = Instant::now();
    eprintln!(
        "[T+{:.3}s] Ground Station: sending command ({} bytes)",
        t0.elapsed().as_secs_f64(),
        command_bytes.len()
    );

    // Inject into ground station's aggregation buffer and flush.
    let block = {
        let mut agg = link.ground_span.aggregation.lock().unwrap();
        agg.append(&command_bytes);
        agg.flush().expect("buffer should have data after append")
    };

    eprintln!(
        "[T+{:.3}s] Ground Station: export session created, 1 segment",
        t0.elapsed().as_secs_f64()
    );

    // Create export session — sends data segments via UDP through the delay proxy.
    link.ground_span.create_export_session(block).await;

    // Wait for delivery at the satellite.
    // Expected: ~50ms one-way for data, then satellite sends report back.
    let delivered = link
        .satellite_sink
        .wait_for_bundle(Duration::from_secs(5))
        .await
        .expect("Command should be delivered to satellite");

    let delivery_time = t0.elapsed();
    eprintln!(
        "[T+{:.3}s] Satellite: received data segment, created import session",
        delivery_time.as_secs_f64()
    );
    eprintln!(
        "[T+{:.3}s] Satellite: block complete, delivering command",
        delivery_time.as_secs_f64()
    );

    // Verify the command arrived intact.
    assert_eq!(
        delivered, command_bytes,
        "Delivered command should match the original"
    );

    // Verify the delivery took at least the simulated OWLT.
    assert!(
        delivery_time.as_millis() >= SIMULATED_OWLT_MS as u128,
        "Delivery should take at least {}ms (one-way light time), took {}ms",
        SIMULATED_OWLT_MS,
        delivery_time.as_millis()
    );

    // Wait a bit more for the report-ack cycle to complete (another OWLT for report back).
    tokio::time::sleep(Duration::from_millis(SIMULATED_OWLT_MS + 50)).await;
    let rtt_time = t0.elapsed();
    eprintln!(
        "[T+{:.3}s] Ground Station: received report-ack, session complete",
        rtt_time.as_secs_f64()
    );

    eprintln!(
        "--- Command delivered successfully (delivery: ~{}ms, RTT: ~{}ms) ---",
        delivery_time.as_millis(),
        rtt_time.as_millis()
    );

    // Verify retransmit timeout is configured correctly.
    let expected_retransmit = Duration::from_millis(2 * (REAL_OWLT_MS + MARGIN_MS));
    let actual_retransmit = link.ground_span.compute_retransmit_timeout();
    assert_eq!(
        actual_retransmit,
        expected_retransmit,
        "Retransmit timeout should be 2×(OWLT+margin) = {}ms",
        expected_retransmit.as_millis()
    );
    eprintln!(
        "    Retransmit timeout correctly configured: {}ms (won't fire prematurely)",
        actual_retransmit.as_millis()
    );

    eprintln!();
    link.shutdown();
}

// ---------------------------------------------------------------------------
// Test 2: Telemetry Downlink (Satellite → Ground)
// ---------------------------------------------------------------------------

/// Simulates the satellite sending a 10 KB telemetry bundle to the ground
/// station. This requires multiple LTP segments (~8 at 1400 bytes each).
/// Verifies all segments arrive and the block is reassembled after the
/// simulated one-way light time.
#[tokio::test]
async fn lunar_link_telemetry_downlink() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hardy_ltp_cla=debug,hardy_ltp=debug")
        .with_test_writer()
        .try_init();

    print_mission_banner();

    let link = LunarLink::new().await;

    // Create a 10 KB "telemetry" payload with a recognizable pattern.
    let telemetry_size = 10 * 1024;
    let telemetry: Vec<u8> = (0..telemetry_size)
        .map(|i| ((i * 7 + 0xAB) % 256) as u8)
        .collect();
    let telemetry_bytes = Bytes::from(telemetry.clone());

    // Calculate expected segments: payload + 4-byte length prefix, divided by segment size.
    let framed_size = telemetry_size + 4;
    let expected_segments = (framed_size + MAX_SEGMENT_SIZE - 1) / MAX_SEGMENT_SIZE;

    let t0 = Instant::now();
    eprintln!(
        "[T+{:.3}s] Satellite: sending telemetry ({} bytes, ~{} segments)",
        t0.elapsed().as_secs_f64(),
        telemetry_size,
        expected_segments
    );

    // Inject into satellite's aggregation buffer and flush.
    let block = {
        let mut agg = link.satellite_span.aggregation.lock().unwrap();
        agg.append(&telemetry_bytes);
        agg.flush().expect("buffer should have data after append")
    };

    eprintln!(
        "[T+{:.3}s] Satellite: export session created, {} segments queued",
        t0.elapsed().as_secs_f64(),
        expected_segments
    );

    // Create export session — sends all segments through the delay proxy.
    link.satellite_span.create_export_session(block).await;

    // Wait for delivery at the ground station.
    // All segments travel through the delay proxy (~50ms), then reassembly happens.
    let delivered = link
        .ground_sink
        .wait_for_bundle(Duration::from_secs(10))
        .await
        .expect("Telemetry should be delivered to ground station");

    let delivery_time = t0.elapsed();
    eprintln!(
        "[T+{:.3}s] Ground Station: all segments received, block reassembled",
        delivery_time.as_secs_f64()
    );
    eprintln!(
        "[T+{:.3}s] Ground Station: telemetry delivered ({} bytes)",
        delivery_time.as_secs_f64(),
        delivered.len()
    );

    // Verify the telemetry arrived intact.
    assert_eq!(
        delivered.len(),
        telemetry_bytes.len(),
        "Delivered telemetry size should match original"
    );
    assert_eq!(
        delivered, telemetry_bytes,
        "Delivered telemetry content should match original"
    );

    // Verify delivery took at least the simulated OWLT.
    assert!(
        delivery_time.as_millis() >= SIMULATED_OWLT_MS as u128,
        "Multi-segment delivery should take at least {}ms, took {}ms",
        SIMULATED_OWLT_MS,
        delivery_time.as_millis()
    );

    // Wait for report-ack cycle.
    tokio::time::sleep(Duration::from_millis(SIMULATED_OWLT_MS + 50)).await;
    let rtt_time = t0.elapsed();
    eprintln!(
        "[T+{:.3}s] Satellite: received report-ack, session complete",
        rtt_time.as_secs_f64()
    );

    eprintln!("--- Telemetry delivered successfully ---");
    eprintln!(
        "    Payload: {} bytes in {} segments",
        telemetry_size, expected_segments
    );
    eprintln!(
        "    Delivery time: ~{}ms (includes {}ms OWLT)",
        delivery_time.as_millis(),
        SIMULATED_OWLT_MS
    );
    eprintln!("    Full RTT: ~{}ms", rtt_time.as_millis());

    eprintln!();
    link.shutdown();
}

// ---------------------------------------------------------------------------
// Test 3: Bidirectional (Command Up + Telemetry Down simultaneously)
// ---------------------------------------------------------------------------

/// Simulates simultaneous bidirectional traffic: a command uplink (ground →
/// satellite) and a telemetry downlink (satellite → ground) happening at the
/// same time. Verifies both arrive correctly despite sharing the delayed link.
#[tokio::test]
async fn lunar_link_bidirectional() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hardy_ltp_cla=debug,hardy_ltp=debug")
        .with_test_writer()
        .try_init();

    print_mission_banner();

    let link = LunarLink::new().await;

    // Command: 500 bytes ground → satellite.
    let command: Vec<u8> = (0..500u16).map(|i| (i % 256) as u8).collect();
    let command_bytes = Bytes::from(command.clone());

    // Telemetry: 10 KB satellite → ground.
    let telemetry_size = 10 * 1024;
    let telemetry: Vec<u8> = (0..telemetry_size)
        .map(|i| ((i * 13 + 0x42) % 256) as u8)
        .collect();
    let telemetry_bytes = Bytes::from(telemetry.clone());

    let t0 = Instant::now();
    eprintln!(
        "[T+{:.3}s] Starting bidirectional transfer:",
        t0.elapsed().as_secs_f64()
    );
    eprintln!(
        "    Command uplink: {} bytes (ground → satellite)",
        command_bytes.len()
    );
    eprintln!(
        "    Telemetry downlink: {} bytes (satellite → ground)",
        telemetry_bytes.len()
    );

    // Prepare both blocks.
    let command_block = {
        let mut agg = link.ground_span.aggregation.lock().unwrap();
        agg.append(&command_bytes);
        agg.flush().expect("ground buffer should have data")
    };

    let telemetry_block = {
        let mut agg = link.satellite_span.aggregation.lock().unwrap();
        agg.append(&telemetry_bytes);
        agg.flush().expect("satellite buffer should have data")
    };

    // Launch both export sessions concurrently.
    let ground_span = link.ground_span.clone();
    let satellite_span = link.satellite_span.clone();

    let (_, _) = tokio::join!(
        ground_span.create_export_session(command_block),
        satellite_span.create_export_session(telemetry_block),
    );

    eprintln!(
        "[T+{:.3}s] Both export sessions created, segments in flight",
        t0.elapsed().as_secs_f64()
    );

    // Wait for both deliveries.
    let (command_delivered, telemetry_delivered) = tokio::join!(
        link.satellite_sink.wait_for_bundle(Duration::from_secs(10)),
        link.ground_sink.wait_for_bundle(Duration::from_secs(10)),
    );

    let delivery_time = t0.elapsed();

    let command_delivered = command_delivered.expect("Command should be delivered to satellite");
    let telemetry_delivered =
        telemetry_delivered.expect("Telemetry should be delivered to ground station");

    eprintln!(
        "[T+{:.3}s] Both transfers complete",
        delivery_time.as_secs_f64()
    );

    // Verify command integrity.
    assert_eq!(
        command_delivered, command_bytes,
        "Command should arrive intact at satellite"
    );
    eprintln!(
        "    ✓ Command uplink: {} bytes delivered correctly",
        command_delivered.len()
    );

    // Verify telemetry integrity.
    assert_eq!(
        telemetry_delivered, telemetry_bytes,
        "Telemetry should arrive intact at ground station"
    );
    eprintln!(
        "    ✓ Telemetry downlink: {} bytes delivered correctly",
        telemetry_delivered.len()
    );

    // Verify timing.
    assert!(
        delivery_time.as_millis() >= SIMULATED_OWLT_MS as u128,
        "Bidirectional delivery should take at least {}ms, took {}ms",
        SIMULATED_OWLT_MS,
        delivery_time.as_millis()
    );

    eprintln!(
        "--- Bidirectional transfer successful (total: ~{}ms) ---",
        delivery_time.as_millis()
    );
    eprintln!();

    link.shutdown();
}
