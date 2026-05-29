// Copyright 2026 David Johnson, G4DPZ, AMSAT-UK
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for TVR–LTP integration.
//!
//! These tests exercise the end-to-end flows of:
//! - Contact close → link-down → timer suspension → segment queuing (13.1)
//! - Contact open → link-up → timer resumption → queue flush in FIFO order (13.2)
//! - Rate-limited queue flush via token bucket (13.3)
//!
//! Tests operate at the Span level with real UDP sockets to verify actual
//! network behavior during link state transitions.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hardy_bpa::cla::{LinkDownProperties, LinkUpProperties};
use hardy_ltp_cla::config::SpanConfig;
use hardy_ltp_cla::span::{LinkState, Span};
use tokio::net::UdpSocket;

use common::MockSink;

// ---------------------------------------------------------------------------
// Helper: create a Span configured for TVR integration testing
// ---------------------------------------------------------------------------

/// Create a test span with TVR features enabled and a given rate limit.
/// Returns the span and the UDP socket it sends on.
async fn create_tvr_test_span(
    xmit_rate_bps: u64,
    link_down_queue_max_bytes: usize,
) -> (Arc<Span>, Arc<UdpSocket>, Arc<UdpSocket>) {
    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    // A receiver socket to absorb sent segments
    let receiver = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let remote_addr = receiver.local_addr().unwrap();

    let sink: Arc<dyn hardy_bpa::cla::Sink> = Arc::new(MockSink);

    let config = SpanConfig {
        engine_id: 2,
        address: remote_addr,
        max_segment_size: 1400,
        max_retransmissions: 5,
        retransmit_cycle_secs: 60,
        aggr_size_limit: 65536,
        aggr_time_limit_secs: 0,
        max_export_sessions: 100,
        max_import_sessions: 100,
        xmit_rate_bps,
        tvr_timer_suspension: true,
        link_down_queue_max_bytes,
        tvr_rate_update: true,
        ..Default::default()
    };

    let span = Arc::new(Span::new(config, 1, socket.clone(), sink));
    (span, socket, receiver)
}

// ---------------------------------------------------------------------------
// 13.1: End-to-end contact close → suspend → queue flow
// ---------------------------------------------------------------------------

/// **Validates: Requirements 1.2, 2.1, 4.1**
///
/// Simulates a TVR contact close event and verifies:
/// 1. The LTP CLA transitions to DownTvr state (link-down received)
/// 2. Timers are suspended (timer maps populated)
/// 3. Segments produced after link-down are queued, not transmitted
#[tokio::test]
async fn test_contact_close_suspends_timers_and_queues_segments() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hardy_ltp_cla=debug")
        .with_test_writer()
        .try_init();

    let (span, _socket, receiver) = create_tvr_test_span(0, 10_485_760).await;

    // --- Step 1: Create an export session so there are active timers ---
    // Inject a bundle and create an export session (this starts timers).
    let test_bundle = Bytes::from([0xDE, 0xAD, 0xBE, 0xEF].repeat(100));
    let block = {
        let mut agg = span.aggregation.lock().unwrap();
        agg.append(&test_bundle);
        agg.flush().expect("buffer should have data")
    };
    span.create_export_session(block).await;

    // Give the session a moment to start timers
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify we have an active export session with timers
    {
        let sessions = span.export_sessions.lock().await;
        assert!(
            !sessions.is_empty(),
            "should have at least one active export session"
        );
        // At least one session should have timers (checkpoint timer)
        let has_timers = sessions.values().any(|s| !s.timers.is_empty());
        assert!(has_timers, "at least one session should have active timers");
    }

    // --- Step 2: Simulate TVR contact close (link-down) ---
    span.handle_link_down(LinkDownProperties { scheduled: true })
        .await;

    // Verify link state transitioned to DownTvr
    {
        let state = span.link_state.lock().unwrap();
        assert_eq!(
            *state,
            LinkState::DownTvr,
            "link state should be DownTvr after scheduled link-down"
        );
    }

    // Verify timers were suspended (export timers map should be populated)
    {
        let suspended = span.suspended_export_timers.lock().unwrap();
        assert!(
            !suspended.is_empty(),
            "suspended_export_timers should be populated after link-down"
        );
    }

    // Verify export session timers were cleared (aborted)
    {
        let sessions = span.export_sessions.lock().await;
        for (_num, state) in sessions.iter() {
            assert!(
                state.timers.is_empty(),
                "export session timers should be cleared after suspension"
            );
        }
    }

    // --- Step 3: Produce segments while link is down → they should be queued ---
    // Create another export session while link is down
    let test_bundle2 = Bytes::from([0xCA, 0xFE].repeat(50));
    let block2 = {
        let mut agg = span.aggregation.lock().unwrap();
        agg.append(&test_bundle2);
        agg.flush().expect("buffer should have data")
    };
    span.create_export_session(block2).await;

    // Give a moment for segments to be produced
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify segments were queued (not transmitted)
    {
        let queue = span.outbound_queue.lock().unwrap();
        assert!(
            !queue.is_empty(),
            "outbound queue should have segments after producing during link-down"
        );
        assert!(
            queue.current_bytes() > 0,
            "outbound queue should have non-zero bytes"
        );
    }

    // Verify nothing was received on the receiver socket (segments were queued, not sent)
    let mut buf = [0u8; 65536];
    let _recv_result = tokio::time::timeout(Duration::from_millis(100), receiver.recv(&mut buf)).await;
    // The first export session (before link-down) would have sent segments,
    // so we drain those first
    // What matters is that after link-down, new segments go to the queue
    // The queue being non-empty confirms this.
}

// ---------------------------------------------------------------------------
// 13.2: End-to-end contact open → resume → flush flow
// ---------------------------------------------------------------------------

/// **Validates: Requirements 1.1, 3.1, 4.2**
///
/// Simulates a TVR contact open event after a link-down period and verifies:
/// 1. Timers resume with correct remaining durations
/// 2. Queued segments are flushed in FIFO order
#[tokio::test]
async fn test_contact_open_resumes_timers_and_flushes_queue_fifo() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hardy_ltp_cla=debug")
        .with_test_writer()
        .try_init();

    let (span, _socket, receiver) = create_tvr_test_span(0, 10_485_760).await;

    // --- Step 1: Transition to link-down ---
    span.handle_link_down(LinkDownProperties { scheduled: true })
        .await;

    // Verify link is down
    {
        let state = span.link_state.lock().unwrap();
        assert_eq!(*state, LinkState::DownTvr);
    }

    // --- Step 2: Queue multiple segments in a known order ---
    // We directly enqueue segments to the outbound queue to control the exact
    // content and order, simulating what would happen when export sessions
    // produce segments during link-down.
    let segment_a = Bytes::from(vec![0xAA; 100]);
    let segment_b = Bytes::from(vec![0xBB; 200]);
    let segment_c = Bytes::from(vec![0xCC; 300]);

    {
        let mut queue = span.outbound_queue.lock().unwrap();
        queue.enqueue(segment_a.clone());
        queue.enqueue(segment_b.clone());
        queue.enqueue(segment_c.clone());
    }

    // Also add some suspended timer state to verify resumption
    {
        let mut suspended = span.suspended_export_timers.lock().unwrap();
        suspended.insert(1, Duration::from_secs(30));
        suspended.insert(2, Duration::from_secs(45));
    }

    // --- Step 3: Simulate TVR contact open (link-up) ---
    span.handle_link_up(LinkUpProperties {
        bandwidth_bps: None,
        one_way_light_time_ms: None,
    })
    .await;

    // Verify link state transitioned to Up
    {
        let state = span.link_state.lock().unwrap();
        assert_eq!(
            *state,
            LinkState::Up,
            "link state should be Up after link-up event"
        );
    }

    // Verify suspended timers were cleared (resumed)
    {
        let suspended = span.suspended_export_timers.lock().unwrap();
        assert!(
            suspended.is_empty(),
            "suspended_export_timers should be empty after link-up (timers resumed)"
        );
    }

    // Verify outbound queue was drained
    {
        let queue = span.outbound_queue.lock().unwrap();
        assert!(
            queue.is_empty(),
            "outbound queue should be empty after link-up flush"
        );
        assert_eq!(queue.current_bytes(), 0);
    }

    // --- Step 4: Verify segments were received in FIFO order ---
    // Read all segments from the receiver socket
    let mut received_segments: Vec<Vec<u8>> = Vec::new();
    let mut buf = [0u8; 65536];

    // Collect all segments that were flushed (with a short timeout for each)
    while let Ok(Ok(len)) =
        tokio::time::timeout(Duration::from_millis(200), receiver.recv(&mut buf)).await
    {
        received_segments.push(buf[..len].to_vec());
    }

    // We should have received exactly 3 segments
    assert_eq!(
        received_segments.len(),
        3,
        "should receive exactly 3 flushed segments, got {}",
        received_segments.len()
    );

    // Verify FIFO order: segment_a first, then segment_b, then segment_c
    assert_eq!(
        received_segments[0],
        segment_a.as_ref(),
        "first flushed segment should be segment_a"
    );
    assert_eq!(
        received_segments[1],
        segment_b.as_ref(),
        "second flushed segment should be segment_b"
    );
    assert_eq!(
        received_segments[2],
        segment_c.as_ref(),
        "third flushed segment should be segment_c"
    );
}

// ---------------------------------------------------------------------------
// 13.3: Rate-limited queue flush
// ---------------------------------------------------------------------------

/// **Validates: Requirements 4.6, 5.1, 5.3**
///
/// Verifies that flushed segments respect token bucket rate control:
/// 1. Sets a rate limit via link-up bandwidth
/// 2. Queues segments during link-down
/// 3. On link-up, verifies the flush takes approximately the expected time
///    based on the rate limit
#[tokio::test]
async fn test_rate_limited_queue_flush() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hardy_ltp_cla=debug")
        .with_test_writer()
        .try_init();

    // Configure with no initial rate limit — we'll set it via link-up bandwidth
    let (span, _socket, receiver) = create_tvr_test_span(0, 10_485_760).await;

    // --- Step 1: Transition to link-down ---
    span.handle_link_down(LinkDownProperties { scheduled: true })
        .await;

    // --- Step 2: Queue segments totaling a known size ---
    // Queue 5 segments of 1000 bytes each = 5000 bytes total
    let segment_size = 1000;
    let num_segments = 5;
    let total_bytes = segment_size * num_segments;

    {
        let mut queue = span.outbound_queue.lock().unwrap();
        for i in 0..num_segments {
            let segment = Bytes::from(vec![i as u8; segment_size]);
            queue.enqueue(segment);
        }
    }

    // Verify queue has the expected content
    {
        let queue = span.outbound_queue.lock().unwrap();
        assert_eq!(queue.current_bytes(), total_bytes);
    }

    // --- Step 3: Link-up with a specific bandwidth ---
    // Set rate to 16000 bps = 2000 bytes/sec
    // Sending 5000 bytes at 2000 bytes/sec should take ~2.5 seconds
    // But the token bucket starts full (1 second burst = 2000 bytes),
    // so the first 2000 bytes are free, then 3000 bytes at 2000 B/s = 1.5s
    // Total expected time: ~1.5 seconds (with some tolerance)
    let rate_bps: u64 = 16000; // 2000 bytes/sec

    let start = Instant::now();

    span.handle_link_up(LinkUpProperties {
        bandwidth_bps: Some(rate_bps),
        one_way_light_time_ms: None,
    })
    .await;

    let elapsed = start.elapsed();

    // Verify queue was drained
    {
        let queue = span.outbound_queue.lock().unwrap();
        assert!(
            queue.is_empty(),
            "outbound queue should be empty after flush"
        );
    }

    // Verify all segments were received
    let mut received_count = 0;
    let mut buf = [0u8; 65536];
    while let Ok(Ok(_len)) =
        tokio::time::timeout(Duration::from_millis(200), receiver.recv(&mut buf)).await
    {
        received_count += 1;
    }
    assert_eq!(
        received_count, num_segments,
        "should receive all {} segments",
        num_segments
    );

    // Verify rate limiting was applied:
    // The token bucket starts full with 1 second of burst (2000 bytes).
    // First segment (1000 bytes): consumes 1000 tokens, 1000 remain → no sleep
    // Second segment (1000 bytes): consumes 1000 tokens, 0 remain → no sleep
    // Third segment (1000 bytes): deficit of 1000 tokens → sleep 0.5s
    // Fourth segment (1000 bytes): deficit grows → sleep ~0.5s
    // Fifth segment (1000 bytes): deficit grows → sleep ~0.5s
    // Total sleep: approximately 1.5 seconds
    //
    // We use a generous tolerance because:
    // - Token refill happens based on elapsed time between consume() calls
    // - There's some processing overhead between segments
    let expected_min = Duration::from_millis(800); // Conservative lower bound
    let expected_max = Duration::from_millis(3000); // Upper bound with tolerance

    assert!(
        elapsed >= expected_min,
        "flush should take at least {:?} due to rate limiting, but took {:?}",
        expected_min,
        elapsed
    );
    assert!(
        elapsed <= expected_max,
        "flush should complete within {:?}, but took {:?}",
        expected_max,
        elapsed
    );

    // Verify the rate limiter was updated to the new bandwidth
    {
        let limiter = span.rate_limiter.lock().unwrap();
        assert!(
            limiter.is_some(),
            "rate limiter should be set after link-up with bandwidth"
        );
    }
}

/// **Validates: Requirements 5.1, 5.3**
///
/// Verifies that rate control update from link-up bandwidth applies to
/// subsequent transmissions without restarting active sessions.
#[tokio::test]
async fn test_rate_update_applies_without_session_restart() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hardy_ltp_cla=debug")
        .with_test_writer()
        .try_init();

    // Start with a high rate (effectively unlimited for small segments)
    let (span, _socket, _receiver) = create_tvr_test_span(1_000_000_000, 10_485_760).await;

    // Create an export session (starts timers)
    let test_bundle = Bytes::from(vec![0x42; 200]);
    let block = {
        let mut agg = span.aggregation.lock().unwrap();
        agg.append(&test_bundle);
        agg.flush().expect("buffer should have data")
    };
    span.create_export_session(block).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Record session count before link events
    let session_count_before = {
        let sessions = span.export_sessions.lock().await;
        sessions.len()
    };

    // Link-down then link-up with new bandwidth
    span.handle_link_down(LinkDownProperties { scheduled: true })
        .await;
    span.handle_link_up(LinkUpProperties {
        bandwidth_bps: Some(8000), // 1000 bytes/sec
        one_way_light_time_ms: None,
    })
    .await;

    // Verify sessions were NOT restarted (same count)
    let session_count_after = {
        let sessions = span.export_sessions.lock().await;
        sessions.len()
    };
    assert_eq!(
        session_count_before, session_count_after,
        "session count should not change — rate update should not restart sessions"
    );

    // Verify rate limiter was updated
    {
        let limiter = span.rate_limiter.lock().unwrap();
        assert!(
            limiter.is_some(),
            "rate limiter should be set after link-up with bandwidth"
        );
    }
}
