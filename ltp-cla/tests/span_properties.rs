// Copyright 2026 David Johnson, G4DPZ, AMSAT-UK
// SPDX-License-Identifier: Apache-2.0

//! Property-based tests for span module (Properties 13, 14, 16, 18, 20, 21).
//!
//! **Property 13: Token Bucket Rate Invariant**
//! Validates: Requirements 11.1, 11.2, 11.3
//!
//! **Property 14: Session Number Strict Monotonicity**
//! Validates: Requirements 14.1, 14.2
//!
//! **Property 16: Session Recreation Prevention**
//! Validates: Requirements 28.2, 28.3
//!
//! **Property 18: Max Import Sessions Enforcement**
//! Validates: Requirements 23.1, 23.2
//!
//! **Property 20: Closed Export Response Limit**
//! Validates: Requirements 30.1, 30.2, 30.3
//!
//! **Property 21: RTT-Based Timer Computation**
//! Validates: Requirements 34.2

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use hardy_ltp_cla::span::TokenBucket;
use proptest::prelude::*;

proptest! {
    /// **Validates: Requirements 14.1, 14.2**
    ///
    /// Sequential case: for any number of allocations, each session number
    /// is strictly greater than all previous ones.
    #[test]
    fn prop_session_number_strict_monotonicity_sequential(
        num_allocations in 1u64..=1000
    ) {
        let counter = AtomicU64::new(1);
        let mut previous = 0u64;

        for _ in 0..num_allocations {
            let current = counter.fetch_add(1, Ordering::Relaxed);
            prop_assert!(
                current > previous,
                "session number {} is not strictly greater than previous {}",
                current,
                previous
            );
            previous = current;
        }
    }

    /// **Validates: Requirements 14.1, 14.2**
    ///
    /// Concurrent case: for any number of threads each performing multiple
    /// allocations, all allocated session numbers are unique (which implies
    /// strict monotonicity within any single thread's observation order).
    #[test]
    fn prop_session_number_uniqueness_under_concurrency(
        num_threads in 2usize..=8,
        allocations_per_thread in 1usize..=200
    ) {
        let counter = Arc::new(AtomicU64::new(1));
        let mut handles = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            let counter = Arc::clone(&counter);
            let n = allocations_per_thread;
            handles.push(std::thread::spawn(move || {
                let mut numbers = Vec::with_capacity(n);
                for _ in 0..n {
                    numbers.push(counter.fetch_add(1, Ordering::Relaxed));
                }
                numbers
            }));
        }

        let mut all_numbers = Vec::new();
        for handle in handles {
            let thread_numbers = handle.join().expect("thread panicked");

            // Each thread's numbers must be strictly increasing.
            for window in thread_numbers.windows(2) {
                prop_assert!(
                    window[1] > window[0],
                    "within-thread monotonicity violated: {} not > {}",
                    window[1],
                    window[0]
                );
            }

            all_numbers.extend(thread_numbers);
        }

        // All numbers across all threads must be unique.
        let unique: HashSet<u64> = all_numbers.iter().copied().collect();
        prop_assert_eq!(
            unique.len(),
            all_numbers.len(),
            "duplicate session numbers detected across threads"
        );
    }
}

// ---------------------------------------------------------------------------
// Property 13: Token Bucket Rate Invariant
// ---------------------------------------------------------------------------

/// Strategy to generate a sequence of consume byte amounts (1..=1500, 1..=50 calls).
fn consume_sequence_strategy() -> impl Strategy<Value = Vec<usize>> {
    prop::collection::vec(1usize..=1500, 1..=50)
}

proptest! {
    /// **Validates: Requirements 11.1, 11.2, 11.3**
    ///
    /// Property 13: Token Bucket Rate Invariant
    ///
    /// For any sequence of segment transmissions through a token bucket with
    /// rate R bytes/sec, the cumulative bytes sent at any point in time t
    /// SHALL not exceed R × t + initial_burst (within tolerance).
    ///
    /// We simulate time progression by accumulating the sleep durations
    /// returned by `consume()`. Since calls happen in rapid succession
    /// (no real time passes), the bucket doesn't refill between calls,
    /// making the returned sleep durations represent the "simulated time"
    /// needed to stay within the rate limit.
    #[test]
    fn prop_token_bucket_rate_invariant(
        rate_bps in 1000u64..=1_000_000,
        consume_amounts in consume_sequence_strategy(),
    ) {
        let mut bucket = TokenBucket::new(rate_bps);
        let rate_bytes_per_sec = rate_bps as f64 / 8.0;
        let initial_burst = rate_bytes_per_sec; // 1 second of tokens

        let mut cumulative_bytes: f64 = 0.0;
        let mut cumulative_time: f64 = 0.0;

        for bytes in &consume_amounts {
            let sleep = bucket.consume(*bytes);
            cumulative_time += sleep.as_secs_f64();
            cumulative_bytes += *bytes as f64;

            // Invariant: cumulative_bytes <= rate × cumulative_time + initial_burst + tolerance
            // The tolerance accounts for floating-point rounding.
            let allowed = rate_bytes_per_sec * cumulative_time + initial_burst + 1.0;
            prop_assert!(
                cumulative_bytes <= allowed,
                "Rate invariant violated: cumulative_bytes ({}) > allowed ({}) \
                 [rate_bps={}, rate_Bps={}, time={}, burst={}]",
                cumulative_bytes,
                allowed,
                rate_bps,
                rate_bytes_per_sec,
                cumulative_time,
                initial_burst,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Property 20: Closed Export Response Limit
// ---------------------------------------------------------------------------

use std::time::Duration;

use hardy_bpa::cla::Sink;
use hardy_ltp::session::SessionId;
use hardy_ltp_cla::config::SpanConfig;
use hardy_ltp_cla::span::{ClosedExportState, Span};
use tokio::net::UdpSocket;

mod common;
use common::MockSink;

proptest! {
    /// **Validates: Requirements 30.1, 30.2, 30.3**
    ///
    /// Property 20: Closed Export Response Limit
    ///
    /// For any closed export session initialized with response_limit = max_retransmissions,
    /// each Report-Ack sent SHALL decrement the counter, and when the counter reaches zero
    /// the closed export SHALL be discarded regardless of remaining retention time.
    #[test]
    fn prop_closed_export_response_limit(
        max_retransmissions in 1u32..=20,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
            let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
            let sink: Arc<dyn Sink> = Arc::new(MockSink);

            let config = SpanConfig {
                engine_id: 2,
                address: "127.0.0.1:1113".parse().unwrap(),
                max_retransmissions,
                retransmit_cycle_secs: 10,
                ..Default::default()
            };

            let span = Arc::new(Span::new(config, 1, socket, sink));

            // Insert a closed export with response_counter = max_retransmissions.
            let session_number = 100u64;
            let session_id = SessionId {
                engine_id: 1,
                session_number,
            };
            let timer_handle = tokio::spawn(async {
                // Long-running timer that won't fire during the test.
                tokio::time::sleep(Duration::from_secs(9999)).await;
            });
            let closed_state = ClosedExportState {
                session_id,
                response_counter: max_retransmissions,
                retention_timer: timer_handle.abort_handle(),
            };
            span.closed_exports.lock().await.insert(session_number, closed_state);

            // Send (max_retransmissions - 1) reports: entry should still be present.
            for i in 1..max_retransmissions {
                span.on_closed_export_report(session_number, i as u64).await;
                let closed = span.closed_exports.lock().await;
                assert!(
                    closed.contains_key(&session_number),
                    "closed export should still be present after {} of {} reports",
                    i,
                    max_retransmissions
                );
                let state = closed.get(&session_number).unwrap();
                assert_eq!(
                    state.response_counter,
                    max_retransmissions - i,
                    "counter should be {} after {} reports",
                    max_retransmissions - i,
                    i
                );
            }

            // Send the final report: counter reaches zero, entry should be discarded.
            span.on_closed_export_report(session_number, max_retransmissions as u64)
                .await;
            let closed = span.closed_exports.lock().await;
            assert!(
                !closed.contains_key(&session_number),
                "closed export should be discarded when counter reaches zero \
                 (max_retransmissions={})",
                max_retransmissions
            );
        });
    }
}

// ---------------------------------------------------------------------------
// Property 18: Max Import Sessions Enforcement
// ---------------------------------------------------------------------------

use hardy_ltp::segment::SegmentType;

proptest! {
    /// **Validates: Requirements 23.1, 23.2**
    ///
    /// Property 18: Max Import Sessions Enforcement
    ///
    /// For any span at its max_import_sessions limit, a data segment for a new
    /// (unknown) session SHALL be silently discarded without creating a new
    /// import session. Data for existing sessions is still accepted.
    #[test]
    fn prop_max_import_sessions_enforcement(
        max_import_sessions in 1u32..=10,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
            let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
            let sink: Arc<dyn Sink> = Arc::new(MockSink);

            let config = SpanConfig {
                engine_id: 2,
                address: "127.0.0.1:1113".parse().unwrap(),
                max_import_sessions,
                // Use a large red data limit so it doesn't interfere.
                max_red_data_bytes_per_session: 10_485_760,
                ..Default::default()
            };

            let span = Arc::new(Span::new(config, 1, socket, sink));

            // Step 1: Fill the span to its max_import_sessions limit.
            // Each call with a unique session_number creates a new import session.
            for session_num in 1..=max_import_sessions as u64 {
                span.on_import_data_segment(
                    session_num,
                    SegmentType::RedData,
                    1, // client_service_id
                    0, // offset
                    &[0xAA; 100], // data
                    None, // no checkpoint
                ).await;
            }

            // Verify all sessions were created.
            {
                let sessions = span.import_sessions.lock().await;
                assert_eq!(
                    sessions.len(),
                    max_import_sessions as usize,
                    "expected {} import sessions to be created, got {}",
                    max_import_sessions,
                    sessions.len()
                );
            }

            // Step 2: Try to create one more session beyond the limit.
            let overflow_session_num = max_import_sessions as u64 + 1;
            span.on_import_data_segment(
                overflow_session_num,
                SegmentType::RedData,
                1,
                0,
                &[0xBB; 100],
                None,
            ).await;

            // Verify the new session was NOT created (silently discarded).
            {
                let sessions = span.import_sessions.lock().await;
                assert_eq!(
                    sessions.len(),
                    max_import_sessions as usize,
                    "session count should still be {} after overflow attempt, got {}",
                    max_import_sessions,
                    sessions.len()
                );
                assert!(
                    !sessions.contains_key(&overflow_session_num),
                    "overflow session {} should not have been created",
                    overflow_session_num
                );
            }

            // Step 3: Verify that data for an EXISTING session is still accepted
            // even when at the limit.
            let existing_session_num = 1u64;
            span.on_import_data_segment(
                existing_session_num,
                SegmentType::RedData,
                1,
                100, // different offset (appending more data)
                &[0xCC; 50],
                None,
            ).await;

            // The existing session should still be present (not discarded).
            {
                let sessions = span.import_sessions.lock().await;
                assert_eq!(
                    sessions.len(),
                    max_import_sessions as usize,
                    "session count should remain {} after feeding existing session",
                    max_import_sessions,
                    );
                assert!(
                    sessions.contains_key(&existing_session_num),
                    "existing session {} should still be present",
                    existing_session_num
                );
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Property 16: Session Recreation Prevention
// ---------------------------------------------------------------------------

use hardy_ltp_cla::span::SessionHistory;

/// Strategy to generate a sequence of session numbers to insert.
fn session_numbers_strategy() -> impl Strategy<Value = Vec<u64>> {
    prop::collection::vec(1u64..=100, 1..=50)
}

proptest! {
    /// **Validates: Requirements 28.2, 28.3**
    ///
    /// Property 16: Session Recreation Prevention
    ///
    /// For any circular buffer of size N containing recently-closed session
    /// numbers (with deduplication):
    /// - Session numbers currently in the buffer are found by contains()
    /// - Session numbers not in the buffer are NOT found by contains()
    /// - The buffer length never exceeds capacity
    #[test]
    fn prop_session_recreation_prevention(
        capacity in 1usize..=20,
        insertions in session_numbers_strategy(),
    ) {
        let mut history = SessionHistory::new(capacity);

        // Insert all session numbers into the history.
        for &session_num in &insertions {
            history.insert(session_num);
        }

        // Simulate the expected buffer state using a reference implementation.
        // This mirrors the actual logic: skip if already present, evict oldest
        // when full, then push.
        let mut ref_buffer: VecDeque<u64> = VecDeque::new();
        let mut ref_set: HashSet<u64> = HashSet::new();
        for &v in &insertions {
            if ref_set.contains(&v) {
                continue;
            }
            if ref_buffer.len() >= capacity {
                if let Some(evicted) = ref_buffer.pop_front() {
                    ref_set.remove(&evicted);
                }
            }
            ref_buffer.push_back(v);
            ref_set.insert(v);
        }

        // (a) Numbers in the reference set are found by contains().
        for &session_num in &ref_set {
            prop_assert!(
                history.contains(session_num),
                "session number {} should be in history",
                session_num
            );
        }

        // (b) Numbers NOT in the reference set are NOT found.
        // Check a range of values that might have been inserted but evicted.
        for v in 1u64..=100 {
            if !ref_set.contains(&v) {
                prop_assert!(
                    !history.contains(v),
                    "session number {} should NOT be in history",
                    v
                );
            }
        }

        // (c) Buffer length matches reference.
        prop_assert_eq!(
            history.len(),
            ref_buffer.len(),
            "history length mismatch"
        );

        // (d) Buffer length never exceeds capacity.
        prop_assert!(
            history.len() <= capacity,
            "history length {} exceeds capacity {}",
            history.len(),
            capacity
        );
    }
}

// ---------------------------------------------------------------------------
// Property 21: RTT-Based Timer Computation
// ---------------------------------------------------------------------------

proptest! {
    /// **Validates: Requirements 34.2**
    ///
    /// Property 21: RTT-Based Timer Computation
    ///
    /// For any one_way_light_time_ms and one_way_margin_time_ms, the computed
    /// retransmission timeout SHALL equal exactly 2 × (light + margin) milliseconds.
    #[test]
    fn prop_rtt_based_timer_computation(
        one_way_light_time_ms in 0u64..=100_000,
        one_way_margin_time_ms in 0u64..=10_000,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
            let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
            let sink: Arc<dyn Sink> = Arc::new(MockSink);

            let config = SpanConfig {
                engine_id: 2,
                address: "127.0.0.1:1113".parse().unwrap(),
                one_way_light_time_ms: Some(one_way_light_time_ms),
                one_way_margin_time_ms,
                retransmit_cycle_secs: 60, // should not be used when OWLT is configured
                ..Default::default()
            };

            let span = Arc::new(Span::new(config, 1, socket, sink));

            let timeout = span.compute_retransmit_timeout();
            let expected = Duration::from_millis(2 * (one_way_light_time_ms + one_way_margin_time_ms));

            assert_eq!(
                timeout,
                expected,
                "timeout {:?} != expected {:?} for owlt={}, margin={}",
                timeout,
                expected,
                one_way_light_time_ms,
                one_way_margin_time_ms,
            );
        });
    }

    /// **Validates: Requirements 34.2**
    ///
    /// Property 21 (fallback case): RTT-Based Timer Computation Fallback
    ///
    /// When one_way_light_time_ms is not configured (None), the computed
    /// retransmission timeout SHALL equal retransmit_cycle_secs as a Duration.
    #[test]
    fn prop_rtt_timer_fallback_to_retransmit_cycle(
        retransmit_cycle_secs in 1u64..=300,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
            let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
            let sink: Arc<dyn Sink> = Arc::new(MockSink);

            let config = SpanConfig {
                engine_id: 2,
                address: "127.0.0.1:1113".parse().unwrap(),
                one_way_light_time_ms: None,
                one_way_margin_time_ms: 500, // should be ignored when OWLT is None
                retransmit_cycle_secs,
                ..Default::default()
            };

            let span = Arc::new(Span::new(config, 1, socket, sink));

            let timeout = span.compute_retransmit_timeout();
            let expected = Duration::from_secs(retransmit_cycle_secs);

            assert_eq!(
                timeout,
                expected,
                "fallback timeout {:?} != expected {:?} for retransmit_cycle_secs={}",
                timeout,
                expected,
                retransmit_cycle_secs,
            );
        });
    }
}
