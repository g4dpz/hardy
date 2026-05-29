//! Property-based tests for the LTP import session extent map and import session.
//!
//! These tests validate correctness properties of the ExtentMap and ImportSession
//! using the `proptest` framework.

use hardy_ltp::segment::{self, CheckpointInfo, ReceptionClaim, Segment, SegmentType};
use hardy_ltp::session::{
    CancelDirection, CancelReason, ExtentMap, ImportAction, ImportConfig, ImportSession,
    ImportState, SessionId,
};
use proptest::prelude::*;
use std::time::Duration;

/// Compute the union coverage of a set of ranges using a naive approach.
/// Returns the total number of distinct bytes covered by all ranges.
fn naive_union_coverage(ranges: &[(u64, u64)]) -> u64 {
    if ranges.is_empty() {
        return 0;
    }

    // Collect all ranges as (start, end) and sort by start
    let mut sorted: Vec<(u64, u64)> = ranges.to_vec();
    sorted.sort_by_key(|&(s, _)| s);

    // Merge overlapping/adjacent ranges
    let mut merged: Vec<(u64, u64)> = Vec::new();
    for (start, end) in sorted {
        if start >= end {
            continue; // skip invalid ranges
        }
        if let Some(last) = merged.last_mut() {
            if start <= last.1 {
                // Overlapping or adjacent — extend
                last.1 = last.1.max(end);
            } else {
                merged.push((start, end));
            }
        } else {
            merged.push((start, end));
        }
    }

    merged.iter().map(|(s, e)| e - s).sum()
}

// ---------------------------------------------------------------------------
// Property 9: Extent Map Invariant
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// **Validates: Requirements 4.1, 19.1, 19.2**
    ///
    /// Property 9: Extent Map Invariant
    ///
    /// For any sequence of byte ranges inserted into an extent map, the
    /// resulting map SHALL contain no adjacent or overlapping entries (all
    /// mergeable extents are merged), and the union of all map entries SHALL
    /// equal the union of all inserted ranges.
    #[test]
    fn prop_extent_map_invariant(
        ranges in prop::collection::vec(
            (0u64..1000u64, 1u64..200u64),
            0..50
        )
    ) {
        let mut map = ExtentMap::new();

        // Convert (start, length) pairs to (start, end) and insert
        let insert_ranges: Vec<(u64, u64)> = ranges
            .iter()
            .map(|&(start, len)| (start, start + len))
            .collect();

        for &(start, end) in &insert_ranges {
            map.insert(start, end);
        }

        let claims = map.claims();

        // Invariant (a): No two entries overlap.
        // For any two claims (s1, l1) and (s2, l2) where s1 < s2, s1+l1 < s2
        // (strictly less, meaning they don't even touch).
        for i in 1..claims.len() {
            let (prev_start, prev_len) = claims[i - 1];
            let (curr_start, _curr_len) = claims[i];
            let prev_end = prev_start + prev_len;

            prop_assert!(
                prev_end < curr_start,
                "Entries overlap or are adjacent: [{}, {}) and [{}, {})",
                prev_start, prev_end, curr_start, curr_start + _curr_len
            );
        }

        // Invariant (b): No two entries are adjacent (already covered above
        // since we check strict less-than, not less-than-or-equal).

        // Invariant (c): Total coverage equals the size of the union of all
        // inserted ranges (computed independently via naive approach).
        let expected_coverage = naive_union_coverage(&insert_ranges);
        let actual_coverage = map.total_coverage();

        prop_assert_eq!(
            actual_coverage,
            expected_coverage,
            "Total coverage mismatch: map has {} but naive union gives {}",
            actual_coverage,
            expected_coverage
        );
    }
}

// ---------------------------------------------------------------------------
// Property 10: Report Segment Generation Constraints
// ---------------------------------------------------------------------------

/// Strategy to generate a set of disjoint segments with random offsets and lengths.
/// Returns a sorted Vec of (offset, length) pairs that are guaranteed non-overlapping.
fn arb_disjoint_segments() -> impl Strategy<Value = Vec<(u64, u64)>> {
    // Generate 1..30 segments with offsets in 0..2000 and lengths 1..50
    prop::collection::vec((0u64..2000u64, 1u64..50u64), 1..30).prop_map(|raw| {
        // Sort by offset and make them disjoint by adjusting overlaps
        let mut sorted: Vec<(u64, u64)> = raw;
        sorted.sort_by_key(|&(off, _)| off);

        let mut disjoint = Vec::new();
        let mut cursor: u64 = 0;

        for (off, len) in sorted {
            // Ensure this segment starts after the previous one ends
            let start = off.max(cursor);
            let end = start + len;
            disjoint.push((start, len));
            cursor = end + 1; // +1 to ensure a gap between segments
        }

        disjoint
    })
}

/// Helper: decode all Report segments from a list of ImportActions.
fn decode_reports(actions: &[ImportAction]) -> Vec<Segment> {
    actions
        .iter()
        .filter_map(|action| {
            if let ImportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).ok()?;
                if matches!(seg, Segment::Report { .. }) {
                    Some(seg)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// **Validates: Requirements 4.2, 4.3**
    ///
    /// Property 10: Report Segment Generation Constraints
    ///
    /// For any set of disjoint data segments fed into an ImportSession:
    /// (a) Each Report Segment has at most `max_claims_per_report` claims.
    /// (b) Multiple RS have non-overlapping windows: for RS_i and RS_{i+1},
    ///     RS_i.upper_bound <= RS_{i+1}.lower_bound.
    /// (c) The union of all claims across all RS equals the received extents.
    #[test]
    fn prop_report_segment_generation_constraints(
        segments in arb_disjoint_segments()
    ) {
        let max_claims_per_report: usize = 5;

        let session_id = SessionId {
            engine_id: 10,
            session_number: 1,
        };
        let config = ImportConfig {
            max_reports: None,
            retransmit_timeout: Duration::from_secs(30),
            max_claims_per_report,
            expected_client_service_id: 1,
            max_red_data_bytes: None,
            defer_report_ms: 0,
        };

        let mut session = ImportSession::new(session_id, config);

        // Feed all segments as RedData (no checkpoint yet)
        for &(offset, length) in &segments {
            let data = vec![0xAA; length as usize];
            session.on_data_segment(
                SegmentType::RedData,
                1,
                offset,
                &data,
                None,
            );
        }

        // Determine the checkpoint upper bound (highest offset + length)
        let checkpoint_upper: u64 = segments
            .iter()
            .map(|&(off, len)| off + len)
            .max()
            .unwrap_or(0);

        // Send a RedCheckpoint at the end to trigger report generation
        let ckpt_data = vec![0xBB; 1];
        // Place checkpoint data at checkpoint_upper so it extends to checkpoint_upper + 1
        let actions = session.on_data_segment(
            SegmentType::RedCheckpoint,
            1,
            checkpoint_upper,
            &ckpt_data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        // Decode all Report segments from the actions
        let reports = decode_reports(&actions);

        // There should be at least one report
        prop_assert!(!reports.is_empty(), "Expected at least one Report Segment");

        // Collect report details for verification
        let mut all_claims: Vec<ReceptionClaim> = Vec::new();
        let mut report_windows: Vec<(u64, u64)> = Vec::new(); // (lower_bound, upper_bound)

        for report in &reports {
            if let Segment::Report {
                claims,
                lower_bound,
                upper_bound,
                ..
            } = report
            {
                // (a) Each RS has at most max_claims_per_report claims
                prop_assert!(
                    claims.len() <= max_claims_per_report,
                    "Report has {} claims, exceeds max of {}",
                    claims.len(),
                    max_claims_per_report
                );

                report_windows.push((*lower_bound, *upper_bound));
                all_claims.extend_from_slice(claims);
            }
        }

        // (b) Multiple RS have non-overlapping windows
        for i in 1..report_windows.len() {
            let (_, prev_upper) = report_windows[i - 1];
            let (curr_lower, _) = report_windows[i];
            prop_assert!(
                prev_upper <= curr_lower,
                "Report windows overlap: RS[{}] upper_bound={} > RS[{}] lower_bound={}",
                i - 1,
                prev_upper,
                i,
                curr_lower
            );
        }

        // (c) The union of all claims across all RS equals the received extents.
        // Build the expected extents from the input segments + the checkpoint data.
        let mut expected_extents = ExtentMap::new();
        for &(offset, length) in &segments {
            expected_extents.insert(offset, offset + length);
        }
        // The checkpoint data itself is also recorded
        expected_extents.insert(checkpoint_upper, checkpoint_upper + 1);

        // Build actual extents from claims (only within [0, checkpoint_upper + 1))
        let mut actual_extents = ExtentMap::new();
        for claim in &all_claims {
            actual_extents.insert(claim.offset, claim.offset + claim.length);
        }

        // The expected claims are the extents within [0, checkpoint_upper + 1)
        let expected_claims = expected_extents.claims();
        let actual_claims = actual_extents.claims();

        prop_assert_eq!(
            actual_claims,
            expected_claims,
            "Union of report claims does not match received extents"
        );
    }
}

// ---------------------------------------------------------------------------
// Property 11: Import Session Delivers Complete Block
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// **Validates: Requirements 4.5**
    ///
    /// Property 11: Import Session Delivers Complete Block
    ///
    /// For any segment arrival order covering all bytes, the delivered block
    /// equals the original. Specifically: for any non-empty block split into
    /// segments of at most max_segment_size bytes, shuffled into an arbitrary
    /// order, and fed into an ImportSession, the session SHALL deliver a block
    /// identical to the original.
    #[test]
    fn prop_import_session_delivers_complete_block(
        block_data in prop::collection::vec(any::<u8>(), 1..=2048),
        max_segment_size in 1usize..=256,
        shuffle_seed in any::<u64>(),
    ) {
        let block_len = block_data.len();

        // Split the block into segments of at most max_segment_size bytes.
        // Each segment is (offset, data_slice).
        let mut segments: Vec<(usize, Vec<u8>)> = Vec::new();
        let mut offset = 0;
        while offset < block_len {
            let end = (offset + max_segment_size).min(block_len);
            segments.push((offset, block_data[offset..end].to_vec()));
            offset = end;
        }

        // The last segment by offset is the one with the highest offset —
        // it will be RedEob (type 3) with a checkpoint.
        // All other segments are RedData (type 0).
        let last_segment_offset = segments.last().unwrap().0;

        // Shuffle segments using a deterministic permutation derived from shuffle_seed.
        // We use a simple Fisher-Yates shuffle with a seeded PRNG.
        let mut indices: Vec<usize> = (0..segments.len()).collect();
        let mut seed = shuffle_seed;
        for i in (1..indices.len()).rev() {
            // Simple xorshift-based pseudo-random for deterministic shuffle
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let j = (seed as usize) % (i + 1);
            indices.swap(i, j);
        }

        // Create the import session
        let session_id = SessionId {
            engine_id: 1,
            session_number: 1,
        };
        let config = ImportConfig {
            max_reports: None,
            retransmit_timeout: Duration::from_secs(60),
            max_claims_per_report: 20,
            expected_client_service_id: 1,
            max_red_data_bytes: None,
            defer_report_ms: 0,
        };
        let mut session = ImportSession::new(session_id, config);

        // Feed segments in shuffled order
        let mut all_actions: Vec<ImportAction> = Vec::new();
        for &idx in &indices {
            let (seg_offset, ref seg_data) = segments[idx];

            // Determine segment type: RedEob for the last-by-offset segment,
            // RedData for all others.
            let (seg_type, checkpoint) = if seg_offset == last_segment_offset {
                (
                    SegmentType::RedEob,
                    Some(CheckpointInfo {
                        serial: 1,
                        responding_report_serial: 0,
                    }),
                )
            } else {
                (SegmentType::RedData, None)
            };

            let actions = session.on_data_segment(
                seg_type,
                1, // client_service_id
                seg_offset as u64,
                seg_data,
                checkpoint,
            );
            all_actions.extend(actions);
        }

        // Verify: session state is Complete
        prop_assert_eq!(
            session.state(),
            ImportState::Complete,
            "Session should be Complete after all segments delivered"
        );

        // Verify: a DeliverBlock action was emitted
        let delivered_block = all_actions.iter().find_map(|a| {
            if let ImportAction::DeliverBlock(data) = a {
                Some(data.clone())
            } else {
                None
            }
        });

        prop_assert!(
            delivered_block.is_some(),
            "Expected a DeliverBlock action to be emitted"
        );

        // Verify: delivered block data equals the original block
        let delivered = delivered_block.unwrap();
        prop_assert_eq!(
            delivered.as_ref(),
            block_data.as_slice(),
            "Delivered block data does not match original block"
        );
    }
}


// ---------------------------------------------------------------------------
// Property 19: Client Service ID Validation
// ---------------------------------------------------------------------------

/// Strategy to generate any data segment type (red or green).
fn arb_data_segment_type() -> impl Strategy<Value = SegmentType> {
    prop_oneof![
        Just(SegmentType::RedData),
        Just(SegmentType::RedCheckpoint),
        Just(SegmentType::RedEorp),
        Just(SegmentType::RedEob),
        Just(SegmentType::GreenData),
        Just(SegmentType::GreenEob),
    ]
}

/// Helper: decode all Cancel segments from a list of ImportActions.
fn decode_cancels(actions: &[ImportAction]) -> Vec<Segment> {
    actions
        .iter()
        .filter_map(|action| {
            if let ImportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).ok()?;
                if matches!(seg, Segment::Cancel { .. }) {
                    Some(seg)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// **Validates: Requirements 33.2**
    ///
    /// Property 19: Client Service ID Validation
    ///
    /// For any data segment with a mismatched client_service_id, the import
    /// session SHALL emit a Cancel-from-Receiver with reason ClientSvcUnreachable
    /// and transition to the Cancelled state.
    #[test]
    fn prop_client_service_id_validation_mismatch(
        expected_id in 1u64..=100,
        actual_id_offset in 1u64..=100,
        segment_type in arb_data_segment_type(),
        data in prop::collection::vec(any::<u8>(), 1..=100),
    ) {
        // Ensure actual_id is different from expected_id
        let actual_id = if expected_id + actual_id_offset > 200 {
            // Wrap around to ensure it's different
            expected_id.wrapping_sub(actual_id_offset).max(0) 
        } else {
            expected_id + actual_id_offset
        };
        // Double-check they're different (the offset guarantees this)
        prop_assume!(actual_id != expected_id);

        let session_id = SessionId {
            engine_id: 42,
            session_number: 7,
        };
        let config = ImportConfig {
            max_reports: None,
            retransmit_timeout: Duration::from_secs(30),
            max_claims_per_report: 20,
            expected_client_service_id: expected_id,
            max_red_data_bytes: None,
            defer_report_ms: 0,
        };

        let mut session = ImportSession::new(session_id, config);

        // Build checkpoint info if the segment type requires it
        let checkpoint = if segment_type.is_checkpoint() {
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            })
        } else {
            None
        };

        // Feed the data segment with mismatched client_service_id
        let actions = session.on_data_segment(
            segment_type,
            actual_id,
            0,
            &data,
            checkpoint,
        );

        // Verify (a): session state is Cancelled
        prop_assert_eq!(
            session.state(),
            ImportState::Cancelled,
            "Session should be Cancelled after mismatched client_service_id"
        );

        // Verify (b): A SendSegment action was emitted containing a Cancel segment
        let cancels = decode_cancels(&actions);
        prop_assert!(
            !cancels.is_empty(),
            "Expected at least one Cancel segment to be emitted"
        );

        // Verify (c): The Cancel segment has reason ClientSvcUnreachable and direction FromReceiver
        let cancel = &cancels[0];
        if let Segment::Cancel { reason, direction, .. } = cancel {
            prop_assert_eq!(
                *reason,
                CancelReason::ClientSvcUnreachable,
                "Cancel reason should be ClientSvcUnreachable"
            );
            prop_assert_eq!(
                *direction,
                CancelDirection::FromReceiver,
                "Cancel direction should be FromReceiver"
            );
        } else {
            prop_assert!(false, "Expected a Cancel segment");
        }
    }

    /// **Validates: Requirements 33.2**
    ///
    /// Property 19 (positive case): Client Service ID Validation
    ///
    /// When client_service_id matches the expected value, the import session
    /// SHALL NOT cancel and SHALL remain in the Receiving state (or Complete
    /// for EOB segments).
    #[test]
    fn prop_client_service_id_validation_match(
        expected_id in 1u64..=100,
        segment_type in arb_data_segment_type(),
        data in prop::collection::vec(any::<u8>(), 1..=100),
    ) {
        let session_id = SessionId {
            engine_id: 42,
            session_number: 7,
        };
        let config = ImportConfig {
            max_reports: None,
            retransmit_timeout: Duration::from_secs(30),
            max_claims_per_report: 20,
            expected_client_service_id: expected_id,
            max_red_data_bytes: None,
            defer_report_ms: 0,
        };

        let mut session = ImportSession::new(session_id, config);

        // Build checkpoint info if the segment type requires it
        let checkpoint = if segment_type.is_checkpoint() {
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            })
        } else {
            None
        };

        // Feed the data segment with MATCHING client_service_id
        let actions = session.on_data_segment(
            segment_type,
            expected_id,
            0,
            &data,
            checkpoint,
        );

        // Verify: session should NOT be Cancelled
        prop_assert_ne!(
            session.state(),
            ImportState::Cancelled,
            "Session should NOT be Cancelled when client_service_id matches"
        );

        // Verify: no Cancel segments emitted
        let cancels = decode_cancels(&actions);
        prop_assert!(
            cancels.is_empty(),
            "No Cancel segments should be emitted when client_service_id matches"
        );
    }
}


// ---------------------------------------------------------------------------
// Property 17: Max Red Data Size Enforcement
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// **Validates: Requirements 24.2**
    ///
    /// Property 17: Max Red Data Size Enforcement (negative case)
    ///
    /// For any segment whose offset+length exceeds the configured
    /// max_red_data_bytes limit, the import session SHALL emit a
    /// Cancel-from-Receiver with reason ByEngine and transition to the
    /// Cancelled state.
    #[test]
    fn prop_max_red_data_size_enforcement_exceeds(
        max_red_data_bytes in 100u64..=10000,
        // Generate an offset and length such that offset + length > max_red_data_bytes
        excess in 1u64..=5000,
        offset_fraction in 0u64..=100,
    ) {
        // Compute offset and data length such that offset + data.len() > max_red_data_bytes
        let target_end = max_red_data_bytes + excess; // guaranteed > max_red_data_bytes
        // Split target_end into offset + length. offset is a fraction of target_end.
        let offset = (target_end * offset_fraction) / 101; // offset in [0, target_end)
        let length = target_end - offset; // length > 0, and offset + length == target_end > max_red_data_bytes

        // Sanity: ensure offset + length > max_red_data_bytes
        prop_assume!(offset + length > max_red_data_bytes);
        // Limit data size to avoid huge allocations in tests
        prop_assume!(length <= 20000);

        let session_id = SessionId {
            engine_id: 99,
            session_number: 42,
        };
        let config = ImportConfig {
            max_reports: None,
            retransmit_timeout: Duration::from_secs(30),
            max_claims_per_report: 20,
            expected_client_service_id: 1,
            max_red_data_bytes: Some(max_red_data_bytes),
            defer_report_ms: 0,
        };

        let mut session = ImportSession::new(session_id, config);

        let data = vec![0xCC; length as usize];

        // Feed a red data segment that exceeds the limit
        let actions = session.on_data_segment(
            SegmentType::RedData,
            1, // matching client_service_id
            offset,
            &data,
            None,
        );

        // Verify (a): session state is Cancelled
        prop_assert_eq!(
            session.state(),
            ImportState::Cancelled,
            "Session should be Cancelled when offset+length ({}) exceeds max_red_data_bytes ({})",
            offset + length,
            max_red_data_bytes
        );

        // Verify (b): A Cancel-from-Receiver with reason ByEngine was emitted
        let cancels = decode_cancels(&actions);
        prop_assert!(
            !cancels.is_empty(),
            "Expected at least one Cancel segment to be emitted"
        );

        let cancel = &cancels[0];
        if let Segment::Cancel { reason, direction, .. } = cancel {
            prop_assert_eq!(
                *reason,
                CancelReason::ByEngine,
                "Cancel reason should be ByEngine"
            );
            prop_assert_eq!(
                *direction,
                CancelDirection::FromReceiver,
                "Cancel direction should be FromReceiver"
            );
        } else {
            prop_assert!(false, "Expected a Cancel segment");
        }
    }

    /// **Validates: Requirements 24.2**
    ///
    /// Property 17: Max Red Data Size Enforcement (positive case)
    ///
    /// For any segment whose offset+length is within the configured
    /// max_red_data_bytes limit, the import session SHALL NOT cancel.
    #[test]
    fn prop_max_red_data_size_enforcement_within_limit(
        max_red_data_bytes in 100u64..=10000,
        // Generate offset and length such that offset + length <= max_red_data_bytes
        offset in 0u64..=5000,
        length in 1u64..=5000,
    ) {
        // Ensure offset + length <= max_red_data_bytes
        prop_assume!(offset + length <= max_red_data_bytes);
        // Limit data size to avoid huge allocations
        prop_assume!(length <= 10000);

        let session_id = SessionId {
            engine_id: 99,
            session_number: 42,
        };
        let config = ImportConfig {
            max_reports: None,
            retransmit_timeout: Duration::from_secs(30),
            max_claims_per_report: 20,
            expected_client_service_id: 1,
            max_red_data_bytes: Some(max_red_data_bytes),
            defer_report_ms: 0,
        };

        let mut session = ImportSession::new(session_id, config);

        let data = vec![0xDD; length as usize];

        // Feed a red data segment within the limit
        let actions = session.on_data_segment(
            SegmentType::RedData,
            1, // matching client_service_id
            offset,
            &data,
            None,
        );

        // Verify: session should NOT be Cancelled
        prop_assert_ne!(
            session.state(),
            ImportState::Cancelled,
            "Session should NOT be Cancelled when offset+length ({}) <= max_red_data_bytes ({})",
            offset + length,
            max_red_data_bytes
        );

        // Verify: no Cancel segments emitted
        let cancels = decode_cancels(&actions);
        prop_assert!(
            cancels.is_empty(),
            "No Cancel segments should be emitted when within limit"
        );
    }
}
