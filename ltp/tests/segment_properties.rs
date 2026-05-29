//! Property-based tests for the LTP segment wire format codec.
//!
//! These tests validate correctness properties of the segment encode/decode
//! functions using the `proptest` framework.

use bytes::{Bytes, BytesMut};
use hardy_ltp::segment::{self, CheckpointInfo, ReceptionClaim, Segment, SegmentType};
use hardy_ltp::session::{CancelDirection, CancelReason, SessionId};
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Strategy for generating arbitrary SessionId values.
fn arb_session_id() -> impl Strategy<Value = SessionId> {
    (any::<u64>(), any::<u64>()).prop_map(|(engine_id, session_number)| SessionId {
        engine_id,
        session_number,
    })
}

/// Strategy for generating arbitrary CancelReason values.
fn arb_cancel_reason() -> impl Strategy<Value = CancelReason> {
    prop_oneof![
        Just(CancelReason::ByUser),
        Just(CancelReason::ClientSvcUnreachable),
        Just(CancelReason::RetransmitLimitExceeded),
        Just(CancelReason::MiscoloredSegment),
        Just(CancelReason::ByEngine),
    ]
}

/// Strategy for generating arbitrary CancelDirection values.
fn arb_cancel_direction() -> impl Strategy<Value = CancelDirection> {
    prop_oneof![
        Just(CancelDirection::FromSender),
        Just(CancelDirection::FromReceiver),
    ]
}

/// Strategy for generating a data segment type that is NOT a checkpoint.
fn arb_non_checkpoint_data_type() -> impl Strategy<Value = SegmentType> {
    prop_oneof![
        Just(SegmentType::RedData),
        Just(SegmentType::GreenData),
        Just(SegmentType::GreenEob),
    ]
}

/// Strategy for generating a data segment type that IS a checkpoint.
fn arb_checkpoint_data_type() -> impl Strategy<Value = SegmentType> {
    prop_oneof![
        Just(SegmentType::RedCheckpoint),
        Just(SegmentType::RedEorp),
        Just(SegmentType::RedEob),
    ]
}

/// Strategy for generating arbitrary data payloads (0-200 bytes).
fn arb_data_payload() -> impl Strategy<Value = Bytes> {
    prop::collection::vec(any::<u8>(), 0..=200).prop_map(Bytes::from)
}

/// Strategy for generating a Data segment without checkpoint.
fn arb_data_segment_no_checkpoint() -> impl Strategy<Value = Segment> {
    (
        arb_session_id(),
        arb_non_checkpoint_data_type(),
        any::<u64>(),
        any::<u64>(),
        arb_data_payload(),
    )
        .prop_map(
            |(session_id, segment_type, client_service_id, offset, data)| Segment::Data {
                session_id,
                segment_type,
                client_service_id,
                offset,
                data,
                checkpoint: None,
            },
        )
}

/// Strategy for generating a Data segment with checkpoint.
fn arb_data_segment_with_checkpoint() -> impl Strategy<Value = Segment> {
    (
        arb_session_id(),
        arb_checkpoint_data_type(),
        any::<u64>(),
        any::<u64>(),
        arb_data_payload(),
        any::<u64>(),
        any::<u64>(),
    )
        .prop_map(
            |(session_id, segment_type, client_service_id, offset, data, serial, resp_rpt)| {
                Segment::Data {
                    session_id,
                    segment_type,
                    client_service_id,
                    offset,
                    data,
                    checkpoint: Some(CheckpointInfo {
                        serial,
                        responding_report_serial: resp_rpt,
                    }),
                }
            },
        )
}

/// Strategy for generating a Report segment with valid claims.
///
/// Claims must have offset >= lower_bound and offset + length <= upper_bound.
fn arb_report_segment() -> impl Strategy<Value = Segment> {
    (
        arb_session_id(),
        any::<u64>(),
        any::<u64>(),
        // Generate lower_bound and a span (upper - lower) to ensure upper > lower
        (0u64..u64::MAX / 2, 1u64..10000u64),
        0usize..=20usize,
    )
        .prop_flat_map(
            |(session_id, report_serial, checkpoint_serial, (lower_bound, span), claim_count)| {
                let upper_bound = lower_bound.saturating_add(span);
                let available = upper_bound - lower_bound;

                // Generate claims within [lower_bound, upper_bound)
                let claims_strategy =
                    prop::collection::vec((0u64..available, 1u64..=available.max(1)), claim_count)
                        .prop_map(move |raw_claims| {
                            raw_claims
                                .into_iter()
                                .map(|(rel_offset, length)| {
                                    // Ensure claim fits within bounds
                                    let offset = lower_bound + (rel_offset % available);
                                    let max_len = upper_bound.saturating_sub(offset).max(1);
                                    let length = (length % max_len).max(1);
                                    ReceptionClaim { offset, length }
                                })
                                .collect::<Vec<_>>()
                        });

                claims_strategy.prop_map(move |claims| Segment::Report {
                    session_id,
                    report_serial,
                    checkpoint_serial,
                    upper_bound,
                    lower_bound,
                    claims,
                })
            },
        )
}

/// Strategy for generating a ReportAck segment.
fn arb_report_ack_segment() -> impl Strategy<Value = Segment> {
    (arb_session_id(), any::<u64>()).prop_map(|(session_id, report_serial)| Segment::ReportAck {
        session_id,
        report_serial,
    })
}

/// Strategy for generating a Cancel segment.
fn arb_cancel_segment() -> impl Strategy<Value = Segment> {
    (
        arb_session_id(),
        arb_cancel_reason(),
        arb_cancel_direction(),
    )
        .prop_map(|(session_id, reason, direction)| Segment::Cancel {
            session_id,
            reason,
            direction,
        })
}

/// Strategy for generating a CancelAck segment.
fn arb_cancel_ack_segment() -> impl Strategy<Value = Segment> {
    (arb_session_id(), arb_cancel_direction()).prop_map(|(session_id, direction)| {
        Segment::CancelAck {
            session_id,
            direction,
        }
    })
}

/// Strategy for generating any valid Segment variant.
fn arb_segment() -> impl Strategy<Value = Segment> {
    prop_oneof![
        arb_data_segment_no_checkpoint(),
        arb_data_segment_with_checkpoint(),
        arb_report_segment(),
        arb_report_ack_segment(),
        arb_cancel_segment(),
        arb_cancel_ack_segment(),
    ]
}

// ---------------------------------------------------------------------------
// Property 3: Segment Wire Format Round-Trip
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.7, 2.8, 2.9, 2.11, 20.1, 20.2**
    ///
    /// Property 3: Segment Wire Format Round-Trip
    ///
    /// For any valid Segment value (Data, Report, ReportAck, Cancel, CancelAck
    /// with any valid field values), encoding to wire format and then decoding
    /// SHALL produce a structurally equivalent Segment.
    #[test]
    fn prop_segment_round_trip(seg in arb_segment()) {
        let mut buf = BytesMut::new();
        segment::encode(&seg, &mut buf);

        let mut reader = &buf[..];
        let decoded = segment::decode(&mut reader)
            .expect("decode should succeed for a validly-encoded segment");

        // All bytes should be consumed
        prop_assert_eq!(reader.len(), 0, "trailing bytes remain after decode");

        // Decoded segment must equal the original
        prop_assert_eq!(&decoded, &seg);
    }
}

// ---------------------------------------------------------------------------
// Property 4: Report Segment Claim Offset Encoding
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// **Validates: Requirements 2.5, 2.6**
    ///
    /// Property 4: Report Segment Claim Offset Encoding
    ///
    /// For any Report Segment with claims at absolute offsets (where each
    /// claim.offset >= lower_bound), the encoded wire representation SHALL
    /// contain claim offsets equal to (absolute_offset - lower_bound), and
    /// decoding SHALL reconstruct the original absolute offsets.
    #[test]
    fn prop_report_claim_offset_encoding(seg in arb_report_segment()) {
        // Extract the report fields for verification
        let (lower_bound, claims) = match &seg {
            Segment::Report { lower_bound, claims, .. } => (*lower_bound, claims.clone()),
            _ => unreachable!("arb_report_segment always produces Report variant"),
        };

        // Verify precondition: all claim offsets >= lower_bound
        for claim in &claims {
            prop_assert!(
                claim.offset >= lower_bound,
                "precondition violated: claim offset {} < lower_bound {}",
                claim.offset,
                lower_bound
            );
        }

        // Encode the segment
        let mut buf = BytesMut::new();
        segment::encode(&seg, &mut buf);

        // Decode and verify absolute offsets are reconstructed
        let mut reader = &buf[..];
        let decoded = segment::decode(&mut reader)
            .expect("decode should succeed for a validly-encoded report segment");

        match decoded {
            Segment::Report {
                claims: decoded_claims,
                lower_bound: decoded_lower_bound,
                ..
            } => {
                prop_assert_eq!(decoded_lower_bound, lower_bound);
                prop_assert_eq!(decoded_claims.len(), claims.len());

                for (original, decoded) in claims.iter().zip(decoded_claims.iter()) {
                    // Decoded absolute offset must match original
                    prop_assert_eq!(
                        decoded.offset, original.offset,
                        "claim offset mismatch: expected {}, got {}",
                        original.offset, decoded.offset
                    );
                    prop_assert_eq!(
                        decoded.length, original.length,
                        "claim length mismatch: expected {}, got {}",
                        original.length, decoded.length
                    );
                }
            }
            _ => prop_assert!(false, "decoded segment should be a Report variant"),
        }
    }
}

// ---------------------------------------------------------------------------
// Property 5: Segment Decode Never Panics
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// **Validates: Requirements 2.10**
    ///
    /// Property 5: Segment Decode Never Panics
    ///
    /// For any arbitrary byte sequence (including random, empty, and adversarial
    /// inputs), the segment decode function SHALL either return a valid Segment
    /// or a descriptive error — never panic or cause undefined behavior.
    #[test]
    fn prop_segment_decode_never_panics(data in prop::collection::vec(any::<u8>(), 0..=1024)) {
        let mut reader = &data[..];
        // The test passes as long as this does not panic.
        // It may return Ok or Err — both are acceptable.
        let _result = segment::decode(&mut reader);
    }
}
