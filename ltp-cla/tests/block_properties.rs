// Copyright 2026 David Johnson, G4DPZ, AMSAT-UK
// SPDX-License-Identifier: Apache-2.0

//! Property-based tests for bundle aggregation round-trip (Property 12).
//!
//! **Validates: Requirements 9.1, 9.4, 9.5**
//!
//! Property 12: Bundle Aggregation Round-Trip
//! For any sequence of non-empty byte sequences, pack then unpack produces
//! original sequence in order.

use bytes::Bytes;
use hardy_ltp_cla::block::unpack_block;
use hardy_ltp_cla::config::BlockFraming;
use hardy_ltp_cla::span::AggregationBuffer;
use proptest::prelude::*;

/// Strategy: generate 1..=20 non-empty byte sequences, each 1..=200 bytes.
fn arb_bundle_sequence() -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::vec(prop::collection::vec(any::<u8>(), 1..=200), 1..=20)
}

proptest! {
    /// **Validates: Requirements 9.1, 9.4, 9.5**
    ///
    /// For any sequence of non-empty byte sequences, packing all bundles into
    /// an AggregationBuffer (with a large size limit so no intermediate flushes)
    /// and then unpacking the flushed block produces the original sequence in order.
    #[test]
    fn prop_bundle_aggregation_round_trip_single_flush(
        bundles in arb_bundle_sequence()
    ) {
        // Use a very large size limit so all bundles fit in one block.
        let mut buffer = AggregationBuffer::new(usize::MAX, BlockFraming::LengthPrefixed);

        for bundle in &bundles {
            let flushed = buffer.append(bundle);
            // With usize::MAX limit, no intermediate flush should occur.
            prop_assert!(flushed.is_none(), "unexpected intermediate flush");
        }

        // Flush the buffer to get the aggregated block.
        let block = buffer.flush().expect("buffer should not be empty");

        // Unpack the block.
        let result = unpack_block(block, BlockFraming::LengthPrefixed);

        // Verify no error was returned from unpacking.
        prop_assert!(
            result.error.is_none(),
            "unpack_block returned error: {:?}",
            result.error
        );

        // Verify the number of unpacked bundles equals the number of input bundles.
        prop_assert_eq!(
            result.bundles.len(),
            bundles.len(),
            "bundle count mismatch"
        );

        // Verify each unpacked bundle equals the corresponding input bundle.
        for (i, (unpacked, original)) in result.bundles.iter().zip(bundles.iter()).enumerate() {
            prop_assert_eq!(
                &unpacked[..],
                &original[..],
                "bundle {} content mismatch",
                i
            );
        }
    }

    /// **Validates: Requirements 9.1, 9.4, 9.5**
    ///
    /// Multi-flush case: with a small size limit, multiple flushes occur.
    /// Collecting all flushed blocks, unpacking each separately, and
    /// concatenating the results produces the original sequence in order.
    #[test]
    fn prop_bundle_aggregation_round_trip_multi_flush(
        bundles in arb_bundle_sequence()
    ) {
        // Use a small size limit (50 bytes) so multiple flushes occur.
        let mut buffer = AggregationBuffer::new(50, BlockFraming::LengthPrefixed);
        let mut blocks: Vec<Bytes> = Vec::new();

        for bundle in &bundles {
            if let Some(flushed_block) = buffer.append(bundle) {
                blocks.push(flushed_block);
            }
        }

        // Final flush to get any remaining bundles.
        if let Some(final_block) = buffer.flush() {
            blocks.push(final_block);
        }

        // Unpack each block separately and concatenate all unpacked bundles.
        let mut all_unpacked: Vec<Bytes> = Vec::new();
        for block in &blocks {
            let result = unpack_block(block.clone(), BlockFraming::LengthPrefixed);
            prop_assert!(
                result.error.is_none(),
                "unpack_block returned error: {:?}",
                result.error
            );
            all_unpacked.extend(result.bundles);
        }

        // Verify the concatenated result equals the original sequence in order.
        prop_assert_eq!(
            all_unpacked.len(),
            bundles.len(),
            "total bundle count mismatch after multi-flush"
        );

        for (i, (unpacked, original)) in all_unpacked.iter().zip(bundles.iter()).enumerate() {
            prop_assert_eq!(
                &unpacked[..],
                &original[..],
                "bundle {} content mismatch in multi-flush",
                i
            );
        }
    }
}
