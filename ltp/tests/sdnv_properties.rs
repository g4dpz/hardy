// Copyright 2026 David Johnson, G4DPZ, AMSAT-UK
// SPDX-License-Identifier: Apache-2.0

//! Property-based tests for the LTP SDNV codec.
//!
//! These tests validate correctness properties of the SDNV encode/decode
//! functions using the `proptest` framework.

use bytes::BytesMut;
use hardy_ltp::sdnv::{self, SdnvError};
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Property 1: SDNV Round-Trip
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// **Validates: Requirements 1.1, 1.2, 1.5**
    ///
    /// Property 1: SDNV Round-Trip
    ///
    /// For any u64 value, encoding it as an SDNV and then decoding the
    /// resulting byte sequence SHALL produce the original u64 value.
    #[test]
    fn prop_sdnv_round_trip(value in any::<u64>()) {
        let mut buf = BytesMut::new();
        sdnv::encode(value, &mut buf);

        let mut reader = &buf[..];
        let decoded = sdnv::decode(&mut reader)
            .expect("decode should succeed for a validly-encoded SDNV");

        // All bytes should be consumed
        prop_assert_eq!(reader.len(), 0, "trailing bytes remain after decode");

        // Decoded value must equal the original
        prop_assert_eq!(decoded, value);
    }
}

// ---------------------------------------------------------------------------
// Property 2: SDNV Rejects Invalid Input
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// **Validates: Requirements 1.3, 1.4**
    ///
    /// Property 2: SDNV Rejects Invalid Input
    ///
    /// For any byte sequence exceeding 10 bytes where all bytes have the
    /// continuation bit (MSB) set, decode SHALL return Overflow error.
    #[test]
    fn prop_sdnv_rejects_overflow(
        // Generate 11-20 bytes, all with continuation bit set (MSB = 1),
        // followed by a terminating byte (MSB = 0) to ensure the decoder
        // encounters more than 10 continuation bytes before any terminator.
        prefix_len in 11usize..=20usize,
        prefix_values in prop::collection::vec(0u8..=0x7F, 20),
        terminator in 0u8..=0x7F,
    ) {
        // Build a byte sequence with >10 bytes that have continuation bit set
        let mut data: Vec<u8> = prefix_values[..prefix_len]
            .iter()
            .map(|&b| b | 0x80) // Set continuation bit on all prefix bytes
            .collect();
        // Add a terminating byte (no continuation bit)
        data.push(terminator);

        let mut reader = &data[..];
        let result = sdnv::decode(&mut reader);

        prop_assert_eq!(
            result,
            Err(SdnvError::Overflow),
            "Expected Overflow for {} continuation bytes",
            prefix_len,
        );
    }

    /// **Validates: Requirements 1.3, 1.4**
    ///
    /// Property 2: SDNV Rejects Invalid Input
    ///
    /// For any non-empty byte sequence where the last byte has the continuation
    /// bit set (indicating more bytes expected but none available), decode SHALL
    /// return Incomplete error.
    #[test]
    fn prop_sdnv_rejects_truncated(
        // Generate 1-10 bytes where ALL bytes have the continuation bit set.
        // This means the sequence is truncated (no terminating byte).
        len in 1usize..=10usize,
        values in prop::collection::vec(0u8..=0x7F, 10),
    ) {
        // Build a truncated sequence: all bytes have continuation bit set
        let data: Vec<u8> = values[..len]
            .iter()
            .map(|&b| b | 0x80) // Set continuation bit on all bytes
            .collect();

        let mut reader = &data[..];
        let result = sdnv::decode(&mut reader);

        prop_assert_eq!(
            result,
            Err(SdnvError::Incomplete),
            "Expected Incomplete for truncated sequence of {} bytes",
            len,
        );
    }
}
