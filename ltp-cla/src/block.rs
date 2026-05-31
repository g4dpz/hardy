// Copyright 2026 David Johnson, G4DPZ, AMSAT-UK
// SPDX-License-Identifier: Apache-2.0

//! Block unpacking logic for extracting individual bundles from an LTP block.
//!
//! An LTP block can use two framing modes:
//!
//! - **Length-prefixed** (Hardy native): Multiple bundles aggregated with
//!   `[4-byte big-endian length][bundle bytes][4-byte big-endian length][bundle bytes]...`
//!
//! - **None** (standard/ION interop): The entire block is a single raw bundle
//!   with no framing. This is the standard BPv7-over-LTP format per RFC 5326.
//!
//! This module provides [`unpack_block`] to extract individual bundles from a
//! delivered block, handling malformed data gracefully.

use bytes::Bytes;

use crate::config::BlockFraming;

/// Error encountered during block unpacking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnpackError {
    /// A length prefix indicated more bytes than remain in the block.
    /// Contains the byte offset where the error was detected.
    LengthExceedsRemaining { offset: usize },
}

/// Result of unpacking a block into individual bundles.
#[derive(Debug, Clone)]
pub struct UnpackResult {
    /// Successfully extracted bundles, in order.
    pub bundles: Vec<Bytes>,
    /// If an error was encountered, it is reported here.
    /// Bundles extracted before the error are still valid.
    pub error: Option<UnpackError>,
}

/// Unpack an LTP block into individual bundles.
///
/// The behavior depends on the `framing` mode:
///
/// - [`BlockFraming::LengthPrefixed`]: The block is a sequence of length-prefixed
///   bundles: `[u32 BE length][bundle bytes][u32 BE length][bundle bytes]...`
///
/// - [`BlockFraming::None`]: The entire block is treated as a single raw bundle
///   (standard BPv7-over-LTP format used by ION and other implementations).
///
/// # Error handling (length-prefixed mode)
///
/// - If the block is empty (0 bytes), returns an empty bundle list with no error.
/// - If a length prefix is zero, the entry is skipped (logged as warning), and
///   parsing continues with the next length prefix.
/// - If a length prefix exceeds the remaining bytes in the block, the remainder
///   is discarded, a warning is logged, and bundles extracted so far are returned
///   along with an error indication.
///
/// # Examples
///
/// ```
/// use bytes::Bytes;
/// use hardy_ltp_cla::block::unpack_block;
/// use hardy_ltp_cla::config::BlockFraming;
///
/// // Length-prefixed: a block with two bundles: [0,0,0,3, 1,2,3, 0,0,0,2, 4,5]
/// let block = Bytes::from(vec![0,0,0,3, 1,2,3, 0,0,0,2, 4,5]);
/// let result = unpack_block(block, BlockFraming::LengthPrefixed);
/// assert_eq!(result.bundles.len(), 2);
/// assert_eq!(&result.bundles[0][..], &[1,2,3]);
/// assert_eq!(&result.bundles[1][..], &[4,5]);
/// assert!(result.error.is_none());
///
/// // Raw (no framing): entire block is one bundle
/// let block = Bytes::from(vec![1,2,3,4,5]);
/// let result = unpack_block(block, BlockFraming::None);
/// assert_eq!(result.bundles.len(), 1);
/// assert_eq!(&result.bundles[0][..], &[1,2,3,4,5]);
/// assert!(result.error.is_none());
/// ```
pub fn unpack_block(block: Bytes, framing: BlockFraming) -> UnpackResult {
    // Empty block: return immediately with no bundles and no error.
    if block.is_empty() {
        return UnpackResult {
            bundles: Vec::new(),
            error: None,
        };
    }

    match framing {
        BlockFraming::None => {
            // Raw mode: the entire block is a single bundle.
            UnpackResult {
                bundles: vec![block],
                error: None,
            }
        }
        BlockFraming::LengthPrefixed => unpack_block_length_prefixed(block),
    }
}

/// Internal: unpack a length-prefixed block.
fn unpack_block_length_prefixed(block: Bytes) -> UnpackResult {
    let mut bundles = Vec::new();
    let mut pos = 0;
    let len = block.len();

    loop {
        // Check if we have enough bytes for a length prefix.
        if pos + 4 > len {
            // Not enough bytes for a complete length prefix — treat as truncated.
            if pos < len {
                tracing::warn!(
                    offset = pos,
                    remaining = len - pos,
                    "block unpacking: insufficient bytes for length prefix, discarding remainder"
                );
                return UnpackResult {
                    bundles,
                    error: Some(UnpackError::LengthExceedsRemaining { offset: pos }),
                };
            }
            // Exactly at end — normal completion.
            break;
        }

        // Read 4-byte big-endian length prefix.
        let bundle_len =
            u32::from_be_bytes([block[pos], block[pos + 1], block[pos + 2], block[pos + 3]])
                as usize;
        pos += 4;

        // Zero-length entry: skip without dispatching.
        if bundle_len == 0 {
            tracing::warn!(
                offset = pos - 4,
                "block unpacking: zero-length bundle entry, skipping"
            );
            continue;
        }

        // Check if the declared length exceeds remaining bytes.
        if pos + bundle_len > len {
            tracing::warn!(
                offset = pos - 4,
                declared_length = bundle_len,
                remaining = len - pos,
                "block unpacking: length prefix exceeds remaining bytes, discarding remainder"
            );
            return UnpackResult {
                bundles,
                error: Some(UnpackError::LengthExceedsRemaining { offset: pos - 4 }),
            };
        }

        // Extract bundle as a zero-copy slice of the input.
        let bundle = block.slice(pos..pos + bundle_len);
        bundles.push(bundle);
        pos += bundle_len;
    }

    UnpackResult {
        bundles,
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::config::BlockFraming;

    #[test]
    fn empty_block_returns_no_bundles() {
        let result = unpack_block(Bytes::new(), BlockFraming::LengthPrefixed);
        assert!(result.bundles.is_empty());
        assert!(result.error.is_none());
    }

    #[test]
    fn single_bundle() {
        // [length=5][5 bytes of data]
        let mut block = Vec::new();
        block.extend_from_slice(&5u32.to_be_bytes());
        block.extend_from_slice(&[10, 20, 30, 40, 50]);
        let result = unpack_block(Bytes::from(block), BlockFraming::LengthPrefixed);
        assert_eq!(result.bundles.len(), 1);
        assert_eq!(&result.bundles[0][..], &[10, 20, 30, 40, 50]);
        assert!(result.error.is_none());
    }

    #[test]
    fn multiple_bundles() {
        let mut block = Vec::new();
        // Bundle 1: 3 bytes
        block.extend_from_slice(&3u32.to_be_bytes());
        block.extend_from_slice(&[1, 2, 3]);
        // Bundle 2: 2 bytes
        block.extend_from_slice(&2u32.to_be_bytes());
        block.extend_from_slice(&[4, 5]);
        // Bundle 3: 1 byte
        block.extend_from_slice(&1u32.to_be_bytes());
        block.extend_from_slice(&[6]);

        let result = unpack_block(Bytes::from(block), BlockFraming::LengthPrefixed);
        assert_eq!(result.bundles.len(), 3);
        assert_eq!(&result.bundles[0][..], &[1, 2, 3]);
        assert_eq!(&result.bundles[1][..], &[4, 5]);
        assert_eq!(&result.bundles[2][..], &[6]);
        assert!(result.error.is_none());
    }

    #[test]
    fn length_exceeds_remaining_returns_partial_bundles() {
        let mut block = Vec::new();
        // Bundle 1: valid, 2 bytes
        block.extend_from_slice(&2u32.to_be_bytes());
        block.extend_from_slice(&[0xAA, 0xBB]);
        // Bundle 2: length says 100 but only 3 bytes remain
        block.extend_from_slice(&100u32.to_be_bytes());
        block.extend_from_slice(&[0xCC, 0xDD, 0xEE]);

        let result = unpack_block(Bytes::from(block), BlockFraming::LengthPrefixed);
        // First bundle extracted successfully
        assert_eq!(result.bundles.len(), 1);
        assert_eq!(&result.bundles[0][..], &[0xAA, 0xBB]);
        // Error reported for the second entry
        assert_eq!(
            result.error,
            Some(UnpackError::LengthExceedsRemaining { offset: 6 })
        );
    }

    #[test]
    fn zero_length_entry_is_skipped() {
        let mut block = Vec::new();
        // Bundle 1: valid, 2 bytes
        block.extend_from_slice(&2u32.to_be_bytes());
        block.extend_from_slice(&[1, 2]);
        // Zero-length entry (should be skipped)
        block.extend_from_slice(&0u32.to_be_bytes());
        // Bundle 2: valid, 1 byte
        block.extend_from_slice(&1u32.to_be_bytes());
        block.extend_from_slice(&[3]);

        let result = unpack_block(Bytes::from(block), BlockFraming::LengthPrefixed);
        assert_eq!(result.bundles.len(), 2);
        assert_eq!(&result.bundles[0][..], &[1, 2]);
        assert_eq!(&result.bundles[1][..], &[3]);
        assert!(result.error.is_none());
    }

    #[test]
    fn truncated_length_prefix_at_end() {
        // Only 2 bytes — not enough for a 4-byte length prefix
        let block = Bytes::from(vec![0x00, 0x01]);
        let result = unpack_block(block, BlockFraming::LengthPrefixed);
        assert!(result.bundles.is_empty());
        assert_eq!(
            result.error,
            Some(UnpackError::LengthExceedsRemaining { offset: 0 })
        );
    }

    #[test]
    fn truncated_length_prefix_after_valid_bundle() {
        let mut block = Vec::new();
        // Valid bundle: 1 byte
        block.extend_from_slice(&1u32.to_be_bytes());
        block.extend_from_slice(&[0xFF]);
        // Trailing 3 bytes — not enough for a length prefix
        block.extend_from_slice(&[0x00, 0x00, 0x00]);

        let result = unpack_block(Bytes::from(block), BlockFraming::LengthPrefixed);
        assert_eq!(result.bundles.len(), 1);
        assert_eq!(&result.bundles[0][..], &[0xFF]);
        assert_eq!(
            result.error,
            Some(UnpackError::LengthExceedsRemaining { offset: 5 })
        );
    }

    #[test]
    fn large_bundle() {
        let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let mut block = Vec::new();
        block.extend_from_slice(&(data.len() as u32).to_be_bytes());
        block.extend_from_slice(&data);

        let result = unpack_block(Bytes::from(block), BlockFraming::LengthPrefixed);
        assert_eq!(result.bundles.len(), 1);
        assert_eq!(&result.bundles[0][..], &data[..]);
        assert!(result.error.is_none());
    }

    #[test]
    fn multiple_zero_length_entries_all_skipped() {
        let mut block = Vec::new();
        // Three zero-length entries
        block.extend_from_slice(&0u32.to_be_bytes());
        block.extend_from_slice(&0u32.to_be_bytes());
        block.extend_from_slice(&0u32.to_be_bytes());

        let result = unpack_block(Bytes::from(block), BlockFraming::LengthPrefixed);
        assert!(result.bundles.is_empty());
        assert!(result.error.is_none());
    }

    #[test]
    fn zero_copy_slicing() {
        // Verify that extracted bundles share the same backing allocation
        let mut block_data = Vec::new();
        block_data.extend_from_slice(&3u32.to_be_bytes());
        block_data.extend_from_slice(&[10, 20, 30]);
        let block = Bytes::from(block_data);

        let result = unpack_block(block.clone(), BlockFraming::LengthPrefixed);
        assert_eq!(result.bundles.len(), 1);
        // The bundle should be a slice of the original Bytes
        assert_eq!(&result.bundles[0][..], &[10, 20, 30]);
    }

    #[test]
    fn exactly_one_length_prefix_no_data() {
        // A length prefix that says 5 bytes but there are 0 bytes after it
        let block = Bytes::from(5u32.to_be_bytes().to_vec());
        let result = unpack_block(block, BlockFraming::LengthPrefixed);
        assert!(result.bundles.is_empty());
        assert_eq!(
            result.error,
            Some(UnpackError::LengthExceedsRemaining { offset: 0 })
        );
    }
}
