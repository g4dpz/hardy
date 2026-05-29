//! SDNV (Self-Delimiting Numeric Value) encoding and decoding.
//!
//! Implements the variable-length integer encoding defined in RFC 5050 §4.1,
//! where each byte contributes 7 value bits and the MSB is a continuation flag.

use bytes::{Buf, BufMut};
use thiserror::Error;

/// Maximum number of bytes an SDNV can occupy for a u64 value.
/// 10 bytes × 7 bits = 70 bits capacity, sufficient for 64-bit values.
const MAX_SDNV_BYTES: usize = 10;

/// Errors that can occur when decoding an SDNV.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SdnvError {
    /// The SDNV byte sequence exceeds 10 bytes, which would overflow a u64.
    #[error("SDNV exceeds 10 bytes (64-bit overflow)")]
    Overflow,

    /// The SDNV byte sequence is truncated (the buffer ran out while the
    /// continuation bit was still set on the last byte read).
    #[error("SDNV truncated (continuation bit set on final byte)")]
    Incomplete,
}

/// Returns the number of bytes needed to encode `value` as an SDNV.
pub fn encoded_len(value: u64) -> usize {
    if value == 0 {
        return 1;
    }
    // Number of bits needed to represent the value
    let bits = 64 - value.leading_zeros() as usize;
    // Each SDNV byte carries 7 value bits
    (bits + 6) / 7
}

/// Encodes a u64 value as an SDNV into the provided buffer.
///
/// Each byte contributes 7 value bits. The MSB is a continuation flag:
/// 1 = more bytes follow, 0 = final byte. Bytes are written in big-endian
/// order (most significant value bits first).
pub fn encode(value: u64, buf: &mut impl BufMut) {
    let len = encoded_len(value);

    for i in (0..len).rev() {
        // Extract 7 bits at position i (from LSB side)
        let byte = ((value >> (i * 7)) & 0x7F) as u8;
        if i == 0 {
            // Final byte: continuation bit = 0
            buf.put_u8(byte);
        } else {
            // More bytes follow: continuation bit = 1
            buf.put_u8(byte | 0x80);
        }
    }
}

/// Decodes an SDNV from the provided buffer, returning the u64 value.
///
/// Returns `SdnvError::Overflow` if the encoded value exceeds 10 bytes
/// (would overflow a u64). Returns `SdnvError::Incomplete` if the buffer
/// is exhausted while the continuation bit is still set.
pub fn decode(buf: &mut impl Buf) -> Result<u64, SdnvError> {
    let mut value: u64 = 0;
    let mut bytes_read: usize = 0;

    loop {
        if !buf.has_remaining() {
            return Err(SdnvError::Incomplete);
        }

        let byte = buf.get_u8();
        bytes_read += 1;

        if bytes_read > MAX_SDNV_BYTES {
            return Err(SdnvError::Overflow);
        }

        // Shift existing value left by 7 and add the 7 value bits
        value = (value << 7) | (byte & 0x7F) as u64;

        // If MSB is 0, this is the final byte
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
}

/// Pre-cached SDNV for frequently-used values (e.g., local engine ID).
///
/// Stores the pre-encoded byte representation to avoid repeated encoding
/// on the transmit path.
#[derive(Debug, Clone)]
pub struct CachedSdnv {
    bytes: [u8; MAX_SDNV_BYTES],
    len: u8,
}

impl CachedSdnv {
    /// Creates a new `CachedSdnv` by encoding the given value.
    pub fn new(value: u64) -> Self {
        let mut bytes = [0u8; MAX_SDNV_BYTES];
        let len = encoded_len(value);

        for i in (0..len).rev() {
            let byte = ((value >> (i * 7)) & 0x7F) as u8;
            let idx = len - 1 - i;
            if i == 0 {
                bytes[idx] = byte;
            } else {
                bytes[idx] = byte | 0x80;
            }
        }

        Self {
            bytes,
            len: len as u8,
        }
    }

    /// Returns the pre-encoded SDNV bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    /// Writes the pre-encoded SDNV bytes into the buffer.
    pub fn put(&self, buf: &mut impl BufMut) {
        buf.put_slice(self.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_zero() {
        let mut buf = BytesMut::new();
        encode(0, &mut buf);
        assert_eq!(&buf[..], &[0x00]);
    }

    #[test]
    fn encode_small_value() {
        // 127 fits in one byte (7 bits)
        let mut buf = BytesMut::new();
        encode(127, &mut buf);
        assert_eq!(&buf[..], &[0x7F]);
    }

    #[test]
    fn encode_two_bytes() {
        // 128 = 0b10000000 needs 2 bytes: [0x81, 0x00]
        let mut buf = BytesMut::new();
        encode(128, &mut buf);
        assert_eq!(&buf[..], &[0x81, 0x00]);
    }

    #[test]
    fn encode_larger_value() {
        // 0x4000 = 16384 = 0b100_0000_0000_0000
        // Needs 3 bytes: 7+7+1 bits
        let mut buf = BytesMut::new();
        encode(0x4000, &mut buf);
        assert_eq!(&buf[..], &[0x81, 0x80, 0x00]);
    }

    #[test]
    fn encode_u64_max() {
        let mut buf = BytesMut::new();
        encode(u64::MAX, &mut buf);
        // u64::MAX needs 10 bytes
        assert_eq!(buf.len(), 10);
        // Last byte should not have continuation bit
        assert_eq!(buf[9] & 0x80, 0);
        // All other bytes should have continuation bit
        for i in 0..9 {
            assert_eq!(buf[i] & 0x80, 0x80);
        }
    }

    #[test]
    fn decode_zero() {
        let mut buf = &[0x00u8][..];
        assert_eq!(decode(&mut buf).unwrap(), 0);
    }

    #[test]
    fn decode_small_value() {
        let mut buf = &[0x7F][..];
        assert_eq!(decode(&mut buf).unwrap(), 127);
    }

    #[test]
    fn decode_two_bytes() {
        let mut buf = &[0x81, 0x00][..];
        assert_eq!(decode(&mut buf).unwrap(), 128);
    }

    #[test]
    fn decode_overflow() {
        // 11 bytes all with continuation bit set (except last)
        let data: Vec<u8> = vec![0x81; 11];
        let mut buf = &data[..];
        assert_eq!(decode(&mut buf).unwrap_err(), SdnvError::Overflow);
    }

    #[test]
    fn decode_incomplete_empty() {
        let mut buf = &[][..];
        assert_eq!(decode(&mut buf).unwrap_err(), SdnvError::Incomplete);
    }

    #[test]
    fn decode_incomplete_truncated() {
        // Continuation bit set but no more bytes
        let mut buf = &[0x81][..];
        assert_eq!(decode(&mut buf).unwrap_err(), SdnvError::Incomplete);
    }

    #[test]
    fn round_trip_various_values() {
        let values = [
            0,
            1,
            127,
            128,
            255,
            256,
            16383,
            16384,
            u64::MAX / 2,
            u64::MAX,
        ];
        for &v in &values {
            let mut buf = BytesMut::new();
            encode(v, &mut buf);
            let mut reader = &buf[..];
            let decoded = decode(&mut reader).unwrap();
            assert_eq!(v, decoded, "round-trip failed for {v}");
        }
    }

    #[test]
    fn encoded_len_values() {
        assert_eq!(encoded_len(0), 1);
        assert_eq!(encoded_len(1), 1);
        assert_eq!(encoded_len(127), 1);
        assert_eq!(encoded_len(128), 2);
        assert_eq!(encoded_len(16383), 2);
        assert_eq!(encoded_len(16384), 3);
        assert_eq!(encoded_len(u64::MAX), 10);
    }

    #[test]
    fn cached_sdnv_matches_encode() {
        let values = [0, 1, 42, 127, 128, 1000, u64::MAX];
        for &v in &values {
            let cached = CachedSdnv::new(v);
            let mut buf = BytesMut::new();
            encode(v, &mut buf);
            assert_eq!(cached.as_bytes(), &buf[..], "CachedSdnv mismatch for {v}");
        }
    }

    #[test]
    fn cached_sdnv_put() {
        let cached = CachedSdnv::new(128);
        let mut buf = BytesMut::new();
        cached.put(&mut buf);
        assert_eq!(&buf[..], &[0x81, 0x00]);
    }
}
