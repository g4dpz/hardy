//! LTP segment wire format encoding and decoding.
//!
//! Implements the segment types and wire format defined in RFC 5326,
//! including data segments, report segments, cancel segments, and their acks.

use bytes::{Buf, BufMut, Bytes};
use thiserror::Error;

use crate::sdnv::{self, SdnvError};
use crate::session::{CancelDirection, CancelReason, SessionId};

/// Segment type codes as defined in RFC 5326 §3.2.
///
/// The 4-bit type code occupies the lower nibble of the first header byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SegmentType {
    /// Red data segment (no checkpoint).
    RedData = 0,
    /// Red data segment that is also a checkpoint.
    RedCheckpoint = 1,
    /// Red data segment marking the End-of-Red-Part (checkpoint).
    RedEorp = 2,
    /// Red data segment marking the End-of-Block (checkpoint).
    RedEob = 3,
    /// Green data segment.
    GreenData = 4,
    /// Green data segment marking the End-of-Block.
    GreenEob = 7,
    /// Report segment (receiver acknowledgement).
    Report = 8,
    /// Report acknowledgement segment.
    ReportAck = 9,
    /// Cancel segment from sender.
    CancelFromSender = 12,
    /// Cancel acknowledgement to sender.
    CancelAckToSender = 13,
    /// Cancel segment from receiver.
    CancelFromReceiver = 14,
    /// Cancel acknowledgement to receiver.
    CancelAckToReceiver = 15,
}

impl SegmentType {
    /// Returns `true` if this segment type carries a checkpoint
    /// (types 1, 2, or 3).
    pub fn is_checkpoint(&self) -> bool {
        matches!(
            self,
            SegmentType::RedCheckpoint | SegmentType::RedEorp | SegmentType::RedEob
        )
    }

    /// Returns `true` if this is a data segment type (types 0–4, 7).
    pub fn is_data(&self) -> bool {
        matches!(
            self,
            SegmentType::RedData
                | SegmentType::RedCheckpoint
                | SegmentType::RedEorp
                | SegmentType::RedEob
                | SegmentType::GreenData
                | SegmentType::GreenEob
        )
    }

    /// Returns `true` if this is a red (reliable) data segment type (types 0–3).
    pub fn is_red(&self) -> bool {
        matches!(
            self,
            SegmentType::RedData
                | SegmentType::RedCheckpoint
                | SegmentType::RedEorp
                | SegmentType::RedEob
        )
    }

    /// Returns `true` if this is a green (best-effort) data segment type (types 4, 7).
    pub fn is_green(&self) -> bool {
        matches!(self, SegmentType::GreenData | SegmentType::GreenEob)
    }
}

impl TryFrom<u8> for SegmentType {
    type Error = SegmentError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(SegmentType::RedData),
            1 => Ok(SegmentType::RedCheckpoint),
            2 => Ok(SegmentType::RedEorp),
            3 => Ok(SegmentType::RedEob),
            4 => Ok(SegmentType::GreenData),
            7 => Ok(SegmentType::GreenEob),
            8 => Ok(SegmentType::Report),
            9 => Ok(SegmentType::ReportAck),
            12 => Ok(SegmentType::CancelFromSender),
            13 => Ok(SegmentType::CancelAckToSender),
            14 => Ok(SegmentType::CancelFromReceiver),
            15 => Ok(SegmentType::CancelAckToReceiver),
            _ => Err(SegmentError::UnknownType(value)),
        }
    }
}

/// Checkpoint information included in checkpoint data segments (types 1, 2, 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointInfo {
    /// The checkpoint serial number, strictly increasing within a session.
    pub serial: u64,
    /// The report serial number this checkpoint is responding to (0 for initial).
    pub responding_report_serial: u64,
}

/// A single reception claim within a Report Segment.
///
/// Offsets are stored as absolute byte positions within the block (not
/// relative to the report lower bound). The wire encoding converts to/from
/// relative offsets during encode/decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceptionClaim {
    /// Absolute byte offset of the start of this claimed range.
    pub offset: u64,
    /// Length in bytes of this claimed range.
    pub length: u64,
}

/// An LTP segment with all decoded fields.
///
/// Each variant corresponds to a logical segment category as defined in
/// RFC 5326 §3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    /// A data segment (red or green) carrying block payload bytes.
    Data {
        /// Session identifier (sender engine ID + session number).
        session_id: SessionId,
        /// The specific data segment type code.
        segment_type: SegmentType,
        /// Client service identifier (1 = Bundle Protocol).
        client_service_id: u64,
        /// Byte offset of this segment's data within the block.
        offset: u64,
        /// The segment payload data.
        data: Bytes,
        /// Checkpoint info, present only for checkpoint segment types (1, 2, 3).
        checkpoint: Option<CheckpointInfo>,
    },
    /// A report segment acknowledging received byte ranges.
    Report {
        /// Session identifier.
        session_id: SessionId,
        /// Unique serial number for this report.
        report_serial: u64,
        /// The checkpoint serial number this report responds to.
        checkpoint_serial: u64,
        /// Upper bound (exclusive) of the report scope.
        upper_bound: u64,
        /// Lower bound (inclusive) of the report scope.
        lower_bound: u64,
        /// List of received byte range claims (absolute offsets).
        claims: Vec<ReceptionClaim>,
    },
    /// A report acknowledgement segment.
    ReportAck {
        /// Session identifier.
        session_id: SessionId,
        /// The report serial number being acknowledged.
        report_serial: u64,
    },
    /// A cancel segment (from sender or receiver).
    Cancel {
        /// Session identifier.
        session_id: SessionId,
        /// The reason for cancellation.
        reason: CancelReason,
        /// Which side initiated the cancellation.
        direction: CancelDirection,
    },
    /// A cancel acknowledgement segment.
    CancelAck {
        /// Session identifier.
        session_id: SessionId,
        /// Which side's cancel is being acknowledged.
        direction: CancelDirection,
    },
}

/// Errors that can occur when decoding an LTP segment.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SegmentError {
    /// The segment type code is not a recognized LTP segment type.
    #[error("unknown segment type code: {0}")]
    UnknownType(u8),

    /// The segment byte sequence is truncated (insufficient bytes for the body).
    #[error("segment truncated: insufficient bytes for segment body")]
    Truncated,

    /// The cancel reason code is not a recognized value.
    #[error("invalid cancel reason code: {0}")]
    InvalidReason(u8),

    /// An SDNV decode error occurred while parsing a segment field.
    #[error("SDNV decode error: {0}")]
    Sdnv(#[from] SdnvError),
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// Returns the segment type code for a given segment.
fn segment_type_code(segment: &Segment) -> u8 {
    match segment {
        Segment::Data { segment_type, .. } => *segment_type as u8,
        Segment::Report { .. } => SegmentType::Report as u8,
        Segment::ReportAck { .. } => SegmentType::ReportAck as u8,
        Segment::Cancel { direction, .. } => match direction {
            CancelDirection::FromSender => SegmentType::CancelFromSender as u8,
            CancelDirection::FromReceiver => SegmentType::CancelFromReceiver as u8,
        },
        Segment::CancelAck { direction, .. } => match direction {
            CancelDirection::FromSender => SegmentType::CancelAckToSender as u8,
            CancelDirection::FromReceiver => SegmentType::CancelAckToReceiver as u8,
        },
    }
}

/// Returns the session ID for a given segment.
fn segment_session_id(segment: &Segment) -> &SessionId {
    match segment {
        Segment::Data { session_id, .. }
        | Segment::Report { session_id, .. }
        | Segment::ReportAck { session_id, .. }
        | Segment::Cancel { session_id, .. }
        | Segment::CancelAck { session_id, .. } => session_id,
    }
}

/// Computes the encoded wire size of a segment without actually encoding it.
///
/// This is useful for pre-allocating buffers before encoding.
pub fn encoded_size(segment: &Segment) -> usize {
    let session_id = segment_session_id(segment);

    // Header: 1 byte (version|type) + engine_id SDNV + session_number SDNV + 1 byte (extension counts)
    let header_size = 1
        + sdnv::encoded_len(session_id.engine_id)
        + sdnv::encoded_len(session_id.session_number)
        + 1;

    let body_size = match segment {
        Segment::Data {
            client_service_id,
            offset,
            data,
            checkpoint,
            segment_type,
            ..
        } => {
            let data_len = data.len() as u64;
            let mut size = sdnv::encoded_len(*client_service_id)
                + sdnv::encoded_len(*offset)
                + sdnv::encoded_len(data_len);

            if segment_type.is_checkpoint() {
                if let Some(ckpt) = checkpoint {
                    size += sdnv::encoded_len(ckpt.serial);
                    size += sdnv::encoded_len(ckpt.responding_report_serial);
                }
            }

            size + data.len()
        }
        Segment::Report {
            report_serial,
            checkpoint_serial,
            upper_bound,
            lower_bound,
            claims,
            ..
        } => {
            let mut size = sdnv::encoded_len(*report_serial)
                + sdnv::encoded_len(*checkpoint_serial)
                + sdnv::encoded_len(*upper_bound)
                + sdnv::encoded_len(*lower_bound)
                + sdnv::encoded_len(claims.len() as u64);

            for claim in claims {
                let relative_offset = claim.offset.saturating_sub(*lower_bound);
                size += sdnv::encoded_len(relative_offset);
                size += sdnv::encoded_len(claim.length);
            }

            size
        }
        Segment::ReportAck { report_serial, .. } => sdnv::encoded_len(*report_serial),
        Segment::Cancel { reason, .. } => {
            // Single reason code byte
            let _ = reason;
            1
        }
        Segment::CancelAck { .. } => {
            // Empty body
            0
        }
    };

    header_size + body_size
}

/// Encodes an LTP segment into the provided buffer in wire format.
///
/// The encoding follows RFC 5326 §3:
/// - Header: version nibble (0) | type code nibble, engine ID (SDNV),
///   session number (SDNV), extension counts byte (0x00).
/// - Body: variant-specific fields.
///
/// Report segment claim offsets are encoded relative to `lower_bound` on the wire.
pub fn encode(segment: &Segment, buf: &mut impl BufMut) {
    let session_id = segment_session_id(segment);
    let type_code = segment_type_code(segment);

    // Header byte: version (0) in upper nibble, type code in lower nibble
    buf.put_u8(type_code & 0x0F);

    // Engine ID and session number as SDNVs
    sdnv::encode(session_id.engine_id, buf);
    sdnv::encode(session_id.session_number, buf);

    // Extension counts byte: 0x00 (no extensions emitted)
    buf.put_u8(0x00);

    // Body
    match segment {
        Segment::Data {
            client_service_id,
            offset,
            data,
            checkpoint,
            segment_type,
            ..
        } => {
            sdnv::encode(*client_service_id, buf);
            sdnv::encode(*offset, buf);
            sdnv::encode(data.len() as u64, buf);

            if segment_type.is_checkpoint() {
                if let Some(ckpt) = checkpoint {
                    sdnv::encode(ckpt.serial, buf);
                    sdnv::encode(ckpt.responding_report_serial, buf);
                }
            }

            buf.put_slice(data);
        }
        Segment::Report {
            report_serial,
            checkpoint_serial,
            upper_bound,
            lower_bound,
            claims,
            ..
        } => {
            sdnv::encode(*report_serial, buf);
            sdnv::encode(*checkpoint_serial, buf);
            sdnv::encode(*upper_bound, buf);
            sdnv::encode(*lower_bound, buf);
            sdnv::encode(claims.len() as u64, buf);

            for claim in claims {
                // CRITICAL: Encode claim offsets RELATIVE to lower_bound
                let relative_offset = claim.offset.saturating_sub(*lower_bound);
                sdnv::encode(relative_offset, buf);
                sdnv::encode(claim.length, buf);
            }
        }
        Segment::ReportAck { report_serial, .. } => {
            sdnv::encode(*report_serial, buf);
        }
        Segment::Cancel { reason, .. } => {
            buf.put_u8(*reason as u8);
        }
        Segment::CancelAck { .. } => {
            // Empty body — nothing to encode beyond the header
        }
    }
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Parses and skips a single extension (tag SDNV, length SDNV, value bytes).
fn skip_extension(buf: &mut impl Buf) -> Result<(), SegmentError> {
    // tag SDNV
    let _tag = sdnv::decode(buf)?;
    // length SDNV
    let length = sdnv::decode(buf)?;
    // value bytes
    let length = length as usize;
    if buf.remaining() < length {
        return Err(SegmentError::Truncated);
    }
    buf.advance(length);
    Ok(())
}

/// Decodes an LTP segment from the provided buffer.
///
/// The decoding follows RFC 5326 §3:
/// - Header: version nibble (must be 0) | type code nibble, engine ID (SDNV),
///   session number (SDNV), extension counts byte.
/// - Header extensions are parsed and skipped.
/// - Body: variant-specific fields.
/// - Trailer extensions are parsed and skipped.
///
/// Report segment claim offsets on the wire are relative to `lower_bound`.
/// The decoder reconstructs absolute offsets: `absolute_offset = lower_bound + wire_offset`.
pub fn decode(buf: &mut impl Buf) -> Result<Segment, SegmentError> {
    // --- Header ---
    if buf.remaining() < 1 {
        return Err(SegmentError::Truncated);
    }

    let first_byte = buf.get_u8();
    let version = (first_byte >> 4) & 0x0F;
    if version != 0 {
        return Err(SegmentError::UnknownType(first_byte));
    }
    let type_code = first_byte & 0x0F;
    let segment_type = SegmentType::try_from(type_code)?;

    // Engine ID and session number
    let engine_id = sdnv::decode(buf)?;
    let session_number = sdnv::decode(buf)?;
    let session_id = SessionId {
        engine_id,
        session_number,
    };

    // Extension counts byte
    if buf.remaining() < 1 {
        return Err(SegmentError::Truncated);
    }
    let ext_byte = buf.get_u8();
    let header_ext_count = (ext_byte >> 4) & 0x0F;
    let trailer_ext_count = ext_byte & 0x0F;

    // Parse and skip header extensions
    for _ in 0..header_ext_count {
        skip_extension(buf)?;
    }

    // --- Body (variant-specific) ---
    let segment = match segment_type {
        SegmentType::RedData
        | SegmentType::RedCheckpoint
        | SegmentType::RedEorp
        | SegmentType::RedEob
        | SegmentType::GreenData
        | SegmentType::GreenEob => decode_data_segment(buf, session_id, segment_type)?,

        SegmentType::Report => decode_report_segment(buf, session_id)?,

        SegmentType::ReportAck => decode_report_ack(buf, session_id)?,

        SegmentType::CancelFromSender => decode_cancel(buf, session_id, CancelDirection::FromSender)?,

        SegmentType::CancelAckToSender => Segment::CancelAck {
            session_id,
            direction: CancelDirection::FromSender,
        },

        SegmentType::CancelFromReceiver => {
            decode_cancel(buf, session_id, CancelDirection::FromReceiver)?
        }

        SegmentType::CancelAckToReceiver => Segment::CancelAck {
            session_id,
            direction: CancelDirection::FromReceiver,
        },
    };

    // Parse and skip trailer extensions
    for _ in 0..trailer_ext_count {
        skip_extension(buf)?;
    }

    Ok(segment)
}

/// Decodes a data segment body (types 0–4, 7).
fn decode_data_segment(
    buf: &mut impl Buf,
    session_id: SessionId,
    segment_type: SegmentType,
) -> Result<Segment, SegmentError> {
    let client_service_id = sdnv::decode(buf)?;
    let offset = sdnv::decode(buf)?;
    let data_length = sdnv::decode(buf)? as usize;

    // Checkpoint fields for types 1, 2, 3
    let checkpoint = if segment_type.is_checkpoint() {
        let serial = sdnv::decode(buf)?;
        let responding_report_serial = sdnv::decode(buf)?;
        Some(CheckpointInfo {
            serial,
            responding_report_serial,
        })
    } else {
        None
    };

    // Raw data bytes
    if buf.remaining() < data_length {
        return Err(SegmentError::Truncated);
    }
    let data = buf.copy_to_bytes(data_length);

    Ok(Segment::Data {
        session_id,
        segment_type,
        client_service_id,
        offset,
        data,
        checkpoint,
    })
}

/// Decodes a report segment body (type 8).
fn decode_report_segment(buf: &mut impl Buf, session_id: SessionId) -> Result<Segment, SegmentError> {
    let report_serial = sdnv::decode(buf)?;
    let checkpoint_serial = sdnv::decode(buf)?;
    let upper_bound = sdnv::decode(buf)?;
    let lower_bound = sdnv::decode(buf)?;
    let claim_count = sdnv::decode(buf)? as usize;

    let mut claims = Vec::with_capacity(claim_count);
    for _ in 0..claim_count {
        // CRITICAL: Wire offsets are relative to lower_bound.
        // Reconstruct absolute offsets: absolute_offset = lower_bound + wire_offset
        let relative_offset = sdnv::decode(buf)?;
        let length = sdnv::decode(buf)?;
        claims.push(ReceptionClaim {
            offset: lower_bound + relative_offset,
            length,
        });
    }

    Ok(Segment::Report {
        session_id,
        report_serial,
        checkpoint_serial,
        upper_bound,
        lower_bound,
        claims,
    })
}

/// Decodes a report-ack body (type 9).
fn decode_report_ack(buf: &mut impl Buf, session_id: SessionId) -> Result<Segment, SegmentError> {
    let report_serial = sdnv::decode(buf)?;
    Ok(Segment::ReportAck {
        session_id,
        report_serial,
    })
}

/// Decodes a cancel body (types 12, 14): single reason code byte.
fn decode_cancel(
    buf: &mut impl Buf,
    session_id: SessionId,
    direction: CancelDirection,
) -> Result<Segment, SegmentError> {
    if buf.remaining() < 1 {
        return Err(SegmentError::Truncated);
    }
    let reason_byte = buf.get_u8();
    let reason = match reason_byte {
        0 => CancelReason::ByUser,
        1 => CancelReason::ClientSvcUnreachable,
        2 => CancelReason::RetransmitLimitExceeded,
        3 => CancelReason::MiscoloredSegment,
        4 => CancelReason::ByEngine,
        _ => return Err(SegmentError::InvalidReason(reason_byte)),
    };

    Ok(Segment::Cancel {
        session_id,
        reason,
        direction,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_type_from_u8_valid() {
        assert_eq!(SegmentType::try_from(0).unwrap(), SegmentType::RedData);
        assert_eq!(SegmentType::try_from(1).unwrap(), SegmentType::RedCheckpoint);
        assert_eq!(SegmentType::try_from(2).unwrap(), SegmentType::RedEorp);
        assert_eq!(SegmentType::try_from(3).unwrap(), SegmentType::RedEob);
        assert_eq!(SegmentType::try_from(4).unwrap(), SegmentType::GreenData);
        assert_eq!(SegmentType::try_from(7).unwrap(), SegmentType::GreenEob);
        assert_eq!(SegmentType::try_from(8).unwrap(), SegmentType::Report);
        assert_eq!(SegmentType::try_from(9).unwrap(), SegmentType::ReportAck);
        assert_eq!(
            SegmentType::try_from(12).unwrap(),
            SegmentType::CancelFromSender
        );
        assert_eq!(
            SegmentType::try_from(13).unwrap(),
            SegmentType::CancelAckToSender
        );
        assert_eq!(
            SegmentType::try_from(14).unwrap(),
            SegmentType::CancelFromReceiver
        );
        assert_eq!(
            SegmentType::try_from(15).unwrap(),
            SegmentType::CancelAckToReceiver
        );
    }

    #[test]
    fn segment_type_from_u8_invalid() {
        assert_eq!(
            SegmentType::try_from(5),
            Err(SegmentError::UnknownType(5))
        );
        assert_eq!(
            SegmentType::try_from(6),
            Err(SegmentError::UnknownType(6))
        );
        assert_eq!(
            SegmentType::try_from(10),
            Err(SegmentError::UnknownType(10))
        );
        assert_eq!(
            SegmentType::try_from(11),
            Err(SegmentError::UnknownType(11))
        );
        assert_eq!(
            SegmentType::try_from(16),
            Err(SegmentError::UnknownType(16))
        );
        assert_eq!(
            SegmentType::try_from(255),
            Err(SegmentError::UnknownType(255))
        );
    }

    #[test]
    fn segment_type_is_checkpoint() {
        assert!(!SegmentType::RedData.is_checkpoint());
        assert!(SegmentType::RedCheckpoint.is_checkpoint());
        assert!(SegmentType::RedEorp.is_checkpoint());
        assert!(SegmentType::RedEob.is_checkpoint());
        assert!(!SegmentType::GreenData.is_checkpoint());
        assert!(!SegmentType::GreenEob.is_checkpoint());
        assert!(!SegmentType::Report.is_checkpoint());
        assert!(!SegmentType::ReportAck.is_checkpoint());
    }

    #[test]
    fn segment_type_is_data() {
        assert!(SegmentType::RedData.is_data());
        assert!(SegmentType::RedCheckpoint.is_data());
        assert!(SegmentType::RedEorp.is_data());
        assert!(SegmentType::RedEob.is_data());
        assert!(SegmentType::GreenData.is_data());
        assert!(SegmentType::GreenEob.is_data());
        assert!(!SegmentType::Report.is_data());
        assert!(!SegmentType::ReportAck.is_data());
        assert!(!SegmentType::CancelFromSender.is_data());
        assert!(!SegmentType::CancelAckToSender.is_data());
        assert!(!SegmentType::CancelFromReceiver.is_data());
        assert!(!SegmentType::CancelAckToReceiver.is_data());
    }

    #[test]
    fn segment_type_is_red() {
        assert!(SegmentType::RedData.is_red());
        assert!(SegmentType::RedCheckpoint.is_red());
        assert!(SegmentType::RedEorp.is_red());
        assert!(SegmentType::RedEob.is_red());
        assert!(!SegmentType::GreenData.is_red());
        assert!(!SegmentType::GreenEob.is_red());
        assert!(!SegmentType::Report.is_red());
    }

    #[test]
    fn segment_type_is_green() {
        assert!(!SegmentType::RedData.is_green());
        assert!(!SegmentType::RedCheckpoint.is_green());
        assert!(SegmentType::GreenData.is_green());
        assert!(SegmentType::GreenEob.is_green());
        assert!(!SegmentType::Report.is_green());
    }

    #[test]
    fn segment_error_from_sdnv_error() {
        let sdnv_err = SdnvError::Overflow;
        let seg_err: SegmentError = sdnv_err.into();
        assert_eq!(seg_err, SegmentError::Sdnv(SdnvError::Overflow));
    }

    #[test]
    fn segment_error_display() {
        assert_eq!(
            SegmentError::UnknownType(5).to_string(),
            "unknown segment type code: 5"
        );
        assert_eq!(
            SegmentError::Truncated.to_string(),
            "segment truncated: insufficient bytes for segment body"
        );
        assert_eq!(
            SegmentError::InvalidReason(99).to_string(),
            "invalid cancel reason code: 99"
        );
        assert_eq!(
            SegmentError::Sdnv(SdnvError::Overflow).to_string(),
            "SDNV decode error: SDNV exceeds 10 bytes (64-bit overflow)"
        );
    }

    #[test]
    fn segment_data_construction() {
        let seg = Segment::Data {
            session_id: SessionId {
                engine_id: 1,
                session_number: 42,
            },
            segment_type: SegmentType::RedCheckpoint,
            client_service_id: 1,
            offset: 0,
            data: Bytes::from_static(b"hello"),
            checkpoint: Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        };
        match seg {
            Segment::Data {
                session_id,
                segment_type,
                checkpoint,
                ..
            } => {
                assert_eq!(session_id.engine_id, 1);
                assert_eq!(session_id.session_number, 42);
                assert!(segment_type.is_checkpoint());
                assert!(checkpoint.is_some());
            }
            _ => panic!("expected Data variant"),
        }
    }

    #[test]
    fn segment_report_construction() {
        let seg = Segment::Report {
            session_id: SessionId {
                engine_id: 2,
                session_number: 100,
            },
            report_serial: 1,
            checkpoint_serial: 1,
            upper_bound: 1000,
            lower_bound: 0,
            claims: vec![
                ReceptionClaim {
                    offset: 0,
                    length: 500,
                },
                ReceptionClaim {
                    offset: 600,
                    length: 400,
                },
            ],
        };
        match seg {
            Segment::Report { claims, .. } => {
                assert_eq!(claims.len(), 2);
                assert_eq!(claims[0].offset, 0);
                assert_eq!(claims[0].length, 500);
                assert_eq!(claims[1].offset, 600);
                assert_eq!(claims[1].length, 400);
            }
            _ => panic!("expected Report variant"),
        }
    }

    #[test]
    fn segment_cancel_construction() {
        let seg = Segment::Cancel {
            session_id: SessionId {
                engine_id: 3,
                session_number: 7,
            },
            reason: CancelReason::RetransmitLimitExceeded,
            direction: CancelDirection::FromSender,
        };
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::RetransmitLimitExceeded);
                assert_eq!(direction, CancelDirection::FromSender);
            }
            _ => panic!("expected Cancel variant"),
        }
    }

    #[test]
    fn segment_cancel_ack_construction() {
        let seg = Segment::CancelAck {
            session_id: SessionId {
                engine_id: 4,
                session_number: 99,
            },
            direction: CancelDirection::FromReceiver,
        };
        match seg {
            Segment::CancelAck { direction, .. } => {
                assert_eq!(direction, CancelDirection::FromReceiver);
            }
            _ => panic!("expected CancelAck variant"),
        }
    }

    // -----------------------------------------------------------------------
    // Encode tests
    // -----------------------------------------------------------------------

    use bytes::BytesMut;

    #[test]
    fn encode_data_segment_no_checkpoint() {
        let seg = Segment::Data {
            session_id: SessionId {
                engine_id: 1,
                session_number: 42,
            },
            segment_type: SegmentType::RedData,
            client_service_id: 1,
            offset: 0,
            data: Bytes::from_static(b"hello"),
            checkpoint: None,
        };

        let mut buf = BytesMut::new();
        encode(&seg, &mut buf);

        // Header: type=0 (RedData), engine_id=1, session_number=42, ext=0x00
        assert_eq!(buf[0], 0x00); // version 0 | type 0
        assert_eq!(buf[1], 0x01); // engine_id SDNV = 1
        assert_eq!(buf[2], 0x2A); // session_number SDNV = 42
        assert_eq!(buf[3], 0x00); // extension counts = 0

        // Body: client_service_id=1, offset=0, length=5, data="hello"
        assert_eq!(buf[4], 0x01); // client_service_id = 1
        assert_eq!(buf[5], 0x00); // offset = 0
        assert_eq!(buf[6], 0x05); // length = 5
        assert_eq!(&buf[7..12], b"hello");
        assert_eq!(buf.len(), 12);
    }

    #[test]
    fn encode_data_segment_with_checkpoint() {
        let seg = Segment::Data {
            session_id: SessionId {
                engine_id: 1,
                session_number: 1,
            },
            segment_type: SegmentType::RedCheckpoint,
            client_service_id: 1,
            offset: 100,
            data: Bytes::from_static(b"AB"),
            checkpoint: Some(CheckpointInfo {
                serial: 5,
                responding_report_serial: 0,
            }),
        };

        let mut buf = BytesMut::new();
        encode(&seg, &mut buf);

        // Header: type=1 (RedCheckpoint)
        assert_eq!(buf[0], 0x01);
        assert_eq!(buf[1], 0x01); // engine_id = 1
        assert_eq!(buf[2], 0x01); // session_number = 1
        assert_eq!(buf[3], 0x00); // ext counts

        // Body: client_service_id=1, offset=100, length=2, ckpt_serial=5, resp_rpt=0, data
        assert_eq!(buf[4], 0x01); // client_service_id = 1
        assert_eq!(buf[5], 0x64); // offset = 100
        assert_eq!(buf[6], 0x02); // length = 2
        assert_eq!(buf[7], 0x05); // checkpoint serial = 5
        assert_eq!(buf[8], 0x00); // responding report serial = 0
        assert_eq!(&buf[9..11], b"AB");
        assert_eq!(buf.len(), 11);
    }

    #[test]
    fn encode_report_segment_relative_offsets() {
        let seg = Segment::Report {
            session_id: SessionId {
                engine_id: 2,
                session_number: 10,
            },
            report_serial: 1,
            checkpoint_serial: 1,
            upper_bound: 1000,
            lower_bound: 500,
            claims: vec![
                ReceptionClaim {
                    offset: 500,
                    length: 200,
                },
                ReceptionClaim {
                    offset: 800,
                    length: 100,
                },
            ],
        };

        let mut buf = BytesMut::new();
        encode(&seg, &mut buf);

        // Header: type=8 (Report)
        assert_eq!(buf[0], 0x08);
        assert_eq!(buf[1], 0x02); // engine_id = 2
        assert_eq!(buf[2], 0x0A); // session_number = 10
        assert_eq!(buf[3], 0x00); // ext counts

        // Body: report_serial=1, ckpt_serial=1, upper=1000, lower=500, count=2
        let mut cursor = &buf[4..];
        let report_serial = sdnv::decode(&mut cursor).unwrap();
        assert_eq!(report_serial, 1);
        let ckpt_serial = sdnv::decode(&mut cursor).unwrap();
        assert_eq!(ckpt_serial, 1);
        let upper = sdnv::decode(&mut cursor).unwrap();
        assert_eq!(upper, 1000);
        let lower = sdnv::decode(&mut cursor).unwrap();
        assert_eq!(lower, 500);
        let count = sdnv::decode(&mut cursor).unwrap();
        assert_eq!(count, 2);

        // Claims: relative offsets (500-500=0, 800-500=300)
        let claim0_offset = sdnv::decode(&mut cursor).unwrap();
        assert_eq!(claim0_offset, 0); // 500 - 500 = 0
        let claim0_length = sdnv::decode(&mut cursor).unwrap();
        assert_eq!(claim0_length, 200);
        let claim1_offset = sdnv::decode(&mut cursor).unwrap();
        assert_eq!(claim1_offset, 300); // 800 - 500 = 300
        let claim1_length = sdnv::decode(&mut cursor).unwrap();
        assert_eq!(claim1_length, 100);
    }

    #[test]
    fn encode_report_ack_segment() {
        let seg = Segment::ReportAck {
            session_id: SessionId {
                engine_id: 5,
                session_number: 77,
            },
            report_serial: 3,
        };

        let mut buf = BytesMut::new();
        encode(&seg, &mut buf);

        assert_eq!(buf[0], 0x09); // type = ReportAck
        assert_eq!(buf[1], 0x05); // engine_id = 5
        assert_eq!(buf[2], 0x4D); // session_number = 77
        assert_eq!(buf[3], 0x00); // ext counts
        assert_eq!(buf[4], 0x03); // report_serial = 3
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn encode_cancel_segment() {
        let seg = Segment::Cancel {
            session_id: SessionId {
                engine_id: 3,
                session_number: 7,
            },
            reason: CancelReason::RetransmitLimitExceeded,
            direction: CancelDirection::FromSender,
        };

        let mut buf = BytesMut::new();
        encode(&seg, &mut buf);

        assert_eq!(buf[0], 0x0C); // type = CancelFromSender (12)
        assert_eq!(buf[1], 0x03); // engine_id = 3
        assert_eq!(buf[2], 0x07); // session_number = 7
        assert_eq!(buf[3], 0x00); // ext counts
        assert_eq!(buf[4], 0x02); // reason = RetransmitLimitExceeded (2)
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn encode_cancel_from_receiver() {
        let seg = Segment::Cancel {
            session_id: SessionId {
                engine_id: 10,
                session_number: 1,
            },
            reason: CancelReason::MiscoloredSegment,
            direction: CancelDirection::FromReceiver,
        };

        let mut buf = BytesMut::new();
        encode(&seg, &mut buf);

        assert_eq!(buf[0], 0x0E); // type = CancelFromReceiver (14)
        assert_eq!(buf[4], 0x03); // reason = MiscoloredSegment (3)
    }

    #[test]
    fn encode_cancel_ack_to_sender() {
        let seg = Segment::CancelAck {
            session_id: SessionId {
                engine_id: 4,
                session_number: 99,
            },
            direction: CancelDirection::FromSender,
        };

        let mut buf = BytesMut::new();
        encode(&seg, &mut buf);

        assert_eq!(buf[0], 0x0D); // type = CancelAckToSender (13)
        assert_eq!(buf[1], 0x04); // engine_id = 4
        assert_eq!(buf[2], 0x63); // session_number = 99
        assert_eq!(buf[3], 0x00); // ext counts
        assert_eq!(buf.len(), 4); // no body
    }

    #[test]
    fn encode_cancel_ack_to_receiver() {
        let seg = Segment::CancelAck {
            session_id: SessionId {
                engine_id: 1,
                session_number: 1,
            },
            direction: CancelDirection::FromReceiver,
        };

        let mut buf = BytesMut::new();
        encode(&seg, &mut buf);

        assert_eq!(buf[0], 0x0F); // type = CancelAckToReceiver (15)
        assert_eq!(buf.len(), 4); // header only, no body
    }

    #[test]
    fn encoded_size_matches_actual_encode() {
        let segments = vec![
            Segment::Data {
                session_id: SessionId {
                    engine_id: 1,
                    session_number: 42,
                },
                segment_type: SegmentType::RedData,
                client_service_id: 1,
                offset: 0,
                data: Bytes::from_static(b"hello"),
                checkpoint: None,
            },
            Segment::Data {
                session_id: SessionId {
                    engine_id: 1000,
                    session_number: 65535,
                },
                segment_type: SegmentType::RedEob,
                client_service_id: 1,
                offset: 4096,
                data: Bytes::from(vec![0u8; 100]),
                checkpoint: Some(CheckpointInfo {
                    serial: 999,
                    responding_report_serial: 42,
                }),
            },
            Segment::Report {
                session_id: SessionId {
                    engine_id: 2,
                    session_number: 10,
                },
                report_serial: 1,
                checkpoint_serial: 1,
                upper_bound: 10000,
                lower_bound: 5000,
                claims: vec![
                    ReceptionClaim {
                        offset: 5000,
                        length: 2000,
                    },
                    ReceptionClaim {
                        offset: 8000,
                        length: 1000,
                    },
                ],
            },
            Segment::ReportAck {
                session_id: SessionId {
                    engine_id: 5,
                    session_number: 77,
                },
                report_serial: 3,
            },
            Segment::Cancel {
                session_id: SessionId {
                    engine_id: 3,
                    session_number: 7,
                },
                reason: CancelReason::ByUser,
                direction: CancelDirection::FromSender,
            },
            Segment::CancelAck {
                session_id: SessionId {
                    engine_id: 4,
                    session_number: 99,
                },
                direction: CancelDirection::FromReceiver,
            },
        ];

        for seg in &segments {
            let predicted = encoded_size(seg);
            let mut buf = BytesMut::new();
            encode(seg, &mut buf);
            assert_eq!(
                predicted,
                buf.len(),
                "encoded_size mismatch for segment: {:?}",
                seg
            );
        }
    }

    #[test]
    fn encode_green_data_segment() {
        let seg = Segment::Data {
            session_id: SessionId {
                engine_id: 1,
                session_number: 5,
            },
            segment_type: SegmentType::GreenData,
            client_service_id: 1,
            offset: 0,
            data: Bytes::from_static(b"green"),
            checkpoint: None,
        };

        let mut buf = BytesMut::new();
        encode(&seg, &mut buf);

        assert_eq!(buf[0], 0x04); // type = GreenData (4)
    }

    #[test]
    fn encode_green_eob_segment() {
        let seg = Segment::Data {
            session_id: SessionId {
                engine_id: 1,
                session_number: 5,
            },
            segment_type: SegmentType::GreenEob,
            client_service_id: 1,
            offset: 100,
            data: Bytes::from_static(b"end"),
            checkpoint: None,
        };

        let mut buf = BytesMut::new();
        encode(&seg, &mut buf);

        assert_eq!(buf[0], 0x07); // type = GreenEob (7)
    }

    // -----------------------------------------------------------------------
    // Decode + round-trip tests
    // -----------------------------------------------------------------------

    #[test]
    fn round_trip_data_with_checkpoint() {
        let original = Segment::Data {
            session_id: SessionId {
                engine_id: 42,
                session_number: 1000,
            },
            segment_type: SegmentType::RedCheckpoint,
            client_service_id: 1,
            offset: 512,
            data: Bytes::from(vec![0xAB; 64]),
            checkpoint: Some(CheckpointInfo {
                serial: 7,
                responding_report_serial: 3,
            }),
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn round_trip_data_without_checkpoint() {
        let original = Segment::Data {
            session_id: SessionId {
                engine_id: 1,
                session_number: 42,
            },
            segment_type: SegmentType::RedData,
            client_service_id: 1,
            offset: 0,
            data: Bytes::from_static(b"hello world"),
            checkpoint: None,
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn round_trip_data_eorp() {
        let original = Segment::Data {
            session_id: SessionId {
                engine_id: 100,
                session_number: 999,
            },
            segment_type: SegmentType::RedEorp,
            client_service_id: 1,
            offset: 1024,
            data: Bytes::from(vec![0xFF; 128]),
            checkpoint: Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_data_eob() {
        let original = Segment::Data {
            session_id: SessionId {
                engine_id: 5,
                session_number: 3,
            },
            segment_type: SegmentType::RedEob,
            client_service_id: 2,
            offset: 0,
            data: Bytes::from_static(b"block"),
            checkpoint: Some(CheckpointInfo {
                serial: 10,
                responding_report_serial: 5,
            }),
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_green_data() {
        let original = Segment::Data {
            session_id: SessionId {
                engine_id: 1,
                session_number: 5,
            },
            segment_type: SegmentType::GreenData,
            client_service_id: 1,
            offset: 0,
            data: Bytes::from_static(b"green data"),
            checkpoint: None,
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_green_eob() {
        let original = Segment::Data {
            session_id: SessionId {
                engine_id: 1,
                session_number: 5,
            },
            segment_type: SegmentType::GreenEob,
            client_service_id: 1,
            offset: 100,
            data: Bytes::from_static(b"end"),
            checkpoint: None,
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_report() {
        let original = Segment::Report {
            session_id: SessionId {
                engine_id: 2,
                session_number: 10,
            },
            report_serial: 1,
            checkpoint_serial: 1,
            upper_bound: 1000,
            lower_bound: 500,
            claims: vec![
                ReceptionClaim {
                    offset: 500,
                    length: 200,
                },
                ReceptionClaim {
                    offset: 800,
                    length: 100,
                },
            ],
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn round_trip_report_with_zero_lower_bound() {
        let original = Segment::Report {
            session_id: SessionId {
                engine_id: 1,
                session_number: 1,
            },
            report_serial: 5,
            checkpoint_serial: 3,
            upper_bound: 2048,
            lower_bound: 0,
            claims: vec![
                ReceptionClaim {
                    offset: 0,
                    length: 512,
                },
                ReceptionClaim {
                    offset: 1024,
                    length: 512,
                },
            ],
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_report_ack() {
        let original = Segment::ReportAck {
            session_id: SessionId {
                engine_id: 5,
                session_number: 77,
            },
            report_serial: 3,
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn round_trip_cancel_from_sender() {
        let original = Segment::Cancel {
            session_id: SessionId {
                engine_id: 3,
                session_number: 7,
            },
            reason: CancelReason::RetransmitLimitExceeded,
            direction: CancelDirection::FromSender,
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn round_trip_cancel_from_receiver() {
        let original = Segment::Cancel {
            session_id: SessionId {
                engine_id: 10,
                session_number: 200,
            },
            reason: CancelReason::ClientSvcUnreachable,
            direction: CancelDirection::FromReceiver,
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_cancel_all_reasons() {
        let reasons = [
            CancelReason::ByUser,
            CancelReason::ClientSvcUnreachable,
            CancelReason::RetransmitLimitExceeded,
            CancelReason::MiscoloredSegment,
            CancelReason::ByEngine,
        ];

        for reason in reasons {
            let original = Segment::Cancel {
                session_id: SessionId {
                    engine_id: 1,
                    session_number: 1,
                },
                reason,
                direction: CancelDirection::FromSender,
            };

            let mut buf = BytesMut::new();
            encode(&original, &mut buf);
            let mut reader = &buf[..];
            let decoded = decode(&mut reader).unwrap();
            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn round_trip_cancel_ack_to_sender() {
        let original = Segment::CancelAck {
            session_id: SessionId {
                engine_id: 4,
                session_number: 99,
            },
            direction: CancelDirection::FromSender,
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn round_trip_cancel_ack_to_receiver() {
        let original = Segment::CancelAck {
            session_id: SessionId {
                engine_id: 1,
                session_number: 1,
            },
            direction: CancelDirection::FromReceiver,
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
    }

    // -----------------------------------------------------------------------
    // Decode error condition tests
    // -----------------------------------------------------------------------

    #[test]
    fn decode_empty_buffer() {
        let mut buf = &[][..];
        assert_eq!(decode(&mut buf).unwrap_err(), SegmentError::Truncated);
    }

    #[test]
    fn decode_unknown_type_code() {
        // Type code 5 is not a valid segment type
        let data = [0x05, 0x01, 0x01, 0x00];
        let mut buf = &data[..];
        assert_eq!(
            decode(&mut buf).unwrap_err(),
            SegmentError::UnknownType(5)
        );
    }

    #[test]
    fn decode_invalid_version() {
        // Version nibble = 1 (non-zero), type = 0
        let data = [0x10, 0x01, 0x01, 0x00];
        let mut buf = &data[..];
        // Non-zero version results in the full byte being treated as unknown
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn decode_truncated_data_segment() {
        // Valid header for RedData but body is truncated (claims data_length=10 but no data)
        let mut wire = BytesMut::new();
        wire.put_u8(0x00); // version 0 | type 0 (RedData)
        sdnv::encode(1, &mut wire); // engine_id
        sdnv::encode(1, &mut wire); // session_number
        wire.put_u8(0x00); // ext counts
        sdnv::encode(1, &mut wire); // client_service_id
        sdnv::encode(0, &mut wire); // offset
        sdnv::encode(10, &mut wire); // data_length = 10 (but no data follows)

        let mut reader = &wire[..];
        assert_eq!(decode(&mut reader).unwrap_err(), SegmentError::Truncated);
    }

    #[test]
    fn decode_invalid_cancel_reason() {
        let mut wire = BytesMut::new();
        wire.put_u8(0x0C); // CancelFromSender
        sdnv::encode(1, &mut wire); // engine_id
        sdnv::encode(1, &mut wire); // session_number
        wire.put_u8(0x00); // ext counts
        wire.put_u8(99); // invalid reason code

        let mut reader = &wire[..];
        assert_eq!(
            decode(&mut reader).unwrap_err(),
            SegmentError::InvalidReason(99)
        );
    }

    #[test]
    fn decode_truncated_cancel_body() {
        let mut wire = BytesMut::new();
        wire.put_u8(0x0C); // CancelFromSender
        sdnv::encode(1, &mut wire); // engine_id
        sdnv::encode(1, &mut wire); // session_number
        wire.put_u8(0x00); // ext counts
        // No reason byte follows

        let mut reader = &wire[..];
        assert_eq!(decode(&mut reader).unwrap_err(), SegmentError::Truncated);
    }

    #[test]
    fn decode_sdnv_error_propagation() {
        // Header byte is valid (type 0), but engine_id SDNV is truncated
        let data = [0x00, 0x81]; // continuation bit set but no more bytes
        let mut buf = &data[..];
        match decode(&mut buf).unwrap_err() {
            SegmentError::Sdnv(SdnvError::Incomplete) => {}
            other => panic!("expected Sdnv(Incomplete), got {:?}", other),
        }
    }

    #[test]
    fn decode_with_header_extensions_skipped() {
        // Build a segment with 1 header extension that should be skipped
        let mut wire = BytesMut::new();
        wire.put_u8(0x09); // ReportAck
        sdnv::encode(1, &mut wire); // engine_id
        sdnv::encode(1, &mut wire); // session_number
        wire.put_u8(0x10); // 1 header extension, 0 trailer extensions

        // Header extension: tag=42, length=3, value=[0xAA, 0xBB, 0xCC]
        sdnv::encode(42, &mut wire); // tag
        sdnv::encode(3, &mut wire); // length
        wire.put_slice(&[0xAA, 0xBB, 0xCC]); // value

        // Body: report_serial = 7
        sdnv::encode(7, &mut wire);

        let mut reader = &wire[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(
            decoded,
            Segment::ReportAck {
                session_id: SessionId {
                    engine_id: 1,
                    session_number: 1,
                },
                report_serial: 7,
            }
        );
    }

    #[test]
    fn decode_with_trailer_extensions_skipped() {
        // Build a CancelAck with 1 trailer extension
        let mut wire = BytesMut::new();
        wire.put_u8(0x0D); // CancelAckToSender
        sdnv::encode(1, &mut wire); // engine_id
        sdnv::encode(1, &mut wire); // session_number
        wire.put_u8(0x01); // 0 header extensions, 1 trailer extension

        // CancelAck has empty body, so trailer extension comes next
        // Trailer extension: tag=99, length=2, value=[0x01, 0x02]
        sdnv::encode(99, &mut wire); // tag
        sdnv::encode(2, &mut wire); // length
        wire.put_slice(&[0x01, 0x02]); // value

        let mut reader = &wire[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(
            decoded,
            Segment::CancelAck {
                session_id: SessionId {
                    engine_id: 1,
                    session_number: 1,
                },
                direction: CancelDirection::FromSender,
            }
        );
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn decode_report_reconstructs_absolute_offsets() {
        // Manually build a report segment with relative offsets on the wire
        let mut wire = BytesMut::new();
        wire.put_u8(0x08); // Report
        sdnv::encode(1, &mut wire); // engine_id
        sdnv::encode(1, &mut wire); // session_number
        wire.put_u8(0x00); // ext counts

        sdnv::encode(1, &mut wire); // report_serial
        sdnv::encode(1, &mut wire); // checkpoint_serial
        sdnv::encode(2000, &mut wire); // upper_bound
        sdnv::encode(1000, &mut wire); // lower_bound
        sdnv::encode(2, &mut wire); // claim_count

        // Claim 1: relative offset 0, length 500 → absolute offset 1000
        sdnv::encode(0, &mut wire);
        sdnv::encode(500, &mut wire);
        // Claim 2: relative offset 600, length 400 → absolute offset 1600
        sdnv::encode(600, &mut wire);
        sdnv::encode(400, &mut wire);

        let mut reader = &wire[..];
        let decoded = decode(&mut reader).unwrap();

        match decoded {
            Segment::Report {
                lower_bound,
                claims,
                ..
            } => {
                assert_eq!(lower_bound, 1000);
                assert_eq!(claims.len(), 2);
                // Absolute offsets reconstructed
                assert_eq!(claims[0].offset, 1000); // 1000 + 0
                assert_eq!(claims[0].length, 500);
                assert_eq!(claims[1].offset, 1600); // 1000 + 600
                assert_eq!(claims[1].length, 400);
            }
            _ => panic!("expected Report variant"),
        }
    }

    #[test]
    fn round_trip_large_values() {
        // Test with large SDNV values to exercise multi-byte encoding
        let original = Segment::Data {
            session_id: SessionId {
                engine_id: u64::MAX / 2,
                session_number: 0xFFFF_FFFF,
            },
            segment_type: SegmentType::RedEorp,
            client_service_id: 65535,
            offset: 1_000_000,
            data: Bytes::from(vec![0x42; 256]),
            checkpoint: Some(CheckpointInfo {
                serial: 999_999,
                responding_report_serial: 888_888,
            }),
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_empty_data() {
        let original = Segment::Data {
            session_id: SessionId {
                engine_id: 1,
                session_number: 1,
            },
            segment_type: SegmentType::RedData,
            client_service_id: 1,
            offset: 0,
            data: Bytes::new(),
            checkpoint: None,
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_report_no_claims() {
        let original = Segment::Report {
            session_id: SessionId {
                engine_id: 1,
                session_number: 1,
            },
            report_serial: 1,
            checkpoint_serial: 1,
            upper_bound: 1000,
            lower_bound: 0,
            claims: vec![],
        };

        let mut buf = BytesMut::new();
        encode(&original, &mut buf);
        let mut reader = &buf[..];
        let decoded = decode(&mut reader).unwrap();
        assert_eq!(original, decoded);
    }
}
