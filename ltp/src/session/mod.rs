//! LTP session state machines.
//!
//! Contains the export (sender) and import (receiver) session state machines,
//! along with shared types used by both.

pub mod export;
pub mod import;

pub use import::{
    ExtentMap, ImportAction, ImportConfig, ImportSession, ImportState, SegmentColor,
};

/// Uniquely identifies an LTP session as the combination of the sender's
/// engine ID and a per-engine session number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId {
    /// The LTP engine ID of the session sender.
    pub engine_id: u64,
    /// The session number, unique within the scope of the sender engine.
    pub session_number: u64,
}

/// Reason code carried in Cancel Segments (CS and CR).
///
/// Defined per RFC 5326 §3.2.4.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CancelReason {
    /// Cancelled by client service user request.
    ByUser = 0,
    /// Client service is unreachable (wrong client service ID).
    ClientSvcUnreachable = 1,
    /// Retransmission limit exceeded.
    RetransmitLimitExceeded = 2,
    /// Miscolored segment received (red in green session or vice versa).
    MiscoloredSegment = 3,
    /// Cancelled by the LTP engine itself (resource limits, inactivity, etc.).
    ByEngine = 4,
}

/// Direction of a cancel or cancel-ack segment, indicating which side
/// initiated the cancellation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelDirection {
    /// Cancel originated from the sender (CS / CAS).
    FromSender,
    /// Cancel originated from the receiver (CR / CAR).
    FromReceiver,
}
