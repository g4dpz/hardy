//! LTP protocol engine library for the Hardy DTN implementation.
//!
//! This crate implements the Licklider Transmission Protocol (RFC 5326)
//! protocol engine including SDNV codec, segment wire format encoding/decoding,
//! and export/import session state machines.
//!
//! The crate has no dependency on `hardy-bpa` and is independently testable.

pub mod sdnv;
pub mod segment;
pub mod session;
