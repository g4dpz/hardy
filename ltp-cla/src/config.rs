// Copyright 2026 David Johnson, G4DPZ, AMSAT-UK
// SPDX-License-Identifier: Apache-2.0

//! Configuration types for the LTP CLA.
//!
//! Provides [`Config`] (top-level CLA configuration) and [`SpanConfig`]
//! (per-link parameters) with optional serde support behind the `serde` feature.

use std::net::SocketAddr;

/// Block framing mode for bundle encapsulation within LTP blocks.
///
/// Controls how bundles are packed into and unpacked from LTP client service
/// data blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case"))]
pub enum BlockFraming {
    /// Each bundle is preceded by a 4-byte big-endian length prefix.
    /// Multiple bundles may be aggregated into a single LTP block.
    /// This is Hardy's native format for Hardy-to-Hardy communication.
    #[default]
    LengthPrefixed,

    /// The LTP block contains exactly one raw bundle with no framing.
    /// This is the standard format used by ION and other implementations
    /// per RFC 5326 with BPv7 (one bundle per block, no length prefix).
    None,
}

/// Top-level configuration for an LTP CLA instance.
///
/// Maps to the `clas` entry in the Hardy configuration file:
///
/// ```yaml
/// clas:
///   - name: ltp0
///     type: ltp
///     bind: "[::]:1113"
///     engine-id: 1
///     client-service-id: 1
///     spans:
///       - engine-id: 2
///         address: "10.0.0.2:1113"
///         ...
/// ```
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case"))]
pub struct Config {
    /// Local UDP socket address to bind (default: `[::]:1113`).
    #[cfg_attr(feature = "serde", serde(default = "default_bind_addr"))]
    pub bind: SocketAddr,

    /// Local LTP engine ID. If not specified, derived from the BPA IPN node number.
    #[cfg_attr(feature = "serde", serde(default))]
    pub engine_id: Option<u64>,

    /// Client service ID used in data segments (default: 1 for Bundle Protocol).
    #[cfg_attr(feature = "serde", serde(default = "default_client_service_id"))]
    pub client_service_id: u64,

    /// Pre-configured spans (links to remote LTP engines).
    #[cfg_attr(feature = "serde", serde(default))]
    pub spans: Vec<SpanConfig>,
}

/// Per-span (per-link) configuration for a remote LTP engine.
///
/// Each span defines the parameters for communicating with a single remote
/// LTP engine, including addressing, segment sizing, session limits, timers,
/// and rate control.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case"))]
pub struct SpanConfig {
    /// Remote LTP engine ID.
    pub engine_id: u64,

    /// UDP address of the remote engine (e.g., `"10.0.0.2:1113"`).
    pub address: SocketAddr,

    /// Maximum segment size in bytes (default: 1400).
    ///
    /// Should be set below the path MTU to avoid IP fragmentation.
    #[cfg_attr(feature = "serde", serde(default = "default_max_segment_size"))]
    pub max_segment_size: usize,

    /// Maximum number of concurrent export sessions (default: 100).
    #[cfg_attr(feature = "serde", serde(default = "default_max_export_sessions"))]
    pub max_export_sessions: u32,

    /// Maximum number of concurrent import sessions (default: 100).
    #[cfg_attr(feature = "serde", serde(default = "default_max_import_sessions"))]
    pub max_import_sessions: u32,

    /// Aggregation buffer size limit in bytes (default: 65536).
    ///
    /// When the buffer reaches this size, it is flushed as a new export session.
    #[cfg_attr(feature = "serde", serde(default = "default_aggr_size_limit"))]
    pub aggr_size_limit: usize,

    /// Aggregation time limit in seconds (default: 1).
    ///
    /// After this duration since the first bundle was added, the buffer is flushed.
    #[cfg_attr(feature = "serde", serde(default = "default_aggr_time_limit_secs"))]
    pub aggr_time_limit_secs: u64,

    /// Maximum number of retransmissions before cancelling a session (default: 10).
    #[cfg_attr(feature = "serde", serde(default = "default_max_retransmissions"))]
    pub max_retransmissions: u32,

    /// Transmit rate limit in bits per second (default: 0 = unlimited).
    ///
    /// When non-zero, a token bucket rate limiter constrains the transmit rate.
    #[cfg_attr(feature = "serde", serde(default))]
    pub xmit_rate_bps: u64,

    /// Retransmission cycle duration in seconds (default: 60).
    ///
    /// Timer interval for checkpoint retransmission when no report is received.
    /// Used as a flat fallback when `one_way_light_time_ms` is not configured.
    #[cfg_attr(feature = "serde", serde(default = "default_retransmit_cycle_secs"))]
    pub retransmit_cycle_secs: u64,

    /// One-way light time in milliseconds (default: None = not configured).
    ///
    /// When set, the retransmission timeout is computed as
    /// `2 × (one_way_light_time_ms + one_way_margin_time_ms)` instead of
    /// using the flat `retransmit_cycle_secs`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub one_way_light_time_ms: Option<u64>,

    /// One-way margin time in milliseconds (default: 0).
    ///
    /// Added to `one_way_light_time_ms` to account for processing overhead
    /// and queuing delays when computing the RTT-based retransmission timeout.
    #[cfg_attr(feature = "serde", serde(default))]
    pub one_way_margin_time_ms: u64,

    /// Maximum red data bytes per import session (default: 10485760 = 10 MB).
    ///
    /// Import sessions exceeding this limit are cancelled.
    #[cfg_attr(
        feature = "serde",
        serde(default = "default_max_red_data_bytes_per_session")
    )]
    pub max_red_data_bytes_per_session: u64,

    /// Session inactivity limit in seconds (default: 0 = disabled).
    ///
    /// Import sessions with no data received for this duration are cancelled.
    #[cfg_attr(feature = "serde", serde(default))]
    pub session_inactivity_limit_secs: u64,

    /// Session recreation history buffer size (default: 0 = disabled).
    ///
    /// When non-zero, recently-closed session numbers are remembered to prevent
    /// stale segments from recreating sessions.
    #[cfg_attr(feature = "serde", serde(default))]
    pub session_recreation_history_size: usize,

    /// Deferred report delay in milliseconds (default: 0 = disabled).
    ///
    /// When non-zero, report generation is delayed to allow in-flight segments
    /// to arrive, reducing unnecessary retransmissions.
    #[cfg_attr(feature = "serde", serde(default))]
    pub defer_report_ms: u64,

    /// Intermediate checkpoint interval (default: 0 = disabled, only EORP).
    ///
    /// When non-zero, every Nth red-data segment is marked as a checkpoint
    /// for earlier loss detection on long blocks.
    #[cfg_attr(feature = "serde", serde(default))]
    pub checkpoint_every_n_segments: u32,

    /// Whether to cancel all export sessions when the link goes down (default: false).
    #[cfg_attr(feature = "serde", serde(default))]
    pub purge_on_link_down: bool,

    /// Ping interval in seconds (default: 0 = disabled).
    ///
    /// When non-zero, periodic keepalive probes are sent to detect link failure.
    #[cfg_attr(feature = "serde", serde(default))]
    pub ping_interval_secs: u64,

    /// Node IDs reachable via this span (e.g., `["ipn:2.0"]`).
    ///
    /// These are registered with the BPA as peers during CLA startup.
    #[cfg_attr(feature = "serde", serde(default))]
    pub node_ids: Vec<String>,

    /// Block framing mode (default: `length-prefixed`).
    ///
    /// Use `"none"` for interoperability with ION and other implementations
    /// that send one raw bundle per LTP block without length-prefix framing.
    /// Use `"length-prefixed"` for Hardy-to-Hardy communication where multiple
    /// bundles may be aggregated into a single LTP block.
    #[cfg_attr(feature = "serde", serde(default))]
    pub framing: BlockFraming,

    /// Enable timer suspension on TVR link events (default: true).
    ///
    /// When enabled, all active retransmission and inactivity timers are
    /// suspended on link-down and resumed on link-up per RFC 5326 §6.5/§6.6.
    #[cfg_attr(feature = "serde", serde(default = "default_true"))]
    pub tvr_timer_suspension: bool,

    /// Maximum outbound queue size in bytes during link-down (default: 10 MB).
    ///
    /// Segments produced while the link is down are queued up to this limit.
    /// When exceeded, the oldest segments are evicted to make room.
    #[cfg_attr(
        feature = "serde",
        serde(default = "default_link_down_queue_max_bytes")
    )]
    pub link_down_queue_max_bytes: usize,

    /// Enable dynamic rate control updates from TVR bandwidth (default: true).
    ///
    /// When enabled, link-up events carrying bandwidth information update
    /// the span's token bucket rate limiter to match the contact capacity.
    #[cfg_attr(feature = "serde", serde(default = "default_true"))]
    pub tvr_rate_update: bool,
}

// --- Default value functions for serde ---

fn default_bind_addr() -> SocketAddr {
    "[::]:1113".parse().unwrap()
}

fn default_client_service_id() -> u64 {
    1
}

fn default_max_segment_size() -> usize {
    1400
}

fn default_max_export_sessions() -> u32 {
    100
}

fn default_max_import_sessions() -> u32 {
    100
}

fn default_aggr_size_limit() -> usize {
    65536
}

fn default_aggr_time_limit_secs() -> u64 {
    1
}

fn default_max_retransmissions() -> u32 {
    10
}

fn default_retransmit_cycle_secs() -> u64 {
    60
}

fn default_max_red_data_bytes_per_session() -> u64 {
    10_485_760
}

fn default_true() -> bool {
    true
}

fn default_link_down_queue_max_bytes() -> usize {
    10_485_760
}

// --- Default trait implementations ---

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: default_bind_addr(),
            engine_id: None,
            client_service_id: default_client_service_id(),
            spans: Vec::new(),
        }
    }
}

impl Default for SpanConfig {
    fn default() -> Self {
        Self {
            engine_id: 0,
            address: "0.0.0.0:1113".parse().unwrap(),
            max_segment_size: default_max_segment_size(),
            max_export_sessions: default_max_export_sessions(),
            max_import_sessions: default_max_import_sessions(),
            aggr_size_limit: default_aggr_size_limit(),
            aggr_time_limit_secs: default_aggr_time_limit_secs(),
            max_retransmissions: default_max_retransmissions(),
            xmit_rate_bps: 0,
            retransmit_cycle_secs: default_retransmit_cycle_secs(),
            one_way_light_time_ms: None,
            one_way_margin_time_ms: 0,
            max_red_data_bytes_per_session: default_max_red_data_bytes_per_session(),
            session_inactivity_limit_secs: 0,
            session_recreation_history_size: 0,
            defer_report_ms: 0,
            checkpoint_every_n_segments: 0,
            purge_on_link_down: false,
            ping_interval_secs: 0,
            node_ids: Vec::new(),
            framing: BlockFraming::default(),
            tvr_timer_suspension: default_true(),
            link_down_queue_max_bytes: default_link_down_queue_max_bytes(),
            tvr_rate_update: default_true(),
        }
    }
}
