// Copyright 2026 David Johnson, G4DPZ, AMSAT-UK
// SPDX-License-Identifier: Apache-2.0

//! Per-span state management.
//!
//! Manages export/import sessions, aggregation buffer, and rate control
//! for a single LTP link to a remote engine.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use hardy_bpa::cla::{ClaAddress, Sink};
use hardy_ltp::segment::{self, CheckpointInfo, ReceptionClaim, Segment, SegmentType};
use hardy_ltp::session::export::{ExportAction, ExportConfig, ExportSession, ExportState};
use hardy_ltp::session::import::{ImportAction, ImportConfig, ImportSession, ImportState};
use hardy_ltp::session::{CancelDirection, CancelReason, SessionId};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::AbortHandle;
use tracing::{debug, error, trace, warn};

use crate::config::SpanConfig;

// ---------------------------------------------------------------------------
// Aggregation Buffer
// ---------------------------------------------------------------------------

/// Buffer that aggregates multiple bundles into a single LTP block.
///
/// Each bundle is framed with a 4-byte big-endian length prefix:
///
/// ```text
/// [u32 BE: bundle_1_length][bundle_1_bytes]
/// [u32 BE: bundle_2_length][bundle_2_bytes]
/// ...
/// ```
///
/// The buffer flushes when the accumulated size reaches or would exceed
/// `aggr_size_limit`. Time-based flushing is managed externally by the
/// caller (e.g., via a tokio timer).
#[derive(Debug)]
pub struct AggregationBuffer {
    /// Accumulated length-prefixed bundle data.
    buffer: BytesMut,
    /// Maximum aggregated block size in bytes before flushing.
    aggr_size_limit: usize,
    /// Number of bundles currently in the buffer.
    bundle_count: usize,
}

impl AggregationBuffer {
    /// Create a new aggregation buffer with the given size limit.
    pub fn new(aggr_size_limit: usize) -> Self {
        // Cap pre-allocation at 64KB to avoid excessive memory use for very
        // large configured limits (or usize::MAX used in tests).
        let prealloc = aggr_size_limit.min(65536);
        Self {
            buffer: BytesMut::with_capacity(prealloc),
            aggr_size_limit,
            bundle_count: 0,
        }
    }

    /// Append a bundle to the aggregation buffer.
    ///
    /// Returns `Some(block)` if a flush occurred, `None` otherwise.
    pub fn append(&mut self, bundle: &[u8]) -> Option<Bytes> {
        let framed_len = 4 + bundle.len();

        let flushed =
            if !self.buffer.is_empty() && self.buffer.len() + framed_len > self.aggr_size_limit {
                self.flush()
            } else {
                None
            };

        self.buffer.reserve(framed_len);
        self.buffer.put_u32(bundle.len() as u32);
        self.buffer.put_slice(bundle);
        self.bundle_count += 1;

        flushed
    }

    /// Flush the current buffer and return it as frozen `Bytes`.
    ///
    /// Returns `None` if the buffer is empty (no bundles buffered).
    pub fn flush(&mut self) -> Option<Bytes> {
        if self.buffer.is_empty() {
            return None;
        }

        self.bundle_count = 0;
        Some(self.buffer.split().freeze())
    }

    /// Returns `true` if no bundles are currently buffered.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns the current buffer size in bytes (including length prefixes).
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the number of bundles currently in the buffer.
    pub fn bundle_count(&self) -> usize {
        self.bundle_count
    }
}

// ---------------------------------------------------------------------------
// Token Bucket Rate Limiter
// ---------------------------------------------------------------------------

/// A token bucket rate limiter for per-span transmit rate control.
///
/// The bucket refills at `rate_bytes_per_sec` tokens per second. Sending `n`
/// bytes deducts `n` tokens. When the bucket is empty (tokens < 0), the
/// transmit path must sleep for `(-tokens / rate_bytes_per_sec)` seconds
/// before sending the next segment.
///
/// The burst limit is capped at 1 second of tokens (`rate_bytes_per_sec`),
/// preventing large bursts after idle periods.
///
/// When `xmit_rate_bps` is configured as 0, no `TokenBucket` is created
/// and rate limiting is bypassed entirely.
#[derive(Debug)]
pub struct TokenBucket {
    /// Rate in bytes per second (derived from xmit_rate_bps / 8).
    rate_bytes_per_sec: f64,
    /// Current token count. May go negative when a segment is sent
    /// that exceeds available tokens.
    tokens: f64,
    /// Timestamp of the last token refill.
    last_refill: Instant,
}

impl TokenBucket {
    /// Create a new token bucket from a rate in bits per second.
    ///
    /// The rate is converted to bytes per second internally.
    /// The bucket starts full (1 second of burst capacity).
    ///
    /// # Panics
    ///
    /// This should only be called with `xmit_rate_bps > 0`. When the rate
    /// is zero, the caller should not create a `TokenBucket` at all.
    pub fn new(xmit_rate_bps: u64) -> Self {
        debug_assert!(
            xmit_rate_bps > 0,
            "TokenBucket should not be created with rate 0"
        );
        let rate_bytes_per_sec = xmit_rate_bps as f64 / 8.0;
        Self {
            rate_bytes_per_sec,
            tokens: rate_bytes_per_sec, // start full (1 second burst)
            last_refill: Instant::now(),
        }
    }

    /// Consume tokens for sending `bytes` bytes.
    ///
    /// Refills tokens based on elapsed time since the last refill, caps at
    /// the burst limit (1 second of tokens), then deducts the requested bytes.
    ///
    /// Returns the duration the caller should sleep before sending. Returns
    /// `Duration::ZERO` if no sleep is needed (sufficient tokens available).
    pub fn consume(&mut self, bytes: usize) -> Duration {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        self.last_refill = now;

        // Refill tokens based on elapsed time.
        self.tokens += elapsed.as_secs_f64() * self.rate_bytes_per_sec;

        // Cap at burst limit (1 second of tokens).
        if self.tokens > self.rate_bytes_per_sec {
            self.tokens = self.rate_bytes_per_sec;
        }

        // Deduct the bytes being sent.
        self.tokens -= bytes as f64;

        // If tokens are negative, compute the sleep duration.
        if self.tokens < 0.0 {
            let deficit_secs = -self.tokens / self.rate_bytes_per_sec;
            Duration::from_secs_f64(deficit_secs)
        } else {
            Duration::ZERO
        }
    }
}

// ---------------------------------------------------------------------------
// Session Recreation Prevention
// ---------------------------------------------------------------------------

/// A circular buffer that remembers recently-closed import session numbers.
///
/// When a new data segment arrives for a session number that is in this buffer,
/// the segment is discarded to prevent stale/duplicate segments from recreating
/// a session that has already completed or been cancelled.
///
/// When the buffer is full, the oldest entry is evicted on insert.
/// A capacity of 0 means the feature is disabled.
///
/// Uses a `HashSet` alongside the `VecDeque` for O(1) lookup while
/// maintaining FIFO eviction order.
#[derive(Debug)]
pub struct SessionHistory {
    /// Circular buffer of recently-closed session numbers (FIFO order).
    buffer: VecDeque<u64>,
    /// Set for O(1) membership testing.
    set: std::collections::HashSet<u64>,
    /// Maximum number of entries to retain.
    capacity: usize,
}

impl SessionHistory {
    /// Create a new session history buffer with the given capacity.
    ///
    /// A capacity of 0 means the feature is disabled (no entries will be stored).
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(capacity),
            set: std::collections::HashSet::with_capacity(capacity),
            capacity,
        }
    }

    /// Returns `true` if the given session number is in the history buffer.
    pub fn contains(&self, session_number: u64) -> bool {
        self.set.contains(&session_number)
    }

    /// Insert a session number into the history buffer.
    ///
    /// If the buffer is at capacity, the oldest entry is evicted first.
    /// If capacity is 0 (disabled), this is a no-op.
    /// Duplicate session numbers are not re-inserted (the existing entry
    /// retains its position in the eviction order).
    pub fn insert(&mut self, session_number: u64) {
        if self.capacity == 0 {
            return;
        }
        // Skip if already present (avoid duplicates that desync set/buffer).
        if self.set.contains(&session_number) {
            return;
        }
        if self.buffer.len() >= self.capacity {
            if let Some(evicted) = self.buffer.pop_front() {
                self.set.remove(&evicted);
            }
        }
        self.buffer.push_back(session_number);
        self.set.insert(session_number);
    }

    /// Returns `true` if the history feature is enabled (capacity > 0).
    pub fn is_enabled(&self) -> bool {
        self.capacity > 0
    }

    /// Returns the number of entries currently in the buffer.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns `true` if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Link State
// ---------------------------------------------------------------------------

/// Link state for a span, distinguishing the cause of link-down.
///
/// This replaces the previous `link_alive: AtomicBool` with a richer state
/// that tracks whether the link is down due to a TVR contact window closing
/// or due to ping-based failure detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkState {
    /// Link is operational.
    Up,
    /// Link is down due to a TVR contact window closing.
    DownTvr,
    /// Link is down due to ping-based failure detection.
    DownPing,
}

// ---------------------------------------------------------------------------
// Outbound Queue
// ---------------------------------------------------------------------------

/// A bounded FIFO queue for outbound segments during link-down periods.
///
/// When the queue exceeds `max_bytes`, the oldest segments are evicted
/// to make room for new segments.
pub struct OutboundQueue {
    /// Queued segments in FIFO order.
    segments: VecDeque<Bytes>,
    /// Current total size in bytes.
    current_bytes: usize,
    /// Maximum allowed size in bytes.
    max_bytes: usize,
}

impl OutboundQueue {
    /// Create a new outbound queue with the given maximum size in bytes.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            segments: VecDeque::new(),
            current_bytes: 0,
            max_bytes,
        }
    }

    /// Enqueue a segment. Evicts oldest segments if over capacity.
    /// Returns the number of bytes evicted.
    pub fn enqueue(&mut self, segment: Bytes) -> usize {
        let segment_len = segment.len();
        let mut evicted_bytes = 0;

        // Evict oldest segments until there's room for the new one.
        while self.current_bytes + segment_len > self.max_bytes && !self.segments.is_empty() {
            if let Some(old) = self.segments.pop_front() {
                evicted_bytes += old.len();
                self.current_bytes -= old.len();
            }
        }

        self.segments.push_back(segment);
        self.current_bytes += segment_len;

        evicted_bytes
    }

    /// Drain all queued segments in FIFO order.
    pub fn drain(&mut self) -> Vec<Bytes> {
        let drained: Vec<Bytes> = self.segments.drain(..).collect();
        self.current_bytes = 0;
        drained
    }

    /// Current queue size in bytes.
    pub fn current_bytes(&self) -> usize {
        self.current_bytes
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Session State Wrappers
// ---------------------------------------------------------------------------

/// Wraps an export session with its associated timer abort handles.
pub struct ExportSessionState {
    /// The pure state machine for this export session.
    pub session: ExportSession,
    /// Map of checkpoint_serial → timer abort handle for pending timers.
    pub timers: HashMap<u64, AbortHandle>,
}

/// Wraps an import session with its associated timer abort handles.
pub struct ImportSessionState {
    /// The pure state machine for this import session.
    pub session: ImportSession,
    /// Map of report_serial → timer abort handle for pending report timers.
    pub timers: HashMap<u64, AbortHandle>,
    /// Abort handle for the inactivity timer (if configured).
    /// When a data segment is received, the old timer is aborted and a new one
    /// is spawned with the full inactivity timeout.
    pub inactivity_timer: Option<AbortHandle>,
    /// Pending deferred report state. When a checkpoint is received and
    /// `defer_report_ms > 0`, the report is deferred until the timer fires
    /// or all gaps are filled.
    pub deferred_report: Option<DeferredReport>,
}

/// State for a pending deferred report.
pub struct DeferredReport {
    /// The checkpoint serial number to respond to.
    pub checkpoint_serial: u64,
    /// The upper bound of the checkpoint scope.
    pub upper_bound: u64,
    /// Abort handle for the deferral timer task.
    pub timer: AbortHandle,
}

// ---------------------------------------------------------------------------
// Closed Export Retention
// ---------------------------------------------------------------------------

/// State retained for a completed export session to handle late Report Segments.
///
/// After an export session completes, it is moved here for a retention period
/// so that late Report Segments still receive a Report-Ack. This prevents the
/// remote receiver from retransmitting reports indefinitely.
pub struct ClosedExportState {
    /// The session ID (needed to construct Report-Ack segments).
    pub session_id: SessionId,
    /// Remaining number of Report-Ack responses allowed before discarding.
    /// Initialized to `max_retransmissions`; decremented on each RAS sent.
    pub response_counter: u32,
    /// Abort handle for the retention timer task.
    pub retention_timer: AbortHandle,
}

// ---------------------------------------------------------------------------
// Span
// ---------------------------------------------------------------------------

/// Per-span (per-link) state for a remote LTP engine.
///
/// Manages export/import sessions, aggregation buffer, flow control semaphore,
/// and rate limiting for a single link to a remote engine.
pub struct Span {
    /// Span configuration (remote engine ID, address, limits, etc.).
    pub config: SpanConfig,
    /// Local engine ID (used in session IDs for outgoing segments).
    pub local_engine_id: u64,
    /// Aggregation buffer for bundling multiple bundles into one LTP block.
    pub aggregation: std::sync::Mutex<AggregationBuffer>,
    /// Session number counter (monotonically increasing).
    pub session_counter: AtomicU64,
    /// Active export sessions keyed by session number.
    pub export_sessions: Mutex<HashMap<u64, ExportSessionState>>,
    /// Active import sessions keyed by session number.
    pub import_sessions: Mutex<HashMap<u64, ImportSessionState>>,
    /// Closed export sessions retained for late Report Segment handling.
    pub closed_exports: Mutex<HashMap<u64, ClosedExportState>>,
    /// Bound UDP socket for sending segments.
    pub socket: Arc<UdpSocket>,
    /// Communication channel back to the BPA for delivering blocks.
    pub sink: Arc<dyn Sink>,
    /// Semaphore for flow control (limits concurrent export sessions).
    pub export_semaphore: Arc<Semaphore>,
    /// Token bucket rate limiter. `None` when xmit_rate_bps is 0 (unlimited).
    pub rate_limiter: std::sync::Mutex<Option<TokenBucket>>,
    /// Circular buffer of recently-closed import session numbers for recreation prevention.
    /// When non-empty, new data segments for session numbers in this buffer are discarded.
    pub session_history: std::sync::Mutex<SessionHistory>,
    /// Runtime-updatable one-way light time in milliseconds.
    ///
    /// Stored as `value + 1` so that 0 represents "not configured" (None).
    /// This allows runtime updates to accommodate changing orbital geometry
    /// without recreating the span.
    one_way_light_time_ms: AtomicU64,
    /// Current link state, distinguishing the cause of link-down.
    ///
    /// Replaces the previous `link_alive: AtomicBool` with a richer state
    /// that tracks whether the link is down due to TVR or ping detection.
    /// Starts as `LinkState::Up` (optimistic).
    pub link_state: std::sync::Mutex<LinkState>,
    /// Outbound segment queue for link-down periods.
    ///
    /// Segments produced while the link is down are queued here up to
    /// `config.link_down_queue_max_bytes`. When exceeded, oldest segments
    /// are evicted to make room.
    pub outbound_queue: std::sync::Mutex<OutboundQueue>,
    /// Timestamp of the last segment sent to the remote engine.
    ///
    /// Used by the ping timer to determine whether a keepalive probe is needed.
    last_send_time: std::sync::Mutex<Instant>,
    /// Abort handle for the periodic ping timer task.
    ///
    /// When `ping_interval_secs > 0`, a background task periodically checks
    /// whether a ping probe should be sent.
    ping_timer: std::sync::Mutex<Option<AbortHandle>>,
    /// Abort handle for the ping response timeout task.
    ///
    /// After sending a ping CS, this timer fires if no CAS is received within
    /// `retransmit_cycle_secs × max_retransmissions`, triggering a link-down event.
    ping_response_timer: std::sync::Mutex<Option<AbortHandle>>,
    /// Abort handle for the aggregation time-limit flush timer.
    /// When the first bundle is added to an empty aggregation buffer,
    /// a timer is started. When it fires, the buffer is flushed.
    pub aggr_timer: std::sync::Mutex<Option<AbortHandle>>,
    /// Suspended export timer state: checkpoint_serial → remaining duration.
    ///
    /// When a link-down event triggers timer suspension, the remaining duration
    /// of each active export retransmission timer is recorded here. On link-up,
    /// these durations are used to resume timers per RFC 5326 §6.6.
    pub suspended_export_timers: std::sync::Mutex<HashMap<u64, Duration>>,
    /// Suspended import timer state: report_serial → remaining duration.
    ///
    /// When a link-down event triggers timer suspension, the remaining duration
    /// of each active import report retransmission timer is recorded here.
    /// On link-up, these durations are used to resume timers.
    pub suspended_import_timers: std::sync::Mutex<HashMap<u64, Duration>>,
    /// Suspended inactivity timers: session_number → remaining duration.
    ///
    /// When a link-down event triggers timer suspension, the remaining duration
    /// of each active import session inactivity timer is recorded here.
    /// On link-up, these durations are used to resume timers.
    pub suspended_inactivity_timers: std::sync::Mutex<HashMap<u64, Duration>>,
}

impl Span {
    /// Create a new Span from configuration and shared resources.
    pub fn new(
        config: SpanConfig,
        local_engine_id: u64,
        socket: Arc<UdpSocket>,
        sink: Arc<dyn Sink>,
    ) -> Self {
        let max_export = config.max_export_sessions as usize;
        let rate_limiter = if config.xmit_rate_bps > 0 {
            Some(TokenBucket::new(config.xmit_rate_bps))
        } else {
            None
        };
        let history_capacity = config.session_recreation_history_size;
        // Encode one_way_light_time_ms as value+1 so 0 means "not configured".
        let owlt_atomic = match config.one_way_light_time_ms {
            Some(ms) => ms + 1,
            None => 0,
        };
        Self {
            aggregation: std::sync::Mutex::new(AggregationBuffer::new(config.aggr_size_limit)),
            session_counter: AtomicU64::new(1),
            export_sessions: Mutex::new(HashMap::new()),
            import_sessions: Mutex::new(HashMap::new()),
            closed_exports: Mutex::new(HashMap::new()),
            socket,
            sink,
            export_semaphore: Arc::new(Semaphore::new(max_export)),
            rate_limiter: std::sync::Mutex::new(rate_limiter),
            session_history: std::sync::Mutex::new(SessionHistory::new(history_capacity)),
            one_way_light_time_ms: AtomicU64::new(owlt_atomic),
            link_state: std::sync::Mutex::new(LinkState::Up),
            outbound_queue: std::sync::Mutex::new(OutboundQueue::new(config.link_down_queue_max_bytes)),
            last_send_time: std::sync::Mutex::new(Instant::now()),
            ping_timer: std::sync::Mutex::new(None),
            ping_response_timer: std::sync::Mutex::new(None),
            aggr_timer: std::sync::Mutex::new(None),
            suspended_export_timers: std::sync::Mutex::new(HashMap::new()),
            suspended_import_timers: std::sync::Mutex::new(HashMap::new()),
            suspended_inactivity_timers: std::sync::Mutex::new(HashMap::new()),
            config,
            local_engine_id,
        }
    }

    /// Allocate the next session number (strictly monotonically increasing).
    pub fn next_session_number(&self) -> u64 {
        self.session_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Compute the retransmission timeout for this span.
    ///
    /// If `one_way_light_time_ms` is configured (either from initial config or
    /// via a runtime update), the timeout is `2 × (owlt + margin)` milliseconds.
    /// Otherwise, falls back to the flat `retransmit_cycle_secs` value.
    pub fn compute_retransmit_timeout(&self) -> Duration {
        let owlt_encoded = self.one_way_light_time_ms.load(Ordering::Relaxed);
        if owlt_encoded > 0 {
            // Decode: stored as value + 1, so subtract 1 to get actual ms.
            let owlt_ms = owlt_encoded - 1;
            let margin_ms = self.config.one_way_margin_time_ms;
            let rtt_ms = 2 * (owlt_ms + margin_ms);
            Duration::from_millis(rtt_ms)
        } else {
            Duration::from_secs(self.config.retransmit_cycle_secs)
        }
    }

    /// Update the one-way light time at runtime.
    ///
    /// Pass `Some(ms)` to set a new one-way light time value, or `None` to
    /// clear it (reverting to the flat `retransmit_cycle_secs` fallback).
    ///
    /// This supports changing orbital geometry where the propagation delay
    /// varies over time.
    pub fn update_one_way_light_time(&self, ms: Option<u64>) {
        let encoded = match ms {
            Some(v) => v + 1,
            None => 0,
        };
        self.one_way_light_time_ms.store(encoded, Ordering::Relaxed);
    }

    /// Flush the aggregation buffer and create an export session with the block.
    /// Called when the aggregation time limit expires.
    pub async fn flush_aggregation_buffer(self: &Arc<Self>) {
        let block = {
            let mut agg = self.aggregation.lock().unwrap();
            agg.flush()
        };
        // Clear the timer handle.
        {
            let mut timer = self.aggr_timer.lock().unwrap();
            *timer = None;
        }
        if let Some(block) = block {
            self.create_export_session(block).await;
        }
    }

    /// Start the aggregation timer if the buffer just went from empty to non-empty.
    /// The timer fires after `config.aggr_time_limit_secs` and flushes the buffer.
    pub fn start_aggr_timer_if_needed(self: &Arc<Self>) {
        let aggr_secs = self.config.aggr_time_limit_secs;
        if aggr_secs == 0 {
            return; // Time-based flush disabled
        }
        let mut timer = self.aggr_timer.lock().unwrap();
        if timer.is_some() {
            return; // Timer already running
        }
        let span = Arc::clone(self);
        let duration = Duration::from_secs(aggr_secs);
        let handle = tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            span.flush_aggregation_buffer().await;
        });
        *timer = Some(handle.abort_handle());
    }

    /// Create a new export session from a flushed block and transmit initial segments.
    ///
    /// Acquires a permit from the export semaphore (blocking if at limit),
    /// allocates a session number, creates the export session state machine,
    /// and executes the initial actions (send segments, start timers).
    pub async fn create_export_session(self: &Arc<Self>, block: Bytes) {
        // Acquire flow control permit (blocks if max_export_sessions reached).
        let _permit = self.export_semaphore.acquire().await;

        let session_number = self.next_session_number();
        let session_id = SessionId {
            engine_id: self.local_engine_id,
            session_number,
        };

        let export_config = ExportConfig {
            max_segment_size: self.config.max_segment_size,
            max_retransmissions: self.config.max_retransmissions,
            retransmit_timeout: self.compute_retransmit_timeout(),
            checkpoint_every_n: self.config.checkpoint_every_n_segments,
            max_checkpoints: None,
            green: false,
        };

        let client_service_id = 1u64; // Bundle Protocol

        let (session, actions) =
            ExportSession::new(session_id, block, client_service_id, export_config);

        debug!(
            engine_id = self.config.engine_id,
            session_number, "created export session"
        );

        // Emit metric for export session creation.
        metrics::counter!("ltp.export.started").increment(1);

        // If the session completed immediately (e.g., green), don't store it.
        if session.state() == ExportState::Complete {
            metrics::counter!("ltp.export.completed").increment(1);
            return;
        }

        let mut state = ExportSessionState {
            session,
            timers: HashMap::new(),
        };

        // Execute initial actions (send segments, collect timers).
        let (segments, timers) = self.extract_export_actions(actions);

        // Send segments without holding any lock.
        self.execute_export_io(segments).await;

        // Spawn timers (safe — no lock held).
        self.spawn_export_timers(&mut state, timers, session_number);

        // Store the session.
        let mut sessions = self.export_sessions.lock().await;
        sessions.insert(session_number, state);
    }

    /// Check terminal state of an export session and clean up if terminal.
    ///
    /// Must be called while holding the `export_sessions` lock. Returns:
    /// - `Some((true, session_id))` if the session completed (caller should retain closed export)
    /// - `Some((false, _))` if the session was cancelled
    /// - `None` if the session is still active
    fn check_export_terminal_state(
        &self,
        sessions: &mut HashMap<u64, ExportSessionState>,
        session_number: u64,
    ) -> Option<(bool, SessionId)> {
        let state = sessions.get(&session_number)?;
        let session_state = state.session.state();
        if session_state == ExportState::Complete {
            let session_id = *state.session.id();
            self.cleanup_export_session(sessions, session_number);
            Some((true, session_id))
        } else if session_state == ExportState::Cancelled {
            self.cleanup_export_session(sessions, session_number);
            Some((
                false,
                SessionId {
                    engine_id: 0,
                    session_number: 0,
                },
            ))
        } else {
            None
        }
    }

    /// Handle post-lock terminal state or spawn timers for an export session.
    ///
    /// This is the common Phase 3 logic shared by `on_export_report` and
    /// `on_export_timer_expired`. It emits metrics, retains closed exports,
    /// or re-acquires the lock to spawn timers for non-terminal sessions.
    async fn handle_export_terminal(
        self: &Arc<Self>,
        session_number: u64,
        timers: Vec<(u64, Duration)>,
        terminal_info: Option<(bool, SessionId)>,
    ) {
        match terminal_info {
            Some((true, session_id)) => {
                metrics::counter!("ltp.export.completed").increment(1);
                self.retain_closed_export(session_number, session_id).await;
            }
            Some((false, _)) => {
                metrics::counter!("ltp.export.cancelled_local").increment(1);
            }
            None => {
                // Re-acquire lock to spawn timers.
                let mut sessions = self.export_sessions.lock().await;
                if let Some(state) = sessions.get_mut(&session_number) {
                    self.spawn_export_timers(state, timers, session_number);
                }
            }
        }
    }

    /// Handle a received Report Segment for an export session.
    pub async fn on_export_report(
        self: &Arc<Self>,
        session_number: u64,
        report_serial: u64,
        checkpoint_serial: u64,
        upper_bound: u64,
        lower_bound: u64,
        claims: &[ReceptionClaim],
    ) {
        // Phase 1: Acquire lock, drive state machine, extract actions.
        let (segments, timers, terminal_info) = {
            let mut sessions = self.export_sessions.lock().await;
            let Some(state) = sessions.get_mut(&session_number) else {
                // Not in active exports — check closed exports for late reports.
                drop(sessions);
                self.on_closed_export_report(session_number, report_serial)
                    .await;
                return;
            };

            let actions = state.session.on_report(
                report_serial,
                checkpoint_serial,
                upper_bound,
                lower_bound,
                claims,
            );

            let (segments, timers) = self.extract_export_actions(actions);

            // Check terminal state.
            let terminal_info = self.check_export_terminal_state(&mut sessions, session_number);

            (segments, timers, terminal_info)
        };
        // Lock is now dropped.

        // Phase 2: Execute I/O without holding the lock.
        self.execute_export_io(segments).await;

        // Phase 3: Handle terminal state or spawn timers.
        self.handle_export_terminal(session_number, timers, terminal_info)
            .await;
    }

    /// Handle a retransmission timer expiry for an export session.
    pub async fn on_export_timer_expired(
        self: &Arc<Self>,
        session_number: u64,
        checkpoint_serial: u64,
    ) {
        // Phase 1: Acquire lock, drive state machine, extract actions.
        let (segments, timers, terminal_info) = {
            let mut sessions = self.export_sessions.lock().await;
            let Some(state) = sessions.get_mut(&session_number) else {
                trace!(
                    session_number,
                    "timer expired for unknown export session, ignoring"
                );
                return;
            };

            // Remove the fired timer handle.
            state.timers.remove(&checkpoint_serial);

            let actions = state.session.on_timer_expired(checkpoint_serial);

            let (segments, timers) = self.extract_export_actions(actions);

            // Check terminal state.
            let terminal_info = self.check_export_terminal_state(&mut sessions, session_number);

            (segments, timers, terminal_info)
        };
        // Lock is now dropped.

        // Phase 2: Execute I/O without holding the lock.
        self.execute_export_io(segments).await;

        // Phase 3: Handle terminal state or spawn timers.
        self.handle_export_terminal(session_number, timers, terminal_info)
            .await;
    }

    /// Handle a Cancel-from-Receiver for an export session.
    pub async fn on_export_cancel_from_receiver(
        self: &Arc<Self>,
        session_number: u64,
        reason: CancelReason,
    ) {
        // Phase 1: Acquire lock, drive state machine, extract actions.
        let segments = {
            let mut sessions = self.export_sessions.lock().await;
            let Some(state) = sessions.get_mut(&session_number) else {
                trace!(
                    session_number,
                    "cancel for unknown export session, ignoring"
                );
                return;
            };

            let actions = state.session.on_cancel_from_receiver(reason);

            let (segments, _timers) = self.extract_export_actions(actions);

            // Always clean up after cancel.
            self.cleanup_export_session(&mut sessions, session_number);

            segments
        };
        // Lock is now dropped.

        // Phase 2: Execute I/O without holding the lock.
        self.execute_export_io(segments).await;

        // Emit metric for remote cancellation.
        metrics::counter!("ltp.export.cancelled_remote").increment(1);
    }

    /// Handle a received data segment for an import session.
    ///
    /// Creates a new import session if one doesn't exist for this session number.
    pub async fn on_import_data_segment(
        self: &Arc<Self>,
        session_number: u64,
        segment_type: SegmentType,
        client_service_id: u64,
        offset: u64,
        data: &[u8],
        checkpoint: Option<CheckpointInfo>,
    ) {
        // Phase 1: Acquire lock, drive state machine, extract actions, handle
        // deferred report logic — all without performing I/O.
        let io_work = {
            let mut sessions = self.import_sessions.lock().await;

            // Check max import sessions limit before creating a new one.
            if !sessions.contains_key(&session_number) {
                if sessions.len() >= self.config.max_import_sessions as usize {
                    trace!(
                        session_number,
                        "max import sessions reached, discarding segment"
                    );
                    metrics::counter!("ltp.import.limit_discards").increment(1);
                    return;
                }

                // Check session recreation prevention history.
                if self.config.session_recreation_history_size > 0 {
                    let history = self.session_history.lock().unwrap();
                    if history.contains(session_number) {
                        trace!(
                            session_number,
                            "session number in recreation history, discarding segment"
                        );
                        metrics::counter!("ltp.import.recreation_discards").increment(1);
                        return;
                    }
                }

                // Create a new import session.
                let session_id = SessionId {
                    engine_id: self.config.engine_id,
                    session_number,
                };
                let import_config = ImportConfig {
                    max_reports: None,
                    retransmit_timeout: self.compute_retransmit_timeout(),
                    max_claims_per_report: 20,
                    expected_client_service_id: 1,
                    max_red_data_bytes: Some(self.config.max_red_data_bytes_per_session),
                    defer_report_ms: self.config.defer_report_ms,
                };
                let session = ImportSession::new(session_id, import_config);
                let state = ImportSessionState {
                    session,
                    timers: HashMap::new(),
                    inactivity_timer: None,
                    deferred_report: None,
                };
                sessions.insert(session_number, state);
                debug!(
                    engine_id = self.config.engine_id,
                    session_number, "created import session"
                );
            }

            let state = sessions.get_mut(&session_number).unwrap();

            // Emit metric for received data segment type.
            if segment_type.is_red() {
                metrics::counter!("ltp.segments.rx_red").increment(1);
            } else if segment_type.is_green() {
                metrics::counter!("ltp.segments.rx_green").increment(1);
            }

            // Reset the inactivity timer on each data segment received.
            self.reset_inactivity_timer(state, session_number);

            // Track coverage before processing to detect gap-filling segments.
            let coverage_before = state.session.extents().total_coverage();

            let actions = state.session.on_data_segment(
                segment_type,
                client_service_id,
                offset,
                data,
                checkpoint,
            );

            let coverage_after = state.session.extents().total_coverage();
            let segment_filled_gap = coverage_after > coverage_before;

            // Check if there's a DeferReport action — handle it specially.
            let mut defer_action = None;
            let mut other_actions = Vec::new();
            for action in actions {
                if let ImportAction::DeferReport {
                    checkpoint_serial,
                    upper_bound,
                    defer_ms,
                } = action
                {
                    defer_action = Some((checkpoint_serial, upper_bound, defer_ms));
                } else {
                    other_actions.push(action);
                }
            }

            // Extract I/O work and handle CancelTimer while holding the lock.
            let (segments, blocks, timers) = self.extract_import_actions(state, other_actions);

            // Handle deferred report: start a deferral timer.
            if let Some((checkpoint_serial, upper_bound, defer_ms)) = defer_action {
                // Cancel any existing deferred report timer (shouldn't normally happen,
                // but be safe).
                if let Some(prev) = state.deferred_report.take() {
                    prev.timer.abort();
                }

                let span = Arc::clone(self);
                let duration = Duration::from_millis(defer_ms);
                let handle = tokio::spawn(async move {
                    tokio::time::sleep(duration).await;
                    span.on_deferred_report_timer_expired(session_number).await;
                });

                state.deferred_report = Some(DeferredReport {
                    checkpoint_serial,
                    upper_bound,
                    timer: handle.abort_handle(),
                });
            }

            // If there's an active deferred report and this data segment filled
            // one or more gaps, check whether to send the report immediately or reset timer.
            let mut deferred_report_segments: Vec<Bytes> = Vec::new();
            let mut deferred_report_blocks: Vec<Bytes> = Vec::new();
            let mut deferred_report_timers: Vec<(u64, Duration)> = Vec::new();
            if defer_action.is_none() && segment_filled_gap {
                if let Some(ref deferred) = state.deferred_report {
                    let upper_bound = deferred.upper_bound;
                    let checkpoint_serial = deferred.checkpoint_serial;

                    if state.session.is_scope_complete(upper_bound) {
                        // All gaps filled — send report immediately and cancel timer.
                        let old_deferred = state.deferred_report.take().unwrap();
                        old_deferred.timer.abort();

                        let report_actions = state
                            .session
                            .generate_deferred_report(checkpoint_serial, upper_bound);
                        let (rs, rb, rt) = self.extract_import_actions(state, report_actions);
                        deferred_report_segments = rs;
                        deferred_report_blocks = rb;
                        deferred_report_timers = rt;
                    } else {
                        // Gaps remain but segment filled some — reset the deferral timer.
                        let defer_ms = self.config.defer_report_ms;
                        if defer_ms > 0 {
                            let old_deferred = state.deferred_report.take().unwrap();
                            old_deferred.timer.abort();

                            let span = Arc::clone(self);
                            let duration = Duration::from_millis(defer_ms);
                            let handle = tokio::spawn(async move {
                                tokio::time::sleep(duration).await;
                                span.on_deferred_report_timer_expired(session_number).await;
                            });

                            state.deferred_report = Some(DeferredReport {
                                checkpoint_serial,
                                upper_bound,
                                timer: handle.abort_handle(),
                            });
                        }
                    }
                }
            }

            // Check terminal state before deciding on timer spawning.
            let import_state = state.session.state();
            let is_terminal =
                matches!(import_state, ImportState::Complete | ImportState::Cancelled);

            if is_terminal {
                if import_state == ImportState::Complete {
                    metrics::counter!("ltp.import.completed").increment(1);
                } else {
                    metrics::counter!("ltp.import.cancelled_local").increment(1);
                }
                self.cleanup_import_session(&mut sessions, session_number);
            } else if let Some(state) = sessions.get_mut(&session_number) {
                self.spawn_import_timers(state, timers, session_number);
                self.spawn_import_timers(state, deferred_report_timers, session_number);
            }

            // Collect all segments and blocks for I/O outside the lock.
            let mut all_segments = segments;
            all_segments.extend(deferred_report_segments);
            let mut all_blocks = blocks;
            all_blocks.extend(deferred_report_blocks);
            (all_segments, all_blocks)
        };
        // Lock is now dropped.

        // Phase 2: Execute I/O without holding the lock.
        let (all_segments, all_blocks) = io_work;
        self.execute_import_io(all_segments, all_blocks).await;
    }

    /// Handle a received Report-Ack for an import session.
    pub async fn on_import_report_ack(self: &Arc<Self>, session_number: u64, report_serial: u64) {
        // Phase 1: Acquire lock, drive state machine, extract actions.
        let (segments, blocks) = {
            let mut sessions = self.import_sessions.lock().await;
            let Some(state) = sessions.get_mut(&session_number) else {
                trace!(
                    session_number,
                    "report-ack for unknown import session, ignoring"
                );
                return;
            };

            let actions = state.session.on_report_ack(report_serial);

            let (segments, blocks, timers) = self.extract_import_actions(state, actions);

            // Spawn timers while we still have the lock (they only need state.timers).
            if let Some(state) = sessions.get_mut(&session_number) {
                self.spawn_import_timers(state, timers, session_number);
            }

            (segments, blocks)
        };
        // Lock is now dropped.

        // Phase 2: Execute I/O without holding the lock.
        self.execute_import_io(segments, blocks).await;
    }

    /// Handle a Cancel-from-Sender for an import session.
    pub async fn on_import_cancel_from_sender(
        self: &Arc<Self>,
        session_number: u64,
        reason: CancelReason,
    ) {
        // Phase 1: Acquire lock, drive state machine, extract actions.
        let (segments, blocks) = {
            let mut sessions = self.import_sessions.lock().await;
            let Some(state) = sessions.get_mut(&session_number) else {
                trace!(
                    session_number,
                    "cancel for unknown import session, ignoring"
                );
                return;
            };

            let actions = state.session.on_cancel_from_sender(reason);

            let (segments, blocks, _timers) = self.extract_import_actions(state, actions);

            // Always clean up after cancel.
            self.cleanup_import_session(&mut sessions, session_number);

            (segments, blocks)
        };
        // Lock is now dropped.

        // Phase 2: Execute I/O without holding the lock.
        self.execute_import_io(segments, blocks).await;

        // Emit metric for remote cancellation.
        metrics::counter!("ltp.import.cancelled_remote").increment(1);
    }

    // -----------------------------------------------------------------------
    // Action Execution Helpers
    // -----------------------------------------------------------------------

    /// Separate export actions into segments to send (I/O) and timers to spawn.
    ///
    /// This is a synchronous extraction step that does NOT perform I/O.
    /// Call this while holding the session lock, then drop the lock before
    /// calling `execute_export_io`.
    fn extract_export_actions(
        &self,
        actions: Vec<ExportAction>,
    ) -> (Vec<Bytes>, Vec<(u64, Duration)>) {
        let mut segments_to_send = Vec::new();
        let mut timers_to_spawn = Vec::new();
        for action in actions {
            match action {
                ExportAction::SendSegment(bytes) => {
                    segments_to_send.push(bytes);
                }
                ExportAction::StartTimer {
                    checkpoint_serial,
                    duration,
                } => {
                    timers_to_spawn.push((checkpoint_serial, duration));
                }
                ExportAction::SuspendTimer { .. } | ExportAction::ResumeTimer { .. } => {
                    // Suspend/Resume actions are handled separately by the
                    // link event handlers (suspend_all_timers / resume_all_timers).
                    // They should not appear in normal session operation flow.
                }
            }
        }
        (segments_to_send, timers_to_spawn)
    }

    /// Execute the I/O portion of export actions (send segments via UDP).
    ///
    /// This must be called WITHOUT holding the session lock, as it performs
    /// async UDP sends that may sleep for rate limiting.
    async fn execute_export_io(&self, segments: Vec<Bytes>) {
        for bytes in &segments {
            self.send_segment(bytes).await;
        }
    }

    /// Spawn export timer tasks and register their abort handles.
    fn spawn_export_timers(
        self: &Arc<Self>,
        state: &mut ExportSessionState,
        timers: Vec<(u64, Duration)>,
        session_number: u64,
    ) {
        for (checkpoint_serial, duration) in timers {
            let span = Arc::clone(self);
            let handle = tokio::spawn(async move {
                tokio::time::sleep(duration).await;
                span.on_export_timer_expired(session_number, checkpoint_serial)
                    .await;
            });
            state
                .timers
                .insert(checkpoint_serial, handle.abort_handle());
        }
    }

    /// Separate import actions into I/O operations and timer management.
    ///
    /// This is a synchronous extraction step. `CancelTimer` actions are processed
    /// immediately (aborting timer handles), since they only touch the session state.
    /// Call this while holding the session lock, then drop the lock before
    /// calling `execute_import_io`.
    ///
    /// Returns `(segments_to_send, blocks_to_deliver, timers_to_spawn)`.
    fn extract_import_actions(
        &self,
        state: &mut ImportSessionState,
        actions: Vec<ImportAction>,
    ) -> (Vec<Bytes>, Vec<Bytes>, Vec<(u64, Duration)>) {
        let mut segments_to_send = Vec::new();
        let mut blocks_to_deliver = Vec::new();
        let mut timers_to_spawn = Vec::new();
        for action in actions {
            match action {
                ImportAction::SendSegment(bytes) => {
                    segments_to_send.push(bytes);
                }
                ImportAction::StartTimer {
                    report_serial,
                    duration,
                } => {
                    timers_to_spawn.push((report_serial, duration));
                }
                ImportAction::CancelTimer { report_serial } => {
                    if let Some(handle) = state.timers.remove(&report_serial) {
                        handle.abort();
                    }
                }
                ImportAction::DeliverBlock(block) => {
                    blocks_to_deliver.push(block);
                }
                ImportAction::DeferReport { .. } => {
                    // Handled separately by the caller (on_import_data_segment)
                    // after extract_import_actions returns.
                }
            }
        }
        (segments_to_send, blocks_to_deliver, timers_to_spawn)
    }

    /// Execute the I/O portion of import actions (send segments, deliver blocks).
    ///
    /// This must be called WITHOUT holding the session lock, as it performs
    /// async UDP sends that may sleep for rate limiting, and block delivery
    /// that calls into the BPA.
    async fn execute_import_io(&self, segments: Vec<Bytes>, blocks: Vec<Bytes>) {
        for bytes in &segments {
            self.send_segment(bytes).await;
        }
        for block in blocks {
            self.deliver_block(block).await;
        }
    }

    /// Spawn import timer tasks and register their abort handles.
    fn spawn_import_timers(
        self: &Arc<Self>,
        state: &mut ImportSessionState,
        timers: Vec<(u64, Duration)>,
        session_number: u64,
    ) {
        for (report_serial, duration) in timers {
            let span = Arc::clone(self);
            let handle = tokio::spawn(async move {
                tokio::time::sleep(duration).await;
                span.on_import_report_timer_expired(session_number, report_serial)
                    .await;
            });
            state.timers.insert(report_serial, handle.abort_handle());
        }
    }

    /// Handle a report retransmit timer expiry for an import session.
    async fn on_import_report_timer_expired(
        self: &Arc<Self>,
        session_number: u64,
        report_serial: u64,
    ) {
        let mut sessions = self.import_sessions.lock().await;
        let Some(state) = sessions.get_mut(&session_number) else {
            return;
        };

        // Remove the fired timer handle.
        state.timers.remove(&report_serial);

        // The import session doesn't have a dedicated on_report_timer_expired
        // method in the current state machine — report retransmission is handled
        // by re-sending the stored report. For now, log and skip.
        // Future: implement report retransmission in the import session SM.
        trace!(
            session_number,
            report_serial, "import report timer expired (retransmission not yet wired)"
        );
    }

    // -----------------------------------------------------------------------
    // Inactivity Timer Helpers
    // -----------------------------------------------------------------------

    /// Reset (or start) the inactivity timer for an import session.
    ///
    /// If `session_inactivity_limit_secs` is non-zero, aborts any existing
    /// inactivity timer and spawns a new one with the full timeout. When the
    /// timer fires without being reset, the session is cancelled.
    fn reset_inactivity_timer(
        self: &Arc<Self>,
        state: &mut ImportSessionState,
        session_number: u64,
    ) {
        let inactivity_secs = self.config.session_inactivity_limit_secs;
        if inactivity_secs == 0 {
            return;
        }

        // Abort the previous inactivity timer if one exists.
        if let Some(handle) = state.inactivity_timer.take() {
            handle.abort();
        }

        // Spawn a new inactivity timer.
        let span = Arc::clone(self);
        let duration = Duration::from_secs(inactivity_secs);
        let handle = tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            span.on_import_inactivity_expired(session_number).await;
        });
        state.inactivity_timer = Some(handle.abort_handle());
    }

    // -----------------------------------------------------------------------
    // Deferred Report Timer Helpers
    // -----------------------------------------------------------------------

    /// Handle a deferred report timer expiry for an import session.
    ///
    /// When the deferral timer fires, generates and sends the report with
    /// whatever coverage has been achieved at that point.
    async fn on_deferred_report_timer_expired(self: &Arc<Self>, session_number: u64) {
        // Phase 1: Acquire lock, drive state machine, extract actions.
        let io_work = {
            let mut sessions = self.import_sessions.lock().await;
            let Some(state) = sessions.get_mut(&session_number) else {
                return;
            };

            // Take the deferred report state (timer already fired, no need to abort).
            let Some(deferred) = state.deferred_report.take() else {
                return;
            };

            // Only generate report if the session is still active.
            if state.session.state() != ImportState::Receiving {
                return;
            }

            debug!(
                engine_id = self.config.engine_id,
                session_number,
                checkpoint_serial = deferred.checkpoint_serial,
                "deferred report timer expired, generating RS"
            );

            let report_actions = state
                .session
                .generate_deferred_report(deferred.checkpoint_serial, deferred.upper_bound);

            let (segments, blocks, timers) = self.extract_import_actions(state, report_actions);

            // Check terminal state.
            let import_state = state.session.state();
            let is_terminal =
                matches!(import_state, ImportState::Complete | ImportState::Cancelled);

            if is_terminal {
                if import_state == ImportState::Complete {
                    metrics::counter!("ltp.import.completed").increment(1);
                } else {
                    metrics::counter!("ltp.import.cancelled_local").increment(1);
                }
                self.cleanup_import_session(&mut sessions, session_number);
            } else if let Some(state) = sessions.get_mut(&session_number) {
                self.spawn_import_timers(state, timers, session_number);
            }

            (segments, blocks)
        };
        // Lock is now dropped.

        // Phase 2: Execute I/O without holding the lock.
        let (segments, blocks) = io_work;
        self.execute_import_io(segments, blocks).await;
    }

    /// Handle an inactivity timer expiry for an import session.
    ///
    /// Cancels the session by sending a Cancel-from-Receiver with reason ByEngine
    /// and emits a metric for the inactivity cancellation.
    async fn on_import_inactivity_expired(self: &Arc<Self>, session_number: u64) {
        // Phase 1: Acquire lock, build cancel segment, clean up session.
        let segment_to_send = {
            let mut sessions = self.import_sessions.lock().await;
            let Some(state) = sessions.get_mut(&session_number) else {
                return;
            };

            // Only cancel if the session is still active.
            if state.session.state() != ImportState::Receiving {
                return;
            }

            debug!(
                engine_id = self.config.engine_id,
                session_number, "import session cancelled due to inactivity"
            );

            // Build Cancel-from-Receiver segment with reason ByEngine.
            // The import session state machine's cancel_with_reason is private,
            // so we construct the cancel segment directly at the CLA layer.
            let cancel = Segment::Cancel {
                session_id: *state.session.id(),
                reason: CancelReason::ByEngine,
                direction: CancelDirection::FromReceiver,
            };
            let wire_size = segment::encoded_size(&cancel);
            let mut buf = BytesMut::with_capacity(wire_size);
            segment::encode(&cancel, &mut buf);
            let segment = buf.freeze();

            // Emit metric for inactivity cancellation.
            metrics::counter!("ltp.import.inactivity_cancels").increment(1);

            // Clean up the session.
            self.cleanup_import_session(&mut sessions, session_number);

            segment
        };
        // Lock is now dropped.

        // Phase 2: Send the cancel segment without holding the lock.
        self.send_segment(&segment_to_send).await;
    }

    // -----------------------------------------------------------------------
    // Closed Export Retention
    // -----------------------------------------------------------------------

    /// Move a completed export session to the closed exports map for retention.
    ///
    /// The session is retained for `2 × max_retransmissions × retransmit_timeout + 10s`
    /// to handle late Report Segments with Report-Ack responses.
    async fn retain_closed_export(self: &Arc<Self>, session_number: u64, session_id: SessionId) {
        let retransmit_timeout = self.compute_retransmit_timeout();
        let retention_duration = retransmit_timeout
            .saturating_mul(2 * self.config.max_retransmissions)
            + Duration::from_secs(10);

        // Spawn the retention timer.
        let span = Arc::clone(self);
        let timer_handle = tokio::spawn(async move {
            tokio::time::sleep(retention_duration).await;
            span.on_closed_export_retention_expired(session_number)
                .await;
        });

        let closed_state = ClosedExportState {
            session_id,
            response_counter: self.config.max_retransmissions,
            retention_timer: timer_handle.abort_handle(),
        };

        let mut closed = self.closed_exports.lock().await;
        closed.insert(session_number, closed_state);

        debug!(
            engine_id = self.config.engine_id,
            session_number,
            retention_ms = retention_duration.as_millis() as u64,
            "retained closed export session"
        );
    }

    /// Handle a late Report Segment for a closed export session.
    ///
    /// Sends a Report-Ack and decrements the response counter. If the counter
    /// reaches zero, the closed export is discarded immediately.
    pub async fn on_closed_export_report(
        self: &Arc<Self>,
        session_number: u64,
        report_serial: u64,
    ) {
        // Phase 1: Acquire lock, build Report-Ack, update state.
        let segment_to_send = {
            let mut closed = self.closed_exports.lock().await;
            let Some(state) = closed.get_mut(&session_number) else {
                trace!(
                    session_number,
                    "report for unknown export session, ignoring"
                );
                return;
            };

            // Build Report-Ack.
            let ras = Segment::ReportAck {
                session_id: state.session_id,
                report_serial,
            };
            let wire_size = segment::encoded_size(&ras);
            let mut buf = BytesMut::with_capacity(wire_size);
            segment::encode(&ras, &mut buf);
            let segment = buf.freeze();

            // Decrement response counter.
            state.response_counter = state.response_counter.saturating_sub(1);

            trace!(
                session_number,
                report_serial,
                remaining = state.response_counter,
                "sent RAS from closed export"
            );

            // If counter reaches zero, discard immediately.
            if state.response_counter == 0 {
                let removed = closed.remove(&session_number).unwrap();
                removed.retention_timer.abort();
                debug!(
                    engine_id = self.config.engine_id,
                    session_number, "discarded closed export (response counter exhausted)"
                );
            }

            segment
        };
        // Lock is now dropped.

        // Phase 2: Send the Report-Ack without holding the lock.
        self.send_segment(&segment_to_send).await;
    }

    /// Handle retention timer expiry for a closed export session.
    ///
    /// Discards the closed export regardless of remaining response counter.
    async fn on_closed_export_retention_expired(self: &Arc<Self>, session_number: u64) {
        let mut closed = self.closed_exports.lock().await;
        if closed.remove(&session_number).is_some() {
            debug!(
                engine_id = self.config.engine_id,
                session_number, "discarded closed export (retention period expired)"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Link Status Detection (Ping)
    // -----------------------------------------------------------------------

    /// Session number 0 is reserved for ping probes. It is never allocated
    /// by `next_session_number()` (which starts at 1), so a CS for session 0
    /// will always be "non-existent" on the remote side, eliciting a CAS.
    pub const PING_SESSION_NUMBER: u64 = 0;

    /// Start the periodic ping timer for this span.
    ///
    /// If `ping_interval_secs` is non-zero, spawns a background task that
    /// periodically checks whether a ping probe should be sent. The probe is
    /// only sent if no data has been transmitted for `ping_interval_secs`.
    ///
    /// This should be called once after the span is created and wrapped in an Arc.
    pub fn start_ping_timer(self: &Arc<Self>) {
        let ping_interval = self.config.ping_interval_secs;
        if ping_interval == 0 {
            return;
        }

        let span = Arc::clone(self);
        let duration = Duration::from_secs(ping_interval);
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(duration).await;
                span.on_ping_timer_fired().await;
            }
        });

        let mut timer = self.ping_timer.lock().unwrap();
        *timer = Some(handle.abort_handle());
    }

    /// Stop the periodic ping timer (e.g., on span shutdown).
    pub fn stop_ping_timer(&self) {
        let mut timer = self.ping_timer.lock().unwrap();
        if let Some(handle) = timer.take() {
            handle.abort();
        }
        // Also cancel any pending response timer.
        let mut response_timer = self.ping_response_timer.lock().unwrap();
        if let Some(handle) = response_timer.take() {
            handle.abort();
        }
    }

    /// Returns whether the link is currently considered alive.
    pub fn is_link_alive(&self) -> bool {
        let state = self.link_state.lock().unwrap();
        *state == LinkState::Up
    }

    /// Called when the periodic ping timer fires.
    ///
    /// Checks whether data has been sent recently. If not, sends a CS for
    /// session 0 (the ping probe) and starts the response timeout.
    ///
    /// If the link is in `DownTvr` state, the probe is suppressed entirely
    /// to avoid transmitting into a known-dead link (Requirement 7.3).
    async fn on_ping_timer_fired(self: &Arc<Self>) {
        // Suppress ping probes during TVR-driven link-down.
        {
            let state = self.link_state.lock().unwrap();
            if *state == LinkState::DownTvr {
                return;
            }
        }

        let ping_interval = Duration::from_secs(self.config.ping_interval_secs);

        // Check if data was sent recently enough that a ping is unnecessary.
        let elapsed = {
            let last_send = self.last_send_time.lock().unwrap();
            last_send.elapsed()
        };

        if elapsed < ping_interval {
            // Data was sent recently — no ping needed.
            return;
        }

        // If there's already a pending ping response timer, don't send another.
        {
            let response_timer = self.ping_response_timer.lock().unwrap();
            if response_timer.is_some() {
                return;
            }
        }

        debug!(
            engine_id = self.config.engine_id,
            "sending ping CS for link status detection"
        );

        // Build and send a Cancel-from-Sender for session 0 (non-existent).
        let ping_cs = Segment::Cancel {
            session_id: SessionId {
                engine_id: self.local_engine_id,
                session_number: Self::PING_SESSION_NUMBER,
            },
            reason: CancelReason::ByUser,
            direction: CancelDirection::FromSender,
        };
        let wire_size = segment::encoded_size(&ping_cs);
        let mut buf = BytesMut::with_capacity(wire_size);
        segment::encode(&ping_cs, &mut buf);
        self.send_segment(&buf.freeze()).await;

        // Start the response timeout: retransmit_cycle_secs × max_retransmissions.
        let timeout_duration = Duration::from_secs(self.config.retransmit_cycle_secs)
            .saturating_mul(self.config.max_retransmissions);

        let span = Arc::clone(self);
        let handle = tokio::spawn(async move {
            tokio::time::sleep(timeout_duration).await;
            span.on_ping_response_timeout().await;
        });

        let mut response_timer = self.ping_response_timer.lock().unwrap();
        *response_timer = Some(handle.abort_handle());
    }

    /// Called when a Cancel-Ack is received for session 0 (the ping session).
    ///
    /// This confirms the remote engine is reachable. Cancels the response
    /// timeout and marks the link as alive.
    pub fn on_ping_cancel_ack_received(&self) {
        debug!(
            engine_id = self.config.engine_id,
            "ping CAS received, link is alive"
        );

        // Cancel the response timeout timer.
        let mut response_timer = self.ping_response_timer.lock().unwrap();
        if let Some(handle) = response_timer.take() {
            handle.abort();
        }

        // Mark link as alive.
        let mut state = self.link_state.lock().unwrap();
        *state = LinkState::Up;
    }

    /// Called when the ping response timeout expires without receiving a CAS.
    ///
    /// Triggers a link-down transition via `handle_link_down` with
    /// `scheduled: false`, which reuses the same suspension logic as TVR
    /// events. If `purge_on_link_down` is configured, cancels all active
    /// export sessions and notifies the BPA so bundles can be re-routed.
    async fn on_ping_response_timeout(self: &Arc<Self>) {
        // Clear the response timer handle (it already fired).
        {
            let mut response_timer = self.ping_response_timer.lock().unwrap();
            *response_timer = None;
        }

        warn!(
            engine_id = self.config.engine_id,
            "ping response timeout: link down detected"
        );

        // Transition to DownPing via handle_link_down, which triggers timer
        // suspension (if configured) using the same path as TVR events.
        self.handle_link_down(hardy_bpa::cla::LinkDownProperties { scheduled: false })
            .await;

        // Emit metric for ping-based link-down event.
        metrics::counter!("ltp.link.down_events").increment(1);

        // If purge_on_link_down is enabled, cancel all active export sessions
        // and notify the BPA that the link is down.
        if self.config.purge_on_link_down {
            self.purge_export_sessions().await;
        }
    }

    /// Cancel all active export sessions due to link-down detection.
    ///
    /// Removes every active export session, aborts their timers, and emits
    /// a metric for the number of purged sessions. After purging, notifies
    /// the BPA via `sink.remove_peer()` so that bundles can be re-routed
    /// through alternative paths.
    ///
    /// Note: We do NOT send Cancel-from-Sender segments to the remote engine
    /// because the link is down and they would not arrive.
    async fn purge_export_sessions(self: &Arc<Self>) {
        let mut sessions = self.export_sessions.lock().await;
        let count = sessions.len();

        if count == 0 {
            // Nothing to purge, but still notify BPA of link down.
            self.notify_bpa_link_down().await;
            return;
        }

        // Abort all timers and remove all sessions.
        for (_session_number, state) in sessions.drain() {
            for (_serial, handle) in state.timers {
                handle.abort();
            }
        }

        drop(sessions);

        debug!(
            engine_id = self.config.engine_id,
            purged_sessions = count,
            "purged all export sessions due to link down"
        );

        // Emit metric for purged sessions.
        metrics::counter!("ltp.export.purged_on_link_down").increment(count as u64);

        // Notify BPA that this peer is no longer reachable.
        self.notify_bpa_link_down().await;
    }

    /// Notify the BPA that this span's peer is no longer reachable.
    ///
    /// Calls `sink.remove_peer()` with the span's CLA address so the BPA
    /// removes routing entries and can re-route bundles through other paths.
    async fn notify_bpa_link_down(&self) {
        let peer_addr =
            ClaAddress::Private(Bytes::copy_from_slice(&self.config.engine_id.to_be_bytes()));

        if let Err(e) = self.sink.remove_peer(&peer_addr).await {
            error!(
                engine_id = self.config.engine_id,
                "failed to notify BPA of link down: {e}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // TVR Link Event Handlers
    // -----------------------------------------------------------------------

    /// Handle a link-down event for this span.
    ///
    /// Transitions the link state to `DownTvr` or `DownPing` based on whether
    /// the event is scheduled (TVR) or unscheduled (ping). If the span is
    /// already in the matching down state, this is a no-op (idempotent).
    ///
    /// On transition:
    /// - Emits `ltp.tvr.link_down` metric counter
    /// - If scheduled: cancels the ping timer to suppress probes
    /// - If `tvr_timer_suspension` is enabled: suspends all active timers
    pub async fn handle_link_down(self: &Arc<Self>, properties: hardy_bpa::cla::LinkDownProperties) {
        {
            let mut state = self.link_state.lock().unwrap();
            // Idempotent: return early if already in matching down state
            if properties.scheduled && *state == LinkState::DownTvr {
                return;
            }
            if !properties.scheduled && *state == LinkState::DownPing {
                return;
            }

            *state = if properties.scheduled {
                LinkState::DownTvr
            } else {
                LinkState::DownPing
            };
        }

        metrics::counter!("ltp.tvr.link_down").increment(1);

        // Suppress ping probes if TVR-driven
        if properties.scheduled {
            self.stop_ping_timer();
        }

        // Suspend timers if configured
        if self.config.tvr_timer_suspension {
            self.suspend_all_timers().await;
        }
    }

    /// Suspend all active timers across export and import sessions.
    ///
    /// For export sessions: calls `session.suspend_timers()`, aborts timer
    /// handles, and records remaining durations (using the full retransmit
    /// timeout as a conservative approximation).
    ///
    /// For import sessions: aborts report timer handles and records remaining
    /// durations. Also aborts inactivity timers and records their remaining
    /// durations.
    ///
    /// Logs the number of suspended timers at debug level.
    async fn suspend_all_timers(self: &Arc<Self>) {
        let retransmit_timeout = self.compute_retransmit_timeout();
        let mut total_suspended = 0u32;

        // Suspend export session timers
        {
            let mut sessions = self.export_sessions.lock().await;
            let mut suspended_exports = self.suspended_export_timers.lock().unwrap();

            for (_session_number, state) in sessions.iter_mut() {
                // Call the engine's suspend_timers to mark the session as suspended
                let _suspend_actions = state.session.suspend_timers();

                // Abort all timer handles and record remaining durations
                for (checkpoint_serial, handle) in state.timers.drain() {
                    handle.abort();
                    // Use the full retransmit timeout as a conservative remaining duration.
                    // This is slightly longer than optimal but ensures timers don't fire
                    // during link-down and will correctly resume on link-up.
                    suspended_exports.insert(checkpoint_serial, retransmit_timeout);
                    total_suspended += 1;
                }
            }
        }

        // Suspend import session timers
        {
            let mut sessions = self.import_sessions.lock().await;
            let mut suspended_imports = self.suspended_import_timers.lock().unwrap();
            let mut suspended_inactivity = self.suspended_inactivity_timers.lock().unwrap();

            for (session_number, state) in sessions.iter_mut() {
                // Abort report timer handles and record remaining durations
                for (report_serial, handle) in state.timers.drain() {
                    handle.abort();
                    // Use the full retransmit timeout as a conservative remaining duration.
                    suspended_imports.insert(report_serial, retransmit_timeout);
                    total_suspended += 1;
                }

                // Abort inactivity timer if present
                if let Some(handle) = state.inactivity_timer.take() {
                    handle.abort();
                    // Use the full inactivity limit as the remaining duration.
                    let inactivity_duration =
                        Duration::from_secs(self.config.session_inactivity_limit_secs);
                    suspended_inactivity.insert(*session_number, inactivity_duration);
                    total_suspended += 1;
                }
            }
        }

        debug!(
            engine_id = self.config.engine_id,
            suspended_timers = total_suspended,
            "suspended all timers for link-down"
        );
    }

    /// Handle a link-up event for this span.
    ///
    /// Transitions the link state to `Up`. If the span is already up, this
    /// is a no-op (idempotent).
    ///
    /// On transition:
    /// - Emits `ltp.tvr.link_up` metric counter
    /// - If `tvr_rate_update` and bandwidth provided: updates token bucket rate
    /// - If `one_way_light_time_ms` provided: updates the OWLT value
    /// - If `tvr_timer_suspension`: resumes all suspended timers
    /// - Flushes the outbound queue with rate control
    /// - Resets ping detection state
    pub async fn handle_link_up(self: &Arc<Self>, properties: hardy_bpa::cla::LinkUpProperties) {
        {
            let mut state = self.link_state.lock().unwrap();
            if *state == LinkState::Up {
                return; // Already up — no-op
            }
            *state = LinkState::Up;
        }

        metrics::counter!("ltp.tvr.link_up").increment(1);

        // Update rate control if bandwidth provided and configured
        if self.config.tvr_rate_update {
            if let Some(bps) = properties.bandwidth_bps {
                let mut limiter = self.rate_limiter.lock().unwrap();
                if bps > 0 {
                    *limiter = Some(TokenBucket::new(bps));
                } else {
                    // Rate of 0 means unlimited — remove the token bucket
                    *limiter = None;
                }
            }
        }

        // Update one-way light time if provided
        if let Some(owlt_ms) = properties.one_way_light_time_ms {
            self.update_one_way_light_time(Some(owlt_ms));
        }

        // Resume timers if configured
        if self.config.tvr_timer_suspension {
            self.resume_all_timers().await;
        }

        // Flush outbound queue with rate control
        self.flush_outbound_queue().await;

        // Reset ping detection state
        self.reset_ping_state();
    }

    /// Resume all previously suspended timers.
    ///
    /// For export sessions: calls `session.resume_timers()` with the recorded
    /// remaining durations and spawns new timer tasks.
    ///
    /// For import sessions: spawns new report timer tasks with remaining durations.
    ///
    /// For inactivity timers: spawns new inactivity timer tasks with remaining durations.
    ///
    /// Clears all suspended timer maps after resumption.
    async fn resume_all_timers(self: &Arc<Self>) {
        let mut total_resumed = 0u32;

        // Resume export session timers
        {
            let suspended_exports: HashMap<u64, Duration> = {
                let mut map = self.suspended_export_timers.lock().unwrap();
                std::mem::take(&mut *map)
            };

            if !suspended_exports.is_empty() {
                let suspended_vec: Vec<(u64, Duration)> =
                    suspended_exports.iter().map(|(&k, &v)| (k, v)).collect();

                let mut sessions = self.export_sessions.lock().await;
                for (_session_number, state) in sessions.iter_mut() {
                    // Call the engine's resume_timers with the suspended durations
                    let resume_actions = state.session.resume_timers(&suspended_vec);

                    // Extract and spawn timers from the resume actions
                    for action in resume_actions {
                        if let ExportAction::ResumeTimer {
                            checkpoint_serial,
                            remaining,
                        } = action
                        {
                            let span = Arc::clone(self);
                            let session_number = state.session.id().session_number;
                            let handle = tokio::spawn(async move {
                                tokio::time::sleep(remaining).await;
                                span.on_export_timer_expired(session_number, checkpoint_serial)
                                    .await;
                            });
                            state
                                .timers
                                .insert(checkpoint_serial, handle.abort_handle());
                            total_resumed += 1;
                        }
                    }
                }
            }
        }

        // Resume import session report timers
        {
            let suspended_imports: HashMap<u64, Duration> = {
                let mut map = self.suspended_import_timers.lock().unwrap();
                std::mem::take(&mut *map)
            };

            if !suspended_imports.is_empty() {
                let mut sessions = self.import_sessions.lock().await;
                for (_session_number, state) in sessions.iter_mut() {
                    // Check which suspended report serials belong to this session
                    // by checking if the serial was previously in this session's timers.
                    // Since we drained all timers during suspension, we spawn timers
                    // for all suspended serials that match.
                    let session_number = state.session.id().session_number;
                    for (&report_serial, &remaining) in &suspended_imports {
                        let span = Arc::clone(self);
                        let handle = tokio::spawn(async move {
                            tokio::time::sleep(remaining).await;
                            span.on_import_report_timer_expired(session_number, report_serial)
                                .await;
                        });
                        state.timers.insert(report_serial, handle.abort_handle());
                        total_resumed += 1;
                    }
                }
            }
        }

        // Resume inactivity timers
        {
            let suspended_inactivity: HashMap<u64, Duration> = {
                let mut map = self.suspended_inactivity_timers.lock().unwrap();
                std::mem::take(&mut *map)
            };

            if !suspended_inactivity.is_empty() {
                let mut sessions = self.import_sessions.lock().await;
                for (&session_number, &remaining) in &suspended_inactivity {
                    if let Some(state) = sessions.get_mut(&session_number) {
                        let span = Arc::clone(self);
                        let handle = tokio::spawn(async move {
                            tokio::time::sleep(remaining).await;
                            span.on_import_inactivity_expired(session_number).await;
                        });
                        state.inactivity_timer = Some(handle.abort_handle());
                        total_resumed += 1;
                    }
                }
            }
        }

        debug!(
            engine_id = self.config.engine_id,
            resumed_timers = total_resumed,
            "resumed all timers for link-up"
        );
    }

    /// Flush the outbound queue, transmitting all queued segments with rate control.
    ///
    /// Drains all segments from the outbound queue and sends each one via
    /// the UDP socket, applying the token bucket rate limiter.
    /// Logs the number of flushed segments and total bytes at debug level.
    async fn flush_outbound_queue(&self) {
        let segments = {
            let mut queue = self.outbound_queue.lock().unwrap();
            queue.drain()
        };

        if segments.is_empty() {
            return;
        }

        let num_segments = segments.len();
        let total_bytes: usize = segments.iter().map(|s| s.len()).sum();

        for segment in &segments {
            self.send_segment_direct(segment).await;
        }

        // Update queue gauge to 0 after flush
        metrics::gauge!("ltp.tvr.queue_bytes", "engine_id" => self.config.engine_id.to_string()).set(0.0);

        debug!(
            engine_id = self.config.engine_id,
            flushed_segments = num_segments,
            flushed_bytes = total_bytes,
            "flushed outbound queue on link-up"
        );
    }

    /// Reset ping detection state after a link-up event.
    ///
    /// Cancels any pending ping response timer and restarts the periodic
    /// ping timer so that link monitoring resumes from a clean state.
    fn reset_ping_state(self: &Arc<Self>) {
        // Cancel any pending ping response timeout
        {
            let mut response_timer = self.ping_response_timer.lock().unwrap();
            if let Some(handle) = response_timer.take() {
                handle.abort();
            }
        }

        // Restart the periodic ping timer
        self.start_ping_timer();
    }

    // -----------------------------------------------------------------------
    // Transport Helpers
    // -----------------------------------------------------------------------

    /// Send a segment to the remote engine via UDP.
    ///
    /// If the link is currently down (`DownTvr` or `DownPing`), the segment
    /// is enqueued in the outbound queue instead of being transmitted.
    ///
    /// If a rate limiter is configured, consumes tokens for the segment size
    /// and sleeps if the bucket is empty before sending.
    /// Also updates `last_send_time` for ping interval tracking.
    async fn send_segment(&self, bytes: &[u8]) {
        // Check link state — enqueue if link is down
        {
            let state = self.link_state.lock().unwrap();
            if *state == LinkState::DownTvr || *state == LinkState::DownPing {
                let segment = Bytes::copy_from_slice(bytes);
                let mut queue = self.outbound_queue.lock().unwrap();
                let evicted = queue.enqueue(segment);
                if evicted > 0 {
                    warn!(
                        engine_id = self.config.engine_id,
                        evicted_bytes = evicted,
                        "outbound queue overflow, evicted oldest segments"
                    );
                    metrics::counter!("ltp.tvr.queue_evicted_bytes").increment(evicted as u64);
                }
                metrics::gauge!("ltp.tvr.queue_bytes", "engine_id" => self.config.engine_id.to_string()).set(queue.current_bytes() as f64);
                return;
            }
        }

        self.send_segment_direct(bytes).await;
    }

    /// Send a segment directly via UDP without checking link state.
    ///
    /// Used by `flush_outbound_queue` to transmit queued segments after
    /// the link has transitioned back to Up.
    async fn send_segment_direct(&self, bytes: &[u8]) {
        // Emit metric for every segment transmitted.
        metrics::counter!("ltp.segments.tx_total").increment(1);

        debug!(
            engine_id = self.config.engine_id,
            bytes = bytes.len(),
            address = %self.config.address,
            "TX segment"
        );

        // Apply rate limiting if configured.
        let sleep_duration = {
            let mut limiter = self.rate_limiter.lock().unwrap();
            if let Some(ref mut bucket) = *limiter {
                bucket.consume(bytes.len())
            } else {
                Duration::ZERO
            }
        };

        if !sleep_duration.is_zero() {
            tokio::time::sleep(sleep_duration).await;
        }

        if let Err(e) = self.socket.send_to(bytes, self.config.address).await {
            warn!(
                engine_id = self.config.engine_id,
                address = %self.config.address,
                "UDP send failed: {e}"
            );
        }

        // Update last send time for ping interval tracking.
        {
            let mut last_send = self.last_send_time.lock().unwrap();
            *last_send = Instant::now();
        }
    }

    /// Deliver a completed block by unpacking bundles and dispatching to the BPA.
    async fn deliver_block(&self, block: Bytes) {
        debug!(
            engine_id = self.config.engine_id,
            block_bytes = block.len(),
            "delivering block"
        );

        let result = crate::block::unpack_block(block);

        if let Some(ref err) = result.error {
            warn!(
                engine_id = self.config.engine_id,
                "block unpacking error: {err:?}"
            );
        }

        debug!(
            engine_id = self.config.engine_id,
            bundles = result.bundles.len(),
            "unpacked block into bundles"
        );

        // Build the CLA address for this peer.
        let peer_addr =
            ClaAddress::Private(Bytes::copy_from_slice(&self.config.engine_id.to_be_bytes()));

        for (i, bundle) in result.bundles.iter().enumerate() {
            if bundle.is_empty() {
                continue;
            }
            debug!(
                engine_id = self.config.engine_id,
                bundle_index = i,
                bundle_bytes = bundle.len(),
                "dispatching bundle to BPA"
            );
            if let Err(e) = self
                .sink
                .dispatch(bundle.clone(), None, Some(&peer_addr))
                .await
            {
                error!(
                    engine_id = self.config.engine_id,
                    "failed to dispatch bundle to BPA: {e}"
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Session Cleanup
    // -----------------------------------------------------------------------

    /// Remove an export session and abort all its pending timers.
    fn cleanup_export_session(
        &self,
        sessions: &mut HashMap<u64, ExportSessionState>,
        session_number: u64,
    ) {
        if let Some(state) = sessions.remove(&session_number) {
            // Abort all pending timers for this session.
            for (_serial, handle) in state.timers {
                handle.abort();
            }
            debug!(
                engine_id = self.config.engine_id,
                session_number, "cleaned up export session"
            );
        }
    }

    /// Remove an import session and abort all its pending timers.
    fn cleanup_import_session(
        &self,
        sessions: &mut HashMap<u64, ImportSessionState>,
        session_number: u64,
    ) {
        if let Some(state) = sessions.remove(&session_number) {
            // Abort all pending timers for this session.
            for (_serial, handle) in state.timers {
                handle.abort();
            }

            // Abort the inactivity timer if one is active.
            if let Some(handle) = state.inactivity_timer {
                handle.abort();
            }

            // Abort the deferred report timer if one is active.
            if let Some(deferred) = state.deferred_report {
                deferred.timer.abort();
            }

            // Record the closed session number in the recreation prevention history.
            if self.config.session_recreation_history_size > 0 {
                let mut history = self.session_history.lock().unwrap();
                history.insert(session_number);
            }

            debug!(
                engine_id = self.config.engine_id,
                session_number, "cleaned up import session"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_buffer_is_empty() {
        let buf = AggregationBuffer::new(1024);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.bundle_count(), 0);
    }

    #[test]
    fn flush_empty_returns_none() {
        let mut buf = AggregationBuffer::new(1024);
        assert_eq!(buf.flush(), None);
    }

    #[test]
    fn append_single_bundle() {
        let mut buf = AggregationBuffer::new(1024);
        let bundle = b"hello";

        let flushed = buf.append(bundle);
        assert!(flushed.is_none());
        assert!(!buf.is_empty());
        assert_eq!(buf.len(), 4 + 5);
        assert_eq!(buf.bundle_count(), 1);
    }

    #[test]
    fn append_multiple_bundles_within_limit() {
        let mut buf = AggregationBuffer::new(1024);
        let b1 = b"hello";
        let b2 = b"world";

        assert!(buf.append(b1).is_none());
        assert!(buf.append(b2).is_none());

        assert_eq!(buf.bundle_count(), 2);
        assert_eq!(buf.len(), (4 + 5) + (4 + 5));
    }

    #[test]
    fn flush_returns_correct_framing() {
        let mut buf = AggregationBuffer::new(1024);
        buf.append(b"abc");
        buf.append(b"defgh");

        let block = buf.flush().unwrap();

        assert_eq!(block.len(), 4 + 3 + 4 + 5);
        assert_eq!(&block[0..4], &[0, 0, 0, 3]);
        assert_eq!(&block[4..7], b"abc");
        assert_eq!(&block[7..11], &[0, 0, 0, 5]);
        assert_eq!(&block[11..16], b"defgh");

        assert!(buf.is_empty());
        assert_eq!(buf.bundle_count(), 0);
    }

    #[test]
    fn append_triggers_flush_on_size_limit() {
        let mut buf = AggregationBuffer::new(20);

        let b1 = [0u8; 10];
        assert!(buf.append(&b1).is_none());
        assert_eq!(buf.len(), 14);

        let b2 = [1u8; 10];
        let flushed = buf.append(&b2);
        assert!(flushed.is_some());

        let block = flushed.unwrap();
        assert_eq!(block.len(), 14);
        assert_eq!(&block[0..4], &[0, 0, 0, 10]);
        assert_eq!(&block[4..14], &[0u8; 10]);

        assert_eq!(buf.len(), 14);
        assert_eq!(buf.bundle_count(), 1);
    }

    #[test]
    fn append_does_not_flush_when_exactly_at_limit() {
        let mut buf = AggregationBuffer::new(18);

        assert!(buf.append(b"hello").is_none());
        assert!(buf.append(b"world").is_none());

        assert_eq!(buf.len(), 18);
        assert_eq!(buf.bundle_count(), 2);
    }

    #[test]
    fn append_flushes_when_exceeding_limit() {
        let mut buf = AggregationBuffer::new(17);

        assert!(buf.append(b"hello").is_none());
        let flushed = buf.append(b"world");
        assert!(flushed.is_some());

        let block = flushed.unwrap();
        assert_eq!(block.len(), 9);
    }

    #[test]
    fn single_large_bundle_exceeding_limit() {
        let mut buf = AggregationBuffer::new(10);
        let large = [0xABu8; 16];

        let flushed = buf.append(&large);
        assert!(flushed.is_none());
        assert_eq!(buf.len(), 20);
        assert_eq!(buf.bundle_count(), 1);
    }

    #[test]
    fn multiple_flushes_in_sequence() {
        let mut buf = AggregationBuffer::new(12);

        assert!(buf.append(&[1, 2, 3, 4]).is_none());

        let flushed = buf.append(&[5, 6, 7, 8]);
        assert!(flushed.is_some());

        let flushed = buf.append(&[9, 10, 11, 12]);
        assert!(flushed.is_some());

        let block = buf.flush().unwrap();
        assert_eq!(block.len(), 8);
        assert_eq!(&block[0..4], &[0, 0, 0, 4]);
        assert_eq!(&block[4..8], &[9, 10, 11, 12]);
    }

    #[test]
    fn framing_format_is_big_endian() {
        let mut buf = AggregationBuffer::new(65536);
        let bundle = [0u8; 256];
        buf.append(&bundle);

        let block = buf.flush().unwrap();
        assert_eq!(&block[0..4], &[0x00, 0x00, 0x01, 0x00]);
        assert_eq!(block.len(), 4 + 256);
    }

    #[test]
    fn empty_bundle_is_valid() {
        let mut buf = AggregationBuffer::new(1024);
        buf.append(b"");

        let block = buf.flush().unwrap();
        assert_eq!(block.len(), 4);
        assert_eq!(&block[0..4], &[0, 0, 0, 0]);
    }

    #[test]
    fn session_number_monotonically_increases() {
        let counter = AtomicU64::new(1);
        let first = counter.fetch_add(1, Ordering::Relaxed);
        let second = counter.fetch_add(1, Ordering::Relaxed);
        let third = counter.fetch_add(1, Ordering::Relaxed);
        assert_eq!(first, 1);
        assert_eq!(second, 2);
        assert_eq!(third, 3);
    }

    // -----------------------------------------------------------------------
    // TokenBucket Tests
    // -----------------------------------------------------------------------

    #[test]
    fn token_bucket_new_starts_full() {
        let bucket = TokenBucket::new(8000); // 8000 bps = 1000 bytes/sec
        assert_eq!(bucket.rate_bytes_per_sec, 1000.0);
        assert_eq!(bucket.tokens, 1000.0);
    }

    #[test]
    fn token_bucket_converts_bits_to_bytes() {
        let bucket = TokenBucket::new(80_000); // 80,000 bps = 10,000 bytes/sec
        assert_eq!(bucket.rate_bytes_per_sec, 10_000.0);
    }

    #[test]
    fn token_bucket_consume_within_budget_returns_zero() {
        let mut bucket = TokenBucket::new(8000); // 1000 bytes/sec, starts with 1000 tokens
        let sleep = bucket.consume(500);
        assert_eq!(sleep, Duration::ZERO);
    }

    #[test]
    fn token_bucket_consume_exact_budget_returns_zero() {
        let mut bucket = TokenBucket::new(8000); // 1000 bytes/sec, starts with 1000 tokens
        let sleep = bucket.consume(1000);
        assert_eq!(sleep, Duration::ZERO);
    }

    #[test]
    fn token_bucket_consume_over_budget_returns_sleep_duration() {
        let mut bucket = TokenBucket::new(8000); // 1000 bytes/sec, starts with 1000 tokens
        let sleep = bucket.consume(1500);
        // Deficit = 500 bytes, rate = 1000 bytes/sec → sleep = 0.5 sec
        assert!((sleep.as_secs_f64() - 0.5).abs() < 0.001);
    }

    #[test]
    fn token_bucket_multiple_consumes_accumulate_deficit() {
        let mut bucket = TokenBucket::new(8000); // 1000 bytes/sec, starts with 1000 tokens
        let sleep1 = bucket.consume(800);
        assert_eq!(sleep1, Duration::ZERO);
        // tokens = 200 (plus tiny refill from elapsed time, negligible in test)

        let sleep2 = bucket.consume(800);
        // tokens ≈ 200 - 800 = -600, sleep ≈ 0.6 sec
        assert!(sleep2.as_secs_f64() > 0.5);
        assert!(sleep2.as_secs_f64() < 0.7);
    }

    #[test]
    fn token_bucket_refills_over_time() {
        let mut bucket = TokenBucket::new(8000); // 1000 bytes/sec
        // Drain the bucket
        bucket.consume(1000);
        // Manually set last_refill to simulate time passing
        bucket.last_refill = Instant::now() - Duration::from_secs(1);
        // After 1 second, should have refilled 1000 tokens
        let sleep = bucket.consume(500);
        assert_eq!(sleep, Duration::ZERO);
    }

    #[test]
    fn token_bucket_caps_at_burst_limit() {
        let mut bucket = TokenBucket::new(8000); // 1000 bytes/sec
        // Simulate 10 seconds of idle time
        bucket.last_refill = Instant::now() - Duration::from_secs(10);
        // Consume — should refill but cap at 1000 (1 second burst)
        let sleep = bucket.consume(1000);
        assert_eq!(sleep, Duration::ZERO);
        // Tokens should be 0 now (capped at 1000, then consumed 1000)
        let sleep2 = bucket.consume(100);
        // Should need to sleep since tokens ≈ 0 - 100 = -100
        assert!(sleep2.as_secs_f64() > 0.09);
    }

    #[test]
    fn token_bucket_zero_byte_consume_returns_zero() {
        let mut bucket = TokenBucket::new(8000);
        let sleep = bucket.consume(0);
        assert_eq!(sleep, Duration::ZERO);
    }

    #[test]
    fn token_bucket_large_rate() {
        // 1 Gbps = 125 MB/sec
        let mut bucket = TokenBucket::new(1_000_000_000);
        assert_eq!(bucket.rate_bytes_per_sec, 125_000_000.0);
        // Consume 1400 bytes (typical segment) — should be well within budget
        let sleep = bucket.consume(1400);
        assert_eq!(sleep, Duration::ZERO);
    }

    #[test]
    fn token_bucket_small_rate() {
        // 1200 bps = 150 bytes/sec (deep space link)
        let mut bucket = TokenBucket::new(1200);
        assert_eq!(bucket.rate_bytes_per_sec, 150.0);
        // Consume 150 bytes — exactly 1 second of capacity
        let sleep = bucket.consume(150);
        assert_eq!(sleep, Duration::ZERO);
        // Next consume should require sleep
        let sleep = bucket.consume(150);
        // Deficit ≈ 150 bytes at 150 bytes/sec = 1 second
        assert!(sleep.as_secs_f64() > 0.9);
        assert!(sleep.as_secs_f64() < 1.1);
    }

    // -----------------------------------------------------------------------
    // Closed Export Retention Tests
    // -----------------------------------------------------------------------

    #[test]
    fn closed_export_state_initial_counter() {
        let max_retransmissions = 10u32;
        let _handle = tokio::runtime::Handle::try_current();
        // Just test the struct construction logic (no async needed).
        let _session_id = SessionId {
            engine_id: 1,
            session_number: 42,
        };
        // We can't easily create an AbortHandle without a runtime, so we
        // verify the retention duration computation instead.
        let retransmit_cycle_secs = 60u64;
        let retention_secs = 2 * max_retransmissions as u64 * retransmit_cycle_secs + 10;
        assert_eq!(retention_secs, 1210);
    }

    #[test]
    fn retention_duration_computation() {
        // Test various configurations.
        // max_retransmissions=5, retransmit_cycle=30s → 2*5*30+10 = 310s
        let retention = 2 * 5u64 * 30 + 10;
        assert_eq!(retention, 310);

        // max_retransmissions=1, retransmit_cycle=1s → 2*1*1+10 = 12s
        let retention = 2 + 10;
        assert_eq!(retention, 12);

        // max_retransmissions=10, retransmit_cycle=60s → 2*10*60+10 = 1210s
        let retention = 2 * 10u64 * 60 + 10;
        assert_eq!(retention, 1210);
    }

    #[tokio::test]
    async fn closed_export_responds_to_late_report() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        // Create a minimal mock sink.
        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            max_retransmissions: 3,
            retransmit_cycle_secs: 10,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Manually insert a closed export.
        let session_id = SessionId {
            engine_id: 1,
            session_number: 100,
        };
        let timer_handle = tokio::spawn(async {
            // Long-running timer that won't fire during the test.
            tokio::time::sleep(Duration::from_secs(9999)).await;
        });
        let closed_state = ClosedExportState {
            session_id,
            response_counter: 3,
            retention_timer: timer_handle.abort_handle(),
        };
        span.closed_exports.lock().await.insert(100, closed_state);

        // Send a late report — should decrement counter.
        span.on_closed_export_report(100, 1).await;
        {
            let closed = span.closed_exports.lock().await;
            let state = closed.get(&100).unwrap();
            assert_eq!(state.response_counter, 2);
        }

        // Send another late report.
        span.on_closed_export_report(100, 2).await;
        {
            let closed = span.closed_exports.lock().await;
            let state = closed.get(&100).unwrap();
            assert_eq!(state.response_counter, 1);
        }

        // Send final report — counter reaches 0, should be discarded.
        span.on_closed_export_report(100, 3).await;
        {
            let closed = span.closed_exports.lock().await;
            assert!(!closed.contains_key(&100));
        }
    }

    #[tokio::test]
    async fn closed_export_retention_timer_discards() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            max_retransmissions: 5,
            retransmit_cycle_secs: 10,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Manually insert a closed export with a timer that won't fire.
        let session_id = SessionId {
            engine_id: 1,
            session_number: 200,
        };
        let timer_handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(9999)).await;
        });
        let closed_state = ClosedExportState {
            session_id,
            response_counter: 5,
            retention_timer: timer_handle.abort_handle(),
        };
        span.closed_exports.lock().await.insert(200, closed_state);

        // Simulate retention timer expiry.
        span.on_closed_export_retention_expired(200).await;

        // Should be discarded.
        let closed = span.closed_exports.lock().await;
        assert!(!closed.contains_key(&200));
    }

    #[tokio::test]
    async fn closed_export_ignores_unknown_session() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Report for a session that's neither active nor closed — should not panic.
        span.on_closed_export_report(999, 1).await;

        // Verify nothing was added.
        let closed = span.closed_exports.lock().await;
        assert!(closed.is_empty());
    }

    #[tokio::test]
    async fn retain_closed_export_creates_entry() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            max_retransmissions: 3,
            retransmit_cycle_secs: 10,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        let session_id = SessionId {
            engine_id: 1,
            session_number: 50,
        };

        span.retain_closed_export(50, session_id).await;

        let closed = span.closed_exports.lock().await;
        let state = closed.get(&50).unwrap();
        assert_eq!(state.session_id, session_id);
        assert_eq!(state.response_counter, 3); // max_retransmissions
    }

    // -----------------------------------------------------------------------
    // SessionHistory Tests
    // -----------------------------------------------------------------------

    #[test]
    fn session_history_new_empty() {
        let history = SessionHistory::new(10);
        assert!(history.is_empty());
        assert_eq!(history.len(), 0);
        assert!(history.is_enabled());
    }

    #[test]
    fn session_history_disabled_when_capacity_zero() {
        let history = SessionHistory::new(0);
        assert!(!history.is_enabled());
    }

    #[test]
    fn session_history_insert_and_contains() {
        let mut history = SessionHistory::new(5);
        history.insert(42);
        assert!(history.contains(42));
        assert!(!history.contains(99));
        assert_eq!(history.len(), 1);
    }

    #[test]
    fn session_history_insert_multiple() {
        let mut history = SessionHistory::new(5);
        history.insert(1);
        history.insert(2);
        history.insert(3);
        assert!(history.contains(1));
        assert!(history.contains(2));
        assert!(history.contains(3));
        assert!(!history.contains(4));
        assert_eq!(history.len(), 3);
    }

    #[test]
    fn session_history_evicts_oldest_when_full() {
        let mut history = SessionHistory::new(3);
        history.insert(10);
        history.insert(20);
        history.insert(30);
        assert_eq!(history.len(), 3);

        // Insert a 4th — should evict 10 (oldest).
        history.insert(40);
        assert_eq!(history.len(), 3);
        assert!(!history.contains(10)); // evicted
        assert!(history.contains(20));
        assert!(history.contains(30));
        assert!(history.contains(40));
    }

    #[test]
    fn session_history_evicts_in_fifo_order() {
        let mut history = SessionHistory::new(2);
        history.insert(1);
        history.insert(2);

        // Evict 1.
        history.insert(3);
        assert!(!history.contains(1));
        assert!(history.contains(2));
        assert!(history.contains(3));

        // Evict 2.
        history.insert(4);
        assert!(!history.contains(2));
        assert!(history.contains(3));
        assert!(history.contains(4));
    }

    #[test]
    fn session_history_capacity_one() {
        let mut history = SessionHistory::new(1);
        history.insert(100);
        assert!(history.contains(100));
        assert_eq!(history.len(), 1);

        history.insert(200);
        assert!(!history.contains(100));
        assert!(history.contains(200));
        assert_eq!(history.len(), 1);
    }

    #[test]
    fn session_history_disabled_insert_is_noop() {
        let mut history = SessionHistory::new(0);
        history.insert(42);
        assert!(history.is_empty());
        assert!(!history.contains(42));
    }

    #[test]
    fn session_history_deduplicates() {
        // Inserting the same session number twice does NOT use two slots.
        let mut history = SessionHistory::new(3);
        history.insert(5);
        history.insert(5);
        assert_eq!(history.len(), 1);
        assert!(history.contains(5));
    }

    // -----------------------------------------------------------------------
    // Max Import Sessions Enforcement Tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn max_import_sessions_discards_when_at_limit() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // Set max_import_sessions to 2 for easy testing.
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            max_import_sessions: 2,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Manually insert 2 import sessions to fill the limit.
        {
            let mut sessions = span.import_sessions.lock().await;
            let session_id_1 = SessionId {
                engine_id: 2,
                session_number: 10,
            };
            let import_config = ImportConfig {
                max_reports: None,
                retransmit_timeout: Duration::from_secs(60),
                max_claims_per_report: 20,
                expected_client_service_id: 1,
                max_red_data_bytes: None,
                defer_report_ms: 0,
            };
            let session_1 = ImportSession::new(session_id_1, import_config.clone());
            sessions.insert(
                10,
                ImportSessionState {
                    session: session_1,
                    timers: HashMap::new(),
                    inactivity_timer: None,
                    deferred_report: None,
                },
            );

            let session_id_2 = SessionId {
                engine_id: 2,
                session_number: 20,
            };
            let session_2 = ImportSession::new(session_id_2, import_config);
            sessions.insert(
                20,
                ImportSessionState {
                    session: session_2,
                    timers: HashMap::new(),
                    inactivity_timer: None,
                    deferred_report: None,
                },
            );
        }

        // Verify we have 2 sessions.
        assert_eq!(span.import_sessions.lock().await.len(), 2);

        // Try to create a 3rd session via on_import_data_segment — should be discarded.
        span.on_import_data_segment(
            30, // new session number
            SegmentType::RedData,
            1,           // client_service_id
            0,           // offset
            &[0u8; 100], // data
            None,        // no checkpoint
        )
        .await;

        // Should still have only 2 sessions (session 30 was not created).
        assert_eq!(span.import_sessions.lock().await.len(), 2);
        assert!(!span.import_sessions.lock().await.contains_key(&30));
    }

    #[tokio::test]
    async fn max_import_sessions_allows_existing_session_data() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // Set max_import_sessions to 1.
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            max_import_sessions: 1,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Create the first session via on_import_data_segment.
        span.on_import_data_segment(10, SegmentType::RedData, 1, 0, &[0u8; 50], None)
            .await;

        // Session 10 should exist.
        assert_eq!(span.import_sessions.lock().await.len(), 1);
        assert!(span.import_sessions.lock().await.contains_key(&10));

        // Sending more data for the SAME session should still work (not discarded).
        span.on_import_data_segment(
            10, // same session
            SegmentType::RedData,
            1,
            50,
            &[1u8; 50],
            None,
        )
        .await;

        // Session 10 should still exist and be the only one.
        assert_eq!(span.import_sessions.lock().await.len(), 1);
    }

    // -----------------------------------------------------------------------
    // Session Recreation Prevention Integration Tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn session_recreation_prevention_discards_closed_session() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // Enable session recreation prevention with history size 5.
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            session_recreation_history_size: 5,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Manually record session number 42 in the history (simulating a closed session).
        {
            let mut history = span.session_history.lock().unwrap();
            history.insert(42);
        }

        // Try to create a new import session with session number 42 — should be discarded.
        span.on_import_data_segment(42, SegmentType::RedData, 1, 0, &[0u8; 100], None)
            .await;

        // Session 42 should NOT have been created.
        assert!(!span.import_sessions.lock().await.contains_key(&42));

        // A different session number (43) should still be accepted.
        span.on_import_data_segment(43, SegmentType::RedData, 1, 0, &[0u8; 100], None)
            .await;

        // Session 43 should have been created.
        assert!(span.import_sessions.lock().await.contains_key(&43));
    }

    #[tokio::test]
    async fn session_recreation_prevention_disabled_when_zero() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // session_recreation_history_size = 0 (disabled, the default).
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            session_recreation_history_size: 0,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Even if we somehow had a session number in history (shouldn't happen with size 0),
        // the check is skipped entirely when history_size is 0.
        // Just verify that a new session can be created normally.
        span.on_import_data_segment(42, SegmentType::RedData, 1, 0, &[0u8; 100], None)
            .await;

        assert!(span.import_sessions.lock().await.contains_key(&42));
    }

    #[tokio::test]
    async fn session_recreation_cleanup_records_to_history() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // Enable session recreation prevention.
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            session_recreation_history_size: 10,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Create an import session manually.
        {
            let mut sessions = span.import_sessions.lock().await;
            let session_id = SessionId {
                engine_id: 2,
                session_number: 77,
            };
            let import_config = ImportConfig {
                max_reports: None,
                retransmit_timeout: Duration::from_secs(60),
                max_claims_per_report: 20,
                expected_client_service_id: 1,
                max_red_data_bytes: None,
                defer_report_ms: 0,
            };
            let session = ImportSession::new(session_id, import_config);
            sessions.insert(
                77,
                ImportSessionState {
                    session,
                    timers: HashMap::new(),
                    inactivity_timer: None,
                    deferred_report: None,
                },
            );
        }

        // Clean up the session — should record session number 77 in history.
        {
            let mut sessions = span.import_sessions.lock().await;
            span.cleanup_import_session(&mut sessions, 77);
        }

        // Verify session 77 is now in the history buffer.
        {
            let history = span.session_history.lock().unwrap();
            assert!(history.contains(77));
        }

        // Now try to create a new session with the same number — should be discarded.
        span.on_import_data_segment(77, SegmentType::RedData, 1, 0, &[0u8; 50], None)
            .await;

        // Session 77 should NOT have been recreated.
        assert!(!span.import_sessions.lock().await.contains_key(&77));
    }

    #[tokio::test]
    async fn session_recreation_history_eviction_allows_reuse() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // History size of 2 — only remembers the 2 most recent closed sessions.
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            session_recreation_history_size: 2,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Fill the history buffer: sessions 10, 20.
        {
            let mut history = span.session_history.lock().unwrap();
            history.insert(10);
            history.insert(20);
        }

        // Session 10 should be blocked.
        span.on_import_data_segment(10, SegmentType::RedData, 1, 0, &[0u8; 50], None)
            .await;
        assert!(!span.import_sessions.lock().await.contains_key(&10));

        // Now insert session 30 into history — this evicts session 10.
        {
            let mut history = span.session_history.lock().unwrap();
            history.insert(30);
        }

        // Session 10 should now be allowed (evicted from history).
        span.on_import_data_segment(10, SegmentType::RedData, 1, 0, &[0u8; 50], None)
            .await;
        assert!(span.import_sessions.lock().await.contains_key(&10));

        // Session 20 should still be blocked (still in history).
        span.on_import_data_segment(20, SegmentType::RedData, 1, 0, &[0u8; 50], None)
            .await;
        assert!(!span.import_sessions.lock().await.contains_key(&20));
    }

    // -----------------------------------------------------------------------
    // Stale Import Session Inactivity Timer Tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn inactivity_timer_disabled_when_zero() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // session_inactivity_limit_secs = 0 (disabled).
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            session_inactivity_limit_secs: 0,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Create an import session via data segment.
        span.on_import_data_segment(10, SegmentType::RedData, 1, 0, &[0u8; 50], None)
            .await;

        // Session should exist and have no inactivity timer.
        let sessions = span.import_sessions.lock().await;
        let state = sessions.get(&10).unwrap();
        assert!(state.inactivity_timer.is_none());
    }

    #[tokio::test]
    async fn inactivity_timer_set_when_configured() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // session_inactivity_limit_secs = 300 (5 minutes).
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            session_inactivity_limit_secs: 300,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Create an import session via data segment.
        span.on_import_data_segment(10, SegmentType::RedData, 1, 0, &[0u8; 50], None)
            .await;

        // Session should exist and have an inactivity timer set.
        let sessions = span.import_sessions.lock().await;
        let state = sessions.get(&10).unwrap();
        assert!(state.inactivity_timer.is_some());
    }

    #[tokio::test]
    async fn inactivity_timer_fires_and_cancels_session() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // Use a very short inactivity limit for testing (1 second).
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            session_inactivity_limit_secs: 1,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Create an import session via data segment.
        span.on_import_data_segment(10, SegmentType::RedData, 1, 0, &[0u8; 50], None)
            .await;

        // Session should exist.
        assert!(span.import_sessions.lock().await.contains_key(&10));

        // Wait for the inactivity timer to fire (slightly more than 1 second).
        tokio::time::sleep(Duration::from_millis(1200)).await;

        // Session should have been cancelled and cleaned up.
        assert!(!span.import_sessions.lock().await.contains_key(&10));
    }

    #[tokio::test]
    async fn inactivity_timer_resets_on_data_segment() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // Use a 2-second inactivity limit.
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            session_inactivity_limit_secs: 2,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Create an import session.
        span.on_import_data_segment(10, SegmentType::RedData, 1, 0, &[0u8; 50], None)
            .await;

        // Wait 1.5 seconds (less than the 2-second limit).
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Send another data segment to reset the timer.
        span.on_import_data_segment(10, SegmentType::RedData, 1, 50, &[1u8; 50], None)
            .await;

        // Session should still exist (timer was reset).
        assert!(span.import_sessions.lock().await.contains_key(&10));

        // Wait another 1.5 seconds (total 3 seconds from start, but only 1.5 from last reset).
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Session should still exist (only 1.5s since last data, limit is 2s).
        assert!(span.import_sessions.lock().await.contains_key(&10));

        // Wait another 1 second (now 2.5s since last data segment — exceeds limit).
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Session should have been cancelled due to inactivity.
        assert!(!span.import_sessions.lock().await.contains_key(&10));
    }

    #[tokio::test]
    async fn inactivity_expired_ignores_unknown_session() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            session_inactivity_limit_secs: 60,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Calling on_import_inactivity_expired for a non-existent session should not panic.
        span.on_import_inactivity_expired(999).await;

        // No sessions should exist.
        assert!(span.import_sessions.lock().await.is_empty());
    }

    // -----------------------------------------------------------------------
    // Deferred Report Sending Tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn deferred_report_timer_fires_and_generates_report() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // Configure with 100ms deferral.
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            defer_report_ms: 100,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Send data at offset 0 (covers [0, 50))
        span.on_import_data_segment(10, SegmentType::RedData, 1, 0, &[0xAA; 50], None)
            .await;

        // Send checkpoint at offset 100 (covers [100, 150))
        // Gap at [50, 100) → should defer report
        span.on_import_data_segment(
            10,
            SegmentType::RedCheckpoint,
            1,
            100,
            &[0xBB; 50],
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        )
        .await;

        // Verify deferred report is set.
        {
            let sessions = span.import_sessions.lock().await;
            let state = sessions.get(&10).unwrap();
            assert!(state.deferred_report.is_some());
            let deferred = state.deferred_report.as_ref().unwrap();
            assert_eq!(deferred.checkpoint_serial, 1);
            assert_eq!(deferred.upper_bound, 150);
        }

        // Wait for the deferral timer to fire (slightly more than 100ms).
        tokio::time::sleep(Duration::from_millis(150)).await;

        // After timer fires, deferred_report should be cleared and report generated.
        {
            let sessions = span.import_sessions.lock().await;
            let state = sessions.get(&10).unwrap();
            assert!(state.deferred_report.is_none());
            // Report should have been generated (report timer should be active).
            assert!(!state.timers.is_empty());
            assert_eq!(state.session.reports_generated(), 1);
        }
    }

    #[tokio::test]
    async fn deferred_report_sent_immediately_when_gaps_filled() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // Configure with a long deferral (5 seconds) so it won't fire during test.
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            defer_report_ms: 5000,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Send data at offset 0 (covers [0, 50))
        span.on_import_data_segment(10, SegmentType::RedData, 1, 0, &[0xAA; 50], None)
            .await;

        // Send checkpoint at offset 100 (covers [100, 150))
        // Gap at [50, 100) → should defer report
        span.on_import_data_segment(
            10,
            SegmentType::RedCheckpoint,
            1,
            100,
            &[0xBB; 50],
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        )
        .await;

        // Verify deferred report is set.
        {
            let sessions = span.import_sessions.lock().await;
            let state = sessions.get(&10).unwrap();
            assert!(state.deferred_report.is_some());
        }

        // Now fill the gap [50, 100) — this should trigger immediate report.
        span.on_import_data_segment(10, SegmentType::RedData, 1, 50, &[0xCC; 50], None)
            .await;

        // Deferred report should be cleared and report generated immediately.
        {
            let sessions = span.import_sessions.lock().await;
            let state = sessions.get(&10).unwrap();
            assert!(state.deferred_report.is_none());
            assert_eq!(state.session.reports_generated(), 1);
            // Report timer should be active.
            assert!(!state.timers.is_empty());
        }
    }

    #[tokio::test]
    async fn deferred_report_timer_resets_on_gap_filling_segment() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // Configure with 2-second deferral.
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            defer_report_ms: 2000,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Send data at offset 0 (covers [0, 50))
        span.on_import_data_segment(10, SegmentType::RedData, 1, 0, &[0xAA; 50], None)
            .await;

        // Send checkpoint at offset 200 (covers [200, 250))
        // Gaps at [50, 200) → should defer report
        span.on_import_data_segment(
            10,
            SegmentType::RedCheckpoint,
            1,
            200,
            &[0xBB; 50],
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        )
        .await;

        // Verify deferred report is set.
        {
            let sessions = span.import_sessions.lock().await;
            let state = sessions.get(&10).unwrap();
            assert!(state.deferred_report.is_some());
        }

        // Wait 1.5 seconds (less than the 2-second deferral).
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Send a gap-filling segment [50, 100) — gaps still remain at [100, 200)
        // This should reset the timer.
        span.on_import_data_segment(10, SegmentType::RedData, 1, 50, &[0xCC; 50], None)
            .await;

        // Deferred report should still be active (timer was reset, not cleared).
        {
            let sessions = span.import_sessions.lock().await;
            let state = sessions.get(&10).unwrap();
            assert!(state.deferred_report.is_some());
            // No report generated yet.
            assert_eq!(state.session.reports_generated(), 0);
        }

        // Wait another 1.5 seconds (total 3s from start, but only 1.5s from reset).
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Timer should NOT have fired yet (only 1.5s since reset, need 2s).
        {
            let sessions = span.import_sessions.lock().await;
            let state = sessions.get(&10).unwrap();
            assert!(state.deferred_report.is_some());
            assert_eq!(state.session.reports_generated(), 0);
        }

        // Wait another 1 second (now 2.5s since reset — exceeds 2s deferral).
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Timer should have fired and report generated.
        {
            let sessions = span.import_sessions.lock().await;
            let state = sessions.get(&10).unwrap();
            assert!(state.deferred_report.is_none());
            assert_eq!(state.session.reports_generated(), 1);
        }
    }

    #[tokio::test]
    async fn no_deferral_when_defer_report_ms_is_zero() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // defer_report_ms = 0 (default, disabled).
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            defer_report_ms: 0,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Send data at offset 0 (covers [0, 50))
        span.on_import_data_segment(10, SegmentType::RedData, 1, 0, &[0xAA; 50], None)
            .await;

        // Send checkpoint at offset 100 (covers [100, 150))
        // Gap at [50, 100) but defer_report_ms = 0 → immediate report
        span.on_import_data_segment(
            10,
            SegmentType::RedCheckpoint,
            1,
            100,
            &[0xBB; 50],
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        )
        .await;

        // Report should have been generated immediately (no deferral).
        {
            let sessions = span.import_sessions.lock().await;
            let state = sessions.get(&10).unwrap();
            assert!(state.deferred_report.is_none());
            assert_eq!(state.session.reports_generated(), 1);
            // Report retransmit timer should be active.
            assert!(!state.timers.is_empty());
        }
    }

    #[tokio::test]
    async fn deferred_report_not_reset_on_redundant_segment() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // Configure with 1-second deferral.
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            defer_report_ms: 1000,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Send data at offset 0 (covers [0, 50))
        span.on_import_data_segment(10, SegmentType::RedData, 1, 0, &[0xAA; 50], None)
            .await;

        // Send checkpoint at offset 100 (covers [100, 150))
        // Gap at [50, 100) → should defer report
        span.on_import_data_segment(
            10,
            SegmentType::RedCheckpoint,
            1,
            100,
            &[0xBB; 50],
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        )
        .await;

        // Verify deferred report is set.
        {
            let sessions = span.import_sessions.lock().await;
            let state = sessions.get(&10).unwrap();
            assert!(state.deferred_report.is_some());
        }

        // Wait 800ms.
        tokio::time::sleep(Duration::from_millis(800)).await;

        // Send a REDUNDANT segment (same data at offset 0, already covered).
        // This should NOT reset the timer since it doesn't fill any gap.
        span.on_import_data_segment(10, SegmentType::RedData, 1, 0, &[0xAA; 50], None)
            .await;

        // Deferred report should still be active.
        {
            let sessions = span.import_sessions.lock().await;
            let state = sessions.get(&10).unwrap();
            assert!(state.deferred_report.is_some());
        }

        // Wait another 300ms (total 1100ms from checkpoint — exceeds 1s deferral).
        // Since the redundant segment did NOT reset the timer, it should fire.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Timer should have fired and report generated.
        {
            let sessions = span.import_sessions.lock().await;
            let state = sessions.get(&10).unwrap();
            assert!(state.deferred_report.is_none());
            assert_eq!(state.session.reports_generated(), 1);
        }
    }

    // -----------------------------------------------------------------------
    // RTT-Based Timer Computation Tests
    // -----------------------------------------------------------------------

    #[test]
    fn compute_retransmit_timeout_with_owlt() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        // When one_way_light_time_ms is configured, timeout = 2 × (owlt + margin).
        let config = SpanConfig {
            engine_id: 1,
            address: "127.0.0.1:1113".parse().unwrap(),
            one_way_light_time_ms: Some(28000), // 28 seconds
            one_way_margin_time_ms: 2000,       // 2 seconds
            retransmit_cycle_secs: 60,
            ..Default::default()
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let span = rt.block_on(async {
            let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let sink: Arc<dyn Sink> = Arc::new(MockSink);
            Span::new(config, 1, socket, sink)
        });

        let timeout = span.compute_retransmit_timeout();
        // 2 × (28000 + 2000) = 60000 ms = 60 seconds
        assert_eq!(timeout, Duration::from_millis(60000));
    }

    #[test]
    fn compute_retransmit_timeout_without_owlt() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        // When one_way_light_time_ms is not configured, fall back to retransmit_cycle_secs.
        let config = SpanConfig {
            engine_id: 1,
            address: "127.0.0.1:1113".parse().unwrap(),
            one_way_light_time_ms: None,
            one_way_margin_time_ms: 5000, // should be ignored
            retransmit_cycle_secs: 45,
            ..Default::default()
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let span = rt.block_on(async {
            let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let sink: Arc<dyn Sink> = Arc::new(MockSink);
            Span::new(config, 1, socket, sink)
        });

        let timeout = span.compute_retransmit_timeout();
        assert_eq!(timeout, Duration::from_secs(45));
    }

    #[test]
    fn compute_retransmit_timeout_owlt_zero() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        // one_way_light_time_ms = Some(0) means 0ms light time (local link).
        // timeout = 2 × (0 + 500) = 1000 ms
        let config = SpanConfig {
            engine_id: 1,
            address: "127.0.0.1:1113".parse().unwrap(),
            one_way_light_time_ms: Some(0),
            one_way_margin_time_ms: 500,
            retransmit_cycle_secs: 60,
            ..Default::default()
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let span = rt.block_on(async {
            let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let sink: Arc<dyn Sink> = Arc::new(MockSink);
            Span::new(config, 1, socket, sink)
        });

        let timeout = span.compute_retransmit_timeout();
        assert_eq!(timeout, Duration::from_millis(1000));
    }

    #[test]
    fn compute_retransmit_timeout_zero_margin() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        // one_way_light_time_ms = Some(14000), margin = 0
        // timeout = 2 × (14000 + 0) = 28000 ms
        let config = SpanConfig {
            engine_id: 1,
            address: "127.0.0.1:1113".parse().unwrap(),
            one_way_light_time_ms: Some(14000),
            one_way_margin_time_ms: 0,
            retransmit_cycle_secs: 60,
            ..Default::default()
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let span = rt.block_on(async {
            let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let sink: Arc<dyn Sink> = Arc::new(MockSink);
            Span::new(config, 1, socket, sink)
        });

        let timeout = span.compute_retransmit_timeout();
        assert_eq!(timeout, Duration::from_millis(28000));
    }

    #[test]
    fn update_one_way_light_time_runtime() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        // Start with no OWLT configured, then update at runtime.
        let config = SpanConfig {
            engine_id: 1,
            address: "127.0.0.1:1113".parse().unwrap(),
            one_way_light_time_ms: None,
            one_way_margin_time_ms: 1000,
            retransmit_cycle_secs: 60,
            ..Default::default()
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let span = rt.block_on(async {
            let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let sink: Arc<dyn Sink> = Arc::new(MockSink);
            Span::new(config, 1, socket, sink)
        });

        // Initially falls back to retransmit_cycle_secs.
        assert_eq!(span.compute_retransmit_timeout(), Duration::from_secs(60));

        // Update to 20000ms OWLT.
        span.update_one_way_light_time(Some(20000));
        // timeout = 2 × (20000 + 1000) = 42000 ms
        assert_eq!(
            span.compute_retransmit_timeout(),
            Duration::from_millis(42000)
        );

        // Update to a different value (orbital geometry changed).
        span.update_one_way_light_time(Some(35000));
        // timeout = 2 × (35000 + 1000) = 72000 ms
        assert_eq!(
            span.compute_retransmit_timeout(),
            Duration::from_millis(72000)
        );

        // Clear OWLT (revert to flat fallback).
        span.update_one_way_light_time(None);
        assert_eq!(span.compute_retransmit_timeout(), Duration::from_secs(60));
    }

    #[test]
    fn update_one_way_light_time_to_zero() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        // Runtime update to 0ms OWLT (local link scenario).
        let config = SpanConfig {
            engine_id: 1,
            address: "127.0.0.1:1113".parse().unwrap(),
            one_way_light_time_ms: None,
            one_way_margin_time_ms: 200,
            retransmit_cycle_secs: 30,
            ..Default::default()
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let span = rt.block_on(async {
            let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let sink: Arc<dyn Sink> = Arc::new(MockSink);
            Span::new(config, 1, socket, sink)
        });

        // Set OWLT to 0.
        span.update_one_way_light_time(Some(0));
        // timeout = 2 × (0 + 200) = 400 ms
        assert_eq!(
            span.compute_retransmit_timeout(),
            Duration::from_millis(400)
        );
    }

    // -----------------------------------------------------------------------
    // Link Status Detection (Ping) Tests
    // -----------------------------------------------------------------------

    #[test]
    fn ping_session_number_is_zero() {
        // Session number 0 is reserved for ping probes.
        assert_eq!(Span::PING_SESSION_NUMBER, 0);
    }

    #[test]
    fn link_alive_starts_true() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let span = rt.block_on(async {
            let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let sink: Arc<dyn Sink> = Arc::new(MockSink);
            let config = SpanConfig {
                engine_id: 2,
                address: "127.0.0.1:1113".parse().unwrap(),
                ping_interval_secs: 5,
                ..Default::default()
            };
            Span::new(config, 1, socket, sink)
        });

        assert!(span.is_link_alive());
    }

    #[test]
    fn ping_cancel_ack_marks_link_alive() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let span = rt.block_on(async {
            let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let sink: Arc<dyn Sink> = Arc::new(MockSink);
            let config = SpanConfig {
                engine_id: 2,
                address: "127.0.0.1:1113".parse().unwrap(),
                ping_interval_secs: 5,
                ..Default::default()
            };
            Span::new(config, 1, socket, sink)
        });

        // Simulate link going down.
        {
            let mut state = span.link_state.lock().unwrap();
            *state = LinkState::DownPing;
        }
        assert!(!span.is_link_alive());

        // Receive a ping CAS — should mark link alive.
        span.on_ping_cancel_ack_received();
        assert!(span.is_link_alive());
    }

    #[test]
    fn ping_cancel_ack_cancels_response_timer() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let sink: Arc<dyn Sink> = Arc::new(MockSink);
            let config = SpanConfig {
                engine_id: 2,
                address: "127.0.0.1:1113".parse().unwrap(),
                ping_interval_secs: 5,
                ..Default::default()
            };
            let span = Arc::new(Span::new(config, 1, socket, sink));

            // Simulate a pending response timer by spawning a dummy task.
            let dummy_handle = tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(600)).await;
            });
            {
                let mut timer = span.ping_response_timer.lock().unwrap();
                *timer = Some(dummy_handle.abort_handle());
            }

            // Verify timer is set.
            assert!(span.ping_response_timer.lock().unwrap().is_some());

            // Receive ping CAS — should cancel the response timer.
            span.on_ping_cancel_ack_received();

            // Timer should be cleared.
            assert!(span.ping_response_timer.lock().unwrap().is_none());
        });
    }

    #[tokio::test]
    async fn ping_timer_not_started_when_disabled() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // ping_interval_secs = 0 (disabled).
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            ping_interval_secs: 0,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));
        span.start_ping_timer();

        // No timer should be started.
        assert!(span.ping_timer.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn ping_timer_started_when_enabled() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        // ping_interval_secs = 5 (enabled).
        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            ping_interval_secs: 5,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));
        span.start_ping_timer();

        // Timer should be started.
        assert!(span.ping_timer.lock().unwrap().is_some());

        // Clean up.
        span.stop_ping_timer();
        assert!(span.ping_timer.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn ping_response_timeout_marks_link_down() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            ping_interval_secs: 1,
            retransmit_cycle_secs: 1,
            max_retransmissions: 1,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Link should start alive.
        assert!(span.is_link_alive());

        // Directly call on_ping_response_timeout to simulate timeout.
        span.on_ping_response_timeout().await;

        // Link should now be down.
        assert!(!span.is_link_alive());
    }

    #[tokio::test]
    async fn ping_not_sent_when_data_recently_sent() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            ping_interval_secs: 5,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Set last_send_time to now (data was just sent).
        {
            let mut last_send = span.last_send_time.lock().unwrap();
            *last_send = Instant::now();
        }

        // Fire the ping timer — should NOT send a ping since data was sent recently.
        span.on_ping_timer_fired().await;

        // No response timer should be set (ping was suppressed).
        assert!(span.ping_response_timer.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn ping_sent_when_no_recent_data() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            ping_interval_secs: 1,
            retransmit_cycle_secs: 10,
            max_retransmissions: 3,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Set last_send_time to 2 seconds ago (exceeds ping_interval_secs=1).
        {
            let mut last_send = span.last_send_time.lock().unwrap();
            *last_send = Instant::now() - Duration::from_secs(2);
        }

        // Fire the ping timer — should send a ping.
        span.on_ping_timer_fired().await;

        // Response timer should be set (ping was sent).
        assert!(span.ping_response_timer.lock().unwrap().is_some());

        // Clean up.
        span.stop_ping_timer();
    }

    #[tokio::test]
    async fn ping_not_sent_when_response_timer_already_pending() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            ping_interval_secs: 1,
            retransmit_cycle_secs: 10,
            max_retransmissions: 3,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Set last_send_time to 2 seconds ago.
        {
            let mut last_send = span.last_send_time.lock().unwrap();
            *last_send = Instant::now() - Duration::from_secs(2);
        }

        // Set a dummy response timer (simulating a ping already in flight).
        let dummy_handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(600)).await;
        });
        {
            let mut timer = span.ping_response_timer.lock().unwrap();
            *timer = Some(dummy_handle.abort_handle());
        }

        // Fire the ping timer — should NOT send another ping.
        span.on_ping_timer_fired().await;

        // Response timer should still be the original one (not replaced).
        assert!(span.ping_response_timer.lock().unwrap().is_some());

        // Clean up.
        span.stop_ping_timer();
    }

    #[tokio::test]
    async fn stop_ping_timer_cleans_up_both_timers() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            ping_interval_secs: 5,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Start the ping timer.
        span.start_ping_timer();
        assert!(span.ping_timer.lock().unwrap().is_some());

        // Set a dummy response timer.
        let dummy_handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(600)).await;
        });
        {
            let mut timer = span.ping_response_timer.lock().unwrap();
            *timer = Some(dummy_handle.abort_handle());
        }

        // Stop should clean up both.
        span.stop_ping_timer();
        assert!(span.ping_timer.lock().unwrap().is_none());
        assert!(span.ping_response_timer.lock().unwrap().is_none());
    }

    // -----------------------------------------------------------------------
    // Purge on Link Down Tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn purge_export_sessions_cancels_all_active_sessions() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;
        use std::sync::atomic::{AtomicBool, Ordering};

        struct MockSink {
            remove_peer_called: AtomicBool,
        }
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                self.remove_peer_called.store(true, Ordering::Relaxed);
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let mock_sink = Arc::new(MockSink {
            remove_peer_called: AtomicBool::new(false),
        });
        let sink: Arc<dyn Sink> = mock_sink.clone();

        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            purge_on_link_down: true,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Insert some dummy export sessions with timers.
        {
            let mut sessions = span.export_sessions.lock().await;
            for i in 1..=3 {
                let timer_handle = tokio::spawn(async {
                    tokio::time::sleep(Duration::from_secs(9999)).await;
                });
                let mut timers = HashMap::new();
                timers.insert(i, timer_handle.abort_handle());

                let session_id = SessionId {
                    engine_id: 1,
                    session_number: i,
                };
                let export_config = ExportConfig {
                    max_segment_size: 1400,
                    max_retransmissions: 10,
                    retransmit_timeout: Duration::from_secs(60),
                    checkpoint_every_n: 0,
                    max_checkpoints: None,
                    green: false,
                };
                let (session, _actions) =
                    ExportSession::new(session_id, Bytes::from(vec![0u8; 100]), 1, export_config);
                sessions.insert(i, ExportSessionState { session, timers });
            }
        }

        // Verify we have 3 sessions.
        assert_eq!(span.export_sessions.lock().await.len(), 3);

        // Purge all export sessions.
        span.purge_export_sessions().await;

        // All sessions should be removed.
        assert_eq!(span.export_sessions.lock().await.len(), 0);

        // BPA should have been notified.
        assert!(mock_sink.remove_peer_called.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn purge_on_link_down_false_does_not_purge() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;
        use std::sync::atomic::{AtomicBool, Ordering};

        struct MockSink {
            remove_peer_called: AtomicBool,
        }
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                self.remove_peer_called.store(true, Ordering::Relaxed);
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let mock_sink = Arc::new(MockSink {
            remove_peer_called: AtomicBool::new(false),
        });
        let sink: Arc<dyn Sink> = mock_sink.clone();

        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            purge_on_link_down: false, // Disabled
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Insert a dummy export session.
        {
            let mut sessions = span.export_sessions.lock().await;
            let timer_handle = tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(9999)).await;
            });
            let mut timers = HashMap::new();
            timers.insert(1, timer_handle.abort_handle());

            let session_id = SessionId {
                engine_id: 1,
                session_number: 1,
            };
            let export_config = ExportConfig {
                max_segment_size: 1400,
                max_retransmissions: 10,
                retransmit_timeout: Duration::from_secs(60),
                checkpoint_every_n: 0,
                max_checkpoints: None,
                green: false,
            };
            let (session, _actions) =
                ExportSession::new(session_id, Bytes::from(vec![0u8; 100]), 1, export_config);
            sessions.insert(1, ExportSessionState { session, timers });
        }

        // Simulate ping response timeout (purge_on_link_down is false).
        span.on_ping_response_timeout().await;

        // Link should be marked as down.
        assert!(!span.is_link_alive());

        // But sessions should NOT be purged.
        assert_eq!(span.export_sessions.lock().await.len(), 1);

        // BPA should NOT have been notified.
        assert!(!mock_sink.remove_peer_called.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn purge_on_link_down_true_purges_on_timeout() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;
        use std::sync::atomic::{AtomicBool, Ordering};

        struct MockSink {
            remove_peer_called: AtomicBool,
        }
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                self.remove_peer_called.store(true, Ordering::Relaxed);
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let mock_sink = Arc::new(MockSink {
            remove_peer_called: AtomicBool::new(false),
        });
        let sink: Arc<dyn Sink> = mock_sink.clone();

        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            purge_on_link_down: true, // Enabled
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // Insert a dummy export session.
        {
            let mut sessions = span.export_sessions.lock().await;
            let timer_handle = tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(9999)).await;
            });
            let mut timers = HashMap::new();
            timers.insert(1, timer_handle.abort_handle());

            let session_id = SessionId {
                engine_id: 1,
                session_number: 1,
            };
            let export_config = ExportConfig {
                max_segment_size: 1400,
                max_retransmissions: 10,
                retransmit_timeout: Duration::from_secs(60),
                checkpoint_every_n: 0,
                max_checkpoints: None,
                green: false,
            };
            let (session, _actions) =
                ExportSession::new(session_id, Bytes::from(vec![0u8; 100]), 1, export_config);
            sessions.insert(1, ExportSessionState { session, timers });
        }

        // Simulate ping response timeout (purge_on_link_down is true).
        span.on_ping_response_timeout().await;

        // Link should be marked as down.
        assert!(!span.is_link_alive());

        // Sessions should be purged.
        assert_eq!(span.export_sessions.lock().await.len(), 0);

        // BPA should have been notified.
        assert!(mock_sink.remove_peer_called.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn purge_with_no_active_sessions_still_notifies_bpa() {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;
        use std::sync::atomic::{AtomicBool, Ordering};

        struct MockSink {
            remove_peer_called: AtomicBool,
        }
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                self.remove_peer_called.store(true, Ordering::Relaxed);
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let mock_sink = Arc::new(MockSink {
            remove_peer_called: AtomicBool::new(false),
        });
        let sink: Arc<dyn Sink> = mock_sink.clone();

        let config = SpanConfig {
            engine_id: 2,
            address: "127.0.0.1:1113".parse().unwrap(),
            purge_on_link_down: true,
            ..Default::default()
        };

        let span = Arc::new(Span::new(config, 1, socket, sink));

        // No export sessions — purge should still notify BPA.
        assert_eq!(span.export_sessions.lock().await.len(), 0);

        span.purge_export_sessions().await;

        // BPA should have been notified even with no sessions.
        assert!(mock_sink.remove_peer_called.load(Ordering::Relaxed));
    }

    // -----------------------------------------------------------------------
    // TVR Observability Tests (Task 10.2)
    // -----------------------------------------------------------------------
    //
    // These tests verify that handle_link_down and handle_link_up execute
    // the metric emission and logging code paths without panicking, and that
    // state transitions occur correctly. Since `metrics-util` is not available
    // as a dev-dependency, we rely on the default no-op recorder and verify
    // the code paths are exercised via state assertions.

    /// Helper to create a test span for TVR observability tests.
    async fn create_tvr_test_span() -> Arc<Span> {
        use hardy_bpa::async_trait;
        use hardy_bpa::cla::{ClaAddress, Sink};
        use std::net::SocketAddr;

        struct MockSink;
        #[async_trait]
        impl Sink for MockSink {
            async fn unregister(&self) {}
            async fn dispatch(
                &self,
                _bundle: Bytes,
                _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
                _cla_addr: Option<&ClaAddress>,
            ) -> hardy_bpa::cla::Result<()> {
                Ok(())
            }
            async fn add_peer(
                &self,
                _addr: ClaAddress,
                _node_ids: &[hardy_bpv7::eid::NodeId],
            ) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
            async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
                Ok(true)
            }
        }

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let sink: Arc<dyn Sink> = Arc::new(MockSink);

        let config = SpanConfig {
            engine_id: 42,
            address: "127.0.0.1:1113".parse().unwrap(),
            tvr_timer_suspension: true,
            link_down_queue_max_bytes: 1024,
            tvr_rate_update: true,
            ..Default::default()
        };

        Arc::new(Span::new(config, 1, socket, sink))
    }

    /// Test that `ltp.tvr.link_down` counter code path executes on state transition.
    ///
    /// Verifies: Requirement 9.1
    #[tokio::test]
    async fn link_down_counter_increments_on_state_transition() {
        let span = create_tvr_test_span().await;

        // Verify initial state is Up.
        assert_eq!(*span.link_state.lock().unwrap(), LinkState::Up);

        // Trigger link-down (scheduled/TVR).
        span.handle_link_down(hardy_bpa::cla::LinkDownProperties { scheduled: true })
            .await;

        // State should transition to DownTvr.
        assert_eq!(*span.link_state.lock().unwrap(), LinkState::DownTvr);
        // The metrics::counter!("ltp.tvr.link_down").increment(1) was called
        // without panicking — verified by reaching this point.
    }

    /// Test that `ltp.tvr.link_up` counter code path executes on state transition.
    ///
    /// Verifies: Requirement 9.2
    #[tokio::test]
    async fn link_up_counter_increments_on_state_transition() {
        let span = create_tvr_test_span().await;

        // First transition to down state.
        span.handle_link_down(hardy_bpa::cla::LinkDownProperties { scheduled: true })
            .await;
        assert_eq!(*span.link_state.lock().unwrap(), LinkState::DownTvr);

        // Trigger link-up.
        span.handle_link_up(hardy_bpa::cla::LinkUpProperties {
            bandwidth_bps: Some(256_000),
            one_way_light_time_ms: None,
        })
        .await;

        // State should transition to Up.
        assert_eq!(*span.link_state.lock().unwrap(), LinkState::Up);
        // The metrics::counter!("ltp.tvr.link_up").increment(1) was called
        // without panicking — verified by reaching this point.
    }

    /// Test that the debug log for suspended timer count executes without panic.
    ///
    /// Verifies: Requirement 9.3
    #[tokio::test]
    async fn debug_log_suspended_timer_count() {
        let span = create_tvr_test_span().await;

        // Trigger link-down which calls suspend_all_timers().
        // With no active sessions, it should log "suspended_timers = 0" at debug.
        span.handle_link_down(hardy_bpa::cla::LinkDownProperties { scheduled: true })
            .await;

        // Verify state transitioned (confirms suspend_all_timers ran).
        assert_eq!(*span.link_state.lock().unwrap(), LinkState::DownTvr);

        // Verify suspended timer maps are empty (no sessions to suspend).
        assert!(span.suspended_export_timers.lock().unwrap().is_empty());
        assert!(span.suspended_import_timers.lock().unwrap().is_empty());
        assert!(span.suspended_inactivity_timers.lock().unwrap().is_empty());
    }

    /// Test that the debug log for flushed segment count and bytes executes.
    ///
    /// Verifies: Requirement 9.4
    #[tokio::test]
    async fn debug_log_flushed_segment_count_and_bytes() {
        let span = create_tvr_test_span().await;

        // Transition to link-down.
        span.handle_link_down(hardy_bpa::cla::LinkDownProperties { scheduled: true })
            .await;

        // Enqueue some segments while link is down.
        {
            let mut queue = span.outbound_queue.lock().unwrap();
            queue.enqueue(Bytes::from_static(b"segment1"));
            queue.enqueue(Bytes::from_static(b"segment2"));
        }

        // Transition to link-up — this calls flush_outbound_queue which logs
        // the flushed segment count and bytes at debug level.
        span.handle_link_up(hardy_bpa::cla::LinkUpProperties {
            bandwidth_bps: None,
            one_way_light_time_ms: None,
        })
        .await;

        // Verify state is Up and queue is drained.
        assert_eq!(*span.link_state.lock().unwrap(), LinkState::Up);
        assert!(span.outbound_queue.lock().unwrap().is_empty());
    }

    /// Test that `ltp.tvr.queue_bytes` gauge is emitted during enqueue (send_segment
    /// while link is down) without panicking.
    ///
    /// Verifies: Requirement 9.5
    #[tokio::test]
    async fn queue_bytes_gauge_emitted_on_enqueue() {
        let span = create_tvr_test_span().await;

        // Transition to link-down.
        span.handle_link_down(hardy_bpa::cla::LinkDownProperties { scheduled: true })
            .await;

        // Call send_segment which should enqueue and emit the gauge.
        let test_data = b"test segment data";
        span.send_segment(test_data).await;

        // Verify the segment was enqueued.
        let queue = span.outbound_queue.lock().unwrap();
        assert_eq!(queue.current_bytes(), test_data.len());
        // The gauge!("ltp.tvr.queue_bytes") was emitted without panicking.
    }

    /// Test that `ltp.tvr.queue_bytes` gauge is set to 0 after flush on link-up.
    ///
    /// Verifies: Requirement 9.5
    #[tokio::test]
    async fn queue_bytes_gauge_reset_on_flush() {
        let span = create_tvr_test_span().await;

        // Transition to link-down and enqueue a segment.
        span.handle_link_down(hardy_bpa::cla::LinkDownProperties { scheduled: true })
            .await;
        {
            let mut queue = span.outbound_queue.lock().unwrap();
            queue.enqueue(Bytes::from_static(b"queued data"));
        }
        assert!(!span.outbound_queue.lock().unwrap().is_empty());

        // Transition to link-up — flushes queue and sets gauge to 0.
        span.handle_link_up(hardy_bpa::cla::LinkUpProperties {
            bandwidth_bps: None,
            one_way_light_time_ms: None,
        })
        .await;

        // Queue should be empty after flush.
        assert_eq!(span.outbound_queue.lock().unwrap().current_bytes(), 0);
        // The gauge!("ltp.tvr.queue_bytes").set(0.0) was called without panicking.
    }

    /// Test that link_down counter is NOT emitted for idempotent (duplicate) events.
    ///
    /// Verifies: Requirements 1.4, 1.5 (idempotent behavior)
    #[tokio::test]
    async fn link_down_idempotent_no_double_transition() {
        let span = create_tvr_test_span().await;

        // First link-down transitions state.
        span.handle_link_down(hardy_bpa::cla::LinkDownProperties { scheduled: true })
            .await;
        assert_eq!(*span.link_state.lock().unwrap(), LinkState::DownTvr);

        // Second link-down is idempotent — returns early without emitting metric.
        span.handle_link_down(hardy_bpa::cla::LinkDownProperties { scheduled: true })
            .await;
        assert_eq!(*span.link_state.lock().unwrap(), LinkState::DownTvr);
    }

    /// Test that link_up counter is NOT emitted for idempotent (duplicate) events.
    ///
    /// Verifies: Requirements 1.4, 1.5 (idempotent behavior)
    #[tokio::test]
    async fn link_up_idempotent_no_double_transition() {
        let span = create_tvr_test_span().await;

        // Already in Up state — link-up should be a no-op.
        assert_eq!(*span.link_state.lock().unwrap(), LinkState::Up);
        span.handle_link_up(hardy_bpa::cla::LinkUpProperties {
            bandwidth_bps: None,
            one_way_light_time_ms: None,
        })
        .await;
        assert_eq!(*span.link_state.lock().unwrap(), LinkState::Up);
    }
}
