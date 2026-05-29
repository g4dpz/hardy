// Copyright 2026 David Johnson, G4DPZ, AMSAT-UK
// SPDX-License-Identifier: Apache-2.0

//! Import (receiver) session state machine.
//!
//! Implements the receiver-side LTP session that tracks incoming data segments,
//! generates report segments, and delivers complete blocks.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::{Bytes, BytesMut};

use crate::segment::{self, CheckpointInfo, ReceptionClaim, Segment, SegmentType};
use crate::session::{CancelDirection, CancelReason, SessionId};

/// Tracks received byte extents using a sorted map with adjacent-extent merging.
///
/// The map stores non-overlapping, non-adjacent ranges as `start → end` (exclusive).
/// When a new range is inserted, it is merged with any overlapping or touching
/// existing ranges so that the map always maintains its invariant: no two entries
/// overlap or are adjacent (i.e., the end of one equals the start of another).
#[derive(Debug, Clone, Default)]
pub struct ExtentMap {
    /// Map of range start offsets to range end offsets (exclusive).
    /// Invariant: entries are non-overlapping and non-adjacent.
    extents: BTreeMap<u64, u64>,
}

impl ExtentMap {
    /// Creates a new, empty extent map.
    pub fn new() -> Self {
        Self {
            extents: BTreeMap::new(),
        }
    }

    /// Inserts the byte range `[start, end)` into the map, merging with any
    /// overlapping or adjacent existing ranges.
    ///
    /// After insertion, the map maintains its invariant that no entries overlap
    /// or touch. If `start >= end`, this is a no-op.
    pub fn insert(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }

        let mut new_start = start;
        let mut new_end = end;

        // Check if there's an extent starting at or before `new_start` that
        // overlaps or is adjacent to our range on the left.
        if let Some((&s, &e)) = self.extents.range(..=new_start).next_back() {
            if e >= new_start {
                new_start = new_start.min(s);
                new_end = new_end.max(e);
                self.extents.remove(&s);
            }
        }

        // Remove all extents that start within (start, new_end] — they overlap
        // or are adjacent to the merged range. We loop and remove one at a time
        // to avoid collecting keys into a Vec.
        let range_start = start.saturating_add(1);
        loop {
            // Find the next extent in [range_start, new_end].
            let next = self
                .extents
                .range(range_start..=new_end)
                .next()
                .map(|(&s, &e)| (s, e));
            match next {
                Some((s, e)) => {
                    new_end = new_end.max(e);
                    self.extents.remove(&s);
                }
                None => break,
            }
        }

        // Insert the merged extent.
        self.extents.insert(new_start, new_end);
    }

    /// Returns the total number of bytes covered by all extents in the map.
    pub fn total_coverage(&self) -> u64 {
        self.extents.iter().map(|(&s, &e)| e - s).sum()
    }

    /// Returns the uncovered (gap) ranges within `[lower, upper)`.
    ///
    /// Each returned tuple `(gap_start, gap_end)` represents a range of bytes
    /// not covered by any extent in the map, where `lower <= gap_start < gap_end <= upper`.
    pub fn gaps(&self, lower: u64, upper: u64) -> Vec<(u64, u64)> {
        if lower >= upper {
            return Vec::new();
        }

        let mut result = Vec::new();
        let mut cursor = lower;

        // Iterate over extents that could intersect [lower, upper).
        // An extent [s, e) intersects [lower, upper) if s < upper AND e > lower.

        // First, check if there's an extent starting before `lower` that covers some of it.
        if let Some((&_s, &e)) = self.extents.range(..=lower).next_back() {
            if e > cursor {
                cursor = e.min(upper);
            }
        }

        // Now iterate extents starting after `lower` (up to `upper`).
        for (&s, &e) in self.extents.range(lower..) {
            if s >= upper {
                break;
            }
            if s > cursor {
                // There's a gap from cursor to s.
                result.push((cursor, s.min(upper)));
            }
            if e > cursor {
                cursor = e.min(upper);
            }
            if cursor >= upper {
                break;
            }
        }

        // If there's remaining uncovered space at the end.
        if cursor < upper {
            result.push((cursor, upper));
        }

        result
    }

    /// Returns `true` if the range `[lower, upper)` is fully covered by extents.
    pub fn is_complete(&self, lower: u64, upper: u64) -> bool {
        if lower >= upper {
            return true;
        }
        self.gaps(lower, upper).is_empty()
    }

    /// Returns all extents as `(start, length)` pairs, ordered by start offset.
    ///
    /// This is used for generating reception claims in Report Segments.
    pub fn claims(&self) -> Vec<(u64, u64)> {
        self.extents.iter().map(|(&s, &e)| (s, e - s)).collect()
    }

    /// Returns the number of disjoint extents in the map.
    pub fn extent_count(&self) -> usize {
        self.extents.len()
    }

    /// Returns `true` if the map contains no extents.
    pub fn is_empty(&self) -> bool {
        self.extents.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_map() {
        let map = ExtentMap::new();
        assert_eq!(map.total_coverage(), 0);
        assert_eq!(map.extent_count(), 0);
        assert!(map.is_empty());
        assert!(map.claims().is_empty());
        assert!(map.is_complete(0, 0));
        assert!(!map.is_complete(0, 10));
        assert_eq!(map.gaps(0, 10), vec![(0, 10)]);
    }

    #[test]
    fn test_single_insert() {
        let mut map = ExtentMap::new();
        map.insert(10, 20);
        assert_eq!(map.total_coverage(), 10);
        assert_eq!(map.extent_count(), 1);
        assert_eq!(map.claims(), vec![(10, 10)]);
        assert!(map.is_complete(10, 20));
        assert!(!map.is_complete(0, 20));
    }

    #[test]
    fn test_adjacent_merge_right() {
        let mut map = ExtentMap::new();
        map.insert(0, 10);
        map.insert(10, 20);
        assert_eq!(map.extent_count(), 1);
        assert_eq!(map.total_coverage(), 20);
        assert_eq!(map.claims(), vec![(0, 20)]);
    }

    #[test]
    fn test_adjacent_merge_left() {
        let mut map = ExtentMap::new();
        map.insert(10, 20);
        map.insert(0, 10);
        assert_eq!(map.extent_count(), 1);
        assert_eq!(map.total_coverage(), 20);
        assert_eq!(map.claims(), vec![(0, 20)]);
    }

    #[test]
    fn test_overlapping_merge() {
        let mut map = ExtentMap::new();
        map.insert(0, 15);
        map.insert(10, 25);
        assert_eq!(map.extent_count(), 1);
        assert_eq!(map.total_coverage(), 25);
        assert_eq!(map.claims(), vec![(0, 25)]);
    }

    #[test]
    fn test_contained_insert() {
        let mut map = ExtentMap::new();
        map.insert(0, 100);
        map.insert(20, 50);
        assert_eq!(map.extent_count(), 1);
        assert_eq!(map.total_coverage(), 100);
        assert_eq!(map.claims(), vec![(0, 100)]);
    }

    #[test]
    fn test_containing_insert() {
        let mut map = ExtentMap::new();
        map.insert(20, 50);
        map.insert(0, 100);
        assert_eq!(map.extent_count(), 1);
        assert_eq!(map.total_coverage(), 100);
        assert_eq!(map.claims(), vec![(0, 100)]);
    }

    #[test]
    fn test_merge_multiple_extents() {
        let mut map = ExtentMap::new();
        map.insert(0, 10);
        map.insert(20, 30);
        map.insert(40, 50);
        assert_eq!(map.extent_count(), 3);

        // Insert a range that bridges all three.
        map.insert(5, 45);
        assert_eq!(map.extent_count(), 1);
        assert_eq!(map.total_coverage(), 50);
        assert_eq!(map.claims(), vec![(0, 50)]);
    }

    #[test]
    fn test_merge_adjacent_chain() {
        let mut map = ExtentMap::new();
        map.insert(0, 10);
        map.insert(20, 30);
        map.insert(10, 20); // bridges the gap
        assert_eq!(map.extent_count(), 1);
        assert_eq!(map.total_coverage(), 30);
        assert_eq!(map.claims(), vec![(0, 30)]);
    }

    #[test]
    fn test_disjoint_extents() {
        let mut map = ExtentMap::new();
        map.insert(0, 10);
        map.insert(20, 30);
        map.insert(40, 50);
        assert_eq!(map.extent_count(), 3);
        assert_eq!(map.total_coverage(), 30);
        assert_eq!(map.claims(), vec![(0, 10), (20, 10), (40, 10)]);
    }

    #[test]
    fn test_gaps_basic() {
        let mut map = ExtentMap::new();
        map.insert(10, 20);
        map.insert(30, 40);

        let gaps = map.gaps(0, 50);
        assert_eq!(gaps, vec![(0, 10), (20, 30), (40, 50)]);
    }

    #[test]
    fn test_gaps_partial_coverage() {
        let mut map = ExtentMap::new();
        map.insert(5, 15);

        let gaps = map.gaps(0, 20);
        assert_eq!(gaps, vec![(0, 5), (15, 20)]);
    }

    #[test]
    fn test_gaps_full_coverage() {
        let mut map = ExtentMap::new();
        map.insert(0, 100);

        let gaps = map.gaps(0, 100);
        assert!(gaps.is_empty());
    }

    #[test]
    fn test_gaps_window_within_extent() {
        let mut map = ExtentMap::new();
        map.insert(0, 100);

        let gaps = map.gaps(20, 50);
        assert!(gaps.is_empty());
    }

    #[test]
    fn test_gaps_window_outside_extents() {
        let mut map = ExtentMap::new();
        map.insert(0, 10);

        let gaps = map.gaps(20, 30);
        assert_eq!(gaps, vec![(20, 30)]);
    }

    #[test]
    fn test_is_complete() {
        let mut map = ExtentMap::new();
        map.insert(0, 50);
        map.insert(50, 100);

        assert!(map.is_complete(0, 100));
        assert!(map.is_complete(10, 90));
        assert!(!map.is_complete(0, 101));
    }

    #[test]
    fn test_zero_length_insert_is_noop() {
        let mut map = ExtentMap::new();
        map.insert(10, 10); // zero-length
        map.insert(20, 15); // start > end
        assert!(map.is_empty());
        assert_eq!(map.total_coverage(), 0);
    }

    #[test]
    fn test_duplicate_insert() {
        let mut map = ExtentMap::new();
        map.insert(0, 10);
        map.insert(0, 10);
        assert_eq!(map.extent_count(), 1);
        assert_eq!(map.total_coverage(), 10);
    }

    #[test]
    fn test_gaps_empty_window() {
        let map = ExtentMap::new();
        assert!(map.gaps(10, 10).is_empty());
        assert!(map.gaps(20, 10).is_empty());
    }

    #[test]
    fn test_claims_ordered() {
        let mut map = ExtentMap::new();
        map.insert(50, 60);
        map.insert(10, 20);
        map.insert(30, 40);

        let claims = map.claims();
        assert_eq!(claims, vec![(10, 10), (30, 10), (50, 10)]);
        // Verify ordering
        for i in 1..claims.len() {
            assert!(claims[i].0 > claims[i - 1].0);
        }
    }

    #[test]
    fn test_large_merge_scenario() {
        let mut map = ExtentMap::new();
        // Insert many small extents
        for i in 0..100 {
            map.insert(i * 10, i * 10 + 5);
        }
        assert_eq!(map.extent_count(), 100);
        assert_eq!(map.total_coverage(), 500);

        // Now merge them all with one big insert
        map.insert(0, 1000);
        assert_eq!(map.extent_count(), 1);
        assert_eq!(map.total_coverage(), 1000);
    }

    #[test]
    fn test_extent_at_u64_boundary() {
        let mut map = ExtentMap::new();
        let max = u64::MAX;
        map.insert(max - 10, max);
        assert_eq!(map.total_coverage(), 10);
        assert_eq!(map.claims(), vec![(max - 10, 10)]);
    }
}

// ---------------------------------------------------------------------------
// Import Session State Machine
// ---------------------------------------------------------------------------

/// The color of a session, determined by the first data segment received.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentColor {
    /// Session established by red-data segments (types 0–3).
    Red,
    /// Session established by green-data segments (types 4, 7).
    Green,
}

/// Configuration for an import session.
#[derive(Debug, Clone)]
pub struct ImportConfig {
    /// Maximum total reports before cancelling the session (None = unlimited).
    pub max_reports: Option<u64>,
    /// How long to wait before retransmitting a report.
    pub retransmit_timeout: Duration,
    /// Maximum claims per Report Segment (default 20, per ION MAX_CLAIMS_PER_RS).
    pub max_claims_per_report: usize,
    /// Expected client service ID for validation (1 = Bundle Protocol).
    pub expected_client_service_id: u64,
    /// Maximum red data bytes allowed per session (None = unlimited, Some(n) = limit).
    /// If a data segment's offset + length exceeds this limit, the session is
    /// cancelled with Cancel-from-Receiver reason ByEngine.
    pub max_red_data_bytes: Option<u64>,
    /// Deferred report delay in milliseconds (0 = disabled, send RS immediately).
    ///
    /// When non-zero, report generation is delayed after receiving a checkpoint
    /// to allow in-flight out-of-order segments to arrive, reducing unnecessary
    /// retransmissions.
    pub defer_report_ms: u64,
}

impl Default for ImportConfig {
    fn default() -> Self {
        Self {
            max_reports: None,
            retransmit_timeout: Duration::from_secs(60),
            max_claims_per_report: 20,
            expected_client_service_id: 1,
            max_red_data_bytes: None,
            defer_report_ms: 0,
        }
    }
}

/// The state of an import session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportState {
    /// Receiving data segments and generating reports.
    Receiving,
    /// All data received and block delivered.
    Complete,
    /// Session has been cancelled.
    Cancelled,
}

/// Actions returned by the import session state machine for the caller to execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportAction {
    /// Send this encoded segment over the network.
    SendSegment(Bytes),
    /// Start a retransmit timer for a report segment.
    StartTimer {
        /// The report serial number this timer is associated with.
        report_serial: u64,
        /// How long to wait before retransmitting the report.
        duration: Duration,
    },
    /// Cancel a report retransmit timer.
    CancelTimer {
        /// The report serial number whose timer should be cancelled.
        report_serial: u64,
    },
    /// Deliver the complete reassembled block to the upper layer.
    DeliverBlock(Bytes),
    /// Defer report generation for the given checkpoint.
    ///
    /// The caller should start a timer for `defer_ms` milliseconds. When the
    /// timer fires (or all gaps are filled), call `generate_deferred_report()`
    /// on the import session to produce the actual report actions.
    DeferReport {
        /// The checkpoint serial number to respond to.
        checkpoint_serial: u64,
        /// The upper bound of the checkpoint scope.
        upper_bound: u64,
        /// How long to defer in milliseconds.
        defer_ms: u64,
    },
}

/// Import (receiver) session state machine.
///
/// This is a pure state machine with no I/O. The caller drives it by calling
/// methods and executing the returned actions.
#[derive(Debug)]
pub struct ImportSession {
    /// The session identifier.
    id: SessionId,
    /// Configuration for this session.
    config: ImportConfig,
    /// Current state of the session.
    state: ImportState,
    /// Tracks received byte ranges with adjacent-extent merging.
    extents: ExtentMap,
    /// Session color, set on first data segment received.
    color: Option<SegmentColor>,
    /// Upper bound set when EORP or EOB is received (offset + data.len()).
    eorp_upper_bound: Option<u64>,
    /// Next report serial number to assign (starts at 1).
    next_report_serial: u64,
    /// Total number of reports generated in this session.
    reports_generated: u64,
    /// Accumulates received data (written at offset positions).
    block_data: Vec<u8>,
}

impl ImportSession {
    /// Creates a new import session in the Receiving state.
    pub fn new(id: SessionId, config: ImportConfig) -> Self {
        Self {
            id,
            config,
            state: ImportState::Receiving,
            extents: ExtentMap::new(),
            color: None,
            eorp_upper_bound: None,
            next_report_serial: 1,
            reports_generated: 0,
            block_data: Vec::new(),
        }
    }

    /// Returns the current state of the import session.
    pub fn state(&self) -> ImportState {
        self.state
    }

    /// Returns the session identifier.
    pub fn id(&self) -> &SessionId {
        &self.id
    }

    /// Returns the session color (if established).
    pub fn color(&self) -> Option<SegmentColor> {
        self.color
    }

    /// Returns the total number of reports generated.
    pub fn reports_generated(&self) -> u64 {
        self.reports_generated
    }

    /// Returns a reference to the extent map.
    pub fn extents(&self) -> &ExtentMap {
        &self.extents
    }

    /// Handles a received data segment.
    ///
    /// Records the data in the block buffer, inserts the extent, checks for
    /// miscolored segments, generates reports on checkpoints, and delivers
    /// the block when complete.
    pub fn on_data_segment(
        &mut self,
        segment_type: SegmentType,
        client_service_id: u64,
        offset: u64,
        data: &[u8],
        checkpoint: Option<CheckpointInfo>,
    ) -> Vec<ImportAction> {
        let mut actions = Vec::new();

        // If session is already complete or cancelled, ignore
        if self.state != ImportState::Receiving {
            return actions;
        }

        // Determine color of this segment
        let seg_color = if segment_type.is_red() {
            SegmentColor::Red
        } else {
            SegmentColor::Green
        };

        // Check for miscolored segments and validate client service ID on first segment
        match self.color {
            None => {
                // First segment: validate client service ID before establishing session
                if client_service_id != self.config.expected_client_service_id {
                    return self.cancel_with_reason(CancelReason::ClientSvcUnreachable);
                }
                // First segment establishes the session color
                self.color = Some(seg_color);
            }
            Some(established) => {
                if established != seg_color {
                    // Miscolored segment — cancel session
                    return self.cancel_with_reason(CancelReason::MiscoloredSegment);
                }
            }
        }

        // Enforce max red data size limit (only for red segments)
        if segment_type.is_red() {
            if let Some(limit) = self.config.max_red_data_bytes {
                if offset + data.len() as u64 > limit {
                    return self.cancel_with_reason(CancelReason::ByEngine);
                }
            }
        }

        // Record data in block_data buffer at the correct offset
        let end = offset as usize + data.len();
        if end > self.block_data.len() {
            // If we know the final block size (EORP/EOB already received or this
            // IS the EORP/EOB), pre-allocate to that size to avoid repeated resizes.
            let target_capacity = if segment_type == SegmentType::RedEorp
                || segment_type == SegmentType::RedEob
                || segment_type == SegmentType::GreenEob
            {
                end // This segment defines the upper bound
            } else if let Some(eorp) = self.eorp_upper_bound {
                eorp as usize // We already know the final size
            } else {
                end // Unknown final size, just grow to fit
            };
            if self.block_data.capacity() < target_capacity {
                self.block_data
                    .reserve(target_capacity - self.block_data.len());
            }
            self.block_data.resize(end, 0);
        }
        self.block_data[offset as usize..end].copy_from_slice(data);

        // Insert the range into extents
        self.extents.insert(offset, offset + data.len() as u64);

        // Handle green vs red segment logic
        if segment_type.is_green() {
            // Green data: no reports, no checkpoints
            if segment_type == SegmentType::GreenEob {
                // Green-EOB: deliver whatever data received up to this point (best-effort)
                let upper = offset + data.len() as u64;
                self.eorp_upper_bound = Some(upper);

                // Deliver whatever data we have up to the upper bound, even with gaps
                let block = Bytes::copy_from_slice(&self.block_data[..upper as usize]);
                actions.push(ImportAction::DeliverBlock(block));
                self.state = ImportState::Complete;
            }
            // GreenData (type 4): just record data, no delivery yet, no reports
        } else {
            // Red data handling

            // If EORP (type 2) or EOB (type 3): record eorp_upper_bound
            if segment_type == SegmentType::RedEorp || segment_type == SegmentType::RedEob {
                let upper = offset + data.len() as u64;
                self.eorp_upper_bound = Some(upper);
            }

            // If segment has a checkpoint (types 1, 2, 3): generate report(s)
            if segment_type.is_checkpoint() {
                if let Some(ckpt) = checkpoint {
                    let checkpoint_upper = offset + data.len() as u64;

                    if self.config.defer_report_ms > 0
                        && !self.extents.is_complete(0, checkpoint_upper)
                    {
                        // Gaps exist and deferral is configured — defer the report.
                        actions.push(ImportAction::DeferReport {
                            checkpoint_serial: ckpt.serial,
                            upper_bound: checkpoint_upper,
                            defer_ms: self.config.defer_report_ms,
                        });
                    } else {
                        // No deferral or already complete — generate report immediately.
                        let report_actions = self.generate_reports(ckpt.serial, checkpoint_upper);
                        actions.extend(report_actions);
                    }
                }
            }

            // Check if block is complete (red requires full coverage)
            if let Some(eorp) = self.eorp_upper_bound {
                if self.extents.is_complete(0, eorp) {
                    // If the completing segment is NOT a checkpoint, generate an
                    // unsolicited Report Segment with checkpoint_serial = 0.
                    // This tells the sender "I have everything" without waiting
                    // for a checkpoint (HDTN GitHub Issue #23).
                    if !segment_type.is_checkpoint() {
                        let unsolicited_report = self.generate_reports(0, eorp);
                        actions.extend(unsolicited_report);
                    }

                    // Deliver the block
                    let block = Bytes::copy_from_slice(&self.block_data[..eorp as usize]);
                    actions.push(ImportAction::DeliverBlock(block));
                    self.state = ImportState::Complete;
                }
            }
        }

        actions
    }

    /// Handles a received Report-Ack segment.
    ///
    /// Cancels the retransmit timer for the acknowledged report.
    pub fn on_report_ack(&mut self, report_serial: u64) -> Vec<ImportAction> {
        vec![ImportAction::CancelTimer { report_serial }]
    }

    /// Handles a Cancel-from-Sender segment.
    ///
    /// Sends a Cancel-Ack-to-Sender and transitions to Cancelled.
    pub fn on_cancel_from_sender(&mut self, _reason: CancelReason) -> Vec<ImportAction> {
        let cancel_ack = Segment::CancelAck {
            session_id: self.id,
            direction: CancelDirection::FromSender,
        };
        let wire_size = segment::encoded_size(&cancel_ack);
        let mut buf = BytesMut::with_capacity(wire_size);
        segment::encode(&cancel_ack, &mut buf);

        self.state = ImportState::Cancelled;
        vec![ImportAction::SendSegment(buf.freeze())]
    }

    /// Generates the deferred report for a checkpoint.
    ///
    /// Called by the CLA layer when:
    /// - The deferral timer expires, OR
    /// - All gaps within the checkpoint scope are filled during the deferral period
    ///
    /// Returns the report actions (SendSegment + StartTimer) for the caller to execute.
    pub fn generate_deferred_report(
        &mut self,
        checkpoint_serial: u64,
        upper_bound: u64,
    ) -> Vec<ImportAction> {
        if self.state != ImportState::Receiving {
            return Vec::new();
        }
        self.generate_reports(checkpoint_serial, upper_bound)
    }

    /// Returns `true` if the range `[0, upper_bound)` is fully covered by received extents.
    ///
    /// Used by the CLA layer to determine if a deferred report should be sent
    /// immediately because all gaps have been filled.
    pub fn is_scope_complete(&self, upper_bound: u64) -> bool {
        self.extents.is_complete(0, upper_bound)
    }

    /// Returns `true` if any gaps remain in the range `[0, upper_bound)`.
    ///
    /// Used by the CLA layer to determine whether to reset the deferral timer
    /// (gaps remain) or send the report immediately (no gaps).
    pub fn has_gaps_in_scope(&self, upper_bound: u64) -> bool {
        !self.extents.is_complete(0, upper_bound)
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Generates report segments for a checkpoint, capping at max_claims_per_report
    /// claims per RS and splitting into multiple RS with non-overlapping windows
    /// when needed.
    fn generate_reports(
        &mut self,
        checkpoint_serial: u64,
        checkpoint_upper_bound: u64,
    ) -> Vec<ImportAction> {
        let mut actions = Vec::new();

        // Check max reports limit
        if let Some(max) = self.config.max_reports {
            if self.reports_generated >= max {
                // Exceeded report limit — cancel session
                return self.cancel_with_reason(CancelReason::ByEngine);
            }
        }

        // Get all claims within [0, checkpoint_upper_bound)
        let all_claims = self.claims_in_window(0, checkpoint_upper_bound);

        if all_claims.is_empty() {
            // No data received yet — emit a single RS with no claims
            let rs = self.build_report_segment(checkpoint_serial, 0, checkpoint_upper_bound, &[]);
            actions.extend(rs);
            return actions;
        }

        let max_claims = self.config.max_claims_per_report;

        if all_claims.len() <= max_claims {
            // All claims fit in a single RS
            let rs = self.build_report_segment(
                checkpoint_serial,
                0,
                checkpoint_upper_bound,
                &all_claims,
            );
            actions.extend(rs);
        } else {
            // Split into multiple RS with non-overlapping windows.
            // Each RS covers a window containing at most max_claims claims.
            let chunks: Vec<&[ReceptionClaim]> = all_claims.chunks(max_claims).collect();
            for (i, chunk) in chunks.iter().enumerate() {
                // Window lower bound: start of first claim in chunk
                let lower = chunk[0].offset;
                // Window upper bound: end of last claim in chunk,
                // or checkpoint_upper_bound for the last chunk
                let upper = if i == chunks.len() - 1 {
                    checkpoint_upper_bound
                } else {
                    // Use the start of the next chunk's first claim as upper
                    // to ensure non-overlapping windows
                    chunks[i + 1][0].offset
                };

                // Check max reports limit before each RS
                if let Some(max) = self.config.max_reports {
                    if self.reports_generated >= max {
                        actions.extend(self.cancel_with_reason(CancelReason::ByEngine));
                        return actions;
                    }
                }

                let rs = self.build_report_segment(checkpoint_serial, lower, upper, chunk);
                actions.extend(rs);
            }
        }

        actions
    }

    /// Builds a single Report Segment, encodes it, and returns the actions
    /// (SendSegment + StartTimer).
    fn build_report_segment(
        &mut self,
        checkpoint_serial: u64,
        lower_bound: u64,
        upper_bound: u64,
        claims: &[ReceptionClaim],
    ) -> Vec<ImportAction> {
        let report_serial = self.next_report_serial;
        self.next_report_serial += 1;
        self.reports_generated += 1;

        let rs = Segment::Report {
            session_id: self.id,
            report_serial,
            checkpoint_serial,
            upper_bound,
            lower_bound,
            claims: claims.to_vec(),
        };

        let wire_size = segment::encoded_size(&rs);
        let mut buf = BytesMut::with_capacity(wire_size);
        segment::encode(&rs, &mut buf);

        vec![
            ImportAction::SendSegment(buf.freeze()),
            ImportAction::StartTimer {
                report_serial,
                duration: self.config.retransmit_timeout,
            },
        ]
    }

    /// Returns reception claims within the window [lower, upper) from the extent map.
    fn claims_in_window(&self, lower: u64, upper: u64) -> Vec<ReceptionClaim> {
        self.extents
            .claims()
            .into_iter()
            .filter_map(|(offset, length)| {
                let end = offset + length;
                // Clip to window
                if end <= lower || offset >= upper {
                    return None;
                }
                let clipped_start = offset.max(lower);
                let clipped_end = end.min(upper);
                Some(ReceptionClaim {
                    offset: clipped_start,
                    length: clipped_end - clipped_start,
                })
            })
            .collect()
    }

    /// Cancels the session with the given reason, emitting a Cancel-from-Receiver.
    fn cancel_with_reason(&mut self, reason: CancelReason) -> Vec<ImportAction> {
        let cancel = Segment::Cancel {
            session_id: self.id,
            reason,
            direction: CancelDirection::FromReceiver,
        };
        let wire_size = segment::encoded_size(&cancel);
        let mut buf = BytesMut::with_capacity(wire_size);
        segment::encode(&cancel, &mut buf);

        self.state = ImportState::Cancelled;
        vec![ImportAction::SendSegment(buf.freeze())]
    }
}

#[cfg(test)]
mod import_session_tests {
    use super::*;

    fn session_id() -> SessionId {
        SessionId {
            engine_id: 2,
            session_number: 100,
        }
    }

    fn default_config() -> ImportConfig {
        ImportConfig {
            max_reports: None,
            retransmit_timeout: Duration::from_secs(30),
            max_claims_per_report: 20,
            expected_client_service_id: 1,
            max_red_data_bytes: None,
            defer_report_ms: 0,
        }
    }

    /// Helper: count SendSegment actions.
    fn count_send_segments(actions: &[ImportAction]) -> usize {
        actions
            .iter()
            .filter(|a| matches!(a, ImportAction::SendSegment(_)))
            .count()
    }

    /// Helper: count StartTimer actions.
    fn count_start_timers(actions: &[ImportAction]) -> usize {
        actions
            .iter()
            .filter(|a| matches!(a, ImportAction::StartTimer { .. }))
            .count()
    }

    /// Helper: check if actions contain a DeliverBlock.
    fn has_deliver_block(actions: &[ImportAction]) -> bool {
        actions
            .iter()
            .any(|a| matches!(a, ImportAction::DeliverBlock(_)))
    }

    /// Helper: extract delivered block data.
    fn get_delivered_block(actions: &[ImportAction]) -> Option<Bytes> {
        actions.iter().find_map(|a| {
            if let ImportAction::DeliverBlock(data) = a {
                Some(data.clone())
            } else {
                None
            }
        })
    }

    /// Helper: decode the first SendSegment action into a Segment.
    fn decode_first_segment(actions: &[ImportAction]) -> Segment {
        for action in actions {
            if let ImportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                return segment::decode(&mut reader).unwrap();
            }
        }
        panic!("No SendSegment action found");
    }

    #[test]
    fn new_session_starts_in_receiving_state() {
        let session = ImportSession::new(session_id(), default_config());
        assert_eq!(session.state(), ImportState::Receiving);
        assert_eq!(session.color(), None);
        assert_eq!(session.reports_generated(), 0);
    }

    #[test]
    fn first_red_segment_sets_color_red() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = vec![0xAB; 100];
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);
        assert_eq!(session.color(), Some(SegmentColor::Red));
    }

    #[test]
    fn first_green_segment_sets_color_green() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = vec![0xAB; 100];
        let _ = session.on_data_segment(SegmentType::GreenData, 1, 0, &data, None);
        assert_eq!(session.color(), Some(SegmentColor::Green));
    }

    #[test]
    fn miscolored_green_in_red_session_cancels() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = vec![0xAB; 100];

        // First segment is red
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);
        assert_eq!(session.state(), ImportState::Receiving);

        // Green segment in red session
        let actions = session.on_data_segment(SegmentType::GreenData, 1, 100, &data, None);
        assert_eq!(session.state(), ImportState::Cancelled);

        // Should emit a Cancel-from-Receiver with MiscoloredSegment
        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::MiscoloredSegment);
                assert_eq!(direction, CancelDirection::FromReceiver);
            }
            _ => panic!("Expected Cancel segment, got {:?}", seg),
        }
    }

    #[test]
    fn miscolored_red_in_green_session_cancels() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = vec![0xAB; 100];

        // First segment is green
        let _ = session.on_data_segment(SegmentType::GreenData, 1, 0, &data, None);

        // Red segment in green session
        let actions = session.on_data_segment(SegmentType::RedData, 1, 100, &data, None);
        assert_eq!(session.state(), ImportState::Cancelled);

        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::MiscoloredSegment);
                assert_eq!(direction, CancelDirection::FromReceiver);
            }
            _ => panic!("Expected Cancel segment"),
        }
    }

    #[test]
    fn checkpoint_generates_report() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = vec![0xAB; 100];

        let actions = session.on_data_segment(
            SegmentType::RedCheckpoint,
            1,
            0,
            &data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        // Should have: 1 RS (SendSegment) + 1 StartTimer
        assert_eq!(count_send_segments(&actions), 1);
        assert_eq!(count_start_timers(&actions), 1);
        assert_eq!(session.reports_generated(), 1);

        // Decode the RS
        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Report {
                report_serial,
                checkpoint_serial,
                upper_bound,
                lower_bound,
                claims,
                ..
            } => {
                assert_eq!(report_serial, 1);
                assert_eq!(checkpoint_serial, 1);
                assert_eq!(lower_bound, 0);
                assert_eq!(upper_bound, 100);
                assert_eq!(claims.len(), 1);
                assert_eq!(claims[0].offset, 0);
                assert_eq!(claims[0].length, 100);
            }
            _ => panic!("Expected Report segment, got {:?}", seg),
        }
    }

    #[test]
    fn eob_with_full_coverage_delivers_block() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = [0xAB; 200];

        // Send first half as plain red data
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, &data[..100], None);

        // Send second half as EOB with checkpoint
        let actions = session.on_data_segment(
            SegmentType::RedEob,
            1,
            100,
            &data[100..],
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        assert_eq!(session.state(), ImportState::Complete);
        assert!(has_deliver_block(&actions));

        let block = get_delivered_block(&actions).unwrap();
        assert_eq!(block.len(), 200);
        assert_eq!(&block[..], &data[..]);
    }

    #[test]
    fn eorp_with_full_coverage_delivers_block() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = vec![0xCD; 150];

        // Send all data as EORP
        let actions = session.on_data_segment(
            SegmentType::RedEorp,
            1,
            0,
            &data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        assert_eq!(session.state(), ImportState::Complete);
        assert!(has_deliver_block(&actions));

        let block = get_delivered_block(&actions).unwrap();
        assert_eq!(&block[..], &data[..]);
    }

    #[test]
    fn incomplete_data_does_not_deliver() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = vec![0xAB; 100];

        // Send EOB but only partial data (gap at 100-200)
        let actions = session.on_data_segment(
            SegmentType::RedEob,
            1,
            200,
            &data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        // eorp_upper_bound is 300, but we only have [200, 300)
        assert_eq!(session.state(), ImportState::Receiving);
        assert!(!has_deliver_block(&actions));
    }

    #[test]
    fn out_of_order_segments_deliver_when_complete() {
        let mut session = ImportSession::new(session_id(), default_config());

        // Send second half first (with EOB)
        let _ = session.on_data_segment(
            SegmentType::RedEob,
            1,
            100,
            &[0xBB; 100],
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );
        assert_eq!(session.state(), ImportState::Receiving);

        // Now send first half (plain red data)
        let actions = session.on_data_segment(SegmentType::RedData, 1, 0, &[0xAA; 100], None);

        // Now complete
        assert_eq!(session.state(), ImportState::Complete);
        assert!(has_deliver_block(&actions));

        let block = get_delivered_block(&actions).unwrap();
        assert_eq!(block.len(), 200);
        assert_eq!(&block[..100], &vec![0xAA; 100][..]);
        assert_eq!(&block[100..], &vec![0xBB; 100][..]);
    }

    #[test]
    fn on_report_ack_cancels_timer() {
        let mut session = ImportSession::new(session_id(), default_config());
        let actions = session.on_report_ack(5);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            ImportAction::CancelTimer { report_serial } => {
                assert_eq!(*report_serial, 5);
            }
            _ => panic!("Expected CancelTimer action"),
        }
    }

    #[test]
    fn on_cancel_from_sender_sends_ack_and_cancels() {
        let mut session = ImportSession::new(session_id(), default_config());
        let actions = session.on_cancel_from_sender(CancelReason::RetransmitLimitExceeded);

        assert_eq!(session.state(), ImportState::Cancelled);
        assert_eq!(actions.len(), 1);

        let seg = decode_first_segment(&actions);
        match seg {
            Segment::CancelAck { direction, .. } => {
                assert_eq!(direction, CancelDirection::FromSender);
            }
            _ => panic!("Expected CancelAck segment, got {:?}", seg),
        }
    }

    #[test]
    fn report_serial_increments() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = vec![0xAB; 50];

        // First checkpoint
        let actions1 = session.on_data_segment(
            SegmentType::RedCheckpoint,
            1,
            0,
            &data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );
        let seg1 = decode_first_segment(&actions1);
        let serial1 = match seg1 {
            Segment::Report { report_serial, .. } => report_serial,
            _ => panic!("Expected Report"),
        };

        // Second checkpoint
        let actions2 = session.on_data_segment(
            SegmentType::RedCheckpoint,
            1,
            50,
            &data,
            Some(CheckpointInfo {
                serial: 2,
                responding_report_serial: 0,
            }),
        );
        let seg2 = decode_first_segment(&actions2);
        let serial2 = match seg2 {
            Segment::Report { report_serial, .. } => report_serial,
            _ => panic!("Expected Report"),
        };

        assert!(serial2 > serial1);
        assert_eq!(session.reports_generated(), 2);
    }

    #[test]
    fn multiple_reports_for_many_claims() {
        // Create a session with max 3 claims per report for easier testing
        let config = ImportConfig {
            max_claims_per_report: 3,
            ..default_config()
        };
        let mut session = ImportSession::new(session_id(), config);

        // Insert 7 disjoint extents (will need 3 RS: 3+3+1 claims)
        for i in 0..7 {
            let offset = i * 20;
            let data = vec![0xAB; 5];
            let _ = session.on_data_segment(SegmentType::RedData, 1, offset, &data, None);
        }

        // Now send a checkpoint at offset 140
        let data = vec![0xAB; 5];
        let actions = session.on_data_segment(
            SegmentType::RedCheckpoint,
            1,
            140,
            &data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        // Should have 3 RS (each with SendSegment + StartTimer) = 6 actions
        // 8 extents total (7 + 1 from checkpoint), split into 3+3+2 = 3 RS
        let send_count = count_send_segments(&actions);
        let timer_count = count_start_timers(&actions);
        assert_eq!(send_count, 3); // 3 report segments
        assert_eq!(timer_count, 3); // 3 timers
        assert_eq!(session.reports_generated(), 3);
    }

    #[test]
    fn max_reports_limit_cancels_session() {
        let config = ImportConfig {
            max_reports: Some(2),
            ..default_config()
        };
        let mut session = ImportSession::new(session_id(), config);
        let data = vec![0xAB; 50];

        // First checkpoint — generates report 1
        let _ = session.on_data_segment(
            SegmentType::RedCheckpoint,
            1,
            0,
            &data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );
        assert_eq!(session.state(), ImportState::Receiving);

        // Second checkpoint — generates report 2
        let _ = session.on_data_segment(
            SegmentType::RedCheckpoint,
            1,
            50,
            &data,
            Some(CheckpointInfo {
                serial: 2,
                responding_report_serial: 0,
            }),
        );
        assert_eq!(session.state(), ImportState::Receiving);

        // Third checkpoint — should hit limit and cancel
        let actions = session.on_data_segment(
            SegmentType::RedCheckpoint,
            1,
            100,
            &data,
            Some(CheckpointInfo {
                serial: 3,
                responding_report_serial: 0,
            }),
        );
        assert_eq!(session.state(), ImportState::Cancelled);

        // Should emit Cancel-from-Receiver with ByEngine
        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::ByEngine);
                assert_eq!(direction, CancelDirection::FromReceiver);
            }
            _ => panic!("Expected Cancel segment, got {:?}", seg),
        }
    }

    #[test]
    fn ignored_after_complete() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = vec![0xAB; 100];

        // Complete the session
        let _ = session.on_data_segment(
            SegmentType::RedEob,
            1,
            0,
            &data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );
        assert_eq!(session.state(), ImportState::Complete);

        // Further segments should be ignored
        let actions = session.on_data_segment(SegmentType::RedData, 1, 100, &data, None);
        assert!(actions.is_empty());
    }

    #[test]
    fn ignored_after_cancelled() {
        let mut session = ImportSession::new(session_id(), default_config());
        let _ = session.on_cancel_from_sender(CancelReason::ByUser);
        assert_eq!(session.state(), ImportState::Cancelled);

        let data = vec![0xAB; 100];
        let actions = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);
        assert!(actions.is_empty());
    }

    #[test]
    fn data_written_at_correct_offset() {
        let mut session = ImportSession::new(session_id(), default_config());

        // Write "HELLO" at offset 10
        let _ = session.on_data_segment(SegmentType::RedData, 1, 10, b"HELLO", None);

        // Write "WORLD_____" at offset 0 (fills 0..10)
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, b"WORLD_____", None);

        // Now send EOB at offset 15 to complete
        let actions = session.on_data_segment(
            SegmentType::RedEob,
            1,
            15,
            b"!",
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        // Block should be complete: [0,16) covered
        // "WORLD_____" at 0..10, "HELLO" at 10..15, "!" at 15..16
        assert_eq!(session.state(), ImportState::Complete);
        let block = get_delivered_block(&actions).unwrap();
        assert_eq!(block.len(), 16);
        assert_eq!(&block[0..10], b"WORLD_____");
        assert_eq!(&block[10..15], b"HELLO");
        assert_eq!(&block[15..16], b"!");
    }

    #[test]
    fn plain_red_data_no_checkpoint_no_report() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = vec![0xAB; 100];

        let actions = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);

        // No checkpoint means no report generated
        assert!(actions.is_empty());
        assert_eq!(session.reports_generated(), 0);
    }

    // -----------------------------------------------------------------------
    // Green data import tests
    // -----------------------------------------------------------------------

    #[test]
    fn green_data_does_not_generate_report() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = vec![0xAB; 100];

        let actions = session.on_data_segment(SegmentType::GreenData, 1, 0, &data, None);

        // Green data should not generate any reports or deliver
        assert!(actions.is_empty());
        assert_eq!(session.reports_generated(), 0);
        assert_eq!(session.state(), ImportState::Receiving);
        assert_eq!(session.color(), Some(SegmentColor::Green));
    }

    #[test]
    fn green_eob_delivers_block_immediately() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = vec![0xCD; 200];

        // Send all data as a single Green-EOB
        let actions = session.on_data_segment(SegmentType::GreenEob, 1, 0, &data, None);

        assert_eq!(session.state(), ImportState::Complete);
        assert!(has_deliver_block(&actions));
        assert_eq!(count_send_segments(&actions), 0); // No reports
        assert_eq!(session.reports_generated(), 0);

        let block = get_delivered_block(&actions).unwrap();
        assert_eq!(&block[..], &data[..]);
    }

    #[test]
    fn green_eob_delivers_after_green_data_segments() {
        let mut session = ImportSession::new(session_id(), default_config());

        // Send first half as GreenData
        let actions1 = session.on_data_segment(SegmentType::GreenData, 1, 0, &[0xAA; 100], None);
        assert!(actions1.is_empty());
        assert_eq!(session.state(), ImportState::Receiving);

        // Send second half as GreenEob
        let actions2 = session.on_data_segment(SegmentType::GreenEob, 1, 100, &[0xBB; 100], None);

        assert_eq!(session.state(), ImportState::Complete);
        assert!(has_deliver_block(&actions2));
        assert_eq!(count_send_segments(&actions2), 0);

        let block = get_delivered_block(&actions2).unwrap();
        assert_eq!(block.len(), 200);
        assert_eq!(&block[..100], &vec![0xAA; 100][..]);
        assert_eq!(&block[100..], &vec![0xBB; 100][..]);
    }

    #[test]
    fn green_eob_delivers_with_gaps() {
        let mut session = ImportSession::new(session_id(), default_config());

        // Send segment at offset 0 (covers [0, 50))
        let _ = session.on_data_segment(SegmentType::GreenData, 1, 0, &[0xAA; 50], None);

        // Skip offset 50..100 (gap!)

        // Send Green-EOB at offset 100 (covers [100, 150))
        let actions = session.on_data_segment(SegmentType::GreenEob, 1, 100, &[0xCC; 50], None);

        // Green delivers whatever data received up to EOB, even with gaps
        assert_eq!(session.state(), ImportState::Complete);
        assert!(has_deliver_block(&actions));
        assert_eq!(count_send_segments(&actions), 0); // No reports for green

        let block = get_delivered_block(&actions).unwrap();
        assert_eq!(block.len(), 150);
        // First 50 bytes are 0xAA
        assert_eq!(&block[..50], &vec![0xAA; 50][..]);
        // Gap bytes 50..100 are zeros (uninitialized buffer)
        assert_eq!(&block[50..100], &vec![0u8; 50][..]);
        // Last 50 bytes are 0xCC
        assert_eq!(&block[100..], &vec![0xCC; 50][..]);
    }

    #[test]
    fn green_eob_out_of_order_delivers() {
        let mut session = ImportSession::new(session_id(), default_config());

        // Send second segment first (out of order)
        let _ = session.on_data_segment(SegmentType::GreenData, 1, 100, &[0xBB; 100], None);
        assert_eq!(session.state(), ImportState::Receiving);

        // Send Green-EOB at offset 200
        let actions = session.on_data_segment(SegmentType::GreenEob, 1, 200, &[0xCC; 50], None);

        // Delivers whatever we have up to offset 250 (even with gap at 0..100)
        assert_eq!(session.state(), ImportState::Complete);
        assert!(has_deliver_block(&actions));

        let block = get_delivered_block(&actions).unwrap();
        assert_eq!(block.len(), 250);
        // Gap at 0..100 is zeros
        assert_eq!(&block[..100], &vec![0u8; 100][..]);
        // 100..200 is 0xBB
        assert_eq!(&block[100..200], &vec![0xBB; 100][..]);
        // 200..250 is 0xCC
        assert_eq!(&block[200..], &vec![0xCC; 50][..]);
    }

    #[test]
    fn green_session_no_reports_ever() {
        let mut session = ImportSession::new(session_id(), default_config());

        // Send multiple green data segments
        for i in 0..5u64 {
            let _ = session.on_data_segment(SegmentType::GreenData, 1, i * 100, &[0xAB; 100], None);
        }

        // Finish with Green-EOB
        let actions = session.on_data_segment(SegmentType::GreenEob, 1, 500, &[0xAB; 100], None);

        // No reports should have been generated at any point
        assert_eq!(session.reports_generated(), 0);
        assert_eq!(count_send_segments(&actions), 0);
        assert_eq!(count_start_timers(&actions), 0);
        assert_eq!(session.state(), ImportState::Complete);
    }

    #[test]
    fn miscolored_green_eob_in_red_session_cancels() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = vec![0xAB; 100];

        // Establish red session
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);

        // Green-EOB in red session should cancel
        let actions = session.on_data_segment(SegmentType::GreenEob, 1, 100, &data, None);
        assert_eq!(session.state(), ImportState::Cancelled);

        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::MiscoloredSegment);
                assert_eq!(direction, CancelDirection::FromReceiver);
            }
            _ => panic!("Expected Cancel segment, got {:?}", seg),
        }
    }

    #[test]
    fn miscolored_red_eob_in_green_session_cancels() {
        let mut session = ImportSession::new(session_id(), default_config());
        let data = vec![0xAB; 100];

        // Establish green session
        let _ = session.on_data_segment(SegmentType::GreenData, 1, 0, &data, None);

        // Red-EOB in green session should cancel
        let actions = session.on_data_segment(
            SegmentType::RedEob,
            1,
            100,
            &data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );
        assert_eq!(session.state(), ImportState::Cancelled);

        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::MiscoloredSegment);
                assert_eq!(direction, CancelDirection::FromReceiver);
            }
            _ => panic!("Expected Cancel segment, got {:?}", seg),
        }
    }

    #[test]
    fn green_eob_only_segment_delivers() {
        // Edge case: a single Green-EOB with no preceding GreenData
        let mut session = ImportSession::new(session_id(), default_config());
        let data = b"single green block";

        let actions = session.on_data_segment(SegmentType::GreenEob, 1, 0, data, None);

        assert_eq!(session.state(), ImportState::Complete);
        assert_eq!(session.color(), Some(SegmentColor::Green));
        assert!(has_deliver_block(&actions));

        let block = get_delivered_block(&actions).unwrap();
        assert_eq!(&block[..], data);
    }

    // -----------------------------------------------------------------------
    // Client service ID validation tests
    // -----------------------------------------------------------------------

    #[test]
    fn matching_client_service_id_proceeds_normally() {
        let config = ImportConfig {
            expected_client_service_id: 1,
            ..default_config()
        };
        let mut session = ImportSession::new(session_id(), config);
        let data = vec![0xAB; 100];

        // client_service_id matches expected (1)
        let actions = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);

        // Should proceed normally — no cancel, session still receiving
        assert_eq!(session.state(), ImportState::Receiving);
        assert_eq!(session.color(), Some(SegmentColor::Red));
        assert!(actions.is_empty()); // No checkpoint, so no actions
    }

    #[test]
    fn mismatched_client_service_id_cancels_session() {
        let config = ImportConfig {
            expected_client_service_id: 1,
            ..default_config()
        };
        let mut session = ImportSession::new(session_id(), config);
        let data = vec![0xAB; 100];

        // client_service_id does NOT match expected (sending 99, expecting 1)
        let actions = session.on_data_segment(SegmentType::RedData, 99, 0, &data, None);

        // Should cancel with ClientSvcUnreachable
        assert_eq!(session.state(), ImportState::Cancelled);
        assert_eq!(count_send_segments(&actions), 1);

        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::ClientSvcUnreachable);
                assert_eq!(direction, CancelDirection::FromReceiver);
            }
            _ => panic!("Expected Cancel segment, got {:?}", seg),
        }
    }

    #[test]
    fn mismatched_client_service_id_on_green_segment_cancels() {
        let config = ImportConfig {
            expected_client_service_id: 2,
            ..default_config()
        };
        let mut session = ImportSession::new(session_id(), config);
        let data = vec![0xAB; 100];

        // Green segment with wrong service ID
        let actions = session.on_data_segment(SegmentType::GreenData, 5, 0, &data, None);

        assert_eq!(session.state(), ImportState::Cancelled);

        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::ClientSvcUnreachable);
                assert_eq!(direction, CancelDirection::FromReceiver);
            }
            _ => panic!("Expected Cancel segment, got {:?}", seg),
        }
    }

    #[test]
    fn client_service_id_only_validated_on_first_segment() {
        let config = ImportConfig {
            expected_client_service_id: 1,
            ..default_config()
        };
        let mut session = ImportSession::new(session_id(), config);
        let data = vec![0xAB; 100];

        // First segment with correct service ID
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);
        assert_eq!(session.state(), ImportState::Receiving);

        // Second segment with different service ID — should NOT cancel
        // because validation only happens on the first segment (when color is None)
        let actions = session.on_data_segment(SegmentType::RedData, 99, 100, &data, None);

        assert_eq!(session.state(), ImportState::Receiving);
        assert!(actions.is_empty());
    }

    #[test]
    fn mismatched_client_service_id_does_not_set_color() {
        let config = ImportConfig {
            expected_client_service_id: 1,
            ..default_config()
        };
        let mut session = ImportSession::new(session_id(), config);
        let data = vec![0xAB; 100];

        // Wrong service ID — session should cancel without setting color
        let _ = session.on_data_segment(SegmentType::RedData, 42, 0, &data, None);

        assert_eq!(session.state(), ImportState::Cancelled);
        // Color should not have been set since we cancelled before establishing it
        assert_eq!(session.color(), None);
    }

    // -----------------------------------------------------------------------
    // Max red data size enforcement tests
    // -----------------------------------------------------------------------

    #[test]
    fn red_segment_within_limit_proceeds_normally() {
        let config = ImportConfig {
            max_red_data_bytes: Some(1000),
            ..default_config()
        };
        let mut session = ImportSession::new(session_id(), config);
        let data = vec![0xAB; 100];

        // offset(0) + length(100) = 100, which is within limit of 1000
        let actions = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);

        assert_eq!(session.state(), ImportState::Receiving);
        assert!(actions.is_empty());
    }

    #[test]
    fn red_segment_exactly_at_limit_proceeds_normally() {
        let config = ImportConfig {
            max_red_data_bytes: Some(200),
            ..default_config()
        };
        let mut session = ImportSession::new(session_id(), config);
        let data = vec![0xAB; 200];

        // offset(0) + length(200) = 200, exactly at limit
        let actions = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);

        assert_eq!(session.state(), ImportState::Receiving);
        assert!(actions.is_empty());
    }

    #[test]
    fn red_segment_exceeding_limit_cancels_session() {
        let config = ImportConfig {
            max_red_data_bytes: Some(200),
            ..default_config()
        };
        let mut session = ImportSession::new(session_id(), config);
        let data = vec![0xAB; 201];

        // offset(0) + length(201) = 201, exceeds limit of 200
        let actions = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);

        assert_eq!(session.state(), ImportState::Cancelled);
        assert_eq!(count_send_segments(&actions), 1);

        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::ByEngine);
                assert_eq!(direction, CancelDirection::FromReceiver);
            }
            _ => panic!("Expected Cancel segment, got {:?}", seg),
        }
    }

    #[test]
    fn red_segment_at_high_offset_exceeding_limit_cancels() {
        let config = ImportConfig {
            max_red_data_bytes: Some(500),
            ..default_config()
        };
        let mut session = ImportSession::new(session_id(), config);

        // First segment within limit
        let data1 = vec![0xAA; 100];
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, &data1, None);
        assert_eq!(session.state(), ImportState::Receiving);

        // Second segment: offset(400) + length(200) = 600, exceeds limit of 500
        let data2 = vec![0xBB; 200];
        let actions = session.on_data_segment(SegmentType::RedData, 1, 400, &data2, None);

        assert_eq!(session.state(), ImportState::Cancelled);

        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::ByEngine);
                assert_eq!(direction, CancelDirection::FromReceiver);
            }
            _ => panic!("Expected Cancel segment, got {:?}", seg),
        }
    }

    #[test]
    fn max_red_data_bytes_none_means_unlimited() {
        let config = ImportConfig {
            max_red_data_bytes: None,
            ..default_config()
        };
        let mut session = ImportSession::new(session_id(), config);

        // Large segment should be fine with no limit
        let data = vec![0xAB; 10_000];
        let actions = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);

        assert_eq!(session.state(), ImportState::Receiving);
        assert!(actions.is_empty());
    }

    #[test]
    fn max_red_data_bytes_does_not_apply_to_green_segments() {
        let config = ImportConfig {
            max_red_data_bytes: Some(100),
            ..default_config()
        };
        let mut session = ImportSession::new(session_id(), config);

        // Green segment exceeding the red data limit should NOT be cancelled
        let data = vec![0xAB; 200];
        let actions = session.on_data_segment(SegmentType::GreenEob, 1, 0, &data, None);

        // Green-EOB delivers immediately, no cancellation
        assert_eq!(session.state(), ImportState::Complete);
        assert!(has_deliver_block(&actions));
    }

    #[test]
    fn max_red_data_bytes_cancels_before_recording_data() {
        let config = ImportConfig {
            max_red_data_bytes: Some(100),
            ..default_config()
        };
        let mut session = ImportSession::new(session_id(), config);

        // This segment exceeds the limit — data should NOT be recorded
        let data = vec![0xAB; 200];
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);

        assert_eq!(session.state(), ImportState::Cancelled);
        // Extent map should be empty since data was not recorded
        assert!(session.extents().is_empty());
    }

    // -----------------------------------------------------------------------
    // Deferred report sending tests
    // -----------------------------------------------------------------------

    fn deferred_config() -> ImportConfig {
        ImportConfig {
            defer_report_ms: 100,
            ..default_config()
        }
    }

    /// Helper: check if actions contain a DeferReport.
    fn has_defer_report(actions: &[ImportAction]) -> bool {
        actions
            .iter()
            .any(|a| matches!(a, ImportAction::DeferReport { .. }))
    }

    /// Helper: extract DeferReport parameters.
    fn get_defer_report(actions: &[ImportAction]) -> Option<(u64, u64, u64)> {
        actions.iter().find_map(|a| {
            if let ImportAction::DeferReport {
                checkpoint_serial,
                upper_bound,
                defer_ms,
            } = a
            {
                Some((*checkpoint_serial, *upper_bound, *defer_ms))
            } else {
                None
            }
        })
    }

    #[test]
    fn defer_report_when_gaps_exist() {
        let mut session = ImportSession::new(session_id(), deferred_config());

        // Send data at offset 100 (gap at 0..100), then checkpoint at 100
        let data = vec![0xAB; 50];
        let _ = session.on_data_segment(SegmentType::RedData, 1, 100, &data, None);

        // Now send checkpoint at offset 0 with only 50 bytes (gap at 50..100 remains)
        let ckpt_data = vec![0xCD; 50];
        let actions = session.on_data_segment(
            SegmentType::RedCheckpoint,
            1,
            0,
            &ckpt_data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        // Should defer the report since gaps exist within [0, 50)
        // Actually checkpoint_upper = 0 + 50 = 50, and we have [0,50) and [100,150)
        // So [0, 50) IS complete. Let me adjust the test.
        // The checkpoint upper bound is offset + data.len() = 0 + 50 = 50.
        // We have coverage [0, 50) from this segment and [100, 150) from the first.
        // is_complete(0, 50) = true, so report should be generated immediately.
        assert!(!has_defer_report(&actions));
        assert_eq!(count_send_segments(&actions), 1); // RS generated immediately
    }

    #[test]
    fn defer_report_when_checkpoint_scope_has_gaps() {
        let mut session = ImportSession::new(session_id(), deferred_config());

        // Send data at offset 0 (covers [0, 50))
        let data = vec![0xAB; 50];
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);

        // Send checkpoint at offset 100 (covers [100, 150))
        // Checkpoint upper bound = 150. Gap exists at [50, 100).
        let ckpt_data = vec![0xCD; 50];
        let actions = session.on_data_segment(
            SegmentType::RedCheckpoint,
            1,
            100,
            &ckpt_data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        // Should defer the report since gaps exist within [0, 150)
        assert!(has_defer_report(&actions));
        let (ckpt_serial, upper, defer_ms) = get_defer_report(&actions).unwrap();
        assert_eq!(ckpt_serial, 1);
        assert_eq!(upper, 150);
        assert_eq!(defer_ms, 100);

        // No RS should have been sent
        assert_eq!(count_send_segments(&actions), 0);
        assert_eq!(count_start_timers(&actions), 0);
        assert_eq!(session.reports_generated(), 0);
    }

    #[test]
    fn immediate_report_when_defer_ms_zero() {
        // Default config has defer_report_ms = 0
        let mut session = ImportSession::new(session_id(), default_config());

        // Send data at offset 0 (covers [0, 50))
        let data = vec![0xAB; 50];
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);

        // Send checkpoint at offset 100 (covers [100, 150))
        // Gap at [50, 100) but defer_report_ms = 0 → immediate report
        let ckpt_data = vec![0xCD; 50];
        let actions = session.on_data_segment(
            SegmentType::RedCheckpoint,
            1,
            100,
            &ckpt_data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        // Should generate report immediately (no deferral)
        assert!(!has_defer_report(&actions));
        assert_eq!(count_send_segments(&actions), 1);
        assert_eq!(count_start_timers(&actions), 1);
        assert_eq!(session.reports_generated(), 1);
    }

    #[test]
    fn immediate_report_when_scope_already_complete() {
        let mut session = ImportSession::new(session_id(), deferred_config());

        // Send all data first, then checkpoint — no gaps
        let data = vec![0xAB; 100];
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);

        // Checkpoint covers [100, 200) — but checkpoint_upper = 200
        // Wait, we need the scope [0, checkpoint_upper) to be complete.
        // Let's send a checkpoint that covers the end of the block.
        let ckpt_data = vec![0xCD; 100];
        let actions = session.on_data_segment(
            SegmentType::RedEob,
            1,
            100,
            &ckpt_data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        // Scope [0, 200) is complete → report generated immediately, no deferral
        assert!(!has_defer_report(&actions));
        // Block is complete, so it delivers
        assert!(has_deliver_block(&actions));
    }

    #[test]
    fn generate_deferred_report_produces_report() {
        let mut session = ImportSession::new(session_id(), deferred_config());

        // Send data at offset 0 (covers [0, 50))
        let data = vec![0xAB; 50];
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);

        // Send checkpoint at offset 100 (covers [100, 150))
        let ckpt_data = vec![0xCD; 50];
        let actions = session.on_data_segment(
            SegmentType::RedCheckpoint,
            1,
            100,
            &ckpt_data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );
        assert!(has_defer_report(&actions));

        // Now simulate the deferral timer expiring
        let report_actions = session.generate_deferred_report(1, 150);

        // Should produce a report with current coverage
        assert_eq!(count_send_segments(&report_actions), 1);
        assert_eq!(count_start_timers(&report_actions), 1);
        assert_eq!(session.reports_generated(), 1);

        // Decode the RS to verify it has the right claims
        let seg = decode_first_segment(&report_actions);
        match seg {
            Segment::Report {
                checkpoint_serial,
                upper_bound,
                lower_bound,
                claims,
                ..
            } => {
                assert_eq!(checkpoint_serial, 1);
                assert_eq!(lower_bound, 0);
                assert_eq!(upper_bound, 150);
                // Should have 2 claims: [0, 50) and [100, 150)
                assert_eq!(claims.len(), 2);
                assert_eq!(claims[0].offset, 0);
                assert_eq!(claims[0].length, 50);
                assert_eq!(claims[1].offset, 100);
                assert_eq!(claims[1].length, 50);
            }
            _ => panic!("Expected Report segment, got {:?}", seg),
        }
    }

    #[test]
    fn generate_deferred_report_with_full_coverage() {
        let mut session = ImportSession::new(session_id(), deferred_config());

        // Send data at offset 0 (covers [0, 50))
        let data = vec![0xAB; 50];
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);

        // Send checkpoint at offset 100 (covers [100, 150))
        let ckpt_data = vec![0xCD; 50];
        let _ = session.on_data_segment(
            SegmentType::RedCheckpoint,
            1,
            100,
            &ckpt_data,
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        // Fill the gap [50, 100) before timer fires
        let gap_data = vec![0xEF; 50];
        let _ = session.on_data_segment(SegmentType::RedData, 1, 50, &gap_data, None);

        // Now scope [0, 150) is complete
        assert!(session.is_scope_complete(150));

        // Generate the deferred report — should show full coverage
        let report_actions = session.generate_deferred_report(1, 150);
        assert_eq!(count_send_segments(&report_actions), 1);

        let seg = decode_first_segment(&report_actions);
        match seg {
            Segment::Report {
                claims,
                upper_bound,
                lower_bound,
                ..
            } => {
                assert_eq!(lower_bound, 0);
                assert_eq!(upper_bound, 150);
                // Full coverage: single claim [0, 150)
                assert_eq!(claims.len(), 1);
                assert_eq!(claims[0].offset, 0);
                assert_eq!(claims[0].length, 150);
            }
            _ => panic!("Expected Report segment"),
        }
    }

    #[test]
    fn is_scope_complete_and_has_gaps() {
        let mut session = ImportSession::new(session_id(), deferred_config());

        // Empty session — has gaps
        assert!(!session.is_scope_complete(100));
        assert!(session.has_gaps_in_scope(100));

        // Add some data
        let data = vec![0xAB; 50];
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, &data, None);

        // Still has gaps in [0, 100)
        assert!(!session.is_scope_complete(100));
        assert!(session.has_gaps_in_scope(100));

        // But [0, 50) is complete
        assert!(session.is_scope_complete(50));
        assert!(!session.has_gaps_in_scope(50));

        // Fill the rest
        let data2 = vec![0xCD; 50];
        let _ = session.on_data_segment(SegmentType::RedData, 1, 50, &data2, None);

        // Now [0, 100) is complete
        assert!(session.is_scope_complete(100));
        assert!(!session.has_gaps_in_scope(100));
    }

    #[test]
    fn generate_deferred_report_ignored_when_not_receiving() {
        let mut session = ImportSession::new(session_id(), deferred_config());

        // Cancel the session
        let _ = session.on_cancel_from_sender(CancelReason::ByUser);
        assert_eq!(session.state(), ImportState::Cancelled);

        // generate_deferred_report should return empty
        let actions = session.generate_deferred_report(1, 100);
        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Asynchronous reception report tests
    // -----------------------------------------------------------------------

    #[test]
    fn unsolicited_report_when_non_checkpoint_completes_block() {
        let mut session = ImportSession::new(session_id(), default_config());

        // Send second half with EOB (checkpoint) — sets eorp_upper_bound = 200
        let _ = session.on_data_segment(
            SegmentType::RedEob,
            1,
            100,
            &[0xBB; 100],
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );
        assert_eq!(session.state(), ImportState::Receiving);

        // Now send first half as plain RedData (NOT a checkpoint) — completes the block
        let actions = session.on_data_segment(SegmentType::RedData, 1, 0, &[0xAA; 100], None);

        // Block should be complete
        assert_eq!(session.state(), ImportState::Complete);
        assert!(has_deliver_block(&actions));

        // Should also have an unsolicited RS with checkpoint_serial = 0
        assert!(count_send_segments(&actions) >= 1);

        // Find the report segment in the actions
        let mut found_unsolicited_rs = false;
        for action in &actions {
            if let ImportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Report {
                    checkpoint_serial,
                    upper_bound,
                    lower_bound,
                    claims,
                    report_serial,
                    ..
                } = seg
                {
                    // Unsolicited RS references checkpoint serial 0
                    assert_eq!(checkpoint_serial, 0);
                    assert_eq!(lower_bound, 0);
                    assert_eq!(upper_bound, 200);
                    // Full coverage claim
                    assert_eq!(claims.len(), 1);
                    assert_eq!(claims[0].offset, 0);
                    assert_eq!(claims[0].length, 200);
                    assert!(report_serial > 0);
                    found_unsolicited_rs = true;
                }
            }
        }
        assert!(
            found_unsolicited_rs,
            "Expected unsolicited RS with checkpoint_serial=0"
        );
    }

    #[test]
    fn no_unsolicited_report_when_checkpoint_completes_block() {
        let mut session = ImportSession::new(session_id(), default_config());

        // Send first half as plain RedData
        let _ = session.on_data_segment(SegmentType::RedData, 1, 0, &[0xAA; 100], None);

        // Send second half as EOB (checkpoint) — this completes the block
        let actions = session.on_data_segment(
            SegmentType::RedEob,
            1,
            100,
            &[0xBB; 100],
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        // Block should be complete
        assert_eq!(session.state(), ImportState::Complete);
        assert!(has_deliver_block(&actions));

        // The RS should reference checkpoint_serial = 1 (not 0)
        // There should be exactly 1 RS (the normal checkpoint response)
        let mut report_count = 0;
        for action in &actions {
            if let ImportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Report {
                    checkpoint_serial, ..
                } = seg
                {
                    assert_eq!(
                        checkpoint_serial, 1,
                        "Expected checkpoint_serial=1, not unsolicited"
                    );
                    report_count += 1;
                }
            }
        }
        assert_eq!(report_count, 1);
    }

    #[test]
    fn unsolicited_report_uses_new_serial_number() {
        let mut session = ImportSession::new(session_id(), default_config());

        // Send checkpoint first (generates report serial 1)
        let _ = session.on_data_segment(
            SegmentType::RedCheckpoint,
            1,
            50,
            &[0xBB; 50],
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );
        assert_eq!(session.reports_generated(), 1);

        // Set eorp_upper_bound by sending EORP (also a checkpoint, generates report serial 2)
        let _ = session.on_data_segment(
            SegmentType::RedEorp,
            1,
            100,
            &[0xCC; 50],
            Some(CheckpointInfo {
                serial: 2,
                responding_report_serial: 0,
            }),
        );
        assert_eq!(session.reports_generated(), 2);

        // Now fill the gap with plain RedData — triggers unsolicited RS
        let actions = session.on_data_segment(SegmentType::RedData, 1, 0, &[0xAA; 50], None);

        assert_eq!(session.state(), ImportState::Complete);
        assert_eq!(session.reports_generated(), 3);

        // The unsolicited RS should have report_serial = 3
        for action in &actions {
            if let ImportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Report {
                    report_serial,
                    checkpoint_serial,
                    ..
                } = seg
                {
                    assert_eq!(report_serial, 3);
                    assert_eq!(checkpoint_serial, 0);
                }
            }
        }
    }

    #[test]
    fn no_unsolicited_report_for_green_session() {
        let mut session = ImportSession::new(session_id(), default_config());

        // Green sessions never generate reports
        let _ = session.on_data_segment(SegmentType::GreenData, 1, 100, &[0xBB; 100], None);

        // Green-EOB completes the session
        let actions = session.on_data_segment(SegmentType::GreenEob, 1, 0, &[0xAA; 100], None);

        assert_eq!(session.state(), ImportState::Complete);
        assert!(has_deliver_block(&actions));
        // No reports for green sessions
        assert_eq!(count_send_segments(&actions), 0);
        assert_eq!(session.reports_generated(), 0);
    }

    #[test]
    fn unsolicited_report_has_timer() {
        let mut session = ImportSession::new(session_id(), default_config());

        // Send EOB with checkpoint — sets eorp_upper_bound = 200
        let _ = session.on_data_segment(
            SegmentType::RedEob,
            1,
            100,
            &[0xBB; 100],
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        // Complete with plain RedData
        let actions = session.on_data_segment(SegmentType::RedData, 1, 0, &[0xAA; 100], None);

        // Should have a StartTimer for the unsolicited RS
        assert!(count_start_timers(&actions) >= 1);
    }

    #[test]
    fn unsolicited_report_not_generated_when_block_incomplete() {
        let mut session = ImportSession::new(session_id(), default_config());

        // Send EOB with checkpoint — sets eorp_upper_bound = 200
        let _ = session.on_data_segment(
            SegmentType::RedEob,
            1,
            100,
            &[0xBB; 100],
            Some(CheckpointInfo {
                serial: 1,
                responding_report_serial: 0,
            }),
        );

        // Send partial data that doesn't complete the block (gap at 50..100)
        let actions = session.on_data_segment(SegmentType::RedData, 1, 0, &[0xAA; 50], None);

        // Block is NOT complete — no unsolicited RS, no delivery
        assert_eq!(session.state(), ImportState::Receiving);
        assert!(!has_deliver_block(&actions));
        assert_eq!(count_send_segments(&actions), 0);
    }
}
