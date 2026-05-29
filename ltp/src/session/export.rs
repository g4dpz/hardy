// Copyright 2026 David Johnson, G4DPZ, AMSAT-UK
// SPDX-License-Identifier: Apache-2.0

//! Export (sender) session state machine.
//!
//! Implements the sender-side LTP session that segments a block into data
//! segments, handles report acknowledgements, and manages retransmission.

use std::collections::HashSet;
use std::time::Duration;

use bytes::{Bytes, BytesMut};

use super::ExtentMap;
use crate::segment::{self, CheckpointInfo, ReceptionClaim, Segment, SegmentType};
use crate::session::{CancelDirection, CancelReason, SessionId};

/// Configuration for an export session.
#[derive(Debug, Clone)]
pub struct ExportConfig {
    /// Maximum data payload size per segment (bytes). Must be > 0.
    pub max_segment_size: usize,
    /// Maximum number of retransmission attempts before cancelling.
    pub max_retransmissions: u32,
    /// Duration to wait for a report before retransmitting.
    pub retransmit_timeout: Duration,
    /// If non-zero, mark every Nth red-data segment as an intermediate
    /// checkpoint (type 1). The final segment is always EORP/EOB regardless.
    pub checkpoint_every_n: u32,
    /// Optional limit on total checkpoints the session may send.
    pub max_checkpoints: Option<u64>,
    /// If true, transmit as green (best-effort) data with no acknowledgement.
    /// Green sessions use SegmentType::GreenData / GreenEob and complete
    /// immediately after all segments are transmitted.
    pub green: bool,
}

/// Actions returned by the export session state machine for the caller to execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportAction {
    /// Send this encoded segment over the network.
    SendSegment(Bytes),
    /// Start a retransmission timer for the given checkpoint serial number.
    StartTimer {
        /// The checkpoint serial number this timer is associated with.
        checkpoint_serial: u64,
        /// How long to wait before considering the checkpoint unacknowledged.
        duration: Duration,
    },
    /// Suspend a running timer, recording its remaining duration.
    SuspendTimer {
        /// The checkpoint serial number whose timer should be suspended.
        checkpoint_serial: u64,
    },
    /// Resume a previously suspended timer.
    ResumeTimer {
        /// The checkpoint serial number whose timer should be resumed.
        checkpoint_serial: u64,
        /// The remaining duration to schedule for the resumed timer.
        remaining: Duration,
    },
}

/// The state of an export session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportState {
    /// Initial transmission of data segments is in progress or complete,
    /// awaiting report from receiver.
    Sending,
    /// Waiting for a report segment from the receiver.
    AwaitingReport,
    /// Retransmitting unclaimed data after a partial report.
    Retransmitting,
    /// All data has been acknowledged; session is complete.
    Complete,
    /// Session has been cancelled.
    Cancelled,
}

/// Export (sender) session state machine.
///
/// This is a pure state machine with no I/O. The caller drives it by calling
/// methods and executing the returned actions.
#[derive(Debug)]
pub struct ExportSession {
    /// The session identifier.
    id: SessionId,
    /// The client service ID (always 1 for Bundle Protocol).
    client_service_id: u64,
    /// The original block data being transmitted.
    block: Bytes,
    /// Configuration for this session.
    config: ExportConfig,
    /// Current state of the session.
    state: ExportState,
    /// Next checkpoint serial number to assign.
    next_checkpoint_serial: u64,
    /// Number of retransmission attempts so far.
    retry_count: u32,
    /// Acknowledged byte extents. Tracks the cumulative set of bytes the
    /// receiver has confirmed using a sorted, merging extent map.
    acknowledged: ExtentMap,
    /// Checkpoint serial numbers that currently have active (running) timers.
    /// A serial is added when a `StartTimer` action is emitted and removed
    /// when the timer expires (`on_timer_expired`) or is suspended.
    active_timers: HashSet<u64>,
    /// Whether timers are currently suspended (prevents double-suspend).
    timers_suspended: bool,
}

impl ExportSession {
    /// Creates a new export session, segments the block, and returns the
    /// initial set of actions (SendSegment for each data segment, plus a
    /// StartTimer for the final checkpoint).
    ///
    /// The block is split into segments of at most `config.max_segment_size`
    /// bytes. All segments except the last are type `RedData` (0), unless
    /// intermediate checkpoints are enabled (`checkpoint_every_n > 0`), in
    /// which case every Nth segment is type `RedCheckpoint` (1). The final
    /// segment is always `RedEob` (3) with a checkpoint serial number.
    ///
    /// When `config.green` is true, segments use `GreenData` (4) and the
    /// final segment uses `GreenEob` (7). No checkpoint info is included
    /// and no timers are started. The session transitions directly to
    /// `Complete` after all segments are emitted.
    pub fn new(
        id: SessionId,
        block: Bytes,
        client_service_id: u64,
        config: ExportConfig,
    ) -> (Self, Vec<ExportAction>) {
        // Start checkpoint serial at 1 (will be incremented as needed)
        let mut next_checkpoint_serial: u64 = 1;

        let block_len = block.len();
        let max_seg = config.max_segment_size.max(1); // ensure at least 1 byte per segment
        let is_green = config.green;

        // Calculate number of segments
        let num_segments = if block_len == 0 {
            1 // Even an empty block produces one EOB segment
        } else {
            block_len.div_ceil(max_seg)
        };

        // Pre-size the actions vec: each segment produces a SendSegment, plus
        // timers for checkpoints (intermediate + final).
        let checkpoint_every_n = config.checkpoint_every_n as usize;
        let estimated_timers = if is_green {
            0
        } else if let Some(result) = num_segments.checked_div(checkpoint_every_n) {
            result + 1
        } else {
            1
        };
        let mut actions = Vec::with_capacity(num_segments + estimated_timers);

        let mut offset: usize = 0;
        let mut segment_index: usize = 0;

        while segment_index < num_segments {
            let is_last = segment_index == num_segments - 1;
            let remaining = block_len.saturating_sub(offset);
            let seg_len = if is_last {
                remaining
            } else {
                remaining.min(max_seg)
            };

            let data = block.slice(offset..offset + seg_len);

            // Determine segment type and checkpoint info
            let (seg_type, checkpoint) = if is_green {
                // Green data: no checkpoints, no timers
                if is_last {
                    (SegmentType::GreenEob, None)
                } else {
                    (SegmentType::GreenData, None)
                }
            } else if is_last {
                // Final segment is always EOB (type 3) with checkpoint
                let serial = next_checkpoint_serial;
                next_checkpoint_serial += 1;
                (
                    SegmentType::RedEob,
                    Some(CheckpointInfo {
                        serial,
                        responding_report_serial: 0,
                    }),
                )
            } else if config.checkpoint_every_n > 0
                && (segment_index + 1).is_multiple_of(config.checkpoint_every_n as usize)
            {
                // Intermediate checkpoint (type 1) — check max_checkpoints limit
                if let Some(max) = config.max_checkpoints {
                    let checkpoints_sent = next_checkpoint_serial - 1;
                    if checkpoints_sent >= max {
                        // Would exceed limit — skip intermediate checkpoint, send as plain data
                        (SegmentType::RedData, None)
                    } else {
                        let serial = next_checkpoint_serial;
                        next_checkpoint_serial += 1;
                        (
                            SegmentType::RedCheckpoint,
                            Some(CheckpointInfo {
                                serial,
                                responding_report_serial: 0,
                            }),
                        )
                    }
                } else {
                    let serial = next_checkpoint_serial;
                    next_checkpoint_serial += 1;
                    (
                        SegmentType::RedCheckpoint,
                        Some(CheckpointInfo {
                            serial,
                            responding_report_serial: 0,
                        }),
                    )
                }
            } else {
                // Plain red data (type 0)
                (SegmentType::RedData, None)
            };

            // Build the segment
            let seg = Segment::Data {
                session_id: id,
                segment_type: seg_type,
                client_service_id,
                offset: offset as u64,
                data,
                checkpoint,
            };

            // Encode the segment to wire format
            let wire_size = segment::encoded_size(&seg);
            let mut buf = BytesMut::with_capacity(wire_size);
            segment::encode(&seg, &mut buf);
            actions.push(ExportAction::SendSegment(buf.freeze()));

            // If this segment has a checkpoint, start a timer for it
            // (green segments never have checkpoints, so no timers are started)
            if let Some(ref ckpt) = checkpoint {
                actions.push(ExportAction::StartTimer {
                    checkpoint_serial: ckpt.serial,
                    duration: config.retransmit_timeout,
                });
            }

            offset += seg_len;
            segment_index += 1;
        }

        // Green sessions complete immediately; red sessions await reports
        let initial_state = if is_green {
            ExportState::Complete
        } else {
            ExportState::AwaitingReport
        };

        let session = ExportSession {
            id,
            client_service_id,
            block,
            config,
            state: initial_state,
            next_checkpoint_serial,
            retry_count: 0,
            acknowledged: ExtentMap::new(),
            active_timers: HashSet::new(),
            timers_suspended: false,
        };

        // Track which checkpoint serials have active timers
        let mut session_with_timers = session;
        for action in &actions {
            if let ExportAction::StartTimer {
                checkpoint_serial, ..
            } = action
            {
                session_with_timers.active_timers.insert(*checkpoint_serial);
            }
        }

        (session_with_timers, actions)
    }

    /// Returns the current state of the export session.
    pub fn state(&self) -> ExportState {
        self.state
    }

    /// Returns the session identifier.
    pub fn id(&self) -> &SessionId {
        &self.id
    }

    /// Returns the next checkpoint serial number that will be assigned.
    pub fn next_checkpoint_serial(&self) -> u64 {
        self.next_checkpoint_serial
    }

    /// Returns a reference to the original block data.
    pub fn block(&self) -> &Bytes {
        &self.block
    }

    /// Returns the current retry count.
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    /// Returns the acknowledged byte ranges as (offset, length) pairs.
    pub fn acknowledged_ranges(&self) -> Vec<(u64, u64)> {
        self.acknowledged.claims()
    }

    /// Handles a received Report Segment.
    ///
    /// Always emits a Report-Ack segment. Records the claimed byte ranges,
    /// then checks if the entire block is acknowledged. If so, transitions
    /// to Complete. Otherwise, retransmits data segments covering the
    /// unclaimed byte ranges within [lower_bound, upper_bound).
    ///
    /// The final retransmitted segment in each retransmission batch is marked
    /// as a new checkpoint with a new serial number, and a timer is started.
    ///
    /// Receiving a report resets the retry_count (progress was made).
    pub fn on_report(
        &mut self,
        report_serial: u64,
        _checkpoint_serial: u64,
        upper_bound: u64,
        lower_bound: u64,
        claims: &[ReceptionClaim],
    ) -> Vec<ExportAction> {
        let mut actions = Vec::new();

        // Always send a Report-Ack regardless of session state
        let ras = Segment::ReportAck {
            session_id: self.id,
            report_serial,
        };
        let wire_size = segment::encoded_size(&ras);
        let mut buf = BytesMut::with_capacity(wire_size);
        segment::encode(&ras, &mut buf);
        actions.push(ExportAction::SendSegment(buf.freeze()));

        // If session is already complete or cancelled, just send the RAS
        if self.state == ExportState::Complete || self.state == ExportState::Cancelled {
            return actions;
        }

        // Reset retry count — receiving a report means progress was made
        self.retry_count = 0;

        // Record claimed ranges (merge into our acknowledged set)
        for claim in claims {
            self.acknowledged
                .insert(claim.offset, claim.offset + claim.length);
        }

        // Check if the entire block is now acknowledged
        let block_len = self.block.len() as u64;
        if self.acknowledged.is_complete(0, block_len) {
            self.state = ExportState::Complete;
            self.active_timers.clear();
            return actions;
        }

        // Compute unclaimed byte ranges within [lower_bound, upper_bound)
        let unclaimed = self.acknowledged.gaps(lower_bound, upper_bound);

        if unclaimed.is_empty() {
            // All bytes within this report window are acknowledged,
            // but the full block isn't done yet — stay in AwaitingReport
            self.state = ExportState::AwaitingReport;
            return actions;
        }

        // Check max checkpoints limit before creating a new checkpoint
        if let Some(max) = self.config.max_checkpoints {
            let checkpoints_sent = self.next_checkpoint_serial - 1;
            if checkpoints_sent >= max {
                // Exceeded checkpoint limit — cancel session
                let cancel = Segment::Cancel {
                    session_id: self.id,
                    reason: CancelReason::RetransmitLimitExceeded,
                    direction: CancelDirection::FromSender,
                };
                let wire_size = segment::encoded_size(&cancel);
                let mut buf = BytesMut::with_capacity(wire_size);
                segment::encode(&cancel, &mut buf);
                actions.push(ExportAction::SendSegment(buf.freeze()));

                self.state = ExportState::Cancelled;
                self.active_timers.clear();
                return actions;
            }
        }

        // Retransmit unclaimed ranges
        self.state = ExportState::Retransmitting;
        let max_seg = self.config.max_segment_size.max(1);

        let num_unclaimed = unclaimed.len();
        for (range_idx, (start, end)) in unclaimed.iter().enumerate() {
            let is_last_range = range_idx == num_unclaimed - 1;
            let mut offset = *start;

            while offset < *end {
                let remaining = (*end - offset) as usize;
                let seg_len = remaining.min(max_seg);
                let is_last_segment_of_last_range =
                    is_last_range && (offset + seg_len as u64) >= *end;

                let data = self
                    .block
                    .slice(offset as usize..(offset as usize + seg_len));

                // The final segment of the final unclaimed range gets a new checkpoint
                let (seg_type, checkpoint) = if is_last_segment_of_last_range {
                    let serial = self.next_checkpoint_serial;
                    self.next_checkpoint_serial += 1;
                    (
                        SegmentType::RedCheckpoint,
                        Some(CheckpointInfo {
                            serial,
                            responding_report_serial: report_serial,
                        }),
                    )
                } else {
                    (SegmentType::RedData, None)
                };

                let seg = Segment::Data {
                    session_id: self.id,
                    segment_type: seg_type,
                    client_service_id: self.client_service_id,
                    offset,
                    data,
                    checkpoint,
                };

                let wire_size = segment::encoded_size(&seg);
                let mut buf = BytesMut::with_capacity(wire_size);
                segment::encode(&seg, &mut buf);
                actions.push(ExportAction::SendSegment(buf.freeze()));

                // Start timer for the new checkpoint
                if let Some(ref ckpt) = checkpoint {
                    actions.push(ExportAction::StartTimer {
                        checkpoint_serial: ckpt.serial,
                        duration: self.config.retransmit_timeout,
                    });
                    self.active_timers.insert(ckpt.serial);
                }

                offset += seg_len as u64;
            }
        }

        // After retransmission, transition to AwaitingReport
        self.state = ExportState::AwaitingReport;
        actions
    }

    /// Handles a retransmission timer expiry for a given checkpoint serial.
    ///
    /// Increments the retry counter. If the limit is exceeded, emits a Cancel
    /// segment with reason RetransmitLimitExceeded and transitions to Cancelled.
    /// Otherwise, retransmits all unclaimed data for the full block scope and
    /// starts a new timer.
    pub fn on_timer_expired(&mut self, _checkpoint_serial: u64) -> Vec<ExportAction> {
        let mut actions = Vec::new();

        // If session is already complete or cancelled, ignore the timer
        if self.state == ExportState::Complete || self.state == ExportState::Cancelled {
            return actions;
        }

        // The expired timer is no longer active
        self.active_timers.remove(&_checkpoint_serial);

        // Increment retry count
        self.retry_count += 1;

        // Check if we've exceeded the retransmission limit
        if self.retry_count > self.config.max_retransmissions {
            // Send Cancel segment with RetransmitLimitExceeded
            let cancel = Segment::Cancel {
                session_id: self.id,
                reason: CancelReason::RetransmitLimitExceeded,
                direction: CancelDirection::FromSender,
            };
            let wire_size = segment::encoded_size(&cancel);
            let mut buf = BytesMut::with_capacity(wire_size);
            segment::encode(&cancel, &mut buf);
            actions.push(ExportAction::SendSegment(buf.freeze()));

            self.state = ExportState::Cancelled;
            self.active_timers.clear();
            return actions;
        }

        // Check max checkpoints limit before creating a new checkpoint
        if let Some(max) = self.config.max_checkpoints {
            let checkpoints_sent = self.next_checkpoint_serial - 1;
            if checkpoints_sent >= max {
                // Exceeded checkpoint limit — cancel session
                let cancel = Segment::Cancel {
                    session_id: self.id,
                    reason: CancelReason::RetransmitLimitExceeded,
                    direction: CancelDirection::FromSender,
                };
                let wire_size = segment::encoded_size(&cancel);
                let mut buf = BytesMut::with_capacity(wire_size);
                segment::encode(&cancel, &mut buf);
                actions.push(ExportAction::SendSegment(buf.freeze()));

                self.state = ExportState::Cancelled;
                self.active_timers.clear();
                return actions;
            }
        }

        // Retransmit unclaimed data for the full block scope
        let block_len = self.block.len() as u64;
        let unclaimed = self.acknowledged.gaps(0, block_len);

        if unclaimed.is_empty() {
            // Everything is acknowledged — this shouldn't happen but handle gracefully
            self.state = ExportState::Complete;
            return actions;
        }

        self.state = ExportState::Retransmitting;
        let max_seg = self.config.max_segment_size.max(1);

        let num_unclaimed = unclaimed.len();
        for (range_idx, (start, end)) in unclaimed.iter().enumerate() {
            let is_last_range = range_idx == num_unclaimed - 1;
            let mut offset = *start;

            while offset < *end {
                let remaining = (*end - offset) as usize;
                let seg_len = remaining.min(max_seg);
                let is_last_segment_of_last_range =
                    is_last_range && (offset + seg_len as u64) >= *end;

                let data = self
                    .block
                    .slice(offset as usize..(offset as usize + seg_len));

                // The final segment gets a new checkpoint
                let (seg_type, checkpoint) = if is_last_segment_of_last_range {
                    let serial = self.next_checkpoint_serial;
                    self.next_checkpoint_serial += 1;
                    (
                        SegmentType::RedCheckpoint,
                        Some(CheckpointInfo {
                            serial,
                            responding_report_serial: 0,
                        }),
                    )
                } else {
                    (SegmentType::RedData, None)
                };

                let seg = Segment::Data {
                    session_id: self.id,
                    segment_type: seg_type,
                    client_service_id: self.client_service_id,
                    offset,
                    data,
                    checkpoint,
                };

                let wire_size = segment::encoded_size(&seg);
                let mut buf = BytesMut::with_capacity(wire_size);
                segment::encode(&seg, &mut buf);
                actions.push(ExportAction::SendSegment(buf.freeze()));

                // Start timer for the new checkpoint
                if let Some(ref ckpt) = checkpoint {
                    actions.push(ExportAction::StartTimer {
                        checkpoint_serial: ckpt.serial,
                        duration: self.config.retransmit_timeout,
                    });
                    self.active_timers.insert(ckpt.serial);
                }

                offset += seg_len as u64;
            }
        }

        // After retransmission, transition to AwaitingReport
        self.state = ExportState::AwaitingReport;
        actions
    }

    /// Handles a Cancel-from-Receiver segment.
    ///
    /// Emits a Cancel-Ack-to-Receiver segment and transitions to Cancelled.
    pub fn on_cancel_from_receiver(&mut self, _reason: CancelReason) -> Vec<ExportAction> {
        let mut actions = Vec::new();

        // Send Cancel-Ack to Receiver
        let cancel_ack = Segment::CancelAck {
            session_id: self.id,
            direction: CancelDirection::FromReceiver,
        };
        let wire_size = segment::encoded_size(&cancel_ack);
        let mut buf = BytesMut::with_capacity(wire_size);
        segment::encode(&cancel_ack, &mut buf);
        actions.push(ExportAction::SendSegment(buf.freeze()));

        self.state = ExportState::Cancelled;
        self.active_timers.clear();
        actions
    }

    /// Suspend all active timers. Returns actions describing which timers
    /// to suspend. The caller is responsible for recording remaining durations.
    ///
    /// If timers are already suspended or the session is complete/cancelled,
    /// returns an empty vec.
    pub fn suspend_timers(&mut self) -> Vec<ExportAction> {
        // Don't double-suspend, and don't suspend if session is done
        if self.timers_suspended
            || self.state == ExportState::Complete
            || self.state == ExportState::Cancelled
        {
            return Vec::new();
        }

        let actions: Vec<ExportAction> = self
            .active_timers
            .iter()
            .map(|&checkpoint_serial| ExportAction::SuspendTimer { checkpoint_serial })
            .collect();

        self.timers_suspended = true;
        actions
    }

    /// Resume previously suspended timers. Called with the remaining durations
    /// that were recorded during suspension.
    ///
    /// Returns `ResumeTimer` actions for each checkpoint serial that still
    /// exists in this session's active timer set. Checkpoint serials that are
    /// no longer valid (e.g., session was cancelled between suspend and resume)
    /// are silently skipped.
    pub fn resume_timers(&mut self, suspended: &[(u64, Duration)]) -> Vec<ExportAction> {
        // Nothing to resume if not currently suspended or session is done
        if !self.timers_suspended
            || self.state == ExportState::Complete
            || self.state == ExportState::Cancelled
        {
            return Vec::new();
        }

        let actions: Vec<ExportAction> = suspended
            .iter()
            .filter(|(serial, _)| self.active_timers.contains(serial))
            .map(|&(checkpoint_serial, remaining)| ExportAction::ResumeTimer {
                checkpoint_serial,
                remaining,
            })
            .collect();

        self.timers_suspended = false;
        actions
    }

    /// Returns whether timers are currently suspended.
    pub fn timers_suspended(&self) -> bool {
        self.timers_suspended
    }

    /// Returns the set of checkpoint serials with active timers.
    pub fn active_timer_serials(&self) -> &HashSet<u64> {
        &self.active_timers
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::ReceptionClaim;

    fn default_config() -> ExportConfig {
        ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 3,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 0,
            max_checkpoints: None,
            green: false,
        }
    }

    fn session_id() -> SessionId {
        SessionId {
            engine_id: 1,
            session_number: 42,
        }
    }

    /// Helper: create a session with a 200-byte block and 100-byte segments.
    fn create_test_session() -> ExportSession {
        let block = Bytes::from(vec![0xAB; 200]);
        let config = default_config();
        let (session, _actions) = ExportSession::new(session_id(), block, 1, config);
        session
    }

    /// Helper: count SendSegment actions.
    fn count_send_segments(actions: &[ExportAction]) -> usize {
        actions
            .iter()
            .filter(|a| matches!(a, ExportAction::SendSegment(_)))
            .count()
    }

    /// Helper: count StartTimer actions.
    fn count_start_timers(actions: &[ExportAction]) -> usize {
        actions
            .iter()
            .filter(|a| matches!(a, ExportAction::StartTimer { .. }))
            .count()
    }

    /// Helper: decode the first SendSegment action into a Segment.
    fn decode_first_segment(actions: &[ExportAction]) -> Segment {
        for action in actions {
            if let ExportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                return segment::decode(&mut reader).unwrap();
            }
        }
        panic!("No SendSegment action found");
    }

    // -----------------------------------------------------------------------
    // on_report tests
    // -----------------------------------------------------------------------

    #[test]
    fn on_report_sends_report_ack() {
        let mut session = create_test_session();
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 100,
        }];

        let actions = session.on_report(1, 1, 200, 0, &claims);

        // First action should be a ReportAck
        let seg = decode_first_segment(&actions);
        match seg {
            Segment::ReportAck {
                report_serial,
                session_id: sid,
                ..
            } => {
                assert_eq!(report_serial, 1);
                assert_eq!(sid, session_id());
            }
            _ => panic!("Expected ReportAck segment, got {:?}", seg),
        }
    }

    #[test]
    fn on_report_full_ack_transitions_to_complete() {
        let mut session = create_test_session();
        // Acknowledge the entire 200-byte block
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 200,
        }];

        let actions = session.on_report(1, 1, 200, 0, &claims);

        assert_eq!(session.state(), ExportState::Complete);
        // Should only have the ReportAck, no retransmissions
        assert_eq!(count_send_segments(&actions), 1);
        assert_eq!(count_start_timers(&actions), 0);
    }

    #[test]
    fn on_report_partial_ack_retransmits_unclaimed() {
        let mut session = create_test_session();
        // Acknowledge only the first 100 bytes; bytes 100-200 are unclaimed
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 100,
        }];

        let actions = session.on_report(1, 1, 200, 0, &claims);

        // Should have: 1 ReportAck + 1 retransmitted data segment (100 bytes)
        // The retransmitted segment should be a checkpoint
        assert_eq!(count_send_segments(&actions), 2);
        assert_eq!(count_start_timers(&actions), 1);
        assert_eq!(session.state(), ExportState::AwaitingReport);
    }

    #[test]
    fn on_report_resets_retry_count() {
        let mut session = create_test_session();

        // Simulate a timer expiry to increment retry_count
        let _ = session.on_timer_expired(1);
        assert_eq!(session.retry_count(), 1);

        // Now receive a report — retry_count should reset
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 100,
        }];
        let _ = session.on_report(1, 1, 200, 0, &claims);
        assert_eq!(session.retry_count(), 0);
    }

    #[test]
    fn on_report_retransmits_gap_in_middle() {
        let block = Bytes::from(vec![0xAB; 300]);
        let config = ExportConfig {
            max_segment_size: 100,
            ..default_config()
        };
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // Acknowledge bytes 0-100 and 200-300, leaving gap at 100-200
        let claims = vec![
            ReceptionClaim {
                offset: 0,
                length: 100,
            },
            ReceptionClaim {
                offset: 200,
                length: 100,
            },
        ];

        let actions = session.on_report(1, 1, 300, 0, &claims);

        // Should have: 1 ReportAck + 1 data segment (the gap 100-200)
        assert_eq!(count_send_segments(&actions), 2);
        assert_eq!(count_start_timers(&actions), 1);

        // Decode the retransmitted data segment (second SendSegment)
        let mut data_segments = Vec::new();
        for action in &actions {
            if let ExportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Data { offset, data, .. } = &seg {
                    data_segments.push((*offset, data.len()));
                }
            }
        }
        assert_eq!(data_segments.len(), 1);
        assert_eq!(data_segments[0], (100, 100)); // gap at offset 100, length 100
    }

    #[test]
    fn on_report_checkpoint_serial_increases() {
        let mut session = create_test_session();
        let initial_serial = session.next_checkpoint_serial();

        // Partial ack triggers retransmission with new checkpoint
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 100,
        }];
        let _ = session.on_report(1, 1, 200, 0, &claims);

        assert!(session.next_checkpoint_serial() > initial_serial);
    }

    #[test]
    fn on_report_multiple_claims_merge() {
        let block = Bytes::from(vec![0xAB; 400]);
        let config = ExportConfig {
            max_segment_size: 100,
            ..default_config()
        };
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // First report: ack bytes 0-100
        let claims1 = vec![ReceptionClaim {
            offset: 0,
            length: 100,
        }];
        let _ = session.on_report(1, 1, 400, 0, &claims1);

        // Second report: ack bytes 100-200 (adjacent to first)
        let claims2 = vec![ReceptionClaim {
            offset: 100,
            length: 100,
        }];
        let _ = session.on_report(2, 2, 400, 0, &claims2);

        // The acknowledged ranges should be merged into one: [0, 200)
        let ranges = session.acknowledged_ranges();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (0, 200));
    }

    #[test]
    fn on_report_ignored_when_complete() {
        let mut session = create_test_session();

        // Fully acknowledge
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 200,
        }];
        let _ = session.on_report(1, 1, 200, 0, &claims);
        assert_eq!(session.state(), ExportState::Complete);

        // Another report should just send RAS, not change state
        let actions = session.on_report(2, 1, 200, 0, &claims);
        assert_eq!(session.state(), ExportState::Complete);
        assert_eq!(count_send_segments(&actions), 1); // Just the RAS
    }

    // -----------------------------------------------------------------------
    // on_timer_expired tests
    // -----------------------------------------------------------------------

    #[test]
    fn on_timer_expired_increments_retry_count() {
        let mut session = create_test_session();
        assert_eq!(session.retry_count(), 0);

        let _ = session.on_timer_expired(1);
        assert_eq!(session.retry_count(), 1);

        let _ = session.on_timer_expired(2);
        assert_eq!(session.retry_count(), 2);
    }

    #[test]
    fn on_timer_expired_retransmits_full_block() {
        let mut session = create_test_session();

        let actions = session.on_timer_expired(1);

        // Should retransmit the full 200-byte block (2 segments of 100 bytes)
        // The last segment should be a checkpoint
        assert_eq!(count_send_segments(&actions), 2);
        assert_eq!(count_start_timers(&actions), 1);
        assert_eq!(session.state(), ExportState::AwaitingReport);
    }

    #[test]
    fn on_timer_expired_retransmits_only_unclaimed() {
        let mut session = create_test_session();

        // First, acknowledge the first 100 bytes
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 100,
        }];
        let _ = session.on_report(1, 1, 200, 0, &claims);

        // Now timer expires — should only retransmit bytes 100-200
        let actions = session.on_timer_expired(2);

        // Should have 1 data segment (100 bytes) + 1 timer
        assert_eq!(count_send_segments(&actions), 1);
        assert_eq!(count_start_timers(&actions), 1);
    }

    #[test]
    fn on_timer_expired_cancels_when_limit_exceeded() {
        let block = Bytes::from(vec![0xAB; 100]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 2,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 0,
            max_checkpoints: None,
            green: false,
        };
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // Expire timer twice (retry_count goes to 1, then 2)
        let _ = session.on_timer_expired(1);
        assert_eq!(session.retry_count(), 1);
        assert_eq!(session.state(), ExportState::AwaitingReport);

        let _ = session.on_timer_expired(2);
        assert_eq!(session.retry_count(), 2);
        assert_eq!(session.state(), ExportState::AwaitingReport);

        // Third expiry exceeds limit (retry_count 3 > max_retransmissions 2)
        let actions = session.on_timer_expired(3);
        assert_eq!(session.retry_count(), 3);
        assert_eq!(session.state(), ExportState::Cancelled);

        // Should emit a Cancel segment
        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::RetransmitLimitExceeded);
                assert_eq!(direction, CancelDirection::FromSender);
            }
            _ => panic!("Expected Cancel segment, got {:?}", seg),
        }
    }

    #[test]
    fn on_timer_expired_ignored_when_cancelled() {
        let mut session = create_test_session();
        session.on_cancel_from_receiver(CancelReason::ByUser);
        assert_eq!(session.state(), ExportState::Cancelled);

        let actions = session.on_timer_expired(1);
        assert!(actions.is_empty());
        assert_eq!(session.state(), ExportState::Cancelled);
    }

    #[test]
    fn on_timer_expired_ignored_when_complete() {
        let mut session = create_test_session();

        // Fully acknowledge
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 200,
        }];
        let _ = session.on_report(1, 1, 200, 0, &claims);
        assert_eq!(session.state(), ExportState::Complete);

        let actions = session.on_timer_expired(1);
        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // on_cancel_from_receiver tests
    // -----------------------------------------------------------------------

    #[test]
    fn on_cancel_from_receiver_sends_cancel_ack() {
        let mut session = create_test_session();

        let actions = session.on_cancel_from_receiver(CancelReason::MiscoloredSegment);

        assert_eq!(session.state(), ExportState::Cancelled);
        assert_eq!(count_send_segments(&actions), 1);

        let seg = decode_first_segment(&actions);
        match seg {
            Segment::CancelAck { direction, .. } => {
                assert_eq!(direction, CancelDirection::FromReceiver);
            }
            _ => panic!("Expected CancelAck segment, got {:?}", seg),
        }
    }

    #[test]
    fn on_cancel_from_receiver_various_reasons() {
        for reason in [
            CancelReason::ByUser,
            CancelReason::ClientSvcUnreachable,
            CancelReason::RetransmitLimitExceeded,
            CancelReason::MiscoloredSegment,
            CancelReason::ByEngine,
        ] {
            let mut session = create_test_session();
            let actions = session.on_cancel_from_receiver(reason);
            assert_eq!(session.state(), ExportState::Cancelled);
            assert_eq!(count_send_segments(&actions), 1);
        }
    }

    // -----------------------------------------------------------------------
    // State transition tests
    // -----------------------------------------------------------------------

    #[test]
    fn state_transitions_sending_to_awaiting_report() {
        let session = create_test_session();
        // After new(), session should be in AwaitingReport
        assert_eq!(session.state(), ExportState::AwaitingReport);
    }

    #[test]
    fn state_transitions_full_lifecycle() {
        let mut session = create_test_session();
        assert_eq!(session.state(), ExportState::AwaitingReport);

        // Partial report → retransmit → back to AwaitingReport
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 100,
        }];
        let _ = session.on_report(1, 1, 200, 0, &claims);
        assert_eq!(session.state(), ExportState::AwaitingReport);

        // Full ack → Complete
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 200,
        }];
        let _ = session.on_report(2, 2, 200, 0, &claims);
        assert_eq!(session.state(), ExportState::Complete);
    }

    // -----------------------------------------------------------------------
    // Helper method tests
    // -----------------------------------------------------------------------

    #[test]
    fn acknowledged_ranges_adjacent() {
        let mut session = create_test_session();
        session.acknowledged.insert(0, 50);
        session.acknowledged.insert(50, 100);
        assert_eq!(session.acknowledged_ranges(), vec![(0, 100)]);
    }

    #[test]
    fn acknowledged_ranges_overlapping() {
        let mut session = create_test_session();
        session.acknowledged.insert(0, 60);
        session.acknowledged.insert(40, 100);
        assert_eq!(session.acknowledged_ranges(), vec![(0, 100)]);
    }

    #[test]
    fn acknowledged_ranges_disjoint() {
        let mut session = create_test_session();
        session.acknowledged.insert(0, 50);
        session.acknowledged.insert(100, 150);
        assert_eq!(session.acknowledged_ranges(), vec![(0, 50), (100, 50)]);
    }

    #[test]
    fn acknowledged_ranges_fills_gap() {
        let mut session = create_test_session();
        session.acknowledged.insert(0, 50);
        session.acknowledged.insert(100, 150);
        // Fill the gap
        session.acknowledged.insert(50, 100);
        assert_eq!(session.acknowledged_ranges(), vec![(0, 150)]);
    }

    #[test]
    fn gaps_no_acks() {
        let session = create_test_session();
        let unclaimed = session.acknowledged.gaps(0, 200);
        assert_eq!(unclaimed, vec![(0, 200)]);
    }

    #[test]
    fn gaps_partial_ack() {
        let mut session = create_test_session();
        session.acknowledged.insert(0, 100);
        let unclaimed = session.acknowledged.gaps(0, 200);
        assert_eq!(unclaimed, vec![(100, 200)]);
    }

    #[test]
    fn gaps_in_middle() {
        let mut session = create_test_session();
        session.acknowledged.insert(0, 50);
        session.acknowledged.insert(150, 200);
        let unclaimed = session.acknowledged.gaps(0, 200);
        assert_eq!(unclaimed, vec![(50, 150)]);
    }

    #[test]
    fn gaps_fully_acked() {
        let mut session = create_test_session();
        session.acknowledged.insert(0, 200);
        let unclaimed = session.acknowledged.gaps(0, 200);
        assert!(unclaimed.is_empty());
    }

    #[test]
    fn gaps_windowed() {
        let mut session = create_test_session();
        session.acknowledged.insert(50, 100); // acked [50, 100)
        // Window is [0, 150) — unclaimed: [0, 50) and [100, 150)
        let unclaimed = session.acknowledged.gaps(0, 150);
        assert_eq!(unclaimed, vec![(0, 50), (100, 150)]);
    }

    // -----------------------------------------------------------------------
    // Green data export session tests
    // -----------------------------------------------------------------------

    fn green_config() -> ExportConfig {
        ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 3,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 0,
            max_checkpoints: None,
            green: true,
        }
    }

    #[test]
    fn green_session_uses_green_segment_types() {
        let block = Bytes::from(vec![0xAB; 200]);
        let config = green_config();
        let (_session, actions) = ExportSession::new(session_id(), block, 1, config);

        // Decode all data segments and check their types
        let mut segment_types = Vec::new();
        for action in &actions {
            if let ExportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Data { segment_type, .. } = seg {
                    segment_types.push(segment_type);
                }
            }
        }

        // 200 bytes / 100 max = 2 segments
        assert_eq!(segment_types.len(), 2);
        assert_eq!(segment_types[0], SegmentType::GreenData);
        assert_eq!(segment_types[1], SegmentType::GreenEob);
    }

    #[test]
    fn green_session_no_checkpoint_info() {
        let block = Bytes::from(vec![0xAB; 200]);
        let config = green_config();
        let (_session, actions) = ExportSession::new(session_id(), block, 1, config);

        // All segments should have checkpoint = None
        for action in &actions {
            if let ExportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Data { checkpoint, .. } = seg {
                    assert_eq!(
                        checkpoint, None,
                        "Green segments must not have checkpoint info"
                    );
                }
            }
        }
    }

    #[test]
    fn green_session_no_timers() {
        let block = Bytes::from(vec![0xAB; 200]);
        let config = green_config();
        let (_session, actions) = ExportSession::new(session_id(), block, 1, config);

        // No StartTimer actions should be emitted
        assert_eq!(count_start_timers(&actions), 0);
    }

    #[test]
    fn green_session_immediately_complete() {
        let block = Bytes::from(vec![0xAB; 200]);
        let config = green_config();
        let (session, _actions) = ExportSession::new(session_id(), block, 1, config);

        assert_eq!(session.state(), ExportState::Complete);
    }

    #[test]
    fn green_session_on_report_ignored() {
        let block = Bytes::from(vec![0xAB; 200]);
        let config = green_config();
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // Session is already Complete, so on_report should just send RAS
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 200,
        }];
        let actions = session.on_report(1, 1, 200, 0, &claims);

        // Should only emit a ReportAck (the early-return path for Complete state)
        assert_eq!(session.state(), ExportState::Complete);
        assert_eq!(count_send_segments(&actions), 1);
        assert_eq!(count_start_timers(&actions), 0);
    }

    #[test]
    fn green_session_on_timer_expired_ignored() {
        let block = Bytes::from(vec![0xAB; 200]);
        let config = green_config();
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // Session is already Complete, so timer expiry should be a no-op
        let actions = session.on_timer_expired(1);
        assert!(actions.is_empty());
        assert_eq!(session.state(), ExportState::Complete);
    }

    #[test]
    fn green_session_single_segment_uses_green_eob() {
        let block = Bytes::from(vec![0xAB; 50]);
        let config = green_config();
        let (session, actions) = ExportSession::new(session_id(), block, 1, config);

        assert_eq!(session.state(), ExportState::Complete);
        assert_eq!(count_send_segments(&actions), 1);
        assert_eq!(count_start_timers(&actions), 0);

        // The single segment should be GreenEob
        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Data {
                segment_type,
                checkpoint,
                ..
            } => {
                assert_eq!(segment_type, SegmentType::GreenEob);
                assert_eq!(checkpoint, None);
            }
            _ => panic!("Expected Data segment, got {:?}", seg),
        }
    }

    #[test]
    fn green_session_empty_block() {
        let block = Bytes::new();
        let config = green_config();
        let (session, actions) = ExportSession::new(session_id(), block, 1, config);

        assert_eq!(session.state(), ExportState::Complete);
        assert_eq!(count_send_segments(&actions), 1);
        assert_eq!(count_start_timers(&actions), 0);

        // Even an empty block produces one GreenEob segment
        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Data {
                segment_type,
                checkpoint,
                data,
                ..
            } => {
                assert_eq!(segment_type, SegmentType::GreenEob);
                assert_eq!(checkpoint, None);
                assert!(data.is_empty());
            }
            _ => panic!("Expected Data segment, got {:?}", seg),
        }
    }

    #[test]
    fn green_session_multiple_segments() {
        let block = Bytes::from(vec![0xAB; 350]);
        let config = ExportConfig {
            max_segment_size: 100,
            ..green_config()
        };
        let (session, actions) = ExportSession::new(session_id(), block, 1, config);

        assert_eq!(session.state(), ExportState::Complete);
        // 350 / 100 = 4 segments (100 + 100 + 100 + 50)
        assert_eq!(count_send_segments(&actions), 4);
        assert_eq!(count_start_timers(&actions), 0);

        // Decode all segments and verify types
        let mut segment_types = Vec::new();
        for action in &actions {
            if let ExportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Data { segment_type, .. } = seg {
                    segment_types.push(segment_type);
                }
            }
        }

        // First 3 should be GreenData, last should be GreenEob
        assert_eq!(segment_types[0], SegmentType::GreenData);
        assert_eq!(segment_types[1], SegmentType::GreenData);
        assert_eq!(segment_types[2], SegmentType::GreenData);
        assert_eq!(segment_types[3], SegmentType::GreenEob);
    }

    #[test]
    fn green_session_data_reconstructs_block() {
        let block_data = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let block = Bytes::from(block_data.clone());
        let config = ExportConfig {
            max_segment_size: 3,
            ..green_config()
        };
        let (_session, actions) = ExportSession::new(session_id(), block, 1, config);

        // Reconstruct the block from segment payloads
        let mut reconstructed = Vec::new();
        for action in &actions {
            if let ExportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Data { data, .. } = seg {
                    reconstructed.extend_from_slice(&data);
                }
            }
        }

        assert_eq!(reconstructed, block_data);
    }

    // -----------------------------------------------------------------------
    // Max checkpoints limit tests
    // -----------------------------------------------------------------------

    #[test]
    fn max_checkpoints_cancels_on_report_retransmission() {
        // Create a session with max_checkpoints = 2
        // Initial transmission creates 1 checkpoint (the EOB).
        // First retransmission creates checkpoint #2 (at limit).
        // Second retransmission should trigger cancel.
        let block = Bytes::from(vec![0xAB; 200]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 10,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 0,
            max_checkpoints: Some(2),
            green: false,
        };
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // Initial transmission used 1 checkpoint (serial 1, the EOB)
        assert_eq!(session.next_checkpoint_serial(), 2);

        // First partial report — retransmits with checkpoint #2 (at limit)
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 100,
        }];
        let actions = session.on_report(1, 1, 200, 0, &claims);
        assert_eq!(session.state(), ExportState::AwaitingReport);
        assert_eq!(session.next_checkpoint_serial(), 3); // Used checkpoint #2
        // Should have RAS + retransmitted data segment
        assert!(count_send_segments(&actions) >= 2);

        // Second partial report — would need checkpoint #3, but limit is 2
        let claims2 = vec![ReceptionClaim {
            offset: 0,
            length: 100,
        }];
        let actions2 = session.on_report(2, 2, 200, 0, &claims2);
        assert_eq!(session.state(), ExportState::Cancelled);

        // Should have RAS + Cancel segment
        // Find the Cancel segment
        let mut found_cancel = false;
        for action in &actions2 {
            if let ExportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Cancel {
                    reason, direction, ..
                } = seg
                {
                    assert_eq!(reason, CancelReason::RetransmitLimitExceeded);
                    assert_eq!(direction, CancelDirection::FromSender);
                    found_cancel = true;
                }
            }
        }
        assert!(found_cancel, "Expected Cancel segment in actions");
    }

    #[test]
    fn max_checkpoints_cancels_on_timer_retransmission() {
        // Create a session with max_checkpoints = 1
        // Initial transmission creates 1 checkpoint (the EOB) — at limit.
        // Timer expiry should trigger cancel because next checkpoint would exceed limit.
        let block = Bytes::from(vec![0xAB; 100]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 10,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 0,
            max_checkpoints: Some(1),
            green: false,
        };
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // Initial transmission used 1 checkpoint (serial 1)
        assert_eq!(session.next_checkpoint_serial(), 2);

        // Timer expires — would need a new checkpoint but limit is 1
        let actions = session.on_timer_expired(1);
        assert_eq!(session.state(), ExportState::Cancelled);

        // Should emit a Cancel segment
        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::RetransmitLimitExceeded);
                assert_eq!(direction, CancelDirection::FromSender);
            }
            _ => panic!("Expected Cancel segment, got {:?}", seg),
        }
    }

    #[test]
    fn max_checkpoints_none_means_unlimited() {
        // With max_checkpoints = None, retransmissions should proceed normally
        let block = Bytes::from(vec![0xAB; 100]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 10,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 0,
            max_checkpoints: None,
            green: false,
        };
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // Multiple timer expiries should work fine
        for i in 1..=5 {
            let actions = session.on_timer_expired(i);
            assert_eq!(session.state(), ExportState::AwaitingReport);
            assert!(count_send_segments(&actions) > 0);
        }
    }

    #[test]
    fn max_checkpoints_allows_retransmission_within_limit() {
        // max_checkpoints = 3: initial (1) + 2 retransmissions allowed
        let block = Bytes::from(vec![0xAB; 100]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 10,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 0,
            max_checkpoints: Some(3),
            green: false,
        };
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // First timer expiry — checkpoint #2 (within limit)
        let actions1 = session.on_timer_expired(1);
        assert_eq!(session.state(), ExportState::AwaitingReport);
        assert!(count_send_segments(&actions1) > 0);

        // Second timer expiry — checkpoint #3 (at limit)
        let actions2 = session.on_timer_expired(2);
        assert_eq!(session.state(), ExportState::AwaitingReport);
        assert!(count_send_segments(&actions2) > 0);

        // Third timer expiry — would be checkpoint #4, exceeds limit
        let actions3 = session.on_timer_expired(3);
        assert_eq!(session.state(), ExportState::Cancelled);

        let seg = decode_first_segment(&actions3);
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::RetransmitLimitExceeded);
                assert_eq!(direction, CancelDirection::FromSender);
            }
            _ => panic!("Expected Cancel segment, got {:?}", seg),
        }
    }

    #[test]
    fn max_checkpoints_intermediate_checkpoints_count_toward_limit() {
        // With checkpoint_every_n = 1 and a 300-byte block with 100-byte segments,
        // we get 3 segments: seg0 (intermediate ckpt), seg1 (intermediate ckpt), seg2 (EOB ckpt)
        // That's 3 checkpoints. With max_checkpoints = 3, no retransmission is allowed.
        let block = Bytes::from(vec![0xAB; 300]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 10,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 1,
            max_checkpoints: Some(3),
            green: false,
        };
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // Initial transmission: 3 checkpoints used (serials 1, 2, 3)
        assert_eq!(session.next_checkpoint_serial(), 4);

        // Timer expiry — would need checkpoint #4, but limit is 3
        let actions = session.on_timer_expired(1);
        assert_eq!(session.state(), ExportState::Cancelled);

        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::RetransmitLimitExceeded);
                assert_eq!(direction, CancelDirection::FromSender);
            }
            _ => panic!("Expected Cancel segment, got {:?}", seg),
        }
    }

    #[test]
    fn max_checkpoints_skips_intermediate_in_new_when_at_limit() {
        // With checkpoint_every_n = 1 and max_checkpoints = 2, and a 300-byte block:
        // Segments: seg0, seg1, seg2 (EOB)
        // With checkpoint_every_n=1: seg0 would be intermediate ckpt, seg1 would be intermediate ckpt
        // But max_checkpoints=2: seg0 gets ckpt (serial 1, count was 0 < 2),
        // seg1 gets ckpt (serial 2, count was 1 < 2), seg2 is EOB (always gets ckpt, serial 3)
        // After initial transmission, 3 checkpoints used. The limit prevents further
        // retransmission checkpoints.
        let block = Bytes::from(vec![0xAB; 300]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 10,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 1,
            max_checkpoints: Some(2),
            green: false,
        };
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // Initial transmission used checkpoints: 2 intermediate + 1 EOB = 3 total
        // (EOB is always unconditional)
        assert_eq!(session.next_checkpoint_serial(), 4);

        // Timer expiry — would need checkpoint #4, but limit is 2 (already exceeded)
        let actions = session.on_timer_expired(1);
        assert_eq!(session.state(), ExportState::Cancelled);

        let seg = decode_first_segment(&actions);
        match seg {
            Segment::Cancel {
                reason, direction, ..
            } => {
                assert_eq!(reason, CancelReason::RetransmitLimitExceeded);
                assert_eq!(direction, CancelDirection::FromSender);
            }
            _ => panic!("Expected Cancel segment, got {:?}", seg),
        }
    }

    #[test]
    fn max_checkpoints_limits_intermediate_in_new() {
        // With checkpoint_every_n = 1 and max_checkpoints = 1, and a 400-byte block:
        // 4 segments (indices 0, 1, 2, 3 where 3 is last/EOB)
        // seg0: intermediate ckpt → count was 0 < 1, allowed (serial 1)
        // seg1: intermediate ckpt → count is 1 >= 1, SKIPPED (plain RedData)
        // seg2: intermediate ckpt → count is 1 >= 1, SKIPPED (plain RedData)
        // seg3: EOB → always gets checkpoint (serial 2)
        // Total: 2 checkpoints (1 intermediate + 1 EOB)
        let block = Bytes::from(vec![0xAB; 400]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 10,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 1,
            max_checkpoints: Some(1),
            green: false,
        };
        let (session, actions) = ExportSession::new(session_id(), block, 1, config);

        // Count checkpoints in the initial transmission
        let mut checkpoint_count = 0;
        let mut segment_types = Vec::new();
        for action in &actions {
            if let ExportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Data {
                    checkpoint,
                    segment_type,
                    ..
                } = seg
                {
                    segment_types.push(segment_type);
                    if checkpoint.is_some() {
                        checkpoint_count += 1;
                    }
                }
            }
        }

        // Should have 2 checkpoints: 1 intermediate (before limit hit) + 1 EOB (always)
        assert_eq!(checkpoint_count, 2);
        // Segments: RedCheckpoint, RedData, RedData, RedEob
        assert_eq!(segment_types[0], SegmentType::RedCheckpoint);
        assert_eq!(segment_types[1], SegmentType::RedData);
        assert_eq!(segment_types[2], SegmentType::RedData);
        assert_eq!(segment_types[3], SegmentType::RedEob);
        // next_checkpoint_serial should be 3 (used serials 1 and 2)
        assert_eq!(session.next_checkpoint_serial(), 3);
    }

    // -----------------------------------------------------------------------
    // Accelerated retransmission via intermediate checkpoints (Req 27.1-27.4)
    // -----------------------------------------------------------------------

    #[test]
    fn intermediate_checkpoints_marked_every_n_segments() {
        // 500-byte block, 100-byte segments → 5 segments (indices 0..4)
        // checkpoint_every_n = 2: segments at index 1 and 3 would be checkpoints
        // (segment_index + 1) % 2 == 0 → indices 1, 3
        // But index 4 is the last segment → always RedEob
        let block = Bytes::from(vec![0xAB; 500]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 5,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 2,
            max_checkpoints: None,
            green: false,
        };
        let (_session, actions) = ExportSession::new(session_id(), block, 1, config);

        let mut segment_types = Vec::new();
        let mut checkpoint_serials = Vec::new();
        for action in &actions {
            if let ExportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Data {
                    segment_type,
                    checkpoint,
                    ..
                } = seg
                {
                    segment_types.push(segment_type);
                    if let Some(ckpt) = checkpoint {
                        checkpoint_serials.push(ckpt.serial);
                    }
                }
            }
        }

        // 5 segments: seg0=RedData, seg1=RedCheckpoint, seg2=RedData, seg3=RedCheckpoint, seg4=RedEob
        assert_eq!(segment_types.len(), 5);
        assert_eq!(segment_types[0], SegmentType::RedData);
        assert_eq!(segment_types[1], SegmentType::RedCheckpoint);
        assert_eq!(segment_types[2], SegmentType::RedData);
        assert_eq!(segment_types[3], SegmentType::RedCheckpoint);
        assert_eq!(segment_types[4], SegmentType::RedEob);

        // 3 checkpoints total: 2 intermediate + 1 EOB, each with unique serial
        assert_eq!(checkpoint_serials.len(), 3);
        assert_eq!(checkpoint_serials[0], 1); // intermediate
        assert_eq!(checkpoint_serials[1], 2); // intermediate
        assert_eq!(checkpoint_serials[2], 3); // EOB
    }

    #[test]
    fn intermediate_checkpoints_unique_serial_numbers() {
        // Verify each intermediate checkpoint gets a unique, strictly increasing serial
        let block = Bytes::from(vec![0xAB; 600]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 5,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 1, // every segment is a checkpoint
            max_checkpoints: None,
            green: false,
        };
        let (session, actions) = ExportSession::new(session_id(), block, 1, config);

        let mut checkpoint_serials = Vec::new();
        for action in &actions {
            if let ExportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Data {
                    checkpoint: Some(ckpt),
                    ..
                } = seg
                {
                    checkpoint_serials.push(ckpt.serial);
                }
            }
        }

        // 6 segments, all with checkpoints (5 intermediate + 1 EOB)
        assert_eq!(checkpoint_serials.len(), 6);
        // Verify strictly increasing
        for i in 1..checkpoint_serials.len() {
            assert!(
                checkpoint_serials[i] > checkpoint_serials[i - 1],
                "Checkpoint serial {} ({}) should be > {} ({})",
                i,
                checkpoint_serials[i],
                i - 1,
                checkpoint_serials[i - 1]
            );
        }
        // next_checkpoint_serial should be one past the last used
        assert_eq!(session.next_checkpoint_serial(), 7);
    }

    #[test]
    fn intermediate_checkpoint_final_segment_always_eob() {
        // Even when checkpoint_every_n would make the final segment an intermediate
        // checkpoint, it should still be RedEob
        let block = Bytes::from(vec![0xAB; 300]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 5,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 3, // every 3rd segment → index 2 would be checkpoint
            max_checkpoints: None,
            green: false,
        };
        let (_session, actions) = ExportSession::new(session_id(), block, 1, config);

        let mut segment_types = Vec::new();
        for action in &actions {
            if let ExportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Data { segment_type, .. } = seg {
                    segment_types.push(segment_type);
                }
            }
        }

        // 3 segments: seg0=RedData, seg1=RedData, seg2=RedEob (final always EOB)
        assert_eq!(segment_types.len(), 3);
        assert_eq!(segment_types[0], SegmentType::RedData);
        assert_eq!(segment_types[1], SegmentType::RedData);
        assert_eq!(segment_types[2], SegmentType::RedEob); // NOT RedCheckpoint
    }

    #[test]
    fn intermediate_checkpoint_discretionary_optimization_skips_retransmission() {
        // Requirement 27.3: When a report for an intermediate checkpoint shows
        // all bytes in its scope are acknowledged, skip retransmission.
        //
        // Setup: 400-byte block, 100-byte segments, checkpoint_every_n = 2
        // Segments: seg0(0-100)=RedData, seg1(100-200)=RedCheckpoint(serial 1),
        //           seg2(200-300)=RedData, seg3(300-400)=RedEob(serial 2)
        //
        // Simulate: receiver got all bytes 0-200, sends RS for intermediate
        // checkpoint (serial 1) with full coverage [0, 200).
        // The export session should NOT retransmit anything for this scope.
        let block = Bytes::from(vec![0xAB; 400]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 5,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 2,
            max_checkpoints: None,
            green: false,
        };
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // Report for intermediate checkpoint serial 1, scope [0, 200), fully acked
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 200,
        }];
        let actions = session.on_report(1, 1, 200, 0, &claims);

        // Should only have the Report-Ack, no retransmission segments
        assert_eq!(count_send_segments(&actions), 1); // Just the RAS
        assert_eq!(count_start_timers(&actions), 0);
        // Session should remain in AwaitingReport (waiting for final EOB report)
        assert_eq!(session.state(), ExportState::AwaitingReport);
    }

    #[test]
    fn intermediate_checkpoint_partial_ack_triggers_retransmission() {
        // When a report for an intermediate checkpoint has gaps, retransmit
        // the unclaimed bytes within that scope.
        let block = Bytes::from(vec![0xAB; 400]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 5,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 2,
            max_checkpoints: None,
            green: false,
        };
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // Report for intermediate checkpoint serial 1, scope [0, 200),
        // but only bytes 0-100 acknowledged (gap at 100-200)
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 100,
        }];
        let actions = session.on_report(1, 1, 200, 0, &claims);

        // Should have: 1 RAS + 1 retransmitted segment (bytes 100-200) with new checkpoint
        assert_eq!(count_send_segments(&actions), 2);
        assert_eq!(count_start_timers(&actions), 1);
        assert_eq!(session.state(), ExportState::AwaitingReport);

        // Verify the retransmitted segment covers the gap
        let mut data_segments = Vec::new();
        for action in &actions {
            if let ExportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Data { offset, data, .. } = &seg {
                    data_segments.push((*offset, data.len()));
                }
            }
        }
        assert_eq!(data_segments.len(), 1);
        assert_eq!(data_segments[0], (100, 100)); // gap at offset 100, length 100
    }

    #[test]
    fn intermediate_checkpoint_full_block_ack_completes_session() {
        // When the final EOB report comes in with full block coverage,
        // the session should transition to Complete.
        let block = Bytes::from(vec![0xAB; 400]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 5,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 2,
            max_checkpoints: None,
            green: false,
        };
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // First: intermediate checkpoint report with full scope ack
        let claims1 = vec![ReceptionClaim {
            offset: 0,
            length: 200,
        }];
        let _ = session.on_report(1, 1, 200, 0, &claims1);
        assert_eq!(session.state(), ExportState::AwaitingReport);

        // Second: final EOB report with full block ack
        let claims2 = vec![ReceptionClaim {
            offset: 0,
            length: 400,
        }];
        let actions = session.on_report(2, 2, 400, 0, &claims2);
        assert_eq!(session.state(), ExportState::Complete);
        // Only the RAS, no retransmission
        assert_eq!(count_send_segments(&actions), 1);
        assert_eq!(count_start_timers(&actions), 0);
    }

    #[test]
    fn intermediate_checkpoint_timers_started_for_each() {
        // Each intermediate checkpoint should have a timer started
        let block = Bytes::from(vec![0xAB; 500]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 5,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 2,
            max_checkpoints: None,
            green: false,
        };
        let (_session, actions) = ExportSession::new(session_id(), block, 1, config);

        // 5 segments: seg0=RedData, seg1=RedCheckpoint, seg2=RedData,
        //             seg3=RedCheckpoint, seg4=RedEob
        // 3 checkpoints → 3 timers
        assert_eq!(count_start_timers(&actions), 3);

        // Verify timer checkpoint serials
        let mut timer_serials: Vec<u64> = Vec::new();
        for action in &actions {
            if let ExportAction::StartTimer {
                checkpoint_serial, ..
            } = action
            {
                timer_serials.push(*checkpoint_serial);
            }
        }
        assert_eq!(timer_serials, vec![1, 2, 3]);
    }

    #[test]
    fn checkpoint_every_n_zero_means_only_eob_checkpoint() {
        // When checkpoint_every_n = 0, only the final segment gets a checkpoint
        let block = Bytes::from(vec![0xAB; 500]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 5,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 0,
            max_checkpoints: None,
            green: false,
        };
        let (_session, actions) = ExportSession::new(session_id(), block, 1, config);

        let mut segment_types = Vec::new();
        for action in &actions {
            if let ExportAction::SendSegment(wire) = action {
                let mut reader = &wire[..];
                let seg = segment::decode(&mut reader).unwrap();
                if let Segment::Data { segment_type, .. } = seg {
                    segment_types.push(segment_type);
                }
            }
        }

        // 5 segments: first 4 are RedData, last is RedEob
        assert_eq!(segment_types.len(), 5);
        for seg_type in segment_types.iter().take(4) {
            assert_eq!(*seg_type, SegmentType::RedData);
        }
        assert_eq!(segment_types[4], SegmentType::RedEob);
        // Only 1 timer (for the EOB checkpoint)
        assert_eq!(count_start_timers(&actions), 1);
    }

    // -----------------------------------------------------------------------
    // suspend_timers / resume_timers tests
    // -----------------------------------------------------------------------

    #[test]
    fn suspend_timers_returns_suspend_actions_for_active_timers() {
        let (mut session, _actions) = ExportSession::new(
            session_id(),
            Bytes::from(vec![0xAB; 200]),
            1,
            default_config(),
        );

        // Session should have 1 active timer (the EOB checkpoint)
        assert_eq!(session.active_timer_serials().len(), 1);
        assert!(!session.timers_suspended());

        let suspend_actions = session.suspend_timers();
        assert_eq!(suspend_actions.len(), 1);
        match &suspend_actions[0] {
            ExportAction::SuspendTimer {
                checkpoint_serial, ..
            } => {
                assert!(session.active_timer_serials().contains(checkpoint_serial));
            }
            _ => panic!("Expected SuspendTimer action"),
        }
        assert!(session.timers_suspended());
    }

    #[test]
    fn suspend_timers_double_suspend_is_noop() {
        let (mut session, _) = ExportSession::new(
            session_id(),
            Bytes::from(vec![0xAB; 200]),
            1,
            default_config(),
        );

        let first = session.suspend_timers();
        assert_eq!(first.len(), 1);

        // Second suspend should be a no-op
        let second = session.suspend_timers();
        assert!(second.is_empty());
    }

    #[test]
    fn suspend_timers_noop_when_complete() {
        let (mut session, _) = ExportSession::new(
            session_id(),
            Bytes::from(vec![0xAB; 200]),
            1,
            default_config(),
        );

        // Complete the session
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 200,
        }];
        let _ = session.on_report(1, 1, 200, 0, &claims);
        assert_eq!(session.state(), ExportState::Complete);

        let actions = session.suspend_timers();
        assert!(actions.is_empty());
    }

    #[test]
    fn suspend_timers_noop_when_cancelled() {
        let (mut session, _) = ExportSession::new(
            session_id(),
            Bytes::from(vec![0xAB; 200]),
            1,
            default_config(),
        );

        session.on_cancel_from_receiver(CancelReason::ByUser);
        assert_eq!(session.state(), ExportState::Cancelled);

        let actions = session.suspend_timers();
        assert!(actions.is_empty());
    }

    #[test]
    fn suspend_timers_multiple_active_timers() {
        // Use intermediate checkpoints to get multiple active timers
        let block = Bytes::from(vec![0xAB; 400]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 5,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 2,
            max_checkpoints: None,
            green: false,
        };
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // With checkpoint_every_n=2 and 4 segments, we get:
        // seg0: RedData, seg1: RedCheckpoint (serial 1), seg2: RedData, seg3: RedEob (serial 2)
        // So 2 active timers
        assert_eq!(session.active_timer_serials().len(), 2);

        let suspend_actions = session.suspend_timers();
        assert_eq!(suspend_actions.len(), 2);

        for action in &suspend_actions {
            match action {
                ExportAction::SuspendTimer { .. } => {}
                _ => panic!("Expected SuspendTimer action, got {:?}", action),
            }
        }
    }

    #[test]
    fn resume_timers_returns_resume_actions() {
        let (mut session, _) = ExportSession::new(
            session_id(),
            Bytes::from(vec![0xAB; 200]),
            1,
            default_config(),
        );

        // Suspend first
        let suspend_actions = session.suspend_timers();
        assert_eq!(suspend_actions.len(), 1);

        let checkpoint_serial = match &suspend_actions[0] {
            ExportAction::SuspendTimer {
                checkpoint_serial, ..
            } => *checkpoint_serial,
            _ => panic!("Expected SuspendTimer"),
        };

        // Resume with remaining duration
        let remaining = Duration::from_secs(15);
        let resume_actions = session.resume_timers(&[(checkpoint_serial, remaining)]);
        assert_eq!(resume_actions.len(), 1);

        match &resume_actions[0] {
            ExportAction::ResumeTimer {
                checkpoint_serial: serial,
                remaining: dur,
            } => {
                assert_eq!(*serial, checkpoint_serial);
                assert_eq!(*dur, remaining);
            }
            _ => panic!("Expected ResumeTimer action"),
        }

        // Should no longer be suspended
        assert!(!session.timers_suspended());
    }

    #[test]
    fn resume_timers_skips_unknown_serials() {
        let (mut session, _) = ExportSession::new(
            session_id(),
            Bytes::from(vec![0xAB; 200]),
            1,
            default_config(),
        );

        session.suspend_timers();

        // Try to resume with a serial that doesn't exist
        let resume_actions = session.resume_timers(&[(999, Duration::from_secs(10))]);
        assert!(resume_actions.is_empty());
        assert!(!session.timers_suspended());
    }

    #[test]
    fn resume_timers_noop_when_not_suspended() {
        let (mut session, _) = ExportSession::new(
            session_id(),
            Bytes::from(vec![0xAB; 200]),
            1,
            default_config(),
        );

        // Don't suspend first — resume should be a no-op
        let resume_actions = session.resume_timers(&[(1, Duration::from_secs(10))]);
        assert!(resume_actions.is_empty());
    }

    #[test]
    fn resume_timers_noop_when_cancelled() {
        let (mut session, _) = ExportSession::new(
            session_id(),
            Bytes::from(vec![0xAB; 200]),
            1,
            default_config(),
        );

        session.suspend_timers();
        session.on_cancel_from_receiver(CancelReason::ByUser);

        // Session cancelled between suspend and resume — should be no-op
        let resume_actions = session.resume_timers(&[(1, Duration::from_secs(10))]);
        assert!(resume_actions.is_empty());
    }

    #[test]
    fn resume_timers_partial_match() {
        // Create session with multiple timers
        let block = Bytes::from(vec![0xAB; 400]);
        let config = ExportConfig {
            max_segment_size: 100,
            max_retransmissions: 5,
            retransmit_timeout: Duration::from_secs(30),
            checkpoint_every_n: 2,
            max_checkpoints: None,
            green: false,
        };
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        let serials: Vec<u64> = session.active_timer_serials().iter().copied().collect();
        assert_eq!(serials.len(), 2);

        session.suspend_timers();

        // Resume with one valid serial and one invalid
        let resume_input: Vec<(u64, Duration)> = vec![
            (serials[0], Duration::from_secs(10)),
            (999, Duration::from_secs(5)), // invalid serial
        ];
        let resume_actions = session.resume_timers(&resume_input);

        // Only the valid serial should produce a ResumeTimer action
        assert_eq!(resume_actions.len(), 1);
        match &resume_actions[0] {
            ExportAction::ResumeTimer {
                checkpoint_serial,
                remaining,
            } => {
                assert_eq!(*checkpoint_serial, serials[0]);
                assert_eq!(*remaining, Duration::from_secs(10));
            }
            _ => panic!("Expected ResumeTimer"),
        }
    }

    #[test]
    fn suspend_timers_green_session_is_noop() {
        let block = Bytes::from(vec![0xAB; 200]);
        let config = green_config();
        let (mut session, _) = ExportSession::new(session_id(), block, 1, config);

        // Green sessions are immediately Complete, so suspend is a no-op
        assert_eq!(session.state(), ExportState::Complete);
        let actions = session.suspend_timers();
        assert!(actions.is_empty());
    }

    #[test]
    fn active_timers_cleared_on_full_ack() {
        let (mut session, _) = ExportSession::new(
            session_id(),
            Bytes::from(vec![0xAB; 200]),
            1,
            default_config(),
        );

        assert!(!session.active_timer_serials().is_empty());

        // Fully acknowledge
        let claims = vec![ReceptionClaim {
            offset: 0,
            length: 200,
        }];
        let _ = session.on_report(1, 1, 200, 0, &claims);
        assert_eq!(session.state(), ExportState::Complete);
        assert!(session.active_timer_serials().is_empty());
    }

    #[test]
    fn active_timers_cleared_on_cancel() {
        let (mut session, _) = ExportSession::new(
            session_id(),
            Bytes::from(vec![0xAB; 200]),
            1,
            default_config(),
        );

        assert!(!session.active_timer_serials().is_empty());
        session.on_cancel_from_receiver(CancelReason::ByUser);
        assert!(session.active_timer_serials().is_empty());
    }

    #[test]
    fn timer_expired_removes_from_active_set() {
        let (mut session, _) = ExportSession::new(
            session_id(),
            Bytes::from(vec![0xAB; 200]),
            1,
            default_config(),
        );

        // The initial checkpoint serial is 1
        assert!(session.active_timer_serials().contains(&1));

        // Timer expires for serial 1
        let _ = session.on_timer_expired(1);

        // Serial 1 should no longer be active (but a new one was created)
        assert!(!session.active_timer_serials().contains(&1));
        // A new timer was started for the retransmission checkpoint
        assert!(!session.active_timer_serials().is_empty());
    }
}
