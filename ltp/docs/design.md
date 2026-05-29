# Design: LTP Protocol Engine

| Document Info | Details |
| ----- | ----- |
| **Project** | Hardy DTN Router |
| **Crate** | `hardy-ltp` |
| **Repository** | `github.com/ricktaylor/hardy` |
| **RFCs** | RFC 5326 (LTP), RFC 5050 (SDNV) |
| **Status** | Implemented |

## 1. Overview

This document describes the design of the `hardy-ltp` protocol engine library. The crate implements the Licklider Transmission Protocol (RFC 5326) as a standalone library with no dependency on `hardy-bpa`, making it independently testable and reusable outside the Hardy BPA context.

LTP is a point-to-point retransmission protocol designed for high-latency, lossy links such as deep-space communications. It splits data blocks into segments transmitted over an unreliable link, using selective acknowledgement (via reception reports) and timer-driven retransmission to achieve reliability for "red" data while allowing best-effort delivery for "green" data.

### 1.1. Motivation

The protocol engine is separated from the CLA integration layer to provide:

- **Independent testability** — Session state machines are pure functions that can be exhaustively tested with property-based testing without mocking I/O
- **Reusability** — The crate can be used by other applications that need LTP without pulling in the full BPA
- **Follows precedent** — Mirrors the `bpv7`/`bpa` split already established in the workspace

### 1.2. Design Principles

- **No BPA dependency** — The protocol engine is a pure library; it does not spawn tasks, bind sockets, or interact with the BPA. All I/O is delegated to the caller.
- **Action-based interface** — Session methods return `Vec<Action>` describing what the caller should do (send a segment, start a timer, deliver a block), rather than performing side effects directly.
- **Minimal dependencies** — Only `bytes`, `thiserror`, `tokio` (for `time` types), and `tracing`.
- **Property-based testing** — Correctness properties are validated using `proptest` for all codec and session logic.

### 1.3. Reference Implementations

Where applicable, this design draws on:

- **ION-DTN** (NASA JPL) — `ltp/library/libltpP.c`, `ltpP.h`, `ltpmeter.c`
- **HDTN** (NASA GRC) — `common/ltp/src/LtpSessionSender.cpp`, `LtpSessionReceiver.cpp`

## 2. Source Layout

```
ltp/
  Cargo.toml
  src/
    lib.rs              # Public re-exports
    sdnv.rs             # SDNV encode/decode with pre-caching
    segment.rs          # Segment type enum, wire encode/decode
    session/
      mod.rs            # SessionId, shared types, Action enum
      export.rs         # Export (sender) session state machine
      import.rs         # Import (receiver) session state machine
  tests/
    sdnv_properties.rs      # Property tests for SDNV codec
    segment_properties.rs   # Property tests for segment codec
    export_properties.rs    # Property tests for export sessions
    import_properties.rs    # Property tests for import sessions
```

## 3. Key Design Decisions

### 3.1. Action-Based I/O

Session logic is completely separated from I/O. The export and import session structs are pure state machines — you call methods like `on_report()` or `on_data_segment()` and they return a `Vec<Action>` describing what the caller should do. This:

- Enables deterministic property-based testing without mocking
- Makes the protocol logic easy to reason about and audit
- Allows the CLA layer to batch, reorder, or rate-limit actions as needed

### 3.2. Extent Map with Adjacent-Merge

Received byte ranges are tracked in a `BTreeMap<u64, u64>` (`start → end`). On each insert, adjacent and overlapping extents are merged to keep the map compact. This mirrors ION's `redSegments` list and provides O(log n) operations.

### 3.3. Report Claim Offset Convention

Reception claim offsets on the wire are relative to the RS `lower_bound`, not the block start (ION `serializeReportSegment`). The decoder reconstructs absolute offsets by adding `lower_bound`. The encoder subtracts `lower_bound` when serializing. This is a critical detail that differs from a naive reading of RFC 5326.

### 3.4. SDNV Pre-caching

Following ION's approach, the `CachedSdnv` type pre-encodes a value at construction time, storing the wire bytes for zero-allocation serialization on the transmit path. Frequently-used values (engine IDs, session numbers) benefit from this.

## 4. Background: LTP Protocol

### 4.1. Concepts

| Concept | Description |
| --- | --- |
| **Engine ID** | 64-bit numeric identity of an LTP endpoint |
| **Export session** | A sender-side session identified by `(sender_engine_id, session_number)` |
| **Import session** | A receiver-side session tracking data arriving from a remote engine |
| **Block** | The complete service data unit being transferred |
| **Segment** | A fixed-size unit of transmission; one UDP datagram carries exactly one segment |
| **Red data** | Reliably delivered portion; subject to acknowledgement and retransmission |
| **Green data** | Best-effort portion; no acknowledgement, no retransmission |
| **Checkpoint** | A red-data segment that requires a Report Segment in response |
| **Report Segment (RS)** | Receiver acknowledgement listing claimed byte-range extents |
| **Report-Ack (RAS)** | Sender acknowledgement of an RS |
| **Cancel Segment** | Abort a session from the sender (CS) or receiver (CR) side |

### 4.2. Segment Types (RFC 5326 §3.2)

| Value | Name | Description |
| --- | --- | --- |
| 0 | `RedData` | Red-data, not a checkpoint |
| 1 | `RedCheckpoint` | Red-data, checkpoint |
| 2 | `RedEorp` | Red-data, End-of-Red-Part checkpoint |
| 3 | `RedEob` | Red-data, End-of-Block checkpoint |
| 4 | `GreenData` | Green-data, not EOB |
| 7 | `GreenEob` | Green-data, End-of-Block |
| 8 | `Report` | Report segment |
| 9 | `ReportAck` | Report-ack segment |
| 12 | `CancelFromSender` | Cancel by block sender |
| 13 | `CancelAckToSender` | Cancel-ack to sender |
| 14 | `CancelFromReceiver` | Cancel by block receiver |
| 15 | `CancelAckToReceiver` | Cancel-ack to receiver |

### 4.3. Cancel Reason Codes

| Value | Name | Meaning |
| --- | --- | --- |
| 0 | `CancelByUser` | Application requested cancellation |
| 1 | `ClientServiceUnreachable` | No registered client for the service ID |
| 2 | `RetransmitLimitExceeded` | Max retransmissions exhausted |
| 3 | `MiscolouredSegment` | Segment type inconsistent with session colour |
| 4 | `CancelByEngine` | Internal engine error (stale sessions, red data overflow, limits) |

### 4.4. SDNV Encoding

Self-Delimiting Numeric Values (RFC 5050 §4.1) encode arbitrary `u64` values. Each byte contributes 7 bits of value; the MSB is a continuation flag (1 = more bytes follow, 0 = final byte). Bytes are written big-endian.

| Value range | Encoded length |
| --- | --- |
| 0–127 | 1 byte |
| 128–16383 | 2 bytes |
| 16384–2097151 | 3 bytes |
| 0–u64::MAX | up to 10 bytes |

### 4.5. Segment Wire Format

**Header (all segments):**
```
byte  0:    (version=0 << 4) | segment_type_code
SDNV:       sender engine ID
SDNV:       session number
byte:       (header_ext_count << 4) | trailer_ext_count
[header extensions — skipped]
```

**Data segment body:**
```
SDNV: client service ID (always 1 for BP)
SDNV: data offset within block
SDNV: data length
[if checkpoint type (1, 2, 3):]
  SDNV: checkpoint serial number
  SDNV: responding report serial number
[raw data bytes]
[trailer extensions — skipped]
```

**Report segment body:**
```
SDNV: report serial number
SDNV: checkpoint serial number being acknowledged
SDNV: upper bound (exclusive)
SDNV: lower bound (inclusive)
SDNV: reception claim count
for each claim:
  SDNV: claim offset (relative to lower bound)
  SDNV: claim length
```

**Report-ack body:** `SDNV: report serial number`

**Cancel body:** `byte: reason code`

**Cancel-ack body:** empty (header only)

### 4.6. RS Claim Limit

Claims per RS are capped at 20 (ION `MAX_CLAIMS_PER_RS`). When more disjoint extents exist, multiple RS segments are emitted for the same checkpoint, each covering a different window.

## 5. Module Design: SDNV Codec (`sdnv.rs`)

### 5.1. API

```rust
/// Returns the number of bytes needed to encode `value`.
pub fn encoded_len(value: u64) -> usize;

/// Encodes `value` as an SDNV, appending to `buf`.
pub fn encode(value: u64, buf: &mut BytesMut);

/// Decodes an SDNV from `buf`, advancing the cursor.
pub fn decode(buf: &mut impl Buf) -> Result<u64, SdnvError>;
```

### 5.2. Error Handling

| Error | Condition |
| --- | --- |
| `Truncated` | Buffer exhausted before the final byte (MSB=0) |
| `Overflow` | Decoded value would exceed `u64::MAX` (more than 10 continuation bytes) |

### 5.3. CachedSdnv

Pre-encodes a value at construction time for zero-allocation serialization:

```rust
pub struct CachedSdnv { /* pre-encoded bytes */ }

impl CachedSdnv {
    pub fn new(value: u64) -> Self;
    pub fn put(&self, buf: &mut BytesMut);
}
```

## 6. Module Design: Segment Codec (`segment.rs`)

### 6.1. API

```rust
/// Decodes a complete segment from wire bytes.
pub fn decode(buf: &mut impl Buf) -> Result<Segment, SegmentError>;

/// Encodes a segment to wire bytes.
pub fn encode(segment: &Segment, buf: &mut BytesMut);

/// Returns the encoded size of a segment without allocating.
pub fn encoded_size(segment: &Segment) -> usize;
```

### 6.2. Segment Enum

```rust
pub enum Segment {
    Data {
        session_id: SessionId,
        segment_type: SegmentType,
        client_service_id: u64,
        offset: u64,
        data: Bytes,
        checkpoint: Option<CheckpointInfo>,
    },
    Report { session_id, report_serial, checkpoint_serial, upper_bound, lower_bound, claims },
    ReportAck { session_id, report_serial },
    Cancel { session_id, direction, reason },
    CancelAck { session_id, direction },
}
```

## 7. Module Design: Export Session (`session/export.rs`)

### 7.1. States

```
Sending → AwaitingReport → Complete
                         → Cancelled
```

### 7.2. Construction

An export session is created with a data block (`Bytes`) and configuration. The constructor immediately segments the block and returns `SendSegment` actions for all segments plus `StartTimer` actions for checkpoints.

### 7.3. Segmentation

The block is split into segments no larger than `max_segment_size`:
- All segments except the last: `RedData` (0)
- Last segment: `RedEob` (3) — mandatory checkpoint
- If `checkpoint_every_n > 0`: every Nth segment is `RedCheckpoint` (1)
- Green mode: `GreenData` (4) / `GreenEob` (7), no checkpoints or timers

### 7.4. Report Handling (`on_report`)

1. Send a `ReportAck` action
2. Merge claimed byte ranges into the acknowledged set
3. If all bytes acknowledged → `Complete`
4. Otherwise → retransmit unclaimed bytes with a new checkpoint serial, reset retry count

### 7.5. Timer Expiry (`on_timer_expired`)

1. Increment retry count
2. If retry count > `max_retransmissions` → cancel session
3. Otherwise → retransmit all unclaimed bytes with a new checkpoint serial

### 7.6. Intermediate Checkpoint Optimization

When a report for an intermediate checkpoint shows all bytes in its scope already acknowledged, no retransmission is triggered (discretionary checkpoint optimization from HDTN).

### 7.7. Max Checkpoints Limit

When the total checkpoints sent reaches the configured limit, the session is cancelled rather than sending more.

## 8. Module Design: Import Session (`session/import.rs`)

### 8.1. States

```
Receiving → Complete
          → Cancelled
```

### 8.2. Extent Tracking

`BTreeMap<u64, u64>` (`start → end`) with adjacent-merge on insert.

### 8.3. Data Segment Handling (`on_data_segment`)

1. Validate colour consistency (red vs green)
2. Validate client service ID (must be 1)
3. Check max red data size limit
4. Record the byte extent
5. If checkpoint: generate report(s)
6. If EOB and all bytes received: deliver block

### 8.4. Report Generation

Up to 20 claims per RS. Multiple RS segments emitted for the same checkpoint when more claims are needed. Claim offsets stored as absolute values internally, converted to lower-bound-relative on encoding.

### 8.5. Deferred Report Sending

When `defer_report_ms > 0`:
1. Checkpoint scope fully covered → send immediately
2. Otherwise → start deferral timer
3. Gap-filling segment → reset timer
4. All gaps filled → send immediately
5. Timer expiry → send with current coverage

### 8.6. Asynchronous Reception Report

When all red-data extents are received by a non-checkpoint segment, an unsolicited RS is sent covering the full scope with checkpoint serial zero.

### 8.7. Colour Validation

Session colour established by first data segment. Mismatched segments trigger cancel with `MiscolouredSegment`.

### 8.8. Client Service ID Validation

First segment establishes expected ID. Mismatched subsequent segments trigger cancel with `ClientServiceUnreachable`.

### 8.9. Max Red Data Size

`offset + length > max_red_data_bytes` → cancel with `CancelByEngine`.

### 8.10. Max Reports Limit

Total RS count reaches limit → cancel session.

### 8.11. Green Data Delivery

Block delivered immediately on `GreenEob` regardless of gaps. No reports generated.

## 9. Action Types

### 9.1. Export Actions

| Action | Description |
| --- | --- |
| `SendSegment(Bytes)` | Transmit this wire-encoded segment |
| `StartTimer { serial, duration }` | Start a retransmission timer |
| `CancelTimer { serial }` | Cancel a previously started timer |
| `Complete` | Session completed successfully |
| `Cancelled { reason }` | Session was cancelled |

### 9.2. Import Actions

| Action | Description |
| --- | --- |
| `SendSegment(Bytes)` | Transmit this wire-encoded segment (RS, CancelAck) |
| `DeliverBlock(Bytes)` | Deliver the reassembled block |
| `StartTimer { serial, duration }` | Start a report retransmission timer |
| `CancelTimer { serial }` | Cancel a previously started timer |
| `Cancelled { reason }` | Session was cancelled |
| `StartDeferralTimer { duration }` | Start a deferred report timer |
| `ResetDeferralTimer { duration }` | Reset the deferred report timer |
| `CancelDeferralTimer` | Cancel the deferred report timer |
| `StartInactivityTimer { duration }` | Start/reset the inactivity timer |

## 10. Testing & Correctness Properties

### 10.1. Test Suite Summary

| Category | Count | Description |
|----------|-------|-------------|
| Unit tests | 193 | SDNV, segment codec, export/import session logic |
| Property tests (SDNV) | 3 | Round-trip, truncation, overflow |
| Property tests (segment) | 3 | Round-trip, claim offsets, decode safety |
| Property tests (export) | 4 | Segmentation, checkpoint monotonicity, retransmission, intermediate checkpoints |
| Property tests (import) | 7 | Extent map, block delivery, reports, max red data, client service ID |

### 10.2. Correctness Properties

**SDNV:**
- `decode(encode(v)) == v` for all `u64` values
- Truncated buffers produce `Err(Truncated)`
- Values requiring >10 bytes produce `Err(Overflow)`

**Segment codec:**
- `decode(encode(seg)) == seg` for all valid segments
- Claims correctly relativized to lower bound
- Arbitrary byte sequences produce `Ok` or `Err`, never panic

**Export session:**
- Concatenating data segment payloads in offset order = original block
- Checkpoint serials strictly increase within a session
- Retransmitted segments cover exactly the unclaimed byte ranges
- Every Nth segment marked as checkpoint when configured

**Import session:**
- Extents always non-overlapping and sorted
- Complete segment set delivers the original block
- Reports have ≤20 claims and cover the checkpoint scope
- Sessions exceeding max red data are cancelled
- Mismatched client service IDs trigger cancellation

## 11. Performance Characteristics

- **O(log n) extent operations** — BTreeMap-based extent map
- **Zero-allocation encoding** — CachedSdnv for frequent values
- **No heap allocation on decode** — Data payloads extracted as `Bytes` slices (O(1) reference-counted split)
- **Deterministic memory** — No unbounded growth; extent merging keeps map compact

## 12. Dependencies

```toml
[dependencies]
bytes     = "1"
thiserror = { version = "2", default-features = false }
tokio     = { version = "1", features = ["time"] }
tracing   = { version = "0.1", default-features = false }

[dev-dependencies]
proptest  = "1"
```

## 13. TVR Integration (Timer Suspension/Resumption)

The LTP engine supports RFC 5326 §6.5/§6.6 timer suspension and resumption for integration with Hardy's Time-Variant Routing (TVR) agent. When a scheduled contact window closes, the CLA layer calls `suspend_timers()` on active export sessions; when the window reopens, it calls `resume_timers()` with the recorded remaining durations.

### 13.1. Action Types

| Action | Description |
| --- | --- |
| `SuspendTimer { checkpoint_serial }` | Suspend the timer for this checkpoint; caller records remaining duration |
| `ResumeTimer { checkpoint_serial, remaining }` | Resume a suspended timer with the given remaining duration |

### 13.2. Session State

The `ExportSession` tracks:
- `active_timers: HashSet<u64>` — checkpoint serials with running timers
- `timers_suspended: bool` — prevents double-suspend and gates resume

### 13.3. Invariants

- `suspend_timers()` is idempotent (no-op if already suspended or session is complete/cancelled)
- `resume_timers()` skips checkpoint serials no longer in `active_timers` (handles cancellation during suspension)
- Green sessions have no timers and are unaffected by suspend/resume

## 14. Out of Scope (Future Work)

- **LTP extensions** — Header/trailer extensions are parsed and skipped but not processed
- **Security extensions** — LTP authentication (RFC 5327) is not implemented
- **Mixed red/green blocks** — Currently a block is entirely red or entirely green
- **Multiple client service IDs** — Only ID 1 (Bundle Protocol) is supported

## 15. References

| Reference | Title |
| --- | --- |
| RFC 5326 | Licklider Transmission Protocol — Specification |
| RFC 5050 | Bundle Protocol Specification (SDNV definition, §4.1) |
| ION `ltp/library/ltpP.h` | Session structures, segment types, cancel reasons |
| ION `ltp/library/libltpP.c` | Segment serialization, closed-export logic, extent tracking |
| HDTN `common/ltp/src/LtpSessionSender.cpp` | Intermediate checkpoints, discretionary optimization |
| HDTN `common/ltp/src/LtpSessionReceiver.cpp` | Deferred reports, async reports, max red data protection |
