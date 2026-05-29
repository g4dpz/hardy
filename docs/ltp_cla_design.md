# Design: LTP Convergence Layer Adapter

| Document Info | Details |
| ----- | ----- |
| **Project** | Hardy DTN Router |
| **Repository** | `github.com/ricktaylor/hardy` |
| **Status** | Proposed |

## 1. Overview

This document describes the design for adding the Licklider Transmission Protocol (LTP, RFC 5326) to the Hardy workspace as a Convergence Layer Adapter (CLA). LTP is a point-to-point retransmission protocol designed for high-latency, lossy links such as deep-space communications, where round-trip times may be measured in minutes or hours.

### 1.1. Why LTP is Different

Unlike TCPCLv4, LTP cannot be implemented as a thin transport adapter. It is a complete reliability protocol that must be fully embedded within the CLA:

| Aspect | TCPCLv4 | LTP |
| --- | --- | --- |
| **Reliability** | Delegated to TCP | Implemented in the LTP engine itself |
| **Model** | Continuous byte stream | Session-based blocks |
| **Framing** | Stream framing (tokio-util codec) | Datagram — one UDP datagram per segment |
| **Connection** | Persistent per-peer TCP connection | Connectionless; sessions are ephemeral |
| **Suitability** | Low-delay terrestrial networks | High-delay, disrupted, or asymmetric links |

The LTP CLA therefore contains a **full embedded transport stack**, not just an adapter.

### 1.2. Crate Split

The work is split into two crates following the existing project convention:

- **`ltp/`** — protocol engine: segment encoding/decoding, session state machines, SDNV codec
- **`ltp-cla/`** — BPA convergence layer adapter: implements `hardy_bpa::cla::Cla`, UDP transport, span management

Where applicable this design draws on the ION-DTN reference implementation (NASA JPL, Scott Burleigh) — in particular `ltp/library/libltpP.c`, `ltp/library/ltpP.h`, and `ltp/udp/udplso.c` — as an authoritative guide to the segment wire format and session management decisions.

### 1.3. Architecture

```
┌─────────────────────────────────────────────┐
│              Hardy BPA                      │
└────────────────────┬────────────────────────┘
                     │ hardy_bpa::cla::{Cla, Sink}
                     ▼
┌─────────────────────────────────────────────┐
│              LTP CLA (ltp-cla)              │
│  ┌───────────────────────────────────────┐  │
│  │  CLA Adapter  (cla.rs)               │  │
│  │  · on_register / on_unregister       │  │
│  │  · forward() → aggregation buffer   │  │
│  │  · sink.dispatch() ← completed block│  │
│  └──────────────┬────────────────────────┘  │
│                 │                           │
│  ┌──────────────▼────────────────────────┐  │
│  │  LTP Engine  (engine.rs + ltp crate) │  │
│  │  · Export session manager            │  │
│  │  · Import session manager            │  │
│  │  · Retransmission timer system       │  │
│  │  · Closed-export retention           │  │
│  └──────────────┬────────────────────────┘  │
│                 │                           │
│  ┌──────────────▼────────────────────────┐  │
│  │  UDP Transport  (engine.rs)           │  │
│  │  · Single UdpSocket, port 1113        │  │
│  │  · Per-span token-bucket rate control │  │
│  └───────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
                     │
                   Network
```

### 1.4. BPA ↔ LTP Operation Mapping

| BPA operation | LTP action |
| --- | --- |
| `forward(address, bundle)` | Append bundle to span aggregation buffer; flush as new export session when limit reached |
| `sink.dispatch(bundle, peer, addr)` | Called when an import session delivers a complete block; bundles are unpacked from length-prefixed block |
| `sink.add_peer(addr, node_ids)` | Called at startup for each pre-configured span; also when a new remote engine is first seen |
| `sink.remove_peer(addr)` | Called when a span is administratively removed |

## 2. Background: LTP Protocol

### 2.1. Concepts

| Concept | Description |
| --- | --- |
| **Engine ID** | 64-bit numeric identity of an LTP endpoint, conventionally equal to the IPN node number (RFC 9173) |
| **Export session** | A sender-side session (ION terminology: `ExportSession`), identified by `(sender_engine_id, session_number)` |
| **Import session** | A receiver-side session (ION terminology: `ImportSession`) tracking data arriving from a remote engine |
| **Block** | The complete service data unit being transferred; may aggregate multiple bundles |
| **Segment** | A fixed-size unit of transmission; one UDP datagram carries exactly one segment |
| **Red data** | Reliably delivered portion of a block; subject to acknowledgement and retransmission |
| **Green data** | Best-effort portion; no acknowledgement, no retransmission |
| **Checkpoint** | A red-data segment that requires a Report Segment in response; carries a `ckpt_serial_nbr` |
| **Report Segment (RS)** | Receiver acknowledgement listing claimed byte-range extents relative to the RS lower bound |
| **Report-Ack Segment (RAS)** | Sender acknowledgement that it has received an RS |
| **Cancel Segment (CS/CR)** | Abort a session from the sender (CS) or receiver (CR) side |
| **Span** | Pre-configured link to a remote engine: UDP address, MTU, timer values, session limits |
| **Aggregation** | Multiple bundles may be combined into a single LTP block, controlled by size and time thresholds |
| **Client service ID** | Numeric identifier for the upper-layer consumer; always `1` for the Bundle Protocol |

### 2.2. Segment Types (RFC 5326 §3.2)

ION uses these symbolic names in `LtpSegmentTypeCode`:

| Value | ION symbol | Description |
| --- | --- | --- |
| 0 | `LtpDsRed` | Red-data, not a checkpoint |
| 1 | `LtpDsRedCheckpoint` | Red-data, checkpoint |
| 2 | `LtpDsRedEORP` | Red-data, End-of-Red-Part checkpoint |
| 3 | `LtpDsRedEOB` | Red-data, End-of-Block checkpoint |
| 4 | `LtpDsGreen` | Green-data, not EOB |
| 7 | `LtpDsGreenEOB` | Green-data, End-of-Block |
| 8 | `LtpRS` | Report segment |
| 9 | `LtpRAS` | Report-ack segment |
| 12 | `LtpCS` | Cancel by block sender |
| 13 | `LtpCAS` | Cancel-ack to sender |
| 14 | `LtpCR` | Cancel by block receiver |
| 15 | `LtpCAR` | Cancel-ack to receiver |

### 2.3. Cancel Reason Codes

From ION `LtpCancelReasonCode`:

| Value | Meaning |
| --- | --- |
| 0 | `LtpCancelByUser` — application requested cancellation |
| 1 | `LtpClientSvcUnreachable` — no registered client for the service ID |
| 2 | `LtpRetransmitLimitExceeded` — max retransmissions exhausted |
| 3 | `LtpMiscoloredSegment` — segment type inconsistent with session colour |
| 4 | `LtpCancelByEngine` — internal engine error (also used for stale sessions, red data overflow, and report/checkpoint limits) |

### 2.4. SDNV Encoding

LTP encodes all integer fields using Self-Delimiting Numeric Values (SDNVs, RFC 5050 §4.1). Each byte contributes 7 bits of value; the most-significant bit is a continuation flag. The `ltp` crate must provide encode and decode functions for this format.

ION pre-caches frequently-used SDNVs (e.g., the local engine ID, session numbers) at construction time to avoid repeated encoding on the transmit path. The `ltp` crate should do the same.

### 2.5. Segment Wire Format

Derived from `serializeHeader`, `serializeDataSegment`, `serializeReportSegment`, and related functions in ION's `libltpP.c`:

**Header (all segment types):**

```
byte  0:    (version=0 << 4) | segment_type_code
SDNV  1..n: sender engine ID
SDNV  n..m: session number
byte  m+1:  (header_ext_count << 4) | trailer_ext_count
[header extensions]
```

**Data segment body** (after header):
```
SDNV: client service ID  (always 1 for BP)
SDNV: data offset within block
SDNV: data length
[if segment type in {1,2,3} — checkpoint]
  SDNV: checkpoint serial number
  SDNV: responding report serial number (0 for first checkpoint)
[raw data bytes]
[trailer extensions]
```

**Report segment body:**
```
SDNV: report serial number
SDNV: checkpoint serial number being acknowledged
SDNV: upper bound (exclusive, relative to block start)
SDNV: lower bound (inclusive, relative to block start)
SDNV: reception claim count
for each claim:
  SDNV: claim offset (relative to lower bound, NOT block start)  ← ION detail
  SDNV: claim length
```

**Report-ack segment body:**
```
SDNV: report serial number
```

**Cancel segment body (CS or CR):**
```
byte: reason code  (LtpCancelReasonCode)
```

Cancel-ack segments (CAS, CAR) have no body beyond the header.

> **Important:** Reception claim offsets in an RS are relative to the RS `lower_bound`, not to the block start. ION compresses them in `serializeReportSegment` at serialization time only; internally it stores absolute offsets. The decoder must add `lower_bound` back when storing received claims.

### 2.6. RS Claim Limit

ION caps claims per RS at `MAX_CLAIMS_PER_RS = 20`. When the receiver has more disjoint extents than this, it emits multiple RS segments for the same checkpoint, each covering a different window of the block (using different `lower_bound`/`upper_bound` values). The design should replicate this limit.

## 3. Crate: `ltp`

### 3.1. Purpose

A protocol-only library with no dependency on `hardy-bpa`. This allows the LTP engine to be used independently of the BPA if required, and keeps the protocol logic separately testable.

### 3.2. Source Layout

```
ltp/
  Cargo.toml
  src/
    lib.rs          # Public re-exports
    sdnv.rs         # SDNV encode / decode; pre-caching of frequent values
    segment.rs      # Segment type enum, wire encode/decode (pure Bytes, no SDR)
    session/
      mod.rs        # SessionId type, shared timer types
      export.rs     # Export (sender) session state machine
      import.rs     # Import (receiver) session state machine
```

### 3.3. Dependencies

```toml
[dependencies]
bytes     = "1"
thiserror = "2"
tokio     = { version = "1", features = ["time"] }
tracing   = { version = "0.1", default-features = false }
```

### 3.4. Session State Machines

ION names the two sides **ExportSession** (sender) and **ImportSession** (receiver). This terminology is used throughout.

**ExportSession** states: `Sending → AwaitingReport → Retransmitting → Complete | Cancelled`

- Transmits red-data segments sized to the span MTU. The last segment carries type `LtpDsRedEOB` (3).
- Checkpoint serial numbers start at a random value and increment on each retransmission.
- On RS receipt: sends a RAS, notes claimed byte ranges, retransmits any unclaimed bytes within the RS window.
- On retransmit timer expiry: resends all unclaimed checkpoints; increments the retry counter.
- After `max_retransmissions` exhausted: sends CS with reason `LtpRetransmitLimitExceeded`, moves to `Cancelled`.

**ImportSession** states: `Receiving → Reporting → Complete | Cancelled`

- Tracks received byte extents in a `BTreeMap<u64, u64>` (`offset → end`) with adjacent-extent merging on insert. This mirrors ION's `redSegments` list.
- On each checkpoint: generates one or more RS segments capped at `MAX_CLAIMS_PER_RS = 20` claims, spanning different `lower_bound`/`upper_bound` windows. Claim offsets on the wire are relative to `lower_bound`, not the block start (ION `serializeReportSegment`); the decoder must add `lower_bound` back to recover absolute offsets.
- On RAS receipt: cancels the corresponding RS retransmit timer. When all extents up to EORP upper bound are covered, the block is delivered.
- On CS receipt: sends CAS and discards the session.

**Closed-export retention (ION `closedExports`):** Completed export sessions are retained for `2 × max_retransmissions × retransmit_cycle_secs + 10` seconds. Any RS arriving for a closed session within this window receives a RAS; this prevents the remote receiver from looping indefinitely when the final RAS was lost. Each closed export has a **response limit** (initialized to `max_retransmissions`); once the limit is exhausted the closed export is discarded early, preventing a misbehaving receiver from keeping it alive indefinitely (ION `ClosedExport.responseLimit`).

**Miscoloured segment detection:** A session established by a red-data segment must only receive red segments. A green segment in a red session (or vice versa) triggers a CR with reason `LtpMiscoloredSegment`.

**Stale import session cleanup (ION `LtpStaleImportSession`):** Import sessions that receive no data segments for longer than `session_inactivity_limit_secs` are automatically cancelled with reason `LtpCancelByEngine`. This prevents half-received sessions from crashed senders from leaking memory indefinitely. HDTN computes this as `RTT × (maxRetries + 1) + 2 × housekeeping_interval`.

**Max import sessions enforcement (ION `maxImportSessions`):** When the number of active import sessions for a span reaches the configured limit, new incoming sessions are silently dropped. This prevents a flood of new sessions from exhausting receiver memory.

**Max red data size protection (HDTN `maxRedRxBytesPerSession`):** If a data segment's offset + length would exceed the configured `max_red_data_bytes_per_session`, the import session is cancelled with reason `LtpCancelByEngine`. This prevents a malicious or buggy sender from causing unbounded memory allocation.

**Client service ID validation:** The receiver validates that incoming data segments carry the expected client service ID (1 for Bundle Protocol). Segments with a mismatched ID trigger a CR with reason `LtpClientSvcUnreachable` (ION + HDTN behaviour).

**Max reports / max checkpoints limits (ION `maxReports`, `maxCheckpoints`):** Import sessions limit the total number of RS segments they may generate; export sessions limit the total number of checkpoints they may send. These are computed from the red-part length, segment loss rate, and max retransmissions. When the limit is reached, the session is cancelled. This prevents pathological link conditions from causing unbounded report/checkpoint cycling.

**Session recreation prevention (HDTN `LtpSessionRecreationPreventer`):** A circular buffer of recently-closed import session numbers is maintained per remote engine. If a data segment arrives for a session number in this history, it is discarded without creating a new session. This mitigates an anomaly where old session numbers reappear due to IP fragmentation or network duplication.

**Asynchronous reception report (HDTN GitHub Issue #23):** When all red-data extents are received by a non-checkpoint segment (out-of-order delivery), the receiver sends an unsolicited RS covering the full red-part scope with checkpoint serial number zero. This guarantees the sender learns that all data has been received and stops retransmitting.

**Deferred report sending (HDTN `delaySendingOfReportSegmentsTimeMsOrZeroToDisable`):** After receiving a checkpoint, the receiver may delay generating the RS for a configurable period (`defer_report_ms`) to allow in-flight out-of-order segments to arrive first. The timer resets on each gap-filling segment. If all gaps are filled during the deferral, the RS is sent immediately with full coverage. This dramatically reduces unnecessary retransmissions on links with reordering.

**Accelerated retransmission / intermediate checkpoints (HDTN `checkpointEveryNthDataPacketSender`):** Every Nth red-data segment can be marked as a checkpoint (type 1) in addition to the mandatory EORP checkpoint. This enables faster loss detection on long blocks. When a report for an intermediate checkpoint shows all bytes in its scope already acknowledged, no retransmission is triggered (discretionary checkpoint optimization).

## 4. Crate: `ltp-cla`

### 4.1. Purpose

Integrates the `ltp` protocol engine with the Hardy BPA via the `hardy_bpa::cla::Cla` trait. Manages UDP I/O, span configuration, session lifecycle, and peer registration.

### 4.2. Source Layout

```
ltp-cla/
  Cargo.toml
  src/
    lib.rs          # pub use Cla, Config
    cla.rs          # Implements hardy_bpa::cla::Cla
    span.rs         # Per-link state: UDP address, MTU, timers, active sessions
    engine.rs       # Tokio task: UDP rx loop, segment dispatch
    config.rs       # Serde Config and SpanConfig types
```

### 4.3. Dependencies

```toml
[dependencies]
hardy-async  = { version = "0.1", path = "../async" }
hardy-bpa    = { version = "0.1", path = "../bpa" }
hardy-bpv7   = { version = "0.5", path = "../bpv7" }
hardy-ltp    = { version = "0.1", path = "../ltp" }
tokio        = { version = "1", features = ["macros", "net", "time"] }
bytes        = "1"
thiserror    = "2"
trace-err    = "1"
tracing      = { version = "0.1", default-features = false }
metrics      = "0.24"
serde        = { version = "1", features = ["derive"], optional = true }
```

### 4.4. Address Type

LTP peers are identified by their 64-bit engine ID, not by a TCP address. The BPA address for an LTP peer is `ClaAddress::Private(engine_id.to_be_bytes().into())`. The `ltp-cla` span table maps engine IDs to UDP `SocketAddr` values internally.

This is preferred over `ClaAddress::Tcp` because:

- LTP transport is UDP, not TCP; reusing the TCP address type would be misleading.
- The same remote LTP engine could in principle be reachable at multiple UDP addresses (multi-homed links).
- Engine IDs align with IPN node numbers, making routing table integration straightforward.

### 4.5. Aggregation

A key feature of LTP (used extensively by ION) is that multiple bundles may be aggregated into a single LTP block before transmission. ION controls this with per-span `aggrSizeLimit` (bytes) and `aggrTimeLimit` (seconds): the current block is flushed when either limit is reached.

This improves efficiency on deep-space links by amortising the RS/RAS exchange across multiple small bundles. The implementation should:

- Buffer incoming `forward()` calls for a span into a pending block.
- Flush and start an export session when: (a) the buffered block reaches `aggr_size_limit`, or (b) `aggr_time_limit` seconds have elapsed since the first bundle was added.
- Each bundle is length-prefixed within the block (prepend a 4-byte big-endian length) so the receiver can extract individual bundles after delivery.
- `max_export_sessions` (ION span parameter) limits how many concurrent export sessions may be active per span; new sessions must wait if the limit is reached. This is LTP's flow control window.

### 4.6. Implementing `Cla`

```rust
impl Cla for LtpCla {
    fn address_type(&self) -> Option<ClaAddress> {
        None  // Private addresses; not a TCP CLA
    }

    fn on_register(&self, sink: Arc<dyn Sink>, node_ids: NodeIds) {
        // Store sink (must be kept alive for the lifetime of the CLA)
        self.sink.set(sink.clone()).ok();

        // Derive local engine ID from IPN node number if not configured
        // Bind UDP socket, spawn rx task (engine.rs)

        // Register pre-configured spans as peers
        for span in &self.config.spans {
            let addr = ClaAddress::Private(
                span.engine_id.to_be_bytes().to_vec().into()
            );
            sink.add_peer(addr, span.node_ids.clone());
        }
    }

    fn on_unregister(&self) {
        // Cancel UDP rx task; drop active sessions
    }

    async fn forward(
        &self,
        _queue: u32,
        address: ClaAddress,
        bundle: Bytes,
    ) -> Result<(), Error> {
        // Decode engine ID from ClaAddress::Private bytes
        // Look up span; return error if unknown
        // Append bundle to span's aggregation buffer (length-prefixed)
        // Flush immediately if buffer >= aggr_size_limit, else arm timer
    }

    fn queue_count(&self) -> u32 { 0 }
}
```

### 4.7. UDP Engine Task

A single `UdpSocket` is bound to the configured address (default port 1113, IANA-assigned). Each received datagram is one LTP segment. The engine task:

1. Reads a datagram.
2. Decodes the segment header to extract the session ID.
3. Routes control segments (RS, RAS, CS, CAS, CR, CAR) to the appropriate export or import session.
4. Routes data segments to the appropriate import session (creating one if needed).
5. Sends any outbound segments (RAS, RS, CAS, CAR, retransmits) generated by the state machines.
6. When an import session delivers a complete block, unpacks the length-prefixed bundles and calls `sink.dispatch()` for each.

No framing codec is required because UDP is already a datagram protocol.

### 4.8. Rate Control

ION's `udplso.c` implements a **token bucket** algorithm for per-span transmit rate limiting. For deep-space links where the far-end buffer is small and the link rate is precisely known, exceeding the nominal rate will cause receiver buffer overflow and retransmissions that take OWLTs to discover.

The span configuration should include an optional `xmit_rate_bps` value. When set, the UDP transmit path throttles by sleeping after each send proportional to `bytes_sent / xmit_rate_bps`. ION notes that `nanosleep` granularity (≥250 µs effective floor) must be accounted for by tracking elapsed time across sends.

## 5. Configuration

### 5.1. Schema

The span fields mirror the ION `LtpSpan` parameters, adapted to YAML:

```yaml
clas:
  - name: ltp0
    type: ltp
    bind: "[::]:1113"          # UDP address to listen on
    engine-id: 1               # Local engine ID (defaults to IPN node number)
    client-service-id: 1       # Expected client service ID (default: 1 = Bundle Protocol)
    spans:
      - engine-id: 2
        address: "10.0.0.2:1113"
        max-segment-size: 1400       # bytes; ≤ path MTU (ION: maxSegmentSize)
        max-export-sessions: 100     # flow control window (ION: maxExportSessions)
        max-import-sessions: 100     # receiver session limit (ION: maxImportSessions)
        aggr-size-limit: 65536       # bytes before flushing block (ION: aggrSizeLimit)
        aggr-time-limit-secs: 1      # seconds before flushing block (ION: aggrTimeLimit)
        max-retransmissions: 10      # (ION: maxTimeouts)
        xmit-rate-bps: 0             # 0 = unlimited (ION: xmitRate via token bucket)
        node-ids:
          - "ipn:2.0"

        # Timer configuration (choose one model):
        retransmit-cycle-secs: 60    # flat retransmit timeout (simple model)
        # OR RTT-based model (preferred for deep-space):
        # one-way-light-time-ms: 28000   # one-way propagation delay
        # one-way-margin-time-ms: 2000   # processing overhead margin
        # (retransmit timeout = 2 × (light_time + margin_time))

        # Receiver protection:
        max-red-data-bytes-per-session: 10485760  # 10 MB; cancel session if exceeded
        session-inactivity-limit-secs: 0          # 0 = disabled; cancel stale imports after N secs
        session-recreation-history-size: 0        # 0 = disabled; remember N closed session numbers

        # Report/checkpoint limits (computed from loss rate if not set):
        # max-reports-per-session: auto
        # max-checkpoints-per-session: auto

        # Out-of-order compensation:
        defer-report-ms: 0           # 0 = disabled; delay RS generation to absorb reordering

        # Accelerated retransmission:
        checkpoint-every-n-segments: 0  # 0 = disabled; make every Nth segment a checkpoint

        # Link management:
        purge-on-link-down: false    # cancel all export sessions when link goes down
        ping-interval-secs: 0        # 0 = disabled; probe remote engine liveness
```

### 5.2. Engine ID Derivation

If `engine-id` is not present in configuration, the CLA derives it from the BPA's IPN node number passed to `on_register`. This follows the convention in RFC 9173 and removes a common misconfiguration risk.

## 6. Integration with `bpa-server`

### 6.1. `bpa-server/Cargo.toml`

Add an optional feature:

```toml
[features]
ltp = ["dep:hardy-ltp-cla"]

[dependencies]
hardy-ltp-cla = { version = "0.1", path = "../ltp-cla", optional = true,
                  features = ["serde"] }
```

### 6.2. `bpa-server/src/config/cla.rs`

```rust
#[cfg(feature = "ltp")]
use hardy_ltp_cla::Cla as LtpCla;

// In ClaType enum:
#[cfg(feature = "ltp")]
#[serde(rename = "ltp")]
Ltp(hardy_ltp_cla::Config),

// In build():
#[cfg(feature = "ltp")]
ClaType::Ltp(config) => Ok(Some(Arc::new(
    LtpCla::new(config).map_err(|e| {
        anyhow::anyhow!("Failed to create CLA '{}': {e}", self.name)
    })?
))),
```

### 6.3. `Cargo.toml` Workspace Members

```toml
members = [
    # ... existing members ...
    "ltp",
    "ltp-cla",
]
```

## 7. Key Implementation Challenges

### 7.1. Session Number Allocation

Session numbers must be strictly monotonically increasing per remote engine. Use an `AtomicU64` counter stored in the span state. RFC 5326 §6.19 permits a cold-start delay (equal to the expected maximum session lifetime) to avoid session number reuse after a restart; persisting the counter to disk satisfies this requirement without the delay.

### 7.2. Retransmission Timers

Each checkpoint starts a `tokio::time::sleep`. ION's timer event model maps cleanly to `tokio::time::Instant`-based futures. There are four timer types (mirroring ION's `LtpEventType`):

| Timer | Fires when | Action |
| --- | --- | --- |
| `ResendCheckpoint` | No RS received for `retransmit_cycle_secs` | Retransmit all unclaimed red data; increment retry count |
| `ResendXmitCancel` | No CAS received after CS | Retransmit CS |
| `ResendReport` | No RAS received after RS | Retransmit RS |
| `ResendRecvCancel` | No CAR received after CR | Retransmit CR |

### 7.3. RS Claim Sets and the 20-Claim Limit

The receiver tracks received byte extents in a `BTreeMap<u64, u64>` (`offset → end`). Adjacent-extent merging on insert keeps the map compact. When generating RS segments, cap at 20 claims per RS (ION `MAX_CLAIMS_PER_RS`); emit multiple RS segments for the same checkpoint if needed, each covering a non-overlapping window of the block.

Wire claim offsets are relative to the RS `lower_bound`. The decoder must reconstruct absolute offsets by adding `lower_bound` before inserting into the extent map.

### 7.4. MTU Segmentation and Aggregation

The aggregation buffer for a span accumulates length-prefixed bundles until either `aggr_size_limit` bytes or `aggr_time_limit` seconds are reached, then flushes as a new export session. The flushed block is split into data segments no larger than `max_segment_size` bytes. All segments except the last are plain `LtpDsRed` (0); the final red segment is `LtpDsRedEOB` (3), which acts as the checkpoint triggering the RS exchange.

The `max_export_sessions` limit per span acts as LTP's flow control window: if the limit is reached, new `forward()` calls must wait until an active session completes. ION allows green-only bundles to bypass this restriction, since green sessions never hold retransmission buffers.

### 7.5. Green vs. Red Data Policy

RFC 5326 allows a red portion followed by a green portion within one block. The initial implementation treats all data as red (fully reliable). A future enhancement could expose a per-span `green_fraction` to send low-priority bundles as all-green, bypassing the retransmission machinery entirely.

### 7.6. Rate Control (Token Bucket)

ION's `udplso.c` implements a token bucket for each span. The bucket refills at `xmit_rate_bps / 8` bytes per second; sending `n` bytes deducts `n` tokens. When the bucket is empty the transmit loop sleeps for `deficit_bytes × 8 / xmit_rate_bps` seconds. ION notes the effective sleep floor is ~250 µs on Linux; elapsed-time compensation prevents cumulative drift. When `xmit_rate_bps = 0` the bucket is disabled.

### 7.7. Deferred Report Sending

HDTN's out-of-order compensation delays RS generation after receiving a checkpoint. The logic:

1. If the checkpoint scope is already fully covered (no gaps), send the RS immediately.
2. Otherwise, start a deferral timer (`defer_report_ms`).
3. If a data segment arrives that fills one or more gaps, reset the timer.
4. If all gaps are filled during the deferral, send the RS immediately with full coverage.
5. When the timer expires, send the RS with whatever coverage has been achieved.

This is critical for links with significant reordering (e.g., multi-path routing) where the checkpoint may arrive before earlier data segments.

### 7.8. Asynchronous Reception Report

When all red-data extents are received by a non-checkpoint segment (HDTN GitHub Issue #23), the receiver must send an unsolicited RS covering the full red-part scope. This RS references checkpoint serial number zero. The sender processes it as a normal report — recording claimed extents and sending a RAS. Without this, the sender would continue retransmitting data that has already been fully received until the next checkpoint timeout.

### 7.9. Accelerated Retransmission (Intermediate Checkpoints)

HDTN's `checkpointEveryNthDataPacketSender` marks every Nth red-data segment as a checkpoint (type 1). This enables faster loss detection on long blocks. The discretionary checkpoint optimization: when a report for an intermediate checkpoint shows all bytes in its scope already acknowledged (from a later report), no retransmission is triggered. The final segment is always EORP/EOB regardless of the interval.

### 7.10. Session Recreation Prevention

HDTN maintains a circular buffer of recently-closed import session numbers per remote engine (`LtpSessionRecreationPreventer`). When a data segment arrives for a session number in this history, it is discarded. This was introduced to mitigate an anomaly observed during testing with IP fragmentation where old closed session numbers would reappear much later during multi-session transmission.

Implementation: `HashSet` for O(1) lookup + `Vec` as a circular queue for eviction. When the buffer is full, the oldest entry is evicted before inserting the new one.

### 7.11. Stale Import Session Cleanup

Both ION and HDTN implement inactivity timeouts for import sessions. ION uses a per-span `sessionInactivityLimit` (seconds); HDTN computes `stagnantRxSessionTime` from `RTT × (maxRetries + 1) + 2 × housekeeping_interval`.

When an import session has received no data segments for longer than the limit, it is cancelled with reason `LtpCancelByEngine`. The inactivity timer is reset on each received data segment.

### 7.12. Max Reports and Max Checkpoints

ION computes per-session limits on the total number of RS segments an import session may generate and the total number of checkpoints an export session may send. The formula accounts for the red-part length, expected segment loss rate, and max retransmissions. When the limit is reached, the session is cancelled. This prevents pathological link conditions (e.g., 100% loss) from causing unbounded report/checkpoint cycling that would consume memory and bandwidth indefinitely.

### 7.13. Link Status Detection (Ping)

HDTN implements a "ping" mechanism for proactive link failure detection. When no data segments have been transmitted for `ping_interval_secs`, the engine sends a Cancel-from-Sender for a known non-existent session number (generated by `LtpRandomNumberGenerator::GetPingSession`). The remote engine responds with a Cancel-Ack. If no Cancel-Ack is received within `RTT × maxRetries`, a link-down event is emitted.

When combined with `purge_on_link_down`, this enables automatic re-routing of bundles through alternative paths when a link fails.

### 7.14. RTT-Based Timer Computation

HDTN computes retransmission timeouts as `2 × (oneWayLightTime + oneWayMarginTime)` rather than a flat `retransmit_cycle_secs`. This is more appropriate for deep-space links where the one-way light time is known from orbital mechanics and changes over time. The implementation should support runtime updates to the one-way light time to accommodate changing orbital geometry (e.g., Mars conjunction).

## 8. Observability

Following the `tcpclv4` pattern and ION's 27-counter span statistics (`LTP_SPAN_STATS`), the CLA should emit OpenTelemetry metrics. The ION counter names are shown for cross-reference:

| Metric | ION counter | Description |
| --- | --- | --- |
| `ltp.export_sessions.started` | `OUT_SEG_QUEUED` proxy | Export sessions opened |
| `ltp.export_sessions.completed` | `EXPORT_COMPLETE` | Export sessions completed successfully |
| `ltp.export_sessions.cancelled` | `EXPORT_CANCEL_XMIT` | Export sessions cancelled by local engine |
| `ltp.export_sessions.cancel_recv` | `EXPORT_CANCEL_RECV` | Export sessions cancelled by remote |
| `ltp.import_sessions.completed` | `IMPORT_COMPLETE` | Import sessions completed |
| `ltp.import_sessions.cancelled` | `IMPORT_CANCEL_XMIT` | Import sessions cancelled by local engine |
| `ltp.import_sessions.cancel_recv` | `IMPORT_CANCEL_RECV` | Import sessions cancelled by remote |
| `ltp.import_sessions.stale_cancelled` | — | Import sessions cancelled due to inactivity timeout |
| `ltp.import_sessions.limit_dropped` | — | Segments dropped due to max import sessions limit |
| `ltp.import_sessions.red_overflow` | — | Sessions cancelled due to exceeding max red data size |
| `ltp.import_sessions.recreation_blocked` | — | Segments dropped due to session recreation prevention |
| `ltp.import_sessions.wrong_client_svc` | `IN_SEG_UNK_CLIENT` | Sessions cancelled due to client service ID mismatch |
| `ltp.segments.tx` | `OUT_SEG_POPPED` | Segments transmitted (initial) |
| `ltp.segments.rx.red` | `IN_SEG_RECV_RED` | Red-data segments received |
| `ltp.segments.rx.green` | `IN_SEG_RECV_GREEN` | Green-data segments received |
| `ltp.segments.retransmitted` | `SEG_RE_XMIT` | Data segments retransmitted |
| `ltp.segments.rx.redundant` | `IN_SEG_REDUNDANT` | Duplicate segments discarded |
| `ltp.segments.rx.malformed` | `IN_SEG_MALFORMED` | Segments that failed to decode |
| `ltp.checkpoints.tx` | `CKPT_XMIT` | Checkpoints sent |
| `ltp.checkpoints.retransmitted` | `CKPT_RE_XMIT` | Checkpoints retransmitted |
| `ltp.reports.tx.positive` | `POS_RPT_XMIT` | Positive RS sent (full coverage) |
| `ltp.reports.tx.negative` | `NEG_RPT_XMIT` | Negative RS sent (gaps present) |
| `ltp.reports.retransmitted` | `RPT_RE_XMIT` | Reports retransmitted |
| `ltp.pings.sent` | — | Ping probes sent |
| `ltp.pings.acked` | — | Ping probes acknowledged (link alive) |
| `ltp.link_down_events` | — | Link-down events emitted |

## 9. Out of Scope

The following are explicitly deferred and not part of this design:

- **Non-UDP transports**: LTP can run over SCPS, CCSDS, etc. Only UDP is in scope here.
- **Persistent session recovery**: Sessions are not persisted across process restarts. Bundles in flight when the process exits will be retransmitted by the BPA if they are still in the egress queue. (Session *numbers* are persisted to avoid reuse; session *state* is not.)
- **Standalone `ltp-server` binary**: Analogous to `tcpclv4-server`. This could be added later following the same pattern.
- **LTP security extensions (RFC 5327)**: Authentication and integrity protection of LTP segments. This is a significant addition that is deferred to a separate design.
- **UDP_MULTISEND (`sendmmsg`)**: ION supports batching multiple UDP sends into a single syscall for high-throughput links. This is a platform-specific Linux optimisation that can be added later.
- **Burst signals (ION `BURST_SIGNALS_ENABLED`)**: ION's optimization to suppress timer-setting for segments that are part of a burst. This is a performance optimization that can be added later.
- **Disk-backed session data (HDTN `activeSessionDataOnDiskNewFileDurationMs`)**: HDTN supports keeping session data on disk instead of RAM for high-rate links with extremely long delays. This is deferred.
- **Multiple client service IDs**: The initial implementation supports only client service ID 1 (Bundle Protocol). ION supports up to 8 concurrent clients; this could be added later if needed.

## 10. Phased Implementation Plan

The recommendation from `ltp_cla_design_2.md` is to build incrementally: validate the data flow first, then layer in reliability. This maps to five phases:

### Phase 1 — Scaffolding

- Create `ltp/` and `ltp-cla/` crate skeletons with workspace entries.
- Implement SDNV encode/decode and segment type definitions.
- Implement the `Cla` trait stub (always-succeeding `forward`, no-op `on_register`).
- Wire into `bpa-server` behind the `ltp` feature flag.
- **Exit criterion**: `cargo build --features ltp` succeeds; CLA registers with BPA.

### Phase 2 — Basic Sessions (Green-only / Best-effort)

- Implement the UDP rx/tx loop (`engine.rs`).
- Implement green-data export and import sessions (no acknowledgement, no retransmission).
- Implement block aggregation with size and time limits.
- Implement bundle length-prefix packing and unpacking.
- **Exit criterion**: Bundles can be exchanged between two Hardy instances over UDP using all-green transfers; verified with `file-cla` or echo-service.

### Phase 3 — Full Reliability (Red data)

- Implement export session checkpoint/RS/RAS exchange.
- Implement import session RS generation with 20-claim limit and multi-RS-per-checkpoint.
- Implement all four retransmission timer types.
- Implement cancel/cancel-ack flows with reason codes.
- Implement closed-export retention for late RS handling.
- Implement miscoloured segment detection.
- **Exit criterion**: Reliable transfers complete correctly under simulated packet loss; interoperability test against ION-DTN.

### Phase 4 — Production Hardening

- Implement token bucket rate control per span.
- Implement `max_export_sessions` flow control.
- Implement `max_import_sessions` enforcement (silently drop new sessions at limit).
- Implement `max_red_data_bytes_per_session` protection.
- Implement stale import session cleanup (`session_inactivity_limit_secs`).
- Implement session recreation prevention (circular buffer of closed session numbers).
- Implement client service ID validation (cancel with `LtpClientSvcUnreachable`).
- Implement max reports / max checkpoints limits per session.
- Implement closed export response limit.
- Implement session number persistence across restarts.
- Add all observability metrics.
- Fuzz the segment decoder (`ltp/fuzz/`).
- **Exit criterion**: Passes PICS conformance tests; stable under sustained load; no memory leaks under adversarial input.

### Phase 5 — Advanced Features

- Implement deferred report sending (`defer_report_ms`) for out-of-order compensation.
- Implement asynchronous reception report (unsolicited RS when all red data received by non-checkpoint).
- Implement accelerated retransmission via intermediate checkpoints (`checkpoint_every_n_segments`).
- Implement link status detection via ping (`ping_interval_secs`).
- Implement purge on link down (`purge_on_link_down`).
- Implement RTT-based timer computation (`one_way_light_time_ms` + `one_way_margin_time_ms`).
- Implement runtime one-way light time updates for changing orbital geometry.
- **Exit criterion**: All features from ION-DTN and HDTN reference implementations are covered; interoperability verified against both.

## 11. References

| Reference | Title |
| --- | --- |
| RFC 5326 | Licklider Transmission Protocol — Specification |
| RFC 5327 | Licklider Transmission Protocol — Security Extensions |
| RFC 5050 | Bundle Protocol Specification (SDNV definition) |
| RFC 9171 | Bundle Protocol Version 7 |
| RFC 9173 | Default Node ID Scheme for the Bundle Protocol (IPN) |
| RFC 9174 | Delay-Tolerant Networking TCP Convergence-Layer Protocol Version 4 |
| ION `ltp/library/ltpP.h` | LtpSpan, LtpPdu, LtpSegmentTypeCode, LtpCancelReasonCode, session structures |
| ION `ltp/library/libltpP.c` | `serializeHeader`, `serializeDataSegment`, `serializeReportSegment`, closed-export logic, stale session handling, max reports/checkpoints |
| ION `ltp/library/libltp.c` | `ltp_send`, aggregation buffer logic, `sduCanBeAppendedToBlock` |
| ION `ltp/daemon/ltpmeter.c` | Block segmentation and export session lifecycle |
| ION `ltp/udp/udplso.c` | Token bucket rate control, UDP send loop |
| ION `ltp/udp/udplsi.c` | UDP receive loop, dual-stack address binding |
| HDTN `common/ltp/include/LtpEngine.h` | Engine architecture, timer managers, session lifecycle callbacks, ping mechanism |
| HDTN `common/ltp/include/LtpEngineConfig.h` | All configurable parameters: RTT, deferred reports, checkpoints, rate limiting, session recreation prevention |
| HDTN `common/ltp/src/LtpSessionSender.cpp` | Accelerated retransmission (checkpoint every Nth), discretionary checkpoint optimization |
| HDTN `common/ltp/src/LtpSessionReceiver.cpp` | Deferred report sending, asynchronous reception report, max red data protection |
| HDTN `common/ltp/include/LtpSessionRecreationPreventer.h` | Circular buffer session recreation prevention |
| HDTN `common/ltp/include/LtpFragmentSet.h` | Extent tracking, report segment splitting, gap detection |
