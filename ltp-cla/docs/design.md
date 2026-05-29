# Design: LTP Convergence Layer Adapter

| Document Info | Details |
| ----- | ----- |
| **Project** | Hardy DTN Router |
| **Crate** | `hardy-ltp-cla` |
| **Repository** | `github.com/ricktaylor/hardy` |
| **RFCs** | RFC 5326 (LTP), RFC 5050 (SDNV), RFC 9173 (IPN) |
| **Status** | Implemented |

## 1. Overview

This document describes the design of the LTP Convergence Layer Adapter for the Hardy DTN router. The implementation enables Hardy to operate over high-latency, lossy links — such as deep-space communications, satellite backhaul, and challenged terrestrial networks — where TCP-based convergence layers are impractical.

### 1.1. Motivation

Hardy supports TCPCLv4 and file-based convergence layers. These work well for terrestrial links with reasonable round-trip times, but they are unsuitable for:

- **Deep-space links** with one-way light times of seconds to hours
- **Satellite links** with high loss rates and asymmetric bandwidth
- **Disrupted terrestrial links** where TCP's congestion control and connection semantics break down

LTP is the standard convergence layer for these environments. It is specified in RFC 5326, widely deployed in NASA's DTN infrastructure (ION-DTN), and supported by HDTN.

### 1.2. Why LTP is Different

Unlike TCPCLv4, LTP cannot be implemented as a thin transport adapter. It is a complete reliability protocol that must be fully embedded within the CLA:

| Aspect | TCPCLv4 | LTP |
| --- | --- | --- |
| **Reliability** | Delegated to TCP | Implemented in the LTP engine itself |
| **Model** | Continuous byte stream | Session-based blocks |
| **Framing** | Stream framing (tokio-util codec) | Datagram — one UDP datagram per segment |
| **Connection** | Persistent per-peer TCP connection | Connectionless; sessions are ephemeral |
| **Suitability** | Low-delay terrestrial networks | High-delay, disrupted, or asymmetric links |

### 1.3. Crate Split

The implementation follows Hardy's existing workspace conventions with a two-crate split:

- **`ltp/`** (`hardy-ltp`) — Protocol engine: segment encoding/decoding, session state machines, SDNV codec. No dependency on `hardy-bpa`.
- **`ltp-cla/`** (`hardy-ltp-cla`) — BPA convergence layer adapter: implements `hardy_bpa::cla::Cla`, UDP transport, span management.

This split provides:
1. **Independent testability** — Session state machines are pure functions tested with property-based testing
2. **Reusability** — The protocol engine can be used without the full BPA
3. **Follows precedent** — Mirrors the `bpv7`/`bpa` split already established in the workspace

### 1.4. Architecture

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

### 1.5. BPA ↔ LTP Operation Mapping

| BPA operation | LTP action |
| --- | --- |
| `forward(address, bundle)` | Append bundle to span aggregation buffer; flush as new export session when limit reached |
| `sink.dispatch(bundle, peer, addr)` | Called when an import session delivers a complete block; bundles are unpacked from length-prefixed block |
| `sink.add_peer(addr, node_ids)` | Called at startup for each pre-configured span; also when a new remote engine is first seen |
| `sink.remove_peer(addr)` | Called when a span is administratively removed |

## 2. Source Layout

```
ltp-cla/
  Cargo.toml
  src/
    lib.rs          # pub use Cla, Config
    cla.rs          # Implements hardy_bpa::cla::Cla
    span.rs         # Per-link state: sessions, aggregation, rate control, timers
    engine.rs       # UDP receive loop, segment dispatch
    config.rs       # Serde Config and SpanConfig types
    block.rs        # Bundle aggregation/unpacking (length-prefixed framing)
  tests/
    common/mod.rs           # Shared test utilities
    block_properties.rs     # Property tests for aggregation round-trip
    span_properties.rs      # Property tests for span logic
    end_to_end.rs           # Full LTP transfer over localhost UDP
    full_stack_test.rs      # BPv7 bundles over LTP
    lunar_link_test.rs      # Simulated deep-space scenario
    lunar_link_bpa_test.rs  # Full BPA-to-BPA lunar link
```

## 3. Key Design Decisions

### 3.1. Pure State Machines with Action-Based I/O

Session logic is completely separated from I/O. The export and import session structs (in `hardy-ltp`) are pure state machines — methods like `on_report()` or `on_data_segment()` return a `Vec<Action>` describing what the caller should do. This design:

- Enables deterministic property-based testing without mocking
- Makes the protocol logic easy to reason about and audit
- Allows the CLA layer to batch, reorder, or rate-limit actions as needed

### 3.2. Zero-Copy Receive Path

Received UDP datagrams are wrapped in `Bytes` before decoding. The segment decoder uses `Buf::copy_to_bytes()` which, on a `Bytes` input, performs an O(1) reference-counted split rather than a memcpy. Data segment payloads (the hot path — often 1000+ bytes) are extracted without allocation.

### 3.3. Private Addresses

LTP peers are identified by their 64-bit engine ID, not by a TCP address. The BPA address for an LTP peer is `ClaAddress::Private(engine_id.to_be_bytes().into())`. The span table maps engine IDs to UDP `SocketAddr` values internally.

This is preferred over `ClaAddress::Tcp` because:
- LTP transport is UDP, not TCP; reusing the TCP address type would be misleading
- The same remote LTP engine could be reachable at multiple UDP addresses
- Engine IDs align with IPN node numbers, making routing table integration straightforward

### 3.4. Token Bucket Rate Control

Per-span transmit rate limiting uses a token bucket algorithm (following ION's `udplso.c`). When `xmit_rate_bps` is configured, each segment send consumes tokens proportional to its size. If the bucket is empty, the send path sleeps until tokens refill. When rate is 0, the limiter is bypassed entirely.

### 3.5. RTT-Based Timer Computation

Retransmission timeouts can be configured as either a flat `retransmit_cycle_secs` or computed from `2 × (one_way_light_time_ms + one_way_margin_time_ms)`. The one-way light time is stored in an `AtomicU64` and can be updated at runtime to accommodate changing orbital geometry.

## 4. Background: LTP Protocol

### 4.1. Concepts

| Concept | Description |
| --- | --- |
| **Engine ID** | 64-bit numeric identity of an LTP endpoint, conventionally equal to the IPN node number |
| **Export session** | A sender-side session identified by `(sender_engine_id, session_number)` |
| **Import session** | A receiver-side session tracking data arriving from a remote engine |
| **Block** | The complete service data unit being transferred; may aggregate multiple bundles |
| **Segment** | A fixed-size unit of transmission; one UDP datagram carries exactly one segment |
| **Red data** | Reliably delivered portion of a block; subject to acknowledgement and retransmission |
| **Green data** | Best-effort portion; no acknowledgement, no retransmission |
| **Checkpoint** | A red-data segment that requires a Report Segment in response |
| **Report Segment (RS)** | Receiver acknowledgement listing claimed byte-range extents |
| **Report-Ack Segment (RAS)** | Sender acknowledgement of an RS |
| **Cancel Segment (CS/CR)** | Abort a session from the sender or receiver side |
| **Span** | Pre-configured link to a remote engine: UDP address, MTU, timer values, session limits |
| **Aggregation** | Multiple bundles combined into a single LTP block |
| **Client service ID** | Numeric identifier for the upper-layer consumer; always `1` for the Bundle Protocol |

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

| Value | Meaning |
| --- | --- |
| 0 | `CancelByUser` — application requested cancellation |
| 1 | `ClientSvcUnreachable` — no registered client for the service ID |
| 2 | `RetransmitLimitExceeded` — max retransmissions exhausted |
| 3 | `MiscoloredSegment` — segment type inconsistent with session colour |
| 4 | `CancelByEngine` — internal engine error |

### 4.4. SDNV Encoding

LTP encodes all integer fields using Self-Delimiting Numeric Values (SDNVs, RFC 5050 §4.1). Each byte contributes 7 bits of value; the MSB is a continuation flag.

### 4.5. Segment Wire Format

**Header (all segment types):**
```
byte  0:    (version=0 << 4) | segment_type_code
SDNV:       sender engine ID
SDNV:       session number
byte:       (header_ext_count << 4) | trailer_ext_count
[header extensions]
```

**Data segment body:**
```
SDNV: client service ID (always 1 for BP)
SDNV: data offset within block
SDNV: data length
[if checkpoint type (1,2,3):]
  SDNV: checkpoint serial number
  SDNV: responding report serial number
[raw data bytes]
[trailer extensions]
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

> **Important:** Reception claim offsets are relative to the RS `lower_bound`, not the block start. The decoder adds `lower_bound` back when storing received claims.

### 4.6. RS Claim Limit

Claims per RS are capped at 20 (ION `MAX_CLAIMS_PER_RS`). When more disjoint extents exist, multiple RS segments are emitted for the same checkpoint, each covering a different window.

## 5. CLA Implementation (`cla.rs`)

### 5.1. Implementing the `Cla` Trait

```rust
impl Cla for LtpCla {
    fn on_register(&self, sink: Arc<dyn Sink>, node_ids: NodeIds) {
        // Store sink, derive engine ID, bind UDP socket, spawn rx task
        // Register pre-configured spans as peers via sink.add_peer()
    }

    fn on_unregister(&self) {
        // Cancel UDP rx task; drop active sessions
    }

    async fn forward(&self, _queue: u32, address: ClaAddress, bundle: Bytes)
        -> Result<(), Error>
    {
        // Decode engine ID from ClaAddress::Private
        // Append bundle to span's aggregation buffer (length-prefixed)
        // Flush if buffer >= aggr_size_limit, else arm timer
    }
}
```

### 5.2. Engine ID Derivation

If `engine-id` is not configured, the CLA derives it from the BPA's IPN node number passed to `on_register`. This follows RFC 9173 conventions.

## 6. UDP Engine (`engine.rs`)

A single `UdpSocket` is bound to the configured address (default port 1113). Each received datagram is one LTP segment. The engine task:

1. Reads a datagram
2. Decodes the segment header to extract the session ID
3. Routes control segments (RS, RAS, CS, CAS, CR, CAR) to the appropriate session
4. Routes data segments to the appropriate import session (creating one if needed)
5. Sends outbound segments generated by the state machines
6. When an import session delivers a complete block, unpacks bundles and calls `sink.dispatch()`

## 7. Span Management (`span.rs`)

### 7.1. Aggregation

Multiple bundles are aggregated into a single LTP block before transmission. Each bundle is length-prefixed (4-byte big-endian) within the block. The block is flushed when:
- Buffered data reaches `aggr_size_limit` bytes, or
- `aggr_time_limit` seconds have elapsed since the first bundle was added

### 7.2. Session Lifecycle

- **Export sessions** — Created when the aggregation buffer flushes. Limited by `max_export_sessions` (flow control window).
- **Import sessions** — Created on first data segment from a new remote session. Limited by `max_import_sessions`.
- **Closed export retention** — Completed sessions are retained for `2 × max_retransmissions × retransmit_cycle_secs + 10` seconds to respond to late Report Segments.

### 7.3. Retransmission Timers

Four timer types (mirroring ION's `LtpEventType`):

| Timer | Fires when | Action |
| --- | --- | --- |
| `ResendCheckpoint` | No RS received within timeout | Retransmit unclaimed data |
| `ResendXmitCancel` | No CAS received after CS | Retransmit CS |
| `ResendReport` | No RAS received after RS | Retransmit RS |
| `ResendRecvCancel` | No CAR received after CR | Retransmit CR |

### 7.4. Rate Control

Token bucket algorithm per span. Bucket refills at `xmit_rate_bps / 8` bytes per second. Sending `n` bytes deducts `n` tokens. When empty, the transmit path sleeps for `deficit_bytes × 8 / xmit_rate_bps` seconds.

### 7.5. Link Status Detection (Ping)

When no data has been transmitted for `ping_interval_secs`, the engine sends a Cancel-from-Sender for session number 0 (a known non-existent session). The remote responds with a Cancel-Ack. If no response within the timeout, a link-down event is emitted.

### 7.6. Session Recreation Prevention

A circular buffer of recently-closed import session numbers is maintained per remote engine. Segments arriving for a session number in this history are discarded. Implementation: `HashSet` for O(1) lookup + `Vec` as a circular queue for eviction.

### 7.7. Stale Import Session Cleanup

Import sessions that receive no data segments for longer than `session_inactivity_limit_secs` are cancelled with reason `CancelByEngine`. The timer resets on each received data segment.

## 8. Block Unpacking (`block.rs`)

Bundles within a block are length-prefixed with a 4-byte big-endian u32:

```
[4-byte length][bundle bytes][4-byte length][bundle bytes]...
```

The unpacker iterates through the block, extracting each bundle as a `Bytes` slice (zero-copy via `Bytes::slice()`). Zero-length entries are skipped. Truncated entries at the end are discarded.

## 9. Configuration

### 9.1. Schema

```yaml
clas:
  - name: ltp0
    type: ltp
    bind: "[::]:1113"
    engine-id: 1
    client-service-id: 1
    spans:
      - engine-id: 2
        address: "10.0.0.2:1113"
        node-ids: ["ipn:2.0"]
        max-segment-size: 1400
        max-export-sessions: 100
        max-import-sessions: 100
        aggr-size-limit: 65536
        aggr-time-limit-secs: 1
        max-retransmissions: 10
        xmit-rate-bps: 0                       # 0 = unlimited
        retransmit-cycle-secs: 60              # flat timeout
        one-way-light-time-ms: 28000           # RTT-based (overrides flat)
        one-way-margin-time-ms: 2000
        max-red-data-bytes-per-session: 10485760
        session-inactivity-limit-secs: 0       # 0 = disabled
        session-recreation-history-size: 0     # 0 = disabled
        defer-report-ms: 0                     # 0 = disabled
        checkpoint-every-n-segments: 0         # 0 = disabled
        ping-interval-secs: 0                  # 0 = disabled
        purge-on-link-down: false
```

## 10. Integration with `bpa-server`

### 10.1. Feature Flag

```toml
[features]
ltp = ["dep:hardy-ltp-cla"]

[dependencies]
hardy-ltp-cla = { version = "0.1", path = "../ltp-cla", optional = true, features = ["serde"] }
```

### 10.2. CLA Registration

The LTP CLA is registered as a `ClaType::Ltp` variant in the BPA server's CLA configuration, following the same pattern as TCPCLv4.

## 11. Observability

Following the `tcpclv4` pattern and ION's span statistics, the CLA emits OpenTelemetry metrics:

| Metric | Description |
| --- | --- |
| `ltp.export_sessions.started` | Export sessions opened |
| `ltp.export_sessions.completed` | Export sessions completed successfully |
| `ltp.export_sessions.cancelled` | Export sessions cancelled by local engine |
| `ltp.export_sessions.cancel_recv` | Export sessions cancelled by remote |
| `ltp.import_sessions.completed` | Import sessions completed |
| `ltp.import_sessions.cancelled` | Import sessions cancelled by local engine |
| `ltp.import_sessions.cancel_recv` | Import sessions cancelled by remote |
| `ltp.import_sessions.stale_cancelled` | Sessions cancelled due to inactivity |
| `ltp.import_sessions.limit_dropped` | Segments dropped at session limit |
| `ltp.import_sessions.red_overflow` | Sessions cancelled for exceeding red data size |
| `ltp.import_sessions.recreation_blocked` | Segments dropped by recreation prevention |
| `ltp.import_sessions.wrong_client_svc` | Sessions cancelled for client service ID mismatch |
| `ltp.segments.tx` | Segments transmitted |
| `ltp.segments.rx.red` | Red-data segments received |
| `ltp.segments.rx.green` | Green-data segments received |
| `ltp.segments.retransmitted` | Data segments retransmitted |
| `ltp.segments.rx.redundant` | Duplicate segments discarded |
| `ltp.segments.rx.malformed` | Segments that failed to decode |
| `ltp.checkpoints.tx` | Checkpoints sent |
| `ltp.checkpoints.retransmitted` | Checkpoints retransmitted |
| `ltp.reports.tx.positive` | Positive RS sent (full coverage) |
| `ltp.reports.tx.negative` | Negative RS sent (gaps present) |
| `ltp.reports.retransmitted` | Reports retransmitted |
| `ltp.pings.sent` | Ping probes sent |
| `ltp.pings.acked` | Ping probes acknowledged |
| `ltp.link_down_events` | Link-down events emitted |

## 12. Performance Characteristics

- **Zero-copy receive path** — Data segment payloads extracted via O(1) `Bytes::split_to`
- **O(log n) extent operations** — BTreeMap-based extent map for import sessions
- **O(1) session history lookup** — HashSet-backed circular buffer for recreation prevention
- **Lock-free session numbering** — `AtomicU64` for strictly monotonic allocation
- **Lock release before I/O** — Session map locks are dropped before UDP sends and rate-limiter sleeps
- **Pre-allocated buffers** — Aggregation buffer and import block_data avoid repeated reallocations

## 13. Testing

### 13.1. Test Suite Summary

| Category | Count | Description |
|----------|-------|-------------|
| Unit tests (hardy-ltp) | 193 | Segment codec, SDNV, export/import session logic |
| Unit tests (hardy-ltp-cla) | 87 | Aggregation, rate control, span management |
| Property tests (hardy-ltp) | 17 | SDNV, segment, segmentation, retransmission, extent map |
| Property tests (hardy-ltp-cla) | 10 | Aggregation, session monotonicity, token bucket, limits |
| Integration tests | 12 | End-to-end, full-stack, lunar link scenarios |
| Doc tests | 1 | Block unpacking example |

### 13.2. Property-Based Testing Approach

Each correctness property is encoded as an executable property test using `proptest`:

- **SDNV round-trip**: ∀ u64 values, encode→decode = identity
- **Segment round-trip**: ∀ valid segments, encode→decode = identity
- **Block segmentation**: ∀ blocks, concatenating segment payloads = original
- **Retransmission coverage**: ∀ partial acks, retransmitted bytes = exactly unclaimed bytes
- **Token bucket rate**: ∀ send sequences, cumulative bytes ≤ R×t + burst
- **Extent map invariant**: ∀ insertion sequences, no adjacent/overlapping entries

### 13.3. Integration Test Scenarios

- **End-to-end** — 100 KB and 1 MB file transfers over localhost UDP
- **Full-stack** — Real BPv7 bundles encoded, transported over LTP, and delivered
- **Lunar link** — Simulated deep-space scenario with realistic OWLT and rate constraints
- **Lunar link BPA** — Full BPA-to-BPA test with ground station and spacecraft nodes

## 14. Compatibility

The wire format and session semantics are compatible with:
- **ION-DTN** (NASA JPL reference implementation)
- **HDTN** (NASA GRC high-rate DTN implementation)
- Any RFC 5326-compliant LTP implementation

## 15. Out of Scope (Future Work)

- **Non-UDP transports** — LTP over CCSDS, SCPS, etc.
- **LTP security extensions** — RFC 5327 authentication and integrity
- **Persistent session recovery** — Sessions are not persisted across restarts
- **Standalone `ltp-server` binary** — Analogous to `tcpclv4-server`
- **UDP_MULTISEND (`sendmmsg`)** — Batching multiple UDP sends
- **Adaptive rate control** — Currently static per-span
- **Multiple client service IDs** — Only ID 1 (Bundle Protocol) is supported

## 16. Dependencies

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

## 17. References

| Reference | Title |
| --- | --- |
| RFC 5326 | Licklider Transmission Protocol — Specification |
| RFC 5327 | Licklider Transmission Protocol — Security Extensions |
| RFC 5050 | Bundle Protocol Specification (SDNV definition) |
| RFC 9171 | Bundle Protocol Version 7 |
| RFC 9173 | Default Node ID Scheme for the Bundle Protocol (IPN) |
| RFC 9174 | Delay-Tolerant Networking TCP Convergence-Layer Protocol Version 4 |
| ION `ltp/library/libltpP.c` | Segment serialization, closed-export logic, session management |
| ION `ltp/udp/udplso.c` | Token bucket rate control, UDP send loop |
| ION `ltp/udp/udplsi.c` | UDP receive loop |
| HDTN `common/ltp/include/LtpEngine.h` | Engine architecture, timer managers, ping mechanism |
| HDTN `common/ltp/include/LtpEngineConfig.h` | All configurable parameters |
| HDTN `common/ltp/src/LtpSessionSender.cpp` | Accelerated retransmission, discretionary optimization |
| HDTN `common/ltp/src/LtpSessionReceiver.cpp` | Deferred reports, async reports, max red data protection |
| HDTN `common/ltp/include/LtpSessionRecreationPreventer.h` | Session recreation prevention |
