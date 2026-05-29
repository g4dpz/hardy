# Proposal: LTP Convergence Layer Adapter for Hardy

## Summary

This document proposes the addition of a Licklider Transmission Protocol (LTP, RFC 5326) Convergence Layer Adapter to the Hardy DTN router. The implementation is complete, tested, and ready for review. It enables Hardy to operate over high-latency, lossy links — such as deep-space communications, satellite backhaul, and challenged terrestrial networks — where TCP-based convergence layers are impractical.

## Motivation

Hardy currently supports TCPCLv4 and file-based convergence layers. These work well for terrestrial links with reasonable round-trip times, but they are unsuitable for:

- **Deep-space links** with one-way light times of seconds to hours
- **Satellite links** with high loss rates and asymmetric bandwidth
- **Disrupted terrestrial links** where TCP's congestion control and connection semantics break down

LTP is the standard convergence layer for these environments. It is specified in RFC 5326, widely deployed in NASA's DTN infrastructure (ION-DTN), and supported by HDTN. Adding LTP to Hardy makes it a viable option for space networking and other challenged environments.

## Architecture

The implementation follows Hardy's existing workspace conventions with a two-crate split:

```
ltp/        (hardy-ltp)      — Protocol engine library
ltp-cla/    (hardy-ltp-cla)  — BPA integration via Cla trait
```

### Why two crates?

1. **Independent testability** — The protocol engine has no dependency on `hardy-bpa`. Session state machines are pure functions that can be exhaustively tested with property-based testing.
2. **Reusability** — The `hardy-ltp` crate could be used by other applications that need LTP without pulling in the full BPA.
3. **Follows precedent** — Mirrors the `bpv7`/`bpa` split already established in the workspace.

### Component Overview

| Component | Responsibility |
|-----------|---------------|
| `ltp/src/sdnv.rs` | SDNV variable-length integer codec |
| `ltp/src/segment.rs` | Segment wire format encode/decode (all 10 segment types) |
| `ltp/src/session/export.rs` | Export (sender) session state machine |
| `ltp/src/session/import.rs` | Import (receiver) session state machine + ExtentMap |
| `ltp-cla/src/cla.rs` | `Cla` trait implementation (`LtpCla`) |
| `ltp-cla/src/engine.rs` | UDP receive loop and segment routing |
| `ltp-cla/src/span.rs` | Per-link state: sessions, aggregation, rate control, timers |
| `ltp-cla/src/block.rs` | Bundle aggregation/unpacking (length-prefixed framing) |
| `ltp-cla/src/config.rs` | Configuration types with serde support |

## Key Design Decisions

### Pure state machines with action-based I/O

Session logic is completely separated from I/O. The export and import session structs are pure state machines — you call methods like `on_report()` or `on_data_segment()` and they return a `Vec<Action>` describing what the caller should do (send segments, start timers, deliver blocks). This design:

- Enables deterministic property-based testing without mocking
- Makes the protocol logic easy to reason about and audit
- Allows the CLA layer to batch, reorder, or rate-limit actions as needed

### Zero-copy receive path

Received UDP datagrams are wrapped in `Bytes` before decoding. The segment decoder uses `Buf::copy_to_bytes()` which, on a `Bytes` input, performs an O(1) reference-counted split rather than a memcpy. Data segment payloads (the hot path — often 1000+ bytes) are extracted without allocation.

### Token bucket rate control

Per-span transmit rate limiting uses a token bucket algorithm. When `xmit_rate_bps` is configured, each segment send consumes tokens proportional to its size. If the bucket is empty, the send path sleeps until tokens refill. When rate is 0, the limiter is bypassed entirely (no overhead).

### RTT-based timer computation

Retransmission timeouts can be configured as either a flat `retransmit_cycle_secs` or computed from `2 × (one_way_light_time_ms + one_way_margin_time_ms)`. The one-way light time is stored in an `AtomicU64` and can be updated at runtime to accommodate changing orbital geometry.

## Features Implemented

### Core Protocol (RFC 5326)

- SDNV encoding/decoding with overflow and truncation detection
- All 10 segment types: RedData, RedCheckpoint, RedEorp, RedEob, GreenData, GreenEob, Report, ReportAck, Cancel (sender/receiver), CancelAck (sender/receiver)
- Export session: block segmentation, checkpoint management, report handling, retransmission of unclaimed bytes, cancel handling
- Import session: extent tracking with adjacent-merge, report generation (capped at 20 claims/RS), block delivery, cancel handling
- Green (best-effort) data: transmit without acknowledgement, deliver on GreenEob regardless of gaps

### Reliability and Hardening

- **Closed export retention** — Completed sessions are retained for a configurable period to respond to late Report Segments with Report-Ack
- **Max red data size enforcement** — Import sessions exceeding the configured byte limit are cancelled
- **Max import sessions** — Silently discards segments when the per-span session limit is reached
- **Session recreation prevention** — Circular buffer (with O(1) HashSet lookup) prevents stale segments from recreating closed sessions
- **Max reports/checkpoints limits** — Prevents resource exhaustion from pathological peers
- **Stale session cleanup** — Inactivity timer cancels import sessions that stop receiving data

### Advanced Features

- **Deferred report sending** — Delays RS generation to allow in-flight gap-filling segments to arrive, reducing unnecessary retransmissions
- **Asynchronous reception report** — Generates unsolicited RS when a non-checkpoint segment completes the block
- **Accelerated retransmission** — Intermediate checkpoints every N segments for earlier loss detection on large blocks
- **RTT-based timers** — Computed from one-way light time + margin, with runtime updates
- **Link status detection** — Periodic ping (Cancel for session 0) with response timeout for link-down detection
- **Purge on link down** — Cancels all active export sessions and notifies BPA for re-routing

### Integration

- **BPA server integration** — `ltp` feature flag in `bpa-server/Cargo.toml`, configuration via YAML (`type: ltp`)
- **Metrics** — Comprehensive counters for sessions, segments, checkpoints, reports, and error conditions
- **Serde configuration** — All config types derive Serialize/Deserialize behind a feature flag

## Testing

The implementation includes 308 tests:

| Category | Count | Description |
|----------|-------|-------------|
| Unit tests (hardy-ltp) | 193 | Segment codec, SDNV, export/import session logic |
| Unit tests (hardy-ltp-cla) | 87 | Aggregation, rate control, span management, session history |
| Property tests (hardy-ltp) | 17 | SDNV round-trip, segment round-trip, segmentation, retransmission, checkpoints, extent map, report generation, block delivery, client service ID, max red data |
| Property tests (hardy-ltp-cla) | 10 | Aggregation round-trip, session monotonicity, token bucket rate invariant, closed export response limit, max import sessions, session recreation, RTT computation |
| Doc tests | 1 | Block unpacking example |

### Property-based testing approach

Each correctness property from the design is encoded as an executable property test using `proptest`. These validate universal invariants rather than specific examples:

- **SDNV round-trip**: ∀ u64 values, encode→decode = identity
- **Segment round-trip**: ∀ valid segments, encode→decode = identity
- **Block segmentation**: ∀ blocks, concatenating segment payloads = original
- **Retransmission coverage**: ∀ partial acks, retransmitted bytes = exactly unclaimed bytes
- **Token bucket rate**: ∀ send sequences, cumulative bytes ≤ R×t + burst
- **Extent map invariant**: ∀ insertion sequences, no adjacent/overlapping entries

## Configuration Example

```yaml
clas:
  - name: ltp-deep-space
    type: ltp
    bind: "[::]:1113"
    engine-id: 1
    client-service-id: 1
    spans:
      - engine-id: 2
        address: "10.0.0.2:1113"
        node-ids: ["ipn:2.0"]
        max-segment-size: 1400
        max-retransmissions: 10
        retransmit-cycle-secs: 60
        one-way-light-time-ms: 28000
        one-way-margin-time-ms: 2000
        xmit-rate-bps: 256000
        aggr-size-limit: 65536
        aggr-time-limit-secs: 1
        max-import-sessions: 100
        max-export-sessions: 100
        max-red-data-bytes-per-session: 10485760
        session-inactivity-limit-secs: 300
        session-recreation-history-size: 100
        defer-report-ms: 500
        checkpoint-every-n-segments: 10
        ping-interval-secs: 30
        purge-on-link-down: true
```

## Performance Characteristics

- **Zero-copy receive path** — Data segment payloads extracted via O(1) Bytes::split_to
- **O(log n) extent operations** — BTreeMap-based extent map for both import and export sessions
- **O(1) session history lookup** — HashSet-backed circular buffer for recreation prevention
- **Lock-free session numbering** — AtomicU64 for strictly monotonic allocation
- **Lock release before I/O** — Session map locks are dropped before UDP sends and rate-limiter sleeps
- **Pre-allocated buffers** — Aggregation buffer and import block_data avoid repeated reallocations

## Compatibility

The wire format and session semantics are compatible with:
- **ION-DTN** (NASA's reference implementation)
- **HDTN** (NASA's high-rate DTN implementation)
- Any RFC 5326-compliant LTP implementation

## What's Not Included (Future Work)

- **LTP over CCSDS** — Currently UDP-only; CCSDS framing could be added as an alternative transport
- **LTP extensions** — Header/trailer extensions are parsed and skipped but not processed
- **Security extensions** — LTP authentication (RFC 5327) is not implemented
- **Adaptive rate control** — Rate is currently static per-span; adaptive algorithms could be added

## How to Try It

```bash
# Build with LTP support
cargo build -p hardy-bpa-server --features ltp

# Run tests
cargo test -p hardy-ltp -p hardy-ltp-cla

# Check the full workspace still compiles
cargo check --workspace
```

## Conclusion

This LTP CLA implementation gives Hardy the ability to operate over deep-space and challenged links — a critical capability for DTN deployments beyond terrestrial networks. The two-crate architecture, pure state machine design, and comprehensive property-based test suite provide confidence in correctness while keeping the code maintainable and extensible.
