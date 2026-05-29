# Executive Summary: LTP Convergence Layer Adapter for Hardy

| | |
| --- | --- |
| **Project** | Hardy DTN Router |
| **Feature** | Licklider Transmission Protocol (LTP) CLA |
| **Status** | Proposed |
| **RFCs** | RFC 5326 (LTP), RFC 5050 (SDNV), RFC 9173 (IPN) |
| **Reference Implementations** | ION-DTN (NASA JPL), HDTN (NASA GRC) |

## Purpose

This proposal adds deep-space link support to Hardy by implementing LTP (RFC 5326) as a Convergence Layer Adapter. LTP is the standard reliability protocol for links where round-trip times are measured in minutes or hours — cislunar, Mars relay, and other high-latency or disrupted paths where TCP is fundamentally unsuitable.

## What LTP Provides

| Capability | Benefit |
| --- | --- |
| Checkpoint-based reliable transfer | Bundles are delivered exactly once over lossy links |
| Bundle aggregation | Amortises protocol overhead across many small bundles |
| Per-span rate control | Prevents receiver buffer overflow on bandwidth-constrained links |
| Green (best-effort) mode | Low-priority data without retransmission overhead |
| Session flow control | Bounded memory usage under sustained load |
| Link probing (ping) | Proactive detection of link failure for re-routing |

## Architecture

The implementation is split into two new crates:

```
ltp/        Protocol engine (no BPA dependency)
            · SDNV codec
            · Segment wire format encode/decode
            · Export and import session state machines
            · Retransmission timer system

ltp-cla/    BPA integration
            · Implements hardy_bpa::cla::Cla trait
            · UDP transport (port 1113)
            · Span management and configuration
            · Bundle aggregation and unpacking
            · Token bucket rate control
            · OpenTelemetry metrics
```

The `ltp` crate has no dependency on `hardy-bpa`, making the protocol engine independently testable and reusable. The `ltp-cla` crate integrates it with the BPA via the existing `Cla` trait, following the same pattern as `tcpclv4`.

## Scope: 34 Requirements

The full requirements document specifies 34 requirements across these categories:

| Category | Requirements | Description |
| --- | --- | --- |
| Protocol codec | 1–2 | SDNV encoding, segment wire format |
| Session state machines | 3–4 | Export (sender) and import (receiver) sessions |
| Reliability | 5–6, 17–18 | Closed-export retention, timers, green data, cancel flows |
| BPA integration | 7, 13, 16 | Cla trait, bpa-server feature flag, workspace structure |
| Transport & aggregation | 8–9 | UDP I/O, bundle aggregation |
| Flow & rate control | 10–11 | Max export sessions, token bucket |
| Configuration | 12, 14 | Span parameters, session number management |
| Observability | 15 | OpenTelemetry metrics (25 counters) |
| Robustness | 19–21 | Duplicate handling, extensions, block unpacking errors |
| Receiver protection | 22–24 | Stale session cleanup, import session limits, red data size cap |
| Out-of-order compensation | 25–26 | Deferred reports, asynchronous reception reports |
| Advanced features | 27–28 | Intermediate checkpoints, session recreation prevention |
| DoS protection | 29–30 | Report/checkpoint limits, closed export response limit |
| Link management | 31–32 | Purge on link down, ping-based link detection |
| Validation & timers | 33–34 | Client service ID validation, RTT-based timer computation |

## Implementation Phases

| Phase | Focus | Exit Criterion |
| --- | --- | --- |
| **1 — Scaffolding** | Crate skeletons, SDNV, segment types, feature flag | `cargo build --features ltp` succeeds |
| **2 — Green Data** | UDP I/O, green sessions, aggregation, unpacking | Bundles exchanged between two Hardy nodes |
| **3 — Reliability** | Red sessions, checkpoints, RS/RAS, retransmission, cancel flows | Correct transfer under simulated packet loss |
| **4 — Hardening** | Rate control, flow control, receiver protection, DoS limits, metrics, fuzzing | Stable under sustained load and adversarial input |
| **5 — Advanced** | Deferred reports, async reports, intermediate checkpoints, ping, purge, RTT timers | Feature parity with ION-DTN and HDTN |

Phases 1–3 deliver a functional, interoperable LTP CLA. Phase 4 makes it production-ready. Phase 5 adds optimisations drawn from operational experience in both reference implementations.

## Key Design Decisions

1. **Two-crate split** — Protocol engine is independently testable; CLA is a thin integration layer.

2. **Private addresses** — LTP peers are identified by 64-bit engine ID (not TCP address), stored as `ClaAddress::Private`. This aligns with IPN node numbers and avoids conflating UDP transport with TCP semantics.

3. **Configurable defaults** — All advanced features (deferred reports, ping, intermediate checkpoints, session recreation prevention) default to disabled. Operators opt in per-span based on link characteristics.

4. **RTT-based timers** — Supports both a simple flat `retransmit_cycle_secs` and the more precise `one_way_light_time_ms + one_way_margin_time_ms` model for deep-space links with known orbital geometry.

5. **Feature flag** — The LTP CLA is gated behind `--features ltp` in bpa-server, adding zero overhead to builds that don't need it.

## Risks and Mitigations

| Risk | Mitigation |
| --- | --- |
| Memory exhaustion from malicious senders | Max import sessions, max red data size, stale session cleanup |
| Report/checkpoint cycling on pathological links | Per-session report and checkpoint limits |
| Late segments from old sessions | Session recreation prevention (circular buffer) |
| Undetected link failure | Ping mechanism with configurable interval |
| Unnecessary retransmissions on reordering links | Deferred report sending, asynchronous reception reports |

## Dependencies

- No new external crate dependencies beyond what Hardy already uses (`tokio`, `bytes`, `thiserror`, `tracing`, `metrics`, `serde`).
- Interoperability testing requires access to ION-DTN or HDTN for cross-implementation validation.

## Deliverables

1. `ltp/` crate — protocol engine library
2. `ltp-cla/` crate — BPA convergence layer adapter
3. `bpa-server` integration behind `ltp` feature flag
4. Configuration schema documentation
5. Property-based tests for SDNV and segment codec round-trips
6. Fuzz targets for segment decoder
7. Interoperability test suite against ION-DTN
