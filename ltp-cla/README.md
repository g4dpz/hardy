# hardy-ltp-cla

LTP Convergence Layer Adapter for the Bundle Protocol, implementing [RFC 5326](https://datatracker.ietf.org/doc/html/rfc5326) transport over UDP.

Part of the [Hardy](https://github.com/ricktaylor/hardy) DTN Bundle Protocol implementation.

## Installation

```toml
[dependencies]
hardy-ltp-cla = "0.1"
```

## Overview

This crate integrates the [`hardy-ltp`](../ltp/) protocol engine with the Hardy BPA via the `Cla` trait, providing a complete UDP-based convergence layer for bundle transport over high-delay links. It manages per-peer spans, bundle aggregation, rate limiting, session lifecycle, and link health monitoring.

Designed for deep-space and challenged network scenarios, the adapter supports configurable one-way light time, token-bucket rate control, session recreation prevention, and ping-based link liveness detection.

## Modules

| Module | Description |
|--------|-------------|
| `cla` | CLA trait implementation (`LtpCla`) — registers with the BPA and manages span lifecycle. |
| `span` | Per-link state: export/import sessions, aggregation buffer, rate control, timers, and link health. |
| `engine` | UDP receive loop, segment dispatch, and inbound session routing. |
| `config` | Configuration types (`Config`, `SpanConfig`) with serde support. |
| `block` | Bundle aggregation framing — packs multiple bundles into a single LTP block with length-prefix encoding. |

## Features

- **UDP transport** — sends and receives LTP segments over UDP sockets
- **Bundle aggregation** — packs multiple bundles into a single LTP block to reduce per-bundle overhead
- **Token-bucket rate control** — configurable bits-per-second rate limiting with burst allowance
- **One-way light time** — adjustable OWLT for retransmission timer computation on deep-space links
- **Session management** — concurrent export/import sessions with configurable limits
- **Link health monitoring** — ping-based liveness detection with configurable intervals and timeouts
- **Session recreation prevention** — history-based deduplication to reject stale segments
- **Deferred reports** — configurable delay before generating reception reports to batch acknowledgements
- **TVR integration** — timer suspension/resumption on scheduled contact window transitions (RFC 5326 §6.5/§6.6), outbound segment queuing during link-down, and dynamic rate control updates from contact bandwidth
- **Metrics** — OpenTelemetry-compatible counters for sessions, segments, and throughput
- Feature flag: `serde` — enables serialization for configuration structs

## Usage

```rust
use hardy_ltp_cla::{cla::LtpCla, config::Config};

// Create and configure the LTP CLA
let config = Config {
    bind_address: "0.0.0.0:1113".parse().unwrap(),
    spans: vec![span_config],
    ..Default::default()
};

let ltp_cla = LtpCla::new(config).await?;

// Register with the BPA
ltp_cla.register(&bpa, "ltp0".to_string()).await?;
```

## Configuration

Key span configuration options:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `max_segment_size` | Maximum LTP segment payload size (bytes) | 1400 |
| `max_retransmissions` | Retransmission attempts before cancellation | 10 |
| `one_way_light_time_secs` | Link OWLT for timer computation | 0 |
| `rate_limit_bps` | Token-bucket rate limit (bits/sec), 0 = unlimited | 0 |
| `max_import_sessions` | Concurrent inbound session limit | 100 |
| `checkpoint_every_n` | Intermediate checkpoint interval (segments), 0 = EOB only | 0 |
| `ping_interval_secs` | Link liveness ping interval, 0 = disabled | 0 |
| `defer_report_ms` | Delay before generating reports (ms), 0 = immediate | 0 |
| `tvr_timer_suspension` | Suspend timers on TVR link events (RFC 5326 §6.5/§6.6) | true |
| `link_down_queue_max_bytes` | Max outbound queue size during link-down (bytes) | 10 MB |
| `tvr_rate_update` | Update rate limiter from TVR contact bandwidth | true |

## Testing

The crate includes unit tests, property-based tests, integration tests, and end-to-end tests:

```bash
# All tests
cargo test -p hardy-ltp-cla

# Integration tests only
cargo test -p hardy-ltp-cla --test end_to_end --test full_stack_test --test lunar_link_test
```

Test suites:
- **Unit tests** — span logic, aggregation buffer, rate control, session management, TVR link state transitions
- **Property tests** — session number monotonicity, rate invariants, recreation prevention, queue FIFO ordering, timer suspend/resume, link state idempotency
- **End-to-end** — full LTP transfer of 100 KB and 1 MB payloads over localhost UDP
- **Full-stack** — BPv7 bundles encoded, transported over LTP, and delivered to a sink
- **Lunar link** — simulated deep-space scenario with realistic OWLT and rate constraints
- **TVR integration** — contact close/open flows, timer suspension/resumption, rate-limited queue flush

## Documentation

- [Design](docs/design.md)
- [Test Architecture](docs/test_architecture.md)
- [LunarLink Architecture](docs/lunar_link_architecture.md)
- [LunarLink Full-BPA Architecture](docs/lunar_link_full_bpa_architecture.md)

## Licence

Apache 2.0 — see [LICENSE](../LICENSE)
