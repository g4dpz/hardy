# hardy-ltp

Licklider Transmission Protocol (LTP) engine library implementing [RFC 5326](https://datatracker.ietf.org/doc/html/rfc5326).

Part of the [Hardy](https://github.com/ricktaylor/hardy) DTN Bundle Protocol implementation.

## Installation

```toml
[dependencies]
hardy-ltp = "0.1"
```

## Overview

This crate implements the LTP protocol engine for reliable data transfer over high-delay, asymmetric links such as deep-space communication channels. It provides the wire-format codec, session state machines, and retransmission logic without any dependency on the Hardy BPA — making it independently testable and reusable.

LTP splits data blocks into segments transmitted over an unreliable link, using selective acknowledgement (via reception reports) and timer-driven retransmission to achieve reliability for "red" data while allowing best-effort delivery for "green" data.

## Modules

| Module | Description |
|--------|-------------|
| `sdnv` | Self-Delimiting Numeric Value (SDNV) encoder/decoder as specified in RFC 5326 §3.1. |
| `segment` | LTP segment wire format: encoding, decoding, and type definitions for all 13 segment types. |
| `session` | Export and import session state machines managing segmentation, checkpointing, retransmission, and block reassembly. |

## Features

- **SDNV codec** — encode/decode arbitrary `u64` values with overflow detection and length computation
- **Full segment codec** — all LTP segment types including data (red/green), reports, report-acks, cancels, and cancel-acks
- **Export sessions** — block segmentation, configurable intermediate checkpoints, timer-driven retransmission, and max-checkpoint cancellation
- **Import sessions** — segment reassembly with extent tracking, deferred report generation, colour validation, and max-size enforcement
- **Green data** — best-effort delivery path with no timers or reports
- **No runtime dependency** — uses only `tokio::time` for timer representation; no spawned tasks

## Usage

```rust
use hardy_ltp::{sdnv, segment, session};
use bytes::{Bytes, BytesMut};

// Encode/decode an SDNV value
let mut buf = BytesMut::new();
sdnv::encode(42, &mut buf);
let value = sdnv::decode(&mut &buf[..]).unwrap();
assert_eq!(value, 42);

// Decode a segment from wire bytes
let mut reader = &wire_data[..];
let seg = segment::decode(&mut reader).unwrap();
```

## Testing

The crate includes extensive unit tests and property-based tests (via `proptest`):

```bash
cargo test -p hardy-ltp
```

Property tests verify:
- SDNV round-trip correctness, truncation rejection, and overflow detection
- Segment encode/decode round-trip for all segment types
- Export session checkpoint serial monotonicity and retransmission coverage
- Import session block delivery and extent map invariants

## Documentation

- [Design](docs/design.md)

## Licence

Apache 2.0 — see [LICENSE](../LICENSE)
