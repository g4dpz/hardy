# Hardy LTP Interoperability Test

Bidirectional bundle exchange between two Hardy BPA servers using the LTP convergence layer adapter over UDP.

## Architecture

```
┌─────────────────┐         UDP/LTP          ┌─────────────────┐
│   Hardy Node 1  │◄───────────────────────►  │   Hardy Node 2  │
│   ipn:1.0       │    port 1113 ↔ 1114      │   ipn:2.0       │
│   engine-id: 1  │                           │   engine-id: 2  │
│   echo svc: 7   │                           │   echo svc: 7   │
└─────────────────┘                           └─────────────────┘
```

Both nodes use in-memory storage (no external dependencies).

## Running

```bash
# Full build + test
./tests/interop/hardy-ltp/test_ltp_ping.sh

# Skip build (if already built with --features ltp)
./tests/interop/hardy-ltp/test_ltp_ping.sh --skip-build

# Custom ping count
./tests/interop/hardy-ltp/test_ltp_ping.sh --count 20
```

## Prerequisites

- Rust toolchain (for building)
- No external services required (uses in-memory storage)

## What it tests

1. **Node 1 → Node 2**: Bundle sent from ipn:1.12345 to ipn:2.7 (echo service) via LTP/UDP. Verifies the echo response arrives back.
2. **Node 2 → Node 1**: Same test in reverse direction.

This exercises the full LTP stack:
- Bundle aggregation (length-prefixed framing)
- Export session creation and segmentation
- UDP segment transmission
- Import session reassembly
- Report generation and acknowledgement
- Block delivery and bundle unpacking
- BPA dispatch to echo service
- Return path (echo response via LTP)

## Configuration

Each node is configured with:
- `max-segment-size: 1400` (fits in standard MTU)
- `retransmit-cycle-secs: 5` (fast retransmission for local testing)
- `aggr-time-limit-secs: 1` (flush aggregation buffer after 1 second)
- In-memory metadata and bundle storage

## Troubleshooting

If tests fail, the script preserves the temp directory with logs:
- `node1.log` — Node 1 BPA server output
- `node2.log` — Node 2 BPA server output
- `node1_config.toml` / `node2_config.toml` — Generated configs

Common issues:
- **Port conflict**: Ports 1113/1114 already in use. Kill existing processes or change ports in the script.
- **Build failure**: Ensure `--features ltp` is passed. The LTP CLA is behind a feature flag.
- **Timeout**: Increase `--count` or check that UDP isn't being blocked by a firewall.
