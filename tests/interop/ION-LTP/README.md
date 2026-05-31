# ION LTP Interoperability Test

Bidirectional BPv7 bundle exchange between Hardy and NASA JPL's
[ION](https://github.com/nasa/ION-DTN) implementation over LTP/UDP.

Unlike the [STCP-based ION test](../ION/), this test uses Hardy's
built-in LTP CLA (`--features ltp`) directly — no external CLA process
is needed. ION is configured with `udplso`/`udplsi` for LTP transport.

## Prerequisites

- **Docker** — builds and runs the ION container image
- **Rust toolchain** — builds Hardy with the `ltp` and `tcpclv4` features
- **iproute2** (Linux) — provides `tc netem` for packet loss simulation (Test 4)
- **NET_ADMIN capability** — required for `tc netem` inside the container

## Quick Start

```bash
# Full build + test (builds Hardy, Docker image, runs all 4 tests)
./tests/interop/ION-LTP/test_ion_ltp_ping.sh

# Skip Hardy rebuild (use existing binaries)
./tests/interop/ION-LTP/test_ion_ltp_ping.sh --skip-build

# Custom ping count
./tests/interop/ION-LTP/test_ion_ltp_ping.sh --skip-build --count 10

# Use local ION binaries instead of Docker
./tests/interop/ION-LTP/test_ion_ltp_ping.sh --no-docker
```

## CLI Options

| Option | Description |
|--------|-------------|
| `--skip-build` | Skip building Hardy binaries (use existing `target/release/` binaries) |
| `--count N` | Number of pings to send per test (default: 5) |
| `--no-docker` | Use locally installed ION binaries instead of Docker container |

## Test Scenarios

### Test 1 — Hardy → ION echo (basic LTP session lifecycle)

Hardy sends BPv7 echo requests to `ipn:2.7` via LTP/UDP. ION's `bpecho`
service responds. Validates that Hardy's LTP export sessions produce
segments ION can receive and reassemble, and that Hardy correctly
processes ION's report segments.

### Test 2 — ION → Hardy echo (reverse direction)

ION's `bping` tool sends BPv7 echo requests to `ipn:1.7` via LTP/UDP.
Hardy's echo service responds. Validates that Hardy's LTP import sessions
correctly reassemble segments produced by ION's `udplso` and that Hardy
generates valid report segments.

### Test 3 — Large bundle multi-segment transfer (100KB, ~73 segments)

Sends a 100KB payload from Hardy to ION's echo service. At 1400 bytes per
segment, this requires approximately 73 LTP data segments, exercising the
full segmentation and reassembly path in both directions (request and
echo response).

### Test 4 — Packet loss recovery (20% loss with tc netem)

Applies 20% packet loss on the loopback interface using `tc netem`, then
verifies that LTP retransmission recovers lost segments and bundle
delivery eventually succeeds within 60 seconds. Exercises the
checkpoint/report/retransmit cycle defined in RFC 5326.

If `tc netem` fails (missing `NET_ADMIN` capability), this test is
skipped with a warning rather than failing the suite.

## Interactive Mode

The `start_ion_ltp.sh` script launches Hardy and ION for manual testing:

```bash
./tests/interop/ION-LTP/start_ion_ltp.sh [--skip-build]
```

This starts both nodes and prints connection info. You can then manually
run commands like:

```bash
# Hardy pings ION
bp ping ipn:2.7 127.0.0.1:4560 --source ipn:1.12345 --count 3 --no-sign -W 10s

# ION pings Hardy
docker exec ion-ltp-interop-test bping -c 3 -q 5 ipn:2.1 ipn:1.7
```

Press Ctrl+C to stop both nodes and clean up.

## Network Topology

```
┌─────────────────────────┐       UDP/LTP (loopback)       ┌─────────────────────────┐
│      Hardy Node         │◄──────────────────────────────►│   ION Container         │
│   ipn:1.0               │   Hardy:1114 ↔ ION:1113        │   ipn:2.0               │
│   engine-id: 1          │                                │   engine-id: 2          │
│   LTP bind: [::]:1114   │                                │   udplsi: 0.0.0.0:1113  │
│   echo svc: 7           │                                │   bpecho svc: 7         │
│   TCPCLv4: [::]:4560    │                                │   udplso → host:1114    │
└─────────────────────────┘                                └─────────────────────────┘
        ▲                                                           ▲
        │ TCPCLv4 (localhost)                                        │ docker exec
        │                                                           │
   ┌────┴────┐                                                 ┌────┴────┐
   │ bp ping │                                                 │  bping  │
   └─────────┘                                                 └─────────┘
```

| Parameter | Hardy | ION |
|-----------|-------|-----|
| Node ID | `ipn:1.0` | `ipn:2.0` |
| Engine ID | 1 | 2 |
| LTP port (UDP) | 1114 | 1113 |
| Echo service | 7 | 7 |
| TCPCLv4 port | 4560 | — |
| Segment size | 1400 bytes | 1400 bytes |

Hardy uses TCPCLv4 on port 4560 as the local interface for the `bp` CLI
tool. LTP/UDP on ports 1113–1114 carries the actual inter-node traffic.

## File Layout

```
ION-LTP/
  README.md                    # This file
  test_ion_ltp_ping.sh         # Automated test runner
  start_ion_ltp.sh             # Interactive launcher for manual testing
  docker/
    Dockerfile                 # Multi-stage ION build with LTP support
    start_ion_ltp              # Container entrypoint (generates ionrc/ltprc/bprc/ipnrc)
```

## Troubleshooting

### ION shared memory issues

ION uses SysV shared memory for its SDR. If a previous run crashed, stale
segments may prevent ION from starting:

```bash
# Clean up ION shared memory (run on host since container uses --ipc=host)
docker run --rm --ipc=host --entrypoint killm ion-ltp-interop

# Or manually remove shared memory segments
ipcrm --all=shm
ipcrm --all=sem
```

### Port conflicts (1113, 1114 already in use)

If another process is using the LTP ports:

```bash
# Check what's using the ports
ss -ulnp | grep -E ':(1113|1114) '

# Kill stale ION containers
docker rm -f ion-ltp-interop-test
```

### Missing NET_ADMIN capability (Test 4 skipped)

Test 4 requires `NET_ADMIN` to apply `tc netem` packet loss rules. If
running in a restricted environment (e.g., rootless Docker), Test 4 will
be skipped automatically. The other three tests still run normally.

### Docker image build failures

If the ION Docker image fails to build:

```bash
# Rebuild from scratch
docker rmi ion-ltp-interop
./tests/interop/ION-LTP/test_ion_ltp_ping.sh
```

Common causes: network issues downloading ION source, missing build
dependencies in the base image.

### Hardy fails to start (LTP feature not enabled)

If `hardy-bpa-server` exits immediately with a config parse error about
`type = "ltp"`, the binary was built without the LTP feature:

```bash
cargo build --release -p hardy-bpa-server --features hardy-bpa-server/ltp,hardy-bpa-server/tcpclv4
```

## Differences from STCP Test

This test (`ION-LTP/`) differs from the STCP-based test (`ION/`) in
several ways:

| | ION-LTP (this test) | ION (STCP test) |
|---|---|---|
| Transport | LTP/UDP | STCP/TCP |
| Hardy CLA | Built-in LTP (`--features ltp`) | External `mtcp-cla` binary |
| ION daemons | `udplso` / `udplsi` | `stcpclo` / `stcpcli` |
| Ports | UDP 1113, 1114 | TCP 4557, 4558 |
| Extra tests | Multi-segment, packet loss | — |
| Docker caps | `NET_ADMIN` (for netem) | — |
| ION config | `ltprc` + `ltpadmin` | STCP in `bprc` |
