# bp send / bp recv — File Transfer over LTP/UDP between Two Nodes

Transfer files between a macOS machine and a Linux machine using Bundle Protocol v7 over LTP/UDP. Each machine runs its own `bpa-server` with an LTP CLA, and the `bp send`/`bp recv` tools connect locally via TCPCLv4 to their respective BPA.

## Architecture

```
┌──────────┐  TCPCLv4   ┌──────────────┐   LTP/UDP    ┌──────────────┐  TCPCLv4   ┌──────────┐
│  bp send │ ──────────▶│  bpa-server  │◀────────────▶│  bpa-server  │◀────────── │  bp recv │
│  (macOS) │  localhost  │   (macOS)    │   network    │   (Linux)    │  localhost  │  (Linux) │
└──────────┘             │  ipn:1.0     │              │  ipn:2.0     │             └──────────┘
                         └──────────────┘              └──────────────┘
```

- Each machine runs a `bpa-server` with both a TCPCLv4 listener (for local tool connections) and an LTP CLA (for inter-node communication over UDP).
- `bp send` connects to the local macOS BPA via TCPCLv4, which forwards the bundle over LTP/UDP to the Linux BPA.
- `bp recv` connects to the local Linux BPA via TCPCLv4 and receives bundles delivered by the LTP CLA.

## Prerequisites

Build the workspace on both machines with the LTP feature enabled:

```bash
cargo build --release -p hardy-tools -p hardy-bpa-server --features hardy-bpa-server/ltp
```

Binaries: `target/release/bp` and `target/release/hardy-bpa-server`.

Ensure UDP port 1113 is open in both directions between the machines.

## Step 1: Configure and start the BPA on Linux

Assume the Linux machine has IP `192.168.1.100`.

Create `/etc/hardy/config.yaml`:

```yaml
node-ids: "ipn:2.0"

storage:
  metadata:
    type: "memory"
  bundle:
    type: "memory"

static-routes:
  routes-file: "/etc/hardy/static_routes"

clas:
  # TCPCLv4 for local tool connections
  - name: "tcpclv4-local"
    type: "tcpclv4"
    address: "127.0.0.1:4556"

  # LTP over UDP for inter-node communication
  - name: "ltp0"
    type: "ltp"
    bind: "0.0.0.0:1113"
    engine-id: 2
    spans:
      - engine-id: 1
        address: "192.168.1.50:1113"    # macOS machine IP
        node-ids: ["ipn:1.0"]
        max-segment-size: 1400
        aggr-size-limit: 65536
        aggr-time-limit-secs: 1
```

Create `/etc/hardy/static_routes`:

```
# Route all traffic for node 1 via the LTP span
ipn:1.*.* via ipn:1.0.0
```

Start the server:

```bash
./target/release/bpa-server --config /etc/hardy/config.yaml
```

## Step 2: Configure and start the BPA on macOS

Assume the macOS machine has IP `192.168.1.50`.

Create `~/hardy/config.yaml`:

```yaml
node-ids: "ipn:1.0"

storage:
  metadata:
    type: "memory"
  bundle:
    type: "memory"

static-routes:
  routes-file: "static_routes"

clas:
  # TCPCLv4 for local tool connections
  - name: "tcpclv4-local"
    type: "tcpclv4"
    address: "127.0.0.1:4556"

  # LTP over UDP for inter-node communication
  - name: "ltp0"
    type: "ltp"
    bind: "0.0.0.0:1113"
    engine-id: 1
    spans:
      - engine-id: 2
        address: "192.168.1.100:1113"   # Linux machine IP
        node-ids: ["ipn:2.0"]
        max-segment-size: 1400
        aggr-size-limit: 65536
        aggr-time-limit-secs: 1
```

Create `~/hardy/static_routes`:

```
# Route all traffic for node 2 via the LTP span
ipn:2.*.* via ipn:2.0.0
```

Start the server:

```bash
./target/release/bpa-server --config ~/hardy/config.yaml
```

## Step 3: Receive a file on Linux

On the Linux machine, start the receiver:

```bash
mkdir -p ./received

./target/release/bp recv \
  --peer 127.0.0.1:4556 \
  --node-id ipn:2.1 \
  --service 1 \
  --output ./received \
  --count 1
```

This connects to the local BPA via TCPCLv4, registers service endpoint `ipn:2.1.1`, and waits for an incoming bundle.

## Step 4: Send a file from macOS

On the Mac, send a file to the Linux receiver:

```bash
./target/release/bp send \
  ipn:2.1.1 \
  /path/to/myfile.txt \
  --peer 127.0.0.1:4556
```

The flow:
1. `bp send` connects to the local macOS BPA via TCPCLv4
2. The macOS BPA routes the bundle to `ipn:2.*.*` via the LTP span
3. LTP segments the bundle and sends it over UDP to the Linux BPA
4. The Linux BPA delivers the bundle to `bp recv` registered on `ipn:2.1.1`

The file appears at `./received/ipn_2_1_1_<timestamp>_<seq>` on the Linux machine.

## Complete Example: Send from macOS to Linux

### Linux (192.168.1.100)

Terminal 1 — BPA:
```bash
./target/release/bpa-server --config /etc/hardy/config.yaml
```

Terminal 2 — Receiver:
```bash
mkdir -p ./received
./target/release/bp recv \
  --peer 127.0.0.1:4556 \
  --node-id ipn:2.1 \
  --service 1 \
  --output ./received \
  --count 1
```

### macOS (192.168.1.50)

Terminal 1 — BPA:
```bash
./target/release/bpa-server --config ~/hardy/config.yaml
```

Terminal 2 — Send:
```bash
./target/release/bp send \
  ipn:2.1.1 \
  ~/Documents/report.pdf \
  --peer 127.0.0.1:4556
```

## Sending from Linux to macOS

Reverse the direction — start a receiver on macOS and send from Linux:

### macOS — Receiver:
```bash
./target/release/bp recv \
  --peer 127.0.0.1:4556 \
  --node-id ipn:1.1 \
  --service 1 \
  --output ./received \
  --count 1
```

### Linux — Send:
```bash
./target/release/bp send \
  ipn:1.1.1 \
  /path/to/file.tar.gz \
  --peer 127.0.0.1:4556
```

## Piping from stdin

```bash
echo "hello DTN over LTP" | ./target/release/bp send ipn:2.1.1 --peer 127.0.0.1:4556
```

## Receiving to stdout

```bash
./target/release/bp recv --peer 127.0.0.1:4556 --node-id ipn:2.1 --count 1 > received.bin
```

## LTP Tuning Options

Key span parameters in the BPA config:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `max-segment-size` | Max LTP segment payload (bytes). Keep below path MTU. | 1400 |
| `aggr-size-limit` | Flush aggregation buffer at this size (bytes) | 65536 |
| `aggr-time-limit-secs` | Flush aggregation buffer after this delay | 1 |
| `xmit-rate-bps` | Rate limit in bits/sec (0 = unlimited) | 0 |
| `max-retransmissions` | Retries before cancelling a session | 10 |
| `retransmit-cycle-secs` | Retransmission timer interval | 60 |
| `one-way-light-time-ms` | OWLT for deep-space links (overrides retransmit-cycle) | — |
| `framing` | `length-prefixed` (Hardy-to-Hardy) or `none` (ION interop) | length-prefixed |

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Connection refused on 4556 | Ensure `bpa-server` is running on that machine |
| No bundle received | Check static routes point to the correct node. Verify UDP 1113 is open. |
| "stdin is a terminal" error | Provide a file path or pipe data to `bp send` |
| LTP retransmissions | Check UDP connectivity: `nc -u <IP> 1113` |
| Transfer interrupted | First Ctrl+C = graceful shutdown; second = immediate exit |

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Transfer refused by remote BPA |
| 2 | Error (connection failure, file I/O, etc.) |

## Verbose Output

```bash
./target/release/bp send ipn:2.1.1 myfile.bin --peer 127.0.0.1:4556 -v=debug
```

For BPA-level LTP debugging, set `log-level: "debug"` in the BPA config.
