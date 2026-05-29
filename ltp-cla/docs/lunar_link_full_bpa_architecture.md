# LunarLink Full-BPA Integration Test — Architecture

## Mission Scenario

A lunar communications mission where a Mission Operations Centre (MOC) on Earth communicates with a Spacecraft in lunar orbit via a Ground Station. The Ground Station runs the complete Hardy DTN stack with both TCPCLv4 (terrestrial link to MOC) and LTP (space link to Spacecraft).

```
┌─────────┐              ┌──────────────────┐                    ┌───────────────┐
│   MOC   │──TCPCLv4───▶│  Ground Station  │═══════LTP/UDP═════▶│  Spacecraft   │
│(client) │◀──TCPCLv4──│    ipn:2.0       │◀══════LTP/UDP═════│   ipn:3.0     │
└─────────┘    ~1ms     │  Engine ID: 2    │    1.3s OWLT       │  Engine ID: 3 │
  Emits &               │  TCPCLv4 :4560   │    256 kbps        │  LTP :11002   │
  consumes              │  LTP     :11001  │                    │               │
  BP bundles            └──────────────────┘                    └───────────────┘
```

### Node Roles

| Node | Type | CLAs | Role |
|------|------|------|------|
| MOC | TCPCLv4 client (not a BPA) | TCPCLv4 (outbound only) | Emits commands, receives telemetry |
| Ground Station | Full Hardy BPA | TCPCLv4 + LTP | DTN relay: terrestrial ↔ space |
| Spacecraft | Full Hardy BPA | LTP | Receives commands, sends telemetry |

## Target Architecture (3-Node)

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                          │
│  ┌──────────┐         ┌─────────────────────────────────────────┐         ┌──────────┐ │
│  │   MOC    │         │         GROUND STATION BPA              │         │SPACECRAFT│ │
│  │          │         │            ipn:2.0                      │         │  BPA     │ │
│  │ TCPCLv4  │         │                                         │         │ ipn:3.0  │ │
│  │ Client   │  TCP    │  ┌──────────┐       ┌──────────┐       │  UDP    │          │ │
│  │          │────────▶│  │ TCPCLv4  │       │  LTP     │       │────────▶│  ┌─────┐ │ │
│  │ Sends:   │         │  │ CLA      │       │  CLA     │       │         │  │ LTP │ │ │
│  │ ipn:3.42 │         │  │ :4560    │       │  :11001  │       │         │  │ CLA │ │ │
│  │          │◀────────│  │          │       │          │       │◀────────│  │:1100│ │ │
│  │ Receives:│         │  └────┬─────┘       └────┬─────┘       │         │  └──┬──┘ │ │
│  │ ipn:1.x  │         │       │                   │             │         │     │    │ │
│  └──────────┘         │       ▼                   ▼             │         │     ▼    │ │
│                        │  ┌────────────────────────────────┐    │         │ ┌──────┐ │ │
│                        │  │         BPA Dispatcher          │    │         │ │ BPA  │ │ │
│                        │  │                                 │    │         │ │Dispa-│ │ │
│                        │  │  Route ipn:3.* → LTP CLA       │    │         │ │tcher │ │ │
│                        │  │  Route ipn:1.* → TCPCLv4 CLA   │    │         │ └──────┘ │ │
│                        │  │  Local: ipn:2.* → services      │    │         │          │ │
│                        │  └────────────────────────────────┘    │         │ svc 42:  │ │
│                        │                                         │         │ Capture  │ │
│                        └─────────────────────────────────────────┘         └──────────┘ │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## Current Test Implementation (2-Node)

The current `lunar_link_bpa_test.rs` implements the Ground Station ↔ Spacecraft leg with real BPA instances. The MOC role is played by the test harness injecting bundles via the `ServiceSink` API (equivalent to what TCPCLv4 would deliver from the MOC).

```
Test Harness (acts as MOC)  ──ServiceSink──→  Ground Station BPA  ──LTP/UDP──→  Spacecraft BPA
                                               ipn:2.0                            ipn:3.0
```

### Why ServiceSink Instead of TCPCLv4

In the real deployment, the MOC connects via TCPCLv4 to the Ground Station. When it sends a bundle destined for `ipn:3.42`:
1. TCPCLv4 CLA receives the bundle
2. Calls `sink.dispatch(bundle)` into the BPA
3. BPA routes `ipn:3.42` → LTP CLA peer (engine 3)
4. LTP CLA forwards via UDP

In the test, step 2 is equivalent to calling `ServiceSink.send(bundle)` — both inject a bundle into the BPA's routing pipeline. The LTP transport from step 3 onward is identical.

### Future: Full 3-Node Test with TCPCLv4

To test the complete MOC → GS → Spacecraft path including TCPCLv4:

```yaml
# Ground Station config (both CLAs)
clas:
  - name: tcp0
    type: tcpclv4
    address: "[::]:4560"        # MOC connects here
  - name: ltp0
    type: ltp
    bind: "[::]:11001"
    engine-id: 2
    spans:
      - engine-id: 3
        address: "127.0.0.1:11002"
        node-ids: ["ipn:3.0"]
```

The MOC would use `bp ping ipn:3.42 127.0.0.1:4560` — connecting to the Ground Station's TCPCLv4 port and sending a bundle destined for the Spacecraft. The Ground Station BPA routes it onward via LTP.

## System Architecture

Each node is a complete Hardy BPA instance — real routing, real dispatch, real LTP transport. No mocks.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        GROUND STATION (ipn:2.0)                              │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                         Hardy BPA                                   │     │
│  │                                                                     │     │
│  │  ┌─────────────┐    ┌──────────────┐    ┌───────────────────────┐ │     │
│  │  │ Service     │    │  Dispatcher  │    │    CLA Registry       │ │     │
│  │  │ Registry    │    │              │    │                       │ │     │
│  │  │             │    │  • Route     │    │  ┌─────────────────┐  │ │     │
│  │  │ svc 42:     │◀───│    bundle    │───▶│  │   LTP CLA       │  │ │     │
│  │  │ Capture     │    │  • Match     │    │  │   "ltp0"        │  │ │     │
│  │  │ Service     │    │    dest EID  │    │  │                 │  │ │     │
│  │  │             │    │    to peer   │    │  │  Peer: ipn:3.0  │  │ │     │
│  │  └──────┬──────┘    └──────────────┘    │  │  → engine 3     │  │ │     │
│  │         │                                │  │  → 127.0.0.1:   │  │ │     │
│  │         │ ServiceSink                    │  │    11002         │  │ │     │
│  │         │ .send()                        │  └────────┬────────┘  │ │     │
│  │         ▼                                │           │            │ │     │
│  │  ┌─────────────┐                        └───────────┼────────────┘ │     │
│  │  │ In-Memory   │                                    │              │     │
│  │  │ Storage     │                                    │              │     │
│  │  └─────────────┘                                    │              │     │
│  └─────────────────────────────────────────────────────┼──────────────┘     │
│                                                         │                    │
│  ┌──────────────────────────────────────────────────────┼──────────────┐    │
│  │                      LTP CLA Internals               │              │    │
│  │                                                      ▼              │    │
│  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────────────┐  │    │
│  │  │ Aggregation   │  │ Export        │  │ UDP Socket            │  │    │
│  │  │ Buffer        │─▶│ Session       │─▶│ 0.0.0.0:11001         │  │    │
│  │  │ (immediate    │  │ State Machine │  │                       │  │    │
│  │  │  flush)       │  │               │  │ send_to(127.0.0.1:    │  │    │
│  │  └───────────────┘  │ • Segment     │  │         11002)        │  │    │
│  │                      │ • Encode      │  └───────────┬───────────┘  │    │
│  │  ┌───────────────┐  │ • Checkpoint  │              │              │    │
│  │  │ Import        │  └───────────────┘              │              │    │
│  │  │ Session       │                                  │              │    │
│  │  │ State Machine │◀─── Receive Loop ◀──────────────┘              │    │
│  │  │               │     (decode segments)                           │    │
│  │  │ • Reassemble  │                                                 │    │
│  │  │ • Report      │                                                 │    │
│  │  │ • Deliver     │                                                 │    │
│  │  └───────────────┘                                                 │    │
│  └────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘

                              │
                              │  UDP Datagrams (LTP Segments)
                              │  127.0.0.1:11001 ↔ 127.0.0.1:11002
                              │
                              ▼

┌─────────────────────────────────────────────────────────────────────────────┐
│                         SPACECRAFT (ipn:3.0)                                 │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                         Hardy BPA                                   │     │
│  │                                                                     │     │
│  │  ┌─────────────┐    ┌──────────────┐    ┌───────────────────────┐ │     │
│  │  │ Service     │    │  Dispatcher  │    │    CLA Registry       │ │     │
│  │  │ Registry    │    │              │    │                       │ │     │
│  │  │             │    │  • Route     │    │  ┌─────────────────┐  │ │     │
│  │  │ svc 42:     │◀───│    bundle    │───▶│  │   LTP CLA       │  │ │     │
│  │  │ Capture     │    │  • Match     │    │  │   "ltp0"        │  │ │     │
│  │  │ Service     │    │    dest EID  │    │  │                 │  │ │     │
│  │  │             │    │    to peer   │    │  │  Peer: ipn:2.0  │  │ │     │
│  │  └─────────────┘    └──────────────┘    │  │  → engine 2     │  │ │     │
│  │                                          │  │  → 127.0.0.1:   │  │ │     │
│  │                                          │  │    11001         │  │ │     │
│  │                                          │  └────────┬────────┘  │ │     │
│  │                                          └───────────┼────────────┘ │     │
│  └──────────────────────────────────────────────────────┼──────────────┘     │
│                                                          │                    │
│  ┌───────────────────────────────────────────────────────┼──────────────┐    │
│  │                      LTP CLA Internals                │              │    │
│  │                                                       ▼              │    │
│  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────────────┐   │    │
│  │  │ Aggregation   │  │ Export        │  │ UDP Socket            │   │    │
│  │  │ Buffer        │─▶│ Session       │─▶│ 0.0.0.0:11002         │   │    │
│  │  └───────────────┘  └───────────────┘  └───────────┬───────────┘   │    │
│  │                                                      │              │    │
│  │  ┌───────────────┐                                   │              │    │
│  │  │ Import        │◀─── Receive Loop ◀────────────────┘              │    │
│  │  │ Session       │                                                   │    │
│  │  └───────────────┘                                                   │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow: Command Uplink (Ground → Spacecraft)

```
Step  Component                    Action
────  ─────────────────────────    ──────────────────────────────────────────
 1    Test Harness                 build_bundle("ipn:2.42", "ipn:3.42", payload)
 2    Ground CaptureService        .send_bundle(bytes) via ServiceSink
 3    Ground BPA ServiceSink       .send(bundle_bytes) → Dispatcher
 4    Ground BPA Dispatcher        Route ipn:3.42 → peer ipn:3.0 → LTP CLA
 5    Ground LTP CLA forward()     Decode engine_id=3 from ClaAddress::Private
 6    Ground Aggregation Buffer    .append(bundle) → .flush() (immediate)
 7    Ground Export Session        Segment block into ≤1400-byte data segments
 8    Ground Token Bucket          Rate-limit at configured rate
 9    Ground UDP Socket            send_to(127.0.0.1:11002) for each segment
10    [UDP Network]                Localhost loopback
11    Spacecraft UDP Socket        recv_from() → raw bytes
12    Spacecraft Receive Loop      segment::decode() → Segment::Data
13    Spacecraft Import Session    on_data_segment() → record extent → report
14    Spacecraft Import Session    Block complete → DeliverBlock action
15    Spacecraft deliver_block()   unpack_block() → extract bundle bytes
16    Spacecraft Sink.dispatch()   Bundle → BPA Dispatcher
17    Spacecraft BPA Dispatcher    Route ipn:3.42 → local service 42
18    Spacecraft CaptureService    .on_receive(data) → stored in Vec ✓
```

## Data Flow: Telemetry Downlink (Spacecraft → Ground)

Same flow in reverse, with multi-segment transfer for the 10 KB payload:

```
Step  Component                    Action
────  ─────────────────────────    ──────────────────────────────────────────
 1    Spacecraft CaptureService    .send_bundle(10KB telemetry)
 2    Spacecraft BPA Dispatcher    Route ipn:2.42 → peer ipn:2.0 → LTP CLA
 3    Spacecraft LTP CLA           forward() → aggregate → flush
 4    Spacecraft Export Session    Segment into 8 data segments (7×1400 + 1×remainder)
 5    Spacecraft UDP Socket        send_to(127.0.0.1:11001) × 8 segments
 6    [UDP Network]                Localhost loopback
 7    Ground Receive Loop          Decode 8 segments sequentially
 8    Ground Import Session        Record extents: [0,1400) [1400,2800) ... [9800,10318)
 9    Ground Import Session        Segment 8 (RedEob): block complete!
10    Ground Import Session        Generate Report Segment (full coverage)
11    Ground deliver_block()       unpack_block() → 10240-byte bundle
12    Ground BPA Dispatcher        Route ipn:2.42 → local service 42
13    Ground CaptureService        .on_receive(data) → stored ✓
```

## Report/Ack Cycle (Background)

```
Step  Component                    Action
────  ─────────────────────────    ──────────────────────────────────────────
 A    Spacecraft Import Session    Generates Report Segment (claims: [0, block_len))
 B    Spacecraft UDP Socket        send_to(127.0.0.1:11001) RS
 C    Ground Receive Loop          Decode → Segment::Report
 D    Ground Export Session        on_report() → all bytes acknowledged
 E    Ground Export Session        Send Report-Ack, transition to Complete
 F    Ground UDP Socket            send_to(127.0.0.1:11002) RAS
 G    Spacecraft Receive Loop      Decode → Segment::ReportAck
 H    Spacecraft Import Session    on_report_ack() → cancel retransmit timer
```

## Components — Real vs Mock

| Component | Status | Notes |
|-----------|--------|-------|
| Hardy BPA (Dispatcher, Router, Storage) | **REAL** | `BpaBuilder::new().build()` |
| Node ID resolution | **REAL** | `NodeIds::try_from([ipn:2.0])` |
| CLA Registry + Peer Registration | **REAL** | `sink.add_peer()` during LTP CLA startup |
| Bundle routing (EID → CLA) | **REAL** | BPA matches `ipn:3.*` → LTP peer |
| Service Registry + Dispatch | **REAL** | Service 42 registered, bundles delivered |
| LTP CLA (Cla trait impl) | **REAL** | `LtpCla::new(config)` |
| LTP Export/Import Sessions | **REAL** | Full state machines with timers |
| UDP Transport | **REAL** | Actual socket I/O on localhost |
| BPv7 Bundle Construction | **REAL** | `hardy_bpv7::builder::Builder` |
| Storage Backend | **REAL** (in-memory) | `MetadataMemStorage` + `BundleMemStorage` |
| Network Delay | NOT simulated | Localhost is instant; see `lunar_link_test.rs` for delay proxy |

## Configuration

### Ground Station (ipn:2.0)

```rust
Config {
    bind: "0.0.0.0:11001",
    engine_id: Some(2),
    client_service_id: 1,
    spans: [{
        engine_id: 3,                          // Spacecraft
        address: "127.0.0.1:11002",            // Spacecraft LTP port
        node_ids: ["ipn:3.0"],                 // Routes ipn:3.* here
        max_segment_size: 1400,
        retransmit_cycle_secs: 10,
        aggr_time_limit_secs: 0,               // Immediate flush
        one_way_light_time_ms: Some(50),       // 50ms for fast test
        one_way_margin_time_ms: 20,
    }],
}
```

### Spacecraft (ipn:3.0)

```rust
Config {
    bind: "0.0.0.0:11002",
    engine_id: Some(3),
    client_service_id: 1,
    spans: [{
        engine_id: 2,                          // Ground Station
        address: "127.0.0.1:11001",            // Ground LTP port
        node_ids: ["ipn:2.0"],                 // Routes ipn:2.* here
        max_segment_size: 1400,
        retransmit_cycle_secs: 10,
        aggr_time_limit_secs: 0,
        one_way_light_time_ms: Some(50),
        one_way_margin_time_ms: 20,
    }],
}
```

## Test Cases

| Test | Direction | Payload | Segments | Validates |
|------|-----------|---------|----------|-----------|
| `lunar_link_bpa_command_to_spacecraft` | Ground → Spacecraft | 45 bytes ("LUNAR_CMD: ...") | 1 | Full BPA routing + LTP + service delivery |
| `lunar_link_bpa_telemetry_to_ground` | Spacecraft → Ground | 10,240 bytes | ~8 | Multi-segment reassembly through full stack |

## What This Proves

1. **BPA routing works with LTP** — The BPA correctly resolves `ipn:3.42` to the LTP CLA peer registered for `ipn:3.0`
2. **LTP CLA integrates correctly** — The `Cla` trait implementation (forward, on_register, address_type) works with the real BPA
3. **End-to-end bundle integrity** — A BPv7 bundle constructed on one node arrives intact at the service on the other node
4. **Multi-segment reliability** — Large payloads are segmented, transmitted, reassembled, and delivered correctly
5. **Bidirectional operation** — Both nodes can send and receive simultaneously
6. **Service dispatch** — Bundles arrive at the correct service number on the destination node

## Running

```bash
# With debug output
cargo test -p hardy-ltp-cla --test lunar_link_bpa_test -- --nocapture

# Quick pass/fail
cargo test -p hardy-ltp-cla --test lunar_link_bpa_test
```

## Example Output

```
=== LunarLink Full-BPA Test ===
    Ground Station: ipn:2.0 (LTP port 11001)
    Spacecraft: ipn:3.0 (LTP port 11002)

[Ground Station] Sending command to spacecraft (ipn:3.42)...
DEBUG hardy_ltp_cla::span: created export session engine_id=3 session_number=1
DEBUG hardy_ltp_cla::span: TX segment engine_id=3 bytes=138 address=127.0.0.1:11002
DEBUG hardy_ltp_cla::span: created import session engine_id=2 session_number=1
DEBUG hardy_ltp_cla::span: delivering block engine_id=2 block_bytes=125
DEBUG hardy_ltp_cla::span: dispatching bundle to BPA engine_id=2 bundle_index=0 bundle_bytes=121
[Spacecraft] Received bundle (121 bytes) via LTP
=== Command delivered successfully via full BPA + LTP pipeline ===
```

## Relationship to Other Tests

| Test File | What It Tests | BPA? | Network Delay? |
|-----------|---------------|------|----------------|
| `end_to_end.rs` | LTP Span-to-Span (raw bytes) | No (mock sink) | No |
| `full_stack_test.rs` | LTP Span-to-Span (real BPv7 bundles) | No (mock sink) | No |
| `lunar_link_test.rs` | LTP Span-to-Span with delay proxy | No (mock sink) | Yes (50ms) |
| **`lunar_link_bpa_test.rs`** | **Full BPA-to-BPA via LTP** | **Yes (real)** | No |
