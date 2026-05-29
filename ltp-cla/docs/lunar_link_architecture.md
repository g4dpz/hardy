# LunarLink Mission Simulation — Architecture

## Mission Scenario

A lunar communications mission where a Mission Operations Centre (MOC) on Earth communicates with a satellite in lunar orbit via a ground station. The ground station and satellite both run the Hardy DTN stack with LTP.

```
                    Earth                              Space
    ┌─────────┐              ┌──────────────┐                    ┌───────────┐
    │   MOC   │──TCPCLv4───▶│Ground Station│══════LTP/UDP══════▶│ Satellite │
    │ ipn:1.x │◀──TCPCLv4──│   ipn:2.x    │◀═════LTP/UDP══════│  ipn:3.x  │
    └─────────┘   ~1ms      └──────────────┘   1.3s OWLT       └───────────┘
                                                 256 kbps
```

## Test Architecture

The integration test simulates this topology on localhost using a **delay proxy** to inject realistic propagation delay between the ground station and satellite LTP spans.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Test Harness                                         │
│                                                                                   │
│  ┌────────────────────────┐                      ┌────────────────────────┐      │
│  │   GROUND STATION       │                      │      SATELLITE         │      │
│  │   Engine ID: 2         │                      │      Engine ID: 3      │      │
│  │   ipn:2.0              │                      │      ipn:3.0           │      │
│  │                        │                      │                        │      │
│  │  ┌──────────────────┐  │                      │  ┌──────────────────┐  │      │
│  │  │ Aggregation Buf  │  │                      │  │ Aggregation Buf  │  │      │
│  │  │ (command packing) │  │                      │  │ (telemetry pack) │  │      │
│  │  └────────┬─────────┘  │                      │  └────────┬─────────┘  │      │
│  │           │             │                      │           │             │      │
│  │  ┌────────▼─────────┐  │                      │  ┌────────▼─────────┐  │      │
│  │  │ Export Session    │  │                      │  │ Export Session    │  │      │
│  │  │ (segmentation)   │  │                      │  │ (segmentation)   │  │      │
│  │  └────────┬─────────┘  │                      │  └────────┬─────────┘  │      │
│  │           │             │                      │           │             │      │
│  │  ┌────────▼─────────┐  │                      │  ┌────────▼─────────┐  │      │
│  │  │ Token Bucket      │  │                      │  │ Token Bucket      │  │      │
│  │  │ (256 kbps rate)   │  │                      │  │ (256 kbps rate)   │  │      │
│  │  └────────┬─────────┘  │                      │  └────────┬─────────┘  │      │
│  │           │             │                      │           │             │      │
│  │  ┌────────▼─────────┐  │                      │  ┌────────▼─────────┐  │      │
│  │  │ UDP Socket A      │  │                      │  │ UDP Socket B      │  │      │
│  │  │ 127.0.0.1:A       │  │                      │  │ 127.0.0.1:B       │  │      │
│  │  └────────┬─────────┘  │                      │  └────────┬─────────┘  │      │
│  │           │ ▲           │                      │           │ ▲           │      │
│  └───────────┼─┼───────────┘                      └───────────┼─┼───────────┘      │
│              │ │                                               │ │                   │
│              │ │  ┌─────────────────────────────────────────┐  │ │                   │
│              │ │  │         DELAY PROXY (Space Link)         │  │ │                   │
│              │ │  │                                           │  │ │                   │
│              │ │  │  ┌─────────────┐    ┌─────────────┐     │  │ │                   │
│              │ └──┼──│ Proxy Sock B│    │ Proxy Sock A│──┐  │  │ │                   │
│              │    │  │ (sat→gnd)   │    │ (gnd→sat)   │  │  │  │ │                   │
│              │    │  └──────┬──────┘    └──────┬──────┘  │  │  │ │                   │
│              │    │         │                   │         │  │  │ │                   │
│              │    │    ┌────▼────┐         ┌────▼────┐   │  │  │ │                   │
│              │    │    │ sleep   │         │ sleep   │   │  │  │ │                   │
│              │    │    │ 50ms    │         │ 50ms    │   │  │  │ │                   │
│              │    │    │(1.3s    │         │(1.3s    │   │  │  │ │                   │
│              │    │    │ real)   │         │ real)   │   │  │  │ │                   │
│              │    │    └────┬────┘         └────┬────┘   │  │  │ │                   │
│              │    │         │                   │         │  │  │ │                   │
│              │    │         ▼                   ▼         │  │  │ │                   │
│              │    │    send_to(A)          send_to(B)     │  │  │ │                   │
│              └────┼─────────┘                   └────────┼──┘  │ │                   │
│                   │                                       │     │ │                   │
│                   └───────────────────────────────────────┘     │ │                   │
│                                                                  │ │                   │
│  ┌───────────────────────────────────────────────────────────────┼─┼─────────────┐   │
│  │                     RECEIVE LOOPS (tokio tasks)                │ │             │   │
│  │                                                                │ │             │   │
│  │  Socket A recv → decode → route to Ground Span                 │ │             │   │
│  │    • Report segments → on_export_report()                      │ │             │   │
│  │    • Data segments from satellite → on_import_data_segment()   │ │             │   │
│  │                                                                │ │             │   │
│  │  Socket B recv → decode → route to Satellite Span ◀────────────┘ │             │   │
│  │    • Data segments from ground → on_import_data_segment()         │             │   │
│  │    • Report segments → on_export_report()                         │             │   │
│  └───────────────────────────────────────────────────────────────────┘             │   │
│                                                                                     │   │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │   │
│  │                         CAPTURE SINKS (Mock BPA)                              │  │   │
│  │                                                                                │  │   │
│  │  Ground Sink: captures bundles delivered TO the ground station                 │  │   │
│  │  Satellite Sink: captures bundles delivered TO the satellite                   │  │   │
│  │                                                                                │  │   │
│  │  Test asserts: delivered bytes == original bytes                                │  │   │
│  └──────────────────────────────────────────────────────────────────────────────┘  │   │
│                                                                                       │
└───────────────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow: Command Uplink (Ground → Satellite)

```
T=0ms    Test injects 500-byte command into Ground Span aggregation buffer
         │
         ▼
T=0ms    AggregationBuffer.flush() → 504-byte block (4-byte prefix + 500-byte payload)
         │
         ▼
T=0ms    ExportSession::new() → 1 data segment (RedEob, 504 bytes + LTP header)
         │
         ▼
T=0ms    TokenBucket.consume(~520 bytes) → rate-limited send
         │
         ▼
T=0ms    UDP send_to(Proxy Socket A)
         │
         ▼
T=0ms    Proxy Socket A receives datagram
         │
         ▼
         ┌──────────────────────┐
         │  tokio::time::sleep  │
         │      (50ms)          │  ← Simulates 1.3s Earth-Moon propagation
         └──────────┬───────────┘
                    │
                    ▼
T=50ms   Proxy forwards to Satellite Socket B
         │
         ▼
T=50ms   Satellite Receive Loop: decode → Segment::Data (RedEob)
         │
         ▼
T=50ms   ImportSession.on_data_segment():
         • Records data at offset 0
         • Inserts extent [0, 504) into ExtentMap
         • EORP upper bound = 504
         • Block complete! (extents cover [0, 504))
         • Generates Report Segment (checkpoint_serial=1, full coverage)
         • Returns: [SendSegment(RS), StartTimer, DeliverBlock(504 bytes)]
         │
         ├──────────────────────────────────────────────────────┐
         │                                                      │
         ▼                                                      ▼
T=50ms   deliver_block():                              send_segment(RS):
         • unpack_block() → 1 bundle (500 bytes)      • UDP send_to(Proxy Socket B)
         • sink.dispatch(bundle)                       │
         • CaptureSink records bundle ✓                ▼
                                                T=50ms  Proxy Socket B receives RS
                                                        │
                                                        ▼
                                                        ┌──────────────────────┐
                                                        │  tokio::time::sleep  │
                                                        │      (50ms)          │
                                                        └──────────┬───────────┘
                                                                   │
                                                                   ▼
                                                        T=100ms Proxy forwards RS to Ground Socket A
                                                                   │
                                                                   ▼
                                                        T=100ms Ground Receive Loop: decode → Report
                                                                   │
                                                                   ▼
                                                        T=100ms ExportSession.on_report():
                                                                • Records claims [0, 504)
                                                                • Block fully acknowledged
                                                                • Sends Report-Ack
                                                                • Transitions to Complete ✓
```

## Data Flow: Telemetry Downlink (Satellite → Ground)

```
T=0ms    Test injects 10,240-byte telemetry into Satellite Span
         │
         ▼
T=0ms    AggregationBuffer.flush() → 10,244-byte block
         │
         ▼
T=0ms    ExportSession::new() → 8 data segments:
         • Seg 0: RedData, offset=0, 1400 bytes
         • Seg 1: RedData, offset=1400, 1400 bytes
         • Seg 2: RedData, offset=2800, 1400 bytes
         • Seg 3: RedData, offset=4200, 1400 bytes
         • Seg 4: RedData, offset=5600, 1400 bytes
         • Seg 5: RedData, offset=7000, 1400 bytes
         • Seg 6: RedData, offset=8400, 1400 bytes
         • Seg 7: RedEob, offset=9800, 444 bytes (checkpoint serial=1)
         │
         ▼
T=0ms    All 8 segments sent to Proxy Socket B (rate-limited at 256 kbps)
         │
         ▼
         ┌──────────────────────┐
         │  8× tokio::sleep     │
         │      (50ms each)     │  ← All segments delayed by OWLT
         └──────────┬───────────┘
                    │
                    ▼
T=50ms   All 8 segments arrive at Ground Socket A (near-simultaneously)
         │
         ▼
T=50ms   Ground Receive Loop processes each segment:
         • ImportSession.on_data_segment() × 8
         • ExtentMap grows: [0,1400) → [0,2800) → ... → [0,10244)
         • On segment 7 (RedEob): checkpoint triggers report generation
         • Block complete! All extents cover [0, 10244)
         • Generates Report Segment with full coverage
         • DeliverBlock(10,244 bytes)
         │
         ▼
T=50ms   deliver_block():
         • unpack_block() → 1 bundle (10,240 bytes)
         • sink.dispatch(bundle)
         • CaptureSink records telemetry ✓
```

## Link Parameters

| Parameter | Simulated | Real Lunar | Purpose |
|-----------|-----------|------------|---------|
| One-way light time | 50 ms | 1,300 ms | Propagation delay Earth↔Moon |
| Margin time | 200 ms | 200 ms | Processing/queuing overhead |
| Retransmit timeout | 3,000 ms | 3,000 ms | 2×(OWLT+margin), won't fire in test |
| Max segment size | 1,400 bytes | 1,400 bytes | Fits in standard MTU |
| Link rate | 256 kbps | 256 kbps | Realistic deep-space downlink |
| Max retransmissions | 5 | 5 | Before session cancellation |

## What the Test Validates

| Aspect | How |
|--------|-----|
| Delay tolerance | Bundles arrive after OWLT, not before |
| Multi-segment reassembly | 10 KB telemetry split into 8 segments, reassembled correctly |
| Report/ack over delay | RS travels back through proxy, ExportSession completes |
| Bidirectional operation | Simultaneous uplink + downlink on shared delayed link |
| Timer configuration | Retransmit timeout = 3000ms, won't fire during 50ms test |
| Rate limiting | Token bucket at 256 kbps shapes transmit rate |
| Block integrity | Delivered bytes == original bytes (byte-for-byte) |

## Running

```bash
# With full debug output showing the LTP exchange
cargo test -p hardy-ltp-cla --test lunar_link_test -- --nocapture

# Just verify pass/fail
cargo test -p hardy-ltp-cla --test lunar_link_test
```

## Example Output

```
--- LunarLink Mission Simulation ---
    Ground Station: ipn:2.0 (engine-id 2)
    Satellite: ipn:3.0 (engine-id 3)
    One-way light time: 50ms (simulated, real: 1300ms)
    Retransmit timeout: 2 × (1300 + 200) = 3000ms
    Link rate: 256 kbps
    Max segment size: 1400 bytes

[T+0.000s] Ground Station: sending command (500 bytes)
[T+0.000s] Ground Station: export session created, 1 segment
[T+0.053s] Satellite: received data segment, created import session
[T+0.053s] Satellite: block complete, delivering command
[T+0.154s] Ground Station: received report-ack, session complete
--- Command delivered successfully (delivery: ~52ms, RTT: ~154ms) ---
    Retransmit timeout correctly configured: 3000ms (won't fire prematurely)
```
