# LTP Integration Test Architecture

## Overview

The LTP CLA integration tests validate the full protocol stack without requiring external infrastructure. They operate at the Span level — below the BPA routing layer but exercising the complete LTP transport pipeline.

## Test Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Test Harness (Rust)                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────┐              ┌─────────────────────┐          │
│  │     Span 1          │              │     Span 2          │          │
│  │  (Engine ID: 1)     │              │  (Engine ID: 2)     │          │
│  │                     │              │                     │          │
│  │  ┌───────────────┐  │              │  ┌───────────────┐  │          │
│  │  │ Aggregation   │  │              │  │ Import        │  │          │
│  │  │ Buffer        │  │              │  │ Session SM    │  │          │
│  │  └───────┬───────┘  │              │  └───────┬───────┘  │          │
│  │          │ flush     │              │          │ deliver  │          │
│  │  ┌───────▼───────┐  │              │  ┌───────▼───────┐  │          │
│  │  │ Export        │  │   UDP/LTP    │  │ Block         │  │          │
│  │  │ Session SM    │──┼──────────────┼──│ Unpacking     │  │          │
│  │  └───────┬───────┘  │  Segments    │  └───────┬───────┘  │          │
│  │          │ encode    │              │          │ dispatch │          │
│  │  ┌───────▼───────┐  │              │  ┌───────▼───────┐  │          │
│  │  │ UDP Socket    │  │              │  │ Capture Sink  │  │          │
│  │  │ 127.0.0.1:A   │  │              │  │ (Mock BPA)    │  │          │
│  │  └───────────────┘  │              │  └───────────────┘  │          │
│  └─────────────────────┘              └─────────────────────┘          │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Receive Loops (tokio tasks)                    │   │
│  │                                                                   │   │
│  │  Socket A recv → decode segment → route to Span 1 (reports)      │   │
│  │  Socket B recv → decode segment → route to Span 2 (data)         │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

### Outbound (Span 1 → Span 2)

```
Test injects bundle
        │
        ▼
┌─────────────────┐
│ AggregationBuffer│  Pack bundle with 4-byte BE length prefix
│ append() + flush │
└────────┬────────┘
         │ Bytes (length-prefixed block)
         ▼
┌─────────────────┐
│ ExportSession    │  Segment block into ≤1400-byte data segments
│ ::new()          │  Mark final segment as RedEob with checkpoint
└────────┬────────┘
         │ Vec<ExportAction>
         ▼
┌─────────────────┐
│ send_segment()   │  Encode each segment to wire format
│                  │  Apply rate limiting (token bucket)
│                  │  UDP send_to(127.0.0.1:B)
└────────┬────────┘
         │ UDP datagrams
         ▼
    ═══════════════
    ║  Network    ║  (localhost loopback)
    ═══════════════
         │
         ▼
┌─────────────────┐
│ Receive Loop B   │  recv_from() → Bytes
│                  │  segment::decode()
│                  │  Route by session_id.engine_id
└────────┬────────┘
         │ Segment::Data
         ▼
┌─────────────────┐
│ ImportSession    │  Record data at offset in block_data
│ on_data_segment()│  Insert extent into ExtentMap
│                  │  Generate Report Segment (RS)
│                  │  Detect block complete → DeliverBlock
└────────┬────────┘
         │ Vec<ImportAction>
         ▼
┌─────────────────┐
│ deliver_block()  │  unpack_block() → extract bundles
│                  │  sink.dispatch(bundle) for each
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ CaptureSink      │  Store bundle in Vec<Bytes>
│ (Mock)           │  Notify test harness
└─────────────────┘
```

### Inbound (Report: Span 2 → Span 1)

```
ImportSession generates Report Segment
        │
        ▼
┌─────────────────┐
│ send_segment()   │  Encode RS to wire format
│ (Span 2)         │  UDP send_to(127.0.0.1:A)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Receive Loop A   │  recv_from() → decode → Segment::Report
│                  │  Route to Span 1 (engine_id matches)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ ExportSession    │  Record acknowledged ranges
│ on_report()      │  Send Report-Ack
│                  │  Transition to Complete
└─────────────────┘
```

## Component Roles

| Component | Role in Test | Real System Equivalent |
|-----------|-------------|----------------------|
| Test harness | Injects bundles, asserts delivery | BPA Dispatcher (forward path) |
| Span 1 | Sender: aggregation + export sessions | LTP CLA outbound processing |
| Span 2 | Receiver: import sessions + delivery | LTP CLA inbound processing |
| Receive Loop A | Routes reports back to Span 1 | `engine.rs` receive loop |
| Receive Loop B | Routes data segments to Span 2 | `engine.rs` receive loop |
| CaptureSink | Captures delivered bundles | BPA Dispatcher (dispatch path) |
| UDP Sockets | Localhost transport | Real UDP network |

## What's Tested

| Layer | Coverage |
|-------|----------|
| Bundle framing | 4-byte BE length prefix pack/unpack |
| SDNV codec | All integer fields in segment headers |
| Segment encoding | All segment types (Data, Report, ReportAck) |
| Segment decoding | Wire format parsing with zero-copy |
| Export session SM | Block segmentation, checkpoint assignment |
| Import session SM | Extent tracking, report generation, block delivery |
| Aggregation | Single and multi-bundle blocks |
| Multi-segment | Large payloads split across many segments |
| Report/Ack cycle | Receiver acknowledges, sender completes |
| BPv7 integrity | Real bundles survive the transport unchanged |

## What's NOT Tested (requires full BPA)

| Feature | Why |
|---------|-----|
| BPA routing decisions | Needs Dispatcher + RIB |
| EID-to-CLA resolution | Needs peer registry |
| Bundle security (BPSec) | Needs key registry |
| Retransmission under loss | Needs network impairment (tc netem) |
| Rate limiting under load | Needs sustained traffic generation |
| Multi-hop forwarding | Needs 3+ BPA instances |

## Running the Tests

```bash
# All LTP integration tests (with debug output)
cargo test -p hardy-ltp-cla --test end_to_end --test full_stack_test -- --nocapture

# Just the BPv7 full-stack tests
cargo test -p hardy-ltp-cla --test full_stack_test -- --nocapture

# Just the raw payload tests (100KB, 1MB)
cargo test -p hardy-ltp-cla --test end_to_end -- --nocapture
```

## Example Output

```
--- Sending BPv7 bundle (89 bytes) over LTP ---
    Source: ipn:1.1
    Destination: ipn:2.7
DEBUG hardy_ltp_cla::span: created export session engine_id=2 session_number=1
DEBUG hardy_ltp_cla::span: TX segment engine_id=2 bytes=106 address=127.0.0.1:52431
DEBUG hardy_ltp_cla::span: created import session engine_id=1 session_number=1
DEBUG hardy_ltp_cla::span: TX segment engine_id=1 bytes=11 address=127.0.0.1:52430
DEBUG hardy_ltp_cla::span: delivering block engine_id=1 block_bytes=93
DEBUG hardy_ltp_cla::span: unpacked block into bundles engine_id=1 bundles=1
DEBUG hardy_ltp_cla::span: dispatching bundle to BPA engine_id=1 bundle_index=0 bundle_bytes=89
DEBUG hardy_ltp_cla::span: cleaned up import session engine_id=1 session_number=1
--- BPv7 bundle delivered and verified successfully over LTP ---
```
