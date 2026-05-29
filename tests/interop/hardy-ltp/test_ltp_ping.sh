#!/bin/bash
# Interoperability test: Hardy <-> Hardy over LTP (UDP)
#
# This script tests bidirectional ping/echo between two Hardy BPA servers
# using the LTP convergence layer adapter over UDP:
#   1. Node 1 (ipn:1.0, engine-id 1, port 1113) pings Node 2's echo service
#   2. Node 2 (ipn:2.0, engine-id 2, port 1114) pings Node 1's echo service
#
# Prerequisites:
#   - Hardy tools and bpa-server built with --features ltp
#
# Usage:
#   ./tests/interop/hardy-ltp/test_ltp_ping.sh [--skip-build] [--count N]
#
# Options:
#   --skip-build   Skip building Hardy binaries
#   --count N      Number of pings to send (default: 5)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Configuration
NODE1_NUM=1
NODE2_NUM=2
NODE1_LTP_PORT=1113
NODE2_LTP_PORT=1114
NODE1_TCP_PORT=4560
NODE2_TCP_PORT=4561
PING_COUNT=5
PING_SERVICE=12345

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $*"; }

# Parse options
SKIP_BUILD=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --count|-c)
            PING_COUNT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Cleanup function
NODE1_PID=""
NODE2_PID=""
CLEANUP_IN_PROGRESS=""

kill_process() {
    local pid=$1
    local name=$2
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        log_info "Stopping $name (PID $pid)..."
        kill "$pid" 2>/dev/null || true
        local count=0
        while kill -0 "$pid" 2>/dev/null && [ $count -lt 30 ]; do
            sleep 0.1
            count=$((count + 1))
        done
        if kill -0 "$pid" 2>/dev/null; then
            log_warn "Force killing $name (PID $pid)..."
            kill -9 "$pid" 2>/dev/null || true
        fi
        wait "$pid" 2>/dev/null || true
    fi
}

cleanup() {
    if [ -n "$CLEANUP_IN_PROGRESS" ]; then
        return
    fi
    CLEANUP_IN_PROGRESS=1

    log_info "Cleaning up..."
    kill_process "$NODE1_PID" "hardy-node-1"
    kill_process "$NODE2_PID" "hardy-node-2"

    if [ -n "$TEST_DIR" ] && [ -d "$TEST_DIR" ]; then
        rm -rf "$TEST_DIR"
    fi

    log_info "Cleanup complete"
}
trap cleanup EXIT INT TERM

# Create temporary directory
TEST_DIR=$(mktemp -d)
log_info "Using test directory: $TEST_DIR"

# Build Hardy with LTP support
if [ "$SKIP_BUILD" = false ]; then
    log_step "Building Hardy tools and bpa-server (with LTP)..."
    cd "$WORKSPACE_DIR"
    cargo build --release -p hardy-tools -p hardy-bpa-server --features hardy-bpa-server/ltp,hardy-bpa-server/tcpclv4
fi

BP_BIN="$WORKSPACE_DIR/target/release/bp"
BPA_BIN="$WORKSPACE_DIR/target/release/hardy-bpa-server"

if [ ! -x "$BP_BIN" ]; then
    log_error "bp binary not found at $BP_BIN"
    exit 1
fi

if [ ! -x "$BPA_BIN" ]; then
    log_error "hardy-bpa-server binary not found at $BPA_BIN"
    exit 1
fi

# =============================================================================
# Create configuration files
# =============================================================================
log_step "Creating LTP configuration files..."

cat > "$TEST_DIR/node1_config.toml" << EOF
log-level = "info"
status-reports = true
node-ids = "ipn:$NODE1_NUM.0"

[built-in-services]
echo = [7]

[storage.metadata]
type = "memory"

[storage.bundle]
type = "memory"

# TCPCLv4 listener for the bp ping tool to connect to
[[clas]]
name = "tcp0"
type = "tcpclv4"
address = "[::]:$NODE1_TCP_PORT"

# LTP CLA for inter-node transport
[[clas]]
name = "ltp0"
type = "ltp"
bind = "[::]:$NODE1_LTP_PORT"
engine-id = $NODE1_NUM
client-service-id = 1

[[clas.spans]]
engine-id = $NODE2_NUM
address = "127.0.0.1:$NODE2_LTP_PORT"
node-ids = ["ipn:$NODE2_NUM.0"]
max-segment-size = 1400
max-retransmissions = 5
retransmit-cycle-secs = 5
aggr-size-limit = 65536
aggr-time-limit-secs = 0
max-import-sessions = 100
max-export-sessions = 100
EOF

cat > "$TEST_DIR/node2_config.toml" << EOF
log-level = "info"
status-reports = true
node-ids = "ipn:$NODE2_NUM.0"

[built-in-services]
echo = [7]

[storage.metadata]
type = "memory"

[storage.bundle]
type = "memory"

# TCPCLv4 listener for the bp ping tool to connect to
[[clas]]
name = "tcp0"
type = "tcpclv4"
address = "[::]:$NODE2_TCP_PORT"

# LTP CLA for inter-node transport
[[clas]]
name = "ltp0"
type = "ltp"
bind = "[::]:$NODE2_LTP_PORT"
engine-id = $NODE2_NUM
client-service-id = 1

[[clas.spans]]
engine-id = $NODE1_NUM
address = "127.0.0.1:$NODE1_LTP_PORT"
node-ids = ["ipn:$NODE1_NUM.0"]
max-segment-size = 1400
max-retransmissions = 5
retransmit-cycle-secs = 5
aggr-size-limit = 65536
aggr-time-limit-secs = 0
max-import-sessions = 100
max-export-sessions = 100
EOF

# =============================================================================
# Start both Hardy BPA servers with LTP
# =============================================================================
log_step "Starting Hardy BPA servers with LTP..."

log_info "Starting Node 1 (ipn:$NODE1_NUM.0, engine-id $NODE1_NUM, UDP port $NODE1_LTP_PORT)..."
"$BPA_BIN" -c "$TEST_DIR/node1_config.toml" > "$TEST_DIR/node1.log" 2>&1 &
NODE1_PID=$!

log_info "Starting Node 2 (ipn:$NODE2_NUM.0, engine-id $NODE2_NUM, UDP port $NODE2_LTP_PORT)..."
"$BPA_BIN" -c "$TEST_DIR/node2_config.toml" > "$TEST_DIR/node2.log" 2>&1 &
NODE2_PID=$!

# Wait for servers to start and bind sockets
sleep 3

# Verify both are running
if ! kill -0 "$NODE1_PID" 2>/dev/null; then
    log_error "Node 1 BPA server failed to start. Log:"
    cat "$TEST_DIR/node1.log"
    exit 1
fi
log_info "Node 1 started with PID $NODE1_PID"

if ! kill -0 "$NODE2_PID" 2>/dev/null; then
    log_error "Node 2 BPA server failed to start. Log:"
    cat "$TEST_DIR/node2.log"
    exit 1
fi
log_info "Node 2 started with PID $NODE2_PID"

# =============================================================================
# TEST 1: Node 1 pings Node 2's echo service over LTP
# =============================================================================
echo ""
echo "============================================================"
log_info "TEST 1: Node 1 pings Node 2's echo service (over LTP/UDP)"
echo "============================================================"

log_step "Pinging ipn:$NODE2_NUM.7 via LTP (source: ipn:$NODE1_NUM.$PING_SERVICE)..."
echo ""

PING_OUTPUT=$("$BP_BIN" ping "ipn:$NODE2_NUM.7" "127.0.0.1:$NODE1_TCP_PORT" \
    --source "ipn:$NODE1_NUM.$PING_SERVICE" \
    --count "$PING_COUNT" \
    --no-sign \
    -W 10s \
    2>&1) && EXIT_CODE=0 || EXIT_CODE=$?

echo "$PING_OUTPUT"
echo ""

STATS_LINE=$(echo "$PING_OUTPUT" | grep -E '[0-9]+ (bundles )?transmitted' | head -1)
TRANSMITTED=$(echo "$STATS_LINE" | sed -E 's/^([0-9]+).*/\1/')
RECEIVED=$(echo "$STATS_LINE" | sed -E 's/.*, ([0-9]+) received.*/\1/')

if [ $EXIT_CODE -eq 0 ] && [ "$RECEIVED" = "$TRANSMITTED" ] && [ -n "$RECEIVED" ]; then
    log_info "TEST 1 PASSED: Successfully pinged Node 2 over LTP ($RECEIVED/$TRANSMITTED)"
    TEST1_RESULT="PASS"
else
    log_error "TEST 1 FAILED: exit=$EXIT_CODE received=$RECEIVED transmitted=$TRANSMITTED"
    TEST1_RESULT="FAIL"
    log_warn "Node 1 log (last 20 lines):"
    tail -20 "$TEST_DIR/node1.log"
    echo ""
    log_warn "Node 2 log (last 20 lines):"
    tail -20 "$TEST_DIR/node2.log"
fi

# =============================================================================
# TEST 2: Node 2 pings Node 1's echo service over LTP
# =============================================================================
echo ""
echo "============================================================"
log_info "TEST 2: Node 2 pings Node 1's echo service (over LTP/UDP)"
echo "============================================================"

log_step "Pinging ipn:$NODE1_NUM.7 via LTP (source: ipn:$NODE2_NUM.$PING_SERVICE)..."
echo ""

PING_OUTPUT=$("$BP_BIN" ping "ipn:$NODE1_NUM.7" "127.0.0.1:$NODE2_TCP_PORT" \
    --source "ipn:$NODE2_NUM.$PING_SERVICE" \
    --count "$PING_COUNT" \
    --no-sign \
    -W 10s \
    2>&1) && EXIT_CODE=0 || EXIT_CODE=$?

echo "$PING_OUTPUT"
echo ""

STATS_LINE=$(echo "$PING_OUTPUT" | grep -E '[0-9]+ (bundles )?transmitted' | head -1)
TRANSMITTED=$(echo "$STATS_LINE" | sed -E 's/^([0-9]+).*/\1/')
RECEIVED=$(echo "$STATS_LINE" | sed -E 's/.*, ([0-9]+) received.*/\1/')

if [ $EXIT_CODE -eq 0 ] && [ "$RECEIVED" = "$TRANSMITTED" ] && [ -n "$RECEIVED" ]; then
    log_info "TEST 2 PASSED: Successfully pinged Node 1 over LTP ($RECEIVED/$TRANSMITTED)"
    TEST2_RESULT="PASS"
else
    log_error "TEST 2 FAILED: exit=$EXIT_CODE received=$RECEIVED transmitted=$TRANSMITTED"
    TEST2_RESULT="FAIL"
    log_warn "Node 1 log (last 20 lines):"
    tail -20 "$TEST_DIR/node1.log"
    echo ""
    log_warn "Node 2 log (last 20 lines):"
    tail -20 "$TEST_DIR/node2.log"
fi

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "============================================================"
log_info "TEST SUMMARY — Hardy LTP (UDP) Interoperability"
echo "============================================================"
echo ""
echo "  Transport: LTP over UDP (RFC 5326)"
echo "  Node 1: ipn:$NODE1_NUM.0 (engine-id $NODE1_NUM, port $NODE1_LTP_PORT)"
echo "  Node 2: ipn:$NODE2_NUM.0 (engine-id $NODE2_NUM, port $NODE2_LTP_PORT)"
echo ""
echo "  TEST 1 (Node 1 → Node 2 echo): $TEST1_RESULT"
echo "  TEST 2 (Node 2 → Node 1 echo): $TEST2_RESULT"
echo ""

if [ "$TEST1_RESULT" = "PASS" ] && [ "$TEST2_RESULT" = "PASS" ]; then
    log_info "All LTP interoperability tests PASSED"
    exit 0
else
    log_error "Some tests FAILED"
    echo ""
    log_warn "Debug: check logs in $TEST_DIR/ (preserved on failure)"
    # Don't delete test dir on failure so logs can be inspected
    TEST_DIR=""
    exit 1
fi
