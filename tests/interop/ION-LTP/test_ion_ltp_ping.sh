#!/usr/bin/env bash
# Interoperability test: Hardy <-> ION ping/echo via LTP/UDP
#
# This script tests bidirectional ping/echo between Hardy and ION over LTP:
#   1. Hardy sends bundle to ION via LTP (Hardy → ION echo)
#   2. ION sends bundle to Hardy via LTP (ION bping → Hardy echo)
#   3. Large bundle multi-segment transfer (100KB payload)
#   4. Packet loss recovery with tc netem (20% loss)
#
# Prerequisites:
#   - Docker installed (for ION container)
#   - Hardy tools and bpa-server built with LTP feature
#   - ION Docker image built (ion-ltp-interop)
#
# Usage:
#   ./tests/interop/ION-LTP/test_ion_ltp_ping.sh [--skip-build] [--count N] [--no-docker]
#
# Options:
#   --skip-build   Skip building Hardy binaries
#   --count N      Number of pings to send (default: 5)
#   --no-docker    Use local ION binaries instead of Docker

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Configuration constants
HARDY_NODE_NUM=1
ION_NODE_NUM=2
ION_LTP_PORT=1113
HARDY_LTP_PORT=1114
HARDY_TCP_PORT=4560
ION_IMAGE="ion-ltp-interop"
ION_CONTAINER_NAME="ion-ltp-interop-test"
PING_COUNT=5

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $*"; }

# Parse options
SKIP_BUILD=false
USE_DOCKER=true
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --no-docker)
            USE_DOCKER=false
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
HARDY_PID=""
ION_CONTAINER=""
CLEANUP_IN_PROGRESS=""

# Helper to kill a process with SIGTERM, then SIGKILL if needed
kill_process() {
    local pid=$1
    local name=$2
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        log_info "Stopping $name (PID $pid)..."
        kill "$pid" 2>/dev/null || true
        local count=0
        while kill -0 "$pid" 2>/dev/null && [ $count -lt 50 ]; do
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

    # Stop Hardy BPA server
    kill_process "$HARDY_PID" "hardy-bpa-server"

    # Stop and remove ION container
    if [ -n "$ION_CONTAINER" ]; then
        docker stop -t 2 "$ION_CONTAINER" 2>/dev/null || true
        docker rm -f "$ION_CONTAINER" 2>/dev/null || true
    fi
    docker rm -f "$ION_CONTAINER_NAME" 2>/dev/null || true

    # Clean up ION shared memory (killm via Docker)
    if [ "$USE_DOCKER" = true ]; then
        docker run --rm --ipc=host --entrypoint killm "$ION_IMAGE" 2>/dev/null || true
    fi

    # Remove temp directory
    if [ -n "${TEST_DIR:-}" ] && [ -d "$TEST_DIR" ]; then
        rm -rf "$TEST_DIR"
    fi

    log_info "Cleanup complete"
}
trap cleanup EXIT INT TERM

# Create temporary directory
TEST_DIR=$(mktemp -d)
log_info "Using test directory: $TEST_DIR"

# =============================================================================
# Build Hardy
# =============================================================================
if [ "$SKIP_BUILD" = false ]; then
    log_step "Building Hardy tools and bpa-server (with LTP and TCPCLv4)..."
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
# Build or check Docker image
# =============================================================================
if [ "$USE_DOCKER" = true ]; then
    log_step "Checking for $ION_IMAGE Docker image..."
    if ! docker image inspect "$ION_IMAGE" &>/dev/null; then
        log_info "Building $ION_IMAGE Docker image (this may take a while)..."
        docker build -t "$ION_IMAGE" "$SCRIPT_DIR/docker"
    else
        log_info "Using existing $ION_IMAGE image"
    fi
else
    if ! command -v ionstart &> /dev/null; then
        log_error "ION not found in PATH"
        log_error "Install ION or use Docker mode"
        exit 1
    fi
    log_info "Found ION at: $(which ionstart)"
fi

# =============================================================================
# Write Hardy TOML configuration
# =============================================================================
log_step "Writing Hardy configuration..."

cat > "$TEST_DIR/hardy_config.toml" << EOF
log-level = "info"
status-reports = true
node-ids = "ipn:$HARDY_NODE_NUM.0"

[built-in-services]
echo = [7]

[storage.metadata]
type = "memory"

[storage.bundle]
type = "memory"

# TCPCLv4 listener for bp ping tool to connect
[[clas]]
name = "tcp0"
type = "tcpclv4"
address = "[::]:$HARDY_TCP_PORT"

# LTP CLA for ION communication
[[clas]]
name = "ltp0"
type = "ltp"
bind = "[::]:$HARDY_LTP_PORT"
engine-id = $HARDY_NODE_NUM
client-service-id = 1

[[clas.spans]]
engine-id = $ION_NODE_NUM
address = "127.0.0.1:$ION_LTP_PORT"
node-ids = ["ipn:$ION_NODE_NUM.0"]
max-segment-size = 1400
max-retransmissions = 5
retransmit-cycle-secs = 5
aggr-size-limit = 65536
aggr-time-limit-secs = 0
max-import-sessions = 100
max-export-sessions = 100
framing = "none"
EOF

# =============================================================================
# Start Hardy BPA server
# =============================================================================
log_step "Starting Hardy BPA server (ipn:$HARDY_NODE_NUM.0, engine-id $HARDY_NODE_NUM, LTP port $HARDY_LTP_PORT)..."

"$BPA_BIN" -c "$TEST_DIR/hardy_config.toml" > "$TEST_DIR/hardy.log" 2>&1 &
HARDY_PID=$!

sleep 3

if ! kill -0 "$HARDY_PID" 2>/dev/null; then
    log_error "Hardy BPA server failed to start. Log:"
    cat "$TEST_DIR/hardy.log"
    exit 1
fi
log_info "Hardy BPA server started with PID $HARDY_PID"

# =============================================================================
# Start ION container
# =============================================================================
if [ "$USE_DOCKER" = true ]; then
    log_step "Starting ION container (ipn:$ION_NODE_NUM.0, engine-id $ION_NODE_NUM, LTP port $ION_LTP_PORT)..."

    docker rm -f "$ION_CONTAINER_NAME" 2>/dev/null || true

    ION_CONTAINER=$(docker run -d \
        --name "$ION_CONTAINER_NAME" \
        --network host \
        --ipc=host \
        --cap-add=NET_ADMIN \
        -e REMOTE_HOST="127.0.0.1" \
        -e REMOTE_LTP_PORT="$HARDY_LTP_PORT" \
        -e REMOTE_NODE="$HARDY_NODE_NUM" \
        "$ION_IMAGE")

    log_info "Started ION container: ${ION_CONTAINER:0:12}"

    # Wait for ION's udplsi to bind on the LTP port
    log_info "Waiting for ION udplsi to bind on port $ION_LTP_PORT..."
    WAIT_TIMEOUT=30
    WAIT_COUNT=0
    while [ $WAIT_COUNT -lt $WAIT_TIMEOUT ]; do
        if ! docker ps -q -f "id=$ION_CONTAINER" | grep -q .; then
            log_error "ION container exited unexpectedly. Logs:"
            docker logs "$ION_CONTAINER" 2>&1 | tail -50
            docker rm "$ION_CONTAINER" 2>/dev/null || true
            exit 1
        fi

        if ss -uln 2>/dev/null | grep -q ":$ION_LTP_PORT "; then
            log_info "ION udplsi is listening on UDP port $ION_LTP_PORT (took ${WAIT_COUNT}s)"
            break
        fi

        sleep 1
        WAIT_COUNT=$((WAIT_COUNT + 1))
    done

    if [ $WAIT_COUNT -ge $WAIT_TIMEOUT ]; then
        log_error "ION did not start listening on UDP port $ION_LTP_PORT within ${WAIT_TIMEOUT}s"
        docker logs "$ION_CONTAINER" 2>&1 | tail -30
        exit 1
    fi

    # Give ION time to finish internal setup after port opens
    sleep 2
else
    log_error "Native ION mode not yet implemented - use Docker mode"
    exit 1
fi

log_info "Setup complete. Hardy and ION are running."
echo ""

# =============================================================================
# TESTS (added by subsequent tasks 2.2 - 2.6)
# =============================================================================
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

# --- Test 1: Hardy sends bundle to ION via LTP (task 2.2) ---
echo ""
echo "============================================================"
log_step "Test 1: Hardy sends bundle to ION via LTP"
echo "============================================================"

log_info "Hardy pinging ION echo service at ipn:$ION_NODE_NUM.7 via LTP..."

PING_OUTPUT=$("$BP_BIN" ping "ipn:$ION_NODE_NUM.7" "127.0.0.1:$HARDY_TCP_PORT" \
    --source "ipn:$HARDY_NODE_NUM.12345" \
    --count "$PING_COUNT" \
    --no-sign \
    -W 10s \
    2>&1) && PING_EXIT=0 || PING_EXIT=$?

echo "$PING_OUTPUT"
echo ""

# Parse transmitted/received counts from ping output
STATS_LINE=$(echo "$PING_OUTPUT" | grep -E '[0-9]+ (bundles )?transmitted' | head -1)
TRANSMITTED=$(echo "$STATS_LINE" | sed -E 's/^([0-9]+).*/\1/')
RECEIVED=$(echo "$STATS_LINE" | sed -E 's/.*, ([0-9]+) received.*/\1/')

if [ $PING_EXIT -eq 0 ] && [ -n "$RECEIVED" ] && [ -n "$TRANSMITTED" ] && [ "$RECEIVED" = "$TRANSMITTED" ]; then
    log_info "TEST 1 PASSED: Hardy successfully pinged ION via LTP ($RECEIVED/$TRANSMITTED bundles)"
    TEST1_RESULT="PASS"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    if [ -n "$RECEIVED" ] && [ -n "$TRANSMITTED" ]; then
        log_error "TEST 1 FAILED: Only $RECEIVED/$TRANSMITTED echo responses received"
    elif [ $PING_EXIT -ne 0 ]; then
        log_error "TEST 1 FAILED: bp ping exited with code $PING_EXIT"
    else
        log_error "TEST 1 FAILED: Could not parse ping statistics"
    fi
    TEST1_RESULT="FAIL"
    TESTS_FAILED=$((TESTS_FAILED + 1))

    # Display last 20 lines of Hardy and ION logs for debugging
    echo ""
    log_error "--- Hardy log (last 20 lines) ---"
    tail -20 "$TEST_DIR/hardy.log" 2>/dev/null || echo "(no log available)"
    echo ""
    log_error "--- ION container log (last 20 lines) ---"
    docker logs "$ION_CONTAINER_NAME" 2>&1 | tail -20 || echo "(no log available)"
    echo ""
fi

# --- Test 2: ION sends bundle to Hardy via LTP (task 2.3) ---
echo ""
echo "============================================================"
log_step "Test 2: ION sends bundle to Hardy via LTP"
echo "============================================================"

PING_TIMEOUT=$((PING_COUNT * 10 + 30))
log_info "Running bping from ION (ipn:$ION_NODE_NUM.1) to Hardy echo (ipn:$HARDY_NODE_NUM.7)..."

BPING_OUTPUT=$(timeout "${PING_TIMEOUT}s" docker exec "$ION_CONTAINER_NAME" \
    bping -c "$PING_COUNT" -q 5 \
    "ipn:$ION_NODE_NUM.1" "ipn:$HARDY_NODE_NUM.7" \
    2>&1) || true

echo "$BPING_OUTPUT"
echo ""

# bping reports "N bundles transmitted, M bundles received, X% bundle loss"
STATS_LINE=$(echo "$BPING_OUTPUT" | grep "bundle loss" | head -1)
RECEIVED=$(echo "$STATS_LINE" | sed -E 's/.*, ([0-9]+) bundles received.*/\1/')
TRANSMITTED=$(echo "$STATS_LINE" | sed -E 's/^([0-9]+) bundles transmitted.*/\1/')

if [ -n "$RECEIVED" ] && [ "$RECEIVED" -ge 1 ] 2>/dev/null; then
    if echo "$STATS_LINE" | grep -q "0.00% bundle loss"; then
        log_info "TEST 2 PASSED: ION successfully pinged Hardy ($RECEIVED/$TRANSMITTED bundles)"
        TEST2_RESULT="PASS"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_error "TEST 2 FAILED: Partial loss ($STATS_LINE)"
        TEST2_RESULT="FAIL"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
else
    log_error "TEST 2 FAILED: No echo responses received from Hardy"
    TEST2_RESULT="FAIL"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

if [ "$TEST2_RESULT" = "FAIL" ]; then
    echo ""
    log_error "--- Hardy log (last 20 lines) ---"
    tail -20 "$TEST_DIR/hardy.log" 2>/dev/null || echo "(no log available)"
    echo ""
    log_error "--- ION container logs (last 20 lines) ---"
    docker logs "$ION_CONTAINER_NAME" 2>&1 | tail -20 || echo "(no log available)"
    echo ""
fi

# --- Test 3: Multi-segment large bundle transfer (task 2.4) ---
echo ""
echo "============================================================"
log_step "Test 3: Multi-segment large bundle transfer (100KB)"
echo "============================================================"

PAYLOAD_SIZE=102400
SEGMENT_COUNT=$((PAYLOAD_SIZE / 1400 + 1))
log_info "Sending ${PAYLOAD_SIZE}-byte payload (~${SEGMENT_COUNT} LTP segments) to ION echo service..."
log_info "Using bp ping with --size $PAYLOAD_SIZE to ipn:$ION_NODE_NUM.7"

PING_OUTPUT=$("$BP_BIN" ping "ipn:$ION_NODE_NUM.7" "127.0.0.1:$HARDY_TCP_PORT" \
    --source "ipn:$HARDY_NODE_NUM.12345" \
    --count 1 \
    --size "$PAYLOAD_SIZE" \
    --no-sign \
    -W 30s \
    2>&1) && PING_EXIT=0 || PING_EXIT=$?

echo "$PING_OUTPUT"
echo ""

# Parse transmitted/received counts from ping output
STATS_LINE=$(echo "$PING_OUTPUT" | grep -E '[0-9]+ (bundles )?transmitted' | head -1)
TRANSMITTED=$(echo "$STATS_LINE" | sed -E 's/^([0-9]+).*/\1/')
RECEIVED=$(echo "$STATS_LINE" | sed -E 's/.*, ([0-9]+) received.*/\1/')

if [ $PING_EXIT -eq 0 ] && [ -n "$RECEIVED" ] && [ "$RECEIVED" -ge 1 ] 2>/dev/null; then
    log_info "TEST 3 PASSED: Large bundle (${PAYLOAD_SIZE} bytes, ~${SEGMENT_COUNT} segments) echoed successfully ($RECEIVED/$TRANSMITTED)"
    TEST3_RESULT="PASS"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    if [ -n "$RECEIVED" ] && [ -n "$TRANSMITTED" ]; then
        log_error "TEST 3 FAILED: Large bundle echo failed ($RECEIVED/$TRANSMITTED received)"
    elif [ $PING_EXIT -ne 0 ]; then
        log_error "TEST 3 FAILED: bp ping exited with code $PING_EXIT"
    else
        log_error "TEST 3 FAILED: Could not parse ping statistics"
    fi
    TEST3_RESULT="FAIL"
    TESTS_FAILED=$((TESTS_FAILED + 1))

    # Display logs for debugging
    echo ""
    log_error "--- Hardy log (last 20 lines) ---"
    tail -20 "$TEST_DIR/hardy.log" 2>/dev/null || echo "(no log available)"
    echo ""
    log_error "--- ION container log (last 20 lines) ---"
    docker logs "$ION_CONTAINER_NAME" 2>&1 | tail -20 || echo "(no log available)"
    echo ""
fi

# --- Test 4: Packet loss recovery with tc netem (task 2.5) ---
echo ""
echo "============================================================"
log_step "Test 4: Packet loss recovery with tc netem (20% loss)"
echo "============================================================"

# Helper to remove netem rule (called in both success and failure paths)
remove_netem() {
    docker exec --user root "$ION_CONTAINER_NAME" tc qdisc del dev lo root netem 2>/dev/null || true
}

# Apply 20% packet loss on loopback inside the ION container
log_info "Applying 20% packet loss via tc netem..."
if ! docker exec --user root "$ION_CONTAINER_NAME" tc qdisc add dev lo root netem loss 20% 2>&1; then
    log_warn "tc netem failed (missing NET_ADMIN capability?) - skipping packet loss test"
    TEST4_RESULT="SKIP"
    TESTS_SKIPPED=$((TESTS_SKIPPED + 1))
else
    log_info "Packet loss applied. Running ping with extended timeout..."

    PING_OUTPUT=$("$BP_BIN" ping "ipn:$ION_NODE_NUM.7" "127.0.0.1:$HARDY_TCP_PORT" \
        --source "ipn:$HARDY_NODE_NUM.12345" \
        --count "$PING_COUNT" \
        --no-sign \
        -W 60s \
        2>&1) && PING_EXIT=0 || PING_EXIT=$?

    # Always remove netem regardless of test outcome
    remove_netem

    echo "$PING_OUTPUT"
    echo ""

    # Parse transmitted/received counts from ping output
    STATS_LINE=$(echo "$PING_OUTPUT" | grep -E '[0-9]+ (bundles )?transmitted' | head -1)
    TRANSMITTED=$(echo "$STATS_LINE" | sed -E 's/^([0-9]+).*/\1/')
    RECEIVED=$(echo "$STATS_LINE" | sed -E 's/.*, ([0-9]+) received.*/\1/')

    # PASS if at least 1 bundle was received (delivery eventually succeeds under loss)
    if [ -n "$RECEIVED" ] && [ "$RECEIVED" -ge 1 ] 2>/dev/null; then
        log_info "TEST 4 PASSED: Delivery succeeded under 20% packet loss ($RECEIVED/$TRANSMITTED bundles received)"
        TEST4_RESULT="PASS"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_error "TEST 4 FAILED: No bundles delivered under 20% packet loss within 60s timeout"
        TEST4_RESULT="FAIL"
        TESTS_FAILED=$((TESTS_FAILED + 1))

        # Display logs for debugging
        echo ""
        log_error "--- Hardy log (last 20 lines) ---"
        tail -20 "$TEST_DIR/hardy.log" 2>/dev/null || echo "(no log available)"
        echo ""
        log_error "--- ION container log (last 20 lines) ---"
        docker logs "$ION_CONTAINER_NAME" 2>&1 | tail -20 || echo "(no log available)"
        echo ""
    fi
fi

# --- Test summary and exit code (task 2.6) ---

# Helper to colorize result text
colorize_result() {
    case "$1" in
        PASS) echo -e "${GREEN}PASS${NC}" ;;
        FAIL) echo -e "${RED}FAIL${NC}" ;;
        SKIP) echo -e "${YELLOW}SKIP${NC}" ;;
        *)    echo "$1" ;;
    esac
}

echo ""
echo "============================================================"
echo "                      TEST SUMMARY"
echo "============================================================"
echo ""
printf "  Test 1: Hardy → ION via LTP .............. %s\n" "$(colorize_result "$TEST1_RESULT")"
printf "  Test 2: ION → Hardy via LTP .............. %s\n" "$(colorize_result "$TEST2_RESULT")"
printf "  Test 3: Large bundle (100KB) ............. %s\n" "$(colorize_result "$TEST3_RESULT")"
printf "  Test 4: Packet loss recovery ............. %s\n" "$(colorize_result "$TEST4_RESULT")"
echo ""
echo "------------------------------------------------------------"
echo "  Transport: LTP/UDP"
echo "  Hardy:     ipn:${HARDY_NODE_NUM}.0 (engine-id ${HARDY_NODE_NUM}, port ${HARDY_LTP_PORT})"
echo "  ION:       ipn:${ION_NODE_NUM}.0 (engine-id ${ION_NODE_NUM}, port ${ION_LTP_PORT})"
echo "------------------------------------------------------------"
echo ""
echo "  Passed: $TESTS_PASSED, Failed: $TESTS_FAILED, Skipped: $TESTS_SKIPPED"
echo ""

if [ "$TESTS_FAILED" -gt 0 ]; then
    # Preserve temp directory for log inspection by unsetting TEST_DIR
    # so the cleanup trap won't delete it
    PRESERVE_DIR="$TEST_DIR"
    TEST_DIR=""
    log_warn "Preserving test directory for log inspection: $PRESERVE_DIR"
    log_error "RESULT: $TESTS_FAILED test(s) failed"
    exit 1
else
    log_info "RESULT: All tests passed"
    exit 0
fi
