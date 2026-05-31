#!/usr/bin/env bash
# Start ION LTP interop environment for interactive testing
#
# This script starts Hardy (with built-in LTP CLA) and ION (in Docker with
# udplso/udplsi) so you can manually test bundle exchange over LTP/UDP.
#
# Usage:
#   ./tests/interop/ION-LTP/start_ion_ltp.sh [--skip-build]
#
# Options:
#   --skip-build   Skip building Hardy binaries (use existing build)
#
# Press Ctrl+C to stop and clean up.

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
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--skip-build]"
            exit 1
            ;;
    esac
done

# Cleanup function
HARDY_PID=""
ION_CONTAINER=""
CLEANUP_IN_PROGRESS=""

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

    echo ""
    log_info "Cleaning up..."

    # Stop Hardy BPA server
    kill_process "$HARDY_PID" "hardy-bpa-server"

    # Stop and remove ION container
    if [ -n "$ION_CONTAINER" ]; then
        docker stop -t 2 "$ION_CONTAINER" 2>/dev/null || true
        docker rm -f "$ION_CONTAINER" 2>/dev/null || true
    fi
    docker rm -f "$ION_CONTAINER_NAME" 2>/dev/null || true

    # Clean up ION shared memory
    docker run --rm --ipc=host --entrypoint killm "$ION_IMAGE" 2>/dev/null || true

    # Remove temp directory
    if [ -n "${CONFIG_DIR:-}" ] && [ -d "$CONFIG_DIR" ]; then
        rm -rf "$CONFIG_DIR"
    fi

    log_info "Cleanup complete"
    exit 0
}
trap cleanup INT TERM

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
log_step "Checking for $ION_IMAGE Docker image..."
if ! docker image inspect "$ION_IMAGE" &>/dev/null; then
    log_info "Building $ION_IMAGE Docker image (this may take a while)..."
    docker build -t "$ION_IMAGE" "$SCRIPT_DIR/docker"
else
    log_info "Using existing $ION_IMAGE image"
fi

# =============================================================================
# Write Hardy TOML configuration
# =============================================================================
CONFIG_DIR=$(mktemp -d)
log_step "Writing Hardy configuration to $CONFIG_DIR..."

cat > "$CONFIG_DIR/hardy_config.toml" << EOF
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
EOF

# =============================================================================
# Start Hardy BPA server
# =============================================================================
log_step "Starting Hardy BPA server (ipn:$HARDY_NODE_NUM.0, engine-id $HARDY_NODE_NUM, LTP port $HARDY_LTP_PORT)..."

"$BPA_BIN" -c "$CONFIG_DIR/hardy_config.toml" > "$CONFIG_DIR/hardy.log" 2>&1 &
HARDY_PID=$!

sleep 3

if ! kill -0 "$HARDY_PID" 2>/dev/null; then
    log_error "Hardy BPA server failed to start. Log:"
    cat "$CONFIG_DIR/hardy.log"
    exit 1
fi
log_info "Hardy BPA server started with PID $HARDY_PID"

# =============================================================================
# Start ION container
# =============================================================================
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

# =============================================================================
# Print usage instructions
# =============================================================================
echo ""
echo "============================================================"
echo "ION LTP Interop Environment Ready"
echo "============================================================"
echo ""
echo "Hardy: ipn:$HARDY_NODE_NUM.0 (engine-id $HARDY_NODE_NUM, LTP port $HARDY_LTP_PORT, TCPCLv4 port $HARDY_TCP_PORT)"
echo "ION:   ipn:$ION_NODE_NUM.0 (engine-id $ION_NODE_NUM, LTP port $ION_LTP_PORT)"
echo ""
echo "Usage examples:"
echo "  # Hardy pings ION echo service"
echo "  ./target/release/bp ping ipn:$ION_NODE_NUM.7 127.0.0.1:$HARDY_TCP_PORT --source ipn:$HARDY_NODE_NUM.12345 --no-sign"
echo ""
echo "  # ION pings Hardy echo service"
echo "  docker exec $ION_CONTAINER_NAME bping ipn:$ION_NODE_NUM.1 ipn:$HARDY_NODE_NUM.7"
echo ""
echo "  # View ION logs"
echo "  docker logs -f $ION_CONTAINER_NAME"
echo ""
echo "Press Ctrl+C to stop and clean up."
echo "============================================================"

# Wait indefinitely until user presses Ctrl+C
while true; do
    sleep 1
done
