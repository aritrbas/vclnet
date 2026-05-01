#!/bin/bash
# run_production_demo.sh — Start VPP, run production server + client, stop.
# Usage: sudo bash run_production_demo.sh [workers]
set -e

NUM_WORKERS="${1:-0}"

VPP_BIN=/home/aritrbas/vpp/vpp/build-root/install-vpp-native/vpp/bin/vpp
VPPCTL=/home/aritrbas/vpp/vpp/build-root/install-vpp-native/vpp/bin/vppctl
CLI_SOCK=/tmp/vclnet-test/cli.sock
APP_SOCK=/tmp/vclnet-test/app_ns_sockets/default
VCL_CONF=/tmp/vclnet-share/vcl.conf
LIB_PATH=/home/aritrbas/vpp/vpp/build-root/install-vpp-native/vpp/lib/x86_64-linux-gnu
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VCLNET_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

VPP_PID=""
SERVER_PID=""

cleanup() {
    # Kill server first (graceful — sends SIGTERM which triggers Shutdown).
    if [ -n "$SERVER_PID" ]; then
        kill -TERM $SERVER_PID 2>/dev/null || true
        # Wait up to 2s for graceful shutdown.
        for i in $(seq 1 20); do
            kill -0 $SERVER_PID 2>/dev/null || break
            sleep 0.1
        done
        kill -9 $SERVER_PID 2>/dev/null || true
    fi
    # Then kill VPP (after server has disconnected).
    if [ -n "$VPP_PID" ]; then
        kill $VPP_PID 2>/dev/null || true
        wait $VPP_PID 2>/dev/null || true
        echo "VPP stopped."
    fi
}
trap cleanup EXIT INT TERM

pkill -f "cli-listen /tmp/vclnet-test" 2>/dev/null || true
sleep 1
rm -rf /tmp/vclnet-test
mkdir -p /tmp/vclnet-test/app_ns_sockets

mkdir -p /tmp/vclnet-share
cat > "$VCL_CONF" <<'EOF'
vcl {
  rx-fifo-size 4000000
  tx-fifo-size 4000000
  app-scope-local
  app-scope-global
  use-mq-eventfd
  app-socket-api /tmp/vclnet-test/app_ns_sockets/default
}
EOF

CPU_STANZA=""
if [ "$NUM_WORKERS" -gt 0 ]; then
    CPU_STANZA="cpu { workers $NUM_WORKERS }"
fi

"$VPP_BIN" \
  unix { nodaemon log /tmp/vpp.log full-coredump cli-listen "$CLI_SOCK" runtime-dir /tmp/vclnet-test } \
  api-trace { on } \
  $CPU_STANZA \
  session { enable use-app-socket-api } &
VPP_PID=$!

for i in $(seq 1 30); do
    if [ -S "$CLI_SOCK" ] && [ -S "$APP_SOCK" ]; then break; fi
    sleep 1
done
if [ ! -S "$APP_SOCK" ]; then
    echo "ERROR: VPP not ready after 30s"
    exit 1
fi
chmod 777 "$CLI_SOCK" "$APP_SOCK"

"$VPPCTL" -s "$CLI_SOCK" create loopback interface
"$VPPCTL" -s "$CLI_SOCK" set interface state loop0 up
"$VPPCTL" -s "$CLI_SOCK" set interface ip address loop0 127.0.0.1/8
echo "VPP ready. Loopback: 127.0.0.1/8"

cd "$VCLNET_DIR"

# Build first (so go run doesn't add compilation delay).
sudo -u aritrbas env PATH="$PATH" HOME=/home/aritrbas \
  /usr/local/go/bin/go build -o /tmp/vclnet-test/server ./examples/production_server
sudo -u aritrbas env PATH="$PATH" HOME=/home/aritrbas \
  /usr/local/go/bin/go build -o /tmp/vclnet-test/client ./examples/production_client

# Start server.
sudo -u aritrbas env LD_LIBRARY_PATH="$LIB_PATH" VCL_CONFIG="$VCL_CONF" \
  /tmp/vclnet-test/server &
SERVER_PID=$!

# Wait for server READY.
sleep 2
echo ""
echo "=== Running client (10 workers x 5 requests) ==="
sudo -u aritrbas env LD_LIBRARY_PATH="$LIB_PATH" VCL_CONFIG="$VCL_CONF" \
  /tmp/vclnet-test/client
echo ""
echo "=== Demo complete ==="
