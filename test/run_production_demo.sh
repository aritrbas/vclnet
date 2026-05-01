#!/bin/bash
# run_production_demo.sh — Start VPP, run production server + client, stop.
#
# Usage: sudo -E bash run_production_demo.sh [workers]
#
# VPP paths and the invoking user are resolved by test/env.sh. Override
# VPP_PREFIX (or VPP_BIN + VPPCTL + VPP_LIB) and RUN_AS_USER on the environment
# if you're not using the defaults. `sudo -E` preserves those overrides.
set -e

NUM_WORKERS="${1:-0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VCLNET_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"
require_vpp_paths

CLI_SOCK=/tmp/vclnet-test/cli.sock
APP_SOCK=/tmp/vclnet-test/app_ns_sockets/default
VCL_CONF=/tmp/vclnet-share/vcl.conf

VPP_PID=""
SERVER_PID=""

cleanup() {
    # Kill server first (graceful — sends SIGTERM which triggers Shutdown).
    if [ -n "$SERVER_PID" ]; then
        kill -TERM "$SERVER_PID" 2>/dev/null || true
        for _ in $(seq 1 20); do
            kill -0 "$SERVER_PID" 2>/dev/null || break
            sleep 0.1
        done
        kill -9 "$SERVER_PID" 2>/dev/null || true
    fi
    # Then kill VPP (after server has disconnected).
    if [ -n "$VPP_PID" ]; then
        kill "$VPP_PID" 2>/dev/null || true
        wait "$VPP_PID" 2>/dev/null || true
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

# We deliberately use `bash -c` here so the CPU_STANZA words don't get quoted
# together. VPP CLI parses them as separate tokens.
bash -c "\"$VPP_BIN\" \
  unix { nodaemon log /tmp/vpp.log full-coredump cli-listen \"$CLI_SOCK\" runtime-dir /tmp/vclnet-test } \
  api-trace { on } \
  $CPU_STANZA \
  session { enable use-app-socket-api }" &
VPP_PID=$!

for _ in $(seq 1 30); do
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

if [ -f "$VCLNET_DIR/pkgconfig/vppcom.pc" ]; then
    export PKG_CONFIG_PATH="$VCLNET_DIR/pkgconfig:${PKG_CONFIG_PATH:-}"
fi

cd "$VCLNET_DIR"

# Build first (so go run doesn't add compilation delay).
run_as_user "$GO_BIN" build -o /tmp/vclnet-test/server ./examples/production_server
run_as_user "$GO_BIN" build -o /tmp/vclnet-test/client ./examples/production_client

# Start server.
VCL_CONFIG="$VCL_CONF" run_as_user /tmp/vclnet-test/server &
SERVER_PID=$!

sleep 2
echo ""
echo "=== Running client (10 workers x 5 requests) ==="
VCL_CONFIG="$VCL_CONF" run_as_user /tmp/vclnet-test/client
echo ""
echo "=== Demo complete ==="
