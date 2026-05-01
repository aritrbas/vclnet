#!/bin/bash
# run_multiworker.sh — Start VPP with multiple worker threads, run stress tests.
#
# Usage: sudo -E bash run_multiworker.sh [workers] [test-filter]
# Example: sudo -E bash run_multiworker.sh 4 TestMultiWorker
#
# This validates vclnet under production-like conditions where VPP distributes
# sessions across multiple worker threads. Tests exercise:
#   - High-concurrency connect/accept across VPP workers
#   - Parallel I/O from many goroutines simultaneously
#   - Shared-mode VLS access while VPP distributes session work
#   - Cut-through sessions under worker distribution
#
# VPP paths and the invoking user are resolved by test/env.sh — see that file
# for override variables (VPP_PREFIX, RUN_AS_USER, etc.).
set -e

NUM_WORKERS="${1:-4}"
TEST_FILTER="${2:-TestMultiWorker}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VCLNET_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"
require_vpp_paths

CLI_SOCK=/tmp/vclnet-test/cli.sock
APP_SOCK=/tmp/vclnet-test/app_ns_sockets/default
VCL_CONF=/tmp/vclnet-share/vcl.conf

VPP_PID=""

cleanup() {
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

echo "Starting VPP with $NUM_WORKERS worker threads..."

"$VPP_BIN" \
  unix { nodaemon log /tmp/vpp.log full-coredump cli-listen "$CLI_SOCK" runtime-dir /tmp/vclnet-test } \
  api-trace { on } \
  cpu { workers "$NUM_WORKERS" } \
  session { enable use-app-socket-api } &
VPP_PID=$!

echo "VPP PID: $VPP_PID"
echo "Waiting for VPP sockets..."

for _ in $(seq 1 30); do
    if [ -S "$CLI_SOCK" ] && [ -S "$APP_SOCK" ]; then
        break
    fi
    sleep 1
done

if [ ! -S "$APP_SOCK" ]; then
    echo "ERROR: VPP app socket not found after 30s"
    tail -20 /tmp/vpp.log || true
    kill "$VPP_PID" 2>/dev/null || true
    exit 1
fi

chmod o+w "$CLI_SOCK" "$APP_SOCK"

echo ""
echo "VPP ready (multi-worker):"
"$VPPCTL" -s "$CLI_SOCK" show version
"$VPPCTL" -s "$CLI_SOCK" show threads
echo ""

"$VPPCTL" -s "$CLI_SOCK" create loopback interface
"$VPPCTL" -s "$CLI_SOCK" set interface state loop0 up
"$VPPCTL" -s "$CLI_SOCK" set interface ip address loop0 127.0.0.1/8
"$VPPCTL" -s "$CLI_SOCK" set interface ip address loop0 ::1/128
echo "Loopback configured with 127.0.0.1/8 and ::1/128"
echo ""

if [ -f "$VCLNET_DIR/pkgconfig/vppcom.pc" ]; then
    export PKG_CONFIG_PATH="$VCLNET_DIR/pkgconfig:${PKG_CONFIG_PATH:-}"
fi

cd "$VCLNET_DIR"

echo "=== Running multi-worker integration tests (filter: $TEST_FILTER, workers: $NUM_WORKERS) ==="
set +e
VCL_CONFIG="$VCL_CONF" \
VCLNET_MULTI_WORKER=1 \
VCLNET_VPP_WORKERS="$NUM_WORKERS" \
    run_as_user \
      env VCLNET_MULTI_WORKER=1 VCLNET_VPP_WORKERS="$NUM_WORKERS" \
      "$GO_BIN" test -v -count=1 -timeout 300s -run "$TEST_FILTER" . 2>&1
TEST_RC=$?
set -e

echo ""
if [ $TEST_RC -ne 0 ]; then
    echo "=== Multi-worker tests FAILED (exit code: $TEST_RC) ==="
    echo ""
    echo "VPP session state:"
    "$VPPCTL" -s "$CLI_SOCK" show session verbose 2>/dev/null || true
    exit $TEST_RC
fi

echo "=== All multi-worker tests passed (workers: $NUM_WORKERS) ==="
exit 0
