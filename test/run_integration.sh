#!/bin/bash
# run_integration.sh — Start VPP, run integration tests, stop VPP.
#
# Usage: sudo -E bash run_integration.sh [--mode 2|3] [test-filter]
# Example: sudo -E bash run_integration.sh --mode 2 TestTCPIPv4EchoSingle
#
# VPP paths and the invoking user are resolved by test/env.sh. Override
# VPP_PREFIX (or VPP_BIN + VPPCTL + VPP_LIB) and RUN_AS_USER on the environment
# for non-default installs. `sudo -E` preserves those overrides.
set -e

VLS_MODE=3
if [ "${1:-}" = "--mode" ]; then
    VLS_MODE="${2:-}"
    shift 2
fi
if [ "$VLS_MODE" != "2" ] && [ "$VLS_MODE" != "3" ]; then
    echo "ERROR: --mode must be 2 or 3"
    exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VCLNET_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"
require_vpp_paths

CLI_SOCK=/tmp/vclnet-test/cli.sock
APP_SOCK=/tmp/vclnet-test/app_ns_sockets/default
VCL_CONF=/tmp/vclnet-share/vcl.conf
TEST_FILTER="${1:-TestTCP|TestHTTP|TestUDP|TestTLS}"

VPP_PID=""

cleanup() {
    if [ -n "$VPP_PID" ]; then
        kill "$VPP_PID" 2>/dev/null || true
        wait "$VPP_PID" 2>/dev/null || true
        echo "VPP stopped."
    fi
}
trap cleanup EXIT INT TERM

# Kill any leftover test VPP (don't touch production Calico-VPP in containers).
pkill -f "cli-listen /tmp/vclnet-test" 2>/dev/null || true
sleep 1
rm -rf /tmp/vclnet-test
mkdir -p /tmp/vclnet-test/app_ns_sockets

# Ensure VCL config exists.
mkdir -p /tmp/vclnet-share
MODE_TOKEN=""
if [ "$VLS_MODE" = "2" ]; then
    MODE_TOKEN="  multi-thread-workers"
fi
cat > "$VCL_CONF" <<EOF
vcl {
$MODE_TOKEN
  rx-fifo-size 4000000
  tx-fifo-size 4000000
  app-scope-local
  app-scope-global
  use-mq-eventfd
  app-socket-api /tmp/vclnet-test/app_ns_sockets/default
}
EOF

# Start VPP in background (nodaemon keeps it in foreground of the bg job).
"$VPP_BIN" \
  unix { nodaemon log /tmp/vpp.log full-coredump cli-listen "$CLI_SOCK" runtime-dir /tmp/vclnet-test } \
  api-trace { on } \
  session { enable use-app-socket-api } &
VPP_PID=$!

echo "VPP PID: $VPP_PID"
echo "Waiting for VPP sockets..."

for _ in $(seq 1 20); do
    if [ -S "$CLI_SOCK" ] && [ -S "$APP_SOCK" ]; then
        break
    fi
    sleep 1
done

if [ ! -S "$APP_SOCK" ]; then
    echo "ERROR: VPP app socket not found after 20s"
    tail -20 /tmp/vpp.log || true
    kill "$VPP_PID" 2>/dev/null || true
    exit 1
fi

chmod o+w "$CLI_SOCK" "$APP_SOCK"

echo "VPP ready:"
"$VPPCTL" -s "$CLI_SOCK" show version
echo ""

# Create loopback interface for IPv4/IPv6 testing.
"$VPPCTL" -s "$CLI_SOCK" create loopback interface
"$VPPCTL" -s "$CLI_SOCK" set interface state loop0 up
"$VPPCTL" -s "$CLI_SOCK" set interface ip address loop0 127.0.0.1/8
"$VPPCTL" -s "$CLI_SOCK" set interface ip address loop0 ::1/128
echo "Loopback configured with 127.0.0.1/8 and ::1/128"
echo ""

# Ensure pkg-config finds our rendered vppcom.pc when the caller has staged
# one under $VCLNET_DIR/pkgconfig. The Makefile `pc` target populates this.
if [ -f "$VCLNET_DIR/pkgconfig/vppcom.pc" ]; then
    export PKG_CONFIG_PATH="$VCLNET_DIR/pkgconfig:${PKG_CONFIG_PATH:-}"
fi

cd "$VCLNET_DIR"

echo "=== Running vclnet integration tests (filter: $TEST_FILTER, VLS mode: $VLS_MODE) ==="
set +e
MODE_ENV=()
if [ "$VLS_MODE" = "2" ]; then
    MODE_ENV=(VCLNET_VLS_MODE=2 VCLNET_WORKERS=1)
fi
VCL_CONFIG="$VCL_CONF" \
    run_as_user env "${MODE_ENV[@]}" VCL_CONFIG="$VCL_CONF" \
    "$GO_BIN" test -v -count=1 -timeout 120s -run "$TEST_FILTER" . 2>&1
TEST_RC=$?
set -e

if [ $TEST_RC -ne 0 ]; then
    echo ""
    echo "=== vclnet tests FAILED (exit code: $TEST_RC) ==="
    exit $TEST_RC
fi

echo ""
echo "Restarting VPP for vclpoll tests (clean session state)..."
kill "$VPP_PID" 2>/dev/null || true
wait "$VPP_PID" 2>/dev/null || true
VPP_PID=""
sleep 1
rm -rf /tmp/vclnet-test
mkdir -p /tmp/vclnet-test/app_ns_sockets

# vclpoll tests always use Mode 3 (vclpoll.AppInit hardcodes Mode 3), so
# write a plain VCL config without multi-thread-workers regardless of the
# top-level VLS_MODE selection.
VCLPOLL_CONF=/tmp/vclnet-share/vcl-vclpoll.conf
cat > "$VCLPOLL_CONF" <<'EOF'
vcl {
  rx-fifo-size 4000000
  tx-fifo-size 4000000
  app-scope-local
  app-scope-global
  use-mq-eventfd
  app-socket-api /tmp/vclnet-test/app_ns_sockets/default
}
EOF

"$VPP_BIN" \
  unix { nodaemon log /tmp/vpp.log full-coredump cli-listen "$CLI_SOCK" runtime-dir /tmp/vclnet-test } \
  api-trace { on } \
  session { enable use-app-socket-api } &
VPP_PID=$!

echo "VPP PID: $VPP_PID"
for _ in $(seq 1 20); do
    if [ -S "$CLI_SOCK" ] && [ -S "$APP_SOCK" ]; then break; fi
    sleep 1
done
if [ ! -S "$APP_SOCK" ]; then
    echo "ERROR: VPP app socket not found after restart"
    exit 1
fi
chmod o+w "$CLI_SOCK" "$APP_SOCK"

"$VPPCTL" -s "$CLI_SOCK" create loopback interface
"$VPPCTL" -s "$CLI_SOCK" set interface state loop0 up
"$VPPCTL" -s "$CLI_SOCK" set interface ip address loop0 127.0.0.1/8
"$VPPCTL" -s "$CLI_SOCK" set interface ip address loop0 ::1/128
echo "VPP restarted with clean state"
sleep 2  # Give VPP session layer time to fully initialize

echo "=== Running vclpoll integration tests ==="
set +e
VCL_CONFIG="$VCLPOLL_CONF" \
    run_as_user env VCL_CONFIG="$VCLPOLL_CONF" \
    "$GO_BIN" test -v -count=1 -timeout 120s -run 'TestEcho' ./internal/vclpoll/ 2>&1
VCLPOLL_RC=$?
set -e

echo ""
if [ $VCLPOLL_RC -ne 0 ]; then
    echo "=== vclpoll tests FAILED (exit code: $VCLPOLL_RC) ==="
    exit $VCLPOLL_RC
fi

echo "=== All integration tests passed ==="
exit 0
