#!/bin/bash
# run_integration.sh — Start VPP, run integration tests, stop VPP.
# Usage: sudo bash run_integration.sh [test-filter]
# Example: sudo bash run_integration.sh TestTCPIPv4EchoSingle
set -e

VPP_BIN=/home/aritrbas/vpp/vpp/build-root/install-vpp-native/vpp/bin/vpp
VPPCTL=/home/aritrbas/vpp/vpp/build-root/install-vpp-native/vpp/bin/vppctl
CLI_SOCK=/tmp/vclnet-test/cli.sock
APP_SOCK=/tmp/vclnet-test/app_ns_sockets/default
VCL_CONF=/tmp/vclnet-share/vcl.conf
LIB_PATH=/home/aritrbas/vpp/vpp/build-root/install-vpp-native/vpp/lib/x86_64-linux-gnu
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VCLNET_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TEST_FILTER="${1:-TestTCP|TestHTTP|TestUDP|TestTLS}"

VPP_PID=""

cleanup() {
    if [ -n "$VPP_PID" ]; then
        kill $VPP_PID 2>/dev/null || true
        wait $VPP_PID 2>/dev/null || true
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

# Start VPP in background (nodaemon keeps it in foreground of the bg job).
"$VPP_BIN" \
  unix { nodaemon log /tmp/vpp.log full-coredump cli-listen "$CLI_SOCK" runtime-dir /tmp/vclnet-test } \
  api-trace { on } \
  session { enable use-app-socket-api } &
VPP_PID=$!

echo "VPP PID: $VPP_PID"
echo "Waiting for VPP sockets..."

for i in $(seq 1 20); do
    if [ -S "$CLI_SOCK" ] && [ -S "$APP_SOCK" ]; then
        break
    fi
    sleep 1
done

if [ ! -S "$APP_SOCK" ]; then
    echo "ERROR: VPP app socket not found after 20s"
    tail -20 /tmp/vpp.log
    kill $VPP_PID 2>/dev/null
    exit 1
fi

# Set permissions.
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

# Run tests as the regular user.
cd "$VCLNET_DIR"

echo "=== Running vclnet integration tests (filter: $TEST_FILTER) ==="
set +e
sudo -u aritrbas env \
  LD_LIBRARY_PATH="$LIB_PATH" \
  VCL_CONFIG="$VCL_CONF" \
  PATH="$PATH" \
  HOME=/home/aritrbas \
  /usr/local/go/bin/go test -v -count=1 -timeout 120s -run "$TEST_FILTER" . 2>&1
TEST_RC=$?
set -e

if [ $TEST_RC -ne 0 ]; then
    echo ""
    echo "=== vclnet tests FAILED (exit code: $TEST_RC) ==="
    exit $TEST_RC
fi

echo ""
echo "Restarting VPP for vclpoll tests (clean session state)..."
kill $VPP_PID 2>/dev/null || true
wait $VPP_PID 2>/dev/null || true
VPP_PID=""
sleep 1
rm -rf /tmp/vclnet-test
mkdir -p /tmp/vclnet-test/app_ns_sockets

"$VPP_BIN" \
  unix { nodaemon log /tmp/vpp.log full-coredump cli-listen "$CLI_SOCK" runtime-dir /tmp/vclnet-test } \
  api-trace { on } \
  session { enable use-app-socket-api } &
VPP_PID=$!

echo "VPP PID: $VPP_PID"
for i in $(seq 1 20); do
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
sudo -u aritrbas env \
  LD_LIBRARY_PATH="$LIB_PATH" \
  VCL_CONFIG="$VCL_CONF" \
  PATH="$PATH" \
  HOME=/home/aritrbas \
  /usr/local/go/bin/go test -v -count=1 -timeout 120s -run 'TestEcho' ./internal/vclpoll/ 2>&1
VCLPOLL_RC=$?
set -e

echo ""
if [ $VCLPOLL_RC -ne 0 ]; then
    echo "=== vclpoll tests FAILED (exit code: $VCLPOLL_RC) ==="
    exit $VCLPOLL_RC
fi

echo "=== All integration tests passed ==="
exit 0
