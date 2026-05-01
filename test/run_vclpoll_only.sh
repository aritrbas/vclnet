#!/bin/bash
# Run only vclpoll integration tests with a fresh VPP.
#
# Uses the VPP release build by default so it matches the cgo linker rpath
# in internal/vclpoll/cgo.go. Override VPP_BIN/VPPCTL/LIB_PATH if you need
# to test against the debug build.
set -e

VPP_BIN="${VPP_BIN:-/home/aritrbas/vpp/vpp/build-root/install-vpp-native/vpp/bin/vpp}"
VPPCTL="${VPPCTL:-/home/aritrbas/vpp/vpp/build-root/install-vpp-native/vpp/bin/vppctl}"
CLI_SOCK=/run/vpp/cli.sock
APP_SOCK=/run/vpp/app_ns_sockets/default
VCL_CONF=/tmp/vclnet-share/vcl.conf
LIB_PATH="${LIB_PATH:-/home/aritrbas/vpp/vpp/build-root/install-vpp-native/vpp/lib/x86_64-linux-gnu}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VCLNET_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

VPP_PID=""
cleanup() {
    if [ -n "$VPP_PID" ]; then
        kill $VPP_PID 2>/dev/null || true
        wait $VPP_PID 2>/dev/null || true
        echo "VPP stopped."
    fi
}
trap cleanup EXIT INT TERM

killall vpp 2>/dev/null || true
sleep 1
rm -f "$CLI_SOCK"

mkdir -p /tmp/vclnet-share
cat > "$VCL_CONF" <<'EOF'
vcl {
  rx-fifo-size 4000000
  tx-fifo-size 4000000
  app-scope-local
  app-scope-global
  use-mq-eventfd
  app-socket-api /run/vpp/app_ns_sockets/default
}
EOF

"$VPP_BIN" \
  unix { nodaemon log /tmp/vpp.log full-coredump cli-listen "$CLI_SOCK" } \
  api-trace { on } \
  session { enable use-app-socket-api } &
VPP_PID=$!

echo "VPP PID: $VPP_PID"
for i in $(seq 1 20); do
    if [ -S "$CLI_SOCK" ] && [ -S "$APP_SOCK" ]; then break; fi
    sleep 1
done
if [ ! -S "$APP_SOCK" ]; then
    echo "ERROR: VPP app socket not found"
    exit 1
fi
chmod o+w "$CLI_SOCK" "$APP_SOCK"

"$VPPCTL" -s "$CLI_SOCK" create loopback interface
"$VPPCTL" -s "$CLI_SOCK" set interface state loop0 up
"$VPPCTL" -s "$CLI_SOCK" set interface ip address loop0 127.0.0.1/8
"$VPPCTL" -s "$CLI_SOCK" set interface ip address loop0 ::1/128
echo "Loopback ready"

cd "$VCLNET_DIR"
echo "=== Running vclpoll tests ONLY ==="
sudo -u aritrbas env \
  LD_LIBRARY_PATH="$LIB_PATH" \
  VCL_CONFIG="$VCL_CONF" \
  PATH="$PATH" \
  HOME=/home/aritrbas \
  /usr/local/go/bin/go test -v -count=1 -timeout 60s -run 'TestEchoSingleRoundTrip' ./internal/vclpoll/ 2>&1
echo "exit code: $?"
