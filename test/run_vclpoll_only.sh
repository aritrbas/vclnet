#!/bin/bash
# run_vclpoll_only.sh — Run only vclpoll integration tests with a fresh VPP.
#
# VPP paths and the invoking user are resolved by test/env.sh. Override
# VPP_PREFIX (or VPP_BIN + VPPCTL + VPP_LIB) and RUN_AS_USER on the environment
# if you're not using the defaults. `sudo -E` preserves those overrides.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VCLNET_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"
require_vpp_paths

CLI_SOCK=/run/vpp/cli.sock
APP_SOCK=/run/vpp/app_ns_sockets/default
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
for _ in $(seq 1 20); do
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

if [ -f "$VCLNET_DIR/pkgconfig/vppcom.pc" ]; then
    export PKG_CONFIG_PATH="$VCLNET_DIR/pkgconfig:${PKG_CONFIG_PATH:-}"
fi

cd "$VCLNET_DIR"
echo "=== Running vclpoll tests ONLY ==="
VCL_CONFIG="$VCL_CONF" \
    run_as_user "$GO_BIN" test -v -count=1 -timeout 60s -run 'TestEchoSingleRoundTrip' ./internal/vclpoll/ 2>&1
echo "exit code: $?"
