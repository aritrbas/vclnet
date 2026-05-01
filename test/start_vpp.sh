#!/bin/bash
# start_vpp.sh — Start VPP for integration testing.
#
# Uses the release build by default (matching the cgo linker rpath in
# internal/vclpoll/cgo.go). Override VPP_BIN / VPPCTL to point at the
# debug build if you need extra logging.
set -e

VPP_BIN="${VPP_BIN:-/home/aritrbas/vpp/vpp/build-root/install-vpp-native/vpp/bin/vpp}"
VPPCTL="${VPPCTL:-/home/aritrbas/vpp/vpp/build-root/install-vpp-native/vpp/bin/vppctl}"
CLI_SOCK=/run/vpp/cli.sock
APP_SOCK=/run/vpp/app_ns_sockets/default

# Kill existing VPP.
sudo killall vpp 2>/dev/null || true
sleep 1
sudo rm -f "$CLI_SOCK"

# Start VPP (daemonizes itself without nodaemon).
sudo "$VPP_BIN" \
  unix { log /tmp/vpp.log full-coredump cli-listen "$CLI_SOCK" } \
  api-trace { on } \
  session { enable use-app-socket-api }

echo "Waiting for VPP to start..."
for i in $(seq 1 20); do
    if [ -S "$CLI_SOCK" ] && [ -S "$APP_SOCK" ]; then
        break
    fi
    sleep 1
done

if [ ! -S "$APP_SOCK" ]; then
    echo "ERROR: VPP app socket not found after 20s"
    cat /tmp/vpp.log | tail -20
    exit 1
fi

# Set permissions so non-root can use it.
sudo chmod o+w "$CLI_SOCK"
sudo chmod o+w "$APP_SOCK"

echo "VPP started successfully:"
"$VPPCTL" -s "$CLI_SOCK" show version
echo "App socket: $APP_SOCK"
