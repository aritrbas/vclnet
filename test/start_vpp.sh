#!/bin/bash
# start_vpp.sh — Start VPP for integration testing.
#
# VPP paths are resolved by test/env.sh. Override VPP_PREFIX (or the individual
# VPP_BIN / VPPCTL / VPP_LIB variables) on the environment for non-default
# installs.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"
require_vpp_paths

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
for _ in $(seq 1 20); do
    if [ -S "$CLI_SOCK" ] && [ -S "$APP_SOCK" ]; then
        break
    fi
    sleep 1
done

if [ ! -S "$APP_SOCK" ]; then
    echo "ERROR: VPP app socket not found after 20s"
    tail -20 /tmp/vpp.log || true
    exit 1
fi

# Set permissions so non-root can use it.
sudo chmod o+w "$CLI_SOCK"
sudo chmod o+w "$APP_SOCK"

echo "VPP started successfully:"
"$VPPCTL" -s "$CLI_SOCK" show version
echo "App socket: $APP_SOCK"
