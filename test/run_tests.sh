#!/usr/bin/env bash
# run_tests.sh — Set up VCL config and run vclnet's go tests.
#
# Usage:
#   ./run_tests.sh                  # set up configs + run go test ./...
#   ./run_tests.sh setup            # only create VCL configs
#   ./run_tests.sh test             # just `go test ./...` (assumes setup done)
#   ./run_tests.sh manual           # run server + client examples back-to-back
#
# Requirements:
#   - VPP running with `session { enable use-app-socket-api }`.
#   - /run/vpp/app_ns_sockets/default world-writable.
#   - Go ≥ 1.26.
#
# VPP paths are resolved by test/env.sh. Override VPP_PREFIX (or the individual
# VPP_BIN / VPPCTL / VPP_LIB variables) on the environment for non-default
# installs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"

VPP_APP_SOCKET="${VPP_APP_SOCKET:-/run/vpp/app_ns_sockets/default}"

SHARE_DIR="${SHARE_DIR:-/tmp/vclnet-share}"
VCL_CONF="$SHARE_DIR/vcl.conf"

if [ -f "$REPO_DIR/pkgconfig/vppcom.pc" ]; then
    export PKG_CONFIG_PATH="$REPO_DIR/pkgconfig:${PKG_CONFIG_PATH:-}"
fi

GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
pass() { echo -e "${GREEN}[PASS]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; }
info() { echo -e "${CYAN}[INFO]${NC} $*"; }

setup_vcl_config() {
    info "Creating VCL config at $VCL_CONF"
    mkdir -p "$SHARE_DIR"
    cat > "$VCL_CONF" <<EOF
vcl {
  rx-fifo-size 4000000
  tx-fifo-size 4000000
  app-scope-local
  app-scope-global
  use-mq-eventfd
  app-socket-api $VPP_APP_SOCKET
}
EOF
}

preflight() {
    if ! pgrep -x vpp >/dev/null 2>&1 && ! pgrep -f '/bin/vpp' >/dev/null 2>&1; then
        fail "VPP does not appear to be running."
        echo "  Start with the recipe in README.md."
        exit 1
    fi
    if [[ ! -S "$VPP_APP_SOCKET" ]]; then
        fail "VPP app socket not found: $VPP_APP_SOCKET"
        exit 1
    fi
}

run_go_tests() {
    info "Running go test ./... with VCL_CONFIG=$VCL_CONF"
    cd "$REPO_DIR"
    LD_LIBRARY_PATH="$VPP_LIB" \
    VCL_CONFIG="$VCL_CONF" \
        "$GO_BIN" test -v -count=1 -timeout 120s ./...
}

run_unit_tests() {
    info "Running unit tests (no VPP required)..."
    cd "$REPO_DIR"
    "$GO_BIN" test -v -count=1 -run 'TestParse|TestResolve|TestAddr|TestOp|TestTimeout|TestTcp|TestErr|TestNet' .
    "$GO_BIN" test -v -count=1 -run 'TestIPBE|TestPortBE|TestIsAgain' ./internal/vclpoll/
}

run_integration_tcp() {
    info "Running TCP integration tests (IPv4 + IPv6)..."
    cd "$REPO_DIR"
    LD_LIBRARY_PATH="$VPP_LIB" \
    VCL_CONFIG="$VCL_CONF" \
        "$GO_BIN" test -v -count=1 -timeout 120s -run 'TestTCP' .
    LD_LIBRARY_PATH="$VPP_LIB" \
    VCL_CONFIG="$VCL_CONF" \
        "$GO_BIN" test -v -count=1 -timeout 120s -run 'TestEcho' ./internal/vclpoll/
}

run_integration_http() {
    info "Running HTTP integration tests (IPv4 + IPv6)..."
    cd "$REPO_DIR"
    LD_LIBRARY_PATH="$VPP_LIB" \
    VCL_CONFIG="$VCL_CONF" \
        "$GO_BIN" test -v -count=1 -timeout 120s -run 'TestHTTP' .
}

run_manual_echo() {
    info "Starting echo_server_net in background..."
    cd "$REPO_DIR"
    LD_LIBRARY_PATH="$VPP_LIB" VCL_CONFIG="$VCL_CONF" \
        "$GO_BIN" run ./examples/echo_server_net -port 9876 >/tmp/vclnet-server.log 2>&1 &
    local srv=$!
    trap "kill $srv 2>/dev/null || true" EXIT
    sleep 2

    info "Running echo_client_net..."
    LD_LIBRARY_PATH="$VPP_LIB" VCL_CONFIG="$VCL_CONF" \
        "$GO_BIN" run ./examples/echo_client_net -addr 127.0.0.1:9876 -msg "hello vclnet"

    info "Server log:"
    cat /tmp/vclnet-server.log
}

run_manual_http() {
    info "Starting http_server in background..."
    cd "$REPO_DIR"
    LD_LIBRARY_PATH="$VPP_LIB" VCL_CONFIG="$VCL_CONF" \
        "$GO_BIN" run ./examples/http_server -port 8080 >/tmp/vclnet-http.log 2>&1 &
    local srv=$!
    trap "kill $srv 2>/dev/null || true" EXIT
    sleep 2

    info "Running http_client..."
    LD_LIBRARY_PATH="$VPP_LIB" VCL_CONFIG="$VCL_CONF" \
        "$GO_BIN" run ./examples/http_client -url http://127.0.0.1:8080/health

    info "Server log:"
    cat /tmp/vclnet-http.log
}

cmd="${1:-test}"
case "$cmd" in
    setup) setup_vcl_config ;;
    unit)
        run_unit_tests
        ;;
    test)
        setup_vcl_config
        preflight
        run_go_tests
        ;;
    tcp)
        setup_vcl_config
        preflight
        run_integration_tcp
        ;;
    http)
        setup_vcl_config
        preflight
        run_integration_http
        ;;
    manual)
        setup_vcl_config
        preflight
        run_manual_echo
        ;;
    manual-http)
        setup_vcl_config
        preflight
        run_manual_http
        ;;
    *)
        echo "Usage: $0 [setup|unit|test|tcp|http|manual|manual-http]"
        exit 1
        ;;
esac
