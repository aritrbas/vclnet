#!/bin/bash
# env.sh — sourced by test/run_*.sh scripts to resolve VPP paths and the
# invoking (non-root) user in a portable way.
#
# Callers may override any of the following on the environment:
#
#   VPP_PREFIX       Install root that contains bin/, lib/, include/.
#                    Defaults to /usr (system install) if VPP_BIN/VPP_LIB/VPPCTL
#                    are not all set explicitly.
#   VPP_BIN          Full path to the `vpp` binary.
#                    Default: $VPP_PREFIX/bin/vpp
#   VPPCTL           Full path to `vppctl`.
#                    Default: $VPP_PREFIX/bin/vppctl
#   VPP_LIB          Directory containing libvppcom.so (LD_LIBRARY_PATH target).
#                    Default: $VPP_PREFIX/lib/$(dpkg-architecture -qDEB_HOST_MULTIARCH)
#                             or $VPP_PREFIX/lib.
#   RUN_AS_USER      Unprivileged user to run `go test` / examples as when the
#                    script is executed via sudo. Defaults to $SUDO_USER, or
#                    the current user when not running under sudo.
#   RUN_AS_HOME      $HOME for that user. Defaults to that user's passwd entry.
#   GO_BIN           Full path to the Go toolchain. Defaults to `go` on PATH,
#                    falling back to /usr/local/go/bin/go.
#
# Nothing here is workstation-specific. The script exits non-zero (via
# `require_vpp_paths`) if the resolved paths do not exist.

set -o pipefail

# --- VPP install discovery ------------------------------------------------

: "${VPP_PREFIX:=/usr}"

if [ -z "${VPP_BIN:-}" ]; then
    VPP_BIN="$VPP_PREFIX/bin/vpp"
fi
if [ -z "${VPPCTL:-}" ]; then
    VPPCTL="$VPP_PREFIX/bin/vppctl"
fi

if [ -z "${VPP_LIB:-}" ]; then
    _multiarch=""
    if command -v dpkg-architecture >/dev/null 2>&1; then
        _multiarch="$(dpkg-architecture -qDEB_HOST_MULTIARCH 2>/dev/null || true)"
    fi
    for _cand in \
        "$VPP_PREFIX/lib${_multiarch:+/$_multiarch}" \
        "$VPP_PREFIX/lib64" \
        "$VPP_PREFIX/lib"; do
        if [ -d "$_cand" ] && ls "$_cand"/libvppcom.so* >/dev/null 2>&1; then
            VPP_LIB="$_cand"
            break
        fi
    done
    # Fall back even if we didn't find it; require_vpp_paths will complain.
    : "${VPP_LIB:=$VPP_PREFIX/lib${_multiarch:+/$_multiarch}}"
    unset _multiarch _cand
fi

export VPP_PREFIX VPP_BIN VPPCTL VPP_LIB

# --- Non-root runtime user ------------------------------------------------

if [ -z "${RUN_AS_USER:-}" ]; then
    if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ]; then
        RUN_AS_USER="$SUDO_USER"
    else
        RUN_AS_USER="$(id -un)"
    fi
fi
if [ -z "${RUN_AS_HOME:-}" ]; then
    RUN_AS_HOME="$(getent passwd "$RUN_AS_USER" | cut -d: -f6)"
    : "${RUN_AS_HOME:=$HOME}"
fi

export RUN_AS_USER RUN_AS_HOME

# --- Go toolchain --------------------------------------------------------

if [ -z "${GO_BIN:-}" ]; then
    if command -v go >/dev/null 2>&1; then
        GO_BIN="$(command -v go)"
    elif [ -x /usr/local/go/bin/go ]; then
        GO_BIN=/usr/local/go/bin/go
    else
        GO_BIN=go
    fi
fi
export GO_BIN

# --- Validators ----------------------------------------------------------

require_vpp_paths() {
    local missing=0
    for var in VPP_BIN VPPCTL; do
        if [ ! -x "${!var}" ]; then
            echo "env.sh: $var ($(eval echo \$$var)) is not executable." >&2
            missing=1
        fi
    done
    if ! ls "$VPP_LIB"/libvppcom.so* >/dev/null 2>&1; then
        echo "env.sh: VPP_LIB ($VPP_LIB) does not contain libvppcom.so*" >&2
        missing=1
    fi
    if [ $missing -ne 0 ]; then
        cat >&2 <<HINT

Set VPP_PREFIX (or VPP_BIN + VPPCTL + VPP_LIB) to point at your VPP install:

    export VPP_PREFIX=/opt/vpp
    sudo -E bash test/run_integration.sh

HINT
        return 1
    fi
    return 0
}

# run_as_user CMD [ARG ...]
# Run CMD as $RUN_AS_USER with build and runtime environment preserved.
# Extra env can be passed by callers via `env VAR=VALUE ...` in front of CMD.
run_as_user() {
    if [ "$(id -u)" -eq 0 ] && [ "$RUN_AS_USER" != "root" ]; then
        sudo -u "$RUN_AS_USER" env \
            LD_LIBRARY_PATH="$VPP_LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
            VCL_CONFIG="${VCL_CONFIG:-${VCL_CONF:-}}" \
            PKG_CONFIG_PATH="${PKG_CONFIG_PATH:-}" \
            PATH="$PATH" \
            HOME="$RUN_AS_HOME" \
            "$@"
    else
        env \
            LD_LIBRARY_PATH="$VPP_LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
            VCL_CONFIG="${VCL_CONFIG:-${VCL_CONF:-}}" \
            PKG_CONFIG_PATH="${PKG_CONFIG_PATH:-}" \
            "$@"
    fi
}
