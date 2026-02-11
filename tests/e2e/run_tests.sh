#!/bin/sh
# Backward-compatible shim.
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
CACHE_DIR="${E2E_ZIG_GLOBAL_CACHE_DIR:-$REPO_ROOT/.zig-global-cache}"

export E2E_ZIG_GLOBAL_CACHE_DIR="$CACHE_DIR"
export ZIG_GLOBAL_CACHE_DIR="$CACHE_DIR"
exec zig run "$REPO_ROOT/tools/e2e_runner.zig" -- "$@"
