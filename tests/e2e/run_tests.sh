#!/bin/sh
# Backward-compatible shim.
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

exec zig run --global-cache-dir "$REPO_ROOT/.zig-global-cache" "$REPO_ROOT/tools/e2e_runner.zig" -- "$@"
