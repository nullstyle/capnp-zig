#!/bin/bash
# Generic entrypoint script for Cap'n Proto RPC e2e test containers.
#
# Usage:
#   entrypoint.sh server --port PORT
#   entrypoint.sh client --host HOST --port PORT
#
# Each language implementation provides its own server/client binaries
# (or scripts) in /app/bin/. This entrypoint delegates to them.

set -euo pipefail

MODE="${1:-server}"
shift || true

case "$MODE" in
  server)
    echo "[entrypoint] Starting RPC server..."
    if [ -x /app/bin/server ]; then
      exec /app/bin/server "$@"
    else
      echo "[entrypoint] No server binary found at /app/bin/server"
      echo "[entrypoint] Language implementation not yet installed."
      echo "[entrypoint] Keeping container alive for debugging..."
      exec sleep infinity
    fi
    ;;
  client)
    echo "[entrypoint] Starting RPC client..."
    if [ -x /app/bin/client ]; then
      if [ -n "${E2E_TIMEOUT_SEC:-}" ]; then
        # Bound client execution to prevent e2e harness hangs.
        exec timeout --signal=TERM --kill-after=5 "${E2E_TIMEOUT_SEC}" /app/bin/client "$@"
      fi
      exec /app/bin/client "$@"
    else
      echo "[entrypoint] No client binary found at /app/bin/client"
      echo "[entrypoint] Language implementation not yet installed."
      exit 1
    fi
    ;;
  shell)
    echo "[entrypoint] Starting interactive shell..."
    exec /bin/bash
    ;;
  *)
    echo "[entrypoint] Unknown mode: $MODE"
    echo "Usage: entrypoint.sh {server|client|shell} [options]"
    exit 1
    ;;
esac
