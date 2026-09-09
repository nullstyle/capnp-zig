#!/usr/bin/env bash
# A green wrapped test job must mean the child ran and its status was preserved.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .zig-cache
work="$(mktemp -d .zig-cache/watchdog-check.XXXXXX)"
trap 'rm -rf "$work"' EXIT

command_status=0
bash tools/stall_watchdog.sh bash -c 'echo watchdog-failure-executed; exit 23' \
  > "$work/failure.log" 2>&1 || command_status=$?
if [[ "$command_status" != 23 ]]; then
  cat "$work/failure.log"
  echo "watchdog lost the child's failure status: expected 23, got $command_status" >&2
  exit 1
fi
grep -Fx 'watchdog-failure-executed' "$work/failure.log" > /dev/null

bash tools/stall_watchdog.sh bash -c 'echo watchdog-success-executed' \
  > "$work/success.log" 2>&1
grep -Fx 'watchdog-success-executed' "$work/success.log" > /dev/null
echo 'Watchdog executes children, captures output, and preserves success/failure.'
