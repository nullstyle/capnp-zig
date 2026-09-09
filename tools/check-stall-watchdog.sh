#!/usr/bin/env bash
# A green wrapped test job must mean the child ran and its status was preserved.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .zig-cache
work="$(mktemp -d .zig-cache/watchdog-check.XXXXXX)"
trap 'rm -rf "$work"' EXIT

command_status=0
STALL_SECS=1 bash tools/stall_watchdog.sh bash -c 'echo watchdog-failure-executed; exit 23' \
  > "$work/failure.log" 2>&1 || command_status=$?
if [[ "$command_status" != 23 ]]; then
  cat "$work/failure.log"
  echo "watchdog lost the child's failure status: expected 23, got $command_status" >&2
  exit 1
fi
grep -Fx 'watchdog-failure-executed' "$work/failure.log" > /dev/null

STALL_SECS=1 bash tools/stall_watchdog.sh bash -c 'echo watchdog-success-executed' \
  > "$work/success.log" 2>&1
grep -Fx 'watchdog-success-executed' "$work/success.log" > /dev/null

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    # The actual native branch must resolve an MSYS PID and report its owned
    # command tree. The finite silent child then resumes and exits with 27:
    # a diagnostic must neither kill it nor replace its eventual exit status.
    command_status=0
    STALL_SECS=1 bash tools/stall_watchdog.sh bash -c \
      'echo watchdog-diagnostic-child; sleep 12; echo watchdog-child-resumed; exit 27' \
      > "$work/diagnostic.log" 2>&1 || command_status=$?
    cat "$work/diagnostic.log"
    if [[ "$command_status" != 27 ]]; then
      echo "watchdog diagnostic changed child status: expected 27, got $command_status" >&2
      exit 1
    fi
    grep -F 'Wrapped command process tree (Windows root PID ' "$work/diagnostic.log" > /dev/null
    grep -F '  executable: ' "$work/diagnostic.log" > /dev/null
    grep -F '  command: ' "$work/diagnostic.log" | grep -F 'watchdog-diagnostic-child' > /dev/null
    grep -F '  created: ' "$work/diagnostic.log" > /dev/null
    grep -F '  cpu_ms: ' "$work/diagnostic.log" > /dev/null
    grep -F '    tid=' "$work/diagnostic.log" > /dev/null
    grep -Fx 'watchdog-child-resumed' "$work/diagnostic.log" > /dev/null
    if grep -E 'dump skipped|cannot resolve native|snapshot failed' "$work/diagnostic.log"; then
      echo 'watchdog failed to capture Windows process diagnostics' >&2
      exit 1
    fi
    ;;
esac
echo 'Watchdog executes children, captures output, and preserves success/failure.'
