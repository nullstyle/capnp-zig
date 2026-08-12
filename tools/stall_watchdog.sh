#!/usr/bin/env bash
# Run a command; if it stops producing output, dump what every live test binary
# is doing before the CI step cap kills the job.
#
# Why this exists
# ---------------
# A hung test reaches CI as a step that prints its last line, goes silent, and
# is killed at `timeout-minutes` with no test named and no stack. Everything
# useful -- which binary, which thread, which lock -- dies with the runner.
# Recovering it costs a container, a hand-built stall detector and a lot of
# guessing; that has now happened twice on this repo, for two unrelated faults.
#
# This wrapper makes the FIRST occurrence self-diagnosing. It watches the
# command's output for silence and, once quiet for `STALL_SECS`, dumps for
# every live `test` process:
#   * the full command line (which cache-keyed binary is running)
#   * per-thread state / wchan / current syscall  (R = spinning, S = blocked)
#   * an eu-stack backtrace of every thread
#   * the test names embedded in the binary, to identify it without the build
#     graph
# then keeps waiting, so the step still fails the way it would have.
#
# Deliberately Linux-only for the dump (that is where the CI hangs have been,
# and where /proc and elfutils exist). Elsewhere it runs the command
# unmodified, so it is safe to wire into every platform's job.
#
# Usage: tools/stall_watchdog.sh <command...>
#   STALL_SECS  seconds of silence before dumping (default 180)

set -uo pipefail
set +m   # no job-control chatter when the mirror tail is reaped

STALL_SECS="${STALL_SECS:-180}"
OUT="$(mktemp -t stall_watchdog.XXXXXX)"
trap 'rm -f "$OUT"' EXIT

if [ "$#" -eq 0 ]; then
  echo "stall_watchdog: no command given" >&2
  exit 2
fi

"$@" >"$OUT" 2>&1 &
CMD_PID=$!

# Mirror output live so the job log looks unchanged.
tail -f "$OUT" &
TAIL_PID=$!
trap 'kill "$TAIL_PID" 2>/dev/null; wait "$TAIL_PID" 2>/dev/null; rm -f "$OUT"' EXIT

dump_stalled() {
  echo ""
  echo "==================== STALL WATCHDOG ===================="
  echo "No output for ${STALL_SECS}s. The command is still running, so this is"
  echo "a hang, not a slow step. Dumping every live test binary below."
  echo "State R with no syscall means a spin; S means blocked in the kernel."
  echo "========================================================"
  if [ "$(uname -s)" != "Linux" ]; then
    echo "(dump skipped: not Linux)"
    return
  fi
  command -v eu-stack >/dev/null 2>&1 || {
    echo "(installing elfutils for backtraces)"
    (sudo apt-get install -y -qq elfutils >/dev/null 2>&1) || true
  }
  local found=0
  for p in $(pgrep -x test 2>/dev/null); do
    found=1
    echo ""
    echo "--- pid $p ---"
    tr '\0' ' ' < "/proc/$p/cmdline" 2>/dev/null; echo
    echo "  threads:"
    for t in /proc/$p/task/*; do
      [ -e "$t/stat" ] || continue
      echo "    tid=$(basename "$t") state=$(awk '{print $3}' "$t/stat" 2>/dev/null)" \
           "wchan=$(cat "$t/wchan" 2>/dev/null)" \
           "syscall=$(cut -c1-32 "$t/syscall" 2>/dev/null)"
    done
    echo "  backtraces:"
    eu-stack -p "$p" 2>&1 | sed 's/^/    /'
    local exe
    exe="$(readlink -f "/proc/$p/exe" 2>/dev/null)"
    if [ -n "$exe" ]; then
      echo "  test names embedded in this binary:"
      grep -a -o -E 'test\.[a-zA-Z0-9 _().,:-]{8,70}' "$exe" 2>/dev/null | sort -u | head -30 | sed 's/^/    /'
    fi
  done
  [ "$found" -eq 0 ] && echo "(no live 'test' processes found -- the stall is in the build itself)"
  echo "=================== END STALL WATCHDOG =================="
}

last_size=0
quiet=0
dumped=0
while kill -0 "$CMD_PID" 2>/dev/null; do
  sleep 15
  size=$(wc -c <"$OUT" 2>/dev/null || echo 0)
  if [ "$size" = "$last_size" ]; then
    quiet=$((quiet + 15))
  else
    quiet=0
    dumped=0
  fi
  last_size="$size"
  if [ "$quiet" -ge "$STALL_SECS" ] && [ "$dumped" -eq 0 ]; then
    dump_stalled
    dumped=1
  fi
done

wait "$CMD_PID"
rc=$?
sleep 1           # let tail flush the final lines
exit "$rc"
