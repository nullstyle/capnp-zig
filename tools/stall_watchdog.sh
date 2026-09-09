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
# This wrapper captures evidence on the first occurrence. It watches the
# command's output for silence and, once quiet for `STALL_SECS`, dumps for
# every live `test` process:
#   * the full command line (which cache-keyed binary is running)
#   * per-thread state / wchan / current syscall  (R = spinning, S = blocked)
#   * an eu-stack backtrace of every thread
#   * the test names embedded in the binary, to identify it without the build
#     graph
# then keeps waiting, so the step still fails the way it would have.
#
# Linux captures stacks with /proc and elfutils. Windows captures the wrapped
# command's native process tree, binary identities, CPU time and thread states.
# Silence can also mean slow compilation; snapshots do not prove a deadlock.
# Diagnostics do not terminate the command or change its eventual exit status.
#
# Usage: tools/stall_watchdog.sh <command...>
#   STALL_SECS  seconds of silence before dumping (default 180)

set -uo pipefail
set +m   # no job-control chatter when the mirror tail is reaped

STALL_SECS="${STALL_SECS:-180}"
if ! [[ "$STALL_SECS" =~ ^[1-9][0-9]*$ ]]; then
  echo "stall_watchdog: STALL_SECS must be a positive integer" >&2
  exit 2
fi
POLL_SECS=15
if [ "$STALL_SECS" -lt "$POLL_SECS" ]; then POLL_SECS="$STALL_SECS"; fi
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

dump_windows() {
  # Git Bash uses an MSYS PID namespace. task/process APIs need WINPID, not $!.
  local winpid
  winpid="$(ps -l -p "$CMD_PID" | awk '
    NR == 1 { for (i = 1; i <= NF; i++) if ($i == "WINPID") column = i }
    NR > 1 && column { if ($1 !~ /^[0-9]+$/) column++; print $column; exit }
  ')"
  if ! [[ "$winpid" =~ ^[1-9][0-9]*$ ]]; then
    echo "(cannot resolve native Windows PID for wrapped MSYS PID $CMD_PID; command may have exited)"
    return
  fi
  if ! command -v powershell.exe >/dev/null 2>&1; then
    echo "(Windows diagnostics unavailable: powershell.exe not found; wrapped WINPID $winpid)"
    return
  fi
  STALL_WATCHDOG_WINPID="$winpid" powershell.exe -NoLogo -NoProfile -NonInteractive -Command - <<'POWERSHELL'
$ErrorActionPreference = 'Stop'
$rootProcessId = [int]$env:STALL_WATCHDOG_WINPID
Write-Output "Wrapped command process tree (Windows root PID $rootProcessId):"
try {
  $snapshot = @(Get-CimInstance Win32_Process -OperationTimeoutSec 10)
  $owned = @($snapshot | Where-Object { $_.ProcessId -eq $rootProcessId })
  $seen = @{}
  foreach ($entry in $owned) { $seen[[int]$entry.ProcessId] = $true }
  for ($index = 0; $index -lt $owned.Count; $index++) {
    $parent = $owned[$index]
    foreach ($child in $snapshot) {
      if ($child.ParentProcessId -ne $parent.ProcessId) { continue }
      $childProcessId = [int]$child.ProcessId
      if ($seen.ContainsKey($childProcessId)) { continue }
      # A parent PID may have been reused since an older process was created.
      if ($null -eq $child.CreationDate -or $null -eq $parent.CreationDate -or
          $child.CreationDate -lt $parent.CreationDate) { continue }
      $seen[$childProcessId] = $true
      $owned += $child
    }
  }
  if ($owned.Count -eq 0) { Write-Output '(wrapped command exited before the process snapshot)' }
  foreach ($entry in $owned) {
    Write-Output "--- Windows PID $($entry.ProcessId), parent $($entry.ParentProcessId) ---"
    Write-Output "  executable: $($entry.ExecutablePath)"
    Write-Output "  command: $($entry.CommandLine)"
    Write-Output "  created: $($entry.CreationDate.ToString('o'))"
    try {
      $process = Get-Process -Id $entry.ProcessId -ErrorAction Stop
      Write-Output "  cpu_ms: $($process.TotalProcessorTime.TotalMilliseconds)"
      Write-Output "  threads: $($process.Threads.Count)"
      foreach ($thread in $process.Threads) {
        $state = $thread.ThreadState
        $reason = if ($state -eq 'Wait') { $thread.WaitReason } else { '-' }
        Write-Output "    tid=$($thread.Id) state=$state wait=$reason cpu_ms=$($thread.TotalProcessorTime.TotalMilliseconds)"
      }
    } catch { Write-Output "  (thread snapshot unavailable: $($_.Exception.Message))" }
  }
} catch { Write-Output "(Windows process snapshot failed: $($_.Exception.Message))" }

POWERSHELL
}

dump_stalled() {
  echo ""
  echo "==================== STALL WATCHDOG ===================="
  echo "No output for ${STALL_SECS}s while the command is still running."
  echo "Capturing a possible stall or slow step; the command will keep running."
  echo "========================================================"
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*)
      dump_windows
      echo "=================== END STALL WATCHDOG =================="
      return ;;
    Linux) ;;
    *) echo "(dump skipped: no native diagnostics for this platform)"; return ;;
  esac
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
  sleep "$POLL_SECS"
  size=$(wc -c <"$OUT" 2>/dev/null || echo 0)
  if [ "$size" = "$last_size" ]; then
    quiet=$((quiet + POLL_SECS))
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
