# Windows test-runner workaround

The pinned Zig Windows process backend can let concurrently created child
processes inherit one another's output-pipe handles. A test process can complete
every test and exit successfully while another process keeps its pipes open.
Zig's Maker then waits for EOF and reports `test runner failed to respond`
after its unchanged 60-second response deadline.

Use separate compilation and execution commands on Windows:

```sh
mise exec -- zig build test-compile --summary all
mise exec -- zig build test -j1 --summary all
```

Use the same `-Doptimize=ReleaseSafe` on both commands for the full ReleaseSafe
suite. `test-release-fast-compile` warms `test-release-fast`, and
`test-rpc-quic-evidence-compile` warms `test-rpc-quic-evidence` with matching
`-Dquic=true` and optimization options. The `just` full-suite and QUIC evidence
recipes perform both phases on Windows; focused test recipes also use `-j1`.

The compile steps derive their dependencies from the corresponding execution
graph. They reuse the original compile nodes and generated-source prerequisites,
without executing test runners. They are cache warmup, not test evidence. The
second invocation still runs the original suite, and any remaining compilation
also runs serially. Waiting for the first invocation to exit is necessary:
serializing test binaries alone does not prevent overlap with compiler spawns.

This changes Maker job concurrency, not threads, concurrent RPC calls, worker
counts, deadlines, test selection, or skip policy inside test executables.
Linux and macOS retain parallel execution. Existing CI time limits and the
watchdog's child-exit propagation remain in force. Cold serial compilation would
be expensive, so Windows CI warms the selected suite in parallel first.

## Evidence

The original c875835
[Windows ReleaseSafe failure](https://github.com/nullstyle/capnp-zig/actions/runs/34315133881/job/102349647462)
reported the inactive-runner timeout for the TCP teardown executable. It did not
capture the protocol phase or process exit state, so that historical occurrence
cannot be conclusively assigned to pipe inheritance. Its two test callbacks
subsequently passed repeated terminal, direct protocol, and actual Maker runs in
both Debug and ReleaseSafe in
[probe 34319397598](https://github.com/nullstyle/capnp-zig/actions/runs/34319397598).

[Probe 34319916356](https://github.com/nullstyle/capnp-zig/actions/runs/34319916356)
at `f9dcacef7d381af71b769c078e62e80d803ebc0d` independently reproduced pipe
inheritance on the first concurrent process pair in both modes. A serial
control reached EOF while its sibling remained alive. Under concurrent
spawning, the victim exited zero but its pipes stayed open until its sibling
exited.

[Actual Maker probe 34320712747](https://github.com/nullstyle/capnp-zig/actions/runs/34320712747)
at `afacd39cf12708aa13267e84267be8932e14032e` established the exact false-timeout
mechanism. Sixteen precompiled one-test runners overlapped sixteen ordinary
processes with a shared 80-second deadline. The same graph then ran with `-j1`.
A disposable copy of the pinned library added one observational timeout marker;
the installed toolchain, response deadline, test code, and I/O control flow were
unchanged.

| Mode | Parallel false timeouts | Serial control |
| --- | --- | --- |
| Debug | 14 | 37/37 build steps, 16/16 tests, exit 0 |
| ReleaseSafe | 16 | 37/37 build steps, 16/16 tests, exit 0 |

Every qualifying timeout showed all test results accepted, one passing test,
zero failures, the native test process signaled with exit code zero, and both
output pipes empty but still open. Each parallel command exited 1 normally at
about 80 seconds; no external watchdog terminated it. The serial controls also
finished at about 80 seconds. The experiment's successful workflow status means
the expected failure and successful control were both observed.

Artifacts `windows-maker-inheritance-Debug-34320712747` and
`windows-maker-inheritance-ReleaseSafe-34320712747` retain receipts, phase logs,
instrumentation identity, and copied source. The archive SHA-256 values matched
GitHub's artifact digests. Removing the single observational insertion restored
the pinned source SHA-256
`a8030d91998a10b6bc65e14dfa64f4ef37cd535e718862da58e3bad033cba48d`.
One ReleaseSafe marker's suffix interleaved with other diagnostics; its causal
fields and the other fifteen complete markers were independently checked.

Remove this workaround only after a deliberately updated toolchain passes an
equivalent concurrent-process and Maker control, followed by the full Windows
Debug, ReleaseSafe, ReleaseFast, and QUIC gates. A green serial run does not prove
the upstream process-inheritance defect has been fixed.
