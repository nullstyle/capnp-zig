# Windows as a First-Class Target and Development OS

Status: **Complete.** Phases 0–3 landed and verified green on real
Windows runners (CI run on `9608ef0`: native test suite, soak smoke,
check-api, and self-interop e2e all pass on windows-latest). Phase 4 is
upstream-tracking only — nothing actionable locally. Owner: rotating per
session; this document is the source of truth for progress.

Progress notes:

- QUIC follow-up (2026-08-11): boringssl-zig commit `292c70a` and quic-zig
  commit `e00d449` are published, and capnp-zig pins the latter archive.
  BoringSSL's Windows socket link bypasses package-config lookup, so native
  shells and Git Bash no longer invoke `pkg-config.BAT`. QUIC's Windows UDP
  path now uses one cancellable `io.concurrent` receive plus an
  `Io.Condition`; timer ticks preserve the pending receive, and teardown
  cancels/reaps it exactly once before close callbacks.

  The QUIC job includes `windows-latest` and runs native Debug and ReleaseSafe
  evidence. A Zig scanner rejects `SkipZigTest` and requires exactly four
  runnable roots with floors 26 + 1 + 17 + 8 = 52; no output parser or
  CI-only shell/package is involved. Local macOS evidence is 61/61 in both
  modes, and the Windows full test tree cross-compiles 113/113.
  Cross-compilation is not runtime parity: native
  Windows execution remains the hosted acceptance gate after capnp-zig is
  pushed. The broader full-repository QUIC-root gate remains Linux-only.

- TCP truthfulness follow-up (2026-08-11): the Windows soak path no longer has
  a successful no-op. A pass requires positive session, call, chaos-close, and
  applicable deadline-cancellation counters. The thirteen portable
  stream-transport skips are gone; only the documented `TCP_NODELAY` timing
  exception remains.

- Codegen parity follow-up (2026-08-10): the Windows CI job installs the
  checksum-pinned upstream `capnp.exe` archive and now runs the same native
  `capnp`-driven serialization/codegen tests as Linux and macOS. Because that
  archive omits the standard schema tree, every test invocation goes through
  `tests/serialization/support/capnp_cli.zig`, which injects the vendored
  `-Ivendor/ext/capnproto/c++/src` for `compile`, `convert`, and `eval`.
  CI verifies `capnp --version` before tests, preventing missing-tool skips from
  masquerading as coverage. The live matrix in `docs/stability.md` therefore
  records Windows codegen as full, not partial.

- Phase 3 (2026-06-12): the Justfile pins `windows-shell` to `sh` (Git
  Bash) so every recipe works unchanged on Windows; the benchmarks use
  the Io-backed monotonic clock (no libc dependency) and cross-compile
  for Windows; CONTRIBUTING gains a "Developing on Windows" quickstart
  (toolchain, shell, Docker Desktop path for cross-impl e2e, Defender
  exclusion). **Resolved (2026-06-12)**: the project deliberately uses a
  direct-push-to-`main` workflow, not pull requests, so the Windows jobs
  are intentionally *not* required branch-protection checks (required
  status checks would reject direct pushes). Gate parity is satisfied
  instead by the Windows jobs running on every push with exactly the same
  standing as their macOS/Linux peers — there is no per-OS asymmetry in
  what runs or what blocks.

- Phase 2 (2026-06-12): `zig build e2e-self` (tools/e2e_self.zig) drives
  the zig e2e client against the zig e2e server over real loopback TCP
  for all four schemas with TAP accounting — pure `std.Io`, no docker,
  no reference toolchains — and runs as a step in every Test matrix OS
  plus `just e2e-self` locally (also part of `just ci`). The
  cross-implementation docker e2e remains the ubuntu CI gate and works
  locally on Windows via Docker Desktop/WSL2.

- Phase 1 (2026-06-12): two designs were eliminated by upstream
  realities before the third shipped. The `std.Io` read-with-timeout
  path is blocked (Windows `Threaded` batch net ops are blocking-only,
  "TODO integrate with overlapped I/O"), and the `WSAPoll` fallback
  turned out to be impossible too: **std's Windows sockets are raw AFD
  handles** created via `NtCreateFile`/`IOCTL_AFD_*`, which winsock
  (`WSAPoll`, `ws2_32.setsockopt`) rejects — on real Windows runners
  this surfaced as the run loop hanging (poll error degraded to a
  tickless blocking read). The shipped design needs no readiness
  primitive at all: on Windows, `run()` launches each blocking read as a
  cancellable `io.concurrent` task and waits on a timed
  `std.Io.Condition` for read completion, wake, or the tick timeout
  (`WinReadBridge` in connection.zig); buffer ownership passes to the
  task while a read is in flight and back on completion, callbacks all
  stay on the `run()` thread, and `wake()` signals the condition
  directly (no wake channel needed on Windows). A *third* dead end was
  eliminated by real-Windows CI before this stuck: a raw `std.Thread`
  reader (the first cut of this design) cannot be cancelled — std's
  cancellation keys off `Thread.current`, which only io worker threads
  have — so `shutdown`/`close` did not unblock its pending AFD receive
  and teardown wedged for the kernel's multi-minute read timeout, which
  also starved sibling test binaries of CPU. Running the read via
  `io.concurrent` (not `io.async`, which silently runs inline once
  `async_limit ≈ cpu_count` is hit; `concurrent_limit` defaults to
  unlimited) puts it on a cancellable worker so teardown's
  `future.cancel` drives `NtCancelIoFileEx`.
  `TCP_NODELAY` on Windows is blocked upstream for the same AFD reason
  and is a documented no-op; the one test that depends on Nagle-free
  small writes ("traffic resets the idle clock") carries a documented
  Windows skip while ticks/idle/deadline coverage runs there fully.
  Public handle-taking transport entry points take the platform-stable
  `SocketFd` wrapper (fixes the `check-api` width drift; `check-api`
  runs in all three Test matrix OSes), the socket-pair test helpers
  are portable (`createLoopbackSocketPair`, TCP_NODELAY'd on POSIX),
  and soak runs as a 2s smoke per Test job plus a Windows nightly
  lane. Porting also surfaced two real bug classes: test helpers that
  silently swallowed `EBADF` double-closes (`std.Io.Threaded` rightly
  panics on them as use-after-free), and timing tests whose margins
  assumed unloaded schedulers. The `std.Io`-native loop remains the
  long-term direction once upstream lands overlapped net I/O.

- Phase 0 (2026-06-12): `.gitattributes` landed and validated against an
  `autocrlf=true` clone; plugin CLI options now parse on Windows; platform
  matrix added to `docs/stability.md` with a README pointer. The
  `check-api` handle-width fix (item 4) moved into Phase 1: the fix is
  changing the same transport signatures Phase 1 rewires
  (`Connection.init`/`Transport.init*`/`Listener.initFd`/`closeFd` take
  `i32`-rendered `Socket.Handle` params today; they will take the
  symbolically-rendered `std.Io.net.Socket` instead), so they are done in
  one pass, and the Windows `check-api` CI step lands with it.

Goal: Windows is a first-class target and development OS, with the same
standing as macOS and Linux — feature parity in the runtime, test parity in
CI, and a frictionless local development loop.

## Where Windows stands today (2026-06-12)

Already true, enforced per push since commit `d47cf30`:

- The full unit suite (`zig build test`), the ReleaseSafe subset, the
  hardening gate, and `zig build check` run **natively green on
  windows-latest** in CI.
- A Linux-hosted cross-compile lane (`zig build check-compile
  check-test-compile -Dtarget=x86_64-windows`) compiles the plugin, the
  examples, the e2e binaries, and all ~60 registered test binaries for
  Windows on every push, so compile rot cannot land silently.
- The protocol core — serialization, codegen, `Peer` (including
  persistence, promises, three-party state) — is platform-neutral and
  natively tested on Windows.

Known divergences as captured when this plan was written (2026-06-12) — the
gap the plan set out to close. This table is the historical baseline, not
current status: `docs/stability.md` is the live per-platform matrix
(ticks/idle/wake, plugin CLI options, socket suites, and the soak lane have
since landed on Windows; `check-api` platform-stability is enforced — pub
entry points take the `SocketFd`/`OwnerThreadId` wrappers precisely so the
snapshot renders identically everywhere; `TCP_NODELAY` remains blocked
upstream):

| Area | Divergence |
|---|---|
| TCP connection loop | tick cadence, idle reaping, and the wake signal are comptime no-ops on Windows (the wait loop is `poll(2)` + a pipe); deadline sweeps therefore never fire unless the embedder drives `checkDeadlines` manually |
| TCP options | `TCP_NODELAY` is skipped on Windows (Nagle stays on) |
| Plugin CLI | `capnpc-zig` ignores its CLI options on Windows (legacy guard predating the portable args pattern) |
| Tests | socket-level suites (`rpc_tick_idle`, parts of `rpc_connection_failure`, `stream_transport`/`runtime` inline tests) skip on Windows because the shared test helper uses `socketpair(2)` |
| Harnesses | soak (needs ticks), benches (libc `clock_gettime`), e2e runner raw sockets are POSIX-only |
| Hygiene | no `.gitattributes` (a Windows checkout with `core.autocrlf=true` corrupts golden-file comparisons and `fmt-check`); Justfile recipes assume a POSIX shell |
| Gates | `check-api` target-stability is unverified for Windows (transport signatures may render `Socket.Handle` at platform width) |

## Definition of done ("first-class")

1. **Feature parity**: ticks, idle reaping, deadline sweeps, wake,
   `TCP_NODELAY`, and graceful-shutdown semantics behave identically on
   Windows, or the divergence is listed in the platform matrix with an
   upstream tracking note.
2. **Test parity**: Windows runs the same test set; `SkipZigTest` on
   Windows exists only for documented platform features.
3. **CI parity**: windows-latest appears in every job matrix that runs on
   macOS/Linux, except jobs physically impossible on hosted runners
   (Linux-container e2e, coverage-guided fuzzing until upstream COFF
   support) — and those get Windows-equivalent coverage instead.
4. **Dev parity**: a fresh Windows machine can clone and run `just ci`
   (minus the docker e2e phase) green; CONTRIBUTING has a Windows
   quickstart; no line-ending or shell landmines.
5. **Gate parity**: the Windows jobs run on every push with the same
   standing as their macOS/Linux peers. (This project pushes directly to
   `main` rather than using PRs, so "required status check" configuration
   is intentionally not used — parity means no per-OS asymmetry in what
   runs, which the per-push matrix already guarantees.)

## Phase 0 — guardrails and quick wins (~0.5 day)

1. **`.gitattributes`**: normalize all text to LF (`* text=auto eol=lf`,
   explicit entries for `*.zig`, `*.capnp`, golden/fixture trees, and
   binary markers). Validate by cloning with `core.autocrlf=true` and
   running the codegen golden suite and `fmt-check`.
2. **Plugin CLI on Windows**: replace the early-return guard in
   `src/main.zig` with `Args.Iterator.initAllocator` (pattern already
   swept through every tool in `d47cf30`).
3. **Platform matrix**: add a per-layer Windows status table to
   `docs/stability.md` with a README pointer; update it as phases land.
4. **`check-api` on Windows**: run the cross-compiled snapshot checker on
   a Windows runner (one CI step) and fix any drift. Expected: transport
   signatures rendering `Socket.Handle` at platform width — same recipe as
   the `OwnerThreadId` wrapper, or normalize handle renderings in
   `tools/api_snapshot.zig`. Done when `zig build check-api` passes
   natively on Windows.

## Phase 1 — transport parity (~2–4 days; the core engineering)

**Architecture spike first (≤0.5 day).** `std.posix.poll` is a compile
error on Windows that says "use `std.Io` instead" — upstream's position is
that the Io interface is the portable event layer, and this project is
already `std.Io`-polymorphic everywhere else. Two candidate designs:

- **(A, preferred) Port the connection wait loop to `std.Io` primitives**:
  race the socket read against a clock sleep (tick) and a wake signal
  using the Io concurrency facilities available on current master. Pays
  off three ways: Windows support, Evented-backend compatibility, and
  removal of the raw-posix special case entirely.
- **(B, fallback) Windows-only branch**: direct `ws2_32.WSAPoll` extern
  plus a loopback-socket wake pair (pipes are not WSAPoll-able; a
  connected localhost pair is). Self-contained in `connection.zig`;
  leaves the POSIX path untouched.

The spike evaluates (A) against the actual `std.Io` surface on the pinned
master snapshot and picks; if Io's primitives are not ready, ship (B) and
leave (A) as the follow-up tracked here.

Work items:

1. Wait/wake/tick loop per the spike decision (`connection.zig` `run`,
   `enableWake`, `pollRetryIntr`).
2. `TCP_NODELAY` on Windows (`runtime.zig` `enableTcpNoDelay`).
3. **Portable test socket pair**: replace the `socketpair(2)` test helper
   with a loopback accept/connect pair in a shared test util, then remove
   the Windows skips in `rpc_tick_idle_test`, `rpc_connection_failure_test`,
   `stream_transport` and `runtime` inline tests, and any raw-frame
   security tests that skip.
4. **Soak on Windows** (depends on ticks): a 2-second smoke in the Windows
   Test job; a 60-second lane in nightly.
5. Deadline/idle/backpressure semantics proven identical by running the
   same suites — no Windows-specific assertions.

Acceptance: the Windows Test job runs the tick/idle/wake suites with zero
skips, and soak passes on a Windows runner.

## Phase 2 — interop e2e on Windows (~1–2 days)

1. **Self-interop e2e (no docker)**: a runner mode (`--self-only` or a
   slim `zig build e2e-self`) that drives the existing zig e2e client
   against the zig e2e server over loopback for all four schemas with TAP
   accounting. Runs on all three OS CI jobs — this is the real winsock
   end-to-end exercise. Requires making the runner's own socket helpers
   portable or hosting the self-mode in a separate slim harness.
2. **Cross-implementation e2e stays on ubuntu CI** (hosted Windows runners
   cannot run Linux containers). Windows developers get the full matrix
   locally via Docker Desktop/WSL2 — documented in CONTRIBUTING.
3. Non-goal, recorded: a WSL2-based cross-impl lane on hosted Windows
   runners (nested virtualization is unavailable there).

## Phase 3 — development experience (~1 day)

1. **Justfile on Windows**: either `set windows-shell` to a working shell
   or document Git Bash as the supported shell; audit recipes for
   POSIX-isms (e.g. `${VAR:-default}` in `tests/e2e/zig/Justfile`).
   Acceptance: `just test`, `just fmt-check`, `just hardening`,
   `just check` work on a stock setup.
2. **Portable bench clocks**: `readMonotonicNs` via the Io clock (`io` is
   already threaded through the bench arg parsing), so benches run on
   Windows dev machines. The `bench-check` CI gate stays ubuntu-only
   (baselines are machine-relative).
3. **CONTRIBUTING Windows quickstart**: zig master install, `just`,
   `capnp.exe`, optional Docker Desktop for cross-impl e2e, a Defender
   exclusion for `.zig-cache` (build speed), long-path note.
4. ~~**Branch protection**: mark the Windows Test/ReleaseSafe/Hardening jobs
   and the self-interop e2e as required checks.~~ **Dropped** — see the
   Phase-1 resolution above: the direct-push-to-`main` workflow deliberately
   has no required branch-protection checks; parity is satisfied by the
   Windows jobs running on every push with the same standing as their
   macOS/Linux peers.

## Phase 4 — tracked upstream dependencies (no local work)

- **Coverage-guided fuzzing**: zig's fuzzer runtime supports ELF/Mach-O
  only; deterministic fuzz smoke already runs on Windows. Revisit when
  upstream lands COFF support.
- **Evented io backend**: fiber-gated upstream; `EventedBackendUnsupported`
  is the documented contract and `std.Io.Threaded` is the Windows backend.
- **QUIC**: the dependency migration and Windows compilation blockers are
  resolved locally. Targeted native Debug + ReleaseSafe transport evidence is
  in the three-OS CI matrix and rejects skips/vacuous execution; the four-root
  tree cross-compiles for Windows. Native Windows execution is still pending a
  hosted run of the eventual capnp-zig commit, and the broader
  full-repository QUIC-root gate remains Linux-only.
- **aarch64-windows**: add to the cross-compile matrix after x86_64 parity
  lands (cheap insurance, no runtime lane).

## Risks

- `std.Io`'s Windows net layer is young on 0.17-dev; the self-interop e2e
  is designed to flush out winsock corner cases early. Budget for an
  upstream report or two.
- Every zig master bump re-rolls Windows std behavior; the pinned CI
  version plus `check-test-compile` contains the blast radius.
- Hosted Windows runners are ~2× slower than Linux; keep heavy lanes
  (long soak) nightly-only and matrices `fail-fast: false`.

## Order and estimates

Phase 0 (0.5d) → Phase 1 spike (0.5d) → Phase 1 (2–4d) → Phase 2 (1–2d) →
Phase 3 (1d). Roughly 5–8 focused days total; every phase is independently
shippable behind the existing gates.
