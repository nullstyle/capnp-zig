# Windows as a First-Class Target and Development OS

Status: Phases 0–1 landed; Phase 2 next. Owner: rotating per session;
this document is the source of truth for progress.

Progress notes:

- Phase 1 (2026-06-12): the spike confirmed `std.Io`'s portable
  read-with-timeout path is blocked upstream (Windows `Threaded` batch
  net ops are blocking-only, "TODO integrate with overlapped I/O"), so
  the fallback design shipped: the connection wait loop uses `WSAPoll`
  on Windows (locally declared; std has no binding) with the same
  shape as the POSIX `poll(2)` path, the wake channel is a loopback TCP
  pair where POSIX uses `socketpair(2)`, wake writes/drains go through
  the io vtable on all platforms, and `TCP_NODELAY` is set via a local
  `ws2_32.setsockopt` declaration. Public handle-taking transport entry
  points now take the platform-stable `SocketFd` wrapper (fixes the
  `check-api` width drift; `check-api` now runs on all three Test
  matrix OSes). The socket-pair test helpers are portable
  (`createLoopbackSocketPair` via `std.Io`), the tick/idle,
  connection-failure, and raw-frame suites run on Windows with zero
  skips, and the soak harness runs as a 2s smoke in every Test job
  plus a Windows nightly lane. Porting found one real bug class: the
  old POSIX test helpers silently swallowed `EBADF` double-closes that
  `std.Io.Threaded` correctly treats as use-after-free.
  The `std.Io`-native loop remains the long-term direction once
  upstream lands overlapped net I/O for the Windows Threaded backend.

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

Known divergences (the gap this plan closes):

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
5. **Gate parity**: the Windows jobs are required checks alongside their
   macOS/Linux peers.

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
4. **Branch protection**: mark the Windows Test/ReleaseSafe/Hardening jobs
   and the self-interop e2e as required checks.

## Phase 4 — tracked upstream dependencies (no local work)

- **Coverage-guided fuzzing**: zig's fuzzer runtime supports ELF/Mach-O
  only; deterministic fuzz smoke already runs on Windows. Revisit when
  upstream lands COFF support.
- **Evented io backend**: fiber-gated upstream; `EventedBackendUnsupported`
  is the documented contract and `std.Io.Threaded` is the Windows backend.
- **QUIC**: blocked on the quic-zig master migration globally; when
  repinned, add Windows to the QUIC matrix and verify the TLS dependency
  builds there.
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
