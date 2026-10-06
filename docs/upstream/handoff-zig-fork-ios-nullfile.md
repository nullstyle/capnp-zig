# HANDOFF — zig fork change branch: `Io.Threaded` does not compile for iOS

- **To:** the nullstyle Zig fork (plan §3 row H2). Same file name in both repos: the owner copies this file from capnp-swift `docs/handoffs/` to capnp-zig `docs/upstream/handoff-zig-fork-ios-nullfile.md`, next to the other Zig-fork handoffs.
- **From:** capnp-swift
- **Date:** 2026-10-06
- **Status:** DRAFT (the owner sends it)
- **Needed by:** M5 (iOS). capnp-swift does not block on it: the root workaround below is enough for its own static library.

Paste this into a session working on the nullstyle zig fork. It is
self-contained. It joins the fork's change-branch list (see the other
`handoff-zig-fork-*.md` files in capnp-zig `docs/upstream/`). Suggested
branch: `fix/ios-threaded-replace`.

This is a document, not an upstream issue. Nothing here was filed upstream.

## The defect

At tagged **0.17.0**, any reference to `std.Io.Threaded.io()` fails to
compile for iOS, tvOS, watchOS and visionOS. Line numbers are 0.17.0's.

- `lib/std/Io/Threaded.zig:345-370`: `NullFile` has no `fd` field for
  `.wasi, .ios, .tvos, .visionos, .watchos` (`:356-360`).
- `getDevNullFd` (`:15482`) reads `t.null_file.fd` (`:15486`, again at
  `:15498-15502`).
- `processReplace` (`:15192`) checks only `process.can_replace` (`:15195`).
  That is `true` for the iOS family (`lib/std/process.zig:256-259`). With
  `is_darwin` (`Threaded.zig:6`, true for the iOS family) it calls
  `spawnDarwin` (`:15217`), and `spawnDarwin` calls `getDevNullFd` (`:17468`).
- `Threaded.io()` puts `processReplace` in its `Io.VTable` literal (`:1905`),
  so naming `io()` analyzes it.

`processSpawn` is already right: `:15261-15266` maps the iOS family to
`processSpawnUnsupported`, and `process.can_spawn` is false there
(`process.zig:262-265`). Only the replace path was missed.

Two common ways to reach it:

1. **The default panic handler.** In Debug and ReleaseSafe any safety check
   (an integer overflow is enough) calls `defaultPanic`, which calls
   `std.Options.debug_io.vtable.crashHandler` (`lib/std/debug.zig:569`).
   The default `debug_io` is `debug_threaded_io.?.io()` (`lib/std/std.zig:228`).
   Other readers of `debug_io`: the default `logFn` (`log.zig:102`),
   `std.debug.print` and the stack-trace dumpers (`debug.zig:299-927`),
   `posix.unexpectedErrno` under `unexpected_error_tracing` (on by default in
   Debug; `posix.zig:1671-1673`), and `Thread.zig:181`.
2. **Code that wants a std `Io`.** For example capnp-zig v0.20.0
   `src/rpc/transport/unix/fd_closer.zig:284-286` returns
   `std.Io.Threaded.global_single_threaded.io()`. This path fails in every
   optimize mode, and root overrides cannot help it (capnp-zig H1 removes it
   from iOS builds).

The fork's `master` and `upstream/master` (both `5e36170b5f`, 2026-09-07,
checked read-only on 2026-10-06) still have the same three pieces:
`can_replace` unchanged, the iOS-family `NullFile` without `fd`, and
`getDevNullFd` reading it. I did not build them.

## Minimal repro

No dependencies; capnp-zig is not involved.

`add.zig`:

```zig
export fn add(a: u32, b: u32) u32 {
    return a + b;
}
```

`syncio.zig`:

```zig
const std = @import("std");
var mu: std.Io.Mutex = .init;
export fn lockit() void {
    const io = std.Io.Threaded.global_single_threaded.io();
    mu.lockUncancelable(io);
    mu.unlock(io);
}
```

```sh
zig build-lib add.zig    -target aarch64-ios -O Debug       -freference-trace=12
zig build-lib syncio.zig -target aarch64-ios -O ReleaseFast -freference-trace=12
```

**Expected:** two static libraries. **Actual** (2026-10-06, tagged 0.17.0,
macOS 27 arm64, exit 1; paths shortened):

```
lib/std/Io/Threaded.zig:15486:25: error: no field named 'fd' in struct 'Io.Threaded.NullFile__struct_13'
        if (t.null_file.fd != -1) return t.null_file.fd;
                        ^~
lib/std/Io/Threaded.zig:357:48: note: struct declared here
    .wasi, .ios, .tvos, .visionos, .watchos => struct {
                                               ^~~~~~
referenced by:
    spawnDarwin: lib/std/Io/Threaded.zig:17468:59
    processReplace: lib/std/Io/Threaded.zig:15217:37
    io [inlined]: lib/std/Io/Threaded.zig:1905:14
    debug_io: lib/std/std.zig:228:127
    defaultPanic: lib/std/debug.zig:569:32
    integerOverflow: lib/std/debug.zig:164:17
    add: add.zig:6:14
```

```
lib/std/Io/Threaded.zig:15486:25: error: no field named 'fd' in struct 'Io.Threaded.NullFile__struct_10'
referenced by:
    spawnDarwin: lib/std/Io/Threaded.zig:17468:59
    processReplace: lib/std/Io/Threaded.zig:15217:37
    io: lib/std/Io/Threaded.zig:1905:14
    lockit: syncio.zig:6:57
```

The full matrix for `add.zig`:

| Target | Debug | ReleaseSafe | ReleaseFast |
|---|---|---|---|
| `aarch64-ios`, `aarch64-ios-simulator`, `x86_64-ios-simulator` | FAIL | FAIL | ok |
| `aarch64-tvos`, `aarch64-watchos`, `aarch64-visionos` | FAIL | FAIL | ok |
| `aarch64-maccatalyst`, `aarch64-macos` | ok | ok | ok |

`syncio.zig` fails for `aarch64-ios` in Debug, ReleaseSafe, ReleaseFast and
ReleaseSmall.

## The workaround (what capnp-swift ships until the fork fix)

Declare these in the root module (capnp-swift `core/src/apple_root.zig`):

```zig
pub const std_options_debug_io: std.Io = std.Io.failing;
pub const panic = std.debug.FullPanic(trapPanic); // trapPanic calls @trap()
pub const std_options: std.Options = .{ .logFn = noopLog };
```

- `std_options_debug_io = std.Io.failing` is the one that matters: it covers
  every `debug_io` reader above. Alone, it is enough for `add.zig` (Debug
  and ReleaseSafe). For the capnp-zig core, the panic override plus the
  no-op `logFn` without it still failed in Debug, through
  `posix.unexpectedErrno` → `dumpCurrentStackTrace` (an earlier capnp-swift
  probe; its fact-check record is claim 10 in the planning `claims.json`).
- The panic override alone is enough for `add.zig`, because a panic is its
  only path. It also shrinks output: a 3-function ReleaseSafe probe was
  2,369,600 bytes with the default handler and 20,384 bytes with the
  override (macOS).
- Nothing in the root helps code that names `Threaded.io()` itself
  (`syncio.zig`, or capnp-zig's fd closer before H1).

## The fix

**Fix A (recommended), one line in `lib/std/process.zig:256-259`:**

```diff
 pub const can_replace = switch (native_os) {
-    .windows, .haiku, .wasi => false,
+    .windows, .haiku, .wasi, .ios, .tvos, .visionos, .watchos => false,
     else => true,
 };
```

`processReplace` then returns `error.OperationUnsupported` before it reaches
`spawnDarwin`. This matches `can_spawn` and the `NullFile` split, which
already treat these four as targets with no process control.

**Fix B (alternative), in `lib/std/Io/Threaded.zig:356`:** give the iOS
family the POSIX `NullFile` (move `.ios, .tvos, .visionos, .watchos` to the
`else` arm, keep `.wasi`). This compiles too, but then `processReplace` on
iOS tries `posix_spawn` with `POSIX_SPAWN_SETEXEC` at run time. I assume
(not checked here) that a sandboxed iOS app cannot exec, so A states the
truth at compile time.

I did not look at the other `Io` backends (`Dispatch`, `Kqueue`, `Uring`)
for the same pattern.

## Verification

All on a scratch copy of the 0.17.0 `lib/`, passed with `--zig-lib-dir` (or
`ZIG_LIB_DIR`), 2026-10-06, macOS 27 arm64:

1. The repro. With fix A, and separately with fix B, both `add.zig` and
   `syncio.zig` build for `aarch64-ios`, `aarch64-ios-simulator`,
   `aarch64-tvos`, `aarch64-watchos`, `aarch64-visionos` and `aarch64-macos`,
   in Debug and ReleaseFast (24 of 24 for each fix).
2. A std test that needs no device and no SDK, because it only analyzes:

   ```zig
   const std = @import("std");

   test "std.Io.Threaded.io compiles for this target" {
       // Naming `io()` analyzes its VTable literal, processReplace included.
       _ = &std.Io.Threaded.io;
   }
   ```

   ```sh
   zig test threaded_io_test.zig -target aarch64-ios -fno-emit-bin
   ```

   Ablation: at stock 0.17.0 (the defect present) it FAILS with
   `Threaded.zig:15486:25` for `aarch64-ios`, `aarch64-ios-simulator`,
   `aarch64-tvos`, `aarch64-watchos` and `aarch64-visionos`, and passes for
   `aarch64-macos`. With fix A it passes for all six. Run it for every
   iOS-family target in the fork's std test matrix. The defect shipped
   because nothing in a default std test run analyzes `Threaded.io` for
   these targets.
3. Downstream check: capnp-zig with H1 applied and fix A in place,
   `ZIG_LIB_DIR=<patched lib> zig build check-compile -Dtarget=aarch64-ios`
   has no compile errors left. Only link errors remain (1005
   `undefined symbol` errors across 11 executables and test binaries), because
   there is no iOS SDK. Before fix A, all 11 failed with the `15486` error.

## Bookkeeping

Record in the fork's change-branch list: "`process.can_replace` is false
for the iOS family, like `can_spawn`; at 0.17.0 `Threaded.io()` does not
compile for iOS/tvOS/watchOS/visionOS because `processReplace` reaches
`getDevNullFd`, whose `NullFile` has no `fd` there."

Scratch evidence (not in any repo):
`/private/tmp/claude-501/-Users-nullstyle-prj-zig-capnp-zig/d3e1b574-cf0b-4f76-a01c-9b84d5bc10a3/scratchpad/capnp-swift/h2-repro/`
(`add.zig`, `syncio.zig`, `add_dbgio.zig`, `add_panic.zig`,
`threaded_io_test.zig`, `libA/` = fix A, `libB/` = fix B, `out/` = every log).
