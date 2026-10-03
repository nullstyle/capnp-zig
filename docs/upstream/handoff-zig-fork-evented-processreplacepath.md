# HANDOFF — zig fork change branch: evented backends set dropped `Io.VTable` fields

Paste this into a session working on the nullstyle zig fork. Self-contained.
It joins the fork's change-branch list (see the other
`handoff-zig-fork-*.md` files beside this one). Suggested branch:
`fix/evented-vtable-0.17`. It touches the same files as the fork's
`feat/evented-dispatch` work, so it may be simplest to fold it into that
branch's rebase onto 0.17.0.

## The defect

At tagged **0.17.0**, no `std.Io.Evented` compiles. `std.Io.Evented` is
`std.Io.Uring` on Linux and `std.Io.Dispatch` on Darwin (`lib/std/Io.zig:31`),
and both backends build their `Io.VTable` literal in `io()` with entries the
VTable no longer has:

| File (0.17.0) | Line | Entry | Problem |
|---|---|---|---|
| `lib/std/Io/Uring.zig` | 759 | `.processReplacePath = processReplacePath` | field dropped from `Io.VTable` |
| `lib/std/Io/Uring.zig` | 761 | `.processSpawnPath = processSpawnPath` | field dropped from `Io.VTable` |
| `lib/std/Io/Dispatch.zig` | 439 | `.processReplacePath = processReplacePath` | field dropped from `Io.VTable` |
| `lib/std/Io/Dispatch.zig` | 441 | `.processSpawnPath = processSpawnPath` | field dropped from `Io.VTable` |
| both | (unset) | `inheritParentDir`, `inheritParentFile` | new `Io.VTable` fields (`lib/std/Io.zig:221-222`) with no entry |

The compiler reports only the first problem. After removing
`processReplacePath` it reports `processSpawnPath`, and so on, so fix all four
in one pass. A script that diffs the `Io.VTable` field list against each
backend's `io()` literal finds exactly these four, in both files.

The path-relative process operations moved into the options: `ReplaceOptions`
and `SpawnOptions` now carry an `exe` union (`.detect`, `.search`,
`.path: Dir`, `.file: File`, `.explicit`). The backends' own
`processReplacePath` (Uring.zig:4266, Dispatch.zig:4109) and
`processSpawnPath` (Uring.zig:4312, Dispatch.zig:4153) were
`@panic("TODO ...")` bodies, so dropping them loses no behavior.

The fork's `master` and `feat/evented-dispatch` (checked read-only on
2026-10-03) predate this change: their `Io.VTable` still has
`processReplacePath` and `processSpawnPath`. The defect arrives when the fork
rebases onto 0.17.0.

## Evidence

capnp-zig keeps `evented_available = false` in `src/io_backend.zig` because of
this, and gates it with an expected-fail canary: `zig build
check-evented-canary` compiles `tools/evented_canary.zig` (it names
`std.Io.Evented.io`) and passes only while the compile fails with
`error: no field named 'processReplacePath' in struct 'Io.VTable'`. A fixed
std turns that canary red, which is the signal to flip the flag.

## Minimal repro

No dependencies; capnp-zig is not involved:

```zig
const std = @import("std");

test "std.Io.Evented compiles" {
    // Naming the backend's `io()` forces its VTable literal to be analyzed.
    _ = &std.Io.Evented.io;
}
```

```
$ zig test evented_repro.zig                                  # macOS
lib/std/Io/Dispatch.zig:439:14: error: no field named 'processReplacePath' in struct 'Io.VTable'
            .processReplacePath = processReplacePath,
             ^~~~~~~~~~~~~~~~~~

$ zig test -target x86_64-linux-gnu --test-no-exec evented_repro.zig
lib/std/Io/Uring.zig:759:14: error: no field named 'processReplacePath' in struct 'Io.VTable'
            .processReplacePath = processReplacePath,
             ^~~~~~~~~~~~~~~~~~
```

## The fix

In both `Uring.zig` and `Dispatch.zig`, inside `io()`:

1. Delete `.processReplacePath = processReplacePath,` and
   `.processSpawnPath = processSpawnPath,`, plus the two dead functions.
2. Set the two new entries. The smallest correct change uses the failing
   stubs `Io.zig` already exports:

   ```zig
   .inheritParentDir = Io.failingInheritParentDir,
   .inheritParentFile = Io.failingInheritParentFile,
   ```

   The better change ports `Threaded.zig`'s `inheritParentDir` /
   `inheritParentFile` (Threaded.zig:17854-17870): they only validate the
   handle and wrap it, so nothing in them is thread-pool specific.
3. Check that each backend's `processReplace` / `processSpawn` honors
   `options.exe`. At 0.17.0 they resolve `argv[0]` against `PATH` and ignore
   the `.path` / `.file` / `.explicit` arms. That is a behavior gap, not a
   compile error.

This fix makes Evented compile. It does not make it carry sockets. The net
entries are still stubs (Uring implements only `netBindIp` / `netClose` /
`netShutdown`; Dispatch only `netClose`; `netListenIp`, `netAccept` and
`netConnectIp` are `...Unavailable` in both), and Dispatch's `operate`
answers `net_read` / `net_write` / `net_send` / `net_receive` with
`@panic("TODO ...")`. Those belong to the `feat/evented-dispatch` work, not
this branch.

## Verification

Verified 2026-10-03 on macOS (Darwin 27, aarch64) against a copy of the 0.17.0
`lib/` with only steps 1-2 applied (`--zig-lib-dir` / `ZIG_LIB_DIR`):

1. The repro compiles for the host (Dispatch) and for `x86_64-linux-gnu`
   (Uring).
2. A runtime smoke on macOS passes: `ev.init(gpa, .{})`, `io.sleep(1ms)`, and
   `IpAddress.listen` returns `error.NetworkDown` (the socket stub), not a
   crash:

   ```zig
   const std = @import("std");

   test "std.Io.Evented initializes, sleeps and reports its socket gap" {
       var ev: std.Io.Evented = undefined;
       try ev.init(std.testing.allocator, .{});
       defer ev.deinit();
       const io = ev.io();
       try io.sleep(.fromMilliseconds(1), .awake);
       const addr = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
       if (addr.listen(io, .{})) |srv| {
           var s = srv;
           s.deinit(io);
       } else |err| std.debug.print("evented listen: {t}\n", .{err});
   }
   ```

3. capnp-zig's canary turns red against the patched std
   (`ZIG_LIB_DIR=<patched lib> zig build check-evented-canary`: "should
   contain ... processReplacePath ... but not found"). That is the expected
   signal: capnp-zig then flips `evented_available` and retargets the canary.
4. Add the repro to the std tests for every target that has an Evented
   backend. The defect shipped because nothing in a default std test run
   analyzes `Uring.io` or `Dispatch.io`.

## Bookkeeping

Record in the fork's change-branch list: "evented backends must track
`Io.VTable`: drop `processReplacePath` / `processSpawnPath`, set
`inheritParentDir` / `inheritParentFile`; at 0.17.0 no `std.Io.Evented`
compiles without it."
