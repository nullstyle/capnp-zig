# HANDOFF — zig fork change branch: Io/Dispatch non-exhaustive Operation switch

Paste this into a session working on the nullstyle zig fork. Self-contained.
Second item for the fork's change-branch list (the first is
`handoff-zig-fork-netacceptwindows.md`). Suggested branch:
`fix/dispatch-operation-switch`.

## The defect

At 0.17.0-dev.1786+75044cb04, `std.Io.Evented` DOES NOT COMPILE ON macOS.

`lib/std/Io/Dispatch.zig` (the Darwin libdispatch backend) switches over
`Io.Operation` in `batchAwaitConcurrent`:

```
lib/std/Io/Dispatch.zig:2067:38: error: switch must handle all possibilities
```

reached via `Dispatch.batchAwaitConcurrent` (`:1954`) from `Dispatch.io`
(`:370`).

Cause: the `Io.Operation` union gained variants — `net_send` and
`net_write` — as part of moving socket I/O off the `Io.VTable`
(`netWrite`/`netRead` entries deleted, `net_write`/`net_read` operations
added). `Io/Threaded.zig` was updated for the widened union; the
libdispatch backend's switch was not. Since the switch is not `else`-terminated,
it is a compile error rather than a runtime fallthrough.

Comparison at the two snapshots:

| | dev.1683+5ceec001b | dev.1786+75044cb04 |
|---|---|---|
| `Io.VTable.netWrite` / `netRead` | present | **deleted** |
| `Operation` net variants | `net_receive`, `net_read` | `net_receive`, `net_send`, `net_read`, `net_write` |
| `std.Io.Evented` on macOS | compiles | **does not compile** |

## Minimal repro (macOS)

Twelve lines, no dependencies — capnp-zig is not involved:

```zig
const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    var ev: std.Io.Evented = undefined;
    try ev.init(gpa.allocator(), .{});
    defer ev.deinit();
    const io = ev.io();
    std.Io.sleep(io, std.Io.Duration.fromMilliseconds(1), .awake) catch {};
}
```

```
$ zig build-exe dispatch_probe.zig
lib/std/Io/Dispatch.zig:2067:38: error: switch must handle all possibilities
```

Note the analysis is lazy: merely naming `std.Io.Dispatch` compiles fine.
The error needs the batch path instantiated, which any real Evented use
does.

## The fix

Handle the new variants in the `batchAwaitConcurrent` switch (and audit
the file for sibling switches over `Io.Operation` — check whether the
non-batch submit path has the same gap).

Two shapes, pick per the backend's design:

1. **Implement them** — map `net_write`/`net_send` onto libdispatch
   write sources, mirroring how `net_read`/`net_receive` are handled a
   few arms below (preferred: it is what `Threaded` does, and leaving
   them unimplemented silently degrades Evented on Darwin).
2. **Fall back explicitly** — route unhandled operations to the blocking
   path the backend already uses for operations it cannot accelerate, so
   the union can widen again without breaking the build.

Whichever is chosen, prefer a terminal `else =>` arm with an explicit
fallback over an exhaustive list, so the NEXT variant addition is a
behavior question rather than a build break for every Darwin user.

## Verification

1. The repro above compiles and runs.
2. `zig build test` for `std.Io` on macOS.
3. Integration: capnp-zig (`/Users/nullstyle/prj/zig/capnp-zig`) builds
   `zig build check` clean against the patched fork on macOS. Today, at
   stock dev.1786, that command reports exactly this one std error and
   nothing else — capnp-zig's own sources are already clean at 1786 — so
   it is a precise end-to-end check. Also `zig build -Dio-backend=evented check`,
   which exists in that repo specifically to gate the Evented selector.

## Bookkeeping

Record in the fork's change-branch list: "libdispatch backend must handle
the widened Io.Operation union (net_write/net_send) — Evented is
uncompilable on macOS without it."
