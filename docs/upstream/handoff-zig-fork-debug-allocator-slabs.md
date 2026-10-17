# HANDOFF — zig fork change branch: `std.heap.DebugAllocator` keeps emptied bucket slabs, so cyclic alloc/free grows without bound

- **To:** the nullstyle Zig fork (the owner copies this file next to the other `handoff-zig-fork-*.md` files in capnp-zig `docs/upstream/`)
- **From:** capnp-swift (M2 fuzz gate)
- **Date:** 2026-10-06
- **Status:** DRAFT (the owner sends it)
- **Needed by:** nothing blocks on it. capnp-swift's fuzz harness (`core/src/fuzz_abi.zig`) now counts live bytes over `std.heap.page_allocator` instead; its tests keep `std.testing.allocator` (short-lived processes).

This is a document, not an upstream issue. Nothing here was filed upstream.

## The defect

At tagged **0.17.0**, a Debug-mode `std.heap.DebugAllocator(.{})` that allocates and frees small blocks in cycles keeps growing the process, although every allocation is freed and `deinit()` reports `.ok`. Each cycle that reuses a size class maps a fresh 128 KiB slab (`DebugAllocator.alloc` -> `PageAllocator.map(n=131072)`, seen with `lldb` breakpoints on `mmap`), and the slabs of emptied buckets stay mapped.

Observed in capnp-swift's `zig build fuzz-abi -- --seconds 1800` (Debug): with a `DebugAllocator` behind the core's allocator, resident memory grew linearly at about 13 MB/s to 24 GB over 30 minutes while the harness's live-byte counter read 0 after every session (two `Peer`s created and freed per session, roughly 1,500 sessions per second). With `std.heap.page_allocator` in its place the same run stays at 5 MB.

## Minimal repro

No dependencies; capnp-zig is not involved.

`da.zig`:

```zig
const std = @import("std");

pub fn main() !void {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(da.deinit() == .ok);
    const a = da.allocator();
    const sizes = [_]usize{ 16, 32, 64, 128, 1344, 3440 };
    var round: usize = 0;
    while (round < 20_000) : (round += 1) {
        var live: [sizes.len][]u8 = undefined;
        for (sizes, 0..) |size, i| live[i] = try a.alloc(u8, size);
        for (live) |buf| a.free(buf);
    }
}
```

```sh
zig build-exe da.zig -O Debug
/usr/bin/time -l ./da
```

**Expected:** a few MB resident (the same loop over `std.heap.page_allocator` peaks at 3.3 MB). **Actual** (2026-10-06, tagged Zig 0.17.0, macOS 27 arm64, 16 KiB pages): `maximum resident set size 80134144` (80 MB) for 120,000 allocations, and `deinit()` still returns `.ok`. The growth is proportional to the number of cycles.

## Likely cause (read, not patched)

`lib/std/heap/debug_allocator.zig` at 0.17.0: a bucket whose slots are all free is not returned to the backing allocator when the allocation that emptied it is freed (or it is returned but the bucket metadata still forces a fresh slab on the next allocation of that size class). The expected behavior with the default `never_unmap = false` is that an emptied bucket's slab is unmapped, and that a bucket with free slots is reused before a new slab is mapped.

## Suggested test

In `debug_allocator.zig`'s tests: run the loop above for a few thousand rounds with a counting backing allocator (count bytes mapped minus bytes unmapped) and require the count to return to its starting value, and the number of `map` calls to stay bounded by the number of size classes.

## Verification done here

- `page_allocator` alone, same loop and an `ArrayList` grow/free loop: flat (3.3 MB).
- The shrink path (`alloc` 70,000 -> `resize` 20,000 -> `free`): flat (2.5 MB) on 16 KiB pages.
- The fuzz harness with the page-backed counter: 5 MB resident over 30,000 sessions per second, 0 live bytes.

Scratch evidence (not in any repo): `/private/tmp/claude-501/-Users-nullstyle-prj-zig-capnp-swift/08e90148-903c-48bf-adb0-a226a31f27f6/scratchpad/pa-probe/` (`da.zig`, `probe.zig`, `shrink.zig`, `clock.zig`), `lldb-mmapbt.log` (the `mmap` backtraces), `fuzz-1800.log` (the 30-minute run).
