//! Expected-fail canary for `std.Io.Evented` (see build/evented_canary.zig).
//!
//! At the pinned Zig 0.17.0 no evented backend compiles: Uring (Linux) and
//! Dispatch (Darwin) set `Io.VTable.processReplacePath`, a field the VTable
//! dropped (docs/upstream/handoff-zig-fork-evented-processreplacepath.md).
//! `src/io_backend.zig` therefore keeps `evented_available = false`. This file
//! names the backend's `io()` so its VTable literal is analyzed; the
//! `check-evented-canary` step passes only while that analysis fails with the
//! known error.

const std = @import("std");

comptime {
    _ = &std.Io.Evented.io;
}
