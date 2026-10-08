//! Root of the host static library `zig build test-native-abi` links
//! `src/native/abi_test.zig` against: what a C or Swift host's own library
//! root does to ship the native C ABI (docs/native-abi.md).
//!
//! It references `native.abi` so the `capnp_*` exports land in the archive,
//! and supplies the two root declarations abi.zig reads. The allocator is a
//! `DebugAllocator`, not the C allocator: this library links no libc, so it
//! builds for every CI cross target (`check-test-compile`).

const std = @import("std");
const core = @import("capnpc-zig-core");

comptime {
    _ = core.native.abi;
}

/// The Peer logs every refused frame at debug or warn level, and these tests
/// provoke them; a host library writes nothing to its app's stderr.
pub const std_options: std.Options = .{ .log_level = .err };

var debug_allocator: std.heap.DebugAllocator(.{}) = .init;

/// The allocator behind every connection (read by abi.zig).
pub const capnp_core_allocator: std.mem.Allocator = debug_allocator.allocator();

/// What `capnp_core_version()` reports (read by abi.zig). An embedder puts
/// its own version and the capnp-zig package it pins here; abi_test.zig
/// checks this exact string.
pub const capnp_core_version_string: [:0]const u8 = "core 0.0.0-test / capnp-zig test / native-abi-test";
