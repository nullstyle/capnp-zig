//! Root of the native consumer's static library (docs/native-abi.md): what a
//! C or Swift host's own library root needs to ship capnp-zig's C ABI.

const std = @import("std");
const capnp = @import("capnpc-zig-core");

// Referencing `native.abi` is what emits the `capnp_*` symbols.
comptime {
    _ = capnp.native.abi;
}

/// The allocator behind every connection.
pub const capnp_core_allocator: std.mem.Allocator = std.heap.c_allocator;

/// What `capnp_core_version()` reports: your version and the capnp-zig
/// package you pin.
pub const capnp_core_version_string: [:0]const u8 = "core 0.0.0 / capnp-zig package-preflight / native-consumer";
