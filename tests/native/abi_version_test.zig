//! `zig build test-native-abi`: the library root's
//! `capnp_core_version_string` (tests/native/abi_lib_root.zig) reaches
//! `capnp_core_version()` through the header.
//!
//! Kept out of src/native/abi_test.zig on purpose: an embedder can link that
//! file against its own library root (capnp-swift's `apple_root.zig`), whose
//! version string differs.

const std = @import("std");
const c = @import("capnp_core_h");

test "version: the library root's version string reaches capnp_core_version" {
    try std.testing.expectEqualStrings(
        "core 0.0.0-test / capnp-zig test / native-abi-test",
        std.mem.span(c.capnp_core_version()),
    );
}
