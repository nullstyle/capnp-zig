// Core library exports without the POSIX transport/runtime surface.
// Used by the wasm32-freestanding build target and test fixture tooling.
// See lib.zig for the full export surface including RPC transport.
pub const message = @import("serialization/message.zig");
pub const schema = @import("serialization/schema.zig");
pub const reader = @import("serialization/reader.zig");
pub const codegen = @import("capnpc-zig/generator.zig");
pub const request = @import("serialization/request_reader.zig");
pub const schema_validation = @import("serialization/schema_validation.zig");
pub const canonical = @import("serialization/canonical.zig");
pub const rpc = @import("rpc/mod_core.zig");

test {
    @import("std").testing.refAllDecls(@This());
}

// Every serialization declaration `lib.zig` exports must also be reachable
// from this core module.
//
// `capnpc-zig-core` is what the docs tell a serialization-only consumer to
// import, precisely so RPC and transport stay out of their build graph. A
// serialization module added to `lib.zig` alone is therefore invisible to the
// audience most likely to want it — which is exactly what happened when
// `canonical` shipped in v0.8.0: the module reached `lib.zig`, not this file,
// and no gate could see the difference because both files compile fine on
// their own. A clean-room consumer build caught it, after the tag.
//
// `rpc` is deliberately excluded from the comparison: the two modules expose
// deliberately different RPC surfaces (`rpc/mod_core.zig` vs `rpc/mod.zig`),
// which is the entire reason this split exists.
test "core exposes every serialization export the full library does" {
    const std = @import("std");
    const full = @import("lib.zig");
    inline for (@typeInfo(full).@"struct".decl_names) |name| {
        if (comptime std.mem.eql(u8, name, "rpc")) continue;
        if (comptime std.mem.eql(u8, name, "io_backend")) continue;
        if (!@hasDecl(@This(), name)) {
            @compileError("lib.zig exports '" ++ name ++
                "' but src/lib_core.zig does not. Add it to lib_core.zig, or " ++
                "exempt it here with a reason if it genuinely requires the " ++
                "POSIX transport/runtime surface.");
        }
    }
}

// The `rpc` exemption above is about VALUES, not NAMES: this root deliberately
// swaps in a different rpc surface, but it must still expose the same set of
// submodules. Exempting `rpc` wholesale hid `vat` missing from
// `src/rpc/mod_core.zig` -- the third module-root divergence found in two days,
// after the same class had already been fixed at the library-root level twice.
// Compare the name sets so the coarse exemption cannot hide another one.
test "core rpc surface exposes every submodule the full rpc surface does" {
    const full_rpc = @import("rpc/mod.zig");
    const this_rpc = @import("rpc/mod_core.zig");
    inline for (@typeInfo(full_rpc).@"struct".decl_names) |name| {
        if (!@hasDecl(this_rpc, name)) {
            @compileError("src/rpc/mod.zig exports '" ++ name ++
                "' but src/rpc/mod_core.zig does not. Add it there, or exempt it here " ++
                "with a reason if it genuinely requires a transport/runtime.");
        }
    }
}
