/// Pure Zig implementation of Cap'n Proto serialization, code generation, and RPC.
///
/// This QUIC-enabled module includes the optional quic-zig-backed QUIC transport.
/// `build.zig` selects this root only when `-Dquic=true`; serialization and TCP
/// RPC users should use the default root so quic-zig/BoringSSL stay out of their
/// dependency graph.
pub const message = @import("serialization/message.zig");

/// Cap'n Proto schema type definitions (Node, Field, Type, Value) used by
/// the code generator and request reader.
pub const schema = @import("serialization/schema.zig");

/// Convenience re-exports for generated reader types.
pub const reader = @import("serialization/reader.zig");

/// Code generation driver that produces idiomatic Zig Reader/Builder types
/// from Cap'n Proto schema nodes.
pub const codegen = @import("capnpc-zig/generator.zig");

/// Parses a `CodeGeneratorRequest` from the Cap'n Proto compiler plugin
/// protocol (stdin wire format).
pub const request = @import("serialization/request_reader.zig");

/// Validates and canonicalizes Cap'n Proto schema graphs.
pub const schema_validation = @import("serialization/schema_validation.zig");

/// Spec-faithful, schema-FREE canonicalization (Experimental). See `lib.zig`.
pub const canonical = @import("serialization/canonical.zig");

/// Cap'n Proto RPC runtime with TCP plus optional native QUIC transport.
pub const rpc = @import("rpc/mod_quic.zig");

/// Switchable `std.Io` backend selection. Centralises the choice between
/// `std.Io.Threaded`, `std.Io.Evented` when Zig exposes it for the target, and
/// the process-provided default backend.
pub const io_backend = @import("io_backend.zig");

test {
    @import("std").testing.refAllDecls(@This());
}

// Same parity contract as `lib_core.zig`, against the third library root.
// `build.zig` selects THIS root under `-Dquic=true`, so a serialization module
// added to `lib.zig` alone silently disappears for every QUIC build — which is
// exactly what happened to `canonical` in v0.8.0 and was not caught because
// CI's QUIC job runs `test-rpc-quic`, not `-Dquic=true test`.
//
// `rpc` and `io_backend` are compared loosely: this root deliberately swaps in
// `rpc/mod_quic.zig`, and both roots do export `io_backend`.
test "quic root exposes every serialization export the default root does" {
    const std = @import("std");
    const full = @import("lib.zig");
    inline for (@typeInfo(full).@"struct".decl_names) |name| {
        if (comptime std.mem.eql(u8, name, "rpc")) continue;
        if (!@hasDecl(@This(), name)) {
            @compileError("lib.zig exports '" ++ name ++
                "' but src/lib_quic.zig does not. Add it there too, or exempt " ++
                "it here with a reason.");
        }
    }
}
