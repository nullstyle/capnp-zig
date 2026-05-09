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

/// Cap'n Proto RPC runtime with TCP plus optional native QUIC transport.
pub const rpc = @import("rpc/mod_quic.zig");

/// Switchable `std.Io` backend selection. Centralises the choice between
/// `std.Io.Threaded` and the reserved `std.Io.Evented` selector so
/// applications can flip backends in one place once Evented is enabled here.
pub const io_backend = @import("io_backend.zig");

test {
    @import("std").testing.refAllDecls(@This());
}
