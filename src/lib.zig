/// Pure Zig implementation of Cap'n Proto serialization, code generation, and RPC.
///
/// This library provides a complete Cap'n Proto stack: wire-format message
/// encoding/decoding, a compiler plugin for generating idiomatic Zig types
/// from `.capnp` schemas, and an in-progress RPC runtime.
/// Core wire-format primitives: segment management, pointer encoding/decoding,
/// struct/list/text serialization, and packed encoding.
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

/// Cap'n Proto RPC runtime: connection management, capability tables,
/// message framing, and peer protocol implementation. The default build keeps
/// the quic-zig-backed QUIC transport disabled; pass `-Dquic=true` to select
/// `src/lib_quic.zig` and expose the native QUIC transport as `rpc.quic`.
pub const rpc = @import("rpc/mod.zig");

/// Switchable `std.Io` backend selection. Centralises the choice between
/// `std.Io.Threaded` and the reserved `std.Io.Evented` selector so
/// applications can flip backends in one place once Evented is enabled here.
pub const io_backend = @import("io_backend.zig");

test {
    @import("std").testing.refAllDecls(@This());
}
