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

/// Cap'n Proto RPC runtime: capability-based messaging over TCP using libxev.
pub const rpc = @import("rpc/mod.zig");

/// Re-export xev so downstream consumers share the same module instance.
pub const xev = @import("xev").Dynamic;

test {
    @import("std").testing.refAllDecls(@This());
    _ = @import("rpc/level2/connection.zig");
    _ = @import("rpc/level2/transport_xev.zig");
}
