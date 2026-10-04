const std = @import("std");
const capnpc = @import("capnpc-zig");
const common = @import("common.zig");

comptime {
    _ = capnpc.canonical;
    _ = capnpc.codegen.Generator;
}

// The core root has no sockets: `rpc.transport.unix` (listen, connect, fd_io)
// is empty there, like `rpc.transport.tcp`.
comptime {
    if (std.meta.declarations(capnpc.rpc.transport.unix).len != 0) {
        @compileError("the core root exports rpc.transport.unix, but it has no sockets");
    }
}

// Core-root surface: prollytree-zig (src/format/capnp.zig, through
// `capnpc-zig-core`) uses only APIs that common.zig forces for every root:
// MessageBuilder.init/allocateStruct, the StructBuilder/StructReader
// accessors, the U32 list values that readU32List/writeU32List return
// (len/get/set), canonical.canonicalizeFlatFromBuilder and Message.initFlat.
// Each of those lines names prollytree. Add a line here when a core-root
// consumer starts using an API outside that set.

pub fn main() !void {
    try common.exerciseSerialization();
}
