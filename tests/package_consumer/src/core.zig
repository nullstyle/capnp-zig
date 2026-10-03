const capnpc = @import("capnpc-zig");
const common = @import("common.zig");

comptime {
    _ = capnpc.canonical;
    _ = capnpc.codegen.Generator;
}

// Core-root surface: prollytree-zig (src/format/capnp.zig, through
// `capnpc-zig-core`) uses only the shared serialization set that common.zig
// forces for every root: MessageBuilder.init/allocateStruct, the
// StructBuilder/StructReader accessors, canonical.canonicalizeFlatFromBuilder
// and Message.initFlat. Each of those lines names prollytree. Add a line here
// when a core-root consumer starts using an API outside that set.

pub fn main() !void {
    try common.exerciseSerialization();
}
