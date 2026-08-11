const capnpc = @import("capnpc-zig");
const common = @import("common.zig");

comptime {
    _ = capnpc.canonical;
    _ = capnpc.codegen.Generator;
}

pub fn main() !void {
    try common.exerciseSerialization();
}
