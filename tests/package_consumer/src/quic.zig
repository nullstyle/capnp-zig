const capnpc = @import("capnpc-zig");
const common = @import("common.zig");

comptime {
    if (!capnpc.rpc.transport.quic.enabled) {
        @compileError("the QUIC consumer resolved the non-QUIC package root");
    }
    _ = capnpc.rpc.transport.quic.Connection;
    _ = capnpc.canonical;
}

pub fn main() !void {
    try common.exerciseSerialization();
}
