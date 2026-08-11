const capnpc = @import("capnpc-zig");
const common = @import("common.zig");

comptime {
    _ = capnpc.canonical;
    _ = capnpc.rpc.peer.Peer;
}

pub fn main() !void {
    try common.exerciseSerialization();
}
