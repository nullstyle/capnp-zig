const std = @import("std");
const capnpc = @import("capnpc-zig");

pub fn main(init: std.process.Init) !void {
    var backend = try capnpc.io_backend.Backend.init(.process_init, init.gpa, init.io);
    defer backend.deinit();
    const io = backend.io();

    // Every RPC entry point takes `io`. Port 0 asks the OS for a free port.
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try capnpc.rpc.transport.tcp.Listener.init(init.gpa, io, address, .{});
    defer listener.close();
}
