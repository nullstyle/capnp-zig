const std = @import("std");
const capnpc = @import("capnpc-zig");

const rpc = capnpc.rpc;

const Calculator = struct {
    pub const Add = struct {
        pub const Params = struct {
            pub const Reader = struct {
                a: i32,
                b: i32,

                pub fn getA(self: Reader) !i32 {
                    return self.a;
                }

                pub fn getB(self: Reader) !i32 {
                    return self.b;
                }
            };
        };

        pub const Results = struct {
            pub const Builder = struct {
                result: i32 = 0,

                pub fn setResult(self: *Builder, value: i32) !void {
                    self.result = value;
                }
            };

            pub const Reader = struct {
                result: i32,

                pub fn getResult(self: Reader) !i32 {
                    return self.result;
                }
            };
        };

        pub const Response = union(enum) {
            results: Results.Reader,
            exception: rpc.wire.protocol.Exception,
            canceled,
        };

        pub const Handler = *const fn (
            ctx: *anyopaque,
            peer: *rpc.peer.Peer,
            params: Params.Reader,
            results: *Results.Builder,
            caps: *const rpc.caps.table.InboundCapTable,
        ) anyerror!void;

        pub const Callback = *const fn (
            ctx: *anyopaque,
            peer: *rpc.peer.Peer,
            response: Response,
            caps: *const rpc.caps.table.InboundCapTable,
        ) anyerror!void;
    };

    pub const Server = struct {
        ctx: *anyopaque,
        vtable: VTable,
    };

    pub const VTable = struct {
        add: Add.Handler,
    };
};

fn handleAdd(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: Calculator.Add.Params.Reader,
    results: *Calculator.Add.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    _ = ctx_ptr;
    const a = try params.getA();
    const b = try params.getB();
    try results.setResult(a + b);
}

fn onAddReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: Calculator.Add.Response,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    _ = ctx_ptr;
    _ = peer;

    switch (response) {
        .results => |results| try std.testing.expectEqual(@as(i32, 42), try results.getResult()),
        .exception => return error.RemoteException,
        else => return error.UnexpectedResponse,
    }
}

test "getting-started RPC handler and callback snippets use the public domain surface" {
    var results = Calculator.Add.Results.Builder{};
    try handleAdd(
        undefined,
        undefined,
        .{ .a = 40, .b = 2 },
        &results,
        undefined,
    );
    try std.testing.expectEqual(@as(i32, 42), results.result);

    try onAddReturn(
        undefined,
        undefined,
        .{ .results = .{ .result = results.result } },
        undefined,
    );
}

test "getting-started RPC transport snippets use the current std.Io surface" {
    comptime {
        _ = rpc.peer.Peer;
        _ = rpc.caps.table.InboundCapTable;
        _ = rpc.wire.protocol.Exception;
        _ = rpc.transport.tcp.Listener;
        _ = rpc.transport.tcp.Connection;
        _ = rpc.transport.tcp.Runtime;
        _ = rpc.transport.tcp.Transport;
        _ = rpc.events.Observer;
        _ = capnpc.io_backend.Backend;
    }
}
