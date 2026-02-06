const std = @import("std");
const capnpc = @import("capnpc-zig");
const xev = @import("xev");
const pingpong = @import("pingpong.zig");

const rpc = capnpc.rpc;
const PingPong = pingpong.PingPong;

const State = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    server_peer: ?*rpc.peer.Peer = null,
    server_conn: ?*rpc.connection.Connection = null,
    client_peer: ?*rpc.peer.Peer = null,
    client_conn: ?*rpc.connection.Connection = null,
    start_value: u32 = 41,
    done: bool = false,
    err: ?anyerror = null,
};

const CallCtx = struct {
    state: *State,
};

const ServerCtx = struct {
    listener: rpc.runtime.Listener,
    state: *State,
    server: PingPong.Server,
};

var g_state: ?*State = null;

fn onPeerError(peer: *rpc.peer.Peer, err: anyerror) void {
    _ = peer;
    if (g_state) |state| {
        state.err = err;
        state.done = true;
        if (state.client_peer) |p| p.conn.close();
    }
}

fn onPeerClose(peer: *rpc.peer.Peer) void {
    _ = peer;
    if (g_state) |state| {
        state.done = true;
    }
}

fn handlePing(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: PingPong.Ping.Params.Reader,
    results: *PingPong.Ping.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    _ = ctx_ptr;
    const value = try params.getCount();
    try results.setCount(value + 1);
}

fn onAccept(listener: *rpc.runtime.Listener, conn: *rpc.connection.Connection) void {
    const server_ctx: *ServerCtx = @fieldParentPtr("listener", listener);
    const state = server_ctx.state;

    const peer_ptr = state.allocator.create(rpc.peer.Peer) catch {
        state.err = error.OutOfMemory;
        state.done = true;
        return;
    };
    peer_ptr.* = rpc.peer.Peer.init(state.allocator, conn);
    state.server_peer = peer_ptr;
    state.server_conn = conn;

    _ = PingPong.setBootstrap(peer_ptr, &server_ctx.server) catch |err| {
        state.err = err;
        state.done = true;
        return;
    };
    peer_ptr.start(onPeerError, onPeerClose);
}

fn buildPing(ctx_ptr: *anyopaque, params: *PingPong.Ping.Params.Builder) anyerror!void {
    const ctx: *CallCtx = @ptrCast(@alignCast(ctx_ptr));
    try params.setCount(ctx.state.start_value);
}

fn onPingReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: PingPong.Ping.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const ctx: *CallCtx = @ptrCast(@alignCast(ctx_ptr));
    defer peer.allocator.destroy(ctx);

    const state = ctx.state;
    switch (response) {
        .results => |results| {
            const value = try results.getCount();
            std.debug.print("Ping result: {d}\n", .{value});
            state.done = true;
            peer.conn.close();
        },
        .exception => return error.RemoteException,
        else => return error.UnexpectedReturn,
    }
}

fn onBootstrap(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: PingPong.BootstrapResponse,
) anyerror!void {
    const state: *State = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .client => |client| {
            var client_mut = client;
            const call_ctx = try peer.allocator.create(CallCtx);
            call_ctx.* = .{ .state = state };
            _ = try client_mut.callPing(call_ctx, buildPing, onPingReturn);
        },
        .exception => return error.BootstrapFailed,
        else => return error.UnexpectedBootstrapResponse,
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var runtime = try rpc.runtime.Runtime.init(allocator);
    defer runtime.deinit();

    var state = State{
        .allocator = allocator,
        .loop = &runtime.loop,
    };
    g_state = &state;
    defer g_state = null;

    const addr = try std.net.Address.parseIp4("127.0.0.1", 7001);

    var server_ctx = ServerCtx{
        .state = &state,
        .server = PingPong.Server{
            .ctx = &state,
            .vtable = .{ .ping = handlePing },
        },
        .listener = try rpc.runtime.Listener.init(allocator, &runtime.loop, addr, onAccept, .{}),
    };
    server_ctx.listener.start();

    var socket = try xev.TCP.init(addr);
    var connect_completion: xev.Completion = .{};

    const ConnectCtx = struct { state: *State };
    var connect_ctx = ConnectCtx{ .state = &state };

    socket.connect(&runtime.loop, &connect_completion, addr, ConnectCtx, &connect_ctx, struct {
        fn onConnect(
            ctx: ?*ConnectCtx,
            loop_ptr: *xev.Loop,
            _: *xev.Completion,
            s: xev.TCP,
            res: xev.ConnectError!void,
        ) xev.CallbackAction {
            const connect_state = ctx.?.state;
            if (res) |_| {
                const conn_ptr = connect_state.allocator.create(rpc.connection.Connection) catch {
                    connect_state.err = error.OutOfMemory;
                    connect_state.done = true;
                    return .disarm;
                };
                conn_ptr.* = rpc.connection.Connection.init(connect_state.allocator, loop_ptr, s, .{}) catch |err| {
                    connect_state.allocator.destroy(conn_ptr);
                    connect_state.err = err;
                    connect_state.done = true;
                    return .disarm;
                };

                const peer_ptr = connect_state.allocator.create(rpc.peer.Peer) catch {
                    connect_state.err = error.OutOfMemory;
                    connect_state.done = true;
                    return .disarm;
                };
                peer_ptr.* = rpc.peer.Peer.init(connect_state.allocator, conn_ptr);
                connect_state.client_conn = conn_ptr;
                connect_state.client_peer = peer_ptr;

                peer_ptr.start(onPeerError, onPeerClose);
                _ = PingPong.Client.fromBootstrap(peer_ptr, connect_state, onBootstrap) catch |err| {
                    connect_state.err = err;
                    connect_state.done = true;
                };
            } else |err| {
                connect_state.err = err;
                connect_state.done = true;
            }
            return .disarm;
        }
    }.onConnect);

    while (!state.done) {
        try runtime.loop.run(.once);
    }

    if (state.err) |err| return err;

    if (state.client_peer) |peer| {
        peer.deinit();
        allocator.destroy(peer);
    }
    if (state.client_conn) |conn| {
        conn.deinit();
        allocator.destroy(conn);
    }
    if (state.server_peer) |peer| {
        peer.deinit();
        allocator.destroy(peer);
    }
    if (state.server_conn) |conn| {
        conn.deinit();
        allocator.destroy(conn);
    }
}
