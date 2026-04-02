const std = @import("std");
const capnpc = @import("capnpc-zig");
const pingpong = @import("pingpong.zig");

const rpc = capnpc.rpc;
const PingPong = pingpong.PingPong;

// ---------------------------------------------------------------------------
// Server handler
// ---------------------------------------------------------------------------

fn handlePing(
    _: *anyopaque,
    _: *rpc.peer.Peer,
    params: PingPong.Ping.Params.Reader,
    results: *PingPong.Ping.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const value = try params.getCount();
    try results.setCount(value + 1);
}

// ---------------------------------------------------------------------------
// Peer lifecycle callbacks
// ---------------------------------------------------------------------------

fn onPeerError(peer: *rpc.peer.Peer, _: anyerror) void {
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}

fn onPeerClose(peer: *rpc.peer.Peer) void {
    const allocator = peer.allocator;
    const conn = peer.takeAttachedConnection(*rpc.connection.Connection);

    peer.deinit();
    allocator.destroy(peer);

    if (conn) |attached| {
        attached.deinit();
        allocator.destroy(attached);
    }
}

// ---------------------------------------------------------------------------
// Server thread: accept one connection, serve it
// ---------------------------------------------------------------------------

fn serverThread(listener: *rpc.runtime.Listener, server: *PingPong.Server) void {
    const conn = listener.accept() catch return;
    const peer_ptr = conn.allocator.create(rpc.peer.Peer) catch return;
    peer_ptr.* = rpc.peer.Peer.init(conn.allocator, conn);

    _ = PingPong.setBootstrap(peer_ptr, server) catch return;
    peer_ptr.start(onPeerError, onPeerClose);
    conn.run();
}

// ---------------------------------------------------------------------------
// Client bootstrap callback
// ---------------------------------------------------------------------------

const ClientState = struct {
    start_value: u32 = 41,
    done: bool = false,
    result: ?u32 = null,
    err: ?anyerror = null,
};

const CallCtx = struct {
    state: *ClientState,
};

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

    switch (response) {
        .results => |results| {
            ctx.state.result = try results.getCount();
            ctx.state.done = true;
            if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
        },
        .exception => {
            ctx.state.err = error.RemoteException;
            ctx.state.done = true;
            if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
        },
        else => {
            ctx.state.err = error.UnexpectedReturn;
            ctx.state.done = true;
            if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
        },
    }
}

fn onBootstrap(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: PingPong.BootstrapResponse,
) anyerror!void {
    const state: *ClientState = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .client => |client| {
            var client_mut = client;
            const call_ctx = try peer.allocator.create(CallCtx);
            call_ctx.* = .{ .state = state };
            _ = try client_mut.callPing(call_ctx, buildPing, onPingReturn);
        },
        .exception => {
            state.err = error.BootstrapFailed;
            state.done = true;
            if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
        },
        else => {
            state.err = error.UnexpectedBootstrapResponse;
            state.done = true;
            if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
        },
    }
}

// ---------------------------------------------------------------------------
// Client thread: connect, bootstrap, run
// ---------------------------------------------------------------------------

fn clientThread(state: *ClientState, address: std.Io.net.IpAddress, io: std.Io) void {
    const allocator = std.heap.page_allocator;

    const fd = rawTcpConnect(address, io) catch |err| {
        state.err = err;
        state.done = true;
        return;
    };

    const conn = allocator.create(rpc.connection.Connection) catch {
        rpc.runtime.closeFd(io, fd);
        state.err = error.OutOfMemory;
        state.done = true;
        return;
    };
    conn.* = rpc.connection.Connection.init(allocator, io, fd, .{}) catch |err| {
        allocator.destroy(conn);
        rpc.runtime.closeFd(io, fd);
        state.err = err;
        state.done = true;
        return;
    };

    const peer_ptr = allocator.create(rpc.peer.Peer) catch {
        conn.deinit();
        allocator.destroy(conn);
        state.err = error.OutOfMemory;
        state.done = true;
        return;
    };
    peer_ptr.* = rpc.peer.Peer.init(allocator, conn);
    peer_ptr.start(onPeerError, onPeerClose);

    _ = PingPong.Client.fromBootstrap(peer_ptr, state, onBootstrap) catch |err| {
        state.err = err;
        state.done = true;
        return;
    };

    conn.run();
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const address = parseIp4Address("127.0.0.1", 7001);

    var listener = rpc.runtime.Listener.initFd(
        allocator,
        io,
        (try rpc.runtime.createListenSocket(io, address, 1, false)).socket.handle,
        .{},
    );
    defer listener.close();

    var server = PingPong.Server{
        .ctx = undefined,
        .vtable = .{ .ping = handlePing },
    };

    const server_thread = try std.Thread.spawn(.{}, serverThread, .{ &listener, &server });

    var state = ClientState{};
    const client_thread = try std.Thread.spawn(.{}, clientThread, .{ &state, address, io });
    client_thread.join();
    server_thread.join();

    if (state.err) |err| return err;

    if (state.result) |value| {
        std.debug.print("Ping result: {d}\n", .{value});
    }
}

// ---------------------------------------------------------------------------
// Helpers (matching stressor.zig pattern)
// ---------------------------------------------------------------------------

fn parseIp4Address(host: []const u8, port: u16) std.Io.net.IpAddress {
    var bytes: [4]u8 = undefined;
    var byte_idx: usize = 0;
    var iter = std.mem.splitScalar(u8, host, '.');
    while (iter.next()) |octet| {
        if (byte_idx >= 4) unreachable;
        bytes[byte_idx] = std.fmt.parseInt(u8, octet, 10) catch unreachable;
        byte_idx += 1;
    }
    return .{ .ip4 = .{ .bytes = bytes, .port = port } };
}

fn rawTcpConnect(addr: std.Io.net.IpAddress, io: std.Io) !std.posix.fd_t {
    const stream = try std.Io.net.IpAddress.connect(&addr, io, .{ .mode = .stream });
    return stream.socket.handle;
}
