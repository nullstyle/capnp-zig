//! The RPC ping-pong example over a Unix-domain socket:
//! `rpc.transport.unix.listen` on the server side, `rpc.transport.unix.connect`
//! on the client side, and everything else exactly as over TCP
//! (`examples/rpc_pingpong.zig`).
//!
//! `zig build example-rpc-unix`. Linux and macOS only (Experimental); on other
//! targets it prints that and exits.
//!
//! The socket lives in a private directory (mode 0700), the layout
//! `unix.listen` documents: nobody else can swap the socket file there.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const pingpong = @import("pingpong.zig");

const rpc = capnpc.rpc;
const unix = rpc.transport.unix;
const PingPong = pingpong.PingPong;

fn handlePing(
    _: *anyopaque,
    _: *rpc.peer.Peer,
    params: PingPong.Ping.Params.Reader,
    results: *PingPong.Ping.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const value = try params.getCount();
    try results.setCount(value + 1);
}

/// Accept one connection and serve it. `ServerSession.accept` takes the
/// listener `unix.listen` returned, exactly as it takes a TCP one.
fn serverThread(allocator: std.mem.Allocator, listener: *rpc.transport.tcp.Listener, server: *PingPong.Server) void {
    var session = rpc.transport.tcp.ServerSession.accept(allocator, listener, .{}) catch return;
    defer session.deinit();
    if (PingPong.setBootstrap(&session.peer, server)) |_| {
        session.run();
    } else |_| {}
}

const ClientState = struct {
    start_value: u32 = 41,
    result: ?u32 = null,
    err: ?anyerror = null,
};

fn buildPing(ctx_ptr: *anyopaque, params: *PingPong.Ping.Params.Builder) anyerror!void {
    const state: *ClientState = @ptrCast(@alignCast(ctx_ptr));
    try params.setCount(state.start_value);
}

fn onPingReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: PingPong.Ping.Response,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const state: *ClientState = @ptrCast(@alignCast(ctx_ptr));
    defer if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
    const results = response.unwrap() catch |err| {
        state.err = err;
        return;
    };
    state.result = try results.getCount();
}

fn onBootstrap(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: PingPong.BootstrapResponse,
) anyerror!void {
    const state: *ClientState = @ptrCast(@alignCast(ctx_ptr));
    const client = response.unwrap() catch |err| {
        state.err = err;
        if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
        return;
    };
    _ = try client.callPing(state, buildPing, onPingReturn);
}

/// Connect, bootstrap, ping once, and run until the reply closes the session.
fn runClient(allocator: std.mem.Allocator, io: std.Io, path: []const u8, state: *ClientState) !void {
    const session = try unix.connect(allocator, io, path, .{});
    defer session.deinit();
    _ = try PingPong.Client.fromBootstrap(&session.peer, state, onBootstrap);
    session.run();
}

pub fn main(init: std.process.Init) !void {
    if (comptime !unix.supported) {
        std.debug.print("Unix-domain sockets are not compiled in: they need Linux or macOS, built with -Dfd-passing=true (the default)\n", .{});
        return;
    }

    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();
    const io = init.io;

    // A private directory for the socket file (and its `.lock`).
    var dir_buf: [64]u8 = undefined;
    const dir = try std.fmt.bufPrint(&dir_buf, "/tmp/capnp-pingpong-{d}", .{std.posix.system.getpid()});
    const cwd = std.Io.Dir.cwd();
    cwd.deleteTree(io, dir) catch {};
    try cwd.createDir(io, dir, .fromMode(0o700));
    defer cwd.deleteTree(io, dir) catch {};
    var path_buf: [96]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/pingpong.sock", .{dir});

    // Bound, mode 0600, locked; `close` removes the socket file.
    var listener = try unix.listen(allocator, io, path, .{});
    defer listener.close();

    var server = PingPong.Server{
        .ctx = undefined,
        .vtable = .{ .ping = handlePing },
    };
    const server_thread = try std.Thread.spawn(.{}, serverThread, .{ allocator, &listener, &server });

    var state = ClientState{};
    runClient(allocator, io, path, &state) catch |err| {
        state.err = err;
        // The server thread may still wait in accept: closing the listener
        // wakes it.
        listener.close();
    };
    server_thread.join();

    if (state.err) |err| return err;
    const value = state.result orelse return error.NoPingResult;
    std.debug.print("Ping result over {s}: {d}\n", .{ path, value });
}
