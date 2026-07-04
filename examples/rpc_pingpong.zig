const std = @import("std");
const capnpc = @import("capnpc-zig");
const io_backend_options = @import("io_backend_options");
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
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const value = try params.getCount();
    try results.setCount(value + 1);
}

// ---------------------------------------------------------------------------
// Peer lifecycle callbacks
// ---------------------------------------------------------------------------

fn onPeerError(_: ?*anyopaque, peer: *rpc.peer.Peer, _: anyerror) void {
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}

/// Tear down a peer and its heap-allocated connection. Only safe to call
/// after `conn.run()` has returned — freeing the connection from `on_close`
/// (which fires *inside* `run()`) is a use-after-free. See the `on_close`
/// / `on_destroy` docs on `Connection`.
fn teardown(peer: *rpc.peer.Peer, conn: *rpc.transport.tcp.Connection) void {
    const allocator = peer.allocator;
    _ = peer.takeAttachedConnection(*rpc.transport.tcp.Connection);
    peer.deinit();
    allocator.destroy(peer);
    conn.deinit();
    allocator.destroy(conn);
}

// ---------------------------------------------------------------------------
// Server thread: accept one connection, serve it
// ---------------------------------------------------------------------------

fn serverThread(listener: *rpc.transport.tcp.Listener, server: *PingPong.Server) void {
    const conn = listener.accept() catch return;
    const allocator = conn.allocator;
    const peer_ptr = allocator.create(rpc.peer.Peer) catch {
        conn.deinit();
        allocator.destroy(conn);
        return;
    };
    peer_ptr.* = rpc.peer.Peer.init(allocator, conn);

    if (PingPong.setBootstrap(peer_ptr, server)) |_| {
        peer_ptr.start(null, onPeerError, null);
        conn.run();
    } else |_| {}

    teardown(peer_ptr, conn);
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
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const ctx: *CallCtx = @ptrCast(@alignCast(ctx_ptr));
    defer peer.allocator.destroy(ctx);
    defer if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();

    const results = response.unwrap() catch |err| {
        ctx.state.err = err;
        ctx.state.done = true;
        return;
    };
    ctx.state.result = try results.getCount();
    ctx.state.done = true;
}

fn onBootstrap(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: PingPong.BootstrapResponse,
) anyerror!void {
    const state: *ClientState = @ptrCast(@alignCast(ctx_ptr));
    const client = response.unwrap() catch |err| {
        state.err = err;
        state.done = true;
        if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
        return;
    };
    const call_ctx = try peer.allocator.create(CallCtx);
    call_ctx.* = .{ .state = state };
    _ = try client.callPing(call_ctx, buildPing, onPingReturn);
}

// ---------------------------------------------------------------------------
// Client thread: connect, bootstrap, run
// ---------------------------------------------------------------------------

// Takes main's gpa: std.heap.DebugAllocator is thread-safe by default
// (`config.thread_safe = !builtin.single_threaded`), so sharing one
// allocator across the server/client threads is fine — and it keeps the
// whole example under the leak assert in main.
fn clientThread(state: *ClientState, address: std.Io.net.IpAddress, io: std.Io, allocator: std.mem.Allocator) void {
    const session = rpc.transport.tcp.connect(allocator, io, address, .{}) catch |err| {
        state.err = err;
        state.done = true;
        return;
    };
    defer session.deinit();

    if (PingPong.Client.fromBootstrap(&session.peer, state, onBootstrap)) |_| {
        session.run();
    } else |err| {
        state.err = err;
        state.done = true;
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    const backend_kind = capnpc.io_backend.parseKind(io_backend_options.kind) orelse {
        std.debug.print("invalid -Dio-backend selector: {s}\n", .{io_backend_options.kind});
        return error.InvalidIoBackend;
    };
    var backend = try capnpc.io_backend.Backend.init(backend_kind, init.gpa, init.io);
    defer backend.deinit();
    const io = backend.io();

    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 7001);

    var listener = rpc.transport.tcp.Listener.initFd(
        allocator,
        io,
        .{ .handle = (try rpc.transport.tcp.createListenSocket(io, address, 1, false)).socket.handle },
        .{},
    );
    defer listener.close();

    var server = PingPong.Server{
        .ctx = undefined,
        .vtable = .{ .ping = handlePing },
    };

    const server_thread = try std.Thread.spawn(.{}, serverThread, .{ &listener, &server });

    var state = ClientState{};
    const client_thread = try std.Thread.spawn(.{}, clientThread, .{ &state, address, io, allocator });
    client_thread.join();
    server_thread.join();

    if (state.err) |err| return err;

    if (state.result) |value| {
        std.debug.print("Ping result: {d}\n", .{value});
    }
}
