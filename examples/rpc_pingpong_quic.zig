//! The RPC ping-pong example (rpc_pingpong.zig) over QUIC, built from the
//! one-call session APIs:
//!
//! - `quic.serve` binds a UDP listener and gives every accepted QUIC session
//!   its own `Peer`; `on_accept` sets the bootstrap on each one.
//! - `quic.connect` dials one `ClientSession` (QUIC `Connection` + `Peer`).
//!
//! Build with `zig build -Dquic=true example-rpc-quic`. The certificate is a
//! self-signed loopback demo pair (CN and SAN `localhost`/`127.0.0.1`) shared
//! with the QUIC tests. The client verifies it as its CA, so the TLS check
//! stays on. Never deploy it.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const pingpong = @import("pingpong.zig");

const rpc = capnpc.rpc;
const quic = rpc.transport.quic;
const PingPong = pingpong.PingPong;

const cert_pem = @embedFile("quic_example_cert");
const key_pem = @embedFile("quic_example_key");

comptime {
    if (!quic.enabled) @compileError("example-rpc-quic needs -Dquic=true");
}

// ---------------------------------------------------------------------------
// Server: one handler, shared by every session
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

/// Runs on the server's `run()` thread once per accepted QUIC session,
/// before its first frame is handled.
fn onAccept(ctx: ?*anyopaque, session: *quic.PeerServer.Session) anyerror!void {
    const server: *PingPong.Server = @ptrCast(@alignCast(ctx.?));
    _ = try PingPong.setBootstrap(&session.peer, server);
}

fn serverThread(server: *quic.PeerServer) void {
    server.run();
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

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
    // One round trip is the whole demo: closing ends `session.run()`.
    defer quic.ClientSession.fromPeer(peer).close();
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
        quic.ClientSession.fromPeer(peer).close();
        return;
    };
    _ = try client.callPing(state, buildPing, onPingReturn);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();
    const io = init.io;

    var ping_server = PingPong.Server{
        .ctx = undefined,
        .vtable = .{ .ping = handlePing },
    };

    // Port 0: the OS picks a free port; `getAddress()` reports it.
    const server = try quic.serve(allocator, io, .{
        .listen_addr = try std.Io.net.IpAddress.parse("127.0.0.1", 0),
        .tls_cert_pem = cert_pem,
        .tls_key_pem = key_pem,
        .max_concurrent_connections = 4,
    }, .{
        .ctx = &ping_server,
        .on_accept = onAccept,
    });
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, serverThread, .{server});
    // Runs before `server.deinit()` (defers unwind in reverse).
    defer {
        server.requestStop();
        server_thread.join();
    }

    var state = ClientState{};
    const session = try quic.connect(allocator, io, .{
        .conn = .{
            .remote_addr = server.getAddress(),
            .server_name = "localhost",
            .ca_pem = cert_pem,
        },
    });
    defer session.deinit();

    _ = try PingPong.Client.fromBootstrap(&session.peer, &state, onBootstrap);
    session.run();

    if (state.err) |err| return err;
    const value = state.result orelse return error.PingDidNotReturn;
    if (value != state.start_value + 1) return error.UnexpectedPingResult;
    std.debug.print("QUIC ping result: {d}\n", .{value});
}
