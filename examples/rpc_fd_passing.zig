//! Fd passing over a Unix-domain socket (Experimental, Linux and macOS).
//!
//! The server attaches the write end of a pipe to its bootstrap capability
//! (`Peer.setExportFd`). The client gets the capability, borrows the fd that
//! came with it (`client.peer.importFd(client.cap_id)`), checks what kind of
//! file it is, writes a line through it, and pings. The main thread then
//! reads the line from the pipe's read end, and waits until every copy of
//! the write end is closed: the dup the server's transport sent, and the
//! client's received fd, which the client's peer closes when it releases
//! the capability. Fd passing leaves nothing open behind it.
//!
//! Both ends turn fd passing on (`fd_passing.max_fds_per_message > 0`). A
//! connection that does not keeps no fd it receives and sends none.
//!
//! `zig build example-rpc-fd`. On other targets it prints that and exits.
//! See docs/rpc-unix-sockets.md for the rules and the threat model.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const pingpong = @import("pingpong.zig");

const rpc = capnpc.rpc;
const unix = rpc.transport.unix;
const PingPong = pingpong.PingPong;

/// Both ends keep at most one fd per message, and send fds only because
/// this is above 0.
const fd_passing: unix.FdPassing = .{ .max_fds_per_message = 1 };

const greeting = "hello through a passed fd\n";

/// How long the main thread waits for the last copy of the write end to
/// close. The closer threads do it within milliseconds.
const all_closed_timeout_ms: i32 = 10_000;

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

const ServerState = struct {
    listener: *rpc.transport.tcp.Listener,
    server: *PingPong.Server,
    /// Borrowed by the export: it stays open until the session ends.
    pipe_write_end: unix.FdHandle,
    err: ?anyerror = null,
};

/// Accept one connection, attach the pipe's write end to the bootstrap
/// capability, and serve until the client hangs up.
fn serverThread(allocator: std.mem.Allocator, state: *ServerState) void {
    var session = rpc.transport.tcp.ServerSession.accept(allocator, state.listener, .{}) catch |err| {
        state.err = err;
        return;
    };
    defer session.deinit();
    const export_id = PingPong.setBootstrap(&session.peer, state.server) catch |err| {
        state.err = err;
        return;
    };
    // Every message that sends this capability now carries a dup of the fd.
    session.peer.setExportFd(export_id, state.pipe_write_end) catch |err| {
        state.err = err;
        return;
    };
    session.run();
}

const ClientState = struct {
    io: std.Io,
    wrote: bool = false,
    result: ?u32 = null,
    err: ?anyerror = null,
};

fn buildPing(_: *anyopaque, params: *PingPong.Ping.Params.Builder) anyerror!void {
    try params.setCount(41);
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

/// Write `greeting` through the fd the server attached to `client`'s
/// capability. The fd is borrowed: valid until the capability is released,
/// and never closed here.
fn writeThroughAttachedFd(io: std.Io, client: PingPong.Client) !void {
    const handle = client.peer.importFd(client.cap_id) orelse return error.NoFdAttached;
    const file: std.Io.File = .{ .handle = handle.fd, .flags = .{ .nonblocking = false } };
    // A peer can attach any open file: check that it is what the protocol
    // promises before using it.
    const stat = try file.stat(io);
    if (stat.kind != .named_pipe) return error.UnexpectedFdKind;
    try file.writeStreamingAll(io, greeting);
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
    writeThroughAttachedFd(state.io, client) catch |err| {
        state.err = err;
        if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
        return;
    };
    state.wrote = true;
    _ = try client.callPing(state, buildPing, onPingReturn);
}

fn runClient(allocator: std.mem.Allocator, io: std.Io, path: []const u8, state: *ClientState) !void {
    const session = try unix.connect(allocator, io, path, .{ .fd_passing = fd_passing });
    // Deinit releases the capability, and with it the received fd.
    defer session.deinit();
    _ = try PingPong.Client.fromBootstrap(&session.peer, state, onBootstrap);
    session.run();
}

/// A pipe whose ends are close-on-exec: `.{ read_end, write_end }`.
fn openPipe() ![2]i32 {
    var fds: [2]i32 = undefined;
    switch (std.posix.errno(std.posix.system.pipe(&fds))) {
        .SUCCESS => {},
        else => return error.PipeFailed,
    }
    for (fds) |fd| _ = std.posix.system.fcntl(fd, std.posix.F.SETFD, @as(usize, std.posix.FD_CLOEXEC));
    return fds;
}

fn closeFd(fd: i32) void {
    _ = std.posix.system.close(fd);
}

/// Read from `read_end` until every copy of the write end is closed (end of
/// stream), for at most `timeout_ms` in all. Returns the bytes read.
fn readUntilAllWritersClosed(read_end: i32, buf: []u8, timeout_ms: i32) ![]const u8 {
    var len: usize = 0;
    var waited_ms: i32 = 0;
    const step_ms: i32 = 50;
    while (true) {
        var pfd = [1]std.posix.pollfd{.{ .fd = read_end, .events = std.posix.POLL.IN, .revents = 0 }};
        if (try std.posix.poll(&pfd, step_ms) == 0) {
            waited_ms += step_ms;
            if (waited_ms >= timeout_ms) return error.WriteEndStillOpen;
            continue;
        }
        if (len == buf.len) return error.UnexpectedPipeData;
        const n = try std.posix.read(read_end, buf[len..]);
        if (n == 0) return buf[0..len];
        len += n;
    }
}

pub fn main(init: std.process.Init) !void {
    if (comptime !unix.supported) {
        std.debug.print("Fd passing is not compiled in: it needs Linux or macOS, built with -Dfd-passing=true (the default)\n", .{});
        return;
    }

    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();
    const io = init.io;

    // A private directory (mode 0700) for the socket file and its `.lock`.
    var dir_buf: [64]u8 = undefined;
    const dir = try std.fmt.bufPrint(&dir_buf, "/tmp/capnp-fd-{d}", .{std.posix.system.getpid()});
    const cwd = std.Io.Dir.cwd();
    cwd.deleteTree(io, dir) catch {};
    try cwd.createDir(io, dir, .fromMode(0o700));
    defer cwd.deleteTree(io, dir) catch {};
    var path_buf: [96]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/fd.sock", .{dir});

    const pipe = try openPipe();
    defer closeFd(pipe[0]);
    var write_end_open = true;
    defer if (write_end_open) closeFd(pipe[1]);

    var listener = try unix.listen(allocator, io, path, .{ .fd_passing = fd_passing });
    defer listener.close();

    var server = PingPong.Server{ .ctx = undefined, .vtable = .{ .ping = handlePing } };
    var server_state = ServerState{
        .listener = &listener,
        .server = &server,
        .pipe_write_end = .{ .fd = pipe[1] },
    };
    const server_thread = try std.Thread.spawn(.{}, serverThread, .{ allocator, &server_state });

    var client_state = ClientState{ .io = io };
    runClient(allocator, io, path, &client_state) catch |err| {
        client_state.err = err;
        // The server thread may still wait in accept: closing the listener
        // wakes it.
        listener.close();
    };
    server_thread.join();
    if (server_state.err) |err| return err;
    if (client_state.err) |err| return err;
    if (!client_state.wrote) return error.NothingWritten;
    const value = client_state.result orelse return error.NoPingResult;

    // The session is over, so the export no longer borrows the write end:
    // close this process's own copy. The read end sees end of stream once
    // the closer threads have closed the other two.
    closeFd(pipe[1]);
    write_end_open = false;
    var buf: [64]u8 = undefined;
    const line = try readUntilAllWritersClosed(pipe[0], &buf, all_closed_timeout_ms);
    if (!std.mem.eql(u8, line, greeting)) return error.UnexpectedPipeData;

    std.debug.print("Ping result over {s}: {d}\n", .{ path, value });
    std.debug.print("Read through the passed fd: {s}", .{line});
    std.debug.print("Every copy of the passed fd is closed\n", .{});
}
