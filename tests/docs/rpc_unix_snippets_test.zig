//! The code snippets of docs/rpc-unix-sockets.md, compiled and run.
//!
//! Each region of this file between a `// snippet: <name>` line and the
//! next `// snippet end` line appears in the doc word for word (less the
//! marker line's indentation). The first test checks that, so the doc and
//! this file cannot drift apart. The other tests run the snippets over a
//! real socket file on Linux and macOS; other targets skip them, and check
//! instead that `listen` and `connect` return `UnixSocketsUnsupported`.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const pingpong = @import("pingpong");

const testing = std.testing;
const rpc = capnpc.rpc;
const unix = rpc.transport.unix;
const tcp = rpc.transport.tcp;
const PingPong = pingpong.PingPong;

const doc = @embedFile("rpc-unix-sockets-doc");
const this_file = @embedFile("rpc_unix_snippets_test.zig");

/// Every snippet the doc shows. A name missing here or there is a failure.
const snippet_names = [_][]const u8{ "listen", "serve", "connect", "export-fd", "import-fd", "events", "budget" };

// ---------------------------------------------------------------------------
// The doc and this file agree
// ---------------------------------------------------------------------------

const begin_marker = "// snippet: ";
const end_marker = "// snippet end";

/// The text of snippet `name`, each line without the marker line's
/// indentation, every line ending in a newline. Null when the file has no
/// such snippet.
fn extractSnippet(allocator: std.mem.Allocator, source: []const u8, name: []const u8) !?[]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var lines = std.mem.splitScalar(u8, source, '\n');
    var indent: ?usize = null;
    while (lines.next()) |raw_line| {
        // .gitattributes keeps both files LF; a stray CR still must not
        // hide a marker.
        const line = std.mem.trimEnd(u8, raw_line, "\r");
        const trimmed = std.mem.trimStart(u8, line, " ");
        if (indent) |n| {
            if (std.mem.eql(u8, trimmed, end_marker)) return try out.toOwnedSlice(allocator);
            const body = if (line.len >= n and std.mem.allEqual(u8, line[0..n], ' ')) line[n..] else trimmed;
            try out.appendSlice(allocator, body);
            try out.append(allocator, '\n');
        } else if (std.mem.startsWith(u8, trimmed, begin_marker) and
            std.mem.eql(u8, trimmed[begin_marker.len..], name))
        {
            indent = line.len - trimmed.len;
        }
    }
    if (indent != null) return error.UnterminatedSnippet;
    return null;
}

fn countSnippets(source: []const u8) usize {
    var count: usize = 0;
    var lines = std.mem.splitScalar(u8, source, '\n');
    while (lines.next()) |line| {
        if (std.mem.startsWith(u8, std.mem.trimStart(u8, line, " "), begin_marker)) count += 1;
    }
    return count;
}

test "every snippet in this file appears word for word in docs/rpc-unix-sockets.md" {
    const allocator = testing.allocator;
    try testing.expectEqual(snippet_names.len, countSnippets(this_file));
    var missing: usize = 0;
    for (snippet_names) |name| {
        const snippet = (try extractSnippet(allocator, this_file, name)) orelse {
            std.debug.print("snippet '{s}' is not in rpc_unix_snippets_test.zig\n", .{name});
            missing += 1;
            continue;
        };
        defer allocator.free(snippet);
        try testing.expect(snippet.len > 0);
        if (std.mem.indexOf(u8, doc, snippet) == null) {
            std.debug.print("snippet '{s}' is not in docs/rpc-unix-sockets.md word for word:\n{s}\n", .{ name, snippet });
            missing += 1;
        }
    }
    try testing.expectEqual(@as(usize, 0), missing);
}

// ---------------------------------------------------------------------------
// Shared pieces
// ---------------------------------------------------------------------------

/// A private (0700) directory under /tmp for one test's socket file and its
/// `.lock`, removed with everything in it.
const PrivateDir = struct {
    dir_buf: [64]u8 = undefined,
    dir_len: usize = 0,
    path_buf: [96]u8 = undefined,
    path_len: usize = 0,

    fn create(self: *PrivateDir, io: std.Io, tag: []const u8) !void {
        const dir = try std.fmt.bufPrint(&self.dir_buf, "/tmp/czdoc-{d}-{s}", .{ std.posix.system.getpid(), tag });
        self.dir_len = dir.len;
        const cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, dir) catch {};
        try cwd.createDir(io, dir, .fromMode(0o700));
        const path = try std.fmt.bufPrint(&self.path_buf, "{s}/rpc.sock", .{dir});
        self.path_len = path.len;
    }

    fn socketPath(self: *const PrivateDir) []const u8 {
        return self.path_buf[0..self.path_len];
    }

    fn remove(self: *const PrivateDir, io: std.Io) void {
        std.Io.Dir.cwd().deleteTree(io, self.dir_buf[0..self.dir_len]) catch {};
    }
};

fn handlePing(
    _: *anyopaque,
    _: *rpc.peer.Peer,
    params: PingPong.Ping.Params.Reader,
    results: *PingPong.Ping.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    try results.setCount((try params.getCount()) + 1);
}

const ClientState = struct {
    io: std.Io = undefined,
    result: ?u32 = null,
    /// The bootstrap capability came with an fd (`importFd` was not null).
    had_fd: bool = false,
    wrote: bool = false,
    err: ?anyerror = null,
};

fn closeSession(peer: *rpc.peer.Peer) void {
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}

fn buildPing(_: *anyopaque, params: *PingPong.Ping.Params.Builder) anyerror!void {
    try params.setCount(41);
}

fn onPingReturn(ctx: *anyopaque, peer: *rpc.peer.Peer, response: PingPong.Ping.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
    const state: *ClientState = @ptrCast(@alignCast(ctx));
    defer closeSession(peer);
    const results = response.unwrap() catch |err| {
        state.err = err;
        return;
    };
    state.result = try results.getCount();
}

fn onBootstrap(ctx: *anyopaque, peer: *rpc.peer.Peer, response: PingPong.BootstrapResponse) anyerror!void {
    const state: *ClientState = @ptrCast(@alignCast(ctx));
    const client = response.unwrap() catch |err| {
        state.err = err;
        closeSession(peer);
        return;
    };
    state.had_fd = client.peer.importFd(client.cap_id) != null;
    _ = client.callPing(state, buildPing, onPingReturn) catch |err| {
        state.err = err;
        closeSession(peer);
    };
}

/// Accept, serve one session, and record how that ended.
const ServerThread = struct {
    listener: *tcp.Listener,
    server: PingPong.Server = .{ .ctx = undefined, .vtable = .{ .ping = handlePing } },
    fd: ?unix.FdHandle = null,
    err: ?anyerror = null,
    thread: std.Thread = undefined,

    fn start(self: *ServerThread) !void {
        self.thread = try std.Thread.spawn(.{}, main, .{self});
    }

    fn main(self: *ServerThread) void {
        const result = if (self.fd) |fd|
            serveWithFd(testing.allocator, self.listener, &self.server, fd)
        else
            serveOne(testing.allocator, self.listener, &self.server);
        result catch |err| {
            self.err = err;
        };
    }
};

/// Reads `read_end` until every copy of its write end is closed, for at
/// most `timeout_ms`. Returns the bytes read.
fn readUntilAllWritersClosed(read_end: i32, buf: []u8, timeout_ms: i32) ![]const u8 {
    var len: usize = 0;
    var waited_ms: i32 = 0;
    while (true) {
        var pfd = [1]std.posix.pollfd{.{ .fd = read_end, .events = std.posix.POLL.IN, .revents = 0 }};
        if (try std.posix.poll(&pfd, 50) == 0) {
            waited_ms += 50;
            if (waited_ms >= timeout_ms) return error.WriteEndStillOpen;
            continue;
        }
        if (len == buf.len) return error.UnexpectedPipeData;
        const n = try std.posix.read(read_end, buf[len..]);
        if (n == 0) return buf[0..len];
        len += n;
    }
}

fn openPipe() ![2]i32 {
    var fds: [2]i32 = undefined;
    if (std.posix.errno(std.posix.system.pipe(&fds)) != .SUCCESS) return error.PipeFailed;
    return fds;
}

fn closeFd(fd: i32) void {
    _ = std.posix.system.close(fd);
}

// ---------------------------------------------------------------------------
// Quick start
// ---------------------------------------------------------------------------

// snippet: serve
fn serveOne(gpa: std.mem.Allocator, listener: *rpc.transport.tcp.Listener, server: *PingPong.Server) !void {
    // ServerSession.accept takes a Unix listener as it takes a TCP one.
    var session = try rpc.transport.tcp.ServerSession.accept(gpa, listener, .{});
    defer session.deinit();
    _ = try PingPong.setBootstrap(&session.peer, server);
    session.run();
}
// snippet end

// snippet: connect
fn connectAndPing(gpa: std.mem.Allocator, io: std.Io, path: []const u8, state: *ClientState) !void {
    const session = try rpc.transport.unix.connect(gpa, io, path, .{
        .connect_timeout_ms = 5_000, // Linux waits while the backlog is full
    });
    defer session.deinit();
    _ = try PingPong.Client.fromBootstrap(&session.peer, state, onBootstrap);
    session.run();
}
// snippet end

test "quick start: listen in a private directory, serve one session, connect and call" {
    if (comptime !unix.supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    const io = testing.io;
    var dir: PrivateDir = .{};
    try dir.create(io, "quick");
    defer dir.remove(io);
    const path = dir.socketPath();

    // snippet: listen
    var listener = try rpc.transport.unix.listen(gpa, io, path, .{
        .socket_mode = 0o600, // the default: only this user can connect
        .reclaim_stale = false, // the default: a file left at `path` is AddressInUse
    });
    defer listener.close(); // removes the socket file if it is still ours
    // getAddress() means nothing for a socket file; unixPath() is `path`.
    const bound: []const u8 = listener.unixPath().?;
    // snippet end
    try testing.expectEqualStrings(path, bound);

    var server: ServerThread = .{ .listener = &listener };
    try server.start();
    var state: ClientState = .{ .io = io };
    connectAndPing(gpa, io, path, &state) catch |err| {
        listener.close();
        server.thread.join();
        return err;
    };
    server.thread.join();

    try testing.expectEqual(@as(?anyerror, null), server.err);
    try testing.expectEqual(@as(?anyerror, null), state.err);
    try testing.expectEqual(@as(?u32, 42), state.result);
    try testing.expect(!state.had_fd);
}

test "where AF_UNIX is not supported, listen and connect return UnixSocketsUnsupported" {
    if (comptime unix.supported) return error.SkipZigTest;
    try testing.expectError(error.UnixSocketsUnsupported, unix.listen(testing.allocator, testing.io, "/tmp/rpc.sock", .{}));
    try testing.expectError(error.UnixSocketsUnsupported, unix.connect(testing.allocator, testing.io, "/tmp/rpc.sock", .{}));
}

// ---------------------------------------------------------------------------
// Fd passing
// ---------------------------------------------------------------------------

// snippet: export-fd
fn serveWithFd(gpa: std.mem.Allocator, listener: *rpc.transport.tcp.Listener, server: *PingPong.Server, fd: rpc.transport.unix.FdHandle) !void {
    var session = try rpc.transport.tcp.ServerSession.accept(gpa, listener, .{});
    defer session.deinit();
    const export_id = try PingPong.setBootstrap(&session.peer, server);
    // Borrowed: keep `fd` open while the export has it, and call
    // clearExportFd before you close it early.
    try session.peer.setExportFd(export_id, fd);
    session.run();
}
// snippet end

// snippet: import-fd
fn writeThroughAttachedFd(io: std.Io, client: PingPong.Client) !void {
    // Borrowed: valid until the capability is released. Do not close it;
    // dup it to keep it longer.
    const handle = client.peer.importFd(client.cap_id) orelse return error.NoFdAttached;
    const file: std.Io.File = .{ .handle = handle.fd, .flags = .{ .nonblocking = false } };
    // A peer can attach any open file: check its kind before you use it.
    if ((try file.stat(io)).kind != .named_pipe) return error.UnexpectedFdKind;
    try file.writeStreamingAll(io, "hello\n");
}
// snippet end

fn onBootstrapWithFd(ctx: *anyopaque, peer: *rpc.peer.Peer, response: PingPong.BootstrapResponse) anyerror!void {
    const state: *ClientState = @ptrCast(@alignCast(ctx));
    const client = response.unwrap() catch |err| {
        state.err = err;
        closeSession(peer);
        return;
    };
    state.had_fd = client.peer.importFd(client.cap_id) != null;
    writeThroughAttachedFd(state.io, client) catch |err| {
        state.err = err;
        closeSession(peer);
        return;
    };
    state.wrote = true;
    _ = client.callPing(state, buildPing, onPingReturn) catch |err| {
        state.err = err;
        closeSession(peer);
    };
}

/// Serve one session whose bootstrap capability carries the write end of a
/// pipe, connect with `client_fd_passing`, and return the client's state
/// and the bytes the pipe got. Fails unless every copy of the write end is
/// closed at the end.
fn passPipeEnd(client_fd_passing: unix.FdPassing, tag: []const u8, observer: ?rpc.events.Observer, state: *ClientState, buf: []u8) ![]const u8 {
    const gpa = testing.allocator;
    const io = testing.io;
    var dir: PrivateDir = .{};
    try dir.create(io, tag);
    defer dir.remove(io);
    const path = dir.socketPath();

    const pipe = try openPipe();
    defer closeFd(pipe[0]);
    var write_end_open = true;
    defer if (write_end_open) closeFd(pipe[1]);

    var listener = try unix.listen(gpa, io, path, .{ .fd_passing = .{ .max_fds_per_message = 1 } });
    defer listener.close();
    var server: ServerThread = .{ .listener = &listener, .fd = .{ .fd = pipe[1] } };
    try server.start();
    state.io = io;
    run: {
        const session = unix.connect(gpa, io, path, .{
            .fd_passing = client_fd_passing,
            .session = .{ .observer = observer },
        }) catch |err| {
            listener.close();
            server.thread.join();
            return err;
        };
        defer session.deinit();
        _ = PingPong.Client.fromBootstrap(&session.peer, state, onBootstrapWithFd) catch |err| {
            state.err = err;
            break :run;
        };
        session.run();
    }
    server.thread.join();
    if (server.err) |err| return err;

    closeFd(pipe[1]);
    write_end_open = false;
    return readUntilAllWritersClosed(pipe[0], buf, 10_000);
}

test "fd passing: export-fd and import-fd pass a pipe's write end, and every copy is closed at the end" {
    if (comptime !unix.supported) return error.SkipZigTest;
    var state: ClientState = .{};
    var buf: [16]u8 = undefined;
    // The same switch on both ends.
    const got = try passPipeEnd(.{ .max_fds_per_message = 1 }, "fd", null, &state, &buf);
    try testing.expectEqual(@as(?anyerror, null), state.err);
    try testing.expect(state.had_fd);
    try testing.expect(state.wrote);
    try testing.expectEqual(@as(?u32, 42), state.result);
    try testing.expectEqualStrings("hello\n", got);
}

// ---------------------------------------------------------------------------
// Events and the budget
// ---------------------------------------------------------------------------

// snippet: events
const FdEvents = struct {
    /// Fds this side closed instead of keeping (drain mode, a limit, the
    /// budget, a truncated read); `err` names the cause.
    rejections: usize = 0,
    /// Sends with fds this side refused (`error.FdQueueFull`).
    backpressure: usize = 0,
    last_err: ?anyerror = null,

    fn onEvent(ctx: *anyopaque, event: rpc.events.Event) void {
        const self: *FdEvents = @ptrCast(@alignCast(ctx));
        switch (event) {
            .resource_rejection => |e| if (e.resource == .attached_fds) {
                self.rejections += 1;
                self.last_err = e.err;
            },
            .backpressure => |e| if (e.resource == .attached_fds) {
                self.backpressure += 1;
                self.last_err = e.err;
            },
            else => {},
        }
    }
};
// snippet end

test "events: a client without fd passing closes the fd the server attached, and says so" {
    if (comptime !unix.supported) return error.SkipZigTest;
    var events: FdEvents = .{};
    var state: ClientState = .{};
    var buf: [16]u8 = undefined;
    // The server sends (its side is on); the client keeps nothing (drain
    // mode, the default).
    const got = try passPipeEnd(.{}, "ev", rpc.events.Observer.init(&events, FdEvents.onEvent), &state, &buf);
    try testing.expectEqual(@as(?anyerror, error.NoFdAttached), state.err);
    try testing.expect(!state.had_fd);
    try testing.expectEqualStrings("", got);
    try testing.expect(events.rejections >= 1);
    try testing.expectEqual(@as(?anyerror, error.AttachedFdsRejected), events.last_err);
    try testing.expectEqual(@as(usize, 0), events.backpressure);
}

test "budget: setLimit replaces the process fd budget and returns the one before" {
    if (comptime !unix.supported) return error.SkipZigTest;
    // snippet: budget
    // Once, before the first AF_UNIX connection: the fds fd passing may
    // hold in this process (default RLIMIT_NOFILE / 4).
    const previous_limit = rpc.transport.unix.fd_io.budget.setLimit(256);
    // snippet end
    defer _ = unix.fd_io.budget.setLimit(previous_limit);
    try testing.expectEqual(@as(usize, 256), unix.fd_io.budget.limit());
    try testing.expect(previous_limit >= unix.fd_io.budget.min_limit);
}
