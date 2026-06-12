//! RPC soak harness: sustained concurrent bootstrap+call traffic over real
//! loopback TCP, with optional chaos (abrupt mid-flight disconnects) and
//! deadline-cancellation sessions.
//!
//! A WorkerPool server exports an echo bootstrap capability; W client worker
//! threads run connect → bootstrap → N calls → close sessions in a loop until
//! the configured duration elapses. Every chaos-eligible session tears the
//! connection down with a call still in flight; every deadline session runs
//! with a ~1ms call deadline so cancellation races real server Returns.
//!
//! Exit code is nonzero when invariants fail: zero successful calls, an
//! unexpected exception reason, or a client-side allocation leak.
//!
//! Usage: zig build soak -- [--seconds N] [--workers N] [--calls N]
//!                          [--no-chaos] [--no-deadlines]

const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const rpc = capnpc.rpc;
const protocol = rpc.wire.protocol;
const cap_table = rpc.caps.table;
const Peer = rpc.peer.Peer;
const Connection = rpc.transport.tcp.Connection;
const WorkerPool = rpc.integration.worker_pool.WorkerPool;
const net = std.Io.net;

const Config = struct {
    seconds: u64 = 2,
    workers: u32 = 4,
    calls_per_session: u32 = 25,
    chaos: bool = true,
    deadlines: bool = true,
};

const Totals = struct {
    sessions: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    calls_ok: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    calls_cancelled: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    chaos_closes: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    transport_errors: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    unexpected_exceptions: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

fn nowNs(io: std.Io) i64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

// -- Server ------------------------------------------------------------------

const EchoServer = struct {
    /// Calls to this method sleep server-side before answering, so client
    /// deadlines (1ms) expire and the cancellation path runs against a
    /// real server that still sends its (late) Return.
    pub const slow_method_id: u16 = 2;
    pub const slow_method_delay_ms: u64 = 10;

    var server_io: ?std.Io = null;

    fn onCall(
        _: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        if (call.method_id == slow_method_id) {
            if (server_io) |io| sleepMs(io, slow_method_delay_ms);
        }
        try peer.sendReturnEmptyStruct(call.question_id);
    }

    fn onAccept(
        _: *anyopaque,
        peer: *Peer,
        _: *Connection,
        _: u32,
    ) anyerror!WorkerPool.AcceptDecision {
        _ = try peer.setBootstrap(.{ .ctx = @ptrCast(&server_io), .on_call = EchoServer.onCall });
        peer.start(null, null);
        return .accept;
    }
};

fn poolThreadMain(pool: *WorkerPool) void {
    pool.run() catch |err| {
        std.debug.print("soak: worker pool run failed: {}\n", .{err});
    };
}

// -- Client session ----------------------------------------------------------

const SessionMode = enum { normal, chaos, deadline };

const Session = struct {
    allocator: std.mem.Allocator,
    conn: *Connection,
    peer: *Peer,
    totals: *Totals,
    mode: SessionMode,
    calls_target: u32,
    calls_done: u32 = 0,
    bootstrap_import_id: ?u32 = null,
    failed: bool = false,

    fn onBootstrapReturn(
        ctx: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *Session = @ptrCast(@alignCast(ctx));
        if (ret.tag != .results) {
            self.noteException(ret);
            self.conn.close();
            return;
        }
        const payload = ret.results orelse return error.MissingPayload;
        const cap = try payload.content.getCapability();
        const resolved = try caps.resolveCapability(cap);
        switch (resolved) {
            .imported => |imported| self.bootstrap_import_id = imported.id,
            else => return error.UnexpectedResolvedCapability,
        }
        try self.sendNextCall(peer);
    }

    fn buildEmptyCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        _ = try call.initCapTableTyped(0);
    }

    fn sendNextCall(self: *Session, peer: *Peer) anyerror!void {
        const target = self.bootstrap_import_id orelse return error.MissingBootstrapImport;
        // Deadline sessions call the slow server method so the 1ms call
        // deadline expires before the (10ms-delayed) Return arrives.
        const method_id: u16 = if (self.mode == .deadline) EchoServer.slow_method_id else 1;
        _ = try peer.sendCallResolved(
            .{ .imported = .{ .id = target } },
            0x5050_5050,
            method_id,
            self,
            buildEmptyCall,
            onCallReturn,
        );
    }

    fn onCallReturn(
        ctx: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *Session = @ptrCast(@alignCast(ctx));
        switch (ret.tag) {
            .results => _ = self.totals.calls_ok.fetchAdd(1, .monotonic),
            .exception => self.noteException(ret),
            else => {},
        }
        self.calls_done += 1;

        if (self.calls_done >= self.calls_target) {
            self.conn.close();
            return;
        }

        // Chaos: leave one call in flight and rip the connection down.
        if (self.mode == .chaos and self.calls_done == self.calls_target / 2) {
            self.sendNextCall(peer) catch {};
            _ = self.totals.chaos_closes.fetchAdd(1, .monotonic);
            self.conn.close();
            return;
        }

        self.sendNextCall(peer) catch |err| {
            if (err == error.PeerShuttingDown) return;
            self.failed = true;
            self.conn.close();
        };
    }

    fn noteException(self: *Session, ret: protocol.Return) void {
        const reason = if (ret.exception) |ex| ex.reason else "";
        if (std.mem.eql(u8, reason, "deadline exceeded")) {
            _ = self.totals.calls_cancelled.fetchAdd(1, .monotonic);
        } else {
            _ = self.totals.unexpected_exceptions.fetchAdd(1, .monotonic);
            std.debug.print("soak: unexpected exception reason: '{s}'\n", .{reason});
        }
    }

    // Transport errors are expected during chaos sessions; counted at the
    // session level in runSession teardown instead.
    fn onPeerError(_: *Peer, _: anyerror) void {}

    fn onPeerClose(_: *Peer) void {}
};

const Worker = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    address: net.IpAddress,
    cfg: *const Config,
    totals: *Totals,
    stop_at_ns: i64,
    index: u32,

    fn main(self: Worker) void {
        var session_index: u64 = 0;
        while (nowNs(self.io) < self.stop_at_ns) : (session_index += 1) {
            self.runSession(session_index) catch |err| {
                _ = self.totals.transport_errors.fetchAdd(1, .monotonic);
                if (err != error.ConnectionRefused) {
                    std.debug.print("soak: worker {} session error: {}\n", .{ self.index, err });
                }
                // Brief backoff so a failing endpoint cannot spin the loop.
                sleepMs(self.io, 5);
            };
        }
    }

    fn pickMode(self: Worker, session_index: u64) SessionMode {
        if (self.cfg.chaos and session_index % 5 == 1) return .chaos;
        if (self.cfg.deadlines and session_index % 4 == 2) return .deadline;
        return .normal;
    }

    fn runSession(self: Worker, session_index: u64) !void {
        const stream = try net.IpAddress.connect(&self.address, self.io, .{ .mode = .stream });
        const fd = stream.socket.handle;

        const conn = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(conn);
        conn.* = try Connection.init(self.allocator, self.io, .{ .handle = fd }, .{
            .tick_interval_ms = 5,
            .idle_timeout_ms = 2_000,
        });

        const peer = try self.allocator.create(Peer);
        errdefer self.allocator.destroy(peer);
        peer.* = Peer.init(self.allocator, conn);

        const mode = self.pickMode(session_index);
        peer.setClockIo(self.io);
        if (mode == .deadline) {
            peer.setTimeouts(.{ .default_call_timeout_ms = 1 });
        }

        var session = Session{
            .allocator = self.allocator,
            .conn = conn,
            .peer = peer,
            .totals = self.totals,
            .mode = mode,
            // Each deadline-session call burns a 10ms server sleep; keep
            // those sessions short so they don't starve the pool workers.
            .calls_target = if (mode == .deadline)
                @min(self.cfg.calls_per_session, 6)
            else
                self.cfg.calls_per_session,
        };

        peer.start(Session.onPeerError, Session.onPeerClose);
        _ = try peer.sendBootstrap(&session, Session.onBootstrapReturn);

        conn.run();

        _ = self.totals.sessions.fetchAdd(1, .monotonic);
        if (session.failed) {
            _ = self.totals.transport_errors.fetchAdd(1, .monotonic);
        }

        _ = peer.takeAttachedConnection(*Connection);
        peer.deinit();
        self.allocator.destroy(peer);
        conn.deinit();
        self.allocator.destroy(conn);
    }
};

// -- Helpers -----------------------------------------------------------------

fn sleepMs(io: std.Io, ms: u64) void {
    const duration: std.Io.Clock.Duration = .{
        .raw = .{ .nanoseconds = @as(i96, @intCast(ms)) * std.time.ns_per_ms },
        .clock = .awake,
    };
    duration.sleep(io) catch {};
}

fn getSockPort(fd: std.posix.fd_t) !u16 {
    var storage: std.posix.sockaddr.storage = undefined;
    var addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.storage);
    if (std.posix.errno(std.posix.system.getsockname(fd, @ptrCast(&storage), &addr_len)) != .SUCCESS) {
        return error.GetSockNameFailed;
    }
    const sa: *const std.posix.sockaddr = @ptrCast(&storage);
    if (sa.family == std.posix.AF.INET) {
        const sa_in: *const std.posix.sockaddr.in = @ptrCast(@alignCast(sa));
        return std.mem.bigToNative(u16, sa_in.port);
    }
    return error.UnsupportedAddressFamily;
}

fn parseArgs(allocator: std.mem.Allocator, args: std.process.Args) !Config {
    var cfg = Config{};
    // initAllocator is the cross-platform form; plain init is a compile
    // error on Windows. All values parse to integers, so nothing
    // outlives the iterator.
    var iter = try std.process.Args.Iterator.initAllocator(args, allocator);
    defer iter.deinit();
    _ = iter.skip();
    while (iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--seconds")) {
            cfg.seconds = try std.fmt.parseUnsigned(u64, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--workers")) {
            cfg.workers = try std.fmt.parseUnsigned(u32, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--calls")) {
            cfg.calls_per_session = try std.fmt.parseUnsigned(u32, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--no-chaos")) {
            cfg.chaos = false;
        } else if (std.mem.eql(u8, arg, "--no-deadlines")) {
            cfg.deadlines = false;
        } else {
            std.debug.print("soak: unknown argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.workers == 0 or cfg.calls_per_session == 0) return error.InvalidArgument;
    return cfg;
}

pub fn main(init: std.process.Init) !void {
    if (comptime builtin.target.os.tag == .windows) {
        std.debug.print("soak: unsupported on Windows (tick/poll path unavailable)\n", .{});
        return;
    }

    var gpa: std.heap.DebugAllocator(.{ .thread_safe = true }) = .init;
    const allocator = gpa.allocator();
    const io = init.io;

    const cfg = try parseArgs(allocator, init.minimal.args);

    var totals = Totals{};
    EchoServer.server_io = io;

    var pool = try WorkerPool.init(
        allocator,
        io,
        .{ .ip4 = .loopback(0) },
        @ptrCast(&totals),
        EchoServer.onAccept,
        .{ .concurrency = @max(2, cfg.workers / 2) },
    );
    const port = try getSockPort(pool.server.socket.handle);
    const address: net.IpAddress = .{ .ip4 = .loopback(port) };

    const pool_thread = try std.Thread.spawn(.{}, poolThreadMain, .{&pool});
    std.debug.print("soak: server listening on port {} (pool concurrency {})\n", .{ port, @max(2, cfg.workers / 2) });

    const stop_at_ns = nowNs(io) + @as(i64, @intCast(cfg.seconds)) * std.time.ns_per_s;

    const worker_threads = try allocator.alloc(std.Thread, cfg.workers);
    for (worker_threads, 0..) |*t, i| {
        t.* = try std.Thread.spawn(.{}, Worker.main, .{Worker{
            .allocator = allocator,
            .io = io,
            .address = address,
            .cfg = &cfg,
            .totals = &totals,
            .stop_at_ns = stop_at_ns,
            .index = @intCast(i),
        }});
    }
    for (worker_threads) |t| t.join();
    allocator.free(worker_threads);
    std.debug.print("soak: workers joined, draining pool\n", .{});

    pool.shutdownGraceful(2_000);
    std.debug.print("soak: pool drained, joining pool thread\n", .{});
    pool_thread.join();
    pool.deinit();
    std.debug.print("soak: pool deinit complete\n", .{});

    const sessions = totals.sessions.load(.acquire);
    const ok = totals.calls_ok.load(.acquire);
    const cancelled = totals.calls_cancelled.load(.acquire);
    const chaos_closes = totals.chaos_closes.load(.acquire);
    const transport_errors = totals.transport_errors.load(.acquire);
    const unexpected = totals.unexpected_exceptions.load(.acquire);

    std.debug.print(
        "soak: sessions={} calls_ok={} cancelled={} chaos_closes={} transport_errors={} unexpected_exceptions={}\n",
        .{ sessions, ok, cancelled, chaos_closes, transport_errors, unexpected },
    );

    var failed = false;
    if (sessions == 0 or ok == 0) {
        std.debug.print("soak: FAIL — no successful traffic\n", .{});
        failed = true;
    }
    if (unexpected != 0) {
        std.debug.print("soak: FAIL — unexpected exception reasons observed\n", .{});
        failed = true;
    }
    if (cfg.deadlines and cancelled == 0 and sessions > 8) {
        std.debug.print("soak: FAIL — deadline sessions produced no cancellations\n", .{});
        failed = true;
    }
    if (gpa.deinit() != .ok) {
        std.debug.print("soak: FAIL — client-side allocation leaks detected\n", .{});
        failed = true;
    }
    if (failed) return error.SoakFailed;
    std.debug.print("soak: PASS\n", .{});
}
