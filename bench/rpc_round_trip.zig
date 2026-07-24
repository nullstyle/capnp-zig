//! RPC round-trip benchmark: measures an actual Cap'n Proto RPC call
//! round-trip over real loopback TCP with a warmed, steady-state client.
//!
//! A WorkerPool server exports an echo bootstrap capability. A single client
//! peer connects, bootstraps once, then issues N calls and records the
//! wall-clock latency of each round-trip (send → Return). Two modes:
//!
//!   sequential — one outstanding call at a time (classic round-trip latency)
//!   pipelined  — up to `--inflight K` outstanding calls at once (throughput)
//!
//! Reports p50 / p99 / max latency (ns) and calls/sec for the timed window,
//! after a warmup phase that is excluded from the statistics.
//!
//! Emitting `--json` prints a machine-readable line consumed by
//! tools/bench_check.zig for regression gating (metrics: p50_ns, p99_ns,
//! max_ns, calls_per_sec).
//!
//! Usage: zig build bench-rpc -- [--mode sequential|pipelined] [--calls N]
//!                               [--warmup N] [--inflight K] [--json]

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

const Mode = enum { sequential, pipelined };

const Config = struct {
    mode: Mode = .sequential,
    calls: u32 = 20_000,
    warmup: u32 = 2_000,
    inflight: u32 = 16,
    json: bool = false,
};

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

// -- Server ------------------------------------------------------------------

const EchoServer = struct {
    // A stable non-null ctx pointer for the bootstrap export. The echo handler
    // ignores it, but the peer's call orchestration requires a non-null ctx.
    var ctx_anchor: u8 = 0;

    fn onCall(
        _: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        try peer.sendReturnEmptyStruct(call.question_id);
    }

    fn onAccept(
        _: *anyopaque,
        peer: *Peer,
        _: *Connection,
        _: u32,
    ) anyerror!WorkerPool.AcceptDecision {
        _ = try peer.setBootstrap(.{ .ctx = @ptrCast(&ctx_anchor), .on_call = EchoServer.onCall });
        peer.start(null, null, null);
        return .accept;
    }
};

fn poolThreadMain(pool: *WorkerPool) void {
    pool.run() catch |err| {
        std.debug.print("bench-rpc: worker pool run failed: {}\n", .{err});
    };
}

// -- Client session ----------------------------------------------------------

/// Drives the warmed round-trip loop entirely from Peer callbacks. Timestamps
/// are captured at call-issue time and closed out in onCallReturn, so each
/// recorded sample is a true send→Return wall-clock latency.
const Session = struct {
    io: std.Io,
    conn: *Connection,
    cfg: *const Config,
    // Recorded latencies, in nanoseconds, for the timed (post-warmup) window.
    samples: []u64,
    // Per-outstanding-question send timestamps, indexed by inflight slot.
    send_ts: []u64,

    bootstrap_import_id: ?u32 = null,

    // issued counts every call ever sent; completed counts every Return seen.
    issued: u32 = 0,
    completed: u32 = 0,
    recorded: u32 = 0,
    failed: bool = false,

    // Wall-clock bounds of the timed window (excludes warmup).
    timed_start_ns: u64 = 0,
    timed_end_ns: u64 = 0,

    fn totalCalls(self: *const Session) u32 {
        return self.cfg.warmup + self.cfg.calls;
    }

    fn maxInflight(self: *const Session) u32 {
        return switch (self.cfg.mode) {
            .sequential => 1,
            .pipelined => self.cfg.inflight,
        };
    }

    fn onBootstrapReturn(
        ctx: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *Session = @ptrCast(@alignCast(ctx));
        if (ret.tag != .results) {
            self.failed = true;
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
        // Prime the pipeline up to the inflight ceiling.
        try self.pump(peer);
    }

    fn buildEmptyCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        _ = try call.initCapTableTyped(0);
    }

    /// Issue calls until either the inflight ceiling or the total call budget
    /// is reached. Each issued call captures its send timestamp in a slot
    /// keyed by (issued % maxInflight) — safe because at most maxInflight
    /// calls are ever outstanding at once.
    fn pump(self: *Session, peer: *Peer) !void {
        const target = self.bootstrap_import_id orelse return error.MissingBootstrapImport;
        const ceiling = self.maxInflight();
        while (self.issued < self.totalCalls() and (self.issued - self.completed) < ceiling) {
            // The first post-warmup issue opens the timed window.
            if (self.issued == self.cfg.warmup) {
                self.timed_start_ns = nowNs(self.io);
            }
            const slot = self.issued % ceiling;
            self.send_ts[slot] = nowNs(self.io);
            _ = try peer.sendCallResolved(
                .{ .imported = .{ .id = target } },
                0x5050_5050,
                1,
                self,
                buildEmptyCall,
                onCallReturn,
            );
            self.issued += 1;
        }
    }

    fn onCallReturn(
        ctx: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *Session = @ptrCast(@alignCast(ctx));
        const end_ns = nowNs(self.io);

        if (ret.tag != .results) {
            self.failed = true;
            self.conn.close();
            return;
        }

        const done_index = self.completed;
        self.completed += 1;

        // Record latency only for the timed window. Returns arrive in issue
        // order for sequential mode; for pipelined mode we key the timestamp
        // slot off the completion index, which matches issue order because
        // the server answers FIFO on a single connection.
        if (done_index >= self.cfg.warmup) {
            const ceiling = self.maxInflight();
            const slot = done_index % ceiling;
            const latency = end_ns -| self.send_ts[slot];
            self.samples[self.recorded] = latency;
            self.recorded += 1;
        }

        if (self.completed >= self.totalCalls()) {
            self.timed_end_ns = nowNs(self.io);
            self.conn.close();
            return;
        }

        self.pump(peer) catch |err| {
            if (err == error.PeerShuttingDown) return;
            self.failed = true;
            self.conn.close();
        };
    }

    fn onPeerError(_: ?*anyopaque, _: *Peer, _: anyerror) void {}
    fn onPeerClose(_: ?*anyopaque, _: *Peer) void {}
};

// -- Statistics --------------------------------------------------------------

const Stats = struct {
    p50_ns: f64,
    p99_ns: f64,
    max_ns: f64,
    min_ns: f64,
    mean_ns: f64,
    calls_per_sec: f64,
    samples: usize,
};

fn percentile(sorted: []const u64, pct: f64) u64 {
    if (sorted.len == 0) return 0;
    if (sorted.len == 1) return sorted[0];
    // Nearest-rank on a 0-based sorted slice.
    const rank = pct / 100.0 * @as(f64, @floatFromInt(sorted.len - 1));
    const idx: usize = @intFromFloat(@round(rank));
    return sorted[@min(idx, sorted.len - 1)];
}

fn computeStats(samples: []u64, timed_ns: u64) Stats {
    std.mem.sort(u64, samples, {}, std.sort.asc(u64));
    var sum: u128 = 0;
    for (samples) |s| sum += s;
    const n = samples.len;
    const n_f: f64 = @floatFromInt(n);
    const timed_s = @as(f64, @floatFromInt(timed_ns)) / 1_000_000_000.0;
    return .{
        .p50_ns = @floatFromInt(percentile(samples, 50.0)),
        .p99_ns = @floatFromInt(percentile(samples, 99.0)),
        .max_ns = @floatFromInt(if (n > 0) samples[n - 1] else 0),
        .min_ns = @floatFromInt(if (n > 0) samples[0] else 0),
        .mean_ns = if (n > 0) @as(f64, @floatFromInt(@as(u64, @intCast(sum)))) / n_f else 0,
        .calls_per_sec = if (timed_s > 0) n_f / timed_s else 0,
        .samples = n,
    };
}

// -- Argument parsing --------------------------------------------------------

fn parseU32(arg: []const u8) !u32 {
    return std.fmt.parseUnsigned(u32, arg, 10);
}

fn parseArgs(allocator: std.mem.Allocator, args: std.process.Args) !?Config {
    var cfg = Config{};
    var iter = try std.process.Args.Iterator.initAllocator(args, allocator);
    defer iter.deinit();
    _ = iter.skip();
    while (iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--mode")) {
            const val = iter.next() orelse return error.InvalidArgument;
            if (std.mem.eql(u8, val, "sequential")) {
                cfg.mode = .sequential;
            } else if (std.mem.eql(u8, val, "pipelined")) {
                cfg.mode = .pipelined;
            } else {
                std.debug.print("bench-rpc: unknown mode: {s}\n", .{val});
                return error.InvalidArgument;
            }
        } else if (std.mem.eql(u8, arg, "--calls")) {
            cfg.calls = try parseU32(iter.next() orelse return error.InvalidArgument);
        } else if (std.mem.eql(u8, arg, "--warmup")) {
            cfg.warmup = try parseU32(iter.next() orelse return error.InvalidArgument);
        } else if (std.mem.eql(u8, arg, "--inflight")) {
            cfg.inflight = try parseU32(iter.next() orelse return error.InvalidArgument);
        } else if (std.mem.eql(u8, arg, "--json")) {
            cfg.json = true;
        } else if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            printUsage();
            return null;
        } else {
            std.debug.print("bench-rpc: unknown argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.calls == 0) return error.InvalidArgument;
    if (cfg.inflight == 0) return error.InvalidArgument;
    return cfg;
}

fn printUsage() void {
    std.debug.print(
        \\Usage: zig build bench-rpc -- [options]
        \\  --mode M     sequential (default) or pipelined
        \\  --calls N    Timed calls (default: 20000)
        \\  --warmup N   Warmup calls, excluded from stats (default: 2000)
        \\  --inflight K Max outstanding calls in pipelined mode (default: 16)
        \\  --json       Emit machine-readable JSON
        \\  -h, --help   Show this help
        \\
    , .{});
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

pub fn main(init: std.process.Init) !void {
    if (comptime builtin.target.os.tag == .windows) {
        std.debug.print("bench-rpc: unsupported on Windows (tick/poll path unavailable)\n", .{});
        return;
    }

    // Keep timings close to release behavior; the client path is single
    // connection so a fast general-purpose allocator is appropriate.
    const allocator = std.heap.smp_allocator;
    const io = init.io;

    const cfg = (parseArgs(allocator, init.minimal.args) catch |err| {
        std.debug.print("bench-rpc: argument error: {s}\n", .{@errorName(err)});
        return;
    }) orelse return;

    // -- Stand up the loopback echo server --------------------------------
    var pool = try WorkerPool.init(
        allocator,
        io,
        .{ .ip4 = .loopback(0) },
        undefined,
        EchoServer.onAccept,
        .{ .concurrency = 2 },
    );
    const port = try getSockPort(pool.server.socket.handle);
    const address: net.IpAddress = .{ .ip4 = .loopback(port) };

    const pool_thread = try std.Thread.spawn(.{}, poolThreadMain, .{&pool});

    // -- Client connection ------------------------------------------------
    const stream = try net.IpAddress.connect(&address, io, .{ .mode = .stream });
    const fd = stream.socket.handle;
    // Match the real client transport (ClientSession sets TCP_NODELAY on
    // connect): without it the sequential mode's small frame writes hit the
    // Nagle/delayed-ACK interaction — ~40ms per round trip on Linux — and the
    // bench measures the kernel's coalescing timer instead of the RPC stack.
    rpc.transport.tcp.runtime.setTcpNoDelay(.{ .handle = fd });

    const conn = try allocator.create(Connection);
    conn.* = try Connection.init(allocator, io, .{ .handle = fd }, .{
        .tick_interval_ms = 5,
        .idle_timeout_ms = 30_000,
    });

    const peer = try allocator.create(Peer);
    peer.* = Peer.init(allocator, conn);
    peer.setClockIo(io);

    const samples = try allocator.alloc(u64, cfg.calls);
    defer allocator.free(samples);
    const ceiling = switch (cfg.mode) {
        .sequential => @as(u32, 1),
        .pipelined => cfg.inflight,
    };
    const send_ts = try allocator.alloc(u64, ceiling);
    defer allocator.free(send_ts);

    var session = Session{
        .io = io,
        .conn = conn,
        .cfg = &cfg,
        .samples = samples,
        .send_ts = send_ts,
    };

    peer.start(null, Session.onPeerError, Session.onPeerClose);
    _ = try peer.sendBootstrap(&session, Session.onBootstrapReturn);

    // Blocks until the session closes the connection (all calls returned).
    conn.run();

    _ = peer.takeAttachedConnection(*Connection);
    peer.deinit();
    allocator.destroy(peer);
    conn.deinit();
    allocator.destroy(conn);

    // -- Drain the server -------------------------------------------------
    pool.shutdownGraceful(2_000);
    pool_thread.join();
    pool.deinit();

    if (session.failed) {
        std.debug.print("bench-rpc: FAIL — session encountered an error\n", .{});
        return error.BenchRpcFailed;
    }
    if (session.recorded == 0) {
        std.debug.print("bench-rpc: FAIL — no timed samples recorded\n", .{});
        return error.BenchRpcFailed;
    }

    const timed_ns = session.timed_end_ns -| session.timed_start_ns;
    const stats = computeStats(session.samples[0..session.recorded], timed_ns);

    if (cfg.json) {
        var out_buffer: [1024]u8 = undefined;
        var out = std.Io.File.stdout().writer(io, &out_buffer);
        try out.interface.print(
            "{{\"benchmark\":\"rpc_round_trip\",\"mode\":\"{s}\",\"calls\":{d},\"warmup\":{d},\"inflight\":{d},\"samples\":{d},\"timed_ns\":{d},\"p50_ns\":{d:.3},\"p99_ns\":{d:.3},\"max_ns\":{d:.3},\"min_ns\":{d:.3},\"mean_ns\":{d:.3},\"calls_per_sec\":{d:.3}}}\n",
            .{
                @tagName(cfg.mode),
                cfg.calls,
                cfg.warmup,
                if (cfg.mode == .pipelined) cfg.inflight else @as(u32, 1),
                stats.samples,
                timed_ns,
                stats.p50_ns,
                stats.p99_ns,
                stats.max_ns,
                stats.min_ns,
                stats.mean_ns,
                stats.calls_per_sec,
            },
        );
        try out.interface.flush();
        return;
    }

    var out_buffer: [2048]u8 = undefined;
    var out = std.Io.File.stdout().writer(io, &out_buffer);
    try out.interface.print("RPC round-trip benchmark ({s})\n", .{@tagName(cfg.mode)});
    try out.interface.print("timed calls: {d} (warmup {d})\n", .{ cfg.calls, cfg.warmup });
    if (cfg.mode == .pipelined) try out.interface.print("max inflight: {d}\n", .{cfg.inflight});
    try out.interface.print("samples: {d}\n", .{stats.samples});
    try out.interface.print("timed window: {d:.3} ms\n", .{@as(f64, @floatFromInt(timed_ns)) / 1_000_000.0});
    try out.interface.print("p50 latency: {d:.0} ns\n", .{stats.p50_ns});
    try out.interface.print("p99 latency: {d:.0} ns\n", .{stats.p99_ns});
    try out.interface.print("max latency: {d:.0} ns\n", .{stats.max_ns});
    try out.interface.print("min latency: {d:.0} ns\n", .{stats.min_ns});
    try out.interface.print("mean latency: {d:.0} ns\n", .{stats.mean_ns});
    try out.interface.print("calls/sec: {d:.1}\n", .{stats.calls_per_sec});
    try out.interface.flush();
}
