//! QUIC RPC benchmark: measures real Cap'n Proto RPC over a real loopback
//! QUIC connection, with TLS, a warmed client, and a live congestion
//! controller.
//!
//! ## Why this exists
//!
//! `bench/rpc_round_trip.zig` is TCP-only, so nothing in this repo measured
//! the QUIC transport at all — a green `bench-check` said nothing about QUIC
//! because no benchmark touched it. This closes that gap for the transport
//! itself; see the scope note below for the part it does not close.
//!
//! ## Why there is a bulk mode
//!
//! Small round-trips are the wrong instrument for congestion control: one
//! in-flight request never fills a window. `bulk` pushes large payloads with
//! several transfers outstanding and reports bytes/sec.
//!
//! ## What this DOES and DOES NOT prove — measured, not assumed
//!
//! It measures our QUIC stack end to end (TLS handshake, framing, stream
//! scheduling, the Peer call path) and will catch a regression in any of them.
//!
//! It does NOT separate congestion-control variants, and `--no-pacing` exists
//! to show that rather than to hide it. Measured on loopback, 300x64KiB:
//!
//!     pacing on   11.44  11.73  12.52  MiB/s
//!     pacing off  11.30  11.37  11.74  MiB/s
//!
//! The distributions overlap: a ~4% difference in means sits inside a ~9%
//! run-to-run spread. That is structural, not a sampling problem — loopback
//! has no bottleneck, so cwnd grows unbounded, the pacer's rate ceiling never
//! binds, and the limiting factor is our own serialization and frame handling
//! rather than the network. Validating CUBIC/pacing/HyStart++ needs a
//! constrained link, which is what upstream's QUIC Network Simulator and
//! quic-go interop gates are for; it is not something this benchmark can
//! honestly claim.
//!
//! So: treat a change here as a signal about OUR transport, not upstream's
//! congestion defaults. And note the noise floor above before reading small
//! movements as regressions — the baselines are toleranced accordingly.
//!
//! Modes:
//!   sequential — one outstanding call (round-trip latency; p50/p99)
//!   pipelined  — up to `--inflight K` outstanding (call throughput)
//!   bulk       — large payloads, `--payload N` bytes each (bytes/sec)
//!
//! `--json` prints the machine-readable line `tools/bench_check.zig` gates on
//! (metrics: p50_ns, p99_ns, max_ns, calls_per_sec, bytes_per_sec).
//!
//! Usage: zig build -Dquic=true bench-quic -- [--mode sequential|pipelined|bulk]
//!            [--calls N] [--warmup N] [--inflight K] [--payload BYTES] [--json]

const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const rpc = capnpc.rpc;
const protocol = rpc.wire.protocol;
const cap_table = rpc.caps.table;
const Peer = rpc.peer.Peer;
const quic = rpc.transport.quic;

const cert_pem = @embedFile("quic_bench_cert");
const key_pem = @embedFile("quic_bench_key");

const Mode = enum { sequential, pipelined, bulk };

const Config = struct {
    mode: Mode = .sequential,
    calls: u32 = 2_000,
    warmup: u32 = 200,
    inflight: u32 = 16,
    /// Bytes of call payload. Only `bulk` defaults this non-zero; the latency
    /// modes deliberately send empty calls so they measure the stack rather
    /// than memcpy.
    payload: u32 = 0,
    json: bool = false,
    /// Disable packet pacing on the client. Present so the benchmark can
    /// demonstrate that it actually observes congestion configuration —
    /// a throughput benchmark whose number does not move when the pacer is
    /// switched off is not measuring the pacer.
    no_pacing: bool = false,
};

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

// -- Server ------------------------------------------------------------------

const EchoServer = struct {
    var ctx_anchor: u8 = 0;

    fn onCall(
        _: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        // Empty Return regardless of request size: the measurement is the
        // client's send path plus one round trip, and echoing the payload
        // back would make every sample two bulk transfers instead of one.
        try peer.sendReturnEmptyStruct(call.question_id);
    }
};

fn runConnection(conn: *quic.Connection) void {
    conn.run();
}

// -- Client session ----------------------------------------------------------

const Session = struct {
    io: std.Io,
    conn: *quic.Connection,
    cfg: *const Config,
    samples: []u64,
    send_ts: []u64,
    /// Reused call payload buffer; filled once, not per call.
    payload_buf: []u8,

    bootstrap_import_id: ?u32 = null,

    issued: u32 = 0,
    completed: u32 = 0,
    recorded: u32 = 0,
    failed: bool = false,

    timed_start_ns: u64 = 0,
    timed_end_ns: u64 = 0,
    /// Payload bytes issued inside the timed window, for bytes/sec.
    timed_payload_bytes: u64 = 0,

    fn totalCalls(self: *const Session) u32 {
        return self.cfg.warmup + self.cfg.calls;
    }

    fn maxInflight(self: *const Session) u32 {
        return switch (self.cfg.mode) {
            .sequential => 1,
            // Bulk keeps several transfers in flight on purpose: a single
            // outstanding large call would idle the link between round trips
            // and measure latency wearing a throughput costume.
            .pipelined, .bulk => self.cfg.inflight,
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
        try self.pump(peer);
    }

    fn buildCall(ctx: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const self: *Session = @ptrCast(@alignCast(ctx));
        if (self.payload_buf.len == 0) {
            _ = try call.initCapTableTyped(0);
            return;
        }
        var payload = try call.payloadTyped();
        try payload.setContentData(self.payload_buf);
        _ = try call.initCapTableTyped(0);
    }

    fn pump(self: *Session, peer: *Peer) !void {
        const target = self.bootstrap_import_id orelse return error.MissingBootstrapImport;
        const ceiling = self.maxInflight();
        while (self.issued < self.totalCalls() and (self.issued - self.completed) < ceiling) {
            if (self.issued == self.cfg.warmup) {
                self.timed_start_ns = nowNs(self.io);
            }
            const slot = self.issued % ceiling;
            self.send_ts[slot] = nowNs(self.io);
            _ = try peer.sendCallResolved(
                .{ .imported = .{ .id = target } },
                0x5155_4943,
                1,
                self,
                buildCall,
                onCallReturn,
            );
            if (self.issued >= self.cfg.warmup) {
                self.timed_payload_bytes += self.payload_buf.len;
            }
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
    samples: usize,
    timed_ns: u64,
    p50_ns: f64,
    p99_ns: f64,
    max_ns: f64,
    min_ns: f64,
    mean_ns: f64,
    calls_per_sec: f64,
    bytes_per_sec: f64,
};

fn percentile(sorted: []const u64, pct: f64) u64 {
    if (sorted.len == 0) return 0;
    const rank = pct / 100.0 * @as(f64, @floatFromInt(sorted.len - 1));
    const idx: usize = @intFromFloat(@round(rank));
    return sorted[@min(idx, sorted.len - 1)];
}

fn computeStats(samples: []u64, timed_ns: u64, payload_bytes: u64) Stats {
    std.mem.sort(u64, samples, {}, std.sort.asc(u64));
    var total: u128 = 0;
    for (samples) |s| total += s;
    const n_f: f64 = @floatFromInt(samples.len);
    const timed_s = @as(f64, @floatFromInt(timed_ns)) / std.time.ns_per_s;
    return .{
        .samples = samples.len,
        .timed_ns = timed_ns,
        .p50_ns = @floatFromInt(percentile(samples, 50.0)),
        .p99_ns = @floatFromInt(percentile(samples, 99.0)),
        .max_ns = if (samples.len > 0) @floatFromInt(samples[samples.len - 1]) else 0,
        .min_ns = if (samples.len > 0) @floatFromInt(samples[0]) else 0,
        .mean_ns = if (samples.len > 0) @as(f64, @floatFromInt(@as(u64, @intCast(total)))) / n_f else 0,
        .calls_per_sec = if (timed_s > 0) n_f / timed_s else 0,
        .bytes_per_sec = if (timed_s > 0) @as(f64, @floatFromInt(payload_bytes)) / timed_s else 0,
    };
}

fn parseU32(arg: []const u8) !u32 {
    return std.fmt.parseInt(u32, arg, 10);
}

fn printUsage() void {
    std.debug.print(
        \\bench-quic — Cap'n Proto RPC over loopback QUIC
        \\
        \\  --mode M     sequential | pipelined | bulk  (default sequential)
        \\  --calls N    timed calls (default 2000)
        \\  --warmup N   untimed warmup calls (default 200)
        \\  --inflight K outstanding calls for pipelined/bulk (default 16)
        \\  --payload N  call payload bytes; bulk defaults to 65536
        \\  --no-pacing  disable client packet pacing (A/B the congestion config)
        \\  --json       emit machine-readable JSON
        \\
    , .{});
}

fn parseArgs(allocator: std.mem.Allocator, args: std.process.Args) !?Config {
    var cfg = Config{};
    var payload_set = false;
    var iter = try std.process.Args.Iterator.initAllocator(args, allocator);
    defer iter.deinit();
    _ = iter.skip();
    while (iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--mode")) {
            const v = iter.next() orelse return error.InvalidArgument;
            cfg.mode = std.meta.stringToEnum(Mode, v) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--calls")) {
            cfg.calls = try parseU32(iter.next() orelse return error.InvalidArgument);
        } else if (std.mem.eql(u8, arg, "--warmup")) {
            cfg.warmup = try parseU32(iter.next() orelse return error.InvalidArgument);
        } else if (std.mem.eql(u8, arg, "--inflight")) {
            cfg.inflight = try parseU32(iter.next() orelse return error.InvalidArgument);
        } else if (std.mem.eql(u8, arg, "--payload")) {
            cfg.payload = try parseU32(iter.next() orelse return error.InvalidArgument);
            payload_set = true;
        } else if (std.mem.eql(u8, arg, "--no-pacing")) {
            cfg.no_pacing = true;
        } else if (std.mem.eql(u8, arg, "--json")) {
            cfg.json = true;
        } else if (std.mem.eql(u8, arg, "--help")) {
            printUsage();
            return null;
        } else {
            std.debug.print("bench-quic: unknown argument '{s}'\n", .{arg});
            printUsage();
            return null;
        }
    }
    // Bulk without a payload would silently measure the same thing as
    // pipelined, so give it a real default rather than a degenerate run.
    if (cfg.mode == .bulk and !payload_set) cfg.payload = 64 * 1024;
    if (cfg.inflight == 0) cfg.inflight = 1;
    return cfg;
}

pub fn main(init: std.process.Init) !void {
    if (comptime builtin.target.os.tag == .windows) {
        std.debug.print("bench-quic: unsupported on Windows (see docs/stability.md)\n", .{});
        return;
    }

    const allocator = std.heap.smp_allocator;
    const io = init.io;

    const cfg = (parseArgs(allocator, init.minimal.args) catch |err| {
        std.debug.print("bench-quic: argument error: {s}\n", .{@errorName(err)});
        return;
    }) orelse return;

    var server_conn = try quic.Connection.initServer(allocator, io, .{
        .listen_addr = .{ .ip4 = .loopback(0) },
        .tls_cert_pem = cert_pem,
        .tls_key_pem = key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
    });
    defer server_conn.deinit();

    var client_conn = try quic.Connection.initClient(allocator, io, .{
        .remote_addr = server_conn.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
        .enable_pacing = !cfg.no_pacing,
    });
    defer client_conn.deinit();

    var server_peer = Peer.init(allocator, &server_conn);
    defer server_peer.deinit();
    server_peer.disableThreadAffinity();
    _ = try server_peer.setBootstrap(.{
        .ctx = @ptrCast(&EchoServer.ctx_anchor),
        .on_call = EchoServer.onCall,
    });
    server_peer.start(null, null, null);

    const samples = try allocator.alloc(u64, cfg.calls);
    defer allocator.free(samples);
    const ceiling: u32 = switch (cfg.mode) {
        .sequential => 1,
        .pipelined, .bulk => cfg.inflight,
    };
    const send_ts = try allocator.alloc(u64, ceiling);
    defer allocator.free(send_ts);
    const payload_buf = try allocator.alloc(u8, cfg.payload);
    defer allocator.free(payload_buf);
    for (payload_buf, 0..) |*b, i| b.* = @truncate(i);

    var client_peer = Peer.init(allocator, &client_conn);
    defer client_peer.deinit();
    client_peer.disableThreadAffinity();

    var session = Session{
        .io = io,
        .conn = &client_conn,
        .cfg = &cfg,
        .samples = samples,
        .send_ts = send_ts,
        .payload_buf = payload_buf,
    };

    client_peer.start(&session, Session.onPeerError, Session.onPeerClose);
    _ = try client_peer.sendBootstrap(&session, Session.onBootstrapReturn);

    const server_thread = try std.Thread.spawn(.{}, runConnection, .{&server_conn});
    client_conn.run();
    server_conn.requestClose();
    server_thread.join();

    if (session.failed or session.recorded == 0) {
        std.debug.print("bench-quic: run failed (recorded={d})\n", .{session.recorded});
        return error.BenchmarkFailed;
    }

    const timed_ns = session.timed_end_ns -| session.timed_start_ns;
    const stats = computeStats(samples[0..session.recorded], timed_ns, session.timed_payload_bytes);

    var out_buffer: [1024]u8 = undefined;
    var out = std.Io.File.stdout().writer(io, &out_buffer);
    if (cfg.json) {
        try out.interface.print(
            "{{\"benchmark\":\"quic_round_trip\",\"mode\":\"{s}\",\"calls\":{d},\"warmup\":{d},\"inflight\":{d},\"payload\":{d},\"samples\":{d},\"timed_ns\":{d},\"p50_ns\":{d:.3},\"p99_ns\":{d:.3},\"max_ns\":{d:.3},\"min_ns\":{d:.3},\"mean_ns\":{d:.3},\"calls_per_sec\":{d:.3},\"bytes_per_sec\":{d:.3}}}\n",
            .{
                @tagName(cfg.mode),
                cfg.calls,
                cfg.warmup,
                ceiling,
                cfg.payload,
                stats.samples,
                stats.timed_ns,
                stats.p50_ns,
                stats.p99_ns,
                stats.max_ns,
                stats.min_ns,
                stats.mean_ns,
                stats.calls_per_sec,
                stats.bytes_per_sec,
            },
        );
    } else {
        try out.interface.print("mode: {s}  payload: {d}B  samples: {d}\n", .{ @tagName(cfg.mode), cfg.payload, stats.samples });
        try out.interface.print("p50 latency: {d:.0} ns\n", .{stats.p50_ns});
        try out.interface.print("p99 latency: {d:.0} ns\n", .{stats.p99_ns});
        try out.interface.print("calls/sec:   {d:.0}\n", .{stats.calls_per_sec});
        if (cfg.payload > 0) {
            try out.interface.print("bytes/sec:   {d:.0} ({d:.1} MiB/s)\n", .{
                stats.bytes_per_sec,
                stats.bytes_per_sec / (1024.0 * 1024.0),
            });
        }
    }
    try out.interface.flush();
}
