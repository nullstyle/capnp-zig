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
//! Beyond counts, the harness reports:
//!   * p50 / p99 / max call latency across all successful round-trips, and
//!   * a periodic live-heap memory-growth curve (allocated − freed bytes,
//!     sampled on a background thread), with a PROGRAMMATIC flat-memory pass
//!     criterion — steady-state live heap must not trend upward beyond
//!     allocator noise, not merely be leak-free at the end.
//!
//! Concurrency scales via --workers (≥100 supported) and per-session
//! outstanding-question depth via --inflight (high-in-flight mode).
//!
//! Exit code is nonzero when invariants fail: zero successful calls, an
//! unexpected exception reason, a client-side allocation leak, or a
//! steady-state live-heap growth beyond the configured threshold.
//!
//! Usage: zig build soak -- [--seconds N] [--workers N] [--calls N]
//!                          [--inflight K] [--mem-sample-ms N]
//!                          [--mem-growth-pct P] [--no-chaos] [--no-deadlines]
//!                          [--transport tcp|quic]  (quic needs -Dquic=true)
//!                          [--cc default|cubic|bbr|newreno]  (quic only)
//!                          [--abrupt-death-every-ms N]  (quic only: kill+restart
//!                           the server with no close ceremony every N ms)
//!                          [--heal-workers K]  (quic only: K workers run
//!                           persistent WarmRedialClients that auto-heal
//!                           across abrupt deaths instead of churn sessions)

const std = @import("std");
const capnpc = @import("capnpc-zig");

const rpc = capnpc.rpc;
const protocol = rpc.wire.protocol;
const cap_table = rpc.caps.table;
const Peer = rpc.peer.Peer;
const Connection = rpc.transport.tcp.Connection;
const quic = rpc.transport.quic;
const WorkerPool = rpc.integration.worker_pool.WorkerPool;
const net = std.Io.net;

const Transport = enum { tcp, quic };

/// Congestion-control selection for the QUIC soak (--cc). A local enum so
/// the TCP-only build never references the quic module's types; mapped to
/// quic-zig's CongestionAlgorithm at the guarded call sites. `default`
/// leaves both sides on the transport's own defaults.
const CcChoice = enum { default, cubic, bbr, newreno };

const Config = struct {
    seconds: u64 = 2,
    workers: u32 = 4,
    // Transport under soak. `quic` requires a -Dquic=true build; the QUIC
    // variant exists so per-connection footprint and steady-state behavior
    // are measurable BEFORE/AFTER a quic-zig pin bump (a rig that cannot
    // observe a change reports "no change").
    transport: Transport = .tcp,
    // QUIC congestion-control override for A/B runs (ignored over tcp).
    cc: CcChoice = .default,
    calls_per_session: u32 = 25,
    // High-in-flight mode: max outstanding questions per session. 1 keeps the
    // classic sequential behavior; larger values keep many calls in flight.
    inflight: u32 = 1,
    chaos: bool = true,
    deadlines: bool = true,
    // Abrupt-death chaos (QUIC only): every N ms the harness KILLS the
    // server with no close ceremony and restarts it on the same port with
    // the same stateless_reset_key — the crash-restart shape the death
    // certificate exists for. Clients' next datagrams draw stateless
    // resets, so sessions close `.stateless_reset` instead of timing out.
    abrupt_death_every_ms: ?u64 = null,
    // Healing workers (QUIC only): the first K workers each run ONE
    // persistent WarmRedialClient for the whole run — restore-backed echo
    // traffic that auto-heals across abrupt server deaths — instead of the
    // churn session loop. Pair with --abrupt-death-every-ms for the
    // churn-scale self-healing proof.
    heal_workers: u32 = 0,
    // Memory-curve sampling interval and the steady-state growth ceiling.
    mem_sample_ms: u64 = 100,
    mem_growth_pct: f64 = 25.0,
};

/// Counters are `usize`, not `u64`, so this harness cross-compiles for
/// 32-bit targets: `@atomicLoad`/`@atomicRmw` reject operands wider than the
/// pointer width, and `zig build check-compile -Dtarget=x86-linux-gnu` builds
/// this file. `usize` is u64 on every machine the soak actually runs on, so
/// the counters are unchanged there; the 32-bit build is compile-only rot
/// detection, never an execution target.
const Totals = struct {
    sessions: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    calls_ok: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    calls_cancelled: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    chaos_closes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    transport_errors: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    expected_disconnects: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    // Disconnect-reason Returns on non-chaos sessions: legitimate at high peer
    // counts (idle-timeout drops under contention), not a correctness failure.
    contention_disconnects: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    unexpected_exceptions: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    // Terminal transport close cause per session (the death certificate):
    // indexed by @intFromEnum(rpc.events.DisconnectCause). TCP sessions all
    // land in .unknown today; QUIC splits local/peer/idle/reset/error.
    disconnects_by_cause: [cause_count]std.atomic.Value(usize) = @splat(std.atomic.Value(usize).init(0)),
};

const cause_count = std.enums.values(rpc.events.DisconnectCause).len;

fn nowNs(io: std.Io) i64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

fn nowNsU(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

// -- Thread-safe live-heap counting allocator --------------------------------
//
// Wraps the backing allocator and tracks live bytes (allocated − freed) so a
// background sampler can watch the heap over time. Counters are atomic so
// worker threads and the sampler never tear a read. This is the "is memory
// flat?" signal: steady churn holds live_bytes roughly constant; a leak or
// unbounded growth makes it climb.

const CountingAllocator = struct {
    backing: std.mem.Allocator,
    allocated: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    freed: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    const vtable = std.mem.Allocator.VTable{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    fn init(backing: std.mem.Allocator) CountingAllocator {
        return .{ .backing = backing };
    }

    fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// Live bytes = allocated − freed. Saturating so a transient read that
    /// observes freed slightly ahead of allocated cannot underflow.
    fn liveBytes(self: *const CountingAllocator) usize {
        return self.allocated.load(.monotonic) -| self.freed.load(.monotonic);
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        _ = self.allocated.fetchAdd(len, .monotonic);
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ok = self.backing.rawResize(memory, alignment, new_len, ret_addr);
        if (ok) {
            if (new_len > memory.len) {
                _ = self.allocated.fetchAdd(new_len - memory.len, .monotonic);
            } else {
                _ = self.freed.fetchAdd(memory.len - new_len, .monotonic);
            }
        }
        return ok;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        if (new_len > memory.len) {
            _ = self.allocated.fetchAdd(new_len - memory.len, .monotonic);
        } else {
            _ = self.freed.fetchAdd(memory.len - new_len, .monotonic);
        }
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        _ = self.freed.fetchAdd(memory.len, .monotonic);
        self.backing.rawFree(memory, alignment, ret_addr);
    }
};

// -- Latency accounting ------------------------------------------------------
//
// Each worker owns its own growable latency buffer (no cross-thread locking on
// the hot path). Buffers are merged after all workers join.

const LatencyBuf = struct {
    /// Telemetry allocator — deliberately NOT the counted allocator the
    /// memory gate watches. Every successful call appends 8 bytes here,
    /// and the buffer is only freed after the memory verdict, so counting
    /// it turns the steady-state gate into a calls-per-run gate. That is
    /// exactly how the Windows soak lane failed every nightly from its
    /// first run (2026-08-12): Windows completes 2-4x the calls of the
    /// Nagle-capped Linux client in the same 120s, and the "growth" was
    /// ~6 B/call of the harness's own samples on both platforms — the
    /// terminal leak check passed on every red run.
    allocator: std.mem.Allocator,
    list: std.ArrayList(u64) = .empty,

    fn record(self: *LatencyBuf, ns: u64) void {
        // A dropped sample under memory pressure is acceptable for a soak
        // percentile estimate; never abort traffic over it.
        self.list.append(self.allocator, ns) catch {};
    }

    fn deinit(self: *LatencyBuf) void {
        self.list.deinit(self.allocator);
    }
};

const Percentiles = struct {
    p50: u64 = 0,
    p99: u64 = 0,
    max: u64 = 0,
    count: usize = 0,
};

fn percentile(sorted: []const u64, pct: f64) u64 {
    if (sorted.len == 0) return 0;
    if (sorted.len == 1) return sorted[0];
    const rank = pct / 100.0 * @as(f64, @floatFromInt(sorted.len - 1));
    const idx: usize = @intFromFloat(@round(rank));
    return sorted[@min(idx, sorted.len - 1)];
}

// -- Server ------------------------------------------------------------------

const EchoServer = struct {
    /// Calls to this method sleep server-side before answering, so client
    /// deadlines (1ms) expire and the cancellation path runs against a
    /// real server that still sends its (late) Return.
    pub const slow_method_id: u16 = 2;
    pub const slow_method_delay_ms: u64 = 10;

    // Stable non-null anchor for the bootstrap export ctx (the handler
    // ignores it, but the call orchestration requires a non-null ctx).
    var ctx_anchor: u8 = 0;
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
        _ = try peer.setBootstrap(.{ .ctx = @ptrCast(&ctx_anchor), .on_call = EchoServer.onCall });
        peer.start(null, null, null);
        return .accept;
    }

    /// The vat-level restore convention the healing workers ride: any peer
    /// presenting the fixed soak sturdy ref gets a fresh echo export. The
    /// re-export-per-restore shape is idempotent at the app layer, which is
    /// what a 0-RTT-riding restore requires.
    const sturdy_ref = "sturdy:soak-echo/1";

    fn onRestore(_: *anyopaque, peer: *Peer, ref: []const u8) anyerror!rpc.peer.RestoreOutcome {
        if (!std.mem.eql(u8, ref, sturdy_ref)) return .unknown;
        return .{ .existing = try peer.addExport(.{ .ctx = @ptrCast(&ctx_anchor), .on_call = onCall }) };
    }
};

fn poolThreadMain(pool: *WorkerPool) void {
    pool.run() catch |err| {
        std.debug.print("soak: worker pool run failed: {}\n", .{err});
    };
}

// -- Client session ----------------------------------------------------------

const SessionMode = enum { normal, chaos, deadline };

fn SessionOf(comptime ConnT: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        io: std.Io,
        conn: *ConnT,
        peer: *Peer,
        totals: *Totals,
        latency: *LatencyBuf,
        mode: SessionMode,
        calls_target: u32,
        // Max outstanding questions for this session. Deadline/chaos sessions run
        // sequentially (inflight 1) so their timing semantics stay exact.
        max_inflight: u32,

        issued: u32 = 0,
        completed: u32 = 0,
        bootstrap_import_id: ?u32 = null,
        chaos_close_initiated: bool = false,
        failed: bool = false,

        // Per-outstanding-question send timestamps, keyed by (index % max_inflight).
        // Sized to MAX_INFLIGHT_SLOTS; --inflight is clamped to it.
        send_ts: [MAX_INFLIGHT_SLOTS]u64 = @splat(0),

        const MAX_INFLIGHT_SLOTS = 256;

        fn onBootstrapReturn(
            ctx: *anyopaque,
            peer: *Peer,
            ret: protocol.Return,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ctx));
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
            try self.pump(peer);
        }

        fn buildEmptyCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            _ = try call.initCapTableTyped(0);
        }

        /// Issue calls up to the inflight ceiling / remaining budget. Chaos
        /// sessions never pump past the half-way rip point.
        fn pump(self: *Self, peer: *Peer) !void {
            const target = self.bootstrap_import_id orelse return error.MissingBootstrapImport;
            // Deadline sessions call the slow server method so the 1ms call
            // deadline expires before the (10ms-delayed) Return arrives.
            const method_id: u16 = if (self.mode == .deadline) EchoServer.slow_method_id else 1;
            while (self.issued < self.calls_target and (self.issued - self.completed) < self.max_inflight) {
                const slot = self.issued % self.max_inflight;
                self.send_ts[slot] = nowNsU(self.io);
                _ = try peer.sendCallResolved(
                    .{ .imported = .{ .id = target } },
                    0x5050_5050,
                    method_id,
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
            const self: *Self = @ptrCast(@alignCast(ctx));
            const done_index = self.completed;
            switch (ret.tag) {
                .results => {
                    _ = self.totals.calls_ok.fetchAdd(1, .monotonic);
                    // Returns are answered FIFO on a single connection, so the
                    // completion index matches issue order for the slot key.
                    const slot = done_index % self.max_inflight;
                    const latency = nowNsU(self.io) -| self.send_ts[slot];
                    self.latency.record(latency);
                },
                .exception => self.noteException(ret),
                else => {},
            }
            self.completed += 1;

            if (self.completed >= self.calls_target) {
                self.conn.close();
                return;
            }

            // Chaos: leave one call in flight and rip the connection down. The
            // peer's close contract fails that in-flight question with a
            // synthetic "disconnected" exception Return, which noteException
            // must treat as the expected outcome for this session.
            if (self.mode == .chaos and self.completed == self.calls_target / 2) {
                self.chaos_close_initiated = true;
                self.sendOne(peer) catch {};
                _ = self.totals.chaos_closes.fetchAdd(1, .monotonic);
                self.conn.close();
                return;
            }

            self.pump(peer) catch |err| {
                if (err == error.PeerShuttingDown) return;
                self.failed = true;
                self.conn.close();
            };
        }

        /// Issue exactly one extra call (used by chaos to leave a question in
        /// flight); ignores the inflight ceiling on purpose.
        fn sendOne(self: *Self, peer: *Peer) !void {
            const target = self.bootstrap_import_id orelse return error.MissingBootstrapImport;
            const method_id: u16 = if (self.mode == .deadline) EchoServer.slow_method_id else 1;
            _ = try peer.sendCallResolved(
                .{ .imported = .{ .id = target } },
                0x5050_5050,
                method_id,
                self,
                buildEmptyCall,
                onCallReturn,
            );
            self.issued += 1;
        }

        fn noteException(self: *Self, ret: protocol.Return) void {
            const reason = if (ret.exception) |ex| ex.reason else "";
            if (std.mem.eql(u8, reason, "deadline exceeded")) {
                _ = self.totals.calls_cancelled.fetchAdd(1, .monotonic);
            } else if (std.mem.eql(u8, reason, rpc.peer.disconnected_reason)) {
                // The peer's close contract fails every in-flight question with a
                // synthetic "disconnected" Return. This is the expected outcome
                // when a chaos session rips its own connection down, and also a
                // legitimate outcome at high peer counts where a starved event
                // loop trips its own idle timeout and drops the connection. Keep
                // the chaos-specific tally exact; count the rest as contention.
                if (self.chaos_close_initiated) {
                    _ = self.totals.expected_disconnects.fetchAdd(1, .monotonic);
                } else {
                    _ = self.totals.contention_disconnects.fetchAdd(1, .monotonic);
                }
            } else {
                _ = self.totals.unexpected_exceptions.fetchAdd(1, .monotonic);
                std.debug.print("soak: unexpected exception reason: '{s}'\n", .{reason});
            }
        }

        fn onPeerError(_: ?*anyopaque, _: *Peer, _: anyerror) void {}
        fn onPeerClose(_: ?*anyopaque, _: *Peer) void {}
    };
}

const Session = SessionOf(Connection);

fn dialTcp(allocator: std.mem.Allocator, io: std.Io, address: net.IpAddress, _: *const Config) anyerror!Connection {
    var addr = address;
    const stream = try net.IpAddress.connect(&addr, io, .{ .mode = .stream });
    return try Connection.init(allocator, io, .{ .handle = stream.socket.handle }, .{
        .tick_interval_ms = 5,
        // Generous idle timeout: under high peer counts a session's event
        // loop can be starved for a while; a tight timeout would trip it
        // and drop otherwise-healthy connections. Deadline cancellation
        // uses the per-call timeout, not this, so keeping it loose is safe.
        .idle_timeout_ms = 15_000,
    });
}

fn dialQuic(allocator: std.mem.Allocator, io: std.Io, address: net.IpAddress, cfg: *const Config) anyerror!quic.Connection {
    var options = quic.ClientOptions{
        .remote_addr = address,
        .server_name = "localhost",
        // Loopback soak against the checked-in self-signed fixture cert.
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
    };
    applyCc(&options.congestion_control, cfg.cc);
    return try quic.Connection.initClient(allocator, io, options);
}

/// Map the soak's local `--cc` choice onto quic-zig's enum; `.default`
/// leaves the transport's own default in place.
fn applyCc(field: anytype, choice: CcChoice) void {
    switch (choice) {
        .default => {},
        .cubic => field.* = .cubic,
        .bbr => field.* = .bbr,
        .newreno => field.* = .new_reno,
    }
}

fn WorkerOf(
    comptime ConnT: type,
    comptime dialFn: fn (std.mem.Allocator, std.Io, net.IpAddress, *const Config) anyerror!ConnT,
) type {
    return struct {
        const Self = @This();
        const SessionT = SessionOf(ConnT);

        allocator: std.mem.Allocator,
        io: std.Io,
        address: net.IpAddress,
        cfg: *const Config,
        totals: *Totals,
        latency: *LatencyBuf,
        stop_at_ns: i64,
        index: u32,

        fn main(self: Self) void {
            var session_index: u64 = 0;
            while (nowNs(self.io) < self.stop_at_ns) : (session_index += 1) {
                self.runSession(session_index) catch |err| {
                    _ = self.totals.transport_errors.fetchAdd(1, .monotonic);
                    if (err != error.ConnectionRefused) {
                        std.debug.print("soak: worker {} session error: {}\n", .{ self.index, err });
                    }
                    sleepMs(self.io, 5);
                };
            }
        }

        fn pickMode(self: Self, session_index: u64) SessionMode {
            if (self.cfg.chaos and session_index % 5 == 1) return .chaos;
            if (self.cfg.deadlines and session_index % 4 == 2) return .deadline;
            return .normal;
        }

        fn runSession(self: Self, session_index: u64) !void {
            const conn = try self.allocator.create(ConnT);
            errdefer self.allocator.destroy(conn);
            conn.* = try dialFn(self.allocator, self.io, self.address, self.cfg);

            const peer = try self.allocator.create(Peer);
            errdefer self.allocator.destroy(peer);
            peer.* = Peer.init(self.allocator, conn);

            const mode = self.pickMode(session_index);
            peer.setClockIo(self.io);
            if (mode == .deadline) {
                peer.setTimeouts(.{ .default_call_timeout_ms = 1 });
            }

            // Only normal sessions run high-in-flight; chaos/deadline keep exact
            // sequential timing so their invariants hold. Clamp to the slot cap.
            const inflight: u32 = switch (mode) {
                .normal => @min(@max(self.cfg.inflight, 1), SessionT.MAX_INFLIGHT_SLOTS),
                else => 1,
            };

            var session = SessionT{
                .allocator = self.allocator,
                .io = self.io,
                .conn = conn,
                .peer = peer,
                .totals = self.totals,
                .latency = self.latency,
                .mode = mode,
                // Each deadline-session call burns a 10ms server sleep; keep
                // those sessions short so they don't starve the pool workers.
                .calls_target = if (mode == .deadline)
                    @min(self.cfg.calls_per_session, 6)
                else
                    self.cfg.calls_per_session,
                .max_inflight = inflight,
            };

            peer.start(null, SessionT.onPeerError, SessionT.onPeerClose);
            _ = try peer.sendBootstrap(&session, SessionT.onBootstrapReturn);

            conn.run();

            _ = self.totals.sessions.fetchAdd(1, .monotonic);
            const cause = peer.lastDisconnectCause();
            _ = self.totals.disconnects_by_cause[@backingInt(cause)].fetchAdd(1, .monotonic);
            if (session.failed) {
                _ = self.totals.transport_errors.fetchAdd(1, .monotonic);
            }

            _ = peer.takeAttachedConnection(*ConnT);
            peer.deinit();
            self.allocator.destroy(peer);
            conn.deinit();
            self.allocator.destroy(conn);
        }
    };
}

const Worker = WorkerOf(Connection, dialTcp);
const QuicWorker = if (quic.enabled) WorkerOf(quic.Connection, dialQuic) else void;

// -- Healing worker (QUIC only) ----------------------------------------------
//
// One persistent WarmRedialClient per healing worker: restore-backed echo
// traffic that chains a new call on every Return (a reset only reaches a
// client that SENDS) and auto-heals across abrupt server deaths. The app
// state mirrors the heal e2e's shape.

const HealApp = if (quic.enabled) struct {
    echo_ok: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    gave_up: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    // Run-thread-only within a generation.
    last_cap: ?cap_table.ResolvedCap = null,
    outcome: ?quic.WarmRedialClient.Outcome = null,

    fn onRebind(ctx: ?*anyopaque, peer: *Peer, cap: cap_table.ResolvedCap) void {
        const self: *HealApp = @ptrCast(@alignCast(ctx.?));
        self.last_cap = cap;
        self.sendEcho(peer, cap);
    }

    fn sendEcho(self: *HealApp, peer: *Peer, cap: cap_table.ResolvedCap) void {
        _ = peer.sendCallResolved(cap, 0, 1, self, null, onEchoReturn) catch {};
    }

    fn onEchoReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        _ = caps;
        const self: *HealApp = @ptrCast(@alignCast(ctx));
        if (ret.tag != .results) return; // the death's synthetic Return ends this chain
        _ = self.echo_ok.fetchAdd(1, .monotonic);
        self.sendEcho(peer, self.last_cap orelse return);
    }

    fn onGiveUp(ctx: ?*anyopaque, cause: rpc.events.DisconnectCause) void {
        const self: *HealApp = @ptrCast(@alignCast(ctx.?));
        _ = cause;
        self.gave_up.store(true, .release);
    }

    fn threadMain(self: *HealApp, client: *quic.WarmRedialClient) void {
        self.outcome = client.run() catch null;
    }
} else void;

// -- QUIC server harness -----------------------------------------------------
//
// TCP's WorkerPool equivalent for the fanout QUIC server: ONE thread owns the
// whole server (accept, session servicing, reaping) plus the Peer lifecycle
// for every accepted session, because the Server's session-list accessors are
// loop-thread-only by contract. New sessions get an EchoServer bootstrap Peer
// bound on the pass after acceptance (the engines dispatch frames that
// buffered before binding); Peers whose session id has disappeared (the
// server reaps sessions once closed) are torn down on the same thread.

const soak_quic_cert_pem = @embedFile("soak_certs/loopback_cert.pem");
const soak_quic_key_pem = @embedFile("soak_certs/loopback_key.pem");

const QuicServerHarness = struct {
    allocator: std.mem.Allocator = undefined,
    server: if (quic.enabled) ?*quic.Server else ?void = null,
    thread: ?std.Thread = null,
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    bound: if (quic.enabled) std.AutoHashMap(u64, *Peer) else void = undefined,
    // Reset-emitter field data (the churn signals upstream's stability case
    // for the reset surfaces asks for). unroutable_* count LogEvent
    // .unroutable_dcid; resets_sent snapshots the server counter at drain.
    // The harness always sets a reset key, so resets_sent==0 here means no
    // stale-CID traffic arrived — NOT a keyless-config artifact. The
    // unroutable_* pair is the signal that holds either way.
    unroutable_seen: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    unroutable_reset_queued: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    resets_sent: u64 = 0,
    // Abrupt-death mode state (loop-thread except where noted).
    io: std.Io = undefined,
    workers_hint: u32 = 0,
    cc: CcChoice = .default,
    death_every_ms: ?u64 = null,
    // Concrete bound address captured after the first bind; every restart
    // re-binds this exact port so surviving clients keep sending into it.
    listen_addr: if (quic.enabled) net.IpAddress else void = undefined,
    deaths: u64 = 0,
    // True while the Server value at `server` is deinitialized (a rebind
    // race exhausted its retries): shutdown must not deinit it again.
    server_destroyed: bool = false,
    rebind_failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn makeOptions(self: *QuicServerHarness, listen_addr: net.IpAddress) quic.ServerOptions {
        if (comptime !quic.enabled) unreachable;
        var server_options = quic.ServerOptions{
            .listen_addr = listen_addr,
            .tls_cert_pem = soak_quic_cert_pem,
            .tls_key_pem = soak_quic_key_pem,
            .receive_timeout = std.Io.Duration.fromMilliseconds(5),
            // Each worker holds at most one live session; x2 covers
            // close/reap overlap during churn.
            .max_concurrent_connections = self.workers_hint * 2 + 4,
            // Fixed soak key: turns churned-away connection state into
            // observable stateless resets instead of silent timeouts.
            // MUST be byte-identical across abrupt-death incarnations, or
            // surviving clients lose the certificate and stall to idle
            // timeout instead.
            .stateless_reset_key = @as(quic.StatelessResetKey, @splat(0x51)),
            .log_callback = QuicServerHarness.onServerLog,
            .log_user_data = self,
        };
        applyCc(&server_options.congestion_control, self.cc);
        return server_options;
    }

    fn start(self: *QuicServerHarness, allocator: std.mem.Allocator, io: std.Io, workers: u32, cc: CcChoice, death_every_ms: ?u64) !net.IpAddress {
        if (comptime !quic.enabled) unreachable;
        self.allocator = allocator;
        self.io = io;
        self.workers_hint = workers;
        self.cc = cc;
        self.death_every_ms = death_every_ms;
        self.bound = std.AutoHashMap(u64, *Peer).init(allocator);
        const server = try allocator.create(quic.Server);
        errdefer allocator.destroy(server);
        server.* = try quic.Server.init(allocator, io, self.makeOptions(.{ .ip4 = .loopback(0) }));
        self.server = server;
        self.listen_addr = server.getAddress();
        self.thread = try std.Thread.spawn(.{}, QuicServerHarness.threadMain, .{self});
        return self.listen_addr;
    }

    fn shutdown(self: *QuicServerHarness) void {
        if (comptime !quic.enabled) unreachable;
        self.stop.store(true, .release);
        // In abrupt-death mode the loop thread may be mid-kill (the Server
        // value transiently deinitialized), so a cross-thread wake() would
        // race a use-after-free. stepOnce's 5ms receive timeout bounds the
        // un-woken shutdown latency instead.
        if (self.death_every_ms == null) {
            if (self.server) |server| server.wake();
        }
        if (self.thread) |t| t.join();
        if (self.server) |server| {
            if (!self.server_destroyed) server.deinit();
            self.allocator.destroy(server);
        }
        self.bound.deinit();
    }

    fn onServerLog(user_data: ?*anyopaque, ev: quic.ServerLogEvent) void {
        if (comptime !quic.enabled) unreachable;
        const self: *QuicServerHarness = @ptrCast(@alignCast(user_data.?));
        switch (ev) {
            .unroutable_dcid => |info| {
                _ = self.unroutable_seen.fetchAdd(1, .monotonic);
                if (info.reset_queued) _ = self.unroutable_reset_queued.fetchAdd(1, .monotonic);
            },
            // LogEvent is additive upstream; never switch exhaustively.
            else => {},
        }
    }

    /// Crash the server the way a real crash does: snapshot counters, deinit
    /// with NO close ceremony (sans-IO teardown guarantees nothing reaches
    /// the wire), destroy the bound peers, then restart on the SAME port
    /// with the SAME reset key. Loop-thread only.
    fn executeAbruptDeath(self: *QuicServerHarness) void {
        if (comptime !quic.enabled) unreachable;
        const server = self.server.?;
        // The counter lives on the Server value; accumulate before the kill
        // or the incarnation's resets vanish from the report.
        self.resets_sent += server.statelessResetsSent();
        // Order is load-bearing: deinit fires each live session's close
        // callback into its still-alive bound Peer; only then is it safe to
        // detach and destroy the peers (the ServerSessions are gone).
        server.deinit();
        var it = self.bound.valueIterator();
        while (it.next()) |peer_ptr| {
            const peer = peer_ptr.*;
            _ = peer.takeAttachedConnection(*quic.ServerSession);
            peer.deinit();
            self.allocator.destroy(peer);
        }
        // Clear the whole map: a fresh server can reissue overlapping
        // session ids, and stale entries would alias them.
        self.bound.clearRetainingCapacity();
        // Bounded rebind retry: the freed port transiently returns to the
        // kernel pool (same shape as the crash-restart e2e's 20x5ms loop).
        var attempt: u32 = 0;
        while (true) : (attempt += 1) {
            server.* = quic.Server.init(self.allocator, self.io, self.makeOptions(self.listen_addr)) catch |err| {
                if (attempt >= 20) {
                    std.debug.print("soak: abrupt-death rebind failed after {} attempts: {}\n", .{ attempt + 1, err });
                    self.server_destroyed = true;
                    self.rebind_failed.store(true, .release);
                    self.stop.store(true, .release);
                    return;
                }
                sleepMs(self.io, 5);
                continue;
            };
            break;
        }
        self.deaths += 1;
    }

    fn threadMain(self: *QuicServerHarness) void {
        if (comptime !quic.enabled) unreachable;
        const server = self.server.?;
        var next_death_ns: u64 = if (self.death_every_ms) |ms| nowNsU(self.io) + ms * std.time.ns_per_ms else 0;
        while (!self.stop.load(.acquire)) {
            _ = server.stepOnce(.wait) catch |err| {
                std.debug.print("soak: quic server step failed: {}\n", .{err});
            };
            self.bindNewSessions();
            self.sweepDeadPeers();
            if (self.death_every_ms) |ms| {
                if (nowNsU(self.io) >= next_death_ns) {
                    self.executeAbruptDeath();
                    if (self.server_destroyed) return;
                    next_death_ns = nowNsU(self.io) + ms * std.time.ns_per_ms;
                }
            }
        }
        // Drain: close every live session, keep stepping until the server
        // reaps them (bounded), then tear down every remaining Peer before
        // the server itself is deinitialized.
        server.requestClose();
        var budget: u32 = 400;
        while (budget > 0 and server.sessionCount() > 0) : (budget -= 1) {
            _ = server.stepOnce(.wait) catch {};
            self.sweepDeadPeers();
        }
        var it = self.bound.valueIterator();
        while (it.next()) |peer_ptr| {
            const peer = peer_ptr.*;
            _ = peer.takeAttachedConnection(*quic.ServerSession);
            peer.deinit();
            self.allocator.destroy(peer);
        }
        self.bound.clearRetainingCapacity();
        // Loop-thread-only accessor; snapshot before the loop thread exits.
        // ACCUMULATE: abrupt-death incarnations already banked their counts.
        self.resets_sent += server.statelessResetsSent();
    }

    fn bindNewSessions(self: *QuicServerHarness) void {
        if (comptime !quic.enabled) unreachable;
        const server = self.server.?;
        var i: usize = 0;
        while (i < server.sessionCount()) : (i += 1) {
            const sess = server.sessionAt(i) orelse continue;
            if (self.bound.contains(sess.id)) continue;
            const peer = self.allocator.create(Peer) catch continue;
            peer.* = Peer.init(self.allocator, sess);
            _ = peer.setBootstrap(.{
                .ctx = @ptrCast(&EchoServer.ctx_anchor),
                .on_call = EchoServer.onCall,
            }) catch {
                _ = peer.takeAttachedConnection(*quic.ServerSession);
                peer.deinit();
                self.allocator.destroy(peer);
                continue;
            };
            peer.setRestorer(@ptrCast(&EchoServer.ctx_anchor), EchoServer.onRestore) catch {
                _ = peer.takeAttachedConnection(*quic.ServerSession);
                peer.deinit();
                self.allocator.destroy(peer);
                continue;
            };
            peer.start(null, null, null);
            self.bound.put(sess.id, peer) catch {
                _ = peer.takeAttachedConnection(*quic.ServerSession);
                peer.deinit();
                self.allocator.destroy(peer);
            };
        }
    }

    fn sweepDeadPeers(self: *QuicServerHarness) void {
        if (comptime !quic.enabled) unreachable;
        const server = self.server.?;
        // Bounded batch per pass; stragglers get the next pass.
        var dead: [64]u64 = undefined;
        var n: usize = 0;
        var it = self.bound.keyIterator();
        while (it.next()) |id_ptr| {
            if (server.sessionById(id_ptr.*) != null) continue;
            if (n == dead.len) break;
            dead[n] = id_ptr.*;
            n += 1;
        }
        for (dead[0..n]) |id| {
            const entry = self.bound.fetchRemove(id) orelse continue;
            const peer = entry.value;
            // The server already destroyed the session at reap time (the
            // Peer's close callback fired then); detach so peer teardown
            // never touches the dead pointer.
            _ = peer.takeAttachedConnection(*quic.ServerSession);
            peer.deinit();
            self.allocator.destroy(peer);
        }
    }
};

// -- Memory sampler ----------------------------------------------------------

const MemSampler = struct {
    counter: *const CountingAllocator,
    io: std.Io,
    interval_ms: u64,
    stop_at_ns: i64,
    stop_flag: *std.atomic.Value(bool),
    // Sample series (live bytes) written by the sampler, read after join.
    samples: *std.ArrayList(u64),
    samples_allocator: std.mem.Allocator,

    fn main(self: MemSampler) void {
        while (!self.stop_flag.load(.acquire) and nowNs(self.io) < self.stop_at_ns) {
            const live = self.counter.liveBytes();
            self.samples.append(self.samples_allocator, live) catch {};
            sleepMs(self.io, self.interval_ms);
        }
    }
};

/// Programmatic flat-memory check: split the steady-state window (samples
/// after an initial warmup ramp) into a head and tail quarter and compare
/// their means. Growth beyond the configured percentage (with an absolute
/// floor so tiny heaps do not trip on noise) fails the run. This is a slope
/// check over the whole run, not a final leak snapshot.
const MemVerdict = struct {
    ok: bool,
    head_mean: f64,
    tail_mean: f64,
    growth_pct: f64,
    peak: u64,
    warmup_samples: usize,
};

fn meanOf(slice: []const u64) f64 {
    if (slice.len == 0) return 0;
    var sum: u128 = 0;
    for (slice) |v| sum += v;
    return @as(f64, @floatFromInt(@as(u64, @intCast(sum)))) / @as(f64, @floatFromInt(slice.len));
}

fn assessMemory(samples: []const u64, growth_pct: f64) MemVerdict {
    var peak: u64 = 0;
    for (samples) |v| peak = @max(peak, v);

    // Drop the first ~20% as warmup ramp (connection pools, arenas priming).
    const warmup = samples.len / 5;
    const steady = samples[@min(warmup, samples.len)..];
    if (steady.len < 4) {
        // Not enough steady-state signal to judge a slope; treat as pass but
        // report what we have. (Short runs still get the final leak check.)
        return .{
            .ok = true,
            .head_mean = meanOf(steady),
            .tail_mean = meanOf(steady),
            .growth_pct = 0,
            .peak = peak,
            .warmup_samples = warmup,
        };
    }

    const q = steady.len / 4;
    const head = steady[0..@max(q, 1)];
    const tail = steady[steady.len - @max(q, 1) ..];
    const head_mean = meanOf(head);
    const tail_mean = meanOf(tail);

    const growth = tail_mean - head_mean;
    const rel_pct = if (head_mean > 0) (growth / head_mean) * 100.0 else 0;

    // Absolute floor: ignore growth under 256 KiB — that is churn/fragmentation
    // noise, not a leak, and small enough not to matter over a bounded soak.
    const abs_floor_bytes: f64 = 256 * 1024;
    const ok = (growth <= abs_floor_bytes) or (rel_pct <= growth_pct);

    return .{
        .ok = ok,
        .head_mean = head_mean,
        .tail_mean = tail_mean,
        .growth_pct = rel_pct,
        .peak = peak,
        .warmup_samples = warmup,
    };
}

test "assessMemory: a flat steady-state series passes" {
    // ~1 MiB heap wobbling within a few percent — allocator noise, not a leak.
    var series: [40]u64 = undefined;
    for (&series, 0..) |*v, i| v.* = 1_000_000 + (i % 5) * 8_000;
    const verdict = assessMemory(&series, 25.0);
    try std.testing.expect(verdict.ok);
}

test "assessMemory: a monotonically growing series above the floor fails" {
    // Steady-state climb from ~1 MiB to ~5 MiB — a genuine leak/growth trend
    // well past the 256 KiB absolute floor and the 25% relative threshold.
    var series: [40]u64 = undefined;
    for (&series, 0..) |*v, i| v.* = 1_000_000 + i * 100_000;
    const verdict = assessMemory(&series, 25.0);
    try std.testing.expect(!verdict.ok);
    try std.testing.expect(verdict.growth_pct > 25.0);
}

test "assessMemory: sub-floor growth on a tiny heap is tolerated" {
    // Grows 100% relatively, but only a handful of KiB — under the absolute
    // floor, so churn on a tiny heap must not trip the gate.
    var series: [40]u64 = undefined;
    for (&series, 0..) |*v, i| v.* = 4_096 + i * 128;
    const verdict = assessMemory(&series, 25.0);
    try std.testing.expect(verdict.ok);
}

// -- Helpers -----------------------------------------------------------------

fn sleepMs(io: std.Io, ms: u64) void {
    const duration: std.Io.Clock.Duration = .{
        .raw = .{ .nanoseconds = @as(i96, @intCast(ms)) * std.time.ns_per_ms },
        .clock = .awake,
    };
    duration.sleep(io) catch {};
}

fn parseArgs(allocator: std.mem.Allocator, args: std.process.Args) !Config {
    var cfg = Config{};
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
        } else if (std.mem.eql(u8, arg, "--inflight")) {
            cfg.inflight = try std.fmt.parseUnsigned(u32, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--mem-sample-ms")) {
            cfg.mem_sample_ms = try std.fmt.parseUnsigned(u64, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--mem-growth-pct")) {
            cfg.mem_growth_pct = try std.fmt.parseFloat(f64, iter.next() orelse return error.InvalidArgument);
        } else if (std.mem.eql(u8, arg, "--abrupt-death-every-ms")) {
            cfg.abrupt_death_every_ms = try std.fmt.parseUnsigned(u64, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--heal-workers")) {
            cfg.heal_workers = try std.fmt.parseUnsigned(u32, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--no-chaos")) {
            cfg.chaos = false;
        } else if (std.mem.eql(u8, arg, "--no-deadlines")) {
            cfg.deadlines = false;
        } else if (std.mem.eql(u8, arg, "--transport")) {
            const value = iter.next() orelse return error.InvalidArgument;
            cfg.transport = std.meta.stringToEnum(Transport, value) orelse
                return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--cc")) {
            const value = iter.next() orelse return error.InvalidArgument;
            cfg.cc = std.meta.stringToEnum(CcChoice, value) orelse
                return error.InvalidArgument;
        } else {
            std.debug.print("soak: unknown argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.workers == 0 or cfg.calls_per_session == 0 or cfg.inflight == 0) return error.InvalidArgument;
    if (cfg.transport == .quic and !quic.enabled) {
        std.debug.print("soak: --transport quic requires a -Dquic=true build\n", .{});
        return error.InvalidArgument;
    }
    if (cfg.mem_sample_ms == 0) return error.InvalidArgument;
    if (cfg.abrupt_death_every_ms) |ms| {
        // TCP sessions all close `.unknown` and carry no reset-token proof,
        // so the mode would measure nothing there.
        if (cfg.transport != .quic) {
            std.debug.print("soak: --abrupt-death-every-ms requires --transport quic\n", .{});
            return error.InvalidArgument;
        }
        if (ms == 0) return error.InvalidArgument;
    }
    if (cfg.heal_workers > 0) {
        if (cfg.transport != .quic) {
            std.debug.print("soak: --heal-workers requires --transport quic\n", .{});
            return error.InvalidArgument;
        }
        if (cfg.heal_workers > cfg.workers) {
            std.debug.print("soak: --heal-workers must not exceed --workers\n", .{});
            return error.InvalidArgument;
        }
    }
    return cfg;
}

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{ .thread_safe = true }) = .init;
    var counter = CountingAllocator.init(gpa.allocator());
    const allocator = counter.allocator();
    // Harness telemetry (latency samples, the memory curve itself) goes
    // straight to the DebugAllocator, BYPASSING the counter: the
    // steady-state memory gate must measure the system under test, not
    // the instrument. Still leak-checked by the terminal gpa.deinit.
    // See the LatencyBuf doc for the nightly failure this caused.
    const telemetry_allocator = gpa.allocator();
    const io = init.io;

    const cfg = try parseArgs(allocator, init.minimal.args);

    var totals = Totals{};
    EchoServer.server_io = io;

    // Per-worker latency buffers (merged after join).
    const latency_bufs = try telemetry_allocator.alloc(LatencyBuf, cfg.workers);
    for (latency_bufs) |*b| b.* = .{ .allocator = telemetry_allocator };

    var pool: WorkerPool = undefined;
    var pool_thread: std.Thread = undefined;
    var quic_srv = QuicServerHarness{};
    var address: net.IpAddress = undefined;
    switch (cfg.transport) {
        .tcp => {
            pool = try WorkerPool.init(
                allocator,
                io,
                .{ .ip4 = .loopback(0) },
                @ptrCast(&totals),
                EchoServer.onAccept,
                .{ .concurrency = @max(2, cfg.workers / 2) },
            );
            // `listen()` publishes the kernel-selected ephemeral port in its
            // portable address value. Reading that value avoids POSIX
            // `getsockname` and keeps the real soak path buildable under
            // Windows' native socket handle type.
            const port = pool.server.socket.address.getPort();
            address = .{ .ip4 = .loopback(port) };
            pool_thread = try std.Thread.spawn(.{}, poolThreadMain, .{&pool});
            std.debug.print(
                "soak: tcp server listening on port {} (workers {}, inflight {}, pool concurrency {})\n",
                .{ port, cfg.workers, cfg.inflight, @max(2, cfg.workers / 2) },
            );
        },
        .quic => {
            // parseArgs already rejected .quic on a non-QUIC build, so this
            // branch is only reachable when the harness compiles for real.
            if (comptime quic.enabled) {
                address = try quic_srv.start(allocator, io, cfg.workers, cfg.cc, cfg.abrupt_death_every_ms);
                std.debug.print(
                    "soak: quic server listening on port {} (workers {}, inflight {})\n",
                    .{ address.getPort(), cfg.workers, cfg.inflight },
                );
            } else unreachable;
        },
    }

    const stop_at_ns = nowNs(io) + @as(i64, @intCast(cfg.seconds)) * std.time.ns_per_s;

    // Memory sampler thread: watches live heap for the run duration.
    var mem_samples: std.ArrayList(u64) = .empty;
    defer mem_samples.deinit(telemetry_allocator);
    var mem_stop = std.atomic.Value(bool).init(false);
    const mem_thread = try std.Thread.spawn(.{}, MemSampler.main, .{MemSampler{
        .counter = &counter,
        .io = io,
        .interval_ms = cfg.mem_sample_ms,
        .stop_at_ns = stop_at_ns + std.time.ns_per_s, // sample a bit past worker stop
        .stop_flag = &mem_stop,
        .samples = &mem_samples,
        .samples_allocator = telemetry_allocator,
    }});

    // Healing clients (QUIC only; parseArgs enforced the pairing): stable
    // storage created before their threads spawn, stopped by this thread
    // once the run window ends.
    const heal_count: u32 = cfg.heal_workers;
    var heal_apps: if (quic.enabled) []HealApp else void =
        if (comptime quic.enabled) try allocator.alloc(HealApp, heal_count) else {};
    var heal_clients: if (quic.enabled) []quic.WarmRedialClient else void =
        if (comptime quic.enabled) try allocator.alloc(quic.WarmRedialClient, heal_count) else {};
    if (comptime quic.enabled) {
        for (heal_apps) |*app| app.* = .{};
        for (heal_clients, 0..) |*client, i| {
            var base = quic.ClientOptions{
                .remote_addr = address,
                .server_name = "localhost",
                .insecure_skip_verify = true,
                .receive_timeout = std.Io.Duration.fromMilliseconds(5),
            };
            applyCc(&base.congestion_control, cfg.cc);
            client.* = try quic.WarmRedialClient.init(
                allocator,
                io,
                base,
                EchoServer.sturdy_ref,
                // Effectively unbounded within the run; the run window is
                // the real budget.
                .{ .max_redials = std.math.maxInt(u32), .backoff_ms = 25 },
                &heal_apps[i],
                HealApp.onRebind,
                HealApp.onGiveUp,
            );
        }
    }

    const worker_threads = try allocator.alloc(std.Thread, cfg.workers);
    for (worker_threads, 0..) |*t, i| {
        if (comptime quic.enabled) {
            if (cfg.transport == .quic and i < heal_count) {
                t.* = try std.Thread.spawn(.{}, HealApp.threadMain, .{ &heal_apps[i], &heal_clients[i] });
                continue;
            }
        }
        t.* = switch (cfg.transport) {
            .tcp => try std.Thread.spawn(.{}, Worker.main, .{Worker{
                .allocator = allocator,
                .io = io,
                .address = address,
                .cfg = &cfg,
                .totals = &totals,
                .latency = &latency_bufs[i],
                .stop_at_ns = stop_at_ns,
                .index = @intCast(i),
            }}),
            .quic => if (comptime quic.enabled) try std.Thread.spawn(.{}, QuicWorker.main, .{QuicWorker{
                .allocator = allocator,
                .io = io,
                .address = address,
                .cfg = &cfg,
                .totals = &totals,
                .latency = &latency_bufs[i],
                .stop_at_ns = stop_at_ns,
                .index = @intCast(i),
            }}) else unreachable,
        };
    }
    // Churn workers self-terminate at the window's end; healing clients run
    // until told to stop. Join churn first, then stop and join the healers
    // (when every worker heals, wait out the window explicitly).
    for (worker_threads[heal_count..]) |t| t.join();
    if (heal_count == cfg.workers) {
        while (nowNs(io) < stop_at_ns) sleepMs(io, 50);
    }
    var heal_rebinds: usize = 0;
    var heal_redials: usize = 0;
    var heal_give_ups: usize = 0;
    var heal_echo: usize = 0;
    var heal_min_rebinds: usize = std.math.maxInt(usize);
    if (comptime quic.enabled) {
        for (heal_clients) |*client| client.requestStop();
        for (worker_threads[0..heal_count]) |t| t.join();
        for (heal_apps, heal_clients) |*app, *client| {
            if (app.outcome) |o| {
                heal_rebinds += o.rebinds;
                heal_redials += o.redials;
                heal_min_rebinds = @min(heal_min_rebinds, o.rebinds);
            } else {
                heal_min_rebinds = 0;
            }
            if (app.gave_up.load(.acquire)) heal_give_ups += 1;
            heal_echo += app.echo_ok.load(.acquire);
            client.deinit();
        }
        allocator.free(heal_clients);
        allocator.free(heal_apps);
    }
    allocator.free(worker_threads);
    std.debug.print("soak: workers joined, draining server\n", .{});

    mem_stop.store(true, .release);
    mem_thread.join();

    switch (cfg.transport) {
        .tcp => {
            pool.shutdownGraceful(2_000);
            std.debug.print("soak: pool drained, joining pool thread\n", .{});
            pool_thread.join();
            pool.deinit();
            std.debug.print("soak: pool deinit complete\n", .{});
        },
        .quic => {
            if (comptime quic.enabled) {
                quic_srv.shutdown();
                std.debug.print("soak: quic server drained\n", .{});
                std.debug.print(
                    "soak-quic: resets_sent={} unroutable_dcid={} (reset_queued={}) abrupt_deaths={}\n",
                    .{ quic_srv.resets_sent, quic_srv.unroutable_seen.load(.acquire), quic_srv.unroutable_reset_queued.load(.acquire), quic_srv.deaths },
                );
                if (cfg.heal_workers > 0) {
                    std.debug.print(
                        "soak-heal: clients={} rebinds={} redials={} give_ups={} echo_ok={} min_rebinds={}\n",
                        .{ cfg.heal_workers, heal_rebinds, heal_redials, heal_give_ups, heal_echo, heal_min_rebinds },
                    );
                }
            } else unreachable;
        },
    }

    // -- Merge latency samples & compute percentiles ----------------------
    var all_latencies: std.ArrayList(u64) = .empty;
    defer all_latencies.deinit(telemetry_allocator);
    for (latency_bufs) |b| {
        all_latencies.appendSlice(telemetry_allocator, b.list.items) catch {};
    }
    var lat = Percentiles{};
    if (all_latencies.items.len > 0) {
        std.mem.sort(u64, all_latencies.items, {}, std.sort.asc(u64));
        lat = .{
            .p50 = percentile(all_latencies.items, 50.0),
            .p99 = percentile(all_latencies.items, 99.0),
            .max = all_latencies.items[all_latencies.items.len - 1],
            .count = all_latencies.items.len,
        };
    }

    const sessions = totals.sessions.load(.acquire);
    const ok = totals.calls_ok.load(.acquire);
    const cancelled = totals.calls_cancelled.load(.acquire);
    const chaos_closes = totals.chaos_closes.load(.acquire);
    const transport_errors = totals.transport_errors.load(.acquire);
    const expected_disconnects = totals.expected_disconnects.load(.acquire);
    const contention_disconnects = totals.contention_disconnects.load(.acquire);
    const unexpected = totals.unexpected_exceptions.load(.acquire);

    std.debug.print(
        "soak: sessions={} calls_ok={} cancelled={} chaos_closes={} transport_errors={} expected_disconnects={} contention_disconnects={} unexpected_exceptions={}\n",
        .{ sessions, ok, cancelled, chaos_closes, transport_errors, expected_disconnects, contention_disconnects, unexpected },
    );
    std.debug.print("soak: session close causes:", .{});
    inline for (comptime std.enums.values(rpc.events.DisconnectCause), 0..) |cause, i| {
        std.debug.print(" {s}={}", .{ @tagName(cause), totals.disconnects_by_cause[i].load(.acquire) });
    }
    std.debug.print("\n", .{});
    std.debug.print(
        "soak: latency p50={}ns p99={}ns max={}ns (samples={})\n",
        .{ lat.p50, lat.p99, lat.max, lat.count },
    );

    // -- Memory-growth curve + flat assessment ----------------------------
    const verdict = assessMemory(mem_samples.items, cfg.mem_growth_pct);
    std.debug.print("soak: memory curve (live bytes, {} samples @ {}ms):\n", .{ mem_samples.items.len, cfg.mem_sample_ms });
    printMemCurve(mem_samples.items);
    std.debug.print(
        "soak: memory steady-state head_mean={d:.0}B tail_mean={d:.0}B growth={d:.2}% peak={}B threshold={d:.1}%\n",
        .{ verdict.head_mean, verdict.tail_mean, verdict.growth_pct, verdict.peak, cfg.mem_growth_pct },
    );

    // -- Free per-worker buffers before the leak check --------------------
    for (latency_bufs) |*b| b.deinit();
    telemetry_allocator.free(latency_bufs);

    var failed = false;
    if (sessions == 0 or ok == 0) {
        std.debug.print("soak: FAIL — no successful traffic\n", .{});
        failed = true;
    }
    if (cfg.chaos and chaos_closes == 0) {
        std.debug.print("soak: FAIL — chaos mode produced no transport closes\n", .{});
        failed = true;
    }
    if (unexpected != 0) {
        std.debug.print("soak: FAIL — unexpected exception reasons observed\n", .{});
        failed = true;
    }
    if (expected_disconnects > chaos_closes) {
        std.debug.print("soak: FAIL — more disconnected exceptions than chaos closes\n", .{});
        failed = true;
    }
    if (cfg.deadlines and cancelled == 0) {
        std.debug.print("soak: FAIL — deadline sessions produced no cancellations\n", .{});
        failed = true;
    }
    if (cfg.abrupt_death_every_ms != null) {
        if (comptime quic.enabled) {
            const reset_closes = totals.disconnects_by_cause[@backingInt(rpc.events.DisconnectCause.stateless_reset)].load(.acquire);
            if (quic_srv.rebind_failed.load(.acquire)) {
                std.debug.print("soak: FAIL — abrupt-death rebind race exhausted its retries\n", .{});
                failed = true;
            }
            if (quic_srv.deaths == 0) {
                std.debug.print("soak: FAIL — abrupt-death mode executed no deaths\n", .{});
                failed = true;
            }
            if (quic_srv.deaths > 0 and reset_closes == 0) {
                std.debug.print("soak: FAIL — abrupt deaths produced no stateless_reset-certified session closes\n", .{});
                failed = true;
            }
            if (quic_srv.deaths > 0 and quic_srv.resets_sent == 0 and quic_srv.unroutable_reset_queued.load(.acquire) == 0) {
                std.debug.print("soak: FAIL — abrupt deaths produced no reset traffic at the server\n", .{});
                failed = true;
            }
        } else unreachable;
    }
    if (cfg.heal_workers > 0) {
        if (comptime quic.enabled) {
            if (heal_give_ups != 0) {
                std.debug.print("soak: FAIL — {} healing client(s) gave up\n", .{heal_give_ups});
                failed = true;
            }
            if (heal_echo == 0) {
                std.debug.print("soak: FAIL — healing clients completed no echo round trips\n", .{});
                failed = true;
            }
            if (cfg.abrupt_death_every_ms != null and quic_srv.deaths > 0 and heal_min_rebinds < 2) {
                std.debug.print("soak: FAIL — a healing client never healed across an abrupt death (min_rebinds={})\n", .{heal_min_rebinds});
                failed = true;
            }
        } else unreachable;
    }
    if (!verdict.ok) {
        std.debug.print(
            "soak: FAIL — steady-state live heap grew {d:.2}% (> {d:.1}% threshold)\n",
            .{ verdict.growth_pct, cfg.mem_growth_pct },
        );
        failed = true;
    }
    // Everything above must be freed before this final leak check. mem_samples
    // and all_latencies are freed by their `defer`s after this scope, so they
    // are not yet freed here — free them explicitly first so the DebugAllocator
    // sees a clean slate.
    all_latencies.deinit(telemetry_allocator);
    all_latencies = .empty;
    mem_samples.deinit(telemetry_allocator);
    mem_samples = .empty;
    if (gpa.deinit() != .ok) {
        std.debug.print("soak: FAIL — client-side allocation leaks detected\n", .{});
        failed = true;
    }
    if (failed) return error.SoakFailed;
    std.debug.print("soak: PASS\n", .{});
}

/// Print a compact ASCII sparkline of the live-heap series so the curve is
/// visible at a glance without a plotting tool. Downsamples to <=60 columns.
fn printMemCurve(samples: []const u64) void {
    if (samples.len == 0) {
        std.debug.print("  (no samples)\n", .{});
        return;
    }
    var lo: u64 = std.math.maxInt(u64);
    var hi: u64 = 0;
    for (samples) |v| {
        lo = @min(lo, v);
        hi = @max(hi, v);
    }
    const bars = [_][]const u8{ "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█" };
    const span: f64 = if (hi > lo) @floatFromInt(hi - lo) else 1.0;

    const max_cols: usize = 60;
    const cols = @min(samples.len, max_cols);
    var buf: [max_cols * 3]u8 = undefined;
    var w: usize = 0;
    var c: usize = 0;
    while (c < cols) : (c += 1) {
        // Map column c to a source sample (nearest).
        const src = if (cols == 1) 0 else (c * (samples.len - 1)) / (cols - 1);
        const norm = (@as(f64, @floatFromInt(samples[src] -| lo)) / span);
        var level: usize = @intFromFloat(@round(norm * @as(f64, @floatFromInt(bars.len - 1))));
        level = @min(level, bars.len - 1);
        const bar = bars[level];
        @memcpy(buf[w .. w + bar.len], bar);
        w += bar.len;
    }
    std.debug.print("  [{s}]  lo={}B hi={}B\n", .{ buf[0..w], lo, hi });
}
