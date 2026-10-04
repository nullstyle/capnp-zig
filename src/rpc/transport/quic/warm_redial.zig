//! Auto warm redial — the durable-caps ladder's integration rung
//! (docs/quic-durable-caps-plan.md): when a QUIC client's transport dies
//! with the crash-restart proof (`Peer.lastDisconnectCause() ==
//! .stateless_reset`), dial a fresh connection resuming via the latest
//! captured session ticket and re-restore the saved sturdy ref, so the
//! application's capability heals without operator action.
//!
//! Shape (each dictated by the runtime's contracts, not preference):
//!
//! - A `Peer` is terminally one-connection (`transport_close_notified`
//!   latches), so every redial builds a FRESH Connection + Peer generation;
//!   nothing from a dead generation is reused.
//! - Healing happens at the STURDY-REF layer: import ids die with their
//!   peer, so the app receives a brand-new capability through `on_rebind`
//!   — existing handles cannot be revived in place.
//! - Bootstrap + restore are enqueued BEFORE the generation's loop starts;
//!   restore rides the promised bootstrap answer (`sendRestorePipelined`),
//!   so on a resumed dial both frames ride 0-RTT early data. Restore is
//!   the idempotent call that makes the replay window acceptable — the
//!   layer sends nothing else in early data.
//! - Only `.stateless_reset` redials by default: it is the one cause that
//!   PROVES crash-restart, provided that no other instance holding the
//!   server's reset key can receive this connection's packets (RFC 9000
//!   §21.11; "Sharing the key" in docs/quic-transport.md).
//!   `.idle_timeout` says nothing about liveness and is opt-in via policy.
//!   A server only sends stateless resets when it has a
//!   `stateless_reset_key`, which `withProductionServerHardening` requires.
//! - The redial budget counts CONSECUTIVE failures, not a lifetime total.
//!   A generation resets the streak only when the server PROVES it stayed
//!   alive for `min_healthy_ms` after the rebind: an authenticated packet
//!   from it arrives that late. Time spent detecting a death never counts,
//!   so a long-lived client heals every crash that a proven-healthy period
//!   separates from the last one, and a server that dies in every
//!   generation still exhausts `max_redials` however late each death is
//!   noticed.
//!
//! Experimental, like the persistence convention it rides.

const std = @import("std");

const connection_mod = @import("./connection.zig");
const options_mod = @import("./options.zig");
const warm_state = @import("./warm_state.zig");
const peer_mod = @import("../../peer/mod.zig");
const rpc_events = @import("../../events.zig");
const cap_table = @import("../../caps/table.zig");
const protocol = @import("../../wire/protocol.zig");

const Connection = connection_mod.Connection;
const ClientOptions = options_mod.ClientOptions;
const Peer = peer_mod.Peer;
const log = std.log.scoped(.rpc_quic_redial);

pub const WarmRedialClient = struct {
    /// Default `Policy.min_healthy_ms`: 10 s. A crash-looping server
    /// (restart, accept, restore, die) cannot keep answering for that long,
    /// and a server that then runs normally clears the streak as soon as
    /// the application has traffic 10 s after the rebind.
    pub const default_min_healthy_ms: u64 = 10_000;

    pub const Policy = struct {
        /// CONSECUTIVE failed generations tolerated before giving up. Each
        /// redial (after a dead generation or a failed dial) spends one; a
        /// healthy generation (see `min_healthy_ms`) resets the count to
        /// zero. This is not a lifetime budget: a long-lived client heals
        /// any number of crashes, as long as each comes after a healthy
        /// period.
        max_redials: u32 = 3,
        /// Fixed pause before each redial (lets the restarted server bind).
        backoff_ms: u64 = 50,
        /// Opt-in: also redial on idle timeout. Off by default — an idle
        /// timeout carries no proof the server crashed OR survived.
        redial_on_idle_timeout: bool = false,
        /// How long the server must PROVE it stayed alive after a
        /// generation's rebind for that generation to count as healthy:
        /// the generation's last authenticated packet from the server
        /// (`Connection.lastAuthenticatedReceiveNs`) must arrive at least
        /// this long after the rebind, on the awake clock. A healthy
        /// generation resets `redials` to zero.
        ///
        /// Detection time is not health. A stateless reset is not
        /// authenticated, and an idle connection receives nothing, so
        /// neither waiting for the next call to draw a reset nor an idle
        /// timeout makes a dead generation look healthy. The cost: a
        /// generation that receives nothing from the server once
        /// `min_healthy_ms` has passed since the rebind never counts as
        /// healthy, so a client that stays idle spends one redial per
        /// death, as a lifetime budget would.
        ///
        /// A generation that never rebinds is never healthy. 0 makes every
        /// rebind healthy; `std.math.maxInt(u64)` makes none healthy, which
        /// turns `max_redials` back into a lifetime budget.
        min_healthy_ms: u64 = default_min_healthy_ms,
    };

    /// Fires on the generation's run thread once the sturdy ref has been
    /// re-restored on a fresh peer. `cap` is already retained; the app
    /// swaps its handle and (on the same thread) may call immediately.
    /// The peer/cap pair is valid until that generation dies.
    pub const RebindFn = *const fn (ctx: ?*anyopaque, peer: *Peer, cap: cap_table.ResolvedCap) void;
    /// Fires when the redial budget is exhausted or a non-redialable cause
    /// ended a generation. Terminal for this client.
    pub const GiveUpFn = *const fn (ctx: ?*anyopaque, cause: rpc_events.DisconnectCause) void;

    pub const Outcome = struct {
        generations: u32,
        /// Consecutive redials since the last healthy generation, at exit
        /// (the budget counter compared against `Policy.max_redials`).
        redials: u32,
        /// Every redial over the client's lifetime.
        total_redials: u32,
        rebinds: u32,
        last_cause: rpc_events.DisconnectCause,
    };

    allocator: std.mem.Allocator,
    io: std.Io,
    /// Dial template. The layer OWNS the resumption fields: it overwrites
    /// `resumption_state`, `new_session_callback`, and
    /// `new_session_user_data` on every generation.
    base: ClientOptions,
    policy: Policy,
    sturdy_ref: []u8,
    cb_ctx: ?*anyopaque,
    on_rebind: RebindFn,
    on_give_up: ?GiveUpFn,

    // Cross-thread state (mu guards all of it).
    mu: std.Io.Mutex = .init,
    ticket: ?[]u8 = null,
    token: ?[]u8 = null,
    current_conn: ?*Connection = null,
    stop_requested: bool = false,

    // Run-thread bookkeeping.
    generations: u32 = 0,
    /// Consecutive redials since the last healthy generation.
    redials: u32 = 0,
    /// Lifetime redial count (never reset).
    total_redials: u32 = 0,
    rebinds: u32 = 0,
    restore_failed: bool = false,
    /// Awake-clock time (ns) of the current generation's rebind; null
    /// until it rebinds.
    rebound_at_ns: ?u64 = null,

    /// `sturdy_ref` is copied; the caller keeps ownership of the argument.
    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        base: ClientOptions,
        sturdy_ref: []const u8,
        policy: Policy,
        cb_ctx: ?*anyopaque,
        on_rebind: RebindFn,
        on_give_up: ?GiveUpFn,
    ) !WarmRedialClient {
        const ref_copy = try allocator.dupe(u8, sturdy_ref);
        return .{
            .allocator = allocator,
            .io = io,
            .base = base,
            .policy = policy,
            .sturdy_ref = ref_copy,
            .cb_ctx = cb_ctx,
            .on_rebind = on_rebind,
            .on_give_up = on_give_up,
        };
    }

    pub fn deinit(self: *WarmRedialClient) void {
        self.allocator.free(self.sturdy_ref);
        if (self.ticket) |t| self.allocator.free(t);
        if (self.token) |t| self.allocator.free(t);
        self.* = undefined;
    }

    /// Encode the captured `{ticket, NEW_TOKEN}` pair — the warm half of a
    /// sturdy ref — for the application to persist beside its ref bytes.
    /// Null until a session ticket has been captured. The caller owns the
    /// returned buffer.
    pub fn exportWarmState(self: *WarmRedialClient, allocator: std.mem.Allocator) !?[]u8 {
        self.mu.lockUncancelable(self.io);
        defer self.mu.unlock(self.io);
        const ticket = self.ticket orelse return null;
        return try warm_state.encode(allocator, ticket, self.token orelse &.{});
    }

    /// Seed the client from a previously exported envelope, BEFORE `run`:
    /// the first dial then resumes warm (0-RTT + address-validated) across
    /// a process restart instead of paying a cold handshake.
    pub fn seedWarmState(self: *WarmRedialClient, bytes: []const u8) !void {
        const decoded = try warm_state.decode(bytes);
        const ticket_copy = try self.allocator.dupe(u8, decoded.ticket);
        errdefer self.allocator.free(ticket_copy);
        const token_copy: ?[]u8 = if (decoded.token.len > 0)
            try self.allocator.dupe(u8, decoded.token)
        else
            null;
        self.mu.lockUncancelable(self.io);
        const old_ticket = self.ticket;
        const old_token = self.token;
        self.ticket = ticket_copy;
        self.token = token_copy;
        self.mu.unlock(self.io);
        if (old_ticket) |t| self.allocator.free(t);
        if (old_token) |t| self.allocator.free(t);
    }

    /// Cross-thread stop: ends the current generation and makes `run`
    /// return after its teardown instead of redialing.
    pub fn requestStop(self: *WarmRedialClient) void {
        self.mu.lockUncancelable(self.io);
        self.stop_requested = true;
        const conn = self.current_conn;
        self.mu.unlock(self.io);
        // Cross-thread-safe by the QUIC connection's contract.
        if (conn) |c| c.requestClose();
    }

    /// Blocking generation loop on the calling thread (the thread also
    /// becomes every generation's owner thread). Returns when stopped, when
    /// `max_redials` consecutive redials have failed, or when a
    /// non-redialable cause ends a generation.
    pub fn run(self: *WarmRedialClient) !Outcome {
        var last_cause: rpc_events.DisconnectCause = .unknown;
        while (true) {
            const stopped = self.runGeneration(&last_cause) catch |err| {
                // Dial/enqueue failure burns a redial slot like a dead
                // generation does; the restarted server may need a beat.
                log.debug("generation setup failed: {}", .{err});
                if (self.stopRequested()) break;
                if (!self.spendRedial()) {
                    if (self.on_give_up) |cb| cb(self.cb_ctx, last_cause);
                    return err;
                }
                sleepMs(self.io, self.policy.backoff_ms);
                continue;
            };
            if (stopped) break;
            if (!self.causeRedials(last_cause)) {
                if (self.on_give_up) |cb| cb(self.cb_ctx, last_cause);
                break;
            }
            if (!self.spendRedial()) {
                if (self.on_give_up) |cb| cb(self.cb_ctx, last_cause);
                break;
            }
            sleepMs(self.io, self.policy.backoff_ms);
        }
        return .{
            .generations = self.generations,
            .redials = self.redials,
            .total_redials = self.total_redials,
            .rebinds = self.rebinds,
            .last_cause = last_cause,
        };
    }

    /// Take one redial from the consecutive-failure budget; false when it
    /// is exhausted.
    fn spendRedial(self: *WarmRedialClient) bool {
        if (self.redials >= self.policy.max_redials) return false;
        self.redials += 1;
        self.total_redials +|= 1;
        return true;
    }

    /// Called once the generation's connection has ended: a generation that
    /// rebound and whose server then proved itself alive for
    /// `min_healthy_ms` ends the failure streak. The proof is the
    /// connection's last authenticated receive, NOT the time the
    /// connection ended: the gap between the two is detection latency (a
    /// rare caller's wait for a stateless reset, or an idle timeout), and
    /// counting it would let a crash loop refund its own budget.
    fn settleGenerationHealth(self: *WarmRedialClient, conn: *const Connection) void {
        const rebound_at = self.rebound_at_ns orelse return;
        self.rebound_at_ns = null;
        const alive_at = conn.lastAuthenticatedReceiveNs() orelse return;
        const proven_up_ns = alive_at -| rebound_at;
        if (proven_up_ns >= self.policy.min_healthy_ms *| std.time.ns_per_ms) self.redials = 0;
    }

    /// One connection generation: dial (resumed when a ticket exists),
    /// enqueue bootstrap + pipelined restore pre-loop, run to completion.
    /// Returns true when a stop was requested.
    fn runGeneration(self: *WarmRedialClient, last_cause: *rpc_events.DisconnectCause) !bool {
        // Snapshot ticket + NEW_TOKEN into generation-local storage: the
        // connection borrows them while it lives, and the sinks may replace
        // the shared copies from the run thread mid-generation.
        const ticket_snapshot: ?[]u8 = blk: {
            self.mu.lockUncancelable(self.io);
            defer self.mu.unlock(self.io);
            break :blk if (self.ticket) |t| try self.allocator.dupe(u8, t) else null;
        };
        defer if (ticket_snapshot) |t| self.allocator.free(t);
        const token_snapshot: ?[]u8 = blk: {
            self.mu.lockUncancelable(self.io);
            defer self.mu.unlock(self.io);
            break :blk if (self.token) |t| try self.allocator.dupe(u8, t) else null;
        };
        defer if (token_snapshot) |t| self.allocator.free(t);

        var options = self.base;
        options.resumption_state = ticket_snapshot;
        options.new_session_callback = onNewSession;
        options.new_session_user_data = self;
        options.new_token = token_snapshot;
        options.new_token_callback = onNewToken;
        options.new_token_user_data = self;

        var conn = try Connection.initClient(self.allocator, self.io, options);
        var conn_alive = true;
        defer if (conn_alive) conn.deinit();

        {
            self.mu.lockUncancelable(self.io);
            defer self.mu.unlock(self.io);
            if (self.stop_requested) return true;
            self.current_conn = &conn;
        }
        defer {
            self.mu.lockUncancelable(self.io);
            self.current_conn = null;
            self.mu.unlock(self.io);
        }

        var peer = Peer.init(self.allocator, &conn);
        var peer_alive = true;
        // Error-path teardown mirrors ClientSession's blessed order: detach
        // before peer.deinit so releases never write into the transport.
        defer if (peer_alive) {
            _ = peer.takeAttachedConnection(*Connection);
            peer.deinit();
        };
        peer.setClockIo(self.io);
        peer.start(self, null, null);

        // Both frames enqueue before the loop: on a resumed dial the engine
        // opens the RPC stream pre-handshake and they ride 0-RTT.
        const bootstrap_qid = try peer.sendBootstrap(self, onBootstrapReturn);
        _ = try peer.sendRestorePipelined(bootstrap_qid, self.sturdy_ref, self, onRestoreResponse);

        self.generations += 1;
        self.rebound_at_ns = null;
        conn.run();

        last_cause.* = peer.lastDisconnectCause();
        self.settleGenerationHealth(&conn);

        _ = peer.takeAttachedConnection(*Connection);
        peer.deinit();
        peer_alive = false;
        conn.deinit();
        conn_alive = false;

        return self.stopRequested();
    }

    fn stopRequested(self: *WarmRedialClient) bool {
        self.mu.lockUncancelable(self.io);
        defer self.mu.unlock(self.io);
        return self.stop_requested;
    }

    fn causeRedials(self: *const WarmRedialClient, cause: rpc_events.DisconnectCause) bool {
        // Named causes are listed (not `else`) so a cause added to
        // `DisconnectCause` must get a redial decision here at compile time.
        return switch (cause) {
            .stateless_reset => true,
            .idle_timeout => self.policy.redial_on_idle_timeout,
            .unknown,
            .local_close,
            .peer_close,
            .transport_error,
            .handshake_timeout,
            => false,
            // A cause this build does not name proves nothing about the
            // remote's state, so it is treated like `.unknown`.
            _ => false,
        };
    }

    fn onNewToken(user_data: ?*anyopaque, token: []const u8) void {
        const self: *WarmRedialClient = @ptrCast(@alignCast(user_data orelse return));
        const copy = self.allocator.dupe(u8, token) catch return;
        self.mu.lockUncancelable(self.io);
        const old = self.token;
        self.token = copy;
        self.mu.unlock(self.io);
        if (old) |t| self.allocator.free(t);
    }

    fn onNewSession(user_data: ?*anyopaque, ticket: []const u8) void {
        // The layer always installs itself as user_data; a null here would
        // be a transport bug — drop the ticket rather than trap on it.
        const self: *WarmRedialClient = @ptrCast(@alignCast(user_data orelse return));
        // Latest-wins, tear-free: the bytes are borrowed, so copy under the
        // lock before the transport reuses them.
        const copy = self.allocator.dupe(u8, ticket) catch return;
        self.mu.lockUncancelable(self.io);
        const old = self.ticket;
        self.ticket = copy;
        self.mu.unlock(self.io);
        if (old) |t| self.allocator.free(t);
    }

    fn onBootstrapReturn(
        ctx: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        // The bootstrap answer only exists as the pipelined restore's
        // target; nothing is retained from it here.
        _ = ctx;
        _ = peer;
        _ = ret;
        _ = inbound_caps;
    }

    fn onRestoreResponse(ctx: *anyopaque, peer: *Peer, response: peer_mod.RestoreResponse) anyerror!void {
        const self: *WarmRedialClient = @ptrCast(@alignCast(ctx));
        switch (response) {
            .cap => |cap| {
                self.rebinds += 1;
                self.rebound_at_ns = nowNs(self.io);
                self.on_rebind(self.cb_ctx, peer, cap);
            },
            .exception => |ex| {
                self.restore_failed = true;
                log.warn("restore failed on redial generation: {s}", .{ex.reason});
            },
            .other => |tag| {
                self.restore_failed = true;
                log.warn("restore returned unexpected tag: {}", .{tag});
            },
        }
    }
};

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

fn sleepMs(io: std.Io, ms: u64) void {
    const duration: std.Io.Clock.Duration = .{
        .raw = .{ .nanoseconds = @as(i96, @intCast(ms)) * std.time.ns_per_ms },
        .clock = .awake,
    };
    duration.sleep(io) catch {};
}
