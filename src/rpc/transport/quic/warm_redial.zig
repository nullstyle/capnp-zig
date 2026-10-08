//! Auto warm redial — the durable-caps ladder's integration rung
//! (docs/quic-durable-caps-plan.md): when a QUIC client's transport dies
//! with the crash-restart proof (`Peer.lastDisconnectCause() ==
//! .stateless_reset`), dial a fresh connection and re-restore the saved
//! sturdy ref, so the application's capability heals without operator
//! action. The dial offers the latest captured session ticket. A restarted
//! server accepts it only when it loads the same persisted session-ticket key
//! (`ServerOptions.session_ticket_key`); otherwise BoringSSL's per-process
//! key cannot decrypt it, and the heal pays a full handshake.
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
//!   so when a dial does resume (the same server process, for example a
//!   client restart seeded with `seedWarmState`) both frames ride 0-RTT
//!   early data. Restore is the idempotent call that makes the replay
//!   window acceptable — the layer sends nothing else in early data.
//! - A redial binds the previous generation's local UDP port again (the one
//!   thing a generation hands on besides the warm state): quic-zig binds a
//!   NEW_TOKEN to the client's address and port, and a server with Retry on
//!   skips the Retry, and its round trip, only for a token that is valid
//!   from where the dial comes. When the port is taken, the redial falls
//!   back to an ephemeral port and counts it
//!   (`Outcome.port_fallback_generations`); its 0-RTT restore still runs
//!   early, behind the Retry.
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
const quic_zig_adapter = @import("./quic_zig_adapter.zig");
const warm_state = @import("./warm_state.zig");
const peer_mod = @import("../../peer/mod.zig");
const rpc_events = @import("../../events.zig");
const cap_table = @import("../../caps/table.zig");
const protocol = @import("../../wire/protocol.zig");

const Connection = connection_mod.Connection;
const ClientOptions = options_mod.ClientOptions;
const Net = std.Io.net;
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
        /// Generations whose first flight, the Bootstrap and Restore
        /// frames included, rode 0-RTT: the server accepted the dial's
        /// early data (`EarlyDataStatus.accepted`), so the restore ran
        /// before the server's handshake completed. After a crash-restart
        /// this needs a server that loads the same `session_ticket_key`. A
        /// dial that got a Retry counts here too (and in
        /// `retried_generations`): since quic-zig v0.27.0 the client sends
        /// its 0-RTT data again after the Retry, so the restore still runs
        /// early, one round trip later. Through quic-zig v0.25.0 such a
        /// dial restored only after the handshake and never counted here.
        zero_rtt_generations: u32 = 0,
        /// Generations whose dial got a Retry from the server: each one
        /// cost one more round trip, 0-RTT or not. Under the hardened
        /// preset, a dial without a valid NEW_TOKEN gets a Retry; a
        /// NEW_TOKEN is valid only from the address and port that earned
        /// it, and only while the server keeps the same `new_token_key`.
        /// The first dial of a client always has a new port, unless
        /// `base.local_addr` names one. A generation in both counters
        /// restored in 0-RTT behind a Retry; one in `zero_rtt_generations`
        /// only saved the full round trip.
        retried_generations: u32 = 0,
        /// Generations that dialed from a new ephemeral port because the
        /// previous generation's local port could not be bound (another
        /// socket took it). Under the hardened preset such a dial also gets
        /// a Retry, because its NEW_TOKEN was earned from the old port.
        port_fallback_generations: u32 = 0,
        last_cause: rpc_events.DisconnectCause,
    };

    allocator: std.mem.Allocator,
    io: std.Io,
    /// Dial template. The layer OWNS the resumption fields: it overwrites
    /// `resumption_state`, `new_session_callback`, `new_session_user_data`,
    /// `new_token`, `new_token_callback` and `new_token_user_data` on every
    /// generation. When `local_addr` is null or names port 0, every
    /// generation after the first dials from the previous generation's port
    /// (falling back to an ephemeral port when it is taken); a `local_addr`
    /// with a port is used as it is.
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
    /// The live generation's connection, a local of the run thread's
    /// `runGeneration` frame. Use it only while holding `mu`: the run thread
    /// clears it under `mu` before it tears the connection down.
    current_conn: ?*Connection = null,
    stop_requested: bool = false,

    // Run-thread bookkeeping.
    generations: u32 = 0,
    /// Consecutive redials since the last healthy generation.
    redials: u32 = 0,
    /// Lifetime redial count (never reset).
    total_redials: u32 = 0,
    rebinds: u32 = 0,
    /// Generations whose first flight rode accepted 0-RTT (with or without
    /// a Retry).
    zero_rtt_generations: u32 = 0,
    /// Generations whose dial got a Retry.
    retried_generations: u32 = 0,
    /// Generations that fell back to an ephemeral port.
    port_fallback_generations: u32 = 0,
    /// Local UDP port of the last generation that dialed, which the next
    /// generation dials from again (see `keptLocalAddr`). Null before the
    /// first dial.
    last_local_port: ?u16 = null,
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
    /// return after its teardown instead of redialing. Safe at any time,
    /// including while a generation ends on its own.
    pub fn requestStop(self: *WarmRedialClient) void {
        self.mu.lockUncancelable(self.io);
        defer self.mu.unlock(self.io);
        self.stop_requested = true;
        // Close under `mu`. `current_conn` lives in the run thread's stack
        // frame, and the run thread clears it under `mu` before it tears the
        // connection down. Once `mu` is released, the generation may end, the
        // connection may be freed, and `run` may return. requestClose is
        // cross-thread-safe by the QUIC connection's contract and takes no
        // lock that the run thread holds while it waits for `mu`.
        if (self.current_conn) |c| c.requestClose();
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
            .zero_rtt_generations = self.zero_rtt_generations,
            .retried_generations = self.retried_generations,
            .port_fallback_generations = self.port_fallback_generations,
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

    /// The address the next dial binds to keep the last generation's port,
    /// or null when there is nothing to keep: before the first dial, or when
    /// `base.local_addr` already names a port. The IP part is
    /// `base.local_addr`'s, or the unspecified address that a dial without
    /// one binds.
    fn keptLocalAddr(self: *const WarmRedialClient) ?Net.IpAddress {
        const port = self.last_local_port orelse return null;
        var addr = self.base.local_addr orelse quic_zig_adapter.defaultClientBindAddress(self.base.remote_addr);
        if (addr.getPort() != 0) return null;
        addr.setPort(port);
        return addr;
    }

    /// Dial from the last generation's local port, and fall back to the
    /// port `options` asks for (ephemeral unless `base.local_addr` names
    /// one) when that port cannot be bound.
    ///
    /// Why the port matters: quic-zig binds a NEW_TOKEN to the client's IP
    /// address and port. A server with Retry on (the hardened preset)
    /// validates a returning client's address with that token and skips the
    /// Retry only for a dial from the same address and port. A Retry costs a
    /// round trip (see `Outcome.retried_generations`); the 0-RTT restore
    /// still runs before the handshake completes. The previous generation's socket
    /// is closed by now, so the port is normally free; when another socket
    /// took it, the dial falls back and `port_fallback_generations` counts
    /// it.
    fn dial(self: *WarmRedialClient, options: ClientOptions) !Connection {
        const kept = self.keptLocalAddr() orelse return Connection.initClient(self.allocator, self.io, options);
        var kept_options = options;
        kept_options.local_addr = kept;
        if (Connection.initClient(self.allocator, self.io, kept_options)) |conn| {
            return conn;
        } else |err| switch (err) {
            // The errors that say this address cannot be bound now. Any other
            // error would fail the fallback dial too.
            error.AddressInUse, error.AddressUnavailable, error.AccessDenied => {
                log.debug("local port {d} unavailable ({}); dialing from an ephemeral port", .{ kept.getPort(), err });
            },
            else => return err,
        }
        const conn = try Connection.initClient(self.allocator, self.io, options);
        self.port_fallback_generations +|= 1;
        return conn;
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

        var conn = try self.dial(options);
        var conn_alive = true;
        defer if (conn_alive) conn.deinit();
        // The next generation dials from this port: a NEW_TOKEN that this
        // connection receives is valid only from here.
        const bound_port = conn.getAddress().getPort();
        self.last_local_port = if (bound_port != 0) bound_port else null;

        {
            self.mu.lockUncancelable(self.io);
            defer self.mu.unlock(self.io);
            if (self.stop_requested) return true;
            self.current_conn = &conn;
        }
        // Unpublish before `conn.deinit()` on every path: the error-path
        // defers run this before the `conn` defer above, and the normal path
        // calls it before its teardown below. After it returns, no
        // `requestStop` can reach `conn`.
        var conn_published = true;
        defer if (conn_published) self.unpublishConn();

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
        // The quic connection outlives `run()` until `conn.deinit()`, and
        // its verdict is final once the handshake completed.
        if (conn.activeQuicConnection()) |quic_conn| {
            // A Retry costs a round trip. Since quic-zig v0.27.0 the client
            // sends its 0-RTT data again after the Retry (RFC 9000
            // 17.2.5.3), so an `.accepted` verdict means the restore rode
            // 0-RTT with or without one: the transport tests "after a Retry
            // the resumed dial's restore still arrives in 0-RTT (quic-zig F8
            // fixed)" and the peer test "WarmRedialClient heal falls back to
            // an ephemeral port when its previous port is taken, and pays a
            // Retry" pin it. Through v0.25.0 a retried dial restored only
            // after the handshake, and did not count as 0-RTT.
            if (quic_conn.retryAccepted()) self.retried_generations +|= 1;
            if (quic_conn.earlyDataStatus() == .accepted) self.zero_rtt_generations +|= 1;
        }

        // A `requestStop` that holds `mu` finishes its requestClose first.
        self.unpublishConn();
        conn_published = false;
        _ = peer.takeAttachedConnection(*Connection);
        peer.deinit();
        peer_alive = false;
        conn.deinit();
        conn_alive = false;

        return self.stopRequested();
    }

    /// Withdraw the generation's connection from `requestStop`. It waits for
    /// a `requestStop` that holds `mu` to finish with the connection.
    fn unpublishConn(self: *WarmRedialClient) void {
        self.mu.lockUncancelable(self.io);
        defer self.mu.unlock(self.io);
        self.current_conn = null;
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
