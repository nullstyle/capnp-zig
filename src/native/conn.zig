//! `capnp_conn`: one sans-IO RPC connection (plan §2, §4). Zig API first;
//! `abi.zig` wraps it in `capnp_*` C exports later.
//!
//! A `Conn` is a capnp-zig `Peer` (detached: it owns no socket) plus a
//! `wire.framing.Framer`, an effect queue (`effects.zig`) and the per-export /
//! per-question host bookkeeping. The host pushes transport bytes in with
//! `pushBytes`, drives time with `tick`, and drains `nextEffect` /
//! `commitEffect` after every call. Every Peer callback only queues an
//! effect; the core never calls the host.
//!
//! Moved here from capnp-swift's core (handoff H7). It imports capnp-zig only
//! as "capnpc-zig", the library module's import of itself.
//!
//! Peer API used (written against capnp-zig v0.21.0, `src/rpc/peer/mod.zig`):
//!   initDetachedWithLimits, disableThreadAffinity, attachTransportBinding,
//!   setClock, setTimeouts, setObserver, start, handleFrame,
//!   notifyTransportClosed, checkDeadlines, sendBuilder, sendBootstrap,
//!   sendCallWithOptions and sendCallPromisedWithOpsWithOptions (+ CallOptions
//!   .retained), setQuestionDeinitCtx, finishRetainedQuestion,
//!   cancelQuestionTyped, setQuestionDeadline, releaseImport,
//!   addExportWithDeinit, setBootstrap, addPromiseExport,
//!   resolvePromiseExportToExport/ToImport/ToException, shutdown,
//!   sendReturnResults, sendReturnExceptionTyped, getLastRemoteAbortReason,
//!   deinit; field `caps` (the cap table) is read for handle validation and
//!   written to note PROMISED payload caps (cap_remap.zig).

const std = @import("std");
const capnp = @import("capnpc-zig");
const effects = @import("effects.zig");
const cap_remap = @import("cap_remap.zig");

const rpc = capnp.rpc;
const Peer = rpc.peer.Peer;
const protocol = rpc.wire.protocol;
const cap_table = rpc.caps.table;
const Framer = rpc.wire.framing.Framer;
const events = rpc.events;

pub const Cap = effects.Cap;
pub const CapKind = effects.CapKind;
pub const Effect = effects.Effect;
pub const ReturnKind = effects.ReturnKind;

pub const Options = struct {
    /// The host's monotonic clock now (`DispatchTime` uptime ns), the same
    /// clock later `tick`s pass. Required: deadlines of questions sent before
    /// the first tick count from it, so a made-up 0 would let the first real
    /// tick expire them at once.
    now_ns: i64,
    limits: rpc.peer.PeerLimits = .{},
    timeouts: ?rpc.peer.PeerTimeouts = null,
    /// Framer buffer cap (bytes of one in-progress inbound frame). In
    /// `.u32_le` mode this caps one length-prefixed payload.
    max_frame_bytes: usize = Framer.default_max_buffered_bytes,
    /// Queue an EVENT effect for every Peer observer event.
    observer: bool = false,
    /// Inbound byte-stream framing (plan §2: TCP/Unix/TLS segment table,
    /// QUIC baseline u32 LE). Outgoing frames always carry a standalone
    /// segment-table message; in `.u32_le` each is prefixed with its
    /// little-endian u32 length on the way out and de-prefixed on the way
    /// in, matching capnp-zig's QUIC baseline `LengthDelimitedFramer`.
    framing: Framing = .segment_table,
};

/// The byte-stream framing of one connection (Options.framing).
pub const Framing = enum { segment_table, u32_le };

/// Inbound codec for `.u32_le`: bytes arrive as 4-byte LE length prefixes,
/// each naming one standalone (segment-table) message. A zero length is a
/// framing error; the payload cap is `max_frame_bytes` (QUIC's
/// `max_message_bytes` role).
const LengthCodec = struct {
    pending: std.ArrayList(u8) = .empty,
    /// Total payload length once the prefix is complete.
    expected: ?usize = null,
    /// Payload cap (from Conn opts).
    max_bytes: usize = 0,

    fn deinit(self: *LengthCodec, allocator: std.mem.Allocator) void {
        self.pending.deinit(allocator);
    }

    /// Feed bytes; `deliver` runs for every complete payload.
    fn push(
        self: *LengthCodec,
        allocator: std.mem.Allocator,
        bytes: []const u8,
        ctx: anytype,
        comptime deliver: fn (@TypeOf(ctx), []const u8) anyerror!void,
    ) !void {
        var rest = bytes;
        while (true) {
            const have = self.pending.items.len;
            const expected = self.expected orelse {
                const need = 4 - have;
                if (rest.len < need) {
                    try self.pending.appendSlice(allocator, rest);
                    return;
                }
                var prefix: [4]u8 = undefined;
                @memcpy(prefix[0..have], self.pending.items);
                @memcpy(prefix[have..], rest[0..need]);
                rest = rest[need..];
                self.pending.clearRetainingCapacity();
                const len = std.mem.readInt(u32, &prefix, .little);
                if (len == 0) return error.InvalidLengthPrefix;
                if (len > self.max_bytes) return error.FrameTooLarge;
                self.expected = len;
                continue;
            };
            const want = expected - have;
            if (rest.len < want) {
                try self.pending.appendSlice(allocator, rest);
                return;
            }
            const payload_end = have + want;
            try self.pending.appendSlice(allocator, rest[0..want]);
            rest = rest[want..];
            const payload = try allocator.dupe(u8, self.pending.items[0..payload_end]);
            defer allocator.free(payload);
            self.pending.clearRetainingCapacity();
            self.expected = null;
            try deliver(ctx, payload);
            if (rest.len == 0) return;
        }
    }
};

/// Diagnostic counters. They saturate: a long-lived connection must never
/// reach an overflow trap through bookkeeping.
pub const Stats = struct {
    /// Questions that ended through their `on_return` callback.
    terminal_via_on_return: u64 = 0,
    /// Questions that ended ONLY through `deinit_ctx` (claims.json #5:
    /// synthetic-Return OOM, swept third-party awaits).
    terminal_via_deinit_ctx: u64 = 0,
    /// Questions `transportClosed` ended itself because the Peer left them
    /// open (capnp-zig v0.20.0 skips every question when its cancel list
    /// cannot be allocated; handoff H8).
    terminal_via_close_sweep: u64 = 0,
    exports_dropped: u64 = 0,
    events_dropped: u64 = 0,
};

/// Reason text of the fallback RETURN when copying results ran out of memory.
pub const oom_results_reason = "capnp-swift core: out of memory copying results";
/// Reason text of the RETURN when the results would copy larger than the
/// frame they arrived in (aliased pointers; `cap_remap.copyInbound`).
pub const oversized_results_reason = "capnp-swift core: results payload copies larger than its frame";
/// Reason text of the RETURN when the results could not be copied otherwise.
pub const bad_results_reason = "capnp-swift core: results payload could not be copied";
/// Reason text of the RETURN when the results carry a capability that is a
/// still-unresolved promised answer of ours (`cap_remap.copyInbound`).
pub const promised_results_reason = "capnp-swift core: results carry an unresolved promised capability";
/// Reason the Peer synthesizes for a question the host cancelled.
pub const cancel_reason = "canceled by the host";

/// The name of a Peer observer event tag (`effects.Event.tag`), or "unknown".
pub fn eventName(tag: u8) [:0]const u8 {
    const Tag = std.meta.Tag(events.Event);
    const t = std.enums.fromInt(Tag, tag) orelse return "unknown";
    return @tagName(t);
}

pub const Conn = struct {
    allocator: std.mem.Allocator,
    peer: Peer,
    framer: Framer,
    framing: Framing,
    length_codec: LengthCodec,
    queue: effects.Queue = .{},
    /// Monotonic time of the last `tick`; the Peer's clock reads it.
    now_ns: i64 = 0,
    /// Bootstrap export wrapper (no `deinit_ctx`: the bootstrap export lives
    /// until `deinit`, and never produces EXPORT_DROPPED).
    bootstrap_ctx: ?*ExportCtx = null,
    /// Answer ids handed to the host as INBOUND_CALL and not yet answered.
    pending_answers: std.AutoHashMap(u32, void),
    /// CLOSE_REQUESTED node reserved at init so a close request never needs
    /// memory. Null once queued.
    close_node: ?*effects.Node = null,
    /// The question context whose `sendCall` is on the stack (see `call`).
    sending_qctx: ?*QuestionCtx = null,
    /// Adopted questions whose terminal has not fired yet (intrusive list,
    /// swept without allocation at transport close).
    live_questions: ?*QuestionCtx = null,
    /// The same questions by id (cancel, deadlines, PROMISED validation).
    /// Capacity is reserved before each send, so adoption cannot fail.
    questions: std.AutoHashMap(u32, *QuestionCtx),

    close_requested: bool = false,
    transport_closed: bool = false,
    /// Set once the local side is gone (transport closed, teardown): a
    /// synthetic `.disconnected` exception after this point is reported as
    /// RETURN{DISCONNECTED}, not as a remote EXCEPTION.
    local_disconnect: bool = false,
    /// Set during `deinit`: callbacks free their payloads instead of queuing
    /// (plan §4.1 teardown contract: no effects during free).
    discarding: bool = false,
    /// A protocol error closed this connection; further input is refused.
    failed: bool = false,
    /// `shutdown` was called: no new questions; input and ticks still flow
    /// so the open questions can drain; the Peer closes when they are done
    /// or the drain timeout passes.
    shutting_down: bool = false,
    /// The remote closed this connection with an Abort; further input is
    /// refused. Not a protocol failure: the shim sends no Abort back.
    remote_aborted: bool = false,
    /// What `capnp_conn_take_error` reports (M1). `error.RemoteAbort` after
    /// a remote Abort, with the remote's reason in `remote_abort_reason`.
    last_error: ?anyerror = null,
    /// Owned copy of the remote Abort's reason (null if none arrived, or if
    /// copying it ran out of memory). Also the reason of every RETURN that
    /// ends a question because of that Abort. Freed after the effect queue.
    remote_abort_reason: ?[]const u8 = null,
    stats: Stats = .{},

    pub fn init(allocator: std.mem.Allocator, opts: Options) !*Conn {
        const self = try allocator.create(Conn);
        errdefer allocator.destroy(self);
        const close_node = try effects.Node.create(allocator);
        errdefer close_node.destroy(allocator);
        close_node.effect = .close_requested;

        self.* = .{
            .allocator = allocator,
            .peer = Peer.initDetachedWithLimits(allocator, opts.limits),
            .framer = Framer.initWithOptions(allocator, .{ .max_buffered_bytes = opts.max_frame_bytes }),
            .framing = opts.framing,
            .length_codec = .{ .max_bytes = opts.max_frame_bytes },
            .pending_answers = std.AutoHashMap(u32, void).init(allocator),
            .questions = std.AutoHashMap(u32, *QuestionCtx).init(allocator),
            .close_node = close_node,
            .now_ns = opts.now_ns,
        };
        // Swift actors hop threads; the host serializes calls per connection.
        self.peer.disableThreadAffinity();
        // A detached peer with no binding drops close requests (plan §1.1).
        self.peer.attachTransportBinding(.{
            .ctx = self,
            .send = bindingSend,
            .close = bindingClose,
            .is_closing = bindingIsClosing,
        });
        // With a clock, deadlines and the validation-work budget are live.
        self.peer.setClock(.{ .ctx = self, .now_fn = clockNow });
        if (opts.timeouts) |t| self.peer.setTimeouts(t);
        if (opts.observer) self.peer.setObserver(.{ .ctx = self, .on_event = onEvent });
        self.peer.start(self, onPeerError, onPeerClose);
        return self;
    }

    /// Teardown contract (plan §4.1): the Peer is torn down while every
    /// callback discards; then the queue (and any in-flight effect) is freed.
    /// No effect is produced during `deinit`.
    pub fn deinit(self: *Conn) void {
        const a = self.allocator;
        self.discarding = true;
        self.local_disconnect = true;
        self.peer.deinit();
        if (self.bootstrap_ctx) |ec| {
            ec.dropped.destroy(a);
            a.destroy(ec);
        }
        if (self.close_node) |n| n.destroy(a);
        self.queue.deinit(a);
        // After the queue: RETURN effects may borrow it.
        if (self.remote_abort_reason) |r| a.free(r);
        self.pending_answers.deinit();
        self.questions.deinit();
        self.framer.deinit();
        self.length_codec.deinit(a);
        a.destroy(self);
    }

    // ------------------------------------------------------------------
    // Transport side
    // ------------------------------------------------------------------

    /// Feed transport bytes. Complete frames are dispatched to the Peer
    /// before this returns. On a framing or protocol error the connection
    /// fails: an Abort OUT_FRAME is queued (the Peer's own, or ours when the
    /// Peer sent none), then CLOSE_REQUESTED, and `error.Protocol` is
    /// returned (`last_error` holds the cause).
    ///
    /// A remote Abort is an orderly close, not a protocol failure: no Abort
    /// goes back, CLOSE_REQUESTED is queued, `error.Closed` is returned, and
    /// `last_error` is `error.RemoteAbort` with the remote's reason in
    /// `remote_abort_reason`. Questions still open then end with that reason
    /// once the host reports the transport closed.
    pub fn pushBytes(self: *Conn, bytes: []const u8) !void {
        if (self.isClosed()) return error.Closed;
        switch (self.framing) {
            .segment_table => {
                self.framer.push(bytes) catch |err| return self.failFraming(err);
                while (true) {
                    const frame = (self.framer.popFrame() catch |err| return self.failFraming(err)) orelse break;
                    defer self.allocator.free(frame);
                    try self.handleOneFrame(frame);
                }
            },
            .u32_le => {
                // The codec owns reassembly; each complete payload is exactly
                // one standalone message.
                self.length_codec.push(self.allocator, bytes, self, deliverLengthDelimited) catch |err| return self.failFraming(err);
            },
        }
    }

    fn handleOneFrame(self: *Conn, frame: []const u8) !void {
        const mark = self.queue.tail;
        self.peer.handleFrame(frame) catch |err| {
            if (err == error.RemoteAbort) return self.closeByRemoteAbort();
            if (!self.abortQueuedSince(mark)) self.sendAbort(err);
            return self.failProtocol(err);
        };
    }

    fn deliverLengthDelimited(self: *Conn, payload: []const u8) anyerror!void {
        try self.handleOneFrame(payload);
    }

    /// Advance the Peer's clock and run its maintenance (deadlines, Finish
    /// retries). Returns the number of questions cancelled.
    pub fn tick(self: *Conn, now_ns: i64) usize {
        self.now_ns = now_ns;
        return self.peer.checkDeadlines();
    }

    /// The host's transport is gone. Every open question ends with one
    /// RETURN{DISCONNECTED}, queued before this returns.
    pub fn transportClosed(self: *Conn) void {
        if (self.transport_closed) return;
        self.transport_closed = true;
        self.local_disconnect = true;
        self.peer.notifyTransportClosed();
        // The Peer may leave questions open: under OOM capnp-zig v0.20.0
        // cancels none of them (its id list is allocated, `catch break`;
        // handoff H8), and claims.json #5 lets terminals come later. End
        // every one still open here. If the Peer calls back for one later,
        // that callback only frees its context.
        self.sweepOpenQuestions();
    }

    pub fn nextEffect(self: *Conn) error{Busy}!?*const Effect {
        return self.queue.next();
    }

    pub fn commitEffect(self: *Conn) void {
        self.queue.commit(self.allocator);
    }

    // ------------------------------------------------------------------
    // Client side
    // ------------------------------------------------------------------

    /// Ask for the remote bootstrap capability. The RETURN carries a message
    /// whose root is a capability pointer to `caps[0]`.
    pub fn bootstrap(self: *Conn) !u32 {
        try self.checkOpen();
        const qc = try self.newQuestionCtx();
        self.sending_qctx = qc;
        const qid = self.peer.sendBootstrap(qc, onQuestionReturn) catch |err| {
            self.sending_qctx = null;
            self.abandonQuestionCtx(qc);
            return err;
        };
        return self.adoptQuestion(qc, qid);
    }

    /// Call `method_id` of `interface_id` on `target`: an IMPORT, or a
    /// PROMISED answer (one of this side's open questions plus a pipeline
    /// path: the call goes out before that question returns). `msg` is a
    /// standalone message whose root is the params struct; its capability
    /// pointers index `caps` (IMPORT, EXPORT, NONE or PROMISED entries). The
    /// question is `.retained`: the host must `finish` it after its RETURN.
    pub fn call(
        self: *Conn,
        target: Cap,
        interface_id: u64,
        method_id: u16,
        msg: []const u8,
        caps: []const Cap,
        flags: u32,
    ) !u32 {
        try self.checkOpen();
        if (flags != 0) return error.Unsupported; // STREAMING: later
        switch (target.kind) {
            .import => if (cap_remap.importRefCount(&self.peer.caps, target.id) == 0) return error.BadId,
            .promised => if (!self.questions.contains(target.id)) return error.BadId,
            .none, .@"export" => return error.Unsupported,
        }
        for (caps) |cap| {
            if (cap.kind == .promised and !self.questions.contains(cap.id)) return error.BadId;
        }
        const qc = try self.newQuestionCtx();
        qc.msg = msg;
        qc.caps = caps;
        self.sending_qctx = qc;
        const qid = switch (target.kind) {
            .import => self.peer.sendCallWithOptions(
                target.id,
                interface_id,
                method_id,
                qc,
                buildCall,
                onQuestionReturn,
                .{ .result_lifetime = .retained },
            ),
            .promised => blk: {
                const ops = cap_remap.promisedOps(self.allocator, target) catch |err| break :blk err;
                defer self.allocator.free(ops);
                break :blk self.peer.sendCallPromisedWithOpsWithOptions(
                    target.id,
                    ops,
                    interface_id,
                    method_id,
                    qc,
                    buildCall,
                    onQuestionReturn,
                    .{ .result_lifetime = .retained },
                );
            },
            // Refused by the first switch above; the same error if reached.
            .none, .@"export" => error.Unsupported,
        } catch |err| {
            self.sending_qctx = null;
            self.abandonQuestionCtx(qc);
            return err;
        };
        return self.adoptQuestion(qc, qid);
    }

    /// Cancel an open question. The Peer sends Finish and ends the question
    /// at once with a locally synthesized exception, which the host sees as
    /// one RETURN{CANCELED}; a late Return from the remote is absorbed.
    /// `error.BadId` for a question that already ended. A no-op once closed.
    pub fn cancel(self: *Conn, qid: u32) !void {
        if (self.isClosed()) return;
        const qc = self.questions.get(qid) orelse return error.BadId;
        qc.cancel_requested = true;
        self.peer.cancelQuestionTyped(qid, cancel_reason, .failed) catch |err| {
            qc.cancel_requested = false;
            return err;
        };
    }

    /// Set (or replace) the deadline of an open question, in ms from the
    /// clock's now; it fires on a later `tick` as RETURN{EXCEPTION overloaded}.
    pub fn setDeadline(self: *Conn, qid: u32, timeout_ms: u64) !void {
        if (self.isClosed()) return error.Closed;
        if (!self.questions.contains(qid)) return error.BadId;
        try self.peer.setQuestionDeadline(qid, timeout_ms);
    }

    /// Begin a graceful shutdown: no new questions; input and ticks still
    /// flow so open questions can return. When none is left (or the drain
    /// timeout passes on a `tick`, ending the rest with RETURN{DISCONNECTED}),
    /// the Peer asks the host to close (CLOSE_REQUESTED). Idempotent.
    pub fn shutdown(self: *Conn) void {
        if (self.isClosed() or self.shutting_down) return;
        self.shutting_down = true;
        self.local_disconnect = true;
        self.peer.shutdown(null);
    }

    /// Finish a retained question (the host dropped its last handle on it).
    pub fn finish(self: *Conn, qid: u32, release_result_caps: bool) !void {
        // Once closed nothing more may be sent; the Peer frees its retained
        // records at deinit.
        if (self.isClosed()) return;
        try self.peer.finishRetainedQuestion(qid, release_result_caps);
    }

    /// Release `count` wire references the host holds on `import_id`.
    pub fn release(self: *Conn, import_id: u32, count: u32) !void {
        if (count == 0) return;
        // Once closed nothing more may be sent; the Peer frees its import
        // table at deinit.
        if (self.isClosed()) return;
        if (cap_remap.importRefCount(&self.peer.caps, import_id) < count) return error.BadId;
        try self.peer.releaseImport(import_id, count);
    }

    // ------------------------------------------------------------------
    // Server side
    // ------------------------------------------------------------------

    /// Export a host object. Calls on it arrive as INBOUND_CALL carrying
    /// `host_tag`; EXPORT_DROPPED fires once when the remote has released it
    /// (or at connection teardown, which `deinit` discards).
    pub fn exportCap(self: *Conn, host_tag: u64) !u32 {
        const ec = try self.newExportCtx(host_tag);
        errdefer self.destroyExportCtx(ec);
        const id = try self.peer.addExportWithDeinit(.{ .ctx = ec, .on_call = onExportCall }, onExportDeinit);
        ec.export_id = id;
        return id;
    }

    /// Make the host object `host_tag` this connection's bootstrap. It lives
    /// until `deinit` and never produces EXPORT_DROPPED.
    pub fn setBootstrap(self: *Conn, host_tag: u64) !u32 {
        if (self.bootstrap_ctx != null) return error.Invalid;
        const ec = try self.newExportCtx(host_tag);
        errdefer self.destroyExportCtx(ec);
        const id = try self.peer.setBootstrap(.{ .ctx = ec, .on_call = onExportCall });
        ec.export_id = id;
        self.bootstrap_ctx = ec;
        return id;
    }

    /// Export a promise: a capability the host will resolve later with
    /// `resolvePromise` or `rejectPromise`. Calls on it queue in the Peer
    /// until then. It carries no host tag and never produces EXPORT_DROPPED.
    pub fn promiseExport(self: *Conn) !u32 {
        try self.checkOpen();
        return self.peer.addPromiseExport();
    }

    /// Resolve promise export `promise_id` to `to`: one of this side's EXPORTs
    /// or an IMPORT it holds. A no-op once closed.
    pub fn resolvePromise(self: *Conn, promise_id: u32, to: Cap) !void {
        if (self.isClosed()) return;
        switch (to.kind) {
            .@"export" => try self.peer.resolvePromiseExportToExport(promise_id, to.id),
            .import => {
                if (cap_remap.importRefCount(&self.peer.caps, to.id) == 0) return error.BadId;
                try self.peer.resolvePromiseExportToImport(promise_id, to.id);
            },
            .none, .promised => return error.Unsupported,
        }
    }

    /// Reject promise export `promise_id` with an exception (`failed`; a
    /// typed rejection needs capnp-zig handoff H5). A no-op once closed.
    pub fn rejectPromise(self: *Conn, promise_id: u32, reason: []const u8) !void {
        if (self.isClosed()) return;
        try self.peer.resolvePromiseExportToException(promise_id, reason);
    }

    /// Answer an INBOUND_CALL with results (`msg` + `caps`, as for `call`).
    pub fn returnResults(self: *Conn, answer_id: u32, msg: []const u8, caps: []const Cap) !void {
        if (!self.pending_answers.contains(answer_id)) return error.BadId;
        for (caps) |cap| {
            if (cap.kind == .promised and !self.questions.contains(cap.id)) return error.BadId;
        }
        var bc: ReturnBuildCtx = .{ .conn = self, .msg = msg, .caps = caps };
        try self.peer.sendReturnResults(answer_id, &bc, buildReturn);
        _ = self.pending_answers.remove(answer_id);
    }

    /// Answer an INBOUND_CALL with an exception.
    pub fn returnException(self: *Conn, answer_id: u32, exception_type: u16, reason: []const u8) !void {
        if (!self.pending_answers.contains(answer_id)) return error.BadId;
        try self.peer.sendReturnExceptionTyped(answer_id, reason, @fromBackingInt(exception_type));
        _ = self.pending_answers.remove(answer_id);
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    fn isClosed(self: *const Conn) bool {
        return self.failed or self.remote_aborted or self.transport_closed;
    }

    fn checkOpen(self: *Conn) !void {
        if (self.isClosed() or self.shutting_down) return error.Closed;
    }

    /// The remote sent Abort (the Peer kept its reason and returned
    /// `error.RemoteAbort`). Close without answering it.
    fn closeByRemoteAbort(self: *Conn) error{Closed} {
        self.last_error = error.RemoteAbort;
        self.remote_aborted = true;
        if (self.remote_abort_reason == null) {
            if (self.peer.getLastRemoteAbortReason()) |reason| {
                // Best effort: without memory the RETURNs keep the generic reason.
                self.remote_abort_reason = self.allocator.dupe(u8, reason) catch null;
            }
        }
        self.framer.reset();
        self.requestClose();
        return error.Closed;
    }

    /// Reason of a RETURN{DISCONNECTED}: the remote's Abort reason when the
    /// remote closed the connection, else the Peer's generic one.
    fn disconnectReason(self: *const Conn) []const u8 {
        return self.remote_abort_reason orelse rpc.peer.disconnected_reason;
    }

    /// End every question still in `live_questions` with RETURN{DISCONNECTED},
    /// from the node each reserved at send time (no allocation).
    fn sweepOpenQuestions(self: *Conn) void {
        while (self.live_questions) |qc| {
            self.unlinkQuestion(qc);
            qc.swept = true;
            self.stats.terminal_via_close_sweep +|= 1;
            const node = qc.node;
            node.freePayload(self.allocator);
            node.effect = .{ .@"return" = .{
                .qid = qc.qid,
                .kind = .disconnected,
                .exception_type = @backingInt(protocol.ExceptionType.disconnected),
                .reason = self.disconnectReason(),
            } };
            self.pushNode(node);
        }
    }

    fn linkQuestion(self: *Conn, qc: *QuestionCtx) void {
        qc.prev = null;
        qc.next = self.live_questions;
        if (self.live_questions) |head| head.prev = qc;
        self.live_questions = qc;
        qc.linked = true;
        // Capacity was reserved in newQuestionCtx.
        self.questions.putAssumeCapacity(qc.qid, qc);
    }

    fn unlinkQuestion(self: *Conn, qc: *QuestionCtx) void {
        if (!qc.linked) return;
        if (qc.prev) |p| p.next = qc.next else self.live_questions = qc.next;
        if (qc.next) |n| n.prev = qc.prev;
        qc.prev = null;
        qc.next = null;
        qc.linked = false;
        _ = self.questions.remove(qc.qid);
    }

    fn pushNode(self: *Conn, node: *effects.Node) void {
        self.queue.push(node);
    }

    fn requestClose(self: *Conn) void {
        if (self.close_requested or self.discarding) return;
        self.close_requested = true;
        const node = self.close_node orelse return;
        self.close_node = null;
        self.pushNode(node);
    }

    /// The byte stream is unrecoverable (bad segment table, frame too big):
    /// the Peer never saw it, so the Abort is ours.
    fn failFraming(self: *Conn, err: anyerror) error{Protocol} {
        self.sendAbort(err);
        return self.failProtocol(err);
    }

    fn failProtocol(self: *Conn, err: anyerror) error{Protocol} {
        self.last_error = err;
        self.failed = true;
        self.framer.reset();
        self.requestClose();
        return error.Protocol;
    }

    /// True when an OUT_FRAME queued after `mark` is an Abort (the Peer sends
    /// one itself for decode/validation failures, not for dispatch errors).
    fn abortQueuedSince(self: *Conn, mark: ?*effects.Node) bool {
        var it = self.queue.iteratorSince(mark);
        while (it.next()) |node| {
            switch (node.effect) {
                .out_frame => |bytes| {
                    var decoded = protocol.DecodedMessage.init(self.allocator, bytes) catch continue;
                    defer decoded.deinit();
                    if (decoded.tag == .abort) return true;
                },
                else => {},
            }
        }
        return false;
    }

    fn sendAbort(self: *Conn, err: anyerror) void {
        var mb = protocol.MessageBuilder.init(self.allocator);
        defer mb.deinit();
        mb.buildAbortTyped(@errorName(err), .failed) catch return;
        self.peer.sendBuilder(&mb) catch {};
    }

    fn newQuestionCtx(self: *Conn) !*QuestionCtx {
        try self.questions.ensureUnusedCapacity(1);
        const node = try effects.Node.create(self.allocator);
        errdefer node.destroy(self.allocator);
        const qc = try self.allocator.create(QuestionCtx);
        qc.* = .{ .conn = self, .node = node };
        return qc;
    }

    /// The send failed: no callback will ever see `qc`.
    fn abandonQuestionCtx(self: *Conn, qc: *QuestionCtx) void {
        if (!qc.done) qc.node.destroy(self.allocator);
        self.allocator.destroy(qc);
    }

    /// The send succeeded. If the question already ended inside the send
    /// (synchronous loopback), `qc` was kept alive for us; free it now.
    fn adoptQuestion(self: *Conn, qc: *QuestionCtx, qid: u32) u32 {
        self.sending_qctx = null;
        if (qc.done) {
            self.allocator.destroy(qc);
            return qid;
        }
        qc.qid = qid;
        qc.msg = &.{};
        qc.caps = &.{};
        // Each question ends through on_return OR deinit_ctx, never both
        // (claims.json #5). Also turns off restore_on_return_error.
        self.peer.setQuestionDeinitCtx(qid, onQuestionDeinit);
        self.linkQuestion(qc);
        return qid;
    }

    fn releaseQuestionCtx(self: *Conn, qc: *QuestionCtx) void {
        if (self.sending_qctx == qc) {
            qc.done = true;
            return;
        }
        self.unlinkQuestion(qc);
        self.allocator.destroy(qc);
    }

    fn newExportCtx(self: *Conn, host_tag: u64) !*ExportCtx {
        const dropped = try effects.Node.create(self.allocator);
        errdefer dropped.destroy(self.allocator);
        const ec = try self.allocator.create(ExportCtx);
        ec.* = .{ .conn = self, .host_tag = host_tag, .dropped = dropped };
        return ec;
    }

    fn destroyExportCtx(self: *Conn, ec: *ExportCtx) void {
        ec.dropped.destroy(self.allocator);
        self.allocator.destroy(ec);
    }

    fn fillReturn(self: *Conn, node: *effects.Node, ret: protocol.Return, caps: *const cap_table.InboundCapTable, canceled: bool) !void {
        const qid = ret.answer_id;
        switch (ret.tag) {
            .results => {
                const payload = ret.results orelse return error.MissingResults;
                // copyInbound retains the imports as its last, infallible
                // step: nothing below may fail once it returns.
                const in = try cap_remap.copyInbound(self.allocator, payload.content, caps);
                node.owned_bytes = in.msg;
                node.owned_caps = in.caps;
                node.effect = .{ .@"return" = .{ .qid = qid, .kind = .results, .msg = in.msg, .caps = in.caps } };
            },
            .exception => {
                const t = self.exceptionTerminal(ret, canceled);
                const reason = try self.allocator.dupe(u8, t.text);
                node.owned_reason = reason;
                node.effect = .{ .@"return" = .{
                    .qid = qid,
                    .kind = t.kind,
                    .exception_type = t.exception_type,
                    .reason = reason,
                } };
            },
            .canceled => node.effect = .{ .@"return" = .{ .qid = qid, .kind = .canceled } },
            else => node.effect = .{ .@"return" = .{
                .qid = qid,
                .kind = .exception,
                .exception_type = @backingInt(protocol.ExceptionType.unimplemented),
                .reason = "capnp-swift core: unsupported Return variant",
            } },
        }
    }

    /// The terminal `fillReturn` could not build (its copy failed), built
    /// without memory: every reason is static. An exception Return keeps the
    /// kind and type `fillReturn` gives it, and its reason when that is one
    /// the Peer or the shim synthesizes (`staticReason`). Only the reason
    /// copy can fail there, so since capnp-zig 0.23.0 (handoff H8) this is
    /// how every cancel, deadline and disconnect reaches the host under OOM:
    /// the Peer delivers those without the heap.
    fn fallbackReturn(self: *const Conn, ret: protocol.Return, canceled: bool, err: anyerror) effects.Return {
        if (ret.tag == .exception) {
            const t = self.exceptionTerminal(ret, canceled);
            return .{
                .qid = ret.answer_id,
                .kind = t.kind,
                .exception_type = t.exception_type,
                .reason = staticReason(t.text) orelse switch (t.kind) {
                    .canceled => cancel_reason,
                    .disconnected => self.disconnectReason(),
                    .exception, .results => oom_results_reason,
                },
            };
        }
        return .{
            .qid = ret.answer_id,
            .kind = .exception,
            .exception_type = @backingInt(protocol.ExceptionType.failed),
            .reason = switch (err) {
                error.OutOfMemory => oom_results_reason,
                error.PayloadCopyExceedsFrame => oversized_results_reason,
                error.PromisedCapUnsupported => promised_results_reason,
                else => bad_results_reason,
            },
        };
    }

    const ExceptionTerminal = struct {
        kind: ReturnKind,
        exception_type: u16,
        /// Borrowed from `ret`, or this connection's remote Abort reason.
        text: []const u8,
    };

    /// How an exception Return reaches the host. `cancel` ends the question
    /// through a Peer-synthesized exception: the host asked, so it is
    /// CANCELED. A `disconnected` exception after the local side went away
    /// is DISCONNECTED (with the remote Abort's reason when there was one).
    /// Anything else is a remote (or deadline) EXCEPTION.
    fn exceptionTerminal(self: *const Conn, ret: protocol.Return, canceled: bool) ExceptionTerminal {
        const ex_type: u16 = if (ret.exception) |e| e.type_value else @backingInt(protocol.ExceptionType.failed);
        const local = self.local_disconnect and ex_type == @backingInt(protocol.ExceptionType.disconnected);
        const text = if (local and self.remote_abort_reason != null)
            self.disconnectReason()
        else if (ret.exception) |e| e.reason else "";
        const kind: ReturnKind = if (canceled) .canceled else if (local) .disconnected else .exception;
        return .{ .kind = kind, .exception_type = ex_type, .text = text };
    }
};

/// `text` as static storage when it is a reason the Peer synthesizes itself
/// (or the shim's `cancel_reason`), so a terminal can carry it without a
/// copy; null for any other text.
fn staticReason(text: []const u8) ?[]const u8 {
    const known = [_][]const u8{
        cancel_reason,
        rpc.peer.deadline_reason,
        rpc.peer.disconnected_reason,
        rpc.peer.shutdown_reason,
    };
    for (known) |reason| {
        if (std.mem.eql(u8, text, reason)) return reason;
    }
    return null;
}

const QuestionCtx = struct {
    conn: *Conn,
    qid: u32 = 0,
    /// RETURN node reserved at send time, so the terminal effect of a
    /// `deinit_ctx`-only end never needs memory.
    node: *effects.Node,
    /// Set when the question ended while its send was still on the stack.
    done: bool = false,
    /// Set when `sweepOpenQuestions` already queued this question's RETURN
    /// (its `node` belongs to the queue now). The Peer still holds the ctx;
    /// its later on_return / deinit_ctx only frees it.
    swept: bool = false,
    /// Set by `cancel` before the Peer synthesizes the terminal exception.
    cancel_requested: bool = false,
    /// `Conn.live_questions` links: adopted, terminal not fired yet.
    linked: bool = false,
    prev: ?*QuestionCtx = null,
    next: ?*QuestionCtx = null,
    /// `call` inputs, borrowed for the duration of `sendCall` only.
    msg: []const u8 = &.{},
    caps: []const Cap = &.{},
};

const ExportCtx = struct {
    conn: *Conn,
    export_id: u32 = 0,
    host_tag: u64,
    /// EXPORT_DROPPED node reserved at export time (`deinit_ctx` cannot fail).
    dropped: *effects.Node,
};

const ReturnBuildCtx = struct {
    conn: *Conn,
    msg: []const u8,
    caps: []const Cap,
};

fn castPtr(comptime T: type, ctx: *anyopaque) *T {
    return @ptrCast(@alignCast(ctx));
}

// ---- transport binding --------------------------------------------------

fn bindingSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
    const self = castPtr(Conn, ctx);
    if (self.discarding) return;
    if (self.transport_closed) return error.TransportClosed;
    const node = try effects.Node.create(self.allocator);
    errdefer node.destroy(self.allocator);
    if (self.framing == .u32_le) {
        const bytes = try self.allocator.alloc(u8, 4 + frame.len);
        std.mem.writeInt(u32, bytes[0..4], @intCast(frame.len), .little);
        @memcpy(bytes[4..], frame);
        node.owned_bytes = bytes;
        node.effect = .{ .out_frame = bytes };
    } else {
        const bytes = try self.allocator.dupe(u8, frame);
        node.owned_bytes = bytes;
        node.effect = .{ .out_frame = bytes };
    }
    self.pushNode(node);
}

fn bindingClose(ctx: *anyopaque) void {
    castPtr(Conn, ctx).requestClose();
}

fn bindingIsClosing(ctx: *anyopaque) bool {
    const self = castPtr(Conn, ctx);
    return self.close_requested or self.transport_closed;
}

fn clockNow(ctx: *anyopaque) i64 {
    return castPtr(Conn, ctx).now_ns;
}

fn onPeerError(ctx: ?*anyopaque, _: *Peer, err: anyerror) void {
    const self = castPtr(Conn, ctx orelse return);
    self.last_error = err;
}

fn onPeerClose(_: ?*anyopaque, _: *Peer) void {
    // Only reached from `notifyTransportClosed`, i.e. the host already knows.
}

fn onEvent(ctx: *anyopaque, event: events.Event) void {
    const self = castPtr(Conn, ctx);
    if (self.discarding) return;
    const node = effects.Node.create(self.allocator) catch {
        self.stats.events_dropped +|= 1;
        return;
    };
    const err_name: []const u8 = switch (event) {
        inline else => |payload| blk: {
            const P = @TypeOf(payload);
            if (@hasField(P, "err")) {
                const e = payload.err;
                if (@TypeOf(e) == anyerror) break :blk @errorName(e);
                if (e) |some| break :blk @errorName(some);
            }
            break :blk "";
        },
    };
    node.effect = .{ .event = .{ .tag = @intCast(@backingInt(std.meta.activeTag(event))), .err_name = err_name } };
    self.pushNode(node);
}

// ---- questions ----------------------------------------------------------

fn buildCall(ctx: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
    const qc = castPtr(QuestionCtx, ctx);
    var payload = try call_builder.payloadTyped();
    try cap_remap.writeHostContent(qc.conn.allocator, &qc.conn.peer.caps, &payload, qc.msg, qc.caps);
}

fn onQuestionReturn(
    ctx: *anyopaque,
    _: *Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    const qc = castPtr(QuestionCtx, ctx);
    const self = qc.conn;
    if (qc.swept) {
        // The close sweep already reported this question. Imports are not
        // retained, so the Peer releases them after this callback.
        self.allocator.destroy(qc);
        return;
    }
    const node = qc.node;
    const canceled = qc.cancel_requested;
    self.releaseQuestionCtx(qc);
    self.stats.terminal_via_on_return +|= 1;
    if (self.discarding) {
        node.destroy(self.allocator);
        return;
    }
    self.fillReturn(node, ret, caps, canceled) catch |err| {
        // Never lose the terminal: report it without the payload. Imports were
        // not retained, so the Peer releases them after this callback.
        node.freePayload(self.allocator);
        node.effect = .{ .@"return" = self.fallbackReturn(ret, canceled, err) };
    };
    self.pushNode(node);
}

fn onQuestionDeinit(allocator: std.mem.Allocator, ctx: *anyopaque) void {
    const qc = castPtr(QuestionCtx, ctx);
    const self = qc.conn;
    if (qc.swept) {
        allocator.destroy(qc);
        return;
    }
    const node = qc.node;
    const qid = qc.qid;
    self.releaseQuestionCtx(qc);
    self.stats.terminal_via_deinit_ctx +|= 1;
    if (self.discarding) {
        node.destroy(allocator);
        return;
    }
    node.freePayload(allocator);
    node.effect = .{ .@"return" = .{
        .qid = qid,
        .kind = .disconnected,
        .exception_type = @backingInt(protocol.ExceptionType.disconnected),
        .reason = self.disconnectReason(),
    } };
    self.pushNode(node);
}

// ---- exports ------------------------------------------------------------

fn onExportCall(
    ctx: *anyopaque,
    _: *Peer,
    call_msg: protocol.Call,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    const ec = castPtr(ExportCtx, ctx);
    const self = ec.conn;
    if (self.discarding) return error.ConnectionClosing;
    const node = try effects.Node.create(self.allocator);
    errdefer node.destroy(self.allocator);
    try self.pending_answers.ensureUnusedCapacity(1);
    // Retains the param imports as its last step; nothing below may fail.
    // An error here (e.g. error.PayloadCopyExceedsFrame for aliased params)
    // makes the Peer answer the call with an exception named after it.
    const in = try cap_remap.copyInbound(self.allocator, call_msg.params.content, caps);
    node.owned_bytes = in.msg;
    node.owned_caps = in.caps;
    node.effect = .{ .inbound_call = .{
        .answer_id = call_msg.question_id,
        .export_id = ec.export_id,
        .host_tag = ec.host_tag,
        .interface_id = call_msg.interface_id,
        .method_id = call_msg.method_id,
        .msg = in.msg,
        .caps = in.caps,
    } };
    self.pending_answers.putAssumeCapacity(call_msg.question_id, {});
    self.pushNode(node);
    // No reply here: the host answers later with returnResults/returnException.
}

fn onExportDeinit(allocator: std.mem.Allocator, ctx: *anyopaque) void {
    const ec = castPtr(ExportCtx, ctx);
    const self = ec.conn;
    const node = ec.dropped;
    const export_id = ec.export_id;
    const host_tag = ec.host_tag;
    allocator.destroy(ec);
    if (self.discarding) {
        node.destroy(allocator);
        return;
    }
    node.effect = .{ .export_dropped = .{ .export_id = export_id, .host_tag = host_tag } };
    self.stats.exports_dropped +|= 1;
    self.pushNode(node);
}

fn buildReturn(ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
    const bc = castPtr(ReturnBuildCtx, ctx);
    var payload = try ret.payloadTyped();
    try cap_remap.writeHostContent(bc.conn.allocator, &bc.conn.peer.caps, &payload, bc.msg, bc.caps);
}
