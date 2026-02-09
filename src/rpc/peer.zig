const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_peer);
const protocol = @import("protocol.zig");
const cap_table = @import("cap_table.zig");
const message = @import("../message.zig");
const peer_dispatch = @import("peer/peer_dispatch.zig");
const peer_control = @import("peer/peer_control.zig");
const peer_call_targets = @import("peer/call/peer_call_targets.zig");
const peer_call_sender = @import("peer/call/peer_call_sender.zig");
const payload_remap = @import("payload_remap.zig");
const peer_promises = @import("peer/peer_promises.zig");
const peer_inbound_release = @import("peer/peer_inbound_release.zig");
const peer_embargo_accepts = @import("peer/peer_embargo_accepts.zig");
const peer_join_state = @import("peer/provide/peer_join_state.zig");
const peer_provides_state = @import("peer/provide/peer_provides_state.zig");
const peer_provide_join_orchestration = @import("peer/provide/peer_provide_join_orchestration.zig");
const peer_forward_orchestration = @import("peer/forward/peer_forward_orchestration.zig");
const peer_forward_return_callbacks = @import("peer/forward/peer_forward_return_callbacks.zig");
const peer_cap_lifecycle = @import("peer/peer_cap_lifecycle.zig");
const peer_outbound_control = @import("peer/peer_outbound_control.zig");
const peer_call_orchestration = @import("peer/call/peer_call_orchestration.zig");
const peer_return_orchestration = @import("peer/return/peer_return_orchestration.zig");
const peer_third_party_adoption = @import("peer/third_party/peer_third_party_adoption.zig");
const peer_return_dispatch = @import("peer/return/peer_return_dispatch.zig");
const peer_third_party_returns = @import("peer/third_party/peer_third_party_returns.zig");
const peer_return_send_helpers = @import("peer/return/peer_return_send_helpers.zig");
const peer_transport_callbacks = @import("peer/peer_transport_callbacks.zig");
const peer_transport_state = @import("peer/peer_transport_state.zig");
const peer_question_state = @import("peer/peer_question_state.zig");
const peer_cleanup = @import("peer/peer_cleanup.zig");
const peer_state_types = @import("peer/peer_state_types.zig");

/// Callback invoked to populate a `CallBuilder` before sending an outbound call.
pub const CallBuildFn = *const fn (ctx: *anyopaque, call: *protocol.CallBuilder) anyerror!void;
/// Callback invoked to populate a `ReturnBuilder` before sending a return.
pub const ReturnBuildFn = *const fn (ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void;
/// Handler invoked when an inbound call arrives for an exported capability.
pub const CallHandler = *const fn (ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void;
/// Callback invoked when a return message arrives for a previously sent question.
pub const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
/// Optional override for outbound frame delivery (used in testing).
pub const SendFrameOverride = *const fn (ctx: *anyopaque, frame: []const u8) anyerror!void;
/// Transport callback: start listening for inbound frames.
pub const TransportStartFn = *const fn (ctx: *anyopaque, peer: *Peer) void;
/// Transport callback: send a framed message to the remote peer.
pub const TransportSendFn = *const fn (ctx: *anyopaque, frame: []const u8) anyerror!void;
/// Transport callback: close the underlying connection.
pub const TransportCloseFn = *const fn (ctx: *anyopaque) void;
/// Transport callback: check if the connection is in the process of closing.
pub const TransportIsClosingFn = *const fn (ctx: *anyopaque) bool;

/// An exported capability: a context pointer and its call handler.
pub const Export = struct {
    ctx: *anyopaque,
    on_call: CallHandler,
};

const ExportEntry = struct {
    handler: ?Export = null,
    ref_count: u32,
    is_promise: bool = false,
    resolved: ?cap_table.ResolvedCap = null,
};

const ResolvedAnswer = struct {
    frame: []u8,
};

const PendingCall = struct {
    frame: []u8,
    caps: cap_table.InboundCapTable,
};

const ProvideTarget = peer_state_types.ProvideTarget;
const ProvideEntry = peer_state_types.ProvideEntry;
const JoinKeyPart = peer_state_types.JoinKeyPart;
const JoinPartEntry = peer_state_types.JoinPartEntry;
const JoinState = peer_state_types.JoinState;
const PendingJoinQuestion = peer_state_types.PendingJoinQuestion;
const PendingEmbargoedAccept = peer_state_types.PendingEmbargoedAccept;

const ForwardReturnMode = peer_forward_orchestration.ForwardReturnMode;

const ResolvedImport = struct {
    cap: ?cap_table.ResolvedCap,
    embargo_id: ?u32 = null,
    embargoed: bool = false,
};

const QuestionDeinitCtxFn = *const fn (std.mem.Allocator, *anyopaque) void;

const Question = struct {
    ctx: *anyopaque,
    on_return: QuestionCallback,
    deinit_ctx: ?QuestionDeinitCtxFn = null,
    is_loopback: bool = false,
    suppress_auto_finish: bool = false,
};

const PendingThirdPartyAwait = struct {
    question_id: u32,
    question: Question,
};

const ForwardCallContext = struct {
    peer: *Peer,
    payload: protocol.Payload,
    inbound_caps: *const cap_table.InboundCapTable,
    send_results_to: protocol.SendResultsToTag,
    send_results_to_third_party_payload: ?[]u8 = null,
    answer_id: u32,
    mode: ForwardReturnMode,

    fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
        const ctx: *ForwardCallContext = @ptrCast(@alignCast(ctx_ptr));
        if (ctx.send_results_to_third_party_payload) |payload| allocator.free(payload);
        allocator.destroy(ctx);
    }
};

const ForwardReturnBuildContext = struct {
    peer: *Peer,
    payload: protocol.Payload,
    inbound_caps: *const cap_table.InboundCapTable,
};

fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
    return @ptrCast(@alignCast(ctx));
}

/// A Cap'n Proto RPC peer that manages one side of a two-party connection.
///
/// `Peer` tracks exported capabilities, outstanding questions, promise
/// resolution, and three-party handoff state. It is **not** thread-safe;
/// all calls must be made from the thread that owns the associated event
/// loop (or under external synchronization). In debug builds, key entry
/// points assert thread affinity to catch violations early. Use
/// `initDetached` for environments without a real transport (WASM, unit
/// tests).
///
/// ## Peer lifecycle (state machine)
///
/// ```text
///   +------------+     attachConnection / attachTransport     +----------+
///   |  Detached  | ----------------------------------------> | Attached |
///   | (no I/O)   | <---------------------------------------- | (idle)   |
///   +------------+     detachConnection / detachTransport     +----------+
///                                                                  |
///                                                           start()
///                                                                  v
///                                                             +----------+
///                                                             |  Active  |
///                                                             | (r/w)    |
///                                                             +----------+
///                                                                  |
///                                             transport close / error / deinit
///                                                                  v
///                                                             +----------+
///                                                             |  Closed  |
///                                                             +----------+
/// ```
///
/// * **Detached** -- Created via `initDetached`. No transport is wired up;
///   frames can still be injected manually via `handleFrame` and sent
///   through a `setSendFrameOverride` callback.
/// * **Attached** -- A transport (typically a `Connection`) has been bound
///   via `attachConnection`/`attachTransport`, but `start()` has not been
///   called yet. No I/O occurs.
/// * **Active** -- After `start()`, the transport begins reading inbound
///   frames and the peer processes them. Outbound calls, returns, and
///   control messages flow through `sendFrame`.
/// * **Closed** -- The transport signaled a close (EOF, error, or explicit
///   `close`). The `on_close` callback fires. The peer should be
///   `deinit`-ed after this.
///
/// ## State maps
///
/// The peer maintains many hash maps to track the RPC protocol state.
/// Here is a summary of each:
///
/// | Map | Key | Value | Purpose |
/// |-----|-----|-------|---------|
/// | `exports` | export ID | `ExportEntry` | Local capabilities offered to the remote peer. Ref-counted; removed on Release. |
/// | `questions` | question ID | `Question` | Outstanding outbound calls awaiting a Return. Removed when the Return arrives. |
/// | `resolved_answers` | question ID | `ResolvedAnswer` | Cached Return frames for answered questions (used to resolve PromisedAnswer references). Removed on Finish. |
/// | `pending_promises` | question ID | `ArrayList(PendingCall)` | Calls targeting a PromisedAnswer whose Return has not yet arrived. Replayed once the answer resolves. |
/// | `pending_export_promises` | export ID | `ArrayList(PendingCall)` | Calls targeting a promise export not yet resolved. Replayed on `resolvePromiseExportToExport`. |
/// | `forwarded_questions` | original answer ID | forwarded question ID | Maps an inbound call's answer ID to the question ID of the forwarded outbound call. |
/// | `forwarded_tail_questions` | original answer ID | forwarded question ID | Like `forwarded_questions` but for tail-call forwarding (takeFromOtherQuestion). |
/// | `provides_by_question` | question ID | `ProvideEntry` | Active Provide operations indexed by their answer question ID. |
/// | `provides_by_key` | recipient key (bytes) | question ID | Active Provide operations indexed by their recipient key for Accept lookup. |
/// | `pending_joins` | join ID | `JoinState` | In-progress Join operations collecting parts from multiple peers. |
/// | `pending_join_questions` | question ID | `PendingJoinQuestion` | Maps a Join answer's question ID back to its join ID and part number. |
/// | `pending_accepts_by_embargo` | embargo key (bytes) | `ArrayList(PendingEmbargoedAccept)` | Accept messages waiting for a disembargo before delivery. |
/// | `pending_accept_embargo_by_question` | question ID | embargo key (bytes) | Maps a question to its embargo key for cleanup on Finish. |
/// | `pending_third_party_awaits` | recipient key (bytes) | `PendingThirdPartyAwait` | Outbound third-party handoffs awaiting a ThirdPartyAnswer. |
/// | `pending_third_party_answers` | recipient key (bytes) | answer ID | Completed third-party answers awaiting adoption. |
/// | `pending_third_party_returns` | question ID | frame (bytes) | Return frames for third-party questions received before adoption. |
/// | `adopted_third_party_answers` | original question ID | adopted answer ID | Maps third-party questions to their locally adopted answer IDs. |
/// | `resolved_imports` | promise ID | `ResolvedImport` | Resolved promise imports (after a Resolve message). Tracks embargo state. |
/// | `pending_embargoes` | embargo ID | promise ID | In-flight disembargo operations, keyed by the embargo ID we allocated. |
/// | `loopback_questions` | question ID | void | Questions whose Return should be delivered locally (loopback / exported-cap calls). |
/// | `send_results_to_yourself` | answer ID | void | Inbound calls with `sendResultsTo = yourself`, meaning we send `resultsSentElsewhere`. |
/// | `send_results_to_third_party` | answer ID | optional payload | Inbound calls with `sendResultsTo = thirdParty`. Payload is the serialized recipient. |
///
/// ## Invariants
///
/// * Question IDs are monotonically increasing (mod 2^32) and never reused
///   within a single peer lifetime.
/// * Each export ID has at most one entry in `exports`. The entry's
///   `ref_count` tracks how many times the remote peer has received the
///   capability in a message payload.
/// * A question is removed from `questions` when its Return is fully
///   handled, but its `resolved_answers` entry persists until Finish so
///   that PromisedAnswer references can be resolved.
/// * `pending_promises` entries are drained (replayed or errored) when the
///   corresponding answer resolves, never left dangling.
pub const Peer = struct {
    allocator: std.mem.Allocator,

    // -- Transport binding --------------------------------------------------

    /// Opaque pointer to the attached transport/connection. Must remain
    /// valid from `attachTransport` until `detachTransport` or `deinit`.
    transport_ctx: ?*anyopaque = null,
    transport_start: ?TransportStartFn = null,
    transport_send: ?TransportSendFn = null,
    transport_close: ?TransportCloseFn = null,
    transport_is_closing: ?TransportIsClosingFn = null,

    // -- Capability bookkeeping ---------------------------------------------

    /// Central capability table shared with cap_table helpers.
    caps: cap_table.CapTable,
    /// Exported (local) capabilities offered to the remote peer.
    exports: std.AutoHashMap(u32, ExportEntry),

    // -- Question / answer tracking -----------------------------------------

    /// Outstanding outbound calls (question ID -> callback).
    questions: std.AutoHashMap(u32, Question),
    /// Cached Return frames for answered questions, kept until Finish.
    resolved_answers: std.AutoHashMap(u32, ResolvedAnswer),

    // -- Promise queueing ---------------------------------------------------

    /// Calls blocked on an unresolved PromisedAnswer (question ID -> queue).
    pending_promises: std.AutoHashMap(u32, std.ArrayList(PendingCall)),
    /// Calls blocked on an unresolved promise export (export ID -> queue).
    pending_export_promises: std.AutoHashMap(u32, std.ArrayList(PendingCall)),

    // -- Forwarding ---------------------------------------------------------

    /// Maps an inbound answer ID to the outbound question ID it was forwarded to.
    forwarded_questions: std.AutoHashMap(u32, u32),
    /// Same as `forwarded_questions` but for tail-call (takeFromOtherQuestion) forwarding.
    forwarded_tail_questions: std.AutoHashMap(u32, u32),

    // -- Three-party handoff (provide / accept / join) ----------------------

    /// Active Provide operations keyed by answer question ID.
    provides_by_question: std.AutoHashMap(u32, ProvideEntry),
    /// Active Provide operations keyed by serialized recipient for Accept lookup.
    provides_by_key: std.StringHashMap(u32),
    /// In-progress Join operations collecting parts.
    pending_joins: std.AutoHashMap(u32, JoinState),
    /// Maps a Join answer's question ID to its join ID + part number.
    pending_join_questions: std.AutoHashMap(u32, PendingJoinQuestion),
    /// Accept messages waiting for a disembargo.
    pending_accepts_by_embargo: std.StringHashMap(std.ArrayList(PendingEmbargoedAccept)),
    /// Maps question IDs to embargo keys for cleanup on Finish.
    pending_accept_embargo_by_question: std.AutoHashMap(u32, []u8),
    /// Outbound third-party handoffs awaiting ThirdPartyAnswer.
    pending_third_party_awaits: std.StringHashMap(PendingThirdPartyAwait),
    /// Completed third-party answers awaiting adoption.
    pending_third_party_answers: std.StringHashMap(u32),
    /// Return frames for third-party questions received before adoption.
    pending_third_party_returns: std.AutoHashMap(u32, []u8),
    /// Maps third-party questions to their adopted answer IDs.
    adopted_third_party_answers: std.AutoHashMap(u32, u32),

    // -- Resolve / embargo --------------------------------------------------

    /// Resolved promise imports (after Resolve). Tracks embargo state.
    resolved_imports: std.AutoHashMap(u32, ResolvedImport),
    /// In-flight disembargo operations (embargo ID -> promise ID).
    pending_embargoes: std.AutoHashMap(u32, u32),

    // -- Loopback / sendResultsTo routing -----------------------------------

    /// Questions whose Return should be delivered locally (calls to our own exports).
    loopback_questions: std.AutoHashMap(u32, void),
    /// Inbound calls with sendResultsTo=yourself.
    send_results_to_yourself: std.AutoHashMap(u32, void),
    /// Inbound calls with sendResultsTo=thirdParty.
    send_results_to_third_party: std.AutoHashMap(u32, ?[]u8),

    // -- Counters and scalars -----------------------------------------------

    /// Monotonically increasing question ID counter.
    next_question_id: u32 = 0,
    /// Monotonically increasing embargo ID counter.
    next_embargo_id: u32 = 0,
    /// Export ID of the bootstrap capability, if set.
    bootstrap_export_id: ?u32 = null,

    // -- Send frame override (for testing) ----------------------------------

    send_frame_ctx: ?*anyopaque = null,
    send_frame_override: ?SendFrameOverride = null,

    // -- Lifecycle callbacks -------------------------------------------------

    on_error: ?*const fn (peer: *Peer, err: anyerror) void = null,
    on_close: ?*const fn (peer: *Peer) void = null,

    // -- Diagnostics --------------------------------------------------------

    /// Tag of the most recently decoded inbound message (for debugging).
    last_inbound_tag: ?protocol.MessageTag = null,
    /// Reason string from the most recent remote Abort message, if any.
    last_remote_abort_reason: ?[]u8 = null,

    // -- Thread-affinity check (debug only) ---------------------------------

    /// Thread ID captured at init time. In debug builds, key entry points
    /// assert that the current thread matches this value. Initialized to
    /// null and set to the real thread ID in `initDetached`.
    owner_thread_id: ?std.Thread.Id = null,

    /// Assert that the caller is on the thread that created this peer.
    /// This is a no-op in release builds. In debug builds, it panics
    /// with a clear message if the current thread is not the owner.
    fn assertThreadAffinity(self: *const Peer) void {
        if (comptime builtin.target.os.tag == .freestanding) return;
        if (builtin.mode == .Debug) {
            const owner = self.owner_thread_id orelse return;
            const current = std.Thread.getCurrentId();
            if (current != owner) {
                @panic("Peer method called from wrong thread: Peer is not thread-safe, all calls must be on the owner thread");
            }
        }
    }

    /// Create a peer and immediately attach it to a connection/transport.
    pub fn init(allocator: std.mem.Allocator, conn: anytype) Peer {
        var peer = initDetached(allocator);
        peer.attachConnection(conn);
        return peer;
    }

    /// Create a peer without an attached transport.
    ///
    /// Useful for WASM, unit tests, or manual frame injection via
    /// `handleFrame` and `setSendFrameOverride`.
    pub fn initDetached(allocator: std.mem.Allocator) Peer {
        return .{
            .allocator = allocator,
            .owner_thread_id = if (comptime builtin.target.os.tag == .freestanding) null else std.Thread.getCurrentId(),
            .caps = cap_table.CapTable.init(allocator),
            .exports = std.AutoHashMap(u32, ExportEntry).init(allocator),
            .questions = std.AutoHashMap(u32, Question).init(allocator),
            .resolved_answers = std.AutoHashMap(u32, ResolvedAnswer).init(allocator),
            .pending_promises = std.AutoHashMap(u32, std.ArrayList(PendingCall)).init(allocator),
            .pending_export_promises = std.AutoHashMap(u32, std.ArrayList(PendingCall)).init(allocator),
            .forwarded_questions = std.AutoHashMap(u32, u32).init(allocator),
            .forwarded_tail_questions = std.AutoHashMap(u32, u32).init(allocator),
            .provides_by_question = std.AutoHashMap(u32, ProvideEntry).init(allocator),
            .provides_by_key = std.StringHashMap(u32).init(allocator),
            .pending_joins = std.AutoHashMap(u32, JoinState).init(allocator),
            .pending_join_questions = std.AutoHashMap(u32, PendingJoinQuestion).init(allocator),
            .pending_accepts_by_embargo = std.StringHashMap(std.ArrayList(PendingEmbargoedAccept)).init(allocator),
            .pending_accept_embargo_by_question = std.AutoHashMap(u32, []u8).init(allocator),
            .pending_third_party_awaits = std.StringHashMap(PendingThirdPartyAwait).init(allocator),
            .pending_third_party_answers = std.StringHashMap(u32).init(allocator),
            .pending_third_party_returns = std.AutoHashMap(u32, []u8).init(allocator),
            .adopted_third_party_answers = std.AutoHashMap(u32, u32).init(allocator),
            .resolved_imports = std.AutoHashMap(u32, ResolvedImport).init(allocator),
            .pending_embargoes = std.AutoHashMap(u32, u32).init(allocator),
            .loopback_questions = std.AutoHashMap(u32, void).init(allocator),
            .send_results_to_yourself = std.AutoHashMap(u32, void).init(allocator),
            .send_results_to_third_party = std.AutoHashMap(u32, ?[]u8).init(allocator),
        };
    }

    /// Bind a typed connection to this peer, wiring up transport callbacks.
    pub fn attachConnection(self: *Peer, conn: anytype) void {
        self.assertThreadAffinity();
        const ConnPtr = @TypeOf(conn);
        comptime {
            const info = @typeInfo(ConnPtr);
            if (info != .pointer) @compileError("attachConnection expects a pointer type");
        }

        self.attachTransport(
            conn,
            struct {
                fn call(ctx: *anyopaque, peer: *Peer) void {
                    const typed: ConnPtr = castCtx(ConnPtr, ctx);
                    typed.start(
                        peer,
                        peer_transport_callbacks.onConnectionMessageFor(Peer, ConnPtr, Peer.handleFrame),
                        peer_transport_callbacks.onConnectionErrorFor(Peer, ConnPtr, onConnectionError),
                        peer_transport_callbacks.onConnectionCloseFor(Peer, ConnPtr, onConnectionClose),
                    );
                }
            }.call,
            struct {
                fn call(ctx: *anyopaque, frame: []const u8) anyerror!void {
                    const typed: ConnPtr = castCtx(ConnPtr, ctx);
                    try typed.sendFrame(frame);
                }
            }.call,
            struct {
                fn call(ctx: *anyopaque) void {
                    const typed: ConnPtr = castCtx(ConnPtr, ctx);
                    typed.close();
                }
            }.call,
            struct {
                fn call(ctx: *anyopaque) bool {
                    const typed: ConnPtr = castCtx(ConnPtr, ctx);
                    return typed.isClosing();
                }
            }.call,
        );
    }

    pub fn detachConnection(self: *Peer) void {
        self.assertThreadAffinity();
        self.detachTransport();
    }

    pub fn attachTransport(
        self: *Peer,
        ctx: *anyopaque,
        start_fn: ?TransportStartFn,
        send_fn: ?TransportSendFn,
        close_fn: ?TransportCloseFn,
        is_closing: ?TransportIsClosingFn,
    ) void {
        self.assertThreadAffinity();
        peer_transport_state.attachTransportForPeer(
            Peer,
            TransportStartFn,
            TransportSendFn,
            TransportCloseFn,
            TransportIsClosingFn,
            self,
            ctx,
            start_fn,
            send_fn,
            close_fn,
            is_closing,
        );
    }

    pub fn detachTransport(self: *Peer) void {
        self.assertThreadAffinity();
        peer_transport_state.detachTransportForPeer(Peer, self);
    }

    pub fn hasAttachedTransport(self: *const Peer) bool {
        self.assertThreadAffinity();
        return peer_transport_state.hasAttachedTransportForPeer(Peer, self);
    }

    pub fn closeAttachedTransport(self: *Peer) void {
        self.assertThreadAffinity();
        peer_transport_state.closeAttachedTransportForPeer(Peer, self);
    }

    pub fn isAttachedTransportClosing(self: *const Peer) bool {
        self.assertThreadAffinity();
        return peer_transport_state.isAttachedTransportClosingForPeer(Peer, self);
    }

    pub fn takeAttachedConnection(self: *Peer, comptime ConnPtr: type) ?ConnPtr {
        self.assertThreadAffinity();
        return peer_transport_state.takeAttachedConnectionForPeer(
            Peer,
            ConnPtr,
            self,
            detachTransport,
        );
    }

    pub fn getAttachedConnection(self: *const Peer, comptime ConnPtr: type) ?ConnPtr {
        self.assertThreadAffinity();
        return peer_transport_state.getAttachedConnectionForPeer(
            Peer,
            ConnPtr,
            self,
        );
    }

    /// Release all owned state: pending calls, resolved answers, export
    /// entries, and the capability table.
    pub fn deinit(self: *Peer) void {
        self.assertThreadAffinity();
        peer_cleanup.deinitPendingCallMapOwned(
            @TypeOf(self.pending_promises),
            self.allocator,
            &self.pending_promises,
        );
        peer_cleanup.deinitPendingCallMapOwned(
            @TypeOf(self.pending_export_promises),
            self.allocator,
            &self.pending_export_promises,
        );
        peer_cleanup.deinitResolvedAnswerMap(
            @TypeOf(self.resolved_answers),
            self.allocator,
            &self.resolved_answers,
        );
        {
            var q_it = self.questions.valueIterator();
            while (q_it.next()) |q| {
                if (q.deinit_ctx) |deinit_ctx| deinit_ctx(self.allocator, q.ctx);
            }
        }
        self.questions.deinit();
        self.exports.deinit();
        self.forwarded_questions.deinit();
        self.forwarded_tail_questions.deinit();
        peer_cleanup.deinitProvideEntryMap(
            @TypeOf(self.provides_by_question),
            self.allocator,
            &self.provides_by_question,
        );
        self.provides_by_key.deinit();

        peer_cleanup.deinitJoinStateMap(
            @TypeOf(self.pending_joins),
            self.allocator,
            &self.pending_joins,
        );
        self.pending_join_questions.deinit();

        peer_cleanup.deinitOwnedStringKeyListMap(
            @TypeOf(self.pending_accepts_by_embargo),
            self.allocator,
            &self.pending_accepts_by_embargo,
        );
        peer_cleanup.deinitOwnedBytesMap(
            @TypeOf(self.pending_accept_embargo_by_question),
            self.allocator,
            &self.pending_accept_embargo_by_question,
        );
        peer_cleanup.deinitOwnedStringKeyMap(
            @TypeOf(self.pending_third_party_awaits),
            self.allocator,
            &self.pending_third_party_awaits,
        );
        peer_cleanup.deinitOwnedStringKeyMap(
            @TypeOf(self.pending_third_party_answers),
            self.allocator,
            &self.pending_third_party_answers,
        );
        peer_cleanup.deinitOwnedBytesMap(
            @TypeOf(self.pending_third_party_returns),
            self.allocator,
            &self.pending_third_party_returns,
        );
        self.adopted_third_party_answers.deinit();

        self.resolved_imports.deinit();
        self.pending_embargoes.deinit();
        self.loopback_questions.deinit();
        self.send_results_to_yourself.deinit();
        peer_cleanup.deinitOptionalOwnedBytesMap(
            @TypeOf(self.send_results_to_third_party),
            self.allocator,
            &self.send_results_to_third_party,
        );
        peer_cleanup.clearOptionalOwnedBytes(self.allocator, &self.last_remote_abort_reason);
        self.caps.deinit();
    }

    /// Start the peer, registering error/close callbacks and initiating the
    /// transport (if attached).
    pub fn start(
        self: *Peer,
        on_error: ?*const fn (peer: *Peer, err: anyerror) void,
        on_close: ?*const fn (peer: *Peer) void,
    ) void {
        self.assertThreadAffinity();
        self.on_error = on_error;
        self.on_close = on_close;
        if (self.transport_start) |start_fn| {
            const ctx = self.transport_ctx orelse return;
            start_fn(ctx, self);
        }
    }

    pub fn setSendFrameOverride(self: *Peer, ctx: ?*anyopaque, callback: ?SendFrameOverride) void {
        self.assertThreadAffinity();
        self.send_frame_ctx = ctx;
        self.send_frame_override = callback;
    }

    pub fn getLastInboundTag(self: *const Peer) ?protocol.MessageTag {
        self.assertThreadAffinity();
        return self.last_inbound_tag;
    }

    pub fn getLastRemoteAbortReason(self: *const Peer) ?[]const u8 {
        self.assertThreadAffinity();
        return self.last_remote_abort_reason;
    }

    /// Register a local capability for export and return its export ID.
    pub fn addExport(self: *Peer, exported: Export) !u32 {
        self.assertThreadAffinity();
        const id = try self.caps.allocExportId();
        try self.exports.put(id, .{
            .handler = exported,
            .ref_count = 0,
            .is_promise = false,
            .resolved = null,
        });
        log.debug("added export id={}", .{id});
        return id;
    }

    /// Export a promise capability that will be resolved later via
    /// `resolvePromiseExportToExport` or `resolvePromiseExportToException`.
    pub fn addPromiseExport(self: *Peer) !u32 {
        self.assertThreadAffinity();
        const id = try self.caps.allocExportId();
        try self.exports.put(id, .{
            .handler = null,
            .ref_count = 0,
            .is_promise = true,
            .resolved = null,
        });
        errdefer _ = self.exports.remove(id);
        try self.caps.markExportPromise(id);
        return id;
    }

    /// Register a capability as this peer's bootstrap interface.
    ///
    /// Returns the export ID. The remote peer can obtain this capability
    /// by sending a Bootstrap message.
    pub fn setBootstrap(self: *Peer, exported: Export) !u32 {
        self.assertThreadAffinity();
        const id = try self.addExport(exported);
        self.bootstrap_export_id = id;
        log.debug("bootstrap set export_id={}", .{id});
        return id;
    }

    /// Send a Bootstrap request to obtain the remote peer's bootstrap capability.
    ///
    /// Returns the question ID. When the remote peer responds, `on_return`
    /// is invoked with the bootstrap capability in the return payload.
    pub fn sendBootstrap(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        self.assertThreadAffinity();
        const question_id = try self.allocateQuestion(ctx, on_return);

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        try builder.buildBootstrap(question_id);
        try self.sendBuilder(&builder);
        log.debug("sent bootstrap question_id={}", .{question_id});
        return question_id;
    }

    /// Resolve a previously exported promise to point at a concrete export.
    pub fn resolvePromiseExportToExport(self: *Peer, promise_id: u32, export_id: u32) !void {
        self.assertThreadAffinity();
        var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
        if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
        if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;

        const descriptor = protocol.CapDescriptor{
            .tag = if (self.caps.isExportPromise(export_id)) .sender_promise else .sender_hosted,
            .id = export_id,
            .promised_answer = null,
            .attached_fd = null,
        };
        try peer_outbound_control.sendResolveCapViaSendFrame(
            Peer,
            self,
            promise_id,
            descriptor,
            Peer.sendFrame,
        );

        promise_entry.value_ptr.resolved = .{ .exported = .{ .id = export_id } };
        self.caps.clearExportPromise(promise_id);
        try self.replayResolvedPromiseExport(promise_id, promise_entry.value_ptr.resolved.?);
    }

    /// Resolve a previously exported promise to an exception.
    pub fn resolvePromiseExportToException(self: *Peer, promise_id: u32, reason: []const u8) !void {
        self.assertThreadAffinity();
        var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
        if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
        if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;

        try peer_outbound_control.sendResolveExceptionViaSendFrame(
            Peer,
            self,
            promise_id,
            reason,
            Peer.sendFrame,
        );
        promise_entry.value_ptr.resolved = .none;
        self.caps.clearExportPromise(promise_id);
        try self.replayResolvedPromiseExport(promise_id, .none);
    }

    /// Send an RPC call to an imported capability, returning the question ID.
    pub fn sendCall(
        self: *Peer,
        target_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        self.assertThreadAffinity();
        log.debug("sendCall target_id={} interface_id=0x{x} method_id={}", .{ target_id, interface_id, method_id });
        if (self.resolved_imports.get(target_id)) |entry| {
            if (!entry.embargoed and entry.cap != null) {
                return self.sendCallResolved(entry.cap.?, interface_id, method_id, ctx, build, on_return);
            }
        }

        return peer_call_sender.sendCallToImport(
            Peer,
            CallBuildFn,
            QuestionCallback,
            self.allocator,
            &self.caps,
            self,
            onOutboundCap,
            self,
            target_id,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            Peer.allocateQuestion,
            Peer.removeQuestion,
            Peer.sendBuilder,
        );
    }

    pub fn sendCallResolved(
        self: *Peer,
        target: cap_table.ResolvedCap,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        self.assertThreadAffinity();
        return switch (target) {
            .imported => |cap| peer_call_sender.sendCallToImport(
                Peer,
                CallBuildFn,
                QuestionCallback,
                self.allocator,
                &self.caps,
                self,
                onOutboundCap,
                self,
                cap.id,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                Peer.allocateQuestion,
                Peer.removeQuestion,
                Peer.sendBuilder,
            ),
            .promised => |promised| self.sendCallPromised(promised, interface_id, method_id, ctx, build, on_return),
            .exported => |cap| peer_call_sender.sendCallToExport(
                Peer,
                Question,
                CallBuildFn,
                QuestionCallback,
                self.allocator,
                &self.caps,
                self,
                onOutboundCap,
                self,
                &self.questions,
                &self.loopback_questions,
                cap.id,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                Peer.allocateQuestion,
                Peer.handleFrame,
            ),
            .none => error.CapabilityUnavailable,
        };
    }

    pub fn sendCallPromised(
        self: *Peer,
        promised: protocol.PromisedAnswer,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        self.assertThreadAffinity();
        return peer_call_sender.sendCallPromised(
            Peer,
            CallBuildFn,
            QuestionCallback,
            self.allocator,
            &self.caps,
            self,
            onOutboundCap,
            self,
            promised,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            Peer.allocateQuestion,
            Peer.removeQuestion,
            Peer.sendBuilder,
        );
    }

    fn forwardResolvedCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        target: cap_table.ResolvedCap,
        mode: ForwardReturnMode,
    ) !void {
        const ctx = try self.allocator.create(ForwardCallContext);
        ctx.* = .{
            .peer = self,
            .payload = call.params,
            .inbound_caps = inbound_caps,
            .send_results_to = .caller,
            .send_results_to_third_party_payload = null,
            .answer_id = call.question_id,
            .mode = mode,
        };
        var ctx_owned = true;
        errdefer if (ctx_owned) ForwardCallContext.deinit(self.allocator, ctx);

        const forwarded_plan = try peer_forward_orchestration.buildForwardCallPlan(
            Peer,
            self,
            mode,
            call.send_results_to.third_party,
            peer_control.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
        );
        ctx.send_results_to = forwarded_plan.send_results_to;
        ctx.send_results_to_third_party_payload = forwarded_plan.send_results_to_third_party_payload;

        const forwarded_question_id = try self.sendCallResolved(
            target,
            call.interface_id,
            call.method_id,
            ctx,
            buildForwardedCall,
            onForwardedReturn,
        );
        // Once the question is registered, its deinit_ctx handles cleanup
        // on peer.deinit(); the local errdefer must no longer fire.
        if (self.questions.getPtr(forwarded_question_id)) |q| {
            q.deinit_ctx = ForwardCallContext.deinit;
        }
        ctx_owned = false;
        const completion = try peer_forward_orchestration.finishForwardResolvedCall(
            Question,
            mode,
            call.question_id,
            forwarded_question_id,
            &self.forwarded_questions,
            &self.forwarded_tail_questions,
            &self.questions,
        );
        if (completion.send_take_from_other_question) {
            try self.sendReturnTakeFromOtherQuestion(call.question_id, forwarded_question_id);
        }
    }

    fn buildForwardedCall(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
        const ctx: *const ForwardCallContext = castCtx(*const ForwardCallContext, ctx_ptr);
        try peer_control.applyForwardedCallSendResults(
            Peer,
            ctx.peer,
            call_builder,
            ctx.send_results_to,
            ctx.send_results_to_third_party_payload,
            peer_control.setForwardedCallThirdPartyFromPayloadForPeerFn(Peer),
        );

        const payload_builder = try call_builder.payloadBuilder();
        try ctx.peer.clonePayloadWithRemappedCaps(
            call_builder.call.builder,
            payload_builder,
            ctx.payload,
            ctx.inbound_caps,
        );
    }

    fn onForwardedReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *ForwardCallContext = castCtx(*ForwardCallContext, ctx_ptr);
        defer {
            if (ctx.send_results_to_third_party_payload) |payload| peer.allocator.free(payload);
            peer.allocator.destroy(ctx);
        }
        peer_forward_orchestration.removeForwardedQuestionForPeer(Peer, peer, ret.answer_id);
        try peer_forward_return_callbacks.handleForwardedReturnWithPeerCallbacks(
            Peer,
            cap_table.InboundCapTable,
            ForwardReturnBuildContext,
            buildForwardedReturn,
            peer,
            peer_forward_orchestration.toControlMode(ctx.mode),
            ctx.answer_id,
            ret,
            inbound_caps,
            peer_control.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            peer_control.freeOwnedFrameForPeerFn(Peer),
            ctx.send_results_to_third_party_payload,
            sendReturnTag,
            peer_forward_orchestration.lookupForwardedQuestionForPeerFn(Peer),
            sendReturnTakeFromOtherQuestion,
            sendReturnAcceptFromThirdParty,
        );
    }

    fn buildForwardedReturn(ctx_ptr: *anyopaque, ret_builder: *protocol.ReturnBuilder) anyerror!void {
        const ctx: *const ForwardReturnBuildContext = castCtx(*const ForwardReturnBuildContext, ctx_ptr);
        const payload_builder = try ret_builder.payloadBuilder();
        try ctx.peer.clonePayloadWithRemappedCaps(
            ret_builder.ret.builder,
            payload_builder,
            ctx.payload,
            ctx.inbound_caps,
        );
    }

    fn clonePayloadWithRemappedCaps(
        self: *Peer,
        builder: *message.MessageBuilder,
        payload_builder: message.StructBuilder,
        source: protocol.Payload,
        inbound_caps: *const cap_table.InboundCapTable,
    ) !void {
        try payload_remap.clonePayloadWithRemappedCaps(
            Peer,
            self.allocator,
            self,
            builder,
            payload_builder,
            source,
            inbound_caps,
            mapInboundCapForForward,
        );
    }

    fn mapInboundCapForForward(
        self: *Peer,
        inbound_caps: *const cap_table.InboundCapTable,
        cap_index: u32,
    ) !?u32 {
        const entry = try inbound_caps.get(cap_index);
        return switch (entry) {
            .none => null,
            .imported => |cap| cap.id,
            .exported => |cap| cap.id,
            .promised => |promised| try self.caps.noteReceiverAnswer(promised),
        };
    }

    /// Send a return with results for a previously received call.
    pub fn sendReturnResults(self: *Peer, answer_id: u32, ctx: *anyopaque, build: ReturnBuildFn) !void {
        self.assertThreadAffinity();
        // sendResultsTo routing is resolved in precedence order:
        // third-party handoff > local results-sent-elsewhere marker > normal results payload.
        if (self.send_results_to_third_party.fetchRemove(answer_id)) |entry| {
            _ = self.send_results_to_yourself.remove(answer_id);
            defer if (entry.value) |payload| self.allocator.free(payload);
            try self.sendReturnAcceptFromThirdParty(answer_id, entry.value);
            return;
        }

        if (self.send_results_to_yourself.remove(answer_id)) {
            try self.sendReturnTag(answer_id, .results_sent_elsewhere);
            return;
        }

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        var ret = try builder.beginReturn(answer_id, .results);
        try build(ctx, &ret);
        _ = try cap_table.encodeReturnPayloadCaps(&self.caps, &ret, self, onOutboundCap);

        const bytes = try builder.finish();
        defer self.allocator.free(bytes);

        try self.sendReturnFrameWithLoopback(answer_id, bytes);

        const copy = try self.allocator.alloc(u8, bytes.len);
        std.mem.copyForwards(u8, copy, bytes);
        try self.recordResolvedAnswer(answer_id, copy);
    }

    pub fn sendPrebuiltReturnFrame(self: *Peer, ret: protocol.Return, frame: []const u8) !void {
        self.assertThreadAffinity();
        try self.noteOutboundReturnCapRefs(ret);
        self.clearSendResultsRouting(ret.answer_id);
        try self.sendReturnFrameWithLoopback(ret.answer_id, frame);

        if (ret.tag == .results) {
            const copy = try self.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try self.recordResolvedAnswer(ret.answer_id, copy);
        }
    }

    /// Send a return with an exception for a previously received call.
    pub fn sendReturnException(self: *Peer, answer_id: u32, reason: []const u8) !void {
        self.assertThreadAffinity();
        try peer_return_dispatch.sendReturnExceptionForPeer(
            Peer,
            self,
            answer_id,
            reason,
            clearSendResultsRouting,
            sendReturnFrameWithLoopback,
        );
    }

    fn sendReturnTag(self: *Peer, answer_id: u32, tag: protocol.ReturnTag) !void {
        try peer_return_dispatch.sendReturnTagForPeer(
            Peer,
            self,
            answer_id,
            tag,
            clearSendResultsRouting,
            sendReturnFrameWithLoopback,
        );
    }

    fn sendReturnTakeFromOtherQuestion(self: *Peer, answer_id: u32, other_question_id: u32) !void {
        try peer_return_dispatch.sendReturnTakeFromOtherQuestionForPeer(
            Peer,
            self,
            answer_id,
            other_question_id,
            clearSendResultsRouting,
            sendReturnFrameWithLoopback,
        );
    }

    fn sendReturnAcceptFromThirdParty(self: *Peer, answer_id: u32, await_payload: ?[]const u8) !void {
        try peer_return_dispatch.sendReturnAcceptFromThirdPartyForPeer(
            Peer,
            self,
            answer_id,
            await_payload,
            clearSendResultsRouting,
            sendReturnFrameWithLoopback,
        );
    }

    fn clearSendResultsRouting(self: *Peer, answer_id: u32) void {
        peer_return_send_helpers.clearSendResultsRoutingForPeer(
            Peer,
            self,
            answer_id,
            clearSendResultsToThirdParty,
        );
    }

    fn sendReturnFrameWithLoopback(self: *Peer, answer_id: u32, bytes: []const u8) !void {
        try peer_return_send_helpers.sendReturnFrameWithLoopbackForPeer(
            Peer,
            self,
            answer_id,
            bytes,
            deliverLoopbackReturn,
            sendFrame,
        );
    }

    fn sendReturnProvidedTarget(self: *Peer, answer_id: u32, target: *const ProvideTarget) !void {
        const BuildCtx = struct {
            peer: *Peer,
            target: *const ProvideTarget,

            fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const ctx: *const @This() = castCtx(*const @This(), ctx_ptr);
                var any = try ret.getResultsAnyPointer();

                const cap_id = switch (ctx.target.*) {
                    .cap_id => |id| id,
                    .promised => |promised| try ctx.peer.caps.noteReceiverAnswerOps(
                        promised.question_id,
                        promised.ops,
                    ),
                };
                try any.setCapability(.{ .id = cap_id });
            }
        };

        var ctx = BuildCtx{
            .peer = self,
            .target = target,
        };
        try self.sendReturnResults(answer_id, &ctx, BuildCtx.build);
    }

    fn noteOutboundReturnCapRefs(self: *Peer, ret: protocol.Return) !void {
        try peer_return_send_helpers.noteOutboundReturnCapRefsForPeer(
            Peer,
            self,
            ret,
            noteExportRef,
        );
    }

    fn clearSendResultsToThirdParty(self: *Peer, answer_id: u32) void {
        peer_return_send_helpers.clearSendResultsToThirdPartyForPeer(
            Peer,
            self,
            answer_id,
        );
    }

    fn provideTargetsEqual(a: *const ProvideTarget, b: *const ProvideTarget) bool {
        return switch (a.*) {
            .cap_id => |cap_id| switch (b.*) {
                .cap_id => |other_cap_id| cap_id == other_cap_id,
                else => false,
            },
            .promised => |promised| switch (b.*) {
                .promised => |other_promised| blk: {
                    if (promised.question_id != other_promised.question_id) break :blk false;
                    if (promised.ops.len != other_promised.ops.len) break :blk false;
                    for (promised.ops, 0..) |op, idx| {
                        const other = other_promised.ops[idx];
                        if (op.tag != other.tag or op.pointer_index != other.pointer_index) break :blk false;
                    }
                    break :blk true;
                },
                else => false,
            },
        };
    }

    fn captureAnyPointerPayload(
        allocator: std.mem.Allocator,
        ptr: ?message.AnyPointerReader,
    ) !?[]u8 {
        const any = ptr orelse return null;
        if (any.isNull()) return null;

        var builder = message.MessageBuilder.init(allocator);
        defer builder.deinit();

        const root = try builder.initRootAnyPointer();
        try message.cloneAnyPointer(any, root);

        const bytes = try builder.toBytes();
        return @constCast(bytes);
    }

    fn noteSendResultsToYourself(self: *Peer, answer_id: u32) !void {
        try peer_return_send_helpers.noteSendResultsToYourselfForPeer(
            Peer,
            self,
            answer_id,
            clearSendResultsToThirdParty,
        );
    }

    fn noteSendResultsToThirdParty(
        self: *Peer,
        answer_id: u32,
        ptr: ?message.AnyPointerReader,
    ) !void {
        try peer_return_send_helpers.noteSendResultsToThirdPartyForPeer(
            Peer,
            self,
            answer_id,
            ptr,
            captureAnyPointerPayload,
        );
    }

    /// Release references to an imported capability, sending a Release message
    /// to the remote peer when the reference count drops to zero.
    pub fn releaseImport(self: *Peer, import_id: u32, count: u32) anyerror!void {
        self.assertThreadAffinity();
        log.debug("releasing import id={} count={}", .{ import_id, count });
        try peer_cap_lifecycle.releaseImport(
            Peer,
            self,
            import_id,
            count,
            peer_cap_lifecycle.releaseImportRefForPeerFn(Peer),
            Peer.releaseResolvedImport,
            peer_outbound_control.sendReleaseViaSendFrameForPeerFn(Peer, Peer.sendFrame),
        );
    }

    pub fn sendReleaseForHost(self: *Peer, import_id: u32, count: u32) !void {
        self.assertThreadAffinity();
        try peer_outbound_control.sendReleaseViaSendFrame(
            Peer,
            self,
            import_id,
            count,
            Peer.sendFrame,
        );
    }

    pub fn sendFinishForHost(
        self: *Peer,
        question_id: u32,
        release_result_caps: bool,
        require_early_cancellation: bool,
    ) !void {
        self.assertThreadAffinity();
        try peer_outbound_control.sendFinishWithFlagsViaSendFrame(
            Peer,
            self,
            question_id,
            release_result_caps,
            require_early_cancellation,
            Peer.sendFrame,
        );
    }

    fn sendBuilder(self: *Peer, builder: *protocol.MessageBuilder) !void {
        const bytes = try builder.finish();
        defer self.allocator.free(bytes);
        try self.sendFrame(bytes);
    }

    fn sendFrame(self: *Peer, frame: []const u8) !void {
        if (self.send_frame_override) |cb| {
            const ctx = self.send_frame_ctx orelse {
                log.debug("send frame override missing callback context", .{});
                return error.MissingCallbackContext;
            };
            try cb(ctx, frame);
            return;
        }
        const send = self.transport_send orelse {
            log.debug("cannot send frame: transport not attached", .{});
            return error.TransportNotAttached;
        };
        const ctx = self.transport_ctx orelse {
            log.debug("cannot send frame: transport not attached", .{});
            return error.TransportNotAttached;
        };
        try send(ctx, frame);
    }

    fn onOutboundCap(ctx: *anyopaque, tag: protocol.CapDescriptorTag, id: u32) anyerror!void {
        const peer: *Peer = castCtx(*Peer, ctx);
        switch (tag) {
            .sender_hosted, .sender_promise => try peer.noteExportRef(id),
            else => {},
        }
    }

    fn noteExportRef(self: *Peer, id: u32) !void {
        try peer_cap_lifecycle.noteExportRef(
            ExportEntry,
            &self.exports,
            id,
        );
    }

    fn releaseExport(self: *Peer, id: u32, count: u32) void {
        peer_cap_lifecycle.releaseExport(
            Peer,
            ExportEntry,
            PendingCall,
            self,
            self.allocator,
            &self.exports,
            &self.pending_export_promises,
            self.bootstrap_export_id,
            id,
            count,
            peer_cap_lifecycle.clearExportPromiseForPeerFn(Peer),
            peer_promises.deinitPendingCallOwnedFrameForPeerFn(Peer, PendingCall),
        );
    }

    fn releaseInboundCaps(self: *Peer, inbound: *cap_table.InboundCapTable) !void {
        try peer_inbound_release.releaseInboundCaps(
            Peer,
            self.allocator,
            self,
            inbound,
            peer_cap_lifecycle.releaseImportRefForPeerFn(Peer),
            Peer.releaseResolvedImport,
            peer_outbound_control.sendReleaseViaSendFrameForPeerFn(Peer, Peer.sendFrame),
        );
    }

    fn storeResolvedImport(
        self: *Peer,
        promise_id: u32,
        cap: ?cap_table.ResolvedCap,
        embargo_id: ?u32,
        embargoed: bool,
    ) !void {
        try peer_cap_lifecycle.storeResolvedImport(
            Peer,
            ResolvedImport,
            cap_table.ResolvedCap,
            self,
            &self.resolved_imports,
            &self.pending_embargoes,
            promise_id,
            cap,
            embargo_id,
            embargoed,
            Peer.releaseResolvedCap,
        );
    }

    fn releaseResolvedImport(self: *Peer, promise_id: u32) anyerror!void {
        try peer_cap_lifecycle.releaseResolvedImport(
            Peer,
            ResolvedImport,
            cap_table.ResolvedCap,
            self,
            &self.resolved_imports,
            &self.pending_embargoes,
            promise_id,
            Peer.releaseResolvedCap,
        );
    }

    fn releaseResolvedCap(self: *Peer, resolved: cap_table.ResolvedCap) anyerror!void {
        switch (resolved) {
            .imported => |cap| try self.releaseImport(cap.id, 1),
            else => {},
        }
    }

    fn deliverLoopbackReturn(self: *Peer, frame: []const u8) !void {
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        if (decoded.tag != .return_) return error.UnexpectedMessage;
        try self.handleReturn(frame, try decoded.asReturn());
    }

    fn resolvePromisedAnswer(self: *Peer, promised: protocol.PromisedAnswer) !cap_table.ResolvedCap {
        const entry = self.resolved_answers.get(promised.question_id) orelse return error.PromiseUnresolved;
        var decoded = try protocol.DecodedMessage.init(self.allocator, entry.frame);
        defer decoded.deinit();
        const ret = try decoded.asReturn();
        if (ret.tag != .results or ret.results == null) return error.PromisedAnswerMissing;
        return cap_table.resolvePromisedAnswer(ret.results.?, promised.transform);
    }

    fn releaseResultCaps(self: *Peer, frame: []const u8) !void {
        try peer_cap_lifecycle.releaseResultCaps(
            Peer,
            self,
            self.allocator,
            frame,
            Peer.releaseExport,
        );
    }

    fn allocateQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        return peer_question_state.allocateQuestion(
            Question,
            &self.questions,
            &self.next_question_id,
            .{
                .ctx = ctx,
                .on_return = on_return,
                .is_loopback = false,
            },
        );
    }

    fn removeQuestion(self: *Peer, question_id: u32) void {
        _ = self.questions.remove(question_id);
    }

    fn onConnectionError(self: *Peer, err: anyerror) void {
        log.debug("connection error: {}", .{err});
        if (self.on_error) |cb| cb(self, err);
    }

    fn onConnectionClose(self: *Peer) void {
        log.debug("connection closed", .{});
        if (self.on_close) |cb| cb(self);
    }

    /// Process a single inbound Cap'n Proto RPC frame.
    ///
    /// Decodes the message tag and dispatches to the appropriate handler
    /// (call, return, finish, resolve, etc.). Unknown message types trigger
    /// an Unimplemented response per the Cap'n Proto RPC spec.
    pub fn handleFrame(self: *Peer, frame: []const u8) !void {
        self.assertThreadAffinity();
        var decoded = protocol.DecodedMessage.init(self.allocator, frame) catch |err| {
            if (err == error.InvalidMessageTag) {
                log.debug("unknown message tag in frame, sending unimplemented", .{});
                try self.sendUnimplementedForFrame(frame);
                return;
            }
            log.debug("failed to decode inbound frame: {}", .{err});
            self.sendAbortForError(err);
            return err;
        };
        defer decoded.deinit();
        self.last_inbound_tag = decoded.tag;
        log.debug("dispatching inbound {s}", .{@tagName(decoded.tag)});

        try peer_dispatch.dispatchDecodedForPeer(
            Peer,
            self,
            frame,
            &decoded,
            Peer.handleUnimplemented,
            Peer.handleAbort,
            Peer.handleBootstrap,
            Peer.handleCall,
            Peer.handleReturn,
            Peer.handleFinish,
            Peer.handleRelease,
            Peer.handleResolve,
            Peer.handleDisembargo,
            Peer.handleProvide,
            Peer.handleAccept,
            Peer.handleJoin,
            Peer.handleThirdPartyAnswer,
            Peer.sendUnimplemented,
        );
    }

    fn sendAbortForError(self: *Peer, err: anyerror) void {
        if (err == error.OutOfMemory) return;
        peer_outbound_control.sendAbort(
            Peer,
            self,
            @errorName(err),
            Peer.sendBuilder,
        ) catch |abort_err| {
            log.debug("failed to send abort: {}", .{abort_err});
        };
    }

    fn sendUnimplemented(self: *Peer, original: message.AnyPointerReader) !void {
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildUnimplementedFromAnyPointer(original);
        try self.sendBuilder(&builder);
    }

    fn sendUnimplementedForFrame(self: *Peer, frame: []const u8) !void {
        var msg = try message.Message.init(self.allocator, frame);
        defer msg.deinit();
        const root = try msg.getRootAnyPointer();
        try self.sendUnimplemented(root);
    }

    fn handleUnimplemented(self: *Peer, unimplemented: protocol.Unimplemented) !void {
        try peer_control.handleUnimplemented(
            Peer,
            self,
            unimplemented,
            peer_control.handleUnimplementedQuestionForPeerFn(
                Peer,
                Peer.handleReturn,
            ),
        );
    }

    fn handleAbort(self: *Peer, abort: protocol.Abort) !void {
        log.debug("received abort from remote: {s}", .{abort.exception.reason});
        try peer_control.handleAbort(self.allocator, &self.last_remote_abort_reason, abort);
    }

    fn handleBootstrap(self: *Peer, bootstrap: protocol.Bootstrap) !void {
        try peer_control.handleBootstrap(
            Peer,
            self,
            self.allocator,
            bootstrap,
            self.bootstrap_export_id,
            noteExportRef,
            sendReturnException,
            sendFrame,
            recordResolvedAnswer,
        );
    }

    fn handleFinish(self: *Peer, finish: protocol.Finish) !void {
        const ops = peer_control.FinishOps(Peer){
            .remove_send_results_to_yourself = peer_forward_orchestration.removeSendResultsToYourselfForPeerFn(Peer),
            .clear_send_results_to_third_party = clearSendResultsToThirdParty,
            .clear_provide = peer_provides_state.clearProvideForPeerFn(
                Peer,
                ProvideEntry,
                ProvideTarget,
                ProvideTarget.deinit,
            ),
            .clear_pending_join_question = peer_join_state.clearPendingJoinQuestionForPeerFn(
                Peer,
                JoinState,
                PendingJoinQuestion,
                ProvideTarget,
                ProvideTarget.deinit,
                JoinState.deinit,
            ),
            .clear_pending_accept_question = peer_embargo_accepts.clearPendingAcceptQuestionForPeerFn(
                Peer,
                PendingEmbargoedAccept,
            ),
            .take_forwarded_tail_question = peer_forward_orchestration.takeForwardedTailQuestionForPeerFn(Peer),
            .send_finish = peer_outbound_control.sendFinishViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            .take_resolved_answer_frame = peer_control.takeResolvedAnswerFrameForPeerFn(Peer),
            .release_caps_for_frame = releaseResultCaps,
            .free_frame = peer_control.freeOwnedFrameForPeerFn(Peer),
        };
        try peer_control.handleFinishWithOps(
            Peer,
            self,
            finish.question_id,
            finish.release_result_caps,
            ops,
        );
    }

    fn handleRelease(self: *Peer, release: protocol.Release) !void {
        peer_control.handleRelease(Peer, self, release, releaseExport);
    }

    fn handleResolve(self: *Peer, resolve: protocol.Resolve) !void {
        const ops = peer_control.ResolveOps(Peer){
            .has_known_promise = peer_control.hasKnownResolvePromiseForPeerFn(Peer),
            .resolve_cap_descriptor = peer_control.resolveCapDescriptorForPeerFn(Peer),
            .release_resolved_cap = releaseResolvedCap,
            .alloc_embargo_id = peer_control.allocateEmbargoIdForPeerFn(Peer),
            .remember_pending_embargo = peer_control.rememberPendingEmbargoForPeerFn(Peer),
            .send_disembargo_sender_loopback = peer_outbound_control.sendDisembargoSenderLoopbackViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            .store_resolved_import = storeResolvedImport,
        };
        try peer_control.handleResolveWithOps(Peer, self, resolve, ops);
    }

    fn handleDisembargo(self: *Peer, disembargo: protocol.Disembargo) !void {
        const ops = peer_control.DisembargoOps(Peer){
            .send_disembargo_receiver_loopback = peer_outbound_control.sendDisembargoReceiverLoopbackViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            .take_pending_embargo_promise = peer_control.takePendingEmbargoPromiseForPeerFn(Peer),
            .clear_resolved_import_embargo = peer_control.clearResolvedImportEmbargoForPeerFn(Peer),
            .release_embargoed_accepts = peer_embargo_accepts.releaseEmbargoedAcceptsForPeerFn(
                Peer,
                PendingEmbargoedAccept,
                ProvideEntry,
                ProvideTarget,
                Peer.sendReturnProvidedTarget,
                Peer.sendReturnException,
            ),
        };
        try peer_control.handleDisembargoWithOps(Peer, self, disembargo, ops);
    }

    fn makeProvideTarget(self: *Peer, resolved: cap_table.ResolvedCap) !ProvideTarget {
        return switch (resolved) {
            .none => error.PromisedAnswerMissing,
            .imported => |cap| .{ .cap_id = cap.id },
            .exported => |cap| .{ .cap_id = cap.id },
            .promised => |promised| .{
                .promised = try cap_table.OwnedPromisedAnswer.fromPromised(self.allocator, promised),
            },
        };
    }

    fn handleProvide(self: *Peer, provide: protocol.Provide) !void {
        try peer_provide_join_orchestration.handleProvide(
            Peer,
            ProvideEntry,
            ProvideTarget,
            self,
            self.allocator,
            provide,
            &self.provides_by_question,
            &self.provides_by_key,
            peer_provide_join_orchestration.captureProvideRecipientForPeerFn(
                Peer,
                peer_control.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            ),
            peer_control.freeOwnedFrameForPeerFn(Peer),
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            peer_control.resolveProvideTargetForPeerFn(
                Peer,
                peer_control.resolveProvideImportedCapForPeerFn(Peer),
                peer_control.resolveProvidePromisedAnswerForPeerFn(Peer, Peer.resolvePromisedAnswer),
            ),
            makeProvideTarget,
            ProvideTarget.deinit,
        );
    }

    fn handleAccept(self: *Peer, accept: protocol.Accept) !void {
        try peer_provide_join_orchestration.handleAccept(
            Peer,
            ProvideEntry,
            ProvideTarget,
            self,
            accept,
            &self.provides_by_question,
            &self.provides_by_key,
            peer_provide_join_orchestration.captureAcceptProvisionForPeerFn(
                Peer,
                peer_control.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            ),
            peer_control.freeOwnedFrameForPeerFn(Peer),
            peer_embargo_accepts.queueEmbargoedAcceptForPeerFn(Peer, PendingEmbargoedAccept),
            Peer.sendReturnProvidedTarget,
            Peer.sendReturnException,
        );
    }

    fn handleJoin(self: *Peer, join: protocol.Join) !void {
        try peer_provide_join_orchestration.handleJoin(
            Peer,
            JoinKeyPart,
            JoinState,
            PendingJoinQuestion,
            ProvideTarget,
            self,
            self.allocator,
            join,
            &self.pending_joins,
            &self.pending_join_questions,
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            peer_control.resolveProvideTargetForPeerFn(
                Peer,
                peer_control.resolveProvideImportedCapForPeerFn(Peer),
                peer_control.resolveProvidePromisedAnswerForPeerFn(Peer, Peer.resolvePromisedAnswer),
            ),
            makeProvideTarget,
            ProvideTarget.deinit,
            JoinState.init,
            peer_join_state.completeJoinForPeerFn(
                Peer,
                JoinState,
                PendingJoinQuestion,
                ProvideTarget,
                provideTargetsEqual,
                Peer.sendReturnProvidedTarget,
                Peer.sendReturnException,
                JoinState.deinit,
            ),
            Peer.sendReturnException,
        );
    }

    fn handleThirdPartyAnswer(self: *Peer, third_party_answer: protocol.ThirdPartyAnswer) !void {
        try peer_third_party_adoption.handleThirdPartyAnswer(
            Peer,
            PendingThirdPartyAwait,
            self.allocator,
            self,
            third_party_answer,
            &self.pending_third_party_awaits,
            &self.pending_third_party_answers,
            peer_third_party_adoption.captureThirdPartyCompletionForPeerFn(
                Peer,
                peer_control.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            ),
            peer_control.freeOwnedFrameForPeerFn(Peer),
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            peer_third_party_adoption.adoptPendingAwaitEntryForPeerFn(
                Peer,
                Question,
                PendingThirdPartyAwait,
                adoptThirdPartyAnswer,
            ),
        );
    }

    fn handleCall(self: *Peer, frame: []const u8, call: protocol.Call) !void {
        // Reject duplicate question IDs from the remote peer (spec violation).
        if (self.resolved_answers.contains(call.question_id) or
            self.send_results_to_yourself.contains(call.question_id) or
            self.send_results_to_third_party.contains(call.question_id))
        {
            return error.DuplicateQuestionId;
        }

        peer_call_orchestration.handleCallForPeer(
            Peer,
            self,
            frame,
            call,
            peer_call_orchestration.handleCallImportedTargetForPeerFn(
                Peer,
                cap_table.InboundCapTable,
                Peer.queuePromiseExportCall,
                Peer.releaseInboundCaps,
                peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
                peer_control.noteCallSendResultsForPeerFn(
                    Peer,
                    Peer.noteSendResultsToYourself,
                    Peer.noteSendResultsToThirdParty,
                ),
                Peer.sendReturnException,
                Peer.handleResolvedCall,
            ),
            peer_call_orchestration.handleCallPromisedTargetForPeerFn(
                Peer,
                cap_table.InboundCapTable,
                Peer.resolvePromisedAnswer,
                peer_call_targets.hasUnresolvedPromiseExportForPeerFn(Peer),
                Peer.queuePromisedCall,
                Peer.queuePromiseExportCall,
                Peer.sendReturnException,
                Peer.handleResolvedCall,
                Peer.releaseInboundCaps,
                peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
            ),
        ) catch |err| {
            log.debug("call routing error for question {}: {}", .{ call.question_id, err });
            try self.sendReturnException(call.question_id, @errorName(err));
        };
    }

    fn handleResolvedCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        resolved: cap_table.ResolvedCap,
    ) !void {
        try peer_control.handleResolvedCall(
            Peer,
            cap_table.InboundCapTable,
            self,
            call,
            inbound_caps,
            resolved,
            peer_call_orchestration.handleResolvedExportedCallForPeerFn(
                Peer,
                cap_table.InboundCapTable,
                peer_control.noteCallSendResultsForPeerFn(
                    Peer,
                    Peer.noteSendResultsToYourself,
                    Peer.noteSendResultsToThirdParty,
                ),
                Peer.handleResolvedCall,
                Peer.sendReturnException,
            ),
            peer_forward_orchestration.forwardResolvedCallForPeerFn(
                Peer,
                cap_table.InboundCapTable,
                Peer.forwardResolvedCall,
            ),
            Peer.sendReturnException,
        );
    }

    fn recordResolvedAnswer(self: *Peer, question_id: u32, frame: []u8) !void {
        try peer_promises.recordResolvedAnswer(
            Peer,
            ResolvedAnswer,
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            self,
            question_id,
            frame,
            &self.resolved_answers,
            &self.pending_promises,
            Peer.resolvePromisedAnswer,
            Peer.sendReturnException,
            Peer.handleResolvedCall,
            Peer.releaseInboundCaps,
            peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
        );
    }

    fn queuePromisedCall(self: *Peer, question_id: u32, frame: []const u8, inbound_caps: cap_table.InboundCapTable) !void {
        try peer_promises.queuePendingCall(
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            &self.pending_promises,
            question_id,
            frame,
            inbound_caps,
        );
    }

    fn queuePromiseExportCall(self: *Peer, export_id: u32, frame: []const u8, inbound_caps: cap_table.InboundCapTable) !void {
        try peer_promises.queuePendingCall(
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            &self.pending_export_promises,
            export_id,
            frame,
            inbound_caps,
        );
    }

    fn replayResolvedPromiseExport(self: *Peer, export_id: u32, resolved: cap_table.ResolvedCap) !void {
        try peer_promises.replayResolvedPromiseExport(
            Peer,
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            self,
            export_id,
            resolved,
            &self.pending_export_promises,
            Peer.handleResolvedCall,
            Peer.sendReturnException,
            Peer.releaseInboundCaps,
            peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
        );
    }

    fn adoptThirdPartyAnswer(
        self: *Peer,
        question_id: u32,
        adopted_answer_id: u32,
        question: Question,
    ) anyerror!void {
        try peer_third_party_adoption.adoptThirdPartyAnswer(
            Peer,
            Question,
            self,
            question_id,
            adopted_answer_id,
            question,
            &self.questions,
            &self.adopted_third_party_answers,
            &self.pending_third_party_returns,
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            peer_control.freeOwnedFrameForPeerFn(Peer),
            peer_third_party_returns.handlePendingReturnFrameForPeerFn(
                Peer,
                Peer.handleReturn,
            ),
        );
    }

    fn handleReturn(self: *Peer, frame: []const u8, ret: protocol.Return) anyerror!void {
        try peer_return_orchestration.handleReturn(
            Peer,
            Question,
            cap_table.InboundCapTable,
            self,
            frame,
            ret,
            peer_return_orchestration.fetchRemoveQuestionForPeerFn(Peer, Question),
            peer_return_orchestration.handleMissingReturnQuestionForPeerFn(Peer),
            peer_return_orchestration.initInboundCapsForPeerFn(Peer, cap_table.InboundCapTable),
            peer_return_orchestration.deinitInboundCapsForTypeFn(cap_table.InboundCapTable),
            peer_third_party_adoption.handleReturnAcceptFromThirdPartyForPeerFn(
                Peer,
                Question,
                PendingThirdPartyAwait,
                cap_table.InboundCapTable,
                peer_control.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
                peer_control.freeOwnedFrameForPeerFn(Peer),
                peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrame),
                adoptThirdPartyAnswer,
            ),
            peer_return_dispatch.maybeSendAutoFinishForPeerFn(
                Peer,
                Question,
                peer_outbound_control.sendFinishViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            ),
            peer_return_orchestration.handleReturnRegularForPeerFn(
                Peer,
                Question,
                cap_table.InboundCapTable,
                Peer.releaseInboundCaps,
                peer_outbound_control.sendFinishViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            ),
        );
    }

    /// Exposed for integration tests that exercise internal Peer methods.
    /// Not part of the public API.
    pub const test_hooks = struct {
        pub const ForwardCallContextType = ForwardCallContext;

        pub fn sendFrame(self: *Peer, frame: []const u8) !void {
            return Peer.sendFrame(self, frame);
        }

        pub fn collectReleaseCounts(
            self: *Peer,
            inbound: *cap_table.InboundCapTable,
        ) !std.AutoHashMap(u32, u32) {
            const release_import = peer_cap_lifecycle.releaseImportRefForPeerFn(Peer);
            var releases = std.AutoHashMap(u32, u32).init(self.allocator);
            errdefer releases.deinit();
            var idx: u32 = 0;
            while (idx < inbound.len()) : (idx += 1) {
                if (inbound.isRetained(idx)) continue;
                const entry = try inbound.get(idx);
                switch (entry) {
                    .imported => |cap| {
                        const removed = release_import(self, cap.id);
                        if (removed) {
                            try Peer.releaseResolvedImport(self, cap.id);
                        }
                        const slot = try releases.getOrPut(cap.id);
                        if (!slot.found_existing) {
                            slot.value_ptr.* = 1;
                        } else {
                            slot.value_ptr.* +%= 1;
                        }
                    },
                    else => {},
                }
            }
            return releases;
        }

        pub fn clonePayloadWithRemappedCaps(
            self: *Peer,
            builder: *message.MessageBuilder,
            payload_builder: message.StructBuilder,
            source: protocol.Payload,
            inbound_caps: *const cap_table.InboundCapTable,
        ) !void {
            return Peer.clonePayloadWithRemappedCaps(self, builder, payload_builder, source, inbound_caps);
        }

        pub fn onForwardedReturn(
            ctx_ptr: *anyopaque,
            self: *Peer,
            ret: protocol.Return,
            inbound_caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            return Peer.onForwardedReturn(ctx_ptr, self, ret, inbound_caps);
        }

        pub fn handleResolvedCall(
            self: *Peer,
            call: protocol.Call,
            inbound_caps: *const cap_table.InboundCapTable,
            resolved: cap_table.ResolvedCap,
        ) !void {
            return Peer.handleResolvedCall(self, call, inbound_caps, resolved);
        }

        pub fn handleFinish(self: *Peer, finish: protocol.Finish) !void {
            return Peer.handleFinish(self, finish);
        }

        pub fn handleCall(self: *Peer, frame: []const u8, call: protocol.Call) !void {
            return Peer.handleCall(self, frame, call);
        }
    };
};
