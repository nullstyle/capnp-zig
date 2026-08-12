const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_peer);
const events = @import("../events.zig");
const rpc_time = @import("../time.zig");
const protocol = @import("../wire/protocol.zig");
const cap_table = @import("../caps/table.zig");
const message = @import("../../serialization/message.zig");
pub const dispatch = @import("./dispatch.zig");
pub const bootstrap = @import("./bootstrap.zig");
pub const persistence = @import("./persistence.zig");
pub const finish = @import("./finish.zig");
pub const resolve = @import("./resolve.zig");
pub const disembargo = @import("./disembargo.zig");
pub const provide_accept_join = @import("./provide_accept_join.zig");
pub const third_party = @import("./third_party.zig");

const peer_call_targets = @import("./call/peer_call_targets.zig");
const peer_call_sender = @import("./call/peer_call_sender.zig");
const payload_remap = @import("../caps/payload_remap.zig");
const pending_calls = @import("../promises/pending_calls.zig");
const peer_inbound_release = @import("./peer_inbound_release.zig");
const peer_forward_orchestration = @import("./forward/peer_forward_orchestration.zig");
const peer_forward_return_callbacks = @import("./forward/peer_forward_return_callbacks.zig");
const peer_cap_lifecycle = @import("./peer_cap_lifecycle.zig");
const peer_outbound_control = @import("./peer_outbound_control.zig");
const peer_call_orchestration = @import("./call/peer_call_orchestration.zig");
const peer_return_orchestration = @import("./return/peer_return_orchestration.zig");
const peer_return_dispatch = @import("./return/peer_return_dispatch.zig");
const return_routing = @import("../promises/return_routing.zig");
const return_send = @import("../promises/return_send.zig");
const peer_transport = @import("./transport.zig");
const vat_network = @import("../vat/network.zig");
const peer_question_state = @import("./peer_question_state.zig");
const retained_question_state = @import("./retained_questions.zig");
const provide_forward_target = @import("./provide/forward_target.zig");
const peer_cleanup = @import("./peer_cleanup.zig");
const peer_return_frames = @import("./return/peer_return_frames.zig");
const join_network = @import("../vat/join.zig");
const vat_provisions = @import("../vat/provisions.zig");
const peer_provision_hosting = @import("./provision/peer_provision_hosting.zig");
const peer_provision_drain = @import("./provision/peer_provision_drain.zig");
const peer_return_send = @import("./return/peer_return_send.zig");
const peer_export_release = @import("./peer_export_release.zig");
const vat_host = @import("../vat/host.zig");
const promises_promised_answer = @import("../promises/promised_answer.zig");

pub const errors = @import("./errors.zig");
pub const state = @import("./state.zig");

/// Typed failure surface of generated Response.unwrap() (see errors.zig).
/// Re-exported here so generated code and consumers can spell it
/// `rpc.peer.CallError`.
pub const CallError = errors.CallError;

/// Callback invoked to populate a `CallBuilder` before sending an outbound call.
pub const CallBuildFn = *const fn (ctx: *anyopaque, call: *protocol.CallBuilder) anyerror!void;
/// Callback invoked to populate a `ReturnBuilder` before sending a return.
pub const ReturnBuildFn = *const fn (ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void;
/// Handler invoked when an inbound call arrives for an exported capability.
pub const CallHandler = *const fn (ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void;
/// Callback invoked when a return message arrives for a previously sent question.
pub const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
/// Callback invoked when a Level-3 three-party handoff auto-pickup completes: an
/// inbound `thirdPartyHosted` Resolve for `promise_id` (on the peer holding the
/// promise, VatA↔VatB) drove a `Provide` pickup, and the `Accept` `Return`
/// carrying the direct capability just arrived on `accept_peer` (VatA↔VatC).
/// The handler imports the direct cap from `ret`/`accept_caps` on `accept_peer`
/// (retain it there — see `InboundCapTable.retainCapability`) so subsequent calls
/// go straight to the third vat. `promise_id` names the fulfilled promise import
/// on the promise-holding peer. The vine is released by the runtime immediately
/// after this returns (driving VatB's Provide `Finish`), so the handler must not
/// depend on the vine surviving. Errors are reported non-fatally.
pub const HandoffPickupCallback = *const fn (
    ctx: *anyopaque,
    promise_peer: *Peer,
    promise_id: u32,
    accept_peer: *Peer,
    ret: protocol.Return,
    accept_caps: *const cap_table.InboundCapTable,
) anyerror!void;
/// Optional override for outbound frame delivery (used in testing).
pub const SendFrameOverride = *const fn (ctx: *anyopaque, frame: []const u8) anyerror!void;

/// Exception reason delivered to every outstanding question's `on_return` when
/// the transport closes (see `onConnectionClose`). The terminal
/// disconnect-failure signal is a synthetic exception `Return` routed through
/// the normal `QuestionCallback`; it is never recorded in `resolved_answers`
/// nor replayed to pipelining, so callers observe a one-shot failure.
pub const disconnected_reason = "disconnected";

/// Exception reason for a question cancelled by deadline expiry (see
/// `checkDeadlines`). Exported so response-unwrapping code can match the
/// locally synthesized reason without duplicating the literal.
pub const deadline_reason = "deadline exceeded";

/// Exception reason for questions force-cancelled when the graceful-shutdown
/// drain bound expires (see `checkDeadlines`).
pub const shutdown_reason = "peer shutting down";

/// An exported capability: a context pointer and its call handler.
pub const Export = struct {
    ctx: *anyopaque,
    on_call: CallHandler,
};

/// Handler invoked when the remote calls `Persistent.save()` on an export
/// marked persistent via `Peer.setPersistentExport`. Returns the app-defined
/// sturdy-ref payload, allocated with `peer.allocator`; the peer embeds it
/// in the `SaveResults` Return and frees it. `seal_for` is the raw
/// `SaveParams.sealFor` pointer when the caller supplied one (sealing is
/// realm-defined; handlers are free to ignore it).
pub const SaveHandler = *const fn (ctx: *anyopaque, peer: *Peer, export_id: u32, seal_for: ?message.AnyPointerReader) anyerror![]u8;
/// What a restorer hook resolved a sturdy ref to (see `Peer.setRestorer`).
pub const RestoreOutcome = persistence.RestoreOutcome(Export);
/// Handler invoked when the remote calls the vat-level restore method on
/// the bootstrap capability. The sturdy-ref bytes are borrowed from the
/// inbound frame; copy them to retain beyond the call.
pub const RestoreHandler = *const fn (ctx: *anyopaque, peer: *Peer, sturdy_ref: []const u8) anyerror!RestoreOutcome;
/// Outcome of a `Peer.sendSave` question, delivered to its callback.
pub const SaveResponse = union(enum) {
    /// Sturdy-ref payload, borrowed from the Return frame; copy to retain.
    sturdy_ref: []const u8,
    exception: protocol.Exception,
    /// Any other Return tag (canceled, resultsSentElsewhere, ...).
    other: protocol.ReturnTag,
};
/// Callback invoked when a `Peer.sendSave` question completes.
pub const SaveResponseCallback = *const fn (ctx: *anyopaque, peer: *Peer, response: SaveResponse) anyerror!void;
/// Outcome of a `Peer.sendRestore` question, delivered to its callback.
pub const RestoreResponse = union(enum) {
    /// The restored capability (retained; typically `.imported`). Pass to
    /// `sendCall`/`sendCallResolved` to resume calling.
    cap: cap_table.ResolvedCap,
    exception: protocol.Exception,
    /// Any other Return tag (canceled, resultsSentElsewhere, ...).
    other: protocol.ReturnTag,
};
/// Callback invoked when a `Peer.sendRestore` question completes.
pub const RestoreResponseCallback = *const fn (ctx: *anyopaque, peer: *Peer, response: RestoreResponse) anyerror!void;

/// A registered save hook: context and handler always travel together so
/// dispatch never has to unwrap them independently.
const SaveHook = struct {
    ctx: *anyopaque,
    on_save: SaveHandler,
};

/// A registered restore hook (see `SaveHook`).
const RestoreHook = struct {
    ctx: *anyopaque,
    on_restore: RestoreHandler,
};

/// Persistence hooks installed on one export. The peer wraps the export's
/// original handler with a trampoline that serves `Persistent.save()` and
/// vat-level restore calls, forwarding everything else to the original.
const PersistenceState = struct {
    export_id: u32,
    original: Export,
    save: ?SaveHook = null,
    restore: ?RestoreHook = null,
};

const ExportEntry = state.ExportEntry(Export);
const ResolvedAnswer = state.ResolvedAnswer;
const FailedAnswer = state.FailedAnswer;
const PendingCall = state.PendingCall;

const ProvideTarget = state.ProvideTarget;
const ProvideEntry = state.ProvideEntry;

/// The record an accept peer keeps for each queued/parked cross-peer Accept.
/// `embargo_key` is the accept peer's OWN dupe (its allocator), so no cleanup
/// path ever hashes bytes owned by — and possibly freed with — the provision's
/// embargo map.
const CrossPeerAcceptRecord = struct {
    /// UNCOUNTED back-link (INV-REC: the pending slot's / parked entry's +1
    /// keeps the provision alive as long as this record exists).
    provision: *ProvisionIndex.Provision,
    /// Owned by THIS peer's allocator; null for a parked non-embargoed accept.
    embargo_key: ?[]u8,
    parked: bool,
};
const JoinKeyPart = state.JoinKeyPart;
const JoinPartEntry = state.JoinPartEntry;
const JoinState = state.JoinState;
const PendingJoinQuestion = state.PendingJoinQuestion;
const PendingEmbargoedAccept = state.PendingEmbargoedAccept;

const ForwardReturnMode = peer_forward_orchestration.ForwardReturnMode;

const ResolvedImport = state.ResolvedImport;

pub const PeerLimits = state.PeerLimits;
pub const PeerTimeouts = state.PeerTimeouts;
pub const ResultLifetime = retained_question_state.ResultLifetime;
pub const CallOptions = retained_question_state.CallOptions;

const QuestionDeinitCtxFn = state.QuestionDeinitCtxFn;
const ExportDeinitCtxFn = *const fn (std.mem.Allocator, *anyopaque) void;
const Question = state.Question(QuestionCallback);
const PendingThirdPartyAwait = state.PendingThirdPartyAwait(Question);

/// Low bound of the callee-allocated `ThirdPartyAnswer` answer-id range
/// (rpc.capnp:936-941): bit 30 set. The high bound is 2^31 (bit 31 clear).
/// Every id `Peer.sendThirdPartyAnswer` mints satisfies
/// `third_party.isThirdPartyAnswerId`.
const third_party_answer_id_base: u32 = 0x4000_0000;
const third_party_answer_id_limit: u32 = 0x8000_0000;

const ForwardCallContext = struct {
    peer: *Peer,
    payload: protocol.Payload,
    inbound_caps: cap_table.InboundCapTable,
    send_results_to: protocol.SendResultsToTag,
    send_results_to_third_party_payload: ?[]u8 = null,
    answer_id: u32,
    mode: ForwardReturnMode,

    fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
        const ctx: *ForwardCallContext = @ptrCast(@alignCast(ctx_ptr));
        ctx.inbound_caps.deinit();
        if (ctx.send_results_to_third_party_payload) |payload| allocator.free(payload);
        allocator.destroy(ctx);
    }
};

const ForwardReturnBuildContext = struct {
    peer: *Peer,
    payload: protocol.Payload,
    inbound_caps: *const cap_table.InboundCapTable,
};

/// L3 parked-call FORWARDING relay state (issue #56). Threads a call that was
/// pipelined by VatA on a handed-off promise — parked at VatB, then replayed
/// onto the handoff vine — through to VatC and relays VatC's Return back to
/// complete VatA's ORIGINAL pipelined question.
///
/// It spans two peers: the forwarded call goes out on `provide_peer` (B↔C, the
/// peer field carried by the outbound question), but the relayed result must
/// complete VatA's question on `recipient_peer` (B↔A) under
/// `recipient_answer_id`. Heap-allocated because the forwarded question outlives
/// the synchronous replay in a real async transport; freed by
/// `forwardVineReturn` on the single Return, or by the forwarded question's
/// `deinit_ctx` if the B↔C peer tears down first.
const ForwardVineCallContext = struct {
    /// The peer used to forward the call to VatC (B↔C). Parameter-cap proxy
    /// exports created while building the forwarded call live on this peer.
    forward_peer: *Peer,
    /// The peer that received VatA's original pipelined call (B↔A). Its
    /// `recipient_answer_id` active-inbound question is completed with VatC's
    /// result.
    recipient_peer: ?*Peer,
    /// VatA's original pipelined question id on `recipient_peer` (B↔A).
    recipient_answer_id: u32,
    /// VatA's parked call params, cloned into the forwarded call sent to VatC.
    /// Read ONLY by `forwardVineParams`, which runs synchronously inside the
    /// `sendCall` that registers this ctx as the outbound question's callback —
    /// while the parked frame these readers point into is still alive. It is
    /// never touched after `sendCall` returns (the later `forwardVineReturn`
    /// reads only `recipient_peer` / `recipient_answer_id` / `settled_flag`), so
    /// the ctx may safely outlive the parked frame.
    source_params: protocol.Payload,
    /// VatA's parked call cap table. Only used synchronously by
    /// `forwardVineParams`, but stored here so capability pointers in
    /// `source_params` can be remapped into B↔C proxy exports.
    source_inbound_caps: *cap_table.InboundCapTable,
    created_param_proxy_ids: std.ArrayList(u32) = .empty,
    param_proxies_committed: bool = false,
    /// Back-link registration on `recipient_peer`. It lets recipient teardown
    /// null this borrowed pointer before the async forwarding peer can invoke
    /// the Return callback.
    recipient_link_registered: bool = false,
    /// True only after the async forwarded question owns this context. If its
    /// peer tears down without invoking the Return callback, `deinit` fails the
    /// still-live upstream answer exactly once.
    recipient_answer_pending: bool = false,
    /// One-shot back-signal to `forwardVineCallToProvidedTarget`, set true by
    /// `forwardVineReturn` the instant the return callback runs (before it frees
    /// this ctx). It points at a stack-local `bool` in that caller and is valid
    /// ONLY while the callback runs SYNCHRONOUSLY nested inside `sendCall` (the
    /// loopback case), where that frame is still live. The caller nulls it before
    /// its frame unwinds on the async hand-off path, so a later callback never
    /// writes through a dangling pointer. It lets the caller's post-`sendCall`
    /// code (including its error path) distinguish "the callback already answered
    /// VatA and freed this ctx" from "the callback never ran" — so a `sendCall`
    /// that fails in post-callback work does not double-answer VatA or double-free
    /// this ctx.
    settled_flag: ?*bool = null,

    fn rollbackParamProxies(ctx: *ForwardVineCallContext) void {
        if (ctx.param_proxies_committed) return;
        for (ctx.created_param_proxy_ids.items) |id| {
            ctx.forward_peer.destroyUnreferencedProxyExport(id);
        }
    }

    fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
        const ctx: *ForwardVineCallContext = @ptrCast(@alignCast(ctx_ptr));
        if (ctx.recipient_peer) |recipient| {
            if (ctx.recipient_link_registered) {
                recipient.deregisterForwardVineRelay(ctx);
                ctx.recipient_link_registered = false;
            }
            ctx.recipient_peer = null;
            if (ctx.recipient_answer_pending) {
                ctx.recipient_answer_pending = false;
                recipient.sendReturnException(
                    ctx.recipient_answer_id,
                    "forwarded handoff connection closed",
                ) catch |err| {
                    log.debug("forwarded handoff teardown: failed to settle question {}: {}", .{
                        ctx.recipient_answer_id,
                        err,
                    });
                };
            }
        }
        ctx.rollbackParamProxies();
        ctx.created_param_proxy_ids.deinit(allocator);
        allocator.destroy(ctx);
    }
};

const CrossPeerProxyContext = struct {
    owner_peer: *Peer,
    export_id: u32 = 0,
    /// Peer whose cap table can call the original capability represented by
    /// `target`. When the proxy is released, an owned source import ref is
    /// released on this peer if one was retained.
    source_peer: ?*Peer,
    target: cap_table.ResolvedCap,
    release_source_import_id: ?u32 = null,
    /// A handoff-held pin on a SOURCE-peer export that this proxy owns and
    /// releases exactly once in deinit (ownership transfers AT the
    /// `addCrossPeerProxyExport` call, success or failure — the caller never
    /// rolls it back afterwards). Keeps the proxied export alive across the
    /// introducer's Finish + the remote's wire Releases; abandoned (nulled,
    /// never released) if the source peer dies first, same rule as
    /// `release_source_import_id`.
    release_source_export_pin_id: ?u32 = null,
    /// A handoff pin on a SOURCE-peer IMPORT (the receiverHosted serve shape),
    /// owned and released exactly once in deinit with the same ownership
    /// transfer and once-only discipline as `release_source_export_pin_id`.
    /// SEPARATE from the export pin on purpose: import and export ids share
    /// one numeric space per connection, so a shared field would unpin the
    /// wrong table by a colliding bare id. Abandoned (nulled, never released)
    /// if the source peer dies first — its import table dies with it.
    release_source_import_pin_id: ?u32 = null,

    fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
        const ctx: *CrossPeerProxyContext = @ptrCast(@alignCast(ctx_ptr));
        if (ctx.source_peer) |source_peer| {
            source_peer.deregisterCrossPeerProxy(ctx.owner_peer, ctx.export_id);
            if (ctx.release_source_import_id) |import_id| {
                source_peer.releaseImport(import_id, 1) catch |err| {
                    log.debug("cross-peer proxy: failed to release source import {}: {}", .{ import_id, err });
                };
            }
            if (ctx.release_source_export_pin_id) |pin_id| {
                source_peer.releaseHandoffHeldExport(pin_id);
            }
            if (ctx.release_source_import_pin_id) |pin_id| {
                // Rule-5 posture: this deinit is an infallible context (proxy
                // destruction by wire Release or peer teardown), so the unpin
                // failure is catch-logged — the same once-only, best-effort
                // shape as `release_source_import_id` above.
                source_peer.releaseHandoffImportPin(pin_id) catch |err| {
                    log.debug("cross-peer proxy: failed to release source import handoff pin {}: {}", .{ pin_id, err });
                };
            }
        }
        allocator.destroy(ctx);
    }

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *CrossPeerProxyContext = castCtx(*CrossPeerProxyContext, ctx_ptr);
        const source_peer = ctx.source_peer orelse {
            try peer.sendReturnException(call.question_id, "cross-peer proxy source disconnected");
            return;
        };
        try peer.forwardCrossPeerProxyCall(call, inbound_caps, source_peer, ctx.target);
    }
};

const CrossPeerProxyCallContext = struct {
    recipient_peer: *Peer,
    recipient_answer_id: u32,
    forward_peer: *Peer,
    target: cap_table.ResolvedCap,
    source_params: protocol.Payload,
    source_inbound_caps: *cap_table.InboundCapTable,
    created_param_proxy_ids: std.ArrayList(u32) = .empty,
    param_proxies_committed: bool = false,
    settled_flag: ?*bool = null,

    fn rollbackParamProxies(ctx: *CrossPeerProxyCallContext) void {
        if (ctx.param_proxies_committed) return;
        for (ctx.created_param_proxy_ids.items) |id| {
            ctx.forward_peer.destroyUnreferencedProxyExport(id);
        }
    }

    fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
        const ctx: *CrossPeerProxyCallContext = @ptrCast(@alignCast(ctx_ptr));
        ctx.rollbackParamProxies();
        ctx.created_param_proxy_ids.deinit(allocator);
        allocator.destroy(ctx);
    }
};

const CrossPeerJoinRelayContext = struct {
    owner_peer: *Peer,
    owner_answer_id: u32,
    settled_flag: ?*bool = null,

    fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
        const ctx: *CrossPeerJoinRelayContext = @ptrCast(@alignCast(ctx_ptr));
        allocator.destroy(ctx);
    }
};

const CrossPeerCapMapContext = struct {
    inbound_peer: *Peer,
    outbound_peer: *Peer,
    inbound_caps: *cap_table.InboundCapTable,
    created_proxy_ids: *std.ArrayList(u32),
    pin_source_caps: bool,
    remapped_by_index: std.AutoHashMap(u32, u32),

    fn init(
        inbound_peer: *Peer,
        outbound_peer: *Peer,
        inbound_caps: *cap_table.InboundCapTable,
        created_proxy_ids: *std.ArrayList(u32),
        pin_source_caps: bool,
    ) CrossPeerCapMapContext {
        return .{
            .inbound_peer = inbound_peer,
            .outbound_peer = outbound_peer,
            .inbound_caps = inbound_caps,
            .created_proxy_ids = created_proxy_ids,
            .pin_source_caps = pin_source_caps,
            .remapped_by_index = std.AutoHashMap(u32, u32).init(outbound_peer.allocator),
        };
    }

    fn deinit(ctx: *CrossPeerCapMapContext) void {
        ctx.remapped_by_index.deinit();
    }
};

const CrossPeerReturnRelayContext = struct {
    source_peer: *Peer,
    target_peer: *Peer,
    source: protocol.Payload,
    source_inbound_caps: *cap_table.InboundCapTable,
    release_param_caps: bool = true,
    created_result_proxy_ids: std.ArrayList(u32) = .empty,
    result_proxies_committed: bool = false,
    /// Results authored by a local handler use the source peer's capability-id
    /// space directly.  When they cross a connection boundary, proxy every
    /// source capability under a handoff pin so it remains callable until the
    /// recipient releases the proxy.
    pin_source_caps: bool = false,

    fn rollbackResultProxies(ctx: *CrossPeerReturnRelayContext) void {
        if (ctx.result_proxies_committed) return;
        for (ctx.created_result_proxy_ids.items) |id| {
            ctx.target_peer.destroyUnreferencedProxyExport(id);
        }
    }

    fn deinit(ctx: *CrossPeerReturnRelayContext, allocator: std.mem.Allocator) void {
        ctx.rollbackResultProxies();
        ctx.created_result_proxy_ids.deinit(allocator);
    }
};

/// Source-owned state for an automatic `sendResultsTo.thirdParty` redirect.
///
/// The source peer (the callee-facing introducer connection) owns the allocation
/// and indexes it by the original answer id.  The result peer keeps only a
/// borrowed backlink keyed by the synthetic callee-allocated answer id.  This
/// makes either peer's teardown able to neutralize the other side without
/// guessing whether a Return was already visible on either transport.
const AutomaticThirdPartyRoute = struct {
    const TargetOutcome = enum {
        connected,
        canceled,
        disconnected,
        settled,
    };

    source_peer: ?*Peer,
    source_answer_id: u32,
    target_peer: ?*Peer,
    target_answer_id: u32,
    target_outcome: TargetOutcome = .connected,
    /// True while an outbound ThirdPartyAnswer / redirected Return can invoke a
    /// synchronous transport callback.  Finish is allowed to clear routing in
    /// that callback; destruction is deferred until the outer operation stops
    /// borrowing this allocation.
    operation_active: bool = false,
    delivering_result: bool = false,
    clear_requested: bool = false,
    /// Target delivery committed, but the source resultsSentElsewhere write
    /// reported failure. Keep the already-owned route allocation as a
    /// no-allocation tombstone until handler dispatch tries to translate the
    /// error into an exception; that translation must be refused to avoid a
    /// possible second source terminal.
    source_marker_failed: bool = false,
};

fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
    return @ptrCast(@alignCast(ctx));
}

fn msToNs(ms: u64) i64 {
    return @as(i64, @intCast(ms)) * std.time.ns_per_ms;
}

/// True when an inbound Call's params cap table contains at least one
/// descriptor that grants this vat a wire reference — the descriptors
/// `InboundCapTable.init` turns into `noteImport` calls, and therefore the
/// ones this vat later settles with an explicit `Release` frame.
///
/// Read straight off the caller's frame (tags only; no import is taken here)
/// so the answering Return's `releaseParamCaps` can be decided before dispatch
/// runs — see `Peer.returnReleasesParamCaps`.
///
/// A descriptor this peer cannot even decode counts as ref-granting, but purely
/// as a defensive default: no Return ever reads that value. `InboundCapTable`
/// construction is about to fail on the same entry, and that failure unwinds
/// `handleCall`, whose `errdefer` drops this answer's `active_inbound_questions`
/// record — while the dispatch-error path sends no Return at all for a Call it
/// could not dispatch. (Nor is a reference stranded: the table's own `errdefer`
/// releases every import it noted before the failing entry.) The conservative
/// direction is still the right one to be wrong in, because it can only ever
/// withhold an implicit release, never invent a second one.
fn callParamsGrantImportRefs(cap_table_list: ?message.StructListReader) bool {
    const list = cap_table_list orelse return false;
    var idx: u32 = 0;
    while (idx < list.len()) : (idx += 1) {
        const reader = list.get(idx) catch return true;
        const descriptor = protocol.CapDescriptor.fromReader(reader) catch return true;
        switch (descriptor.tag) {
            // `resolveDescriptor` notes an import for each of these: the two
            // sender-side forms, and the vine of a third-party hand-off.
            .senderHosted, .senderPromise, .thirdPartyHosted => return true,
            // `receiverHosted` names one of our own exports and `receiverAnswer`
            // one of our own answers; neither grants us a reference to release.
            .none, .receiverHosted, .receiverAnswer => {},
        }
    }
    return false;
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
///   +------------+ attachConnection / attachTransportBinding  +----------+
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
///   via `attachConnection`, `attachTransport`, or `attachTransportBinding`,
///   but `start()` has not been
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
/// | `exports` | export ID | `ExportEntry` | Local capabilities offered to the remote peer. Ref-counted (wire refs from Release plus answer-held refs from recorded answers); removed when both counts reach zero. |
/// | `questions` | question ID | `Question` | Outstanding outbound calls awaiting a Return. Removed when the Return arrives. |
/// | `question_param_export_refs` | question ID | export IDs (list) | Wire refs the question's Call params took on local exports (one per emitted senderHosted/senderPromise descriptor). Spent when the Return arrives with `releaseParamCaps = true` (the rpc.capnp default), dropped when it is false (the remote sends explicit Releases), freed unspent when the question dies without a wire Return. |
/// | `resolved_answers` | question ID | `ResolvedAnswer` | Cached Return frames for answered questions (used to resolve PromisedAnswer references). Holds one answer-held reference per results export so pipeline targets survive an early Release. Removed on Finish, releasing those references. |
/// | `failed_answers` | question ID | `FailedAnswer` | The exception already returned for a failed answer (results-only `resolved_answers` never records it). Lets a call pipelined on that answer arriving after the Return be failed with a copy of the same exception instead of queueing forever. Removed on Finish. |
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
/// | `persistent_exports` | export ID | `*PersistenceState` | Save/restore hooks for persistent exports. Removed when the hooks are cleared or the export is released. |
///
/// ## Invariants
///
/// * Question IDs are monotonically increasing (mod 2^32) and never reused
///   within a single peer lifetime.
/// * Each export ID has at most one entry in `exports`. The entry's
///   `ref_count` tracks how many times the remote peer has received the
///   capability in a message payload; its `answer_ref_count` tracks how many
///   recorded resolved answers carry it in their results. An inbound Release
///   spends only wire refs, so a still-unfinished answer keeps its pipeline
///   targets dispatchable regardless of how eagerly the remote releases.
/// * A question is removed from `questions` when its Return is fully
///   handled, but its `resolved_answers` entry persists until Finish so
///   that PromisedAnswer references can be resolved.
/// * `pending_promises` entries are drained (replayed or errored) when the
///   corresponding answer resolves, never left dangling.
/// Grouped transport callback bindings. Set/cleared atomically by
/// `Peer.attachTransportBinding` / `Peer.detachTransport`.
pub const TransportBinding = peer_transport.Binding(Peer);
pub const TransportStartFn = TransportBinding.StartFn;
pub const TransportSendFn = TransportBinding.SendFn;
pub const TransportCloseFn = TransportBinding.CloseFn;
pub const TransportIsClosingFn = TransportBinding.IsClosingFn;

/// Level-3 three-party addressing seam (see `rpc.vat.network`). Application
/// supplies one when originating `Provide`/`Accept` handoffs; the two-party core
/// runs unchanged without it.
pub const VatNetwork = vat_network.VatNetwork(Peer);
pub const Introduction = vat_network.Introduction;
pub const Introduced = vat_network.Introduced(Peer);
pub const JoinNetwork = join_network.JoinNetwork(Peer);
/// Experimental vat-wide L3 provision index (three-party HOSTING across
/// multiple connections of one vat). See src/rpc/vat/provisions.zig.
pub const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
pub const ProvisionIndexLimits = vat_provisions.ProvisionIndexLimits;
/// Experimental multi-connection vat facade (owns the provision index + the
/// accept-embargo CSPRNG; one `enroll(peer)` per connection).
pub const Vat = vat_host.Vat(Peer);

/// Experimental source of random bytes for accept-embargo ids (rpc.capnp
/// requires "globally-unique ... chosen at random with enough entropy").
/// Mirrors the `rpc_time.Clock` seam: ctx + fn pointer, trivially fakeable in
/// tests. Installed by the tcp session constructors from a
/// `randomSecure`-seeded CSPRNG; peers without one keep the legacy per-peer
/// counter (correctness never depends on this — the host keys embargoes per
/// provision — it is spec compliance and cross-host hygiene).
pub const EntropySource = struct {
    ctx: *anyopaque,
    fill: *const fn (ctx: *anyopaque, buf: []u8) void,

    pub fn fromCsprng(rng: *std.Random.DefaultCsprng) EntropySource {
        const F = struct {
            fn fill(ctx: *anyopaque, buf: []u8) void {
                const r: *std.Random.DefaultCsprng = @ptrCast(@alignCast(ctx));
                r.random().bytes(buf);
            }
        };
        return .{ .ctx = rng, .fill = F.fill };
    }
};

/// Seed a CSPRNG from the OS entropy syscall — FAIL CLOSED on
/// `error.EntropyUnavailable`: never fall back to `Io.random`'s
/// pointer+timestamp emergency seed (collision-resistant but guessable).
pub fn seedEntropyCsprng(io: std.Io) std.Io.RandomSecureError!std.Random.DefaultCsprng {
    var seed: [std.Random.DefaultCsprng.secret_seed_length]u8 = undefined;
    try io.randomSecure(&seed);
    return std.Random.DefaultCsprng.init(seed);
}
pub const Joined = join_network.Joined(Peer);

/// Handle returned by `Peer.sendProvide`: the ids the caller needs to drive the
/// paired `thirdPartyHosted` descriptor emission and to observe completion.
pub const ProvideHandle = struct {
    /// The held-open Provide question id (no Return; Finished when the recipient
    /// releases the vine).
    question_id: u32,
    /// The vine export id minted for this handoff. Marked third-party-hosted, so
    /// the next descriptor emitted for the provided cap resolves to
    /// `thirdPartyHosted{ contact, vineId = this }`.
    vine_id: u32,
};

/// Experimental Level-4 JoinResult coordinator for the compact Zig Join pilot.
///
/// This is the first high-level joiner-side helper above raw
/// `sendJoinExperimental`: it originates local compact Join key parts, collects
/// matching Zig `JoinResult` payloads through a `JoinNetwork`, sends the direct
/// follow-up `Accept`, retains the accepted capability, and Finishes the
/// JoinResult questions after the direct pickup succeeds. It remains
/// Experimental and Zig-shape-only; it does not define a stable Join key/result
/// format or a cross-implementation L4 contract.
pub const JoinCoordinator = @import("./join/join_coordinator.zig").JoinCoordinator(Peer);

/// Couples a vine export (minted on the host-of-recipient connection, VatB↔VatA)
/// to the held-open `Provide` question it anchors (on the host-of-provided-cap
/// connection, VatB↔VatC). When the recipient releases the vine — the wire
/// signal that the `Accept` completed (or was abandoned) — VatB Finishes the
/// paired Provide question, unregistering the provision on VatC.
///
/// The coupling deliberately spans two peers: the vine lives on THIS peer's cap
/// table (the B↔A connection), while the Provide question lives on a DIFFERENT
/// peer (the B↔C connection), which `provide_peer` points at.
///
/// LIVENESS: `provide_peer` is a borrowed pointer, but the coupling is made
/// resilient to arbitrary per-connection teardown order by a symmetric
/// back-registration. Every live coupling records a back-link on `provide_peer`
/// (`Peer.coupled_vines`); when the provided-cap peer (B↔C) deinits, it walks
/// those back-links and NULLs `provide_peer` in each recipient's coupling entry
/// BEFORE its own memory is freed (see `neutralizeCoupledVinesOnProvidePeer`).
/// A subsequent vine Release on the recipient then finds a null `provide_peer`
/// and skips the Finish — a correct no-op, because the provided-cap peer's own
/// `deinit` already force-cancelled (removed + Finished) the held-open Provide
/// question. So the freed pointer is removed, never read after free, regardless
/// of which peer is torn down first.
const OutboundProvide = struct {
    /// The peer on which the held-open Provide question was sent (B↔C).
    /// Borrowed. Null once that peer has deinited (it neutralizes every coupling
    /// pointing at it before freeing itself), so `finishOriginatedProvide` is
    /// never handed a dangling pointer.
    provide_peer: ?*Peer,
    /// The Provide question id to Finish when the vine is released.
    provide_question_id: u32,
    /// Caller-owned answer whose lifetime transferred into this handoff. The
    /// source answer and the Provide question are distinct wire questions; a
    /// completed coupling must Finish both. Null for ordinary `sendProvide`.
    retained_source_question_id: ?u32 = null,
    /// When the handoff was originated by resolving a promise EXPORT to the
    /// third party (`resolvePromiseExportToThirdParty`), this is that promise
    /// export id on THIS peer, and it resolved locally to the vine (so replayed
    /// pipelined calls dispatch to the vine — where the owned forwarding target
    /// sends them to VatC per issue #56 rather than rejecting them). The
    /// vine's destruction on Release invalidates that resolution target, so the
    /// vine teardown in `handleRelease` nulls the promise export's `resolved` link
    /// to avoid a dangling target. Null for a bare `sendProvide` handoff (no
    /// promise export involved, e.g. the P0–P2 return-a-vine path).
    resolved_promise_export_id: ?u32 = null,
    /// Owned call target for Level-1/2 vine fallback. Imported targets retain
    /// the original id; promised-answer targets own their transform operations
    /// so fallback can forward without borrowing the Provide frame.
    forward_target: provide_forward_target.ForwardTarget,
    /// E-ORDER (rpc.capnp:898-903): true once `replayResolvedPromiseExport` has
    /// forwarded this coupling's parked pre-resolution calls (or the resolve
    /// path is past the point where any could exist). Until then a
    /// `context.accept` Disembargo from the recipient must NOT be forwarded to
    /// the host: under a synchronous transport the forward would release the
    /// host's embargoed Accept — delivering the pickup and any post-pickup
    /// direct calls — BEFORE the parked calls reach the host, inverting e-order
    /// and (worse) completing the handoff so the vine teardown strands the
    /// parked calls entirely.
    replay_flushed: bool = false,
    /// The recipient's forwarded accept-Disembargo, held while `replay_flushed`
    /// is false. Duped embargo bytes, owned by the INTRODUCER peer's allocator;
    /// freed on flush or with the coupling. Single slot: one Disembargo per
    /// handoff is the protocol shape, so a second stash while occupied is
    /// dropped with a log rather than queued.
    stashed_accept_disembargo: ?[]u8 = null,

    /// Stash the recipient's accept-Disembargo until the parked-call replay has
    /// run (see `replay_flushed`).
    fn stashAcceptDisembargo(op: *OutboundProvide, allocator: std.mem.Allocator, embargo: []const u8) !void {
        if (op.stashed_accept_disembargo != null) {
            log.warn("second accept-disembargo stashed before replay; dropping duplicate", .{});
            return;
        }
        op.stashed_accept_disembargo = try allocator.dupe(u8, embargo);
    }

    /// Free the stash slot without dropping the forwarding target. Used while
    /// a live coupling remains in the map.
    fn deinitStash(op: *OutboundProvide, allocator: std.mem.Allocator) void {
        if (op.stashed_accept_disembargo) |stash| allocator.free(stash);
        op.stashed_accept_disembargo = null;
    }

    fn deinit(op: *OutboundProvide, allocator: std.mem.Allocator) void {
        op.deinitStash(allocator);
        op.forward_target.deinit(allocator);
    }
};

const ProvideOriginationTarget = union(enum) {
    message_target: protocol.MessageTarget,
    retained_answer: struct {
        question_id: u32,
        ops: []const protocol.PromisedAnswerOp,
    },

    fn cloneForwardTarget(
        target: ProvideOriginationTarget,
        allocator: std.mem.Allocator,
    ) !provide_forward_target.ForwardTarget {
        return switch (target) {
            .message_target => |message_target| provide_forward_target.ForwardTarget.fromMessageTarget(
                allocator,
                message_target,
            ),
            .retained_answer => |retained| provide_forward_target.ForwardTarget.fromQuestionAndOps(
                allocator,
                retained.question_id,
                retained.ops,
            ),
        };
    }

    fn buildProvide(
        target: ProvideOriginationTarget,
        builder: *protocol.MessageBuilder,
        provide_question_id: u32,
        recipient: message.AnyPointerReader,
    ) !void {
        switch (target) {
            .message_target => |message_target| try builder.buildProvide(
                provide_question_id,
                message_target,
                recipient,
            ),
            .retained_answer => |retained| try builder.buildProvidePromisedAnswerWithOps(
                provide_question_id,
                retained.question_id,
                retained.ops,
                recipient,
            ),
        }
    }
};

/// Liveness back-link recorded on the host-of-provided-cap peer (B↔C) for each
/// coupling that peer anchors. Names the recipient peer (B↔A) whose
/// `outbound_provides[vine_id]` entry borrows a pointer back to this peer, so
/// this peer's `deinit` can find and NULL that borrowed pointer before it frees
/// itself. The reverse of the `OutboundProvide.provide_peer` edge.
const CoupledVine = struct {
    /// The host-of-recipient peer (B↔A) that owns the coupling entry.
    recipient_peer: *Peer,
    /// The vine export id keying the coupling in `recipient_peer.outbound_provides`.
    vine_id: u32,
};

/// Liveness back-link for a cross-peer capability proxy. The proxy export lives
/// on `owner_peer`, but its handler borrows a pointer to the source peer so it
/// can forward calls and release the retained source import. If the source peer
/// deinits first, it walks these links and nulls those borrowed pointers.
const CrossPeerProxyLink = struct {
    owner_peer: *Peer,
    proxy_export_id: u32,
};

const CrossPeerJoinRelay = struct {
    source_peer: ?*Peer,
    source_question_id: u32,
    deadline_ns: ?i64 = null,
    /// Set before attempting the one upstream terminal Return. A relay may
    /// remain live afterward only to forward Finish.releaseResultCaps; expiry
    /// must not emit a second Return while retiring that bookkeeping.
    upstream_terminal_started: bool = false,
};

const CrossPeerJoinRelayLink = struct {
    owner_peer: *Peer,
    owner_answer_id: u32,
};

/// Canonical owner of one completed L4 JoinResult handoff. The result-path
/// peer allocates and owns this object, captures the exact JoinNetwork that
/// created its provision, and charges the provision bytes once. Result answer
/// records and the direct Accept host only borrow this pointer.
const HostedJoin = struct {
    owner_peer: *Peer,
    accept_peer: ?*Peer,
    network: JoinNetwork,
    provision: []u8,
    /// Number of JoinResult Returns successfully published. Unlike
    /// `result_refs`, this is not decremented by Finish or transport close;
    /// it records that the direct-Accept lease was committed on the wire.
    published_results: usize = 0,
    /// Representative inbound answer id retained for a redacted timeout event
    /// after result-path close has detached all answer bookkeeping.
    timeout_answer_id: ?u32 = null,
    result_refs: usize = 0,
    accept_live: bool = false,
    network_live: bool = true,
    /// Forced cleanup (Finish, expiry, Accept-host close, owner teardown) has
    /// made the provision unusable. A successful early Accept retires the
    /// network provision without setting this bit, so remaining pre-reserved
    /// JoinResult Returns may still complete coherently.
    cancelled: bool = false,
    /// True while `hosted_joins` exposes the live hosted-provision record.
    /// Network retirement clears this before invoking the captured callback;
    /// result-answer records can keep the allocation alive independently.
    owner_record_live: bool = false,
    bytes_charged: bool = false,
    operation_depth: u32 = 0,
    /// Hosted-Accept phase deadline, sampled in `accept_peer`'s clock domain.
    deadline_ns: ?i64 = null,
};

const PendingJoinAccept = struct {
    hosted: *HostedJoin,
    target: ProvideTarget,
};

/// Answer-ID tombstone used while a complete Join bucket is detached but the
/// application JoinNetwork callback has not yet produced a canonical
/// HostedJoin. Finish marks the tombstone without freeing it, so reentrant ID
/// reuse remains rejected until completion has coherently settled every part.
const CompletingJoinAnswer = struct {
    /// Completion transitions preserve the detached bucket's aggregate record
    /// footprint. Terminal expiry settlements reserve IDs too, but are already
    /// retired for operability purposes and therefore set this false.
    counts_as_join_record: bool = false,
    finished: bool = false,
    release_result_caps: bool = false,
    /// Set only after the host callback has produced the canonical lease.
    /// Every completing answer takes its result reference before the first
    /// Return, so a synchronous Finish of an early fanout member cannot retire
    /// the provision out from under later members.
    hosted: ?*HostedJoin = null,
    /// A successful JoinResult Return was emitted for this answer.  At the end
    /// of the callback-bearing fanout this tombstone is atomically exchanged
    /// for a steady-state `pending_join_result_answers` record.
    result_sent: bool = false,
};

const PendingJoinResultAnswer = struct {
    hosted: *HostedJoin,
    /// True immediately before the corresponding Return send begins. This
    /// lets an error unwind retire only still-unsent reservations while
    /// preserving answers whose terminal Return is already observable.
    published: bool = false,
};

const JoinAcceptHostLink = struct {
    hosted: *HostedJoin,
};

const JoinCoordinatorAcceptLink = struct {
    coordinator: *JoinCoordinator,
};

const JoinCoordinatorResultLink = struct {
    coordinator: *JoinCoordinator,
    question_id: u32,
};

/// Reason sent when refusing an inbound Call whose results were redirected to a
/// third vat that this peer cannot contact.
const third_party_results_unsupported =
    "sendResultsTo.thirdParty unsupported: this vat cannot route results to a third party";

/// Reason sent to calls pipelined on an answer whose results went to a third
/// vat: this peer never observes those results, so it cannot resolve a
/// promised-answer target against them.
const automatic_third_party_target_canceled =
    "automatic third-party result recipient canceled before delivery";

pub const Peer = struct {
    allocator: std.mem.Allocator,
    limits: PeerLimits,

    // -- Transport binding --------------------------------------------------

    /// Attached transport callbacks. Set/cleared atomically by
    /// `attachTransportBinding` / `detachTransport`.
    transport: TransportBinding = .{},

    /// Optional Level-3 three-party addressing seam. Null for a plain two-party
    /// peer; set via `attachVatNetwork` before originating `Provide`/`Accept`
    /// handoffs. Borrowed — its `ctx` must outlive the peer.
    vat_network: ?VatNetwork = null,
    third_party_result_policy: ThirdPartyResultPolicy = .reject,

    /// Optional Experimental Level-4 Join addressing seam. When present, inbound
    /// `Join` completion returns Zig `JoinResult` payloads and registers a
    /// one-shot direct `Accept` provision instead of returning the cap directly.
    /// Borrowed — its `ctx` must outlive the peer.
    join_network: ?JoinNetwork = null,

    /// Optional Level-3 recipient auto-pickup handler. When this peer holds a
    /// promise import and receives a `thirdPartyHosted` Resolve for it AND a
    /// `vat_network` is attached, the runtime connects to the third vat, sends an
    /// `Accept`, and — when the accepted capability's `Return` arrives — invokes
    /// this handler with the direct cap (see `HandoffPickupCallback`). Null (or a
    /// null `vat_network`) keeps the Level-1/2 proxy-via-vine fallback: the
    /// promise resolves to the vine import and no pickup is attempted. Set via
    /// `setHandoffPickupHandler`. `handoff_pickup_ctx` is borrowed and must
    /// outlive the peer.
    handoff_pickup_ctx: ?*anyopaque = null,
    on_handoff_pickup: ?HandoffPickupCallback = null,

    // -- Capability bookkeeping ---------------------------------------------

    /// Central capability table shared with cap_table helpers.
    caps: cap_table.CapTable,
    /// Exported (local) capabilities offered to the remote peer.
    exports: std.AutoHashMap(u32, ExportEntry),

    // -- Question / answer tracking -----------------------------------------

    /// Outstanding outbound calls (question ID -> callback).
    questions: std.AutoHashMap(u32, Question),
    /// Outbound calls whose remote answers are caller-owned after Return.
    /// Registration happens with question allocation, before a synchronous
    /// transport can deliver the Return callback.
    retained_questions: retained_question_state.Registry,
    /// Wire refs an outstanding outbound Call's params took on local exports
    /// (question ID -> export ids, one per senderHosted/senderPromise
    /// descriptor emitted; duplicates meaningful — each occurrence is one
    /// ref). Consumed by the question's inbound Return: `releaseParamCaps =
    /// true` (the rpc.capnp default, relied on by conforming peers) spends
    /// the refs through `releaseExport`; false drops the record and leaves
    /// the refs for the remote's explicit Release messages.
    question_param_export_refs: std.AutoHashMap(u32, std.ArrayList(u32)),
    /// Cached Return frames for answered questions, kept until Finish.
    resolved_answers: std.AutoHashMap(u32, ResolvedAnswer),
    /// Inbound call question IDs accepted from the remote peer until Return or
    /// Finish. The value is that answer's `owes_param_cap_releases`: TRUE when
    /// the Call's params carried `senderHosted`/`senderPromise` descriptors, so
    /// this vat took wire references on the caller's capabilities and settles
    /// them with explicit `Release` frames (the post-dispatch auto-release, or
    /// an application that kept a param cap and drops it later).
    ///
    /// It is the sole input to `returnReleasesParamCaps`, i.e. to the answering
    /// Return's `releaseParamCaps` flag: rpc.capnp forbids sending separate
    /// `Release` messages once that flag is true, so an answer that owes
    /// Releases must say `false`. Entries are created in `handleCall` and
    /// removed by the Return (`sendReturnFrameWithLoopback`) or the Finish,
    /// which is exactly the window in which the flag can be read.
    active_inbound_questions: std.AutoHashMap(u32, bool),
    /// Answer IDs whose results Return is currently being synchronously delivered
    /// and may receive a reentrant Finish before the resolved answer is committed.
    resolving_answers: std.AutoHashMap(u32, void),
    /// Count of ResolvedAnswerReservation values currently outstanding
    /// (reserved but not yet committed or rolled back). A reservation is held
    /// open across the transport send, and a synchronous transport can deliver
    /// a nested inbound Call/Bootstrap that reserves and commits a DIFFERENT
    /// answer while the outer reservation is pending. Each reserve therefore
    /// ensures map capacity for itself PLUS every outstanding reservation so
    /// the outer infallible commit can never underflow the map's reserved-slot
    /// accounting (getOrPutAssumeCapacity).
    resolved_answer_reservations: u32 = 0,
    /// Inbound question ids whose Finish arrived before their (async) Return.
    /// A late Return for one of these must NOT be recorded in resolved_answers:
    /// no further Finish will ever clear it, and a stale entry poisons legal
    /// reuse of the id (DuplicateQuestionId against a compliant peer). Bounded
    /// by max_active_inbound_questions.
    /// The bool records Finish.releaseResultCaps so the later Return sender can
    /// apply the Finish after delivery. If the sender already reserved a
    /// resolved answer, it commits first to drain queued promised calls, then
    /// immediately removes the recorded answer through normal Finish cleanup.
    finished_early_answers: std.AutoHashMap(u32, bool),
    /// Inbound answers that already returned an EXCEPTION, kept until Finish
    /// (the mirror of `resolved_answers`, which records results only). A call
    /// pipelined on such an answer that arrives AFTER the exception Return
    /// would otherwise queue in `pending_promises` forever — the failed
    /// answer can never replay it; this record answers it with a copy of the
    /// same exception instead. Bounded by max_active_inbound_questions,
    /// best-effort under pressure (a skipped record degrades to the old
    /// queue-forever behavior for that answer, never a crash).
    failed_answers: std.AutoHashMap(u32, FailedAnswer),

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
    /// Attached vat-wide provision index (BORROWED; back-linked via
    /// `ProvisionIndex.attached_peers`). Null = all L3 host behavior is
    /// exactly today's per-peer behavior. Set via `attachProvisionIndex`.
    provision_index: ?*ProvisionIndex = null,
    /// Optional random source for accept-embargo ids (BORROWED ctx; must
    /// outlive the peer). Null = legacy 8-byte per-peer counter ids.
    entropy: ?EntropySource = null,
    /// OWNER side of vat-wide provisions: Provide question id -> provision
    /// object (+1 ref each). Lives BESIDE `provides_by_question` so
    /// `ProvideEntry`'s shape and every generic helper that constructs it stay
    /// untouched.
    provisions_by_question: std.AutoHashMap(u32, *ProvisionIndex.Provision),
    /// HOLDER side: queued/parked cross-peer Accept answer id -> record.
    /// Records are UNCOUNTED back-links: the +1 lives in the provision's
    /// pending slot / parked entry; a record exists IFF its slot/entry exists;
    /// removing a record NEVER releases (invariant INV-REC).
    cross_peer_pending_accepts: std.AutoHashMap(u32, CrossPeerAcceptRecord),
    /// Peer-local subset of vat-wide parked-Accept accounting. These gauges
    /// enforce fair admission across sibling connections and are updated only
    /// through `ProvisionIndex.commit/refundParkAdmission`.
    parked_accept_count: usize = 0,
    parked_accept_bytes: usize = 0,
    /// ORIGINATION (mirror of the inbound provide tables): vine export id ->
    /// the paired held-open Provide question this peer originated. Keyed by the
    /// vine export minted on THIS connection; the Provide question it anchors
    /// may live on a different peer (see `OutboundProvide`). Consulted from
    /// `handleRelease` to Finish the Provide once the recipient drops the vine.
    outbound_provides: std.AutoHashMap(u32, OutboundProvide),
    /// LIVENESS back-links for the cross-peer `outbound_provides` coupling. When
    /// THIS peer holds a held-open Provide question anchored by a vine minted on
    /// ANOTHER (recipient) peer, each such coupling records a `CoupledVine` here
    /// naming that recipient peer + vine id. On `deinit` this peer walks the list
    /// and NULLs its `provide_peer` back-pointer in every recipient's coupling
    /// entry, so a later vine Release can never dereference this freed peer.
    /// Entries are removed as soon as their coupling drains (vine Release, or the
    /// recipient peer's own deinit). Empty for a peer that originated no handoffs.
    coupled_vines: std.ArrayList(CoupledVine),
    /// Async forwarded-vine contexts that borrow this peer as the recipient.
    /// Recipient close/deinit nulls every borrow before this peer can die; the
    /// forwarding peer's question owns and eventually frees each context.
    forward_vine_relay_links: std.ArrayList(*ForwardVineCallContext),
    /// Auto-pickup contexts owned by Accept questions on third-vat peers that
    /// borrow this peer for the promise/vine side of the handoff.
    handoff_pickup_links: std.ArrayList(*HandoffPickupContext),
    /// Back-links from proxy exports on other peers that borrow this peer as
    /// their source. Walked at deinit to null those borrowed pointers before this
    /// peer's memory is freed.
    cross_peer_proxy_links: std.ArrayList(CrossPeerProxyLink),
    /// Back-links for Join relay questions sent through this peer. Walked at
    /// deinit to null owner-peer relay records before this peer is freed.
    cross_peer_join_relay_links: std.ArrayList(CrossPeerJoinRelayLink),
    /// In-progress Join operations collecting parts.
    pending_joins: std.AutoHashMap(u32, JoinState),
    /// Maps a Join answer's question ID to its join ID + part number.
    pending_join_questions: std.AutoHashMap(u32, PendingJoinQuestion),
    /// Completing L4 answer IDs reserved across host callbacks and fan-out.
    completing_join_answers: std.AutoHashMap(u32, CompletingJoinAnswer),
    /// Cross-peer transparent-proxy Join relays keyed by upstream answer id.
    pending_join_relays: std.AutoHashMap(u32, CrossPeerJoinRelay),
    /// Accept messages waiting for a disembargo.
    pending_accepts_by_embargo: std.StringHashMap(std.ArrayList(PendingEmbargoedAccept)),
    /// Maps question IDs to embargo keys for cleanup on Finish.
    pending_accept_embargo_by_question: std.AutoHashMap(u32, []u8),
    /// Canonically-owned completed JoinResult handoffs. The key is also the
    /// value so map membership is an O(1) ownership/refund guard.
    hosted_joins: std.AutoHashMap(*HostedJoin, void),
    /// Experimental L4 JoinResult provisions waiting for a direct Accept. Keys
    /// borrow `HostedJoin.provision`; values own the cloned ProvideTarget.
    pending_join_accepts: std.StringHashMap(PendingJoinAccept),
    /// Experimental L4 JoinResult answer ids -> borrowed canonical owner.
    pending_join_result_answers: std.AutoHashMap(u32, PendingJoinResultAnswer),
    /// Back-links for canonical HostedJoins whose direct Accept lives here.
    join_accept_host_links: std.ArrayList(JoinAcceptHostLink),
    /// Back-links from Experimental JoinCoordinator instances holding an accepted
    /// cap or unfinished Accept answer through this peer.
    join_coordinator_accept_links: std.ArrayList(JoinCoordinatorAcceptLink),
    /// Back-links from JoinCoordinator result records that retain this peer
    /// after the outbound question Return has already been consumed.
    join_coordinator_result_links: std.ArrayList(JoinCoordinatorResultLink),
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
    /// Automatic `.vat_network` redirects owned by this peer, keyed by the
    /// original caller-facing answer id.
    automatic_third_party_routes: std.AutoHashMap(u32, *AutomaticThirdPartyRoute),
    /// Borrowed backlinks for automatic redirects whose synthetic result
    /// answer lives on this peer, keyed by that synthetic answer id.
    incoming_automatic_third_party_routes: std.AutoHashMap(u32, *AutomaticThirdPartyRoute),
    /// Results frames for answered `sendResultsTo=yourself` calls, awaiting the
    /// `takeFromOtherQuestion` redirect that names the local question they feed
    /// (the reflected-loopback / Level-1 tail-call optimization). Keyed by the
    /// answered call's answer id (the forwarder's forwarded-question id); the
    /// value is an owned Return-results frame. Consumed inline when the matching
    /// `takeFromOtherQuestion` arrives (see `handleReturn`); freed at deinit if
    /// that redirect never comes. NOT freed on the answered call's Finish — the
    /// forwarder's `Finish` for the forwarded question can arrive BEFORE the
    /// `takeFromOtherQuestion`, so freeing there would drop the results early.
    /// Only cap-free results are stashed.
    loopback_result_stash: std.AutoHashMap(u32, []u8),

    // -- Persistence (sturdy refs) -------------------------------------------

    /// Save/restore hooks for persistent exports (export ID -> state).
    persistent_exports: std.AutoHashMap(u32, *PersistenceState),
    /// Export currently serving vat-level restore calls (the wrapped
    /// bootstrap export), if a restorer hook is installed.
    restorer_export_id: ?u32 = null,
    /// Total `Persistent.save()` calls served by this peer.
    saves_served: u64 = 0,
    /// Total vat-level restore calls served by this peer.
    restores_served: u64 = 0,

    // -- Counters and scalars -----------------------------------------------

    /// Monotonically increasing question ID counter.
    next_question_id: u32 = 0,
    /// Descending cursor for REFLECTED (loopback) question ids — see
    /// `allocateLoopbackQuestion`. Loopback Call frames are fed back into this
    /// peer's own `handleFrame`, so their id also lands in the inbound answer
    /// namespace, which the REMOTE owns. Every implementation hands out wire
    /// question ids ascending from 0, so drawing loopback ids from the top of
    /// the space keeps the two apart; nothing carrying one of these ids is ever
    /// written to a socket.
    next_loopback_question_id: u32 = std.math.maxInt(u32),
    /// Monotonically increasing embargo ID counter.
    next_embargo_id: u32 = 0,
    /// Monotonically increasing counter for Level-3 three-party handoff
    /// `Accept` embargo ids. Distinct from `next_embargo_id`: an accept embargo
    /// is an opaque BYTE string carried in `Accept.embargo` and matched
    /// byte-for-byte by the capability host (VatC) against a `context.accept`
    /// `Disembargo` (rpc.capnp:870-903), whereas `next_embargo_id` names the
    /// numeric senderLoopback embargo namespace. Keeping them separate avoids
    /// cross-talk between the two ordering mechanisms.
    next_accept_embargo_id: u64 = 0,
    /// Next callee-allocated answer id for an outbound `ThirdPartyAnswer`
    /// (`Peer.sendThirdPartyAnswer`). Per rpc.capnp:936-941 the callee — not the
    /// caller — chooses this id, and it MUST fall in the range [2^30, 2^31):
    /// bit 30 set, bit 31 clear (see `third_party.isThirdPartyAnswerId`). The
    /// counter starts at 2^30 and is masked back into range on wrap so it never
    /// collides with the caller-allocated question-id space (low ids) or the
    /// `onlyPromisePipeline` range [2^31, 2^32).
    next_third_party_answer_id: u32 = third_party_answer_id_base,
    /// Export ID of the bootstrap capability, if set.
    bootstrap_export_id: ?u32 = null,

    // -- Send frame override (for testing) ----------------------------------

    send_frame_ctx: ?*anyopaque = null,
    send_frame_override: ?SendFrameOverride = null,

    // -- Lifecycle callbacks -------------------------------------------------

    callback_ctx: ?*anyopaque = null,
    on_error: ?*const fn (ctx: ?*anyopaque, peer: *Peer, err: anyerror) void = null,
    on_close: ?*const fn (ctx: ?*anyopaque, peer: *Peer) void = null,
    /// Terminal transport-close notification is delivered at most once.
    /// `detachTransport` deliberately does not set this bit: detaching a live
    /// transport is non-terminal and permits rebinding.
    transport_close_notified: bool = false,
    /// Reentrant `deinit()` from a synchronous automatic third-party send or
    /// automatic Call dispatch is deferred until the outer operation unwinds.
    /// Both the source and result peers take this guard while their maps are
    /// borrowed; dispatch depth covers the handler interval between route
    /// publication and terminal result delivery.
    automatic_third_party_operation_depth: u32 = 0,
    automatic_third_party_deinit_deferred: bool = false,
    /// A terminal close observed while an automatic route operation borrows
    /// either peer is completed when the outermost operation unwinds. Unlike
    /// `detachTransport`, this remains terminal and still notifies the user
    /// exactly once.
    automatic_third_party_close_deferred: bool = false,
    /// Encloses only inbound `.vat_network` Call dispatch, so a source deinit
    /// requested from a nested result send runs after the handler and dispatch
    /// error path have stopped borrowing the peer. Ordinary RPC frames retain
    /// their established deinit semantics.
    automatic_third_party_dispatch_depth: u32 = 0,
    /// Keeps Join-owned maps and canonical cross-peer HostedJoin borrows live
    /// across synchronous Return/Finish/network callbacks. Close/deinit use
    /// the same deferred lifecycle path as automatic redirected results.
    join_operation_depth: u32 = 0,
    observer: ?events.Observer = null,

    // -- Time / deadlines -----------------------------------------------------

    /// Custom monotonic clock used for call deadlines and the shutdown
    /// drain bound. The clock's `ctx` pointer must outlive the peer.
    /// Tests inject `rpc.time.TestClock` here.
    clock: ?rpc_time.Clock = null,
    /// `std.Io` used as the monotonic time source when no custom `clock`
    /// is set; see `setClockIo`. Stored by value so peer moves cannot
    /// dangle it. Null (with no custom clock) disables time-based
    /// enforcement entirely.
    clock_io: ?std.Io = null,
    /// Opt-in time-domain policy; see `state.PeerTimeouts`.
    timeouts: PeerTimeouts = .{},
    /// Earliest deadline across partial buckets, relays, and hosted Accepts in
    /// this peer's clock domain. The inbound hot path reads only this field.
    next_join_deadline_ns: ?i64 = null,
    join_sweep_in_progress: bool = false,
    join_clock_sample_in_progress: bool = false,
    /// One record replaces each detached complete Join bucket until canonical
    /// HostedJoin ownership is published or the completion is failed.
    completing_join_records: usize = 0,
    /// O(1) counted subset of `completing_join_answers`; terminal settlement
    /// tombstones intentionally remain invisible to gauges/pressure admission.
    completing_join_answer_records: usize = 0,
    /// Captured JoinNetwork callbacks in flight. Not an operability gauge, but
    /// attach/detach treats every borrow as dependent state even after the
    /// source bucket has been detached.
    join_network_borrows: usize = 0,
    /// Provision bytes charged exactly once by canonical HostedJoin owners.
    join_accept_bytes: usize = 0,
    /// Absolute drain bound stamped by `shutdown()` when a drain timeout is
    /// configured. Enforced by `checkDeadlines`.
    shutdown_deadline_ns: ?i64 = null,

    // -- Validation-work rate limit -----------------------------------------

    /// Token-bucket state for the per-connection validation-work budget (see
    /// PeerLimits.max_validation_words_per_second). `validation_tokens` counts
    /// available traversal-words; it starts full on the first charged frame and
    /// refills by elapsed time. Inert until a Clock is configured.
    validation_tokens: i64 = 0,
    validation_last_refill_ns: ?i64 = null,

    // -- Diagnostics --------------------------------------------------------

    /// Tag of the most recently decoded inbound message (for debugging).
    last_inbound_tag: ?protocol.MessageTag = null,
    /// Reason string from the most recent remote Abort message, if any.
    last_remote_abort_reason: ?[]u8 = null,

    // -- Graceful shutdown ---------------------------------------------------

    /// When true, no new outbound calls are accepted and the peer drains
    /// in-flight questions before invoking `shutdown_callback`.
    is_shutting_down: bool = false,
    /// True while `deinit` runs. Suppresses `completeShutdown` (which touches
    /// the transport and fires the shutdown callback) when the terminal
    /// question pass drains the last question of a mid-shutdown peer —
    /// deinit itself IS the completion, and the transport may already be
    /// gone.
    in_deinit: bool = false,
    /// Prevent nested transport delivery from recursively retrying the same
    /// deferred Finish batch.
    finish_maintenance_in_progress: bool = false,
    /// Optional callback fired once all outstanding questions have been
    /// answered and the shutdown sequence completes.
    shutdown_callback: ?*const fn (peer: *Peer) void = null,

    // -- Thread-affinity check (debug only) ---------------------------------

    /// Thread ID captured at init time. In debug builds, key entry points
    /// assert that the current thread matches this value. Initialized to
    /// null and set to the real thread ID in `initDetached`.
    owner_thread_id: ?state.OwnerThreadId = null,

    /// When true, thread-affinity checks also run in release builds (they
    /// are always on in Debug). See `enableRuntimeThreadChecks`.
    runtime_thread_checks: bool = false,

    /// Disable the thread-affinity check so that any thread may call
    /// methods on this peer. This is intended for use by the WASM ABI
    /// layer where a global mutex provides synchronization and the peer
    /// may be accessed from different host threads.
    pub fn disableThreadAffinity(self: *Peer) void {
        self.owner_thread_id = null;
    }

    /// Re-capture thread affinity on the current thread. Legal only at a
    /// quiescent handoff point — no other thread may touch this peer
    /// concurrently (e.g. `ClientSession.run()` adopting a session that
    /// was connected on another thread before its run loop started).
    pub fn adoptOwnerThread(self: *Peer) void {
        self.owner_thread_id = state.initialOwnerThreadId();
    }

    /// Opt in to thread-affinity checking in release builds.
    ///
    /// `Peer` is single-threaded by contract: every method must be called
    /// from the owning thread (or under external synchronization with
    /// `disableThreadAffinity`). Debug builds always enforce this with a
    /// panic; release builds skip the check by default for performance.
    /// Deployments that can afford one thread-id read per entry point
    /// should enable this — a violated affinity contract is a data race,
    /// and panicking beats corrupting protocol state.
    pub fn enableRuntimeThreadChecks(self: *Peer, enabled: bool) void {
        self.runtime_thread_checks = enabled;
    }

    /// Assert that the caller is on the thread that created this peer.
    /// Always enforced in Debug builds; enforced in release builds only
    /// after `enableRuntimeThreadChecks(true)`. Panics on violation.
    pub fn assertThreadAffinity(self: *const Peer) void {
        state.assertThreadAffinity(self.owner_thread_id, self.runtime_thread_checks);
    }

    /// Canonical `Peer` constructor: create a peer and immediately attach it
    /// to a connection/transport. This is the single supported entry point for
    /// normal RPC use — `ClientSession.connect` and `ServerSession.accept` both
    /// build their peer this way. Prefer it unless you have a specific reason
    /// to construct a detached peer (see `initDetached`).
    ///
    /// Under the hood this is `initDetached` followed by `attachConnection`;
    /// the two-step form exists only for advanced/embedded scenarios.
    pub fn init(allocator: std.mem.Allocator, conn: anytype) Peer {
        var peer = initDetached(allocator);
        peer.attachConnection(conn);
        return peer;
    }

    /// Advanced/internal: create a peer WITHOUT an attached transport.
    ///
    /// Most callers want the canonical `init`, which attaches a connection in
    /// one step. Reach for `initDetached` only when there is no connection to
    /// attach yet — WASM, unit tests, or manual frame injection via
    /// `handleFrame` and `setSendFrameOverride`. Pair it with a later
    /// `attachConnection` (or `attachTransport*`) before driving real traffic.
    /// The provide-target type, re-exported for generics that are parameterized
    /// over the peer type (vat/provisions.zig) without importing peer state.
    pub const ProvideTargetType = ProvideTarget;

    pub fn initDetached(allocator: std.mem.Allocator) Peer {
        return initDetachedWithLimits(allocator, .{});
    }

    /// Advanced/internal: like `initDetached`, but with explicit resource
    /// limits instead of the defaults.
    ///
    /// Prefer the canonical `init` and set limits afterward via `setLimits`
    /// when you can; this variant exists to construct a detached peer with a
    /// non-default `PeerLimits` in one call. Not the normal-use entry point.
    pub fn initDetachedWithLimits(allocator: std.mem.Allocator, limits: PeerLimits) Peer {
        return .{
            .allocator = allocator,
            .limits = limits,
            .owner_thread_id = state.initialOwnerThreadId(),
            .caps = cap_table.CapTable.init(allocator),
            .exports = std.AutoHashMap(u32, ExportEntry).init(allocator),
            .questions = std.AutoHashMap(u32, Question).init(allocator),
            .retained_questions = retained_question_state.Registry.init(allocator),
            .question_param_export_refs = std.AutoHashMap(u32, std.ArrayList(u32)).init(allocator),
            .resolved_answers = std.AutoHashMap(u32, ResolvedAnswer).init(allocator),
            .active_inbound_questions = std.AutoHashMap(u32, bool).init(allocator),
            .resolving_answers = std.AutoHashMap(u32, void).init(allocator),
            .finished_early_answers = std.AutoHashMap(u32, bool).init(allocator),
            .failed_answers = std.AutoHashMap(u32, FailedAnswer).init(allocator),
            .pending_promises = std.AutoHashMap(u32, std.ArrayList(PendingCall)).init(allocator),
            .pending_export_promises = std.AutoHashMap(u32, std.ArrayList(PendingCall)).init(allocator),
            .forwarded_questions = std.AutoHashMap(u32, u32).init(allocator),
            .forwarded_tail_questions = std.AutoHashMap(u32, u32).init(allocator),
            .provides_by_question = std.AutoHashMap(u32, ProvideEntry).init(allocator),
            .provides_by_key = std.StringHashMap(u32).init(allocator),
            .provisions_by_question = std.AutoHashMap(u32, *ProvisionIndex.Provision).init(allocator),
            .cross_peer_pending_accepts = std.AutoHashMap(u32, CrossPeerAcceptRecord).init(allocator),
            .outbound_provides = std.AutoHashMap(u32, OutboundProvide).init(allocator),
            .coupled_vines = .empty,
            .forward_vine_relay_links = .empty,
            .handoff_pickup_links = .empty,
            .cross_peer_proxy_links = .empty,
            .cross_peer_join_relay_links = .empty,
            .pending_joins = std.AutoHashMap(u32, JoinState).init(allocator),
            .pending_join_questions = std.AutoHashMap(u32, PendingJoinQuestion).init(allocator),
            .completing_join_answers = std.AutoHashMap(u32, CompletingJoinAnswer).init(allocator),
            .pending_join_relays = std.AutoHashMap(u32, CrossPeerJoinRelay).init(allocator),
            .pending_accepts_by_embargo = std.StringHashMap(std.ArrayList(PendingEmbargoedAccept)).init(allocator),
            .pending_accept_embargo_by_question = std.AutoHashMap(u32, []u8).init(allocator),
            .hosted_joins = std.AutoHashMap(*HostedJoin, void).init(allocator),
            .pending_join_accepts = std.StringHashMap(PendingJoinAccept).init(allocator),
            .pending_join_result_answers = std.AutoHashMap(u32, PendingJoinResultAnswer).init(allocator),
            .join_accept_host_links = .empty,
            .join_coordinator_accept_links = .empty,
            .join_coordinator_result_links = .empty,
            .pending_third_party_awaits = std.StringHashMap(PendingThirdPartyAwait).init(allocator),
            .pending_third_party_answers = std.StringHashMap(u32).init(allocator),
            .pending_third_party_returns = std.AutoHashMap(u32, []u8).init(allocator),
            .adopted_third_party_answers = std.AutoHashMap(u32, u32).init(allocator),
            .resolved_imports = std.AutoHashMap(u32, ResolvedImport).init(allocator),
            .pending_embargoes = std.AutoHashMap(u32, u32).init(allocator),
            .loopback_questions = std.AutoHashMap(u32, void).init(allocator),
            .send_results_to_yourself = std.AutoHashMap(u32, void).init(allocator),
            .send_results_to_third_party = std.AutoHashMap(u32, ?[]u8).init(allocator),
            .automatic_third_party_routes = std.AutoHashMap(u32, *AutomaticThirdPartyRoute).init(allocator),
            .incoming_automatic_third_party_routes = std.AutoHashMap(u32, *AutomaticThirdPartyRoute).init(allocator),
            .loopback_result_stash = std.AutoHashMap(u32, []u8).init(allocator),
            .persistent_exports = std.AutoHashMap(u32, *PersistenceState).init(allocator),
        };
    }

    /// Replace peer resource limits. Existing state is not evicted; future
    /// insertions fail until usage is back under the configured limits.
    pub fn setLimits(self: *Peer, limits: PeerLimits) void {
        self.assertThreadAffinity();
        self.limits = limits;
    }

    /// Install a custom monotonic clock used for deadlines, overriding any
    /// connection-derived time source. The clock's `ctx` must outlive the
    /// peer. Pass `null` to fall back to the connection-derived source
    /// (or disable enforcement when there is none).
    pub fn setClock(self: *Peer, clock: ?rpc_time.Clock) void {
        self.assertThreadAffinity();
        self.clock = clock;
    }

    /// Provide the `std.Io` whose monotonic clock backs deadline features.
    /// `WorkerPool` sets this on accepted peers automatically; embedders
    /// that construct peers directly should call this with the same io the
    /// transport uses. A custom `setClock` overrides this source.
    pub fn setClockIo(self: *Peer, io: ?std.Io) void {
        self.assertThreadAffinity();
        self.clock_io = io;
    }

    /// Current monotonic time in nanoseconds, or null when the peer has no
    /// time source (no custom clock and no io provided via `setClockIo`).
    /// All deadline features are inert in the null case.
    fn clockNow(self: *const Peer) ?i64 {
        if (self.clock) |clock| return clock.now();
        if (self.clock_io) |io| {
            return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
        }
        return null;
    }

    /// Replace the peer's time-domain policy. Affects questions allocated
    /// after this call; existing question deadlines are unchanged.
    pub fn setTimeouts(self: *Peer, timeouts: PeerTimeouts) void {
        self.assertThreadAffinity();
        self.timeouts = timeouts;
    }

    fn newJoinDeadline(self: *Peer) !?i64 {
        const ttl_ms = self.timeouts.join_timeout_ms orelse return null;
        // A nested Join must never turn a configured TTL into an implicit
        // opt-out merely because an application Clock callback re-entered the
        // peer. The nested admission is refused generically by its caller.
        if (self.join_clock_sample_in_progress) return error.JoinClockReentrant;
        self.join_clock_sample_in_progress = true;
        defer self.join_clock_sample_in_progress = false;
        const now = self.clockNow() orelse return null;
        const sum = @as(i128, now) + @as(i128, ttl_ms) * std.time.ns_per_ms;
        return @intCast(std.math.clamp(sum, std.math.minInt(i64), std.math.maxInt(i64)));
    }

    fn noteJoinDeadline(self: *Peer, deadline_ns: ?i64) void {
        const deadline = deadline_ns orelse return;
        if (self.next_join_deadline_ns) |next| {
            if (deadline >= next) return;
        }
        self.next_join_deadline_ns = deadline;
    }

    fn refreshNextJoinDeadline(self: *Peer) void {
        var next: ?i64 = null;
        var join_it = self.pending_joins.valueIterator();
        while (join_it.next()) |join_state| noteEarliestDeadline(&next, join_state.deadline_ns);
        var relay_it = self.pending_join_relays.valueIterator();
        while (relay_it.next()) |relay| noteEarliestDeadline(&next, relay.deadline_ns);
        var accept_it = self.pending_join_accepts.valueIterator();
        while (accept_it.next()) |accept| noteEarliestDeadline(&next, accept.hosted.deadline_ns);
        self.next_join_deadline_ns = next;
    }

    fn noteEarliestDeadline(next: *?i64, candidate: ?i64) void {
        const deadline = candidate orelse return;
        if (next.*) |current| {
            if (deadline >= current) return;
        }
        next.* = deadline;
    }

    fn sampleJoinSweepNow(self: *Peer) ?i64 {
        if (self.join_sweep_in_progress or self.join_clock_sample_in_progress) return null;
        const deadline = self.next_join_deadline_ns orelse return null;
        self.join_clock_sample_in_progress = true;
        const now_opt = self.clockNow();
        self.join_clock_sample_in_progress = false;
        const now = now_opt orelse return null;
        if (now < deadline) return null;
        return now;
    }

    /// Detach every due L4 Join phase. The earliest-deadline cache makes the
    /// common inbound-frame/tick path O(1); a due pass is allocation-free and
    /// detaches records before any Return, Finish, or network callback can
    /// re-enter. Returns the number of phase records detached (buckets, relays,
    /// or hosted Accept provisions), not the number of outbound calls canceled.
    pub fn sweepExpiredJoins(self: *Peer) usize {
        self.assertThreadAffinity();
        self.enterJoinOperation();
        defer self.leaveJoinOperation();
        const now = self.sampleJoinSweepNow() orelse return 0;
        self.join_sweep_in_progress = true;
        defer {
            self.join_sweep_in_progress = false;
            self.refreshNextJoinDeadline();
        }

        var detached: usize = 0;
        while (true) {
            var expired_id: ?u32 = null;
            var it = self.pending_joins.iterator();
            while (it.next()) |entry| {
                const deadline = entry.value_ptr.deadline_ns orelse continue;
                if (now >= deadline) {
                    expired_id = entry.key_ptr.*;
                    break;
                }
            }
            const join_id = expired_id orelse break;
            const removed = self.pending_joins.fetchRemove(join_id) orelse continue;
            var join_state = removed.value;
            // Admission pre-reserved this allocation-free settlement table.
            // Exchange the bucket+part footprint for one transition record
            // plus one tombstone per answer before the first observer or send.
            var part_it = join_state.parts.iterator();
            while (part_it.next()) |entry| {
                self.putCompletingJoinAnswerAssumeCapacity(entry.value_ptr.question_id, false);
                _ = self.pending_join_questions.remove(entry.value_ptr.question_id);
            }
            part_it = join_state.parts.iterator();
            while (part_it.next()) |entry| {
                events.emitJoinTimeout(self.observer, entry.value_ptr.question_id);
                self.sendReturnException(entry.value_ptr.question_id, "join unavailable") catch |err| {
                    log.debug("expired Join exception send failed for answer {}: {}", .{ entry.value_ptr.question_id, err });
                };
            }
            part_it = join_state.parts.iterator();
            while (part_it.next()) |entry| {
                _ = self.removeCompletingJoinAnswer(entry.value_ptr.question_id);
                _ = self.finished_early_answers.remove(entry.value_ptr.question_id);
            }
            JoinState.deinit(&join_state, self.allocator);
            detached += 1;
        }

        while (true) {
            var expired_id: ?u32 = null;
            var it = self.pending_join_relays.iterator();
            while (it.next()) |entry| {
                const deadline = entry.value_ptr.deadline_ns orelse continue;
                if (now >= deadline) {
                    expired_id = entry.key_ptr.*;
                    break;
                }
            }
            const answer_id = expired_id orelse break;
            if (self.retirePendingJoinRelayTerminal(answer_id, true, true)) detached += 1;
        }

        while (true) {
            var expired: ?*HostedJoin = null;
            var it = self.pending_join_accepts.valueIterator();
            while (it.next()) |accept| {
                const deadline = accept.hosted.deadline_ns orelse continue;
                if (now >= deadline) {
                    expired = accept.hosted;
                    break;
                }
            }
            const hosted = expired orelse break;
            const owner = hosted.owner_peer;
            const observer = owner.observer;
            const timeout_answer_id = hosted.timeout_answer_id;
            if (owner != self) owner.enterJoinOperation();
            defer if (owner != self) owner.leaveJoinOperation();
            // Canonical maps, counters, and backlinks retire before observer or
            // network callbacks. The operation guard keeps `owner` alive while
            // the captured observer is notified after retirement.
            owner.cancelHostedJoin(hosted);
            if (timeout_answer_id) |answer_id| events.emitJoinTimeout(observer, answer_id);
            detached += 1;
        }
        return detached;
    }

    /// Set or replace the deadline on an outstanding question, overriding
    /// any default stamped at send time. Requires a time source.
    pub fn setQuestionDeadline(self: *Peer, question_id: u32, timeout_ms: u64) !void {
        self.assertThreadAffinity();
        const now = self.clockNow() orelse return error.NoClockConfigured;
        const logical_question_id = if (self.retained_questions.contains(question_id))
            question_id
        else
            self.retained_questions.logicalQuestionIdForWire(question_id) orelse question_id;
        const wire_answer_id = if (self.retained_questions.get(logical_question_id)) |retained|
            retained.wire_answer_id
        else
            question_id;
        const entry = self.questions.getPtr(wire_answer_id) orelse return error.UnknownQuestion;
        if (entry.cancelled) return error.QuestionCancelled;
        entry.deadline_ns = now + msToNs(timeout_ms);
    }

    /// Remove the deadline from an outstanding question.
    pub fn clearQuestionDeadline(self: *Peer, question_id: u32) !void {
        self.assertThreadAffinity();
        const wire_answer_id = if (self.retained_questions.get(question_id)) |retained|
            retained.wire_answer_id
        else
            question_id;
        const entry = self.questions.getPtr(wire_answer_id) orelse return error.UnknownQuestion;
        entry.deadline_ns = null;
    }

    /// Canonical transport-attach: bind a typed connection to this peer,
    /// wiring up transport callbacks. This is the single supported way to give
    /// a peer a transport — `Peer.init` calls it, and it is what
    /// `ClientSession`/`ServerSession` rely on. Prefer it whenever you have a
    /// connection object (e.g. `transport.tcp.Connection`).
    ///
    /// The lower-level `attachTransport` / `attachTransportBinding` seams exist
    /// for building brand-new transports and are not the normal-use path.
    ///
    /// Asserts that no transport is already attached. Call `detachConnection`
    /// first if you need to replace an existing transport.
    pub fn attachConnection(self: *Peer, conn: anytype) void {
        self.assertThreadAffinity();
        if (self.hasAttachedTransport()) {
            @panic("attachConnection called while a transport is already attached; call detachConnection first");
        }
        const ConnPtr = @TypeOf(conn);
        comptime {
            const info = @typeInfo(ConnPtr);
            if (info != .pointer) @compileError("attachConnection expects a pointer type");
        }

        self.attachTransportBinding(
            peer_transport.callbacks.bindingForConnection(
                Peer,
                ConnPtr,
                conn,
                Peer.handleFrame,
                onConnectionError,
                onConnectionClose,
            ),
        );

        // Connections with a tick cadence drive the deadline sweep. Note:
        // no time source is captured here — tests attach partially
        // initialized connections, so the io must be provided explicitly
        // via `setClockIo` (or `setClock`).
        const Conn = @typeInfo(ConnPtr).pointer.child;
        if (comptime @hasField(Conn, "on_tick")) {
            conn.on_tick = peer_transport.callbacks.onConnectionTickFor(Peer, ConnPtr);
        }
    }

    /// Detach the connection without closing it. The transport is unbound
    /// but the underlying connection remains open for potential reuse.
    pub fn detachConnection(self: *Peer) void {
        self.assertThreadAffinity();
        self.detachTransport();
    }

    /// Advanced: attach raw transport callbacks (start, send, close,
    /// isClosing) to this peer.
    ///
    /// Prefer the canonical `attachConnection` when you have a typed connection
    /// object — it wires these callbacks for you. Use `attachTransport` only
    /// when bridging a transport that is not a `Connection`-shaped object and
    /// you must supply each callback individually. Panics if a transport is
    /// already attached.
    pub fn attachTransport(
        self: *Peer,
        ctx: *anyopaque,
        start_fn: ?TransportStartFn,
        send_fn: ?TransportSendFn,
        close_fn: ?TransportCloseFn,
        is_closing: ?TransportIsClosingFn,
    ) void {
        self.attachTransportBinding(.init(ctx, start_fn, send_fn, close_fn, is_closing));
    }

    /// Advanced: attach a named transport binding to this peer.
    ///
    /// Prefer the canonical `attachConnection` for normal use. This is the
    /// preferred low-level boundary when *building a new transport*: adapt the
    /// transport into a `TransportBinding`, then attach the binding here.
    /// `attachConnection` and `attachTransport` are thin wrappers over it.
    pub fn attachTransportBinding(self: *Peer, binding: TransportBinding) void {
        self.assertThreadAffinity();
        if (self.hasAttachedTransport()) {
            @panic("attachTransportBinding called while a transport is already attached; call detachTransport first");
        }
        self.transport = binding;
    }

    /// Detach the transport without closing it, clearing all transport callbacks.
    pub fn detachTransport(self: *Peer) void {
        self.assertThreadAffinity();
        peer_transport.state.detachTransportForPeer(Peer, self);
    }

    /// Callee policy for an inbound `Call` carrying `sendResultsTo = thirdParty`.
    ///
    /// That field asks the callee to connect to a third vat, deliver the results
    /// there, and answer its immediate caller with `resultsSentElsewhere`. It is
    /// a Level-3 feature, a callee is not required to implement it, and neither
    /// reference implementation does: go-capnp echoes `Unimplemented` and drops
    /// the call before touching its answer table, and the C++ stack fails the
    /// requirement and aborts the connection.
    pub const ThirdPartyResultPolicy = enum {
        /// Default. Answer with a single exception `Return` and never dispatch
        /// the call.
        reject,
        /// The application performs the redirect itself: the call dispatches
        /// normally, and the handler is responsible for delivering results to
        /// the third vat and then settling this answer with
        /// `sendReturnResultsSentElsewhere`.
        application,
        /// Experimental automatic redirect. The peer resolves the call's
        /// `ThirdPartyToContact` through its attached `VatNetwork`, registers a
        /// synthetic answer on the introduced peer, and delivers the handler's
        /// Return there before answering the immediate caller with
        /// `resultsSentElsewhere`.
        vat_network,
    };

    /// Select how inbound `sendResultsTo = thirdParty` calls are handled. Per
    /// `Peer`, not per call: in the canonical handoff topology only the callee's
    /// peer facing the introducer needs `.application` or `.vat_network`.
    pub fn setThirdPartyResultPolicy(self: *Peer, policy: ThirdPartyResultPolicy) void {
        self.assertThreadAffinity();
        self.third_party_result_policy = policy;
    }

    /// Attach the Level-3 three-party addressing seam. Required before
    /// originating `Provide`/`Accept` handoffs (`sendProvide`/`sendAccept`) or
    /// selecting automatic `.vat_network` redirected results; a plain
    /// two-party peer never needs one. The network's `ctx` must outlive the
    /// peer.
    pub fn attachVatNetwork(self: *Peer, network: VatNetwork) void {
        self.assertThreadAffinity();
        self.vat_network = network;
    }

    /// Detach the vat network. Does not tear down any in-flight handoffs.
    pub fn detachVatNetwork(self: *Peer) void {
        self.assertThreadAffinity();
        self.vat_network = null;
    }

    /// Attach the Experimental Level-4 Join network seam. When attached, inbound
    /// Join completion uses Zig `JoinResult` payloads plus a direct follow-up
    /// `Accept`; when absent, the legacy raw Join pilot returns the cap directly.
    pub fn attachJoinNetwork(self: *Peer, network: JoinNetwork) !void {
        self.assertThreadAffinity();
        if (self.join_network) |current| {
            if (joinNetworksEqual(current, network)) return;
        }
        if (self.joinNetworkInUse()) return error.JoinNetworkInUse;
        self.join_network = network;
    }

    /// Detach the Experimental L4 Join network only when no partial or hosted
    /// lifecycle still depends on it. This prevents replacing callbacks while
    /// provisions remain live; HostedJoin also captures the creating network
    /// so later cleanup never consults mutable peer configuration.
    pub fn detachJoinNetwork(self: *Peer) !void {
        self.assertThreadAffinity();
        if (self.join_network == null) return;
        if (self.joinNetworkInUse()) return error.JoinNetworkInUse;
        self.join_network = null;
    }

    fn joinNetworksEqual(a: JoinNetwork, b: JoinNetwork) bool {
        return a.ctx == b.ctx and
            a.host_join_result == b.host_join_result and
            a.connect_joined == b.connect_joined and
            a.cancel_host_join_result == b.cancel_host_join_result;
    }

    fn joinNetworkInUse(self: *const Peer) bool {
        return self.join_network_borrows != 0 or
            self.pending_joins.count() != 0 or
            self.hosted_joins.count() != 0;
    }

    fn beginJoinNetworkBorrow(self: *Peer) !JoinNetwork {
        const network = self.join_network orelse return error.NoJoinNetwork;
        self.join_network_borrows = try std.math.add(usize, self.join_network_borrows, 1);
        return network;
    }

    fn endJoinNetworkBorrow(self: *Peer) void {
        std.debug.assert(self.join_network_borrows > 0);
        self.join_network_borrows -= 1;
    }

    /// Install the Level-3 recipient auto-pickup handler. With both a
    /// `vat_network` (see `attachVatNetwork`) and this handler set, an inbound
    /// `thirdPartyHosted` Resolve for a promise this peer holds triggers an
    /// automatic `Accept` against the third vat; the handler receives the direct
    /// capability from the Accept `Return` (see `HandoffPickupCallback`). Without
    /// it, the Level-1/2 proxy-via-vine fallback is used. `ctx` is borrowed and
    /// must outlive the peer.
    pub fn setHandoffPickupHandler(self: *Peer, ctx: *anyopaque, on_pickup: HandoffPickupCallback) void {
        self.assertThreadAffinity();
        self.handoff_pickup_ctx = ctx;
        self.on_handoff_pickup = on_pickup;
    }

    /// Remove the Level-3 recipient auto-pickup handler, reverting to the
    /// Level-1/2 proxy-via-vine fallback for inbound `thirdPartyHosted` Resolves.
    pub fn clearHandoffPickupHandler(self: *Peer) void {
        self.assertThreadAffinity();
        self.handoff_pickup_ctx = null;
        self.on_handoff_pickup = null;
    }

    /// Return whether a transport is currently attached to this peer.
    pub fn hasAttachedTransport(self: *const Peer) bool {
        self.assertThreadAffinity();
        return peer_transport.state.hasAttachedTransportForPeer(Peer, self);
    }

    /// Close the attached transport, signaling the remote peer.
    pub fn closeAttachedTransport(self: *Peer) void {
        self.assertThreadAffinity();
        peer_transport.state.closeAttachedTransportForPeer(Peer, self);
    }

    /// Return whether the attached transport is currently in the process of closing.
    pub fn isAttachedTransportClosing(self: *const Peer) bool {
        self.assertThreadAffinity();
        return peer_transport.state.isAttachedTransportClosingForPeer(Peer, self);
    }

    /// Detach and return the owned connection, cast to `ConnPtr`.
    /// Returns `null` if no transport is attached or the context type does not match.
    pub fn takeAttachedConnection(self: *Peer, comptime ConnPtr: type) ?ConnPtr {
        self.assertThreadAffinity();
        return peer_transport.state.takeAttachedConnectionForPeer(
            Peer,
            ConnPtr,
            self,
            detachTransport,
        );
    }

    /// Return the attached connection cast to `ConnPtr` without detaching it.
    /// Returns `null` if no transport is attached.
    pub fn getAttachedConnection(self: *const Peer, comptime ConnPtr: type) ?ConnPtr {
        self.assertThreadAffinity();
        return peer_transport.state.getAttachedConnectionForPeer(
            Peer,
            ConnPtr,
            self,
        );
    }

    /// Release all owned state: pending calls, resolved answers, export
    /// entries, and the capability table.
    ///
    /// Outstanding questions receive a terminal synthetic "disconnected"
    /// exception Return through their callbacks BEFORE any state is torn
    /// down — deinit of a live peer must not strand callers awaiting a
    /// Return (their heap contexts previously leaked through the
    /// callback-less deinit_ctx sweep). After a transport close this pass
    /// is a no-op: `onConnectionClose` already delivered the terminals.
    pub fn deinit(self: *Peer) void {
        self.assertThreadAffinity();
        if (self.in_deinit) return;
        if (self.automatic_third_party_operation_depth != 0 or
            self.automatic_third_party_dispatch_depth != 0 or
            self.join_operation_depth != 0)
        {
            self.automatic_third_party_deinit_deferred = true;
            self.is_shutting_down = true;
            return;
        }
        self.automatic_third_party_deinit_deferred = false;
        self.in_deinit = true;
        self.is_shutting_down = true;
        // Automatic redirected results borrow peers in both directions. Sever
        // target backlinks first, then retire routes this peer owns, before any
        // callback-bearing question/export teardown can destroy a sibling.
        self.neutralizeAutomaticThirdPartyRoutesOnTargetPeer();
        self.neutralizeAutomaticThirdPartyRoutesOnSourcePeer();
        // LIVENESS (BUG #55): this peer may host held-open Provide questions that
        // recipient peers' `outbound_provides` entries borrow a `provide_peer`
        // pointer back to. `forceCancelAllQuestions` below removes and Finishes
        // those Provide questions, but the borrowed pointer to THIS peer would
        // still dangle. Neutralize it here — null every recipient's back-pointer
        // BEFORE this peer's memory is freed — so a later vine Release is a safe
        // no-op instead of a freed-peer deref. Must run before we return; running
        // it first keeps it independent of any error path below.
        // Vat-wide provision teardown, split per the canonical procedure:
        // infallible neutralize (sever the index back-link, move the owner map
        // out, mark provisions closed, clear holder records) BEFORE
        // forceCancelAllQuestions; the send-bearing drain AFTER it.
        self.detachCrossPeerAcceptsOnHolderPeer();
        self.drainOutboundProvidesOnRecipientPeer();
        var provision_teardown = self.neutralizeProvisionsOnOwnerPeer();
        self.neutralizeCoupledVinesOnProvidePeer();
        self.neutralizeForwardVineRelaysOnRecipientPeer();
        self.neutralizeHandoffPickupsOnPromisePeer();
        self.neutralizeCrossPeerProxiesOnSourcePeer();
        self.neutralizeCrossPeerJoinRelaysOnSourcePeer();
        self.cancelJoinAcceptHostLinks();
        self.cancelAllHostedJoins();
        _ = self.forceCancelAllQuestions(disconnected_reason, .disconnected);
        self.neutralizeJoinCoordinatorResultLinks();
        self.neutralizeJoinCoordinatorAcceptLinks();
        self.drainClosedProvisionsOnOwnerPeer(&provision_teardown);
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
        // NOTE: deinit_ctx callbacks must NOT access peer maps (loopback_questions,
        // forwarded_questions, etc.) as they may be deinited after this loop.
        {
            var q_it = self.questions.valueIterator();
            while (q_it.next()) |q| {
                if (q.deinit_ctx) |deinit_ctx| deinit_ctx(self.allocator, q.ctx);
            }
        }
        // Questions parked in pending_third_party_awaits were moved out of the
        // questions map, so the loop above never sees them. Invoke their
        // deinit_ctx too (heap Save/Restore/ForwardCallContext), or a teardown
        // with in-flight three-party handoffs leaks them. The owned map keys are
        // freed by deinitOwnedStringKeyMap below.
        {
            var await_it = self.pending_third_party_awaits.valueIterator();
            while (await_it.next()) |pending_await| {
                if (pending_await.question.deinit_ctx) |deinit_ctx| {
                    deinit_ctx(self.allocator, pending_await.question.ctx);
                }
            }
        }
        self.questions.deinit();
        self.retained_questions.deinit();
        // forceCancelAllQuestions above already freed the param-export record
        // of every question still in the map; sweep any stragglers (there
        // should be none — records never outlive their question) so a
        // bookkeeping bug degrades to a counter leak, not a memory leak.
        {
            var rec_it = self.question_param_export_refs.valueIterator();
            while (rec_it.next()) |ids| ids.deinit(self.allocator);
        }
        self.question_param_export_refs.deinit();
        self.active_inbound_questions.deinit();
        self.resolving_answers.deinit();
        self.finished_early_answers.deinit();
        {
            var f_it = self.failed_answers.valueIterator();
            while (f_it.next()) |failed| self.allocator.free(failed.reason);
        }
        self.failed_answers.deinit();
        {
            var p_it = self.persistent_exports.valueIterator();
            while (p_it.next()) |st| self.allocator.destroy(st.*);
        }
        self.persistent_exports.deinit();
        {
            var e_it = self.exports.valueIterator();
            while (e_it.next()) |entry| {
                if (entry.deinit_ctx) |deinit_ctx| {
                    if (entry.handler) |handler| deinit_ctx(self.allocator, handler.ctx);
                }
            }
        }
        self.exports.deinit();
        self.forwarded_questions.deinit();
        self.forwarded_tail_questions.deinit();
        peer_cleanup.deinitProvideEntryMap(
            @TypeOf(self.provides_by_question),
            self.allocator,
            &self.provides_by_question,
        );
        self.provides_by_key.deinit();
        // The FRESH (post-neutralize) vat-provision maps: contents were moved
        // out and drained above; only the backing stores remain.
        self.provisions_by_question.deinit();
        self.cross_peer_pending_accepts.deinit();
        // Recipient-side couplings were moved out and converted to durable
        // provider Finish requests before any callback-bearing teardown.
        std.debug.assert(self.outbound_provides.count() == 0);
        self.outbound_provides.deinit();
        // Symmetric back-link list (this peer as a provide_peer). Any residual
        // entries were neutralized at the top of deinit; free the backing store.
        self.coupled_vines.deinit(self.allocator);
        self.forward_vine_relay_links.deinit(self.allocator);
        self.handoff_pickup_links.deinit(self.allocator);
        self.cross_peer_proxy_links.deinit(self.allocator);
        self.cross_peer_join_relay_links.deinit(self.allocator);

        peer_cleanup.deinitJoinStateMap(
            @TypeOf(self.pending_joins),
            self.allocator,
            &self.pending_joins,
        );
        self.pending_join_questions.deinit();
        std.debug.assert(self.completing_join_answers.count() == 0);
        std.debug.assert(self.completing_join_records == 0);
        std.debug.assert(self.completing_join_answer_records == 0);
        std.debug.assert(self.join_network_borrows == 0);
        self.completing_join_answers.deinit();
        {
            // Move the entire owner map out, then sever every reciprocal link
            // before the first Finish send can re-enter either peer. The fresh
            // empty map is what callbacks and public stats observe.
            var owned_relays = self.pending_join_relays;
            self.pending_join_relays = std.AutoHashMap(u32, CrossPeerJoinRelay).init(self.allocator);
            var relay_it = owned_relays.iterator();
            while (relay_it.next()) |entry| {
                if (entry.value_ptr.source_peer) |source_peer| {
                    source_peer.deregisterCrossPeerJoinRelay(self, entry.key_ptr.*);
                }
            }
            relay_it = owned_relays.iterator();
            while (relay_it.next()) |entry| {
                if (entry.value_ptr.source_peer) |source_peer| {
                    var guards = JoinOperationGuards{};
                    guards.add(self);
                    guards.add(source_peer);
                    guards.enter();
                    source_peer.sendJoinRelayFinishAndNeutralize(entry.value_ptr.source_question_id, false) catch |err| {
                        log.debug("cross-peer join relay: failed to finish downstream question {} during deinit: {}", .{
                            entry.value_ptr.source_question_id,
                            err,
                        });
                        source_peer.neutralizeJoinRelayQuestion(entry.value_ptr.source_question_id);
                    };
                    guards.leave();
                }
            }
            owned_relays.deinit();
            self.pending_join_relays.deinit();
        }

        peer_cleanup.deinitOwnedStringKeyListMap(
            @TypeOf(self.pending_accepts_by_embargo),
            self.allocator,
            &self.pending_accepts_by_embargo,
        );
        // Values in pending_accept_embargo_by_question are borrowed from
        // pending_accepts_by_embargo (already freed above), so just deinit.
        self.pending_accept_embargo_by_question.deinit();
        std.debug.assert(self.pending_join_accepts.count() == 0);
        self.pending_join_accepts.deinit();
        std.debug.assert(self.pending_join_result_answers.count() == 0);
        self.pending_join_result_answers.deinit();
        std.debug.assert(self.hosted_joins.count() == 0);
        self.hosted_joins.deinit();
        self.join_accept_host_links.deinit(self.allocator);
        self.join_coordinator_accept_links.deinit(self.allocator);
        self.join_coordinator_result_links.deinit(self.allocator);
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
        {
            var stash_it = self.loopback_result_stash.valueIterator();
            while (stash_it.next()) |frame| self.allocator.free(frame.*);
        }
        self.loopback_result_stash.deinit();
        peer_cleanup.deinitOptionalOwnedBytesMap(
            @TypeOf(self.send_results_to_third_party),
            self.allocator,
            &self.send_results_to_third_party,
        );
        std.debug.assert(self.automatic_third_party_routes.count() == 0);
        std.debug.assert(self.incoming_automatic_third_party_routes.count() == 0);
        self.automatic_third_party_routes.deinit();
        self.incoming_automatic_third_party_routes.deinit();
        peer_cleanup.clearOptionalOwnedBytes(self.allocator, &self.last_remote_abort_reason);
        self.releaseAllImports();
        self.caps.deinit();
    }

    /// Best-effort: send Release messages for all remaining imports so the
    /// remote peer can decrement its export ref counts.
    ///
    /// Errors are logged but not propagated because this runs during `deinit`
    /// when the transport may already be closed or in an error state.
    ///
    /// If the transport send function is null (i.e. the connection was
    /// already destroyed or never attached), we skip sending entirely --
    /// there is no peer to receive the Release messages.
    fn releaseAllImports(self: *Peer) void {
        // If neither a send-frame override nor the transport send function is
        // available, the connection is already gone -- skip sending.
        if (self.send_frame_override == null and self.transport.send == null) {
            log.debug("releaseAllImports: transport not attached, skipping release messages", .{});
            return;
        }
        var it = self.caps.imports.iterator();
        while (it.next()) |entry| {
            // The sweep stays EAGER by design (never routed through the
            // handoff withhold seam), and it settles the whole debt: live
            // wire refs PLUS any `deferred_release` still withheld under an
            // abandoned handoff pin — that tally was never emitted and the
            // table dies right after this walk, so this is its only exit.
            peer_outbound_control.sendReleaseViaSendFrame(
                Peer,
                self,
                entry.key_ptr.*,
                entry.value_ptr.ref_count +| entry.value_ptr.deferred_release,
                Peer.sendFrameControl,
            ) catch |err| {
                log.debug("releaseAllImports: failed to send release for import {}: {}", .{ entry.key_ptr.*, err });
            };
        }
    }

    /// Start the peer, registering error/close callbacks and initiating the
    /// transport (if attached). `cb_ctx` is handed back to both callbacks;
    /// it must outlive the peer.
    pub fn start(
        self: *Peer,
        cb_ctx: ?*anyopaque,
        on_error: ?*const fn (ctx: ?*anyopaque, peer: *Peer, err: anyerror) void,
        on_close: ?*const fn (ctx: ?*anyopaque, peer: *Peer) void,
    ) void {
        self.assertThreadAffinity();
        self.callback_ctx = cb_ctx;
        self.on_error = on_error;
        self.on_close = on_close;
        events.emitConnection(self.observer, .peer, .unknown, .started);
        self.transport.startIfPresent(self);
    }

    /// Install or clear a redacted RPC observer for peer-level dispatch events.
    pub fn setObserver(self: *Peer, observer: ?events.Observer) void {
        self.assertThreadAffinity();
        self.observer = observer;
    }

    /// Set a hook to intercept all outbound frames before they reach the transport.
    /// Pass `null` to clear a previously installed override.
    pub fn setSendFrameOverride(self: *Peer, ctx: ?*anyopaque, callback: ?SendFrameOverride) void {
        self.assertThreadAffinity();
        self.send_frame_ctx = ctx;
        self.send_frame_override = callback;
    }

    /// Return the message tag of the most recently processed inbound message, or `null` if none.
    pub fn getLastInboundTag(self: *const Peer) ?protocol.MessageTag {
        self.assertThreadAffinity();
        return self.last_inbound_tag;
    }

    /// Return the reason string from the last remote Abort message, or `null` if none received.
    pub fn getLastRemoteAbortReason(self: *const Peer) ?[]const u8 {
        self.assertThreadAffinity();
        return self.last_remote_abort_reason;
    }

    /// Point-in-time gauge snapshot of peer protocol state, for metrics
    /// scraping. Cheap: map counts plus one walk of the queued-call
    /// buckets and question table.
    pub const PeerStats = struct {
        /// Outstanding outbound calls, including cancelled entries that
        /// are still absorbing their late Return.
        outbound_questions: u32,
        /// Caller-owned retained answers, including retained calls still
        /// awaiting their terminal Return. Excludes answers whose ownership
        /// has transferred into another protocol lifecycle.
        retained_questions: usize,
        /// Retained source answers currently owned by a coupled protocol
        /// lifecycle and awaiting its exactly-once Finish.
        transferred_retained_questions: usize,
        /// Subset of `outbound_questions` that were cancelled locally.
        cancelled_questions: u32,
        /// Inbound calls accepted and not yet returned/finished.
        active_inbound_questions: u32,
        /// Local capabilities currently exported to the remote.
        exports: u32,
        /// Cached Return frames held for promise pipelining.
        resolved_answers: u32,
        /// Resolved promise imports being tracked.
        resolved_imports: u32,
        /// Calls queued behind unresolved promises.
        pending_queued_calls: usize,
        /// Bytes held by calls queued behind unresolved promises.
        pending_queued_call_bytes: usize,
        /// Accepts parked by this connection in its vat's shared provision
        /// index while they await a matching Provide.
        parked_accepts: usize,
        /// Per-Accept attributable bytes for `parked_accepts`: normalized
        /// recipient-token bytes plus embargo bytes.
        parked_accept_bytes: usize,
        /// Aggregate live L4 Join records (buckets, parts, relays, hosted
        /// provisions, result answers, and direct Accept records).
        join_records: usize,
        /// Parts retained by incomplete inbound Join buckets.
        join_parts: usize,
        /// Provision bytes charged once by canonical HostedJoin owners.
        join_accept_bytes: usize,
        /// Exports with persistence hooks installed.
        persistent_exports: u32,
        /// Total `Persistent.save()` calls served since peer creation.
        saves_served: u64,
        /// Total vat-level restore calls served since peer creation.
        restores_served: u64,
    };

    pub fn stats(self: *const Peer) PeerStats {
        self.assertThreadAffinity();
        var cancelled: u32 = 0;
        var it = self.questions.valueIterator();
        while (it.next()) |q| {
            if (q.cancelled) cancelled += 1;
        }
        const queued = self.pendingQueuedCallStats();
        return .{
            .outbound_questions = self.questions.count(),
            .retained_questions = self.retained_questions.countCallerOwned(),
            .transferred_retained_questions = self.retained_questions.countTransferred(),
            .cancelled_questions = cancelled,
            .active_inbound_questions = self.active_inbound_questions.count(),
            .exports = self.exports.count(),
            .resolved_answers = self.resolved_answers.count(),
            .resolved_imports = self.resolved_imports.count(),
            .pending_queued_calls = queued.calls,
            .pending_queued_call_bytes = queued.bytes,
            .parked_accepts = self.parked_accept_count,
            .parked_accept_bytes = self.parked_accept_bytes,
            .join_records = self.joinRecordCount(),
            .join_parts = self.pending_join_questions.count(),
            .join_accept_bytes = self.join_accept_bytes,
            .persistent_exports = self.persistent_exports.count(),
            .saves_served = self.saves_served,
            .restores_served = self.restores_served,
        };
    }

    const PendingQueuedCallStats = struct {
        calls: usize = 0,
        bytes: usize = 0,
    };

    fn saturatingAdd(a: usize, b: usize) usize {
        return std.math.add(usize, a, b) catch std.math.maxInt(usize);
    }

    fn joinRecordCount(self: *const Peer) usize {
        var total: usize = self.pending_joins.count();
        total = saturatingAdd(total, self.pending_join_questions.count());
        total = saturatingAdd(total, self.completing_join_answer_records);
        total = saturatingAdd(total, self.completing_join_records);
        total = saturatingAdd(total, self.pending_join_relays.count());
        total = saturatingAdd(total, self.hosted_joins.count());
        total = saturatingAdd(total, self.pending_join_accepts.count());
        total = saturatingAdd(total, self.pending_join_result_answers.count());
        return total;
    }

    fn ensureJoinRecordCapacity(self: *const Peer, additional: usize) !void {
        const current = self.joinRecordCount();
        if (current > self.limits.max_pending_join_records) return error.JoinRecordLimitExceeded;
        if (additional > self.limits.max_pending_join_records - current) {
            return error.JoinRecordLimitExceeded;
        }
    }

    pub fn ensureCountLimit(found_existing: bool, current_count: usize, max_count: usize) !void {
        if (!found_existing and current_count >= max_count) return error.PeerLimitExceeded;
    }

    pub fn ensureByteLimit(current_bytes: usize, added_bytes: usize, max_bytes: usize) !void {
        if (current_bytes > max_bytes) return error.PeerLimitExceeded;
        if (added_bytes > max_bytes - current_bytes) return error.PeerLimitExceeded;
    }

    fn joinWireReason(err: anyerror) []const u8 {
        return switch (err) {
            error.PeerLimitExceeded,
            error.JoinRecordLimitExceeded,
            error.JoinClockReentrant,
            error.DuplicateJoinProvision,
            error.UnknownDirectJoinPeer,
            => "join unavailable",
            else => @errorName(err),
        };
    }

    fn addPendingQueuedCallStats(totals: *PendingQueuedCallStats, list: *const std.ArrayList(PendingCall)) void {
        for (list.items) |pending| {
            totals.calls = saturatingAdd(totals.calls, 1);
            totals.bytes = saturatingAdd(totals.bytes, pending.frame.len);
        }
    }

    fn pendingQueuedCallStats(self: *const Peer) PendingQueuedCallStats {
        var totals = PendingQueuedCallStats{};
        var pending_it = self.pending_promises.valueIterator();
        while (pending_it.next()) |list| addPendingQueuedCallStats(&totals, list);
        var pending_export_it = self.pending_export_promises.valueIterator();
        while (pending_export_it.next()) |list| addPendingQueuedCallStats(&totals, list);
        return totals;
    }

    fn ensurePendingQueuedCallBudget(
        self: *const Peer,
        pending_map: *const std.AutoHashMap(u32, std.ArrayList(PendingCall)),
        key: u32,
        frame_len: usize,
        max_buckets: usize,
    ) !void {
        try ensureCountLimit(pending_map.contains(key), pending_map.count(), max_buckets);

        const totals = self.pendingQueuedCallStats();
        if (totals.calls >= self.limits.max_pending_queued_calls) return error.PeerLimitExceeded;
        try ensureByteLimit(totals.bytes, frame_len, self.limits.max_pending_queued_call_bytes);
    }

    fn optionalPayloadBytes(payload: ?[]u8) usize {
        return if (payload) |bytes| bytes.len else 0;
    }

    fn sendResultsToThirdPartyBytesExcluding(self: *const Peer, answer_id: u32) usize {
        var total: usize = 0;
        var it = self.send_results_to_third_party.iterator();
        while (it.next()) |entry| {
            if (entry.key_ptr.* == answer_id) continue;
            total = saturatingAdd(total, optionalPayloadBytes(entry.value_ptr.*));
        }
        return total;
    }

    pub fn pendingThirdPartyReturnBytesExcluding(self: *const Peer, answer_id: u32) usize {
        var total: usize = 0;
        var it = self.pending_third_party_returns.iterator();
        while (it.next()) |entry| {
            if (entry.key_ptr.* == answer_id) continue;
            total = saturatingAdd(total, entry.value_ptr.*.len);
        }
        return total;
    }

    fn pendingAcceptEmbargoKeyBytes(self: *const Peer) usize {
        var total: usize = 0;
        var it = self.pending_accepts_by_embargo.iterator();
        while (it.next()) |entry| {
            total = saturatingAdd(total, entry.key_ptr.*.len);
        }
        return total;
    }

    fn activeProvideKeyBytes(self: *const Peer) usize {
        var total: usize = 0;
        var it = self.provides_by_question.valueIterator();
        while (it.next()) |entry| {
            total = saturatingAdd(total, entry.recipient_key.len);
        }
        return total;
    }

    fn pendingThirdPartyCompletionBytes(self: *const Peer) usize {
        var total: usize = 0;
        var await_it = self.pending_third_party_awaits.iterator();
        while (await_it.next()) |entry| {
            total = saturatingAdd(total, entry.key_ptr.*.len);
        }
        var answer_it = self.pending_third_party_answers.iterator();
        while (answer_it.next()) |entry| {
            total = saturatingAdd(total, entry.key_ptr.*.len);
        }
        return total;
    }

    fn ensureProvideBudget(self: *Peer, question_id: u32, recipient_key: []const u8) !void {
        try ensureCountLimit(
            self.provides_by_question.contains(question_id),
            self.provides_by_question.count(),
            self.limits.max_active_provides,
        );
        try ensureCountLimit(
            self.provides_by_key.contains(recipient_key),
            self.provides_by_key.count(),
            self.limits.max_active_provides,
        );
        try ensureByteLimit(
            self.activeProvideKeyBytes(),
            recipient_key.len,
            self.limits.max_active_provide_key_bytes,
        );
    }

    fn ensureJoinBudget(self: *Peer, join_key_part: JoinKeyPart, question_id: u32) !void {
        if (@as(usize, join_key_part.part_count) > self.limits.max_join_parts_per_join) {
            events.emitResourceRejection(
                self.observer,
                .peer,
                .unknown,
                .join_parts,
                join_key_part.part_count,
                self.limits.max_join_parts_per_join,
                error.PeerLimitExceeded,
            );
            return error.PeerLimitExceeded;
        }
        const additional: usize = @as(usize, 1) +
            @as(usize, @intFromBool(!self.pending_joins.contains(join_key_part.join_id)));
        self.ensureJoinRecordCapacity(additional) catch {
            events.emitResourceRejection(
                self.observer,
                .peer,
                .unknown,
                .join_records,
                saturatingAdd(self.joinRecordCount(), additional),
                self.limits.max_pending_join_records,
                error.PeerLimitExceeded,
            );
            return error.PeerLimitExceeded;
        };
        try ensureCountLimit(
            self.pending_joins.contains(join_key_part.join_id),
            self.pending_joins.count(),
            self.limits.max_pending_joins,
        );
        try ensureCountLimit(
            self.pending_join_questions.contains(question_id),
            self.pending_join_questions.count(),
            self.limits.max_pending_join_questions,
        );

        // Admission also reserves the allocation needed to detach every live
        // Join answer into the callback-safe completing namespace. Completion
        // and expiry must be allocation-free after the final part commits: an
        // OOM at that point cannot leave a complete raw-TTL-null bucket with no
        // future frame capable of retriggering it. The result map receives the
        // same aggregate reservation so L4 fanout can publish all steady-state
        // answer refs without allocating after `hostJoinResult` side effects.
        const answer_reservations = std.math.add(
            usize,
            self.pending_join_questions.count(),
            self.completing_join_answers.count(),
        ) catch return error.PeerLimitExceeded;
        const with_new_answer = std.math.add(usize, answer_reservations, 1) catch
            return error.PeerLimitExceeded;
        const completing_capacity = std.math.cast(u32, with_new_answer) orelse
            return error.PeerLimitExceeded;
        try self.completing_join_answers.ensureTotalCapacity(completing_capacity);
        const result_reservations = std.math.add(
            usize,
            self.pending_join_result_answers.count(),
            with_new_answer,
        ) catch return error.PeerLimitExceeded;
        const result_capacity = std.math.cast(u32, result_reservations) orelse
            return error.PeerLimitExceeded;
        try self.pending_join_result_answers.ensureTotalCapacity(result_capacity);
    }

    fn ensurePendingThirdPartyAwaitBudget(self: *Peer, completion_key: []const u8) !void {
        try ensureCountLimit(
            self.pending_third_party_awaits.contains(completion_key),
            self.pending_third_party_awaits.count(),
            self.limits.max_pending_third_party_awaits,
        );
        try ensureByteLimit(
            self.pendingThirdPartyCompletionBytes(),
            completion_key.len,
            self.limits.max_pending_third_party_completion_bytes,
        );
    }

    fn ensurePendingThirdPartyAnswerBudget(self: *Peer, completion_key: []const u8) !void {
        try ensureCountLimit(
            self.pending_third_party_answers.contains(completion_key),
            self.pending_third_party_answers.count(),
            self.limits.max_pending_third_party_answers,
        );
        try ensureByteLimit(
            self.pendingThirdPartyCompletionBytes(),
            completion_key.len,
            self.limits.max_pending_third_party_completion_bytes,
        );
    }

    fn ensureThirdPartyAdoptionBudget(self: *Peer, adopted_answer_id: u32) !void {
        try ensureCountLimit(false, self.questions.count(), self.limits.max_outbound_questions);
        try ensureCountLimit(
            self.adopted_third_party_answers.contains(adopted_answer_id),
            self.adopted_third_party_answers.count(),
            self.limits.max_adopted_third_party_answers,
        );
    }

    /// Register a local capability for export and return its export ID.
    pub fn addExport(self: *Peer, exported: Export) !u32 {
        return self.addExportWithDeinit(exported, null);
    }

    fn addExportWithDeinit(self: *Peer, exported: Export, deinit_ctx: ?ExportDeinitCtxFn) !u32 {
        self.assertThreadAffinity();
        const id = try self.caps.allocExportId();
        try self.caps.noteExport(id);
        errdefer self.caps.clearExport(id);
        try self.exports.put(id, .{
            .handler = exported,
            .deinit_ctx = deinit_ctx,
            .ref_count = 0,
            .is_promise = false,
            .resolved = null,
        });
        log.debug("added export id={}", .{id});
        return id;
    }

    /// Register a local capability under a caller-chosen export id, if the
    /// id is not already exported.
    ///
    /// Host-relay integrations (see `HostPeer.respondHostCallReturnFrame`)
    /// let the trusted host own the export-id space: a prebuilt Return frame
    /// may name an export this peer has never seen. Registering the id here
    /// before the frame is sent gives the peer a dispatchable entry without
    /// disturbing `allocExportId`, which already skips ids present in the
    /// cap table.
    ///
    /// Returns true when a new entry was created, false when `id` was
    /// already exported.
    pub fn ensureExportAt(self: *Peer, id: u32, exported: Export) !bool {
        self.assertThreadAffinity();
        if (self.exports.contains(id)) return false;
        try self.caps.noteExportAt(id);
        errdefer self.caps.clearExport(id);
        try self.exports.put(id, .{
            .handler = exported,
            .deinit_ctx = null,
            .ref_count = 0,
            .is_promise = false,
            .resolved = null,
        });
        log.debug("registered export at explicit id={}", .{id});
        return true;
    }

    /// Remove an export entry that carries no wire, answer-held, or
    /// promise-held references. Used by host-relay rollback paths to undo a
    /// speculative `ensureExportAt` when the frame that would have taken the
    /// first reference fails before sending. No-op when the entry is
    /// missing, still referenced, or is the bootstrap export.
    pub fn removeUnreferencedExport(self: *Peer, id: u32) void {
        self.assertThreadAffinity();
        if (self.bootstrap_export_id) |bootstrap_id| {
            if (bootstrap_id == id) return;
        }
        const entry = self.exports.getPtr(id) orelse return;
        if (entry.ref_count != 0 or
            entry.answer_ref_count != 0 or
            entry.promise_ref_count != 0 or
            entry.handoff_ref_count != 0)
        {
            return;
        }
        _ = self.exports.remove(id);
        self.caps.clearExport(id);
    }

    /// Export a promise capability that will be resolved later via
    /// `resolvePromiseExportToExport` or `resolvePromiseExportToException`.
    pub fn addPromiseExport(self: *Peer) !u32 {
        self.assertThreadAffinity();
        const id = try self.caps.allocExportId();
        try self.caps.noteExport(id);
        errdefer self.caps.clearExport(id);
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
        if (self.is_shutting_down) return error.PeerShuttingDown;
        const question_id = try self.allocateQuestion(ctx, on_return);
        errdefer self.removeQuestion(question_id);

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        try builder.buildBootstrap(question_id);
        try self.sendBuilder(&builder);
        log.debug("sent bootstrap question_id={}", .{question_id});
        return question_id;
    }

    // -- Three-party handoff ORIGINATION (Provide / Accept) -------------------

    /// No-op Return callback for a held-open Provide question. A `Provide`
    /// receives NO `Return` (rpc.capnp:834-847); the question is registered only
    /// to reserve its id and to be Finished later (on vine release or teardown).
    /// The single case that invokes this is the shutdown drain delivering a
    /// synthetic local exception, which the held-open provision safely ignores.
    fn onProvideNoReturn(
        _: *anyopaque,
        _: *Peer,
        _: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {}

    /// Originate a three-party handoff: hand the capability named by
    /// `provided_target` (a `MessageTarget` local to the vat this peer is
    /// connected to — the HOST of the provided cap, VatC in the spec) to a third
    /// party (VatA), so VatA can pick it up directly with an `Accept`.
    ///
    /// Message direction follows rpc.capnp: the `Provide` goes to the HOST of
    /// the provided cap (this `self` peer = the VatB↔VatC connection). The vine
    /// export and the `thirdPartyHosted` descriptor, by contrast, belong on the
    /// host-of-recipient connection (`host_of_recipient` = the VatB↔VatA
    /// connection), since that is where VatA takes its wire reference and where
    /// the vine's later `Release` arrives.
    ///
    /// Steps:
    ///   1. Register a held-open Provide question on `self` (no Return; Finished
    ///      when the recipient releases the vine — see `handleRelease`).
    ///   2. Mint a vine export on `host_of_recipient` and mark it
    ///      third-party-hosted with `contact_payload` (the `ThirdPartyToContact`)
    ///      so the next descriptor emitted for the vine resolves to
    ///      `thirdPartyHosted{ id = contact, vineId }`.
    ///   3. Record the vine → Provide-question coupling on `host_of_recipient`.
    ///   4. Send the `Provide` to `self` (VatC) with `recipient` = the
    ///      `ThirdPartyToAwait`.
    ///
    /// Returns the vine id + Provide question id. The caller then drives the
    /// `thirdPartyHosted` emission by sending `host_of_recipient` a payload that
    /// carries the vine export (which is marked); the recipient picks it up and
    /// eventually `sendAccept`s on its own connection to VatC.
    pub fn sendProvide(
        self: *Peer,
        provided_target: protocol.MessageTarget,
        recipient: message.AnyPointerReader,
        host_of_recipient: *Peer,
        contact_payload: []const u8,
    ) !ProvideHandle {
        return self.sendProvideInternal(
            .{ .message_target = provided_target },
            recipient,
            host_of_recipient,
            contact_payload,
            null,
        );
    }

    /// Originate a Provide whose target is an explicitly retained outbound
    /// answer on this peer. The answer must have returned and remain
    /// caller-owned. A successful send transfers that answer into the
    /// vine/Provide coupling; manual Finish then rejects it, and coupling
    /// completion Finishes both the Provide and source questions.
    pub fn sendProvideFromRetainedAnswer(
        self: *Peer,
        retained_question_id: u32,
        ops: []const protocol.PromisedAnswerOp,
        recipient: message.AnyPointerReader,
        host_of_recipient: *Peer,
        contact_payload: []const u8,
    ) !ProvideHandle {
        self.assertThreadAffinity();
        host_of_recipient.assertThreadAffinity();
        const wire_answer_id = try self.claimRetainedQuestionForTransfer(retained_question_id);
        var transfer_claimed = true;
        errdefer if (transfer_claimed) self.rollbackRetainedQuestionTransfer(retained_question_id);

        const handle = try self.sendProvideInternal(
            .{ .retained_answer = .{
                .question_id = wire_answer_id,
                .ops = ops,
            } },
            recipient,
            host_of_recipient,
            contact_payload,
            retained_question_id,
        );
        // `sendBuilder` may synchronously drive recipient teardown. A terminal
        // close drains the coupling before this frame resumes; in that case the
        // transfer never commits and caller ownership is restored by errdefer.
        if (!host_of_recipient.outbound_provides.contains(handle.vine_id)) {
            return error.HandoffClosedDuringTransfer;
        }
        self.commitRetainedQuestionTransfer(retained_question_id) catch |err| {
            // The Provide and vine are already published, so a failed commit
            // must explicitly unwind them rather than returning an ownerless
            // handle. Finish publication is durable even if its wire send fails.
            if (host_of_recipient.outbound_provides.fetchRemove(handle.vine_id)) |removed| {
                var op = removed.value;
                op.deinit(host_of_recipient.allocator);
            }
            host_of_recipient.caps.clearThirdPartyHosted(handle.vine_id);
            host_of_recipient.releaseVineExport(handle.vine_id);
            self.deregisterCoupledVine(host_of_recipient, handle.vine_id);
            host_of_recipient.finishOriginatedProvide(
                self,
                handle.question_id,
                retained_question_id,
            );
            return err;
        };
        transfer_claimed = false;
        return handle;
    }

    fn sendProvideInternal(
        self: *Peer,
        provided_target: ProvideOriginationTarget,
        recipient: message.AnyPointerReader,
        host_of_recipient: *Peer,
        contact_payload: []const u8,
        retained_source_question_id: ?u32,
    ) !ProvideHandle {
        self.assertThreadAffinity();
        host_of_recipient.assertThreadAffinity();
        if (self.is_shutting_down or host_of_recipient.is_shutting_down) return error.PeerShuttingDown;
        if (self.transport_close_notified or host_of_recipient.transport_close_notified) {
            return error.TransportClosed;
        }

        var forward_target = try provided_target.cloneForwardTarget(host_of_recipient.allocator);
        var forward_target_owned = true;
        defer if (forward_target_owned) forward_target.deinit(host_of_recipient.allocator);

        // (1) Held-open Provide question on the host-of-provided-cap connection.
        //     ctx is unused by onProvideNoReturn; pass a valid pointer (self)
        //     rather than undefined so no dispatch path ever reads garbage.
        const question_id = try self.allocateQuestion(self, onProvideNoReturn);
        errdefer self.removeQuestion(question_id);
        // A Provide is deliberately held open until the recipient drops its
        // vine; it never receives a Return. The ordinary outbound-call timeout
        // therefore must not cancel it while a valid handoff is in flight.
        if (self.questions.getPtr(question_id)) |question| {
            question.deadline_ns = null;
        }

        // (2) Vine export on the host-of-recipient connection. ctx is unused by
        //     vineRejectingCall; pass host_of_recipient rather than undefined.
        const vine_id = try host_of_recipient.addExport(.{
            .ctx = host_of_recipient,
            .on_call = vineRejectingCall,
        });
        errdefer host_of_recipient.releaseVineExport(vine_id);
        try host_of_recipient.caps.markThirdPartyHosted(vine_id, contact_payload, vine_id);
        errdefer host_of_recipient.caps.clearThirdPartyHosted(vine_id);

        // (3) Couple the vine to the held-open Provide question so a later
        //     vine Release (post-Accept) Finishes the provision on VatC.
        try ensureCountLimit(
            host_of_recipient.outbound_provides.contains(vine_id),
            host_of_recipient.outbound_provides.count(),
            host_of_recipient.limits.max_active_provides,
        );
        try host_of_recipient.outbound_provides.put(vine_id, .{
            .provide_peer = self,
            .provide_question_id = question_id,
            .retained_source_question_id = retained_source_question_id,
            .forward_target = forward_target,
        });
        forward_target_owned = false;
        errdefer if (host_of_recipient.outbound_provides.fetchRemove(vine_id)) |removed| {
            var op = removed.value;
            op.deinit(host_of_recipient.allocator);
        };

        // (3b) LIVENESS (BUG #55): register the reverse back-link on `self` (the
        //      host-of-provided-cap peer). If `self` deinits before the vine is
        //      released, this lets it null the borrowed pointer above so the
        //      later Release never dereferences freed memory. Must land or the
        //      whole coupling unwinds (the errdefers above roll it back).
        try self.registerCoupledVine(host_of_recipient, vine_id);
        errdefer self.deregisterCoupledVine(host_of_recipient, vine_id);

        // (4) Send the Provide to the host of the provided cap (VatC).
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try provided_target.buildProvide(&builder, question_id, recipient);
        try self.sendBuilder(&builder);

        log.debug("sent provide question_id={} vine_id={}", .{ question_id, vine_id });
        return .{ .question_id = question_id, .vine_id = vine_id };
    }

    /// Pick up a capability a third party provided to us: send `Accept` to the
    /// HOST of the provided cap (this `self` peer = the VatA↔VatC connection).
    /// `provision` is the `ThirdPartyCompletion` obtained from the VatNetwork,
    /// matching the `ThirdPartyToAwait` VatB placed in its `Provide`. The
    /// `on_return` callback receives the `Return` carrying the accepted cap
    /// (standard import-from-return; see the host side `sendReturnProvidedTarget`).
    ///
    /// This slice sends `embargo = null` (no in-flight-promise ordering — that
    /// is Phase 4). Returns the Accept question id.
    pub fn sendAccept(
        self: *Peer,
        provision: message.AnyPointerReader,
        embargo: ?[]const u8,
        ctx: *anyopaque,
        on_return: QuestionCallback,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;

        const question_id = try self.allocateQuestion(ctx, on_return);
        errdefer self.removeQuestion(question_id);

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildAccept(question_id, provision, embargo);
        try self.sendBuilder(&builder);

        log.debug("sent accept question_id={}", .{question_id});
        return question_id;
    }

    pub fn sendAcceptNoRestore(
        self: *Peer,
        provision: message.AnyPointerReader,
        embargo: ?[]const u8,
        ctx: *anyopaque,
        on_return: QuestionCallback,
        suppress_auto_finish: bool,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;

        const question_id = try self.allocateQuestionNoRestore(ctx, on_return);
        errdefer self.removeQuestion(question_id);
        if (suppress_auto_finish) {
            const question = self.questions.getPtr(question_id) orelse return error.MissingAllocatedQuestion;
            question.suppress_auto_finish = true;
        }

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildAccept(question_id, provision, embargo);
        try self.sendBuilder(&builder);

        log.debug("sent accept question_id={}", .{question_id});
        return question_id;
    }

    /// Experimental Level-4 Join origination. This is a low-level/manual
    /// helper: callers provide the raw `Join.keyPart` AnyPointer and receive the
    /// ordinary `Return` callback for the Join question. It intentionally does
    /// not implement the full E-join workflow, public `JoinResult` processing,
    /// or direct-connection formation.
    pub fn sendJoinExperimental(
        self: *Peer,
        target: protocol.MessageTarget,
        key_part: ?message.AnyPointerReader,
        ctx: *anyopaque,
        on_return: QuestionCallback,
    ) !u32 {
        return self.sendJoinExperimentalWithAutoFinish(target, key_part, ctx, on_return, false);
    }

    fn sendJoinExperimentalWithAutoFinish(
        self: *Peer,
        target: protocol.MessageTarget,
        key_part: ?message.AnyPointerReader,
        ctx: *anyopaque,
        on_return: QuestionCallback,
        suppress_auto_finish: bool,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;

        const question_id = try self.allocateQuestion(ctx, on_return);
        errdefer self.removeQuestion(question_id);
        if (suppress_auto_finish) {
            const question = self.questions.getPtr(question_id) orelse return error.MissingAllocatedQuestion;
            question.suppress_auto_finish = true;
        }

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildJoin(question_id, target, key_part);
        try self.sendBuilder(&builder);

        log.debug("sent experimental join question_id={}", .{question_id});
        return question_id;
    }

    /// Allocate the next callee-chosen answer id for an outbound
    /// `ThirdPartyAnswer`. Per rpc.capnp:936-941 the value must have bit 30 set
    /// and bit 31 clear ([2^30, 2^31)); this is enforced by masking the counter
    /// back into range on every wrap, and asserted via
    /// `third_party.isThirdPartyAnswerId`. The chosen id is NOT recorded in
    /// any local table on the callee: it names the answer on the *recipient's*
    /// (VatA's) connection, and the callee only re-uses it as the `answerId` of
    /// the follow-up `Return` (which is a plain outbound frame on the same
    /// wire). Callee and recipient are different vats, so there is no collision
    /// with the callee's own caller-allocated question ids.
    fn allocateThirdPartyAnswerId(self: *Peer) u32 {
        var id = self.next_third_party_answer_id;
        // Keep bit 30 set / bit 31 clear even across a full-range wrap.
        if (id < third_party_answer_id_base or id >= third_party_answer_id_limit) {
            id = third_party_answer_id_base;
        }
        var next = id + 1;
        if (next >= third_party_answer_id_limit) next = third_party_answer_id_base;
        self.next_third_party_answer_id = next;
        std.debug.assert(third_party.isThirdPartyAnswerId(id));
        return id;
    }

    fn allocateUnusedThirdPartyAnswerId(self: *Peer) !u32 {
        const first = self.next_third_party_answer_id;
        while (true) {
            const id = self.allocateThirdPartyAnswerId();
            if (!(try self.inboundQuestionIdInUse(id)) and
                !self.incoming_automatic_third_party_routes.contains(id))
            {
                return id;
            }
            if (self.next_third_party_answer_id == first) return error.AnswerIdExhausted;
        }
    }

    fn sendThirdPartyAnswerWithId(
        self: *Peer,
        answer_id: u32,
        completion: message.AnyPointerReader,
    ) !void {
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildThirdPartyAnswer(answer_id, completion);
        try self.sendBuilder(&builder);
        log.debug("sent thirdPartyAnswer answer_id={}", .{answer_id});
    }

    /// Callee → third party: send a `ThirdPartyAnswer` on the connection to the
    /// vat that will receive this call's results (VatA). This is the
    /// redirected-return origination for a call that arrived with
    /// `sendResultsTo = thirdParty` (rpc.capnp:494-505, 906-942): the callee
    /// (VatC) connects to the third party and adopts the call into that
    /// connection by announcing a callee-allocated `answerId`. The recipient
    /// matches `completion` (the `ThirdPartyCompletion`) against the
    /// `awaitFromThirdParty` bookkeeping it was primed with, adopts its parked
    /// question under `answer_id`, and thereafter accepts the follow-up `Return`
    /// (which the callee sends under the SAME `answer_id`) as that question's
    /// results.
    ///
    /// `completion` must serialize to bytes byte-identical to the recipient's
    /// awaited `ThirdPartyToAwait`/`ThirdPartyCompletion` — the recipient keys
    /// its await table on those bytes (see `handleThirdPartyAnswer`).
    ///
    /// Returns the callee-allocated answer id, which the caller then re-uses as
    /// the `answerId` of the follow-up results `Return` on this same peer.
    pub fn sendThirdPartyAnswer(
        self: *Peer,
        completion: message.AnyPointerReader,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;

        const answer_id = try self.allocateUnusedThirdPartyAnswerId();
        try self.sendThirdPartyAnswerWithId(answer_id, completion);
        return answer_id;
    }

    /// Third-party (recipient) side: park an outbound question that awaits a
    /// `ThirdPartyAnswer` from the callee, keyed by the completion bytes it will
    /// present. This is the recipient-side half of the `awaitFromThirdParty`
    /// bookkeeping the receive path already reads: when the callee later sends a
    /// `ThirdPartyAnswer` whose completion matches `completion`, `handleThirdPartyAnswer`
    /// adopts this parked question under the callee-allocated answer id, and the
    /// follow-up `Return` completes it via `on_return`.
    ///
    /// In the spec's canonical proxy topology this parking happens implicitly
    /// when the recipient receives a `Return{awaitFromThirdParty}` for a call it
    /// itself made (`handleReturnAcceptFromThirdParty`). In a first-class
    /// origination the recipient (VatA) never made that call — the introducer
    /// (VatB) did — so the recipient is primed explicitly here. `completion`
    /// must serialize byte-identically to the completion the callee will send.
    ///
    /// The parked question is delivered exactly one `Return` (carrying the real
    /// results) and needs no auto-Finish of its own; adoption inserts it into
    /// the normal questions table under the adopted id, after which it follows
    /// the standard Return lifecycle.
    pub fn registerPendingThirdPartyAwait(
        self: *Peer,
        completion: message.AnyPointerReader,
        ctx: *anyopaque,
        on_return: QuestionCallback,
    ) !void {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;

        const completion_key = (try captureAnyPointerPayload(self.allocator, completion)) orelse
            return error.MissingThirdPartyPayload;
        var owns_key = true;
        errdefer if (owns_key) self.allocator.free(completion_key);

        if (self.pending_third_party_awaits.contains(completion_key)) {
            return error.DuplicateThirdPartyAwait;
        }

        // Reserve an answer-id slot's worth of question budget so the later
        // adoption into `questions` cannot overflow silently. The parked
        // question is a normal outbound question that has not yet been assigned
        // a wire id (the callee chooses it), so it is NOT inserted into
        // `questions` here — only into the await table.
        try self.ensurePendingThirdPartyAwaitBudget(completion_key);

        const question = Question{
            .ctx = ctx,
            .on_return = on_return,
            // The adopted question follows the standard Return lifecycle: when
            // the callee's follow-up results `Return` (under the callee-chosen
            // answer id) arrives, the recipient sends a `Finish` for that id
            // back to the callee, per rpc.capnp:924-926 ("the receiver ... must
            // eventually send a Finish message"). That Finish clears the
            // callee's answer-table entry for the redirected results, so
            // auto-finish must NOT be suppressed here.
        };

        try third_party.putPendingAwait(
            PendingThirdPartyAwait,
            &self.pending_third_party_awaits,
            completion_key,
            .{ .question_id = 0, .question = question },
        );
        owns_key = false;
    }

    /// Call handler installed on a vine export. A vine is primarily a
    /// liveness/refcount anchor for a three-party handoff. When a caller (VatA)
    /// pipelined calls on the handed-off promise, those calls are replayed onto
    /// this vine and are FORWARDED to VatC by `maybeForwardVineCall` (issue #56)
    /// BEFORE dispatch ever reaches this handler — so this handler only runs for
    /// a call on a vine with no live forwarding coupling (`provide_peer` already
    /// torn down). That is genuine non-handoff misuse
    /// or a raced teardown; answer it with a clean exception rather than
    /// silently dropping it.
    fn vineRejectingCall(
        _: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        try peer.sendReturnException(call.question_id, "vine capability is not directly callable");
    }

    /// L3 parked-call FORWARDING pre-dispatch hook (issue #56). Runs for every
    /// call replayed onto an export. Returns `true` (call consumed) only when
    /// `export_id` is a handoff VINE with a live forwarding coupling — a
    /// `provide_peer` still attached (BUG #55 nulls it on provided-cap-peer
    /// teardown). The owned target may be either an import or a promised answer;
    /// in both cases the call is forwarded cross-peer to VatC and its result is
    /// relayed back to complete VatA's original pipelined question. Returns
    /// `false` for any non-vine export or a torn-down provide peer, letting
    /// normal export dispatch — and thus
    /// `vineRejectingCall` for the vine — run.
    fn maybeForwardVineCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        export_id: u32,
    ) anyerror!bool {
        const coupling = self.outbound_provides.get(export_id) orelse return false;
        const provide_peer = coupling.provide_peer orelse return false;
        if (provide_peer.is_shutting_down) return false;

        try self.forwardVineCallToProvidedTarget(call, inbound_caps, provide_peer, coupling.forward_target);
        return true;
    }

    /// Forward one replayed pipelined call from the handoff vine (on `self`,
    /// B↔A) to the provided capability on VatC (`provide_peer`, B↔C), and relay
    /// VatC's Return back to complete VatA's original pipelined question
    /// (`call.question_id`) on `self`.
    ///
    /// E-ORDER: parked calls are replayed in the exact order VatA sent them
    /// (`pending_export_promises` is an ordered ArrayList drained front-to-back),
    /// and each is sent to VatC synchronously here before the next replay, so
    /// their arrival order at VatC matches VatA's send order. Because the forward
    /// is a normal outbound Call on `provide_peer`, it also orders correctly
    /// against any later direct calls VatA makes on the accepted cap (which only
    /// exist AFTER the embargo clears — the pipelined-before-direct guarantee).
    ///
    /// REFCOUNTS: the forwarded call takes no new ref on the vine (it targets
    /// VatC's cap directly); VatA's original question is completed exactly once
    /// (VatC returns exactly once); the relay context is freed on that Return or
    /// by the forwarded question's `deinit_ctx` on a raced peer teardown. The
    /// vine's own lifetime is unchanged — still driven solely by VatA's Release.
    ///
    /// CAP-CARRYING PAYLOADS: params and results are cloned across independent
    /// connection id spaces by minting proxy exports on the outgoing peer. Those
    /// proxies retain the source-side inbound import ref until the destination
    /// releases the proxy, so callbacks can flow through the forwarded handoff
    /// without dangling the original call/return cap table.
    fn forwardVineCallToProvidedTarget(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        provide_peer: *Peer,
        provided_target: provide_forward_target.ForwardTarget,
    ) !void {
        // One heap ctx serves BOTH callbacks `sendCall` drives with a single
        // ctx: `forwardVineParams` (synchronous build, reads `source_params`)
        // and `forwardVineReturn` (later, reads `recipient_*`).
        // Async ownership transfers into a question on `provide_peer`; allocate
        // the context and its proxy-id list from that same peer so Return and
        // question-teardown callbacks always free with the matching allocator.
        const relay = try provide_peer.allocator.create(ForwardVineCallContext);
        relay.* = .{
            .forward_peer = provide_peer,
            .recipient_peer = self,
            .recipient_answer_id = call.question_id,
            .source_params = call.params,
            .source_inbound_caps = @constCast(inbound_caps),
        };
        var relay_owned = true;
        errdefer if (relay_owned) ForwardVineCallContext.deinit(provide_peer.allocator, relay);
        try self.registerForwardVineRelay(relay);

        // Set true by `forwardVineReturn` iff it runs SYNCHRONOUSLY nested inside
        // the `sendCall` below (the loopback case) — meaning the return callback
        // already answered VatA and freed `relay`. `sendCall` can still fail in
        // post-callback work AFTER that (e.g. OOM sending the forwarded question's
        // auto-Finish to VatC), so its error path must NOT then re-answer VatA or
        // re-free `relay`. This flag distinguishes "callback already settled
        // everything" from "the call never went out". It lives on this stack
        // frame; the async hand-off below nulls the ctx's back-pointer so a later
        // callback never writes through it once this frame has unwound.
        var forward_settled = false;
        relay.settled_flag = &forward_settled;

        // Send the forwarded call to VatC on the B↔C connection. The build
        // closure clones VatA's params synchronously (source readers valid for
        // the duration of this send); the return closure relays VatC's result
        // back onto VatA's question on `self`. `sendForwardedVineCall` allocates
        // the question with `restore_on_return_error = false` so a post-callback
        // error (e.g. OOM in the auto-Finish) does NOT restore a question whose
        // ctx `forwardVineReturn` already freed — critical in synchronous loopback
        // where the return is processed inside this send, before any post-send
        // code could clear that flag.
        const forwarded_question_id = provide_peer.sendForwardedVineCallTarget(
            provided_target,
            call.interface_id,
            call.method_id,
            relay,
            forwardVineParams,
            forwardVineReturn,
        ) catch |err| {
            if (forward_settled) {
                // The synchronous return callback already answered VatA and freed
                // `relay` before `sendCall`'s post-callback work failed. VatA is
                // settled exactly once; swallow the trailing error.
                relay_owned = false;
                return;
            }
            // The callback never ran: the forwarded call did not go out. `relay`
            // is still ours — free it explicitly (a plain `return` would skip the
            // errdefer) and answer VatA with a clean exception rather than leaving
            // its question hung. Failing to send that exception is itself
            // best-effort (logged, not propagated): VatA's own connection settles
            // the question on teardown if this last resort also fails.
            relay_owned = false;
            relay.recipient_answer_pending = false;
            ForwardVineCallContext.deinit(provide_peer.allocator, relay);
            self.sendReturnException(call.question_id, @errorName(err)) catch |send_err| {
                log.debug("forwarded pipelined call: failed to fail question {}: {}", .{ call.question_id, send_err });
            };
            return;
        };
        if (forward_settled) {
            // Synchronous loopback: the return callback already ran, answered
            // VatA, and freed `relay`. Nothing left to own or hand off.
            relay_owned = false;
            return;
        }
        // Async transport (or VatC deferred its answer): the return callback has
        // not run yet. Detach the stack-local settle flag — this frame is about
        // to unwind — so the deferred callback never writes through a dangling
        // pointer, then hand `relay` ownership to the forwarded question. Its
        // `deinit_ctx` frees it if the B↔C peer tears down before the Return
        // (which never completes VatA's question; VatA's own connection settles
        // it independently). The question is already `restore_on_return_error =
        // false` from `sendForwardedVineCall`.
        relay.param_proxies_committed = true;
        relay.settled_flag = null;
        if (provide_peer.questions.getPtr(forwarded_question_id)) |q| {
            relay.recipient_answer_pending = true;
            q.deinit_ctx = ForwardVineCallContext.deinit;
        } else {
            relay_owned = false;
            ForwardVineCallContext.deinit(provide_peer.allocator, relay);
            self.sendReturnException(call.question_id, "forwarded handoff question disappeared") catch |send_err| {
                log.debug("forwarded pipelined call: failed to settle vanished question {}: {}", .{
                    call.question_id,
                    send_err,
                });
            };
            return;
        }
        relay_owned = false;
    }

    /// Build closure for the forwarded call to VatC: clone VatA's parked params
    /// into the new call's payload, remapping capability pointers into B↔C
    /// proxy exports. Runs synchronously inside `sendCall`.
    fn forwardVineParams(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
        const ctx: *ForwardVineCallContext = castCtx(*ForwardVineCallContext, ctx_ptr);
        const recipient = ctx.recipient_peer orelse return error.ForwardVineRecipientClosed;
        const payload_builder = try call_builder.payloadTyped();
        try ctx.forward_peer.clonePayloadAcrossPeers(
            call_builder.call.builder,
            payload_builder,
            ctx.source_params,
            recipient,
            ctx.source_inbound_caps,
            &ctx.created_param_proxy_ids,
            false,
        );
    }

    /// Return closure for the forwarded call: relay VatC's Return back onto
    /// VatA's original pipelined question on the recipient peer (B↔A). Runs on
    /// `provide_peer` (B↔C) as the forwarded question's callback; the `peer`
    /// arg here is that B↔C peer, NOT the recipient. Completes VatA's question
    /// exactly once and frees the relay ctx.
    ///
    /// This callback deliberately NEVER propagates an error: it relays
    /// best-effort (a send failure completing VatA's question is logged, not
    /// returned) and always frees the ctx. It also sets `settled_flag` — when the
    /// caller is still on the stack (the synchronous-loopback case) — the instant
    /// it runs, BEFORE freeing the ctx. Together these keep the ctx single-freed
    /// and VatA answered exactly once even when `sendCall` ITSELF fails in
    /// post-callback work AFTER this callback already answered VatA and freed the
    /// ctx (e.g. OOM sending the forwarded question's auto-Finish): the caller's
    /// error path sees `settled_flag == true` and neither re-answers VatA nor
    /// re-frees. Swallowing here alone is not sufficient — the trailing failure
    /// originates in `sendCall`, not in this callback — so the flag is load-bearing.
    fn forwardVineReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *ForwardVineCallContext = castCtx(*ForwardVineCallContext, ctx_ptr);
        const recipient = ctx.recipient_peer;
        const answer_id = ctx.recipient_answer_id;
        const release_param_caps = ctx.created_param_proxy_ids.items.len == 0;
        // Signal the synchronous caller that the callback ran (it is about to
        // answer VatA and free this ctx) BEFORE freeing — while `settled_flag`,
        // if set, still points at that caller's live stack frame. On the async
        // hand-off path the caller has already nulled it, so this is a no-op.
        if (ctx.settled_flag) |flag| flag.* = true;
        if (recipient) |live_recipient| {
            live_recipient.deregisterForwardVineRelay(ctx);
        }
        ctx.recipient_peer = null;
        ctx.recipient_answer_pending = false;
        defer ForwardVineCallContext.deinit(peer.allocator, ctx);

        const live_recipient = recipient orelse return;
        relayForwardedVineReturn(live_recipient, answer_id, peer, ret, inbound_caps, release_param_caps) catch |err| {
            log.debug("forwarded pipelined return relay failed for question {}: {}", .{ answer_id, err });
        };
    }

    /// Complete VatA's original pipelined question (`answer_id` on `recipient`,
    /// B↔A) from VatC's forwarded Return. Results are cloned onto VatA's Return
    /// with the same cross-peer capability proxying used for forwarded params.
    /// Its errors are swallowed by `forwardVineReturn`.
    fn relayForwardedVineReturn(
        recipient: *Peer,
        answer_id: u32,
        source_peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
        release_param_caps: bool,
    ) !void {
        try relayReturnAcrossPeers(recipient, answer_id, source_peer, ret, inbound_caps, release_param_caps);
    }

    /// Destroy a freshly-minted vine export on the origination error path,
    /// before any `thirdPartyHosted` descriptor has been emitted (so it holds no
    /// wire reference and no Release is owed). Removes the export table entry and
    /// clears its cap-table identity directly — `releaseExport(_, 0)` is a no-op.
    fn releaseVineExport(self: *Peer, vine_id: u32) void {
        _ = self.exports.remove(vine_id);
        self.caps.clearExport(vine_id);
    }

    // -- Persistence (sturdy refs, RPC level 2) -------------------------------

    /// Mark an existing export persistent: when the remote calls
    /// `Persistent.save()` on it, `on_save` produces the sturdy-ref payload
    /// for the `SaveResults` Return. The save call is dispatched through the
    /// normal inbound-call path; every other interface still reaches the
    /// export's original handler. Calling this again replaces the hooks.
    pub fn setPersistentExport(self: *Peer, export_id: u32, ctx: *anyopaque, on_save: SaveHandler) !void {
        self.assertThreadAffinity();
        const st = try self.ensurePersistenceState(export_id);
        st.save = .{ .ctx = ctx, .on_save = on_save };
        log.debug("export {} marked persistent", .{export_id});
    }

    /// Remove the save hook from an export. The original call handler is
    /// restored once no persistence hooks remain on the export.
    pub fn clearPersistentExport(self: *Peer, export_id: u32) void {
        self.assertThreadAffinity();
        const st = self.persistent_exports.get(export_id) orelse return;
        st.save = null;
        self.dropPersistenceStateIfUnused(export_id, st);
    }

    /// Install the vat-level restorer hook on the bootstrap capability.
    ///
    /// Restore is vat-specific per the RPC spec; capnp-zig serves it as a
    /// call on the bootstrap export using the `Restorer` convention in
    /// `peer/persistence.zig` (`restore(sturdyRef :Data) -> (cap :Capability)`).
    /// `on_restore` maps the sturdy-ref bytes to a capability (or `.unknown`,
    /// which answers with an "unknown sturdy ref" exception). Requires
    /// `setBootstrap` to have been called; call it again after replacing the
    /// bootstrap export.
    pub fn setRestorer(self: *Peer, ctx: *anyopaque, on_restore: RestoreHandler) !void {
        self.assertThreadAffinity();
        const bootstrap_id = self.bootstrap_export_id orelse return error.BootstrapNotConfigured;
        const st = try self.ensurePersistenceState(bootstrap_id);
        st.restore = .{ .ctx = ctx, .on_restore = on_restore };
        self.restorer_export_id = bootstrap_id;
        log.debug("restorer installed on bootstrap export {}", .{bootstrap_id});
    }

    /// Remove the restorer hook installed by `setRestorer`.
    pub fn clearRestorer(self: *Peer) void {
        self.assertThreadAffinity();
        const export_id = self.restorer_export_id orelse return;
        self.restorer_export_id = null;
        const st = self.persistent_exports.get(export_id) orelse return;
        st.restore = null;
        self.dropPersistenceStateIfUnused(export_id, st);
    }

    /// Call `Persistent.save()` on an imported capability. The callback
    /// receives the sturdy-ref payload from the remote's `SaveResults`
    /// (or the exception/other Return outcome). Returns the question ID,
    /// which supports `cancelQuestion` / `setQuestionDeadline` as usual.
    pub fn sendSave(self: *Peer, target_id: u32, ctx: *anyopaque, on_response: SaveResponseCallback) !u32 {
        self.assertThreadAffinity();
        const heap = try self.allocator.create(SaveQuestionContext);
        heap.* = .{ .user_ctx = ctx, .callback = on_response };
        var heap_owned = true;
        errdefer if (heap_owned) self.allocator.destroy(heap);
        const question_id = try self.sendCall(
            target_id,
            persistence.persistent_interface_id,
            persistence.save_method_id,
            heap,
            buildSaveCallParams,
            onSaveReturn,
        );
        heap_owned = false;
        if (self.questions.getPtr(question_id)) |q| {
            q.deinit_ctx = SaveQuestionContext.deinitCtx;
            q.restore_on_return_error = false;
        }
        return question_id;
    }

    /// Call the vat-level restore method on an imported capability
    /// (normally the remote's bootstrap, obtained via `sendBootstrap`).
    /// `sturdy_ref` is copied into the outbound frame before this returns.
    /// The callback receives the restored capability, already retained;
    /// release it via `releaseImport` when done. Documented client flow:
    /// connect -> `sendBootstrap` -> `sendRestore(ref)` -> resume calling.
    pub fn sendRestore(
        self: *Peer,
        target_id: u32,
        sturdy_ref: []const u8,
        ctx: *anyopaque,
        on_response: RestoreResponseCallback,
    ) !u32 {
        self.assertThreadAffinity();
        const heap = try self.allocator.create(RestoreQuestionContext);
        heap.* = .{ .user_ctx = ctx, .callback = on_response, .sturdy_ref = sturdy_ref };
        var heap_owned = true;
        errdefer if (heap_owned) self.allocator.destroy(heap);
        const question_id = try self.sendCall(
            target_id,
            persistence.restorer_interface_id,
            persistence.restore_method_id,
            heap,
            buildRestoreCallParams,
            onRestoreReturn,
        );
        heap_owned = false;
        if (self.questions.getPtr(question_id)) |q| {
            q.deinit_ctx = RestoreQuestionContext.deinitCtx;
            q.restore_on_return_error = false;
        }
        return question_id;
    }

    /// Look up or create the persistence state for an export, swapping the
    /// export's stored handler for the persistence trampoline. The original
    /// handler keeps serving every non-persistence interface.
    fn ensurePersistenceState(self: *Peer, export_id: u32) !*PersistenceState {
        const entry = self.exports.getEntry(export_id) orelse return error.UnknownExport;
        if (self.persistent_exports.get(export_id)) |st| return st;
        const original = entry.value_ptr.handler orelse return error.ExportHasNoHandler;

        const persistent_before = self.persistent_exports.count();
        try ensureCountLimit(false, persistent_before, self.limits.max_persistent_exports);
        const st = try self.allocator.create(PersistenceState);
        errdefer self.allocator.destroy(st);
        st.* = .{ .export_id = export_id, .original = original };
        try self.persistent_exports.put(export_id, st);
        entry.value_ptr.handler = .{ .ctx = st, .on_call = persistenceOnCall };
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .persistent_exports,
            persistent_before,
            self.persistent_exports.count(),
            self.limits.max_persistent_exports,
        );
        return st;
    }

    /// Drop a persistence state once both hook sets are cleared, restoring
    /// the export's original handler.
    fn dropPersistenceStateIfUnused(self: *Peer, export_id: u32, st: *PersistenceState) void {
        if (st.save != null or st.restore != null) return;
        if (self.exports.getEntry(export_id)) |entry| {
            if (entry.value_ptr.handler) |handler| {
                if (handler.on_call == persistenceOnCall and handler.ctx == @as(*anyopaque, @ptrCast(st))) {
                    entry.value_ptr.handler = st.original;
                }
            }
        }
        _ = self.persistent_exports.remove(export_id);
        self.allocator.destroy(st);
    }

    /// Free persistence state for an export that left the exports table
    /// (remote released it). The original handler is gone with the export.
    pub fn dropPersistenceStateForRemovedExport(self: *Peer, export_id: u32) void {
        if (self.persistent_exports.fetchRemove(export_id)) |removed| {
            self.allocator.destroy(removed.value);
            if (self.restorer_export_id == export_id) self.restorer_export_id = null;
        }
    }

    /// Trampoline installed as the export handler for persistent exports.
    /// Serves `Persistent.save()` and vat-level restore; forwards every
    /// other interface to the export's original handler.
    fn persistenceOnCall(
        ctx: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const st: *PersistenceState = castCtx(*PersistenceState, ctx);
        if (call.interface_id == persistence.persistent_interface_id) {
            if (st.save) |hook| {
                if (call.method_id == persistence.save_method_id) {
                    return peer.servePersistentSave(st.export_id, hook, call);
                }
                return peer.sendReturnException(call.question_id, "unknown method");
            }
        } else if (call.interface_id == persistence.restorer_interface_id) {
            if (st.restore) |hook| {
                if (call.method_id == persistence.restore_method_id) {
                    return peer.serveRestore(hook, call);
                }
                return peer.sendReturnException(call.question_id, "unknown method");
            }
        }
        return st.original.on_call(st.original.ctx, peer, call, caps);
    }

    fn servePersistentSave(self: *Peer, export_id: u32, hook: SaveHook, call: protocol.Call) !void {
        const seal_for = persistence.readSealFor(call.params);
        const sturdy_ref = hook.on_save(hook.ctx, self, export_id, seal_for) catch |err| {
            if (err == error.OutOfMemory) return err;
            log.debug("save handler failed for export {}: {}", .{ export_id, err });
            return self.sendReturnException(call.question_id, @errorName(err));
        };
        defer self.allocator.free(sturdy_ref);
        if (sturdy_ref.len > self.limits.max_sturdy_ref_bytes) {
            events.emitResourceRejection(
                self.observer,
                .peer,
                .unknown,
                .sturdy_ref_bytes,
                sturdy_ref.len,
                self.limits.max_sturdy_ref_bytes,
                error.PeerLimitExceeded,
            );
            return self.sendReturnException(call.question_id, "sturdy ref too large");
        }
        var build_ctx = persistence.SturdyRefReturnContext{ .sturdy_ref = sturdy_ref };
        try self.sendReturnResults(call.question_id, &build_ctx, persistence.buildSaveResultsReturn);
        self.saves_served +%= 1;
    }

    fn serveRestore(self: *Peer, hook: RestoreHook, call: protocol.Call) !void {
        const sturdy_ref = persistence.readSturdyRefParam(call.params) catch |err| {
            return self.sendReturnException(call.question_id, @errorName(err));
        };
        if (sturdy_ref.len > self.limits.max_sturdy_ref_bytes) {
            events.emitResourceRejection(
                self.observer,
                .peer,
                .unknown,
                .sturdy_ref_bytes,
                sturdy_ref.len,
                self.limits.max_sturdy_ref_bytes,
                error.PeerLimitExceeded,
            );
            return self.sendReturnException(call.question_id, "sturdy ref too large");
        }
        const outcome = hook.on_restore(hook.ctx, self, sturdy_ref) catch |err| {
            if (err == error.OutOfMemory) return err;
            log.debug("restore handler failed: {}", .{err});
            return self.sendReturnException(call.question_id, @errorName(err));
        };
        var hosted_export_id: ?u32 = null;
        const export_id: u32 = switch (outcome) {
            .unknown => return self.sendReturnException(call.question_id, "unknown sturdy ref"),
            .existing => |id| blk: {
                if (!self.exports.contains(id)) {
                    return self.sendReturnException(call.question_id, "unknown sturdy ref");
                }
                break :blk id;
            },
            .host => |exported| blk: {
                const id = try self.addExport(exported);
                hosted_export_id = id;
                break :blk id;
            },
        };
        errdefer if (hosted_export_id) |id| self.destroyUnreferencedExport(id);
        var build_ctx = persistence.CapabilityReturnContext{ .cap_id = export_id };
        try self.sendReturnResults(call.question_id, &build_ctx, persistence.buildCapabilityReturn);
        self.restores_served +%= 1;
    }

    const SaveQuestionContext = struct {
        user_ctx: *anyopaque,
        callback: SaveResponseCallback,

        fn deinitCtx(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
            const ctx: *SaveQuestionContext = @ptrCast(@alignCast(ctx_ptr));
            allocator.destroy(ctx);
        }
    };

    const RestoreQuestionContext = struct {
        user_ctx: *anyopaque,
        callback: RestoreResponseCallback,
        /// Borrowed from the caller; valid only while `sendRestore` runs
        /// (the build callback copies it into the frame synchronously).
        sturdy_ref: []const u8,

        fn deinitCtx(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
            const ctx: *RestoreQuestionContext = @ptrCast(@alignCast(ctx_ptr));
            allocator.destroy(ctx);
        }
    };

    fn buildSaveCallParams(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
        _ = ctx_ptr;
        try persistence.writeEmptySaveParams(call_builder);
    }

    fn buildRestoreCallParams(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
        const ctx: *const RestoreQuestionContext = castCtx(*const RestoreQuestionContext, ctx_ptr);
        try persistence.writeRestoreParams(call_builder, ctx.sturdy_ref);
    }

    fn onSaveReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        _ = inbound_caps;
        const ctx: *SaveQuestionContext = castCtx(*SaveQuestionContext, ctx_ptr);
        defer peer.allocator.destroy(ctx);
        const response: SaveResponse = switch (ret.tag) {
            .results => blk: {
                const payload = ret.results orelse return error.MissingReturnPayload;
                break :blk .{ .sturdy_ref = try persistence.readSturdyRefResult(payload) };
            },
            .exception => .{ .exception = ret.exception orelse return error.MissingException },
            else => .{ .other = ret.tag },
        };
        try ctx.callback(ctx.user_ctx, peer, response);
    }

    fn onRestoreReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *RestoreQuestionContext = castCtx(*RestoreQuestionContext, ctx_ptr);
        defer peer.allocator.destroy(ctx);
        var retained_cap: ?cap_table.ResolvedCap = null;
        const response: RestoreResponse = switch (ret.tag) {
            .results => blk: {
                const payload = ret.results orelse return error.MissingReturnPayload;
                if (payload.content.isNull()) return error.MissingRestoredCapability;
                const results_struct = payload.content.getStruct() catch return error.MissingRestoredCapability;
                const field = results_struct.readAnyPointer(0) catch return error.MissingRestoredCapability;
                const cap = field.getCapability() catch return error.MissingRestoredCapability;
                var mutable_caps = inbound_caps.*;
                try mutable_caps.retainCapability(cap);
                const resolved = try inbound_caps.resolveCapability(cap);
                retained_cap = resolved;
                break :blk .{ .cap = resolved };
            },
            .exception => .{ .exception = ret.exception orelse return error.MissingException },
            else => .{ .other = ret.tag },
        };
        ctx.callback(ctx.user_ctx, peer, response) catch |err| {
            if (retained_cap) |resolved| {
                peer.releaseResolvedCap(resolved) catch |release_err| {
                    if (release_err == error.OutOfMemory) return error.OutOfMemory;
                    log.debug("failed to release restored cap after callback error: {}", .{release_err});
                };
            }
            return err;
        };
    }

    /// Begin a graceful shutdown: reject new outbound calls, wait for
    /// outstanding questions to receive their Return, then fire `on_complete`.
    ///
    /// If there are no outstanding questions the callback fires immediately.
    /// Calling `shutdown` a second time is a no-op.
    pub fn shutdown(self: *Peer, on_complete: ?*const fn (peer: *Peer) void) void {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return;
        self.is_shutting_down = true;
        self.shutdown_callback = on_complete;

        if (self.questions.count() == 0) {
            self.completeShutdown();
            return;
        }
        // Stamp the drain bound; `checkDeadlines` force-cancels stragglers
        // once it passes.
        if (self.clockNow()) |now| {
            if (self.timeouts.shutdown_drain_timeout_ms) |ms| {
                self.shutdown_deadline_ns = now + msToNs(ms);
            }
        }
    }

    /// Finish the graceful-shutdown sequence: close the transport (if any)
    /// and fire the shutdown callback. Public so cross-module question
    /// bookkeeping (return orchestration) can complete a drain; not
    /// intended for direct application use.
    pub fn completeShutdown(self: *Peer) void {
        self.assertThreadAffinity();
        if (self.transport.ctx) |transport_ctx| {
            // Close transport if attached and not already closing.
            if (self.transport.close) |close_fn| {
                if (self.transport.is_closing) |is_closing_fn| {
                    if (!is_closing_fn(transport_ctx)) {
                        close_fn(transport_ctx);
                    }
                } else {
                    close_fn(transport_ctx);
                }
            }
        }
        if (self.shutdown_callback) |cb| {
            self.shutdown_callback = null;
            cb(self);
        }
    }

    /// Finish a completed caller-owned retained answer. The entry is removed
    /// only after the Finish frame is accepted by the transport; a send failure
    /// restores it to the returned state so the caller can retry. A retained
    /// loopback result has no remote answer table and retires locally.
    pub fn finishRetainedQuestion(
        self: *Peer,
        question_id: u32,
        release_result_caps: bool,
    ) !void {
        self.assertThreadAffinity();
        const target = try self.retained_questions.beginCallerFinish(question_id);
        errdefer self.retained_questions.rollbackCallerFinish(question_id);
        if (!target.is_loopback) {
            try peer_outbound_control.sendFinishWithFlagsViaSendFrame(
                Peer,
                self,
                target.wire_answer_id,
                release_result_caps,
                false,
                Peer.sendFrameControl,
            );
        }
        _ = self.retained_questions.completeFinish(question_id);
    }

    /// Internal ownership seam for Level-3 handoff integration. Claim is
    /// allocation-free and is valid only after the terminal Return became
    /// callback-visible. Commit keeps the entry tracked as transferred until
    /// that lifecycle successfully Finishes the source answer.
    fn claimRetainedQuestionForTransfer(self: *Peer, question_id: u32) !u32 {
        self.assertThreadAffinity();
        return try self.retained_questions.beginTransfer(question_id);
    }

    fn rollbackRetainedQuestionTransfer(self: *Peer, question_id: u32) void {
        self.assertThreadAffinity();
        self.retained_questions.rollbackTransfer(question_id);
    }

    fn commitRetainedQuestionTransfer(self: *Peer, question_id: u32) !void {
        self.assertThreadAffinity();
        if (!self.retained_questions.commitTransfer(question_id)) {
            return error.RetainedQuestionTransferNotInProgress;
        }
    }

    /// Internal transfer-owner Finish. On failure the transferred answer stays
    /// live and retryable; on success it leaves all retained gauges exactly once.
    fn finishTransferredRetainedQuestion(
        self: *Peer,
        question_id: u32,
        release_result_caps: bool,
    ) !void {
        self.assertThreadAffinity();
        const target = try self.retained_questions.beginTransferredFinish(question_id);
        errdefer self.retained_questions.rollbackTransferredFinish(question_id);
        if (!target.is_loopback) {
            try peer_outbound_control.sendFinishWithFlagsViaSendFrame(
                Peer,
                self,
                target.wire_answer_id,
                release_result_caps,
                false,
                Peer.sendFrameControl,
            );
        }
        _ = self.retained_questions.completeFinish(question_id);
    }

    /// Cancel an outstanding outbound question.
    ///
    /// The local callback is delivered an exception Return carrying
    /// `reason` immediately, and a Finish with `releaseResultCaps` is sent
    /// to the remote. Per the Cap'n Proto RPC spec the remote still sends
    /// exactly one Return for the question (possibly `canceled`); the
    /// question entry stays in the table, marked cancelled, so that Return
    /// is absorbed silently when it arrives. Loopback questions complete
    /// locally and are removed outright.
    pub fn cancelQuestion(self: *Peer, question_id: u32, reason: []const u8) !void {
        return self.cancelQuestionTyped(question_id, reason, .failed);
    }

    /// `cancelQuestion` carrying an explicit `Exception.Type` for the locally
    /// synthesized exception the caller observes.
    pub fn cancelQuestionTyped(
        self: *Peer,
        question_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) !void {
        self.assertThreadAffinity();
        const logical_question_id = if (self.retained_questions.contains(question_id))
            question_id
        else
            self.retained_questions.logicalQuestionIdForWire(question_id) orelse question_id;
        const wire_answer_id = if (self.retained_questions.get(logical_question_id)) |retained|
            retained.wire_answer_id
        else
            question_id;
        const entry = self.questions.getPtr(wire_answer_id) orelse return error.UnknownQuestion;
        if (entry.cancelled) return;
        const question = entry.*;

        // Cancellation owns the answer lifetime from this point onward: it
        // emits Finish itself and absorbs the mandatory late Return, so no
        // caller-owned retained record remains.
        _ = self.retained_questions.retire(logical_question_id);

        if (question.is_loopback) {
            _ = self.loopback_questions.remove(wire_answer_id);
            self.removeQuestion(wire_answer_id);
            try self.deliverLocalException(question, logical_question_id, reason, ex_type);
            return;
        }

        // Delivery transfers ctx ownership to the callback; drop the
        // undelivered-cleanup hook so peer deinit cannot double-free.
        entry.cancelled = true;
        entry.deadline_ns = null;
        entry.deinit_ctx = null;

        // Tell the remote we no longer want the answer. A send failure is
        // tolerated: the local caller still observes the exception, and
        // transport teardown reconciles remote state.
        peer_outbound_control.sendFinishWithFlagsViaSendFrame(
            Peer,
            self,
            wire_answer_id,
            true,
            false,
            Peer.sendFrameControl,
        ) catch |err| {
            log.debug("cancel finish send failed for question {}: {}", .{ wire_answer_id, err });
        };

        try self.deliverLocalException(question, logical_question_id, reason, ex_type);
    }

    /// Cancel every question whose deadline has passed, and enforce the
    /// shutdown drain bound. Returns the number of questions cancelled.
    ///
    /// Call this periodically — typically from a transport tick (see
    /// `Connection.Options.tick_interval_ms`) or a test harness. A peer
    /// without a clock returns 0 immediately.
    pub fn checkDeadlines(self: *Peer) usize {
        self.assertThreadAffinity();
        self.enterJoinOperation();
        defer self.leaveJoinOperation();

        // The vat-wide parked-Accept clock may be configured independently of
        // this peer's outbound-question clock. Sweep it before the early
        // return below, and do not include detached parks in this method's
        // documented outbound-question cancellation count.
        if (self.provision_index) |idx| _ = idx.sweepExpiredParkedAccepts();
        _ = self.sweepExpiredJoins();

        // A failed Finish send must not orphan a completed Provide or its
        // transferred source answer. Retry those locally-ended lifetimes on
        // the normal maintenance path, without changing this method's return
        // value (which remains the count of cancelled questions).
        self.retryDeferredFinishes();

        const now = self.clockNow() orelse return 0;

        var expired: std.ArrayList(u32) = .empty;
        defer expired.deinit(self.allocator);
        var it = self.questions.iterator();
        while (it.next()) |kv| {
            if (kv.value_ptr.cancelled or kv.value_ptr.finish_on_maintenance) continue;
            const deadline = kv.value_ptr.deadline_ns orelse continue;
            if (now >= deadline) expired.append(self.allocator, kv.key_ptr.*) catch break;
        }

        var cancelled: usize = 0;
        for (expired.items) |question_id| {
            events.emitTimeout(self.observer, .peer, .unknown, .call_deadline, question_id);
            self.cancelQuestionTyped(question_id, deadline_reason, .overloaded) catch |err| {
                log.debug("deadline cancel failed for question {}: {}", .{ question_id, err });
                continue;
            };
            cancelled += 1;
        }

        // Parked third-party-await questions are not in the questions map, so
        // the loop above misses them; sweep expired ones so they cannot escape
        // deadline enforcement.
        cancelled += self.sweepThirdPartyAwaits(true);

        if (self.is_shutting_down) {
            if (self.shutdown_deadline_ns) |drain_deadline| {
                if (now >= drain_deadline and self.questions.count() != 0) {
                    self.shutdown_deadline_ns = null;
                    events.emitTimeout(self.observer, .peer, .unknown, .shutdown_drain, null);
                    cancelled += self.forceCancelAllQuestions(shutdown_reason, .disconnected);
                }
            }
        }
        return cancelled;
    }

    /// Synthesize an exception Return for `question_id` and deliver it to
    /// the question's callback, exactly as if the remote had answered.
    ///
    /// Ctx ownership: once the callback runs it owns `question.ctx`
    /// (generated callbacks destroy it unconditionally). If synthesis fails
    /// before the callback could run, the ctx is freed here via
    /// `question.deinit_ctx` — callers have already removed the entry from
    /// the questions map (or dropped its cleanup hook), so nothing else can.
    fn deliverLocalException(
        self: *Peer,
        question: Question,
        question_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) !void {
        var callback_ran = false;
        errdefer if (!callback_ran) {
            if (question.deinit_ctx) |deinit_ctx| deinit_ctx(self.allocator, question.ctx);
        };
        // Synthetic and LOCAL: delivered straight to our own question callback,
        // never sent, and never routed through `handleReturn`. Nothing consumes
        // its `releaseParamCaps`, so it keeps the rpc.capnp default.
        const frame = try peer_return_frames.buildReturnExceptionFrame(
            self.allocator,
            question_id,
            reason,
            ex_type,
            true,
        );
        defer self.allocator.free(frame);
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        const ret = try decoded.asReturn();
        var inbound_caps = try cap_table.InboundCapTable.init(self.allocator, null, &self.caps);
        defer inbound_caps.deinit();
        callback_ran = true;
        question.on_return(question.ctx, self, ret, &inbound_caps) catch |err| {
            if (err == error.OutOfMemory) return error.OutOfMemory;
            log.debug("cancel exception callback error for question {}: {}", .{ question_id, err });
        };
    }

    /// Remove and cancel every outstanding question (drain-bound
    /// enforcement). Unlike `cancelQuestion` this does not keep entries
    /// for late Returns — the transport is about to close.
    fn forceCancelAllQuestions(self: *Peer, reason: []const u8, ex_type: protocol.ExceptionType) usize {
        var ids: std.ArrayList(u32) = .empty;
        defer ids.deinit(self.allocator);
        var it = self.questions.keyIterator();
        while (it.next()) |key| ids.append(self.allocator, key.*) catch break;

        var cancelled: usize = 0;
        for (ids.items) |question_id| {
            const removed = self.questions.fetchRemove(question_id) orelse continue;
            const question = removed.value;
            const logical_question_id = self.retained_questions.logicalQuestionIdForWire(question_id) orelse question_id;
            _ = self.retained_questions.retire(logical_question_id);
            _ = self.loopback_questions.remove(question_id);
            // No wire Return will consume this question's param-export
            // record; free it without spending the refs (transport teardown
            // reconciles export state, as before the record existed).
            self.freeQuestionParamExports(question_id);
            if (question.cancelled) continue; // exception already delivered
            if (!question.is_loopback) {
                peer_outbound_control.sendFinishWithFlagsViaSendFrame(
                    Peer,
                    self,
                    question_id,
                    true,
                    false,
                    Peer.sendFrameControl,
                ) catch |err| {
                    log.debug("drain finish send failed for question {}: {}", .{ question_id, err });
                };
            }
            self.deliverLocalException(question, logical_question_id, reason, ex_type) catch |err| {
                log.debug("drain exception delivery failed for question {}: {}", .{ logical_question_id, err });
            };
            cancelled += 1;
        }
        cancelled += self.sweepThirdPartyAwaits(false);
        if (self.is_shutting_down and !self.in_deinit and self.questions.count() == 0) {
            self.completeShutdown();
        }
        return cancelled;
    }

    /// Remove parked third-party-await questions, freeing each question's heap
    /// ctx (via deinit_ctx) and the owned map key. With `only_expired` true,
    /// only deadline-expired awaits are removed; otherwise all are. This bounds
    /// the memory a peer can pin by answering questions with awaitFromThirdParty
    /// and never completing the handoff — otherwise those contexts escape both
    /// deadline enforcement and the shutdown drain entirely.
    fn sweepThirdPartyAwaits(self: *Peer, only_expired: bool) usize {
        // Null cutoff means "sweep everything" (shutdown drain); a non-null
        // cutoff sweeps only entries whose deadline has passed.
        const cutoff: ?i64 = if (only_expired) (self.clockNow() orelse return 0) else null;

        var keys: std.ArrayList([]const u8) = .empty;
        defer keys.deinit(self.allocator);
        {
            var it = self.pending_third_party_awaits.iterator();
            while (it.next()) |kv| {
                if (cutoff) |now| {
                    const deadline = kv.value_ptr.question.deadline_ns orelse continue;
                    if (now < deadline) continue;
                }
                keys.append(self.allocator, kv.key_ptr.*) catch break;
            }
        }

        var swept: usize = 0;
        for (keys.items) |key| {
            const removed = self.pending_third_party_awaits.fetchRemove(key) orelse continue;
            _ = self.retained_questions.retire(removed.value.question_id);
            if (removed.value.question.deinit_ctx) |deinit_ctx| {
                deinit_ctx(self.allocator, removed.value.question.ctx);
            }
            self.allocator.free(removed.key);
            if (only_expired) {
                events.emitTimeout(self.observer, .peer, .unknown, .call_deadline, removed.value.question_id);
            }
            swept += 1;
        }
        return swept;
    }

    /// Resolve a previously exported promise to point at a concrete export.
    pub fn resolvePromiseExportToExport(self: *Peer, promise_id: u32, export_id: u32) !void {
        self.assertThreadAffinity();
        var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
        if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
        if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;
        if (!self.exports.contains(export_id)) return error.UnknownExport;

        const descriptor = protocol.CapDescriptor{
            .tag = if (self.caps.isExportPromise(export_id)) .senderPromise else .senderHosted,
            .id = export_id,
            .promised_answer = null,
            .attached_fd = null,
        };

        // The Resolve's cap descriptor hands the remote a reference to the
        // target export, exactly like a call/return descriptor would: the
        // remote spends it with a wire Release once it drops the resolved cap
        // (see resolveDescriptor's noteImport in caps/inbound.zig), or — if
        // it does not implement Resolve — echoes the message back inside an
        // Unimplemented and handleUnimplementedResolve releases it there.
        // Rolled back only while the send can still fail: past a successful
        // send the remote holds the descriptor and owns the ref (matching
        // handleBootstrap's pattern).
        try self.noteExportRef(export_id);
        var rollback_wire_ref = true;
        errdefer if (rollback_wire_ref) self.rollbackExportRef(export_id);

        // Pin the resolution target for the promise export's own lifetime. Once
        // the promise routes inbound calls at `export_id` (via
        // replayResolvedPromiseExport / handleResolvedExportedCall below), an
        // inbound Release that zeroes the target's wire ref_count — or the
        // echoed-Unimplemented cleanup above, which also spends a wire ref —
        // must not destroy the target out from under the still-live promise
        // export. This promise-held reference is separate from the wire ref
        // taken above (which the remote owns and spends): it is released only
        // when the promise export is itself destroyed (see
        // finalizeExportRelease / releasePromiseHeldCap). Rolled back only
        // while the send can still fail; past a committed resolution the
        // promise owns it and its destruction releases it.
        try self.notePromiseExportRef(export_id);
        var rollback_promise_ref = true;
        errdefer if (rollback_promise_ref) self.rollbackPromiseExportRef(export_id);

        try peer_outbound_control.sendResolveCapViaSendFrame(
            Peer,
            self,
            promise_id,
            descriptor,
            Peer.sendFrame,
        );
        rollback_wire_ref = false;

        promise_entry.value_ptr.resolved = .{ .exported = .{ .id = export_id } };
        // The resolution is committed: the promise export now routes to
        // `export_id` and its eventual destruction releases this promise-held
        // ref, so keep it even if replay below fails.
        rollback_promise_ref = false;
        self.caps.clearExportPromise(promise_id);
        try self.replayResolvedPromiseExport(promise_id, promise_entry.value_ptr.resolved.?);
    }

    /// Resolve a previously exported promise to a capability the *remote* peer
    /// hosts (one we hold as an import). This is the "reflected capability"
    /// case: the promise resolves to a cap reached by a different path than the
    /// promise itself, so a conformant remote will run the embargo/Disembargo
    /// handshake against it. Mirror of `resolvePromiseExportToExport`, but the
    /// resolution target lives in the import table, not the export table.
    pub fn resolvePromiseExportToImport(self: *Peer, promise_id: u32, import_id: u32) !void {
        self.assertThreadAffinity();
        var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
        if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
        if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;
        if (!self.caps.hasImport(import_id)) return error.UnknownImport;

        // `receiverHosted` names the REMOTE's own export (our import), so — unlike
        // resolvePromiseExportToExport's senderHosted/senderPromise descriptor —
        // this does NOT hand the remote a fresh wire reference on one of OUR
        // exports. The remote already owns `import_id`; it will not spend a
        // Release against us on account of receiving it in the Resolve. So we
        // take NO outbound wire ref here.
        const descriptor = protocol.CapDescriptor{
            .tag = .receiverHosted,
            .id = import_id,
            .promised_answer = null,
            .attached_fd = null,
        };

        // Pin the resolution target (our import) for the promise export's own
        // lifetime. Once the promise routes inbound calls back out to
        // `import_id` (via replayResolvedPromiseExport / forwardResolvedCall
        // below), the host handler that originally received this import as a
        // call/return cap may drop its own wire reference before the promise
        // export is gone — an inbound path that zeroes the import's wire
        // `ref_count` must not remove the import entry out from under the
        // still-live promise export. This is a purely LOCAL pin
        // (`promise_ref_count`), NOT a `noteImport`: a `receiverHosted`
        // descriptor creates no new wire reference, so dropping this pin must
        // never send a wire Release. It is released when the promise export is
        // destroyed (see finalizeExportRelease's import-target cascade, which
        // calls releasePromiseImportRef — local, no frame). Rolled back only
        // while the send can still fail; past a committed resolution the promise
        // owns it.
        try self.caps.notePromiseImportRef(import_id);
        var rollback_import_ref = true;
        errdefer if (rollback_import_ref) {
            _ = self.caps.releasePromiseImportRef(import_id);
        };

        try peer_outbound_control.sendResolveCapViaSendFrame(
            Peer,
            self,
            promise_id,
            descriptor,
            Peer.sendFrame,
        );

        promise_entry.value_ptr.resolved = .{ .imported = .{ .id = import_id } };
        // The resolution is committed: the promise export now routes to
        // `import_id` and its eventual destruction drops this promise-held pin
        // (no wire Release), so keep it even if replay below fails.
        rollback_import_ref = false;
        self.caps.clearExportPromise(promise_id);
        try self.replayResolvedPromiseExport(promise_id, promise_entry.value_ptr.resolved.?);
    }

    /// Resolve a previously exported promise to a capability hosted by a THIRD
    /// vat (Level-3 three-party handoff ORIGINATION). Mirror of
    /// `resolvePromiseExportToImport`, but the resolution target lives on neither
    /// this peer's export table nor its import table — it is hosted by VatC, so
    /// the Resolve carries a `thirdPartyHosted{ id = ThirdPartyToContact, vineId }`
    /// descriptor and this vat originates the paired `Provide` to VatC.
    ///
    /// `self` is the host-of-recipient connection (VatB↔VatA), where the promise
    /// export lives, the vine is minted, and the Resolve is sent. `provide_peer`
    /// is the host-of-provided-cap connection (VatB↔VatC), where the held-open
    /// `Provide` is sent naming `provided_target` (VatC's Carol) and `recipient`
    /// (the `ThirdPartyToAwait`). `contact_payload` is the serialized
    /// `ThirdPartyToContact` VatA will redeem via its VatNetwork to reach VatC.
    ///
    /// This slice sends the Resolve with `embargo = null` semantics: there is no
    /// in-flight-promise embargo/disembargo during the handoff (Phase 4).
    ///
    /// Returns the `ProvideHandle` (Provide question id + vine id). The vine's
    /// wire reference is held by the emitted `thirdPartyHosted` descriptor; when
    /// VatA releases the vine (after picking up Carol directly), `handleRelease`
    /// Finishes the Provide on `provide_peer`.
    pub fn resolvePromiseExportToThirdParty(
        self: *Peer,
        promise_id: u32,
        provide_peer: *Peer,
        provided_target: protocol.MessageTarget,
        recipient: message.AnyPointerReader,
        contact_payload: []const u8,
    ) !ProvideHandle {
        return self.resolvePromiseExportToThirdPartyInternal(
            promise_id,
            provide_peer,
            .{ .message_target = provided_target },
            recipient,
            contact_payload,
        );
    }

    /// Resolve an exported promise through a Level-3 handoff whose provided
    /// target is an open, explicitly retained answer on `provide_peer`.
    /// `ops` selects the capability within that answer. A successful Provide
    /// transfers the answer lifetime into the vine coupling; callers must not
    /// subsequently Finish it directly.
    pub fn resolvePromiseExportToThirdPartyFromRetainedAnswer(
        self: *Peer,
        promise_id: u32,
        provide_peer: *Peer,
        retained_question_id: u32,
        ops: []const protocol.PromisedAnswerOp,
        recipient: message.AnyPointerReader,
        contact_payload: []const u8,
    ) !ProvideHandle {
        return self.resolvePromiseExportToThirdPartyInternal(
            promise_id,
            provide_peer,
            .{ .retained_answer = .{
                .question_id = retained_question_id,
                .ops = ops,
            } },
            recipient,
            contact_payload,
        );
    }

    fn resolvePromiseExportToThirdPartyInternal(
        self: *Peer,
        promise_id: u32,
        provide_peer: *Peer,
        provided_target: ProvideOriginationTarget,
        recipient: message.AnyPointerReader,
        contact_payload: []const u8,
    ) !ProvideHandle {
        self.assertThreadAffinity();
        provide_peer.assertThreadAffinity();
        if (self.is_shutting_down or provide_peer.is_shutting_down) return error.PeerShuttingDown;

        {
            // Validate the promise export up front, but do NOT hold the entry
            // pointer across `sendProvide`: it mints the vine into `self.exports`,
            // which can rehash and invalidate any captured entry pointer. Re-fetch
            // after origination (below) before mutating the promise entry.
            const promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
            if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
            if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;
        }

        // Originate the handoff: mint the vine on `self`, mark it
        // third-party-hosted with the contact, couple vine → held-open Provide
        // on `provide_peer`, and send the Provide to VatC. `sendProvide` rolls
        // back all of that on any failure before it returns.
        const retained_source_question_id: ?u32 = switch (provided_target) {
            .message_target => null,
            .retained_answer => |retained| retained.question_id,
        };
        const handle = switch (provided_target) {
            .message_target => |target| try provide_peer.sendProvide(target, recipient, self, contact_payload),
            .retained_answer => |retained| try provide_peer.sendProvideFromRetainedAnswer(
                retained.question_id,
                retained.ops,
                recipient,
                self,
                contact_payload,
            ),
        };
        // Undo the whole origination if the Resolve emission below fails: destroy
        // the vine export + its handoff mark, drop the coupling, and Finish the
        // Provide we just sent so VatC does not leak the provision.
        var origination_owned = true;
        errdefer if (origination_owned) {
            if (self.outbound_provides.fetchRemove(handle.vine_id)) |removed| {
                var op = removed.value;
                op.deinit(self.allocator);
            }
            self.caps.clearThirdPartyHosted(handle.vine_id);
            self.releaseVineExport(handle.vine_id);
            // Drop the liveness back-link `sendProvide` registered on the
            // provide_peer (BUG #55) so unwinding leaves no stale coupling.
            provide_peer.deregisterCoupledVine(self, handle.vine_id);
            self.finishOriginatedProvide(provide_peer, handle.question_id, retained_source_question_id);
        };

        // Build the Resolve's resolved descriptor: thirdPartyHosted{ id, vineId }.
        // The recipient takes a wire reference on the VINE (the handoff anchor),
        // exactly as the payload emitter does (caps/outbound.zig), so account for
        // that reference here — buildResolveCap does NOT run the outbound-cap
        // callback that would otherwise note it.
        var contact_msg = try message.Message.initUnvalidated(self.allocator, contact_payload);
        defer contact_msg.deinit();
        const contact = try contact_msg.getRootAnyPointer();
        const descriptor = protocol.CapDescriptor{
            .tag = .thirdPartyHosted,
            .id = null,
            .promised_answer = null,
            .third_party = .{ .id = contact, .vine_id = handle.vine_id },
            .attached_fd = null,
        };

        try self.noteExportRef(handle.vine_id);
        var rollback_wire_ref = true;
        errdefer if (rollback_wire_ref) self.rollbackExportRef(handle.vine_id);

        // Record the resolving promise export on the coupling BEFORE emitting the
        // Resolve. Two reasons: (1) the vine teardown clears the promise's
        // resolution target (see handleRelease); (2) — critically for Phase 4 —
        // emitting the Resolve can synchronously drive the recipient's auto-pickup
        // to send a `context.accept` Disembargo straight back to us on the promise
        // path (single-threaded loopback), and `handleAcceptDisembargo` must find
        // this coupling by `resolved_promise_export_id` to forward the Disembargo
        // on to the capability host. If we set it only after the Resolve returns,
        // a synchronous Disembargo would find no coupling and never reach VatC.
        if (self.outbound_provides.getPtr(handle.vine_id)) |op| {
            op.resolved_promise_export_id = promise_id;
        }
        errdefer if (origination_owned) {
            if (self.outbound_provides.getPtr(handle.vine_id)) |op| {
                op.resolved_promise_export_id = null;
            }
        };

        try peer_outbound_control.sendResolveCapViaSendFrame(
            Peer,
            self,
            promise_id,
            descriptor,
            Peer.sendFrame,
        );
        rollback_wire_ref = false;
        origination_owned = false;

        // The Resolve is on the wire: the recipient's synchronous auto-pickup
        // may already have sent back a `context.accept` Disembargo, which
        // `handleAcceptDisembargo` STASHED on the coupling (e-order: it must
        // not reach the host before the parked-call replay below). From here,
        // EVERY exit — including the two error returns below — must flush the
        // stash, or the recipient's embargoed Accept hangs forever.
        errdefer self.flushStashedAcceptDisembargo(handle.vine_id);

        // Re-fetch the promise entry: the vine insert above may have rehashed
        // `self.exports`, invalidating the pointer captured during validation.
        var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;

        // With no in-flight promise call, direct auto-pickup can synchronously
        // Accept and Release the vine inside the Resolve send above. In that
        // case `handleRelease` has already drained the coupling and destroyed
        // the vine; never publish a resolved export pointing at that dead id.
        // The remote recipient already owns the direct cap, so locally settle
        // the obsolete promise export to none and fail any impossible late
        // parked calls cleanly.
        if (!self.outbound_provides.contains(handle.vine_id) or
            !self.exports.contains(handle.vine_id))
        {
            promise_entry.value_ptr.resolved = .none;
            self.caps.clearExportPromise(promise_id);
            try self.replayResolvedPromiseExport(promise_id, .none);
            self.flushStashedAcceptDisembargo(handle.vine_id);
            log.debug("resolved promise export {} completed during synchronous third-party pickup", .{promise_id});
            return handle;
        }

        // Route pipelined calls that arrive on the promise export through the
        // vine. The coupling owns either the imported or promised-answer target,
        // so replay FORWARDS each parked call to VatC over the B↔C connection
        // (issue #56, `maybeForwardVineCall`); only a coupling whose provide peer
        // has torn down falls back to the vine's rejecting handler. NO
        // promise-held pin is taken on
        // the vine: its lifetime is driven solely by VatA's wire Release (the
        // handoff-completion signal), which must destroy it to Finish the Provide.
        // A pin would keep the vine alive past that Release and stall the Finish.
        promise_entry.value_ptr.resolved = .{ .exported = .{ .id = handle.vine_id } };
        self.caps.clearExportPromise(promise_id);
        try self.replayResolvedPromiseExport(promise_id, promise_entry.value_ptr.resolved.?);

        // Parked pre-resolution calls are now on their way to the host; the
        // recipient's Disembargo (if one was stashed during the Resolve send)
        // may follow them. This also arms immediate forwarding for any later
        // Disembargo on this coupling.
        self.flushStashedAcceptDisembargo(handle.vine_id);

        log.debug("resolved promise export {} to third party via vine {}", .{ promise_id, handle.vine_id });
        return handle;
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

    /// Register a cleanup callback for a question's heap-allocated ctx,
    /// invoked by `Peer.deinit` if the question is still outstanding (never
    /// answered — e.g. the connection dropped without a full shutdown drain).
    /// Also clears `restore_on_return_error`, since the callback frees the ctx
    /// on the normal return path: restoring the question after a callback
    /// error would leave `deinit_ctx` pointing at freed memory. No-op if the
    /// question id is unknown. Intended for generated client stubs and other
    /// external callers that cannot reach the questions table directly; it
    /// mirrors what `sendSave`/`sendRestore` do inline.
    pub fn setQuestionDeinitCtx(self: *Peer, question_id: u32, deinit_ctx: state.QuestionDeinitCtxFn) void {
        self.assertThreadAffinity();
        if (self.questions.getPtr(question_id)) |q| {
            q.deinit_ctx = deinit_ctx;
            q.restore_on_return_error = false;
        }
    }

    /// Send an RPC call to an imported capability, returning the question ID.
    ///
    /// Error set is intentionally left open (inferred/`anyerror`). Every send
    /// path in this family synchronously invokes the caller-supplied `build`
    /// closure (`CallBuildFn`, `try build_fn(ctx, &call)` in
    /// `peer_call_sender`), whose `anyerror` propagates out. Narrowing below
    /// `anyerror` would require changing the `CallBuildFn` contract, so — like
    /// the user-callback typedefs above — this stays open. (Applies to all four
    /// `sendCall*` entry points.)
    pub fn sendCall(
        self: *Peer,
        target_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        return self.sendCallWithOptions(target_id, interface_id, method_id, ctx, build, on_return, .{});
    }

    /// Send a call with an explicit result-lifetime policy. `.automatic`
    /// preserves `sendCall` exactly; `.retained` withholds Finish after Return
    /// until `finishRetainedQuestion` or an ownership transfer completes.
    pub fn sendCallWithOptions(
        self: *Peer,
        target_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
    ) !u32 {
        return self.sendCallWithOptionsRestore(
            target_id,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            options,
            true,
        );
    }

    /// Generator plumbing for heap-owned typed call contexts. Unlike the raw
    /// callback API, generated callbacks own and free their context, so a
    /// synchronous callback error must never restore a question that points at
    /// freed memory.
    pub fn sendCallGeneratedWithOptions(
        self: *Peer,
        target_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
    ) !u32 {
        return self.sendCallWithOptionsRestore(
            target_id,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            options,
            false,
        );
    }

    fn sendCallWithOptionsRestore(
        self: *Peer,
        target_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
        restore_on_return_error: bool,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;
        log.debug("sendCall target_id={} interface_id=0x{x} method_id={}", .{ target_id, interface_id, method_id });
        if (self.resolved_imports.get(target_id)) |entry| {
            if (!entry.embargoed and entry.cap != null) {
                return self.sendCallResolvedWithOptionsRestore(
                    entry.cap.?,
                    interface_id,
                    method_id,
                    ctx,
                    build,
                    on_return,
                    options,
                    restore_on_return_error,
                );
            }
        }

        const allocate_question: *const fn (*Peer, *anyopaque, QuestionCallback) anyerror!u32 = switch (options.result_lifetime) {
            .automatic => if (restore_on_return_error) Peer.allocateQuestion else Peer.allocateQuestionNoRestore,
            .retained => Peer.allocateRetainedQuestion,
        };

        const question_id = try peer_call_sender.sendCallToImport(
            Peer,
            CallBuildFn,
            QuestionCallback,
            self.allocator,
            &self.caps,
            self,
            onOutboundCap,
            rollbackOutboundCap,
            self,
            target_id,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            allocate_question,
            Peer.removeQuestion,
            Peer.recordQuestionParamExports,
            Peer.sendBuilder,
        );
        // Record the promise-import target on the question so the Level-3
        // recipient auto-pickup can observe an in-flight pipelined call against
        // this (as-yet unresolved) import — the spec condition for embargoing
        // the handoff Accept (rpc.capnp:885-888). Only calls to imports NOT yet
        // in `resolved_imports` reach here (resolved caps take the fast path
        // above), so this marks exactly the still-in-flight-promise targets. The
        // mark is dropped implicitly when the question leaves the table.
        if (self.questions.getPtr(question_id)) |q| {
            q.target_promise_import = target_id;
        }
        return question_id;
    }

    /// Send a forwarded L3 vine call to a plain import (issue #56). Mirrors the
    /// import path of `sendCall` but allocates the question with
    /// `restore_on_return_error = false` (`allocateQuestionNoRestore`): the return
    /// callback (`forwardVineReturn`) frees the relay ctx, so a restore after a
    /// post-callback error would re-reference freed memory. It deliberately skips
    /// `sendCall`'s resolved-import fast path and the `target_promise_import` mark:
    /// the forward targets B's own (non-promise, non-embargoed) import of VatC's
    /// cap and must not be treated as an in-flight promise for auto-pickup.
    fn sendForwardedVineCall(
        self: *Peer,
        target_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;
        return peer_call_sender.sendCallToImport(
            Peer,
            CallBuildFn,
            QuestionCallback,
            self.allocator,
            &self.caps,
            self,
            onOutboundCap,
            rollbackOutboundCap,
            self,
            target_id,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            Peer.allocateQuestionNoRestore,
            Peer.removeQuestion,
            Peer.recordQuestionParamExports,
            Peer.sendBuilder,
        );
    }

    /// Forward a fallback call to the exact target named by the original
    /// Provide. The promised-answer branch deliberately uses the no-restore
    /// allocator for the same callback-owns-context invariant as the imported
    /// branch above.
    fn sendForwardedVineCallTarget(
        self: *Peer,
        target: provide_forward_target.ForwardTarget,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;
        return switch (target) {
            .imported => |target_id| self.sendForwardedVineCall(
                target_id,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
            ),
            .promised => |promised| peer_call_sender.sendCallPromisedWithOps(
                Peer,
                CallBuildFn,
                QuestionCallback,
                self.allocator,
                &self.caps,
                self,
                onOutboundCap,
                rollbackOutboundCap,
                self,
                promised.question_id,
                promised.ops,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                Peer.allocateQuestionNoRestore,
                Peer.removeQuestion,
                Peer.recordQuestionParamExports,
                Peer.sendBuilder,
            ),
        };
    }

    /// Send a call to a resolved (non-promise) capability. Dispatches to the
    /// appropriate path based on the resolved cap type (imported, exported, or promised).
    pub fn sendCallResolved(
        self: *Peer,
        target: cap_table.ResolvedCap,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        return self.sendCallResolvedWithOptions(target, interface_id, method_id, ctx, build, on_return, .{});
    }

    pub fn sendCallResolvedWithOptions(
        self: *Peer,
        target: cap_table.ResolvedCap,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
    ) !u32 {
        return self.sendCallResolvedWithOptionsRestore(
            target,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            options,
            true,
        );
    }

    fn sendCallResolvedWithOptionsRestore(
        self: *Peer,
        target: cap_table.ResolvedCap,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
        restore_on_return_error: bool,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;
        const allocate_question: *const fn (*Peer, *anyopaque, QuestionCallback) anyerror!u32 = switch (options.result_lifetime) {
            .automatic => if (restore_on_return_error) Peer.allocateQuestion else Peer.allocateQuestionNoRestore,
            .retained => Peer.allocateRetainedQuestion,
        };
        return switch (target) {
            .imported => |cap| peer_call_sender.sendCallToImport(
                Peer,
                CallBuildFn,
                QuestionCallback,
                self.allocator,
                &self.caps,
                self,
                onOutboundCap,
                rollbackOutboundCap,
                self,
                cap.id,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                allocate_question,
                Peer.removeQuestion,
                Peer.recordQuestionParamExports,
                Peer.sendBuilder,
            ),
            .promised => |promised| self.sendCallPromisedWithOptionsRestore(
                promised,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                options,
                restore_on_return_error,
            ),
            .exported => |cap| blk: {
                try ensureCountLimit(false, self.loopback_questions.count(), self.limits.max_loopback_questions);
                const allocate_loopback_question: *const fn (*Peer, *anyopaque, QuestionCallback) anyerror!u32 = switch (options.result_lifetime) {
                    .automatic => if (restore_on_return_error) Peer.allocateLoopbackQuestion else Peer.allocateLoopbackQuestionNoRestore,
                    .retained => Peer.allocateRetainedLoopbackQuestion,
                };
                break :blk peer_call_sender.sendCallToExport(
                    Peer,
                    Question,
                    CallBuildFn,
                    QuestionCallback,
                    self.allocator,
                    &self.caps,
                    self,
                    onOutboundCap,
                    rollbackOutboundCap,
                    self,
                    &self.questions,
                    &self.loopback_questions,
                    cap.id,
                    interface_id,
                    method_id,
                    ctx,
                    build,
                    on_return,
                    allocate_loopback_question,
                    Peer.removeQuestion,
                    Peer.handleLoopbackFrame,
                );
            },
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
        return self.sendCallPromisedWithOptions(promised, interface_id, method_id, ctx, build, on_return, .{});
    }

    pub fn sendCallPromisedWithOptions(
        self: *Peer,
        promised: protocol.PromisedAnswer,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
    ) !u32 {
        return self.sendCallPromisedWithOptionsRestore(
            promised,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            options,
            true,
        );
    }

    fn sendCallPromisedWithOptionsRestore(
        self: *Peer,
        promised: protocol.PromisedAnswer,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
        restore_on_return_error: bool,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;
        const allocate_question: *const fn (*Peer, *anyopaque, QuestionCallback) anyerror!u32 = switch (options.result_lifetime) {
            .automatic => if (restore_on_return_error) Peer.allocateQuestion else Peer.allocateQuestionNoRestore,
            .retained => Peer.allocateRetainedQuestion,
        };
        return peer_call_sender.sendCallPromised(
            Peer,
            CallBuildFn,
            QuestionCallback,
            self.allocator,
            &self.caps,
            self,
            onOutboundCap,
            rollbackOutboundCap,
            self,
            promised,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            allocate_question,
            Peer.removeQuestion,
            Peer.recordQuestionParamExports,
            Peer.sendBuilder,
        );
    }

    /// Like `sendCallPromised` but takes a question ID and a slice of
    /// `PromisedAnswerOp` directly, avoiding the need to build a reader-backed
    /// `PromisedAnswer`. Used by generated pipeline code.
    pub fn sendCallPromisedWithOps(
        self: *Peer,
        question_id_target: u32,
        ops: []const protocol.PromisedAnswerOp,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        return self.sendCallPromisedWithOpsWithOptions(
            question_id_target,
            ops,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            .{},
        );
    }

    pub fn sendCallPromisedWithOpsWithOptions(
        self: *Peer,
        question_id_target: u32,
        ops: []const protocol.PromisedAnswerOp,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
    ) !u32 {
        return self.sendCallPromisedWithOpsWithOptionsRestore(
            question_id_target,
            ops,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            options,
            true,
        );
    }

    /// Generator plumbing for pipelined typed calls with heap-owned contexts;
    /// see `sendCallGeneratedWithOptions`.
    pub fn sendCallPromisedWithOpsGeneratedWithOptions(
        self: *Peer,
        question_id_target: u32,
        ops: []const protocol.PromisedAnswerOp,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
    ) !u32 {
        return self.sendCallPromisedWithOpsWithOptionsRestore(
            question_id_target,
            ops,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            options,
            false,
        );
    }

    fn sendCallPromisedWithOpsWithOptionsRestore(
        self: *Peer,
        question_id_target: u32,
        ops: []const protocol.PromisedAnswerOp,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
        restore_on_return_error: bool,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;
        const allocate_question: *const fn (*Peer, *anyopaque, QuestionCallback) anyerror!u32 = switch (options.result_lifetime) {
            .automatic => if (restore_on_return_error) Peer.allocateQuestion else Peer.allocateQuestionNoRestore,
            .retained => Peer.allocateRetainedQuestion,
        };
        return peer_call_sender.sendCallPromisedWithOps(
            Peer,
            CallBuildFn,
            QuestionCallback,
            self.allocator,
            &self.caps,
            self,
            onOutboundCap,
            rollbackOutboundCap,
            self,
            question_id_target,
            ops,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            allocate_question,
            Peer.removeQuestion,
            Peer.recordQuestionParamExports,
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
            .inbound_caps = try inbound_caps.clone(),
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
            third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
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
            q.restore_on_return_error = false;
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

    /// Send the tail-Finish for a forwarded question (relayed from the upstream
    /// caller's Finish) and neutralize that forwarded question so any LATE
    /// forwarded return is absorbed cleanly rather than emitting a spurious
    /// Return to the now-finished upstream question.
    ///
    /// Both forwarding modes that hold the forwarded question open until the
    /// upstream Finish route their forwarded Finish through here
    /// (`forwarded_tail_questions`, see finishForwardResolvedCall):
    ///   - `.sent_elsewhere` (spec tail-call): the late return is a bare
    ///     `resultsSentElsewhere` marker — harmless either way.
    ///   - `.translate_to_caller` (W1 reflected loopback): the late return
    ///     carries REAL results that `onForwardedReturn` would otherwise
    ///     translate onto the upstream answer — but the upstream caller already
    ///     Finished, so that Return would be spurious. Neutralizing here frees
    ///     the ForwardCallContext, drops the forwarded_questions mapping, and
    ///     marks the question cancelled so `handleReturn` absorbs the late
    ///     return via its cancelled-question path without emitting or leaking.
    ///
    /// In the common (non-race) order the forwarded return already arrived and
    /// removed the forwarded question before this Finish relay fires, so the
    /// neutralize step is a no-op beyond sending the Finish.
    fn sendTailFinishAndNeutralize(self: *Peer, tail_question_id: u32, release_result_caps: bool) !void {
        try peer_outbound_control.sendFinishWithFlagsViaSendFrame(
            Peer,
            self,
            tail_question_id,
            release_result_caps,
            false,
            Peer.sendFrameControl,
        );
        if (self.questions.getPtr(tail_question_id)) |question| {
            if (question.deinit_ctx) |deinit_ctx| {
                deinit_ctx(self.allocator, question.ctx);
                question.deinit_ctx = null;
            }
            question.cancelled = true;
        }
        _ = self.forwarded_questions.remove(tail_question_id);
    }

    fn neutralizeJoinRelayQuestion(self: *Peer, question_id: u32) void {
        if (self.questions.getPtr(question_id)) |question| {
            if (question.deinit_ctx) |deinit_ctx| {
                deinit_ctx(self.allocator, question.ctx);
                question.deinit_ctx = null;
            }
            question.cancelled = true;
        }
    }

    fn sendJoinRelayFinishAndNeutralize(self: *Peer, question_id: u32, release_result_caps: bool) !void {
        try peer_outbound_control.sendFinishWithFlagsViaSendFrame(
            Peer,
            self,
            question_id,
            release_result_caps,
            false,
            Peer.sendFrameControl,
        );
        self.neutralizeJoinRelayQuestion(question_id);
    }

    fn buildForwardedCall(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
        const ctx: *const ForwardCallContext = castCtx(*const ForwardCallContext, ctx_ptr);
        try third_party.applyForwardedCallSendResults(
            Peer,
            ctx.peer,
            call_builder,
            ctx.send_results_to,
            ctx.send_results_to_third_party_payload,
            third_party.setForwardedCallThirdPartyFromPayloadForPeerFn(Peer),
        );

        const payload_builder = try call_builder.payloadTyped();
        try ctx.peer.clonePayloadWithRemappedCaps(
            call_builder.call.builder,
            payload_builder,
            ctx.payload,
            &ctx.inbound_caps,
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
            ctx.inbound_caps.deinit();
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
            third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            finish.freeOwnedFrameForPeerFn(Peer),
            ctx.send_results_to_third_party_payload,
            sendReturnTag,
            peer_forward_orchestration.lookupForwardedQuestionForPeerFn(Peer),
            sendReturnTakeFromOtherQuestion,
            sendReturnAcceptFromThirdParty,
        );
    }

    fn buildForwardedReturn(ctx_ptr: *anyopaque, ret_builder: *protocol.ReturnBuilder) anyerror!void {
        const ctx: *const ForwardReturnBuildContext = castCtx(*const ForwardReturnBuildContext, ctx_ptr);
        const payload_builder = try ret_builder.payloadTyped();
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
        payload_builder: protocol.PayloadBuilder,
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
    ) !?payload_remap.RemappedCap {
        const entry = try inbound_caps.get(cap_index);
        // Re-encode each forwarded cap under its KNOWN origin: an inbound import
        // (a cap the far peer hosts) forwards back as receiverHosted, an inbound
        // export (one of our own) as senderHosted, and a promised answer as a
        // freshly-noted receiverAnswer. The origin travels with the id so the
        // outbound encoder never re-guesses the id space — which is ambiguous
        // when a remote-forced export/import id collision exists.
        const descriptors = cap_table.descriptors;
        return switch (entry) {
            .none => null,
            .imported => |cap| .{ .origin_code = descriptors.originCodeForTag(.receiverHosted), .cap_id = cap.id },
            // An inbound .exported cap is one of our own exports; it may be an
            // unresolved export PROMISE (senderPromise), which must be preserved
            // so the peer still expects the later Resolve. classifyCap made this
            // distinction (isExportPromise before hasExport); do the same here.
            .exported => |cap| .{ .origin_code = descriptors.originCodeForTag(self.exportedCapTag(cap.id)), .cap_id = cap.id },
            .promised => |promised| .{
                .origin_code = descriptors.originCodeForTag(.receiverAnswer),
                .cap_id = try self.caps.noteReceiverAnswer(promised),
            },
        };
    }

    /// The outbound descriptor variant for one of our own exports: senderPromise
    /// for an unresolved export promise, senderHosted otherwise. Mirrors
    /// classifyCap's isExportPromise-before-hasExport ordering so the
    /// origin-carrying forward/provide paths match the app-authored path.
    fn exportedCapTag(self: *Peer, cap_id: u32) protocol.CapDescriptorTag {
        return if (self.caps.isExportPromise(cap_id)) .senderPromise else .senderHosted;
    }

    pub fn addCrossPeerProxyExport(
        self: *Peer,
        source_peer: *Peer,
        target: cap_table.ResolvedCap,
        release_source_import_id: ?u32,
        release_source_export_pin_id: ?u32,
        release_source_import_pin_id: ?u32,
    ) !u32 {
        self.assertThreadAffinity();
        // OWNERSHIP: every source-peer lease (the retained import ref, the
        // handoff export pin, and the handoff import pin) transfers to this
        // call — released EXACTLY ONCE on any failure. Three disjoint
        // custodians, tracked so no two can both fire:
        //   - before the ctx exists (`leases_transferred == false`): the
        //     errdefer's manual arm releases the raw leases;
        //   - while the ctx exists un-consumed (`proxy_ctx != null`): the
        //     errdefer deinits the ctx, which releases them;
        //   - after a failed registerCrossPeerProxy: the destroy sweep below
        //     consumes the ctx (its deinit releases them) — the errdefer must
        //     then release NOTHING, which is what `leases_transferred`
        //     staying true guarantees. (Without it, the manual arm fired a
        //     SECOND release here and stole a coexisting provision pin.)
        // The caller must NEVER roll any lease back after invoking.
        var proxy_ctx: ?*CrossPeerProxyContext = null;
        var leases_transferred = false;
        errdefer {
            if (proxy_ctx) |ctx| {
                CrossPeerProxyContext.deinit(self.allocator, ctx);
            } else if (!leases_transferred) {
                if (release_source_import_id) |import_id| {
                    source_peer.releaseImport(import_id, 1) catch |err| {
                        log.debug("cross-peer proxy: failed to release source import {} after allocation failure: {}", .{ import_id, err });
                    };
                }
                if (release_source_export_pin_id) |pin_id| {
                    source_peer.releaseHandoffHeldExport(pin_id);
                }
                if (release_source_import_pin_id) |pin_id| {
                    source_peer.releaseHandoffImportPin(pin_id) catch |err| {
                        log.debug("cross-peer proxy: failed to release source import handoff pin {} after allocation failure: {}", .{ pin_id, err });
                    };
                }
            }
        }

        const ctx = try self.allocator.create(CrossPeerProxyContext);
        proxy_ctx = ctx;
        leases_transferred = true;
        ctx.* = .{
            .owner_peer = self,
            .source_peer = source_peer,
            .target = target,
            .release_source_import_id = release_source_import_id,
            .release_source_export_pin_id = release_source_export_pin_id,
            .release_source_import_pin_id = release_source_import_pin_id,
        };

        const id = try self.addExportWithDeinit(
            .{ .ctx = ctx, .on_call = CrossPeerProxyContext.onCall },
            CrossPeerProxyContext.deinit,
        );
        ctx.export_id = id;
        source_peer.registerCrossPeerProxy(self, id) catch |err| {
            // The destroy sweep runs the ctx deinit — the leases' single
            // release. Null the ctx so the errdefer arm fires neither branch.
            proxy_ctx = null;
            self.destroyUnreferencedProxyExport(id);
            return err;
        };
        proxy_ctx = null;
        return id;
    }

    pub fn destroyUnreferencedProxyExport(self: *Peer, id: u32) void {
        self.destroyUnreferencedExport(id);
    }

    fn destroyUnreferencedExport(self: *Peer, id: u32) void {
        const entry = self.exports.get(id) orelse return;
        if (entry.ref_count != 0 or entry.answer_ref_count != 0 or entry.promise_ref_count != 0 or entry.handoff_ref_count != 0) return;

        const removed = self.exports.fetchRemove(id) orelse return;
        self.caps.clearExport(id);
        if (removed.value.deinit_ctx) |deinit_ctx| {
            if (removed.value.handler) |handler| deinit_ctx(self.allocator, handler.ctx);
        }
    }

    fn clonePayloadAcrossPeers(
        self: *Peer,
        builder: *message.MessageBuilder,
        payload_builder: protocol.PayloadBuilder,
        source: protocol.Payload,
        inbound_peer: *Peer,
        inbound_caps: *cap_table.InboundCapTable,
        created_proxy_ids: *std.ArrayList(u32),
        pin_source_caps: bool,
    ) !void {
        var map_ctx = CrossPeerCapMapContext.init(inbound_peer, self, inbound_caps, created_proxy_ids, pin_source_caps);
        defer map_ctx.deinit();
        try payload_remap.clonePayloadWithRemappedCaps(
            CrossPeerCapMapContext,
            self.allocator,
            &map_ctx,
            builder,
            payload_builder,
            source,
            inbound_caps,
            mapCrossPeerInboundCap,
        );
    }

    fn mapCrossPeerInboundCap(
        ctx: *CrossPeerCapMapContext,
        _: *const cap_table.InboundCapTable,
        cap_index: u32,
    ) !?payload_remap.RemappedCap {
        if (ctx.remapped_by_index.get(cap_index)) |proxy_id| {
            return .{
                .origin_code = cap_table.descriptors.originCodeForTag(.senderHosted),
                .cap_id = proxy_id,
            };
        }

        const entry = try ctx.inbound_caps.get(cap_index);
        if (entry == .none) return null;

        var release_source_import_id: ?u32 = null;
        var release_source_export_pin_id: ?u32 = null;
        var release_source_import_pin_id: ?u32 = null;
        if (ctx.pin_source_caps) {
            switch (entry) {
                .exported => |cap| {
                    try ctx.inbound_peer.noteHandoffExportRef(cap.id);
                    release_source_export_pin_id = cap.id;
                },
                .imported => |cap| {
                    try ctx.inbound_peer.noteHandoffImportPin(cap.id);
                    release_source_import_pin_id = cap.id;
                },
                // A receiverAnswer's transform reader borrows the temporary
                // source Return. Supporting it requires an owned transform in
                // CrossPeerProxyContext; fail before publishing a proxy rather
                // than retaining a dangling reader.
                .promised => return error.RedirectedPromisedCapabilityUnsupported,
                .none => return null,
            }
        } else switch (entry) {
            .imported => |cap| {
                try ctx.inbound_caps.retainIndex(cap_index);
                release_source_import_id = cap.id;
            },
            else => {},
        }

        var pins_transferred = false;
        errdefer if (!pins_transferred) {
            if (release_source_export_pin_id) |id| ctx.inbound_peer.rollbackHandoffExportRef(id);
            if (release_source_import_pin_id) |id| ctx.inbound_peer.rollbackHandoffImportPin(id);
        };
        // addCrossPeerProxyExport owns every supplied lease on success AND
        // failure; no caller-side rollback is legal past this point.
        pins_transferred = true;

        const proxy_id = try ctx.outbound_peer.addCrossPeerProxyExport(
            ctx.inbound_peer,
            entry,
            release_source_import_id,
            release_source_export_pin_id,
            release_source_import_pin_id,
        );
        errdefer ctx.outbound_peer.destroyUnreferencedProxyExport(proxy_id);
        try ctx.created_proxy_ids.append(ctx.outbound_peer.allocator, proxy_id);
        try ctx.remapped_by_index.put(cap_index, proxy_id);

        return .{
            .origin_code = cap_table.descriptors.originCodeForTag(.senderHosted),
            .cap_id = proxy_id,
        };
    }

    fn forwardCrossPeerProxyCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        forward_peer: *Peer,
        target: cap_table.ResolvedCap,
    ) !void {
        if (target == .none) {
            try self.sendReturnException(call.question_id, "cross-peer proxy target unavailable");
            return;
        }

        const relay = try forward_peer.allocator.create(CrossPeerProxyCallContext);
        relay.* = .{
            .recipient_peer = self,
            .recipient_answer_id = call.question_id,
            .forward_peer = forward_peer,
            .target = target,
            .source_params = call.params,
            .source_inbound_caps = @constCast(inbound_caps),
        };
        var relay_owned = true;
        errdefer if (relay_owned) CrossPeerProxyCallContext.deinit(forward_peer.allocator, relay);

        var forward_settled = false;
        relay.settled_flag = &forward_settled;

        const forwarded_question_id = forward_peer.sendCrossPeerProxyResolvedCall(
            target,
            call.interface_id,
            call.method_id,
            relay,
            buildCrossPeerProxyCall,
            onCrossPeerProxyReturn,
        ) catch |err| {
            if (forward_settled) {
                relay_owned = false;
                return;
            }
            relay_owned = false;
            CrossPeerProxyCallContext.deinit(forward_peer.allocator, relay);
            self.sendReturnException(call.question_id, @errorName(err)) catch |send_err| {
                log.debug("cross-peer proxy: failed to fail question {}: {}", .{ call.question_id, send_err });
            };
            return;
        };

        if (forward_settled) {
            relay_owned = false;
            return;
        }
        relay.param_proxies_committed = true;
        relay.settled_flag = null;
        if (forward_peer.questions.getPtr(forwarded_question_id)) |q| {
            q.deinit_ctx = CrossPeerProxyCallContext.deinit;
            q.restore_on_return_error = false;
        }
        relay_owned = false;
    }

    fn sendCrossPeerProxyResolvedCall(
        self: *Peer,
        target: cap_table.ResolvedCap,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        return switch (target) {
            .imported => |cap| self.sendForwardedVineCall(cap.id, interface_id, method_id, ctx, build, on_return),
            .exported, .promised => self.sendCallResolved(target, interface_id, method_id, ctx, build, on_return),
            .none => error.CapabilityUnavailable,
        };
    }

    fn buildCrossPeerProxyCall(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
        const ctx: *CrossPeerProxyCallContext = castCtx(*CrossPeerProxyCallContext, ctx_ptr);
        const payload_builder = try call_builder.payloadTyped();
        try ctx.forward_peer.clonePayloadAcrossPeers(
            call_builder.call.builder,
            payload_builder,
            ctx.source_params,
            ctx.recipient_peer,
            ctx.source_inbound_caps,
            &ctx.created_param_proxy_ids,
            false,
        );
    }

    fn onCrossPeerProxyReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *CrossPeerProxyCallContext = castCtx(*CrossPeerProxyCallContext, ctx_ptr);
        const recipient = ctx.recipient_peer;
        const answer_id = ctx.recipient_answer_id;
        const release_param_caps = ctx.created_param_proxy_ids.items.len == 0;
        if (ctx.settled_flag) |flag| flag.* = true;
        defer CrossPeerProxyCallContext.deinit(peer.allocator, ctx);

        relayReturnAcrossPeers(recipient, answer_id, peer, ret, inbound_caps, release_param_caps) catch |err| {
            log.debug("cross-peer proxy return relay failed for question {}: {}", .{ answer_id, err });
        };
    }

    fn relayReturnAcrossPeers(
        recipient: *Peer,
        answer_id: u32,
        source_peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
        release_param_caps: bool,
    ) !void {
        switch (ret.tag) {
            .results => {
                const payload = ret.results orelse {
                    try recipient.sendReturnException(answer_id, "cross-peer forwarded call: missing results");
                    return;
                };
                var results_ctx = CrossPeerReturnRelayContext{
                    .source_peer = source_peer,
                    .target_peer = recipient,
                    .source = payload,
                    .source_inbound_caps = @constCast(inbound_caps),
                    .release_param_caps = release_param_caps,
                };
                defer results_ctx.deinit(recipient.allocator);
                try recipient.sendReturnResults(answer_id, &results_ctx, buildCrossPeerReturnResults);
                results_ctx.result_proxies_committed = true;
            },
            .exception => {
                const reason = if (ret.exception) |e| e.reason else "cross-peer forwarded call failed";
                // Relay the origin's type verbatim: a `disconnected` upstream
                // must not reach the caller as a generic `failed`, or the
                // caller cannot tell a retryable loss from an application error.
                const ex_type = if (ret.exception) |e| e.kind() else protocol.ExceptionType.failed;
                try recipient.sendReturnExceptionTyped(answer_id, reason, ex_type);
            },
            else => {
                try recipient.sendReturnException(answer_id, "cross-peer forwarded call: unexpected return");
            },
        }
    }

    fn buildCrossPeerReturnResults(ctx_ptr: *anyopaque, ret_builder: *protocol.ReturnBuilder) anyerror!void {
        const ctx: *CrossPeerReturnRelayContext = castCtx(*CrossPeerReturnRelayContext, ctx_ptr);
        ret_builder.setReleaseParamCaps(ctx.release_param_caps);
        const payload_builder = try ret_builder.payloadTyped();
        try ctx.target_peer.clonePayloadAcrossPeers(
            ret_builder.ret.builder,
            payload_builder,
            ctx.source,
            ctx.source_peer,
            ctx.source_inbound_caps,
            &ctx.created_result_proxy_ids,
            ctx.pin_source_caps,
        );
    }

    /// Interpret a locally-authored outbound payload's encoded descriptor table
    /// from the source peer's point of view. Unlike InboundCapTable.init, this
    /// does not take wire import refs: senderHosted is one of our exports and
    /// receiverHosted is one of our imports. The table exists only while the
    /// payload is cloned into cross-peer proxy exports.
    fn sourceResolvedCapsForPayload(self: *Peer, payload: protocol.Payload) !cap_table.InboundCapTable {
        const count: u32 = if (payload.cap_table) |list| list.len() else 0;
        var inbound = cap_table.InboundCapTable{
            .allocator = self.allocator,
            .entries = try self.allocator.alloc(cap_table.ResolvedCap, count),
            .retained = undefined,
        };
        errdefer self.allocator.free(inbound.entries);
        inbound.retained = try self.allocator.alloc(bool, count);
        errdefer self.allocator.free(inbound.retained);
        @memset(inbound.retained, false);

        const list = payload.cap_table orelse return inbound;
        var idx: u32 = 0;
        while (idx < count) : (idx += 1) {
            const descriptor = try protocol.CapDescriptor.fromReader(try list.get(idx));
            inbound.entries[idx] = switch (descriptor.tag) {
                .none => .none,
                .senderHosted, .senderPromise => .{ .exported = .{
                    .id = descriptor.id orelse return error.MissingCapDescriptorId,
                } },
                .receiverHosted => .{ .imported = .{
                    .id = descriptor.id orelse return error.MissingCapDescriptorId,
                } },
                .receiverAnswer => .{ .promised = descriptor.promised_answer orelse
                    return error.MissingPromisedAnswer },
                .thirdPartyHosted => .{ .exported = .{
                    .id = (descriptor.third_party orelse
                        return error.MissingThirdPartyCapDescriptor).vine_id,
                } },
            };
        }
        return inbound;
    }

    fn detachSettledAutomaticThirdPartyTarget(route: *AutomaticThirdPartyRoute) void {
        if (route.target_peer) |target| {
            if (target.incoming_automatic_third_party_routes.get(route.target_answer_id) == route) {
                _ = target.incoming_automatic_third_party_routes.remove(route.target_answer_id);
            }
        }
        route.target_peer = null;
        route.target_outcome = .settled;
    }

    fn completeAutomaticThirdPartySourceMarker(
        self: *Peer,
        route: *AutomaticThirdPartyRoute,
    ) !void {
        self.sendReturnResultsSentElsewhere(route.source_answer_id) catch |err| {
            route.operation_active = false;
            route.source_marker_failed = true;
            // The target terminal is already visible. Preserve this owned
            // allocation as a tombstone while the handler error unwinds; Call
            // dispatch must not answer the ambiguous send with another Return.
            log.debug("automatic third-party redirect: source settlement send failed after target commit: {}", .{err});
            // The transport send may have delivered the marker before
            // reporting failure. Propagate a classified terminal error so Call
            // dispatch closes/fails the connection without attempting a second
            // Return for this answer id.
            return error.AutomaticThirdPartySourceSettlementFailed;
        };
        route.operation_active = false;
        // A successful marker clears routing through the deferred-clear guard.
        std.debug.assert(route.clear_requested);
        self.finalizeAutomaticThirdPartyRoute(route, null);
    }

    pub fn sendAutomaticThirdPartyResults(
        self: *Peer,
        route: *AutomaticThirdPartyRoute,
        ctx: *anyopaque,
        build: ReturnBuildFn,
    ) !void {
        self.enterAutomaticThirdPartyOperation();
        defer self.leaveAutomaticThirdPartyOperation();
        if (route.operation_active) return error.ThirdPartyRedirectReentrant;
        route.operation_active = true;
        var route_borrowed = true;
        errdefer if (route_borrowed) {
            route.operation_active = false;
        };

        switch (route.target_outcome) {
            .canceled => {
                route_borrowed = false;
                return self.completeAutomaticThirdPartySourceMarker(route);
            },
            .disconnected => {
                route.operation_active = false;
                const answer_id = route.source_answer_id;
                route_borrowed = false;
                self.finalizeAutomaticThirdPartyRoute(route, null);
                try self.sendReturnExceptionTyped(
                    answer_id,
                    "automatic third-party result connection closed",
                    .disconnected,
                );
                return;
            },
            .settled => {
                route.operation_active = false;
                route_borrowed = false;
                return error.ThirdPartyResultsAlreadyDelivered;
            },
            .connected => {},
        }
        const target = route.target_peer orelse {
            route.operation_active = false;
            return error.ThirdPartyResultPeerUnavailable;
        };
        target.enterAutomaticThirdPartyOperation();
        defer target.leaveAutomaticThirdPartyOperation();

        // Build exactly once in the handler/source peer's capability id-space,
        // then encode a temporary cap table whose descriptor variants preserve
        // that space for the cross-peer remapper below.
        var source_builder = protocol.MessageBuilder.init(self.allocator);
        defer source_builder.deinit();
        var source_ret = try source_builder.beginReturn(route.source_answer_id, .results);
        source_ret.setReleaseParamCaps(self.returnReleasesParamCaps(route.source_answer_id));
        try build(ctx, &source_ret);
        var source_effects = cap_table.OutboundCapEffects.init(self.allocator, null, null);
        defer source_effects.deinit();
        _ = try cap_table.encodeReturnPayloadCapsWithEffects(&self.caps, &source_ret, null, &source_effects);

        const source_frame = try source_builder.finish();
        defer self.allocator.free(source_frame);
        var decoded = try protocol.DecodedMessage.init(self.allocator, source_frame);
        defer decoded.deinit();
        const decoded_ret = try decoded.asReturn();
        const source_payload = decoded_ret.results orelse return error.MissingReturnResults;
        var source_caps = try self.sourceResolvedCapsForPayload(source_payload);
        defer source_caps.deinit();

        var relay = CrossPeerReturnRelayContext{
            .source_peer = self,
            .target_peer = target,
            .source = source_payload,
            .source_inbound_caps = &source_caps,
            .release_param_caps = false,
            .pin_source_caps = true,
        };
        defer relay.deinit(target.allocator);

        route.delivering_result = true;
        target.sendReturnResults(route.target_answer_id, &relay, buildCrossPeerReturnResults) catch |err| {
            route.delivering_result = false;
            route.operation_active = false;
            if (route.clear_requested) {
                route_borrowed = false;
                self.finalizeAutomaticThirdPartyRoute(route, null);
            }
            return err;
        };
        route.delivering_result = false;
        relay.result_proxies_committed = true;
        detachSettledAutomaticThirdPartyTarget(route);

        // Source Finish may have arrived synchronously while the target Return
        // was in flight. The target result is committed, but the canceled source
        // answer needs no resultsSentElsewhere marker.
        if (route.clear_requested) {
            route.operation_active = false;
            route_borrowed = false;
            self.finalizeAutomaticThirdPartyRoute(route, null);
            return;
        }
        route_borrowed = false;
        try self.completeAutomaticThirdPartySourceMarker(route);
    }

    pub fn sendAutomaticThirdPartyException(
        self: *Peer,
        route: *AutomaticThirdPartyRoute,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) !void {
        self.enterAutomaticThirdPartyOperation();
        defer self.leaveAutomaticThirdPartyOperation();
        if (route.operation_active) return error.ThirdPartyRedirectReentrant;
        route.operation_active = true;
        var route_borrowed = true;
        errdefer if (route_borrowed) {
            route.operation_active = false;
        };

        switch (route.target_outcome) {
            .connected => {
                const target = route.target_peer orelse {
                    route.operation_active = false;
                    return error.ThirdPartyResultPeerUnavailable;
                };
                target.enterAutomaticThirdPartyOperation();
                defer target.leaveAutomaticThirdPartyOperation();
                route.delivering_result = true;
                target.sendReturnExceptionNoDrain(route.target_answer_id, reason, ex_type) catch |err| {
                    route.delivering_result = false;
                    route.operation_active = false;
                    if (route.clear_requested) {
                        route_borrowed = false;
                        self.finalizeAutomaticThirdPartyRoute(route, null);
                    }
                    return err;
                };
                target.failQueuedPromisedCalls(route.target_answer_id, reason, ex_type);
                route.delivering_result = false;
                detachSettledAutomaticThirdPartyTarget(route);
            },
            .canceled => {},
            .disconnected => {
                route.operation_active = false;
                const answer_id = route.source_answer_id;
                route_borrowed = false;
                self.finalizeAutomaticThirdPartyRoute(route, null);
                try self.sendReturnExceptionNoDrain(answer_id, reason, ex_type);
                self.failQueuedPromisedCalls(answer_id, reason, ex_type);
                return;
            },
            .settled => {},
        }

        if (route.clear_requested) {
            route.operation_active = false;
            route_borrowed = false;
            self.finalizeAutomaticThirdPartyRoute(route, null);
            return;
        }
        route_borrowed = false;
        try self.completeAutomaticThirdPartySourceMarker(route);
    }

    /// Send a return with results for a previously received call.
    // ================= Outbound Return send family ==========================
    //
    // Bodies live in return/peer_return_send.zig, generic over Peer (the
    // JoinCoordinator extraction contract). The thunks below keep every
    // caller-visible name, signature, and hook fn-type on Peer itself.

    const ReturnSendImpl = peer_return_send.ReturnSend(Peer);

    /// Send a Return carrying results for a previously received call. `build`
    /// writes the payload; routing precedence (third-party handoff >
    /// results-sent-elsewhere marker > self-loopback > wire) is applied here.
    /// Body in `return/peer_return_send.zig`.
    pub fn sendReturnResults(self: *Peer, answer_id: u32, ctx: *anyopaque, build: ReturnBuildFn) !void {
        return ReturnSendImpl.sendReturnResults(self, answer_id, ctx, build);
    }

    /// Deliver a stashed self-loopback results frame to the local question
    /// named by a `takeFromOtherQuestion` redirect; body in
    /// `return/peer_return_send.zig`.
    fn deliverStashedLoopbackResults(self: *Peer, target_question_id: u32, frame: []const u8) !void {
        return ReturnSendImpl.deliverStashedLoopbackResults(self, target_question_id, frame);
    }

    /// Send a pre-built return frame, tracking outbound cap refs and recording
    /// the resolved answer; body in `return/peer_return_send.zig`.
    pub fn sendPrebuiltReturnFrame(self: *Peer, ret: protocol.Return, frame: []const u8) !void {
        return ReturnSendImpl.sendPrebuiltReturnFrame(self, ret, frame);
    }

    /// Send a Return carrying an exception (type `.failed`); drains queued
    /// pipelined children. Body in `return/peer_return_send.zig`.
    pub fn sendReturnException(self: *Peer, answer_id: u32, reason: []const u8) !void {
        return ReturnSendImpl.sendReturnException(self, answer_id, reason);
    }

    /// `sendReturnException` carrying an explicit `Exception.Type`; body in
    /// `return/peer_return_send.zig`.
    pub fn sendReturnExceptionTyped(
        self: *Peer,
        answer_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) !void {
        return ReturnSendImpl.sendReturnExceptionTyped(self, answer_id, reason, ex_type);
    }

    /// Send an exception Return without draining queued pipelined children;
    /// body in `return/peer_return_send.zig`.
    fn sendReturnExceptionNoDrain(
        self: *Peer,
        answer_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) !void {
        return ReturnSendImpl.sendReturnExceptionNoDrain(self, answer_id, reason, ex_type);
    }

    /// Planner hook (`planPromisedTarget`): the recorded exception for an
    /// already-failed inbound answer, if any; body in
    /// `return/peer_return_send.zig`.
    fn lookupFailedAnswer(self: *Peer, answer_id: u32) ?peer_call_targets.FailedAnswerView {
        return ReturnSendImpl.lookupFailedAnswer(self, answer_id);
    }

    /// Fail and drain every pipelined call queued against `answer_id`; body in
    /// `return/peer_return_send.zig`.
    fn failQueuedPromisedCalls(
        self: *Peer,
        answer_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) void {
        ReturnSendImpl.failQueuedPromisedCalls(self, answer_id, reason, ex_type);
    }

    fn reportNonfatalError(self: *Peer, err: anyerror) void {
        ReturnSendImpl.reportNonfatalError(self, err);
    }

    /// Post-send commit/rollback of a reserved resolved answer; body in
    /// `return/peer_return_send.zig`.
    fn commitOrRollbackResolvedAnswerAfterSend(
        self: *Peer,
        answer_id: u32,
        frame: []const u8,
        reservation: *?ResolvedAnswerReservation,
    ) void {
        ReturnSendImpl.commitOrRollbackResolvedAnswerAfterSend(self, answer_id, frame, reservation);
    }

    /// Send a return with an empty struct result (streaming auto-ack); body in
    /// `return/peer_return_send.zig`.
    pub fn sendReturnEmptyStruct(self: *Peer, answer_id: u32) !void {
        return ReturnSendImpl.sendReturnEmptyStruct(self, answer_id);
    }

    /// Settle an inbound `sendResultsTo = thirdParty` call after the
    /// application delivered the results itself; body in
    /// `return/peer_return_send.zig`.
    pub fn sendReturnResultsSentElsewhere(self: *Peer, answer_id: u32) !void {
        return ReturnSendImpl.sendReturnResultsSentElsewhere(self, answer_id);
    }

    fn sendReturnTag(self: *Peer, answer_id: u32, tag: protocol.ReturnTag) !void {
        return ReturnSendImpl.sendReturnTag(self, answer_id, tag);
    }

    fn sendReturnTakeFromOtherQuestion(self: *Peer, answer_id: u32, other_question_id: u32) !void {
        return ReturnSendImpl.sendReturnTakeFromOtherQuestion(self, answer_id, other_question_id);
    }

    fn sendReturnAcceptFromThirdParty(self: *Peer, answer_id: u32, await_payload: ?[]const u8) !void {
        return ReturnSendImpl.sendReturnAcceptFromThirdParty(self, answer_id, await_payload);
    }

    fn sendReturnFrameWithLoopback(self: *Peer, answer_id: u32, bytes: []const u8) !void {
        return ReturnSendImpl.sendReturnFrameWithLoopback(self, answer_id, bytes);
    }

    /// Single-capability Return carrying the provided target (origin-tagged);
    /// body in `return/peer_return_send.zig`.
    pub fn sendReturnProvidedTarget(self: *Peer, answer_id: u32, target: *const ProvideTarget) !void {
        return ReturnSendImpl.sendReturnProvidedTarget(self, answer_id, target);
    }

    // ================= L3 vat-wide provision hosting (Experimental) =========
    //
    // The VatC hosting implementation (docs: FINAL-v2 as amended by its
    // adversarial verdict) and its canonical drain/teardown procedure live in
    // provision/peer_provision_hosting.zig and
    // provision/peer_provision_drain.zig, generic over Peer (the
    // JoinCoordinator extraction contract). The thunks below keep every
    // caller-visible name, signature, hook fn-type, and doc on Peer itself.

    const ProvisionHosting = peer_provision_hosting.Hosting(Peer);
    const ProvisionDrain = peer_provision_drain.Drain(Peer);

    /// Install the random source for accept-embargo ids (see `EntropySource`).
    pub fn setEntropySource(self: *Peer, source: EntropySource) void {
        self.assertThreadAffinity();
        self.entropy = source;
    }

    /// Attach this peer to a vat-wide provision index. Preconditions: not
    /// already attached, and no pre-existing per-peer or new-L3 handoff state
    /// (the index must see every provision and pending cross-peer Accept from
    /// the first one). In particular, index-first teardown severs the borrowed
    /// pointer but deliberately leaves active provisions/queues alive; those
    /// records must drain before this peer can join a different index.
    pub fn attachProvisionIndex(self: *Peer, index: *ProvisionIndex) !void {
        return ProvisionHosting.attachProvisionIndex(self, index);
    }

    /// Detach from the provision index. Symmetric precondition: no live
    /// provisions owned by this peer and no queued cross-peer accepts — a
    /// cleanly detached peer can re-attach (its handoff maps are empty).
    pub fn detachProvisionIndex(self: *Peer) !void {
        return ProvisionHosting.detachProvisionIndex(self);
    }

    /// Roll back a handoff pin taken by a ladder that has NOT yet transferred
    /// ownership; body in `provision/peer_provision_hosting.zig`.
    fn rollbackHandoffExportRef(self: *Peer, id: u32) void {
        ProvisionHosting.rollbackHandoffExportRef(self, id);
    }

    /// PHASE A of Provide registration into the vat index; body in
    /// `provision/peer_provision_hosting.zig`.
    fn registerProvisionForProvide(
        self: *Peer,
        idx: *ProvisionIndex,
        provide_question_id: u32,
        adopted: *?*ProvisionIndex.Provision,
    ) !void {
        return ProvisionHosting.registerProvisionForProvide(self, idx, provide_question_id, adopted);
    }

    /// Index-mode Accept path; body in `provision/peer_provision_hosting.zig`.
    fn handleAcceptWithProvisionIndex(self: *Peer, idx: *ProvisionIndex, accept: protocol.Accept) !void {
        return ProvisionHosting.handleAcceptWithProvisionIndex(self, idx, accept);
    }

    /// L9 parked-accept TTL sweep; body in
    /// `provision/peer_provision_hosting.zig`.
    pub fn sweepExpiredParkedAcceptsForProvisionIndex(idx: *ProvisionIndex) usize {
        return ProvisionHosting.sweepExpiredParkedAcceptsForProvisionIndex(idx);
    }

    /// PHASE B of adoption: transition parked accepts a Provide just adopted;
    /// body in `provision/peer_provision_hosting.zig`.
    fn drainAdoptedParkedAccepts(self: *Peer, idx: *ProvisionIndex, prov: *ProvisionIndex.Provision) !void {
        return ProvisionHosting.drainAdoptedParkedAccepts(self, idx, prov);
    }

    /// Release (or pre-mark) one embargo on a provision — the host arm of a
    /// spec-form accept-Disembargo; body in
    /// `provision/peer_provision_hosting.zig`.
    fn releaseProvisionEmbargo(self: *Peer, prov: *ProvisionIndex.Provision, embargo: []const u8) !void {
        return ProvisionHosting.releaseProvisionEmbargo(self, prov, embargo);
    }

    /// FinishOps `clear_pending_accept_question` replacement (same fn type);
    /// body in `provision/peer_provision_hosting.zig`.
    fn clearPendingAcceptQuestionRouted(peer: *Peer, question_id: u32) void {
        ProvisionHosting.clearPendingAcceptQuestionRouted(peer, question_id);
    }

    // -- The canonical drain/teardown procedure ------------------------------
    // (bodies in provision/peer_provision_drain.zig)

    /// Fallible Finish pre-step, called from handleFinish BEFORE the FinishOps
    /// chain; body in `provision/peer_provision_drain.zig`.
    fn detachProvisionForFinish(self: *Peer, question_id: u32) !void {
        return ProvisionDrain.detachProvisionForFinish(self, question_id);
    }

    /// Neutralize step of `deinit` for owned provisions (callback-free); body
    /// in `provision/peer_provision_drain.zig`.
    fn neutralizeProvisionsOnOwnerPeer(self: *Peer) ProvisionDrain.OwnerProvisionTeardown {
        return ProvisionDrain.neutralizeProvisionsOnOwnerPeer(self);
    }

    /// Send-bearing drain phase of `deinit`, after forceCancelAllQuestions;
    /// body in `provision/peer_provision_drain.zig`.
    fn drainClosedProvisionsOnOwnerPeer(self: *Peer, teardown: *ProvisionDrain.OwnerProvisionTeardown) void {
        ProvisionDrain.drainClosedProvisionsOnOwnerPeer(self, teardown);
    }

    /// Holder-peer neutralize: clear this peer's queued/parked cross-peer
    /// accepts; body in `provision/peer_provision_drain.zig`.
    fn detachCrossPeerAcceptsOnHolderPeer(self: *Peer) void {
        ProvisionDrain.detachCrossPeerAcceptsOnHolderPeer(self);
    }

    fn clearSendResultsToThirdPartyPayload(self: *Peer, answer_id: u32) void {
        if (self.send_results_to_third_party.fetchRemove(answer_id)) |entry| {
            if (entry.value) |payload| self.allocator.free(payload);
        }
    }

    fn enterAutomaticThirdPartyOperation(self: *Peer) void {
        self.automatic_third_party_operation_depth += 1;
    }

    /// A Finish can own an automatic route, target one, or (when numeric
    /// answer-id spaces collide) do both at once. Keep exactly the peers whose
    /// route maps/backlinks are borrowed alive across callback-bearing cleanup,
    /// without widening deferred lifecycle behavior to ordinary Finish paths.
    const AutomaticThirdPartyFinishGuards = struct {
        peers: [3]*Peer = undefined,
        len: usize = 0,

        fn add(self: *@This(), peer: *Peer) void {
            for (self.peers[0..self.len]) |existing| {
                if (existing == peer) return;
            }
            std.debug.assert(self.len < self.peers.len);
            self.peers[self.len] = peer;
            self.len += 1;
        }

        fn enter(self: *@This()) void {
            for (self.peers[0..self.len]) |peer| peer.enterAutomaticThirdPartyOperation();
        }

        fn leave(self: *@This()) void {
            var index = self.len;
            while (index != 0) {
                index -= 1;
                self.peers[index].leaveAutomaticThirdPartyOperation();
            }
            self.len = 0;
        }
    };

    const JoinOperationGuards = struct {
        peers: [3]*Peer = undefined,
        len: usize = 0,

        fn add(self: *@This(), peer: *Peer) void {
            for (self.peers[0..self.len]) |existing| {
                if (existing == peer) return;
            }
            std.debug.assert(self.len < self.peers.len);
            self.peers[self.len] = peer;
            self.len += 1;
        }

        fn enter(self: *@This()) void {
            for (self.peers[0..self.len]) |peer| peer.enterJoinOperation();
        }

        fn leave(self: *@This()) void {
            var i = self.len;
            while (i != 0) {
                i -= 1;
                self.peers[i].leaveJoinOperation();
            }
            self.len = 0;
        }
    };

    fn completeDeferredAutomaticThirdPartyLifecycle(self: *Peer) void {
        if (self.automatic_third_party_operation_depth != 0 or
            self.automatic_third_party_dispatch_depth != 0 or
            self.join_operation_depth != 0)
        {
            return;
        }
        if (self.automatic_third_party_close_deferred) {
            self.automatic_third_party_close_deferred = false;
            self.finishTransportClosedNotification();
        }
        if (self.automatic_third_party_deinit_deferred) self.deinit();
    }

    fn leaveAutomaticThirdPartyOperation(self: *Peer) void {
        std.debug.assert(self.automatic_third_party_operation_depth > 0);
        self.automatic_third_party_operation_depth -= 1;
        self.completeDeferredAutomaticThirdPartyLifecycle();
    }

    pub fn enterJoinOperation(self: *Peer) void {
        self.join_operation_depth += 1;
    }

    pub fn leaveJoinOperation(self: *Peer) void {
        std.debug.assert(self.join_operation_depth > 0);
        self.join_operation_depth -= 1;
        self.completeDeferredAutomaticThirdPartyLifecycle();
    }

    fn failAutomaticThirdPartyTargetBestEffort(
        target: *Peer,
        answer_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) void {
        if (target.in_deinit) return;
        if (!target.active_inbound_questions.contains(answer_id)) return;
        if (target.is_shutting_down or target.transport_close_notified) {
            _ = target.active_inbound_questions.remove(answer_id);
            _ = target.finished_early_answers.remove(answer_id);
            return;
        }
        target.enterAutomaticThirdPartyOperation();
        defer target.leaveAutomaticThirdPartyOperation();
        target.sendReturnExceptionNoDrain(answer_id, reason, ex_type) catch |err| {
            log.debug("automatic third-party redirect: failed to settle target answer {}: {}", .{
                answer_id,
                err,
            });
        };
        target.failQueuedPromisedCalls(answer_id, reason, ex_type);
        // A failed best-effort wire send must not keep the synthetic answer
        // locally active forever. The recipient transport will either have
        // seen the terminal or fail independently; this route is retired.
        _ = target.active_inbound_questions.remove(answer_id);
        _ = target.finished_early_answers.remove(answer_id);
    }

    /// Destroy a source-owned automatic route. All backlinks and owned routing
    /// bytes are detached before the optional target exception is emitted, so a
    /// synchronous callback can deinitialize either peer without observing a
    /// half-live route.
    pub fn finalizeAutomaticThirdPartyRoute(
        self: *Peer,
        route: *AutomaticThirdPartyRoute,
        fail_target_reason: ?[]const u8,
    ) void {
        std.debug.assert(!route.operation_active);
        const source_answer_id = route.source_answer_id;
        const target = route.target_peer;
        const target_answer_id = route.target_answer_id;
        const target_was_connected = route.target_outcome == .connected;

        if (self.automatic_third_party_routes.get(source_answer_id) == route) {
            _ = self.automatic_third_party_routes.remove(source_answer_id);
        }
        if (target) |target_peer| {
            if (target_peer.incoming_automatic_third_party_routes.get(target_answer_id) == route) {
                _ = target_peer.incoming_automatic_third_party_routes.remove(target_answer_id);
            }
        }
        route.source_peer = null;
        route.target_peer = null;
        self.clearSendResultsToThirdPartyPayload(source_answer_id);
        self.allocator.destroy(route);

        if (target_was_connected) {
            if (target) |target_peer| {
                if (fail_target_reason) |reason| {
                    failAutomaticThirdPartyTargetBestEffort(
                        target_peer,
                        target_answer_id,
                        reason,
                        .failed,
                    );
                }
            }
        }
    }

    pub fn clearSendResultsToThirdParty(self: *Peer, answer_id: u32) void {
        if (self.automatic_third_party_routes.get(answer_id)) |route| {
            if (route.operation_active) {
                route.clear_requested = true;
                self.clearSendResultsToThirdPartyPayload(answer_id);
                return;
            }
            self.finalizeAutomaticThirdPartyRoute(
                route,
                "automatic third-party redirect canceled before delivering results",
            );
            return;
        }
        self.clearSendResultsToThirdPartyPayload(answer_id);
    }

    /// Target-side Finish hook. A Finish that arrives after ThirdPartyAnswer but
    /// before the result Return cancels only the synthetic recipient answer; it
    /// does not cancel the original call. A Finish reentrant from delivery is
    /// left to normal resolved-answer cleanup.
    fn noteAutomaticThirdPartyTargetFinish(self: *Peer, answer_id: u32) bool {
        const route = self.incoming_automatic_third_party_routes.get(answer_id) orelse return false;
        if (route.target_outcome != .connected) return false;
        // During the ThirdPartyAnswer announcement, Finish means the adopted
        // synthetic answer was canceled before results. During the result send
        // itself it is the normal reentrant Finish-after-Return lifecycle and
        // must leave the route intact until the sender commits its reservation.
        if (route.operation_active and route.delivering_result) return false;
        // Calls already pipelined on this synthetic answer are independent
        // questions and still require their own terminal Returns. Once the
        // parent is canceled no result can resolve their promised targets, so
        // fail and release them before detaching the route.
        self.failQueuedPromisedCalls(
            answer_id,
            automatic_third_party_target_canceled,
            .failed,
        );
        _ = self.incoming_automatic_third_party_routes.remove(answer_id);
        route.target_peer = null;
        route.target_outcome = .canceled;
        return true;
    }

    /// This peer is the target/result connection and is dying. Detach its
    /// borrowed backlinks and mark each source-owned route disconnected. The
    /// source handler's eventual completion turns that state into one caller-
    /// facing exception; retaining source ownership until then also absorbs a
    /// late async handler Return without emitting a second terminal frame.
    fn neutralizeAutomaticThirdPartyRoutesOnTargetPeer(self: *Peer) void {
        var routes = self.incoming_automatic_third_party_routes;
        self.incoming_automatic_third_party_routes = std.AutoHashMap(u32, *AutomaticThirdPartyRoute).init(self.allocator);
        defer routes.deinit();

        var it = routes.valueIterator();
        while (it.next()) |route_ptr| {
            const route = route_ptr.*;
            if (route.target_peer != self) continue;
            _ = self.active_inbound_questions.remove(route.target_answer_id);
            _ = self.finished_early_answers.remove(route.target_answer_id);
            route.target_peer = null;
            route.target_outcome = .disconnected;
        }
    }

    /// This peer owns the source half and is dying. Remove every target
    /// backlink before emitting best-effort terminal exceptions to synthetic
    /// recipient answers.
    fn neutralizeAutomaticThirdPartyRoutesOnSourcePeer(self: *Peer) void {
        var routes = self.automatic_third_party_routes;
        self.automatic_third_party_routes = std.AutoHashMap(u32, *AutomaticThirdPartyRoute).init(self.allocator);
        defer routes.deinit();

        var it = routes.valueIterator();
        while (it.next()) |route_ptr| {
            const route = route_ptr.*;
            const target = route.target_peer;
            const target_answer_id = route.target_answer_id;
            const target_was_connected = route.target_outcome == .connected;
            if (target) |target_peer| {
                if (target_peer.incoming_automatic_third_party_routes.get(target_answer_id) == route) {
                    _ = target_peer.incoming_automatic_third_party_routes.remove(target_answer_id);
                }
            }
            self.clearSendResultsToThirdPartyPayload(route.source_answer_id);
            route.source_peer = null;
            route.target_peer = null;
            self.allocator.destroy(route);

            if (target_was_connected) {
                if (target) |target_peer| {
                    failAutomaticThirdPartyTargetBestEffort(
                        target_peer,
                        target_answer_id,
                        "automatic third-party source connection closed",
                        .disconnected,
                    );
                }
            }
        }
    }

    fn provideTargetsEqual(a: *const ProvideTarget, b: *const ProvideTarget) bool {
        return switch (a.*) {
            .local => |local| switch (b.*) {
                .local => |other_local| local.origin_code == other_local.origin_code and local.cap_id == other_local.cap_id,
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

    pub fn captureAnyPointerPayload(
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
        try ensureCountLimit(
            self.send_results_to_yourself.contains(answer_id),
            self.send_results_to_yourself.count(),
            self.limits.max_send_results_to_yourself,
        );
        try return_routing.noteSendResultsToYourselfForPeer(
            Peer,
            self,
            answer_id,
            clearSendResultsToThirdParty,
        );
    }

    fn beginAutomaticThirdPartyRoute(self: *Peer, answer_id: u32, contact_payload: []const u8) !void {
        self.enterAutomaticThirdPartyOperation();
        defer self.leaveAutomaticThirdPartyOperation();
        if (self.transport_close_notified) return error.TransportClosed;
        const network = self.vat_network orelse return error.NoVatNetwork;

        var contact_msg = try message.Message.initUnvalidated(self.allocator, contact_payload);
        defer contact_msg.deinit();
        const contact = try contact_msg.getRootAnyPointer();

        var introduced = try network.connectToIntroduced(contact);
        defer introduced.deinit(self.allocator);
        const target = introduced.peer;
        target.assertThreadAffinity();
        if (target.is_shutting_down) return error.PeerShuttingDown;
        if (target.transport_close_notified) return error.TransportClosed;
        target.enterAutomaticThirdPartyOperation();
        defer target.leaveAutomaticThirdPartyOperation();

        try ensureCountLimit(
            false,
            target.active_inbound_questions.count(),
            target.limits.max_active_inbound_questions,
        );
        const target_answer_id = try target.allocateUnusedThirdPartyAnswerId();

        // Parse the completion before publishing any cross-peer state. The
        // network owns its encoding; all the peer needs is a reader that stays
        // live through the synchronous ThirdPartyAnswer build.
        var completion_msg = try message.Message.initUnvalidated(self.allocator, introduced.completion);
        defer completion_msg.deinit();
        const completion = try completion_msg.getRootAnyPointer();

        const route = try self.allocator.create(AutomaticThirdPartyRoute);
        var route_owned = true;
        route.* = .{
            .source_peer = self,
            .source_answer_id = answer_id,
            .target_peer = target,
            .target_answer_id = target_answer_id,
        };
        var source_registered = false;
        var target_registered = false;
        var answer_registered = false;
        errdefer {
            if (answer_registered) _ = target.active_inbound_questions.remove(target_answer_id);
            if (target_registered and
                target.incoming_automatic_third_party_routes.get(target_answer_id) == route)
            {
                _ = target.incoming_automatic_third_party_routes.remove(target_answer_id);
            }
            if (source_registered and
                self.automatic_third_party_routes.get(answer_id) == route)
            {
                _ = self.automatic_third_party_routes.remove(answer_id);
            }
            if (route_owned) self.allocator.destroy(route);
        }

        try self.automatic_third_party_routes.put(answer_id, route);
        source_registered = true;
        try target.incoming_automatic_third_party_routes.put(target_answer_id, route);
        target_registered = true;
        // No params crossed onto this connection, so the synthetic answer owes
        // no explicit parameter-cap releases. It otherwise behaves exactly like
        // an ordinary inbound Call answer for promise queueing and Finish.
        try target.active_inbound_questions.put(target_answer_id, false);
        answer_registered = true;

        route.operation_active = true;
        target.sendThirdPartyAnswerWithId(target_answer_id, completion) catch |err| {
            // Unlike a results Return followed by a reentrant Finish, a
            // ThirdPartyAnswer has no protocol acknowledgement that can prove
            // whether an error happened before or after the frame became
            // visible. Roll back our unpublished transaction on every reported
            // send failure. This is load-bearing: the transport owner must
            // treat such an ambiguous send error as terminal, so a recipient
            // that did see the announcement drains its adopted await when that
            // connection closes instead of waiting forever for a Return.
            route.operation_active = false;
            return err;
        };
        route.operation_active = false;

        if (target.automatic_third_party_deinit_deferred) {
            if (target.incoming_automatic_third_party_routes.get(target_answer_id) == route) {
                _ = target.incoming_automatic_third_party_routes.remove(target_answer_id);
            }
            _ = target.active_inbound_questions.remove(target_answer_id);
            route.target_peer = null;
            route.target_outcome = .disconnected;
            target_registered = false;
            answer_registered = false;
        }

        if (self.automatic_third_party_deinit_deferred) {
            source_registered = false;
            target_registered = false;
            answer_registered = false;
            route_owned = false;
            self.finalizeAutomaticThirdPartyRoute(
                route,
                "automatic third-party source connection closed",
            );
            return error.PeerShuttingDown;
        }

        // A synchronous Finish on the source connection may have cleared the
        // redirect while ThirdPartyAnswer was being delivered. Retire the
        // target answer now that the outer send no longer borrows `route`.
        if (route.clear_requested) {
            source_registered = false;
            target_registered = false;
            answer_registered = false;
            route_owned = false;
            self.finalizeAutomaticThirdPartyRoute(
                route,
                "automatic third-party redirect canceled during announcement",
            );
            return;
        }

        source_registered = false;
        target_registered = false;
        answer_registered = false;
        route_owned = false;
    }

    fn noteSendResultsToThirdParty(
        self: *Peer,
        answer_id: u32,
        ptr: ?message.AnyPointerReader,
    ) !void {
        _ = self.send_results_to_yourself.remove(answer_id);

        const payload = try captureAnyPointerPayload(self.allocator, ptr);
        var payload_owned = true;
        errdefer if (payload_owned) {
            if (payload) |bytes| self.allocator.free(bytes);
        };

        try ensureCountLimit(
            self.send_results_to_third_party.contains(answer_id),
            self.send_results_to_third_party.count(),
            self.limits.max_send_results_to_third_party,
        );
        try ensureByteLimit(
            self.sendResultsToThirdPartyBytesExcluding(answer_id),
            optionalPayloadBytes(payload),
            self.limits.max_send_results_to_third_party_bytes,
        );

        const entry = try self.send_results_to_third_party.getOrPut(answer_id);
        if (entry.found_existing) {
            if (entry.value_ptr.*) |existing| self.allocator.free(existing);
        }
        entry.value_ptr.* = payload;
        payload_owned = false;

        if (self.third_party_result_policy == .vat_network) {
            errdefer self.clearSendResultsToThirdPartyPayload(answer_id);
            const contact_payload = payload orelse return error.MissingThirdPartyContact;
            try self.beginAutomaticThirdPartyRoute(answer_id, contact_payload);
        }
    }

    // ================= Cap refcount / release / frame send ==================
    //
    // Bodies live in peer_export_release.zig, generic over Peer (the
    // JoinCoordinator extraction contract). The thunks below keep every
    // caller-visible name — including the FROZEN `releaseImport` — with its
    // exact signature on Peer itself.

    const ExportReleaseImpl = peer_export_release.ExportRelease(Peer);

    /// Release references to an imported capability, sending a Release message
    /// to the remote peer when the reference count drops to zero.
    ///
    /// Error set is intentionally left open (`anyerror`): the Release send
    /// path funnels through `sendFrameControl` -> `sendFrame`, whose
    /// `SendFrameOverride` callback is arbitrary user code. Body in
    /// `peer_export_release.zig`.
    pub fn releaseImport(self: *Peer, import_id: u32, count: u32) anyerror!void {
        return ExportReleaseImpl.releaseImport(self, import_id, count);
    }

    /// Body in `peer_export_release.zig`.
    pub fn sendReleaseForHost(self: *Peer, import_id: u32, count: u32) !void {
        return ExportReleaseImpl.sendReleaseForHost(self, import_id, count);
    }

    /// Body in `peer_export_release.zig`.
    pub fn forgetImportRefsForHost(self: *Peer, import_id: u32, count: u32) !void {
        return ExportReleaseImpl.forgetImportRefsForHost(self, import_id, count);
    }

    /// Body in `peer_export_release.zig`.
    pub fn sendFinishForHost(
        self: *Peer,
        question_id: u32,
        release_result_caps: bool,
        require_early_cancellation: bool,
    ) !void {
        return ExportReleaseImpl.sendFinishForHost(self, question_id, release_result_caps, require_early_cancellation);
    }

    /// Body in `peer_export_release.zig`.
    pub fn sendBuilder(self: *Peer, builder: *protocol.MessageBuilder) !void {
        return ExportReleaseImpl.sendBuilder(self, builder);
    }

    /// Body in `peer_export_release.zig`.
    fn sendBuilderControl(self: *Peer, builder: *protocol.MessageBuilder) !void {
        return ExportReleaseImpl.sendBuilderControl(self, builder);
    }

    /// Body in `peer_export_release.zig`.
    pub fn sendFrameControl(self: *Peer, frame: []const u8) !void {
        return ExportReleaseImpl.sendFrameControl(self, frame);
    }

    /// Body in `peer_export_release.zig`.
    fn sendFrame(self: *Peer, frame: []const u8) !void {
        return ExportReleaseImpl.sendFrame(self, frame);
    }

    /// Body in `peer_export_release.zig`.
    pub fn onOutboundCap(ctx: *anyopaque, tag: protocol.CapDescriptorTag, id: u32) anyerror!void {
        return ExportReleaseImpl.onOutboundCap(ctx, tag, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn rollbackOutboundCap(ctx: *anyopaque, tag: protocol.CapDescriptorTag, id: u32) void {
        return ExportReleaseImpl.rollbackOutboundCap(ctx, tag, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn noteExportRef(self: *Peer, id: u32) !void {
        return ExportReleaseImpl.noteExportRef(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn rollbackExportRef(self: *Peer, id: u32) void {
        return ExportReleaseImpl.rollbackExportRef(self, id);
    }

    /// Body in `peer_export_release.zig`.
    fn noteAnswerExportRef(self: *Peer, id: u32) !void {
        return ExportReleaseImpl.noteAnswerExportRef(self, id);
    }

    /// Body in `peer_export_release.zig`.
    fn rollbackAnswerExportRef(self: *Peer, id: u32) void {
        return ExportReleaseImpl.rollbackAnswerExportRef(self, id);
    }

    /// Body in `peer_export_release.zig`.
    fn notePromiseExportRef(self: *Peer, id: u32) !void {
        return ExportReleaseImpl.notePromiseExportRef(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn noteHandoffExportRef(self: *Peer, id: u32) !void {
        return ExportReleaseImpl.noteHandoffExportRef(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn releaseHandoffHeldExport(self: *Peer, id: u32) void {
        return ExportReleaseImpl.releaseHandoffHeldExport(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn noteHandoffImportPin(self: *Peer, id: u32) !void {
        return ExportReleaseImpl.noteHandoffImportPin(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn rollbackHandoffImportPin(self: *Peer, id: u32) void {
        return ExportReleaseImpl.rollbackHandoffImportPin(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn releaseHandoffImportPin(self: *Peer, id: u32) anyerror!void {
        return ExportReleaseImpl.releaseHandoffImportPin(self, id);
    }

    /// Body in `peer_export_release.zig`.
    fn rollbackPromiseExportRef(self: *Peer, id: u32) void {
        return ExportReleaseImpl.rollbackPromiseExportRef(self, id);
    }

    /// Body in `peer_export_release.zig`.
    fn finalizeExportRelease(self: *Peer, id: u32, promise_target: ?u32, import_target: ?u32) void {
        return ExportReleaseImpl.finalizeExportRelease(self, id, promise_target, import_target);
    }

    /// Body in `peer_export_release.zig`.
    fn releasePromiseHeldCap(self: *Peer, id: u32) !void {
        return ExportReleaseImpl.releasePromiseHeldCap(self, id);
    }

    /// Body in `peer_export_release.zig`.
    fn releaseExport(self: *Peer, id: u32, count: u32) !void {
        return ExportReleaseImpl.releaseExport(self, id, count);
    }

    /// Body in `peer_export_release.zig`.
    pub fn releaseInboundCaps(self: *Peer, inbound: *cap_table.InboundCapTable) !void {
        return ExportReleaseImpl.releaseInboundCaps(self, inbound);
    }

    /// Body in `peer_export_release.zig`.
    fn storeResolvedImport(
        self: *Peer,
        promise_id: u32,
        cap: ?cap_table.ResolvedCap,
        embargo_id: ?u32,
        embargoed: bool,
    ) !void {
        return ExportReleaseImpl.storeResolvedImport(self, promise_id, cap, embargo_id, embargoed);
    }

    /// Body in `peer_export_release.zig`.
    fn rememberPendingEmbargo(self: *Peer, embargo_id: u32, promise_id: u32) !void {
        return ExportReleaseImpl.rememberPendingEmbargo(self, embargo_id, promise_id);
    }

    /// Body in `peer_export_release.zig`.
    fn releaseResolvedImport(self: *Peer, promise_id: u32) anyerror!void {
        return ExportReleaseImpl.releaseResolvedImport(self, promise_id);
    }

    /// Body in `peer_export_release.zig`.
    fn bufferPendingThirdPartyReturn(self: *Peer, answer_id: u32, frame: []const u8) !void {
        return ExportReleaseImpl.bufferPendingThirdPartyReturn(self, answer_id, frame);
    }

    /// Body in `peer_export_release.zig`.
    fn handleMissingReturnQuestion(self: *Peer, frame: []const u8, answer_id: u32) !void {
        return ExportReleaseImpl.handleMissingReturnQuestion(self, frame, answer_id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn releaseResolvedCap(self: *Peer, resolved: cap_table.ResolvedCap) anyerror!void {
        return ExportReleaseImpl.releaseResolvedCap(self, resolved);
    }

    /// Body in `peer_export_release.zig`.
    pub fn deliverLoopbackReturn(self: *Peer, frame: []const u8) !void {
        return ExportReleaseImpl.deliverLoopbackReturn(self, frame);
    }

    /// Body in `peer_export_release.zig`.
    pub fn resolveProvidePromisedOps(self: *Peer, question_id: u32, ops: []const protocol.PromisedAnswerOp) !cap_table.ResolvedCap {
        return ExportReleaseImpl.resolveProvidePromisedOps(self, question_id, ops);
    }

    /// Body in `peer_export_release.zig`.
    pub fn resolvePromisedAnswer(self: *Peer, promised: protocol.PromisedAnswer) !cap_table.ResolvedCap {
        return ExportReleaseImpl.resolvePromisedAnswer(self, promised);
    }

    /// Body in `peer_export_release.zig`.
    pub fn releaseResultCaps(self: *Peer, frame: []const u8) !void {
        return ExportReleaseImpl.releaseResultCaps(self, frame);
    }

    /// Body in `peer_export_release.zig`.
    pub fn releaseAnswerHeldResultCaps(self: *Peer, frame: []const u8) !void {
        return ExportReleaseImpl.releaseAnswerHeldResultCaps(self, frame);
    }

    pub fn allocateQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        return self.allocateQuestionWithRestore(ctx, on_return, true);
    }

    /// Allocate and register a retained question as one atomic pre-send step.
    /// The outbound-question allocator can synchronously notify observers, so
    /// retained admission is checked both before allocation and immediately
    /// before registration. No Call is emitted until registration completes.
    fn allocateRetainedQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        try self.checkRetainedQuestionAdmission();

        // A retained callback is delivered at most once. If it reports an
        // error after observing the Return, the answer remains explicitly
        // finishable, but the question callback is not restored and replayed.
        const question_id = try self.allocateQuestionWithRestore(ctx, on_return, false);
        errdefer self.removeQuestion(question_id);
        try self.registerRetainedQuestion(question_id);
        return question_id;
    }

    fn checkRetainedQuestionAdmission(self: *Peer) !void {
        const retained_count = self.retained_questions.count();
        if (retained_count >= self.limits.max_retained_questions) {
            events.emitResourceRejection(
                self.observer,
                .peer,
                .unknown,
                .retained_questions,
                retained_count +| 1,
                self.limits.max_retained_questions,
                error.PeerLimitExceeded,
            );
            return error.PeerLimitExceeded;
        }
    }

    fn registerRetainedQuestion(self: *Peer, question_id: u32) !void {
        try self.checkRetainedQuestionAdmission();
        const retained_before = self.retained_questions.count();
        const question = self.questions.getPtr(question_id) orelse return error.QuestionClosed;
        question.suppress_auto_finish = true;
        question.restore_on_return_error = false;
        try self.retained_questions.register(question_id);
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .retained_questions,
            retained_before,
            self.retained_questions.count(),
            self.limits.max_retained_questions,
        );
    }

    /// Allocate an outbound question whose `on_return` callback OWNS and FREES
    /// the ctx (so the question must NOT be restored on a post-callback error —
    /// a restored copy would reference a ctx the callback already freed, a UAF at
    /// teardown). Used by `sendForwardedVineCall` (issue #56): its return callback
    /// frees the relay ctx, and in synchronous loopback the return is processed
    /// INSIDE the send, so `restore_on_return_error` must be false from creation —
    /// it cannot be cleared after the send the way async paths do.
    fn allocateQuestionNoRestore(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        return self.allocateQuestionWithRestore(ctx, on_return, false);
    }

    /// Allocate the question id for a REFLECTED (loopback) call — one whose
    /// synthesized `Call` frame `sendCallToExport` feeds straight back into this
    /// peer's own `handleFrame` instead of writing it to the transport.
    ///
    /// On the wire, outbound question ids and inbound answer ids are INDEPENDENT
    /// namespaces: both peers legally start at 0. Reflection merges them — the
    /// id we picked as an outbound question also becomes an inbound answer id in
    /// `active_inbound_questions`/`resolved_answers`. Drawing loopback ids from
    /// `next_question_id` therefore collides with whatever the remote happens to
    /// have open, and `handleCall` rejects the reflected frame with
    /// `DuplicateQuestionId`. Cross-impl contact hit exactly that: the C++
    /// reference held its answer 0 open (awaiting `Finish`) while a cross-peer
    /// proxy reflected its first loopback call, also id 0.
    ///
    /// Loopback ids are drawn from the TOP of the space, descending, skipping
    /// anything live in either namespace. No frame carrying one of these ids is
    /// ever written to a socket, and every implementation (this one included)
    /// hands out wire question ids ascending from 0, so the two stay apart.
    fn allocateLoopbackQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        return self.allocateLoopbackQuestionWithRestore(ctx, on_return, true);
    }

    fn allocateLoopbackQuestionNoRestore(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        return self.allocateLoopbackQuestionWithRestore(ctx, on_return, false);
    }

    fn allocateLoopbackQuestionWithRestore(
        self: *Peer,
        ctx: *anyopaque,
        on_return: QuestionCallback,
        restore_on_return_error: bool,
    ) !u32 {
        const scan_start = self.next_loopback_question_id;
        while (self.questions.contains(self.next_loopback_question_id) or
            self.retained_questions.containsLogicalOrWire(self.next_loopback_question_id) or
            (try self.inboundQuestionIdInUse(self.next_loopback_question_id)))
        {
            self.next_loopback_question_id -%= 1;
            if (self.next_loopback_question_id == scan_start) return error.QuestionIdExhausted;
        }
        const question_id = try self.allocateQuestionFrom(
            &self.next_loopback_question_id,
            ctx,
            on_return,
            restore_on_return_error,
        );
        // The shared allocator advances its cursor upward; walk it back down so
        // loopback ids keep descending from the top of the space.
        self.next_loopback_question_id = question_id -% 1;
        return question_id;
    }

    fn allocateRetainedLoopbackQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        try self.checkRetainedQuestionAdmission();
        const question_id = try self.allocateLoopbackQuestion(ctx, on_return);
        errdefer self.removeQuestion(question_id);
        try self.registerRetainedQuestion(question_id);
        return question_id;
    }

    fn allocateQuestionWithRestore(
        self: *Peer,
        ctx: *anyopaque,
        on_return: QuestionCallback,
        restore_on_return_error: bool,
    ) !u32 {
        return self.allocateQuestionFrom(&self.next_question_id, ctx, on_return, restore_on_return_error);
    }

    fn allocateQuestionFrom(
        self: *Peer,
        cursor: *u32,
        ctx: *anyopaque,
        on_return: QuestionCallback,
        restore_on_return_error: bool,
    ) !u32 {
        const questions_before = self.questions.count();
        try ensureCountLimit(false, questions_before, self.limits.max_outbound_questions);
        var deadline_ns: ?i64 = null;
        var started_ns: ?i64 = null;
        if (self.clockNow()) |now| {
            started_ns = now;
            if (self.timeouts.default_call_timeout_ms) |ms| {
                deadline_ns = now + msToNs(ms);
            }
        }
        const question_id = try peer_question_state.allocateQuestionExcluding(
            Question,
            retained_question_state.Registry,
            &self.questions,
            &self.retained_questions,
            retained_question_state.Registry.containsLogicalOrWire,
            cursor,
            .{
                .ctx = ctx,
                .on_return = on_return,
                .is_loopback = false,
                .deadline_ns = deadline_ns,
                .started_ns = started_ns,
                .restore_on_return_error = restore_on_return_error,
            },
        );
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .outbound_questions,
            questions_before,
            self.questions.count(),
            self.limits.max_outbound_questions,
        );
        return question_id;
    }

    pub fn removeQuestion(self: *Peer, question_id: u32) void {
        if (self.questions.remove(question_id)) {
            _ = self.retained_questions.retireLogicalOrWire(question_id);
            // The question is being discarded without a wire Return (send
            // rollback, loopback cancel, test drain): free any param-export
            // record without spending the refs.
            self.freeQuestionParamExports(question_id);
        }
        if (self.is_shutting_down and !self.in_deinit and self.questions.count() == 0) {
            self.completeShutdown();
        }
    }

    fn removeQuestionAndDeinit(self: *Peer, question_id: u32) void {
        if (self.questions.fetchRemove(question_id)) |removed| {
            if (removed.value.deinit_ctx) |deinit_ctx| {
                deinit_ctx(self.allocator, removed.value.ctx);
            }
            self.freeQuestionParamExports(question_id);
            _ = self.retained_questions.retireLogicalOrWire(question_id);
        }
        if (self.is_shutting_down and !self.in_deinit and self.questions.count() == 0) {
            self.completeShutdown();
        }
    }

    /// Record, under `question_id`, the wire refs an outbound Call's params
    /// just took on our exports (the senderHosted/senderPromise entries of
    /// `effects.callback_applied`, one ref per occurrence), so the inbound
    /// Return can spend them when it carries `releaseParamCaps = true`.
    ///
    /// Called by the wire senders BEFORE the frame is sent: OOM propagates
    /// and the senders' errdefers (question removal — which frees the record
    /// — plus cap-effects rollback) undo everything, so no partial record
    /// can survive a failed send.
    fn recordQuestionParamExports(
        self: *Peer,
        question_id: u32,
        entries: []const cap_table.OutboundEntry,
    ) !void {
        var ids: std.ArrayList(u32) = .empty;
        errdefer ids.deinit(self.allocator);
        for (entries) |entry| {
            switch (entry.tag) {
                .senderHosted, .senderPromise => try ids.append(self.allocator, entry.id),
                else => {},
            }
        }
        if (ids.items.len == 0) {
            ids.deinit(self.allocator);
            return;
        }
        const slot = try self.question_param_export_refs.getOrPut(question_id);
        if (slot.found_existing) {
            // Question ids are never reused while outstanding, so a stale
            // record here is a bookkeeping bug; free it rather than leak.
            log.warn("replacing stale param-export record for question {}", .{question_id});
            slot.value_ptr.deinit(self.allocator);
        }
        slot.value_ptr.* = ids;
    }

    /// Free (without spending) the param-export record of a question that
    /// died without a wire Return — sender rollback, loopback cancel, drain,
    /// or deinit. The still-held refs reconcile at transport teardown,
    /// exactly as they did before records existed.
    fn freeQuestionParamExports(self: *Peer, question_id: u32) void {
        if (self.question_param_export_refs.fetchRemove(question_id)) |removed| {
            var ids = removed.value;
            ids.deinit(self.allocator);
        }
    }

    /// Consume the param-export record for an inbound Return (rpc.capnp
    /// `Return.releaseParamCaps`, which DEFAULTS to true): the receiver of
    /// our call implicitly dropped the refs our param caps took on our
    /// exports instead of sending explicit Release messages. Spend them
    /// through the same `releaseExport` path an inbound Release frame uses
    /// (over-release therefore surfaces as the existing
    /// `error.ReleaseCountExceeded` protocol error, never an underflow).
    /// With `releaseParamCaps = false` the record is just dropped — the
    /// remote will send explicit Releases later. Runs for every inbound
    /// Return, including ones absorbed for locally cancelled questions, so
    /// the refs are spent exactly once per question.
    fn consumeQuestionParamExports(self: *Peer, answer_id: u32, release_param_caps: bool) !void {
        const removed = self.question_param_export_refs.fetchRemove(answer_id) orelse return;
        var ids = removed.value;
        defer ids.deinit(self.allocator);
        if (!release_param_caps) return;
        // Aggregate duplicate occurrences into one count per export id,
        // mirroring how a Release frame would spend them.
        std.mem.sort(u32, ids.items, {}, std.sort.asc(u32));
        var idx: usize = 0;
        while (idx < ids.items.len) {
            const id = ids.items[idx];
            var count: u32 = 0;
            while (idx < ids.items.len and ids.items[idx] == id) : (idx += 1) {
                count += 1;
            }
            try self.releaseExport(id, count);
        }
    }

    /// The `releaseParamCaps` flag for the Return that answers `answer_id`.
    ///
    /// rpc.capnp: with `releaseParamCaps = true` the caller drops the wire refs
    /// its param capabilities took, and "the sender must not send separate
    /// `Release` messages for them". This vat DOES send them — the
    /// post-dispatch auto-release for params the handler ignored, and the
    /// application's own `releaseImport` for ones it kept — so any answer that
    /// took param import refs must say `false`, exactly as the C++ reference
    /// does on every Return it sends. Claiming `true` alongside those frames
    /// releases the caller's export twice; a compliant peer answers that with
    /// "Tried to release invalid export ID" and drops the connection.
    ///
    /// Returns the schema default `true` for every answer with no
    /// `active_inbound_questions` record: Bootstrap/Provide/Accept/Join Returns
    /// (whose messages have no params cap table at all), the `sendResultsTo =
    /// thirdParty` refusal issued before the record exists, and any answer whose
    /// Call params carried no ref-granting descriptor. In all of those the
    /// caller recorded no param exports, so the flag is a no-op either way.
    pub fn returnReleasesParamCaps(self: *Peer, answer_id: u32) bool {
        return !(self.active_inbound_questions.get(answer_id) orelse false);
    }

    fn onConnectionError(self: *Peer, err: anyerror) void {
        log.debug("connection error: {}", .{err});
        if (self.on_error) |cb| cb(self.callback_ctx, self, err);
    }

    /// Deliver terminal transport-close lifecycle exactly once.
    ///
    /// Bound transports reach this path through `onConnectionClose`; manual
    /// integrations such as `HostPeer` call it when their socket reaches EOF
    /// or reset. Holder-side parked and embargo-queued Accept reservations are
    /// detached before the user callback. Provider-owned active provisions
    /// deliberately remain live until Finish or peer deinit, preserving the
    /// disconnect-after-Provide direct-pickup contract.
    pub fn notifyTransportClosed(self: *Peer) void {
        self.assertThreadAffinity();
        if (self.transport_close_notified) return;
        self.transport_close_notified = true;
        if (self.automatic_third_party_operation_depth != 0 or
            self.automatic_third_party_dispatch_depth != 0 or
            self.join_operation_depth != 0)
        {
            self.automatic_third_party_close_deferred = true;
            return;
        }
        self.finishTransportClosedNotification();
    }

    fn finishTransportClosedNotification(self: *Peer) void {
        const guards_automatic_routes = self.automatic_third_party_routes.count() != 0 or
            self.incoming_automatic_third_party_routes.count() != 0;
        if (guards_automatic_routes) self.enterAutomaticThirdPartyOperation();
        defer if (guards_automatic_routes) self.leaveAutomaticThirdPartyOperation();

        log.debug("connection closed", .{});
        self.neutralizeAutomaticThirdPartyRoutesOnTargetPeer();
        self.neutralizeAutomaticThirdPartyRoutesOnSourcePeer();
        self.detachCrossPeerAcceptsOnHolderPeer();
        self.drainOutboundProvidesOnRecipientPeer();
        self.neutralizeForwardVineRelaysOnRecipientPeer();
        self.neutralizeHandoffPickupsOnPromisePeer();
        self.drainIncompleteJoinsOnTransportClose();
        self.detachJoinResultAnswersOnTransportClose();
        // This peer's role as a distinct direct-Accept host is terminal on
        // transport close. Canonical JoinResults owned by this result-path
        // peer are deliberately untouched, preserving direct pickup through a
        // still-live sibling Accept host.
        self.cancelJoinAcceptHostLinks();
        events.emitClose(self.observer, .peer, .unknown, null);
        events.emitConnection(self.observer, .peer, .unknown, .closed);
        // Resolve every still-outstanding question with a synthetic
        // "disconnected" exception Return delivered through its on_return
        // callback, BEFORE the owner's on_close (which typically deinits the
        // peer). Without this a caller awaiting a Return whose transport just
        // dropped hangs forever — a liveness gap and a trivial DoS. The peer's
        // maps are still intact here, so a callback that re-enters the peer is
        // safe; the synthetic Return is delivered only to the question's
        // callback and is never cached as an answer or replayed to pipelining.
        _ = self.forceCancelAllQuestions(disconnected_reason, .disconnected);
        // Completed/transferred retained answers are no longer reachable once
        // the transport is gone. Drop their local ownership records before the
        // user close callback; repeated close and later deinit remain no-ops.
        self.retained_questions.clearRetainingCapacity();
        if (self.on_close) |cb| cb(self.callback_ctx, self);
    }

    fn onConnectionClose(self: *Peer) void {
        self.notifyTransportClosed();
    }

    /// Process a single inbound Cap'n Proto RPC frame.
    ///
    /// Decodes the message tag and dispatches to the appropriate handler
    /// (call, return, finish, resolve, etc.). Unknown message types trigger
    /// an Unimplemented response per the Cap'n Proto RPC spec.
    /// Charge `words` (the traversal words an inbound frame's validation walk
    /// visited) against the per-connection token bucket, refilling by elapsed
    /// time first. Bounds the aggregate validation CPU one connection can drive
    /// with amplifying frames. Inert when the rate is 0 or no Clock is set.
    fn chargeValidationBudget(self: *Peer, words: usize) !void {
        const rate = self.limits.max_validation_words_per_second;
        if (rate == 0) return;
        const now = self.clockNow() orelse return;

        const i64_max: u64 = std.math.maxInt(i64);
        const burst: i64 = @intCast(@min(@as(u64, self.limits.max_validation_burst_words), i64_max));
        if (self.validation_last_refill_ns) |last| {
            const elapsed_ns = now - last;
            if (elapsed_ns > 0) {
                const refill = @divTrunc(@as(i128, elapsed_ns) * @as(i128, @as(u64, rate)), std.time.ns_per_s);
                const refilled = @as(i128, self.validation_tokens) + refill;
                self.validation_tokens = @intCast(@min(refilled, @as(i128, burst)));
            }
        } else {
            // First charged frame: start the bucket full.
            self.validation_tokens = burst;
        }
        self.validation_last_refill_ns = now;

        const cost: i64 = @intCast(@min(@as(u64, words), i64_max));
        if (cost > self.validation_tokens) return error.ValidationBudgetExceeded;
        self.validation_tokens -= cost;
    }

    pub fn handleFrame(self: *Peer, frame: []const u8) !void {
        self.assertThreadAffinity();
        if (self.transport_close_notified) return error.TransportClosed;

        return self.handleFrameImpl(frame);
    }

    /// Dispatch an in-process call to one of this peer's local exports. Local
    /// capabilities deliberately remain callable after transport close (the
    /// L3 disconnect-after-Provide contract), while public/transport ingress
    /// is rejected by `handleFrame` once close has been notified.
    fn handleLoopbackFrame(self: *Peer, frame: []const u8) !void {
        self.assertThreadAffinity();
        return self.handleFrameImpl(frame);
    }

    fn handleFrameImpl(self: *Peer, frame: []const u8) !void {
        self.enterJoinOperation();
        defer self.leaveJoinOperation();
        // Drive vat-wide expiry before decoding or validation. A connection
        // that stays busy — including one sending malformed traffic — must not
        // suppress reclamation merely by avoiding Accept frames or idle ticks.
        if (self.provision_index) |idx| _ = idx.sweepExpiredParkedAccepts();
        _ = self.sweepExpiredJoins();

        events.emitFrame(self.observer, .peer, .unknown, .received, frame.len);

        // Decode with cost accounting so the validation-work budget is charged
        // even when decoding fails. `decode_cost` holds the traversal words the
        // validating walk spent whether it succeeded, hit an unknown message
        // tag after a full walk, or aborted partway on a malformed frame.
        var decode_cost: usize = 0;
        var decoded = protocol.DecodedMessage.initCounting(self.allocator, frame, &decode_cost) catch |err| {
            // Charge the failed frame's validation work FIRST — before the
            // Unimplemented echo (which re-walks and clones the same payload)
            // and before returning — so a hostile peer cannot get unlimited
            // validation CPU, or amplify through the echo, by sending frames
            // that never dispatch.
            self.chargeValidationBudget(decode_cost) catch |budget_err| {
                log.warn("validation-work budget exceeded on undispatchable frame (words={}); aborting connection", .{decode_cost});
                events.emitProtocolError(self.observer, .peer, .unknown, budget_err, null);
                self.sendAbortForError(budget_err);
                if (!self.isAttachedTransportClosing()) self.closeAttachedTransport();
                return budget_err;
            };
            if (err == error.InvalidMessageTag) {
                log.debug("unknown message tag in frame, sending unimplemented", .{});
                events.emitProtocolError(self.observer, .peer, .unknown, err, null);
                try self.sendUnimplementedForFrame(frame);
                return;
            }
            if (err == error.OutOfMemory) {
                log.debug("failed to decode inbound frame (len={}): {}", .{ frame.len, err });
            } else {
                log.warn("failed to decode inbound frame (len={}): {}", .{ frame.len, err });
            }
            events.emitProtocolError(self.observer, .peer, .unknown, err, null);
            self.sendAbortForError(err);
            return err;
        };
        defer decoded.deinit();

        // Charge this frame's validation cost against the per-connection budget.
        // On exhaustion the peer is spending disproportionate CPU on validation
        // amplification: abort and drop the connection.
        self.chargeValidationBudget(decoded.msg.traversal_words_used) catch |err| {
            log.warn("validation-work budget exceeded (frame words={}); aborting connection", .{decoded.msg.traversal_words_used});
            events.emitProtocolError(self.observer, .peer, .unknown, err, null);
            self.sendAbortForError(err);
            if (!self.isAttachedTransportClosing()) self.closeAttachedTransport();
            return err;
        };

        self.last_inbound_tag = decoded.tag;
        log.debug("dispatching inbound {s}", .{@tagName(decoded.tag)});

        const automatic_third_party_dispatch = if (decoded.tag == .call and
            self.third_party_result_policy == .vat_network)
        blk: {
            const call = decoded.asCall() catch break :blk false;
            break :blk call.send_results_to.tag == .thirdParty;
        } else false;
        if (automatic_third_party_dispatch) self.automatic_third_party_dispatch_depth += 1;
        defer if (automatic_third_party_dispatch) {
            std.debug.assert(self.automatic_third_party_dispatch_depth > 0);
            self.automatic_third_party_dispatch_depth -= 1;
            self.completeDeferredAutomaticThirdPartyLifecycle();
        };

        dispatch.dispatchDecodedForPeer(
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
        ) catch |err| {
            if (err == error.OutOfMemory) {
                log.debug("dispatch error for {s}: {}", .{ @tagName(decoded.tag), err });
            } else {
                log.warn("dispatch error for {s}: {}", .{ @tagName(decoded.tag), err });
            }
            switch (err) {
                error.PeerLimitExceeded => events.emitResourceRejection(
                    self.observer,
                    .peer,
                    .unknown,
                    .peer_state,
                    frame.len,
                    null,
                    err,
                ),
                else => events.emitProtocolError(
                    self.observer,
                    .peer,
                    .unknown,
                    err,
                    @tagName(decoded.tag),
                ),
            }
            return err;
        };
    }

    fn sendAbortForError(self: *Peer, err: anyerror) void {
        if (err == error.OutOfMemory) return;
        peer_outbound_control.sendAbortTyped(
            Peer,
            self,
            @errorName(err),
            errors.exceptionTypeForError(err),
            Peer.sendBuilderControl,
        ) catch |abort_err| {
            log.debug("failed to send abort: {}", .{abort_err});
        };
    }

    fn sendUnimplemented(self: *Peer, original: message.AnyPointerReader) !void {
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildUnimplementedFromAnyPointer(original);
        try self.sendBuilderControl(&builder);
    }

    fn sendUnimplementedForFrame(self: *Peer, frame: []const u8) !void {
        var msg = try message.Message.initUnvalidated(self.allocator, frame);
        defer msg.deinit();
        const root = try msg.getRootAnyPointer();
        try self.sendUnimplemented(root);
    }

    fn handleUnimplemented(self: *Peer, unimplemented: protocol.Unimplemented) !void {
        if (unimplemented.message_tag == .resolve) {
            const echoed = unimplemented.resolve orelse return;
            try self.handleUnimplementedResolve(echoed);
            return;
        }
        try bootstrap.handleUnimplemented(
            Peer,
            self,
            unimplemented,
            bootstrap.handleUnimplementedQuestionForPeerFn(
                Peer,
                Peer.handleReturn,
            ),
            peer_outbound_control.sendAbortForPeerFn(Peer, Peer.sendBuilderControl),
        );
    }

    /// The remote echoed one of our Resolve messages back inside an
    /// Unimplemented: it does not implement Resolve (a level-0 peer), so it
    /// never picked up the resolution's cap descriptor. Release the wire
    /// reference that descriptor carried (taken in
    /// resolvePromiseExportToExport) through the same releaseExport path an
    /// inbound Release frame uses; otherwise the target export's ref would
    /// leak for the connection lifetime. Outgoing Resolve descriptors are
    /// only ever senderHosted/senderPromise; other descriptor tags carried no
    /// local export reference, and an echoed resolve-to-exception carried no
    /// cap at all, so both are no-ops. A forged echo naming an export with no
    /// spendable refs fails releaseExport's ReleaseCountExceeded guard, the
    /// same trust boundary a bogus Release frame hits.
    fn handleUnimplementedResolve(self: *Peer, echoed: protocol.Resolve) !void {
        if (echoed.tag != .cap) return;
        const descriptor = echoed.cap orelse return;
        switch (descriptor.tag) {
            .senderHosted, .senderPromise => {
                const id = descriptor.id orelse return;
                try self.releaseExport(id, 1);
            },
            else => {},
        }
    }

    fn handleAbort(self: *Peer, abort: protocol.Abort) !void {
        const stored_len = @min(abort.exception.reason.len, self.limits.max_remote_abort_reason_bytes);
        log.debug("received abort from remote (reason_len={}, stored_len={})", .{
            abort.exception.reason.len,
            stored_len,
        });
        const capped_abort = protocol.Abort{
            .exception = .{
                .reason = abort.exception.reason[0..stored_len],
                .trace = abort.exception.trace,
                .type_value = abort.exception.type_value,
            },
        };
        try bootstrap.handleAbort(self.allocator, &self.last_remote_abort_reason, capped_abort);
    }

    fn handleBootstrap(self: *Peer, bootstrap_msg: protocol.Bootstrap) !void {
        try bootstrap.handleBootstrap(
            Peer,
            self,
            self.allocator,
            bootstrap_msg,
            self.bootstrap_export_id,
            inboundQuestionIdInUse,
            noteExportRef,
            rollbackExportRef,
            sendReturnException,
            sendAndRecordBootstrapReturn,
        );
    }

    /// Deliver a Bootstrap results Return and record its resolved answer with
    /// the same reserve → send → commit-or-cleanup discipline as
    /// sendReturnResults: the record resources (count budget, map slot, frame
    /// copy, answer-held export refs) are reserved before the frame is sent so
    /// recording cannot fail once the frame is on the wire, and the id is
    /// marked resolving so a Finish delivered synchronously during the send
    /// leaves a finished-early tombstone for the post-send step instead of
    /// vanishing — without this, the reentrant Finish would find nothing to
    /// clean and the record afterward would strand a resolved answer (plus its
    /// answer-held export reference) that no later Finish could ever clear.
    fn sendAndRecordBootstrapReturn(self: *Peer, question_id: u32, bytes: []const u8) anyerror!void {
        var reservation: ?ResolvedAnswerReservation = try self.reserveResolvedAnswer(question_id, bytes);
        errdefer if (reservation) |r| r.deinit(self);
        try self.resolving_answers.put(question_id, {});
        var resolving_answer = true;
        errdefer if (resolving_answer) {
            _ = self.resolving_answers.remove(question_id);
        };

        try self.sendFrame(bytes);
        _ = self.resolving_answers.remove(question_id);
        resolving_answer = false;

        self.commitOrRollbackResolvedAnswerAfterSend(question_id, bytes, &reservation);
    }

    fn handleFinish(self: *Peer, finish_msg: protocol.Finish) !void {
        const qid = finish_msg.question_id;
        // Automatic route retirement can synchronously settle the opposite
        // peer and invoke arbitrary callbacks. Snapshot every borrowed peer
        // before either map is mutated, deduplicating same-peer and answer-id
        // collision cases, and guard only this automatic-route Finish path.
        const owned_automatic_route = self.automatic_third_party_routes.get(qid);
        const incoming_automatic_route = self.incoming_automatic_third_party_routes.get(qid);
        var automatic_guards = AutomaticThirdPartyFinishGuards{};
        if (owned_automatic_route != null or incoming_automatic_route != null) {
            automatic_guards.add(self);
            if (owned_automatic_route) |route| {
                if (route.target_peer) |target| automatic_guards.add(target);
            }
            if (incoming_automatic_route) |route| {
                if (route.source_peer) |source| automatic_guards.add(source);
            }
            automatic_guards.enter();
        }
        defer automatic_guards.leave();
        const canceled_automatic_target = self.noteAutomaticThirdPartyTargetFinish(qid);
        // Vat-wide provision close (Finish of a Provide question) — UNGATED:
        // the map lookup inside is the gate. Runs in this fallible context so
        // an OOM in the fan-out propagates out of dispatch instead of being
        // force-swallowed by the void FinishOps hook below.
        try self.detachProvisionForFinish(qid);
        const was_active = self.active_inbound_questions.remove(qid);
        const was_resolving = self.resolving_answers.contains(qid);
        // The failed-answer record lives exactly as long as resolved_answers
        // entries do: until the remote finishes the question.
        if (self.failed_answers.fetchRemove(qid)) |failed| {
            self.allocator.free(failed.value.reason);
        }
        const finished_completing_join = self.finishCompletingJoinAnswer(qid, finish_msg.release_result_caps);
        if (!finished_completing_join) self.clearPendingJoinResultAnswer(qid);
        try self.clearPendingJoinRelay(qid, true, finish_msg.release_result_caps);
        // Cancellation race: a Finish for an in-flight inbound call (still
        // active, not yet resolved), or for a synchronous Return whose frame is
        // on the wire but whose resolved answer is not committed yet, means the
        // Return sender must not later leave a resolved_answers entry that no
        // further Finish will clear. Preserve releaseResultCaps for the sender
        // so it can either roll back a late async Return or commit+cleanup a
        // synchronous Return after replaying queued promised calls.
        if ((was_active or was_resolving or finished_completing_join) and
            !self.resolved_answers.contains(qid) and
            self.finished_early_answers.count() < self.limits.max_active_inbound_questions)
        {
            self.finished_early_answers.put(qid, finish_msg.release_result_caps) catch |err| self.reportNonfatalError(err);
        }
        if (!finish_msg.require_early_cancellation) {
            // Default behavior: if Finish arrives before a promised-target call is
            // deliverable, cancel the queued call immediately.
            _ = try self.cancelQueuedPendingQuestion(finish_msg.question_id);
        }
        const ops = finish.FinishOps(Peer){
            .remove_send_results_to_yourself = peer_forward_orchestration.removeSendResultsToYourselfForPeerFn(Peer),
            .clear_send_results_to_third_party = clearSendResultsToThirdParty,
            .clear_provide = provide_accept_join.provides_state.clearProvideForPeerFn(
                Peer,
                ProvideEntry,
                ProvideTarget,
                ProvideTarget.deinit,
            ),
            .clear_pending_join_question = provide_accept_join.join_state.clearPendingJoinQuestionForPeerFn(
                Peer,
                JoinState,
                PendingJoinQuestion,
                ProvideTarget,
                ProvideTarget.deinit,
                JoinState.deinit,
            ),
            .clear_pending_accept_question = Peer.clearPendingAcceptQuestionRouted,
            .take_forwarded_tail_question = peer_forward_orchestration.takeForwardedTailQuestionForPeerFn(Peer),
            // The tail Finish is only ever sent for a forwarded question held open
            // by finishForwardResolvedCall; neutralize that question here so a late
            // forwarded return is absorbed rather than re-emitted (see
            // sendTailFinishAndNeutralize / W1).
            .send_finish = Peer.sendTailFinishAndNeutralize,
            .take_resolved_answer_frame = finish.takeResolvedAnswerFrameForPeerFn(Peer),
            .release_answer_caps_for_frame = releaseAnswerHeldResultCaps,
            .release_caps_for_frame = releaseResultCaps,
            .free_frame = finish.freeOwnedFrameForPeerFn(Peer),
        };
        const cleared_partial_join = self.pending_join_questions.contains(qid);
        try finish.handleFinishWithOps(
            Peer,
            self,
            finish_msg.question_id,
            finish_msg.release_result_caps,
            ops,
        );
        if (cleared_partial_join) self.refreshNextJoinDeadline();
        // No Return will follow for a synthetic answer canceled before result
        // delivery, so its ordinary finished-early tombstone must not reserve
        // the high-range id forever.
        if (canceled_automatic_target) {
            _ = self.finished_early_answers.remove(qid);
        }
    }

    fn cancelQueuedPendingQuestionInMap(
        self: *Peer,
        pending_map: *std.AutoHashMap(u32, std.ArrayList(PendingCall)),
        question_id: u32,
    ) !bool {
        var canceled = false;
        var empty_keys = std.ArrayList(u32).empty;
        defer empty_keys.deinit(self.allocator);

        var pending_it = pending_map.iterator();
        while (pending_it.next()) |entry| {
            const pending_list = entry.value_ptr;
            var idx: usize = 0;
            while (idx < pending_list.items.len) {
                // Match on the id decoded once at enqueue — no per-scan re-parse.
                if (pending_list.items[idx].question_id != question_id) {
                    idx += 1;
                    continue;
                }

                // orderedRemove, not swapRemove: the surviving queued calls
                // must keep send order so replay honors Cap'n Proto's E-order
                // guarantee (calls to one target delivered in send order).
                var pending_call = pending_list.orderedRemove(idx);
                defer {
                    pending_call.caps.deinit();
                    self.allocator.free(pending_call.frame);
                }
                try self.releaseInboundCaps(&pending_call.caps);
                // Spec: every Call receives exactly one Return, even when
                // cancelled by a Finish. Send Return(canceled) so the caller's
                // question entry can drain (a compliant peer keeps it reserved
                // until this arrives) and shutdown can complete. Non-draining
                // by construction (sendReturnTag does not touch
                // pending_promises), so it is safe while iterating that map.
                self.sendReturnTag(question_id, .canceled) catch |err| {
                    self.reportNonfatalError(err);
                };
                canceled = true;
            }

            if (pending_list.items.len == 0) {
                try empty_keys.append(self.allocator, entry.key_ptr.*);
            }
        }

        for (empty_keys.items) |key| {
            if (pending_map.fetchRemove(key)) |removed| {
                var pending_list = removed.value;
                pending_list.deinit(self.allocator);
            }
        }
        return canceled;
    }

    fn cancelQueuedPendingQuestion(self: *Peer, question_id: u32) !bool {
        const canceled_promised = try self.cancelQueuedPendingQuestionInMap(&self.pending_promises, question_id);
        const canceled_export = try self.cancelQueuedPendingQuestionInMap(&self.pending_export_promises, question_id);
        const canceled = canceled_promised or canceled_export;
        if (canceled) {
            // The cancelled call's answer will never be produced, so any calls
            // pipelined ON it (queued under pending_promises[question_id], and
            // transitively) can never be satisfied. Fail-drain them, each with
            // its own exception Return, so the exactly-one-Return-per-call
            // invariant holds and a stale orphan bucket cannot later replay
            // against an unrelated answer if the remote reuses the id. Run here,
            // outside the pending-map iteration above, since the drain
            // fetchRemoves buckets. Mirrors sendReturnException's drain.
            self.failQueuedPromisedCalls(question_id, "pipelined on a cancelled call", .failed);
        }
        return canceled;
    }

    fn handleRelease(self: *Peer, release: protocol.Release) !void {
        // ORIGINATION: is this Release dropping a vine we minted for a handoff?
        // Snapshot the coupling BEFORE releasing the export, then check whether
        // the release fully dropped the vine (destroyed the export). The vine's
        // Release is the spec's signal that the Accept completed (or was
        // abandoned); once the vine is gone we Finish the paired Provide on the
        // host-of-provided-cap connection, unregistering the provision.
        const coupled = self.outbound_provides.get(release.id);

        try peer_cap_lifecycle.handleRelease(Peer, self, release, releaseExport);

        if (coupled) |entry| {
            if (!self.exports.contains(release.id)) {
                // Vine fully released. Drop the coupling and its handoff mark,
                // then Finish the held-open Provide question on its own peer.
                // Free the stash from the LIVE entry (not the pre-release
                // snapshot): nested delivery during the release may have
                // stashed or flushed since the snapshot was taken.
                if (self.outbound_provides.fetchRemove(release.id)) |removed| {
                    var op = removed.value;
                    op.deinit(self.allocator);
                }
                self.caps.clearThirdPartyHosted(release.id);
                // If this vine anchored a promise-export resolution
                // (`resolvePromiseExportToThirdParty`), that promise resolved
                // locally to the now-destroyed vine. Clear the stale target so a
                // later Release of the promise export does not cascade into a
                // vanished vine (benign no-op, but avoids a dangling resolution).
                if (entry.resolved_promise_export_id) |promise_id| {
                    if (self.exports.getEntry(promise_id)) |pe| {
                        if (pe.value_ptr.resolved) |resolved| {
                            if (resolved == .exported and resolved.exported.id == release.id) {
                                pe.value_ptr.resolved = null;
                            }
                        }
                    }
                }
                // LIVENESS (BUG #55): if `provide_peer` is null the
                // host-of-provided-cap peer already deinited — it removed and
                // Finished the held-open Provide question itself and neutralized
                // this coupling — so Finishing is a safe no-op we skip. Otherwise
                // Finish the question and drop the reverse back-link on that peer.
                if (entry.provide_peer) |provide_peer| {
                    provide_peer.deregisterCoupledVine(self, release.id);
                    self.finishOriginatedProvide(
                        provide_peer,
                        entry.provide_question_id,
                        entry.retained_source_question_id,
                    );
                }
            }
        }
    }

    /// Finish a Provide question this peer originated, on the peer that owns it
    /// (the host-of-provided-cap connection). Removes the held-open question and
    /// sends the wire `Finish` so the host unregisters the provision. Both the
    /// Provide and a transferred retained source answer publish durable finish
    /// requests before either frame is sent; transport failures stay queued for
    /// `retryDeferredFinishes` rather than losing the lifetime owner when the
    /// vine coupling disappears.
    fn requestOriginatedProvideFinish(
        provide_peer: *Peer,
        provide_question_id: u32,
        retained_source_question_id: ?u32,
    ) void {
        if (provide_peer.questions.getPtr(provide_question_id)) |question| {
            question.finish_on_maintenance = true;
            question.finish_release_result_caps = false;
        }
        if (retained_source_question_id) |source_question_id| {
            provide_peer.retained_questions.requestTransferredFinish(source_question_id) catch |err| {
                log.debug("retained source cleanup request failed for question {}: {}", .{ source_question_id, err });
            };
        }
    }

    fn finishOriginatedProvide(
        _: *Peer,
        provide_peer: *Peer,
        provide_question_id: u32,
        retained_source_question_id: ?u32,
    ) void {
        requestOriginatedProvideFinish(
            provide_peer,
            provide_question_id,
            retained_source_question_id,
        );
        provide_peer.retryDeferredFinishes();
    }

    /// Recipient-side terminal cleanup for vine couplings. Move the entire map
    /// out first, publish every Provide/source-answer Finish request without
    /// callbacks, then retry each distinct provider only after this peer owns no
    /// borrowed coupling state. Thus a synchronous send callback can close or
    /// deinit the recipient without stranding a later entry.
    fn drainOutboundProvidesOnRecipientPeer(self: *Peer) void {
        if (self.outbound_provides.count() == 0) return;

        const allocator = self.allocator;
        var moved = self.outbound_provides;
        self.outbound_provides = std.AutoHashMap(u32, OutboundProvide).init(allocator);
        defer moved.deinit();

        var retry_peers: std.ArrayList(*Peer) = .empty;
        defer retry_peers.deinit(allocator);

        var it = moved.iterator();
        while (it.next()) |entry| {
            const vine_id = entry.key_ptr.*;
            const op = entry.value_ptr;
            if (op.provide_peer) |provide_peer| {
                provide_peer.deregisterCoupledVine(self, vine_id);
                requestOriginatedProvideFinish(
                    provide_peer,
                    op.provide_question_id,
                    op.retained_source_question_id,
                );
                if (provide_peer != self and
                    std.mem.indexOfScalar(*Peer, retry_peers.items, provide_peer) == null)
                {
                    retry_peers.append(allocator, provide_peer) catch {};
                }
            }
            if (op.resolved_promise_export_id) |promise_id| {
                if (self.exports.getEntry(promise_id)) |promise_entry| {
                    if (promise_entry.value_ptr.resolved) |resolved| {
                        if (resolved == .exported and resolved.exported.id == vine_id) {
                            promise_entry.value_ptr.resolved = null;
                        }
                    }
                }
            }
            self.caps.clearThirdPartyHosted(vine_id);
            self.releaseVineExport(vine_id);
            op.deinit(allocator);
        }

        for (retry_peers.items) |provide_peer| {
            provide_peer.retryDeferredFinishes();
        }
    }

    /// Retry protocol lifetimes which have ended locally but whose Finish
    /// frame was rejected by the transport. State is published before any
    /// send, and the guard makes synchronous nested delivery observe a single
    /// owner rather than recursively finishing the same question.
    fn retryDeferredFinishes(self: *Peer) void {
        if (self.finish_maintenance_in_progress) return;
        self.finish_maintenance_in_progress = true;
        defer self.finish_maintenance_in_progress = false;

        var provide_ids: std.ArrayList(u32) = .empty;
        defer provide_ids.deinit(self.allocator);
        var question_it = self.questions.iterator();
        while (question_it.next()) |entry| {
            if (!entry.value_ptr.finish_on_maintenance) continue;
            provide_ids.append(self.allocator, entry.key_ptr.*) catch break;
        }
        for (provide_ids.items) |question_id| {
            const question = self.questions.get(question_id) orelse continue;
            if (!question.finish_on_maintenance) continue;
            peer_outbound_control.sendFinishWithFlagsViaSendFrame(
                Peer,
                self,
                question_id,
                question.finish_release_result_caps,
                false,
                Peer.sendFrameControl,
            ) catch |err| {
                log.debug("deferred provide finish failed for question {}: {}", .{ question_id, err });
                continue;
            };
            self.removeQuestion(question_id);
        }

        var retained_ids: std.ArrayList(u32) = .empty;
        defer retained_ids.deinit(self.allocator);
        var retained_it = self.retained_questions.entries.iterator();
        while (retained_it.next()) |entry| {
            if (!entry.value_ptr.finish_requested or entry.value_ptr.state != .transferred) continue;
            retained_ids.append(self.allocator, entry.key_ptr.*) catch break;
        }
        for (retained_ids.items) |question_id| {
            self.finishTransferredRetainedQuestion(question_id, false) catch |err| {
                log.debug("deferred retained source finish failed for question {}: {}", .{ question_id, err });
            };
        }
    }

    /// LIVENESS (BUG #55). Record a back-link on the host-of-provided-cap peer
    /// (`self` == the peer that owns the held-open Provide question) naming the
    /// recipient peer whose `outbound_provides[vine_id]` entry borrows a pointer
    /// back to `self`. Called when a coupling is created; the paired
    /// `deregisterCoupledVine` drops it when the coupling drains. On `self`'s
    /// `deinit`, `neutralizeCoupledVinesOnProvidePeer` walks these links and
    /// nulls the borrowed pointer so it is never dereferenced after free.
    ///
    /// Returns `error.OutOfMemory` if the back-link cannot be recorded; the
    /// caller must then unwind the coupling rather than leave a half-registered
    /// (untracked, thus un-neutralizable) borrow.
    fn registerCoupledVine(self: *Peer, recipient_peer: *Peer, vine_id: u32) !void {
        try self.coupled_vines.append(self.allocator, .{
            .recipient_peer = recipient_peer,
            .vine_id = vine_id,
        });
    }

    /// LIVENESS (BUG #55). Drop the back-link recorded by `registerCoupledVine`
    /// for `(recipient_peer, vine_id)`. Called when the coupling drains normally
    /// (vine Release on the recipient) or when the recipient peer deinits while
    /// this provide_peer is still alive. Idempotent: a missing link is a no-op
    /// (e.g. already removed, or never registered on a rollback path).
    fn deregisterCoupledVine(self: *Peer, recipient_peer: *Peer, vine_id: u32) void {
        var i: usize = 0;
        while (i < self.coupled_vines.items.len) {
            const link = self.coupled_vines.items[i];
            if (link.recipient_peer == recipient_peer and link.vine_id == vine_id) {
                _ = self.coupled_vines.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    /// LIVENESS (BUG #55). Called from `deinit` on the host-of-provided-cap peer.
    /// For every coupling anchored here, null the borrowed `provide_peer` pointer
    /// in the recipient peer's `outbound_provides` entry so a subsequent vine
    /// Release finds no provide_peer and skips the Finish (a correct no-op — this
    /// peer's own `forceCancelAllQuestions` already removed and Finished the
    /// held-open Provide question). This runs BEFORE this peer's memory is freed,
    /// which is exactly what makes the borrow liveness-safe under arbitrary
    /// per-connection teardown order.
    fn neutralizeCoupledVinesOnProvidePeer(self: *Peer) void {
        for (self.coupled_vines.items) |link| {
            if (link.recipient_peer.outbound_provides.getPtr(link.vine_id)) |op| {
                // Only clear the edge that points back at THIS peer; a recipient
                // could in principle re-key the same vine id to another provide
                // peer, though the id space makes that vanishingly unlikely.
                if (op.provide_peer == self) op.provide_peer = null;
            }
        }
        self.coupled_vines.clearRetainingCapacity();
    }

    fn registerForwardVineRelay(self: *Peer, relay: *ForwardVineCallContext) !void {
        try self.forward_vine_relay_links.append(self.allocator, relay);
        relay.recipient_link_registered = true;
    }

    fn deregisterForwardVineRelay(self: *Peer, relay: *ForwardVineCallContext) void {
        if (!relay.recipient_link_registered) return;
        var i: usize = 0;
        while (i < self.forward_vine_relay_links.items.len) : (i += 1) {
            if (self.forward_vine_relay_links.items[i] == relay) {
                _ = self.forward_vine_relay_links.swapRemove(i);
                relay.recipient_link_registered = false;
                return;
            }
        }
        relay.recipient_link_registered = false;
    }

    /// Null every async forwarded-vine context that borrows this recipient
    /// before transport-close callbacks or peer teardown can free it. The
    /// forwarding question still owns the context and will free it later, but
    /// its Return/deinit path can no longer dereference this peer.
    fn neutralizeForwardVineRelaysOnRecipientPeer(self: *Peer) void {
        for (self.forward_vine_relay_links.items) |relay| {
            if (relay.recipient_peer == self) {
                relay.recipient_peer = null;
                relay.recipient_link_registered = false;
                relay.recipient_answer_pending = false;
            }
        }
        self.forward_vine_relay_links.clearRetainingCapacity();
    }

    fn registerHandoffPickup(self: *Peer, ctx: *HandoffPickupContext) !void {
        try self.handoff_pickup_links.append(self.allocator, ctx);
        ctx.promise_link_registered = true;
    }

    fn deregisterHandoffPickup(self: *Peer, ctx: *HandoffPickupContext) void {
        if (!ctx.promise_link_registered) return;
        var i: usize = 0;
        while (i < self.handoff_pickup_links.items.len) : (i += 1) {
            if (self.handoff_pickup_links.items[i] == ctx) {
                _ = self.handoff_pickup_links.swapRemove(i);
                ctx.promise_link_registered = false;
                return;
            }
        }
        ctx.promise_link_registered = false;
    }

    /// The Accept question on the third-vat peer owns each context. Promise
    /// peer teardown abandons its local vine ref (the cap table is dying) and
    /// nulls the callback borrow; the eventual Accept Return/teardown then only
    /// finishes and frees its own side.
    fn neutralizeHandoffPickupsOnPromisePeer(self: *Peer) void {
        for (self.handoff_pickup_links.items) |ctx| {
            if (ctx.promise_peer == self) {
                ctx.promise_peer = null;
                ctx.promise_link_registered = false;
                ctx.vine_owned = false;
            }
        }
        self.handoff_pickup_links.clearRetainingCapacity();
    }

    fn registerCrossPeerProxy(self: *Peer, owner_peer: *Peer, proxy_export_id: u32) !void {
        try self.cross_peer_proxy_links.append(self.allocator, .{
            .owner_peer = owner_peer,
            .proxy_export_id = proxy_export_id,
        });
    }

    fn deregisterCrossPeerProxy(self: *Peer, owner_peer: *Peer, proxy_export_id: u32) void {
        var i: usize = 0;
        while (i < self.cross_peer_proxy_links.items.len) {
            const link = self.cross_peer_proxy_links.items[i];
            if (link.owner_peer == owner_peer and link.proxy_export_id == proxy_export_id) {
                _ = self.cross_peer_proxy_links.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    fn neutralizeCrossPeerProxiesOnSourcePeer(self: *Peer) void {
        for (self.cross_peer_proxy_links.items) |link| {
            if (link.owner_peer.exports.getPtr(link.proxy_export_id)) |entry| {
                if (entry.handler) |handler| {
                    const proxy_ctx = castCtx(*CrossPeerProxyContext, handler.ctx);
                    if (proxy_ctx.source_peer == self) {
                        proxy_ctx.source_peer = null;
                        proxy_ctx.release_source_import_id = null;
                        // Abandon (never release): the pinned export lives on
                        // this dying peer; its entry is freed with the peer.
                        proxy_ctx.release_source_export_pin_id = null;
                        // Same rule for the import pin: the source peer's
                        // import table is going away with it.
                        proxy_ctx.release_source_import_pin_id = null;
                    }
                }
            }
        }
        self.cross_peer_proxy_links.clearRetainingCapacity();
    }

    fn registerCrossPeerJoinRelay(self: *Peer, owner_peer: *Peer, owner_answer_id: u32) !void {
        try self.cross_peer_join_relay_links.append(self.allocator, .{
            .owner_peer = owner_peer,
            .owner_answer_id = owner_answer_id,
        });
    }

    fn deregisterCrossPeerJoinRelay(self: *Peer, owner_peer: *Peer, owner_answer_id: u32) void {
        var i: usize = 0;
        while (i < self.cross_peer_join_relay_links.items.len) {
            const link = self.cross_peer_join_relay_links.items[i];
            if (link.owner_peer == owner_peer and link.owner_answer_id == owner_answer_id) {
                _ = self.cross_peer_join_relay_links.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    fn neutralizeCrossPeerJoinRelaysOnSourcePeer(self: *Peer) void {
        for (self.cross_peer_join_relay_links.items) |link| {
            if (link.owner_peer.pending_join_relays.getPtr(link.owner_answer_id)) |relay| {
                if (relay.source_peer == self) relay.source_peer = null;
            }
        }
        self.cross_peer_join_relay_links.clearRetainingCapacity();
    }

    fn registerJoinAcceptHost(self: *Peer, hosted: *HostedJoin) !void {
        for (self.join_accept_host_links.items) |link| {
            if (link.hosted == hosted) return;
        }
        try self.join_accept_host_links.append(self.allocator, .{ .hosted = hosted });
    }

    fn deregisterJoinAcceptHost(self: *Peer, hosted: *HostedJoin) void {
        var i: usize = 0;
        while (i < self.join_accept_host_links.items.len) {
            const link = self.join_accept_host_links.items[i];
            if (link.hosted == hosted) {
                _ = self.join_accept_host_links.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    fn cancelJoinAcceptHostLinks(self: *Peer) void {
        while (self.join_accept_host_links.items.len != 0) {
            const hosted = self.join_accept_host_links.items[0].hosted;
            hosted.owner_peer.cancelHostedJoin(hosted);
        }
    }

    fn drainIncompleteJoinsOnTransportClose(self: *Peer) void {
        while (self.pending_joins.count() != 0) {
            var it = self.pending_joins.keyIterator();
            const join_id = (it.next() orelse break).*;
            const removed = self.pending_joins.fetchRemove(join_id) orelse continue;
            var join_state = removed.value;
            var part_it = join_state.parts.valueIterator();
            while (part_it.next()) |part| _ = self.pending_join_questions.remove(part.question_id);
            JoinState.deinit(&join_state, self.allocator);
        }
        while (self.pending_join_relays.count() != 0) {
            var it = self.pending_join_relays.keyIterator();
            const answer_id = (it.next() orelse break).*;
            _ = self.retirePendingJoinRelayTerminal(answer_id, false, false);
        }
        self.refreshNextJoinDeadline();
    }

    /// Transport close retires the result-path answer bookkeeping without
    /// interpreting it as explicit Finish. A committed lease hosted by a
    /// distinct live Accept peer therefore remains pickable, while the closed
    /// result peer exposes no stale answer records to its user close callback.
    fn detachJoinResultAnswersOnTransportClose(self: *Peer) void {
        while (self.pending_join_result_answers.count() != 0) {
            var it = self.pending_join_result_answers.iterator();
            const entry = it.next() orelse break;
            const answer_id = entry.key_ptr.*;
            const hosted = entry.value_ptr.hosted;
            _ = self.pending_join_result_answers.remove(answer_id);
            std.debug.assert(hosted.owner_peer == self);
            std.debug.assert(hosted.result_refs > 0);
            hosted.result_refs -= 1;
            // `accept_live` retains a committed distinct-host lease. If the
            // Accept was already consumed/cancelled, dropping the final result
            // reference can retire the canonical object immediately.
            self.maybeDestroyHostedJoin(hosted);
        }
    }

    pub fn registerJoinCoordinatorAccept(self: *Peer, coordinator: *JoinCoordinator) !void {
        for (self.join_coordinator_accept_links.items) |link| {
            if (link.coordinator == coordinator) return;
        }
        try self.join_coordinator_accept_links.append(self.allocator, .{
            .coordinator = coordinator,
        });
    }

    pub fn deregisterJoinCoordinatorAccept(self: *Peer, coordinator: *JoinCoordinator) void {
        var i: usize = 0;
        while (i < self.join_coordinator_accept_links.items.len) {
            const link = self.join_coordinator_accept_links.items[i];
            if (link.coordinator == coordinator) {
                _ = self.join_coordinator_accept_links.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    fn registerJoinCoordinatorResult(self: *Peer, coordinator: *JoinCoordinator, question_id: u32) !void {
        for (self.join_coordinator_result_links.items) |link| {
            if (link.coordinator == coordinator and link.question_id == question_id) return;
        }
        try self.join_coordinator_result_links.append(self.allocator, .{
            .coordinator = coordinator,
            .question_id = question_id,
        });
    }

    pub fn deregisterJoinCoordinatorResult(self: *Peer, coordinator: *JoinCoordinator, question_id: u32) void {
        var i: usize = 0;
        while (i < self.join_coordinator_result_links.items.len) {
            const link = self.join_coordinator_result_links.items[i];
            if (link.coordinator == coordinator and link.question_id == question_id) {
                _ = self.join_coordinator_result_links.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    fn neutralizeJoinCoordinatorResultLinks(self: *Peer) void {
        while (self.join_coordinator_result_links.items.len != 0) {
            const link = self.join_coordinator_result_links.pop() orelse break;
            link.coordinator.neutralizeResultPeer(self, link.question_id);
        }
    }

    fn neutralizeJoinCoordinatorAcceptLinks(self: *Peer) void {
        for (self.join_coordinator_accept_links.items) |link| {
            link.coordinator.neutralizeAcceptedPeer(self);
        }
        self.join_coordinator_accept_links.clearRetainingCapacity();
    }

    fn handleResolve(self: *Peer, resolve_msg: protocol.Resolve) !void {
        // Level-3 recipient auto-pickup: an inbound `thirdPartyHosted` Resolve for
        // a promise we hold, when a VatNetwork + pickup handler are configured,
        // is picked up directly from the third vat instead of proxied through the
        // vine. This branch is fully isolated from the two-party/reflected-
        // loopback resolve path below — it only fires for the thirdPartyHosted
        // descriptor tag with both L3 seams attached, and returns before the
        // generic ops run. Every other resolve (cap/exception, incl. the fragile
        // reflected-loopback `receiverHosted`/`senderHosted` cases) is unchanged.
        if (resolve_msg.tag == .cap) {
            if (resolve_msg.cap) |descriptor| {
                if (descriptor.tag == .thirdPartyHosted and
                    self.vat_network != null and
                    self.on_handoff_pickup != null and
                    self.caps.imports.contains(resolve_msg.promise_id))
                {
                    if (try self.tryAutoPickupThirdParty(resolve_msg.promise_id, descriptor)) {
                        return;
                    }
                    // tryAutoPickup returned false: not actionable (e.g. the
                    // network could not resolve the contact). Fall through to the
                    // Level-1/2 vine fallback so the promise still resolves.
                }
            }
        }

        const ops = resolve.ResolveOps(Peer){
            .has_known_promise = resolve.hasKnownResolvePromiseForPeerFn(Peer),
            .resolve_cap_descriptor = resolve.resolveCapDescriptorForPeerFn(Peer),
            .release_resolved_cap = releaseResolvedCap,
            .alloc_embargo_id = resolve.allocateEmbargoIdForPeerFn(Peer),
            .remember_pending_embargo = Peer.rememberPendingEmbargo,
            .forget_pending_embargo = resolve.forgetPendingEmbargoForPeerFn(Peer),
            .send_disembargo_sender_loopback = peer_outbound_control.sendDisembargoSenderLoopbackViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            .store_resolved_import = storeResolvedImport,
        };
        try resolve.handleResolveWithOps(Peer, self, resolve_msg, ops);
    }

    /// Heap context threaded through the auto-pickup `Accept` question. Owns a
    /// small deferred-release list for failed pickup callbacks; freed by
    /// `onHandoffAcceptReturn` on the normal async path, by the synchronous
    /// sender after nested delivery settles, or by its `deinit_ctx` if the
    /// accept peer tears down first.
    const HandoffPickupContext = struct {
        allocator: std.mem.Allocator,
        /// The peer holding the promise import (VatA↔VatB). Borrowed while the
        /// symmetric `handoff_pickup_links` registration is live.
        promise_peer: ?*Peer,
        /// The promise import id being fulfilled by this handoff.
        promise_id: u32,
        /// The vine import id on `promise_peer`, released after pickup to drive
        /// VatB's Provide Finish.
        vine_id: u32,
        /// Application pickup handler + its ctx, copied from the promise peer.
        user_ctx: *anyopaque,
        user_cb: HandoffPickupCallback,
        /// Stack flag owned by `tryAutoPickupThirdParty` while the Accept send
        /// is in progress. The callback sets it if synchronous loopback has
        /// already freed this ctx and released the vine before sendAccept
        /// returns (or returns an error).
        settled_flag: ?*bool = null,
        /// Captured from the Accept Return so synchronous loopback can delay
        /// the Finish until the host has committed the resolved answer.
        accept_answer_id: ?u32 = null,
        accept_no_finish_needed: bool = false,
        /// Imports from a failed pickup callback that the app did not retain.
        /// In synchronous loopback, these are released by the sender after the
        /// host has committed the Return's export refs; in async delivery, the
        /// callback releases them before freeing this ctx.
        deferred_failed_imports: std.ArrayList(u32) = .empty,
        promise_link_registered: bool = false,
        vine_owned: bool = false,

        fn deinitCtx(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
            _ = allocator;
            const ctx: *HandoffPickupContext = @ptrCast(@alignCast(ctx_ptr));
            ctx.deinitSelf();
        }

        fn deinitSelf(ctx: *HandoffPickupContext) void {
            if (ctx.promise_peer) |promise_peer| {
                if (ctx.promise_link_registered) {
                    promise_peer.deregisterHandoffPickup(ctx);
                    ctx.promise_link_registered = false;
                }
                ctx.promise_peer = null;
                if (ctx.vine_owned) {
                    ctx.vine_owned = false;
                    promise_peer.releaseImport(ctx.vine_id, 1) catch |err| {
                        log.debug("auto-pickup teardown vine release failed for promise {}: {}", .{
                            ctx.promise_id,
                            err,
                        });
                    };
                }
            }
            ctx.deferred_failed_imports.deinit(ctx.allocator);
            ctx.allocator.destroy(ctx);
        }

        fn retainFailedUnretainedImports(
            ctx: *HandoffPickupContext,
            accept_caps: *const cap_table.InboundCapTable,
        ) !void {
            var mutable_caps: *cap_table.InboundCapTable = @constCast(accept_caps);
            var idx: u32 = 0;
            while (idx < mutable_caps.len()) : (idx += 1) {
                if (mutable_caps.isRetained(idx)) continue;
                const entry = try mutable_caps.get(idx);
                switch (entry) {
                    .imported => |cap| {
                        try ctx.deferred_failed_imports.append(ctx.allocator, cap.id);
                        try mutable_caps.retainIndex(idx);
                    },
                    else => {},
                }
            }
        }

        fn releaseDeferredFailedImports(ctx: *HandoffPickupContext, accept_peer: *Peer) void {
            for (ctx.deferred_failed_imports.items) |import_id| {
                accept_peer.releaseImport(import_id, 1) catch |release_err| {
                    log.debug("auto-pickup failed-handler cap release failed for promise {}: {}", .{
                        ctx.promise_id,
                        release_err,
                    });
                };
            }
            ctx.deferred_failed_imports.clearRetainingCapacity();
        }

        fn finishAcceptAnswer(ctx: *HandoffPickupContext, accept_peer: *Peer, answer_id: u32, no_finish_needed: bool) void {
            if (no_finish_needed) return;
            var attempts: u8 = 0;
            while (attempts < 2) : (attempts += 1) {
                accept_peer.sendFinishForHost(answer_id, false, false) catch |err| {
                    log.debug("auto-pickup Accept Finish failed for promise {} answer {}: {}", .{
                        ctx.promise_id,
                        answer_id,
                        err,
                    });
                    continue;
                };
                return;
            }
        }
    };

    /// Attempt the Level-3 recipient auto-pickup for an inbound `thirdPartyHosted`
    /// Resolve. Returns true if the pickup was initiated (an `Accept` was sent and
    /// ownership of the vine reference transferred to the pickup flow); false if
    /// the handoff is not actionable and the caller should fall back to the
    /// Level-1/2 vine proxy. On a true return the promise import's vine reference
    /// has been noted and will be released when the Accept `Return` arrives.
    fn tryAutoPickupThirdParty(self: *Peer, promise_id: u32, descriptor: protocol.CapDescriptor) !bool {
        const third = descriptor.third_party orelse return false;
        const contact = third.id orelse return false;
        const network = self.vat_network orelse return false;
        const user_cb = self.on_handoff_pickup orelse return false;
        const user_ctx = self.handoff_pickup_ctx orelse return false;

        // Resolve the ThirdPartyToContact to a live peer connected to the third
        // vat plus the completion to present in the Accept, BEFORE noting the vine
        // import. A network that cannot place the contact returns false, leaving
        // the caller on the vine-proxy fallback (which does its own noteImport) —
        // so we must not have taken a vine ref yet on that path. Every early
        // return below this point is either an error (handled by errdefer) or
        // happens before `noteImport`.
        var introduced = network.connectToIntroduced(contact) catch |err| {
            log.debug("auto-pickup connectToIntroduced failed: {}; using vine fallback", .{err});
            return false;
        };
        defer introduced.deinit(self.allocator);
        const accept_peer = introduced.peer;
        accept_peer.assertThreadAffinity();
        if (accept_peer.is_shutting_down) return false;

        // Account for the wire reference the descriptor hands us on the vine. We
        // own it now and release it once the direct cap is in hand (below /
        // onHandoffAcceptReturn). This is the ref whose eventual Release signals
        // VatB to Finish its Provide (rpc.capnp:1232-1237). Past this point every
        // remaining failure is an error return, so the errdefer covers rollback;
        // there is no non-error `return false` that could leak the ref.
        try self.caps.noteImport(third.vine_id);
        var vine_owned = true;
        errdefer if (vine_owned) {
            self.releaseImport(third.vine_id, 1) catch |err| {
                log.debug("auto-pickup vine rollback release failed: {}", .{err});
            };
        };

        var completion_msg = try message.Message.initUnvalidated(self.allocator, introduced.completion);
        defer completion_msg.deinit();
        const provision = try completion_msg.getRootAnyPointer();

        const heap = try self.allocator.create(HandoffPickupContext);
        heap.* = .{
            .allocator = self.allocator,
            .promise_peer = self,
            .promise_id = promise_id,
            .vine_id = third.vine_id,
            .user_ctx = user_ctx,
            .user_cb = user_cb,
        };
        var heap_owned = true;
        errdefer if (heap_owned) heap.deinitSelf();
        try self.registerHandoffPickup(heap);
        heap.vine_owned = true;
        vine_owned = false;
        var pickup_settled = false;
        heap.settled_flag = &pickup_settled;

        // PHASE 4 — embargo/disembargo ordering during a live-promise handoff.
        //
        // If VatA (this peer, holding the promise import) still has a pipelined
        // call in flight against the promise being resolved, the accepted cap
        // MUST be embargoed so that the pipelined call (which VatB will forward
        // to VatC on the old path) is delivered to VatC BEFORE any post-pickup
        // direct call — preserving Alice's e-order (rpc.capnp:885-903). We:
        //   (1) allocate an opaque accept-embargo byte id,
        //   (2) send `Accept{embargo=id}` to VatC (VatC holds the Return + any
        //       pipelined calls until the disembargo arrives), and
        //   (3) send `Disembargo{context.accept=id}` to VatB on the promise path;
        //       VatB forwards it to VatC behind the already-forwarded pipelined
        //       call, releasing the held Accept in e-order.
        // If NO call is in flight, keep the P3 fast path (`embargo = null`): there
        // is nothing to order the handoff against.
        var accept_embargo_buf: [ACCEPT_EMBARGO_ID_LEN]u8 = undefined;
        const embargo: ?[]const u8 = if (self.promiseImportHasInFlightCall(promise_id))
            self.nextAcceptEmbargoId(&accept_embargo_buf)
        else
            null;

        // Send the Accept on the third-vat connection. The pickup callback owns
        // and frees `heap`, so allocate the question with no restore from the
        // start: synchronous loopback can deliver the Accept Return before this
        // send call returns, and a callback/post-callback error must not restore
        // a question whose ctx has already been freed. Auto-Finish is also
        // suppressed so the callback can Finish the Accept answer after the
        // handler/vine cleanup while the context is still valid, with a bounded
        // retry for transient send failures.
        const question_id = accept_peer.sendAcceptNoRestore(provision, embargo, heap, onHandoffAcceptReturn, true) catch |err| {
            if (pickup_settled) {
                heap_owned = false;
                log.debug("auto-pickup Accept settled before send returned trailing error: {}", .{err});
                if (heap.accept_answer_id) |answer_id| {
                    heap.finishAcceptAnswer(accept_peer, answer_id, heap.accept_no_finish_needed);
                }
                heap.releaseDeferredFailedImports(accept_peer);
                heap.deinitSelf();
                return true;
            }
            return err;
        };
        if (pickup_settled) {
            heap_owned = false;
            heap.finishAcceptAnswer(accept_peer, question_id, heap.accept_no_finish_needed);
            heap.releaseDeferredFailedImports(accept_peer);
            heap.deinitSelf();
            return true;
        }
        heap.settled_flag = null;
        heap_owned = false;
        accept_peer.setQuestionDeinitCtx(question_id, HandoffPickupContext.deinitCtx);

        // (3) Emit the paired `context.accept` Disembargo on the promise path to
        // VatB. Best-effort AFTER the Accept is safely on the wire and ownership
        // has transferred: a failed Disembargo cannot roll back the sent Accept
        // (that would desync VatC's provide table), and VatC would then simply
        // hold the embargoed Accept until a later Finish/teardown drains it. We
        // therefore log rather than propagate.
        if (embargo) |embargo_bytes| {
            self.sendHandoffAcceptDisembargo(promise_id, embargo_bytes) catch |err| {
                log.warn("auto-pickup: accept-disembargo send failed for promise {}: {}", .{ promise_id, err });
            };
        }

        log.debug("auto-pickup: sent Accept question={} for promise={} vine={} embargoed={}", .{
            question_id,
            promise_id,
            third.vine_id,
            embargo != null,
        });
        return true;
    }

    /// Length of an opaque accept-embargo byte id. A big-endian encoding of the
    /// per-peer `next_accept_embargo_id` counter: unique per handoff on this
    /// peer. The HOST keys embargoes per provision, so id collisions across
    /// recipients cannot co-drain — but the spec asks for globally-unique,
    /// entropy-rich ids (rpc.capnp:776-778), which the `EntropySource` path
    /// provides.
    const ACCEPT_EMBARGO_ID_LEN = 16;

    /// Fill `buf` with the next opaque accept-embargo byte id and return it:
    /// 16 random bytes when an entropy source is installed, else the legacy
    /// 8-byte big-endian per-peer counter (wasm32-freestanding and detached
    /// test peers run unchanged).
    fn nextAcceptEmbargoId(self: *Peer, buf: *[ACCEPT_EMBARGO_ID_LEN]u8) []const u8 {
        if (self.entropy) |source| {
            source.fill(source.ctx, buf[0..]);
            return buf[0..];
        }
        const id = self.next_accept_embargo_id;
        self.next_accept_embargo_id +%= 1;
        std.mem.writeInt(u64, buf[0..8], id, .big);
        return buf[0..8];
    }

    /// True if this peer has an outbound Call still in flight (Return not yet
    /// received) that targeted the promise import `promise_id`. The mark is set
    /// on the `Question` by `sendCall` (see `target_promise_import`) and cleared
    /// implicitly when the question leaves the table, so a live matching question
    /// is exactly an in-flight pipelined call against the promise.
    fn promiseImportHasInFlightCall(self: *Peer, promise_id: u32) bool {
        var it = self.questions.valueIterator();
        while (it.next()) |q| {
            if (q.cancelled) continue;
            if (q.target_promise_import == promise_id) return true;
        }
        return false;
    }

    /// Send a `Disembargo{context.accept}` on the promise path (this peer, the
    /// VatA↔VatB connection) targeting the promise import being handed off. VatB
    /// forwards it to VatC (see `handleAcceptDisembargo`/`forwardAcceptDisembargo`),
    /// which releases the matching held `Accept`. The target names the promise import
    /// (an entry in the sender's import table → the receiver's export table),
    /// matching how VatB keys its promise export.
    fn sendHandoffAcceptDisembargo(self: *Peer, promise_id: u32, embargo: []const u8) !void {
        const target = protocol.MessageTarget{
            .tag = .importedCap,
            .imported_cap = promise_id,
            .promised_answer = null,
        };
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildDisembargoAccept(target, embargo);
        try self.sendBuilder(&builder);
    }

    /// Return callback for the auto-pickup `Accept`. Runs on the third-vat peer
    /// (VatA↔VatC). Delivers the accepted direct capability to the application's
    /// pickup handler, then releases the vine on the promise peer (VatA↔VatB) —
    /// the wire signal that drives VatB's Provide Finish.
    fn onHandoffAcceptReturn(
        ctx_ptr: *anyopaque,
        accept_peer: *Peer,
        ret: protocol.Return,
        accept_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *HandoffPickupContext = castCtx(*HandoffPickupContext, ctx_ptr);
        const defer_to_sender = ctx.settled_flag != null;
        ctx.accept_answer_id = ret.answer_id;
        ctx.accept_no_finish_needed = ret.no_finish_needed;
        defer if (!defer_to_sender) ctx.deinitSelf();
        defer if (!defer_to_sender) ctx.finishAcceptAnswer(accept_peer, ret.answer_id, ret.no_finish_needed);
        const promise_peer = ctx.promise_peer orelse {
            if (ctx.settled_flag) |flag| flag.* = true;
            return;
        };

        // Deliver the direct cap to the app FIRST (so it retains the accepted
        // import on `accept_peer` before we touch the vine), then release the
        // vine regardless of handler outcome — the pickup is complete on the wire
        // and the vine's job (holding the third-party cap alive until pickup) is
        // done. Releasing it drives VatB's Provide Finish.
        var handler_err: ?anyerror = null;
        ctx.user_cb(ctx.user_ctx, promise_peer, ctx.promise_id, accept_peer, ret, accept_caps) catch |err| {
            handler_err = err;
            log.debug("auto-pickup handler failed for promise {}: {}", .{ ctx.promise_id, err });
        };

        // The user callback is unrestricted and may close/deinit the promise
        // peer. Re-check the registered borrow before touching it again.
        if (ctx.promise_peer) |live_promise_peer| {
            live_promise_peer.deregisterHandoffPickup(ctx);
            ctx.promise_peer = null;
            if (ctx.vine_owned) {
                ctx.vine_owned = false;
                live_promise_peer.releaseImport(ctx.vine_id, 1) catch |err| {
                    log.debug("auto-pickup vine release failed for promise {}: {}", .{ ctx.promise_id, err });
                };
            }
        }

        if (ctx.settled_flag) |flag| flag.* = true;
        if (handler_err) |err| {
            ctx.retainFailedUnretainedImports(accept_caps) catch |retain_err| {
                log.debug("auto-pickup failed-handler deferred release capture failed for promise {}: {}", .{
                    ctx.promise_id,
                    retain_err,
                });
                return retain_err;
            };
            log.debug("auto-pickup handler error cleaned up for promise {}: {}", .{ ctx.promise_id, err });
            if (!defer_to_sender) ctx.releaseDeferredFailedImports(accept_peer);
        }
    }

    fn hasKnownDisembargoTarget(self: *Peer, target: protocol.MessageTarget) bool {
        return switch (target.tag) {
            .importedCap => blk: {
                // Per the RPC spec, MessageTarget.importedCap names an entry in
                // the *sender's* import table — i.e. one of OUR exports. This
                // gate only runs on the senderLoopback path (see
                // disembargo.zig:61), and the only senderLoopback disembargo a
                // compliant peer originates targets the promise it imported from
                // us as it resolves (resolve.zig:88-94 sets
                // imported_cap = promise_id). On the wire that id is the promise
                // we EXPORTED, so it is always an entry in our export table when
                // the disembargo is legitimate — never an import. Validate
                // against exports only; accepting an import id too would over-
                // accept a target the spec never names here (supported-surface.md
                // known limitation #2).
                const cap_id = target.imported_cap orelse break :blk false;
                break :blk self.exports.contains(cap_id);
            },
            .promisedAnswer => blk: {
                const promised = target.promised_answer orelse break :blk false;
                break :blk self.resolved_answers.contains(promised.question_id) or
                    self.pending_promises.contains(promised.question_id) or
                    self.send_results_to_yourself.contains(promised.question_id) or
                    self.send_results_to_third_party.contains(promised.question_id);
            },
        };
    }

    fn handleDisembargo(self: *Peer, disembargo_msg: protocol.Disembargo) !void {
        const ops = disembargo.DisembargoOps(Peer){
            .has_known_disembargo_target = Peer.hasKnownDisembargoTarget,
            .send_disembargo_receiver_loopback = peer_outbound_control.sendDisembargoReceiverLoopbackViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            .take_pending_embargo_promise = disembargo.takePendingEmbargoPromiseForPeerFn(Peer),
            .clear_resolved_import_embargo = disembargo.clearResolvedImportEmbargoForPeerFn(Peer),
            .release_embargoed_accepts = Peer.handleAcceptDisembargo,
        };
        try disembargo.handleDisembargoWithOps(Peer, self, disembargo_msg, ops);
    }

    /// Dispatch an inbound `context.accept` Disembargo. Role is decided by state,
    /// not by the target (which is meaningless once forwarded off the promise
    /// path): a vat is the capability HOST for this embargo iff it holds a queued
    /// `Accept` under these exact embargo bytes.
    ///
    ///   * HOST (VatC): holds a pending embargoed `Accept` for `embargo`. Release
    ///     it, delivering the queued Return + parked pipelined calls in e-order
    ///     (rpc.capnp:900-903).
    ///
    ///   * INTRODUCER (VatB): does NOT hold a matching `Accept`. The Disembargo
    ///     arrived on the promise path this peer used to originate a
    ///     `Provide`+`thirdPartyHosted` Resolve; forward it to the capability host
    ///     on the connection the paired `Provide` was sent on, behind the
    ///     already-forwarded pipelined call (rpc.capnp:889-899). The `target`
    ///     names this peer's promise EXPORT (VatA's promise import id), matched
    ///     against the `resolved_promise_export_id` recorded at origination.
    ///
    /// Deciding by "do I hold this Accept?" rather than by target-matching keeps
    /// the two roles unambiguous even for a vat that is simultaneously an
    /// introducer for one handoff and a host for another. A Disembargo that
    /// matches neither role is a benign no-op (the release drains nothing and no
    /// forward target is found).
    fn handleAcceptDisembargo(self: *Peer, target: protocol.MessageTarget, embargo: []const u8) !void {
        // HOST role: we hold the queued Accept for this embargo → release it.
        if (self.pending_accepts_by_embargo.contains(embargo)) {
            try provide_accept_join.embargo_accepts.releaseEmbargoedAcceptsForPeer(
                Peer,
                PendingEmbargoedAccept,
                ProvideEntry,
                ProvideTarget,
                self,
                embargo,
                Peer.sendReturnProvidedTarget,
                Peer.sendReturnException,
            );
            return;
        }

        // HOST role, spec form: the Disembargo addresses the Provide question
        // itself (promisedAnswer target in OUR answer space, no transform) —
        // gated on the per-peer LOOKUP, never the index field, so a matched
        // handoff keeps releasing after index death, and no-index peers
        // simply miss (their map is empty) and fall through.
        if (target.tag == .promisedAnswer) {
            if (target.promised_answer) |pa| {
                if (pa.transform.len() != 0) {
                    peer_outbound_control.sendAbortViaSendFrame(Peer, self, "accept disembargo must not carry a transform", Peer.sendFrameControl) catch |send_err| {
                        log.debug("disembargo transform abort send failed: {}", .{send_err});
                    };
                    return error.InvalidDisembargoTarget;
                }
                if (self.provisions_by_question.get(pa.question_id)) |prov| {
                    try self.releaseProvisionEmbargo(prov, embargo);
                    return;
                }
                // Unknown qid: fall through to the drop arm (a late/duplicate
                // Disembargo after Finish is benign).
            }
        }

        // INTRODUCER role: forward toward the host we originated the Provide on.
        if (target.tag == .importedCap) {
            if (target.imported_cap) |promise_export_id| {
                if (self.findOriginatedProvideForPromise(promise_export_id)) |op| {
                    if (op.provide_peer) |provide_peer| {
                        // E-ORDER (M-11): hold the forward until the parked
                        // pre-resolution calls have been replayed toward the
                        // host, or the released Accept overtakes them.
                        if (!op.replay_flushed) {
                            try op.stashAcceptDisembargo(self.allocator, embargo);
                            return;
                        }
                        // Copy out of the map entry before sending: the nested
                        // delivery may mutate `outbound_provides` and
                        // invalidate `op`.
                        const provide_question_id = op.provide_question_id;
                        try self.forwardAcceptDisembargo(provide_peer, provide_question_id, embargo);
                        return;
                    }
                }
            }
        }

        // Neither role: nothing to release, nothing to forward. Benign — a late
        // or duplicate Disembargo whose Accept already drained (or a target we do
        // not recognise). Dropping it silently matches the receiver-loopback arm.
        log.debug("accept-disembargo matched no held Accept and no originated provide; dropping", .{});
    }

    /// If this peer originated a three-party handoff by resolving the promise
    /// EXPORT `promise_export_id` to a third party, return that coupling.
    /// Otherwise null — this peer is not the introducer for that promise. The
    /// returned pointer aliases the `outbound_provides` map entry: copy what
    /// you need out of it before any send (nested delivery can mutate the map).
    fn findOriginatedProvideForPromise(self: *Peer, promise_export_id: u32) ?*OutboundProvide {
        var it = self.outbound_provides.valueIterator();
        while (it.next()) |op| {
            if (op.resolved_promise_export_id == promise_export_id) return op;
        }
        return null;
    }

    /// Forward a `context.accept` Disembargo to the capability host (VatC) on
    /// the connection this introducer (VatB) sent the paired `Provide` on. The
    /// recipient's original target names its promise import in the A↔B id space
    /// and is meaningless on this hop; per rpc.capnp:899 (and matching the C++
    /// reference host, which resolves the target against its answer table) the
    /// forwarded frame is REWRITTEN to address the Provide question itself: a
    /// promisedAnswer target in the B↔C answer space, with no transform.
    fn forwardAcceptDisembargo(_: *Peer, provide_peer: *Peer, provide_question_id: u32, embargo: []const u8) !void {
        provide_peer.assertThreadAffinity();
        const target = protocol.MessageTarget{
            .tag = .promisedAnswer,
            .imported_cap = null,
            .promised_answer = .{ .question_id = provide_question_id, .transform = .{ .list = null } },
        };
        var builder = protocol.MessageBuilder.init(provide_peer.allocator);
        defer builder.deinit();
        try builder.buildDisembargoAccept(target, embargo);
        try provide_peer.sendBuilder(&builder);
    }

    /// Flush (or arm) the accept-Disembargo path for a resolve-originated
    /// coupling: mark the parked-call replay as done, and if a Disembargo was
    /// stashed while the replay was pending, forward it now. Called on BOTH the
    /// success path of `resolvePromiseExportToThirdParty` (after the replay)
    /// and its post-Resolve error paths — once the Resolve is on the wire, a
    /// stashed Disembargo must never be stranded (the recipient's Accept would
    /// hang forever behind it). Best-effort: a forward failure is logged, not
    /// propagated.
    fn flushStashedAcceptDisembargo(self: *Peer, vine_id: u32) void {
        const op = self.outbound_provides.getPtr(vine_id) orelse return;
        op.replay_flushed = true;
        const stash = op.stashed_accept_disembargo orelse return;
        op.stashed_accept_disembargo = null;
        defer self.allocator.free(stash);
        const provide_peer = op.provide_peer orelse return;
        const provide_question_id = op.provide_question_id;
        self.forwardAcceptDisembargo(provide_peer, provide_question_id, stash) catch |err| {
            log.warn("failed to flush stashed accept-disembargo for vine {}: {}", .{ vine_id, err });
        };
    }

    fn makeProvideTarget(self: *Peer, resolved: cap_table.ResolvedCap) !ProvideTarget {
        const descriptors = cap_table.descriptors;
        return switch (resolved) {
            .none => error.PromisedAnswerMissing,
            .imported => |cap| .{ .local = .{ .origin_code = descriptors.originCodeForTag(.receiverHosted), .cap_id = cap.id } },
            // Preserve the export-promise sub-classification (see exportedCapTag).
            .exported => |cap| .{ .local = .{ .origin_code = descriptors.originCodeForTag(self.exportedCapTag(cap.id)), .cap_id = cap.id } },
            .promised => |promised| .{
                .promised = try cap_table.OwnedPromisedAnswer.fromPromised(self.allocator, promised),
            },
        };
    }

    fn cloneProvideTarget(self: *Peer, target: *const ProvideTarget) !ProvideTarget {
        return switch (target.*) {
            .local => |local| .{ .local = local },
            .promised => |promised| .{
                .promised = try cap_table.OwnedPromisedAnswer.fromQuestionAndOps(
                    self.allocator,
                    promised.question_id,
                    promised.ops,
                ),
            },
        };
    }

    fn putPendingJoinAcceptOwned(self: *Peer, hosted: *HostedJoin, target: ProvideTarget) !void {
        // Sample application time before publishing either side of the
        // cross-peer lease. The caller holds Join operation guards for both
        // owner and Accept peers, so close/deinit requested by Clock.now is
        // deferred until this admission finishes.
        const deadline_ns = try self.newJoinDeadline();
        try ensureCountLimit(
            self.pending_join_accepts.contains(hosted.provision),
            self.pending_join_accepts.count(),
            self.limits.max_pending_join_accepts,
        );
        try self.ensureJoinRecordCapacity(1);
        try self.registerJoinAcceptHost(hosted);
        errdefer self.deregisterJoinAcceptHost(hosted);

        const entry = try self.pending_join_accepts.getOrPut(hosted.provision);
        if (entry.found_existing) return error.DuplicateJoinProvision;
        entry.value_ptr.* = .{ .hosted = hosted, .target = target };
        hosted.accept_peer = self;
        hosted.accept_live = true;
        hosted.deadline_ns = deadline_ns;
        self.noteJoinDeadline(hosted.deadline_ns);
    }

    fn takePendingJoinAccept(self: *Peer, provision: []const u8) ?ProvideTarget {
        if (self.pending_join_accepts.fetchRemove(provision)) |removed| {
            const hosted = removed.value.hosted;
            const target = removed.value.target;
            self.deregisterJoinAcceptHost(hosted);
            hosted.accept_live = false;
            hosted.accept_peer = null;
            hosted.deadline_ns = null;
            self.refreshNextJoinDeadline();
            hosted.owner_peer.retireHostedJoinNetwork(hosted);
            return target;
        }
        return null;
    }

    fn rememberPendingJoinResultAnswer(self: *Peer, answer_id: u32, hosted: *HostedJoin) !void {
        try ensureCountLimit(
            self.pending_join_result_answers.contains(answer_id),
            self.pending_join_result_answers.count(),
            self.limits.max_pending_join_questions,
        );
        try self.ensureJoinRecordCapacity(1);
        const entry = try self.pending_join_result_answers.getOrPut(answer_id);
        if (entry.found_existing) return error.DuplicateJoinQuestionId;
        entry.value_ptr.* = .{ .hosted = hosted };
        hosted.result_refs = std.math.add(usize, hosted.result_refs, 1) catch {
            _ = self.pending_join_result_answers.remove(answer_id);
            return error.PeerLimitExceeded;
        };
    }

    fn clearPendingJoinResultAnswer(self: *Peer, answer_id: u32) void {
        const removed = self.pending_join_result_answers.fetchRemove(answer_id) orelse return;
        const hosted = removed.value.hosted;
        std.debug.assert(hosted.result_refs > 0);
        hosted.result_refs -= 1;
        // Explicitly Finishing the final JoinResult answer cancels its direct
        // pickup lease. Result-path transport close is intentionally different:
        // it detaches these answer records without interpreting them as Finish,
        // so a distinct live Accept host can still serve the committed
        // provision until TTL or owner teardown.
        if (hosted.result_refs == 0 and hosted.accept_live) {
            // Canonical cancellation guards both the result owner and a
            // distinct Accept host across the captured JoinNetwork callback.
            // Calling through the Accept peer directly would let a reentrant
            // callback deinit that peer while its cleanup frame still borrowed
            // the peer allocator.
            self.cancelHostedJoin(hosted);
            return;
        }
        self.maybeDestroyHostedJoin(hosted);
    }

    /// Drop the canonical result reference held by a completing-answer
    /// tombstone.  The tombstone itself remains published until the whole
    /// fanout has finished, keeping the inbound answer ID unavailable to
    /// callback-triggered reuse.
    fn dropCompletingJoinResultRef(self: *Peer, completing: *CompletingJoinAnswer) void {
        const hosted = completing.hosted orelse return;
        completing.hosted = null;
        std.debug.assert(hosted.owner_peer == self);
        std.debug.assert(hosted.result_refs > 0);
        hosted.result_refs -= 1;
        if (hosted.result_refs == 0 and hosted.accept_live) {
            self.cancelHostedJoin(hosted);
            return;
        }
        self.maybeDestroyHostedJoin(hosted);
    }

    fn putCompletingJoinAnswerAssumeCapacity(
        self: *Peer,
        answer_id: u32,
        counts_as_join_record: bool,
    ) void {
        std.debug.assert(!self.completing_join_answers.contains(answer_id));
        self.completing_join_answers.putAssumeCapacity(answer_id, .{
            .counts_as_join_record = counts_as_join_record,
        });
        if (counts_as_join_record) self.completing_join_answer_records += 1;
    }

    fn retireCompletingJoinAnswerAccounting(self: *Peer, completing: *CompletingJoinAnswer) void {
        if (!completing.counts_as_join_record) return;
        completing.counts_as_join_record = false;
        std.debug.assert(self.completing_join_answer_records > 0);
        self.completing_join_answer_records -= 1;
    }

    fn removeCompletingJoinAnswer(self: *Peer, answer_id: u32) bool {
        const removed = self.completing_join_answers.fetchRemove(answer_id) orelse return false;
        if (removed.value.counts_as_join_record) {
            std.debug.assert(self.completing_join_answer_records > 0);
            self.completing_join_answer_records -= 1;
        }
        return true;
    }

    /// Finish may arrive synchronously from an observer or Return transport
    /// callback while a complete Join is still fanning out.  Marking rather
    /// than removing preserves the answer reservation through later sends.
    fn finishCompletingJoinAnswer(self: *Peer, answer_id: u32, release_result_caps: bool) bool {
        const completing = self.completing_join_answers.getPtr(answer_id) orelse return false;
        if (completing.finished) return true;
        completing.finished = true;
        completing.release_result_caps = release_result_caps;
        // Finish makes this answer operationally retired immediately, even
        // though its uncounted tombstone still reserves the wire ID until the
        // surrounding fanout unwinds.
        self.retireCompletingJoinAnswerAccounting(completing);
        self.dropCompletingJoinResultRef(completing);
        return true;
    }

    fn forgetPendingJoinResultAnswer(self: *Peer, answer_id: u32) void {
        self.clearPendingJoinResultAnswer(answer_id);
    }

    fn retireCompletingJoinTransitionAccounting(
        self: *Peer,
        join_state: *JoinState,
        transition_live: *bool,
    ) void {
        if (transition_live.*) {
            std.debug.assert(self.completing_join_records > 0);
            self.completing_join_records -= 1;
            transition_live.* = false;
        }
        var it = join_state.parts.valueIterator();
        while (it.next()) |part| {
            if (self.completing_join_answers.getPtr(part.question_id)) |completing| {
                self.retireCompletingJoinAnswerAccounting(completing);
            }
        }
    }

    fn completeJoinLegacy(self: *Peer, join_id: u32) !void {
        const pending = self.pending_joins.get(join_id) orelse return;
        if (pending.parts.count() == 0) return;
        // `ensureJoinBudget` reserved the completing map before the final part
        // was published. Updating the scalar record first can therefore fail
        // only on an impossible configured-state overflow, while the bucket is
        // still wholly live and retryable.
        const completing_records = try std.math.add(usize, self.completing_join_records, 1);
        const removed = self.pending_joins.fetchRemove(join_id) orelse return;
        var join_state = removed.value;
        defer JoinState.deinit(&join_state, self.allocator);
        self.refreshNextJoinDeadline();

        self.completing_join_records = completing_records;
        var transition_live = true;
        defer if (transition_live) {
            std.debug.assert(self.completing_join_records > 0);
            self.completing_join_records -= 1;
        };
        var reserve_it = join_state.parts.valueIterator();
        while (reserve_it.next()) |part| {
            self.putCompletingJoinAnswerAssumeCapacity(part.question_id, true);
        }
        defer {
            var cleanup_answers = join_state.parts.valueIterator();
            while (cleanup_answers.next()) |part| {
                _ = self.removeCompletingJoinAnswer(part.question_id);
                _ = self.finished_early_answers.remove(part.question_id);
            }
        }
        var detach_it = join_state.parts.valueIterator();
        while (detach_it.next()) |part| _ = self.pending_join_questions.remove(part.question_id);

        var first_target: ?*const ProvideTarget = null;
        var all_equal = true;
        var target_it = join_state.parts.valueIterator();
        while (target_it.next()) |part| {
            if (first_target) |target| {
                if (!provideTargetsEqual(target, &part.target)) {
                    all_equal = false;
                    break;
                }
            } else first_target = &part.target;
        }

        // Legacy completion has no steady Join lease. Retire every gauge before
        // the first Return while retaining uncounted answer tombstones across
        // synchronous Finish/reuse callbacks.
        std.debug.assert(self.completing_join_records > 0);
        self.completing_join_records -= 1;
        transition_live = false;
        var retire_it = join_state.parts.valueIterator();
        while (retire_it.next()) |part| {
            if (self.completing_join_answers.getPtr(part.question_id)) |completing| {
                self.retireCompletingJoinAnswerAccounting(completing);
            }
        }

        var send_it = join_state.parts.valueIterator();
        while (send_it.next()) |part| {
            const completing = self.completing_join_answers.get(part.question_id) orelse continue;
            if (completing.finished) continue;
            if (all_equal) {
                self.sendReturnProvidedTarget(part.question_id, first_target orelse &part.target) catch |err| {
                    try self.sendReturnException(part.question_id, joinWireReason(err));
                };
            } else {
                try self.sendReturnException(part.question_id, "join target mismatch");
            }
        }
    }

    const DetachedJoinAccept = struct {
        allocator: std.mem.Allocator,
        target: ProvideTarget,
    };

    /// Detach the Accept-host half without invoking the application-supplied
    /// JoinNetwork callback. Callers can therefore remove every cross-peer
    /// borrow before cancellation re-enters arbitrary host code.
    fn detachHostedJoinAcceptNoCallback(self: *Peer, hosted: *HostedJoin) ?DetachedJoinAccept {
        if (!hosted.accept_live or hosted.accept_peer != self) return null;
        const removed = self.pending_join_accepts.fetchRemove(hosted.provision) orelse return null;
        std.debug.assert(removed.value.hosted == hosted);
        self.deregisterJoinAcceptHost(hosted);
        hosted.accept_live = false;
        hosted.accept_peer = null;
        hosted.deadline_ns = null;
        self.refreshNextJoinDeadline();
        return .{ .allocator = self.allocator, .target = removed.value.target };
    }

    fn ownHostedJoin(self: *Peer, hosted: *HostedJoin) !void {
        self.ensureJoinRecordCapacity(1) catch {
            events.emitResourceRejection(
                self.observer,
                .peer,
                .unknown,
                .join_records,
                saturatingAdd(self.joinRecordCount(), 1),
                self.limits.max_pending_join_records,
                error.PeerLimitExceeded,
            );
            return error.PeerLimitExceeded;
        };
        ensureByteLimit(
            self.join_accept_bytes,
            hosted.provision.len,
            self.limits.max_pending_join_accept_bytes,
        ) catch return error.PeerLimitExceeded;
        try self.hosted_joins.putNoClobber(hosted, {});
        hosted.owner_record_live = true;
        self.join_accept_bytes += hosted.provision.len;
        hosted.bytes_charged = true;
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .join_accept_bytes,
            self.join_accept_bytes - hosted.provision.len,
            self.join_accept_bytes,
            self.limits.max_pending_join_accept_bytes,
        );
    }

    /// Publish canonical ownership by consuming the one record reserved for a
    /// detached complete bucket.  Capacity was checked before the JoinNetwork
    /// callback; unlike `ownHostedJoin`, this is a record-for-record exchange,
    /// not an additional admission.
    fn ownHostedJoinFromCompletion(self: *Peer, hosted: *HostedJoin) !void {
        ensureByteLimit(
            self.join_accept_bytes,
            hosted.provision.len,
            self.limits.max_pending_join_accept_bytes,
        ) catch return error.PeerLimitExceeded;
        try self.hosted_joins.putNoClobber(hosted, {});
        std.debug.assert(self.completing_join_records > 0);
        self.completing_join_records -= 1;
        hosted.owner_record_live = true;
        self.join_accept_bytes += hosted.provision.len;
        hosted.bytes_charged = true;
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .join_accept_bytes,
            self.join_accept_bytes - hosted.provision.len,
            self.join_accept_bytes,
            self.limits.max_pending_join_accept_bytes,
        );
    }

    /// Cancel the captured network provision once. State that can re-enter this
    /// owner is already detached; `operation_depth` prevents a nested cleanup
    /// from destroying the canonical object while the callback borrows it.
    fn retireHostedJoinNetwork(self: *Peer, hosted: *HostedJoin) void {
        self.enterJoinOperation();
        defer self.leaveJoinOperation();
        if (!hosted.network_live) {
            self.maybeDestroyHostedJoin(hosted);
            return;
        }
        hosted.network_live = false;
        self.detachHostedJoinOwnerRecordNoCallback(hosted);
        hosted.operation_depth += 1;
        hosted.network.cancelHostJoinResult(hosted.provision);
        hosted.operation_depth -= 1;
        self.maybeDestroyHostedJoin(hosted);
    }

    /// Remove the live owner provision record before an observer or captured
    /// network callback can re-enter either peer. A successful Accept can
    /// retire the network token while result answers still anchor the
    /// provision allocation; keep those bytes charged until the final result
    /// reference is Finished. Forced cancellation has already detached every
    /// result reference, so its bytes are refunded before the callback.
    fn detachHostedJoinOwnerRecordNoCallback(self: *Peer, hosted: *HostedJoin) void {
        if (hosted.owner_record_live) {
            std.debug.assert(self.hosted_joins.remove(hosted));
            hosted.owner_record_live = false;
        }
        if (hosted.cancelled or hosted.result_refs == 0) self.refundHostedJoinBytes(hosted);
    }

    fn refundHostedJoinBytes(self: *Peer, hosted: *HostedJoin) void {
        if (!hosted.bytes_charged) return;
        std.debug.assert(self.join_accept_bytes >= hosted.provision.len);
        self.join_accept_bytes -= hosted.provision.len;
        hosted.bytes_charged = false;
    }

    fn maybeDestroyHostedJoin(self: *Peer, hosted: *HostedJoin) void {
        if (hosted.owner_peer != self) return;
        if (hosted.accept_live or hosted.result_refs != 0 or hosted.operation_depth != 0) return;
        if (hosted.network_live) {
            self.retireHostedJoinNetwork(hosted);
            return;
        }
        self.detachHostedJoinOwnerRecordNoCallback(hosted);
        self.refundHostedJoinBytes(hosted);
        self.allocator.free(hosted.provision);
        self.allocator.destroy(hosted);
    }

    /// Canonical forced retirement used by expiry, Accept-host close, and
    /// owner teardown. Every map/backlink is detached before the network
    /// callback, making repeated close/deinit and callback reentrancy no-ops.
    fn cancelHostedJoin(self: *Peer, hosted: *HostedJoin) void {
        if (hosted.owner_peer != self) return;

        hosted.cancelled = true;

        var guards = JoinOperationGuards{};
        guards.add(self);
        if (hosted.accept_peer) |accept_peer| guards.add(accept_peer);
        guards.enter();
        defer guards.leave();

        var detached_accept: ?DetachedJoinAccept = null;
        if (hosted.accept_peer) |accept_peer| {
            detached_accept = accept_peer.detachHostedJoinAcceptNoCallback(hosted);
        }

        while (hosted.result_refs != 0) {
            var answer_id: ?u32 = null;
            var it = self.pending_join_result_answers.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.hosted == hosted) {
                    answer_id = entry.key_ptr.*;
                    break;
                }
            }
            if (answer_id) |id| {
                _ = self.pending_join_result_answers.remove(id);
                hosted.result_refs -= 1;
                continue;
            }

            // A canonical may be canceled synchronously while its JoinResult
            // fanout is still running. Neutralize those pre-reserved refs but
            // keep their answer-ID tombstones live so the outer fanout emits
            // one generic terminal per remaining answer without stale pickup.
            var completing_it = self.completing_join_answers.valueIterator();
            var detached_completing = false;
            while (completing_it.next()) |completing| {
                if (completing.hosted != hosted) continue;
                self.retireCompletingJoinAnswerAccounting(completing);
                completing.hosted = null;
                hosted.result_refs -= 1;
                detached_completing = true;
                break;
            }
            if (!detached_completing) {
                std.debug.assert(hosted.result_refs == 0);
                break;
            }
        }

        if (detached_accept) |*detached| detached.target.deinit(detached.allocator);
        self.retireHostedJoinNetwork(hosted);
    }

    fn cancelAllHostedJoins(self: *Peer) void {
        while (self.hosted_joins.count() != 0) {
            var it = self.hosted_joins.keyIterator();
            const hosted = (it.next() orelse break).*;
            self.cancelHostedJoin(hosted);
        }
        // A successfully consumed Accept retires the hosted provision before
        // its JoinResult answers are explicitly Finished. Those canonicals are
        // anchored only by this answer table and still need owner teardown.
        while (self.pending_join_result_answers.count() != 0) {
            var it = self.pending_join_result_answers.valueIterator();
            const hosted = (it.next() orelse break).hosted;
            self.cancelHostedJoin(hosted);
        }
    }

    fn sendReturnJoinResultPayload(self: *Peer, answer_id: u32, result_payload: []const u8) !void {
        const BuildCtx = struct {
            allocator: std.mem.Allocator,
            result_payload: []const u8,

            fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const ctx: *const @This() = castCtx(*const @This(), ctx_ptr);
                var result_msg = try message.Message.initUnvalidated(ctx.allocator, ctx.result_payload);
                defer result_msg.deinit();
                const result = try result_msg.getRootAnyPointer();

                var payload = try ret.payloadTyped();
                const content = try payload.initContent();
                try message.cloneAnyPointer(result, content);
                _ = try ret.initCapTableTyped(0);
            }
        };

        var ctx = BuildCtx{
            .allocator = self.allocator,
            .result_payload = result_payload,
        };
        try self.sendReturnResults(answer_id, &ctx, BuildCtx.build);
    }

    fn crossPeerProxyContextForExport(self: *Peer, export_id: u32) ?*CrossPeerProxyContext {
        const entry = self.exports.getPtr(export_id) orelse return null;
        const handler = entry.handler orelse return null;
        if (handler.on_call != CrossPeerProxyContext.onCall) return null;
        return castCtx(*CrossPeerProxyContext, handler.ctx);
    }

    fn crossPeerJoinTargetForResolved(target: cap_table.ResolvedCap) !protocol.MessageTarget {
        return switch (target) {
            .imported => |cap| .{
                .tag = .importedCap,
                .imported_cap = cap.id,
                .promised_answer = null,
            },
            .exported, .promised, .none => error.UnsupportedCrossPeerJoinTarget,
        };
    }

    fn putPendingJoinRelay(
        self: *Peer,
        owner_answer_id: u32,
        source_peer: *Peer,
        source_question_id: u32,
    ) !void {
        // Do not call an application clock while a map entry or reciprocal
        // backlink is half-published.
        const deadline_ns = try self.newJoinDeadline();
        self.ensureJoinRecordCapacity(1) catch {
            events.emitResourceRejection(
                self.observer,
                .peer,
                .unknown,
                .join_records,
                saturatingAdd(self.joinRecordCount(), 1),
                self.limits.max_pending_join_records,
                error.PeerLimitExceeded,
            );
            return error.PeerLimitExceeded;
        };
        try ensureCountLimit(
            self.pending_join_relays.contains(owner_answer_id),
            self.pending_join_relays.count(),
            self.limits.max_pending_join_questions,
        );
        const settlement_answers = std.math.add(
            usize,
            self.completing_join_answers.count(),
            self.pending_join_questions.count(),
        ) catch return error.PeerLimitExceeded;
        const with_relays = std.math.add(
            usize,
            settlement_answers,
            self.pending_join_relays.count(),
        ) catch return error.PeerLimitExceeded;
        const settlement_capacity_usize = std.math.add(usize, with_relays, 1) catch
            return error.PeerLimitExceeded;
        const settlement_capacity = std.math.cast(u32, settlement_capacity_usize) orelse
            return error.PeerLimitExceeded;
        try self.completing_join_answers.ensureTotalCapacity(settlement_capacity);
        const entry = try self.pending_join_relays.getOrPut(owner_answer_id);
        if (entry.found_existing) return error.DuplicateJoinQuestionId;
        entry.value_ptr.* = .{
            .source_peer = source_peer,
            .source_question_id = source_question_id,
            .deadline_ns = deadline_ns,
        };
        errdefer _ = self.pending_join_relays.remove(owner_answer_id);
        try source_peer.registerCrossPeerJoinRelay(self, owner_answer_id);
        self.noteJoinDeadline(entry.value_ptr.deadline_ns);
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .join_records,
            self.joinRecordCount() - 1,
            self.joinRecordCount(),
            self.limits.max_pending_join_records,
        );
    }

    fn clearPendingJoinRelay(
        self: *Peer,
        owner_answer_id: u32,
        send_downstream_finish: bool,
        release_result_caps: bool,
    ) !void {
        const pending = self.pending_join_relays.get(owner_answer_id) orelse return;
        const source_peer = pending.source_peer;
        var guards = JoinOperationGuards{};
        guards.add(self);
        if (source_peer) |peer| guards.add(peer);
        guards.enter();
        defer guards.leave();

        const removed = self.detachPendingJoinRelay(owner_answer_id) orelse return;
        if (source_peer) |peer| {
            if (send_downstream_finish) {
                peer.sendJoinRelayFinishAndNeutralize(
                    removed.source_question_id,
                    release_result_caps,
                ) catch |err| {
                    // Restore both halves only after the failed callback has
                    // returned. During the send, observers see the relay and
                    // reciprocal backlink wholly detached.
                    if (!self.pending_join_relays.contains(owner_answer_id)) {
                        self.pending_join_relays.putAssumeCapacity(owner_answer_id, removed);
                        peer.registerCrossPeerJoinRelay(self, owner_answer_id) catch |restore_err| {
                            _ = self.pending_join_relays.remove(owner_answer_id);
                            peer.neutralizeJoinRelayQuestion(removed.source_question_id);
                            self.refreshNextJoinDeadline();
                            return restore_err;
                        };
                        self.noteJoinDeadline(removed.deadline_ns);
                    }
                    return err;
                };
            }
        }
    }

    /// Remove both halves of a relay without invoking sends or callbacks.
    /// Callers guard `self` and the optional source peer before entering.
    fn detachPendingJoinRelay(self: *Peer, owner_answer_id: u32) ?CrossPeerJoinRelay {
        const removed = self.pending_join_relays.fetchRemove(owner_answer_id) orelse return null;
        if (removed.value.source_peer) |source_peer| {
            source_peer.deregisterCrossPeerJoinRelay(self, owner_answer_id);
        }
        self.refreshNextJoinDeadline();
        return removed.value;
    }

    /// Terminal relay retirement for timeout/transport close. Unlike explicit
    /// Finish retry, a failed best-effort downstream Finish cannot resurrect an
    /// expired or disconnected record.
    fn retirePendingJoinRelayTerminal(
        self: *Peer,
        owner_answer_id: u32,
        emit_timeout: bool,
        send_upstream_exception: bool,
    ) bool {
        const pending = self.pending_join_relays.get(owner_answer_id) orelse return false;
        const source_peer = pending.source_peer;
        var guards = JoinOperationGuards{};
        guards.add(self);
        if (source_peer) |peer| guards.add(peer);
        guards.enter();
        defer guards.leave();

        const detached = self.detachPendingJoinRelay(owner_answer_id) orelse return false;
        self.putCompletingJoinAnswerAssumeCapacity(owner_answer_id, false);
        defer {
            _ = self.removeCompletingJoinAnswer(owner_answer_id);
            _ = self.finished_early_answers.remove(owner_answer_id);
        }
        // From this point onward every local record and reciprocal backlink is
        // gone. Observer, wire-send, and deinit callbacks may safely re-enter.
        if (emit_timeout) events.emitJoinTimeout(self.observer, owner_answer_id);
        if (source_peer) |peer| {
            peer.sendJoinRelayFinishAndNeutralize(detached.source_question_id, false) catch |err| {
                log.debug("terminal Join relay Finish failed for question {}: {}", .{ detached.source_question_id, err });
                peer.neutralizeJoinRelayQuestion(detached.source_question_id);
            };
        }
        if (send_upstream_exception and !detached.upstream_terminal_started) {
            if (self.completing_join_answers.getPtr(owner_answer_id)) |completing| {
                // Relay teardown state is already fully detached. Preserve only
                // the uncounted answer-ID tombstone across the terminal send.
                self.retireCompletingJoinAnswerAccounting(completing);
            }
            self.sendReturnException(owner_answer_id, "join unavailable") catch |err| {
                log.debug("expired Join relay exception send failed for answer {}: {}", .{ owner_answer_id, err });
            };
        }
        return true;
    }

    fn tryHandleCrossPeerProxyJoin(self: *Peer, join: protocol.Join) !bool {
        if (join.target.tag != .importedCap) return false;
        const export_id = join.target.imported_cap orelse return false;
        const proxy_ctx = self.crossPeerProxyContextForExport(export_id) orelse return false;
        const source_peer = proxy_ctx.source_peer orelse {
            try self.sendReturnException(join.question_id, "cross-peer proxy source disconnected");
            return true;
        };
        if (source_peer.is_shutting_down) {
            try self.sendReturnException(join.question_id, "cross-peer proxy source disconnected");
            return true;
        }
        try self.forwardCrossPeerProxyJoin(join, source_peer, proxy_ctx.target);
        return true;
    }

    fn forwardCrossPeerProxyJoin(
        self: *Peer,
        join: protocol.Join,
        source_peer: *Peer,
        source_target: cap_table.ResolvedCap,
    ) !void {
        var guards = JoinOperationGuards{};
        guards.add(self);
        guards.add(source_peer);
        guards.enter();
        defer guards.leave();

        const downstream_target = crossPeerJoinTargetForResolved(source_target) catch |err| {
            try self.sendReturnException(join.question_id, @errorName(err));
            return;
        };

        const relay = try source_peer.allocator.create(CrossPeerJoinRelayContext);
        relay.* = .{
            .owner_peer = self,
            .owner_answer_id = join.question_id,
        };
        var relay_owned = true;
        errdefer if (relay_owned) source_peer.allocator.destroy(relay);

        const source_question_id = source_peer.allocateQuestionNoRestore(relay, onCrossPeerJoinReturn) catch |err| {
            try self.sendReturnException(join.question_id, joinWireReason(err));
            return;
        };
        var question_owned = true;
        errdefer if (question_owned) source_peer.removeQuestionAndDeinit(source_question_id);

        const source_question = source_peer.questions.getPtr(source_question_id) orelse return error.MissingAllocatedQuestion;
        source_question.suppress_auto_finish = true;
        // The owner relay's Join-domain deadline is authoritative. A generic
        // outbound-call deadline here would race it in the source peer's clock
        // domain and leak "deadline exceeded" instead of the redacted Join
        // terminal.
        source_question.deadline_ns = null;
        source_question.deinit_ctx = CrossPeerJoinRelayContext.deinit;
        relay_owned = false;

        self.putPendingJoinRelay(join.question_id, source_peer, source_question_id) catch |err| {
            question_owned = false;
            source_peer.removeQuestionAndDeinit(source_question_id);
            const reason = joinWireReason(err);
            try self.sendReturnException(join.question_id, reason);
            return;
        };
        var relay_registered = true;
        errdefer if (relay_registered) {
            self.clearPendingJoinRelay(join.question_id, false, false) catch |err| {
                log.debug("cross-peer join relay: failed to roll back relay {}: {}", .{ join.question_id, err });
            };
        };

        var relay_settled = false;
        relay.settled_flag = &relay_settled;

        var builder = protocol.MessageBuilder.init(source_peer.allocator);
        defer builder.deinit();
        try builder.buildJoin(source_question_id, downstream_target, join.key_part);
        source_peer.sendBuilder(&builder) catch |err| {
            if (relay_settled) {
                question_owned = false;
                relay_registered = false;
                return;
            }
            relay_registered = false;
            self.clearPendingJoinRelay(join.question_id, false, false) catch |clear_err| {
                log.debug("cross-peer join relay: failed to clear relay {} after send failure: {}", .{
                    join.question_id,
                    clear_err,
                });
            };
            question_owned = false;
            source_peer.removeQuestionAndDeinit(source_question_id);
            try self.sendReturnException(join.question_id, joinWireReason(err));
            return;
        };

        if (relay_settled) {
            question_owned = false;
            relay_registered = false;
            return;
        }
        relay.settled_flag = null;
        question_owned = false;
        relay_registered = false;
    }

    fn onCrossPeerJoinReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *CrossPeerJoinRelayContext = castCtx(*CrossPeerJoinRelayContext, ctx_ptr);
        const owner_peer = ctx.owner_peer;
        const owner_answer_id = ctx.owner_answer_id;
        if (ctx.settled_flag) |flag| flag.* = true;
        var guards = JoinOperationGuards{};
        guards.add(owner_peer);
        guards.add(peer);
        guards.enter();
        defer guards.leave();
        // Registered after guards.leave so LIFO destroys the source-owned ctx
        // while both peers are still protected from callback-triggered deinit.
        defer CrossPeerJoinRelayContext.deinit(peer.allocator, ctx);

        if (!owner_peer.pending_join_relays.contains(owner_answer_id)) return;

        switch (ret.tag) {
            .results => {
                const relay = owner_peer.pending_join_relays.getPtr(owner_answer_id) orelse return;
                relay.upstream_terminal_started = true;
                relayReturnAcrossPeers(owner_peer, owner_answer_id, peer, ret, inbound_caps, true) catch |err| {
                    const detached = owner_peer.detachPendingJoinRelay(owner_answer_id) orelse return;
                    owner_peer.putCompletingJoinAnswerAssumeCapacity(owner_answer_id, false);
                    defer {
                        _ = owner_peer.removeCompletingJoinAnswer(owner_answer_id);
                        _ = owner_peer.finished_early_answers.remove(owner_answer_id);
                    }
                    peer.sendJoinRelayFinishAndNeutralize(detached.source_question_id, false) catch |clear_err| {
                        log.debug("cross-peer join relay: failed to finish downstream question after relay error: {}", .{clear_err});
                        peer.neutralizeJoinRelayQuestion(detached.source_question_id);
                    };
                    owner_peer.sendReturnException(owner_answer_id, joinWireReason(err)) catch |send_err| {
                        log.debug("cross-peer join relay: failed to fail upstream question {}: {}", .{
                            owner_answer_id,
                            send_err,
                        });
                    };
                };
            },
            .exception => {
                const detached = owner_peer.detachPendingJoinRelay(owner_answer_id) orelse return;
                owner_peer.putCompletingJoinAnswerAssumeCapacity(owner_answer_id, false);
                defer {
                    _ = owner_peer.removeCompletingJoinAnswer(owner_answer_id);
                    _ = owner_peer.finished_early_answers.remove(owner_answer_id);
                }
                const reason = if (ret.exception) |exception| exception.reason else "cross-peer join failed";
                owner_peer.sendReturnException(owner_answer_id, reason) catch |send_err| {
                    log.debug("cross-peer join relay: failed to relay exception for question {}: {}", .{
                        owner_answer_id,
                        send_err,
                    });
                };
                peer.sendJoinRelayFinishAndNeutralize(detached.source_question_id, false) catch |clear_err| {
                    log.debug("cross-peer join relay: failed to finish exception result {}: {}", .{ owner_answer_id, clear_err });
                    peer.neutralizeJoinRelayQuestion(detached.source_question_id);
                };
            },
            else => {
                const detached = owner_peer.detachPendingJoinRelay(owner_answer_id) orelse return;
                owner_peer.putCompletingJoinAnswerAssumeCapacity(owner_answer_id, false);
                defer {
                    _ = owner_peer.removeCompletingJoinAnswer(owner_answer_id);
                    _ = owner_peer.finished_early_answers.remove(owner_answer_id);
                }
                owner_peer.sendReturnException(owner_answer_id, "cross-peer join relay: unexpected return") catch |send_err| {
                    log.debug("cross-peer join relay: failed to fail unexpected return for question {}: {}", .{
                        owner_answer_id,
                        send_err,
                    });
                };
                peer.sendJoinRelayFinishAndNeutralize(detached.source_question_id, false) catch |clear_err| {
                    log.debug("cross-peer join relay: failed to finish unexpected result {}: {}", .{ owner_answer_id, clear_err });
                    peer.neutralizeJoinRelayQuestion(detached.source_question_id);
                };
            },
        }
    }

    fn queueEmbargoedAccept(
        self: *Peer,
        answer_id: u32,
        provided_question_id: u32,
        embargo: []const u8,
    ) !void {
        try ensureCountLimit(
            self.pending_accept_embargo_by_question.contains(answer_id),
            self.pending_accept_embargo_by_question.count(),
            self.limits.max_pending_accepts,
        );
        if (!self.pending_accepts_by_embargo.contains(embargo)) {
            try ensureCountLimit(
                false,
                self.pending_accepts_by_embargo.count(),
                self.limits.max_pending_accept_embargo_buckets,
            );
            try ensureByteLimit(
                self.pendingAcceptEmbargoKeyBytes(),
                embargo.len,
                self.limits.max_pending_accept_embargo_bytes,
            );
        }

        try provide_accept_join.embargo_accepts.queueEmbargoedAcceptForPeer(
            Peer,
            PendingEmbargoedAccept,
            self,
            answer_id,
            provided_question_id,
            embargo,
        );
    }

    fn handleProvide(self: *Peer, provide: protocol.Provide) !void {
        // Expire due Accept-before-Provide reservations before a Provide can
        // adopt them. The cached due check makes the common path O(1).
        if (self.provision_index) |idx| _ = idx.sweepExpiredParkedAccepts();

        // A Provide must not reuse a question id already live as a Call /
        // Bootstrap answer or a pending Join (spec violation). Same-type
        // (provide) collisions fall through to the orchestration's specific
        // "duplicate provide question" abort below.
        if (try self.inboundAnswerQuestionIdInUse(provide.question_id) or
            self.pending_join_questions.contains(provide.question_id))
        {
            return error.DuplicateQuestionId;
        }
        try provide_accept_join.orchestration.handleProvide(
            Peer,
            ProvideEntry,
            ProvideTarget,
            self,
            self.allocator,
            provide,
            &self.provides_by_question,
            &self.provides_by_key,
            provide_accept_join.orchestration.captureProvideRecipientForPeerFn(
                Peer,
                third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            ),
            finish.freeOwnedFrameForPeerFn(Peer),
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
            Peer.ensureProvideBudget,
            provide_accept_join.resolveProvideTargetForPeerFn(
                Peer,
                provide_accept_join.resolveProvideImportedCapForPeerFn(Peer),
                provide_accept_join.resolveProvidePromisedAnswerForPeerFn(Peer, Peer.resolvePromisedAnswer),
            ),
            makeProvideTarget,
            ProvideTarget.deinit,
        );

        // Vat-wide registration (index mode only). On failure, roll the
        // just-stored per-peer provide back, send ONE abort with the matched
        // reason, and propagate the ORIGINAL error.
        if (self.provision_index) |idx| {
            var adopted: ?*ProvisionIndex.Provision = null;
            self.registerProvisionForProvide(idx, provide.question_id, &adopted) catch |err| {
                provide_accept_join.provides_state.clearProvideForPeer(
                    Peer,
                    ProvideEntry,
                    ProvideTarget,
                    self,
                    provide.question_id,
                    ProvideTarget.deinit,
                );
                const reason: []const u8 = switch (err) {
                    error.DuplicateProvideRecipient => "duplicate provide recipient",
                    else => @errorName(err),
                };
                peer_outbound_control.sendAbortViaSendFrame(Peer, self, reason, Peer.sendFrameControl) catch |send_err| {
                    log.debug("provide registration abort send failed: {}", .{send_err});
                };
                return err;
            };
            // PHASE B (adoption drain) runs OUTSIDE the rollback catch: the
            // provision is active and may serve accepts — a Provide rollback
            // would be wrong here. Only a terminal OOM re-raises, after every
            // parked entry was terminally handled.
            if (adopted) |prov| try self.drainAdoptedParkedAccepts(idx, prov);
        }
    }

    fn handleAccept(self: *Peer, accept: protocol.Accept) !void {
        if (try self.tryHandleJoinAccept(accept)) return;
        if (self.provision_index) |idx| return self.handleAcceptWithProvisionIndex(idx, accept);
        try provide_accept_join.orchestration.handleAccept(
            Peer,
            ProvideEntry,
            ProvideTarget,
            self,
            accept,
            &self.provides_by_question,
            &self.provides_by_key,
            provide_accept_join.orchestration.captureAcceptProvisionForPeerFn(
                Peer,
                third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            ),
            finish.freeOwnedFrameForPeerFn(Peer),
            Peer.queueEmbargoedAccept,
            Peer.sendReturnProvidedTarget,
            Peer.sendReturnException,
        );
    }

    fn tryHandleJoinAccept(self: *Peer, accept: protocol.Accept) !bool {
        const key = try provide_accept_join.orchestration.captureAcceptProvisionForPeer(
            Peer,
            self,
            accept,
            third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
        );
        defer if (key) |bytes| self.allocator.free(bytes);
        const provision = key orelse return false;

        // First establish that this token names an L4 lease without consuming
        // it. The Accept answer ID must be free across the complete inbound
        // namespace before canonical pickup becomes irreversible; a duplicate
        // frame leaves the provision wholly live for a fresh ID retry.
        if (!self.pending_join_accepts.contains(provision)) return false;
        if (try self.inboundQuestionIdInUse(accept.question_id)) {
            return error.DuplicateQuestionId;
        }

        var target = self.takePendingJoinAccept(provision) orelse return error.MissingJoinProvision;
        defer target.deinit(self.allocator);

        if (accept.embargo != null) {
            try self.sendReturnException(accept.question_id, "l4 join accept embargo unsupported");
            return true;
        }

        self.sendReturnProvidedTarget(accept.question_id, &target) catch |err| {
            try self.sendReturnException(accept.question_id, joinWireReason(err));
        };
        return true;
    }

    fn completeJoinWithL4Runtime(self: *Peer, join_id: u32) !void {
        self.enterJoinOperation();
        defer self.leaveJoinOperation();
        if (self.join_network == null) return error.NoJoinNetwork;
        const pending = self.pending_joins.get(join_id) orelse return;
        if (pending.parts.count() == 0) return;
        const completing_records = try std.math.add(usize, self.completing_join_records, 1);
        const network = try self.beginJoinNetworkBorrow();
        defer self.endJoinNetworkBorrow();
        const removed = self.pending_joins.fetchRemove(join_id) orelse return;
        var join_state = removed.value;
        defer JoinState.deinit(&join_state, self.allocator);
        self.refreshNextJoinDeadline();

        var first_target: ?*const ProvideTarget = null;
        var all_equal = true;
        var check_it = join_state.parts.iterator();
        while (check_it.next()) |entry| {
            if (first_target) |target| {
                if (!provideTargetsEqual(target, &entry.value_ptr.target)) {
                    all_equal = false;
                    break;
                }
            } else {
                first_target = &entry.value_ptr.target;
            }
        }

        // Reserve every completing answer id before the first application
        // callback or Return. These tombstones preserve the old bucket+parts
        // aggregate footprint (one transition record plus one per answer), so
        // callback reentry cannot steal capacity, Finish cannot vanish, and no
        // later answer in this fanout can be reused under us.
        self.completing_join_records = completing_records;
        var transition_live = true;
        defer if (transition_live) {
            std.debug.assert(self.completing_join_records > 0);
            self.completing_join_records -= 1;
        };
        var reserve_it = join_state.parts.valueIterator();
        while (reserve_it.next()) |part| {
            self.putCompletingJoinAnswerAssumeCapacity(part.question_id, true);
        }
        // This defer also owns any canonical result refs attached later. It is
        // intentionally installed before fanout guards so their operation pin
        // unwinds first; forced cleanup can then invoke callbacks without
        // destroying a canonical still borrowed by the fanout stack.
        defer {
            var cleanup_answers = join_state.parts.valueIterator();
            while (cleanup_answers.next()) |part| {
                if (self.completing_join_answers.getPtr(part.question_id)) |completing| {
                    self.retireCompletingJoinAnswerAccounting(completing);
                    self.dropCompletingJoinResultRef(completing);
                }
                _ = self.removeCompletingJoinAnswer(part.question_id);
                _ = self.finished_early_answers.remove(part.question_id);
            }
        }

        // Detach every part record before host callbacks. The local join_state
        // owns targets and the counted completion tombstones own IDs from here.
        var cleanup_it = join_state.parts.iterator();
        while (cleanup_it.next()) |entry| {
            _ = self.pending_join_questions.remove(entry.value_ptr.question_id);
        }

        if (!all_equal) {
            self.retireCompletingJoinTransitionAccounting(&join_state, &transition_live);
            var mismatch_it = join_state.parts.iterator();
            while (mismatch_it.next()) |entry| {
                try self.sendReturnException(entry.value_ptr.question_id, "join target mismatch");
            }
            return;
        }

        const target = first_target orelse return;
        const hosted_result = network.hostJoinResult(self.allocator, self, join_id) catch |err| {
            self.retireCompletingJoinTransitionAccounting(&join_state, &transition_live);
            var err_it = join_state.parts.iterator();
            while (err_it.next()) |entry| {
                try self.sendReturnException(entry.value_ptr.question_id, joinWireReason(err));
            }
            return;
        };
        const accept_peer = hosted_result.accept_peer;
        if (accept_peer != self) accept_peer.enterJoinOperation();
        defer if (accept_peer != self) accept_peer.leaveJoinOperation();
        defer self.allocator.free(hosted_result.result);

        // `hostJoinResult` is the only callback that can reveal the Accept
        // peer. The detached transition already reserves the complete owner
        // footprint; only the direct Accept is a positive record delta, and it
        // belongs either here or to the distinct Accept host. Reentrant Join
        // admission is refused while the captured network borrow is live, so
        // this capacity cannot be stolen between preflight and publication.
        const capacity_peer = accept_peer;
        capacity_peer.ensureJoinRecordCapacity(1) catch {
            const attempted_records = saturatingAdd(capacity_peer.joinRecordCount(), 1);
            self.retireCompletingJoinTransitionAccounting(&join_state, &transition_live);
            network.cancelHostJoinResult(hosted_result.provision);
            self.allocator.free(hosted_result.provision);
            events.emitResourceRejection(
                capacity_peer.observer,
                .peer,
                .unknown,
                .join_records,
                attempted_records,
                capacity_peer.limits.max_pending_join_records,
                error.PeerLimitExceeded,
            );
            var err_it = join_state.parts.iterator();
            while (err_it.next()) |entry| {
                try self.sendReturnException(entry.value_ptr.question_id, "join unavailable");
            }
            return;
        };

        var target_copy = accept_peer.cloneProvideTarget(target) catch |err| {
            self.retireCompletingJoinTransitionAccounting(&join_state, &transition_live);
            network.cancelHostJoinResult(hosted_result.provision);
            self.allocator.free(hosted_result.provision);
            var err_it = join_state.parts.iterator();
            while (err_it.next()) |entry| {
                try self.sendReturnException(entry.value_ptr.question_id, joinWireReason(err));
            }
            return;
        };
        var target_owned = true;
        defer if (target_owned) target_copy.deinit(accept_peer.allocator);

        const canonical = self.allocator.create(HostedJoin) catch |err| {
            self.retireCompletingJoinTransitionAccounting(&join_state, &transition_live);
            network.cancelHostJoinResult(hosted_result.provision);
            self.allocator.free(hosted_result.provision);
            var err_it = join_state.parts.iterator();
            while (err_it.next()) |entry| try self.sendReturnException(entry.value_ptr.question_id, joinWireReason(err));
            return;
        };
        canonical.* = .{
            .owner_peer = self,
            .accept_peer = null,
            .network = network,
            .provision = hosted_result.provision,
        };
        var canonical_unpublished = true;
        defer if (canonical_unpublished) {
            network.cancelHostJoinResult(canonical.provision);
            self.allocator.free(canonical.provision);
            self.allocator.destroy(canonical);
        };

        self.ownHostedJoinFromCompletion(canonical) catch |err| {
            const attempted_bytes = saturatingAdd(self.join_accept_bytes, canonical.provision.len);
            self.retireCompletingJoinTransitionAccounting(&join_state, &transition_live);
            canonical_unpublished = false;
            network.cancelHostJoinResult(canonical.provision);
            self.allocator.free(canonical.provision);
            self.allocator.destroy(canonical);
            if (err == error.PeerLimitExceeded) {
                events.emitResourceRejection(
                    self.observer,
                    .peer,
                    .unknown,
                    .join_accept_bytes,
                    attempted_bytes,
                    self.limits.max_pending_join_accept_bytes,
                    err,
                );
            }
            var err_it = join_state.parts.iterator();
            const reason = joinWireReason(err);
            while (err_it.next()) |entry| try self.sendReturnException(entry.value_ptr.question_id, reason);
            return;
        };
        canonical_unpublished = false;
        transition_live = false;

        accept_peer.putPendingJoinAcceptOwned(canonical, target_copy) catch |err| {
            const attempted_records = saturatingAdd(accept_peer.joinRecordCount(), 1);
            const emit_record_rejection = err == error.PeerLimitExceeded or
                err == error.JoinRecordLimitExceeded;
            self.retireCompletingJoinTransitionAccounting(&join_state, &transition_live);
            self.cancelHostedJoin(canonical);
            if (emit_record_rejection) {
                events.emitResourceRejection(
                    accept_peer.observer,
                    .peer,
                    .unknown,
                    .join_records,
                    attempted_records,
                    accept_peer.limits.max_pending_join_records,
                    error.PeerLimitExceeded,
                );
            }
            var err_it = join_state.parts.iterator();
            const reason = joinWireReason(err);
            while (err_it.next()) |entry| try self.sendReturnException(entry.value_ptr.question_id, reason);
            return;
        };
        target_owned = false;

        // Attach every unfinished tombstone to the canonical before the first
        // Return. The result map's capacity was reserved at part admission,
        // but publication waits until all callback-bearing sends finish so the
        // IDs remain coherently transitional as a group.
        var result_refs: usize = 0;
        var reserve_results_it = join_state.parts.valueIterator();
        while (reserve_results_it.next()) |part| {
            const completing = self.completing_join_answers.getPtr(part.question_id) orelse continue;
            if (completing.finished) continue;
            completing.hosted = canonical;
            result_refs += 1;
        }
        canonical.result_refs = result_refs;

        canonical.operation_depth += 1;
        defer {
            canonical.operation_depth -= 1;
            self.maybeDestroyHostedJoin(canonical);
        }
        if (canonical.result_refs == 0) {
            self.cancelHostedJoin(canonical);
            return;
        }

        var send_it = join_state.parts.iterator();
        while (send_it.next()) |entry| {
            const answer_id = entry.value_ptr.question_id;
            const before_send = self.completing_join_answers.get(answer_id) orelse continue;
            if (before_send.finished) continue;
            if (canonical.cancelled or before_send.hosted != canonical) {
                if (self.completing_join_answers.getPtr(answer_id)) |completing| {
                    self.retireCompletingJoinAnswerAccounting(completing);
                }
                try self.sendReturnException(answer_id, "join unavailable");
                continue;
            }

            self.sendReturnJoinResultPayload(answer_id, hosted_result.result) catch |err| {
                if (self.completing_join_answers.getPtr(answer_id)) |completing| {
                    if (!completing.finished) {
                        self.retireCompletingJoinAnswerAccounting(completing);
                        self.dropCompletingJoinResultRef(completing);
                        try self.sendReturnException(answer_id, joinWireReason(err));
                    }
                }
                continue;
            };
            canonical.published_results += 1;
            if (canonical.timeout_answer_id == null) {
                canonical.timeout_answer_id = answer_id;
            }
            if (self.completing_join_answers.getPtr(answer_id)) |completing| {
                if (!completing.finished and completing.hosted == canonical) {
                    completing.result_sent = true;
                }
            }
        }

        // Atomically exchange each surviving tombstone for its steady-state
        // result record. No callback occurs between remove and assume-capacity
        // insert, so the answer namespace never observes a reusable ID.
        var publish_it = join_state.parts.valueIterator();
        while (publish_it.next()) |part| {
            const answer_id = part.question_id;
            const completing = self.completing_join_answers.getPtr(answer_id) orelse continue;
            if (completing.result_sent and !completing.finished and completing.hosted == canonical) {
                _ = self.removeCompletingJoinAnswer(answer_id);
                self.pending_join_result_answers.putAssumeCapacity(answer_id, .{
                    .hosted = canonical,
                    .published = true,
                });
                continue;
            }
            self.retireCompletingJoinAnswerAccounting(completing);
            self.dropCompletingJoinResultRef(completing);
            _ = self.removeCompletingJoinAnswer(answer_id);
            _ = self.finished_early_answers.remove(answer_id);
        }

        if (canonical.result_refs == 0 and canonical.accept_live) {
            self.cancelHostedJoin(canonical);
        }
    }

    fn handleJoin(self: *Peer, join: protocol.Join) !void {
        // A captured JoinNetwork callback may synchronously inject another
        // Join on this peer. The callback cannot reveal the final Accept host
        // until it returns, so admitting nested work would let it consume the
        // positive-delta slot preflighted for the outer canonical lease.
        // Refuse generically; never reinterpret callback reentrancy as a
        // TTL/quota opt-out or expose which completion is in flight.
        if (self.join_network_borrows != 0) {
            try self.sendReturnException(join.question_id, "join unavailable");
            return;
        }
        // A Join must not reuse a question id already live as a Call /
        // Bootstrap answer or a Provide (spec violation). Same-type (join)
        // collisions fall through to the orchestration's specific "duplicate
        // join question" abort below.
        if (try self.inboundAnswerQuestionIdInUse(join.question_id) or
            self.provides_by_question.contains(join.question_id))
        {
            return error.DuplicateQuestionId;
        }
        if (try self.tryHandleCrossPeerProxyJoin(join)) return;
        const deadline_key = provide_accept_join.join_state.parseJoinKeyPart(JoinKeyPart, join.key_part) catch null;
        const sampled_deadline = if (deadline_key) |key|
            if (!self.pending_joins.contains(key.join_id))
                self.newJoinDeadline() catch {
                    try self.sendReturnException(join.question_id, "join unavailable");
                    return;
                }
            else
                null
        else
            null;
        const records_before = self.joinRecordCount();
        provide_accept_join.orchestration.handleJoin(
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
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
            provide_accept_join.resolveProvideTargetForPeerFn(
                Peer,
                provide_accept_join.resolveProvideImportedCapForPeerFn(Peer),
                provide_accept_join.resolveProvidePromisedAnswerForPeerFn(Peer, Peer.resolvePromisedAnswer),
            ),
            makeProvideTarget,
            ProvideTarget.deinit,
            JoinState.init,
            JoinState.deinit,
            Peer.ensureJoinBudget,
            if (self.join_network != null)
                Peer.completeJoinWithL4Runtime
            else
                Peer.completeJoinLegacy,
            Peer.sendReturnException,
        ) catch |err| {
            if (err == error.PeerLimitExceeded or
                err == error.JoinRecordLimitExceeded or
                err == error.JoinClockReentrant)
            {
                try self.sendReturnException(join.question_id, "join unavailable");
                return;
            }
            return err;
        };
        if (deadline_key) |key| {
            if (self.pending_joins.getPtr(key.join_id)) |join_state| {
                if (!join_state.deadline_initialized) {
                    join_state.deadline_ns = sampled_deadline;
                    join_state.deadline_initialized = true;
                    self.noteJoinDeadline(join_state.deadline_ns);
                }
            }
        }
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .join_records,
            records_before,
            self.joinRecordCount(),
            self.limits.max_pending_join_records,
        );
    }

    fn handleThirdPartyAnswer(self: *Peer, third_party_answer: protocol.ThirdPartyAnswer) !void {
        try third_party.adoption.handleThirdPartyAnswer(
            Peer,
            PendingThirdPartyAwait,
            self.allocator,
            self,
            third_party_answer,
            &self.pending_third_party_awaits,
            &self.pending_third_party_answers,
            third_party.adoption.captureThirdPartyCompletionForPeerFn(
                Peer,
                third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            ),
            finish.freeOwnedFrameForPeerFn(Peer),
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
            Peer.ensurePendingThirdPartyAnswerBudget,
            third_party.adoption.adoptPendingAwaitEntryForPeerFn(
                Peer,
                Question,
                PendingThirdPartyAwait,
                adoptThirdPartyAnswer,
            ),
        );
    }

    fn pendingMapHasQueuedQuestionId(
        self: *Peer,
        pending_map: *std.AutoHashMap(u32, std.ArrayList(PendingCall)),
        question_id: u32,
    ) bool {
        _ = self;
        var pending_it = pending_map.valueIterator();
        while (pending_it.next()) |pending_list| {
            // Compare the id decoded once at enqueue — no per-scan re-parse.
            for (pending_list.items) |pending_call| {
                if (pending_call.question_id == question_id) return true;
            }
        }
        return false;
    }

    fn hasQueuedPendingQuestionId(self: *Peer, question_id: u32) bool {
        return self.pendingMapHasQueuedQuestionId(&self.pending_promises, question_id) or
            self.pendingMapHasQueuedQuestionId(&self.pending_export_promises, question_id);
    }

    /// True when `question_id` is already consumed by an inbound Call or
    /// Bootstrap answer (active, resolved, failed, resolving, finished-early,
    /// forwarded, or queued for a promised target). This is the shared answer
    /// namespace;
    /// it deliberately excludes the provide/join question tables so their
    /// handlers can report their own specific errors for same-type collisions.
    ///
    /// A finished-early tombstone keeps the id reserved until the late Return
    /// drains it: a compliant caller may not reuse a question id before it has
    /// received that Return (which consumes the tombstone), so only a violator
    /// is rejected here. Without this, a reused id's Return sender would
    /// fetchRemove the STALE tombstone — skipping the record and applying the
    /// old Finish's releaseResultCaps flag to the new answer's result caps, a
    /// remote-forceable premature export release.
    fn inboundAnswerQuestionIdInUse(self: *Peer, question_id: u32) !bool {
        return self.resolved_answers.contains(question_id) or
            self.failed_answers.contains(question_id) or
            self.active_inbound_questions.contains(question_id) or
            self.resolving_answers.contains(question_id) or
            self.finished_early_answers.contains(question_id) or
            self.send_results_to_yourself.contains(question_id) or
            self.send_results_to_third_party.contains(question_id) or
            self.forwarded_questions.contains(question_id) or
            self.forwarded_tail_questions.contains(question_id) or
            self.completing_join_answers.contains(question_id) or
            self.pending_join_result_answers.contains(question_id) or
            self.pending_join_relays.contains(question_id) or
            self.pending_accept_embargo_by_question.contains(question_id) or
            self.cross_peer_pending_accepts.contains(question_id) or
            self.hasQueuedPendingQuestionId(question_id);
    }

    /// True when `question_id` is already consumed by any inbound question:
    /// the shared Call/Bootstrap answer namespace plus in-flight Provide and
    /// Join questions. Used by handlers that own no question table of their own
    /// (Call, Bootstrap) so a spec-violating reuse across message types is
    /// rejected before a second Return can be emitted on the id.
    fn inboundQuestionIdInUse(self: *Peer, question_id: u32) !bool {
        return (try self.inboundAnswerQuestionIdInUse(question_id)) or
            self.provides_by_question.contains(question_id) or
            self.pending_join_questions.contains(question_id) or
            self.pending_join_relays.contains(question_id);
    }

    fn handleCall(self: *Peer, frame: []const u8, call: protocol.Call) !void {
        // Reject duplicate question IDs from the remote peer (spec violation).
        // Covers the shared answer namespace plus in-flight Provide/Join
        // questions so a Call can never collide with any other inbound question.
        if (try self.inboundQuestionIdInUse(call.question_id)) {
            return error.DuplicateQuestionId;
        }

        // `sendResultsTo = thirdParty` asks this vat to connect to a third vat
        // and deliver the results there. Unless the host opted in, we cannot —
        // and accepting the call only to drop its results is the one outcome the
        // protocol never permits. Refuse with a single exception Return.
        //
        // Placement is load-bearing: after the duplicate-id check, so a reused
        // id still reports DuplicateQuestionId; before the answer bookkeeping,
        // so there is nothing to unwind; and before the inbound cap table is
        // built, so no import references are taken and the exception Return's
        // `releaseParamCaps` — still the schema default TRUE, because no
        // `active_inbound_questions` entry exists yet to say otherwise —
        // correctly tells the sender to drop its export refs. This also covers
        // in-process loopback calls, which are delivered through handleFrame.
        if (call.send_results_to.tag == .thirdParty and
            self.third_party_result_policy == .reject)
        {
            try self.sendReturnException(call.question_id, third_party_results_unsupported);
            return;
        }

        const inbound_before = self.active_inbound_questions.count();
        try ensureCountLimit(false, inbound_before, self.limits.max_active_inbound_questions);
        // Decide the answering Return's `releaseParamCaps` up front, from the
        // frame the caller actually sent: a `senderHosted`/`senderPromise` param
        // descriptor is a wire reference the caller recorded and this vat is
        // about to take (`InboundCapTable.init`), and every such reference is
        // settled here with an explicit `Release` frame — never implicitly.
        // rpc.capnp: "If true, all capabilities that were in the params should
        // be considered released. The sender must not send separate `Release`
        // messages for them."
        try self.active_inbound_questions.put(
            call.question_id,
            callParamsGrantImportRefs(call.params.cap_table),
        );
        errdefer _ = self.active_inbound_questions.remove(call.question_id);
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .active_inbound_questions,
            inbound_before,
            self.active_inbound_questions.count(),
            self.limits.max_active_inbound_questions,
        );

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
                third_party.noteCallSendResultsForPeerFn(
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
                Peer.lookupFailedAnswer,
                Peer.queuePromisedCall,
                Peer.queuePromiseExportCall,
                Peer.sendReturnException,
                Peer.sendReturnExceptionTyped,
                Peer.handleResolvedCall,
                Peer.releaseInboundCaps,
                peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
            ),
        ) catch |err| {
            log.debug("call routing error for question {}: {}", .{ call.question_id, err });
            // The redirected result is already committed on the target
            // connection and the resultsSentElsewhere write may itself have
            // reached the source caller. Never risk a second terminal Return
            // on that answer id; let the transport-level error close the peer.
            if (err == error.AutomaticThirdPartySourceSettlementFailed) return err;
            try self.sendReturnException(call.question_id, @errorName(err));
        };
        if (self.automatic_third_party_routes.get(call.question_id)) |route| {
            if (route.source_marker_failed) {
                self.finalizeAutomaticThirdPartyRoute(route, null);
                return error.AutomaticThirdPartySourceSettlementFailed;
            }
        }
    }

    fn handleResolvedCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        resolved: cap_table.ResolvedCap,
    ) !void {
        try peer_forward_orchestration.handleResolvedCall(
            Peer,
            cap_table.InboundCapTable,
            self,
            call,
            inbound_caps,
            resolved,
            peer_call_orchestration.handleResolvedExportedCallForPeerFn(
                Peer,
                cap_table.InboundCapTable,
                third_party.noteCallSendResultsForPeerFn(
                    Peer,
                    Peer.noteSendResultsToYourself,
                    Peer.noteSendResultsToThirdParty,
                ),
                Peer.handleResolvedCall,
                Peer.sendReturnException,
                Peer.maybeForwardVineCall,
            ),
            peer_forward_orchestration.forwardResolvedCallForPeerFn(
                Peer,
                cap_table.InboundCapTable,
                Peer.forwardResolvedCall,
            ),
            Peer.sendReturnException,
        );
    }

    /// Pre-reserved resources for recording a resolved answer, obtained from
    /// `reserveResolvedAnswer` before the Return frame is sent so that
    /// `commitReservedResolvedAnswer` (run after the frame is on the wire)
    /// cannot fail. See `reserveResolvedAnswer`.
    pub const ResolvedAnswerReservation = struct {
        frame_copy: []u8,
        /// Exports on which the reservation took the answer-held reference
        /// (one entry per results cap descriptor). deinit rolls the
        /// references back; commit transfers them to the recorded answer,
        /// which releases them at Finish via the stored frame's cap table.
        held_export_ids: []u32,

        pub fn deinit(self: ResolvedAnswerReservation, peer: *Peer) void {
            peer.resolved_answer_reservations -= 1;
            var idx = self.held_export_ids.len;
            while (idx > 0) {
                idx -= 1;
                peer.rollbackAnswerExportRef(self.held_export_ids[idx]);
            }
            peer.allocator.free(self.held_export_ids);
            peer.allocator.free(self.frame_copy);
        }
    };

    /// Reserve everything needed to record a resolved answer BEFORE the Return
    /// frame is sent: the resolved-answers count budget, one unused map slot,
    /// the frame copy, and the answer-held references on every export in the
    /// frame's results cap table. This must precede the send so that recording
    /// — which happens only once the frame is already on the wire — is
    /// infallible.
    ///
    /// If the record step could fail after the send, the propagating error would
    /// drive the call-dispatch catch to emit a SECOND (exception) Return for the
    /// same answer_id: two Returns for one call, a remote-forceable protocol
    /// violation (a peer fills resolved_answers to max_resolved_answers with
    /// Finish-less calls, then any later successful call double-Returns). Audit
    /// 2026-07-03 item 7.
    ///
    /// The answer-held references keep the answer's pipeline targets alive
    /// until its Finish: without them the export dies as soon as the remote
    /// Releases the caps it imported from this Return — legal even before
    /// Finish — and a pipelined call on the answer can no longer dispatch.
    pub fn reserveResolvedAnswer(self: *Peer, question_id: u32, frame: []const u8) !ResolvedAnswerReservation {
        try ensureCountLimit(
            self.resolved_answers.contains(question_id),
            self.resolved_answers.count() + self.resolved_answer_reservations,
            self.limits.max_resolved_answers,
        );
        // Reserve a map slot so the post-send getOrPutAssumeCapacity cannot
        // OOM. Ensure one slot per OUTSTANDING reservation as well: a nested
        // reserve→commit for a different answer id (a synchronous transport
        // delivering a Call/Bootstrap while an outer send is on the stack)
        // would otherwise consume the single slot the outer reservation's
        // ensure counted on, and the outer infallible commit would underflow
        // the map's reserved-slot accounting.
        try self.resolved_answers.ensureUnusedCapacity(1 + self.resolved_answer_reservations);
        const frame_copy = try self.allocator.dupe(u8, frame);
        errdefer self.allocator.free(frame_copy);
        const held_export_ids = try peer_cap_lifecycle.noteAnswerHeldResultCaps(
            Peer,
            self,
            self.allocator,
            frame,
            Peer.noteAnswerExportRef,
            rollbackAnswerExportRef,
        );
        self.resolved_answer_reservations += 1;
        return .{ .frame_copy = frame_copy, .held_export_ids = held_export_ids };
    }

    /// Record a resolved answer using a reservation obtained (before the send)
    /// from `reserveResolvedAnswer`. Infallible: the map slot, frame copy, and
    /// answer-held export references are already reserved, so this only stores
    /// into the map. Takes ownership of the reservation: `frame_copy` moves
    /// into the map and the answer-held references now belong to the recorded
    /// answer (released at Finish by re-walking the stored frame, so the id
    /// list is no longer needed).
    pub fn commitReservedResolvedAnswer(
        self: *Peer,
        question_id: u32,
        reservation: ResolvedAnswerReservation,
    ) void {
        self.resolved_answer_reservations -= 1;
        self.allocator.free(reservation.held_export_ids);
        pending_calls.recordResolvedAnswerAssumeCapacity(
            Peer,
            ResolvedAnswer,
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            self,
            question_id,
            reservation.frame_copy,
            &self.resolved_answers,
            &self.pending_promises,
            Peer.resolvePromisedAnswer,
            Peer.sendReturnException,
            Peer.handleResolvedCall,
            Peer.releaseInboundCaps,
            peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
        );
    }

    fn recordResolvedAnswer(self: *Peer, question_id: u32, frame: []u8) !void {
        try ensureCountLimit(
            self.resolved_answers.contains(question_id),
            self.resolved_answers.count(),
            self.limits.max_resolved_answers,
        );
        try pending_calls.recordResolvedAnswer(
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
        try self.ensurePendingQueuedCallBudget(
            &self.pending_promises,
            question_id,
            frame.len,
            self.limits.max_pending_promises,
        );
        const stats_before = self.pendingQueuedCallStats();
        try pending_calls.queuePendingCall(
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            &self.pending_promises,
            question_id,
            frame,
            inbound_caps,
        );
        self.emitQueuedCallPressure(stats_before, frame.len);
    }

    fn queuePromiseExportCall(self: *Peer, export_id: u32, frame: []const u8, inbound_caps: cap_table.InboundCapTable) !void {
        try self.ensurePendingQueuedCallBudget(
            &self.pending_export_promises,
            export_id,
            frame.len,
            self.limits.max_pending_export_promises,
        );
        const stats_before = self.pendingQueuedCallStats();
        try pending_calls.queuePendingCall(
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            &self.pending_export_promises,
            export_id,
            frame,
            inbound_caps,
        );
        self.emitQueuedCallPressure(stats_before, frame.len);
    }

    /// Emit queued-call pressure crossings after a successful enqueue.
    fn emitQueuedCallPressure(self: *Peer, before: PendingQueuedCallStats, frame_len: usize) void {
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .queued_calls,
            before.calls,
            before.calls + 1,
            self.limits.max_pending_queued_calls,
        );
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .queued_call_bytes,
            before.bytes,
            before.bytes + frame_len,
            self.limits.max_pending_queued_call_bytes,
        );
    }

    fn replayResolvedPromiseExport(self: *Peer, export_id: u32, resolved: cap_table.ResolvedCap) !void {
        try pending_calls.replayResolvedPromiseExport(
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
        // A retained call is addressed publicly by its original caller-chosen
        // question id, but after awaitFromThirdParty the open remote answer is
        // the callee-chosen adopted id. Publish that wire identity before the
        // adoption helper can replay an already-buffered terminal Return: its
        // callback may synchronously Finish or transfer the retained answer.
        var previous_retained_answer_id: ?u32 = null;
        if (self.retained_questions.contains(question_id)) {
            previous_retained_answer_id = try self.retained_questions.adoptWireAnswer(
                question_id,
                adopted_answer_id,
            );
        }
        errdefer if (previous_retained_answer_id) |previous| {
            self.retained_questions.rollbackWireAnswer(
                question_id,
                adopted_answer_id,
                previous,
            );
        };
        try third_party.adoption.adoptThirdPartyAnswer(
            Peer,
            Question,
            self,
            question_id,
            adopted_answer_id,
            question,
            &self.questions,
            &self.adopted_third_party_answers,
            &self.pending_third_party_returns,
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
            Peer.ensureThirdPartyAdoptionBudget,
            finish.freeOwnedFrameForPeerFn(Peer),
            third_party.returns.handlePendingReturnFrameForPeerFn(
                Peer,
                Peer.handleReturn,
            ),
        );
    }

    pub fn handleReturn(self: *Peer, frame: []const u8, ret: protocol.Return) anyerror!void {
        // The wire effect of a Return on our sent param caps is independent
        // of local dispatch: the moment the remote sent this frame it either
        // dropped the refs (releaseParamCaps=true, the default) or committed
        // to explicit Releases (false). Consume the record first so every
        // Return path — regular, exception, third-party, and the
        // cancelled-question absorb below — settles the refs exactly once.
        try self.consumeQuestionParamExports(ret.answer_id, ret.release_param_caps);

        // `takeFromOtherQuestion` names a call WE answered with
        // `sendResultsTo=yourself` whose results should become this question's
        // results (reflected-loopback / Level-1 tail-call). If we stashed those
        // results (cap-free; see completeSelfLoopbackReturn), deliver them inline
        // to this question instead of surfacing the bare relay tag, so the caller
        // receives the real value without a wire round-trip through the
        // forwarder. Falls through to the normal path when nothing was stashed
        // (results carried caps, the answer is still pending, or the stash
        // overflowed) — the caller then sees `takeFromOtherQuestion` as before.
        if (ret.tag == .takeFromOtherQuestion) {
            if (ret.take_from_other_question) |source_answer_id| {
                if (self.questions.contains(ret.answer_id)) {
                    if (self.loopback_result_stash.fetchRemove(source_answer_id)) |entry| {
                        defer self.allocator.free(entry.value);
                        try self.deliverStashedLoopbackResults(ret.answer_id, entry.value);
                        return;
                    }
                }
            }
        }

        var latency_started_ns: ?i64 = null;
        var retained_return_id: ?u32 = null;
        if (self.questions.get(ret.answer_id)) |question| {
            if (question.cancelled) {
                // Cancelled locally: the caller already saw an exception and
                // a Finish with releaseResultCaps is on the wire, so the
                // remote releases any caps this Return carries. Per spec the
                // remote sends exactly one Return per question — absorb it
                // without importing caps or re-dispatching.
                log.debug("absorbing return for cancelled question {}", .{ret.answer_id});
                self.removeQuestion(ret.answer_id);
                return;
            }
            // awaitFromThirdParty is not call completion; results arrive
            // later through the third party, so no latency sample.
            if (ret.tag != .awaitFromThirdParty) {
                latency_started_ns = question.started_ns;
                const original_id = self.adopted_third_party_answers.get(ret.answer_id) orelse ret.answer_id;
                if (self.retained_questions.contains(original_id)) {
                    self.retained_questions.noteReturn(
                        original_id,
                        ret.no_finish_needed,
                        question.is_loopback,
                    );
                    retained_return_id = original_id;
                }
            }
        }
        defer if (latency_started_ns) |started| {
            if (self.clockNow()) |now| {
                const elapsed = now - started;
                if (elapsed >= 0 and !self.questions.contains(ret.answer_id)) {
                    events.emitCallLatency(self.observer, .peer, .unknown, ret.answer_id, @intCast(elapsed));
                }
            }
        };
        peer_return_orchestration.handleReturn(
            Peer,
            Question,
            cap_table.InboundCapTable,
            self,
            frame,
            ret,
            peer_return_orchestration.getQuestionForPeerFn(Peer, Question),
            peer_return_orchestration.removeQuestionForReturnForPeerFn(Peer),
            peer_return_orchestration.restoreQuestionForReturnForPeerFn(Peer, Question),
            peer_return_orchestration.completeQuestionRemovalForPeerFn(Peer),
            Peer.handleMissingReturnQuestion,
            peer_return_orchestration.initInboundCapsForPeerFn(Peer, cap_table.InboundCapTable),
            peer_return_orchestration.deinitInboundCapsForTypeFn(cap_table.InboundCapTable),
            third_party.adoption.handleReturnAcceptFromThirdPartyForPeerFn(
                Peer,
                Question,
                PendingThirdPartyAwait,
                cap_table.InboundCapTable,
                third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
                finish.freeOwnedFrameForPeerFn(Peer),
                peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
                Peer.ensurePendingThirdPartyAwaitBudget,
                adoptThirdPartyAnswer,
            ),
            peer_return_dispatch.maybeSendAutoFinishForPeerFn(
                Peer,
                Question,
                peer_outbound_control.sendFinishViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
            ),
            peer_return_orchestration.handleReturnRegularForPeerFn(
                Peer,
                Question,
                cap_table.InboundCapTable,
                Peer.releaseInboundCaps,
                peer_outbound_control.sendFinishViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
            ),
        ) catch |err| {
            if (retained_return_id) |question_id| {
                // Failure before question removal (notably inbound-cap-table
                // OOM) means the Return was not callback-visible; restore the
                // pending state. Once removed, retained callbacks are at-most
                // once and the answer remains explicitly finishable.
                if (self.questions.contains(ret.answer_id)) {
                    self.retained_questions.rollbackReturn(question_id);
                } else if (ret.no_finish_needed) {
                    _ = self.retained_questions.completeNoFinishNeeded(question_id);
                }
            }
            return err;
        };
        if (retained_return_id) |question_id| {
            if (ret.no_finish_needed) {
                _ = self.retained_questions.completeNoFinishNeeded(question_id);
            }
        }
    }

    /// Exposed for integration tests that exercise internal Peer methods.
    /// Not part of the public API — gated behind `builtin.is_test` so it is
    /// absent from the frozen consumer surface (`src/lib.zig`) and the
    /// generated `docs/api-snapshot.txt`, and unreachable from application
    /// code. Test builds (`is_test == true`) still see the full facade.
    pub const test_hooks = if (builtin.is_test) struct {
        pub const ForwardCallContextType = ForwardCallContext;

        pub fn sendFrame(self: *Peer, frame: []const u8) !void {
            return Peer.sendFrame(self, frame);
        }

        pub fn noteSendResultsToThirdParty(
            self: *Peer,
            answer_id: u32,
            recipient: ?message.AnyPointerReader,
        ) !void {
            return Peer.noteSendResultsToThirdParty(self, answer_id, recipient);
        }

        pub fn removeQuestion(self: *Peer, question_id: u32) void {
            Peer.removeQuestion(self, question_id);
        }

        pub fn removeQuestionAndDeinit(self: *Peer, question_id: u32) void {
            Peer.removeQuestionAndDeinit(self, question_id);
        }

        pub fn sendJoinExperimentalRetainedResult(
            self: *Peer,
            target: protocol.MessageTarget,
            key_part: ?message.AnyPointerReader,
            ctx: *anyopaque,
            on_return: QuestionCallback,
        ) !u32 {
            return Peer.sendJoinExperimentalWithAutoFinish(self, target, key_part, ctx, on_return, true);
        }

        pub fn addCrossPeerProxyExport(
            self: *Peer,
            source_peer: *Peer,
            target: cap_table.ResolvedCap,
            release_source_import_id: ?u32,
            release_source_export_pin_id: ?u32,
        ) !u32 {
            return Peer.addCrossPeerProxyExport(self, source_peer, target, release_source_import_id, release_source_export_pin_id, null);
        }

        /// Mint a cross-peer proxy whose ctx owns a handoff IMPORT pin on the
        /// source peer (the receiverHosted serve shape).
        pub fn addCrossPeerProxyExportPinnedImport(
            self: *Peer,
            source_peer: *Peer,
            target: cap_table.ResolvedCap,
            release_source_import_id: ?u32,
            release_source_import_pin_id: ?u32,
        ) !u32 {
            return Peer.addCrossPeerProxyExport(self, source_peer, target, release_source_import_id, null, release_source_import_pin_id);
        }

        /// Take a handoff-held (Release-immune) pin on one of this peer's
        /// exports — the lease a caller passes to `addCrossPeerProxyExport` as
        /// `release_source_export_pin_id`.
        pub fn noteHandoffExportRef(self: *Peer, id: u32) !void {
            return Peer.noteHandoffExportRef(self, id);
        }

        /// Release a handoff-held pin directly (tests that never hand it to a
        /// proxy).
        pub fn releaseHandoffHeldExport(self: *Peer, id: u32) void {
            Peer.releaseHandoffHeldExport(self, id);
        }

        /// Take a handoff pin on one of this peer's IMPORTS — the
        /// receiverHosted lift's Provide-time / serve-time lease.
        pub fn noteHandoffImportPin(self: *Peer, id: u32) !void {
            return Peer.noteHandoffImportPin(self, id);
        }

        /// Release a handoff import pin directly (tests that never hand it to
        /// a proxy); the last unpin emits the withheld deferred Release.
        pub fn releaseHandoffImportPin(self: *Peer, id: u32) !void {
            return Peer.releaseHandoffImportPin(self, id);
        }

        pub fn releaseVineExport(self: *Peer, vine_id: u32) void {
            Peer.releaseVineExport(self, vine_id);
        }

        pub fn onConnectionError(self: *Peer, err: anyerror) void {
            Peer.onConnectionError(self, err);
        }

        pub fn onConnectionClose(self: *Peer) void {
            Peer.onConnectionClose(self);
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
            return Peer.clonePayloadWithRemappedCaps(
                self,
                builder,
                protocol.PayloadBuilder.wrap(payload_builder),
                source,
                inbound_caps,
            );
        }

        pub fn makeProvideTarget(self: *Peer, resolved: cap_table.ResolvedCap) !state.ProvideTarget {
            return Peer.makeProvideTarget(self, resolved);
        }

        pub fn sendReturnProvidedTarget(self: *Peer, answer_id: u32, target: *const state.ProvideTarget) !void {
            return Peer.sendReturnProvidedTarget(self, answer_id, target);
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

        pub fn handleFinish(self: *Peer, finish_msg: protocol.Finish) !void {
            return Peer.handleFinish(self, finish_msg);
        }

        pub fn handleCall(self: *Peer, frame: []const u8, call: protocol.Call) !void {
            return Peer.handleCall(self, frame, call);
        }

        pub fn queuePromisedCall(
            self: *Peer,
            question_id: u32,
            frame: []const u8,
            inbound_caps: cap_table.InboundCapTable,
        ) !void {
            return Peer.queuePromisedCall(self, question_id, frame, inbound_caps);
        }

        pub fn rememberPendingEmbargo(self: *Peer, embargo_id: u32, promise_id: u32) !void {
            return Peer.rememberPendingEmbargo(self, embargo_id, promise_id);
        }

        pub fn storeResolvedImport(
            self: *Peer,
            promise_id: u32,
            cap: ?cap_table.ResolvedCap,
            embargo_id: ?u32,
            embargoed: bool,
        ) !void {
            return Peer.storeResolvedImport(self, promise_id, cap, embargo_id, embargoed);
        }

        pub fn bufferPendingThirdPartyReturn(self: *Peer, answer_id: u32, frame: []const u8) !void {
            return Peer.bufferPendingThirdPartyReturn(self, answer_id, frame);
        }

        pub fn queueEmbargoedAccept(
            self: *Peer,
            answer_id: u32,
            provided_question_id: u32,
            embargo: []const u8,
        ) !void {
            return Peer.queueEmbargoedAccept(self, answer_id, provided_question_id, embargo);
        }

        pub fn ensureProvideBudget(self: *Peer, question_id: u32, recipient_key: []const u8) !void {
            return Peer.ensureProvideBudget(self, question_id, recipient_key);
        }

        pub fn ensureJoinBudget(
            self: *Peer,
            join_id: u32,
            part_count: u16,
            part_num: u16,
            question_id: u32,
        ) !void {
            return Peer.ensureJoinBudget(
                self,
                .{
                    .join_id = join_id,
                    .part_count = part_count,
                    .part_num = part_num,
                },
                question_id,
            );
        }

        pub fn ensurePendingThirdPartyAwaitBudget(self: *Peer, completion_key: []const u8) !void {
            return Peer.ensurePendingThirdPartyAwaitBudget(self, completion_key);
        }

        pub fn ensurePendingThirdPartyAnswerBudget(self: *Peer, completion_key: []const u8) !void {
            return Peer.ensurePendingThirdPartyAnswerBudget(self, completion_key);
        }

        pub fn ensureThirdPartyAdoptionBudget(self: *Peer, adopted_answer_id: u32) !void {
            return Peer.ensureThirdPartyAdoptionBudget(self, adopted_answer_id);
        }
    } else struct {};
};
