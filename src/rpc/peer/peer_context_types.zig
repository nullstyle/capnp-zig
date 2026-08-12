const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../caps/table.zig");
const message = @import("../../serialization/message.zig");
const protocol = @import("../wire/protocol.zig");
const state = @import("./state.zig");
const join_network = @import("../vat/join.zig");
const join_coordinator = @import("./join/join_coordinator.zig");
const provide_forward_target = @import("./provide/forward_target.zig");

const factories = @This();
const vat_provisions = @import("../vat/provisions.zig");

/// Peer-parameterized context/record structs, extracted from `peer/mod.zig`
/// (P12): forwarding contexts, cross-peer proxy/relay contexts and links,
/// the automatic third-party route record, L3 outbound-provide and vine
/// records, and the L4 hosted-join family. Each is a comptime factory over
/// the peer type; `peer/mod.zig` instantiates them under their original
/// names, so every existing reference (including the `Peer.*Record`/`*Ctx`
/// aliases the extracted siblings use, and the `ForwardCallContextType`
/// test hook) resolves to the same types unchanged.
/// A registered save hook: context and handler always travel together so
/// dispatch never has to unwrap them independently.
pub fn SaveHookOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const SaveHook = factories.SaveHookOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        ctx: *anyopaque,
        on_save: SaveHandler,
    };
}

/// A registered restore hook (see `SaveHook`).
pub fn RestoreHookOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const RestoreHook = factories.RestoreHookOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        ctx: *anyopaque,
        on_restore: RestoreHandler,
    };
}

/// Persistence hooks installed on one export. The peer wraps the export's
/// original handler with a trampoline that serves `Persistent.save()` and
/// vat-level restore calls, forwarding everything else to the original.
pub fn PersistenceStateOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const PersistenceState = factories.PersistenceStateOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        export_id: u32,
        original: Export,
        save: ?SaveHook = null,
        restore: ?RestoreHook = null,
    };
}

/// The record an accept peer keeps for each queued/parked cross-peer Accept.
/// `embargo_key` is the accept peer's OWN dupe (its allocator), so no cleanup
/// path ever hashes bytes owned by — and possibly freed with — the provision's
/// embargo map.
pub fn CrossPeerAcceptRecordOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        /// UNCOUNTED back-link (INV-REC: the pending slot's / parked entry's +1
        /// keeps the provision alive as long as this record exists).
        provision: *ProvisionIndex.Provision,
        /// Owned by THIS peer's allocator; null for a parked non-embargoed accept.
        embargo_key: ?[]u8,
        parked: bool,
    };
}

pub fn ForwardCallContextOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const ForwardCallContext = factories.ForwardCallContextOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        peer: *Peer,
        payload: protocol.Payload,
        inbound_caps: cap_table.InboundCapTable,
        send_results_to: protocol.SendResultsToTag,
        send_results_to_third_party_payload: ?[]u8 = null,
        answer_id: u32,
        mode: ForwardReturnMode,

        pub fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
            const ctx: *ForwardCallContext = @ptrCast(@alignCast(ctx_ptr));
            ctx.inbound_caps.deinit();
            if (ctx.send_results_to_third_party_payload) |payload| allocator.free(payload);
            allocator.destroy(ctx);
        }
    };
}

pub fn ForwardReturnBuildContextOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        peer: *Peer,
        payload: protocol.Payload,
        inbound_caps: *const cap_table.InboundCapTable,
    };
}

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
pub fn ForwardVineCallContextOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
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

        pub fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
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
}

pub fn CrossPeerProxyContextOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
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

        pub fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
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

        pub fn onCall(
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
}

pub fn CrossPeerProxyCallContextOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
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

        pub fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
            const ctx: *CrossPeerProxyCallContext = @ptrCast(@alignCast(ctx_ptr));
            ctx.rollbackParamProxies();
            ctx.created_param_proxy_ids.deinit(allocator);
            allocator.destroy(ctx);
        }
    };
}

pub fn CrossPeerJoinRelayContextOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        owner_peer: *Peer,
        owner_answer_id: u32,
        settled_flag: ?*bool = null,

        pub fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
            const ctx: *CrossPeerJoinRelayContext = @ptrCast(@alignCast(ctx_ptr));
            allocator.destroy(ctx);
        }
    };
}

pub fn CrossPeerCapMapContextOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        inbound_peer: *Peer,
        outbound_peer: *Peer,
        inbound_caps: *cap_table.InboundCapTable,
        created_proxy_ids: *std.ArrayList(u32),
        pin_source_caps: bool,
        remapped_by_index: std.AutoHashMap(u32, u32),

        pub fn init(
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

        pub fn deinit(ctx: *CrossPeerCapMapContext) void {
            ctx.remapped_by_index.deinit();
        }
    };
}

pub fn CrossPeerReturnRelayContextOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
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

        pub fn deinit(ctx: *CrossPeerReturnRelayContext, allocator: std.mem.Allocator) void {
            ctx.rollbackResultProxies();
            ctx.created_result_proxy_ids.deinit(allocator);
        }
    };
}

/// Source-owned state for an automatic `sendResultsTo.thirdParty` redirect.
///
/// The source peer (the callee-facing introducer connection) owns the allocation
/// and indexes it by the original answer id.  The result peer keeps only a
/// borrowed backlink keyed by the synthetic callee-allocated answer id.  This
/// makes either peer's teardown able to neutralize the other side without
/// guessing whether a Return was already visible on either transport.
pub fn AutomaticThirdPartyRouteOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
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
}

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
pub fn OutboundProvideOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const OutboundProvide = factories.OutboundProvideOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
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
        pub fn stashAcceptDisembargo(op: *OutboundProvide, allocator: std.mem.Allocator, embargo: []const u8) !void {
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

        pub fn deinit(op: *OutboundProvide, allocator: std.mem.Allocator) void {
            op.deinitStash(allocator);
            op.forward_target.deinit(allocator);
        }
    };
}

pub fn ProvideOriginationTargetOf(comptime Peer: type) type {
    return union(enum) {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        message_target: protocol.MessageTarget,
        retained_answer: struct {
            question_id: u32,
            ops: []const protocol.PromisedAnswerOp,
        },

        pub fn cloneForwardTarget(
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

        pub fn buildProvide(
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
}

/// Liveness back-link recorded on the host-of-provided-cap peer (B↔C) for each
/// coupling that peer anchors. Names the recipient peer (B↔A) whose
/// `outbound_provides[vine_id]` entry borrows a pointer back to this peer, so
/// this peer's `deinit` can find and NULL that borrowed pointer before it frees
/// itself. The reverse of the `OutboundProvide.provide_peer` edge.
pub fn CoupledVineOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const CoupledVine = factories.CoupledVineOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        /// The host-of-recipient peer (B↔A) that owns the coupling entry.
        recipient_peer: *Peer,
        /// The vine export id keying the coupling in `recipient_peer.outbound_provides`.
        vine_id: u32,
    };
}

/// Liveness back-link for a cross-peer capability proxy. The proxy export lives
/// on `owner_peer`, but its handler borrows a pointer to the source peer so it
/// can forward calls and release the retained source import. If the source peer
/// deinits first, it walks these links and nulls those borrowed pointers.
pub fn CrossPeerProxyLinkOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        owner_peer: *Peer,
        proxy_export_id: u32,
    };
}

pub fn CrossPeerJoinRelayOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        source_peer: ?*Peer,
        source_question_id: u32,
        deadline_ns: ?i64 = null,
        /// Set before attempting the one upstream terminal Return. A relay may
        /// remain live afterward only to forward Finish.releaseResultCaps; expiry
        /// must not emit a second Return while retiring that bookkeeping.
        upstream_terminal_started: bool = false,
    };
}

pub fn CrossPeerJoinRelayLinkOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        owner_peer: *Peer,
        owner_answer_id: u32,
    };
}

/// Canonical owner of one completed L4 JoinResult handoff. The result-path
/// peer allocates and owns this object, captures the exact JoinNetwork that
/// created its provision, and charges the provision bytes once. Result answer
/// records and the direct Accept host only borrow this pointer.
pub fn HostedJoinOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const HostedJoin = factories.HostedJoinOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
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
}

pub fn PendingJoinAcceptOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        hosted: *HostedJoin,
        target: ProvideTarget,
    };
}

/// Answer-ID tombstone used while a complete Join bucket is detached but the
/// application JoinNetwork callback has not yet produced a canonical
/// HostedJoin. Finish marks the tombstone without freeing it, so reentrant ID
/// reuse remains rejected until completion has coherently settled every part.
pub fn CompletingJoinAnswerOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
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
}

pub fn PendingJoinResultAnswerOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        hosted: *HostedJoin,
        /// True immediately before the corresponding Return send begins. This
        /// lets an error unwind retire only still-unsent reservations while
        /// preserving answers whose terminal Return is already observable.
        published: bool = false,
    };
}

pub fn JoinAcceptHostLinkOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        hosted: *HostedJoin,
    };
}

pub fn JoinCoordinatorAcceptLinkOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        coordinator: *JoinCoordinator,
    };
}

pub fn JoinCoordinatorResultLinkOf(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ProvideTarget = state.ProvideTarget;
        const Export = Peer.ExportRecord;
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const ForwardReturnMode = Peer.ForwardReturnModeEnum;
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const JoinCoordinator = join_coordinator.JoinCoordinator(Peer);

        const SaveHook = factories.SaveHookOf(Peer);
        const RestoreHook = factories.RestoreHookOf(Peer);
        const PersistenceState = factories.PersistenceStateOf(Peer);
        const CrossPeerAcceptRecord = factories.CrossPeerAcceptRecordOf(Peer);
        const ForwardCallContext = factories.ForwardCallContextOf(Peer);
        const ForwardReturnBuildContext = factories.ForwardReturnBuildContextOf(Peer);
        const ForwardVineCallContext = factories.ForwardVineCallContextOf(Peer);
        const CrossPeerProxyContext = factories.CrossPeerProxyContextOf(Peer);
        const CrossPeerProxyCallContext = factories.CrossPeerProxyCallContextOf(Peer);
        const CrossPeerJoinRelayContext = factories.CrossPeerJoinRelayContextOf(Peer);
        const CrossPeerCapMapContext = factories.CrossPeerCapMapContextOf(Peer);
        const CrossPeerReturnRelayContext = factories.CrossPeerReturnRelayContextOf(Peer);
        const AutomaticThirdPartyRoute = factories.AutomaticThirdPartyRouteOf(Peer);
        const OutboundProvide = factories.OutboundProvideOf(Peer);
        const ProvideOriginationTarget = factories.ProvideOriginationTargetOf(Peer);
        const CoupledVine = factories.CoupledVineOf(Peer);
        const CrossPeerProxyLink = factories.CrossPeerProxyLinkOf(Peer);
        const CrossPeerJoinRelay = factories.CrossPeerJoinRelayOf(Peer);
        const CrossPeerJoinRelayLink = factories.CrossPeerJoinRelayLinkOf(Peer);
        const HostedJoin = factories.HostedJoinOf(Peer);
        const PendingJoinAccept = factories.PendingJoinAcceptOf(Peer);
        const CompletingJoinAnswer = factories.CompletingJoinAnswerOf(Peer);
        const PendingJoinResultAnswer = factories.PendingJoinResultAnswerOf(Peer);
        const JoinAcceptHostLink = factories.JoinAcceptHostLinkOf(Peer);
        const JoinCoordinatorAcceptLink = factories.JoinCoordinatorAcceptLinkOf(Peer);

        const JoinCoordinatorResultLink = factories.JoinCoordinatorResultLinkOf(Peer);

        fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }
        coordinator: *JoinCoordinator,
        question_id: u32,
    };
}
