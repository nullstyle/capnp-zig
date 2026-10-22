//! Regression test for the reflected-loopback DIRECT-handler correctness gap.
//!
//! A generated DIRECT (non-deferred) method handler runs its whole body —
//! including the business logic — INSIDE the results-build closure it hands to
//! `Peer.sendReturnResults` (see `X.handleCallDirect` / `buildReturnDirect` in
//! generated code). When such a cap is invoked via a reflected loopback the
//! forwarding peer parks a pipelined call on a promise and later resolves that
//! promise back to a capability the ORIGINAL caller hosts, replaying the parked
//! call to that cap with `sendResultsTo = yourself`. Historically
//! `sendReturnResults` short-circuited the `yourself` case straight to
//! `resultsSentElsewhere` WITHOUT running the build closure, so a direct
//! handler's body silently never executed and the caller's pipelined question
//! received only a bare `takeFromOtherQuestion` relay tag (no value).
//!
//! This test hosts a cap whose handler follows the generated DIRECT pattern
//! (body inside the build closure) and drives the full reflected-loopback
//! cycle, then asserts BOTH:
//!   1. the direct handler body actually ran (counter advanced, results built),
//!      and
//!   2. the caller's pipelined question received the real result VALUE inline
//!      (a `.results` Return, not `takeFromOtherQuestion`).
//!
//! The harness mirrors `rpc_reflected_resolve_disembargo_test.zig`: two real
//! detached peers cross-wired over a synchronous in-process loopback, under
//! `std.testing.allocator` so any refcount/frame leak fails the test.

const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.caps.table;
const message = capnpc.message;
const Peer = peer_impl.Peer;

fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
    return @ptrCast(@alignCast(ctx));
}

// -- Synchronous in-process wire ---------------------------------------------

const EventDir = enum { client_to_server, server_to_client };

const WireEvent = struct {
    dir: EventDir,
    tag: protocol.MessageTag,
    id: u32 = 0,
    disembargo_context: ?protocol.DisembargoContextTag = null,
    return_tag: ?protocol.ReturnTag = null,
};

const Wire = struct {
    allocator: std.mem.Allocator,
    events: std.ArrayList(WireEvent) = .empty,
    client_peer: ?*Peer = null,
    server_peer: ?*Peer = null,
    forwarding: bool = true,
    /// The client's next sends that fail before they reach the wire, as a
    /// full outbound queue fails them (QUIC's `error.OutboundQueueFull`).
    fail_client_sends: usize = 0,

    fn deinit(self: *Wire) void {
        self.events.deinit(self.allocator);
    }

    fn record(self: *Wire, dir: EventDir, frame: []const u8) !void {
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        var event = WireEvent{ .dir = dir, .tag = decoded.tag };
        switch (decoded.tag) {
            .call => event.id = (try decoded.asCall()).question_id,
            .@"return" => {
                const ret = try decoded.asReturn();
                event.id = ret.answer_id;
                event.return_tag = ret.tag;
            },
            .resolve => event.id = (try decoded.asResolve()).promise_id,
            .disembargo => event.disembargo_context = (try decoded.asDisembargo()).context_tag,
            .finish => event.id = (try decoded.asFinish()).question_id,
            else => {},
        }
        try self.events.append(self.allocator, event);
    }

    fn clientSend(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Wire = castCtx(*Wire, ctx_ptr);
        if (self.fail_client_sends > 0) {
            self.fail_client_sends -= 1;
            return error.OutboundQueueFull;
        }
        try self.record(.client_to_server, frame);
        if (self.forwarding) {
            if (self.server_peer) |peer| try peer.handleFrame(frame);
        }
    }

    fn serverSend(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Wire = castCtx(*Wire, ctx_ptr);
        try self.record(.server_to_client, frame);
        if (self.forwarding) {
            if (self.client_peer) |peer| try peer.handleFrame(frame);
        }
    }

    fn hasDisembargo(self: *const Wire, dir: EventDir, context: protocol.DisembargoContextTag) bool {
        for (self.events.items) |event| {
            if (event.dir != dir or event.tag != .disembargo) continue;
            if (event.disembargo_context == context) return true;
        }
        return false;
    }

    /// Returns sent in `dir` for `answer_id`: all of them, or only `tag`.
    fn countReturns(self: *const Wire, dir: EventDir, answer_id: u32, tag: ?protocol.ReturnTag) usize {
        var n: usize = 0;
        for (self.events.items) |event| {
            if (event.dir != dir or event.tag != .@"return" or event.id != answer_id) continue;
            if (tag) |want| {
                if (event.return_tag != want) continue;
            }
            n += 1;
        }
        return n;
    }

    /// Finish frames sent in `dir` for `question_id`.
    fn countFinishes(self: *const Wire, dir: EventDir, question_id: u32) usize {
        var n: usize = 0;
        for (self.events.items) |event| {
            if (event.dir == dir and event.tag == .finish and event.id == question_id) n += 1;
        }
        return n;
    }
};

// -- Call wire shape ---------------------------------------------------------
//
// Params: empty struct. Results: one data word `n :UInt32` at byte offset 0.
// method_id 0 (the only method the hosted cap answers).

const CALL_INTERFACE_ID: u64 = 0xD1_EC_D1_EC_D1_EC_0001;
const CALL_METHOD_ID: u16 = 0;

fn readResultN(payload: protocol.Payload) !u32 {
    const content = try payload.content.getStruct();
    return content.readU32(0);
}

// -- Client A: a cap hosted with a generated-style DIRECT handler ------------
//
// The handler body runs INSIDE the build closure handed to sendReturnResults,
// exactly like generated `buildReturnDirect`. Each invocation returns a
// per-connection monotonic counter (0, 1, ...) and records that it ran plus the
// arrival stamp, so the direct body's execution and ordering are observable.

const DirectHandlerServer = struct {
    counter: u32 = 0,
    calls_seen: u32 = 0,
    ran: bool = false,
    call0_seq: ?u32 = null,
    call1_seq: ?u32 = null,

    const BuildCtx = struct {
        server: *DirectHandlerServer,

        fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            const bc: *const BuildCtx = castCtx(*const BuildCtx, ctx_ptr);
            const self = bc.server;
            // *** Generated DIRECT-handler pattern: the whole handler body,
            // including the business logic, runs here inside the build closure.
            const n = self.counter;
            if (self.calls_seen == 0) self.call0_seq = self.calls_seen;
            if (self.calls_seen == 1) self.call1_seq = self.calls_seen;
            self.calls_seen += 1;
            self.counter += 1;
            self.ran = true;

            var payload = try ret.payloadTyped();
            var any = try payload.initContent();
            const results = try any.initStruct(1, 0);
            results.writeU32(0, n);
        }
    };

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *DirectHandlerServer = castCtx(*DirectHandlerServer, ctx_ptr);
        if (call.interface_id != CALL_INTERFACE_ID or call.method_id != CALL_METHOD_ID) {
            return error.UnexpectedMethod;
        }
        var bc = BuildCtx{ .server = self };
        try peer.sendReturnResults(call.question_id, &bc, BuildCtx.build);
    }
};

// -- Client A: bootstrap-return observer -------------------------------------

const BootstrapProbe = struct {
    reflector_import_id: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *BootstrapProbe = castCtx(*BootstrapProbe, ctx_ptr);
        switch (ret.tag) {
            .results => {},
            else => return error.UnexpectedBootstrapReturn,
        }
        const payload = ret.results orelse return error.MissingBootstrapPayload;
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.reflector_import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.BootstrapNotImported,
        };
    }
};

// -- Client A: reflect call --------------------------------------------------

const ReflectCall = struct {
    target_export_id: u32,
    promise_import_id: ?u32 = null,

    fn build(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const self: *const ReflectCall = castCtx(*const ReflectCall, ctx_ptr);
        var payload = try call.payloadTyped();
        var any = try payload.initContent();
        try any.setCapability(.{ .id = self.target_export_id });
    }

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ReflectCall = castCtx(*ReflectCall, ctx_ptr);
        switch (ret.tag) {
            .results => {},
            else => return error.UnexpectedReflectReturn,
        }
        const payload = ret.results orelse return error.MissingReflectPayload;
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.promise_import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.ReflectPromiseNotImported,
        };
    }
};

// -- Client A: a call on the hosted cap (pipelined or direct) ----------------

const OutboundCall = struct {
    result_n: ?u32 = null,
    returned: bool = false,
    return_tag: ?protocol.ReturnTag = null,

    fn build(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        var payload = try call.payloadTyped();
        var any = try payload.initContent();
        _ = try any.initStruct(0, 0);
    }

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *OutboundCall = castCtx(*OutboundCall, ctx_ptr);
        self.return_tag = ret.tag;
        self.returned = true;
        switch (ret.tag) {
            .results => {
                const payload = ret.results orelse return error.MissingPayload;
                self.result_n = try readResultN(payload);
            },
            .takeFromOtherQuestion => {},
            else => return error.UnexpectedReturn,
        }
    }
};

// -- Server B: ReflectorService bootstrap ------------------------------------

const ReflectorService = struct {
    peer: *Peer,
    target_import_id: ?u32 = null,
    promise_export_id: ?u32 = null,

    const ReturnCtx = struct {
        promise_id: u32,
        fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            const self: *const ReturnCtx = castCtx(*const ReturnCtx, ctx_ptr);
            ret.setReleaseParamCaps(false);
            var payload = try ret.payloadTyped();
            var any = try payload.initContent();
            try any.setCapability(.{ .id = self.promise_id });
        }
    };

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ReflectorService = castCtx(*ReflectorService, ctx_ptr);

        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const target_cap = try call.params.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(target_cap);
        try mutable_caps.retainCapability(target_cap);
        self.target_import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.TargetNotImported,
        };

        const promise_id = try peer.addPromiseExport();
        self.promise_export_id = promise_id;

        var ret_ctx = ReturnCtx{ .promise_id = promise_id };
        try peer.sendReturnResults(call.question_id, &ret_ctx, ReturnCtx.build);
    }
};

test "reflected loopback runs a DIRECT handler and delivers its result value inline" {
    const allocator = std.testing.allocator;

    var wire = Wire{ .allocator = allocator };
    defer wire.deinit();

    var server_peer = Peer.initDetached(allocator);
    server_peer.disableThreadAffinity();
    defer server_peer.deinit();

    var client_peer = Peer.initDetached(allocator);
    client_peer.disableThreadAffinity();
    defer client_peer.deinit();

    defer wire.forwarding = false;

    wire.client_peer = &client_peer;
    wire.server_peer = &server_peer;
    client_peer.setSendFrameOverride(&wire, Wire.clientSend);
    server_peer.setSendFrameOverride(&wire, Wire.serverSend);

    // Server: ReflectorService bootstrap.
    var reflector = ReflectorService{ .peer = &server_peer };
    _ = try server_peer.setBootstrap(.{
        .ctx = &reflector,
        .on_call = ReflectorService.onCall,
    });

    // Client: host the DIRECT-handler cap and bootstrap the reflector.
    var target = DirectHandlerServer{};
    const target_export_id = try client_peer.addExport(.{
        .ctx = &target,
        .on_call = DirectHandlerServer.onCall,
    });

    var bootstrap_probe = BootstrapProbe{};
    _ = try client_peer.sendBootstrap(&bootstrap_probe, BootstrapProbe.onReturn);
    const reflector_import_id = bootstrap_probe.reflector_import_id orelse
        return error.BootstrapDidNotResolve;

    // Q_reflect: reflect(target = our own DIRECT-handler cap).
    var reflect_call = ReflectCall{ .target_export_id = target_export_id };
    _ = try client_peer.sendCall(
        reflector_import_id,
        0x1111_2222_3333_4444,
        0,
        &reflect_call,
        ReflectCall.build,
        ReflectCall.onReturn,
    );
    const promise_import_id = reflect_call.promise_import_id orelse
        return error.ReflectDidNotResolve;

    const target_import_id = reflector.target_import_id orelse return error.TargetNotReflected;
    const promise_export_id = reflector.promise_export_id orelse return error.PromiseNotExported;

    // Q_call1: pipeline call #1 on the (still unresolved) promise import. Parks
    // on P at the server; the direct handler must NOT have run yet.
    var call1 = OutboundCall{};
    _ = try client_peer.sendCall(
        promise_import_id,
        CALL_INTERFACE_ID,
        CALL_METHOD_ID,
        &call1,
        OutboundCall.build,
        OutboundCall.onReturn,
    );
    try std.testing.expect(!call1.returned);
    try std.testing.expect(!target.ran);
    try std.testing.expectEqual(@as(u32, 0), target.calls_seen);

    // Resolve P to the client-hosted direct-handler cap. This forwards the
    // parked call back to the client's cap with sendResultsTo=yourself.
    const resolve_mark = wire.events.items.len;
    try server_peer.resolvePromiseExportToImport(promise_export_id, target_import_id);
    var forwarded_call_id: ?u32 = null;
    for (wire.events.items[resolve_mark..]) |event| {
        if (event.dir == .server_to_client and event.tag == .call) forwarded_call_id = event.id;
    }

    // *** Primary bug: the DIRECT handler body must have run. ***
    try std.testing.expect(target.ran);
    try std.testing.expectEqual(@as(u32, 1), target.calls_seen);
    try std.testing.expect(target.call0_seq != null);

    // *** Level-1 completeness: the caller's pipelined question received the
    //     real result VALUE inline (n == 0), not a bare takeFromOtherQuestion. ***
    try std.testing.expect(call1.returned);
    try std.testing.expectEqual(protocol.ReturnTag.results, call1.return_tag.?);
    try std.testing.expectEqual(@as(u32, 0), call1.result_n.?);
    // The server held the forwarded call open until the client's Finish of
    // the pipelined call, and finished it once, also on this synchronous
    // wire. That Finish took the call's tail mapping: none is left to relay
    // a Finish when the client reuses the pipelined call's id.
    try std.testing.expectEqual(@as(usize, 1), wire.countFinishes(.server_to_client, forwarded_call_id orelse return error.CallNotForwarded));
    try std.testing.expectEqual(@as(usize, 0), server_peer.forwarded_tail_questions.count());
    try std.testing.expectEqual(@as(usize, 0), server_peer.forwarded_questions.count());

    // The reflected resolution embargoed the cap: senderLoopback out,
    // receiverLoopback back.
    try std.testing.expect(wire.hasDisembargo(.client_to_server, .senderLoopback));
    try std.testing.expect(wire.hasDisembargo(.server_to_client, .receiverLoopback));

    // Post-resolution DIRECT call on the now-resolved import returns 1 inline.
    var call2 = OutboundCall{};
    _ = try client_peer.sendCall(
        promise_import_id,
        CALL_INTERFACE_ID,
        CALL_METHOD_ID,
        &call2,
        OutboundCall.build,
        OutboundCall.onReturn,
    );
    try std.testing.expect(call2.returned);
    try std.testing.expectEqual(protocol.ReturnTag.results, call2.return_tag.?);
    try std.testing.expectEqual(@as(u32, 1), call2.result_n.?);
    try std.testing.expectEqual(@as(u32, 2), target.calls_seen);

    // Ordering: the pipelined call reached the handler strictly before the
    // post-embargo direct call.
    try std.testing.expect(target.call0_seq.? < target.call1_seq.?);

    // -- Teardown: release every ref taken so both peers deinit clean. --
    try client_peer.sendFinishForHost(1, false, false);
    try client_peer.releaseImport(promise_import_id, 1);
    try client_peer.releaseImport(reflector_import_id, 1);
    try server_peer.releaseImport(target_import_id, 1);
    try std.testing.expect(!server_peer.caps.hasImport(target_import_id));
    try std.testing.expect(!client_peer.exports.contains(target_export_id));
}

// -- A tail call this peer forwards ------------------------------------------
//
// The server forwards the client's call X as the rpc.capnp tail call
// (`.sent_elsewhere`): it sends Call(Y, sendResultsTo = yourself) to a
// capability the client hosts, then Return(X, takeFromOtherQuestion = Y).
// rpc.capnp `Call.sendResultsTo.yourself` has it finish Y only after the
// client's Finish of X, so the client still holds Y's answer when the
// redirect comes. On this synchronous wire Y's `resultsSentElsewhere` comes
// back inside the send of Y.

/// A client export whose results are one capability: the client's export
/// `cap_export`.
const CapResultServer = struct {
    cap_export: u32,

    const BuildCtx = struct {
        id: u32,
        fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            const self: *const BuildCtx = castCtx(*const BuildCtx, ctx_ptr);
            var payload = try ret.payloadTyped();
            var any = try payload.initContent();
            try any.setCapability(.{ .id = self.id });
        }
    };

    fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *CapResultServer = castCtx(*CapResultServer, ctx_ptr);
        var bc = BuildCtx{ .id = self.cap_export };
        try peer.sendReturnResults(call.question_id, &bc, BuildCtx.build);
    }
};

/// A question whose Return the test does not inspect.
const IgnoredCall = struct {
    returned: bool = false,

    fn build(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        var payload = try call.payloadTyped();
        var any = try payload.initContent();
        _ = try any.initStruct(0, 0);
    }

    fn onReturn(ctx_ptr: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *IgnoredCall = castCtx(*IgnoredCall, ctx_ptr);
        self.returned = true;
    }
};

/// The server's bootstrap. Method 1 takes the client's capability U (its
/// param) and calls it with a retained result (question `qb`). Method 0
/// forwards the call to `qb`'s result, a capability the client hosts, as
/// a promise that resolved to a promised answer is forwarded.
const TailForwardServer = struct {
    qb: ?u32 = null,
    u_import: ?u32 = null,
    qb_call: IgnoredCall = .{},

    fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *TailForwardServer = castCtx(*TailForwardServer, ctx_ptr);
        if (call.method_id == 1) {
            var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
            const cap = try call.params.content.getCapability();
            const resolved = try mutable_caps.resolveCapability(cap);
            try mutable_caps.retainCapability(cap);
            self.u_import = switch (resolved) {
                .imported => |imp| imp.id,
                else => return error.TargetNotImported,
            };
            try peer.sendReturnEmptyStruct(call.question_id);
            self.qb = try peer.sendCallWithOptions(
                self.u_import.?,
                CALL_INTERFACE_ID,
                CALL_METHOD_ID,
                &self.qb_call,
                IgnoredCall.build,
                IgnoredCall.onReturn,
                .{ .result_lifetime = .retained },
            );
            return;
        }
        try Peer.test_hooks.handleResolvedCall(peer, call, caps, .{ .promised = .{
            .question_id = self.qb orelse return error.MissingRetainedQuestion,
            .transform = .{ .list = null },
        } });
    }
};

/// A client call whose params are one capability: the client's export
/// `cap_export`.
const CapParamCall = struct {
    cap_export: u32,
    returned: bool = false,

    fn build(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const self: *const CapParamCall = castCtx(*const CapParamCall, ctx_ptr);
        var payload = try call.payloadTyped();
        var any = try payload.initContent();
        try any.setCapability(.{ .id = self.cap_export });
    }

    fn onReturn(ctx_ptr: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *CapParamCall = castCtx(*CapParamCall, ctx_ptr);
        self.returned = true;
    }
};

test "a tail call this peer forwards on a synchronous transport: the caller gets the value, the named call one Finish" {
    const allocator = std.testing.allocator;
    var wire = Wire{ .allocator = allocator };
    defer wire.deinit();

    var server_peer = Peer.initDetached(allocator);
    server_peer.disableThreadAffinity();
    defer server_peer.deinit();

    var client_peer = Peer.initDetached(allocator);
    client_peer.disableThreadAffinity();
    defer client_peer.deinit();

    defer wire.forwarding = false;

    wire.client_peer = &client_peer;
    wire.server_peer = &server_peer;
    client_peer.setSendFrameOverride(&wire, Wire.clientSend);
    server_peer.setSendFrameOverride(&wire, Wire.serverSend);

    var forwarder = TailForwardServer{};
    _ = try server_peer.setBootstrap(.{ .ctx = &forwarder, .on_call = TailForwardServer.onCall });

    // The client hosts T (a DIRECT handler) and U (whose results are T).
    var target = DirectHandlerServer{ .counter = 42 };
    const t_export = try client_peer.addExport(.{ .ctx = &target, .on_call = DirectHandlerServer.onCall });
    var u_server = CapResultServer{ .cap_export = t_export };
    const u_export = try client_peer.addExport(.{ .ctx = &u_server, .on_call = CapResultServer.onCall });

    var bootstrap_probe = BootstrapProbe{};
    _ = try client_peer.sendBootstrap(&bootstrap_probe, BootstrapProbe.onReturn);
    const server_import = bootstrap_probe.reflector_import_id orelse return error.BootstrapDidNotResolve;

    // The server keeps question QB on U; its result is T.
    var hand_over = CapParamCall{ .cap_export = u_export };
    _ = try client_peer.sendCall(server_import, CALL_INTERFACE_ID, 1, &hand_over, CapParamCall.build, CapParamCall.onReturn);
    try std.testing.expect(hand_over.returned);
    try std.testing.expect(forwarder.qb_call.returned);
    const qb = forwarder.qb orelse return error.MissingRetainedQuestion;

    // X: the server forwards it to QB's result T as the tail call Y.
    var x = OutboundCall{};
    const wire_mark = wire.events.items.len;
    _ = try client_peer.sendCall(server_import, CALL_INTERFACE_ID, CALL_METHOD_ID, &x, OutboundCall.build, OutboundCall.onReturn);
    try std.testing.expect(target.ran);

    var tail_call_id: ?u32 = null;
    for (wire.events.items[wire_mark..]) |event| {
        if (event.dir == .server_to_client and event.tag == .call) tail_call_id = event.id;
    }
    const y = tail_call_id orelse return error.TailCallNotSent;
    try std.testing.expectEqual(@as(usize, 1), wire.countReturns(.client_to_server, y, .resultsSentElsewhere));

    // The redirect found Y's answer: X got T's value, not the bare tag.
    try std.testing.expect(x.returned);
    try std.testing.expectEqual(protocol.ReturnTag.results, x.return_tag.?);
    try std.testing.expectEqual(@as(u32, 42), x.result_n.?);
    try std.testing.expectEqual(@as(usize, 0), client_peer.loopback_result_stash.count());
    // The server finished Y once: when it relayed the client's Finish of X.
    try std.testing.expectEqual(@as(usize, 1), wire.countFinishes(.server_to_client, y));
    try std.testing.expectEqual(@as(usize, 0), server_peer.forwarded_tail_questions.count());
    try std.testing.expectEqual(@as(usize, 0), server_peer.forwarded_questions.count());

    try server_peer.finishRetainedQuestion(qb, false);
    try server_peer.releaseImport(forwarder.u_import.?, 1);
    try client_peer.releaseImport(server_import, 1);
}

// -- Tail calls whose named call the host answers ------------------------------
//
// A tail call is a Return with `takeFromOtherQuestion` that names a call the
// remote sent us with `sendResultsTo = yourself`. The C++ reference sends that
// Return right after the call. When the named call is still unanswered then
// (a deferred handler, or a host such as the native C ABI), the question gets
// the bare `takeFromOtherQuestion` tag at once. Once the call is answered, the
// remote gets `resultsSentElsewhere`, and the Peer keeps a cap-free answer in
// `loopback_result_stash` for a redirect that comes later.
//
// The tests play the remote by hand: they push its frames into the client
// peer, and the client's frames stay on the recorded wire.

/// A server bootstrap that keeps every call unanswered.
const SilentServer = struct {
    fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
};

/// A client export whose handler answers later.
const DeferredTarget = struct {
    answer_id: ?u32 = null,
    /// Answer inside the call instead, as a generated DIRECT handler does,
    /// with a handler that fails while it builds its results. The error
    /// leaves `onCall`, and the Peer's dispatch answers the call with it.
    fail_inline: bool = false,
    /// As `fail_inline`, but answer the build error inside the call with
    /// `sendReturnException`, as generated dispatch does
    /// (`handleCallDirect(...) catch |err| try peer.sendReturnException(...)`).
    fail_inline_answered: bool = false,

    fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *DeferredTarget = castCtx(*DeferredTarget, ctx_ptr);
        self.answer_id = call.question_id;
        if (self.fail_inline) try peer.sendReturnResults(call.question_id, self, FailingBuild.build);
        if (self.fail_inline_answered) {
            peer.sendReturnResults(call.question_id, self, FailingBuild.build) catch |err| {
                try peer.sendReturnException(call.question_id, @errorName(err));
            };
        }
    }
};

/// A results build that fails: the body of a generated DIRECT handler runs
/// inside `sendReturnResults`'s build, so a handler error surfaces here.
const FailingBuild = struct {
    fn build(_: *anyopaque, _: *protocol.ReturnBuilder) anyerror!void {
        return error.TailHandlerFailed;
    }
};

const ResultN = struct {
    n: u32,

    fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        const self: *const ResultN = castCtx(*const ResultN, ctx_ptr);
        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        const results = try any.initStruct(1, 0);
        results.writeU32(0, self.n);
    }
};

/// The caller's question: every Return it gets, by tag.
const TailCaller = struct {
    returns: u32 = 0,
    tag: ?protocol.ReturnTag = null,
    result_n: ?u32 = null,
    reason_buf: [128]u8 = undefined,
    reason_len: usize = 0,

    fn build(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        var payload = try call.payloadTyped();
        var any = try payload.initContent();
        _ = try any.initStruct(0, 0);
    }

    fn onReturn(ctx_ptr: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *TailCaller = castCtx(*TailCaller, ctx_ptr);
        self.returns += 1;
        self.tag = ret.tag;
        switch (ret.tag) {
            .results => self.result_n = try readResultN(ret.results orelse return error.MissingPayload),
            .exception => {
                const text = (ret.exception orelse return error.MissingException).reason;
                self.reason_len = @min(text.len, self.reason_buf.len);
                @memcpy(self.reason_buf[0..self.reason_len], text[0..self.reason_len]);
            },
            else => {},
        }
    }

    fn reason(self: *const TailCaller) []const u8 {
        return self.reason_buf[0..self.reason_len];
    }
};

/// The remote's question id of the tail call (the client's answer id).
const tail_answer_id: u32 = 900;

const TailHarness = struct {
    wire: Wire,
    server_peer: Peer,
    client_peer: Peer,
    silent: u8 = 0,
    target: DeferredTarget = .{},
    caller: TailCaller = .{},
    target_export_id: u32 = 0,
    server_import_id: u32 = 0,
    question_id: u32 = 0,

    /// The client asks the server's bootstrap a question, which the server
    /// keeps. Then the wire stops forwarding: the test plays the server.
    fn start(self: *TailHarness, allocator: std.mem.Allocator) !void {
        try self.startWith(allocator, allocator);
    }

    /// `start`, with the client peer on `client_allocator`.
    fn startWith(self: *TailHarness, allocator: std.mem.Allocator, client_allocator: std.mem.Allocator) !void {
        self.* = .{
            .wire = .{ .allocator = allocator },
            .server_peer = Peer.initDetached(allocator),
            .client_peer = Peer.initDetached(client_allocator),
        };
        self.server_peer.disableThreadAffinity();
        self.client_peer.disableThreadAffinity();
        self.wire.client_peer = &self.client_peer;
        self.wire.server_peer = &self.server_peer;
        self.client_peer.setSendFrameOverride(&self.wire, Wire.clientSend);
        self.server_peer.setSendFrameOverride(&self.wire, Wire.serverSend);

        _ = try self.server_peer.setBootstrap(.{ .ctx = &self.silent, .on_call = SilentServer.onCall });
        self.target_export_id = try self.client_peer.addExport(.{ .ctx = &self.target, .on_call = DeferredTarget.onCall });

        var bootstrap_probe = BootstrapProbe{};
        _ = try self.client_peer.sendBootstrap(&bootstrap_probe, BootstrapProbe.onReturn);
        self.server_import_id = bootstrap_probe.reflector_import_id orelse return error.BootstrapDidNotResolve;
        self.question_id = try self.nextQuestion(&self.caller);
        self.wire.forwarding = false;
    }

    fn deinit(self: *TailHarness) void {
        self.client_peer.deinit();
        self.server_peer.deinit();
        self.wire.deinit();
    }

    /// What the C++ reference sends for a tail call: the `yourself` call on
    /// the client's export, then the redirect for the client's question.
    fn pushTailCall(self: *TailHarness, allocator: std.mem.Allocator) !void {
        try self.pushYourselfCall(allocator, tail_answer_id, self.target_export_id);
        try self.pushRedirect(allocator, self.question_id, tail_answer_id);
    }

    /// The remote calls the client's export `target_export` with question
    /// `answer_id` and `sendResultsTo = yourself`.
    fn pushYourselfCall(self: *TailHarness, allocator: std.mem.Allocator, answer_id: u32, target_export: u32) !void {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        var call = try mb.beginCall(answer_id, CALL_INTERFACE_ID, CALL_METHOD_ID);
        try call.setTargetImportedCap(target_export);
        call.setSendResultsToYourself();
        var payload = try call.payloadTyped();
        _ = try (try payload.initContent()).initStruct(0, 0);
        const frame = try mb.finish();
        defer allocator.free(frame);
        try self.client_peer.handleFrame(frame);
    }

    /// The remote calls the client's export `target_export` with question
    /// `answer_id`; the results go to the remote (`sendResultsTo = caller`).
    fn pushPlainCall(self: *TailHarness, allocator: std.mem.Allocator, answer_id: u32, target_export: u32) !void {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        var call = try mb.beginCall(answer_id, CALL_INTERFACE_ID, CALL_METHOD_ID);
        try call.setTargetImportedCap(target_export);
        var payload = try call.payloadTyped();
        _ = try (try payload.initContent()).initStruct(0, 0);
        const frame = try mb.finish();
        defer allocator.free(frame);
        try self.client_peer.handleFrame(frame);
    }

    /// The remote calls the client's answer `promised` (a call pipelined on
    /// it) with question `answer_id` and `sendResultsTo = yourself`, as the
    /// C++ reference sends a call it bounces back to us as a tail call.
    fn pushPipelinedYourselfCall(
        self: *TailHarness,
        allocator: std.mem.Allocator,
        answer_id: u32,
        promised: u32,
        ops: []const protocol.PromisedAnswerOp,
    ) !void {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        var call = try mb.beginCall(answer_id, CALL_INTERFACE_ID, CALL_METHOD_ID);
        try call.setTargetPromisedAnswerWithOps(promised, ops);
        call.setSendResultsToYourself();
        var payload = try call.payloadTyped();
        _ = try (try payload.initContent()).initStruct(0, 0);
        const frame = try mb.finish();
        defer allocator.free(frame);
        try self.client_peer.handleFrame(frame);
    }

    /// The remote calls the client's answer `promised` (a call pipelined on
    /// it, no transform) with question `answer_id`; the results go to the
    /// remote (`sendResultsTo = caller`).
    fn pushPipelinedPlainCall(self: *TailHarness, allocator: std.mem.Allocator, answer_id: u32, promised: u32) !void {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        var call = try mb.beginCall(answer_id, CALL_INTERFACE_ID, CALL_METHOD_ID);
        try call.setTargetPromisedAnswerWithOps(promised, &.{});
        var payload = try call.payloadTyped();
        _ = try (try payload.initContent()).initStruct(0, 0);
        const frame = try mb.finish();
        defer allocator.free(frame);
        try self.client_peer.handleFrame(frame);
    }

    /// `pushYourselfCall` whose params are one `receiverAnswer` capability:
    /// the result of the client's answer `promised`, pipelined into the
    /// call's arguments.
    fn pushYourselfCallWithAnswerParam(
        self: *TailHarness,
        allocator: std.mem.Allocator,
        answer_id: u32,
        target_export: u32,
        promised: u32,
    ) !void {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        var call = try mb.beginCall(answer_id, CALL_INTERFACE_ID, CALL_METHOD_ID);
        try call.setTargetImportedCap(target_export);
        call.setSendResultsToYourself();
        var payload = try call.payloadTyped();
        try (try payload.initContent()).setCapability(.{ .id = 0 });
        var cap_list = try call.initCapTableTyped(1);
        try protocol.CapDescriptor.writeReceiverAnswer((try cap_list.get(0))._builder, promised, &.{});
        const frame = try mb.finish();
        defer allocator.free(frame);
        try self.client_peer.handleFrame(frame);
    }

    /// The remote answers the client's question `question_id` with
    /// `takeFromOtherQuestion = source`.
    fn pushRedirect(self: *TailHarness, allocator: std.mem.Allocator, question_id: u32, source: u32) !void {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        var ret = try mb.beginReturn(question_id, .takeFromOtherQuestion);
        ret.setReleaseParamCaps(false);
        try ret.setTakeFromOtherQuestion(source);
        const frame = try mb.finish();
        defer allocator.free(frame);
        try self.client_peer.handleFrame(frame);
    }

    /// The remote finishes its question `id` (the client's answer `id`).
    fn pushFinish(self: *TailHarness, allocator: std.mem.Allocator, id: u32) !void {
        try self.pushFinishWith(allocator, id, false);
    }

    /// `pushFinish` with `requireEarlyCancellationWorkaround` as given (the
    /// schema default, and what go-capnp sends, is true).
    fn pushFinishWith(self: *TailHarness, allocator: std.mem.Allocator, id: u32, require_early_cancellation: bool) !void {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        try mb.buildFinish(id, false, require_early_cancellation);
        const frame = try mb.finish();
        defer allocator.free(frame);
        try self.client_peer.handleFrame(frame);
    }

    /// The remote answers the client's question `question_id` with results:
    /// `n = 1`, no capabilities.
    fn pushResults(self: *TailHarness, allocator: std.mem.Allocator, question_id: u32) !void {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        var ret = try mb.beginReturn(question_id, .results);
        ret.setReleaseParamCaps(false);
        var payload = try ret.payloadTyped();
        (try (try payload.initContent()).initStruct(1, 0)).writeU32(0, 1);
        _ = try ret.initCapTableTyped(0);
        const frame = try mb.finish();
        defer allocator.free(frame);
        try self.client_peer.handleFrame(frame);
    }

    /// One more question from the client to the server's bootstrap, which
    /// the test (playing the server) keeps.
    fn nextQuestion(self: *TailHarness, caller: *TailCaller) !u32 {
        return self.client_peer.sendCall(
            self.server_import_id,
            CALL_INTERFACE_ID,
            CALL_METHOD_ID,
            caller,
            TailCaller.build,
            TailCaller.onReturn,
        );
    }
};

test "a tail call whose named call is answered first gets that call's results" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    var result = ResultN{ .n = 7 };
    try h.client_peer.sendReturnResults(tail_answer_id, &result, ResultN.build);
    try std.testing.expectEqual(@as(usize, 1), h.client_peer.loopback_result_stash.count());
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));

    try h.pushRedirect(allocator, h.question_id, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.results, h.caller.tag.?);
    try std.testing.expectEqual(@as(u32, 7), h.caller.result_n.?);
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
    try h.pushFinish(allocator, tail_answer_id);
}

// -- A kept answer never reaches a later tail call -----------------------------
//
// The remote reuses a question id only after its Finish, and the C++
// reference reuses the lowest free id first (rpc.c++ `ExportTable`). An
// answer kept for an earlier call with that id must not answer a tail call
// that names the new one.

test "a yourself call answered after its redirect keeps no answer past the remote's Finish of the call" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // The C++ order: the call, then the redirect, before the host answers.
    // The call is pending, so the question gets the bare tag at once.
    try h.pushTailCall(allocator);
    try std.testing.expectEqual(@as(?u32, tail_answer_id), h.target.answer_id);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.takeFromOtherQuestion, h.caller.tag.?);
    try std.testing.expectEqual(@as(usize, 1), h.wire.countFinishes(.client_to_server, h.question_id));

    // The host answers the call later. No redirect will take this answer.
    var first = ResultN{ .n = 7 };
    try h.client_peer.sendReturnResults(tail_answer_id, &first, ResultN.build);
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));

    // rpc.capnp `Call.sendResultsTo.yourself`: the remote finishes the call
    // after our Finish of the question, so every redirect that names the
    // call has come. Nothing is kept past this Finish.
    try h.pushFinish(allocator, tail_answer_id);
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());

    // The remote reuses the id for its next tail call.
    var caller2 = TailCaller{};
    const q2 = try h.nextQuestion(&caller2);
    h.target.answer_id = null;
    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    try h.pushRedirect(allocator, q2, tail_answer_id);
    try std.testing.expectEqual(@as(?u32, tail_answer_id), h.target.answer_id);
    // The new call is pending too: the bare tag, never the old call's value.
    try std.testing.expectEqual(@as(u32, 1), caller2.returns);
    try std.testing.expectEqual(protocol.ReturnTag.takeFromOtherQuestion, caller2.tag.?);
    try std.testing.expectEqual(@as(?u32, null), caller2.result_n);

    var second = ResultN{ .n = 8 };
    try h.client_peer.sendReturnResults(tail_answer_id, &second, ResultN.build);
    try h.pushFinish(allocator, tail_answer_id);
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
}

test "a question cancelled before its redirect: the named call's answer is not kept past the call's Finish" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // The host cancels the question while the remote's tail call for it is
    // on the way. The cancelled question absorbs the redirect.
    try h.client_peer.cancelQuestion(h.question_id, "host cancel");
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try h.pushTailCall(allocator);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);

    var first = ResultN{ .n = 7 };
    try h.client_peer.sendReturnResults(tail_answer_id, &first, ResultN.build);
    try h.pushFinish(allocator, tail_answer_id);
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());

    // The remote reuses the id for its next tail call: the new question
    // never gets the old call's value.
    var caller2 = TailCaller{};
    const q2 = try h.nextQuestion(&caller2);
    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    try h.pushRedirect(allocator, q2, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 1), caller2.returns);
    try std.testing.expectEqual(protocol.ReturnTag.takeFromOtherQuestion, caller2.tag.?);
    try std.testing.expectEqual(@as(?u32, null), caller2.result_n);

    var second = ResultN{ .n = 8 };
    try h.client_peer.sendReturnResults(tail_answer_id, &second, ResultN.build);
    try h.pushFinish(allocator, tail_answer_id);
}

test "a kept answer of a yourself call is dropped when the remote reuses the call's id" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // The call is answered before any redirect names it: the answer is
    // kept for that redirect.
    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    var first = ResultN{ .n = 7 };
    try h.client_peer.sendReturnResults(tail_answer_id, &first, ResultN.build);
    try std.testing.expectEqual(@as(usize, 1), h.client_peer.loopback_result_stash.count());

    // A remote that reuses the id without its Finish: the new Call makes
    // the kept answer stale, so it is dropped when the Call arrives.
    h.target.answer_id = null;
    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    try std.testing.expectEqual(@as(?u32, tail_answer_id), h.target.answer_id);
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());

    // The redirect names the new call, which is pending: the bare tag.
    try h.pushRedirect(allocator, h.question_id, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.takeFromOtherQuestion, h.caller.tag.?);
    try std.testing.expectEqual(@as(?u32, null), h.caller.result_n);

    var second = ResultN{ .n = 8 };
    try h.client_peer.sendReturnResults(tail_answer_id, &second, ResultN.build);
    try h.pushFinish(allocator, tail_answer_id);
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
}

/// Results whose build delivers the remote's Finish of the call first, as
/// a synchronous transport can while a handler runs.
const FinishInsideBuild = struct {
    h: *TailHarness,
    allocator: std.mem.Allocator,

    fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        const self: *const FinishInsideBuild = castCtx(*const FinishInsideBuild, ctx_ptr);
        try self.h.pushFinish(self.allocator, tail_answer_id);
        var result = ResultN{ .n = 7 };
        try ResultN.build(&result, ret);
    }
};

test "a yourself call finished while its results build keeps no answer" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    var inside = FinishInsideBuild{ .h = &h, .allocator = allocator };
    try h.client_peer.sendReturnResults(tail_answer_id, &inside, FinishInsideBuild.build);
    // The Finish came first: no redirect can name the call any more.
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
}

// -- A yourself call that fails ------------------------------------------------
//
// rpc.capnp answers a `sendResultsTo = yourself` call with
// `resultsSentElsewhere`, also when it fails, and the C++ reference drops the
// connection on an exception Return for its tail call ("Tail call `Return`
// must set `resultsSentElsewhere`, not `exception`", rpc.c++ handleReturn).
// So the remote gets `resultsSentElsewhere`, and the exception is kept for the
// redirect, as cap-free results are. This holds for a call that fails before
// it reaches a handler too: an unknown or broken target, a refused queue, a
// param that names a failed answer, or a promise export it waited on that the
// host rejects.

/// The client sent exactly one Return for `answer_id`, and it is
/// `resultsSentElsewhere`.
fn expectOnlyResultsSentElsewhere(h: *const TailHarness, answer_id: u32) !void {
    try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, answer_id, .exception));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, answer_id, .resultsSentElsewhere));
}

/// The redirect for the harness question, after the tail call failed: the
/// question gets the call's exception, and nothing is kept.
fn expectRedirectGetsException(h: *TailHarness, allocator: std.mem.Allocator, reason: []const u8) !void {
    try h.pushRedirect(allocator, h.question_id, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try std.testing.expectEqualStrings(reason, h.caller.reason());
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
    try h.pushFinish(allocator, tail_answer_id);
}

test "a yourself call to an unknown capability: the remote gets resultsSentElsewhere, the tail call the exception" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushYourselfCall(allocator, tail_answer_id, 0x7fff_0000);
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "unknown capability");
}

test "a yourself call to a rejected promise export: the remote gets resultsSentElsewhere, the tail call the rejection" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const promise_id = try h.client_peer.addPromiseExport();
    try h.client_peer.resolvePromiseExportToException(promise_id, "broken by host");
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "promise broken");
}

test "a yourself call parked on a promise export the host then rejects: the remote gets resultsSentElsewhere" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // The C++ order: the call parks on the promise, and the redirect comes
    // while it waits, so the question gets the bare tag at once.
    const promise_id = try h.client_peer.addPromiseExport();
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    try h.pushRedirect(allocator, h.question_id, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.takeFromOtherQuestion, h.caller.tag.?);
    try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, tail_answer_id, null));

    try h.client_peer.resolvePromiseExportToException(promise_id, "rejected by host");
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    // The remote finishes the call after our Finish of the question:
    // nothing is kept for it.
    try h.pushFinish(allocator, tail_answer_id);
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
}

test "a parked yourself call finished before the host rejects its promise keeps no exception" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const promise_id = try h.client_peer.addPromiseExport();
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    try h.pushRedirect(allocator, h.question_id, tail_answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.takeFromOtherQuestion, h.caller.tag.?);
    // The remote finishes the call with requireEarlyCancellation, so the
    // call stays parked; no redirect can name it any more.
    {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        try mb.buildFinish(tail_answer_id, false, true);
        const frame = try mb.finish();
        defer allocator.free(frame);
        try h.client_peer.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, tail_answer_id, null));

    try h.client_peer.resolvePromiseExportToException(promise_id, "rejected by host");
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    // Nothing is kept for a call the remote has finished, and no mark is
    // left.
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.send_results_to_yourself.count());
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
}

test "a yourself call whose forward fails: the remote gets resultsSentElsewhere, the tail call the failure" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // A promise export that resolved to the server's capability: a call on
    // it is forwarded there. No more outbound questions fit, so the
    // forward fails before it reaches the wire.
    const promise_id = try h.client_peer.addPromiseExport();
    try h.client_peer.resolvePromiseExportToImport(promise_id, h.server_import_id);
    h.client_peer.limits.max_outbound_questions = h.client_peer.questions.count();
    const wire_mark = h.wire.events.items.len;
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    for (h.wire.events.items[wire_mark..]) |event| {
        try std.testing.expect(event.tag != .call);
    }
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "PeerLimitExceeded");
}

test "a yourself call parked on a promise export the host rejects, then its redirect: the tail call gets the rejection" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const promise_id = try h.client_peer.addPromiseExport();
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try h.client_peer.resolvePromiseExportToException(promise_id, "rejected by host");
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "promise broken");
}

test "a yourself call whose queueing is refused: the remote gets resultsSentElsewhere, the tail call the refusal" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // No call may wait on a promise: the call cannot park, and the Peer
    // answers it with the refusal.
    h.client_peer.limits.max_pending_queued_calls = 0;
    const promise_id = try h.client_peer.addPromiseExport();
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "PeerLimitExceeded");
}

test "a call parked on a promise export with a call pipelined on it, finished, then routed back to the remote: the remote reuses both ids" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // Call 600 waits on a promise export, call 700 is pipelined on it, and
    // the remote finishes 600 with requireEarlyCancellationWorkaround =
    // false (what the C++ reference sends). The Peer cancels the waiting
    // call at its Finish and fails 700, as v0.28.0 does.
    const promise_id = try h.client_peer.addPromiseExport();
    try h.pushPlainCall(allocator, 600, promise_id);
    try h.pushPipelinedPlainCall(allocator, 700, 600);
    try h.pushFinish(allocator, 600);
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 600, .canceled));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 700, .exception));

    // The host resolves the promise to the remote's own capability. Nothing
    // is left to forward there, so no question stays open for 600.
    const wire_mark = h.wire.events.items.len;
    try h.client_peer.resolvePromiseExportToImport(promise_id, h.server_import_id);
    for (h.wire.events.items[wire_mark..]) |event| try std.testing.expect(event.tag != .call);
    try std.testing.expect(!h.client_peer.forwarded_tail_questions.contains(600));

    // The remote has both Returns and finishes 700 too. It may now reuse
    // both ids at once (rpc.capnp Call.questionId), and both calls reach
    // the host.
    try h.pushFinish(allocator, 700);
    try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(600)));
    try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(700)));
    var result = ResultN{ .n = 7 };
    for ([_]u32{ 600, 700 }) |id| {
        h.target.answer_id = null;
        try h.pushPlainCall(allocator, id, h.target_export_id);
        try std.testing.expectEqual(@as(?u32, id), h.target.answer_id);
        try h.client_peer.sendReturnResults(id, &result, ResultN.build);
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, id, .results));
    }
}

/// Results whose content is a capability: the client's import `import_id`
/// (a capability the remote hosts), origin-tagged so that an export with
/// the same id does not take its place.
const ResultImport = struct {
    import_id: u32,

    fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        const self: *const ResultImport = castCtx(*const ResultImport, ctx_ptr);
        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        try any.setCapabilityOriginTagged(capnpc.rpc.caps.descriptors.originCodeForTag(.receiverHosted), self.import_id);
    }
};

/// The answer-finished hook (`Peer.setAnswerFinishedHandler`): the answer
/// ids it ran for.
const FinishedHook = struct {
    ids: [8]u32 = undefined,
    count: usize = 0,

    fn onFinished(ctx_ptr: *anyopaque, _: *Peer, answer_id: u32, _: bool) void {
        const self: *FinishedHook = castCtx(*FinishedHook, ctx_ptr);
        if (self.count < self.ids.len) self.ids[self.count] = answer_id;
        self.count += 1;
    }
};

test "go-capnp Finish flags: the remote finishes an answer and the call pipelined on it; the hook runs once, and the remote reuses the call's id" {
    // go-capnp sends requireEarlyCancellationWorkaround = true (the schema
    // default), so the Peer keeps the finished call 700 queued on answer
    // 600. Once the remote has finished 600 and every call pipelined on it,
    // no call waits for 600's results, and the hook runs, in either Finish
    // order. A host that follows it cancels 600, and 700 gets its one
    // Return. Before, the hook did not run. The host's answer named the
    // remote's own capability, so the Peer forwarded 700 there, and nothing
    // ever finished that question. The remote's reuse of id 700 then failed
    // with DuplicateQuestionId.
    const allocator = std.testing.allocator;
    for ([_]bool{ true, false }) |pipelined_call_finished_first| {
        var h: TailHarness = undefined;
        try h.start(allocator);
        defer h.deinit();
        var hook = FinishedHook{};
        h.client_peer.setAnswerFinishedHandler(&hook, FinishedHook.onFinished);

        try h.pushPlainCall(allocator, 600, h.target_export_id);
        try std.testing.expectEqual(@as(?u32, 600), h.target.answer_id);
        try h.pushPipelinedPlainCall(allocator, 700, 600);
        const first: u32 = if (pipelined_call_finished_first) 700 else 600;
        try h.pushFinishWith(allocator, first, true);
        // 700 still waits for 600's results: the hook has not run.
        try std.testing.expectEqual(@as(usize, 0), hook.count);
        try h.pushFinishWith(allocator, if (first == 700) 600 else 700, true);
        const told = hook.count;

        // A host that follows the hook: cancel once it ran, else its
        // handler's answer, which names the remote's own capability.
        const wire_mark = h.wire.events.items.len;
        if (told != 0) {
            try h.client_peer.sendReturnCanceled(600);
        } else {
            var result = ResultImport{ .import_id = h.server_import_id };
            try h.client_peer.sendReturnResults(600, &result, ResultImport.build);
        }
        // The remote answers a call that came to it.
        var forwarded = false;
        var idx = wire_mark;
        while (idx < h.wire.events.items.len) : (idx += 1) {
            const event = h.wire.events.items[idx];
            if (event.dir != .client_to_server or event.tag != .call) continue;
            forwarded = true;
            try h.pushResults(allocator, event.id);
        }

        // The remote has a Return for 700 and finished it: it reuses the id.
        h.target.answer_id = null;
        try h.pushPlainCall(allocator, 700, h.target_export_id);
        try std.testing.expectEqual(@as(?u32, 700), h.target.answer_id);
        try std.testing.expect(!forwarded);
        try std.testing.expectEqual(@as(usize, 1), told);
        try std.testing.expectEqual(@as(u32, 600), hook.ids[0]);
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 600, .canceled));
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 600, null));
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 700, .exception));

        var result = ResultN{ .n = 7 };
        try h.client_peer.sendReturnResults(700, &result, ResultN.build);
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 700, .results));
        try std.testing.expectEqual(@as(usize, 2), h.wire.countReturns(.client_to_server, 700, null));
        try std.testing.expectEqual(@as(usize, 1), hook.count);
        try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(600)));
    }
}

/// The ids of the Calls the client sent from wire event `mark` on, into
/// `out`. Returns how many.
fn clientCallsSince(h: *const TailHarness, mark: usize, out: []u32) !usize {
    var n: usize = 0;
    for (h.wire.events.items[mark..]) |event| {
        if (event.dir != .client_to_server or event.tag != .call) continue;
        if (n == out.len) return error.TestTooManyCalls;
        out[n] = event.id;
        n += 1;
    }
    return n;
}

/// The client peer keeps no record of forwarded question `qf` for answer
/// `answer_id`: no question, no forward mapping, no tail mapping.
fn expectForwardGone(h: *TailHarness, answer_id: u32, qf: u32) !void {
    try std.testing.expect(!h.client_peer.questions.contains(qf));
    try std.testing.expect(!h.client_peer.forwarded_questions.contains(qf));
    try std.testing.expect(!h.client_peer.forwarded_tail_questions.contains(answer_id));
}

/// The remote answers the client's question `question_id` with an
/// exception.
fn pushExceptionReturn(h: *TailHarness, allocator: std.mem.Allocator, question_id: u32) !void {
    var mb = protocol.MessageBuilder.init(allocator);
    defer mb.deinit();
    var ret = try mb.beginReturn(question_id, .exception);
    ret.setReleaseParamCaps(false);
    try ret.setException("remote failed");
    const frame = try mb.finish();
    defer allocator.free(frame);
    try h.client_peer.handleFrame(frame);
}

test "go-capnp Finish flags: a finished call forwarded back to the remote is finished once its Return is relayed; the remote reuses its id" {
    // go-capnp sends Finish with requireEarlyCancellationWorkaround = true
    // (the schema default), so call 700, pipelined on 600, stays queued
    // after its Finish. The host's answer to 600 names the remote's own
    // capability, so the Peer forwards 700 to the remote and turns that
    // question's Return into 700's. The remote's Finish of 700 came before
    // the forward, so no later Finish can end the forwarded question.
    // Before, the Peer held it for one: it was never finished, 700's tail
    // mapping stayed, and the remote's reuse of 700 failed with
    // DuplicateQuestionId. Now the Peer finishes the forwarded question
    // once its Return is relayed. Cases: the two Finish orders, 600
    // finished after the host's answer, an exception from the remote, and
    // a host that answers after the answer-finished hook ran.
    const allocator = std.testing.allocator;
    const Order = enum { pipelined_call_first, answer_first, answer_after_reply };
    const Case = struct { order: Order, hook: bool = false, reply: protocol.ReturnTag = .results };
    for ([_]Case{
        .{ .order = .pipelined_call_first },
        .{ .order = .answer_first },
        .{ .order = .answer_after_reply },
        .{ .order = .pipelined_call_first, .hook = true },
        .{ .order = .answer_after_reply, .hook = true },
        .{ .order = .answer_first, .reply = .exception },
    }) |case| {
        var h: TailHarness = undefined;
        try h.start(allocator);
        defer h.deinit();
        var hook = FinishedHook{};
        if (case.hook) h.client_peer.setAnswerFinishedHandler(&hook, FinishedHook.onFinished);

        try h.pushPlainCall(allocator, 600, h.target_export_id);
        try std.testing.expectEqual(@as(?u32, 600), h.target.answer_id);
        try h.pushPipelinedPlainCall(allocator, 700, 600);
        switch (case.order) {
            .pipelined_call_first => {
                try h.pushFinishWith(allocator, 700, true);
                try h.pushFinishWith(allocator, 600, true);
            },
            .answer_first => {
                try h.pushFinishWith(allocator, 600, true);
                try h.pushFinishWith(allocator, 700, true);
            },
            .answer_after_reply => try h.pushFinishWith(allocator, 700, true),
        }
        const told: usize = if (case.hook and case.order != .answer_after_reply) 1 else 0;
        try std.testing.expectEqual(told, hook.count);

        // The host answers with the remote's own capability, also after
        // the hook ran (a host may answer then instead of canceling).
        const wire_mark = h.wire.events.items.len;
        var result = ResultImport{ .import_id = h.server_import_id };
        try h.client_peer.sendReturnResults(600, &result, ResultImport.build);
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 600, .results));
        var qfs: [2]u32 = undefined;
        try std.testing.expectEqual(@as(usize, 1), try clientCallsSince(&h, wire_mark, &qfs));
        const qf = qfs[0];
        try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, 700, null));
        try std.testing.expectEqual(@as(usize, 0), h.wire.countFinishes(.client_to_server, qf));

        switch (case.reply) {
            .results => try h.pushResults(allocator, qf),
            else => try pushExceptionReturn(&h, allocator, qf),
        }
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 700, case.reply));
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 700, null));
        try std.testing.expectEqual(@as(usize, 1), h.wire.countFinishes(.client_to_server, qf));
        try expectForwardGone(&h, 700, qf);
        if (case.order == .answer_after_reply) try h.pushFinishWith(allocator, 600, true);
        try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(700)));
        try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(600)));

        // The remote has Returns for both and finished both: it reuses
        // both ids, and both calls reach the host.
        var reuse = ResultN{ .n = 7 };
        for ([_]u32{ 700, 600 }) |id| {
            h.target.answer_id = null;
            try h.pushPlainCall(allocator, id, h.target_export_id);
            try std.testing.expectEqual(@as(?u32, id), h.target.answer_id);
            try h.client_peer.sendReturnResults(id, &reuse, ResultN.build);
            try std.testing.expectEqual(@as(usize, 2), h.wire.countReturns(.client_to_server, id, null));
        }
        try std.testing.expectEqual(@as(usize, 1), h.wire.countFinishes(.client_to_server, qf));
        try std.testing.expectEqual(told, hook.count);
    }
}

test "go-capnp Finish flags: a finished call parked on a promise export, forwarded back to the remote, is finished once its Return is relayed" {
    // The pending_export_promises twin of the test above: call 600 waits
    // on a promise export, the remote finishes it with go-capnp's flag, so
    // it stays parked, and the host resolves the promise to the remote's
    // own capability. The Peer forwards 600 there and finishes that
    // question once its Return is relayed. Before, it was never finished,
    // and the remote's reuse of 600 failed with DuplicateQuestionId.
    const allocator = std.testing.allocator;
    for ([_]protocol.ReturnTag{ .results, .exception }) |reply| {
        var h: TailHarness = undefined;
        try h.start(allocator);
        defer h.deinit();

        const promise_id = try h.client_peer.addPromiseExport();
        try h.pushPlainCall(allocator, 600, promise_id);
        try h.pushFinishWith(allocator, 600, true);
        try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, 600, null));

        const wire_mark = h.wire.events.items.len;
        try h.client_peer.resolvePromiseExportToImport(promise_id, h.server_import_id);
        var qfs: [2]u32 = undefined;
        try std.testing.expectEqual(@as(usize, 1), try clientCallsSince(&h, wire_mark, &qfs));
        const qf = qfs[0];
        try std.testing.expectEqual(@as(usize, 0), h.wire.countFinishes(.client_to_server, qf));

        switch (reply) {
            .results => try h.pushResults(allocator, qf),
            else => try pushExceptionReturn(&h, allocator, qf),
        }
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 600, reply));
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 600, null));
        try std.testing.expectEqual(@as(usize, 1), h.wire.countFinishes(.client_to_server, qf));
        try expectForwardGone(&h, 600, qf);
        try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(600)));

        h.target.answer_id = null;
        try h.pushPlainCall(allocator, 600, h.target_export_id);
        try std.testing.expectEqual(@as(?u32, 600), h.target.answer_id);
    }
}

test "go-capnp Finish flags: a finished call forwarded back to the remote ends at transport close or at deinit, with no leak" {
    // The forwarded question waits for the remote's Return. A transport
    // close ends it, and 700 gets its one Return (an exception, which the
    // closed transport drops). Freeing the peer with it open leaks nothing
    // (testing.allocator checks).
    const allocator = std.testing.allocator;
    for ([_]bool{ true, false }) |close_transport| {
        var h: TailHarness = undefined;
        try h.start(allocator);
        defer h.deinit();

        try h.pushPlainCall(allocator, 600, h.target_export_id);
        try h.pushPipelinedPlainCall(allocator, 700, 600);
        try h.pushFinishWith(allocator, 700, true);
        try h.pushFinishWith(allocator, 600, true);
        const wire_mark = h.wire.events.items.len;
        var result = ResultImport{ .import_id = h.server_import_id };
        try h.client_peer.sendReturnResults(600, &result, ResultImport.build);
        var qfs: [2]u32 = undefined;
        try std.testing.expectEqual(@as(usize, 1), try clientCallsSince(&h, wire_mark, &qfs));
        try std.testing.expect(h.client_peer.questions.contains(qfs[0]));
        if (!close_transport) continue;
        h.client_peer.notifyTransportClosed();
        // The close pass sends a best-effort Finish for each open question
        // (the closed transport drops it), and ends the forward.
        try std.testing.expectEqual(@as(usize, 1), h.wire.countFinishes(.client_to_server, qfs[0]));
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 700, .exception));
        try expectForwardGone(&h, 700, qfs[0]);
    }
}

test "a remote call with the id of the peer's forwarded question reaches the host while the forward waits" {
    // Question ids are per direction: the remote's questions and the
    // peer's own questions are counted apart, both up from 0, and go-capnp
    // and C++ take the lowest free id. The forwarded question qf is one of
    // the peer's own. Before, the peer also took qf as one of the remote's
    // ids while the forward waited, and the remote's new call with that id
    // failed with DuplicateQuestionId. Cases: a forward the caller still
    // wants (call 600 parked on a promise export), and one it finished
    // first with go-capnp's Finish flags (call 700 pipelined on 600).
    const allocator = std.testing.allocator;
    for ([_]bool{ false, true }) |finished| {
        var h: TailHarness = undefined;
        try h.start(allocator);
        defer h.deinit();

        const wire_mark = h.wire.events.items.len;
        const fwd_id: u32 = if (finished) 700 else 600;
        if (finished) {
            try h.pushPlainCall(allocator, 600, h.target_export_id);
            try h.pushPipelinedPlainCall(allocator, 700, 600);
            try h.pushFinishWith(allocator, 700, true);
            var result = ResultImport{ .import_id = h.server_import_id };
            try h.client_peer.sendReturnResults(600, &result, ResultImport.build);
        } else {
            const promise_id = try h.client_peer.addPromiseExport();
            try h.pushPlainCall(allocator, 600, promise_id);
            try h.client_peer.resolvePromiseExportToImport(promise_id, h.server_import_id);
        }
        var qfs: [2]u32 = undefined;
        try std.testing.expectEqual(@as(usize, 1), try clientCallsSince(&h, wire_mark, &qfs));
        const qf = qfs[0];
        try std.testing.expect(qf != 600 and qf != 700);
        try std.testing.expect(try h.client_peer.inboundAnswerQuestionIdInUse(fwd_id));
        try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(qf)));

        // While the forward waits, the remote calls with id qf.
        h.target.answer_id = null;
        try h.pushPlainCall(allocator, qf, h.target_export_id);
        try std.testing.expectEqual(@as(?u32, qf), h.target.answer_id);
        var reply = ResultN{ .n = 5 };
        try h.client_peer.sendReturnResults(qf, &reply, ResultN.build);
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, qf, .results));
        try h.pushFinish(allocator, qf);

        // The forward ends as it does without that call.
        try h.pushResults(allocator, qf);
        if (!finished) try h.pushFinish(allocator, fwd_id);
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, fwd_id, .results));
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, fwd_id, null));
        try std.testing.expectEqual(@as(usize, 1), h.wire.countFinishes(.client_to_server, qf));
        try expectForwardGone(&h, fwd_id, qf);
        if (finished) try h.pushFinishWith(allocator, 600, true);

        // Every id is free again, and the remote reuses each.
        for ([_]u32{ qf, 700, 600 }) |id| {
            try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(id)));
            h.target.answer_id = null;
            try h.pushPlainCall(allocator, id, h.target_export_id);
            try std.testing.expectEqual(@as(?u32, id), h.target.answer_id);
        }
    }
}

test "finished calls that owe a Return count in max_active_inbound_questions: a call past it is refused, and every id comes free" {
    // A call the remote finished keeps its record while its Return is
    // owed. Before, those records had a bound of their own, and the peer
    // kept admitting calls while they were full. The Finish of such a call
    // then left no record, so its late Return was recorded as an answer
    // that no Finish clears: the id stayed in use, and the remote's
    // compliant reuse failed with DuplicateQuestionId. That held for the
    // host's answer and for a finished call that the peer forwards back to
    // the remote. Now a finished call that owes a Return counts in the
    // bound on inbound calls, and a call past it gets one exception Return
    // (OwedAnswerLimitExceeded) at once.
    const allocator = std.testing.allocator;
    const Shape = enum { host_answer, forward };
    for ([_]Shape{ .host_answer, .forward }) |shape| {
        var h: TailHarness = undefined;
        try h.start(allocator);
        defer h.deinit();
        h.client_peer.limits.max_active_inbound_questions = 2;
        // The host owes calls 10 and 11, which the remote finished.
        for ([_]u32{ 10, 11 }) |id| {
            try h.pushPlainCall(allocator, id, h.target_export_id);
            try h.pushFinishWith(allocator, id, false);
        }

        h.target.answer_id = null;
        const wire_mark = h.wire.events.items.len;
        try h.pushPlainCall(allocator, 600, h.target_export_id);
        if (shape == .forward) {
            try h.pushPipelinedPlainCall(allocator, 700, 600);
            try h.pushFinishWith(allocator, 700, true);
        }
        try h.pushFinishWith(allocator, 600, true);
        const reached_host = h.target.answer_id != null;
        if (reached_host) {
            // The host answers 600. In the forward shape the answer names
            // the remote's own capability, so 700 goes back to the remote.
            if (shape == .forward) {
                var result = ResultImport{ .import_id = h.server_import_id };
                try h.client_peer.sendReturnResults(600, &result, ResultImport.build);
            } else {
                var result = ResultN{ .n = 1 };
                try h.client_peer.sendReturnResults(600, &result, ResultN.build);
            }
        }
        var qfs: [2]u32 = undefined;
        const n = try clientCallsSince(&h, wire_mark, &qfs);
        for (qfs[0..n]) |qf| try h.pushResults(allocator, qf);
        const ids: []const u32 = if (shape == .forward) &.{ 600, 700 } else &.{600};
        for (ids) |id| try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, id, null));

        // The host answers 10 and 11. The remote has a Return for every
        // call and finished each, so it reuses every id.
        var answer = ResultN{ .n = 2 };
        for ([_]u32{ 10, 11 }) |id| try h.client_peer.sendReturnResults(id, &answer, ResultN.build);
        for ([_]u32{ 10, 11 } ++ [_]u32{ 600, 700 }) |id| {
            if (id == 700 and shape != .forward) continue;
            try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(id)));
            h.target.answer_id = null;
            try h.pushPlainCall(allocator, id, h.target_export_id);
            try std.testing.expectEqual(@as(?u32, id), h.target.answer_id);
            try h.client_peer.sendReturnResults(id, &answer, ResultN.build);
            try h.pushFinish(allocator, id);
        }
        // 600 came past the bound: it never reached the host the first
        // time, and its one Return then was the refusal.
        try std.testing.expect(!reached_host);
        try std.testing.expectEqual(@as(usize, 0), n);
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 600, .exception));
    }
}

/// An allocator over a `std.testing.FailingAllocator` whose induced
/// failure is one-shot: the allocation after the failed one succeeds.
/// (`FailingAllocator` alone fails every later one too.)
const OneShotFailure = struct {
    failing: *std.testing.FailingAllocator,

    fn allocator(self: *OneShotFailure) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *OneShotFailure = castCtx(*OneShotFailure, ctx);
        const result = self.failing.allocator().rawAlloc(len, alignment, ret_addr);
        if (result == null and self.failing.has_induced_failure) self.failing.fail_index = std.math.maxInt(usize);
        return result;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *OneShotFailure = castCtx(*OneShotFailure, ctx);
        return self.failing.allocator().rawResize(memory, alignment, new_len, ret_addr);
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *OneShotFailure = castCtx(*OneShotFailure, ctx);
        return self.failing.allocator().rawRemap(memory, alignment, new_len, ret_addr);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *OneShotFailure = castCtx(*OneShotFailure, ctx);
        self.failing.allocator().rawFree(memory, alignment, ret_addr);
    }
};

test "an allocation failure in a Finish keeps the record of the finished call: its id comes free after its Return" {
    // Before, the record of a call that the remote finished first was put
    // at the Finish, and a failed allocation dropped it. The call's late
    // Return was then recorded as an answer that no Finish clears, and the
    // remote's compliant reuse of the id failed with DuplicateQuestionId.
    // Now the call reserves its record when the peer admits it. One
    // allocation fails, at each index of the Finish. Cases: a call the
    // host answers (the C++ and go-capnp Finish flags), and a call
    // pipelined on it that the peer forwards back to the remote (go-capnp's
    // flags: with the C++ flags the Finish cancels the queued call).
    const allocator = std.testing.allocator;
    for ([_]bool{ false, true }) |forward| {
        for ([_]bool{ false, true }) |go_flags| {
            if (forward and !go_flags) continue;
            const id: u32 = if (forward) 700 else 600;
            var k: usize = 0;
            while (true) : (k += 1) {
                if (k > 1000) return error.TestSweepRunaway;
                var failing = std.testing.FailingAllocator.init(allocator, .{});
                var one_shot = OneShotFailure{ .failing = &failing };
                var h: TailHarness = undefined;
                try h.startWith(allocator, one_shot.allocator());
                defer h.deinit();
                try h.pushPlainCall(allocator, 600, h.target_export_id);
                if (forward) try h.pushPipelinedPlainCall(allocator, 700, 600);
                failing.fail_index = failing.alloc_index + k;
                const finish_ok = if (h.pushFinishWith(allocator, id, go_flags)) true else |_| false;
                const induced = failing.has_induced_failure;
                failing.fail_index = std.math.maxInt(usize);
                // A frame that fails ends the connection: nothing to check.
                if (finish_ok and h.wire.countReturns(.client_to_server, id, null) == 0) {
                    const wire_mark = h.wire.events.items.len;
                    if (forward) {
                        var result = ResultImport{ .import_id = h.server_import_id };
                        try h.client_peer.sendReturnResults(600, &result, ResultImport.build);
                        var qfs: [2]u32 = undefined;
                        const n = try clientCallsSince(&h, wire_mark, &qfs);
                        for (qfs[0..n]) |qf| try h.pushResults(allocator, qf);
                        try h.pushFinish(allocator, 600);
                    } else {
                        var result = ResultN{ .n = 1 };
                        try h.client_peer.sendReturnResults(600, &result, ResultN.build);
                    }
                    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, id, null));
                    try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(id)));
                    h.target.answer_id = null;
                    try h.pushPlainCall(allocator, id, h.target_export_id);
                    try std.testing.expectEqual(@as(?u32, id), h.target.answer_id);
                }
                if (!induced) break;
            }
            try std.testing.expect(k > 0);
        }
    }
}

/// A transport binding for the client peer of a `TailHarness` that only
/// records a close request: the harness still sends through its wire
/// (the send override comes first). The peer asks its transport to close
/// when it cannot send a Return it owes and nothing else will.
const CloseRecorder = struct {
    closed: bool = false,

    fn attach(self: *CloseRecorder, peer: *Peer) void {
        peer.attachTransportBinding(.{ .ctx = self, .send = send, .close = close, .is_closing = isClosing });
    }

    fn send(_: *anyopaque, _: []const u8) anyerror!void {
        return error.TestUnexpectedTransportSend;
    }

    fn close(ctx: *anyopaque) void {
        castCtx(*CloseRecorder, ctx).closed = true;
    }

    fn isClosing(ctx: *anyopaque) bool {
        return castCtx(*CloseRecorder, ctx).closed;
    }
};

const ForwardSweep = enum { answer_import, answer_import_hook, reply_exception, promise_export };

/// One run of a forward after the Finish (the cases above) with the
/// client peer's allocations failing from the `k`-th after setup: every
/// later one in the same step, or only that one (`one_shot`). A frame the
/// peer fails to handle ends the connection there, and so does a close
/// that the peer asks for: the test then tells the peer that the
/// transport closed, as a transport does. Whatever fails: at most one
/// Return per call and one Finish for the forwarded question. While the
/// connection is up, every call gets its one Return, the forwarded
/// question its one Finish, no record stays, and the remote's reuse of
/// each id reaches the host. testing.allocator checks for leaks at deinit.
/// Returns whether a failure was induced (the sweep's end condition).
fn forwardAfterFinishUnderOom(k: usize, variant: ForwardSweep, one_shot: bool) !bool {
    const allocator = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(allocator, .{});
    var one_shot_alloc = OneShotFailure{ .failing = &failing };
    var h: TailHarness = undefined;
    try h.startWith(allocator, if (one_shot) one_shot_alloc.allocator() else failing.allocator());
    defer h.deinit();
    var transport = CloseRecorder{};
    transport.attach(&h.client_peer);
    var hook = FinishedHook{};
    if (variant == .answer_import_hook) h.client_peer.setAnswerFinishedHandler(&hook, FinishedHook.onFinished);
    const promise_id: u32 = if (variant == .promise_export) try h.client_peer.addPromiseExport() else 0;
    // The forwarded call: 700 pipelined on 600, or 600 on the promise.
    const fwd_id: u32 = if (variant == .promise_export) 600 else 700;
    failing.fail_index = failing.alloc_index + k;

    const Step = enum { call, pipelined, finish_pipelined, finish_answer, answer, reply };
    var qf: ?u32 = null;
    var closed = false;
    for (std.enums.values(Step)) |step| {
        const frame_ok: bool = switch (step) {
            .call => if (h.pushPlainCall(allocator, 600, if (variant == .promise_export) promise_id else h.target_export_id)) true else |_| false,
            .pipelined => if (variant == .promise_export) true else (if (h.pushPipelinedPlainCall(allocator, 700, 600)) true else |_| false),
            .finish_pipelined => if (variant == .promise_export) true else (if (h.pushFinishWith(allocator, 700, true)) true else |_| false),
            .finish_answer => if (h.pushFinishWith(allocator, 600, true)) true else |_| false,
            .answer => blk: {
                const wire_mark = h.wire.events.items.len;
                if (variant == .promise_export) {
                    h.client_peer.resolvePromiseExportToImport(promise_id, h.server_import_id) catch {
                        failing.fail_index = std.math.maxInt(usize);
                        h.client_peer.resolvePromiseExportToException(promise_id, "resolve failed") catch {};
                    };
                } else if (h.target.answer_id != null) {
                    var result = ResultImport{ .import_id = h.server_import_id };
                    h.client_peer.sendReturnResults(600, &result, ResultImport.build) catch {
                        failing.fail_index = std.math.maxInt(usize);
                        h.client_peer.sendReturnException(600, "answer failed") catch {};
                    };
                }
                var qfs: [2]u32 = undefined;
                const n = try clientCallsSince(&h, wire_mark, &qfs);
                try std.testing.expect(n <= 1);
                if (n == 1) qf = qfs[0];
                break :blk true;
            },
            .reply => if (qf) |id| (if (variant == .reply_exception)
                (if (pushExceptionReturn(&h, allocator, id)) true else |_| false)
            else
                (if (h.pushResults(allocator, id)) true else |_| false)) else true,
        };
        if (failing.has_induced_failure) failing.fail_index = std.math.maxInt(usize);
        if (!h.client_peer.active_inbound_questions.contains(fwd_id)) {
            try std.testing.expect(!h.client_peer.forwarded_tail_questions.contains(fwd_id));
        }
        if (!frame_ok or transport.closed) {
            h.client_peer.notifyTransportClosed();
            closed = true;
            break;
        }
    }
    failing.fail_index = std.math.maxInt(usize);
    for ([_]u32{ 600, fwd_id }) |id| try std.testing.expect(h.wire.countReturns(.client_to_server, id, null) <= 1);
    if (qf) |id| try std.testing.expect(h.wire.countFinishes(.client_to_server, id) <= 1);
    if (!closed) {
        for ([_]u32{ 600, fwd_id }) |id| try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, id, null));
        if (qf) |id| {
            try std.testing.expectEqual(@as(usize, 1), h.wire.countFinishes(.client_to_server, id));
            try expectForwardGone(&h, fwd_id, id);
        }
        if (!failing.has_induced_failure) {
            _ = qf orelse return error.TestNoForward;
            try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, fwd_id, if (variant == .reply_exception) .exception else .results));
        }
        for ([_]u32{ fwd_id, 600 }) |id| {
            try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(id)));
            h.target.answer_id = null;
            try h.pushPlainCall(allocator, id, h.target_export_id);
            try std.testing.expectEqual(@as(?u32, id), h.target.answer_id);
            var reply = ResultN{ .n = 1 };
            try h.client_peer.sendReturnResults(id, &reply, ResultN.build);
            try h.pushFinish(allocator, id);
        }
    }
    return failing.has_induced_failure;
}

const CancelShape = enum { queued_call, forwarded_call };

/// One run of a Finish (C++ flags) that makes the peer answer a call
/// `canceled` itself, with one allocation failing, the `k`-th of the
/// Finish. `queued_call`: call 700 waits, pipelined on 600, and its Finish
/// cancels it. `forwarded_call`: call 600, parked on a promise export, was
/// forwarded to the remote as qf, and its Finish comes before qf's Return:
/// the peer relays the Finish and answers 600 itself. Either the call gets
/// one Return, or the peer asks its transport to close. While the
/// connection is up, the id comes free and the remote's reuse reaches the
/// host. Returns whether a failure was induced.
fn cancelUnderOneShotOom(k: usize, shape: CancelShape) !bool {
    const allocator = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(allocator, .{});
    var one_shot = OneShotFailure{ .failing = &failing };
    var h: TailHarness = undefined;
    try h.startWith(allocator, one_shot.allocator());
    defer h.deinit();
    var transport = CloseRecorder{};
    transport.attach(&h.client_peer);

    const id: u32 = if (shape == .queued_call) 700 else 600;
    var qf: ?u32 = null;
    switch (shape) {
        .queued_call => {
            try h.pushPlainCall(allocator, 600, h.target_export_id);
            try h.pushPipelinedPlainCall(allocator, 700, 600);
        },
        .forwarded_call => {
            const promise_id = try h.client_peer.addPromiseExport();
            try h.pushPlainCall(allocator, 600, promise_id);
            const wire_mark = h.wire.events.items.len;
            try h.client_peer.resolvePromiseExportToImport(promise_id, h.server_import_id);
            var qfs: [2]u32 = undefined;
            try std.testing.expectEqual(@as(usize, 1), try clientCallsSince(&h, wire_mark, &qfs));
            qf = qfs[0];
        },
    }
    failing.fail_index = failing.alloc_index + k;
    const finish_ok = if (h.pushFinishWith(allocator, id, false)) true else |_| false;
    const induced = failing.has_induced_failure;
    failing.fail_index = std.math.maxInt(usize);
    try std.testing.expect(h.wire.countReturns(.client_to_server, id, null) <= 1);
    if (!finish_ok or transport.closed) {
        h.client_peer.notifyTransportClosed();
        return induced;
    }
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, id, .canceled));
    if (qf) |q| {
        try std.testing.expectEqual(@as(usize, 1), h.wire.countFinishes(.client_to_server, q));
        // The remote's late Return for the forwarded question is absorbed.
        try h.pushResults(allocator, q);
        try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, id, null));
        try expectForwardGone(&h, id, q);
    }
    if (shape == .queued_call) {
        var result = ResultN{ .n = 1 };
        try h.client_peer.sendReturnResults(600, &result, ResultN.build);
        try h.pushFinish(allocator, 600);
    }
    try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(id)));
    h.target.answer_id = null;
    try h.pushPlainCall(allocator, id, h.target_export_id);
    try std.testing.expectEqual(@as(?u32, id), h.target.answer_id);
    return induced;
}

test "a Finish that the peer answers canceled itself: under an allocation failure, one Return or the connection ends" {
    // The peer answers a call canceled itself when the caller's Finish
    // cancels a queued call, or comes before the Return of a forward of
    // that call. Before, a Return(canceled) that could not be sent was only
    // reported: the call had no Return, and the connection stayed up.
    for (std.enums.values(CancelShape)) |shape| {
        var k: usize = 0;
        while (try cancelUnderOneShotOom(k, shape)) : (k += 1) {
            if (k > 1000) return error.TestSweepRunaway;
        }
        try std.testing.expect(k > 2);
    }
}

const HookFollow = enum { cancel, results };

/// One run of the deferred answer-finished hook under an allocation
/// failure: call 600, call 700 pipelined on it, both finished with
/// go-capnp's flags (600 first, or 700 first), then the host answers 600:
/// it cancels 600 once the hook ran (`cancel`), or answers with results.
/// The client peer's allocations fail from the `k`-th after setup: every
/// later one in the step, or only that one (`one_shot`). A refused cancel
/// is sent again, as a host does. A frame that fails, or a close that the
/// peer asks for, ends the connection. Whatever fails: the hook runs at
/// most once, and each call gets at most one Return. While the connection
/// is up: the hook ran exactly once, each call has exactly one Return, and
/// the remote's reuse of both ids reaches the host. Returns whether a
/// failure was induced.
fn deferredHookUnderOom(k: usize, answer_finished_first: bool, follow: HookFollow, one_shot: bool) !bool {
    const allocator = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(allocator, .{});
    var one_shot_alloc = OneShotFailure{ .failing = &failing };
    var h: TailHarness = undefined;
    try h.startWith(allocator, if (one_shot) one_shot_alloc.allocator() else failing.allocator());
    defer h.deinit();
    var transport = CloseRecorder{};
    transport.attach(&h.client_peer);
    var hook = FinishedHook{};
    h.client_peer.setAnswerFinishedHandler(&hook, FinishedHook.onFinished);
    failing.fail_index = failing.alloc_index + k;

    const Step = enum { call, pipelined, first_finish, second_finish, answer };
    var closed = false;
    for (std.enums.values(Step)) |step| {
        const frame_ok: bool = switch (step) {
            .call => if (h.pushPlainCall(allocator, 600, h.target_export_id)) true else |_| false,
            .pipelined => if (h.pushPipelinedPlainCall(allocator, 700, 600)) true else |_| false,
            .first_finish => if (h.pushFinishWith(allocator, if (answer_finished_first) 600 else 700, true)) true else |_| false,
            .second_finish => if (h.pushFinishWith(allocator, if (answer_finished_first) 700 else 600, true)) true else |_| false,
            .answer => blk: {
                if (h.target.answer_id == null) break :blk true;
                if (follow == .cancel and hook.count == 1) {
                    h.client_peer.sendReturnCanceled(600) catch |err| {
                        try std.testing.expectEqual(error.OutOfMemory, err);
                        failing.fail_index = std.math.maxInt(usize);
                        try h.client_peer.sendReturnCanceled(600);
                    };
                } else {
                    var result = ResultN{ .n = 3 };
                    h.client_peer.sendReturnResults(600, &result, ResultN.build) catch {
                        failing.fail_index = std.math.maxInt(usize);
                        try h.client_peer.sendReturnException(600, "answer failed");
                    };
                }
                break :blk true;
            },
        };
        if (failing.has_induced_failure) failing.fail_index = std.math.maxInt(usize);
        if (!frame_ok or transport.closed) {
            h.client_peer.notifyTransportClosed();
            closed = true;
            break;
        }
    }
    failing.fail_index = std.math.maxInt(usize);
    try std.testing.expect(hook.count <= 1);
    for ([_]u32{ 600, 700 }) |id| try std.testing.expect(h.wire.countReturns(.client_to_server, id, null) <= 1);
    if (!closed) {
        try std.testing.expectEqual(@as(usize, 1), hook.count);
        try std.testing.expectEqual(@as(u32, 600), hook.ids[0]);
        for ([_]u32{ 600, 700 }) |id| {
            try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, id, null));
            try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(id)));
            h.target.answer_id = null;
            try h.pushPlainCall(allocator, id, h.target_export_id);
            try std.testing.expectEqual(@as(?u32, id), h.target.answer_id);
            var reply = ResultN{ .n = 1 };
            try h.client_peer.sendReturnResults(id, &reply, ResultN.build);
            try h.pushFinish(allocator, id);
        }
        if (follow == .cancel) try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 600, .canceled));
    }
    return failing.has_induced_failure;
}

test "go-capnp Finish flags: an allocation failure anywhere around the deferred answer-finished hook: the hook runs once, one Return per call" {
    // The hook for answer 600 waits while call 700, pipelined on it, is
    // still wanted, and runs at 700's Finish (P0-1). That path allocates
    // nothing: the Finish records were reserved when the calls were
    // admitted. Both Finish orders, a host that cancels and one that
    // answers, sticky and one-shot failures.
    for ([_]bool{ false, true }) |one_shot| {
        for ([_]bool{ true, false }) |answer_finished_first| {
            for (std.enums.values(HookFollow)) |follow| {
                var k: usize = 0;
                while (try deferredHookUnderOom(k, answer_finished_first, follow, one_shot)) : (k += 1) {
                    if (k > 10_000) return error.TestSweepRunaway;
                }
                try std.testing.expect(k > 10);
            }
        }
    }
}

test "go-capnp Finish flags: an allocation failure anywhere in a forward after the Finish: one Return per call or the connection ends, no leak" {
    // Before, a call that the peer replayed after its Finish could end
    // with no Return while the connection stayed up: its forward failed,
    // and so did the exception Return after it (or a parked call's
    // exception Return, or the cancel of a queued call). The replay only
    // reported the error. Now a Return that the peer owes and cannot send
    // ends the connection, so the remote's question ends at disconnect.
    for ([_]bool{ false, true }) |one_shot| {
        for (std.enums.values(ForwardSweep)) |variant| {
            var k: usize = 0;
            while (try forwardAfterFinishUnderOom(k, variant, one_shot)) : (k += 1) {
                if (k > 10_000) return error.TestSweepRunaway;
            }
            try std.testing.expect(k > 10);
        }
    }
}

/// One run of an exception Return for call 600 whose drain is wider than
/// the drain's stack worklist: `width` calls 700.. pipelined on 600, and
/// one call 800.. pipelined on each of them. One allocation fails, the
/// `k`-th of the Return (`OneShotFailure`). A Return the host could not
/// send is sent again. Every call gets at most one Return. Either the
/// peer asks its transport to close, or every call has its one Return,
/// no id stays in use, and the remote's reuse of an id reaches the host.
/// Returns whether a failure was induced.
fn wideDrainUnderOneShotOom(k: usize, width: u32) !bool {
    const allocator = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(allocator, .{});
    var one_shot = OneShotFailure{ .failing = &failing };
    var h: TailHarness = undefined;
    try h.startWith(allocator, one_shot.allocator());
    defer h.deinit();
    var transport = CloseRecorder{};
    transport.attach(&h.client_peer);
    h.client_peer.limits.max_active_inbound_questions = 2 * width + 2;
    try h.pushPlainCall(allocator, 600, h.target_export_id);
    var i: u32 = 0;
    while (i < width) : (i += 1) try h.pushPipelinedPlainCall(allocator, 700 + i, 600);
    i = 0;
    while (i < width) : (i += 1) try h.pushPipelinedPlainCall(allocator, 800 + i, 700 + i);

    failing.fail_index = failing.alloc_index + k;
    const sent = if (h.client_peer.sendReturnException(600, "answer failed")) true else |_| false;
    const induced = failing.has_induced_failure;
    failing.fail_index = std.math.maxInt(usize);
    if (!sent and !transport.closed) try h.client_peer.sendReturnException(600, "answer failed");

    try std.testing.expect(h.wire.countReturns(.client_to_server, 600, null) <= 1);
    i = 0;
    while (i < width) : (i += 1) {
        for ([_]u32{ 700 + i, 800 + i }) |id| try std.testing.expect(h.wire.countReturns(.client_to_server, id, null) <= 1);
    }
    if (transport.closed) {
        h.client_peer.notifyTransportClosed();
        return induced;
    }
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 600, null));
    i = 0;
    while (i < width) : (i += 1) {
        for ([_]u32{ 700 + i, 800 + i }) |id| {
            try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, id, .exception));
            try h.pushFinish(allocator, id);
            try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(id)));
        }
    }
    try h.pushFinish(allocator, 600);
    for ([_]u32{ 800 + width - 1, 700 + width - 1, 600 }) |id| {
        h.target.answer_id = null;
        try h.pushPlainCall(allocator, id, h.target_export_id);
        try std.testing.expectEqual(@as(?u32, id), h.target.answer_id);
    }
    return induced;
}

test "an exception Return whose drain is wider than its stack worklist: under an allocation failure, one Return per call or the connection ends" {
    // The drain that fails the calls pipelined on a failed answer keeps
    // its first 32 answer ids on the stack, and more in a list that
    // grows. Before, when that list could not grow, the calls pipelined on
    // the 33rd call and later got no Return, and the connection stayed up.
    // Now the peer ends the connection there.
    var k: usize = 0;
    while (try wideDrainUnderOneShotOom(k, 34)) : (k += 1) {
        if (k > 5000) return error.TestSweepRunaway;
    }
    try std.testing.expect(k > 40);
}

/// A forward that the caller still wants: call 600 on a promise export
/// that the host resolves to the remote's capability (W1), or call 700
/// pipelined on 600 when the host answers 600 with that capability.
const WantedForward = enum { promise_export, pipelined };

/// How the relay of the forwarded Return fails: every allocation from
/// the `k`-th (`sticky`), only the `k`-th (`one_shot`), or the next one or
/// two sends (`send`, `k` of them; a full outbound queue).
const RelayFailure = enum { sticky, one_shot, send };

/// One run of a wanted forward whose relayed Return meets a failure: the
/// remote answers the forwarded question qf with results, and the peer
/// relays them as the call's Return. The client peer may admit one call
/// at a time (`max_active_inbound_questions`). Whatever fails: at most one
/// Return for the call and one Finish for qf. Either the peer asks its
/// transport to close, or the call has its one Return; the remote's Finish
/// of the call then sends one Finish for qf, the id comes free, and a new
/// call still reaches the host. Returns whether a failure was induced.
fn wantedForwardRelayUnderFailure(k: usize, shape: WantedForward, failure: RelayFailure) !bool {
    const allocator = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(allocator, .{});
    var one_shot = OneShotFailure{ .failing = &failing };
    var h: TailHarness = undefined;
    try h.startWith(allocator, switch (failure) {
        .sticky => failing.allocator(),
        .one_shot => one_shot.allocator(),
        .send => allocator,
    });
    defer h.deinit();
    var transport = CloseRecorder{};
    transport.attach(&h.client_peer);

    const fwd_id: u32 = if (shape == .promise_export) 600 else 700;
    const wire_mark = h.wire.events.items.len;
    switch (shape) {
        .promise_export => {
            h.client_peer.limits.max_active_inbound_questions = 1;
            const promise_id = try h.client_peer.addPromiseExport();
            try h.pushPlainCall(allocator, 600, promise_id);
            try h.client_peer.resolvePromiseExportToImport(promise_id, h.server_import_id);
        },
        .pipelined => {
            h.client_peer.limits.max_active_inbound_questions = 2;
            try h.pushPlainCall(allocator, 600, h.target_export_id);
            try h.pushPipelinedPlainCall(allocator, 700, 600);
            var result = ResultImport{ .import_id = h.server_import_id };
            try h.client_peer.sendReturnResults(600, &result, ResultImport.build);
            try h.pushFinish(allocator, 600);
        },
    }
    var qfs: [2]u32 = undefined;
    try std.testing.expectEqual(@as(usize, 1), try clientCallsSince(&h, wire_mark, &qfs));
    const qf = qfs[0];

    var induced = false;
    switch (failure) {
        .sticky, .one_shot => failing.fail_index = failing.alloc_index + k,
        .send => h.wire.fail_client_sends = k,
    }
    const reply_ok = if (h.pushResults(allocator, qf)) true else |_| false;
    switch (failure) {
        .sticky, .one_shot => {
            induced = failing.has_induced_failure;
            failing.fail_index = std.math.maxInt(usize);
        },
        .send => {
            induced = k > 0 and h.wire.fail_client_sends < k;
            h.wire.fail_client_sends = 0;
        },
    }

    try std.testing.expect(h.wire.countReturns(.client_to_server, fwd_id, null) <= 1);
    if (!reply_ok or transport.closed) {
        h.client_peer.notifyTransportClosed();
        try std.testing.expect(h.wire.countFinishes(.client_to_server, qf) <= 1);
        return induced;
    }
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, fwd_id, null));
    try h.pushFinish(allocator, fwd_id);
    try std.testing.expectEqual(@as(usize, 1), h.wire.countFinishes(.client_to_server, qf));
    try expectForwardGone(&h, fwd_id, qf);
    for ([_]u32{ fwd_id, 600 }) |id| try std.testing.expect(!(try h.client_peer.inboundAnswerQuestionIdInUse(id)));
    h.target.answer_id = null;
    try h.pushPlainCall(allocator, fwd_id, h.target_export_id);
    try std.testing.expectEqual(@as(?u32, fwd_id), h.target.answer_id);
    return induced;
}

test "a forward the caller still wants: when its relayed Return cannot be sent, the connection ends" {
    // The peer relays the forwarded question's Return as the call's
    // Return, and it alone answers the call. Before, when that relay
    // failed (out of memory, or two failed sends, such as a full outbound
    // queue), the peer only reported it: the call had no Return, the
    // connection stayed up, and the call's later Finish left a record
    // that took a place in max_active_inbound_questions for good. Now the
    // peer ends the connection there.
    for (std.enums.values(WantedForward)) |shape| {
        for ([_]RelayFailure{ .sticky, .one_shot }) |failure| {
            var k: usize = 0;
            while (try wantedForwardRelayUnderFailure(k, shape, failure)) : (k += 1) {
                if (k > 5000) return error.TestSweepRunaway;
            }
            try std.testing.expect(k > 5);
        }
        for ([_]usize{ 1, 2 }) |sends| try std.testing.expect(try wantedForwardRelayUnderFailure(sends, shape, .send));
    }
}

test "a yourself call whose param names a failed answer: the remote gets resultsSentElsewhere, the tail call that failure" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // The remote's plain call 500 fails; then its tail call passes 500's
    // result as a param. The Peer fails the call with 500's exception
    // before the handler runs.
    try h.pushPlainCall(allocator, 500, h.target_export_id);
    try h.client_peer.sendReturnException(500, "answer 500 failed");
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 500, .exception));
    h.target.answer_id = null;
    try h.pushYourselfCallWithAnswerParam(allocator, tail_answer_id, h.target_export_id, 500);
    try std.testing.expectEqual(@as(?u32, null), h.target.answer_id);
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "answer 500 failed");
}

test "a yourself call parked on a promise export that cannot park again on replay: the remote gets resultsSentElsewhere" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // The call parks on promise 1. Promise 1 then resolves to promise 2,
    // which is unresolved, so the replay parks the call again; the queue
    // budget refuses that.
    const promise1 = try h.client_peer.addPromiseExport();
    const promise2 = try h.client_peer.addPromiseExport();
    try h.pushYourselfCall(allocator, tail_answer_id, promise1);
    try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    h.client_peer.limits.max_pending_queued_calls = 0;
    try h.client_peer.resolvePromiseExportToExport(promise1, promise2);
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "PeerLimitExceeded");
}

test "a yourself call pipelined on a failed answer: the remote gets resultsSentElsewhere, the tail call that failure" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushPlainCall(allocator, 500, h.target_export_id);
    try h.client_peer.sendReturnException(500, "answer 500 failed");
    try h.pushPipelinedYourselfCall(allocator, tail_answer_id, 500, &.{});
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "answer 500 failed");
}

// A yourself call pipelined on one of our answers that is still pending
// queues on that answer (`pending_promises`). The C++ reference sends such a
// call when its method tail-calls a capability in the result of a call it
// made to us. The failure drain of that queue answers the call too.

test "a yourself call queued on an answer that then fails: the remote gets resultsSentElsewhere, the tail call that failure" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // Answer 500 is pending (the deferred target keeps it), so the tail
    // call queues on it.
    try h.pushPlainCall(allocator, 500, h.target_export_id);
    try h.pushPipelinedYourselfCall(allocator, tail_answer_id, 500, &.{});
    try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try h.client_peer.sendReturnException(500, "answer 500 failed");
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "answer 500 failed");
    try h.pushFinish(allocator, 500);
}

test "a yourself call queued on an answer whose replay cannot park again: the remote gets resultsSentElsewhere" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // Answer 500 resolves to an unresolved promise export, so the replay
    // parks the queued call on it; the queue budget refuses that.
    const promise_id = try h.client_peer.addPromiseExport();
    try h.pushPlainCall(allocator, 500, h.target_export_id);
    try h.pushPipelinedYourselfCall(allocator, tail_answer_id, 500, &.{});
    try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    h.client_peer.limits.max_pending_queued_calls = 0;
    var result = ResultCap{ .export_id = promise_id };
    try h.client_peer.sendReturnResults(500, &result, ResultCap.build);
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "PeerLimitExceeded");
    try h.pushFinish(allocator, 500);
}

test "a yourself call queued on a call the remote cancels: the remote gets resultsSentElsewhere" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // Call 600 parks on a promise export, and the tail call queues on 600.
    // The remote's Finish of 600 cancels it, and the calls queued on it fail.
    const promise_id = try h.client_peer.addPromiseExport();
    try h.pushPlainCall(allocator, 600, promise_id);
    try h.pushPipelinedYourselfCall(allocator, tail_answer_id, 600, &.{});
    try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try h.pushFinish(allocator, 600);
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 600, .canceled));
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "pipelined on a cancelled call");
}

test "a yourself call pipelined on an answer that holds no capability: the remote gets resultsSentElsewhere" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // Answer 500's results are a plain struct, so a call on them names no
    // capability, and the Peer fails it.
    try h.pushPlainCall(allocator, 500, h.target_export_id);
    var result = ResultN{ .n = 7 };
    try h.client_peer.sendReturnResults(500, &result, ResultN.build);
    h.target.answer_id = null;
    try h.pushPipelinedYourselfCall(allocator, tail_answer_id, 500, &.{});
    try std.testing.expectEqual(@as(?u32, null), h.target.answer_id);
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "promised answer missing");
}

/// Results whose content is a capability: the client's export `export_id`.
const ResultCap = struct {
    export_id: u32,

    fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        const self: *const ResultCap = castCtx(*const ResultCap, ctx_ptr);
        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        try any.setCapability(.{ .id = self.export_id });
    }
};

test "a yourself call pipelined through an answer its transform cannot follow: the remote gets resultsSentElsewhere" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // Answer 500's results are a capability, and the call asks for pointer
    // field 0 of them, as if they were a struct: the Peer cannot resolve
    // the target and fails the call.
    try h.pushPlainCall(allocator, 500, h.target_export_id);
    var result = ResultCap{ .export_id = h.target_export_id };
    try h.client_peer.sendReturnResults(500, &result, ResultCap.build);
    h.target.answer_id = null;
    const field0 = [_]protocol.PromisedAnswerOp{.{ .tag = .getPointerField, .pointer_index = 0 }};
    try h.pushPipelinedYourselfCall(allocator, tail_answer_id, 500, &field0);
    try std.testing.expectEqual(@as(?u32, null), h.target.answer_id);
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try h.pushRedirect(allocator, h.question_id, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
    try h.pushFinish(allocator, tail_answer_id);
    try h.pushFinish(allocator, 500);
}

test "a DIRECT handler that fails a yourself call made through a resolved promise export: the remote gets resultsSentElsewhere" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const promise_id = try h.client_peer.addPromiseExport();
    try h.client_peer.resolvePromiseExportToExport(promise_id, h.target_export_id);
    h.target.fail_inline = true;
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    try std.testing.expectEqual(@as(?u32, tail_answer_id), h.target.answer_id);
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "TailHandlerFailed");
}

test "a DIRECT handler that fails a yourself call: the remote gets resultsSentElsewhere, the tail call the error" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    h.target.fail_inline = true;
    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    try std.testing.expectEqual(@as(?u32, tail_answer_id), h.target.answer_id);
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "TailHandlerFailed");
}

test "a generated DIRECT handler whose results build fails for a yourself call: the remote gets resultsSentElsewhere" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // Generated dispatch answers the build error itself, with
    // sendReturnException; the call is still a `yourself` call then.
    h.target.fail_inline_answered = true;
    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    try std.testing.expectEqual(@as(?u32, tail_answer_id), h.target.answer_id);
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "TailHandlerFailed");
}

test "the host answers a yourself call with an exception: the remote gets resultsSentElsewhere, the tail call the exception" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    try h.client_peer.sendReturnException(tail_answer_id, "target failed");
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try expectRedirectGetsException(&h, allocator, "target failed");
}

test "the host's exception for a yourself call after its redirect: only resultsSentElsewhere on the wire, nothing kept past the Finish" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // The C++ order: the call and the redirect, then the host's answer.
    try h.pushTailCall(allocator);
    try std.testing.expectEqual(protocol.ReturnTag.takeFromOtherQuestion, h.caller.tag.?);
    try h.client_peer.sendReturnException(tail_answer_id, "target failed");
    try expectOnlyResultsSentElsewhere(&h, tail_answer_id);
    try h.pushFinish(allocator, tail_answer_id);
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
}
