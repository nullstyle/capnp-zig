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
    /// When the client sends a Return for this answer id, deliver the
    /// remote's Finish of it to the client inside that send, as a
    /// synchronous transport can.
    reenter_finish_for: ?u32 = null,
    /// Fail the client's next send of a Return for this answer id, before
    /// it reaches the wire (a transport write or out-of-memory failure).
    fail_return_for: ?u32 = null,

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
        if (self.fail_return_for) |answer_id| {
            var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
            defer decoded.deinit();
            if (decoded.tag == .@"return" and (try decoded.asReturn()).answer_id == answer_id) {
                self.fail_return_for = null;
                return error.TestSendFailed;
            }
        }
        try self.record(.client_to_server, frame);
        if (self.reenter_finish_for) |answer_id| {
            const last = self.events.items[self.events.items.len - 1];
            if (last.tag == .@"return" and last.id == answer_id) {
                self.reenter_finish_for = null;
                var mb = protocol.MessageBuilder.init(self.allocator);
                defer mb.deinit();
                try mb.buildFinish(answer_id, false, false);
                const finish_frame = try mb.finish();
                defer self.allocator.free(finish_frame);
                if (self.client_peer) |peer| try peer.handleFrame(finish_frame);
            }
        }
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

    /// Frames with `tag` sent in `dir`.
    fn countTag(self: *const Wire, dir: EventDir, tag: protocol.MessageTag) usize {
        var n: usize = 0;
        for (self.events.items) |event| {
            if (event.dir == dir and event.tag == tag) n += 1;
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

    fn hasDisembargo(self: *const Wire, dir: EventDir, context: protocol.DisembargoContextTag) bool {
        for (self.events.items) |event| {
            if (event.dir != dir or event.tag != .disembargo) continue;
            if (event.disembargo_context == context) return true;
        }
        return false;
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
    try server_peer.resolvePromiseExportToImport(promise_export_id, target_import_id);

    // *** Primary bug: the DIRECT handler body must have run. ***
    try std.testing.expect(target.ran);
    try std.testing.expectEqual(@as(u32, 1), target.calls_seen);
    try std.testing.expect(target.call0_seq != null);

    // *** Level-1 completeness: the caller's pipelined question received the
    //     real result VALUE inline (n == 0), not a bare takeFromOtherQuestion. ***
    try std.testing.expect(call1.returned);
    try std.testing.expectEqual(protocol.ReturnTag.results, call1.return_tag.?);
    try std.testing.expectEqual(@as(u32, 0), call1.result_n.?);

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

// -- A tail call that arrives before the call it names is answered -----------
//
// The C++ reference sends `takeFromOtherQuestion` right after the
// `sendResultsTo = yourself` call it names. A callee whose handler answers
// later (a deferred handler, or a host such as the native C ABI) so receives
// the redirect first. The peer holds it: the caller's question gets the
// call's results, or its exception, when the handler answers, and the remote
// gets `resultsSentElsewhere` either way.

/// A server bootstrap that keeps every call unanswered.
const SilentServer = struct {
    fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
};

/// A client export whose handler answers later.
const DeferredTarget = struct {
    answer_id: ?u32 = null,
    /// Answer inside the call instead, as a generated DIRECT handler does,
    /// with a handler that fails while it builds its results. The error
    /// leaves `onCall`, and the Peer's dispatch turns it into
    /// `sendReturnException(question_id, @errorName(err))`.
    fail_inline: bool = false,

    fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *DeferredTarget = castCtx(*DeferredTarget, ctx_ptr);
        self.answer_id = call.question_id;
        if (self.fail_inline) try peer.sendReturnResults(call.question_id, self, FailingBuild.build);
    }
};

/// A results build that fails: the body of a generated DIRECT handler runs
/// inside `sendReturnResults`'s build, so a handler error surfaces here.
const FailingBuild = struct {
    fn build(_: *anyopaque, _: *protocol.ReturnBuilder) anyerror!void {
        return error.TailHandlerFailed;
    }
};

/// Answer `answer_id` as generated dispatch, or a native host whose
/// `capnp_return_results` failed, does: results whose build fails, then the
/// build error as an exception.
fn answerWithFailingBuild(peer: *Peer, answer_id: u32) !void {
    var unused: u8 = 0;
    peer.sendReturnResults(answer_id, &unused, FailingBuild.build) catch |err| {
        return peer.sendReturnException(answer_id, @errorName(err));
    };
    return error.TestExpectedBuildFailure;
}

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
    /// Send this export of the client as the call's params (the content is
    /// that capability), instead of an empty struct.
    param_cap: ?u32 = null,

    fn build(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const self: *const TailCaller = castCtx(*const TailCaller, ctx_ptr);
        var payload = try call.payloadTyped();
        var any = try payload.initContent();
        if (self.param_cap) |export_id| return any.setCapability(.{ .id = export_id });
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

/// The server's question id of the tail call (the client's answer id).
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
        self.* = .{
            .wire = .{ .allocator = allocator },
            .server_peer = Peer.initDetached(allocator),
            .client_peer = Peer.initDetached(allocator),
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

    /// The remote calls promised answer `promised` (one of the client's
    /// answers) with question `answer_id`: a call pipelined on that answer.
    /// With `yourself`, the results go back to the remote, as the C++
    /// reference sends a call it bounces back to a tail call's pipeline.
    fn pushPipelinedCall(self: *TailHarness, allocator: std.mem.Allocator, answer_id: u32, promised: u32, yourself: bool) !void {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        var call = try mb.beginCall(answer_id, CALL_INTERFACE_ID, CALL_METHOD_ID);
        try call.setTargetPromisedAnswer(promised);
        if (yourself) call.setSendResultsToYourself();
        var payload = try call.payloadTyped();
        _ = try (try payload.initContent()).initStruct(0, 0);
        const frame = try mb.finish();
        defer allocator.free(frame);
        try self.client_peer.handleFrame(frame);
    }

    /// The remote calls the client's export `target_export` with question
    /// `answer_id`; the results go to the remote (`sendResultsTo = caller`).
    /// The params carry the remote's export `param_export` as a
    /// `senderHosted` capability: an import reference the client takes.
    fn pushPlainCall(self: *TailHarness, allocator: std.mem.Allocator, answer_id: u32, target_export: u32, param_export: u32) !void {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        var call = try mb.beginCall(answer_id, CALL_INTERFACE_ID, CALL_METHOD_ID);
        try call.setTargetImportedCap(target_export);
        var payload = try call.payloadTyped();
        try (try payload.initContent()).setCapability(.{ .id = 0 });
        var cap_list = try call.initCapTableTyped(1);
        protocol.CapDescriptor.writeSenderHosted(try cap_list.get(0), param_export);
        const frame = try mb.finish();
        defer allocator.free(frame);
        try self.client_peer.handleFrame(frame);
    }

    /// The remote releases `count` references to the client's export `id`.
    fn pushRelease(self: *TailHarness, allocator: std.mem.Allocator, id: u32, count: u32) !void {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        try mb.buildRelease(id, count);
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
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        try mb.buildFinish(id, false, false);
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

test "a tail call that arrives before its call is answered gets that call's results" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushTailCall(allocator);
    try std.testing.expectEqual(@as(?u32, tail_answer_id), h.target.answer_id);
    // Held: the question has no Return yet, not even the bare redirect.
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);
    try std.testing.expect(h.client_peer.questions.contains(h.question_id));

    var result = ResultN{ .n = 7 };
    try h.client_peer.sendReturnResults(tail_answer_id, &result, ResultN.build);

    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.results, h.caller.tag.?);
    try std.testing.expectEqual(@as(u32, 7), h.caller.result_n.?);
    try std.testing.expect(!h.client_peer.questions.contains(h.question_id));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));
    // rpc.capnp: the question still sends its Finish, after which the remote
    // finishes the tail call. The C++ reference waits for this Finish.
    try std.testing.expectEqual(@as(usize, 1), h.wire.countFinishes(.client_to_server, h.question_id));
}

test "a tail call held on a call that fails gets that call's exception; the remote gets resultsSentElsewhere" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushTailCall(allocator);
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);

    try h.client_peer.sendReturnException(tail_answer_id, "target failed");

    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try std.testing.expectEqualStrings("target failed", h.caller.reason());
    // rpc.capnp: a call whose sendResultsTo is not `caller` is answered with
    // resultsSentElsewhere, also when it fails (the C++ reference rejects an
    // exception Return for its tail call).
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));
}

test "a tail call held on a call whose results build fails gets the build error; the remote gets resultsSentElsewhere" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushTailCall(allocator);
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);

    try answerWithFailingBuild(&h.client_peer, tail_answer_id);

    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try std.testing.expectEqualStrings("TailHandlerFailed", h.caller.reason());
    try std.testing.expect(!h.client_peer.questions.contains(h.question_id));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));
}

test "a DIRECT handler that fails a yourself call before its tail call arrives: the tail call gets the error" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // The handler fails inside the call, so the call is answered before the
    // redirect arrives.
    h.target.fail_inline = true;
    try h.pushTailCall(allocator);

    try std.testing.expectEqual(@as(?u32, tail_answer_id), h.target.answer_id);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try std.testing.expectEqualStrings("TailHandlerFailed", h.caller.reason());
    try std.testing.expect(!h.client_peer.questions.contains(h.question_id));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));
}

test "a held tail call whose call is answered by a prebuilt frame gets the bare redirect at once" {
    // sendPrebuiltReturnFrame puts its frame on the wire as built, so the
    // results never come back to this peer. The remote finishes the call
    // only after this question's Finish (rpc.capnp `Call.sendResultsTo`), so
    // the question must not wait for that Finish: it gets the bare
    // redirect, as before holds existed.
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushTailCall(allocator);
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);
    {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        var ret = try mb.beginReturn(tail_answer_id, .results);
        var payload = try ret.payloadTyped();
        _ = try (try payload.initContent()).initStruct(1, 0);
        const frame = try mb.finish();
        defer allocator.free(frame);
        var decoded = try protocol.DecodedMessage.init(allocator, frame);
        defer decoded.deinit();
        try h.client_peer.sendPrebuiltReturnFrame(try decoded.asReturn(), frame);
    }
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.takeFromOtherQuestion, h.caller.tag.?);
    try std.testing.expect(!h.client_peer.questions.contains(h.question_id));
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.held_tail_calls.count());

    // Spec order: the remote finishes the call after our Finish of the
    // question. It changes nothing for the question.
    try h.pushFinish(allocator, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
}

test "a second Return for a question whose tail call is held is refused" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushTailCall(allocator);
    var mb = protocol.MessageBuilder.init(allocator);
    defer mb.deinit();
    var ret = try mb.beginReturn(h.question_id, .results);
    var payload = try ret.payloadTyped();
    _ = try (try payload.initContent()).initStruct(1, 0);
    const frame = try mb.finish();
    defer allocator.free(frame);
    try std.testing.expectError(error.UnknownQuestion, h.client_peer.handleFrame(frame));
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);
}

// -- A stashed answer never reaches a later tail call --------------------------
//
// The remote reuses a question id only after its Finish, and the C++
// reference reuses the lowest free id first. A Return kept for an earlier
// call with that id must not answer a tail call that names the new one.

test "a question cancelled before its redirect arrives absorbs the tail call's answer; nothing is stashed" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // The host cancels the question while the remote's tail call for it is
    // already on the way.
    try h.client_peer.cancelQuestion(h.question_id, "host cancel");
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try h.pushTailCall(allocator);

    var first = ResultN{ .n = 7 };
    try h.client_peer.sendReturnResults(tail_answer_id, &first, ResultN.build);
    // The cancelled question took the answer and retired: nothing waits for
    // a redirect that has already come.
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
    try std.testing.expect(!h.client_peer.questions.contains(h.question_id));
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));
    // Spec order: the remote finishes the call after our Finish of the
    // question.
    try h.pushFinish(allocator, tail_answer_id);

    // The remote reuses the id for its next tail call.
    var caller2 = TailCaller{};
    const q2 = try h.nextQuestion(&caller2);
    h.target.answer_id = null;
    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    try h.pushRedirect(allocator, q2, tail_answer_id);
    try std.testing.expectEqual(@as(?u32, tail_answer_id), h.target.answer_id);
    try std.testing.expectEqual(@as(u32, 0), caller2.returns);

    var second = ResultN{ .n = 8 };
    try h.client_peer.sendReturnResults(tail_answer_id, &second, ResultN.build);
    try std.testing.expectEqual(@as(u32, 1), caller2.returns);
    try std.testing.expectEqual(protocol.ReturnTag.results, caller2.tag.?);
    try std.testing.expectEqual(@as(u32, 8), caller2.result_n.?);
}

test "a stashed answer whose redirect never came is dropped when the remote reuses its id" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    // rpc.capnp lets the remote skip the redirect: it sends a `yourself`
    // call, then only its Finish. The answer is stashed, and the stash
    // outlives that Finish by design.
    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    var first = ResultN{ .n = 7 };
    try h.client_peer.sendReturnResults(tail_answer_id, &first, ResultN.build);
    try h.pushFinish(allocator, tail_answer_id);
    try std.testing.expectEqual(@as(usize, 1), h.client_peer.loopback_result_stash.count());

    // A new call with that id makes the entry stale.
    h.target.answer_id = null;
    try h.pushTailCall(allocator);
    try std.testing.expectEqual(@as(?u32, tail_answer_id), h.target.answer_id);
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
    // The new call is unanswered: the question waits for it.
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);

    var second = ResultN{ .n = 8 };
    try h.client_peer.sendReturnResults(tail_answer_id, &second, ResultN.build);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.results, h.caller.tag.?);
    try std.testing.expectEqual(@as(u32, 8), h.caller.result_n.?);
}

test "a stashed exception whose redirect never came is dropped when the remote reuses its id" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    try h.client_peer.sendReturnException(tail_answer_id, "old call failed");
    try h.pushFinish(allocator, tail_answer_id);
    try std.testing.expectEqual(@as(usize, 1), h.client_peer.loopback_result_stash.count());

    try h.pushTailCall(allocator);
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);

    var second = ResultN{ .n = 8 };
    try h.client_peer.sendReturnResults(tail_answer_id, &second, ResultN.build);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.results, h.caller.tag.?);
    try std.testing.expectEqual(@as(u32, 8), h.caller.result_n.?);
}

// -- A yourself call that fails or waits before its handler runs ---------------
//
// rpc.capnp answers a `sendResultsTo = yourself` call with
// `resultsSentElsewhere` in every case, and the C++ reference drops the
// connection on an exception Return for its tail call. So a call that never
// reaches its handler (an unknown or broken target), or waits on one of our
// promise exports first, is answered the same way: the remote gets
// `resultsSentElsewhere`, and its tail call gets the exception or results.

test "a yourself call to a rejected promise export: the remote gets resultsSentElsewhere, the tail call the rejection" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const promise_id = try h.client_peer.addPromiseExport();
    try h.client_peer.resolvePromiseExportToException(promise_id, "broken by host");
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));

    try h.pushRedirect(allocator, h.question_id, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try std.testing.expectEqualStrings("promise broken", h.caller.reason());
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
}

test "a yourself call to an unknown capability: the remote gets resultsSentElsewhere, the tail call the exception" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushYourselfCall(allocator, tail_answer_id, 0x7fff_0000);
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));

    try h.pushRedirect(allocator, h.question_id, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try std.testing.expectEqualStrings("unknown capability", h.caller.reason());
}

test "a tail call that names a call parked on a promise export waits; the host's rejection reaches it" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const promise_id = try h.client_peer.addPromiseExport();
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    try h.pushRedirect(allocator, h.question_id, tail_answer_id);
    // The call waits on the promise, and the redirect waits on the call.
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);
    try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, tail_answer_id, null));

    try h.client_peer.resolvePromiseExportToException(promise_id, "rejected by host");
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try std.testing.expectEqualStrings("promise broken", h.caller.reason());
    try std.testing.expect(!h.client_peer.questions.contains(h.question_id));
}

test "a tail call that names a call parked on a promise export waits; the call's answer after the resolve reaches it" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const promise_id = try h.client_peer.addPromiseExport();
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    try h.pushRedirect(allocator, h.question_id, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);

    // The promise resolves to the client's export: the parked call runs.
    try h.client_peer.resolvePromiseExportToExport(promise_id, h.target_export_id);
    try std.testing.expectEqual(@as(?u32, tail_answer_id), h.target.answer_id);
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);

    var result = ResultN{ .n = 7 };
    try h.client_peer.sendReturnResults(tail_answer_id, &result, ResultN.build);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.results, h.caller.tag.?);
    try std.testing.expectEqual(@as(u32, 7), h.caller.result_n.?);
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));
}

// -- A held tail call whose call this peer forwards back to the remote ---------
//
// A `yourself` call parked on one of our promise exports is forwarded back
// to the remote when that promise resolves to a capability the remote
// hosts. The call is then answered with the remote's own
// `resultsSentElsewhere`, and its results never come back here.

/// The question id of the last Call the client sent.
fn lastClientCallId(wire: *const Wire) !u32 {
    var i = wire.events.items.len;
    while (i > 0) {
        i -= 1;
        const event = wire.events.items[i];
        if (event.dir == .client_to_server and event.tag == .call) return event.id;
    }
    return error.NoCall;
}

/// Park a `yourself` call on a client promise export, hold the redirect that
/// names it, then resolve the promise to the server's bootstrap: the client
/// forwards the call to the server. Returns the forwarded question id.
fn forwardParkedTailCall(h: *TailHarness, allocator: std.mem.Allocator) !u32 {
    const promise_id = try h.client_peer.addPromiseExport();
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    try h.pushRedirect(allocator, h.question_id, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);
    const calls_before = h.wire.events.items.len;
    try h.client_peer.resolvePromiseExportToImport(promise_id, h.server_import_id);
    const forwarded = try lastClientCallId(&h.wire);
    try std.testing.expect(h.wire.events.items.len > calls_before);
    try std.testing.expect(forwarded != h.question_id);
    return forwarded;
}

fn pushServerReturn(h: *TailHarness, allocator: std.mem.Allocator, question_id: u32, tag: protocol.ReturnTag, reason: []const u8) !void {
    var mb = protocol.MessageBuilder.init(allocator);
    defer mb.deinit();
    var ret = try mb.beginReturn(question_id, tag);
    ret.setReleaseParamCaps(false);
    if (tag == .exception) try ret.setException(reason);
    const frame = try mb.finish();
    defer allocator.free(frame);
    try h.client_peer.handleFrame(frame);
}

test "a held tail call whose call is forwarded back to the remote gets the bare redirect when the forward returns" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const forwarded = try forwardParkedTailCall(&h, allocator);
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);

    try pushServerReturn(&h, allocator, forwarded, .resultsSentElsewhere, "");
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.takeFromOtherQuestion, h.caller.tag.?);
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.held_tail_calls.count());
}

test "a held tail call whose forwarded call fails gets the exception; the remote gets resultsSentElsewhere" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const forwarded = try forwardParkedTailCall(&h, allocator);
    try pushServerReturn(&h, allocator, forwarded, .exception, "forwarded call failed");
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try std.testing.expectEqualStrings("forwarded call failed", h.caller.reason());
}

test "a held tail call whose parked call the remote finishes ends with tail_call_finished_reason" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const promise_id = try h.client_peer.addPromiseExport();
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    try h.pushRedirect(allocator, h.question_id, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);

    // The Finish cancels the parked call (Return `canceled`); the held
    // question then ends as for any call finished before its answer.
    try h.pushFinish(allocator, tail_answer_id);
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .canceled));
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try std.testing.expectEqualStrings(peer_impl.tail_call_finished_reason, h.caller.reason());
}

/// A results build that first receives the remote's Finish of its own call,
/// as a DIRECT handler body that waits on the connection can.
const FinishInsideBuild = struct {
    h: *TailHarness,
    allocator: std.mem.Allocator,

    fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        const self: *FinishInsideBuild = castCtx(*FinishInsideBuild, ctx_ptr);
        try self.h.pushFinish(self.allocator, tail_answer_id);
        var result = ResultN{ .n = 7 };
        try ResultN.build(&result, ret);
    }
};

test "a Finish during a held tail call's results build ends the question; the results are not kept" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushTailCall(allocator);
    var build_ctx = FinishInsideBuild{ .h = &h, .allocator = allocator };
    try h.client_peer.sendReturnResults(tail_answer_id, &build_ctx, FinishInsideBuild.build);

    // The Finish ended the held question; the redirect is used up, so the
    // results go nowhere and are not kept for a redirect that has come.
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try std.testing.expectEqualStrings(peer_impl.tail_call_finished_reason, h.caller.reason());
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
    // The Finish came first: no record keeps the id in use.
    try std.testing.expect(!h.client_peer.failed_answers.contains(tail_answer_id));
    // The call still gets its one Return.
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));
}

// -- Ordering edges of a held tail call ----------------------------------------

test "a remote Finish inside the resultsSentElsewhere send does not take the held question's results" {
    // A synchronous transport can deliver the remote's Finish of the call
    // inside our resultsSentElsewhere send. The hold is taken before that
    // send, so the Finish finds none, and the question gets the results.
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushTailCall(allocator);
    h.wire.reenter_finish_for = tail_answer_id;
    var result = ResultN{ .n = 5 };
    try h.client_peer.sendReturnResults(tail_answer_id, &result, ResultN.build);
    try std.testing.expectEqual(@as(?u32, null), h.wire.reenter_finish_for);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.results, h.caller.tag.?);
    try std.testing.expectEqual(@as(u32, 5), h.caller.result_n.?);
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
}

test "a remote Finish inside the send of a held tail call's exception does not replace the exception" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushTailCall(allocator);
    h.wire.reenter_finish_for = tail_answer_id;
    try h.client_peer.sendReturnException(tail_answer_id, "target failed");
    try std.testing.expectEqual(@as(?u32, null), h.wire.reenter_finish_for);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try std.testing.expectEqualStrings("target failed", h.caller.reason());
}

test "Peer.deinit with a held tail call gives the question one Return and leaks nothing" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    try h.pushTailCall(allocator);
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);
    h.deinit();
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
}

// -- A resultsSentElsewhere send that fails --------------------------------------
//
// The call's answer is decided before the send: a held question already got
// it, or it is kept for the redirect. When the send then fails, the caller
// answers again with an exception (the generated dispatch does; a native
// host answers a failed capnp_return_results with capnp_return_exception).
// That second answer must still put `resultsSentElsewhere` on the wire,
// never the exception Return the C++ reference rejects for its tail call.

test "a failed resultsSentElsewhere send for a held tail call: the follow-up exception still sends resultsSentElsewhere" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushTailCall(allocator);
    h.wire.fail_return_for = tail_answer_id;
    var result = ResultN{ .n = 7 };
    try std.testing.expectError(error.TestSendFailed, h.client_peer.sendReturnResults(tail_answer_id, &result, ResultN.build));
    // The question got the results; the remote got nothing yet.
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(@as(u32, 7), h.caller.result_n.?);
    try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, tail_answer_id, null));

    try h.client_peer.sendReturnException(tail_answer_id, "results were not sent");
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
}

test "a failed resultsSentElsewhere send before the redirect: the follow-up exception reaches the redirect" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    h.wire.fail_return_for = tail_answer_id;
    var result = ResultN{ .n = 7 };
    try std.testing.expectError(error.TestSendFailed, h.client_peer.sendReturnResults(tail_answer_id, &result, ResultN.build));
    try h.client_peer.sendReturnException(tail_answer_id, "results were not sent");
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));

    // The caller's last answer for the call is the exception.
    try h.pushRedirect(allocator, h.question_id, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try std.testing.expectEqualStrings("results were not sent", h.caller.reason());
}

// -- A call parked on a promise export that the remote releases -----------------
//
// A call parked on one of our promise exports waits for that promise. When
// the remote releases its last reference to the export before it resolves,
// the export is destroyed and the host can no longer resolve it. The C++
// reference does this for a natural tail call: `return
// context.tailCall(p.fooRequest())` on a promise export the host passed it
// as a param sends the `yourself` call on the promise, the redirect, and
// then a Release of the promise as the call context drops its params. The
// parked call then gets an exception Return, as every call must get one
// Return. For a `yourself` call that is `resultsSentElsewhere` on the wire,
// and the exception goes to the question whose redirect names the call.

/// The client sends question X with its promise export `promise_id` as the
/// params, so the remote holds one reference to it. Returns X.
fn sendPromiseInParams(h: *TailHarness, caller: *TailCaller, promise_id: u32) !u32 {
    caller.param_cap = promise_id;
    const qx = try h.nextQuestion(caller);
    try std.testing.expectEqual(@as(u32, 1), h.client_peer.exports.getPtr(promise_id).?.ref_count);
    return qx;
}

test "a held tail call whose parked call loses its promise export to a Release gets an exception" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const promise_id = try h.client_peer.addPromiseExport();
    var caller = TailCaller{};
    const qx = try sendPromiseInParams(&h, &caller, promise_id);
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    try h.pushRedirect(allocator, qx, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 0), caller.returns);
    try std.testing.expectEqual(@as(usize, 1), h.client_peer.held_tail_calls.count());

    try h.pushRelease(allocator, promise_id, 1);
    try std.testing.expect(!h.client_peer.exports.contains(promise_id));
    try std.testing.expectEqual(@as(u32, 1), caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, caller.tag.?);
    try std.testing.expectEqualStrings(peer_impl.promise_export_released_reason, caller.reason());
    // The call gets its one Return, resultsSentElsewhere, and nothing of it
    // stays behind.
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));
    try std.testing.expect(!h.client_peer.send_results_to_yourself.contains(tail_answer_id));
    try std.testing.expect(!h.client_peer.active_inbound_questions.contains(tail_answer_id));
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.held_tail_calls.count());
    // The question sends its Finish; a compliant remote finishes the tail
    // call only after it.
    try std.testing.expectEqual(@as(usize, 1), h.wire.countFinishes(.client_to_server, qx));
}

test "a yourself call parked on a promise export the remote releases is answered at once; its later redirect gets the exception" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const promise_id = try h.client_peer.addPromiseExport();
    var caller = TailCaller{};
    const qx = try sendPromiseInParams(&h, &caller, promise_id);
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    try h.pushRelease(allocator, promise_id, 1);
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));

    try h.pushRedirect(allocator, qx, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 1), caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, caller.tag.?);
    try std.testing.expectEqualStrings(peer_impl.promise_export_released_reason, caller.reason());
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.loopback_result_stash.count());
}

test "a plain call parked on a promise export the remote releases gets one exception Return" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const promise_id = try h.client_peer.addPromiseExport();
    var caller = TailCaller{};
    _ = try sendPromiseInParams(&h, &caller, promise_id);
    // A call whose results go to the remote, parked on the promise, and a
    // call pipelined on that call's answer.
    const remote_export_id: u32 = 77;
    try h.pushPlainCall(allocator, 901, promise_id, remote_export_id);
    try h.pushPipelinedCall(allocator, 902, 901, false);
    try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, 901, null));
    try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, 902, null));
    try std.testing.expect(h.client_peer.caps.hasImport(remote_export_id));
    const releases_before = h.wire.countTag(.client_to_server, .release);

    try h.pushRelease(allocator, promise_id, 1);
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 901, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 901, .exception));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 902, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, 902, .exception));
    try std.testing.expect(!h.client_peer.active_inbound_questions.contains(901));
    try std.testing.expect(!h.client_peer.active_inbound_questions.contains(902));
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.pending_promises.count());
    // The call's param capability is released, not dropped.
    try std.testing.expectEqual(releases_before + 1, h.wire.countTag(.client_to_server, .release));
    try std.testing.expect(!h.client_peer.caps.hasImport(remote_export_id));
}

// -- A call pipelined on a tail call's answer -----------------------------------
//
// The C++ reference sends every call it bounces back over the same
// connection as a tail call (`RpcClient::callNoIntercept` makes a
// `directTailCall`). So a call our app pipelined on question X, which the
// remote answered by tail-calling us (call Y), arrives as Call(W,
// sendResultsTo = yourself, target = promisedAnswer(Y)) plus Return(W0,
// takeFromOtherQuestion = W). Y's results go to the remote's question, not
// to a pipeline here, so W fails with `tail_call_pipelining_reason`: at Y's
// answer when W was queued first, at once when it comes after. W0 then gets
// that exception, and the remote gets `resultsSentElsewhere` for W.

const pipelined_answer_id: u32 = 901;

test "a tail call pipelined on a held yourself call fails when that call is answered" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushTailCall(allocator);
    var w0 = TailCaller{};
    const w0_id = try h.nextQuestion(&w0);
    try h.pushPipelinedCall(allocator, pipelined_answer_id, tail_answer_id, true);
    try h.pushRedirect(allocator, w0_id, pipelined_answer_id);
    try std.testing.expectEqual(@as(u32, 0), w0.returns);

    var result = ResultN{ .n = 7 };
    try h.client_peer.sendReturnResults(tail_answer_id, &result, ResultN.build);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.results, h.caller.tag.?);
    try std.testing.expectEqual(@as(u32, 7), h.caller.result_n.?);
    try std.testing.expectEqual(@as(u32, 1), w0.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, w0.tag.?);
    try std.testing.expectEqualStrings(peer_impl.tail_call_pipelining_reason, w0.reason());
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, pipelined_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, pipelined_answer_id, .resultsSentElsewhere));
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.pending_promises.count());
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.held_tail_calls.count());
}

test "a tail call pipelined on an answered yourself call fails at once" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushTailCall(allocator);
    var result = ResultN{ .n = 7 };
    try h.client_peer.sendReturnResults(tail_answer_id, &result, ResultN.build);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);

    var w0 = TailCaller{};
    const w0_id = try h.nextQuestion(&w0);
    try h.pushPipelinedCall(allocator, pipelined_answer_id, tail_answer_id, true);
    // Answered at once, not queued for results that never come here.
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.pending_promises.count());
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, pipelined_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, pipelined_answer_id, .resultsSentElsewhere));

    try h.pushRedirect(allocator, w0_id, pipelined_answer_id);
    try std.testing.expectEqual(@as(u32, 1), w0.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, w0.tag.?);
    try std.testing.expectEqualStrings(peer_impl.tail_call_pipelining_reason, w0.reason());

    // The record lives until the remote's Finish of the tail call, which
    // frees the id for reuse.
    try h.pushFinish(allocator, tail_answer_id);
    try std.testing.expect(!h.client_peer.failed_answers.contains(tail_answer_id));
    h.target.answer_id = null;
    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    try std.testing.expectEqual(@as(?u32, tail_answer_id), h.target.answer_id);
}

test "a call pipelined on a failed yourself call gets that call's exception" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushTailCall(allocator);
    try h.client_peer.sendReturnException(tail_answer_id, "target failed");
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);

    var w0 = TailCaller{};
    const w0_id = try h.nextQuestion(&w0);
    try h.pushPipelinedCall(allocator, pipelined_answer_id, tail_answer_id, true);
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.pending_promises.count());
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, pipelined_answer_id, .resultsSentElsewhere));
    try h.pushRedirect(allocator, w0_id, pipelined_answer_id);
    try std.testing.expectEqual(@as(u32, 1), w0.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, w0.tag.?);
    try std.testing.expectEqualStrings("target failed", w0.reason());
}

test "a remote Finish inside a yourself call's resultsSentElsewhere send leaves no record of the answer" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushTailCall(allocator);
    h.wire.reenter_finish_for = tail_answer_id;
    var result = ResultN{ .n = 5 };
    try h.client_peer.sendReturnResults(tail_answer_id, &result, ResultN.build);
    try std.testing.expectEqual(@as(?u32, null), h.wire.reenter_finish_for);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    // The Finish came: nothing may keep the id in use.
    try std.testing.expect(!h.client_peer.failed_answers.contains(tail_answer_id));
    h.target.answer_id = null;
    try h.pushYourselfCall(allocator, tail_answer_id, h.target_export_id);
    try std.testing.expectEqual(@as(?u32, tail_answer_id), h.target.answer_id);
}

test "a call that passes an answered yourself call's answer as a param capability fails" {
    // A `receiverAnswer` param names one of our answers. A tail-called
    // answer has no results here to resolve it from, so the call fails
    // with the record's reason, as a call naming a failed answer does.
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    try h.pushTailCall(allocator);
    var result = ResultN{ .n = 7 };
    try h.client_peer.sendReturnResults(tail_answer_id, &result, ResultN.build);
    h.target.answer_id = null;
    {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        var call = try mb.beginCall(pipelined_answer_id, CALL_INTERFACE_ID, CALL_METHOD_ID);
        try call.setTargetImportedCap(h.target_export_id);
        var payload = try call.payloadTyped();
        try (try payload.initContent()).setCapability(.{ .id = 0 });
        var cap_list = try call.initCapTableTyped(1);
        try protocol.CapDescriptor.writeReceiverAnswer((try cap_list.get(0))._builder, tail_answer_id, &.{});
        const frame = try mb.finish();
        defer allocator.free(frame);
        try h.client_peer.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(?u32, null), h.target.answer_id);
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, pipelined_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, pipelined_answer_id, .exception));
}

test "a tail call pipelined on a yourself call forwarded back to the remote fails when the forward returns" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const forwarded = try forwardParkedTailCall(&h, allocator);
    var w0 = TailCaller{};
    const w0_id = try h.nextQuestion(&w0);
    try h.pushPipelinedCall(allocator, pipelined_answer_id, tail_answer_id, true);
    try h.pushRedirect(allocator, w0_id, pipelined_answer_id);
    try std.testing.expectEqual(@as(u32, 0), w0.returns);

    // The forwarded call's results go to the remote: none come back here.
    try pushServerReturn(&h, allocator, forwarded, .resultsSentElsewhere, "");
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.takeFromOtherQuestion, h.caller.tag.?);
    try std.testing.expectEqual(@as(u32, 1), w0.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, w0.tag.?);
    try std.testing.expectEqualStrings(peer_impl.tail_call_pipelining_reason, w0.reason());
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, pipelined_answer_id, .resultsSentElsewhere));
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.pending_promises.count());

    // A call pipelined after that answer fails at once.
    var w1 = TailCaller{};
    const w1_id = try h.nextQuestion(&w1);
    try h.pushPipelinedCall(allocator, pipelined_answer_id + 1, tail_answer_id, true);
    try h.pushRedirect(allocator, w1_id, pipelined_answer_id + 1);
    try std.testing.expectEqual(@as(u32, 1), w1.returns);
    try std.testing.expectEqualStrings(peer_impl.tail_call_pipelining_reason, w1.reason());
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.pending_promises.count());
}

test "a tail call pipelined on a forwarded yourself call that the remote redirects elsewhere fails" {
    // The remote answers the forwarded call with a tail call onto another
    // call this peer forwarded: the peer relays it as takeFromOtherQuestion,
    // and the call's results again never come back here.
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    const other_answer_id: u32 = 950;
    const other_promise = try h.client_peer.addPromiseExport();
    try h.pushYourselfCall(allocator, other_answer_id, other_promise);
    try h.client_peer.resolvePromiseExportToImport(other_promise, h.server_import_id);
    const other_forwarded = try lastClientCallId(&h.wire);

    const forwarded = try forwardParkedTailCall(&h, allocator);
    var w0 = TailCaller{};
    const w0_id = try h.nextQuestion(&w0);
    try h.pushPipelinedCall(allocator, pipelined_answer_id, tail_answer_id, true);
    try h.pushRedirect(allocator, w0_id, pipelined_answer_id);
    try std.testing.expectEqual(@as(u32, 0), w0.returns);

    {
        var mb = protocol.MessageBuilder.init(allocator);
        defer mb.deinit();
        var ret = try mb.beginReturn(forwarded, .takeFromOtherQuestion);
        ret.setReleaseParamCaps(false);
        try ret.setTakeFromOtherQuestion(other_forwarded);
        const frame = try mb.finish();
        defer allocator.free(frame);
        try h.client_peer.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .takeFromOtherQuestion));
    try std.testing.expectEqual(@as(u32, 1), w0.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, w0.tag.?);
    try std.testing.expectEqualStrings(peer_impl.tail_call_pipelining_reason, w0.reason());
    try std.testing.expectEqual(@as(usize, 0), h.client_peer.pending_promises.count());
}

// -- A parked call whose handler fails when it replays ---------------------------
//
// A call parked on a promise export runs when the promise resolves. Its
// handler's error is answered like a fresh call's (the resolved-export
// dispatch sends it as an exception), so a tail call held on it gets the
// error. Only an allocation failure in the replay drain itself can drop a
// Return there (docs/native-abi.md).

test "a held tail call whose parked call's handler fails on replay gets the error" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    h.target.fail_inline = true;
    const promise_id = try h.client_peer.addPromiseExport();
    try h.pushYourselfCall(allocator, tail_answer_id, promise_id);
    try h.pushRedirect(allocator, h.question_id, tail_answer_id);
    try std.testing.expectEqual(@as(u32, 0), h.caller.returns);

    try h.client_peer.resolvePromiseExportToExport(promise_id, h.target_export_id);
    try std.testing.expectEqual(@as(?u32, tail_answer_id), h.target.answer_id);
    try std.testing.expectEqual(@as(u32, 1), h.caller.returns);
    try std.testing.expectEqual(protocol.ReturnTag.exception, h.caller.tag.?);
    try std.testing.expectEqualStrings("TailHandlerFailed", h.caller.reason());
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, tail_answer_id, .resultsSentElsewhere));
    try std.testing.expect(!h.client_peer.active_inbound_questions.contains(tail_answer_id));
}

test "a plain parked call whose handler fails on replay gets one exception Return" {
    const allocator = std.testing.allocator;
    var h: TailHarness = undefined;
    try h.start(allocator);
    defer h.deinit();

    h.target.fail_inline = true;
    const promise_id = try h.client_peer.addPromiseExport();
    try h.pushPlainCall(allocator, pipelined_answer_id, promise_id, 77);
    try std.testing.expectEqual(@as(usize, 0), h.wire.countReturns(.client_to_server, pipelined_answer_id, null));

    try h.client_peer.resolvePromiseExportToExport(promise_id, h.target_export_id);
    try std.testing.expectEqual(@as(?u32, pipelined_answer_id), h.target.answer_id);
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, pipelined_answer_id, null));
    try std.testing.expectEqual(@as(usize, 1), h.wire.countReturns(.client_to_server, pipelined_answer_id, .exception));
    try std.testing.expect(!h.client_peer.active_inbound_questions.contains(pipelined_answer_id));
}
