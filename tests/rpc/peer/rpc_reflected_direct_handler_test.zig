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
