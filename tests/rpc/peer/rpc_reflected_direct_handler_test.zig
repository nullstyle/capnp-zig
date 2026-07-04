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
            .@"return" => event.id = (try decoded.asReturn()).answer_id,
            .resolve => event.id = (try decoded.asResolve()).promise_id,
            .disembargo => event.disembargo_context = (try decoded.asDisembargo()).context_tag,
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
