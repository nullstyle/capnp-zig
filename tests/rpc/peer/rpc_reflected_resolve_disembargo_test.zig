//! Proof test for `Peer.resolvePromiseExportToImport` — the server-side
//! origination of the Cap'n Proto reflected-capability resolve/embargo cycle.
//!
//! Two real detached peers are cross-wired over a synchronous in-process
//! loopback (`Wire`): every frame a peer emits is recorded and then handed
//! straight to the other peer's `handleFrame`, so both peers run their full
//! inbound dispatch on real bytes in both directions (this is the same harness
//! `rpc_typed_pipelining_test.zig` uses for end-to-end pipelining).
//!
//! Scenario (Zig-as-server B reflects a client-hosted cap; Zig-as-client A
//! drives the embargo):
//!
//!   1. A bootstraps B's `ReflectorService`.
//!   2. A calls `reflect(echoer = <A's own Echoer export>)`. Cap-in-params:
//!      A exports its Echoer, B imports it. B's handler RETAINS the import and
//!      returns `promise = <senderPromise export P>` (unresolved).
//!   3. A imports P as a promise and pipelines `promise.ping(n=1)`, which parks
//!      on P at B (no resolution yet).
//!   4. B calls `resolvePromiseExportToImport(P, <imported echoer id>)`:
//!      - sends `Resolve(P -> receiverHosted{echoer})` to A, and
//!      - replays the parked ping by FORWARDING it back out to A's echoer.
//!   5. A receives the Resolve, sees its promise resolved to one of its OWN
//!      exports (reached by a different path than the promise) and sends
//!      `Disembargo(senderLoopback)`; B echoes `receiverLoopback`; A clears the
//!      embargo.
//!   6. The forwarded ping(1) reaches A's echoer and returns 1.
//!   7. After the embargo clears, A sends a direct ping(2) on the resolved cap;
//!      it returns 2. Ordering: ping(1) callback fires before ping(2)'s.
//!
//! Everything runs under `std.testing.allocator`, so any refcount leak fails
//! the test; both peers deinit clean.

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
    /// question_id / answer_id for bootstrap, call, return, finish, resolve; 0
    /// otherwise.
    id: u32 = 0,
    /// For disembargo frames: the context discriminant and embargo id.
    disembargo_context: ?protocol.DisembargoContextTag = null,
    disembargo_embargo_id: ?u32 = null,
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
            .bootstrap => event.id = (try decoded.asBootstrap()).question_id,
            .call => event.id = (try decoded.asCall()).question_id,
            .@"return" => event.id = (try decoded.asReturn()).answer_id,
            .finish => event.id = (try decoded.asFinish()).question_id,
            .resolve => event.id = (try decoded.asResolve()).promise_id,
            .disembargo => {
                const dis = try decoded.asDisembargo();
                event.disembargo_context = dis.context_tag;
                event.disembargo_embargo_id = dis.embargo_id;
            },
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

    fn count(self: *const Wire, dir: EventDir, tag: protocol.MessageTag) usize {
        var n: usize = 0;
        for (self.events.items) |event| {
            if (event.dir == dir and event.tag == tag) n += 1;
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

// -- Ping wire shape ----------------------------------------------------------
//
// Both params and results are a struct with one data word: `n :UInt32` at byte
// offset 0. Kept deliberately minimal so the test needs no generated schema.

const PING_INTERFACE_ID: u64 = 0xE0E0_E0E0_E0E0_E001;
const PING_METHOD_ID: u16 = 0;

fn readPingN(payload: protocol.Payload) !u32 {
    const content = try payload.content.getStruct();
    return content.readU32(0);
}

const PingReturnCtx = struct {
    n: u32,
    fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        const self: *const PingReturnCtx = castCtx(*const PingReturnCtx, ctx_ptr);
        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        const results = try any.initStruct(1, 0);
        results.writeU32(0, self.n);
    }
};

// -- Client A: hosts an Echoer whose ping(n) returns n -----------------------

const EchoerServer = struct {
    ping_calls: u32 = 0,
    /// Monotonic stamp incremented per received ping, so pipelined-before-direct
    /// ordering is observable. `pingN_seq` records the stamp at which the echoer
    /// saw a ping carrying `n == N` (the source of truth that the call reached
    /// its target with the right payload).
    seq: u32 = 0,
    ping1_seq: ?u32 = null,
    ping2_seq: ?u32 = null,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *EchoerServer = castCtx(*EchoerServer, ctx_ptr);
        if (call.interface_id != PING_INTERFACE_ID or call.method_id != PING_METHOD_ID) {
            return error.UnexpectedMethod;
        }
        self.ping_calls += 1;
        self.seq += 1;
        const n = try readPingN(call.params);
        if (n == 1) self.ping1_seq = self.seq;
        if (n == 2) self.ping2_seq = self.seq;
        // Echo n straight back in the results.
        var ret_ctx = PingReturnCtx{ .n = n };
        try peer.sendReturnResults(call.question_id, &ret_ctx, PingReturnCtx.build);
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
        // The bootstrap capability arrives as the payload content pointer (a
        // capability pointer at cap-table index 0). Retain it so the peer does
        // not auto-release the import when this callback returns; the test
        // releases it explicitly during teardown.
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

// -- Client A: reflect call — one ctx drives both the params build and the
//    return observer (sendCall shares a single ctx across build + on_return).

const ReflectCall = struct {
    echoer_export_id: u32,
    promise_import_id: ?u32 = null,

    fn build(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const self: *const ReflectCall = castCtx(*const ReflectCall, ctx_ptr);
        var payload = try call.payloadTyped();
        var any = try payload.initContent();
        // Cap-in-params OUTBOUND: hand the server our own echoer export.
        try any.setCapability(.{ .id = self.echoer_export_id });
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

// -- Client A: ping call — one ctx drives both the params build and the return
//    observer.

const PingCall = struct {
    n: u32,
    result_n: ?u32 = null,
    returned: bool = false,
    /// The Return tag observed for this call. A pipelined call forwarded back to
    /// its own origin (loopback reflection) uses the Level-1 `sendResultsTo =
    /// yourself` tail-call optimization; the runtime runs the handler locally and
    /// delivers the computed results INLINE when the `takeFromOtherQuestion`
    /// redirect resolves, so the caller sees plain `.results` (with the value)
    /// rather than the bare relay tag. A direct post-resolution call likewise
    /// returns `.results`. The `.takeFromOtherQuestion` arm remains as a
    /// tolerated fallback for cases the runtime cannot deliver inline (e.g.
    /// results carrying capabilities).
    return_tag: ?protocol.ReturnTag = null,

    fn build(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const self: *const PingCall = castCtx(*const PingCall, ctx_ptr);
        var payload = try call.payloadTyped();
        var any = try payload.initContent();
        const params = try any.initStruct(1, 0);
        params.writeU32(0, self.n);
    }

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *PingCall = castCtx(*PingCall, ctx_ptr);
        self.return_tag = ret.tag;
        self.returned = true;
        switch (ret.tag) {
            .results => {
                const payload = ret.results orelse return error.MissingPingPayload;
                self.result_n = try readPingN(payload);
            },
            // Tolerated fallback: results the runtime could not deliver inline
            // (e.g. cap-carrying) surface the relay tag; the round-trip value is
            // then asserted at the echoer instead.
            .takeFromOtherQuestion => {},
            else => return error.UnexpectedPingReturn,
        }
    }
};

// -- Server B: ReflectorService bootstrap ------------------------------------
//
// Its single method `reflect(echoer)` imports the client's echoer, retains it,
// exports an unresolved promise P, and returns P in the results. The imported
// echoer id and the promise export id are stashed so the test can drive the
// resolution.

const ReflectorService = struct {
    peer: *Peer,
    echoer_import_id: ?u32 = null,
    promise_export_id: ?u32 = null,

    const ReturnCtx = struct {
        promise_id: u32,
        fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            const self: *const ReturnCtx = castCtx(*const ReturnCtx, ctx_ptr);
            // We RETAIN the client's echoer param import (see onCall), so tell
            // the client NOT to implicitly release its param caps on this
            // Return. The client's echoer export therefore survives past this
            // Return (it is later reflected back to the client as the promise's
            // resolution target). We send the explicit Release for the import
            // when we are done with it (test teardown).
            ret.setReleaseParamCaps(false);
            var payload = try ret.payloadTyped();
            var any = try payload.initContent();
            // Place the promise export directly as the payload content cap
            // pointer. classifyCap encodes a promise export as senderPromise,
            // so the client imports it as a promise (pipelinable).
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

        // Extract the client's echoer from params (cap-table index 0) and
        // retain the import so it outlives this handler.
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const echoer_cap = try call.params.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(echoer_cap);
        try mutable_caps.retainCapability(echoer_cap);
        self.echoer_import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.EchoerNotImported,
        };

        // Export an unresolved promise and return it.
        const promise_id = try peer.addPromiseExport();
        self.promise_export_id = promise_id;

        var ret_ctx = ReturnCtx{ .promise_id = promise_id };
        try peer.sendReturnResults(call.question_id, &ret_ctx, ReturnCtx.build);
    }
};

test "resolvePromiseExportToImport drives the reflected-cap resolve/disembargo cycle end-to-end" {
    const allocator = std.testing.allocator;

    // Wire before peers: peer.deinit may emit frames the overrides record.
    var wire = Wire{ .allocator = allocator };
    defer wire.deinit();

    var server_peer = Peer.initDetached(allocator);
    server_peer.disableThreadAffinity();
    defer server_peer.deinit();

    var client_peer = Peer.initDetached(allocator);
    client_peer.disableThreadAffinity();
    defer client_peer.deinit();

    // Stop forwarding before either peer.deinit (LIFO): teardown frames are
    // still recorded but no longer delivered into a half-torn-down peer.
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

    // Client: host an Echoer and bootstrap the reflector.
    var echoer = EchoerServer{};
    const echoer_export_id = try client_peer.addExport(.{
        .ctx = &echoer,
        .on_call = EchoerServer.onCall,
    });

    var bootstrap_probe = BootstrapProbe{};
    _ = try client_peer.sendBootstrap(&bootstrap_probe, BootstrapProbe.onReturn);
    // The bootstrap answers synchronously through the wire.
    const reflector_import_id = bootstrap_probe.reflector_import_id orelse
        return error.BootstrapDidNotResolve;

    // Q_reflect: reflect(echoer = our own export). Answered synchronously; the
    // server returns the promise export P and we import it as a promise.
    var reflect_call = ReflectCall{ .echoer_export_id = echoer_export_id };
    _ = try client_peer.sendCall(
        reflector_import_id,
        0x1111_2222_3333_4444, // ReflectorService interface id (unused by handler)
        0, // reflect method id
        &reflect_call,
        ReflectCall.build,
        ReflectCall.onReturn,
    );
    const promise_import_id = reflect_call.promise_import_id orelse
        return error.ReflectDidNotResolve;

    // The server imported our echoer and exported the promise.
    const echoer_import_id = reflector.echoer_import_id orelse return error.EchoerNotReflected;
    const promise_export_id = reflector.promise_export_id orelse return error.PromiseNotExported;

    // Sanity: the server's promise export and echoer import both exist, and the
    // echoer import (retained by the handler) sits at wire ref_count 1 with no
    // promise pin yet. The client kept its echoer export alive because the
    // reflect Return set releaseParamCaps=false.
    try std.testing.expect(server_peer.exports.contains(promise_export_id));
    try std.testing.expect(server_peer.caps.hasImport(echoer_import_id));
    try std.testing.expectEqual(@as(u32, 1), server_peer.caps.imports.get(echoer_import_id).?.ref_count);
    try std.testing.expectEqual(@as(u32, 0), server_peer.caps.imports.get(echoer_import_id).?.promise_ref_count);
    try std.testing.expect(client_peer.exports.contains(echoer_export_id));

    // Q_ping1: pipeline ping(1) on the (still unresolved) promise import. This
    // parks on P at the server (no resolution yet).
    var ping1_call = PingCall{ .n = 1 };
    _ = try client_peer.sendCall(
        promise_import_id,
        PING_INTERFACE_ID,
        PING_METHOD_ID,
        &ping1_call,
        PingCall.build,
        PingCall.onReturn,
    );

    // The pipelined ping has NOT returned yet: it is parked behind the
    // unresolved promise at the server, and the echoer has not seen it.
    try std.testing.expect(!ping1_call.returned);
    try std.testing.expectEqual(@as(u32, 0), echoer.ping_calls);
    // *** The API under test *** — the server resolves the promise to the
    // client-hosted echoer it imported. This sends Resolve(P ->
    // receiverHosted{echoer}) and replays the parked ping by forwarding it back
    // out to the client's echoer.
    try server_peer.resolvePromiseExportToImport(promise_export_id, echoer_import_id);

    // The resolve took a LOCAL promise-held pin on the echoer import — NOT a new
    // wire reference (a receiverHosted descriptor names the remote's own export
    // and creates no ref against us). So wire ref_count stays 1 and the pin is 1.
    try std.testing.expectEqual(@as(u32, 1), server_peer.caps.imports.get(echoer_import_id).?.ref_count);
    try std.testing.expectEqual(@as(u32, 1), server_peer.caps.imports.get(echoer_import_id).?.promise_ref_count);

    // The whole cycle completed synchronously through the wire:
    //   - the parked ping(1) was forwarded to the echoer, which saw n==1 and
    //     echoed it back; the client's pipelined question completed and — via the
    //     self-loopback inline delivery — received the real result value (n==1)
    //     rather than a bare takeFromOtherQuestion relay tag.
    try std.testing.expect(ping1_call.returned);
    try std.testing.expectEqual(protocol.ReturnTag.results, ping1_call.return_tag.?);
    try std.testing.expectEqual(@as(u32, 1), ping1_call.result_n.?);
    try std.testing.expectEqual(@as(u32, 1), echoer.ping_calls);
    try std.testing.expect(echoer.ping1_seq != null);

    //   - the client saw its promise resolve to one of its OWN exports and
    //     sent a senderLoopback Disembargo; the server echoed receiverLoopback.
    try std.testing.expect(wire.hasDisembargo(.client_to_server, .senderLoopback));
    try std.testing.expect(wire.hasDisembargo(.server_to_client, .receiverLoopback));

    //   - the server emitted exactly one Resolve for P.
    try std.testing.expectEqual(@as(usize, 1), wire.count(.server_to_client, .resolve));

    // Post-resolution DIRECT call: ping(2) on the now-resolved promise import.
    // The embargo has cleared, so this dispatches straight through to the
    // echoer and returns 2.
    var ping2_call = PingCall{ .n = 2 };
    _ = try client_peer.sendCall(
        promise_import_id,
        PING_INTERFACE_ID,
        PING_METHOD_ID,
        &ping2_call,
        PingCall.build,
        PingCall.onReturn,
    );
    try std.testing.expect(ping2_call.returned);
    try std.testing.expectEqual(@as(u32, 2), echoer.ping_calls);
    try std.testing.expect(echoer.ping2_seq != null);

    // Ordering: the pipelined ping(1) reached the echoer strictly before the
    // post-embargo direct ping(2) — the embargo held the reflected cap until the
    // Disembargo round-trip cleared, so no reordering occurred.
    try std.testing.expect(echoer.ping1_seq.? < echoer.ping2_seq.?);

    // -- Teardown: release every ref the test/handlers took so both peers
    //    deinit with empty tables and std.testing.allocator sees no leak. --

    // First discharge the reflect answer (q1), which still holds an answer-held
    // ref on the promise export P (P was returned in q1's results). NOTE: A
    // already auto-sent Finish(1) reentrantly — synchronously nested inside B's
    // Return(1) delivery, i.e. BEFORE B had committed q1's resolved-answer
    // record — so that early Finish found nothing to clean (a pure artifact of
    // this fully-synchronous loopback; on a real async transport Finish always
    // arrives after the Return commits). Re-send Finish(1) from the top level
    // now that q1's answer is committed, so B releases P's answer-held ref.
    try client_peer.sendFinishForHost(1, false, false);

    // Client releases its promise import (retained in ReflectProbe). The Release
    // reaches the server and destroys the promise export (its only wire ref, now
    // that the answer-held ref is gone). Destroying the promise export runs the
    // finalizeExportRelease import-target cascade, which drops the LOCAL promise
    // pin on the echoer import (promise_ref_count 1 -> 0) WITHOUT sending any
    // wire Release. The echoer import entry survives because its wire ref_count
    // is still 1 (the retained param import), and A's echoer export is untouched.
    try client_peer.releaseImport(promise_import_id, 1);
    try std.testing.expect(!server_peer.exports.contains(promise_export_id));
    try std.testing.expect(server_peer.caps.hasImport(echoer_import_id));
    try std.testing.expectEqual(@as(u32, 0), server_peer.caps.imports.get(echoer_import_id).?.promise_ref_count);
    try std.testing.expectEqual(@as(u32, 1), server_peer.caps.imports.get(echoer_import_id).?.ref_count);
    // The pin drop sent no frame: A's echoer export is still alive.
    try std.testing.expect(client_peer.exports.contains(echoer_export_id));

    // Client releases the reflector bootstrap import (retained in
    // BootstrapProbe). The server's bootstrap export persists by design.
    try client_peer.releaseImport(reflector_import_id, 1);

    // Now the server releases the WIRE reference it holds on the echoer import
    // (the retained param import). This is the ONE and only wire Release for the
    // echoer: it drops the import entry (ref 1 -> 0, pin already 0) and sends a
    // single Release(echoer, 1) to the client, which drops A's echoer export to
    // ref 0 and destroys it.
    try server_peer.releaseImport(echoer_import_id, 1);
    try std.testing.expect(!server_peer.caps.hasImport(echoer_import_id));
    try std.testing.expect(!client_peer.exports.contains(echoer_export_id));
}
