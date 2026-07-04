const std = @import("std");
const capnpc = @import("capnpc-zig");
const e2e = @import("e2e_generated");

const protocol = capnpc.rpc.wire.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.caps.table;
const Peer = peer_impl.Peer;

const Bootstrap = e2e.Bootstrap;

// Runtime probe for the GENERATED typed-pipelining stubs. Everything here
// drives the generated client API from tests/e2e/zig/generated/bootstrap.zig
// (and, through it, game_world.zig) end-to-end across two in-process peers:
//
//   Bootstrap.Client.callGameWorldPipelined
//     -> Bootstrap.GameWorldPipeline.getService()
//     -> game_world.GameWorld.PipelinedClient.callDespawnEntity
//
// The codegen suites only string-match these stubs; this test executes them.

// -- Reflection over generated types ----------------------------------------
//
// bootstrap.zig imports game_world.zig with a non-pub `const game_world`, so
// the sibling module's types (GameWorld.PipelinedClient, GameWorld.Server,
// the per-method Response unions) are NOT nameable from outside the generated
// module. Everything below is derived from what Bootstrap's pub decls expose.

fn ReturnType(comptime f: anytype) type {
    return @typeInfo(@TypeOf(f)).@"fn".return_type.?;
}

fn ParamType(comptime f: anytype, comptime index: usize) type {
    return @typeInfo(@TypeOf(f)).@"fn".param_types[index].?;
}

/// game_world.GameWorld.PipelinedClient, via GameWorldPipeline.getService().
const GameWorldPipelinedClient = ReturnType(Bootstrap.GameWorldPipeline.getService);

/// game_world.GameWorld.Server, via GameWorldResults.Builder.setServiceServer.
const GameWorldServer = @typeInfo(
    ParamType(Bootstrap.GameWorldResults.Builder.setServiceServer, 2),
).pointer.child;

const GameWorldVTable = @FieldType(GameWorldServer, "vtable");

/// game_world.GameWorld.DespawnEntity handler/callback shapes, via the vtable
/// field and the PipelinedClient method signature.
const DespawnHandlerFn = @typeInfo(@FieldType(GameWorldVTable, "despawnEntity")).pointer.child;
const DespawnParamsReader = @typeInfo(DespawnHandlerFn).@"fn".param_types[2].?;
const DespawnResultsBuilderPtr = @typeInfo(DespawnHandlerFn).@"fn".param_types[3].?;

const DespawnCallbackFn = @typeInfo(
    ParamType(GameWorldPipelinedClient.callDespawnEntity, 3),
).pointer.child;
const DespawnResponse = @typeInfo(DespawnCallbackFn).@"fn".param_types[2].?;

/// A vtable stub for methods the test never calls: matches any generated
/// Handler signature and fails loudly if reached.
fn unexpectedHandler(comptime HandlerPtr: type) HandlerPtr {
    const param_types = @typeInfo(@typeInfo(HandlerPtr).pointer.child).@"fn".param_types;
    return &struct {
        fn call(
            _: param_types[0].?,
            _: param_types[1].?,
            _: param_types[2].?,
            _: param_types[3].?,
            _: param_types[4].?,
        ) anyerror!void {
            return error.UnexpectedMethodCall;
        }
    }.call;
}

// -- In-process wire ---------------------------------------------------------

const EventDir = enum { client_to_server, server_to_client };

/// One decoded outbound frame: enough wire shape for the E-order and
/// PromisedAnswer assertions without keeping the frame bytes alive.
const WireEvent = struct {
    dir: EventDir,
    tag: protocol.MessageTag,
    /// question_id / answer_id for bootstrap, call, return, finish; else 0.
    id: u32 = 0,
    target_tag: ?protocol.MessageTargetTag = null,
    target_question_id: ?u32 = null,
    transform_len: u32 = 0,
    transform_op0_tag: ?protocol.PromisedAnswerOpTag = null,
    transform_op0_pointer_index: u16 = 0,
};

/// Cross-wires two detached peers: each send-frame override records the
/// decoded frame as a WireEvent, then synchronously forwards the bytes to the
/// other peer's handleFrame. Declared BEFORE the peers in the test body so
/// peer.deinit (which may still emit frames) runs before this capture's
/// teardown (defer is LIFO).
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
            .@"return" => event.id = (try decoded.asReturn()).answer_id,
            .finish => event.id = (try decoded.asFinish()).question_id,
            .call => {
                const call = try decoded.asCall();
                event.id = call.question_id;
                event.target_tag = call.target.tag;
                if (call.target.promised_answer) |promised| {
                    event.target_question_id = promised.question_id;
                    event.transform_len = promised.transform.len();
                    if (event.transform_len > 0) {
                        const op = try promised.transform.get(0);
                        event.transform_op0_tag = op.tag;
                        event.transform_op0_pointer_index = op.pointer_index;
                    }
                }
            },
            else => {},
        }
        try self.events.append(self.allocator, event);
    }

    fn clientSend(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Wire = @ptrCast(@alignCast(ctx_ptr));
        try self.record(.client_to_server, frame);
        if (self.forwarding) {
            if (self.server_peer) |peer| try peer.handleFrame(frame);
        }
    }

    fn serverSend(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Wire = @ptrCast(@alignCast(ctx_ptr));
        try self.record(.server_to_client, frame);
        if (self.forwarding) {
            if (self.client_peer) |peer| try peer.handleFrame(frame);
        }
    }

    fn indexOf(self: *const Wire, dir: EventDir, tag: protocol.MessageTag, id: u32) ?usize {
        for (self.events.items, 0..) |event, index| {
            if (event.dir == dir and event.tag == tag and event.id == id) return index;
        }
        return null;
    }

    fn count(self: *const Wire, dir: EventDir, tag: protocol.MessageTag, id: u32) usize {
        var n: usize = 0;
        for (self.events.items) |event| {
            if (event.dir == dir and event.tag == tag and event.id == id) n += 1;
        }
        return n;
    }
};

// -- Client-side probe state --------------------------------------------------

const ProbeState = struct {
    client: ?Bootstrap.Client = null,
    /// Monotonic callback sequence so Q1-before-Q2 ordering is observable.
    seq: u32 = 0,
    q1_return_seq: ?u32 = null,
    q1_results_ok: bool = false,
    q2_return_seq: ?u32 = null,
    q2_status_ok: bool = false,

    fn onBootstrap(ctx_ptr: *anyopaque, _: *Peer, response: Bootstrap.BootstrapResponse) anyerror!void {
        const self: *ProbeState = @ptrCast(@alignCast(ctx_ptr));
        switch (response) {
            .client => |client| self.client = client,
            else => return error.UnexpectedBootstrapResponse,
        }
    }

    fn onGameWorldReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        response: Bootstrap.GameWorld.Response,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ProbeState = @ptrCast(@alignCast(ctx_ptr));
        self.seq += 1;
        self.q1_return_seq = self.seq;
        switch (response) {
            .results => |results| {
                // Resolve (and thereby retain) the hosted GameWorld capability
                // through the generated typed resolver, like a real client:
                // without the retain the peer auto-Releases the import as soon
                // as this callback returns.
                _ = try results.resolveService(peer, caps);
                self.q1_results_ok = true;
            },
            else => return error.UnexpectedGameWorldResponse,
        }
    }

    fn onDespawnReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        response: DespawnResponse,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ProbeState = @ptrCast(@alignCast(ctx_ptr));
        self.seq += 1;
        self.q2_return_seq = self.seq;
        switch (response) {
            .results => |results| {
                const status = try results.getStatus();
                self.q2_status_ok = (status == .NotFound);
            },
            else => return error.UnexpectedDespawnResponse,
        }
    }
};

// -- Server-side state ---------------------------------------------------------

const ServerState = struct {
    game_world_calls: u32 = 0,
    despawn_calls: u32 = 0,
    /// Set by the deferred handler; the test resolves it later so the
    /// dependent call is provably parked behind an unanswered question.
    held_sender: ?Bootstrap.GameWorld.ReturnSender = null,

    /// Deferred handler: receive Bootstrap.gameWorld but do NOT answer.
    fn gameWorldDeferred(
        ctx_ptr: *anyopaque,
        _: *Peer,
        _: Bootstrap.GameWorld.Params.Reader,
        _: *const cap_table.InboundCapTable,
        sender: Bootstrap.GameWorld.ReturnSender,
    ) anyerror!void {
        const self: *ServerState = @ptrCast(@alignCast(ctx_ptr));
        self.game_world_calls += 1;
        self.held_sender = sender;
    }

    fn despawnEntity(
        ctx_ptr: *anyopaque,
        _: *Peer,
        _: DespawnParamsReader,
        results: DespawnResultsBuilderPtr,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ServerState = @ptrCast(@alignCast(ctx_ptr));
        self.despawn_calls += 1;
        // A non-default value so the client-side reader assertion is
        // distinguishable from an all-zero results struct.
        try results.setStatus(.NotFound);
    }
};

/// Build ctx for the held Q1 Return: mirrors the generated buildReturnDirect
/// shape (results struct with one pointer slot) and uses the generated
/// setServiceServer to export the GameWorld server into pointer slot 0.
const ResolveCtx = struct {
    peer: *Peer,
    gw_server: *GameWorldServer,

    fn buildGameWorldReturn(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        const self: *ResolveCtx = @ptrCast(@alignCast(ctx_ptr));
        var payload = try ret.payloadTyped();
        var results_any = try payload.initContent();
        const results_builder = try results_any.initStruct(0, 1);
        var results = Bootstrap.GameWorldResults.Builder.wrap(results_builder);
        try results.setServiceServer(self.peer, self.gw_server);
    }
};

test "generated typed pipelining stubs run end-to-end (E-order, wire shape, replay)" {
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

    // Runs before either peer.deinit (LIFO): teardown frames are still
    // recorded but no longer forwarded into a peer that may already be dead.
    defer wire.forwarding = false;

    wire.client_peer = &client_peer;
    wire.server_peer = &server_peer;
    client_peer.setSendFrameOverride(&wire, Wire.clientSend);
    server_peer.setSendFrameOverride(&wire, Wire.serverSend);

    // Server: generated Bootstrap server whose gameWorld method is deferred
    // (stashes the ReturnSender instead of answering).
    var server_state = ServerState{};
    var bootstrap_server = Bootstrap.Server{
        .ctx = &server_state,
        .vtable = .{
            .gameWorld = unexpectedHandler(Bootstrap.GameWorld.Handler),
            .gameWorld_deferred = ServerState.gameWorldDeferred,
            .chatService = unexpectedHandler(Bootstrap.ChatService.Handler),
            .inventoryService = unexpectedHandler(Bootstrap.InventoryService.Handler),
            .matchmakingService = unexpectedHandler(Bootstrap.MatchmakingService.Handler),
        },
    };
    _ = try Bootstrap.setBootstrap(&server_peer, &bootstrap_server);

    // The GameWorld implementation the held Return will export later.
    var gw_server = GameWorldServer{
        .ctx = &server_state,
        .vtable = .{
            .spawnEntity = unexpectedHandler(@FieldType(GameWorldVTable, "spawnEntity")),
            .despawnEntity = ServerState.despawnEntity,
            .getEntity = unexpectedHandler(@FieldType(GameWorldVTable, "getEntity")),
            .moveEntity = unexpectedHandler(@FieldType(GameWorldVTable, "moveEntity")),
            .damageEntity = unexpectedHandler(@FieldType(GameWorldVTable, "damageEntity")),
            .queryArea = unexpectedHandler(@FieldType(GameWorldVTable, "queryArea")),
        },
    };

    var probe = ProbeState{};

    // Bootstrap answers synchronously through the in-process wire, so the
    // callback has fired (and captured the Client) by the time this returns.
    _ = try Bootstrap.Client.fromBootstrap(&client_peer, &probe, ProbeState.onBootstrap);
    var client = probe.client orelse return error.BootstrapDidNotResolve;

    // Q1: callGameWorldPipelined — the server parks it (deferred handler).
    var pipeline = try client.callGameWorldPipelined(&probe, null, ProbeState.onGameWorldReturn);
    const q1 = pipeline.question_id;
    try std.testing.expectEqual(@as(u32, 1), server_state.game_world_calls);
    try std.testing.expect(server_state.held_sender != null);

    // Q2: the dependent call, through the generated pipeline surface only.
    var service = pipeline.getService();
    const q2 = try service.callDespawnEntity(&probe, null, ProbeState.onDespawnReturn);
    try std.testing.expect(q1 != q2);

    // (1) E-order: both Calls are already on the wire, in order, with no
    // server Return for Q1 anywhere between (or after) them yet.
    const q1_call_index = wire.indexOf(.client_to_server, .call, q1) orelse
        return error.MissingQ1Call;
    const q2_call_index = wire.indexOf(.client_to_server, .call, q2) orelse
        return error.MissingQ2Call;
    try std.testing.expect(q1_call_index < q2_call_index);
    try std.testing.expectEqual(@as(usize, 0), wire.count(.server_to_client, .@"return", q1));
    try std.testing.expectEqual(@as(usize, 0), wire.count(.server_to_client, .@"return", q2));

    // (2) Q2's wire shape: PromisedAnswer target on Q1 with exactly
    // [getPointerField(pointer_index=0)] — what the generated stub hardcodes.
    const q2_call = wire.events.items[q2_call_index];
    try std.testing.expectEqual(protocol.MessageTargetTag.promisedAnswer, q2_call.target_tag.?);
    try std.testing.expectEqual(q1, q2_call.target_question_id.?);
    try std.testing.expectEqual(@as(u32, 1), q2_call.transform_len);
    try std.testing.expectEqual(protocol.PromisedAnswerOpTag.getPointerField, q2_call.transform_op0_tag.?);
    try std.testing.expectEqual(@as(u16, 0), q2_call.transform_op0_pointer_index);
    // Q1's own call targets the imported bootstrap capability directly.
    try std.testing.expectEqual(
        protocol.MessageTargetTag.importedCap,
        wire.events.items[q1_call_index].target_tag.?,
    );

    // The parked Q2 has not touched the GameWorld handler yet.
    try std.testing.expectEqual(@as(u32, 0), server_state.despawn_calls);
    try std.testing.expect(probe.q1_return_seq == null);
    try std.testing.expect(probe.q2_return_seq == null);

    // (3) Resolve Q1: send the held Return whose results carry the hosted
    // GameWorld export in pointer slot 0. Committing the resolved answer
    // replays the parked Q2 into the export's despawnEntity handler.
    var resolve_ctx = ResolveCtx{ .peer = &server_peer, .gw_server = &gw_server };
    const sender = server_state.held_sender.?;
    try sender.sendResults(&resolve_ctx, ResolveCtx.buildGameWorldReturn);

    // The parked call dispatched to the export's handler exactly once.
    try std.testing.expectEqual(@as(u32, 1), server_state.despawn_calls);

    // Both client callbacks fired with results, Q1 strictly before Q2.
    try std.testing.expect(probe.q1_results_ok);
    try std.testing.expect(probe.q2_status_ok);
    try std.testing.expectEqual(@as(u32, 1), probe.q1_return_seq.?);
    try std.testing.expectEqual(@as(u32, 2), probe.q2_return_seq.?);

    // Exactly one Return each on the wire, Q1's before Q2's.
    try std.testing.expectEqual(@as(usize, 1), wire.count(.server_to_client, .@"return", q1));
    try std.testing.expectEqual(@as(usize, 1), wire.count(.server_to_client, .@"return", q2));
    const q1_return_index = wire.indexOf(.server_to_client, .@"return", q1).?;
    const q2_return_index = wire.indexOf(.server_to_client, .@"return", q2).?;
    try std.testing.expect(q1_return_index < q2_return_index);

    // (4) Leak-freedom is enforced by std.testing.allocator on teardown.
}
