const std = @import("std");
const capnpc = @import("capnpc-zig");
const io_backend_options = @import("io_backend_options");

const rpc = capnpc.rpc;

const game_types = @import("generated/game_types.zig");
const game_world = @import("generated/game_world.zig");
const chat = @import("generated/chat.zig");
const inventory = @import("generated/inventory.zig");
const matchmaking = @import("generated/matchmaking.zig");
const resolve_disembargo = @import("generated/resolve_disembargo.zig");

const Allocator = std.mem.Allocator;

const Schema = enum {
    game_world,
    chat,
    inventory,
    matchmaking,
    resolve_disembargo,
};

const CliArgs = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 4000,
    schema: Schema = .game_world,
};

const Tap = struct {
    test_num: usize = 0,
    failures: usize = 0,

    fn ok(self: *Tap, pass: bool, desc: []const u8) void {
        self.test_num += 1;
        if (pass) {
            std.debug.print("ok {d} - {s}\n", .{ self.test_num, desc });
        } else {
            std.debug.print("not ok {d} - {s}\n", .{ self.test_num, desc });
            self.failures += 1;
        }
    }
};

const ClientApp = struct {
    allocator: Allocator,
    args: CliArgs,
    tap: Tap = .{},
    done: bool = false,
    err: ?anyerror = null,

    // game_world scenario state
    gw_service: ?game_world.GameWorld.Client = null,
    gw_entity_id: u64 = 0,

    // chat scenario state
    chat_service: ?chat.ChatService.Client = null,
    chat_room: ?chat.ChatRoom.Client = null,

    // inventory scenario state
    inv_service: ?inventory.InventoryService.Client = null,
    trade_session: ?inventory.TradeSession.Client = null,

    // matchmaking scenario state
    mm_service: ?matchmaking.MatchmakingService.Client = null,
    mm_controller: ?matchmaking.MatchController.Client = null,
    mm_match_id: u64 = 0,
    mm_pipelined_info_match_id: u64 = 0,
    mm_find_match_done: bool = false,
    mm_signal_ready_done: bool = false,
    mm_get_info_done: bool = false,
    mm_cancel_started: bool = false,

    // resolve_disembargo scenario state
    rd_reflector: ?resolve_disembargo.Reflector.Client = null,
    // The client-hosted CallSequence that the reflector reflects back. Its
    // getNumber() returns a monotonic counter and records the arrival order of
    // each call, so pipelined-before-direct ordering across the embargo is
    // observable.
    rd_call_sequence: CallSequenceServer = .{},
    // A client handle to our own exported rd_call_sequence, used to name it as
    // the reflect() target via setTargetClient (which needs only the cap id).
    rd_target: ?resolve_disembargo.CallSequence.Client = null,
    // The resolved promise import: the reflector's promise capability, which
    // resolves back to our own rd_call_sequence.
    rd_promise: ?resolve_disembargo.CallSequence.Client = null,
    // getNumber value observed by the PIPELINED call (issued while the promise
    // was still unresolved).
    rd_pipelined_n: ?u32 = null,
    rd_pipelined_done: bool = false,
    rd_resolve_now_done: bool = false,
    // getNumber value observed by the DIRECT call (issued after resolution).
    rd_direct_n: ?u32 = null,
};

// Client-hosted CallSequence: getNumber() returns a per-connection monotonic
// counter (0, then 1, ...) and stamps the counter value at which each call
// arrived, so the test can assert the pipelined call reached us strictly before
// the direct call.
//
// This is implemented as a DEFERRED handler, NOT the plain direct handler, and
// that choice is load-bearing for THIS scenario. A reflected pipelined call
// that loops back to the caller's own export is delivered with
// `sendResultsTo = yourself` (the tail-call / takeFromOtherQuestion relay).
// On that path the runtime's sendReturnResults short-circuits to
// `resultsSentElsewhere` and NEVER invokes a direct handler's results-build
// closure — so a direct handler that recorded its counter inside the build
// closure would be silently skipped and never observe the call. A deferred
// handler body, by contrast, runs unconditionally before any return is sent, so
// the counter increment and arrival stamp are recorded even when the payload
// itself is relayed elsewhere. We still send the results (harmless: the runtime
// relays or drops them per the sendResultsTo mode).
const CallSequenceServer = struct {
    // Next value getNumber() will return; also the running arrival counter.
    counter: u32 = 0,
    // Arrival stamp (pre-increment counter value) of the first and second call,
    // used to prove ordering. call0_seq must be < call1_seq.
    call0_seq: ?u32 = null,
    call1_seq: ?u32 = null,
    calls_seen: u32 = 0,
    server: resolve_disembargo.CallSequence.Server = .{
        .ctx = undefined,
        .vtable = .{
            // A deferred handler still needs a non-null direct handler field in
            // the vtable; it is never called because the deferred variant takes
            // precedence (see generated handleCallDirect).
            .getNumber = unusedDirectGetNumber,
            .getNumber_deferred = onCallSequenceGetNumberDeferred,
        },
    },

    fn bind(self: *CallSequenceServer) void {
        self.server.ctx = self;
    }
};

// Never invoked: the deferred handler takes precedence in the generated
// dispatch. Present only to satisfy the non-optional vtable field.
fn unusedDirectGetNumber(
    _: *anyopaque,
    _: *rpc.peer.Peer,
    _: resolve_disembargo.CallSequence.GetNumberParams.Reader,
    _: *resolve_disembargo.CallSequence.GetNumberResults.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    return error.Unreachable;
}

// Carries the counter value from the deferred handler body into the results
// build closure.
const GetNumberReturnCtx = struct {
    n: u32,

    fn build(ctx_ptr: *anyopaque, ret: *rpc.wire.protocol.ReturnBuilder) anyerror!void {
        const self: *const GetNumberReturnCtx = @ptrCast(@alignCast(ctx_ptr));
        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        const results_builder = try any.initStruct(1, 0);
        var results = resolve_disembargo.CallSequence.GetNumberResults.Builder.wrap(results_builder);
        try results.setN(self.n);
    }
};

fn onCallSequenceGetNumberDeferred(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: resolve_disembargo.CallSequence.GetNumberParams.Reader,
    _: *const rpc.caps.table.InboundCapTable,
    sender: resolve_disembargo.CallSequence.GetNumber.ReturnSender,
) !void {
    const self: *CallSequenceServer = @ptrCast(@alignCast(ctx_ptr));
    const n = self.counter;
    // Record the arrival stamp for the first two calls so ordering is provable.
    // This runs unconditionally, even when the results are relayed elsewhere.
    if (self.calls_seen == 0) self.call0_seq = n;
    if (self.calls_seen == 1) self.call1_seq = n;
    self.calls_seen += 1;
    self.counter += 1;

    var ret_ctx = GetNumberReturnCtx{ .n = n };
    try sender.sendResults(&ret_ctx, GetNumberReturnCtx.build);
}

fn parseSchema(text: []const u8) !Schema {
    if (std.mem.eql(u8, text, "game_world")) return .game_world;
    if (std.mem.eql(u8, text, "chat")) return .chat;
    if (std.mem.eql(u8, text, "inventory")) return .inventory;
    if (std.mem.eql(u8, text, "matchmaking")) return .matchmaking;
    if (std.mem.eql(u8, text, "resolve_disembargo")) return .resolve_disembargo;
    return error.InvalidSchema;
}

fn parseArgs(allocator: Allocator, args: std.process.Args) !CliArgs {
    var out = CliArgs{};
    var host_text: []const u8 = out.host;

    // initAllocator is the cross-platform form; plain init is a compile
    // error on Windows, which the cross-compile gate covers.
    var args_iter = try std.process.Args.Iterator.initAllocator(args, allocator);
    defer args_iter.deinit();
    _ = args_iter.skip(); // skip program name
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            return error.HelpRequested;
        }
        if (std.mem.eql(u8, arg, "--host")) {
            host_text = args_iter.next() orelse return error.MissingArgValue;
            continue;
        }
        if (std.mem.eql(u8, arg, "--port")) {
            const port_str = args_iter.next() orelse return error.MissingArgValue;
            out.port = try std.fmt.parseInt(u16, port_str, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--schema")) {
            const schema_str = args_iter.next() orelse return error.MissingArgValue;
            out.schema = try parseSchema(schema_str);
            continue;
        }
    }

    out.host = try allocator.dupe(u8, host_text);
    return out;
}

fn finish(app: *ClientApp, peer: *rpc.peer.Peer) void {
    // Explicitly release every imported capability this scenario still owns
    // (balances the retains from bootstrap-unwrap and resolveXxx) before
    // closing the transport.
    if (app.gw_service) |client| {
        client.release();
        app.gw_service = null;
    }
    if (app.chat_room) |client| {
        client.release();
        app.chat_room = null;
    }
    if (app.chat_service) |client| {
        client.release();
        app.chat_service = null;
    }
    if (app.trade_session) |client| {
        client.release();
        app.trade_session = null;
    }
    if (app.inv_service) |client| {
        client.release();
        app.inv_service = null;
    }
    if (app.mm_controller) |client| {
        client.release();
        app.mm_controller = null;
    }
    if (app.mm_service) |client| {
        client.release();
        app.mm_service = null;
    }
    // resolve_disembargo: release the promise import (retained by
    // resolvePromise) and the reflector bootstrap import.
    if (app.rd_promise) |client| {
        client.release();
        app.rd_promise = null;
    }
    if (app.rd_reflector) |client| {
        client.release();
        app.rd_reflector = null;
    }
    app.done = true;
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}

fn failAndFinish(app: *ClientApp, peer: *rpc.peer.Peer, desc: []const u8) void {
    app.tap.ok(false, desc);
    finish(app, peer);
}

fn onSessionError(ctx: ?*anyopaque, _: *rpc.transport.tcp.ClientSession, err: anyerror) void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx.?));
    if (!app.done) {
        std.log.err("rpc peer error: {s}", .{@errorName(err)});
        app.err = err;
        app.done = true;
    }
}

fn onSessionClose(ctx: ?*anyopaque, _: *rpc.transport.tcp.ClientSession) void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx.?));
    app.done = true;
}

// ---------------------------------------------------------------------------
// game_world scenario: spawn -> getEntity -> damageEntity (multi-call state),
// then explicit release + clean shutdown. The schema has no sub-capability,
// so this exercises repeated calls against one imported capability.
// ---------------------------------------------------------------------------

fn bootstrapGameWorld(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    _ = try game_world.GameWorld.Client.fromBootstrap(peer, app, onGameWorldBootstrap);
}

fn onGameWorldBootstrap(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, response: game_world.GameWorld.BootstrapResponse) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    const client = response.unwrap() catch {
        failAndFinish(app, peer, "bootstrap game_world capability");
        return;
    };
    app.gw_service = client;
    _ = try client.callSpawnEntity(app, buildSpawnEntity, onSpawnEntityReturn);
}

fn buildSpawnEntity(ctx_ptr: *anyopaque, params: *game_world.GameWorld.SpawnEntity.Params.Builder) !void {
    _ = ctx_ptr;
    var request = try params.initRequest();
    try request.setKind(.Player);
    try request.setName("ZigClientHero");
    var pos = try request.initPosition();
    try pos.setX(10.0);
    try pos.setY(20.0);
    try pos.setZ(30.0);
    try request.setFaction(.Alliance);
    try request.setMaxHealth(100);
}

fn onSpawnEntityReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: game_world.GameWorld.SpawnEntity.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "spawnEntity returns results");
        return;
    };
    app.tap.ok((try results.getStatus()) == .Ok, "spawnEntity returns ok status");
    const entity = try results.getEntity();
    app.tap.ok(std.mem.eql(u8, try entity.getName(), "ZigClientHero"), "spawnEntity returns expected name");
    app.gw_entity_id = try (try entity.getId()).getId();
    app.tap.ok(app.gw_entity_id != 0, "spawnEntity returns non-zero entity id");

    const client = app.gw_service orelse {
        failAndFinish(app, peer, "game_world service client available");
        return;
    };
    _ = try client.callGetEntity(app, buildGetEntity, onGetEntityReturn);
}

fn buildGetEntity(ctx_ptr: *anyopaque, params: *game_world.GameWorld.GetEntity.Params.Builder) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    var id = try params.initId();
    try id.setId(app.gw_entity_id);
}

fn onGetEntityReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: game_world.GameWorld.GetEntity.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "getEntity returns results");
        return;
    };
    app.tap.ok((try results.getStatus()) == .Ok, "getEntity finds the spawned entity");
    const entity = try results.getEntity();
    app.tap.ok(std.mem.eql(u8, try entity.getName(), "ZigClientHero"), "getEntity returns the spawned entity name");
    app.tap.ok(try entity.getAlive(), "getEntity reports the entity alive");

    const client = app.gw_service orelse {
        failAndFinish(app, peer, "game_world service client available");
        return;
    };
    _ = try client.callDamageEntity(app, buildDamageEntity, onDamageEntityReturn);
}

fn buildDamageEntity(ctx_ptr: *anyopaque, params: *game_world.GameWorld.DamageEntity.Params.Builder) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    var id = try params.initId();
    try id.setId(app.gw_entity_id);
    try params.setAmount(150); // Exceeds maxHealth=100, so the entity dies.
}

fn onDamageEntityReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: game_world.GameWorld.DamageEntity.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "damageEntity returns results");
        return;
    };
    app.tap.ok((try results.getStatus()) == .Ok, "damageEntity returns ok status");
    app.tap.ok(try results.getKilled(), "damageEntity reports the entity killed");
    const entity = try results.getEntity();
    app.tap.ok(!(try entity.getAlive()) and (try entity.getHealth()) == 0, "damageEntity leaves the entity dead at zero health");

    finish(app, peer);
}

// ---------------------------------------------------------------------------
// chat scenario: createRoom returns a ChatRoom capability which the client
// then INVOKES (sendMessage, getInfo, leave), releases explicitly, and
// finally proves the session is still healthy with a listRooms call.
// ---------------------------------------------------------------------------

fn bootstrapChat(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    _ = try chat.ChatService.Client.fromBootstrap(peer, app, onChatBootstrap);
}

fn onChatBootstrap(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, response: chat.ChatService.BootstrapResponse) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    const client = response.unwrap() catch {
        failAndFinish(app, peer, "bootstrap chat capability");
        return;
    };
    app.chat_service = client;
    _ = try client.callCreateRoom(app, buildCreateRoom, onCreateRoomReturn);
}

fn buildCreateRoom(ctx_ptr: *anyopaque, params: *chat.ChatService.CreateRoom.Params.Builder) !void {
    _ = ctx_ptr;
    try params.setName("general");
    try params.setTopic("General chat from Zig client");
}

fn onCreateRoomReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: chat.ChatService.CreateRoom.Response,
    caps: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "createRoom returns results");
        return;
    };
    app.tap.ok((try results.getStatus()) == .Ok, "createRoom returns ok status");
    const info = try results.getInfo();
    app.tap.ok(std.mem.eql(u8, try info.getName(), "general"), "createRoom returns expected room info name");

    const room = results.resolveRoom(peer, caps) catch {
        failAndFinish(app, peer, "createRoom returns imported ChatRoom capability");
        return;
    };
    app.tap.ok(true, "createRoom returns imported ChatRoom capability");
    app.chat_room = room;

    _ = try room.callSendMessage(app, buildRoomSendMessage, onRoomSendMessageReturn);
}

fn buildRoomSendMessage(ctx_ptr: *anyopaque, params: *chat.ChatRoom.SendMessage.Params.Builder) !void {
    _ = ctx_ptr;
    try params.setContent("Hello from the Zig e2e client");
}

fn onRoomSendMessageReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: chat.ChatRoom.SendMessage.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "room sendMessage returns results");
        return;
    };
    app.tap.ok((try results.getStatus()) == .Ok, "room sendMessage returns ok status");
    const message = try results.getMessage();
    app.tap.ok(
        std.mem.eql(u8, try message.getContent(), "Hello from the Zig e2e client"),
        "room sendMessage echoes the message content",
    );

    const room = app.chat_room orelse {
        failAndFinish(app, peer, "chat room client available");
        return;
    };
    _ = try room.callGetInfo(app, null, onRoomGetInfoReturn);
}

fn onRoomGetInfoReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: chat.ChatRoom.GetInfo.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "room getInfo returns results");
        return;
    };
    const info = try results.getInfo();
    app.tap.ok(std.mem.eql(u8, try info.getName(), "general"), "room getInfo returns expected room name");

    const room = app.chat_room orelse {
        failAndFinish(app, peer, "chat room client available");
        return;
    };
    _ = try room.callLeave(app, null, onRoomLeaveReturn);
}

fn onRoomLeaveReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: chat.ChatRoom.Leave.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "room leave returns results");
        return;
    };
    app.tap.ok((try results.getStatus()) == .Ok, "room leave returns ok status");

    // Explicitly release the room import mid-session, then keep using the
    // service capability: a well-formed Release must only drop the room.
    if (app.chat_room) |room| {
        room.release();
        app.chat_room = null;
    }

    const service = app.chat_service orelse {
        failAndFinish(app, peer, "chat service client available");
        return;
    };
    _ = try service.callListRooms(app, null, onListRoomsReturn);
}

fn onListRoomsReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: chat.ChatService.ListRooms.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "listRooms returns results");
        return;
    };
    const rooms = try results.getRooms();
    var found = false;
    var i: u32 = 0;
    while (i < rooms.len()) : (i += 1) {
        const info = try rooms.get(i);
        if (std.mem.eql(u8, try info.getName(), "general")) found = true;
    }
    app.tap.ok(found, "listRooms still lists the room after room release");

    finish(app, peer);
}

// ---------------------------------------------------------------------------
// inventory scenario: getInventory, then startTrade returns a TradeSession
// capability which the client INVOKES (getState, accept, cancel) before
// releasing it.
// ---------------------------------------------------------------------------

fn bootstrapInventory(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    _ = try inventory.InventoryService.Client.fromBootstrap(peer, app, onInventoryBootstrap);
}

fn onInventoryBootstrap(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, response: inventory.InventoryService.BootstrapResponse) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    const client = response.unwrap() catch {
        failAndFinish(app, peer, "bootstrap inventory capability");
        return;
    };
    app.inv_service = client;
    _ = try client.callGetInventory(app, buildGetInventory, onGetInventoryReturn);
}

fn buildGetInventory(ctx_ptr: *anyopaque, params: *inventory.InventoryService.GetInventory.Params.Builder) !void {
    _ = ctx_ptr;
    var player = try params.initPlayer();
    try player.setId(42);
}

fn onGetInventoryReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: inventory.InventoryService.GetInventory.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "getInventory returns results");
        return;
    };
    app.tap.ok((try results.getStatus()) == .Ok, "getInventory returns ok status");
    const inv = try results.getInventory();
    app.tap.ok((try inv.getUsedSlots()) == 0, "new inventory has zero used slots");

    const client = app.inv_service orelse {
        failAndFinish(app, peer, "inventory service client available");
        return;
    };
    _ = try client.callStartTrade(app, buildStartTrade, onStartTradeReturn);
}

fn buildStartTrade(ctx_ptr: *anyopaque, params: *inventory.InventoryService.StartTrade.Params.Builder) !void {
    _ = ctx_ptr;
    var initiator = try params.initInitiator();
    try initiator.setId(42);
    var target = try params.initTarget();
    try target.setId(99);
}

fn onStartTradeReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: inventory.InventoryService.StartTrade.Response,
    caps: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "startTrade returns results");
        return;
    };
    app.tap.ok((try results.getStatus()) == .Ok, "startTrade returns ok status");

    const session = results.resolveSession(peer, caps) catch {
        failAndFinish(app, peer, "startTrade returns imported TradeSession capability");
        return;
    };
    app.tap.ok(true, "startTrade returns imported TradeSession capability");
    app.trade_session = session;

    _ = try session.callGetState(app, null, onTradeGetStateReturn);
}

fn onTradeGetStateReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: inventory.TradeSession.GetState.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "trade getState returns results");
        return;
    };
    app.tap.ok((try results.getState()) == .Proposing, "trade session starts in proposing state");

    const session = app.trade_session orelse {
        failAndFinish(app, peer, "trade session client available");
        return;
    };
    _ = try session.callAccept(app, null, onTradeAcceptReturn);
}

fn onTradeAcceptReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: inventory.TradeSession.Accept.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "trade accept returns results");
        return;
    };
    app.tap.ok((try results.getStatus()) == .Ok, "trade accept returns ok status");

    const session = app.trade_session orelse {
        failAndFinish(app, peer, "trade session client available");
        return;
    };
    _ = try session.callCancel(app, null, onTradeCancelReturn);
}

fn onTradeCancelReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: inventory.TradeSession.Cancel.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "trade cancel returns results");
        return;
    };
    app.tap.ok((try results.getState()) == .Cancelled, "trade cancel reports cancelled state");

    finish(app, peer);
}

// ---------------------------------------------------------------------------
// matchmaking scenario: enqueue, then the schema's designed pipelining
// exercise -- callFindMatchPipelined plus signalReady/getInfo on the
// promised MatchController BEFORE findMatch's Return resolves (E-order),
// then a direct call on the resolved controller import.
// ---------------------------------------------------------------------------

fn bootstrapMatchmaking(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    _ = try matchmaking.MatchmakingService.Client.fromBootstrap(peer, app, onMatchmakingBootstrap);
}

fn onMatchmakingBootstrap(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, response: matchmaking.MatchmakingService.BootstrapResponse) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    const client = response.unwrap() catch {
        failAndFinish(app, peer, "bootstrap matchmaking capability");
        return;
    };
    app.mm_service = client;
    _ = try client.callEnqueue(app, buildEnqueue, onEnqueueReturn);
}

fn buildEnqueue(ctx_ptr: *anyopaque, params: *matchmaking.MatchmakingService.Enqueue.Params.Builder) !void {
    _ = ctx_ptr;
    var player = try params.initPlayer();
    var id = try player.initId();
    try id.setId(1);
    try player.setName("ZigQueuePlayer");
    try player.setFaction(.Alliance);
    try player.setLevel(60);
    try params.setMode(.Duel);
}

fn onEnqueueReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: matchmaking.MatchmakingService.Enqueue.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "enqueue returns results");
        return;
    };
    app.tap.ok((try results.getStatus()) == .Ok, "enqueue returns ok status");
    const ticket = try results.getTicket();
    app.tap.ok((try ticket.getTicketId()) > 0, "enqueue returns a non-zero ticket id");

    const service = app.mm_service orelse {
        failAndFinish(app, peer, "matchmaking service client available");
        return;
    };

    // Typed pipelining: issue signalReady and getInfo against the promised
    // MatchController before findMatch's Return has been received. All three
    // Calls hit the wire back to back; the server answers them in E-order.
    const pipeline = try service.callFindMatchPipelined(app, buildFindMatch, onFindMatchReturn);
    const promised_controller = pipeline.getController();
    _ = try promised_controller.callSignalReady(app, buildSignalReady, onSignalReadyReturn);
    _ = try promised_controller.callGetInfo(app, null, onPipelinedGetInfoReturn);
    app.tap.ok(!app.mm_find_match_done, "pipelined calls issued before findMatch resolved");
}

fn buildFindMatch(ctx_ptr: *anyopaque, params: *matchmaking.MatchmakingService.FindMatch.Params.Builder) !void {
    _ = ctx_ptr;
    var player = try params.initPlayer();
    var id = try player.initId();
    try id.setId(1);
    try player.setName("ZigQueuePlayer");
    try player.setFaction(.Alliance);
    try player.setLevel(60);
    try params.setMode(.Duel);
}

fn buildSignalReady(ctx_ptr: *anyopaque, params: *matchmaking.MatchController.SignalReady.Params.Builder) !void {
    _ = ctx_ptr;
    var player = try params.initPlayer();
    try player.setId(1);
}

fn onFindMatchReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: matchmaking.MatchmakingService.FindMatch.Response,
    caps: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "findMatch returns results");
        return;
    };
    app.mm_match_id = try (try results.getMatchId()).getId();
    app.tap.ok(app.mm_match_id != 0, "findMatch returns a non-zero match id");

    const controller = results.resolveController(peer, caps) catch {
        failAndFinish(app, peer, "findMatch returns imported MatchController capability");
        return;
    };
    app.tap.ok(true, "findMatch returns imported MatchController capability");
    app.mm_controller = controller;

    app.mm_find_match_done = true;
    try maybeCancelMatch(app, peer);
}

fn onSignalReadyReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: matchmaking.MatchController.SignalReady.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "pipelined signalReady returns results");
        return;
    };
    app.tap.ok((try results.getStatus()) == .Ok, "pipelined signalReady returns ok status");

    app.mm_signal_ready_done = true;
    try maybeCancelMatch(app, peer);
}

fn onPipelinedGetInfoReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: matchmaking.MatchController.GetInfo.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "pipelined getInfo returns results");
        return;
    };
    const info = try results.getInfo();
    app.mm_pipelined_info_match_id = try (try info.getId()).getId();
    const team_a = try info.getTeamA();
    app.tap.ok(team_a.len() >= 1, "pipelined getInfo returns populated match info");

    app.mm_get_info_done = true;
    try maybeCancelMatch(app, peer);
}

/// Once findMatch and both pipelined calls have resolved, verify pipelined
/// results were served by the same match, then make a DIRECT call on the
/// now-resolved controller import.
fn maybeCancelMatch(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    if (app.mm_cancel_started) return;
    if (!(app.mm_find_match_done and app.mm_signal_ready_done and app.mm_get_info_done)) return;
    app.mm_cancel_started = true;

    app.tap.ok(
        app.mm_pipelined_info_match_id == app.mm_match_id,
        "pipelined getInfo observed the same match as findMatch",
    );

    const controller = app.mm_controller orelse {
        failAndFinish(app, peer, "match controller client available");
        return;
    };
    _ = try controller.callCancelMatch(app, null, onCancelMatchReturn);
}

fn onCancelMatchReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: matchmaking.MatchController.CancelMatch.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "cancelMatch returns results");
        return;
    };
    app.tap.ok((try results.getStatus()) == .Ok, "cancelMatch on resolved controller returns ok status");

    finish(app, peer);
}

// ---------------------------------------------------------------------------
// resolve_disembargo scenario (Zig-client -> reference-server direction).
//
// This is the caller side of the reflected-cap resolve/embargo cycle. We host
// our own CallSequence, hand it to the reflector's reflect(), pipeline a
// getNumber on the returned (still-unresolved) promise so it PARKS at the
// reflector, then call resolveNow() to make the reflector resolve the promise
// back to our CallSequence. That reflected resolution forwards the parked call
// home and runs our senderLoopback -> receiverLoopback Disembargo before we
// issue a DIRECT getNumber on the now-resolved cap.
//
// Assertions prove: reflect returned a promise; the pipelined getNumber reached
// our CallSequence and returned n==0; resolveNow completed; the direct
// getNumber returned n==1; and the two calls arrived in order (pipelined
// strictly before direct) — i.e. the embargo held the reflected cap until the
// Disembargo round-trip cleared, so no reordering occurred.
//
// The proof test tests/rpc/peer/rpc_reflected_resolve_disembargo_test.zig drives
// this same cycle with low-level primitives over a synchronous in-process wire;
// this e2e proves it holds over real async TCP, where the reentrancy artifacts
// that test documents (an early nested Finish) do not arise.
// ---------------------------------------------------------------------------

fn bootstrapResolveDisembargo(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    app.rd_call_sequence.bind();
    // Export our CallSequence once here (where we hold the peer) so the reflect
    // params builder can name it by cap id via setTargetClient.
    const cap_id = try resolve_disembargo.CallSequence.exportServer(peer, &app.rd_call_sequence.server);
    app.rd_target = resolve_disembargo.CallSequence.Client.init(peer, cap_id);
    _ = try resolve_disembargo.Reflector.Client.fromBootstrap(peer, app, onReflectorBootstrap);
}

fn onReflectorBootstrap(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, response: resolve_disembargo.Reflector.BootstrapResponse) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    const client = response.unwrap() catch {
        failAndFinish(app, peer, "bootstrap reflector capability");
        return;
    };
    app.rd_reflector = client;

    // reflect(target = our own CallSequence). buildReflect names our
    // already-exported CallSequence in the params cap table via setTargetClient.
    _ = try client.callReflect(app, buildReflect, onReflectReturn);
}

fn buildReflect(ctx_ptr: *anyopaque, params: *resolve_disembargo.Reflector.ReflectParams.Builder) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    // Name our already-exported CallSequence as the reflect target; this only
    // needs the cap id (no peer access from inside the params builder).
    try params.setTargetClient(app.rd_target.?);
}

fn onReflectReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: resolve_disembargo.Reflector.Reflect.Response,
    caps: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    const results = response.unwrap() catch {
        failAndFinish(app, peer, "reflect returns results");
        return;
    };

    // The `promise` result is an unresolved CallSequence promise import.
    const promise = results.resolvePromise(peer, caps) catch {
        failAndFinish(app, peer, "reflect returns imported promise capability");
        return;
    };
    app.tap.ok(true, "reflect returns imported promise capability");
    app.rd_promise = promise;

    // Pipeline a getNumber on the (still unresolved) promise. It parks at the
    // reflector because the promise has no target yet. It has NOT returned when
    // we issue resolveNow immediately after.
    _ = try promise.callGetNumber(app, null, onPipelinedGetNumberReturn);
    app.tap.ok(!app.rd_pipelined_done, "pipelined getNumber issued before resolution (parked)");

    // Now tell the reflector to resolve the promise to our CallSequence. This
    // forwards the parked getNumber home and drives the Disembargo handshake.
    const reflector = app.rd_reflector orelse {
        failAndFinish(app, peer, "reflector client available");
        return;
    };
    _ = try reflector.callResolveNow(app, null, onResolveNowReturn);
}

fn onPipelinedGetNumberReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: resolve_disembargo.CallSequence.GetNumber.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    // Ordering subtlety: this call was parked on the unresolved promise and then
    // forwarded back to our OWN CallSequence when the reflector resolved the
    // promise. A pipelined call that reflects back to its own origin completes
    // via the Level-1 `takeFromOtherQuestion` tail-call relay, NOT a plain
    // `.results` return — the value does not travel back inline here. Both
    // `.results` and `.take_from_other_question` mean the call completed; the
    // relay path carries no inline payload, so the source of truth is the value
    // our CallSequence deferred handler recorded when the forwarded call arrived
    // (rd_call_sequence.call0_seq). By the time this Return is dispatched, that
    // forwarded call has already been processed, so call0_seq is populated.
    switch (response) {
        .results => |results| app.rd_pipelined_n = try results.getN(),
        .take_from_other_question => app.rd_pipelined_n = app.rd_call_sequence.call0_seq,
        else => {
            failAndFinish(app, peer, "pipelined getNumber completes");
            return;
        },
    }
    app.rd_pipelined_done = true;
    app.tap.ok(app.rd_pipelined_n != null and app.rd_pipelined_n.? == 0, "pipelined getNumber reached CallSequence with n==0");

    try maybeDirectGetNumber(app, peer);
}

fn onResolveNowReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: resolve_disembargo.Reflector.ResolveNow.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    _ = response.unwrap() catch {
        failAndFinish(app, peer, "resolveNow returns results");
        return;
    };
    app.tap.ok(true, "resolveNow completed");
    app.rd_resolve_now_done = true;

    try maybeDirectGetNumber(app, peer);
}

/// Once the pipelined getNumber has come home AND resolveNow has returned (so
/// the promise is resolved and the embargo cleared), issue a DIRECT getNumber
/// on the now-resolved promise import.
fn maybeDirectGetNumber(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    if (!(app.rd_pipelined_done and app.rd_resolve_now_done)) return;
    // Guard against issuing the direct call twice (both callbacks call here).
    if (app.rd_direct_n != null) return;

    const promise = app.rd_promise orelse {
        failAndFinish(app, peer, "resolved promise client available");
        return;
    };
    _ = try promise.callGetNumber(app, null, onDirectGetNumberReturn);
}

fn onDirectGetNumberReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: resolve_disembargo.CallSequence.GetNumber.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    // The resolved promise points at our OWN CallSequence export, so this direct
    // call also loops home; like the pipelined call it may complete via a
    // tail-call relay. Read the value the deferred handler recorded for the
    // second arrival (call1_seq) when the relay carries no inline payload.
    switch (response) {
        .results => |results| app.rd_direct_n = try results.getN(),
        .take_from_other_question => app.rd_direct_n = app.rd_call_sequence.call1_seq,
        else => {
            failAndFinish(app, peer, "direct getNumber completes");
            return;
        },
    }
    app.tap.ok(app.rd_direct_n != null and app.rd_direct_n.? == 1, "direct getNumber returned n==1");

    // Ordering: the pipelined getNumber reached our CallSequence strictly before
    // the post-embargo direct getNumber. The embargo held the reflected cap
    // until the Disembargo round-trip cleared, so no reordering occurred.
    const seq = &app.rd_call_sequence;
    const ordered = seq.call0_seq != null and seq.call1_seq != null and seq.call0_seq.? < seq.call1_seq.?;
    app.tap.ok(ordered, "pipelined getNumber reached CallSequence before the direct call");

    finish(app, peer);
}

fn usage() void {
    std.debug.print(
        \\Usage: e2e-zig-client [--host 127.0.0.1] [--port 4000] [--schema game_world|chat|inventory|matchmaking|resolve_disembargo]\n
    , .{});
}

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    const backend_kind = capnpc.io_backend.parseKind(io_backend_options.kind) orelse {
        std.debug.print("invalid -Dio-backend selector: {s}\n", .{io_backend_options.kind});
        return error.InvalidIoBackend;
    };
    var backend = try capnpc.io_backend.Backend.init(backend_kind, init.gpa, init.io);
    defer backend.deinit();
    const io = backend.io();

    const args = parseArgs(allocator, init.minimal.args) catch |err| switch (err) {
        error.HelpRequested => {
            usage();
            return;
        },
        error.InvalidSchema,
        error.InvalidCharacter,
        error.Overflow,
        error.MissingArgValue,
        => {
            usage();
            return err;
        },
        else => return err,
    };
    defer allocator.free(args.host);

    var app = ClientApp{
        .allocator = allocator,
        .args = args,
    };

    const address = try std.Io.net.IpAddress.parse(args.host, args.port);

    const session = try rpc.transport.tcp.connect(allocator, io, address, .{
        .ctx = &app,
        .on_error = onSessionError,
        .on_close = onSessionClose,
    });
    defer session.deinit();
    const peer = &session.peer;

    const start_result = switch (app.args.schema) {
        .game_world => bootstrapGameWorld(&app, peer),
        .chat => bootstrapChat(&app, peer),
        .inventory => bootstrapInventory(&app, peer),
        .matchmaking => bootstrapMatchmaking(&app, peer),
        .resolve_disembargo => bootstrapResolveDisembargo(&app, peer),
    };

    start_result catch |err| {
        app.err = err;
        session.close();
    };

    session.run();

    if (app.err) |err| return err;

    std.debug.print("1..{d}\n", .{app.tap.test_num});

    if (app.tap.failures > 0) {
        return error.TestFailed;
    }
}
