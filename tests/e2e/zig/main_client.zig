const std = @import("std");
const capnpc = @import("capnpc-zig");
const io_backend_options = @import("io_backend_options");

const rpc = capnpc.rpc;

const game_types = @import("generated/game_types.zig");
const game_world = @import("generated/game_world.zig");
const chat = @import("generated/chat.zig");
const inventory = @import("generated/inventory.zig");
const matchmaking = @import("generated/matchmaking.zig");

const Allocator = std.mem.Allocator;

const Schema = enum {
    game_world,
    chat,
    inventory,
    matchmaking,
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
};

fn parseSchema(text: []const u8) !Schema {
    if (std.mem.eql(u8, text, "game_world")) return .game_world;
    if (std.mem.eql(u8, text, "chat")) return .chat;
    if (std.mem.eql(u8, text, "inventory")) return .inventory;
    if (std.mem.eql(u8, text, "matchmaking")) return .matchmaking;
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

fn usage() void {
    std.debug.print(
        \\Usage: e2e-zig-client [--host 127.0.0.1] [--port 4000] [--schema game_world|chat|inventory|matchmaking]\n
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
