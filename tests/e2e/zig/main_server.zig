const std = @import("std");
const capnpc = @import("capnpc-zig");

const rpc = capnpc.rpc;
const message = capnpc.message;

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
    host: []const u8 = "0.0.0.0",
    port: u16 = 4700,
    schema: Schema = .game_world,
    listen_fd: ?std.posix.fd_t = null,
};

const App = struct {
    allocator: Allocator,
    runtime: rpc.runtime.Runtime,
    schema: Schema,
    game_world_service: GameWorldService,
    chat_service: ChatService,
    inventory_service: InventoryService,
    matchmaking_service: MatchmakingService,

    fn init(allocator: Allocator, schema: Schema) !App {
        return .{
            .allocator = allocator,
            .runtime = try rpc.runtime.Runtime.init(allocator),
            .schema = schema,
            .game_world_service = GameWorldService.init(allocator),
            .chat_service = ChatService.init(allocator),
            .inventory_service = InventoryService.init(allocator),
            .matchmaking_service = MatchmakingService.init(allocator),
        };
    }

    fn bind(self: *App) void {
        self.game_world_service.bind();
        self.chat_service.bind();
        self.inventory_service.bind();
        self.matchmaking_service.bind();
    }

    fn deinit(self: *App) void {
        self.matchmaking_service.deinit();
        self.inventory_service.deinit();
        self.chat_service.deinit();
        self.game_world_service.deinit();
        self.runtime.deinit();
    }
};

const ListenerCtx = struct {
    listener: rpc.runtime.Listener,
    app: *App,
};

const GameEntity = struct {
    id: u64,
    kind: game_world.EntityKind,
    name: []u8,
    x: f32,
    y: f32,
    z: f32,
    health: i32,
    max_health: i32,
    faction: game_world.Faction,
    alive: bool,
};

const GameWorldService = struct {
    allocator: Allocator,
    next_entity_id: u64 = 1,
    entities: std.AutoHashMap(u64, GameEntity),
    server: game_world.GameWorld.Server,

    fn init(allocator: Allocator) GameWorldService {
        return .{
            .allocator = allocator,
            .entities = std.AutoHashMap(u64, GameEntity).init(allocator),
            .server = .{
                .ctx = undefined,
                .vtable = .{
                    .spawnEntity = onSpawnEntity,
                    .despawnEntity = onDespawnEntity,
                    .getEntity = onGetEntity,
                    .moveEntity = onMoveEntity,
                    .damageEntity = onDamageEntity,
                    .queryArea = onQueryArea,
                },
            },
        };
    }

    fn bind(self: *GameWorldService) void {
        self.server.ctx = self;
    }

    fn deinit(self: *GameWorldService) void {
        var it = self.entities.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.name);
        }
        self.entities.deinit();
    }
};

const ChatMsgKind = enum {
    normal,
    emote,
    system,
    whisper,
};

const ChatMsg = struct {
    sender_id: u64,
    sender_name: []u8,
    sender_faction: chat.Faction,
    sender_level: u16,
    content: []u8,
    timestamp_ms: i64,
    kind: ChatMsgKind,
    whisper_target: u64,
};

const ChatRoomState = struct {
    id: u64,
    name: []u8,
    topic: []u8,
    member_count: u32,
    messages: std.ArrayList(ChatMsg),
};

const ChatRoomSession = struct {
    service: *ChatService,
    room: *ChatRoomState,
    sender_id: u64,
    sender_name: []u8,
    sender_faction: chat.Faction,
    sender_level: u16,
    server: chat.ChatRoom.Server,
};

const ChatService = struct {
    allocator: Allocator,
    next_room_id: u64 = 1,
    rooms: std.StringHashMap(*ChatRoomState),
    room_sessions: std.ArrayList(*ChatRoomSession),
    server: chat.ChatService.Server,

    fn init(allocator: Allocator) ChatService {
        return .{
            .allocator = allocator,
            .rooms = std.StringHashMap(*ChatRoomState).init(allocator),
            .room_sessions = std.ArrayList(*ChatRoomSession){},
            .server = .{
                .ctx = undefined,
                .vtable = .{
                    .createRoom = onCreateRoom,
                    .joinRoom = onJoinRoom,
                    .listRooms = onListRooms,
                    .whisper = onWhisper,
                },
            },
        };
    }

    fn bind(self: *ChatService) void {
        self.server.ctx = self;
    }

    fn deinit(self: *ChatService) void {
        var it = self.rooms.valueIterator();
        while (it.next()) |room_ptr| {
            var room = room_ptr.*;
            for (room.messages.items) |msg| {
                self.allocator.free(msg.sender_name);
                self.allocator.free(msg.content);
            }
            room.messages.deinit(self.allocator);
            self.allocator.free(room.name);
            self.allocator.free(room.topic);
            self.allocator.destroy(room);
        }
        self.rooms.deinit();

        for (self.room_sessions.items) |session| {
            if (!std.mem.eql(u8, session.sender_name, "system")) {
                self.allocator.free(session.sender_name);
            }
            self.allocator.destroy(session);
        }
        self.room_sessions.deinit(self.allocator);
    }

    fn createRoomSession(
        self: *ChatService,
        room: *ChatRoomState,
        sender_id: u64,
        sender_name: []const u8,
        sender_faction: chat.Faction,
        sender_level: u16,
    ) !*ChatRoomSession {
        const session = try self.allocator.create(ChatRoomSession);
        session.* = .{
            .service = self,
            .room = room,
            .sender_id = sender_id,
            .sender_name = if (std.mem.eql(u8, sender_name, "system"))
                @constCast("system")
            else
                try self.allocator.dupe(u8, sender_name),
            .sender_faction = sender_faction,
            .sender_level = sender_level,
            .server = .{
                .ctx = undefined,
                .vtable = .{
                    .sendMessage = onSendMessage,
                    .sendEmote = onSendEmote,
                    .getHistory = onGetHistory,
                    .getInfo = onGetInfo,
                    .leave = onLeave,
                },
            },
        };
        session.server.ctx = session;
        try self.room_sessions.append(self.allocator, session);
        return session;
    }
};

const ItemAttr = struct {
    name: []u8,
    value: i32,
};

const InventorySlotState = struct {
    slot_index: u16,
    item_id: u64,
    item_name: []u8,
    rarity: inventory.Rarity,
    item_level: u16,
    stack_size: u32,
    quantity: u32,
    attributes: std.ArrayList(ItemAttr),
};

const PlayerInventory = struct {
    slots: std.ArrayList(InventorySlotState),
    capacity: u16,
};

const TradeSessionState = struct {
    initiator_id: u64,
    target_id: u64,
    state: inventory.TradeState,
    my_offer_slots: std.ArrayList(u16),
};

const TradeSessionServerState = struct {
    service: *InventoryService,
    trade: *TradeSessionState,
    server: inventory.TradeSession.Server,
};

const InventoryService = struct {
    allocator: Allocator,
    inventories: std.AutoHashMap(u64, *PlayerInventory),
    trade_sessions: std.ArrayList(*TradeSessionServerState),
    server: inventory.InventoryService.Server,

    fn init(allocator: Allocator) InventoryService {
        return .{
            .allocator = allocator,
            .inventories = std.AutoHashMap(u64, *PlayerInventory).init(allocator),
            .trade_sessions = std.ArrayList(*TradeSessionServerState){},
            .server = .{
                .ctx = undefined,
                .vtable = .{
                    .getInventory = onGetInventory,
                    .addItem = onAddItem,
                    .removeItem = onRemoveItem,
                    .startTrade = onStartTrade,
                    .filterByRarity = onFilterByRarity,
                },
            },
        };
    }

    fn bind(self: *InventoryService) void {
        self.server.ctx = self;
    }

    fn deinit(self: *InventoryService) void {
        var it = self.inventories.valueIterator();
        while (it.next()) |inv_ptr| {
            var inv = inv_ptr.*;
            for (inv.slots.items) |*slot| {
                self.allocator.free(slot.item_name);
                for (slot.attributes.items) |attr| {
                    self.allocator.free(attr.name);
                }
                slot.attributes.deinit(self.allocator);
            }
            inv.slots.deinit(self.allocator);
            self.allocator.destroy(inv);
        }
        self.inventories.deinit();

        for (self.trade_sessions.items) |session| {
            session.trade.my_offer_slots.deinit(self.allocator);
            self.allocator.destroy(session.trade);
            self.allocator.destroy(session);
        }
        self.trade_sessions.deinit(self.allocator);
    }

    fn getOrCreateInventory(self: *InventoryService, player_id: u64) !*PlayerInventory {
        if (self.inventories.get(player_id)) |existing| return existing;

        const inv = try self.allocator.create(PlayerInventory);
        inv.* = .{
            .slots = std.ArrayList(InventorySlotState){},
            .capacity = 20,
        };
        try self.inventories.put(player_id, inv);
        return inv;
    }

    fn findSlot(inv: *PlayerInventory, slot_index: u16) ?*InventorySlotState {
        for (inv.slots.items) |*slot| {
            if (slot.slot_index == slot_index) return slot;
        }
        return null;
    }
};

const MatchPlayer = struct {
    id: u64,
    name: []u8,
    faction: matchmaking.Faction,
    level: u16,
};

const QueueEntry = struct {
    ticket_id: u64,
    player: MatchPlayer,
    mode: matchmaking.GameMode,
    enqueued_at: i64,
};

const PlayerStat = struct {
    player: MatchPlayer,
    kills: u32,
    deaths: u32,
    assists: u32,
    score: i32,
};

const MatchStateData = struct {
    id: u64,
    mode: matchmaking.GameMode,
    state: matchmaking.MatchState,
    team_a: std.ArrayList(MatchPlayer),
    team_b: std.ArrayList(MatchPlayer),
    created_at: i64,
    ready_set: std.AutoHashMap(u64, bool),
};

const MatchResultData = struct {
    match_id: u64,
    winning_team: u8,
    duration: u32,
    player_stats: std.ArrayList(PlayerStat),
};

const MatchControllerServerState = struct {
    service: *MatchmakingService,
    match_id: u64,
    server: matchmaking.MatchController.Server,
};

const MatchmakingService = struct {
    allocator: Allocator,
    next_ticket: u64 = 1,
    next_match: u64 = 1,
    queue: std.AutoHashMap(u64, QueueEntry),
    matches: std.AutoHashMap(u64, *MatchStateData),
    results: std.AutoHashMap(u64, *MatchResultData),
    controllers: std.ArrayList(*MatchControllerServerState),
    server: matchmaking.MatchmakingService.Server,

    fn init(allocator: Allocator) MatchmakingService {
        return .{
            .allocator = allocator,
            .queue = std.AutoHashMap(u64, QueueEntry).init(allocator),
            .matches = std.AutoHashMap(u64, *MatchStateData).init(allocator),
            .results = std.AutoHashMap(u64, *MatchResultData).init(allocator),
            .controllers = std.ArrayList(*MatchControllerServerState){},
            .server = .{
                .ctx = undefined,
                .vtable = .{
                    .enqueue = onEnqueue,
                    .dequeue = onDequeue,
                    .findMatch = onFindMatch,
                    .getQueueStats = onGetQueueStats,
                    .getMatchResult = onGetMatchResult,
                },
            },
        };
    }

    fn bind(self: *MatchmakingService) void {
        self.server.ctx = self;
    }

    fn deinit(self: *MatchmakingService) void {
        var queue_it = self.queue.valueIterator();
        while (queue_it.next()) |entry| {
            self.allocator.free(entry.player.name);
        }
        self.queue.deinit();

        var match_it = self.matches.valueIterator();
        while (match_it.next()) |match_ptr| {
            var m = match_ptr.*;
            for (m.team_a.items) |p| self.allocator.free(p.name);
            for (m.team_b.items) |p| self.allocator.free(p.name);
            m.team_a.deinit(self.allocator);
            m.team_b.deinit(self.allocator);
            m.ready_set.deinit();
            self.allocator.destroy(m);
        }
        self.matches.deinit();

        var result_it = self.results.valueIterator();
        while (result_it.next()) |result_ptr| {
            var r = result_ptr.*;
            for (r.player_stats.items) |ps| {
                self.allocator.free(ps.player.name);
            }
            r.player_stats.deinit(self.allocator);
            self.allocator.destroy(r);
        }
        self.results.deinit();

        for (self.controllers.items) |controller| {
            self.allocator.destroy(controller);
        }
        self.controllers.deinit(self.allocator);
    }
};

fn parseSchema(text: []const u8) !Schema {
    if (std.mem.eql(u8, text, "game_world")) return .game_world;
    if (std.mem.eql(u8, text, "chat")) return .chat;
    if (std.mem.eql(u8, text, "inventory")) return .inventory;
    if (std.mem.eql(u8, text, "matchmaking")) return .matchmaking;
    return error.InvalidSchema;
}

fn parseArgs(allocator: Allocator) !CliArgs {
    var out = CliArgs{};
    var host_text: []const u8 = out.host;

    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    var idx: usize = 1;
    while (idx < argv.len) : (idx += 1) {
        const arg = argv[idx];
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            return error.HelpRequested;
        }
        if (std.mem.eql(u8, arg, "--host")) {
            idx += 1;
            if (idx >= argv.len) return error.MissingArgValue;
            host_text = argv[idx];
            continue;
        }
        if (std.mem.eql(u8, arg, "--port")) {
            idx += 1;
            if (idx >= argv.len) return error.MissingArgValue;
            out.port = try std.fmt.parseInt(u16, argv[idx], 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--schema")) {
            idx += 1;
            if (idx >= argv.len) return error.MissingArgValue;
            out.schema = try parseSchema(argv[idx]);
            continue;
        }
        if (std.mem.eql(u8, arg, "--listen-fd")) {
            idx += 1;
            if (idx >= argv.len) return error.MissingArgValue;
            out.listen_fd = try std.fmt.parseInt(std.posix.fd_t, argv[idx], 10);
            continue;
        }
    }

    out.host = try allocator.dupe(u8, host_text);
    return out;
}

fn nowMillis() i64 {
    return std.time.milliTimestamp();
}

fn statusOk(comptime T: type) T {
    return @field(T, "Ok");
}

fn statusNotFound(comptime T: type) T {
    return @field(T, "NotFound");
}

fn statusAlreadyExists(comptime T: type) T {
    return @field(T, "AlreadyExists");
}

fn statusResourceExhausted(comptime T: type) T {
    return @field(T, "ResourceExhausted");
}

fn statusInvalidArgument(comptime T: type) T {
    return @field(T, "InvalidArgument");
}

fn distance3(x1: f32, y1: f32, z1: f32, x2: f32, y2: f32, z2: f32) f32 {
    const dx = x1 - x2;
    const dy = y1 - y2;
    const dz = z1 - z2;
    return @sqrt(dx * dx + dy * dy + dz * dz);
}

fn parseGameWorldFilter(query: game_world.AreaQuery.Reader) union(enum) {
    all,
    by_kind: game_world.EntityKind,
    by_faction: game_world.Faction,
} {
    // `AreaQuery.filter` union helpers are not generated yet.
    // Discriminant and payload are read directly from the struct data section.
    const discriminant = query._reader.readU16(4);
    switch (discriminant) {
        1 => {
            const raw_kind = query._reader.readU16(6);
            return .{ .by_kind = @enumFromInt(raw_kind) };
        },
        2 => {
            const raw_faction = query._reader.readU16(6);
            return .{ .by_faction = @enumFromInt(raw_faction) };
        },
        else => return .all,
    }
}

fn fillGameEntity(builder: *game_world.Entity.Builder, entity: *const GameEntity) !void {
    var id = try builder.initId();
    try id.setId(entity.id);
    try builder.setKind(entity.kind);
    try builder.setName(entity.name);
    var pos = try builder.initPosition();
    try pos.setX(entity.x);
    try pos.setY(entity.y);
    try pos.setZ(entity.z);
    try builder.setHealth(entity.health);
    try builder.setMaxHealth(entity.max_health);
    try builder.setFaction(entity.faction);
    try builder.setAlive(entity.alive);
}

fn onSpawnEntity(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: game_world.GameWorld.SpawnEntity.Params.Reader,
    results: *game_world.GameWorld.SpawnEntity.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *GameWorldService = @ptrCast(@alignCast(ctx_ptr));
    const request = try params.getRequest();

    const name = try request.getName();
    const position = try request.getPosition();

    const id = service.next_entity_id;
    service.next_entity_id += 1;

    const entity = GameEntity{
        .id = id,
        .kind = try request.getKind(),
        .name = try service.allocator.dupe(u8, name),
        .x = try position.getX(),
        .y = try position.getY(),
        .z = try position.getZ(),
        .health = try request.getMaxHealth(),
        .max_health = try request.getMaxHealth(),
        .faction = try request.getFaction(),
        .alive = true,
    };

    try service.entities.put(id, entity);

    var out_entity = try results.initEntity();
    try fillGameEntity(&out_entity, &entity);
    try results.setStatus(statusOk(game_world.StatusCode));
}

fn onDespawnEntity(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: game_world.GameWorld.DespawnEntity.Params.Reader,
    results: *game_world.GameWorld.DespawnEntity.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *GameWorldService = @ptrCast(@alignCast(ctx_ptr));
    const id_reader = try params.getId();
    const id = try id_reader.getId();

    if (service.entities.fetchRemove(id)) |entry| {
        service.allocator.free(entry.value.name);
        try results.setStatus(statusOk(game_world.StatusCode));
    } else {
        try results.setStatus(statusNotFound(game_world.StatusCode));
    }
}

fn onGetEntity(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: game_world.GameWorld.GetEntity.Params.Reader,
    results: *game_world.GameWorld.GetEntity.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *GameWorldService = @ptrCast(@alignCast(ctx_ptr));
    const id_reader = try params.getId();
    const id = try id_reader.getId();

    if (service.entities.get(id)) |entity| {
        var out_entity = try results.initEntity();
        try fillGameEntity(&out_entity, &entity);
        try results.setStatus(statusOk(game_world.StatusCode));
    } else {
        try results.setStatus(statusNotFound(game_world.StatusCode));
    }
}

fn onMoveEntity(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: game_world.GameWorld.MoveEntity.Params.Reader,
    results: *game_world.GameWorld.MoveEntity.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *GameWorldService = @ptrCast(@alignCast(ctx_ptr));
    const id_reader = try params.getId();
    const id = try id_reader.getId();

    const new_pos = try params.getNewPosition();
    const x = try new_pos.getX();
    const y = try new_pos.getY();
    const z = try new_pos.getZ();

    if (service.entities.getPtr(id)) |entity| {
        entity.x = x;
        entity.y = y;
        entity.z = z;

        var out_entity = try results.initEntity();
        try fillGameEntity(&out_entity, entity);
        try results.setStatus(statusOk(game_world.StatusCode));
    } else {
        try results.setStatus(statusNotFound(game_world.StatusCode));
    }
}

fn onDamageEntity(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: game_world.GameWorld.DamageEntity.Params.Reader,
    results: *game_world.GameWorld.DamageEntity.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *GameWorldService = @ptrCast(@alignCast(ctx_ptr));
    const id_reader = try params.getId();
    const id = try id_reader.getId();
    const amount = try params.getAmount();

    if (service.entities.getPtr(id)) |entity| {
        entity.health -= amount;
        var killed = false;
        if (entity.health <= 0) {
            entity.health = 0;
            entity.alive = false;
            killed = true;
        }

        var out_entity = try results.initEntity();
        try fillGameEntity(&out_entity, entity);
        try results.setKilled(killed);
        try results.setStatus(statusOk(game_world.StatusCode));
    } else {
        try results.setKilled(false);
        try results.setStatus(statusNotFound(game_world.StatusCode));
    }
}

fn onQueryArea(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: game_world.GameWorld.QueryArea.Params.Reader,
    results: *game_world.GameWorld.QueryArea.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *GameWorldService = @ptrCast(@alignCast(ctx_ptr));
    const query = try params.getQuery();
    const center = try query.getCenter();

    const cx = try center.getX();
    const cy = try center.getY();
    const cz = try center.getZ();
    const radius = try query.getRadius();

    const filter = parseGameWorldFilter(query);

    var matched_ids = std.ArrayList(u64){};
    defer matched_ids.deinit(service.allocator);

    var it = service.entities.iterator();
    while (it.next()) |entry| {
        const entity = entry.value_ptr.*;
        if (distance3(entity.x, entity.y, entity.z, cx, cy, cz) > radius) continue;

        switch (filter) {
            .all => {},
            .by_kind => |kind| if (entity.kind != kind) continue,
            .by_faction => |faction| if (entity.faction != faction) continue,
        }

        try matched_ids.append(service.allocator, entry.key_ptr.*);
    }

    const list = try results.initEntities(@intCast(matched_ids.items.len));
    for (matched_ids.items, 0..) |id, idx| {
        if (service.entities.get(id)) |entity| {
            var dst = try list.get(@intCast(idx));
            try fillGameEntity(&dst, &entity);
        }
    }

    try results.setCount(@intCast(matched_ids.items.len));
}

fn fillChatRoomInfo(builder: *chat.RoomInfo.Builder, room: *const ChatRoomState) !void {
    var id = try builder.initId();
    try id.setId(room.id);
    try builder.setName(room.name);
    try builder.setMemberCount(room.member_count);
    try builder.setTopic(room.topic);
}

fn setChatMessageKind(builder: *chat.ChatMessage.Builder, kind: ChatMsgKind, whisper_target: u64) !void {
    // `ChatMessage.kind` union helpers are not generated yet.
    // We write the discriminant and payload fields directly.
    switch (kind) {
        .normal => builder._builder.writeU16(0, 0),
        .emote => builder._builder.writeU16(0, 1),
        .system => builder._builder.writeU16(0, 2),
        .whisper => {
            builder._builder.writeU16(0, 3);
            const ptr = try builder._builder.initStruct(3, 1, 0);
            var pid = chat.PlayerId.Builder.wrap(ptr);
            try pid.setId(whisper_target);
        },
    }
}

fn fillChatMessage(builder: *chat.ChatMessage.Builder, msg: *const ChatMsg) !void {
    var sender = try builder.initSender();
    var sender_id = try sender.initId();
    try sender_id.setId(msg.sender_id);
    try sender.setName(msg.sender_name);
    try sender.setFaction(msg.sender_faction);
    try sender.setLevel(msg.sender_level);

    try builder.setContent(msg.content);
    var ts = try builder.initTimestamp();
    try ts.setUnixMillis(msg.timestamp_ms);

    try setChatMessageKind(builder, msg.kind, msg.whisper_target);
}

fn onCreateRoom(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: chat.ChatService.CreateRoom.Params.Reader,
    results: *chat.ChatService.CreateRoom.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *ChatService = @ptrCast(@alignCast(ctx_ptr));

    const room_name = try params.getName();
    const room_topic = try params.getTopic();

    if (service.rooms.get(room_name) != null) {
        try results.setStatus(statusAlreadyExists(chat.StatusCode));
        return;
    }

    const room = try service.allocator.create(ChatRoomState);
    room.* = .{
        .id = service.next_room_id,
        .name = try service.allocator.dupe(u8, room_name),
        .topic = try service.allocator.dupe(u8, room_topic),
        .member_count = 0,
        .messages = std.ArrayList(ChatMsg){},
    };
    service.next_room_id += 1;

    try service.rooms.put(room.name, room);

    const session = try service.createRoomSession(room, 0, "system", .Neutral, 0);
    const cap_id = try chat.ChatRoom.exportServer(peer, &session.server);
    try results.setRoomCapability(.{ .id = cap_id });

    var info = try results.initInfo();
    try fillChatRoomInfo(&info, room);
    try results.setStatus(statusOk(chat.StatusCode));
}

fn onJoinRoom(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: chat.ChatService.JoinRoom.Params.Reader,
    results: *chat.ChatService.JoinRoom.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *ChatService = @ptrCast(@alignCast(ctx_ptr));

    const room_name = try params.getName();
    const room = service.rooms.get(room_name) orelse {
        try results.setStatus(statusNotFound(chat.StatusCode));
        return;
    };

    const player = try params.getPlayer();
    const player_id = try (try player.getId()).getId();
    const player_name = try player.getName();
    const player_faction = try player.getFaction();
    const player_level = try player.getLevel();

    room.member_count += 1;

    const session = try service.createRoomSession(room, player_id, player_name, player_faction, player_level);
    const cap_id = try chat.ChatRoom.exportServer(peer, &session.server);
    try results.setRoomCapability(.{ .id = cap_id });
    try results.setStatus(statusOk(chat.StatusCode));
}

fn onListRooms(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: chat.ChatService.ListRooms.Params.Reader,
    results: *chat.ChatService.ListRooms.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *ChatService = @ptrCast(@alignCast(ctx_ptr));

    var count: usize = 0;
    var it = service.rooms.iterator();
    while (it.next()) |_| count += 1;

    const list = try results.initRooms(@intCast(count));

    var idx: usize = 0;
    var it2 = service.rooms.valueIterator();
    while (it2.next()) |room_ptr| : (idx += 1) {
        var info = try list.get(@intCast(idx));
        try fillChatRoomInfo(&info, room_ptr.*);
    }
}

fn onWhisper(
    _: *anyopaque,
    _: *rpc.peer.Peer,
    params: chat.ChatService.Whisper.Params.Reader,
    results: *chat.ChatService.Whisper.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const from = try params.getFrom();
    const from_id = try (try from.getId()).getId();
    const from_name = try from.getName();
    const from_faction = try from.getFaction();
    const from_level = try from.getLevel();

    const to = try params.getTo();
    const to_id = try to.getId();

    const content = try params.getContent();

    var msg = try results.initMessage();
    var sender = try msg.initSender();
    var sid = try sender.initId();
    try sid.setId(from_id);
    try sender.setName(from_name);
    try sender.setFaction(from_faction);
    try sender.setLevel(from_level);

    try msg.setContent(content);
    var ts = try msg.initTimestamp();
    try ts.setUnixMillis(nowMillis());
    try setChatMessageKind(&msg, .whisper, to_id);

    try results.setStatus(statusOk(chat.StatusCode));
}

fn onSendMessage(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: chat.ChatRoom.SendMessage.Params.Reader,
    results: *chat.ChatRoom.SendMessage.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const session: *ChatRoomSession = @ptrCast(@alignCast(ctx_ptr));

    const content = try params.getContent();
    const stored = ChatMsg{
        .sender_id = session.sender_id,
        .sender_name = try session.service.allocator.dupe(u8, session.sender_name),
        .sender_faction = session.sender_faction,
        .sender_level = session.sender_level,
        .content = try session.service.allocator.dupe(u8, content),
        .timestamp_ms = nowMillis(),
        .kind = .normal,
        .whisper_target = 0,
    };
    try session.room.messages.append(session.service.allocator, stored);

    var msg = try results.initMessage();
    try fillChatMessage(&msg, &stored);
    try results.setStatus(statusOk(chat.StatusCode));
}

fn onSendEmote(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: chat.ChatRoom.SendEmote.Params.Reader,
    results: *chat.ChatRoom.SendEmote.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const session: *ChatRoomSession = @ptrCast(@alignCast(ctx_ptr));

    const content = try params.getContent();
    const stored = ChatMsg{
        .sender_id = session.sender_id,
        .sender_name = try session.service.allocator.dupe(u8, session.sender_name),
        .sender_faction = session.sender_faction,
        .sender_level = session.sender_level,
        .content = try session.service.allocator.dupe(u8, content),
        .timestamp_ms = nowMillis(),
        .kind = .emote,
        .whisper_target = 0,
    };
    try session.room.messages.append(session.service.allocator, stored);

    var msg = try results.initMessage();
    try fillChatMessage(&msg, &stored);
    try results.setStatus(statusOk(chat.StatusCode));
}

fn onGetHistory(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: chat.ChatRoom.GetHistory.Params.Reader,
    results: *chat.ChatRoom.GetHistory.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const session: *ChatRoomSession = @ptrCast(@alignCast(ctx_ptr));
    const limit = try params.getLimit();

    const total = session.room.messages.items.len;
    var start: usize = 0;
    if (limit > 0 and total > limit) {
        start = total - @as(usize, @intCast(limit));
    }

    const count = total - start;
    const out = try results.initMessages(@intCast(count));

    var idx: usize = 0;
    var pos = start;
    while (pos < total) : (pos += 1) {
        const msg = session.room.messages.items[pos];
        var dst = try out.get(@intCast(idx));
        try fillChatMessage(&dst, &msg);
        idx += 1;
    }
}

fn onGetInfo(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: chat.ChatRoom.GetInfo.Params.Reader,
    results: *chat.ChatRoom.GetInfo.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const session: *ChatRoomSession = @ptrCast(@alignCast(ctx_ptr));
    var info = try results.initInfo();
    try fillChatRoomInfo(&info, session.room);
}

fn onLeave(
    _: *anyopaque,
    _: *rpc.peer.Peer,
    _: chat.ChatRoom.Leave.Params.Reader,
    results: *chat.ChatRoom.Leave.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    try results.setStatus(statusOk(chat.StatusCode));
}

fn fillInventorySlot(builder: *inventory.InventorySlot.Builder, slot: *const InventorySlotState) !void {
    try builder.setSlotIndex(slot.slot_index);

    var item = try builder.initItem();
    var item_id = try item.initId();
    try item_id.setId(slot.item_id);
    try item.setName(slot.item_name);
    try item.setRarity(slot.rarity);
    try item.setLevel(slot.item_level);
    try item.setStackSize(slot.stack_size);

    const attrs = try item.initAttributes(@intCast(slot.attributes.items.len));
    for (slot.attributes.items, 0..) |attr, idx| {
        var dst = try attrs.get(@intCast(idx));
        try dst.setName(attr.name);
        try dst.setValue(attr.value);
    }

    try builder.setQuantity(slot.quantity);
}

fn onGetInventory(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: inventory.InventoryService.GetInventory.Params.Reader,
    results: *inventory.InventoryService.GetInventory.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *InventoryService = @ptrCast(@alignCast(ctx_ptr));
    const player = try params.getPlayer();
    const player_id = try player.getId();

    const inv = try service.getOrCreateInventory(player_id);

    var out = try results.initInventory();
    var owner = try out.initOwner();
    try owner.setId(player_id);

    const slots = try out.initSlots(@intCast(inv.slots.items.len));
    for (inv.slots.items, 0..) |slot, idx| {
        var dst = try slots.get(@intCast(idx));
        try fillInventorySlot(&dst, &slot);
    }

    try out.setCapacity(inv.capacity);
    try out.setUsedSlots(@intCast(inv.slots.items.len));
    try results.setStatus(statusOk(inventory.StatusCode));
}

fn onAddItem(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: inventory.InventoryService.AddItem.Params.Reader,
    results: *inventory.InventoryService.AddItem.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *InventoryService = @ptrCast(@alignCast(ctx_ptr));

    const player = try params.getPlayer();
    const player_id = try player.getId();
    const item = try params.getItem();
    const quantity = try params.getQuantity();

    const inv = try service.getOrCreateInventory(player_id);
    if (inv.slots.items.len >= inv.capacity) {
        try results.setStatus(statusResourceExhausted(inventory.StatusCode));
        return;
    }

    const item_id = try (try item.getId()).getId();
    const item_name = try item.getName();
    const rarity = try item.getRarity();
    const item_level = try item.getLevel();
    const stack_size = try item.getStackSize();

    var attrs = std.ArrayList(ItemAttr){};
    if (!item._reader.isPointerNull(2)) {
        const in_attrs = try item.getAttributes();
        var i: u32 = 0;
        while (i < in_attrs.len()) : (i += 1) {
            const attr = try in_attrs.get(i);
            try attrs.append(service.allocator, .{
                .name = try service.allocator.dupe(u8, try attr.getName()),
                .value = try attr.getValue(),
            });
        }
    }

    const slot = InventorySlotState{
        .slot_index = @intCast(inv.slots.items.len),
        .item_id = item_id,
        .item_name = try service.allocator.dupe(u8, item_name),
        .rarity = rarity,
        .item_level = item_level,
        .stack_size = stack_size,
        .quantity = quantity,
        .attributes = attrs,
    };

    try inv.slots.append(service.allocator, slot);

    var out_slot = try results.initSlot();
    try fillInventorySlot(&out_slot, &inv.slots.items[inv.slots.items.len - 1]);
    try results.setStatus(statusOk(inventory.StatusCode));
}

fn onRemoveItem(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: inventory.InventoryService.RemoveItem.Params.Reader,
    results: *inventory.InventoryService.RemoveItem.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *InventoryService = @ptrCast(@alignCast(ctx_ptr));

    const player = try params.getPlayer();
    const player_id = try player.getId();
    const slot_index = try params.getSlotIndex();
    const quantity = try params.getQuantity();

    const inv = try service.getOrCreateInventory(player_id);

    if (InventoryService.findSlot(inv, slot_index)) |slot| {
        if (quantity >= slot.quantity) {
            var idx: usize = 0;
            while (idx < inv.slots.items.len) : (idx += 1) {
                if (inv.slots.items[idx].slot_index == slot_index) {
                    var removed = inv.slots.swapRemove(idx);
                    service.allocator.free(removed.item_name);
                    for (removed.attributes.items) |attr| {
                        service.allocator.free(attr.name);
                    }
                    removed.attributes.deinit(service.allocator);
                    break;
                }
            }
        } else {
            slot.quantity -= quantity;
        }
        try results.setStatus(statusOk(inventory.StatusCode));
    } else {
        try results.setStatus(statusNotFound(inventory.StatusCode));
    }
}

fn onStartTrade(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: inventory.InventoryService.StartTrade.Params.Reader,
    results: *inventory.InventoryService.StartTrade.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *InventoryService = @ptrCast(@alignCast(ctx_ptr));

    const initiator = try params.getInitiator();
    const target = try params.getTarget();

    const trade = try service.allocator.create(TradeSessionState);
    trade.* = .{
        .initiator_id = try initiator.getId(),
        .target_id = try target.getId(),
        .state = .Proposing,
        .my_offer_slots = std.ArrayList(u16){},
    };

    const server_state = try service.allocator.create(TradeSessionServerState);
    server_state.* = .{
        .service = service,
        .trade = trade,
        .server = .{
            .ctx = undefined,
            .vtable = .{
                .offerItems = onOfferItems,
                .removeItems = onRemoveTradeItems,
                .accept = onAcceptTrade,
                .confirm = onConfirmTrade,
                .cancel = onCancelTrade,
                .viewOtherOffer = onViewOtherOffer,
                .getState = onGetTradeState,
            },
        },
    };
    server_state.server.ctx = server_state;
    try service.trade_sessions.append(service.allocator, server_state);

    const cap_id = try inventory.TradeSession.exportServer(peer, &server_state.server);
    try results.setSessionCapability(.{ .id = cap_id });
    try results.setStatus(statusOk(inventory.StatusCode));
}

fn onFilterByRarity(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: inventory.InventoryService.FilterByRarity.Params.Reader,
    results: *inventory.InventoryService.FilterByRarity.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *InventoryService = @ptrCast(@alignCast(ctx_ptr));

    const player = try params.getPlayer();
    const player_id = try player.getId();
    const min_rarity = try params.getMinRarity();

    const inv = try service.getOrCreateInventory(player_id);

    var matches = std.ArrayList(usize){};
    defer matches.deinit(service.allocator);

    for (inv.slots.items, 0..) |slot, idx| {
        if (@intFromEnum(slot.rarity) >= @intFromEnum(min_rarity)) {
            try matches.append(service.allocator, idx);
        }
    }

    const out = try results.initItems(@intCast(matches.items.len));
    for (matches.items, 0..) |slot_idx, idx| {
        var dst = try out.get(@intCast(idx));
        try fillInventorySlot(&dst, &inv.slots.items[slot_idx]);
    }
}

fn buildTradeOffer(
    service: *InventoryService,
    trade: *TradeSessionState,
    offer: *inventory.TradeOffer.Builder,
) !void {
    const inv = try service.getOrCreateInventory(trade.initiator_id);

    var offered_indices = std.ArrayList(usize){};
    defer offered_indices.deinit(service.allocator);

    for (trade.my_offer_slots.items) |slot_index| {
        var idx: usize = 0;
        while (idx < inv.slots.items.len) : (idx += 1) {
            if (inv.slots.items[idx].slot_index == slot_index) {
                try offered_indices.append(service.allocator, idx);
                break;
            }
        }
    }

    const items = try offer.initOfferedItems(@intCast(offered_indices.items.len));
    for (offered_indices.items, 0..) |slot_idx, idx| {
        var dst = try items.get(@intCast(idx));
        try fillInventorySlot(&dst, &inv.slots.items[slot_idx]);
    }
    try offer.setAccepted(false);
}

fn onOfferItems(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: inventory.TradeSession.OfferItems.Params.Reader,
    results: *inventory.TradeSession.OfferItems.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const state: *TradeSessionServerState = @ptrCast(@alignCast(ctx_ptr));

    state.trade.my_offer_slots.clearRetainingCapacity();
    const slots = try params.getSlots();
    var i: u32 = 0;
    while (i < slots.len()) : (i += 1) {
        try state.trade.my_offer_slots.append(state.service.allocator, try slots.get(i));
    }

    var offer = try results.initOffer();
    try buildTradeOffer(state.service, state.trade, &offer);
    try results.setStatus(statusOk(inventory.StatusCode));
}

fn onRemoveTradeItems(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: inventory.TradeSession.RemoveItems.Params.Reader,
    results: *inventory.TradeSession.RemoveItems.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const state: *TradeSessionServerState = @ptrCast(@alignCast(ctx_ptr));

    state.trade.my_offer_slots.clearRetainingCapacity();

    var offer = try results.initOffer();
    try offer.setAccepted(false);
    _ = try offer.initOfferedItems(0);
    try results.setStatus(statusOk(inventory.StatusCode));
}

fn onAcceptTrade(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: inventory.TradeSession.Accept.Params.Reader,
    results: *inventory.TradeSession.Accept.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const state: *TradeSessionServerState = @ptrCast(@alignCast(ctx_ptr));

    state.trade.state = .Accepted;
    try results.setState(.Accepted);
    try results.setStatus(statusOk(inventory.StatusCode));
}

fn onConfirmTrade(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: inventory.TradeSession.Confirm.Params.Reader,
    results: *inventory.TradeSession.Confirm.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const state: *TradeSessionServerState = @ptrCast(@alignCast(ctx_ptr));

    state.trade.state = .Confirmed;
    try results.setState(.Confirmed);
    try results.setStatus(statusOk(inventory.StatusCode));
}

fn onCancelTrade(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: inventory.TradeSession.Cancel.Params.Reader,
    results: *inventory.TradeSession.Cancel.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const state: *TradeSessionServerState = @ptrCast(@alignCast(ctx_ptr));

    state.trade.state = .Cancelled;
    try results.setState(.Cancelled);
}

fn onViewOtherOffer(
    _: *anyopaque,
    _: *rpc.peer.Peer,
    _: inventory.TradeSession.ViewOtherOffer.Params.Reader,
    results: *inventory.TradeSession.ViewOtherOffer.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    var offer = try results.initOffer();
    _ = try offer.initOfferedItems(0);
    try offer.setAccepted(false);
}

fn onGetTradeState(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: inventory.TradeSession.GetState.Params.Reader,
    results: *inventory.TradeSession.GetState.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const state: *TradeSessionServerState = @ptrCast(@alignCast(ctx_ptr));
    try results.setState(state.trade.state);
}

fn fillMatchPlayer(builder: *matchmaking.PlayerInfo.Builder, p: *const MatchPlayer) !void {
    var id = try builder.initId();
    try id.setId(p.id);
    try builder.setName(p.name);
    try builder.setFaction(p.faction);
    try builder.setLevel(p.level);
}

fn fillMatchInfo(builder: *matchmaking.MatchInfo.Builder, match_state: *const MatchStateData) !void {
    var id = try builder.initId();
    try id.setId(match_state.id);
    try builder.setMode(match_state.mode);
    try builder.setState(match_state.state);

    const team_a = try builder.initTeamA(@intCast(match_state.team_a.items.len));
    for (match_state.team_a.items, 0..) |p, idx| {
        var dst = try team_a.get(@intCast(idx));
        try fillMatchPlayer(&dst, &p);
    }

    const team_b = try builder.initTeamB(@intCast(match_state.team_b.items.len));
    for (match_state.team_b.items, 0..) |p, idx| {
        var dst = try team_b.get(@intCast(idx));
        try fillMatchPlayer(&dst, &p);
    }

    var created = try builder.initCreatedAt();
    try created.setUnixMillis(match_state.created_at);
}

fn onEnqueue(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: matchmaking.MatchmakingService.Enqueue.Params.Reader,
    results: *matchmaking.MatchmakingService.Enqueue.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *MatchmakingService = @ptrCast(@alignCast(ctx_ptr));

    const player = try params.getPlayer();
    const player_id = try (try player.getId()).getId();
    const player_name = try player.getName();
    const player_faction = try player.getFaction();
    const player_level = try player.getLevel();

    const mode = try params.getMode();
    const ticket_id = service.next_ticket;
    service.next_ticket += 1;

    const entry = QueueEntry{
        .ticket_id = ticket_id,
        .player = .{
            .id = player_id,
            .name = try service.allocator.dupe(u8, player_name),
            .faction = player_faction,
            .level = player_level,
        },
        .mode = mode,
        .enqueued_at = nowMillis(),
    };

    try service.queue.put(ticket_id, entry);

    var ticket = try results.initTicket();
    try ticket.setTicketId(ticket_id);
    var out_player = try ticket.initPlayer();
    try fillMatchPlayer(&out_player, &entry.player);
    try ticket.setMode(mode);
    var enqueued_at = try ticket.initEnqueuedAt();
    try enqueued_at.setUnixMillis(entry.enqueued_at);
    try ticket.setEstimatedWaitSecs(30);
    try results.setStatus(statusOk(matchmaking.StatusCode));
}

fn onDequeue(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: matchmaking.MatchmakingService.Dequeue.Params.Reader,
    results: *matchmaking.MatchmakingService.Dequeue.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *MatchmakingService = @ptrCast(@alignCast(ctx_ptr));
    const ticket_id = try params.getTicketId();

    if (service.queue.fetchRemove(ticket_id)) |entry| {
        service.allocator.free(entry.value.player.name);
        try results.setStatus(statusOk(matchmaking.StatusCode));
    } else {
        try results.setStatus(statusNotFound(matchmaking.StatusCode));
    }
}

fn onFindMatch(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: matchmaking.MatchmakingService.FindMatch.Params.Reader,
    results: *matchmaking.MatchmakingService.FindMatch.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *MatchmakingService = @ptrCast(@alignCast(ctx_ptr));

    const player = try params.getPlayer();
    const player_id = try (try player.getId()).getId();
    const player_name = try player.getName();
    const player_faction = try player.getFaction();
    const player_level = try player.getLevel();

    const match_id = service.next_match;
    service.next_match += 1;

    const match_state = try service.allocator.create(MatchStateData);
    match_state.* = .{
        .id = match_id,
        .mode = try params.getMode(),
        .state = .Waiting,
        .team_a = std.ArrayList(MatchPlayer){},
        .team_b = std.ArrayList(MatchPlayer){},
        .created_at = nowMillis(),
        .ready_set = std.AutoHashMap(u64, bool).init(service.allocator),
    };

    try match_state.team_a.append(service.allocator, .{
        .id = player_id,
        .name = try service.allocator.dupe(u8, player_name),
        .faction = player_faction,
        .level = player_level,
    });

    try match_state.team_b.append(service.allocator, .{
        .id = 999,
        .name = try service.allocator.dupe(u8, "Opponent"),
        .faction = .Neutral,
        .level = 10,
    });

    try service.matches.put(match_id, match_state);

    const controller = try service.allocator.create(MatchControllerServerState);
    controller.* = .{
        .service = service,
        .match_id = match_id,
        .server = .{
            .ctx = undefined,
            .vtable = .{
                .getInfo = onControllerGetInfo,
                .signalReady = onControllerSignalReady,
                .reportResult = onControllerReportResult,
                .cancelMatch = onControllerCancelMatch,
            },
        },
    };
    controller.server.ctx = controller;
    try service.controllers.append(service.allocator, controller);

    const cap_id = try matchmaking.MatchController.exportServer(peer, &controller.server);
    try results.setControllerCapability(.{ .id = cap_id });

    var out_match_id = try results.initMatchId();
    try out_match_id.setId(match_id);
}

fn onGetQueueStats(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: matchmaking.MatchmakingService.GetQueueStats.Params.Reader,
    results: *matchmaking.MatchmakingService.GetQueueStats.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *MatchmakingService = @ptrCast(@alignCast(ctx_ptr));
    const mode = try params.getMode();

    var count: u32 = 0;
    var it = service.queue.valueIterator();
    while (it.next()) |entry| {
        if (entry.mode == mode) count += 1;
    }

    try results.setPlayersInQueue(count);
    try results.setAvgWaitSecs(30);
}

fn onGetMatchResult(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: matchmaking.MatchmakingService.GetMatchResult.Params.Reader,
    results: *matchmaking.MatchmakingService.GetMatchResult.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const service: *MatchmakingService = @ptrCast(@alignCast(ctx_ptr));
    const id = try (try params.getId()).getId();

    if (service.results.get(id)) |result_state| {
        var result = try results.initResult();
        var result_id = try result.initMatchId();
        try result_id.setId(result_state.match_id);
        try result.setWinningTeam(result_state.winning_team);
        try result.setDuration(result_state.duration);

        const stats = try result.initPlayerStats(@intCast(result_state.player_stats.items.len));
        for (result_state.player_stats.items, 0..) |ps, idx| {
            var dst = try stats.get(@intCast(idx));
            var player = try dst.initPlayer();
            try fillMatchPlayer(&player, &ps.player);
            try dst.setKills(ps.kills);
            try dst.setDeaths(ps.deaths);
            try dst.setAssists(ps.assists);
            try dst.setScore(ps.score);
        }

        try results.setStatus(statusOk(matchmaking.StatusCode));
    } else {
        try results.setStatus(statusNotFound(matchmaking.StatusCode));
    }
}

fn onControllerGetInfo(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: matchmaking.MatchController.GetInfo.Params.Reader,
    results: *matchmaking.MatchController.GetInfo.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const controller: *MatchControllerServerState = @ptrCast(@alignCast(ctx_ptr));
    const match_state = controller.service.matches.get(controller.match_id) orelse return;

    var info = try results.initInfo();
    try fillMatchInfo(&info, match_state);
}

fn onControllerSignalReady(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: matchmaking.MatchController.SignalReady.Params.Reader,
    results: *matchmaking.MatchController.SignalReady.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const controller: *MatchControllerServerState = @ptrCast(@alignCast(ctx_ptr));
    const match_state = controller.service.matches.get(controller.match_id) orelse {
        try results.setStatus(statusNotFound(matchmaking.StatusCode));
        try results.setAllReady(false);
        return;
    };

    const player = try params.getPlayer();
    const player_id = try player.getId();
    try match_state.ready_set.put(player_id, true);

    if (match_state.state == .Waiting) {
        match_state.state = .Ready;
    }

    try results.setAllReady(true);
    try results.setStatus(statusOk(matchmaking.StatusCode));
}

fn onControllerReportResult(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: matchmaking.MatchController.ReportResult.Params.Reader,
    results: *matchmaking.MatchController.ReportResult.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const controller: *MatchControllerServerState = @ptrCast(@alignCast(ctx_ptr));
    const service = controller.service;

    const match_state = service.matches.get(controller.match_id) orelse {
        try results.setStatus(statusNotFound(matchmaking.StatusCode));
        return;
    };

    const incoming = try params.getResult();
    const incoming_match_id = try (try incoming.getMatchId()).getId();

    const output = try service.allocator.create(MatchResultData);
    output.* = .{
        .match_id = incoming_match_id,
        .winning_team = try incoming.getWinningTeam(),
        .duration = try incoming.getDuration(),
        .player_stats = std.ArrayList(PlayerStat){},
    };

    const stats = try incoming.getPlayerStats();
    var i: u32 = 0;
    while (i < stats.len()) : (i += 1) {
        const ps = try stats.get(i);
        const p = try ps.getPlayer();
        try output.player_stats.append(service.allocator, .{
            .player = .{
                .id = try (try p.getId()).getId(),
                .name = try service.allocator.dupe(u8, try p.getName()),
                .faction = try p.getFaction(),
                .level = try p.getLevel(),
            },
            .kills = try ps.getKills(),
            .deaths = try ps.getDeaths(),
            .assists = try ps.getAssists(),
            .score = try ps.getScore(),
        });
    }

    if (service.results.fetchRemove(controller.match_id)) |old| {
        for (old.value.player_stats.items) |ps| {
            service.allocator.free(ps.player.name);
        }
        old.value.player_stats.deinit(service.allocator);
        service.allocator.destroy(old.value);
    }

    try service.results.put(controller.match_id, output);
    match_state.state = .Completed;

    try results.setStatus(statusOk(matchmaking.StatusCode));
}

fn onControllerCancelMatch(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: matchmaking.MatchController.CancelMatch.Params.Reader,
    results: *matchmaking.MatchController.CancelMatch.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const controller: *MatchControllerServerState = @ptrCast(@alignCast(ctx_ptr));
    const match_state = controller.service.matches.get(controller.match_id) orelse {
        try results.setStatus(statusNotFound(matchmaking.StatusCode));
        return;
    };

    if (match_state.state == .InProgress or match_state.state == .Completed) {
        try results.setStatus(statusInvalidArgument(matchmaking.StatusCode));
        return;
    }

    match_state.state = .Cancelled;
    try results.setStatus(statusOk(matchmaking.StatusCode));
}

fn onPeerError(peer: *rpc.peer.Peer, err: anyerror) void {
    std.log.err("rpc peer error: {s}", .{@errorName(err)});
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}

fn onPeerClose(peer: *rpc.peer.Peer) void {
    const allocator = peer.allocator;
    const conn = peer.takeAttachedConnection(*rpc.connection.Connection);

    peer.deinit();
    allocator.destroy(peer);

    if (conn) |attached| {
        attached.deinit();
        allocator.destroy(attached);
    }
}

fn onAccept(listener: *rpc.runtime.Listener, conn: *rpc.connection.Connection) void {
    const ctx: *ListenerCtx = @fieldParentPtr("listener", listener);

    const peer = ctx.app.allocator.create(rpc.peer.Peer) catch {
        conn.deinit();
        ctx.app.allocator.destroy(conn);
        return;
    };

    peer.* = rpc.peer.Peer.init(ctx.app.allocator, conn);

    const bootstrap_result = switch (ctx.app.schema) {
        .game_world => game_world.GameWorld.setBootstrap(peer, &ctx.app.game_world_service.server),
        .chat => chat.ChatService.setBootstrap(peer, &ctx.app.chat_service.server),
        .inventory => inventory.InventoryService.setBootstrap(peer, &ctx.app.inventory_service.server),
        .matchmaking => matchmaking.MatchmakingService.setBootstrap(peer, &ctx.app.matchmaking_service.server),
    };

    _ = bootstrap_result catch |err| {
        std.log.err("failed to set bootstrap: {s}", .{@errorName(err)});
        peer.deinit();
        ctx.app.allocator.destroy(peer);
        conn.deinit();
        ctx.app.allocator.destroy(conn);
        return;
    };

    peer.start(onPeerError, onPeerClose);
}

fn usage() void {
    std.debug.print(
        \\Usage: e2e-zig-server [--host 0.0.0.0] [--port 4700] [--schema game_world|chat|inventory|matchmaking]\n
    , .{});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = parseArgs(allocator) catch |err| switch (err) {
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

    var app = try App.init(allocator, args.schema);
    defer app.deinit();
    app.bind();

    var listener_ctx = ListenerCtx{
        .app = &app,
        .listener = if (args.listen_fd) |fd|
            rpc.runtime.Listener.initFd(
                allocator,
                &app.runtime.loop,
                fd,
                onAccept,
                .{},
            )
        else
            try rpc.runtime.Listener.init(
                allocator,
                &app.runtime.loop,
                try std.net.Address.parseIp4(args.host, args.port),
                onAccept,
                .{},
            ),
    };
    defer listener_ctx.listener.close();

    listener_ctx.listener.start();

    std.debug.print("READY\n", .{});

    while (true) {
        try app.runtime.run(.once);
    }
}
