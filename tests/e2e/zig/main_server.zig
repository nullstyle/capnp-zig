const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");
const io_backend_options = @import("io_backend_options");

const rpc = capnpc.rpc;
const message = capnpc.message;

const game_types = @import("generated/game_types.zig");
const game_world = @import("generated/game_world.zig");
const chat = @import("generated/chat.zig");
const inventory = @import("generated/inventory.zig");
const matchmaking = @import("generated/matchmaking.zig");
const resolve_disembargo = @import("generated/resolve_disembargo.zig");
const cap_passing = @import("generated/cap_passing.zig");
const cancellation = @import("generated/cancellation.zig");

const Allocator = std.mem.Allocator;

// tools/e2e_self.zig lists these names by hand (`quic_schemas` runs all of
// them over QUIC): add a new schema there too, or `e2e-quic` skips it.
// pipelined_params_deferred is one exception: its checks fail by design
// (a known limit), and only tools/e2e_runner.zig runs it. The cancellation
// schemas are the others: they have no Zig client yet, and only
// tools/e2e_runner.zig runs them.
const Schema = enum {
    game_world,
    chat,
    inventory,
    matchmaking,
    resolve_disembargo,
    // Both serve the same TokenHost (tests/e2e/schemas/cap_passing.capnp);
    // the client picks the flow.
    pass_back,
    pipelined_params,
    // The same TokenHost for the pipelined_params client, but mint() and
    // mintFail() answer late (see `HeldAnswer`).
    pipelined_params_deferred,
    // Both serve the same CancelHost (tests/e2e/schemas/cancellation.capnp);
    // the client picks the flow.
    cancel_id_reuse,
    cancel_pipelined,
};

const Transport = enum { tcp, quic };

const CliArgs = struct {
    host: []const u8 = "0.0.0.0",
    port: u16 = 4700,
    schema: Schema = .game_world,
    transport: Transport = .tcp,
    /// QUIC server identity (PEM file paths). Required with --transport quic.
    /// Owned (duped by parseArgs); main frees them.
    cert_pem: ?[]const u8 = null,
    key_pem: ?[]const u8 = null,
};

const App = struct {
    allocator: Allocator,
    schema: Schema,
    game_world_service: GameWorldService,
    chat_service: ChatService,
    inventory_service: InventoryService,
    matchmaking_service: MatchmakingService,
    reflect_service: ReflectorService,
    token_host_service: TokenHostService,
    cancel_host_service: CancelHostService,

    fn init(allocator: Allocator, io: std.Io, schema: Schema) !App {
        return .{
            .allocator = allocator,
            .schema = schema,
            .game_world_service = GameWorldService.init(allocator),
            .chat_service = ChatService.init(allocator),
            .inventory_service = InventoryService.init(allocator),
            .matchmaking_service = MatchmakingService.init(allocator),
            .reflect_service = ReflectorService.init(),
            .token_host_service = TokenHostService.init(allocator, io, schema == .pipelined_params_deferred),
            .cancel_host_service = CancelHostService.init(allocator, io),
        };
    }

    fn bind(self: *App) void {
        self.game_world_service.bind();
        self.chat_service.bind();
        self.inventory_service.bind();
        self.matchmaking_service.bind();
        self.reflect_service.bind();
        self.token_host_service.bind();
        self.cancel_host_service.bind();
    }

    fn deinit(self: *App) void {
        self.cancel_host_service.deinit();
        self.token_host_service.deinit();
        self.matchmaking_service.deinit();
        self.inventory_service.deinit();
        self.chat_service.deinit();
        self.game_world_service.deinit();
    }
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
    faction: game_types.Faction,
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
    sender_faction: game_types.Faction,
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
    sender_faction: game_types.Faction,
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
            .room_sessions = std.ArrayList(*ChatRoomSession).empty,
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
        sender_faction: game_types.Faction,
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
    rarity: game_types.Rarity,
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
            .trade_sessions = std.ArrayList(*TradeSessionServerState).empty,
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
            .slots = std.ArrayList(InventorySlotState).empty,
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
    faction: game_types.Faction,
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
            .controllers = std.ArrayList(*MatchControllerServerState).empty,
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

// ---------------------------------------------------------------------------
// resolve_disembargo scenario (reference-client -> Zig-server direction).
//
// The Reflector bootstrap implements the server side of the reflected-cap
// resolve/embargo cycle. The reference client hosts its own CallSequence and
// hands it to `reflect(target)`; we import and RETAIN that target, then export
// an UNRESOLVED promise and return it. The client pipelines getNumber calls on
// the promise (they park here), then calls `resolveNow()`, at which point we
// resolve the promise export to the imported target. The runtime forwards the
// parked calls back to the client's CallSequence and drives the client's
// senderLoopback -> receiverLoopback Disembargo handshake.
//
// See tests/rpc/peer/rpc_reflected_resolve_disembargo_test.zig for the
// low-level proof of the exact frame sequence this handler originates.
//
// State is per-connection. The e2e harness runs one connection at a time
// (worker-pool concurrency = 1) and calls reflect() exactly once before
// resolveNow(), so a single instance on the App is sufficient; the ids are
// reset in bind() and captured/consumed within one reflect/resolveNow pair.
// ---------------------------------------------------------------------------
const ReflectorService = struct {
    // The import id of the client's CallSequence target, captured (and its
    // import RETAINED) by reflect() so it outlives that call and can be named
    // as the promise's resolution target in resolveNow().
    target_import_id: ?u32 = null,
    // The export id of the unresolved promise handed back from reflect().
    promise_export_id: ?u32 = null,
    server: resolve_disembargo.Reflector.Server,

    fn init() ReflectorService {
        return .{
            .server = .{
                .ctx = undefined,
                .vtable = .{
                    // reflect() must set releaseParamCaps=false on its Return to
                    // keep the retained target import alive past the call, so it
                    // runs as a DEFERRED handler that owns its ReturnBuilder.
                    .reflect = undefined,
                    .reflect_deferred = onReflect,
                    .resolveNow = onResolveNow,
                    // invokeCap() invokes the caller-supplied cap itself and can
                    // only answer after that inbound getNumber returns, so it runs
                    // as a DEFERRED handler that stashes its ReturnSender.
                    .invokeCap = undefined,
                    .invokeCap_deferred = onInvokeCap,
                    .disconnectNow = onDisconnectNow,
                },
            },
        };
    }

    fn bind(self: *ReflectorService) void {
        self.server.ctx = self;
        self.target_import_id = null;
        self.promise_export_id = null;
    }
};

// Context carried from onReflect into the Return build closure: the promise
// export id to hand back to the client.
const ReflectReturnCtx = struct {
    promise_id: u32,

    fn build(ctx_ptr: *anyopaque, ret: *rpc.wire.protocol.ReturnBuilder) anyerror!void {
        const self: *const ReflectReturnCtx = @ptrCast(@alignCast(ctx_ptr));
        // We RETAINED the client's target import in onReflect, so instruct the
        // client NOT to implicitly release its param caps on this Return. Its
        // CallSequence export therefore survives past reflect() — it is later
        // named as the promise's resolution target. When we are finished with
        // the import (peer teardown) the runtime sends the explicit Release.
        ret.setReleaseParamCaps(false);

        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        const results_builder = try any.initStruct(0, 1);
        var results = resolve_disembargo.Reflector.ReflectResults.Builder.wrap(results_builder);
        // Place the promise export as the `promise` result capability. A
        // promise export is encoded as senderPromise, so the client imports it
        // as a pipelinable promise.
        try results.setPromiseCapability(.{ .id = self.promise_id });
    }
};

fn onReflect(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: resolve_disembargo.Reflector.ReflectParams.Reader,
    caps: *const rpc.caps.table.InboundCapTable,
    sender: resolve_disembargo.Reflector.Reflect.ReturnSender,
) !void {
    const service: *ReflectorService = @ptrCast(@alignCast(ctx_ptr));

    // Resolve the client's CallSequence from params and RETAIN its import so it
    // outlives this handler. `resolveTarget` performs the retain and returns a
    // Client whose `.cap_id` is the import id.
    const target = try params.resolveTarget(peer, caps);
    service.target_import_id = target.cap_id;

    // Export an unresolved promise and return it as `promise`.
    const promise_id = try peer.addPromiseExport();
    service.promise_export_id = promise_id;

    var ret_ctx = ReflectReturnCtx{ .promise_id = promise_id };
    try sender.sendResults(&ret_ctx, ReflectReturnCtx.build);
}

fn onResolveNow(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    _: resolve_disembargo.Reflector.ResolveNowParams.Reader,
    _: *resolve_disembargo.Reflector.ResolveNowResults.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const service: *ReflectorService = @ptrCast(@alignCast(ctx_ptr));

    const promise_id = service.promise_export_id orelse return error.NoPromiseToResolve;
    const target_id = service.target_import_id orelse return error.NoReflectTarget;

    // Resolve the promise export to the retained client target. This sends
    // Resolve(promise -> receiverHosted{target}) to the client and replays every
    // parked pipelined getNumber by forwarding it back out to the client's
    // CallSequence, which triggers the client's Disembargo handshake.
    try peer.resolvePromiseExportToImport(promise_id, target_id);

    // resolveNow() has empty results (): the ResolveNowResults builder is left
    // untouched.
}

// ---------------------------------------------------------------------------
// invokeCap: SERVER-invokes-caller-cap (cap-in-params, distinct direction from
// reflect). The caller hands us one of its OWN CallSequence caps as `cb`; we
// INVOKE it ourselves — cb.getNumber() — rather than reflecting it back, and
// return the observed counter value as `observed`. This proves a distinct
// client-supplied cap is invoked server-side (versus reflect, which loops a
// pipelined call home). Because we answer only after the inbound getNumber
// returns, the handler is DEFERRED: it stashes the outer question's ReturnSender
// in a per-call heap context and completes the return from the getNumber
// callback.
// ---------------------------------------------------------------------------
const InvokeCapCtx = struct {
    sender: resolve_disembargo.Reflector.InvokeCap.ReturnSender,
    observed: u32 = 0,

    fn build(ctx_ptr: *anyopaque, ret: *rpc.wire.protocol.ReturnBuilder) anyerror!void {
        const self: *const InvokeCapCtx = @ptrCast(@alignCast(ctx_ptr));
        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        const results_builder = try any.initStruct(1, 0);
        var results = resolve_disembargo.Reflector.InvokeCapResults.Builder.wrap(results_builder);
        try results.setObserved(self.observed);
    }
};

fn onInvokeCap(
    _: *anyopaque,
    peer: *rpc.peer.Peer,
    params: resolve_disembargo.Reflector.InvokeCapParams.Reader,
    caps: *const rpc.caps.table.InboundCapTable,
    sender: resolve_disembargo.Reflector.InvokeCap.ReturnSender,
) !void {
    // Resolve the caller-supplied CallSequence (retains its import for the
    // duration of the outbound call) and INVOKE it ourselves.
    const cb = try params.resolveCb(peer, caps);

    // Stash the outer question's ReturnSender so we can answer once the inbound
    // getNumber returns. The context is freed in the getNumber callback.
    const ctx = try peer.allocator.create(InvokeCapCtx);
    errdefer peer.allocator.destroy(ctx);
    ctx.* = .{ .sender = sender };

    _ = try cb.callGetNumber(ctx, null, onInvokeCapInnerReturn);
}

fn onInvokeCapInnerReturn(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: resolve_disembargo.CallSequence.GetNumber.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const ctx: *InvokeCapCtx = @ptrCast(@alignCast(ctx_ptr));
    defer ctx.sender.peer.allocator.destroy(ctx);

    const results = response.unwrap() catch |err| {
        // Surface the failure as an exception on the outer invokeCap question.
        ctx.sender.sendException(@errorName(err)) catch {};
        return;
    };
    ctx.observed = try results.getN();
    try ctx.sender.sendResults(ctx, InvokeCapCtx.build);
}

// ---------------------------------------------------------------------------
// disconnectNow: the handler closes its OWN transport so the caller's
// outstanding call returns a DISCONNECT-class error. Self-closing here mirrors
// the peer error/close paths (onPeerError) — the client observes clean
// forceCancelAllQuestions(disconnected) delivery. Issued LAST by every client.
// ---------------------------------------------------------------------------
fn onDisconnectNow(
    _: *anyopaque,
    peer: *rpc.peer.Peer,
    _: resolve_disembargo.Reflector.DisconnectNowParams.Reader,
    _: *resolve_disembargo.Reflector.DisconnectNowResults.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    // Close our side of the transport. The outstanding disconnectNow question
    // never gets a normal Return; the client sees error.Disconnected instead.
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}

// ---------------------------------------------------------------------------
// pass_back and pipelined_params scenarios (reference-client -> Zig-server
// direction). Both serve this TokenHost; the client picks the flow.
//
// - pass_back: the client mints Tokens, then passes them back to check(). They
//   arrive as `receiverHosted` descriptors naming our own exports, so the
//   generated resolveToken must hand us a local Client (origin `.exported`)
//   whose tag() runs our own HostToken. echo() takes a capability the client
//   hosts and returns it with the generated setTokenClient, which must write
//   it back as the client's own capability (`receiverHosted`).
// - pipelined_params: the client passes mint()'s `token` as check()'s param
//   before mint() returns (a `receiverAnswer` descriptor). The Peer resolves
//   it before check() dispatches, because mint() answered synchronously. When
//   the named answer failed (mintFail()), the Peer fails check() with a copy
//   of that answer's exception and check() never runs.
// - pipelined_params_deferred: the pipelined_params client against a host
//   that answers mint() and mintFail() late, as every native-ABI host does
//   (see `HeldAnswer`). The named answer is still pending when check()
//   dispatches, so the param stays `.promised` and resolveToken fails with
//   error.PromiseUnresolved: the "pending answer" limit in
//   docs/supported-surface.md. tools/e2e_known_limits.zig records the
//   checks that fail.
//
// `token_calls` counts every tag() call that reaches one of our HostTokens,
// so check() can report whether its call reached our own object.
// ---------------------------------------------------------------------------

/// A Token minted by the host. Owned by `TokenHostService.tokens`; its export
/// may go away when the client releases it, but the memory stays until
/// `TokenHostService.deinit`.
const HostToken = struct {
    host: *TokenHostService,
    tag: u32,
    server: cap_passing.Token.Server,
};

/// The reason mintFail() fails with. Clients look for it in the exception
/// that check() returns for a Token pipelined on mintFail().
const mint_fail_reason = "pipelined_params: deliberate mint failure";

/// pipelined_params_deferred: a mint() or mintFail() call that the host has
/// not answered yet. The host answers it at the first of:
/// - the end of the next check() handler on the same connection, after
///   check() has tried to resolve its param. Every pipelined_params client
///   sends check() right after the call that it names, so check() always
///   finds that answer pending, however the client splits its writes;
/// - a loop tick on that connection once the call has been held for
///   `held_answer_idle_ms`. This serves a client that waits for mint()
///   before it sends check() (that check() carries `receiverHosted` and
///   passes). It also lets a future Peer that queues the pipelined call
///   until its answer returns pass, instead of deadlocking.
/// Only a client that waits `held_answer_idle_ms` between mint() and its
/// pipelined check() could see the answer first.
const HeldAnswer = struct {
    peer: *rpc.peer.Peer,
    since_ms: i64,
    call: union(enum) {
        mint: struct { sender: cap_passing.TokenHost.Mint.ReturnSender, tag: u32 },
        mint_fail: cap_passing.TokenHost.MintFail.ReturnSender,
    },
};

/// How long a call is held before a tick answers it. A TCP tick fires only
/// after 100 ms with no input (`WorkerPool`'s tick interval); a QUIC tick
/// fires on every loop step. This age bounds both.
const held_answer_idle_ms: i64 = 500;

/// The pipelined_params_deferred host, for the tick and close callbacks,
/// which get no context pointer. Set once in main; null for other schemas.
var deferred_token_host: ?*TokenHostService = null;

const TokenHostService = struct {
    allocator: Allocator,
    io: std.Io,
    tokens: std.ArrayList(*HostToken) = .empty,
    token_calls: u64 = 0,
    /// pipelined_params_deferred only: calls not answered yet, oldest first.
    held: std.ArrayList(HeldAnswer) = .empty,
    server: cap_passing.TokenHost.Server,

    fn init(allocator: Allocator, io: std.Io, hold_answers: bool) TokenHostService {
        var service = initAnswerAtOnce(allocator, io);
        if (hold_answers) {
            service.server.vtable.mint_deferred = onTokenHostMintHeld;
            service.server.vtable.mintFail_deferred = onTokenHostMintFailHeld;
        }
        return service;
    }

    fn nowMs(self: *const TokenHostService) i64 {
        return std.Io.Timestamp.now(self.io, .awake).toMilliseconds();
    }

    fn hold(self: *TokenHostService, peer: *rpc.peer.Peer, call: @FieldType(HeldAnswer, "call")) !void {
        try self.held.append(self.allocator, .{ .peer = peer, .since_ms = self.nowMs(), .call = call });
    }

    /// Answer the held calls of `peer`; with `min_age_ms`, only those held
    /// that long. One at a time, removed before it is answered, so an answer
    /// that dispatches more calls finds the list consistent.
    fn answerHeld(self: *TokenHostService, peer: *rpc.peer.Peer, min_age_ms: ?i64) void {
        const now = self.nowMs();
        var i: usize = 0;
        while (i < self.held.items.len) {
            const item = self.held.items[i];
            const old_enough = if (min_age_ms) |age| now - item.since_ms >= age else true;
            if (item.peer != peer or !old_enough) {
                i += 1;
                continue;
            }
            _ = self.held.orderedRemove(i);
            switch (item.call) {
                .mint => |m| {
                    var ctx = MintHeldCtx{ .service = self, .peer = peer, .tag = m.tag };
                    m.sender.sendResults(&ctx, MintHeldCtx.build) catch |err| {
                        std.log.err("held mint(): sendResults failed: {s}", .{@errorName(err)});
                        m.sender.sendException(@errorName(err)) catch {};
                    };
                },
                .mint_fail => |sender| sender.sendException(mint_fail_reason) catch |err| {
                    std.log.err("held mintFail(): sendException failed: {s}", .{@errorName(err)});
                },
            }
        }
    }

    /// The connection of `peer` closed: its held calls get no answer.
    fn dropHeld(self: *TokenHostService, peer: *rpc.peer.Peer) void {
        var i: usize = 0;
        while (i < self.held.items.len) {
            if (self.held.items[i].peer == peer) {
                _ = self.held.orderedRemove(i);
            } else i += 1;
        }
    }

    fn initAnswerAtOnce(allocator: Allocator, io: std.Io) TokenHostService {
        return .{
            .allocator = allocator,
            .io = io,
            .server = .{
                .ctx = undefined,
                .vtable = .{
                    .mint = onTokenHostMint,
                    // check() calls the Token and answers from the tag()
                    // callback, so it owns its ReturnSender.
                    .check = undefined,
                    .check_deferred = onTokenHostCheck,
                    // echo() releases the Token it resolved only after its
                    // Return is on the wire.
                    .echo = undefined,
                    .echo_deferred = onTokenHostEcho,
                    .mintFail = undefined,
                    .mintFail_deferred = onTokenHostMintFail,
                },
            },
        };
    }

    fn bind(self: *TokenHostService) void {
        self.server.ctx = self;
    }

    fn deinit(self: *TokenHostService) void {
        for (self.tokens.items) |token| self.allocator.destroy(token);
        self.tokens.deinit(self.allocator);
        self.held.deinit(self.allocator);
    }

    /// Create a HostToken with `tag` and write it as the `token` result.
    fn mintInto(self: *TokenHostService, peer: *rpc.peer.Peer, tag: u32, results: *cap_passing.TokenHost.MintResults.Builder) !void {
        // The list owns the token from its creation on, so a failed export
        // below leaves nothing to free here and nothing freed twice at deinit.
        try self.tokens.ensureUnusedCapacity(self.allocator, 1);
        const token = try self.allocator.create(HostToken);
        self.tokens.appendAssumeCapacity(token);
        token.* = .{
            .host = self,
            .tag = tag,
            .server = .{ .ctx = token, .vtable = .{ .tag = onHostTokenTag } },
        };
        try results.setTokenServer(peer, &token.server);
    }
};

/// A held mint(): builds the results that the direct handler would have.
const MintHeldCtx = struct {
    service: *TokenHostService,
    peer: *rpc.peer.Peer,
    tag: u32,

    fn build(ctx_ptr: *anyopaque, ret: *rpc.wire.protocol.ReturnBuilder) anyerror!void {
        const self: *const MintHeldCtx = @ptrCast(@alignCast(ctx_ptr));
        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        var results = cap_passing.TokenHost.MintResults.Builder.wrap(try any.initStruct(0, 1));
        try self.service.mintInto(self.peer, self.tag, &results);
    }
};

fn onTokenHostMintHeld(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: cap_passing.TokenHost.MintParams.Reader,
    _: *const rpc.caps.table.InboundCapTable,
    sender: cap_passing.TokenHost.Mint.ReturnSender,
) !void {
    const service: *TokenHostService = @ptrCast(@alignCast(ctx_ptr));
    try service.hold(peer, .{ .mint = .{ .sender = sender, .tag = try params.getTag() } });
}

fn onTokenHostMintFailHeld(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    _: cap_passing.TokenHost.MintFailParams.Reader,
    _: *const rpc.caps.table.InboundCapTable,
    sender: cap_passing.TokenHost.MintFail.ReturnSender,
) !void {
    const service: *TokenHostService = @ptrCast(@alignCast(ctx_ptr));
    try service.hold(peer, .{ .mint_fail = sender });
}

/// Chains a held-answer pass after the Peer's own tick (its deadline sweep)
/// on one transport type. `Peer.init` sets `on_tick` to the same function
/// for every connection of a type, so one saved pointer serves them all.
fn HeldAnswerTick(comptime Conn: type) type {
    return struct {
        var peer_tick: ?*const fn (conn: *Conn) void = null;

        fn install(conn: *Conn) void {
            if (conn.on_tick != tick) peer_tick = conn.on_tick;
            conn.on_tick = tick;
        }

        fn tick(conn: *Conn) void {
            if (peer_tick) |cb| cb(conn);
            const ctx = conn.context() orelse return;
            const peer: *rpc.peer.Peer = @ptrCast(@alignCast(ctx));
            if (deferred_token_host) |host| host.answerHeld(peer, held_answer_idle_ms);
            if (cancel_host) |host| host.answerHeldGetSub(peer, held_answer_idle_ms);
        }
    };
}

fn onHostTokenTag(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: cap_passing.Token.TagParams.Reader,
    results: *cap_passing.Token.TagResults.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const token: *HostToken = @ptrCast(@alignCast(ctx_ptr));
    token.host.token_calls += 1;
    try results.setTag(token.tag);
}

fn onTokenHostMint(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: cap_passing.TokenHost.MintParams.Reader,
    results: *cap_passing.TokenHost.MintResults.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const service: *TokenHostService = @ptrCast(@alignCast(ctx_ptr));
    try service.mintInto(peer, try params.getTag(), results);
}

/// check(): carried from the handler into the tag() callback.
const CheckCtx = struct {
    service: *TokenHostService,
    sender: cap_passing.TokenHost.Check.ReturnSender,
    token: cap_passing.Token.Client,
    calls_before: u64,
    tag: u32 = 0,
    local: bool = false,
    // While callTag is still on the handler's stack, points at its flag: the
    // callback sets it before freeing this context, because a local Token
    // answers synchronously and callTag can still fail after that.
    settled_flag: ?*bool = null,

    fn build(ctx_ptr: *anyopaque, ret: *rpc.wire.protocol.ReturnBuilder) anyerror!void {
        const self: *const CheckCtx = @ptrCast(@alignCast(ctx_ptr));
        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        var results = cap_passing.TokenHost.CheckResults.Builder.wrap(try any.initStruct(1, 0));
        try results.setTag(self.tag);
        try results.setLocal(self.local);
    }
};

fn onTokenHostCheck(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: cap_passing.TokenHost.CheckParams.Reader,
    caps: *const rpc.caps.table.InboundCapTable,
    sender: cap_passing.TokenHost.Check.ReturnSender,
) !void {
    const service: *TokenHostService = @ptrCast(@alignCast(ctx_ptr));
    // pipelined_params_deferred: answer the held mint() or mintFail() now
    // that this check() has tried to resolve its param.
    defer service.answerHeld(peer, null);
    // One of our own Tokens comes back as a local Client (origin `.exported`,
    // owns no import); a Token the caller hosts comes back as an import.
    const token = params.resolveToken(peer, caps) catch |err| {
        std.debug.print("e2e-zig-server: check(): resolveToken failed: {s}\n", .{@errorName(err)});
        return err;
    };
    const ctx = peer.allocator.create(CheckCtx) catch |err| {
        token.release();
        return err;
    };
    var settled = false;
    ctx.* = .{
        .service = service,
        .sender = sender,
        .token = token,
        .calls_before = service.token_calls,
        .settled_flag = &settled,
    };
    _ = token.callTag(ctx, null, onTokenHostCheckTagReturn) catch |err| {
        // After the callback ran, it owns the cleanup.
        if (!settled) {
            peer.allocator.destroy(ctx);
            token.release();
        }
        return err;
    };
    if (!settled) ctx.settled_flag = null;
}

fn onTokenHostCheckTagReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: cap_passing.Token.Tag.Response,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const ctx: *CheckCtx = @ptrCast(@alignCast(ctx_ptr));
    if (ctx.settled_flag) |flag| flag.* = true;
    defer peer.allocator.destroy(ctx);
    // Release our import, if the Token is one, only after check()'s Return.
    defer ctx.token.release();

    const results = response.unwrap() catch |err| {
        const reason = switch (response) {
            .exception => |ex| ex.reason,
            else => @errorName(err),
        };
        try ctx.sender.sendException(reason);
        return;
    };
    ctx.tag = try results.getTag();
    // Local means the call ran one of our own HostTokens, and ran nothing
    // else of ours.
    ctx.local = ctx.token.origin == .exported and ctx.service.token_calls == ctx.calls_before + 1;
    try ctx.sender.sendResults(ctx, CheckCtx.build);
}

/// echo(): the resolved Token, written back with the generated setter.
const EchoReturnCtx = struct {
    token: cap_passing.Token.Client,

    fn build(ctx_ptr: *anyopaque, ret: *rpc.wire.protocol.ReturnBuilder) anyerror!void {
        const self: *const EchoReturnCtx = @ptrCast(@alignCast(ctx_ptr));
        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        var results = cap_passing.TokenHost.EchoResults.Builder.wrap(try any.initStruct(0, 1));
        try results.setTokenClient(self.token);
    }
};

fn onTokenHostEcho(
    _: *anyopaque,
    peer: *rpc.peer.Peer,
    params: cap_passing.TokenHost.EchoParams.Reader,
    caps: *const rpc.caps.table.InboundCapTable,
    sender: cap_passing.TokenHost.Echo.ReturnSender,
) !void {
    // A Token the caller hosts resolves to an import we now own (origin
    // `.imported`); setTokenClient must write it as `receiverHosted`, the
    // caller's own capability, even when we export something under the same
    // id. Release our reference only once the Return is sent.
    const token = try params.resolveToken(peer, caps);
    defer token.release();
    var ret_ctx = EchoReturnCtx{ .token = token };
    try sender.sendResults(&ret_ctx, EchoReturnCtx.build);
}

fn onTokenHostMintFail(
    _: *anyopaque,
    _: *rpc.peer.Peer,
    _: cap_passing.TokenHost.MintFailParams.Reader,
    _: *const rpc.caps.table.InboundCapTable,
    sender: cap_passing.TokenHost.MintFail.ReturnSender,
) !void {
    try sender.sendException(mint_fail_reason);
}

// ---------------------------------------------------------------------------
// CancelHost (tests/e2e/schemas/cancellation.capnp)
//
// Both cancellation schemas serve this one host; the client picks the flow.
// The host answers the way capnp-swift does with its default
// `cancelHandlersOnFinish = true`: the answer-finished hook
// (`Peer.setAnswerFinishedHandler`) drops the held call and answers
// `sendReturnCanceled`, with no other check.
//
// - cancel_id_reuse: slow() is held until the caller's Finish, then answered
//   Return{canceled}. The caller can then reuse slow()'s question id, and
//   fast() reports the id each call arrived on.
// - cancel_pipelined: getSub() is held for `held_answer_idle_ms`, so the
//   caller's Finish for it arrives first, while bar() is queued on the
//   answer. The hook must not cancel an answer with calls pipelined on it:
//   then the tick answers getSub() (only after that Finish, see
//   `answerHeldGetSub`), and bar() runs on the Sub. A Peer that reports
//   that Finish gets the answer canceled by this hook, and
//   `sendReturnCanceled` fails bar() with "call canceled".
//
// cancel_id_reuse checks Return{canceled} and the id reuse only. The hook
// drops slow()'s ReturnSender, so this host never sends slow() a late
// answer, and the scenario does not test how the Peer handles one. A second
// Return is a host error: see `Peer.setAnswerFinishedHandler`.
// ---------------------------------------------------------------------------

/// A slow() or getSub() call that the host has not answered yet.
const HeldCancelCall = struct {
    peer: *rpc.peer.Peer,
    since_ms: i64,
    call: union(enum) {
        /// Answered only by the answer-finished hook.
        slow: cancellation.CancelHost.Slow.ReturnSender,
        /// Answered by the hook, or by a loop tick once held
        /// `held_answer_idle_ms`.
        get_sub: cancellation.CancelHost.GetSub.ReturnSender,
    },

    fn answerId(self: HeldCancelCall) u32 {
        return switch (self.call) {
            .slow => |sender| sender.question_id,
            .get_sub => |sender| sender.question_id,
        };
    }

    fn sendException(self: HeldCancelCall, reason: []const u8) !void {
        return switch (self.call) {
            .slow => |sender| sender.sendException(reason),
            .get_sub => |sender| sender.sendException(reason),
        };
    }
};

/// How long a tick waits for the caller's Finish of a held getSub(). Past
/// it, the tick answers getSub() with an exception: without that Finish,
/// cancel_pipelined would pass without testing anything.
const get_sub_finish_wait_ms: i64 = 5_000;

/// What fast() reports for one connection.
const CancelStats = struct {
    peer: *rpc.peer.Peer,
    canceled: u32 = 0,
    last_canceled_id: u32 = 0,
};

/// The value Sub.bar() returns.
const sub_bar_value: u32 = 4242;

/// A Sub made by getSub(). Owned by `CancelHostService.subs` until deinit.
const CancelSub = struct {
    server: cancellation.Sub.Server,
};

/// The cancellation host, for the tick and close callbacks, which get no
/// context pointer. Set once in main; null for other schemas.
var cancel_host: ?*CancelHostService = null;

const CancelHostService = struct {
    allocator: Allocator,
    io: std.Io,
    held: std.ArrayList(HeldCancelCall) = .empty,
    stats: std.ArrayList(CancelStats) = .empty,
    subs: std.ArrayList(*CancelSub) = .empty,
    server: cancellation.CancelHost.Server,

    fn init(allocator: Allocator, io: std.Io) CancelHostService {
        return .{
            .allocator = allocator,
            .io = io,
            .server = .{
                .ctx = undefined,
                .vtable = .{
                    // All three need the ReturnSender: slow() and getSub()
                    // answer late, and fast() reports its question id.
                    .slow = undefined,
                    .slow_deferred = onCancelHostSlow,
                    .fast = undefined,
                    .fast_deferred = onCancelHostFast,
                    .getSub = undefined,
                    .getSub_deferred = onCancelHostGetSub,
                },
            },
        };
    }

    fn bind(self: *CancelHostService) void {
        self.server.ctx = self;
    }

    fn deinit(self: *CancelHostService) void {
        for (self.subs.items) |sub| self.allocator.destroy(sub);
        self.subs.deinit(self.allocator);
        self.stats.deinit(self.allocator);
        self.held.deinit(self.allocator);
    }

    fn nowMs(self: *const CancelHostService) i64 {
        return std.Io.Timestamp.now(self.io, .awake).toMilliseconds();
    }

    fn hold(self: *CancelHostService, peer: *rpc.peer.Peer, call: @FieldType(HeldCancelCall, "call")) !void {
        try self.held.append(self.allocator, .{ .peer = peer, .since_ms = self.nowMs(), .call = call });
    }

    /// The stats of `peer`, made on first use.
    fn statsFor(self: *CancelHostService, peer: *rpc.peer.Peer) !*CancelStats {
        for (self.stats.items) |*item| {
            if (item.peer == peer) return item;
        }
        try self.stats.append(self.allocator, .{ .peer = peer });
        return &self.stats.items[self.stats.items.len - 1];
    }

    /// The caller finished `answer_id` before its Return. Take the held call
    /// out, so it gets no second Return, and answer it canceled.
    ///
    /// When the Peer refuses the cancel, answer the taken call with an
    /// exception that names the refusal. The client then fails at once, with
    /// that reason. Holding the call again instead would hide a hook that
    /// runs when it must not.
    fn answerFinished(self: *CancelHostService, peer: *rpc.peer.Peer, answer_id: u32) void {
        var taken: ?HeldCancelCall = null;
        var i: usize = 0;
        while (i < self.held.items.len) {
            const item = self.held.items[i];
            if (item.peer == peer and item.answerId() == answer_id) {
                taken = self.held.orderedRemove(i);
            } else i += 1;
        }
        peer.sendReturnCanceled(answer_id) catch |err| {
            std.log.err("answer {d}: sendReturnCanceled failed: {s}", .{ answer_id, @errorName(err) });
            const item = taken orelse return;
            var buf: [128]u8 = undefined;
            const reason = std.fmt.bufPrint(&buf, "e2e-zig-server: sendReturnCanceled refused answer {d}: {s}", .{ answer_id, @errorName(err) }) catch "e2e-zig-server: sendReturnCanceled refused the answer";
            item.sendException(reason) catch |send_err| {
                std.log.err("answer {d}: sendException failed: {s}", .{ answer_id, @errorName(send_err) });
            };
            return;
        };
        std.debug.print("e2e-zig-server: answered {d} canceled after its Finish\n", .{answer_id});
        const stats = self.statsFor(peer) catch |err| {
            std.log.err("answer {d}: no stats: {s}", .{ answer_id, @errorName(err) });
            return;
        };
        stats.canceled += 1;
        stats.last_canceled_id = answer_id;
    }

    /// Answer the getSub() calls of `peer` held at least `min_age_ms` whose
    /// caller has sent Finish. One at a time, removed before it is answered,
    /// so an answer that dispatches more calls (the queued bar()) finds the
    /// list consistent.
    ///
    /// cancel_pipelined tests something only when the Peer applies getSub()'s
    /// Finish before this answer. So a call that is not finished yet waits,
    /// up to `get_sub_finish_wait_ms`, and then gets an exception, which
    /// fails bar(). The Peer removes an inbound call from
    /// `active_inbound_questions` at its Finish or its Return. This host has
    /// sent no Return, so a missing entry means the Finish came.
    fn answerHeldGetSub(self: *CancelHostService, peer: *rpc.peer.Peer, min_age_ms: i64) void {
        const now = self.nowMs();
        var i: usize = 0;
        while (i < self.held.items.len) {
            const item = self.held.items[i];
            const sender = switch (item.call) {
                .get_sub => |s| s,
                .slow => {
                    i += 1;
                    continue;
                },
            };
            if (item.peer != peer or now - item.since_ms < min_age_ms) {
                i += 1;
                continue;
            }
            const finished = !peer.active_inbound_questions.contains(sender.question_id);
            if (!finished and now - item.since_ms < get_sub_finish_wait_ms) {
                i += 1;
                continue;
            }
            _ = self.held.orderedRemove(i);
            if (!finished) {
                std.log.err("held getSub() {d}: no Finish from the caller within {d} ms", .{ sender.question_id, get_sub_finish_wait_ms });
                sender.sendException("e2e-zig-server: the caller sent no Finish for getSub() in time, so this check tests nothing") catch |err| {
                    std.log.err("held getSub(): sendException failed: {s}", .{@errorName(err)});
                };
                continue;
            }
            var ctx = GetSubCtx{ .service = self, .peer = peer };
            sender.sendResults(&ctx, GetSubCtx.build) catch |err| {
                std.log.err("held getSub(): sendResults failed: {s}", .{@errorName(err)});
                sender.sendException(@errorName(err)) catch {};
                continue;
            };
            std.debug.print("e2e-zig-server: answered held getSub() {d} after its Finish\n", .{sender.question_id});
        }
    }

    /// The connection of `peer` closed: its held calls get no answer.
    fn dropPeer(self: *CancelHostService, peer: *rpc.peer.Peer) void {
        var i: usize = 0;
        while (i < self.held.items.len) {
            if (self.held.items[i].peer == peer) {
                _ = self.held.orderedRemove(i);
            } else i += 1;
        }
        i = 0;
        while (i < self.stats.items.len) {
            if (self.stats.items[i].peer == peer) {
                _ = self.stats.orderedRemove(i);
            } else i += 1;
        }
    }

    /// Create a Sub and write it as the `sub` result.
    fn makeSubInto(self: *CancelHostService, peer: *rpc.peer.Peer, results: *cancellation.CancelHost.GetSubResults.Builder) !void {
        // The list owns the Sub from its creation on, as with HostToken.
        try self.subs.ensureUnusedCapacity(self.allocator, 1);
        const sub = try self.allocator.create(CancelSub);
        self.subs.appendAssumeCapacity(sub);
        sub.* = .{ .server = .{ .ctx = sub, .vtable = .{ .bar = onCancelSubBar } } };
        try results.setSubServer(peer, &sub.server);
    }
};

fn onCancelAnswerFinished(ctx: *anyopaque, peer: *rpc.peer.Peer, answer_id: u32, _: bool) void {
    const service: *CancelHostService = @ptrCast(@alignCast(ctx));
    service.answerFinished(peer, answer_id);
}

fn onCancelHostSlow(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    _: cancellation.CancelHost.SlowParams.Reader,
    _: *const rpc.caps.table.InboundCapTable,
    sender: cancellation.CancelHost.Slow.ReturnSender,
) !void {
    const service: *CancelHostService = @ptrCast(@alignCast(ctx_ptr));
    try service.hold(peer, .{ .slow = sender });
}

fn onCancelHostGetSub(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    _: cancellation.CancelHost.GetSubParams.Reader,
    _: *const rpc.caps.table.InboundCapTable,
    sender: cancellation.CancelHost.GetSub.ReturnSender,
) !void {
    const service: *CancelHostService = @ptrCast(@alignCast(ctx_ptr));
    try service.hold(peer, .{ .get_sub = sender });
}

/// fast(): its results, built from the ReturnSender's question id.
const FastCtx = struct {
    n: u32,
    answer_id: u32,
    canceled: u32,
    last_canceled_id: u32,

    fn build(ctx_ptr: *anyopaque, ret: *rpc.wire.protocol.ReturnBuilder) anyerror!void {
        const self: *const FastCtx = @ptrCast(@alignCast(ctx_ptr));
        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        var results = cancellation.CancelHost.FastResults.Builder.wrap(try any.initStruct(2, 0));
        try results.setN(self.n);
        try results.setAnswerId(self.answer_id);
        try results.setCanceled(self.canceled);
        try results.setLastCanceledId(self.last_canceled_id);
    }
};

fn onCancelHostFast(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: cancellation.CancelHost.FastParams.Reader,
    _: *const rpc.caps.table.InboundCapTable,
    sender: cancellation.CancelHost.Fast.ReturnSender,
) !void {
    const service: *CancelHostService = @ptrCast(@alignCast(ctx_ptr));
    const stats = try service.statsFor(peer);
    var ctx = FastCtx{
        .n = try params.getN(),
        .answer_id = sender.question_id,
        .canceled = stats.canceled,
        .last_canceled_id = stats.last_canceled_id,
    };
    try sender.sendResults(&ctx, FastCtx.build);
}

/// A held getSub(): builds the results that a direct handler would have.
const GetSubCtx = struct {
    service: *CancelHostService,
    peer: *rpc.peer.Peer,

    fn build(ctx_ptr: *anyopaque, ret: *rpc.wire.protocol.ReturnBuilder) anyerror!void {
        const self: *const GetSubCtx = @ptrCast(@alignCast(ctx_ptr));
        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        var results = cancellation.CancelHost.GetSubResults.Builder.wrap(try any.initStruct(0, 1));
        try self.service.makeSubInto(self.peer, &results);
    }
};

fn onCancelSubBar(
    _: *anyopaque,
    _: *rpc.peer.Peer,
    _: cancellation.Sub.BarParams.Reader,
    results: *cancellation.Sub.BarResults.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    try results.setValue(sub_bar_value);
}

fn parseSchema(text: []const u8) !Schema {
    if (std.mem.eql(u8, text, "game_world")) return .game_world;
    if (std.mem.eql(u8, text, "chat")) return .chat;
    if (std.mem.eql(u8, text, "inventory")) return .inventory;
    if (std.mem.eql(u8, text, "matchmaking")) return .matchmaking;
    if (std.mem.eql(u8, text, "resolve_disembargo")) return .resolve_disembargo;
    if (std.mem.eql(u8, text, "pass_back")) return .pass_back;
    if (std.mem.eql(u8, text, "pipelined_params")) return .pipelined_params;
    if (std.mem.eql(u8, text, "pipelined_params_deferred")) return .pipelined_params_deferred;
    if (std.mem.eql(u8, text, "cancel_id_reuse")) return .cancel_id_reuse;
    if (std.mem.eql(u8, text, "cancel_pipelined")) return .cancel_pipelined;
    return error.InvalidSchema;
}

fn parseArgs(allocator: Allocator, args: std.process.Args) !CliArgs {
    var out = CliArgs{};
    errdefer if (out.cert_pem) |path| allocator.free(path);
    errdefer if (out.key_pem) |path| allocator.free(path);
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
        if (std.mem.eql(u8, arg, "--transport")) {
            const v = args_iter.next() orelse return error.MissingArgValue;
            if (std.mem.eql(u8, v, "tcp")) {
                out.transport = .tcp;
            } else if (std.mem.eql(u8, v, "quic")) {
                out.transport = .quic;
            } else return error.UnknownArgument;
            continue;
        }
        if (std.mem.eql(u8, arg, "--cert-pem")) {
            const v = args_iter.next() orelse return error.MissingArgValue;
            const path = try allocator.dupe(u8, v);
            if (out.cert_pem) |old| allocator.free(old);
            out.cert_pem = path;
            continue;
        }
        if (std.mem.eql(u8, arg, "--key-pem")) {
            const v = args_iter.next() orelse return error.MissingArgValue;
            const path = try allocator.dupe(u8, v);
            if (out.key_pem) |old| allocator.free(old);
            out.key_pem = path;
            continue;
        }
    }

    out.host = try allocator.dupe(u8, host_text);
    return out;
}

fn nowMillis() i64 {
    if (comptime builtin.os.tag == .windows) {
        // The e2e harness only runs this server on POSIX hosts; Windows
        // builds exist for the cross-compile gate and need a well-typed
        // stub rather than a libc clock dependency.
        return 0;
    }
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(std.posix.CLOCK.REALTIME, &ts);
    return @as(i64, ts.sec) * 1000 + @divTrunc(@as(i64, ts.nsec), 1_000_000);
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
    by_faction: game_types.Faction,
} {
    // `AreaQuery.filter` union helpers are not generated yet.
    // Discriminant and payload are read directly from the struct data section.
    const discriminant = query._reader.readU16(4);
    switch (discriminant) {
        1 => {
            const raw_kind = query._reader.readU16(6);
            return .{ .by_kind = @fromBackingInt(@intCast(raw_kind)) };
        },
        2 => {
            const raw_faction = query._reader.readU16(6);
            return .{ .by_faction = @fromBackingInt(@intCast(raw_faction)) };
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
    _: *const rpc.caps.table.InboundCapTable,
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
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onDespawnEntity(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: game_world.GameWorld.DespawnEntity.Params.Reader,
    results: *game_world.GameWorld.DespawnEntity.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const service: *GameWorldService = @ptrCast(@alignCast(ctx_ptr));
    const id_reader = try params.getId();
    const id = try id_reader.getId();

    if (service.entities.fetchRemove(id)) |entry| {
        service.allocator.free(entry.value.name);
        try results.setStatus(statusOk(game_types.StatusCode));
    } else {
        try results.setStatus(statusNotFound(game_types.StatusCode));
    }
}

fn onGetEntity(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: game_world.GameWorld.GetEntity.Params.Reader,
    results: *game_world.GameWorld.GetEntity.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const service: *GameWorldService = @ptrCast(@alignCast(ctx_ptr));
    const id_reader = try params.getId();
    const id = try id_reader.getId();

    if (service.entities.get(id)) |entity| {
        var out_entity = try results.initEntity();
        try fillGameEntity(&out_entity, &entity);
        try results.setStatus(statusOk(game_types.StatusCode));
    } else {
        try results.setStatus(statusNotFound(game_types.StatusCode));
    }
}

fn onMoveEntity(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: game_world.GameWorld.MoveEntity.Params.Reader,
    results: *game_world.GameWorld.MoveEntity.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
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
        try results.setStatus(statusOk(game_types.StatusCode));
    } else {
        try results.setStatus(statusNotFound(game_types.StatusCode));
    }
}

fn onDamageEntity(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: game_world.GameWorld.DamageEntity.Params.Reader,
    results: *game_world.GameWorld.DamageEntity.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
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
        try results.setStatus(statusOk(game_types.StatusCode));
    } else {
        try results.setKilled(false);
        try results.setStatus(statusNotFound(game_types.StatusCode));
    }
}

fn onQueryArea(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: game_world.GameWorld.QueryArea.Params.Reader,
    results: *game_world.GameWorld.QueryArea.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const service: *GameWorldService = @ptrCast(@alignCast(ctx_ptr));
    const query = try params.getQuery();
    const center = try query.getCenter();

    const cx = try center.getX();
    const cy = try center.getY();
    const cz = try center.getZ();
    const radius = try query.getRadius();

    const filter = parseGameWorldFilter(query);

    var matched_ids = std.ArrayList(u64).empty;
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
            var pid = game_types.PlayerId.Builder.wrap(ptr);
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
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const service: *ChatService = @ptrCast(@alignCast(ctx_ptr));

    const room_name = try params.getName();
    const room_topic = try params.getTopic();

    if (service.rooms.get(room_name) != null) {
        try results.setStatus(statusAlreadyExists(game_types.StatusCode));
        return;
    }

    const room = try service.allocator.create(ChatRoomState);
    room.* = .{
        .id = service.next_room_id,
        .name = try service.allocator.dupe(u8, room_name),
        .topic = try service.allocator.dupe(u8, room_topic),
        .member_count = 0,
        .messages = std.ArrayList(ChatMsg).empty,
    };
    service.next_room_id += 1;

    try service.rooms.put(room.name, room);

    const session = try service.createRoomSession(room, 0, "system", .Neutral, 0);
    const cap_id = try chat.ChatRoom.exportServer(peer, &session.server);
    try results.setRoomCapability(.{ .id = cap_id });

    var info = try results.initInfo();
    try fillChatRoomInfo(&info, room);
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onJoinRoom(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: chat.ChatService.JoinRoom.Params.Reader,
    results: *chat.ChatService.JoinRoom.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const service: *ChatService = @ptrCast(@alignCast(ctx_ptr));

    const room_name = try params.getName();
    const room = service.rooms.get(room_name) orelse {
        try results.setStatus(statusNotFound(game_types.StatusCode));
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
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onListRooms(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: chat.ChatService.ListRooms.Params.Reader,
    results: *chat.ChatService.ListRooms.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
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
    _: *const rpc.caps.table.InboundCapTable,
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

    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onSendMessage(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: chat.ChatRoom.SendMessage.Params.Reader,
    results: *chat.ChatRoom.SendMessage.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
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
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onSendEmote(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: chat.ChatRoom.SendEmote.Params.Reader,
    results: *chat.ChatRoom.SendEmote.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
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
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onGetHistory(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: chat.ChatRoom.GetHistory.Params.Reader,
    results: *chat.ChatRoom.GetHistory.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
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
    _: *const rpc.caps.table.InboundCapTable,
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
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    try results.setStatus(statusOk(game_types.StatusCode));
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
    _: *const rpc.caps.table.InboundCapTable,
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
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onAddItem(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: inventory.InventoryService.AddItem.Params.Reader,
    results: *inventory.InventoryService.AddItem.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const service: *InventoryService = @ptrCast(@alignCast(ctx_ptr));

    const player = try params.getPlayer();
    const player_id = try player.getId();
    const item = try params.getItem();
    const quantity = try params.getQuantity();

    const inv = try service.getOrCreateInventory(player_id);
    if (inv.slots.items.len >= inv.capacity) {
        try results.setStatus(statusResourceExhausted(game_types.StatusCode));
        return;
    }

    const item_id = try (try item.getId()).getId();
    const item_name = try item.getName();
    const rarity = try item.getRarity();
    const item_level = try item.getLevel();
    const stack_size = try item.getStackSize();

    var attrs = std.ArrayList(ItemAttr).empty;
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
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onRemoveItem(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: inventory.InventoryService.RemoveItem.Params.Reader,
    results: *inventory.InventoryService.RemoveItem.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
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
        try results.setStatus(statusOk(game_types.StatusCode));
    } else {
        try results.setStatus(statusNotFound(game_types.StatusCode));
    }
}

fn onStartTrade(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: inventory.InventoryService.StartTrade.Params.Reader,
    results: *inventory.InventoryService.StartTrade.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const service: *InventoryService = @ptrCast(@alignCast(ctx_ptr));

    const initiator = try params.getInitiator();
    const target = try params.getTarget();

    const trade = try service.allocator.create(TradeSessionState);
    trade.* = .{
        .initiator_id = try initiator.getId(),
        .target_id = try target.getId(),
        .state = .Proposing,
        .my_offer_slots = std.ArrayList(u16).empty,
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
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onFilterByRarity(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: inventory.InventoryService.FilterByRarity.Params.Reader,
    results: *inventory.InventoryService.FilterByRarity.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const service: *InventoryService = @ptrCast(@alignCast(ctx_ptr));

    const player = try params.getPlayer();
    const player_id = try player.getId();
    const min_rarity = try params.getMinRarity();

    const inv = try service.getOrCreateInventory(player_id);

    var matches = std.ArrayList(usize).empty;
    defer matches.deinit(service.allocator);

    for (inv.slots.items, 0..) |slot, idx| {
        if (@backingInt(slot.rarity) >= @backingInt(min_rarity)) {
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

    var offered_indices = std.ArrayList(usize).empty;
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
    _: *const rpc.caps.table.InboundCapTable,
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
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onRemoveTradeItems(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: inventory.TradeSession.RemoveItems.Params.Reader,
    results: *inventory.TradeSession.RemoveItems.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const state: *TradeSessionServerState = @ptrCast(@alignCast(ctx_ptr));

    state.trade.my_offer_slots.clearRetainingCapacity();

    var offer = try results.initOffer();
    try offer.setAccepted(false);
    _ = try offer.initOfferedItems(0);
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onAcceptTrade(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: inventory.TradeSession.Accept.Params.Reader,
    results: *inventory.TradeSession.Accept.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const state: *TradeSessionServerState = @ptrCast(@alignCast(ctx_ptr));

    state.trade.state = .Accepted;
    try results.setState(.Accepted);
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onConfirmTrade(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: inventory.TradeSession.Confirm.Params.Reader,
    results: *inventory.TradeSession.Confirm.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const state: *TradeSessionServerState = @ptrCast(@alignCast(ctx_ptr));

    state.trade.state = .Confirmed;
    try results.setState(.Confirmed);
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onCancelTrade(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: inventory.TradeSession.Cancel.Params.Reader,
    results: *inventory.TradeSession.Cancel.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
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
    _: *const rpc.caps.table.InboundCapTable,
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
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const state: *TradeSessionServerState = @ptrCast(@alignCast(ctx_ptr));
    try results.setState(state.trade.state);
}

fn fillMatchPlayer(builder: *game_types.PlayerInfo.Builder, p: *const MatchPlayer) !void {
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
    _: *const rpc.caps.table.InboundCapTable,
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
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onDequeue(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: matchmaking.MatchmakingService.Dequeue.Params.Reader,
    results: *matchmaking.MatchmakingService.Dequeue.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const service: *MatchmakingService = @ptrCast(@alignCast(ctx_ptr));
    const ticket_id = try params.getTicketId();

    if (service.queue.fetchRemove(ticket_id)) |entry| {
        service.allocator.free(entry.value.player.name);
        try results.setStatus(statusOk(game_types.StatusCode));
    } else {
        try results.setStatus(statusNotFound(game_types.StatusCode));
    }
}

fn onFindMatch(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: matchmaking.MatchmakingService.FindMatch.Params.Reader,
    results: *matchmaking.MatchmakingService.FindMatch.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
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
        .team_a = std.ArrayList(MatchPlayer).empty,
        .team_b = std.ArrayList(MatchPlayer).empty,
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
    _: *const rpc.caps.table.InboundCapTable,
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
    _: *const rpc.caps.table.InboundCapTable,
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

        try results.setStatus(statusOk(game_types.StatusCode));
    } else {
        try results.setStatus(statusNotFound(game_types.StatusCode));
    }
}

fn onControllerGetInfo(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: matchmaking.MatchController.GetInfo.Params.Reader,
    results: *matchmaking.MatchController.GetInfo.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
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
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const controller: *MatchControllerServerState = @ptrCast(@alignCast(ctx_ptr));
    const match_state = controller.service.matches.get(controller.match_id) orelse {
        try results.setStatus(statusNotFound(game_types.StatusCode));
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
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onControllerReportResult(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: matchmaking.MatchController.ReportResult.Params.Reader,
    results: *matchmaking.MatchController.ReportResult.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const controller: *MatchControllerServerState = @ptrCast(@alignCast(ctx_ptr));
    const service = controller.service;

    const match_state = service.matches.get(controller.match_id) orelse {
        try results.setStatus(statusNotFound(game_types.StatusCode));
        return;
    };

    const incoming = try params.getResult();
    const incoming_match_id = try (try incoming.getMatchId()).getId();

    const output = try service.allocator.create(MatchResultData);
    output.* = .{
        .match_id = incoming_match_id,
        .winning_team = try incoming.getWinningTeam(),
        .duration = try incoming.getDuration(),
        .player_stats = std.ArrayList(PlayerStat).empty,
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

    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onControllerCancelMatch(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: matchmaking.MatchController.CancelMatch.Params.Reader,
    results: *matchmaking.MatchController.CancelMatch.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) !void {
    const controller: *MatchControllerServerState = @ptrCast(@alignCast(ctx_ptr));
    const match_state = controller.service.matches.get(controller.match_id) orelse {
        try results.setStatus(statusNotFound(game_types.StatusCode));
        return;
    };

    if (match_state.state == .InProgress or match_state.state == .Completed) {
        try results.setStatus(statusInvalidArgument(game_types.StatusCode));
        return;
    }

    match_state.state = .Cancelled;
    try results.setStatus(statusOk(game_types.StatusCode));
}

fn onPeerError(_: ?*anyopaque, peer: *rpc.peer.Peer, err: anyerror) void {
    std.log.err("rpc peer error: {s}", .{@errorName(err)});
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}

fn onPeerClose(_: ?*anyopaque, peer: *rpc.peer.Peer) void {
    // A later Peer can get the same address: drop what this one still holds.
    if (deferred_token_host) |host| host.dropHeld(peer);
    if (cancel_host) |host| host.dropPeer(peer);
}

fn onAccept(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, conn: *rpc.transport.tcp.Connection, _: u32) anyerror!rpc.integration.worker_pool.WorkerPool.AcceptDecision {
    const app: *App = @ptrCast(@alignCast(ctx_ptr));

    const bootstrap_result = switch (app.schema) {
        .game_world => game_world.GameWorld.setBootstrap(peer, &app.game_world_service.server),
        .chat => chat.ChatService.setBootstrap(peer, &app.chat_service.server),
        .inventory => inventory.InventoryService.setBootstrap(peer, &app.inventory_service.server),
        .matchmaking => matchmaking.MatchmakingService.setBootstrap(peer, &app.matchmaking_service.server),
        .resolve_disembargo => resolve_disembargo.Reflector.setBootstrap(peer, &app.reflect_service.server),
        .pass_back,
        .pipelined_params,
        .pipelined_params_deferred,
        => cap_passing.TokenHost.setBootstrap(peer, &app.token_host_service.server),
        .cancel_id_reuse,
        .cancel_pipelined,
        => cancellation.CancelHost.setBootstrap(peer, &app.cancel_host_service.server),
    };

    _ = bootstrap_result catch |err| {
        std.log.err("failed to set bootstrap: {s}", .{@errorName(err)});
        return err;
    };

    switch (app.schema) {
        .pipelined_params_deferred => HeldAnswerTick(rpc.transport.tcp.Connection).install(conn),
        .cancel_id_reuse, .cancel_pipelined => {
            peer.setAnswerFinishedHandler(&app.cancel_host_service, onCancelAnswerFinished);
            HeldAnswerTick(rpc.transport.tcp.Connection).install(conn);
        },
        else => {},
    }
    peer.start(null, onPeerError, onPeerClose);
    return .accept;
}

fn usage() void {
    std.debug.print(
        \\Usage: e2e-zig-server [--host 0.0.0.0|unix:/path] [--port 4700] [--schema game_world|chat|inventory|matchmaking|resolve_disembargo|pass_back|pipelined_params|pipelined_params_deferred|cancel_id_reuse|cancel_pipelined]
        \\  --host unix:/path serves on an AF_UNIX socket at /path (Linux and macOS; --port is ignored)
        \\  --transport quic serves over the QUIC baseline wire (needs -Dquic=true; also --cert-pem F --key-pem F)
        \\
    , .{});
}

/// `--host unix:/path` names an AF_UNIX socket path instead of an IP host.
const unix_host_prefix = "unix:";

fn unixPathFromHost(host: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, host, unix_host_prefix)) return null;
    return host[unix_host_prefix.len..];
}

/// Serve the schema on an AF_UNIX socket at `path` (`--host unix:/path`).
///
/// `WorkerPool` takes an IP address only, so this is the pool's worker loop
/// with `.concurrency = 1`, on a listener from `rpc.transport.unix.listen`:
/// one connection at a time, each set up exactly as a pool worker sets it up
/// (the pool's default `Config` timeouts, limits and accept entropy), then
/// handed to the same `onAccept` the TCP server uses. The process runs until
/// it is killed, like the TCP server.
fn serveUnix(allocator: Allocator, io: std.Io, path: []const u8, app: *App) !void {
    const WorkerPool = rpc.integration.worker_pool.WorkerPool;
    const Connection = rpc.transport.tcp.Connection;
    // The defaults the TCP server's `WorkerPool.init(..., .{ .concurrency = 1 })` runs with.
    const pool_config: WorkerPool.Config = .{ .concurrency = 1 };

    var conn_options = pool_config.connection_options;
    if (pool_config.join_timeout_ms != null and conn_options.tick_interval_ms == null) {
        conn_options.tick_interval_ms = 100;
    }
    if (conn_options.idle_timeout_ms == null) conn_options.idle_timeout_ms = pool_config.idle_timeout_ms;

    var listener = try rpc.transport.unix.listen(allocator, io, path, .{
        .conn = conn_options,
        .backlog = pool_config.listen_backlog,
    });
    defer listener.close();

    // One accept-embargo CSPRNG for the one serving thread, as each pool
    // worker keeps one. Fail closed: no connection is served without it.
    var rng = try rpc.peer.seedEntropyCsprng(io);

    std.debug.print("READY\n", .{});

    while (true) {
        const conn: *Connection = listener.accept() catch |err| {
            std.log.err("unix accept failed: {s}", .{@errorName(err)});
            const backoff: std.Io.Clock.Duration = .{
                .raw = .{ .nanoseconds = 20 * std.time.ns_per_ms },
                .clock = .awake,
            };
            backoff.sleep(io) catch {};
            continue;
        };
        conn.first_frame_timeout_ms = pool_config.first_frame_timeout_ms;

        const peer = allocator.create(rpc.peer.Peer) catch {
            conn.deinit();
            allocator.destroy(conn);
            continue;
        };
        peer.* = rpc.peer.Peer.init(allocator, conn);
        peer.setLimits(pool_config.peer_limits);
        peer.setClockIo(io);
        peer.setTimeouts(.{ .join_timeout_ms = pool_config.join_timeout_ms });
        peer.setEntropySource(rpc.peer.EntropySource.fromCsprng(&rng));

        const decision = onAccept(app, peer, conn, 0) catch |err| blk: {
            std.log.err("on_accept failed: {s}", .{@errorName(err)});
            break :blk .reject;
        };
        if (decision == .accept) conn.run();

        // WorkerPool.destroyAccepted: detach, then peer, then connection.
        _ = peer.takeAttachedConnection(*Connection);
        peer.deinit();
        allocator.destroy(peer);
        conn.deinit();
        allocator.destroy(conn);
    }
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
    defer if (args.cert_pem) |path| allocator.free(path);
    defer if (args.key_pem) |path| allocator.free(path);

    var app = try App.init(allocator, io, args.schema);
    defer app.deinit();
    app.bind();
    if (args.schema == .pipelined_params_deferred) deferred_token_host = &app.token_host_service;
    defer deferred_token_host = null;
    if (args.schema == .cancel_id_reuse or args.schema == .cancel_pipelined) cancel_host = &app.cancel_host_service;
    defer cancel_host = null;

    if (unixPathFromHost(args.host)) |path| return serveUnix(allocator, io, path, &app);

    if (args.transport == .quic) {
        if (comptime !rpc.transport.quic.enabled) {
            std.debug.print("e2e-zig-server: --transport quic needs -Dquic=true\n", .{});
            return error.QuicNeedsBuildFlag;
        }
        return serveQuic(allocator, io, args, &app);
    }

    const address = try std.Io.net.IpAddress.parse(args.host, args.port);

    var pool = try rpc.integration.worker_pool.WorkerPool.init(
        allocator,
        io,
        address,
        &app,
        onAccept,
        .{ .concurrency = 1 },
    );
    defer pool.deinit();

    std.debug.print("READY\n", .{});

    try pool.run();
}

/// `--transport quic`: every schema over the QUIC baseline wire
/// (ALPN "capnp-rpc/1", stream 0, u32-LE length-prefixed frames) through
/// `rpc.transport.quic.PeerServer`. Only compiled with `-Dquic=true`; the
/// comptime gate keeps this body unanalyzed for the non-QUIC package root.
fn serveQuic(allocator: Allocator, io: std.Io, args: CliArgs, app: *App) !void {
    const quic = rpc.transport.quic;
    comptime if (!quic.enabled) return error.QuicNeedsBuildFlag;

    const cert_path = args.cert_pem orelse return error.MissingCertPem;
    const key_path = args.key_pem orelse return error.MissingKeyPem;
    const cert_pem = try readFileAlloc(allocator, io, cert_path);
    defer allocator.free(cert_pem);
    const key_pem = try readFileAlloc(allocator, io, key_path);
    defer allocator.free(key_pem);

    const address = try std.Io.net.IpAddress.parse(args.host, args.port);
    var server_options = quic.ServerOptions{
        .listen_addr = address,
        .tls_cert_pem = cert_pem,
        .tls_key_pem = key_pem,
        .max_concurrent_connections = 8,
    };
    // Long enough for slow interop scenarios between the harness's client
    // startup and its first frame.
    server_options.transport_params = quic.defaultTransportParams();
    server_options.transport_params.max_idle_timeout_ms = 120_000;

    const server = try quic.PeerServer.init(allocator, io, server_options, .{
        .ctx = app,
        .on_accept = onQuicAccept,
        .on_close = onQuicSessionClose,
    });
    defer server.deinit();

    std.debug.print("READY\n", .{});
    // tools/e2e_self.zig (`zig build -Dquic=true e2e-quic`) passes `--port 0`
    // and reads the bound port from this line: keep " over quic on HOST:PORT".
    std.debug.print("e2e-zig-server: {s} over quic on {s}:{d}\n", .{ @tagName(args.schema), args.host, server.getAddress().getPort() });

    server.run();
}

fn onQuicAccept(ctx: ?*anyopaque, session: *rpc.transport.quic.PeerServer.Session) anyerror!void {
    const app: *App = @ptrCast(@alignCast(ctx.?));
    _ = try switch (app.schema) {
        .game_world => game_world.GameWorld.setBootstrap(&session.peer, &app.game_world_service.server),
        .chat => chat.ChatService.setBootstrap(&session.peer, &app.chat_service.server),
        .inventory => inventory.InventoryService.setBootstrap(&session.peer, &app.inventory_service.server),
        .matchmaking => matchmaking.MatchmakingService.setBootstrap(&session.peer, &app.matchmaking_service.server),
        .resolve_disembargo => resolve_disembargo.Reflector.setBootstrap(&session.peer, &app.reflect_service.server),
        .pass_back,
        .pipelined_params,
        .pipelined_params_deferred,
        => cap_passing.TokenHost.setBootstrap(&session.peer, &app.token_host_service.server),
        .cancel_id_reuse,
        .cancel_pipelined,
        => cancellation.CancelHost.setBootstrap(&session.peer, &app.cancel_host_service.server),
    };
    switch (app.schema) {
        .pipelined_params_deferred => {},
        .cancel_id_reuse, .cancel_pipelined => session.peer.setAnswerFinishedHandler(&app.cancel_host_service, onCancelAnswerFinished),
        else => return,
    }
    const transport = session.transport orelse return;
    HeldAnswerTick(@TypeOf(transport.*)).install(transport);
}

fn onQuicSessionClose(_: ?*anyopaque, session: *rpc.transport.quic.PeerServer.Session) void {
    if (deferred_token_host) |host| host.dropHeld(&session.peer);
    if (cancel_host) |host| host.dropPeer(&session.peer);
    std.debug.print("e2e-zig-server: quic session closed close_cause={s}\n", .{@tagName(session.closeCause())});
}

var quic_read_buf: [8192]u8 = undefined;

fn readFileAlloc(allocator: Allocator, io: std.Io, path: []const u8) ![]u8 {
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    var reader = file.reader(io, &quic_read_buf);
    const data = try reader.interface.allocRemaining(allocator, .unlimited);
    if (data.len > 1 << 20) {
        allocator.free(@constCast(data));
        return error.TooLarge;
    }
    return @constCast(data);
}
