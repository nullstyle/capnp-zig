const std = @import("std");
const capnpc = @import("capnpc-zig");
const xev = @import("xev");

const rpc = capnpc.rpc;

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
    runtime: rpc.runtime.Runtime,
    args: CliArgs,
    tap: Tap = .{},
    done: bool = false,
    err: ?anyerror = null,
    peer: ?*rpc.peer.Peer = null,
    conn: ?*rpc.connection.Connection = null,
};

var g_client_app: ?*ClientApp = null;

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
    }

    out.host = try allocator.dupe(u8, host_text);
    return out;
}

fn statusOk(comptime T: type) T {
    return @field(T, "Ok");
}

fn finish(app: *ClientApp, peer: *rpc.peer.Peer) void {
    app.done = true;
    if (!peer.conn.isClosing()) peer.conn.close();
}

fn failAndFinish(app: *ClientApp, peer: *rpc.peer.Peer, desc: []const u8) void {
    app.tap.ok(false, desc);
    finish(app, peer);
}

fn onPeerError(peer: *rpc.peer.Peer, err: anyerror) void {
    std.log.err("rpc peer error: {s}", .{@errorName(err)});
    if (!peer.conn.isClosing()) peer.conn.close();
    if (g_client_app) |app| {
        app.err = err;
        app.done = true;
    }
}

fn onPeerClose(peer: *rpc.peer.Peer) void {
    _ = peer;
    if (g_client_app) |app| {
        app.done = true;
    }
}

fn bootstrapGameWorld(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    _ = try game_world.GameWorld.Client.fromBootstrap(peer, app, onGameWorldBootstrap);
}

fn onGameWorldBootstrap(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, response: game_world.GameWorld.BootstrapResponse) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .client => |client| {
            var c = client;
            _ = try c.callSpawnEntity(app, buildSpawnEntity, onSpawnEntityReturn);
        },
        else => failAndFinish(app, peer, "bootstrap game_world capability"),
    }
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
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    switch (response) {
        .results => |results| {
            app.tap.ok((try results.getStatus()) == statusOk(game_world.StatusCode), "spawnEntity returns ok status");
            const entity = try results.getEntity();
            app.tap.ok(std.mem.eql(u8, try entity.getName(), "ZigClientHero"), "spawnEntity returns expected name");
        },
        else => app.tap.ok(false, "spawnEntity returns results"),
    }

    finish(app, peer);
}

fn bootstrapChat(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    _ = try chat.ChatService.Client.fromBootstrap(peer, app, onChatBootstrap);
}

fn onChatBootstrap(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, response: chat.ChatService.BootstrapResponse) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .client => |client| {
            var c = client;
            _ = try c.callCreateRoom(app, buildCreateRoom, onCreateRoomReturn);
        },
        else => failAndFinish(app, peer, "bootstrap chat capability"),
    }
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
    caps: *const rpc.cap_table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    switch (response) {
        .results => |results| {
            app.tap.ok((try results.getStatus()) == statusOk(chat.StatusCode), "createRoom returns ok status");
            const room_cap = try results.getRoom();
            const resolved = try caps.resolveCapability(room_cap);
            app.tap.ok(switch (resolved) {
                .imported => true,
                else => false,
            }, "createRoom returns imported ChatRoom capability");
        },
        else => app.tap.ok(false, "createRoom returns results"),
    }

    finish(app, peer);
}

fn bootstrapInventory(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    _ = try inventory.InventoryService.Client.fromBootstrap(peer, app, onInventoryBootstrap);
}

fn onInventoryBootstrap(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, response: inventory.InventoryService.BootstrapResponse) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .client => |client| {
            var c = client;
            _ = try c.callGetInventory(app, buildGetInventory, onGetInventoryReturn);
        },
        else => failAndFinish(app, peer, "bootstrap inventory capability"),
    }
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
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    switch (response) {
        .results => |results| {
            app.tap.ok((try results.getStatus()) == statusOk(inventory.StatusCode), "getInventory returns ok status");
            const inv = try results.getInventory();
            app.tap.ok((try inv.getUsedSlots()) == 0, "new inventory has zero used slots");
        },
        else => app.tap.ok(false, "getInventory returns results"),
    }

    finish(app, peer);
}

fn bootstrapMatchmaking(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    _ = try matchmaking.MatchmakingService.Client.fromBootstrap(peer, app, onMatchmakingBootstrap);
}

fn onMatchmakingBootstrap(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, response: matchmaking.MatchmakingService.BootstrapResponse) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .client => |client| {
            var c = client;
            _ = try c.callEnqueue(app, buildEnqueue, onEnqueueReturn);
        },
        else => failAndFinish(app, peer, "bootstrap matchmaking capability"),
    }
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
    _: *const rpc.cap_table.InboundCapTable,
) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));

    switch (response) {
        .results => |results| {
            app.tap.ok((try results.getStatus()) == statusOk(matchmaking.StatusCode), "enqueue returns ok status");
            const ticket = try results.getTicket();
            app.tap.ok((try ticket.getTicketId()) > 0, "enqueue returns a non-zero ticket id");
        },
        else => app.tap.ok(false, "enqueue returns results"),
    }

    finish(app, peer);
}

const ConnectCtx = struct {
    app: *ClientApp,
};

fn onConnect(
    ctx: ?*ConnectCtx,
    loop: *xev.Loop,
    _: *xev.Completion,
    socket: xev.TCP,
    res: xev.ConnectError!void,
) xev.CallbackAction {
    const connect_ctx = ctx orelse return .disarm;
    const app = connect_ctx.app;

    if (res) |_| {
        const conn = app.allocator.create(rpc.connection.Connection) catch {
            app.err = error.OutOfMemory;
            app.done = true;
            return .disarm;
        };

        conn.* = rpc.connection.Connection.init(app.allocator, loop, socket, .{}) catch |err| {
            app.allocator.destroy(conn);
            app.err = err;
            app.done = true;
            return .disarm;
        };

        const peer = app.allocator.create(rpc.peer.Peer) catch {
            conn.deinit();
            app.allocator.destroy(conn);
            app.err = error.OutOfMemory;
            app.done = true;
            return .disarm;
        };

        peer.* = rpc.peer.Peer.init(app.allocator, conn);
        app.conn = conn;
        app.peer = peer;

        peer.start(onPeerError, onPeerClose);

        const start_result = switch (app.args.schema) {
            .game_world => bootstrapGameWorld(app, peer),
            .chat => bootstrapChat(app, peer),
            .inventory => bootstrapInventory(app, peer),
            .matchmaking => bootstrapMatchmaking(app, peer),
        };

        start_result catch |err| {
            app.err = err;
            app.done = true;
        };
    } else |err| {
        app.err = err;
        app.done = true;
    }

    return .disarm;
}

fn usage() void {
    std.debug.print(
        \\Usage: e2e-zig-client [--host 127.0.0.1] [--port 4000] [--schema game_world|chat|inventory|matchmaking]\n
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

    var app = ClientApp{
        .allocator = allocator,
        .runtime = try rpc.runtime.Runtime.init(allocator),
        .args = args,
    };
    defer app.runtime.deinit();
    g_client_app = &app;
    defer g_client_app = null;

    const address = try std.net.Address.parseIp4(args.host, args.port);

    var socket = try xev.TCP.init(address);
    var completion: xev.Completion = .{};
    var connect_ctx = ConnectCtx{ .app = &app };

    socket.connect(&app.runtime.loop, &completion, address, ConnectCtx, &connect_ctx, onConnect);

    while (!app.done) {
        try app.runtime.run(.once);
    }

    if (app.peer) |peer| {
        peer.deinit();
        allocator.destroy(peer);
    }
    if (app.conn) |conn| {
        conn.deinit();
        allocator.destroy(conn);
    }

    if (app.err) |err| return err;

    std.debug.print("1..{d}\n", .{app.tap.test_num});

    if (app.tap.failures > 0) {
        return error.TestFailed;
    }
}
