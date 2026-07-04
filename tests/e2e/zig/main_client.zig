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
    peer: ?*rpc.peer.Peer = null,
    conn: ?*rpc.transport.tcp.Connection = null,
};

var g_client_app: ?*ClientApp = null;

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

fn statusOk(comptime T: type) T {
    return @field(T, "Ok");
}

fn finish(app: *ClientApp, peer: *rpc.peer.Peer) void {
    app.done = true;
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}

fn failAndFinish(app: *ClientApp, peer: *rpc.peer.Peer, desc: []const u8) void {
    app.tap.ok(false, desc);
    finish(app, peer);
}

fn onPeerError(_: ?*anyopaque, peer: *rpc.peer.Peer, err: anyerror) void {
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
    if (g_client_app) |app| {
        if (!app.done) {
            std.log.err("rpc peer error: {s}", .{@errorName(err)});
            app.err = err;
            app.done = true;
        }
    }
}

fn onPeerClose(_: ?*anyopaque, peer: *rpc.peer.Peer) void {
    const allocator = peer.allocator;
    const conn = peer.takeAttachedConnection(*rpc.transport.tcp.Connection);
    var retained_conn: ?*rpc.transport.tcp.Connection = null;

    peer.deinit();
    allocator.destroy(peer);

    if (conn) |attached| {
        attached.deinit();
        if (attached.deinitialized) {
            allocator.destroy(attached);
        } else {
            retained_conn = attached;
        }
    }

    if (g_client_app) |app| {
        app.peer = null;
        app.conn = retained_conn;
        app.done = true;
    }
}

fn bootstrapGameWorld(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    _ = try game_world.GameWorld.Client.fromBootstrap(peer, app, onGameWorldBootstrap);
}

fn onGameWorldBootstrap(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, response: game_world.GameWorld.BootstrapResponse) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    const client = response.unwrap() catch {
        failAndFinish(app, peer, "bootstrap game_world capability");
        return;
    };
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
        app.tap.ok(false, "spawnEntity returns results");
        finish(app, peer);
        return;
    };
    app.tap.ok((try results.getStatus()) == statusOk(game_types.StatusCode), "spawnEntity returns ok status");
    const entity = try results.getEntity();
    app.tap.ok(std.mem.eql(u8, try entity.getName(), "ZigClientHero"), "spawnEntity returns expected name");

    finish(app, peer);
}

fn bootstrapChat(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    _ = try chat.ChatService.Client.fromBootstrap(peer, app, onChatBootstrap);
}

fn onChatBootstrap(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, response: chat.ChatService.BootstrapResponse) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    const client = response.unwrap() catch {
        failAndFinish(app, peer, "bootstrap chat capability");
        return;
    };
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
        app.tap.ok(false, "createRoom returns results");
        finish(app, peer);
        return;
    };
    app.tap.ok((try results.getStatus()) == statusOk(game_types.StatusCode), "createRoom returns ok status");
    const room_cap = try results.getRoom();
    const resolved = try caps.resolveCapability(room_cap);
    app.tap.ok(switch (resolved) {
        .imported => true,
        else => false,
    }, "createRoom returns imported ChatRoom capability");

    finish(app, peer);
}

fn bootstrapInventory(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    _ = try inventory.InventoryService.Client.fromBootstrap(peer, app, onInventoryBootstrap);
}

fn onInventoryBootstrap(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, response: inventory.InventoryService.BootstrapResponse) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    const client = response.unwrap() catch {
        failAndFinish(app, peer, "bootstrap inventory capability");
        return;
    };
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
        app.tap.ok(false, "getInventory returns results");
        finish(app, peer);
        return;
    };
    app.tap.ok((try results.getStatus()) == statusOk(game_types.StatusCode), "getInventory returns ok status");
    const inv = try results.getInventory();
    app.tap.ok((try inv.getUsedSlots()) == 0, "new inventory has zero used slots");

    finish(app, peer);
}

fn bootstrapMatchmaking(app: *ClientApp, peer: *rpc.peer.Peer) !void {
    _ = try matchmaking.MatchmakingService.Client.fromBootstrap(peer, app, onMatchmakingBootstrap);
}

fn onMatchmakingBootstrap(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, response: matchmaking.MatchmakingService.BootstrapResponse) !void {
    const app: *ClientApp = @ptrCast(@alignCast(ctx_ptr));
    const client = response.unwrap() catch {
        failAndFinish(app, peer, "bootstrap matchmaking capability");
        return;
    };
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
        app.tap.ok(false, "enqueue returns results");
        finish(app, peer);
        return;
    };
    app.tap.ok((try results.getStatus()) == statusOk(game_types.StatusCode), "enqueue returns ok status");
    const ticket = try results.getTicket();
    app.tap.ok((try ticket.getTicketId()) > 0, "enqueue returns a non-zero ticket id");

    finish(app, peer);
}

fn parseIp4Address(host: []const u8, port: u16) !std.Io.net.IpAddress {
    var bytes: [4]u8 = undefined;
    var byte_idx: usize = 0;
    var iter = std.mem.splitScalar(u8, host, '.');
    while (iter.next()) |octet| {
        if (byte_idx >= 4) return error.InvalidAddress;
        bytes[byte_idx] = std.fmt.parseInt(u8, octet, 10) catch return error.InvalidAddress;
        byte_idx += 1;
    }
    if (byte_idx != 4) return error.InvalidAddress;
    return .{ .ip4 = .{ .bytes = bytes, .port = port } };
}

fn rawTcpConnect(addr: std.Io.net.IpAddress, io: std.Io) !std.posix.fd_t {
    const stream = try std.Io.net.IpAddress.connect(&addr, io, .{ .mode = .stream });
    return stream.socket.handle;
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
    g_client_app = &app;
    defer g_client_app = null;

    const address = try parseIp4Address(args.host, args.port);
    const fd = try rawTcpConnect(address, io);

    const conn = try allocator.create(rpc.transport.tcp.Connection);
    conn.* = rpc.transport.tcp.Connection.init(allocator, io, .{ .handle = fd }, .{}) catch |err| {
        allocator.destroy(conn);
        rpc.transport.tcp.closeFd(io, .{ .handle = fd });
        return err;
    };
    app.conn = conn;

    const peer = try allocator.create(rpc.peer.Peer);
    peer.* = rpc.peer.Peer.init(allocator, conn);
    app.peer = peer;

    peer.start(null, onPeerError, onPeerClose);

    const start_result = switch (app.args.schema) {
        .game_world => bootstrapGameWorld(&app, peer),
        .chat => bootstrapChat(&app, peer),
        .inventory => bootstrapInventory(&app, peer),
        .matchmaking => bootstrapMatchmaking(&app, peer),
    };

    start_result catch |err| {
        app.err = err;
        if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
    };

    conn.run();

    // peer is cleaned up by onPeerClose. If onPeerClose runs on the
    // connection callback stack, Connection.deinit() is deferred until run()
    // can unwind, so destroy the connection storage here.
    if (app.conn) |attached| {
        if (!attached.deinitialized) attached.deinit();
        allocator.destroy(attached);
        app.conn = null;
    }

    if (app.err) |err| return err;

    std.debug.print("1..{d}\n", .{app.tap.test_num});

    if (app.tap.failures > 0) {
        return error.TestFailed;
    }
}
