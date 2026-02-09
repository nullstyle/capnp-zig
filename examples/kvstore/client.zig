const std = @import("std");
const capnpc = @import("capnpc-zig");
const xev = capnpc.xev;
const kvstore = @import("kvstore.zig");

const rpc = capnpc.rpc;
const KvStore = kvstore.KvStore;
const Entry = kvstore.Entry;

const Allocator = std.mem.Allocator;

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

const App = struct {
    allocator: Allocator,
    runtime: rpc.runtime.Runtime,
    done: bool = false,
    err: ?anyerror = null,
    peer: ?*rpc.peer.Peer = null,
    conn: ?*rpc.connection.Connection = null,
    client: ?KvStore.Client = null,
};

var g_app: ?*App = null;

// ---------------------------------------------------------------------------
// Peer lifecycle
// ---------------------------------------------------------------------------

fn onPeerError(peer: *rpc.peer.Peer, err: anyerror) void {
    std.log.err("peer error: {s}", .{@errorName(err)});
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
    if (g_app) |app| {
        app.err = err;
        app.done = true;
    }
}

fn onPeerClose(peer: *rpc.peer.Peer) void {
    _ = peer;
    if (g_app) |app| {
        app.done = true;
    }
}

fn finish(app: *App) void {
    app.done = true;
    if (app.peer) |peer| {
        if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
    }
}

// ---------------------------------------------------------------------------
// Demo sequence — 7 chained RPC calls
// ---------------------------------------------------------------------------

// Step 1: SET "hello" = "world"
fn step1(app: *App) void {
    var client = app.client orelse return;
    _ = client.callSet(app, buildSet1, onSetReturn1) catch |err| {
        app.err = err;
        app.done = true;
        return;
    };
}

fn buildSet1(_: *anyopaque, params: *KvStore.Set.Params.Builder) anyerror!void {
    try params.setKey("hello");
    try params.setValue("world");
}

fn onSetReturn1(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.Set.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const app: *App = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .results => |results| {
            const entry = try results.getEntry();
            const key = try entry.getKey();
            const version = try entry.getVersion();
            std.debug.print("1. SET \"{s}\" = \"world\" -> version {d}\n", .{ key, version });
        },
        .exception => |ex| {
            std.debug.print("1. SET failed: {s}\n", .{ex.reason});
            finish(app);
            return;
        },
        else => {
            std.debug.print("1. SET unexpected response\n", .{});
            finish(app);
            return;
        },
    }
    step2(app);
}

// Step 2: SET "count" = [0,0,0,42] (binary data)
fn step2(app: *App) void {
    var client = app.client orelse return;
    _ = client.callSet(app, buildSet2, onSetReturn2) catch |err| {
        app.err = err;
        app.done = true;
        return;
    };
}

fn buildSet2(_: *anyopaque, params: *KvStore.Set.Params.Builder) anyerror!void {
    try params.setKey("count");
    try params.setValue(&[_]u8{ 0, 0, 0, 42 });
}

fn onSetReturn2(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.Set.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const app: *App = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .results => |results| {
            const entry = try results.getEntry();
            const key = try entry.getKey();
            const version = try entry.getVersion();
            std.debug.print("2. SET \"{s}\" = [0,0,0,42] -> version {d}\n", .{ key, version });
        },
        .exception => |ex| {
            std.debug.print("2. SET failed: {s}\n", .{ex.reason});
            finish(app);
            return;
        },
        else => {
            std.debug.print("2. SET unexpected response\n", .{});
            finish(app);
            return;
        },
    }
    step3(app);
}

// Step 3: GET "hello"
fn step3(app: *App) void {
    var client = app.client orelse return;
    _ = client.callGet(app, buildGet3, onGetReturn3) catch |err| {
        app.err = err;
        app.done = true;
        return;
    };
}

fn buildGet3(_: *anyopaque, params: *KvStore.Get.Params.Builder) anyerror!void {
    try params.setKey("hello");
}

fn onGetReturn3(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.Get.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const app: *App = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .results => |results| {
            const found = try results.getFound();
            const value = try results.getValue();
            std.debug.print("3. GET \"hello\" -> found={}, value=\"{s}\"\n", .{ found, value });
        },
        .exception => |ex| {
            std.debug.print("3. GET failed: {s}\n", .{ex.reason});
            finish(app);
            return;
        },
        else => {
            std.debug.print("3. GET unexpected response\n", .{});
            finish(app);
            return;
        },
    }
    step4(app);
}

// Step 4: SET "hello" = "updated" (overwrite, version bump)
fn step4(app: *App) void {
    var client = app.client orelse return;
    _ = client.callSet(app, buildSet4, onSetReturn4) catch |err| {
        app.err = err;
        app.done = true;
        return;
    };
}

fn buildSet4(_: *anyopaque, params: *KvStore.Set.Params.Builder) anyerror!void {
    try params.setKey("hello");
    try params.setValue("updated");
}

fn onSetReturn4(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.Set.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const app: *App = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .results => |results| {
            const entry = try results.getEntry();
            const key = try entry.getKey();
            const version = try entry.getVersion();
            std.debug.print("4. SET \"{s}\" = \"updated\" -> version {d}\n", .{ key, version });
        },
        .exception => |ex| {
            std.debug.print("4. SET failed: {s}\n", .{ex.reason});
            finish(app);
            return;
        },
        else => {
            std.debug.print("4. SET unexpected response\n", .{});
            finish(app);
            return;
        },
    }
    step5(app);
}

// Step 5: LIST prefix="" limit=10 (list all entries)
fn step5(app: *App) void {
    var client = app.client orelse return;
    _ = client.callList(app, buildList5, onListReturn5) catch |err| {
        app.err = err;
        app.done = true;
        return;
    };
}

fn buildList5(_: *anyopaque, params: *KvStore.List.Params.Builder) anyerror!void {
    try params.setPrefix("");
    try params.setLimit(10);
}

fn onListReturn5(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.List.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const app: *App = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .results => |results| {
            const entries = try results.getEntries();
            const count = entries.len();
            std.debug.print("5. LIST \"\" limit=10 -> {d} entries:\n", .{count});
            for (0..count) |i| {
                const entry = try entries.get(@intCast(i));
                const key = try entry.getKey();
                const value = try entry.getValue();
                const version = try entry.getVersion();
                std.debug.print("     [{d}] \"{s}\" = {d} bytes, version {d}\n", .{ i, key, value.len, version });
            }
        },
        .exception => |ex| {
            std.debug.print("5. LIST failed: {s}\n", .{ex.reason});
            finish(app);
            return;
        },
        else => {
            std.debug.print("5. LIST unexpected response\n", .{});
            finish(app);
            return;
        },
    }
    step6(app);
}

// Step 6: DELETE "count"
fn step6(app: *App) void {
    var client = app.client orelse return;
    _ = client.callDelete(app, buildDelete6, onDeleteReturn6) catch |err| {
        app.err = err;
        app.done = true;
        return;
    };
}

fn buildDelete6(_: *anyopaque, params: *KvStore.Delete.Params.Builder) anyerror!void {
    try params.setKey("count");
}

fn onDeleteReturn6(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.Delete.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const app: *App = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .results => |results| {
            const found = try results.getFound();
            std.debug.print("6. DELETE \"count\" -> found={}\n", .{found});
        },
        .exception => |ex| {
            std.debug.print("6. DELETE failed: {s}\n", .{ex.reason});
            finish(app);
            return;
        },
        else => {
            std.debug.print("6. DELETE unexpected response\n", .{});
            finish(app);
            return;
        },
    }
    step7(app);
}

// Step 7: GET "count" (verify deleted)
fn step7(app: *App) void {
    var client = app.client orelse return;
    _ = client.callGet(app, buildGet7, onGetReturn7) catch |err| {
        app.err = err;
        app.done = true;
        return;
    };
}

fn buildGet7(_: *anyopaque, params: *KvStore.Get.Params.Builder) anyerror!void {
    try params.setKey("count");
}

fn onGetReturn7(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.Get.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const app: *App = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .results => |results| {
            const found = try results.getFound();
            std.debug.print("7. GET \"count\" -> found={}\n", .{found});
            std.debug.print("\nAll operations completed successfully!\n", .{});
        },
        .exception => |ex| {
            std.debug.print("7. GET failed: {s}\n", .{ex.reason});
        },
        else => {
            std.debug.print("7. GET unexpected response\n", .{});
        },
    }
    finish(app);
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

fn onBootstrap(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.BootstrapResponse,
) anyerror!void {
    const app: *App = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .client => |client| {
            std.debug.print("Connected to KvStore server\n\n", .{});
            app.client = client;
            step1(app);
        },
        .exception => |ex| {
            std.debug.print("Bootstrap failed: {s}\n", .{ex.reason});
            app.err = error.BootstrapFailed;
            app.done = true;
        },
        else => {
            std.debug.print("Unexpected bootstrap response\n", .{});
            app.err = error.UnexpectedResponse;
            app.done = true;
        },
    }
}

// ---------------------------------------------------------------------------
// TCP connect
// ---------------------------------------------------------------------------

const ConnectCtx = struct {
    app: *App,
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

        _ = KvStore.Client.fromBootstrap(peer, app, onBootstrap) catch |err| {
            app.err = err;
            app.done = true;
        };
    } else |err| {
        app.err = err;
        app.done = true;
    }

    return .disarm;
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

const CliArgs = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 9000,
};

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
    }

    out.host = try allocator.dupe(u8, host_text);
    return out;
}

fn usage() void {
    std.debug.print(
        \\Usage: kvstore-client [--host 127.0.0.1] [--port 9000]
        \\
    , .{});
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = parseArgs(allocator) catch |err| switch (err) {
        error.HelpRequested => {
            usage();
            return;
        },
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

    var app = App{
        .allocator = allocator,
        .runtime = try rpc.runtime.Runtime.init(allocator),
    };
    defer app.runtime.deinit();
    g_app = &app;
    defer g_app = null;

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
}
