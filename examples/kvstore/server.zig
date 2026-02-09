const std = @import("std");
const capnpc = @import("capnpc-zig");
const kvstore = @import("kvstore.zig");

const rpc = capnpc.rpc;
const KvStore = kvstore.KvStore;
const Entry = kvstore.Entry;

const Allocator = std.mem.Allocator;

// ---------------------------------------------------------------------------
// KV service state
// ---------------------------------------------------------------------------

const StoredEntry = struct {
    key: []const u8,
    value: []const u8,
    version: u64,
};

const KvService = struct {
    allocator: Allocator,
    store: std.StringHashMap(StoredEntry),
    next_version: u64 = 1,
    server: KvStore.Server,

    fn init(allocator: Allocator) KvService {
        return .{
            .allocator = allocator,
            .store = std.StringHashMap(StoredEntry).init(allocator),
            .server = undefined,
        };
    }

    /// Must be called after the KvService is at its final memory location.
    fn bind(self: *KvService) void {
        self.server = .{
            .ctx = self,
            .vtable = .{
                .get = handleGet,
                .set = handleSet,
                .delete = handleDelete,
                .list = handleList,
            },
        };
    }

    fn deinit(self: *KvService) void {
        var it = self.store.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.value);
        }
        self.store.deinit();
    }
};

// ---------------------------------------------------------------------------
// RPC handlers
// ---------------------------------------------------------------------------

fn handleGet(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: KvStore.Get.Params.Reader,
    results: *KvStore.Get.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    const key = try params.getKey();
    std.log.info("GET \"{s}\"", .{key});

    if (svc.store.get(key)) |entry| {
        try results.setValue(entry.value);
        try results.setFound(true);
    } else {
        try results.setFound(false);
    }
}

fn handleSet(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: KvStore.Set.Params.Reader,
    results: *KvStore.Set.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    const key = try params.getKey();
    const value = try params.getValue();

    const version = svc.next_version;
    svc.next_version += 1;

    const owned_value = try svc.allocator.dupe(u8, value);
    errdefer svc.allocator.free(owned_value);

    if (svc.store.getPtr(key)) |existing| {
        std.log.info("SET \"{s}\" ({d} bytes) -> version {d} (update)", .{ key, value.len, version });
        svc.allocator.free(existing.value);
        existing.value = owned_value;
        existing.version = version;
    } else {
        const owned_key = try svc.allocator.dupe(u8, key);
        errdefer svc.allocator.free(owned_key);
        std.log.info("SET \"{s}\" ({d} bytes) -> version {d} (new)", .{ key, value.len, version });
        try svc.store.put(owned_key, .{
            .key = owned_key,
            .value = owned_value,
            .version = version,
        });
    }

    var entry = try results.initEntry();
    try entry.setKey(key);
    try entry.setValue(value);
    try entry.setVersion(version);
}

fn handleDelete(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: KvStore.Delete.Params.Reader,
    results: *KvStore.Delete.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    const key = try params.getKey();
    std.log.info("DELETE \"{s}\"", .{key});

    if (svc.store.fetchRemove(key)) |kv| {
        svc.allocator.free(kv.key);
        svc.allocator.free(kv.value.value);
        try results.setFound(true);
    } else {
        try results.setFound(false);
    }
}

fn handleList(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: KvStore.List.Params.Reader,
    results: *KvStore.List.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    const prefix = try params.getPrefix();
    const limit = try params.getLimit();
    std.log.info("LIST prefix=\"{s}\" limit={d}", .{ prefix, limit });

    // Count matching entries (capped at limit).
    var count: u32 = 0;
    {
        var it = svc.store.iterator();
        while (it.next()) |kv| {
            if (prefix.len == 0 or std.mem.startsWith(u8, kv.key_ptr.*, prefix)) {
                count += 1;
                if (count >= limit) break;
            }
        }
    }

    var entries = try results.initEntries(count);
    var idx: u32 = 0;
    var it = svc.store.iterator();
    while (it.next()) |kv| {
        if (idx >= count) break;
        if (prefix.len == 0 or std.mem.startsWith(u8, kv.key_ptr.*, prefix)) {
            var entry = try entries.get(idx);
            try entry.setKey(kv.value_ptr.key);
            try entry.setValue(kv.value_ptr.value);
            try entry.setVersion(kv.value_ptr.version);
            idx += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Peer lifecycle
// ---------------------------------------------------------------------------

fn onPeerError(peer: *rpc.peer.Peer, err: anyerror) void {
    std.log.err("peer error: {s}", .{@errorName(err)});
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

    std.log.info("client disconnected", .{});
}

// ---------------------------------------------------------------------------
// Listener
// ---------------------------------------------------------------------------

const ListenerCtx = struct {
    listener: rpc.runtime.Listener,
    svc: *KvService,
};

fn onAccept(listener: *rpc.runtime.Listener, conn: *rpc.connection.Connection) void {
    const ctx: *ListenerCtx = @fieldParentPtr("listener", listener);
    const allocator = ctx.svc.allocator;

    const peer = allocator.create(rpc.peer.Peer) catch {
        conn.deinit();
        allocator.destroy(conn);
        return;
    };

    peer.* = rpc.peer.Peer.init(allocator, conn);

    _ = KvStore.setBootstrap(peer, &ctx.svc.server) catch |err| {
        std.log.err("failed to set bootstrap: {s}", .{@errorName(err)});
        peer.deinit();
        allocator.destroy(peer);
        conn.deinit();
        allocator.destroy(conn);
        return;
    };

    peer.start(onPeerError, onPeerClose);
    std.log.info("client connected", .{});
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

const CliArgs = struct {
    host: []const u8 = "0.0.0.0",
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
        \\Usage: kvstore-server [--host 0.0.0.0] [--port 9000]
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

    var runtime = try rpc.runtime.Runtime.init(allocator);
    defer runtime.deinit();

    var svc = KvService.init(allocator);
    defer svc.deinit();
    svc.bind();

    const address = try std.net.Address.parseIp4(args.host, args.port);

    var listener_ctx = ListenerCtx{
        .svc = &svc,
        .listener = try rpc.runtime.Listener.init(
            allocator,
            &runtime.loop,
            address,
            onAccept,
            .{},
        ),
    };
    defer listener_ctx.listener.close();

    listener_ctx.listener.start();

    std.debug.print("READY on {s}:{d}\n", .{ args.host, args.port });

    while (true) {
        try runtime.run(.once);
    }
}
