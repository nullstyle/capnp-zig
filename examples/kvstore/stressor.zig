const std = @import("std");
const capnpc = @import("capnpc-zig");
const kvstore = @import("gen/kvstore.zig");

const xev = capnpc.xev;
const rpc = capnpc.rpc;
const KvStore = kvstore.KvStore;

const Allocator = std.mem.Allocator;
const Timestamp = i128;

const CliArgs = struct {
    host: []u8 = undefined,
    port: u16 = 9000,
    operations: u64 = 10_000,
    batch_size: u32 = 1,
    concurrency: u32 = 32,
    value_size: usize = 256,
    key_prefix: []u8 = undefined,
};

const KvStoreStressor = struct {
    allocator: Allocator,
    runtime: rpc.runtime.Runtime,
    args: CliArgs,

    value: []const u8,

    peer: ?*rpc.peer.Peer = null,
    conn: ?*rpc.connection.Connection = null,
    client: ?KvStore.Client = null,

    operations_started: u64 = 0,
    operations_completed: u64 = 0,
    operations_inflight: u32 = 0,
    operations_failed: u64 = 0,
    operations_applied: u64 = 0,

    total_latency_ns: u128 = 0,
    min_latency_ns: i128 = std.math.maxInt(i128),
    max_latency_ns: i128 = 0,

    start_ns: Timestamp = 0,
    end_ns: Timestamp = 0,
    last_report_ns: Timestamp = 0,

    done: bool = false,
    err: ?anyerror = null,
};

const RequestCtx = struct {
    stressor: *KvStoreStressor,
    id: u64,
    start_ns: Timestamp,
};

var g_stressor: ?*KvStoreStressor = null;

const StresserError = error{
    InvalidArguments,
    BootstrapException,
    RpcFailure,
    SendFailure,
    PeerClosed,
};

fn parseArgs(allocator: Allocator) !CliArgs {
    var out = CliArgs{};
    var host_text: []const u8 = "127.0.0.1";
    var key_prefix: []const u8 = "kvstore-stress";

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
        if (std.mem.eql(u8, arg, "--operations")) {
            idx += 1;
            if (idx >= argv.len) return error.MissingArgValue;
            out.operations = try std.fmt.parseInt(u64, argv[idx], 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--batch-size")) {
            idx += 1;
            if (idx >= argv.len) return error.MissingArgValue;
            const batch_size = try std.fmt.parseInt(u32, argv[idx], 10);
            if (batch_size == 0) return StresserError.InvalidArguments;
            out.batch_size = batch_size;
            continue;
        }
        if (std.mem.eql(u8, arg, "--concurrency")) {
            idx += 1;
            if (idx >= argv.len) return error.MissingArgValue;
            const concurrency = try std.fmt.parseInt(u32, argv[idx], 10);
            if (concurrency == 0) return StresserError.InvalidArguments;
            out.concurrency = concurrency;
            continue;
        }
        if (std.mem.eql(u8, arg, "--value-size")) {
            idx += 1;
            if (idx >= argv.len) return error.MissingArgValue;
            out.value_size = try std.fmt.parseInt(usize, argv[idx], 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--prefix")) {
            idx += 1;
            if (idx >= argv.len) return error.MissingArgValue;
            key_prefix = argv[idx];
            continue;
        }
    }

    out.host = try allocator.dupe(u8, host_text);
    out.key_prefix = try allocator.dupe(u8, key_prefix);
    return out;
}

fn buildValue(allocator: Allocator, value_size: usize) ![]u8 {
    const out = try allocator.alloc(u8, value_size);
    if (value_size == 0) return out;

    for (out, 0..) |*slot, idx| {
        slot.* = @as(u8, @intCast((idx * 7 + 13) % 256));
    }
    return out;
}

fn usage() void {
    std.debug.print(
        \\Usage: kvstore-stressor [--host 127.0.0.1] [--port 9000]
        \\  --operations 10000      Number of writeBatch RPCs to send
        \\  --batch-size 1          Number of puts in each writeBatch
        \\  --concurrency 32        Max number of in-flight writeBatch RPCs
        \\  --value-size 256        Size in bytes of value payload
        \\  --prefix kvstore-stress  Key prefix for generated writes
        \\
    , .{});
}

fn reportProgress(stressor: *KvStoreStressor) void {
    const now = std.time.nanoTimestamp();
    const elapsed_since_last = now - stressor.last_report_ns;
    if (elapsed_since_last < @as(Timestamp, std.time.ns_per_s)) return;
    if (stressor.start_ns == 0) return;

    const elapsed_ns = now - stressor.start_ns;
    if (elapsed_ns <= 0) return;

    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const ops_sec = @as(f64, @floatFromInt(stressor.operations_completed)) / (elapsed_ms / 1_000.0);
    const avg_latency_us = if (stressor.operations_completed == 0)
        0
    else
        @as(f64, @floatFromInt(stressor.total_latency_ns / stressor.operations_completed)) / 1_000.0;

    std.debug.print(
        "\rcompleted {d}/{d} [in-flight {d}]  elapsed {d:.2}ms  throughput {d:.2} ops/s  latency(avg) {d:.2}us",
        .{
            stressor.operations_completed,
            stressor.operations_started,
            stressor.operations_inflight,
            elapsed_ms,
            ops_sec,
            avg_latency_us,
        },
    );
    stressor.last_report_ns = now;
}

fn printSummary(stressor: *KvStoreStressor) void {
    const end_ns = if (stressor.end_ns != 0) stressor.end_ns else std.time.nanoTimestamp();
    const elapsed_ns = if (end_ns <= stressor.start_ns or stressor.start_ns == 0) 0 else end_ns - stressor.start_ns;
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const ops_sec = if (elapsed_ns == 0) 0 else
        @as(f64, @floatFromInt(stressor.operations_completed)) / (@as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0);
    const avg_latency_ns = if (stressor.operations_completed == 0) 0 else stressor.total_latency_ns / stressor.operations_completed;
    const min_latency_ns = if (stressor.operations_completed == 0) @as(i128, 0) else stressor.min_latency_ns;

    std.debug.print(
        "\n\nKVStore stressor summary:\n",
        .{},
    );
    std.debug.print(
        "  duration_ms     : {d:.2}\n",
        .{elapsed_ms},
    );
    std.debug.print(
        "  ops_target      : {d}\n",
        .{stressor.args.operations},
    );
    std.debug.print(
        "  ops_started     : {d}\n",
        .{stressor.operations_started},
    );
    std.debug.print(
        "  ops_completed   : {d}\n",
        .{stressor.operations_completed},
    );
    std.debug.print(
        "  ops_failed      : {d}\n",
        .{stressor.operations_failed},
    );
    std.debug.print(
        "  ops_applied     : {d}\n",
        .{stressor.operations_applied},
    );
    std.debug.print(
        "  throughput_ops  : {d:.2}\n",
        .{ops_sec},
    );
    std.debug.print(
        "  latency_avg_us  : {d:.2}\n",
        .{@as(f64, @floatFromInt(avg_latency_ns)) / 1_000.0},
    );
    std.debug.print(
        "  latency_min_us  : {d:.2}\n",
        .{@as(f64, @floatFromInt(min_latency_ns)) / 1_000.0},
    );
    std.debug.print(
        "  latency_max_us  : {d:.2}\n",
        .{@as(f64, @floatFromInt(stressor.max_latency_ns)) / 1_000.0},
    );
    if (stressor.err) |err| {
        std.debug.print("  error           : {s}\n", .{@errorName(err)});
    }
}

fn fail(stressor: *KvStoreStressor, reason: anyerror, peer: ?*rpc.peer.Peer) void {
    if (stressor.err != null) return;
    if (stressor.done) return;
    stressor.err = reason;
    stressor.done = true;
    if (stressor.end_ns == 0) stressor.end_ns = std.time.nanoTimestamp();
    if (peer) |active_peer| {
        if (!active_peer.isAttachedTransportClosing()) {
            active_peer.closeAttachedTransport();
        }
    }
}

fn onWriteBatchReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: KvStore.WriteBatch.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const request_ctx: *RequestCtx = @ptrCast(@alignCast(ctx_ptr));
    const stressor: *KvStoreStressor = request_ctx.stressor;
    defer stressor.allocator.destroy(request_ctx);

    if (stressor.err != null) return;
    if (stressor.done) return;

    const duration_ns = std.time.nanoTimestamp() - request_ctx.start_ns;
    stressor.operations_inflight -= 1;
    if (duration_ns > 0) {
        stressor.total_latency_ns += @as(u128, @intCast(duration_ns));
    }
    if (duration_ns < stressor.min_latency_ns) stressor.min_latency_ns = duration_ns;
    if (duration_ns > stressor.max_latency_ns) stressor.max_latency_ns = duration_ns;

    switch (response) {
        .results => |results| {
            const applied = try results.getApplied();
            _ = try results.getNextVersion();
            const returned_results = try results.getResults();
            _ = returned_results;

            stressor.operations_completed += 1;
            stressor.operations_applied += applied;
        },
        .exception => |_| {
            stressor.operations_failed += 1;
            fail(stressor, StresserError.RpcFailure, peer);
            return;
        },
        else => {
            stressor.operations_failed += 1;
            fail(stressor, StresserError.RpcFailure, peer);
            return;
        },
    }

    reportProgress(stressor);

    if (stressor.err != null) return;
    if (stressor.done) return;

    if (stressor.operations_completed >= stressor.args.operations and stressor.operations_inflight == 0) {
        stressor.done = true;
        stressor.end_ns = std.time.nanoTimestamp();
        if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
        return;
    }

    pumpRequests(stressor, peer) catch {};
}

fn buildWriteBatch(ctx_ptr: *anyopaque, params: *KvStore.WriteBatch.Params.Builder) anyerror!void {
    const request_ctx: *RequestCtx = @ptrCast(@alignCast(ctx_ptr));
    const stressor = request_ctx.stressor;

    var ops = try params.initOps(stressor.args.batch_size);
    var op_idx: u32 = 0;
    while (op_idx < stressor.args.batch_size) : (op_idx += 1) {
        var out_op = try ops.get(op_idx);
        const key_id = request_ctx.id * @as(u64, stressor.args.batch_size) + @as(u64, op_idx);
        var key_buf: [128]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "{s}-{d}", .{ stressor.args.key_prefix, key_id });
        try out_op.setKey(key);
        try out_op.setPut(stressor.value);
    }
}

fn pumpRequests(stressor: *KvStoreStressor, peer: *rpc.peer.Peer) !void {
    if (stressor.err != null) return;
    if (stressor.done) return;
    if (stressor.client == null) return;
    var client = stressor.client.?;

    while (stressor.operations_started < stressor.args.operations and stressor.operations_inflight < stressor.args.concurrency) {
        if (stressor.done) return;

        const request_id = stressor.operations_started + 1;
        const request_ctx = try stressor.allocator.create(RequestCtx);
        request_ctx.* = .{
            .stressor = stressor,
            .id = request_id,
            .start_ns = std.time.nanoTimestamp(),
        };

        _ = try client.callWriteBatch(
            request_ctx,
            buildWriteBatch,
            onWriteBatchReturn,
        );

        stressor.operations_started += 1;
        stressor.operations_inflight += 1;
        _ = peer;

        reportProgress(stressor);
    }
}

fn onBootstrap(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: KvStore.BootstrapResponse,
) anyerror!void {
    const stressor: *KvStoreStressor = @ptrCast(@alignCast(ctx_ptr));
    if (stressor.err != null) return;
    if (stressor.done) return;

    switch (response) {
        .client => |client| {
            stressor.client = client;
            if (stressor.args.operations == 0) {
                stressor.done = true;
                stressor.end_ns = std.time.nanoTimestamp();
                if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
                return;
            }

            stressor.start_ns = std.time.nanoTimestamp();
            stressor.last_report_ns = stressor.start_ns;
            pumpRequests(stressor, peer) catch |err| {
                fail(stressor, err, peer);
            };
        },
        .exception => {
            fail(stressor, StresserError.BootstrapException, peer);
        },
        else => {
            fail(stressor, StresserError.BootstrapException, peer);
        },
    }
}

fn onPeerError(peer: *rpc.peer.Peer, err: anyerror) void {
    if (!peer.isAttachedTransportClosing()) {
        peer.closeAttachedTransport();
    }

    if (g_stressor) |stressor| {
        fail(stressor, err, peer);
    }
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

    if (g_stressor) |stressor| {
        if (stressor.err == null and !stressor.done) {
            fail(stressor, StresserError.PeerClosed, null);
        }
        stressor.peer = null;
        stressor.conn = null;
        if (stressor.start_ns != 0 and stressor.end_ns == 0) {
            stressor.end_ns = std.time.nanoTimestamp();
        }
    }
}

fn onConnect(
    ctx: ?*KvStoreStressor,
    loop: *xev.Loop,
    _: *xev.Completion,
    socket: xev.TCP,
    res: xev.ConnectError!void,
) xev.CallbackAction {
    const stressor = ctx orelse return .disarm;

    if (res) |_| {
        const conn = stressor.allocator.create(rpc.connection.Connection) catch {
            fail(stressor, error.OutOfMemory, null);
            return .disarm;
        };

        conn.* = rpc.connection.Connection.init(stressor.allocator, loop, socket, .{}) catch |err| {
            stressor.allocator.destroy(conn);
            fail(stressor, err, null);
            return .disarm;
        };

        const peer = stressor.allocator.create(rpc.peer.Peer) catch {
            conn.deinit();
            stressor.allocator.destroy(conn);
            fail(stressor, error.OutOfMemory, null);
            return .disarm;
        };
        peer.* = rpc.peer.Peer.init(stressor.allocator, conn);

        stressor.conn = conn;
        stressor.peer = peer;
        peer.start(onPeerError, onPeerClose);

        _ = KvStore.Client.fromBootstrap(peer, stressor, onBootstrap) catch |err| {
            stressor.peer = peer;
            fail(stressor, err, peer);
        };
    } else |err| {
        fail(stressor, err, null);
    }

    return .disarm;
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
        error.InvalidCharacter,
        error.Overflow,
        error.MissingArgValue,
        error.InvalidArguments,
        => {
            usage();
            return err;
        },
        else => return err,
    };
    defer allocator.free(args.host);
    defer allocator.free(args.key_prefix);

    const value = try buildValue(allocator, args.value_size);
    defer allocator.free(value);

    var stressor = KvStoreStressor{
        .allocator = allocator,
        .runtime = try rpc.runtime.Runtime.init(allocator),
        .args = args,
        .value = value,
    };
    defer stressor.runtime.deinit();

    g_stressor = &stressor;
    defer g_stressor = null;

    const address = try std.net.Address.parseIp4(args.host, args.port);
    var socket = try xev.TCP.init(address);
    var connect_completion: xev.Completion = .{};

    socket.connect(&stressor.runtime.loop, &connect_completion, address, KvStoreStressor, &stressor, onConnect);

    while (!stressor.done) {
        try stressor.runtime.run(.once);
    }

    if (stressor.end_ns == 0 and stressor.start_ns != 0) {
        stressor.end_ns = std.time.nanoTimestamp();
    }

    printSummary(&stressor);
    if (stressor.err) |err| return err;
}
