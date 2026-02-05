const std = @import("std");
const capnp = @import("capnpc-zig");

const message = capnp.message;

const Config = struct {
    iterations: usize = 10_000,
    payload_size: usize = 1024,
    warmup: usize = 100,
    far_payload: bool = false,
};

const IterationResult = struct {
    ping_size: usize,
    pong_size: usize,
    checksum: u64,
};

fn printUsage() void {
    var buffer: [1024]u8 = undefined;
    var out = std.fs.File.stdout().writer(&buffer);
    out.interface.print(
        \\Usage: zig build bench-ping-pong -- [options]
        \\  --iters N    Number of iterations (default: 10000)
        \\  --payload N  Payload size in bytes (default: 1024)
        \\  --warmup N   Warmup iterations (default: 100)
        \\  --far        Place payload in a second segment (far pointer)
        \\  -h, --help   Show this help
        \\
    , .{}) catch {};
    out.interface.flush() catch {};
}

fn parseUsize(arg: []const u8) !usize {
    return std.fmt.parseUnsigned(usize, arg, 10);
}

fn parseArgs(allocator: std.mem.Allocator) !?Config {
    var cfg = Config{};
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--iters")) {
            i += 1;
            if (i >= args.len) return error.InvalidArgument;
            cfg.iterations = try parseUsize(args[i]);
            continue;
        }
        if (std.mem.eql(u8, arg, "--payload")) {
            i += 1;
            if (i >= args.len) return error.InvalidArgument;
            cfg.payload_size = try parseUsize(args[i]);
            continue;
        }
        if (std.mem.eql(u8, arg, "--warmup")) {
            i += 1;
            if (i >= args.len) return error.InvalidArgument;
            cfg.warmup = try parseUsize(args[i]);
            continue;
        }
        if (std.mem.eql(u8, arg, "--far")) {
            cfg.far_payload = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            printUsage();
            return null;
        }

        std.debug.print("Unknown argument: {s}\n", .{arg});
        printUsage();
        return error.InvalidArgument;
    }

    return cfg;
}

fn runPingPong(
    ping_arena: *std.heap.ArenaAllocator,
    pong_arena: *std.heap.ArenaAllocator,
    payload: []const u8,
    id: u64,
    far_payload: bool,
) !IterationResult {
    _ = ping_arena.reset(.retain_capacity);
    _ = pong_arena.reset(.retain_capacity);

    var ping_builder = message.MessageBuilder.init(ping_arena.allocator());
    defer ping_builder.deinit();

    const ping_root = try ping_builder.allocateStruct(2, 1);
    if (far_payload) {
        _ = try ping_builder.createSegment();
    }

    ping_root.writeU64(0, id);
    ping_root.writeU32(8, @as(u32, @intCast(payload.len)));

    if (far_payload) {
        try ping_root.writeTextInSegment(0, payload, 1);
    } else {
        try ping_root.writeText(0, payload);
    }

    const ping_bytes = try ping_builder.toBytes();

    var ping_message = try message.Message.init(ping_arena.allocator(), ping_bytes);
    defer ping_message.deinit();

    const ping_reader = try ping_message.getRootStruct();
    const recv_id = ping_reader.readU64(0);
    const recv_payload = try ping_reader.readText(0);

    var pong_builder = message.MessageBuilder.init(pong_arena.allocator());
    defer pong_builder.deinit();

    const pong_root = try pong_builder.allocateStruct(2, 1);
    if (far_payload) {
        _ = try pong_builder.createSegment();
    }

    pong_root.writeU64(0, recv_id + 1);
    pong_root.writeU32(8, @as(u32, @intCast(recv_payload.len)));

    if (far_payload) {
        try pong_root.writeTextInSegment(0, recv_payload, 1);
    } else {
        try pong_root.writeText(0, recv_payload);
    }

    const pong_bytes = try pong_builder.toBytes();

    var pong_message = try message.Message.init(pong_arena.allocator(), pong_bytes);
    defer pong_message.deinit();

    const pong_reader = try pong_message.getRootStruct();
    const pong_id = pong_reader.readU64(0);
    const pong_payload = try pong_reader.readText(0);

    var checksum: u64 = pong_id;
    checksum +%= @as(u64, @intCast(pong_payload.len));
    if (pong_payload.len > 0) {
        checksum +%= pong_payload[0];
    }

    return .{
        .ping_size = ping_bytes.len,
        .pong_size = pong_bytes.len,
        .checksum = checksum,
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) {
            std.debug.print("warning: allocator reported leaks\n", .{});
        }
    }

    const allocator = gpa.allocator();

    const cfg = (parseArgs(allocator) catch |err| {
        std.debug.print("Argument error: {s}\n", .{@errorName(err)});
        return;
    }) orelse return;

    if (cfg.iterations == 0) {
        std.debug.print("iterations must be greater than zero\n", .{});
        return;
    }

    if (cfg.payload_size > std.math.maxInt(u32)) {
        std.debug.print("payload size exceeds u32 max\n", .{});
        return;
    }

    const payload = try allocator.alloc(u8, cfg.payload_size);
    defer allocator.free(payload);

    for (payload, 0..) |*byte, idx| {
        byte.* = @as(u8, @intCast('a' + (idx % 26)));
    }

    var ping_arena = std.heap.ArenaAllocator.init(allocator);
    defer ping_arena.deinit();

    var pong_arena = std.heap.ArenaAllocator.init(allocator);
    defer pong_arena.deinit();

    const result = try runPingPong(&ping_arena, &pong_arena, payload, 0, cfg.far_payload);
    var checksum: u64 = result.checksum;
    const ping_size = result.ping_size;
    const pong_size = result.pong_size;

    var warmup_iter: usize = 1;
    while (warmup_iter < cfg.warmup) : (warmup_iter += 1) {
        const warm = try runPingPong(&ping_arena, &pong_arena, payload, @as(u64, @intCast(warmup_iter)), cfg.far_payload);
        checksum +%= warm.checksum;
    }

    var timer = try std.time.Timer.start();
    var iter: usize = 0;
    while (iter < cfg.iterations) : (iter += 1) {
        const run = try runPingPong(&ping_arena, &pong_arena, payload, @as(u64, @intCast(iter + 1)), cfg.far_payload);
        checksum +%= run.checksum;
    }
    const elapsed_ns = timer.read();

    const elapsed_ns_f = @as(f64, @floatFromInt(elapsed_ns));
    const seconds = elapsed_ns_f / 1_000_000_000.0;
    const iterations_f = @as(f64, @floatFromInt(cfg.iterations));
    const ns_per_iter = elapsed_ns_f / iterations_f;
    const ops_per_sec = iterations_f / seconds;

    const per_iter_bytes = @as(u64, @intCast(ping_size + pong_size));
    const total_bytes = per_iter_bytes * @as(u64, @intCast(cfg.iterations));
    const mib_per_sec = (@as(f64, @floatFromInt(total_bytes)) / (1024.0 * 1024.0)) / seconds;

    var out_buffer: [4096]u8 = undefined;
    var out = std.fs.File.stdout().writer(&out_buffer);
    try out.interface.print("ping-pong benchmark\n", .{});
    try out.interface.print("iterations: {d}\n", .{cfg.iterations});
    try out.interface.print("payload: {d} bytes\n", .{cfg.payload_size});
    try out.interface.print("far payload: {s}\n", .{if (cfg.far_payload) "yes" else "no"});
    try out.interface.print("ping bytes: {d}\n", .{ping_size});
    try out.interface.print("pong bytes: {d}\n", .{pong_size});
    try out.interface.print("elapsed: {d:.3} ms\n", .{elapsed_ns_f / 1_000_000.0});
    try out.interface.print("ns/op: {d:.2}\n", .{ns_per_iter});
    try out.interface.print("ops/s: {d:.2}\n", .{ops_per_sec});
    try out.interface.print("MiB/s: {d:.2}\n", .{mib_per_sec});
    try out.interface.print("checksum: {d}\n", .{checksum});
    try out.interface.flush();
}
