const std = @import("std");
const capnp = @import("capnpc-zig");

const message = capnp.message;

const Mode = enum {
    pack,
    unpack,
    roundtrip,
};

const Config = struct {
    iterations: usize = 10_000,
    payload_size: usize = 4096,
    list_len: usize = 2048,
    warmup: usize = 100,
    far_payload: bool = false,
    mode: Mode = .roundtrip,
    json: bool = false,
};

fn printUsage() void {
    var buffer: [1024]u8 = undefined;
    var out = std.fs.File.stdout().writer(&buffer);
    out.interface.print(
        \\Usage: zig build bench-packed -- [options]
        \\       zig build bench-unpacked -- [options]
        \\  --mode pack|unpack|roundtrip  Mode (default: roundtrip)
        \\  --iters N                     Number of iterations (default: 10000)
        \\  --payload N                   Payload size in bytes (default: 4096)
        \\  --list-len N                  U64 list length (default: 2048)
        \\  --warmup N                    Warmup iterations (default: 100)
        \\  --far                         Place payload and list in a second segment
        \\  --json                        Emit machine-readable JSON output
        \\  -h, --help                    Show this help
        \\
    , .{}) catch {};
    out.interface.flush() catch {};
}

fn parseUsize(arg: []const u8) !usize {
    return std.fmt.parseUnsigned(usize, arg, 10);
}

fn parseMode(arg: []const u8) !Mode {
    if (std.mem.eql(u8, arg, "pack")) return .pack;
    if (std.mem.eql(u8, arg, "unpack")) return .unpack;
    if (std.mem.eql(u8, arg, "roundtrip")) return .roundtrip;
    return error.InvalidArgument;
}

fn modeName(mode: Mode) []const u8 {
    return switch (mode) {
        .pack => "pack",
        .unpack => "unpack",
        .roundtrip => "roundtrip",
    };
}

fn parseArgs(allocator: std.mem.Allocator) !?Config {
    var cfg = Config{};
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--mode")) {
            i += 1;
            if (i >= args.len) return error.InvalidArgument;
            cfg.mode = try parseMode(args[i]);
            continue;
        }
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
        if (std.mem.eql(u8, arg, "--list-len") or std.mem.eql(u8, arg, "--list")) {
            i += 1;
            if (i >= args.len) return error.InvalidArgument;
            cfg.list_len = try parseUsize(args[i]);
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
        if (std.mem.eql(u8, arg, "--json")) {
            cfg.json = true;
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

fn buildMessage(
    allocator: std.mem.Allocator,
    payload: []const u8,
    list_len: u32,
    far_payload: bool,
) !message.MessageBuilder {
    var builder = message.MessageBuilder.init(allocator);
    errdefer builder.deinit();

    const root = try builder.allocateStruct(2, 2);
    if (far_payload) {
        _ = try builder.createSegment();
    }

    root.writeU64(0, 0x0123456789abcdef);
    root.writeU32(8, @as(u32, @intCast(payload.len)));

    if (far_payload) {
        try root.writeTextInSegment(0, payload, 1);
    } else {
        try root.writeText(0, payload);
    }

    const list = if (far_payload)
        try root.writeU64ListInSegment(1, list_len, 1)
    else
        try root.writeU64List(1, list_len);

    var idx: u32 = 0;
    while (idx < list_len) : (idx += 1) {
        const value: u64 = if (idx % 4 == 0)
            0
        else
            @as(u64, idx) + 0x0102030405060708;
        try list.set(idx, value);
    }

    return builder;
}

fn doPack(allocator: std.mem.Allocator, builder: *message.MessageBuilder) !u64 {
    const packed_bytes = try builder.toPackedBytes();
    defer allocator.free(packed_bytes);

    var sum: u64 = packed_bytes.len;
    if (packed_bytes.len > 0) sum +%= packed_bytes[0];
    if (packed_bytes.len > 1) sum +%= packed_bytes[packed_bytes.len - 1];
    return sum;
}

fn doUnpack(allocator: std.mem.Allocator, packed_bytes: []const u8) !u64 {
    var msg = try message.Message.initPacked(allocator, packed_bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    var sum: u64 = root.readU64(0);
    sum +%= @as(u64, root.readU32(8));

    const text = try root.readText(0);
    sum +%= text.len;

    const list = try root.readU64List(1);
    if (list.len() > 0) {
        sum +%= try list.get(0);
        const last = list.len() - 1;
        sum +%= try list.get(last);
    }

    return sum;
}

fn doRoundTrip(allocator: std.mem.Allocator, builder: *message.MessageBuilder) !u64 {
    const packed_bytes = try builder.toPackedBytes();
    var msg = try message.Message.initPacked(allocator, packed_bytes);
    allocator.free(packed_bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    var sum: u64 = root.readU64(0);
    sum +%= @as(u64, root.readU32(8));

    const text = try root.readText(0);
    sum +%= text.len;

    const list = try root.readU64List(1);
    if (list.len() > 0) {
        sum +%= try list.get(0);
        const last = list.len() - 1;
        sum +%= try list.get(last);
    }

    return sum;
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

    if (cfg.list_len > std.math.maxInt(u32)) {
        std.debug.print("list length exceeds u32 max\n", .{});
        return;
    }

    const payload = try allocator.alloc(u8, cfg.payload_size);
    defer allocator.free(payload);

    for (payload, 0..) |*byte, idx| {
        byte.* = @as(u8, @intCast('a' + (idx % 26)));
    }

    var builder = try buildMessage(
        allocator,
        payload,
        @as(u32, @intCast(cfg.list_len)),
        cfg.far_payload,
    );
    defer builder.deinit();

    const unpacked_once = try builder.toBytes();
    defer allocator.free(unpacked_once);
    const packed_once = try builder.toPackedBytes();
    defer allocator.free(packed_once);

    var checksum: u64 = 0;

    var warmup_iter: usize = 0;
    while (warmup_iter < cfg.warmup) : (warmup_iter += 1) {
        checksum +%= switch (cfg.mode) {
            .pack => try doPack(allocator, &builder),
            .unpack => try doUnpack(allocator, packed_once),
            .roundtrip => try doRoundTrip(allocator, &builder),
        };
    }

    var timer = try std.time.Timer.start();
    var iter: usize = 0;
    while (iter < cfg.iterations) : (iter += 1) {
        checksum +%= switch (cfg.mode) {
            .pack => try doPack(allocator, &builder),
            .unpack => try doUnpack(allocator, packed_once),
            .roundtrip => try doRoundTrip(allocator, &builder),
        };
    }
    const elapsed_ns = timer.read();

    const elapsed_ns_f = @as(f64, @floatFromInt(elapsed_ns));
    const seconds = elapsed_ns_f / 1_000_000_000.0;
    const iterations_f = @as(f64, @floatFromInt(cfg.iterations));
    const ns_per_iter = elapsed_ns_f / iterations_f;
    const ops_per_sec = iterations_f / seconds;

    const per_iter_bytes: u64 = switch (cfg.mode) {
        .pack => @as(u64, @intCast(unpacked_once.len)),
        .unpack => @as(u64, @intCast(packed_once.len)),
        .roundtrip => @as(u64, @intCast(unpacked_once.len + packed_once.len)),
    };

    const total_bytes = per_iter_bytes * @as(u64, @intCast(cfg.iterations));
    const mib_per_sec = (@as(f64, @floatFromInt(total_bytes)) / (1024.0 * 1024.0)) / seconds;

    const packed_len = packed_once.len;
    const unpacked_len = unpacked_once.len;
    const compression = if (packed_len == 0)
        0.0
    else
        @as(f64, @floatFromInt(unpacked_len)) / @as(f64, @floatFromInt(packed_len));

    if (cfg.json) {
        var out_buffer: [4096]u8 = undefined;
        var out = std.fs.File.stdout().writer(&out_buffer);
        try out.interface.print(
            "{{\"benchmark\":\"packed_unpacked\",\"mode\":\"{s}\",\"iterations\":{d},\"payload_size\":{d},\"list_len\":{d},\"far_payload\":{s},\"unpacked_len\":{d},\"packed_len\":{d},\"compression\":{d:.6},\"elapsed_ns\":{d},\"ns_per_iter\":{d:.6},\"ops_per_sec\":{d:.6},\"mib_per_sec\":{d:.6},\"checksum\":{d}}}\n",
            .{
                modeName(cfg.mode),
                cfg.iterations,
                cfg.payload_size,
                cfg.list_len,
                if (cfg.far_payload) "true" else "false",
                unpacked_len,
                packed_len,
                compression,
                elapsed_ns,
                ns_per_iter,
                ops_per_sec,
                mib_per_sec,
                checksum,
            },
        );
        try out.interface.flush();
        return;
    }

    var out_buffer: [4096]u8 = undefined;
    var out = std.fs.File.stdout().writer(&out_buffer);
    try out.interface.print("packed/unpacked benchmark\n", .{});
    try out.interface.print("mode: {s}\n", .{modeName(cfg.mode)});
    try out.interface.print("iterations: {d}\n", .{cfg.iterations});
    try out.interface.print("payload: {d} bytes\n", .{cfg.payload_size});
    try out.interface.print("list length: {d}\n", .{cfg.list_len});
    try out.interface.print("far payload: {s}\n", .{if (cfg.far_payload) "yes" else "no"});
    try out.interface.print("unpacked bytes: {d}\n", .{unpacked_len});
    try out.interface.print("packed bytes: {d}\n", .{packed_len});
    try out.interface.print("compression: {d:.2}x\n", .{compression});
    try out.interface.print("elapsed: {d:.3} ms\n", .{elapsed_ns_f / 1_000_000.0});
    try out.interface.print("ns/op: {d:.2}\n", .{ns_per_iter});
    try out.interface.print("ops/s: {d:.2}\n", .{ops_per_sec});
    try out.interface.print("MiB/s: {d:.2}\n", .{mib_per_sec});
    try out.interface.print("checksum: {d}\n", .{checksum});
    try out.interface.flush();
}
