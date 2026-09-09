//! Reproducible ReleaseSafe receipt: this source also compiles against the
//! preceding sprint's runtime and freshly generated bindings. Output is JSONL.
const std = @import("std");
const capnp = @import("capnpc-zig");
const generated = @import("generated");
const Counter = @import("alloc-counter").CountingAllocator;
const message = capnp.message;
const reflection = capnp.reflection;

fn now(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

fn report(init: std.process.Init, name: []const u8, size: usize, iterations: usize, elapsed: u64, cpu_elapsed: u64, counter: *Counter, checksum: u64) !void {
    var buffer: [2048]u8 = undefined;
    var writer = std.Io.File.stdout().writerStreaming(init.io, &buffer);
    const n: f64 = @floatFromInt(iterations);
    try writer.interface.print("{{\"case\":\"{s}\",\"size\":{d},\"iterations\":{d},\"elapsed_ns\":{d},\"ns_per_op\":{d:.3},\"cpu_ns_per_op\":{d:.3},\"alloc_calls_per_op\":{d:.3},\"allocated_bytes_per_op\":{d:.3},\"checksum\":{d}}}\n", .{
        name,                                             size,                                                 iterations, elapsed, @as(f64, @floatFromInt(elapsed)) / n, @as(f64, @floatFromInt(cpu_elapsed)) / n,
        @as(f64, @floatFromInt(counter.alloc_calls)) / n, @as(f64, @floatFromInt(counter.allocated_bytes)) / n, checksum,
    });
    try writer.interface.flush();
}

fn smallRequest(allocator: std.mem.Allocator, registry: reflection.Registry) ![]const u8 {
    // A single self-contained Payload node from the pinned compiler fixture.
    // Removing its lexical file parent makes it a complete minimal descriptor.
    const source = try (try generated.helpers.Payload.capnpSchema.resolve(registry)).raw();
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try builder.allocateStruct(0, 1);
    const nodes = try root.writeStructList(0, 1, source.data_size, source.pointer_count);
    const dest = try nodes.get(0);
    for (source.getDataSection(), 0..) |value, index| try dest.writeU8Strict(index, value);
    for (0..source.pointer_count) |index| try message.cloneAnyPointer(try source.readAnyPointer(index), try dest.getAnyPointer(index));
    dest.writeU64(16, 0); // schema.Node.scopeId
    return builder.toBytes();
}

fn registryLoad(init: std.process.Init, name: []const u8, request: []const u8, iterations: usize) !void {
    // One untimed warm-up ensures loaded code/pages do not dominate the sample.
    const warm = try reflection.Registry.init(init.gpa, request);
    warm.deinit();
    var counter = Counter.init(init.gpa);
    var checksum: u64 = 0;
    const start = now(init.io);
    const cpu_start = std.Io.Clock.cpu_process.now(init.io).nanoseconds;
    for (0..iterations) |_| {
        const registry = try reflection.Registry.init(counter.allocator(), request);
        checksum +%= registry.nodes().len;
        registry.deinit();
    }
    const elapsed = now(init.io) - start;
    const cpu_elapsed: u64 = @intCast(std.Io.Clock.cpu_process.now(init.io).nanoseconds - cpu_start);
    try report(init, name, request.len, iterations, elapsed, cpu_elapsed, &counter, checksum);
}

fn consumer(init: std.process.Init, registry: reflection.Registry, count: u32) !void {
    const Generated = generated.scalars.Evolution;
    const schema = try (try Generated.capnpSchema.resolve(registry)).asStruct();
    var source = message.MessageBuilder.init(init.gpa);
    defer source.deinit();
    var root = try Generated.Builder.init(&source);
    var entries = try root.initRecords(count);
    for (0..count) |index| {
        var entry = try entries.get(@intCast(index));
        try entry.setValue(index + 1);
        try entry.setLabel("benchmark café");
    }
    const bytes = try source.toBytes();
    defer init.gpa.free(bytes);
    var read_counter = Counter.init(init.gpa);
    var decoded = try message.Message.init(read_counter.allocator(), bytes, .{});
    defer decoded.deinit();
    const typed_root = try Generated.Reader.init(&decoded);
    const typed_entries = try typed_root.getRecords();
    const dynamic_root = try reflection.DynamicStruct.Reader.init(schema, &decoded);
    const dynamic_entries = (try dynamic_root.get("records")).list;
    const reads: usize = if (count < 100) 100000 else 40;
    const copies: usize = if (count < 100) 1000 else 30;
    inline for (.{ false, true }) |dynamic| {
        // The Message allocator points at this stable counter. Reset only the
        // totals after setup; these field reads do not materialize defaults.
        read_counter = Counter.init(init.gpa);
        var checksum: u64 = 0;
        const start = now(init.io);
        const cpu_start = std.Io.Clock.cpu_process.now(init.io).nanoseconds;
        for (0..reads) |_| {
            for (0..count) |index| {
                if (dynamic) {
                    const entry = (try dynamic_entries.get(@intCast(index))).@"struct";
                    checksum +%= (try entry.get("value")).uint64 + (try entry.get("label")).text.len;
                } else {
                    const entry = try typed_entries.get(@intCast(index));
                    checksum +%= try entry.getValue();
                    checksum +%= (try entry.getLabel()).len;
                }
            }
        }
        std.mem.doNotOptimizeAway(checksum);
        const elapsed = now(init.io) - start;
        const cpu_elapsed: u64 = @intCast(std.Io.Clock.cpu_process.now(init.io).nanoseconds - cpu_start);
        try report(init, if (dynamic) "dynamic-read" else "generated-read", count, reads * count, elapsed, cpu_elapsed, &read_counter, checksum);
    }
    inline for (.{ false, true }) |dynamic| {
        var counter = Counter.init(init.gpa);
        var checksum: u64 = 0;
        const start = now(init.io);
        const cpu_start = std.Io.Clock.cpu_process.now(init.io).nanoseconds;
        for (0..copies) |_| {
            var destination = message.MessageBuilder.init(counter.allocator());
            defer destination.deinit();
            if (dynamic) {
                const dest = try reflection.DynamicStruct.Builder.init(schema, &destination);
                try dest.set("records", .{ .list = dynamic_entries });
            } else {
                var dest = try Generated.Builder.init(&destination);
                try dest.setRecords(typed_entries);
            }
            for (destination.segments.items) |segment| checksum +%= segment.items.len;
        }
        const elapsed = now(init.io) - start;
        const cpu_elapsed: u64 = @intCast(std.Io.Clock.cpu_process.now(init.io).nanoseconds - cpu_start);
        try report(init, if (dynamic) "dynamic-copy" else "generated-copy", count, copies, elapsed, cpu_elapsed, &counter, checksum);
    }
}

pub fn main(init: std.process.Init) !void {
    const request = @embedFile("request.bin");
    const registry = try reflection.Registry.init(init.gpa, request);
    defer registry.deinit();
    const small = try smallRequest(init.gpa, registry);
    defer init.gpa.free(small);
    try registryLoad(init, "registry-small", small, 500);
    try registryLoad(init, "registry-corpus", request, 100);
    try consumer(init, registry, 4);
    try consumer(init, registry, 4096);
}
