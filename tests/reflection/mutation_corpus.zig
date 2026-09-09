//! A deterministic operation corpus, emitted by both generated and dynamic
//! builders and independently replayed with C++ DynamicStruct/DynamicList.
const std = @import("std");
const capnp = @import("capnpc-zig");
const generated = @import("generated");
const output = @import("output.zig");
const message = capnp.message;
const reflection = capnp.reflection;
const helpers = capnp.generated_helpers;
const labels = [_][]const u8{ "alpha", "café", "crab 🦀", "omega" };
const old_labels = [_][]const u8{ "zero", "one", "two" };
const unknown_labels = [_][]const u8{ "unknown zero", "unknown one", "unknown two" };

fn emit(init: std.process.Init, name: []const u8, builder: *message.MessageBuilder) !void {
    const bytes = try builder.toBytes();
    defer init.gpa.free(bytes);
    try output.write(init, name, bytes);
}

fn evolution(init: std.process.Init, registry: reflection.Registry, typed: bool, seed: u32, encoding: u32) !void {
    const Generated = generated.scalars.Evolution;
    const schema = try (try Generated.capnpSchema.resolve(registry)).asStruct();
    var arena = message.MessageBuilder.init(init.gpa);
    defer arena.deinit();
    var root = try Generated.Builder.init(&arena);
    const dynamic = reflection.DynamicStruct.Builder{ .schema = schema, .builder = root._builder };
    const data_words: u16 = if (seed % 2 == 0) 1 else 3;
    const pointer_words: u16 = if (seed % 2 == 0) 3 else 1;
    const landing = if (encoding == 0) 0 else try arena.createSegment();
    const content = if (encoding < 2) landing else try arena.createSegment();
    const physical = try root._builder.writeStructListInSegments(0, 3, data_words, pointer_words, landing, content);
    for (0..3) |i| {
        const element = try physical.get(@intCast(i));
        element.writeU64(0, 100 + @as(u64, seed) * 10 + i);
        if (data_words > 1) {
            element.writeU64(8, 200 + i);
            element.writeU64(16, 0x9000 + i);
        }
        try element.writeText(0, old_labels[i]);
        if (pointer_words > 1) {
            try element.writeText(1, "prior note");
            try element.writeText(2, unknown_labels[i]);
        }
    }
    const prefix = try std.fmt.allocPrint(init.gpa, "mutation-evolution-{s}-{d}-{d}", .{ if (typed) "generated" else "dynamic", seed, encoding });
    defer init.gpa.free(prefix);
    const input = try std.fmt.allocPrint(init.gpa, "{s}-input.bin", .{prefix});
    defer init.gpa.free(input);
    try emit(init, input, &arena);
    const index = seed % 3;
    var storage = helpers.ReaderStorage.init(init.gpa);
    defer storage.deinit();
    if (typed) {
        try root.setMarker("mutation corpus");
        var list = try root.getRecords();
        var entry = try list.get(index);
        try entry.setExtra(777 + seed);
        try entry.setLabel(labels[seed]);
        try entry.setNote("mutation note");
        try root.setSingle(try entry.asReader(&storage));
        // A copy retains the old extra even after its source is edited.
        try entry.setExtra(888 + seed);
        try root.setRecords(try (try root.asReader(&storage)).getRecords());
    } else {
        try dynamic.set("marker", .{ .text = "mutation corpus" });
        const list = try dynamic.getList("records");
        const entry = try list.getStruct(index);
        try entry.set("extra", .{ .uint64 = 777 + seed });
        try entry.set("label", .{ .text = labels[seed] });
        try entry.set("note", .{ .text = "mutation note" });
        try dynamic.set("single", .{ .@"struct" = try entry.asReader(&storage) });
        try entry.set("extra", .{ .uint64 = 888 + seed });
        try dynamic.set("records", try (try dynamic.asReader(&storage)).get("records"));
    }
    // Both reader APIs check the written logical values, before the C++ oracle.
    const decoded = try dynamic.asReader(&storage);
    const list = (try decoded.get("records")).list;
    const edited = (try list.get(index)).@"struct";
    try std.testing.expectEqual(@as(u64, 888 + seed), (try edited.get("extra")).uint64);
    try std.testing.expectEqualStrings(labels[seed], (try edited.get("label")).text);
    try std.testing.expectEqual(@as(u64, 777 + seed), (try (try decoded.get("single")).@"struct".get("extra")).uint64);
    const typed_reader = try root.asReader(&storage);
    try std.testing.expectEqual(@as(u64, 888 + seed), try (try (try typed_reader.getRecords()).get(index)).getExtra());
    try std.testing.expectEqual(@as(u64, 777 + seed), try (try typed_reader.getSingle()).getExtra());
    const result = try std.fmt.allocPrint(init.gpa, "{s}.bin", .{prefix});
    defer init.gpa.free(result);
    try emit(init, result, &arena);
    const trace_name = try std.fmt.allocPrint(init.gpa, "{s}.tsv", .{prefix});
    defer init.gpa.free(trace_name);
    const trace = try std.fmt.allocPrint(init.gpa, "seed\t{d}\nencoding\t{d}\nset-marker\tmutation corpus\nreopen-record\t{d}\nset-extra\t{d}\nset-label\t{s}\nset-note\tmutation note\ncopy-to-single\t{d}\nset-extra\t{d}\nself-copy-records\nexpect-single-extra\t{d}\nexpect-record-extra\t{d}\nexpect-unknown-data\t{d}\nexpect-unknown-pointer\t{s}\n", .{ seed, encoding, index, 777 + seed, labels[seed], index, 888 + seed, 777 + seed, 888 + seed, if (data_words > 2) 0x9000 + index else @as(u32, 0), if (pointer_words > 2) unknown_labels[index] else "" });
    defer init.gpa.free(trace);
    try output.write(init, trace_name, trace);
}

fn values(init: std.process.Init, registry: reflection.Registry, typed: bool, seed: u32) !void {
    const Generated = generated.values.Values;
    const schema = try (try Generated.capnpSchema.resolve(registry)).asStruct();
    var arena = message.MessageBuilder.init(init.gpa);
    defer arena.deinit();
    var root = try Generated.Builder.init(&arena);
    const dynamic = reflection.DynamicStruct.Builder{ .schema = schema, .builder = root._builder };
    const prefix = try std.fmt.allocPrint(init.gpa, "mutation-values-{s}-{d}", .{ if (typed) "generated" else "dynamic", seed });
    defer init.gpa.free(prefix);
    const input = try std.fmt.allocPrint(init.gpa, "{s}-input.bin", .{prefix});
    defer init.gpa.free(input);
    try emit(init, input, &arena);
    var storage = helpers.ReaderStorage.init(init.gpa);
    defer storage.deinit();
    const payload = [_]u8{ @intCast(seed), 0x80, 0xff };
    if (typed) {
        try std.testing.expect(!root.hasRecord());
        try root.setHigh(5 + seed);
        var record = try root.getRecord();
        try std.testing.expectEqualStrings("constant é", try record.getLabel());
        try record.setLabel(labels[seed]);
        try root.setRecord(try record.asReader(&storage));
        if (seed % 2 == 0) try root.clearRecord();
        var numbers = try root.getNumbers();
        try numbers.set(1, 11 + seed);
        try root.setNumbers(try (try root.asReader(&storage)).getNumbers());
        var selected = root.initSelected();
        try selected.setName(labels[seed]);
        try selected.setPayload(&payload);
        if (seed % 3 == 0) try root.clearSelected();
        if (seed % 3 == 1) try root.clearNone();
        var details = root.getDetails();
        try details.setEnabled(false);
        try root.clearDetails();
        if (seed == 2) try root.clearHigh();
        try root.setTagged(labels[seed]);
        try root.clearTagged();
    } else {
        try std.testing.expect(!(try dynamic.has("record")));
        try dynamic.set("high", .{ .uint64 = 5 + seed });
        const record = try dynamic.getStruct("record");
        try std.testing.expectEqualStrings("constant é", (try (try record.asReader(&storage)).get("label")).text);
        try record.set("label", .{ .text = labels[seed] });
        try dynamic.set("record", .{ .@"struct" = try record.asReader(&storage) });
        if (seed % 2 == 0) try dynamic.clear("record");
        const numbers = try dynamic.getList("numbers");
        try numbers.set(1, .{ .uint64 = 11 + seed });
        try dynamic.set("numbers", try (try dynamic.asReader(&storage)).get("numbers"));
        const selected = try dynamic.initGroup("selected");
        try selected.set("name", .{ .text = labels[seed] });
        try selected.set("payload", .{ .data = &payload });
        if (seed % 3 == 0) try dynamic.clear("selected");
        if (seed % 3 == 1) try dynamic.clear("none");
        const details = try dynamic.getStruct("details");
        try details.set("enabled", .{ .bool = false });
        try dynamic.clear("details");
        if (seed == 2) try dynamic.clear("high");
        try dynamic.set("tagged", .{ .text = labels[seed] });
        try dynamic.clear("tagged");
    }
    const expected_high = if (seed == 2) std.math.maxInt(u64) else @as(u64, 5 + seed);
    try std.testing.expectEqual(expected_high, (try dynamic.getScalar("high")).uint64);
    try std.testing.expectEqual(seed % 2 != 0, try dynamic.has("record"));
    const typed_reader = try root.asReader(&storage);
    try std.testing.expectEqual(expected_high, try typed_reader.getHigh());
    try std.testing.expectEqual(@as(u64, 11 + seed), try (try typed_reader.getNumbers()).get(1));
    const result = try std.fmt.allocPrint(init.gpa, "{s}.bin", .{prefix});
    defer init.gpa.free(result);
    try emit(init, result, &arena);
    const trace_name = try std.fmt.allocPrint(init.gpa, "{s}.tsv", .{prefix});
    defer init.gpa.free(trace_name);
    const trace = try std.fmt.allocPrint(init.gpa, "seed\t{d}\nmaterialize-record-default\tconstant é\nset-record-label\t{s}\nself-copy-record\nclear-record\t{}\nmutate-and-self-copy-numbers\t{d}\nselect-group\t{s}\nclear-selected\t{}\nselect-none\t{}\nmutate-and-clear-details\nmutate-and-clear-tagged\nexpect-high\t{d}\nexpect-record-present\t{}\nexpect-union\t{s}\n", .{ seed, labels[seed], seed % 2 == 0, 11 + seed, labels[seed], seed % 3 == 0, seed % 3 == 1, expected_high, seed % 2 != 0, if (seed % 3 == 1) "none" else "selected" });
    defer init.gpa.free(trace);
    try output.write(init, trace_name, trace);
}

pub fn run(init: std.process.Init, registry: reflection.Registry) !void {
    for ([_]bool{ false, true }) |typed| {
        for (0..4) |seed| {
            try values(init, registry, typed, @intCast(seed));
            for (0..3) |encoding| try evolution(init, registry, typed, @intCast(seed), @intCast(encoding));
        }
    }
    try output.write(init, "mutation-corpus.tsv", "corpus\tcapnp-zig-mutation-v1\nseeds\t0,1,2,3\nprofiles\tgenerated,dynamic\nencodings\tnear,single-far,double-far\nevolution-cases\t24\nvalue-cases\t8\n");
}
