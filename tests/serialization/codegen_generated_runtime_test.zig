const std = @import("std");
const capnpc = @import("capnpc-zig");
const request_reader = capnpc.request;
const capnp_cli = @import("support/capnp_cli.zig");

fn writeFile(dir: std.Io.Dir, name: []const u8, data: []const u8) !void {
    const io = std.testing.io;
    var file = try dir.createFile(io, name, .{});
    defer file.close(io);
    try file.writeStreamingAll(io, data);
}

fn runGeneratedHarness(
    allocator: std.mem.Allocator,
    schema_path: []const u8,
    harness_source: []const u8,
) !void {
    return runGeneratedHarnessProfile(allocator, schema_path, harness_source, .full);
}

fn runGeneratedHarnessProfile(
    allocator: std.mem.Allocator,
    schema_path: []const u8,
    harness_source: []const u8,
    profile: capnpc.codegen.Generator.ApiProfile,
) !void {
    const io = std.testing.io;

    const capnp_argv = &[_][]const u8{
        "compile",
        "-o-",
        schema_path,
    };
    const capnp_result = try capnp_cli.run(allocator, io, capnp_argv, .{});
    defer allocator.free(capnp_result.stdout);
    defer allocator.free(capnp_result.stderr);
    try std.testing.expect(capnp_result.term == .exited and capnp_result.term.exited == 0);

    const request = try request_reader.parseCodeGeneratorRequest(allocator, capnp_result.stdout);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);
    if (request.requested_files.len == 0) return error.InvalidCodeGeneratorRequest;

    var generator = try capnpc.codegen.Generator.init(allocator, request.nodes);
    defer generator.deinit();
    generator.setApiProfile(profile);
    const generated = try generator.generateFile(request.requested_files[0]);
    defer allocator.free(generated);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeFile(tmp.dir, "generated.zig", generated);
    try writeFile(tmp.dir, "harness.zig", harness_source);

    const harness_path = try tmp.dir.realPathFileAlloc(io, "harness.zig", allocator);
    defer allocator.free(harness_path);

    const lib_path = try std.Io.Dir.cwd().realPathFileAlloc(io, "src/lib.zig", allocator);
    defer allocator.free(lib_path);

    const lib_arg = try std.fmt.allocPrint(allocator, "-Mcapnpc-zig={s}", .{lib_path});
    defer allocator.free(lib_arg);
    const root_arg = try std.fmt.allocPrint(allocator, "-Mroot={s}", .{harness_path});
    defer allocator.free(root_arg);

    var zig_argv = std.ArrayList([]const u8).empty;
    defer zig_argv.deinit(allocator);
    try zig_argv.append(allocator, "zig");
    try zig_argv.append(allocator, "test");
    // root module depends on capnpc-zig
    try zig_argv.append(allocator, "--dep");
    try zig_argv.append(allocator, "capnpc-zig");
    try zig_argv.append(allocator, root_arg);
    // capnpc-zig depends on itself: generated interface code imports the RPC
    // runtime via `@import("capnpc-zig")`, which the library re-exports through
    // its own module name. Without this self-dependency, interface schemas fail
    // to compile with "no module named 'capnpc-zig'".
    try zig_argv.append(allocator, "--dep");
    try zig_argv.append(allocator, "capnpc-zig");
    try zig_argv.append(allocator, lib_arg);

    const zig_result = std.process.run(allocator, io, .{
        .argv = zig_argv.items,
    }) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer allocator.free(zig_result.stdout);
    defer allocator.free(zig_result.stderr);

    if (!(zig_result.term == .exited and zig_result.term.exited == 0)) {
        std.debug.print("zig test stdout:\n{s}\n", .{zig_result.stdout});
        std.debug.print("zig test stderr:\n{s}\n", .{zig_result.stderr});
        return error.GeneratedRuntimeCompileFailed;
    }
}

test "Codegen generated enum list compiles and runs" {
    const allocator = std.testing.allocator;

    try runGeneratedHarness(allocator, "tests/test_schemas/enum_list_runtime.capnp",
        \\const std = @import("std");
        \\const capnpc = @import("capnpc-zig");
        \\const message = capnpc.message;
        \\const generated = @import("generated.zig");
        \\
        \\test "generated enum list runtime" {
        \\    var builder = message.MessageBuilder.init(std.testing.allocator);
        \\    defer builder.deinit();
        \\
        \\    var root = try generated.EnumListDemo.Builder.init(&builder);
        \\    var colors = try root.initColors(3);
        \\    try colors.set(0, .Red);
        \\    try colors.set(1, .Green);
        \\    try colors.set(2, .Blue);
        \\
        \\    const bytes = try builder.toBytes();
        \\    defer std.testing.allocator.free(bytes);
        \\
        \\    var msg = try message.Message.init(std.testing.allocator, bytes, .{});
        \\    defer msg.deinit();
        \\
        \\    const reader = try generated.EnumListDemo.Reader.init(&msg);
        \\    const out = try reader.getColors();
        \\    try std.testing.expectEqual(@as(u32, 3), out.len());
        \\    try std.testing.expectEqual(generated.Color.Red, try out.get(0));
        \\    try std.testing.expectEqual(generated.Color.Green, try out.get(1));
        \\    try std.testing.expectEqual(generated.Color.Blue, try out.get(2));
        \\}
        \\
    );
}

test "Codegen generated default setters preserve logical values" {
    const allocator = std.testing.allocator;

    try runGeneratedHarness(allocator, "tests/test_schemas/default_setter_runtime.capnp",
        \\const std = @import("std");
        \\const capnpc = @import("capnpc-zig");
        \\const message = capnpc.message;
        \\const generated = @import("generated.zig");
        \\
        \\test "generated defaults runtime" {
        \\    {
        \\        var builder = message.MessageBuilder.init(std.testing.allocator);
        \\        defer builder.deinit();
        \\
        \\        var root = try generated.DefaultSetterDemo.Builder.init(&builder);
        \\        try root.setFlag(true);
        \\        try root.setCount(0x1234);
        \\        try root.setDelta(-17);
        \\        try root.setRatio(@as(f32, 1.25));
        \\        try root.setScale(@as(f64, -2.5));
        \\        try root.setColor(.Green);
        \\
        \\        const bytes = try builder.toBytes();
        \\        defer std.testing.allocator.free(bytes);
        \\
        \\        var msg = try message.Message.init(std.testing.allocator, bytes, .{});
        \\        defer msg.deinit();
        \\
        \\        const raw_root = try msg.getRootStruct();
        \\        const raw_data = raw_root.getDataSection();
        \\        for (raw_data) |byte| {
        \\            try std.testing.expectEqual(@as(u8, 0), byte);
        \\        }
        \\
        \\        const reader = try generated.DefaultSetterDemo.Reader.init(&msg);
        \\        try std.testing.expectEqual(true, try reader.getFlag());
        \\        try std.testing.expectEqual(@as(u16, 0x1234), try reader.getCount());
        \\        try std.testing.expectEqual(@as(i32, -17), try reader.getDelta());
        \\        try std.testing.expectEqual(@as(f32, 1.25), try reader.getRatio());
        \\        try std.testing.expectEqual(@as(f64, -2.5), try reader.getScale());
        \\        try std.testing.expectEqual(generated.Color.Green, try reader.getColor());
        \\    }
        \\
        \\    {
        \\        var builder = message.MessageBuilder.init(std.testing.allocator);
        \\        defer builder.deinit();
        \\
        \\        var root = try generated.DefaultSetterDemo.Builder.init(&builder);
        \\        try root.setFlag(false);
        \\        try root.setCount(0x5555);
        \\        try root.setDelta(42);
        \\        try root.setRatio(@as(f32, -3.5));
        \\        try root.setScale(@as(f64, 7.75));
        \\        try root.setColor(.Blue);
        \\
        \\        const bytes = try builder.toBytes();
        \\        defer std.testing.allocator.free(bytes);
        \\
        \\        var msg = try message.Message.init(std.testing.allocator, bytes, .{});
        \\        defer msg.deinit();
        \\
        \\        const raw_root = try msg.getRootStruct();
        \\        const raw_data = raw_root.getDataSection();
        \\        var any_non_zero = false;
        \\        for (raw_data) |byte| {
        \\            if (byte != 0) {
        \\                any_non_zero = true;
        \\                break;
        \\            }
        \\        }
        \\        try std.testing.expect(any_non_zero);
        \\
        \\        const reader = try generated.DefaultSetterDemo.Reader.init(&msg);
        \\        try std.testing.expectEqual(false, try reader.getFlag());
        \\        try std.testing.expectEqual(@as(u16, 0x5555), try reader.getCount());
        \\        try std.testing.expectEqual(@as(i32, 42), try reader.getDelta());
        \\        try std.testing.expectEqual(@as(f32, -3.5), try reader.getRatio());
        \\        try std.testing.expectEqual(@as(f64, 7.75), try reader.getScale());
        \\        try std.testing.expectEqual(generated.Color.Blue, try reader.getColor());
        \\    }
        \\}
        \\
    );
}

test "Codegen generated list wrappers compile and run" {
    const allocator = std.testing.allocator;

    try runGeneratedHarness(allocator, "tests/test_schemas/list_wrappers_runtime.capnp",
        \\const std = @import("std");
        \\const capnpc = @import("capnpc-zig");
        \\const message = capnpc.message;
        \\const generated = @import("generated.zig");
        \\
        \\test "generated list wrappers runtime" {
        \\    var builder = message.MessageBuilder.init(std.testing.allocator);
        \\    defer builder.deinit();
        \\
        \\    var root = try generated.ListWrapperDemo.Builder.init(&builder);
        \\    var children = try root.initChildren(2);
        \\    var c0 = try children.get(0);
        \\    var c1 = try children.get(1);
        \\    try c0.setValue(11);
        \\    try c1.setValue(22);
        \\
        \\    var data_items = try root.initDataItems(2);
        \\    try data_items.set(0, "alpha");
        \\    try data_items.set(1, "beta");
        \\
        \\    var services = try root.initServices(2);
        \\    try services.set(0, .{ .id = 7 });
        \\    try services.set(1, .{ .id = 9 });
        \\
        \\    const bytes = try builder.toBytes();
        \\    defer std.testing.allocator.free(bytes);
        \\
        \\    var msg = try message.Message.initUnvalidated(std.testing.allocator, bytes);
        \\    defer msg.deinit();
        \\
        \\    const reader = try generated.ListWrapperDemo.Reader.init(&msg);
        \\    const out_children = try reader.getChildren();
        \\    try std.testing.expectEqual(@as(u32, 2), out_children.len());
        \\    const out_c0 = try out_children.get(0);
        \\    const out_c1 = try out_children.get(1);
        \\    try std.testing.expectEqual(@as(u16, 11), try out_c0.getValue());
        \\    try std.testing.expectEqual(@as(u16, 22), try out_c1.getValue());
        \\
        \\    const out_data = try reader.getDataItems();
        \\    try std.testing.expectEqual(@as(u32, 2), out_data.len());
        \\    try std.testing.expectEqualStrings("alpha", try out_data.get(0));
        \\    try std.testing.expectEqualStrings("beta", try out_data.get(1));
        \\
        \\    const out_services = try reader.getServices();
        \\    try std.testing.expectEqual(@as(u32, 2), out_services.len());
        \\    try std.testing.expectEqual(@as(u32, 7), (try out_services.get(0)).id);
        \\    try std.testing.expectEqual(@as(u32, 9), (try out_services.get(1)).id);
        \\}
        \\
    );
}

test "Codegen generated recursive nested-list views compile and run" {
    const allocator = std.testing.allocator;

    try runGeneratedHarness(allocator, "tests/test_schemas/nested_lists_runtime.capnp",
        \\const std = @import("std");
        \\const capnpc = @import("capnpc-zig");
        \\const message = capnpc.message;
        \\const generated = @import("generated.zig");
        \\
        \\test "generated recursive nested-list views" {
        \\    var builder = message.MessageBuilder.init(std.testing.allocator);
        \\    defer builder.deinit();
        \\
        \\    var root = try generated.NestedListDemo.Builder.init(&builder);
        \\    var numbers = try root.nestedLists().initNumbersInSegment(3, 1);
        \\    var first = try numbers.initInSegment(0, 3, 2);
        \\    try first.set(0, 11);
        \\    try first.set(1, 22);
        \\    try first.set(2, 33);
        \\    try numbers.setNull(1);
        \\    _ = try numbers.init(2, 0);
        \\
        \\    var deep = try root.nestedLists().initDeepText(1);
        \\    var middle = try deep.init(0, 1);
        \\    var texts = try middle.init(0, 2);
        \\    try texts.set(0, "alpha");
        \\    try texts.set(1, "beta");
        \\
        \\    var records = try root.nestedLists().initRecords(1);
        \\    var record_row = try records.init(0, 2);
        \\    var child0 = try record_row.get(0);
        \\    var child1 = try record_row.get(1);
        \\    try child0.setValue(7);
        \\    try child1.setValue(9);
        \\
        \\    var data_rows = try root.nestedLists().initDataRows(1);
        \\    var data_row = try data_rows.init(0, 2);
        \\    try data_row.set(0, "left");
        \\    try data_row.set(1, "right");
        \\
        \\    var enum_rows = try root.nestedLists().initEnumRows(1);
        \\    var enum_row = try enum_rows.init(0, 2);
        \\    try enum_row.set(0, .Red);
        \\    try enum_row.setOrdinal(1, 77);
        \\
        \\    var services = try root.nestedLists().initServiceRows(1);
        \\    var service_row = try services.init(0, 1);
        \\    try service_row.set(0, .{ .id = 41 });
        \\
        \\    var void_rows = try root.nestedLists().initVoidRows(1);
        \\    const void_row = try void_rows.init(0, 4);
        \\    try std.testing.expectEqual(@as(u32, 4), void_row.len());
        \\
        \\    var grouped = root.getGrouped();
        \\    var bool_rows = try grouped.nestedLists().initGroupRows(1);
        \\    var bool_row = try bool_rows.init(0, 2);
        \\    try bool_row.set(0, true);
        \\    try bool_row.set(1, false);
        \\
        \\    var choice_rows = try root.nestedLists().initChoiceRows(1);
        \\    var choice_row = try choice_rows.init(0, 1);
        \\    try choice_row.set(0, 1234);
        \\
        \\    var i8_row = try (try root.nestedLists().initI8Rows(1)).init(0, 1);
        \\    try i8_row.set(0, -8);
        \\    var u8_row = try (try root.nestedLists().initU8Rows(1)).init(0, 1);
        \\    try u8_row.set(0, 8);
        \\    var i16_row = try (try root.nestedLists().initI16Rows(1)).init(0, 1);
        \\    try i16_row.set(0, -16);
        \\    var i32_row = try (try root.nestedLists().initI32Rows(1)).init(0, 1);
        \\    try i32_row.set(0, -32);
        \\    var i64_row = try (try root.nestedLists().initI64Rows(1)).init(0, 1);
        \\    try i64_row.set(0, -64);
        \\    var u64_row = try (try root.nestedLists().initU64Rows(1)).init(0, 1);
        \\    try u64_row.set(0, 64);
        \\    var f32_row = try (try root.nestedLists().initF32Rows(1)).init(0, 1);
        \\    try f32_row.set(0, 3.25);
        \\    var f64_row = try (try root.nestedLists().initF64Rows(1)).init(0, 1);
        \\    try f64_row.set(0, 6.5);
        \\
        \\    const raw_numbers: message.PointerListBuilder = try root.initNumbers(0);
        \\    _ = raw_numbers;
        \\    numbers = try root.nestedLists().initNumbersInSegment(3, 1);
        \\    first = try numbers.initInSegment(0, 3, 2);
        \\    try first.set(0, 11);
        \\    try first.set(1, 22);
        \\    try first.set(2, 33);
        \\    try numbers.setNull(1);
        \\    _ = try numbers.init(2, 0);
        \\
        \\    const bytes = try builder.toBytes();
        \\    defer std.testing.allocator.free(bytes);
        \\    var msg = try message.Message.initUnvalidated(std.testing.allocator, bytes);
        \\    defer msg.deinit();
        \\
        \\    const reader = try generated.NestedListDemo.Reader.init(&msg);
        \\    const raw_reader: message.PointerListReader = try reader.getNumbers();
        \\    _ = raw_reader;
        \\    const read_numbers = try reader.nestedLists().getNumbers();
        \\    try std.testing.expectEqual(@as(u32, 3), read_numbers.len());
        \\    try std.testing.expect(!try read_numbers.isNull(0));
        \\    try std.testing.expect(try read_numbers.isNull(1));
        \\    try std.testing.expect(!try read_numbers.isNull(2));
        \\    const read_first = try read_numbers.get(0);
        \\    try std.testing.expectEqual(@as(u16, 22), try read_first.get(1));
        \\    try std.testing.expectEqual(@as(u32, 0), (try read_numbers.get(1)).len());
        \\    try std.testing.expectEqual(@as(u32, 0), (try read_numbers.get(2)).len());
        \\
        \\    const read_deep = try reader.nestedLists().getDeepText();
        \\    const read_middle = try read_deep.get(0);
        \\    const read_texts = try read_middle.get(0);
        \\    try std.testing.expectEqualStrings("beta", try read_texts.get(1));
        \\
        \\    const read_records = try reader.nestedLists().getRecords();
        \\    const read_record_row = try read_records.get(0);
        \\    try std.testing.expectEqual(@as(u16, 9), try (try read_record_row.get(1)).getValue());
        \\
        \\    const read_data = try (try reader.nestedLists().getDataRows()).get(0);
        \\    try std.testing.expectEqualStrings("right", try read_data.get(1));
        \\
        \\    const read_enum = try (try reader.nestedLists().getEnumRows()).get(0);
        \\    try std.testing.expectEqual(generated.Shade.Red, try read_enum.get(0));
        \\    try std.testing.expectEqual(@as(u16, 77), try read_enum.getOrdinal(1));
        \\    try std.testing.expectError(error.InvalidEnumValue, read_enum.get(1));
        \\
        \\    const read_services = try (try reader.nestedLists().getServiceRows()).get(0);
        \\    try std.testing.expectEqual(@as(u32, 41), (try read_services.get(0)).id);
        \\    try std.testing.expectEqual(@as(u32, 4), (try (try reader.nestedLists().getVoidRows()).get(0)).len());
        \\    const read_bool_rows = try reader.getGrouped().nestedLists().getGroupRows();
        \\    const read_bool_row = try read_bool_rows.get(0);
        \\    try std.testing.expect(try read_bool_row.get(0));
        \\    try std.testing.expect(!try read_bool_row.get(1));
        \\    try std.testing.expectEqual(@as(u32, 1234), try (try (try reader.nestedLists().getChoiceRows()).get(0)).get(0));
        \\    const defaults = try reader.nestedLists().getDefaultRows();
        \\    try std.testing.expectEqual(@as(u32, 2), defaults.len());
        \\    try std.testing.expectEqual(@as(u16, 5), try (try defaults.get(0)).get(1));
        \\    try std.testing.expectEqual(@as(u32, 0), (try defaults.get(1)).len());
        \\    try std.testing.expectEqual(@as(i8, -8), try (try (try reader.nestedLists().getI8Rows()).get(0)).get(0));
        \\    try std.testing.expectEqual(@as(u8, 8), try (try (try reader.nestedLists().getU8Rows()).get(0)).get(0));
        \\    try std.testing.expectEqual(@as(i16, -16), try (try (try reader.nestedLists().getI16Rows()).get(0)).get(0));
        \\    try std.testing.expectEqual(@as(i32, -32), try (try (try reader.nestedLists().getI32Rows()).get(0)).get(0));
        \\    try std.testing.expectEqual(@as(i64, -64), try (try (try reader.nestedLists().getI64Rows()).get(0)).get(0));
        \\    try std.testing.expectEqual(@as(u64, 64), try (try (try reader.nestedLists().getU64Rows()).get(0)).get(0));
        \\    try std.testing.expectEqual(@as(f32, 3.25), try (try (try reader.nestedLists().getF32Rows()).get(0)).get(0));
        \\    try std.testing.expectEqual(@as(f64, 6.5), try (try (try reader.nestedLists().getF64Rows()).get(0)).get(0));
        \\}
        \\
    );
}

test "Codegen generated recursive nested-list views work in compact profile" {
    const allocator = std.testing.allocator;

    try runGeneratedHarnessProfile(allocator, "tests/test_schemas/nested_lists_runtime.capnp",
        \\const std = @import("std");
        \\const capnpc = @import("capnpc-zig");
        \\const message = capnpc.message;
        \\const generated = @import("generated.zig");
        \\
        \\test "compact recursive nested-list views" {
        \\    var builder = message.MessageBuilder.init(std.testing.allocator);
        \\    defer builder.deinit();
        \\    const raw_root = try builder.allocateStruct(1, 9);
        \\    var root = generated.NestedListDemo.Builder.wrap(raw_root);
        \\    var rows = try root.nestedLists().initNumbers(1);
        \\    var row = try rows.init(0, 2);
        \\    try row.set(0, 5);
        \\    try row.set(1, 8);
        \\    const raw_rows: message.PointerListBuilder = try root.initNumbers(1);
        \\    _ = raw_rows;
        \\    rows = try root.nestedLists().initNumbers(1);
        \\    row = try rows.init(0, 1);
        \\    try row.set(0, 13);
        \\
        \\    const bytes = try builder.toBytes();
        \\    defer std.testing.allocator.free(bytes);
        \\    var msg = try message.Message.initUnvalidated(std.testing.allocator, bytes);
        \\    defer msg.deinit();
        \\    const reader = generated.NestedListDemo.Reader.wrap(try msg.getRootStruct());
        \\    const raw_reader: message.PointerListReader = try reader.getNumbers();
        \\    _ = raw_reader;
        \\    const out = try reader.nestedLists().getNumbers();
        \\    try std.testing.expectEqual(@as(u16, 13), try (try out.get(0)).get(0));
        \\    const defaults = try reader.nestedLists().getDefaultRows();
        \\    try std.testing.expectEqual(@as(u16, 4), try (try defaults.get(0)).get(0));
        \\}
        \\
    , .compact);
}

test "Codegen generated complex constants compile and run" {
    const allocator = std.testing.allocator;

    try runGeneratedHarness(allocator, "tests/test_schemas/defaults.capnp",
        \\const std = @import("std");
        \\const capnpc = @import("capnpc-zig");
        \\const generated = @import("generated.zig");
        \\
        \\test "generated constants runtime" {
        \\    try std.testing.expectEqual(@as(u32, 987), generated.magicNumber);
        \\    try std.testing.expectEqualStrings(&[_]u8{ 0x0a, 0x0b, 0x0c }, generated.magicData);
        \\    try std.testing.expectEqual(@as(u16, 2), @intFromEnum(generated.magicColor));
        \\
        \\    const list = try generated.magicList.get();
        \\    try std.testing.expectEqual(@as(u32, 3), list.len());
        \\    try std.testing.expectEqual(@as(u16, 7), try list.get(0));
        \\    try std.testing.expectEqual(@as(u16, 8), try list.get(1));
        \\    try std.testing.expectEqual(@as(u16, 9), try list.get(2));
        \\
        \\    const inner = try generated.magicInner.get();
        \\    try std.testing.expectEqual(@as(u32, 7), try inner.getId());
        \\    try std.testing.expectEqualStrings("const", try inner.getLabel());
        \\}
        \\
    );
}

test "Codegen generated edge schema compiles and runs" {
    const allocator = std.testing.allocator;

    try runGeneratedHarness(allocator, "tests/test_schemas/edge_codegen.capnp",
        \\const std = @import("std");
        \\const capnpc = @import("capnpc-zig");
        \\const message = capnpc.message;
        \\const generated = @import("generated.zig");
        \\
        \\test "generated edge schema runtime" {
        \\    var builder = message.MessageBuilder.init(std.testing.allocator);
        \\    defer builder.deinit();
        \\
        \\    var root = try generated.EdgeHolder.Builder.init(&builder);
        \\    try root.clearService();
        \\    try root.setServiceCapability(.{ .id = 77 });
        \\
        \\    const bytes = try builder.toBytes();
        \\    defer std.testing.allocator.free(bytes);
        \\
        \\    var msg = try message.Message.initUnvalidated(std.testing.allocator, bytes);
        \\    defer msg.deinit();
        \\
        \\    const reader = try generated.EdgeHolder.Reader.init(&msg);
        \\    const cap = try reader.getService();
        \\    try std.testing.expectEqual(@as(u32, 77), cap.id);
        \\
        \\    try std.testing.expectEqualStrings("", generated.emptyText);
        \\    try std.testing.expectEqualStrings(&[_]u8{0x00}, generated.sampleData);
        \\
        \\    const empty = try generated.emptyList.get();
        \\    try std.testing.expectEqual(@as(u32, 0), empty.len());
        \\}
        \\
    );
}

test "Codegen generated schema evolution compiles and runs" {
    const allocator = std.testing.allocator;

    try runGeneratedHarness(allocator, "tests/test_schemas/schema_evolution_runtime.capnp",
        \\const std = @import("std");
        \\const capnpc = @import("capnpc-zig");
        \\const message = capnpc.message;
        \\const generated = @import("generated.zig");
        \\
        \\test "generated schema evolution runtime" {
        \\    {
        \\        var builder = message.MessageBuilder.init(std.testing.allocator);
        \\        defer builder.deinit();
        \\
        \\        var old_root = try generated.OldVersion.Builder.init(&builder);
        \\        try old_root.setId(7);
        \\        try old_root.setLabel("legacy");
        \\        var old_profile = try old_root.initProfile();
        \\        try old_profile.setName("v1-profile");
        \\
        \\        const bytes = try builder.toBytes();
        \\        defer std.testing.allocator.free(bytes);
        \\
        \\        var msg = try message.Message.init(std.testing.allocator, bytes, .{});
        \\        defer msg.deinit();
        \\
        \\        const root_struct = try msg.getRootStruct();
        \\        const as_new = generated.NewVersion.Reader.wrap(root_struct);
        \\        try std.testing.expectEqual(@as(u64, 7), try as_new.getId());
        \\        try std.testing.expectEqualStrings("legacy", try as_new.getLabel());
        \\        const new_profile = try as_new.getProfile();
        \\        try std.testing.expectEqualStrings("v1-profile", try new_profile.getName());
        \\        try std.testing.expectEqual(@as(u32, 42), try as_new.getRevision());
        \\        try std.testing.expectEqualStrings("new-field-default", try as_new.getNote());
        \\        try std.testing.expectEqual(true, try as_new.getEnabled());
        \\
        \\        // BUG 3: pointer fields added in the newer schema were never
        \\        // written by the old message (null pointers). Reading them must
        \\        // return empty defaults, not error.InvalidPointer.
        \\        const extra_profile = try as_new.getExtraProfile();
        \\        try std.testing.expectEqualStrings("", try extra_profile.getName());
        \\        const tags = try as_new.getTags();
        \\        try std.testing.expectEqual(@as(u32, 0), tags.len());
        \\        const numbers = try as_new.getNumbers();
        \\        try std.testing.expectEqual(@as(u32, 0), numbers.len());
        \\        const blob = try as_new.getBlob();
        \\        try std.testing.expectEqual(@as(usize, 0), blob.len);
        \\        const children = try as_new.getChildren();
        \\        try std.testing.expectEqual(@as(u32, 0), children.len());
        \\    }
        \\
        \\    {
        \\        var builder = message.MessageBuilder.init(std.testing.allocator);
        \\        defer builder.deinit();
        \\
        \\        var new_root = try generated.NewVersion.Builder.init(&builder);
        \\        try new_root.setId(9);
        \\        try new_root.setLabel("modern");
        \\        var new_profile = try new_root.initProfile();
        \\        try new_profile.setName("v2-profile");
        \\        try new_root.setRevision(99);
        \\        try new_root.setNote("explicit-note");
        \\        try new_root.setEnabled(false);
        \\
        \\        const bytes = try builder.toBytes();
        \\        defer std.testing.allocator.free(bytes);
        \\
        \\        var msg = try message.Message.init(std.testing.allocator, bytes, .{});
        \\        defer msg.deinit();
        \\
        \\        const root_struct = try msg.getRootStruct();
        \\        const as_old = generated.OldVersion.Reader.wrap(root_struct);
        \\        try std.testing.expectEqual(@as(u64, 9), try as_old.getId());
        \\        try std.testing.expectEqualStrings("modern", try as_old.getLabel());
        \\        const old_profile = try as_old.getProfile();
        \\        try std.testing.expectEqualStrings("v2-profile", try old_profile.getName());
        \\
        \\        const as_new = generated.NewVersion.Reader.wrap(root_struct);
        \\        try std.testing.expectEqual(@as(u32, 99), try as_new.getRevision());
        \\        try std.testing.expectEqualStrings("explicit-note", try as_new.getNote());
        \\        try std.testing.expectEqual(false, try as_new.getEnabled());
        \\    }
        \\}
        \\
    );
}

test "Codegen generated schema manifest compiles and runs" {
    const allocator = std.testing.allocator;

    try runGeneratedHarness(allocator, "tests/test_schemas/list_wrappers_runtime.capnp",
        \\const std = @import("std");
        \\const generated = @import("generated.zig");
        \\
        \\const ManifestSerdeEntry = struct {
        \\    id: u64,
        \\    type_name: []const u8,
        \\    to_json_export: []const u8,
        \\    from_json_export: []const u8,
        \\};
        \\
        \\const Manifest = struct {
        \\    schema: []const u8,
        \\    module: []const u8,
        \\    serde: []const ManifestSerdeEntry,
        \\};
        \\
        \\test "generated schema manifest runtime" {
        \\    const json_a = generated.capnpSchemaManifestJson();
        \\    const json_b = generated.capnpSchemaManifestJson();
        \\    try std.testing.expectEqualStrings(json_a, json_b);
        \\
        \\    var parsed = try std.json.parseFromSlice(Manifest, std.testing.allocator, json_a, .{});
        \\    defer parsed.deinit();
        \\
        \\    try std.testing.expectEqualStrings("tests/test_schemas/list_wrappers_runtime.capnp", parsed.value.schema);
        \\    try std.testing.expectEqualStrings("list_wrappers_runtime", parsed.value.module);
        \\    try std.testing.expect(parsed.value.serde.len > 0);
        \\
        \\    var saw_root = false;
        \\    var saw_child = false;
        \\    for (parsed.value.serde) |entry| {
        \\        if (std.mem.eql(u8, entry.type_name, "ListWrapperDemo")) {
        \\            saw_root = true;
        \\        }
        \\        if (std.mem.eql(u8, entry.type_name, "Child")) {
        \\            saw_child = true;
        \\        }
        \\    }
        \\    try std.testing.expect(saw_root);
        \\    try std.testing.expect(saw_child);
        \\}
        \\
    );
}

// BUG 2: the pipelined client method's return type must be the sibling
// `{Method}Pipeline` type, not `{Method}.Pipeline`. Zig lazily analyzes unused
// decls, so the broken signature only fails when the decl is actually
// referenced. This test force-analyzes `callGetInnerPipelined` so the compiler
// type-checks its return type.
test "Codegen pipelined client method compiles when referenced" {
    const allocator = std.testing.allocator;

    try runGeneratedHarness(allocator, "tests/test_schemas/nested_interfaces.capnp",
        \\const std = @import("std");
        \\const generated = @import("generated.zig");
        \\
        \\test "pipelined client decl type-checks" {
        \\    // Force semantic analysis of the pipelined method and its return
        \\    // type (Outer.GetInnerPipeline). Before the fix this referenced
        \\    // Outer.GetInner.Pipeline, which does not exist.
        \\    _ = &generated.Outer.Client.callGetInnerPipelined;
        \\    _ = &generated.Outer.Client.callGetInnerWithOptions;
        \\    _ = &generated.Outer.Client.callGetInnerPipelinedWithOptions;
        \\    _ = &generated.Outer.Inner.PipelinedClient.callPingWithOptions;
        \\    _ = generated.Outer.GetInnerPipeline;
        \\    const info = @typeInfo(@TypeOf(generated.Outer.Client.callGetInnerPipelined));
        \\    const ret = info.@"fn".return_type.?;
        \\    const payload = @typeInfo(ret).error_union.payload;
        \\    try std.testing.expect(payload == generated.Outer.GetInnerPipeline);
        \\}
        \\
    );
}

test "Codegen generated retained call context survives synchronous Return reentrancy" {
    const allocator = std.testing.allocator;

    try runGeneratedHarness(allocator, "tests/test_schemas/enum_evolution_v1.capnp",
        \\const std = @import("std");
        \\const capnpc = @import("capnpc-zig");
        \\const generated = @import("generated.zig");
        \\const rpc = capnpc.rpc;
        \\const protocol = rpc.wire.protocol;
        \\const Peer = rpc.peer.Peer;
        \\
        \\const CallbackMode = enum { succeed, fail_nonfatal, fail_oom };
        \\
        \\const CallbackState = struct {
        \\    mode: CallbackMode = .succeed,
        \\    calls: usize = 0,
        \\    error_calls: usize = 0,
        \\    last_error: ?anyerror = null,
        \\
        \\    fn onReturn(
        \\        ctx_ptr: *anyopaque,
        \\        peer: *Peer,
        \\        response: generated.Service.Ping.Response,
        \\        caps: *const rpc.caps.table.InboundCapTable,
        \\    ) anyerror!void {
        \\        _ = peer;
        \\        _ = caps;
        \\        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        \\        self.calls += 1;
        \\        switch (response) {
        \\            .exception => |ex| try std.testing.expectEqualStrings("synchronous return", ex.reason),
        \\            else => return error.UnexpectedResponse,
        \\        }
        \\        return switch (self.mode) {
        \\            .succeed => {},
        \\            .fail_nonfatal => error.TestCallbackError,
        \\            .fail_oom => error.OutOfMemory,
        \\        };
        \\    }
        \\
        \\    fn onError(ctx_ptr: ?*anyopaque, peer: *Peer, err: anyerror) void {
        \\        _ = peer;
        \\        const self: *@This() = @ptrCast(@alignCast(ctx_ptr.?));
        \\        self.error_calls += 1;
        \\        self.last_error = err;
        \\    }
        \\};
        \\
        \\const SyncTransport = struct {
        \\    allocator: std.mem.Allocator,
        \\    peer: *Peer = undefined,
        \\    trailing_error: bool = false,
        \\    question_id: ?u32 = null,
        \\    finish_count: usize = 0,
        \\
        \\    fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        \\        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        \\        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        \\        defer decoded.deinit();
        \\        switch (decoded.tag) {
        \\            .call => {
        \\                const call = try decoded.asCall();
        \\                self.question_id = call.question_id;
        \\                var builder = protocol.MessageBuilder.init(self.allocator);
        \\                defer builder.deinit();
        \\                var ret = try builder.beginReturn(call.question_id, .exception);
        \\                try ret.setException("synchronous return");
        \\                const bytes = try builder.finish();
        \\                defer self.allocator.free(bytes);
        \\                try self.peer.handleFrame(bytes);
        \\                if (self.trailing_error) return error.TestTrailingSendError;
        \\            },
        \\            .finish => self.finish_count += 1,
        \\            else => {},
        \\        }
        \\    }
        \\};
        \\
        \\fn finishRetained(peer: *Peer, transport: *SyncTransport) !void {
        \\    const question_id = transport.question_id orelse return error.MissingQuestionId;
        \\    transport.trailing_error = false;
        \\    try peer.finishRetainedQuestion(question_id, false);
        \\    try std.testing.expectEqual(@as(usize, 1), transport.finish_count);
        \\    try std.testing.expectEqual(@as(usize, 0), peer.stats().retained_questions);
        \\}
        \\
        \\test "retained generated call handles synchronous Return then trailing send error" {
        \\    var peer = Peer.initDetached(std.testing.allocator);
        \\    defer peer.deinit();
        \\    var transport = SyncTransport{ .allocator = std.testing.allocator, .trailing_error = true };
        \\    transport.peer = &peer;
        \\    peer.setSendFrameOverride(&transport, SyncTransport.onFrame);
        \\    var callback = CallbackState{};
        \\    const client = generated.Service.PipelinedClient{
        \\        .peer = &peer,
        \\        .question_id = 91,
        \\        .pointer_index = 0,
        \\    };
        \\    try std.testing.expectError(
        \\        error.TestTrailingSendError,
        \\        client.callPingWithOptions(&callback, null, CallbackState.onReturn, .{ .result_lifetime = .retained }),
        \\    );
        \\    try std.testing.expectEqual(@as(usize, 1), callback.calls);
        \\    try std.testing.expectEqual(@as(usize, 1), peer.stats().retained_questions);
        \\    try finishRetained(&peer, &transport);
        \\}
        \\
        \\test "retained generated call handles synchronous nonfatal callback error" {
        \\    var peer = Peer.initDetached(std.testing.allocator);
        \\    defer peer.deinit();
        \\    var transport = SyncTransport{ .allocator = std.testing.allocator };
        \\    transport.peer = &peer;
        \\    peer.setSendFrameOverride(&transport, SyncTransport.onFrame);
        \\    var callback = CallbackState{ .mode = .fail_nonfatal };
        \\    peer.start(&callback, CallbackState.onError, null);
        \\    const client = generated.Service.Client.init(&peer, 7);
        \\    const question_id = try client.callPingWithOptions(
        \\        &callback,
        \\        null,
        \\        CallbackState.onReturn,
        \\        .{ .result_lifetime = .retained },
        \\    );
        \\    try std.testing.expectEqual(transport.question_id.?, question_id);
        \\    try std.testing.expectEqual(@as(usize, 1), callback.calls);
        \\    try std.testing.expectEqual(@as(usize, 1), callback.error_calls);
        \\    try std.testing.expectEqual(error.TestCallbackError, callback.last_error.?);
        \\    try std.testing.expectEqual(@as(usize, 1), peer.stats().retained_questions);
        \\    try finishRetained(&peer, &transport);
        \\}
        \\
        \\test "retained generated call reports synchronous OOM and remains finishable" {
        \\    var peer = Peer.initDetached(std.testing.allocator);
        \\    defer peer.deinit();
        \\    var transport = SyncTransport{ .allocator = std.testing.allocator };
        \\    transport.peer = &peer;
        \\    peer.setSendFrameOverride(&transport, SyncTransport.onFrame);
        \\    var callback = CallbackState{ .mode = .fail_oom };
        \\    peer.start(&callback, CallbackState.onError, null);
        \\    const client = generated.Service.Client.init(&peer, 7);
        \\    const question_id = try client.callPingWithOptions(
        \\        &callback,
        \\        null,
        \\        CallbackState.onReturn,
        \\        .{ .result_lifetime = .retained },
        \\    );
        \\    try std.testing.expectEqual(transport.question_id.?, question_id);
        \\    try std.testing.expectEqual(@as(usize, 1), callback.calls);
        \\    try std.testing.expectEqual(@as(usize, 1), callback.error_calls);
        \\    try std.testing.expectEqual(error.OutOfMemory, callback.last_error.?);
        \\    try std.testing.expectEqual(@as(usize, 1), peer.stats().retained_questions);
        \\    try finishRetained(&peer, &transport);
        \\}
        \\
        \\test "automatic generated call never restores context after synchronous OOM callback" {
        \\    var peer = Peer.initDetached(std.testing.allocator);
        \\    defer peer.deinit();
        \\    var transport = SyncTransport{ .allocator = std.testing.allocator };
        \\    transport.peer = &peer;
        \\    peer.setSendFrameOverride(&transport, SyncTransport.onFrame);
        \\    var callback = CallbackState{ .mode = .fail_oom };
        \\    const client = generated.Service.Client.init(&peer, 7);
        \\    try std.testing.expectError(
        \\        error.OutOfMemory,
        \\        client.callPingWithOptions(&callback, null, CallbackState.onReturn, .{}),
        \\    );
        \\    try std.testing.expectEqual(@as(usize, 1), callback.calls);
        \\    try std.testing.expectEqual(@as(u32, 0), peer.stats().outbound_questions);
        \\    try std.testing.expectEqual(@as(usize, 0), peer.stats().retained_questions);
        \\}
        \\
    );
}

test "Codegen union group init zeroes stale variant data" {
    const allocator = std.testing.allocator;

    try runGeneratedHarness(allocator, "tests/test_schemas/union_group_reset_runtime.capnp",
        \\const std = @import("std");
        \\const capnpc = @import("capnpc-zig");
        \\const message = capnpc.message;
        \\const generated = @import("generated.zig");
        \\
        \\test "union group init resets group slots" {
        \\    var builder = message.MessageBuilder.init(std.testing.allocator);
        \\    defer builder.deinit();
        \\
        \\    var shape = try generated.Shape.Builder.init(&builder);
        \\    try shape.setName("s");
        \\
        \\    // Populate the group's shared storage (data words + pointer slots)
        \\    // with non-default values via the dims variant.
        \\    {
        \\        var dims0 = shape.initDims();
        \\        try dims0.setWidth(3.5);
        \\        try dims0.setHeight(4.5);
        \\        try dims0.setLabel("stale-label");
        \\        try dims0.setNote("stale-note");
        \\    }
        \\
        \\    // Switch to a scalar variant to leave stale bits in the shared data
        \\    // word, and keep the stale group pointers dangling in the pointer
        \\    // section.
        \\    try shape.setScalar(1234.5);
        \\
        \\    // Now select the group variant again: init must zero the group's own
        \\    // data words and null its pointer slots so nothing leaks through.
        \\    var dims = shape.initDims();
        \\    _ = &dims;
        \\
        \\    const bytes = try builder.toBytes();
        \\    defer std.testing.allocator.free(bytes);
        \\
        \\    var msg = try message.Message.init(std.testing.allocator, bytes, .{});
        \\    defer msg.deinit();
        \\
        \\    const root = try msg.getRootStruct();
        \\    const reader = generated.Shape.Reader.wrap(root);
        \\    const dims_reader = try reader.getDims();
        \\
        \\    // All group fields must read back as defaults, not stale data.
        \\    try std.testing.expectEqual(@as(f32, 0.0), try dims_reader.getWidth());
        \\    try std.testing.expectEqual(@as(f32, 0.0), try dims_reader.getHeight());
        \\    try std.testing.expectEqualStrings("", try dims_reader.getLabel());
        \\    try std.testing.expectEqualStrings("", try dims_reader.getNote());
        \\}
        \\
    );
}

// BUG 1: a group nested inside a union group (its group-typed union variant) and
// a plain group nested inside another group were previously dropped by codegen —
// no nested type, no getXxx/initXxx accessor — leaving the variant unusable.
// This verifies the nested types now exist, can be selected/written, and read
// back correctly, and that switching variants zeroes the group's slots.
test "Codegen nested groups compile and round-trip" {
    const allocator = std.testing.allocator;

    try runGeneratedHarness(allocator, "tests/test_schemas/nested_group_runtime.capnp",
        \\const std = @import("std");
        \\const capnpc = @import("capnpc-zig");
        \\const message = capnpc.message;
        \\const generated = @import("generated.zig");
        \\
        \\test "nested group round-trip" {
        \\    var builder = message.MessageBuilder.init(std.testing.allocator);
        \\    defer builder.deinit();
        \\
        \\    var root = try generated.Shape.Builder.init(&builder);
        \\
        \\    // Select the group-typed union variant `circle` inside group `u`.
        \\    var u = root.getU();
        \\    var circle = u.initCircle();
        \\    try circle.setRadius(3.5);
        \\    try circle.setFilled(true);
        \\
        \\    // A plain group nested inside another plain group.
        \\    var outer = root.getOuter();
        \\    try outer.setLabel("hi");
        \\    var inner = outer.getInner();
        \\    try inner.setValue(42);
        \\
        \\    const bytes = try builder.toBytes();
        \\    defer std.testing.allocator.free(bytes);
        \\
        \\    var msg = try message.Message.init(std.testing.allocator, bytes, .{});
        \\    defer msg.deinit();
        \\
        \\    const reader = try generated.Shape.Reader.init(&msg);
        \\    const ur = reader.getU();
        \\    try std.testing.expectEqual(generated.Shape.U.WhichTag.circle, try ur.which());
        \\    const cr = try ur.getCircle();
        \\    try std.testing.expectEqual(@as(f64, 3.5), try cr.getRadius());
        \\    try std.testing.expectEqual(true, try cr.getFilled());
        \\
        \\    const outr = reader.getOuter();
        \\    try std.testing.expectEqualStrings("hi", try outr.getLabel());
        \\    try std.testing.expectEqual(@as(u32, 42), try outr.getInner().getValue());
        \\}
        \\
        \\test "nested union-group variant switch zeroes slots" {
        \\    var builder = message.MessageBuilder.init(std.testing.allocator);
        \\    defer builder.deinit();
        \\
        \\    var root = try generated.Shape.Builder.init(&builder);
        \\    var u = root.getU();
        \\    // Write a non-zero value into the variant-shared word via `square`,
        \\    // then switch to `circle`; initCircle must zero the group's slots.
        \\    try u.setSquare(9.0);
        \\    var circle = u.initCircle();
        \\    _ = &circle;
        \\
        \\    const bytes = try builder.toBytes();
        \\    defer std.testing.allocator.free(bytes);
        \\
        \\    var msg = try message.Message.init(std.testing.allocator, bytes, .{});
        \\    defer msg.deinit();
        \\
        \\    const reader = try generated.Shape.Reader.init(&msg);
        \\    const ur = reader.getU();
        \\    try std.testing.expectEqual(generated.Shape.U.WhichTag.circle, try ur.which());
        \\    try std.testing.expectEqual(@as(f64, 0.0), try (try ur.getCircle()).getRadius());
        \\}
        \\
    );
}

// BUG 3: a union member getter must check the discriminant and return
// error.WrongUnionMember when a different variant is selected, instead of
// silently reinterpreting the other variant's bits.
test "Codegen union member getters check the discriminant" {
    const allocator = std.testing.allocator;

    try runGeneratedHarness(allocator, "tests/test_schemas/union_member_guard_runtime.capnp",
        \\const std = @import("std");
        \\const capnpc = @import("capnpc-zig");
        \\const message = capnpc.message;
        \\const generated = @import("generated.zig");
        \\
        \\test "wrong union member read errors" {
        \\    var builder = message.MessageBuilder.init(std.testing.allocator);
        \\    defer builder.deinit();
        \\
        \\    var root = try generated.ShapeGuard.Builder.init(&builder);
        \\    try root.setSquare(5.0);
        \\
        \\    const bytes = try builder.toBytes();
        \\    defer std.testing.allocator.free(bytes);
        \\
        \\    var msg = try message.Message.init(std.testing.allocator, bytes, .{});
        \\    defer msg.deinit();
        \\
        \\    const reader = try generated.ShapeGuard.Reader.init(&msg);
        \\    // `square` is selected: reading `circle` or `empty` must error.
        \\    try std.testing.expectError(error.WrongUnionMember, reader.getCircle());
        \\    try std.testing.expectError(error.WrongUnionMember, reader.getEmpty());
        \\    // The selected member reads normally.
        \\    try std.testing.expectEqual(@as(f64, 5.0), try reader.getSquare());
        \\    // A non-union field is always readable.
        \\    _ = try reader.getArea();
        \\}
        \\
    );
}

test "Codegen parent-qualified nesting resolves same-name collisions" {
    const allocator = std.testing.allocator;

    // Two structs each with a nested `Inner`/`Color`, and two interfaces each
    // with a `doIt` method: before parent-qualified codegen this aborted with
    // error.DuplicateGeneratedName. Compiling the harness is itself the proof;
    // the assertions document the distinct nested identities.
    try runGeneratedHarness(allocator, "tests/test_schemas/nested_collisions.capnp",
        \\const std = @import("std");
        \\const generated = @import("generated.zig");
        \\
        \\test "same-name nested types across parents are distinct" {
        \\    try std.testing.expect(generated.Outer1.Inner != generated.Outer2.Inner);
        \\    try std.testing.expect(generated.Outer1.Color != generated.Outer2.Color);
        \\    try std.testing.expect(generated.Svc1.DoItParams != generated.Svc2.DoItParams);
        \\    // The method alias resolves to the nested definition.
        \\    try std.testing.expect(generated.Svc1.DoIt.Params == generated.Svc1.DoItParams);
        \\    // Force analysis of a nested-in-struct type's Reader/Builder, whose
        \\    // self-references must be disambiguated from the parent's.
        \\    _ = generated.Outer1.Inner.Reader;
        \\    _ = generated.Outer2.Inner.Builder;
        \\}
        \\
    );
}

test "Codegen parent-qualified nesting resolves same-name nested interfaces" {
    const allocator = std.testing.allocator;

    // Two interfaces (Alpha, Beta) each nesting an interface named `Handle`, each
    // Handle carrying its own nested struct `Token` and a method. Before
    // parent-qualified nested-interface codegen the two `Handle`s emitted flat at
    // file scope and collided (error.DuplicateGeneratedName). Now each Handle
    // emits inside its parent, self-qualifying its Client/Server/VTable/… so its
    // decls aren't shadowed by the enclosing interface's. Compiling the harness is
    // the proof; the assertions document the distinct nested identities.
    try runGeneratedHarness(allocator, "tests/test_schemas/nested_interface_collisions.capnp",
        \\const std = @import("std");
        \\const generated = @import("generated.zig");
        \\
        \\test "same-name nested interfaces across parents are distinct" {
        \\    try std.testing.expect(generated.Alpha.Handle != generated.Beta.Handle);
        \\    try std.testing.expect(generated.Alpha.Handle.Token != generated.Beta.Handle.Token);
        \\    // Force analysis of each nested interface's Client/Server, whose
        \\    // self-references must be disambiguated from the enclosing interface's.
        \\    _ = generated.Alpha.Handle.Client;
        \\    _ = generated.Beta.Handle.Server;
        \\    _ = generated.Alpha.Handle.Server;
        \\    _ = generated.Beta.Handle.Client;
        \\    // Alpha.open's result resolves to the nested Alpha.Handle: its
        \\    // resolveHandle reader returns an Alpha.Handle.Client.
        \\    const ResolveFn = @TypeOf(generated.Alpha.Open.Results.Reader.resolveHandle);
        \\    const ret = @typeInfo(ResolveFn).@"fn".return_type.?;
        \\    try std.testing.expect(@typeInfo(ret).error_union.payload == generated.Alpha.Handle.Client);
        \\}
        \\
    );
}
