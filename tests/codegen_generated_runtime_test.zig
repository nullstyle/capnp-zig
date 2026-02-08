const std = @import("std");
const capnpc = @import("capnpc-zig");
const request_reader = capnpc.request;

fn writeFile(dir: std.fs.Dir, name: []const u8, data: []const u8) !void {
    var file = try dir.createFile(name, .{});
    defer file.close();
    try file.writeAll(data);
}

fn runGeneratedHarness(
    allocator: std.mem.Allocator,
    schema_path: []const u8,
    harness_source: []const u8,
) !void {
    const capnp_argv = &[_][]const u8{
        "capnp",
        "compile",
        "-o-",
        schema_path,
    };
    const capnp_result = std.process.Child.run(.{
        .allocator = allocator,
        .argv = capnp_argv,
        .max_output_bytes = 10 * 1024 * 1024,
    }) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer allocator.free(capnp_result.stdout);
    defer allocator.free(capnp_result.stderr);
    try std.testing.expect(capnp_result.term == .Exited and capnp_result.term.Exited == 0);

    const request = try request_reader.parseCodeGeneratorRequest(allocator, capnp_result.stdout);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);
    if (request.requested_files.len == 0) return error.InvalidCodeGeneratorRequest;

    var generator = try capnpc.codegen.Generator.init(allocator, request.nodes);
    defer generator.deinit();
    const generated = try generator.generateFile(request.requested_files[0]);
    defer allocator.free(generated);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeFile(tmp.dir, "generated.zig", generated);
    try writeFile(tmp.dir, "harness.zig", harness_source);

    const harness_path = try tmp.dir.realpathAlloc(allocator, "harness.zig");
    defer allocator.free(harness_path);

    const lib_path = try std.fs.cwd().realpathAlloc(allocator, "src/lib.zig");
    defer allocator.free(lib_path);
    const xev_path = try std.fs.cwd().realpathAlloc(allocator, "vendor/ext/libxev/src/main.zig");
    defer allocator.free(xev_path);

    const lib_arg = try std.fmt.allocPrint(allocator, "-Mcapnpc-zig={s}", .{lib_path});
    defer allocator.free(lib_arg);
    const xev_arg = try std.fmt.allocPrint(allocator, "-Mxev={s}", .{xev_path});
    defer allocator.free(xev_arg);
    const root_arg = try std.fmt.allocPrint(allocator, "-Mroot={s}", .{harness_path});
    defer allocator.free(root_arg);

    var zig_argv = std.ArrayList([]const u8){};
    defer zig_argv.deinit(allocator);
    try zig_argv.append(allocator, "zig");
    try zig_argv.append(allocator, "test");
    try zig_argv.append(allocator, "--dep");
    try zig_argv.append(allocator, "capnpc-zig");
    try zig_argv.append(allocator, root_arg);
    try zig_argv.append(allocator, "--dep");
    try zig_argv.append(allocator, "xev");
    try zig_argv.append(allocator, lib_arg);
    try zig_argv.append(allocator, xev_arg);

    const zig_result = std.process.Child.run(.{
        .allocator = allocator,
        .argv = zig_argv.items,
        .max_output_bytes = 10 * 1024 * 1024,
    }) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer allocator.free(zig_result.stdout);
    defer allocator.free(zig_result.stderr);

    if (!(zig_result.term == .Exited and zig_result.term.Exited == 0)) {
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
        \\    var msg = try message.Message.init(std.testing.allocator, bytes);
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
        \\        var msg = try message.Message.init(std.testing.allocator, bytes);
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
        \\        var msg = try message.Message.init(std.testing.allocator, bytes);
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
        \\    var msg = try message.Message.init(std.testing.allocator, bytes);
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
        \\    var msg = try message.Message.init(std.testing.allocator, bytes);
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
        \\        var msg = try message.Message.init(std.testing.allocator, bytes);
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
        \\        var msg = try message.Message.init(std.testing.allocator, bytes);
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
