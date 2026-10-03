//! Writes a freshly generated binding for the plugin/runtime skew check.
//!
//! The `test-codegen-skew` step runs this with the generator from the current
//! tree, then compiles the output against stub runtimes that report other
//! codegen ABIs. Generating at build time (rather than reusing a committed
//! file) means the check follows the emitter itself: drop the guard from
//! `Generator.generateFile` and the step goes red without any regeneration.
//!
//! Usage: generate_fixture <output.zig>

const std = @import("std");
const capnpc = @import("capnpc-zig");
const schema = capnpc.schema;

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = init.io;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.skip();
    const out_path = args.next() orelse return error.MissingOutputPath;

    // struct Person { name @0 :Text; age @1 :UInt32; }
    var fields = [_]schema.Field{
        .{
            .name = "name",
            .code_order = 0,
            .annotations = &.{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 0, .type = .text, .default_value = null },
            .group = null,
        },
        .{
            .name = "age",
            .code_order = 1,
            .annotations = &.{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 0, .type = .uint32, .default_value = null },
            .group = null,
        },
    };
    const person_node = schema.Node{
        .id = 0xA1B2C3D4E5F60011,
        .display_name = "skew.capnp:Person",
        .display_name_prefix_length = 11,
        .scope_id = 0xF0F0F0F0F0F0F011,
        .nested_nodes = &.{},
        .annotations = &.{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 1,
            .pointer_count = 1,
            .preferred_list_encoding = .inline_composite,
            .is_group = false,
            .discriminant_count = 0,
            .discriminant_offset = 0,
            .fields = &fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };
    var nested = [_]schema.Node.NestedNode{.{ .name = "Person", .id = person_node.id }};
    const file_node = schema.Node{
        .id = 0xF0F0F0F0F0F0F011,
        .display_name = "skew.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = nested[0..],
        .annotations = &.{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const nodes = [_]schema.Node{ file_node, person_node };
    var generator = try capnpc.codegen.Generator.init(allocator, &nodes);
    defer generator.deinit();
    const output = try generator.generateFile(.{
        .id = file_node.id,
        .filename = "skew.capnp",
        .imports = &.{},
    });
    defer allocator.free(output);

    var file = try std.Io.Dir.cwd().createFile(io, out_path, .{});
    defer file.close(io);
    try file.writeStreamingAll(io, output);
}
