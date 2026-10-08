const std = @import("std");
const capnpc = @import("capnpc-zig");
const message = capnpc.message;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // Create a message builder
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    // Allocate a struct with 1 data word and 2 pointer words
    const struct_builder = try builder.allocateStruct(1, 2);

    // Write primitive fields
    struct_builder.writeU32(0, 42);
    struct_builder.writeU32(4, 100);

    // Write text fields
    try struct_builder.writeText(0, "Hello");
    try struct_builder.writeText(1, "World");

    // Serialize to bytes
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    // Deserialize (`.{}` keeps the default validation limits)
    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();

    // Read fields (text slices point into `bytes`; nothing is copied)
    std.debug.assert(root.readU32(0) == 42);
    std.debug.assert(root.readU32(4) == 100);
    std.debug.assert(std.mem.eql(u8, try root.readText(0), "Hello"));
    std.debug.assert(std.mem.eql(u8, try root.readText(1), "World"));
}
