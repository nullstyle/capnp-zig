const std = @import("std");
const capnpc = @import("capnpc-zig");
// Generated from examples/addressbook.capnp; your build.zig names the module.
const addressbook = @import("addressbook");
const Person = addressbook.Person;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // Create a Person
    var msg_builder = capnpc.message.MessageBuilder.init(allocator);
    defer msg_builder.deinit();

    var person_builder = try Person.Builder.init(&msg_builder);
    try person_builder.setId(1);
    try person_builder.setName("Alice");
    try person_builder.setEmail("alice@example.com");

    // Serialize
    const bytes = try msg_builder.toBytes();
    defer allocator.free(bytes);

    // Deserialize
    var msg = try capnpc.message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const person_reader = try Person.Reader.init(&msg);

    // Access fields
    std.debug.assert(try person_reader.getId() == 1);
    std.debug.assert(std.mem.eql(u8, try person_reader.getName(), "Alice"));
    std.debug.assert(std.mem.eql(u8, try person_reader.getEmail(), "alice@example.com"));
}
