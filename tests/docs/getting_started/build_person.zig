const std = @import("std");
const capnpc = @import("capnpc-zig");
const message = capnpc.message;
const addressbook = @import("addressbook");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // 1. Create a MessageBuilder
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    // 2. Initialize the root struct
    var person = try addressbook.Person.Builder.init(&builder);

    // 3. Set fields
    try person.setId(1);
    try person.setName("Alice Smith");
    try person.setEmail("alice@example.com");

    // 4. Serialize to bytes
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    // `bytes` now holds the framed Cap'n Proto message: write it to a file
    // or a socket, or read it back as in step 5.
}
