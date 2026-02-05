const std = @import("std");
const testing = std.testing;
const capnpc = @import("capnpc-zig");
const message = capnpc.message;

// Test based on real-world schema from:
// https://github.com/cmackenzie1/capnp-rust-examples/blob/main/schemas/person.capnp
//
// @0xdbb9ad1f14bf0b36;
//
// struct Person {
//   name @0 :Text;
//   email @1 :Text;
// }

const Person = struct {
    pub const Reader = struct {
        _reader: message.StructReader,

        pub fn init(msg: *const message.Message) !Reader {
            const root = try msg.getRootStruct();
            return .{ ._reader = root };
        }

        pub fn getName(self: Reader) ![]const u8 {
            return try self._reader.readText(0);
        }

        pub fn getEmail(self: Reader) ![]const u8 {
            return try self._reader.readText(1);
        }
    };

    pub const Builder = struct {
        _builder: message.StructBuilder,

        pub fn init(msg: *message.MessageBuilder) !Builder {
            // Person has 0 data words (no primitive fields) and 2 pointer words (name, email)
            const builder = try msg.allocateStruct(0, 2);
            return .{ ._builder = builder };
        }

        pub fn setName(self: *Builder, value: []const u8) !void {
            try self._builder.writeText(0, value);
        }

        pub fn setEmail(self: *Builder, value: []const u8) !void {
            try self._builder.writeText(1, value);
        }
    };
};

test "Real-world: Person schema - basic fields" {
    // Create a Person
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var person_builder = try Person.Builder.init(&msg_builder);
    try person_builder.setName("Alice Johnson");
    try person_builder.setEmail("alice@example.com");

    // Serialize
    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    // Deserialize
    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const person_reader = try Person.Reader.init(&msg);

    // Verify
    const name = try person_reader.getName();
    try testing.expectEqualStrings("Alice Johnson", name);

    const email = try person_reader.getEmail();
    try testing.expectEqualStrings("alice@example.com", email);
}

test "Real-world: Person schema - empty strings" {
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var person_builder = try Person.Builder.init(&msg_builder);
    try person_builder.setName("");
    try person_builder.setEmail("");

    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const person_reader = try Person.Reader.init(&msg);

    try testing.expectEqualStrings("", try person_reader.getName());
    try testing.expectEqualStrings("", try person_reader.getEmail());
}

test "Real-world: Person schema - long strings" {
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var person_builder = try Person.Builder.init(&msg_builder);

    const long_name = "Dr. Elizabeth Alexandra Mary Windsor-Mountbatten III";
    const long_email = "elizabeth.alexandra.mary.windsor.mountbatten.the.third@buckingham.palace.gov.uk";

    try person_builder.setName(long_name);
    try person_builder.setEmail(long_email);

    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const person_reader = try Person.Reader.init(&msg);

    try testing.expectEqualStrings(long_name, try person_reader.getName());
    try testing.expectEqualStrings(long_email, try person_reader.getEmail());
}

test "Real-world: Person schema - special characters" {
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var person_builder = try Person.Builder.init(&msg_builder);

    try person_builder.setName("José García-Müller");
    try person_builder.setEmail("josé.garcía@münchen.de");

    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const person_reader = try Person.Reader.init(&msg);

    try testing.expectEqualStrings("José García-Müller", try person_reader.getName());
    try testing.expectEqualStrings("josé.garcía@münchen.de", try person_reader.getEmail());
}
