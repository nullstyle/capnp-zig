const std = @import("std");
const testing = std.testing;
const capnpc = @import("capnpc-zig");
const message = capnpc.message;

// Test based on real-world schema from official Cap'n Proto examples:
// https://capnproto.github.io/pycapnp/quickstart.html
//
// Simplified version (without lists, enums, unions, nested structs)
//
// @0x934efea7f017fff0;
//
// struct Person {
//   id @0 :UInt32;
//   name @1 :Text;
//   email @2 :Text;
//   // phones @3 :List(PhoneNumber);  // Not yet supported
//   // employment :union { ... }       // Not yet supported
// }

const AddressBookPerson = struct {
    pub const Reader = struct {
        _reader: message.StructReader,

        pub fn init(msg: *const message.Message) !Reader {
            const root = try msg.getRootStruct();
            return .{ ._reader = root };
        }

        pub fn getId(self: Reader) u32 {
            return self._reader.readU32(0);
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
            // Person has 1 data word (id: UInt32) and 2 pointer words (name, email)
            const builder = try msg.allocateStruct(1, 2);
            return .{ ._builder = builder };
        }

        pub fn setId(self: *Builder, value: u32) void {
            self._builder.writeU32(0, value);
        }

        pub fn setName(self: *Builder, value: []const u8) !void {
            try self._builder.writeText(0, value);
        }

        pub fn setEmail(self: *Builder, value: []const u8) !void {
            try self._builder.writeText(1, value);
        }
    };
};

test "Real-world: AddressBook Person - complete record" {
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var person_builder = try AddressBookPerson.Builder.init(&msg_builder);
    person_builder.setId(12345);
    try person_builder.setName("Bob Smith");
    try person_builder.setEmail("bob.smith@example.com");

    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const person_reader = try AddressBookPerson.Reader.init(&msg);

    try testing.expectEqual(@as(u32, 12345), person_reader.getId());
    try testing.expectEqualStrings("Bob Smith", try person_reader.getName());
    try testing.expectEqualStrings("bob.smith@example.com", try person_reader.getEmail());
}

test "Real-world: AddressBook Person - minimum id" {
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var person_builder = try AddressBookPerson.Builder.init(&msg_builder);
    person_builder.setId(0);
    try person_builder.setName("Zero Person");
    try person_builder.setEmail("zero@example.com");

    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const person_reader = try AddressBookPerson.Reader.init(&msg);

    try testing.expectEqual(@as(u32, 0), person_reader.getId());
    try testing.expectEqualStrings("Zero Person", try person_reader.getName());
}

test "Real-world: AddressBook Person - maximum id" {
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var person_builder = try AddressBookPerson.Builder.init(&msg_builder);
    person_builder.setId(4294967295); // Max u32
    try person_builder.setName("Max Person");
    try person_builder.setEmail("max@example.com");

    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const person_reader = try AddressBookPerson.Reader.init(&msg);

    try testing.expectEqual(@as(u32, 4294967295), person_reader.getId());
    try testing.expectEqualStrings("Max Person", try person_reader.getName());
}

test "Real-world: AddressBook Person - realistic data" {
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var person_builder = try AddressBookPerson.Builder.init(&msg_builder);
    person_builder.setId(987654);
    try person_builder.setName("Dr. Sarah Chen");
    try person_builder.setEmail("s.chen@university.edu");

    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const person_reader = try AddressBookPerson.Reader.init(&msg);

    try testing.expectEqual(@as(u32, 987654), person_reader.getId());
    try testing.expectEqualStrings("Dr. Sarah Chen", try person_reader.getName());
    try testing.expectEqualStrings("s.chen@university.edu", try person_reader.getEmail());
}

test "Real-world: AddressBook Person - multiple serializations" {
    // Test that we can serialize multiple different people
    const people = [_]struct { id: u32, name: []const u8, email: []const u8 }{
        .{ .id = 1, .name = "Alice", .email = "alice@a.com" },
        .{ .id = 2, .name = "Bob", .email = "bob@b.com" },
        .{ .id = 3, .name = "Charlie", .email = "charlie@c.com" },
    };

    for (people) |person_data| {
        var msg_builder = message.MessageBuilder.init(testing.allocator);
        defer msg_builder.deinit();

        var person_builder = try AddressBookPerson.Builder.init(&msg_builder);
        person_builder.setId(person_data.id);
        try person_builder.setName(person_data.name);
        try person_builder.setEmail(person_data.email);

        const bytes = try msg_builder.toBytes();
        defer testing.allocator.free(bytes);

        var msg = try message.Message.init(testing.allocator, bytes);
        defer msg.deinit();

        const person_reader = try AddressBookPerson.Reader.init(&msg);

        try testing.expectEqual(person_data.id, person_reader.getId());
        try testing.expectEqualStrings(person_data.name, try person_reader.getName());
        try testing.expectEqualStrings(person_data.email, try person_reader.getEmail());
    }
}
