const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const testing = std.testing;

const AddressBook = struct {
    pub const PhoneType = enum(u16) {
        Mobile = 0,
        Home = 1,
        Work = 2,
    };

    pub const PhoneNumber = struct {
        pub const Reader = struct {
            _reader: message.StructReader,

            pub fn wrap(reader: message.StructReader) Reader {
                return .{ ._reader = reader };
            }

            pub fn getNumber(self: Reader) ![]const u8 {
                return try self._reader.readText(0);
            }

            pub fn getType(self: Reader) !PhoneType {
                return @fromBackingInt(@intCast(self._reader.readU16(0)));
            }
        };

        pub const Builder = struct {
            _builder: message.StructBuilder,

            pub fn wrap(builder: message.StructBuilder) Builder {
                return .{ ._builder = builder };
            }

            pub fn setNumber(self: *Builder, value: []const u8) !void {
                try self._builder.writeText(0, value);
            }

            pub fn setType(self: *Builder, value: PhoneType) !void {
                self._builder.writeU16(0, @backingInt(value));
            }
        };
    };

    pub const PhoneNumberListReader = struct {
        _list: message.StructListReader,

        pub fn len(self: PhoneNumberListReader) u32 {
            return self._list.len();
        }

        pub fn get(self: PhoneNumberListReader, index: u32) !PhoneNumber.Reader {
            return PhoneNumber.Reader.wrap(try self._list.get(index));
        }
    };

    pub const PhoneNumberListBuilder = struct {
        _list: message.StructListBuilder,

        pub fn len(self: PhoneNumberListBuilder) u32 {
            return self._list.len();
        }

        pub fn get(self: PhoneNumberListBuilder, index: u32) !PhoneNumber.Builder {
            return PhoneNumber.Builder.wrap(try self._list.get(index));
        }
    };

    pub const Person = struct {
        pub const Reader = struct {
            _reader: message.StructReader,

            pub fn init(msg: *const message.Message) !Reader {
                const root = try msg.getRootStruct();
                return .{ ._reader = root };
            }

            pub fn getName(self: Reader) ![]const u8 {
                return try self._reader.readText(0);
            }

            pub fn getAge(self: Reader) !u32 {
                return self._reader.readU32(0);
            }

            pub fn getEmail(self: Reader) ![]const u8 {
                return try self._reader.readText(1);
            }

            pub fn getPhones(self: Reader) !PhoneNumberListReader {
                return .{ ._list = try self._reader.readStructList(2) };
            }
        };

        pub const Builder = struct {
            _builder: message.StructBuilder,

            pub fn init(msg: *message.MessageBuilder) !Builder {
                const builder = try msg.allocateStruct(1, 3);
                return .{ ._builder = builder };
            }

            pub fn setName(self: *Builder, value: []const u8) !void {
                try self._builder.writeText(0, value);
            }

            pub fn setAge(self: *Builder, value: u32) !void {
                self._builder.writeU32(0, value);
            }

            pub fn setEmail(self: *Builder, value: []const u8) !void {
                try self._builder.writeText(1, value);
            }

            pub fn initPhones(self: *Builder, element_count: u32) !PhoneNumberListBuilder {
                return .{ ._list = try self._builder.writeStructList(2, element_count, 1, 1) };
            }
        };
    };
};

test "README direct message serialization snippet uses current public API" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(1, 2);
    struct_builder.writeU32(0, 42);
    struct_builder.writeU32(4, 100);
    try struct_builder.writeText(0, "Hello");
    try struct_builder.writeText(1, "World");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqual(@as(u32, 42), root.readU32(0));
    try testing.expectEqual(@as(u32, 100), root.readU32(4));
    try testing.expectEqualStrings("Hello", try root.readText(0));
    try testing.expectEqualStrings("World", try root.readText(1));
}

test "getting-started generated Person snippets use current public API" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var person = try AddressBook.Person.Builder.init(&builder);
    try person.setName("Alice Smith");
    try person.setAge(30);
    try person.setEmail("alice@example.com");

    var phones = try person.initPhones(2);
    var phone0 = try phones.get(0);
    try phone0.setNumber("555-1234");
    try phone0.setType(.Mobile);

    var phone1 = try phones.get(1);
    try phone1.setNumber("555-5678");
    try phone1.setType(.Work);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const reader = try AddressBook.Person.Reader.init(&msg);
    try testing.expectEqualStrings("Alice Smith", try reader.getName());
    try testing.expectEqual(@as(u32, 30), try reader.getAge());
    try testing.expectEqualStrings("alice@example.com", try reader.getEmail());

    const phone_readers = try reader.getPhones();
    try testing.expectEqual(@as(u32, 2), phone_readers.len());

    const first_phone = try phone_readers.get(0);
    try testing.expectEqualStrings("555-1234", try first_phone.getNumber());
    try testing.expectEqual(AddressBook.PhoneType.Mobile, try first_phone.getType());

    const second_phone = try phone_readers.get(1);
    try testing.expectEqualStrings("555-5678", try second_phone.getNumber());
    try testing.expectEqual(AddressBook.PhoneType.Work, try second_phone.getType());
}

test "getting-started packed encoding snippet uses current public API" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root = try builder.allocateStruct(1, 1);
    root.writeU32(0, 7);
    try root.writeText(0, "packed");

    const packed_bytes = try builder.toPackedBytes();
    defer testing.allocator.free(packed_bytes);

    var msg = try message.Message.initPacked(testing.allocator, packed_bytes, .{});
    defer msg.deinit();

    const reader = try msg.getRootStruct();
    try testing.expectEqual(@as(u32, 7), reader.readU32(0));
    try testing.expectEqualStrings("packed", try reader.readText(0));
}
