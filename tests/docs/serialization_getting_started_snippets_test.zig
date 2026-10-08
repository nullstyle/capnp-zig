//! The Zig code blocks of docs/getting-started-serialization.md, compiled and
//! run.
//!
//! Every Zig block in the guide sits under a verbatim marker that names this
//! file, tests/docs/getting_started/build_person.zig (the whole section 4
//! program) or examples/addressbook.zig (generated code the guide quotes).
//! `zig build docs-smoke` fails when a block and its file differ by one
//! character, and when a Zig block in the guide has no marker. The comment
//! above each excerpt below names the guide section that copies it; change
//! the two together.
//!
//! The build wires this file the way the guide tells a reader to:
//! `capnpc-zig` is the serialization-only core module, `addressbook` is the
//! REAL generated examples/addressbook.zig, and `guide` is generated during
//! the build from tests/docs/schema/guide.capnp, which holds the field shapes
//! the address book lacks. So a guide snippet that uses a field the schema
//! dropped, or an API the runtime lost, fails `zig build test-docs-snippets`.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const addressbook = @import("addressbook");
const guide = @import("guide");

const message = capnpc.message;
const testing = std.testing;
const Shape = guide.Shape;

const build_person = @import("getting_started/build_person.zig");

test "4. Build a Message" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    try build_person.main(.{
        // The program reads only `gpa`; the fields left undefined stay unread.
        .minimal = undefined,
        .arena = &arena,
        .gpa = testing.allocator,
        .io = testing.io,
        .environ_map = undefined,
        .preopens = undefined,
    });
}

/// A serialized Person, as the section 4 program builds it.
fn personBytes(allocator: std.mem.Allocator) ![]const u8 {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var person = try addressbook.Person.Builder.init(&builder);
    try person.setId(1);
    try person.setName("Alice Smith");
    try person.setEmail("alice@example.com");
    return builder.toBytes();
}

/// Section 5, "Deserialize and Read".
fn readPerson(allocator: std.mem.Allocator, bytes: []const u8) !void {
    // 1. Parse the framed message (`.{}` uses the default validation limits)
    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    // 2. Get a typed Reader for the root struct
    const person = try addressbook.Person.Reader.init(&msg);

    // 3. Read fields
    const id = try person.getId(); // u32
    const name = try person.getName(); // []const u8, points into msg's bytes
    const email = try person.getEmail(); // []const u8

    // The values step 4 wrote
    std.debug.assert(id == 1);
    std.debug.assert(std.mem.eql(u8, name, "Alice Smith"));
    std.debug.assert(std.mem.eql(u8, email, "alice@example.com"));

    try testing.expectEqual(@as(u32, 1), id);
    try testing.expectEqualStrings("Alice Smith", name);
    try testing.expectEqualStrings("alice@example.com", email);
}

test "5. Deserialize and Read" {
    const bytes = try personBytes(testing.allocator);
    defer testing.allocator.free(bytes);
    try readPerson(testing.allocator, bytes);
}

/// Section 6, "Enums": usage.
fn usePhoneType(phone: *addressbook.Person.PhoneNumber.Builder) ![]const u8 {
    // Writing
    try phone.setType(.Mobile);

    // Reading (Readers and Builders both have getType)
    const phone_type = try phone.getType(); // a PhoneType
    const label = switch (phone_type) {
        .Mobile => "mobile",
        .Home => "home",
        .Work => "work",
    };

    return label;
}

/// Section 6, "Enums": forwarding a raw ordinal.
fn forwardPhoneType(
    phone: addressbook.Person.PhoneNumber.Reader,
    forwarded_phone: addressbook.Person.PhoneNumber.Builder,
) !void {
    const ordinal = try phone.enumOrdinals().getType();
    try forwarded_phone.enumOrdinals().setType(ordinal);
}

/// Section 6, "Enums": forwarding a raw ordinal from an enum list.
fn forwardColors(reader: guide.Profile.Reader, builder: *guide.Profile.Builder) !void {
    const colors = try reader.getColors();
    const first_ordinal = try colors.getOrdinal(0);
    const forwarded_colors = try builder.initColors(colors.len());
    try forwarded_colors.setOrdinal(0, first_ordinal);
}

test "6. Enums" {
    var phone_builder = message.MessageBuilder.init(testing.allocator);
    defer phone_builder.deinit();
    var phone = try addressbook.Person.PhoneNumber.Builder.init(&phone_builder);
    try testing.expectEqualStrings("mobile", try usePhoneType(&phone));

    var source = message.MessageBuilder.init(testing.allocator);
    defer source.deinit();
    var profile = try guide.Profile.Builder.init(&source);
    const colors = try profile.initColors(1);
    try colors.setOrdinal(0, 7); // an enumerant this schema does not know

    const bytes = try source.toBytes();
    defer testing.allocator.free(bytes);
    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    var target = message.MessageBuilder.init(testing.allocator);
    defer target.deinit();
    var forwarded = try guide.Profile.Builder.init(&target);
    try forwardColors(try guide.Profile.Reader.init(&msg), &forwarded);
    var storage = capnpc.generated_helpers.ReaderStorage.init(testing.allocator);
    defer storage.deinit();
    const forwarded_reader = try forwarded.asReader(&storage);
    try testing.expectEqual(@as(u16, 7), try (try forwarded_reader.getColors()).getOrdinal(0));
}

test "6. Enums: forwarding a phone type ordinal" {
    var source = message.MessageBuilder.init(testing.allocator);
    defer source.deinit();
    var phone = try addressbook.Person.PhoneNumber.Builder.init(&source);
    try phone.enumOrdinals().setType(9); // an enumerant this schema does not know
    const bytes = try source.toBytes();
    defer testing.allocator.free(bytes);
    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    var target = message.MessageBuilder.init(testing.allocator);
    defer target.deinit();
    const forwarded = try addressbook.Person.PhoneNumber.Builder.init(&target);
    try forwardPhoneType(try addressbook.Person.PhoneNumber.Reader.init(&msg), forwarded);
    try testing.expectEqual(@as(u16, 9), try forwarded.enumOrdinals().getType());
    try testing.expectError(error.InvalidEnumValue, forwarded.getType());
}

/// Section 7, "Lists": primitive lists.
fn writeScores(builder: *guide.Profile.Builder) !void {
    // Init the list with a count, then set each element
    const scores = try builder.initScores(3); // List(UInt32), 3 elements
    try scores.set(0, 100);
    try scores.set(1, 95);
    try scores.set(2, 87);
}

fn sumScores(reader: guide.Profile.Reader) !u32 {
    const scores = try reader.getScores();
    var total: u32 = 0;
    for (0..scores.len()) |i| {
        total += try scores.get(@intCast(i));
    }

    return total;
}

/// Section 7, "Lists": text lists.
fn writeTags(builder: *guide.Profile.Builder) !void {
    const tags = try builder.initTags(2);
    try tags.set(0, "zig");
    try tags.set(1, "capnproto");
}

fn firstTag(reader: guide.Profile.Reader) ![]const u8 {
    const tags = try reader.getTags();
    const tag = try tags.get(0); // []const u8

    return tag;
}

/// Section 7, "Lists": nested lists.
fn writeMatrix(builder: *guide.Profile.Builder) !void {
    const rows = try builder.nestedLists().initMatrix(2);
    const first = try rows.init(0, 3);
    try first.set(0, 10);
    try first.set(1, 20);
    try first.set(2, 30);
    try rows.setNull(1);
}

fn readMatrix(reader: guide.Profile.Reader) !void {
    const rows = try reader.nestedLists().getMatrix();
    const first = try rows.get(0);
    std.debug.assert(try first.get(1) == 20);

    // A null inner-list pointer reads as an empty list, but remains observable.
    std.debug.assert(try rows.isNull(1));
    std.debug.assert((try rows.get(1)).len() == 0);

    try testing.expect(try rows.isNull(1));
}

/// Section 8, "Nested Structs".
fn writeAddress(profile: *guide.Profile.Builder) !void {
    // initAddress allocates a nested struct in the message
    var address = try profile.initAddress();
    try address.setStreet("123 Main St");
    try address.setCity("Springfield");
    try address.setZipCode(62704);
}

fn readStreet(profile: guide.Profile.Reader) ![]const u8 {
    const address = try profile.getAddress();
    const street = try address.getStreet();

    return street;
}

test "7, 8. Lists and Nested Structs" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();
    var profile = try guide.Profile.Builder.init(&builder);
    try writeScores(&profile);
    try writeTags(&profile);
    try writeMatrix(&profile);
    try writeAddress(&profile);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);
    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();
    const reader = try guide.Profile.Reader.init(&msg);

    try testing.expectEqual(@as(u32, 282), try sumScores(reader));
    try testing.expectEqualStrings("zig", try firstTag(reader));
    try readMatrix(reader);
    try testing.expectEqualStrings("123 Main St", try readStreet(reader));
}

/// Section 7, "Lists": struct lists.
fn writePhones(person: *addressbook.Person.Builder) !void {
    // init returns a typed list builder
    const phones = try person.initPhones(2);
    var phone0 = try phones.get(0);
    try phone0.setNumber("555-1234");
    try phone0.setType(.Mobile);
    var phone1 = try phones.get(1);
    try phone1.setNumber("555-5678");
    try phone1.setType(.Work);
}

fn countWorkPhones(person: addressbook.Person.Reader) !usize {
    const phones = try person.getPhones();
    var work: usize = 0;
    for (0..phones.len()) |i| {
        const phone = try phones.get(@intCast(i));
        if (try phone.getType() == .Work) work += 1;
    }

    return work;
}

/// Section 11, "Schema Evolution".
fn emailOrNull(person: addressbook.Person.Reader) !?[]const u8 {
    if (person.hasEmail()) {
        return try person.getEmail(); // present, but may still be ""
    }
    return null;
}

test "7, 11. Struct lists and hasXxx()" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();
    var person = try addressbook.Person.Builder.init(&builder);
    try writePhones(&person);
    try person.setEmail("");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);
    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();
    const reader = try addressbook.Person.Reader.init(&msg);

    try testing.expectEqual(@as(usize, 1), try countWorkPhones(reader));
    // Present but empty is not absent.
    try testing.expectEqualStrings("", (try emailOrNull(reader)).?);
}

test "11. hasXxx() on an absent field" {
    const bytes = try personBytes(testing.allocator);
    defer testing.allocator.free(bytes);
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();
    _ = try addressbook.Person.Builder.init(&builder);
    const empty = try builder.toBytes();
    defer testing.allocator.free(empty);
    var msg = try message.Message.init(testing.allocator, empty, .{});
    defer msg.deinit();
    try testing.expectEqual(@as(?[]const u8, null), try emailOrNull(try addressbook.Person.Reader.init(&msg)));
}

/// Section 9, "Unions": writing a circle.
fn circleBytes(allocator: std.mem.Allocator) ![]const u8 {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var shape = try Shape.Builder.init(&builder);
    try shape.setColor(.Red);
    try shape.setCircle(5.0); // sets the discriminant to .circle

    return builder.toBytes();
}

/// Section 9, "Unions": writing a rectangle, a group arm.
fn rectangleBytes(allocator: std.mem.Allocator) ![]const u8 {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var shape = try Shape.Builder.init(&builder);
    try shape.setColor(.Blue);
    var rect = shape.initRectangle(); // sets the discriminant to .rectangle
    try rect.setWidth(10.0);
    try rect.setHeight(20.0);

    return builder.toBytes();
}

/// Section 9, "Unions": reading.
fn area(allocator: std.mem.Allocator, bytes: []const u8) !f64 {
    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    // Always check which() first
    const shape = try Shape.Reader.init(&msg);
    return switch (try shape.which()) {
        .circle => blk: {
            const radius = try shape.getCircle();
            break :blk std.math.pi * radius * radius;
        },
        .rectangle => blk: {
            const rect = try shape.getRectangle();
            const w = try rect.getWidth();
            const h = try rect.getHeight();
            break :blk w * h;
        },
    };
}

test "9. Unions" {
    const circle = try circleBytes(testing.allocator);
    defer testing.allocator.free(circle);
    try testing.expectApproxEqAbs(@as(f64, std.math.pi * 25.0), try area(testing.allocator, circle), 1e-9);

    const rectangle = try rectangleBytes(testing.allocator);
    defer testing.allocator.free(rectangle);
    try testing.expectEqual(@as(f64, 200.0), try area(testing.allocator, rectangle));
}

test "10. Packed Encoding" {
    const allocator = testing.allocator;
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var person = try addressbook.Person.Builder.init(&builder);
    try person.setName("packed");

    // Serialize to packed format
    const packed_bytes = try builder.toPackedBytes();
    defer allocator.free(packed_bytes);

    // Deserialize from packed format
    var msg = try message.Message.initPacked(allocator, packed_bytes, .{});
    defer msg.deinit();

    const reader = try addressbook.Person.Reader.init(&msg);
    try testing.expectEqualStrings("packed", try reader.getName());
}

/// Section 11, "Pointer-kind and brand sidecars".
fn copyValues(reader: guide.Profile.Reader, builder: *guide.Profile.Builder) !void {
    const any_list = try reader.pointerKinds().getValues();
    const words = try any_list.getU32List();

    const list_slot = try builder.pointerKinds().initValues();
    const output = try list_slot.initU32List(words.len());
    for (0..words.len()) |i| try output.set(@intCast(i), try words.get(@intCast(i)));
}

test "11. Pointer-kind sidecars" {
    var source = message.MessageBuilder.init(testing.allocator);
    defer source.deinit();
    var profile = try guide.Profile.Builder.init(&source);
    const values = try (try profile.pointerKinds().initValues()).initU32List(2);
    try values.set(0, 7);
    try values.set(1, 11);
    const bytes = try source.toBytes();
    defer testing.allocator.free(bytes);
    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    var target = message.MessageBuilder.init(testing.allocator);
    defer target.deinit();
    var copy = try guide.Profile.Builder.init(&target);
    try copyValues(try guide.Profile.Reader.init(&msg), &copy);
    var storage = capnpc.generated_helpers.ReaderStorage.init(testing.allocator);
    defer storage.deinit();
    const copied = try (try (try copy.asReader(&storage)).pointerKinds().getValues()).getU32List();
    try testing.expectEqual(@as(u32, 2), copied.len());
    try testing.expectEqual(@as(u32, 11), try copied.get(1));
}

/// Section 11, the brand-aware validation entry point.
fn validatePerson(
    msg: message.Message,
    nodes: []capnpc.schema.Node,
    root_node: *const capnpc.schema.Node,
    root_brand: capnpc.schema.Brand,
) !void {
    try capnpc.schema_validation.validateMessageWithBrand(
        &msg,
        nodes,
        root_node,
        root_brand,
        .{},
    );
}

test "11. validateMessageWithBrand" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const request = try capnpc.request.parseCodeGeneratorRequest(arena.allocator(), addressbook.CAPNP_SCHEMA_REQUEST);
    const person_node = for (request.nodes) |*node| {
        if (node.id == addressbook.Person.capnpSchema.id) break node;
    } else return error.TestUnexpectedResult;

    const bytes = try personBytes(testing.allocator);
    defer testing.allocator.free(bytes);
    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();
    try validatePerson(msg, request.nodes, person_node, .{ .scopes = &.{} });
}
