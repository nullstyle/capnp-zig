//! The codegen consumer: code generated at build time by the pinned plugin
//! (`dep.artifact("capnpc-zig")`, see ../build.zig), compiled against the same
//! package's runtime. `tools/package_preflight.zig` builds and runs it from the
//! filtered release archive; `zig build test-docs-snippets` runs `exercise`
//! against this checkout.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const addressbook = @import("addressbook");

const AddressBook = addressbook.AddressBook;
const Person = addressbook.Person;

pub fn main(init: std.process.Init) !void {
    try exercise(init.gpa);
}

/// Round-trip one message through the generated Builder and Reader, then load
/// the reflection metadata the plugin embeds by default. A plugin and runtime
/// that disagree fail here, at compile time or with an error.
pub fn exercise(allocator: std.mem.Allocator) !void {
    var builder = capnpc.message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var book = try AddressBook.Builder.init(&builder);
    var people = try book.initPeople(1);
    var person = try people.get(0);
    try person.setId(7);
    try person.setName("Ada Lovelace");
    try person.setAvatar(&.{ 0x89, 0x50, 0x4e, 0x47 });
    try person.setEmployer("Analytical Engines Ltd");
    var phones = try person.initPhones(1);
    var phone = try phones.get(0);
    try phone.setNumber("+1-555-0100");
    try phone.setType(.Work);

    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    var msg = try capnpc.message.Message.init(allocator, bytes, .{});
    defer msg.deinit();
    const read_book = try AddressBook.Reader.init(&msg);
    const read_person = try (try read_book.getPeople()).get(0);
    if (try read_person.getId() != 7) return error.WrongId;
    if (!std.mem.eql(u8, try read_person.getName(), "Ada Lovelace")) return error.WrongName;
    if ((try read_person.getAvatar()).len != 4) return error.WrongAvatar;
    if (try read_person.which() != .employer) return error.WrongUnionArm;
    if (!std.mem.eql(u8, try read_person.getEmployer(), "Analytical Engines Ltd")) return error.WrongEmployer;
    const read_phone = try (try read_person.getPhones()).get(0);
    if (try read_phone.getType() != .Work) return error.WrongPhoneType;

    const registry = try Person.capnpSchema.load(allocator);
    defer registry.deinit();
    const person_schema = try (try Person.capnpSchema.resolve(registry)).asStruct();
    _ = try person_schema.field("employer");
}
