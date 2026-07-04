//! Standalone serialization example — no RPC.
//!
//! A complete, runnable companion to `docs/getting-started-serialization.md`.
//! It builds a Cap'n Proto message from the schema in
//! `examples/addressbook.capnp`, serializes it in both the plain and the
//! packed wire encodings, then reads each back **zero-copy** and prints a few
//! fields. Along the way it exercises the interesting wire-format paths a
//! serialization adopter cares about: nested structs, an enum, lists (of
//! struct), an (unnamed) union, and both Text and Data fields.
//!
//! Dependency note: this program wires the generated code and the runtime
//! through the `capnpc-zig-core` module (imported here under the name
//! `capnpc-zig`, which is what generated code expects). `-core` is the
//! serialization-only surface — no TCP/QUIC transport is pulled into the build.
//! See `docs/supported-surface.md#modules--which-to-import`.
//!
//! Run it with: `zig build example-serialization`

const std = @import("std");
const capnpc = @import("capnpc-zig");
const addressbook = @import("addressbook");

const message = capnpc.message;
const AddressBook = addressbook.AddressBook;
const Person = addressbook.Person;

pub fn main(_: std.process.Init) !void {
    // A leak-checking allocator so the example doubles as a memory-safety
    // demonstration: every byte the builder and message allocate is freed by
    // the `defer`s below, and `gpa.deinit()` asserts nothing leaked.
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    // ---------------------------------------------------------------------
    // 1. Build a message.
    // ---------------------------------------------------------------------
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var book = try AddressBook.Builder.init(&builder);
    var people = try book.initPeople(2);

    // Person 0 — an employer, two phone numbers, a tiny "avatar" blob.
    {
        var person = try people.get(0);
        try person.setId(1);
        try person.setName("Ada Lovelace");
        try person.setEmail("ada@analytical-engines.example");
        try person.setAvatar(&[_]u8{ 0x89, 0x50, 0x4e, 0x47 }); // Data field
        try person.setEmployer("Analytical Engines Ltd"); // sets the union arm

        var phones = try person.initPhones(2);
        var mobile = try phones.get(0);
        try mobile.setNumber("+1-555-0100");
        try mobile.setType(.Mobile); // enum
        var work = try phones.get(1);
        try work.setNumber("+1-555-0111");
        try work.setType(.Work);
    }

    // Person 1 — self-employed (a void union arm), one phone number.
    {
        var person = try people.get(1);
        try person.setId(2);
        try person.setName("Alan Turing");
        try person.setEmail("alan@turing.example");
        try person.setAvatar(&[_]u8{ 0x42, 0x4d }); // Data field
        try person.setSelfEmployed({}); // sets the union arm

        var phones = try person.initPhones(1);
        var home = try phones.get(0);
        try home.setNumber("+44-20-7946-0000");
        try home.setType(.Home);
    }

    // ---------------------------------------------------------------------
    // 2. Serialize — plain and packed encodings.
    // ---------------------------------------------------------------------
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    const packed_bytes = try builder.toPackedBytes();
    defer allocator.free(packed_bytes);

    std.debug.print("serialized: {d} bytes plain, {d} bytes packed\n", .{
        bytes.len,
        packed_bytes.len,
    });

    // ---------------------------------------------------------------------
    // 3. Read the plain encoding back — zero-copy.
    // ---------------------------------------------------------------------
    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const book_reader = try AddressBook.Reader.init(&msg);
    try printBook(book_reader);

    // ---------------------------------------------------------------------
    // 4. Read the packed encoding back and confirm it round-trips.
    // ---------------------------------------------------------------------
    var packed_msg = try message.Message.initPacked(allocator, packed_bytes, .{});
    defer packed_msg.deinit();

    const packed_book = try AddressBook.Reader.init(&packed_msg);
    const first_plain = try (try book_reader.getPeople()).get(0);
    const first_packed = try (try packed_book.getPeople()).get(0);
    std.debug.assert(std.mem.eql(u8, try first_plain.getName(), try first_packed.getName()));
    std.debug.print("packed round-trip: OK ({s})\n", .{try first_packed.getName()});
}

/// Walk an AddressBook reader and print each person. The slices returned by
/// text/data getters point directly into the message buffer (zero-copy), so
/// the backing `Message` must outlive them — it does here.
fn printBook(book: AddressBook.Reader) !void {
    const people = try book.getPeople();
    std.debug.print("address book: {d} people\n", .{people.len()});

    var i: u32 = 0;
    while (i < people.len()) : (i += 1) {
        const person = try people.get(i);
        std.debug.print("  #{d} {s} <{s}>\n", .{
            try person.getId(),
            try person.getName(),
            try person.getEmail(),
        });

        // Enum + list-of-struct.
        const phones = try person.getPhones();
        var j: u32 = 0;
        while (j < phones.len()) : (j += 1) {
            const phone = try phones.get(j);
            const kind: []const u8 = switch (try phone.getType()) {
                .Mobile => "mobile",
                .Home => "home",
                .Work => "work",
            };
            std.debug.print("      {s}: {s}\n", .{ kind, try phone.getNumber() });
        }

        // Union — always branch on which() before reading an arm.
        switch (try person.which()) {
            .unemployed => std.debug.print("      employment: unemployed\n", .{}),
            .employer => std.debug.print("      employment: employed at {s}\n", .{try person.getEmployer()}),
            .school => std.debug.print("      employment: student at {s}\n", .{try person.getSchool()}),
            .selfEmployed => std.debug.print("      employment: self-employed\n", .{}),
        }

        // Data — raw bytes.
        std.debug.print("      avatar: {d} bytes\n", .{(try person.getAvatar()).len});
    }
}
