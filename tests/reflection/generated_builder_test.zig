const std = @import("std");
const capnp = @import("capnpc-zig");
const generated = @import("generated");
const message = capnp.message;
const Scalars = generated.scalars.Scalars;
const Values = generated.values.Values;

test "generated Builder reads scalar defaults and current values" {
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var value = try Scalars.Builder.init(&arena);
    try std.testing.expect(try value.getBoolean());
    try std.testing.expectEqual(@as(i8, -12), try value.getSigned8());
    try std.testing.expectEqual(@as(i16, -1234), try value.getSigned16());
    try std.testing.expectEqual(@as(i32, -123456), try value.getSigned32());
    try std.testing.expectEqual(@as(i64, -123456789012345), try value.getSigned64());
    try std.testing.expectEqual(@as(u8, 240), try value.getUnsigned8());
    try std.testing.expectEqual(@as(u16, 60000), try value.getUnsigned16());
    try std.testing.expectEqual(@as(u32, 4000000000), try value.getUnsigned32());
    try std.testing.expectEqual(@as(u64, 18000000000000000000), try value.getUnsigned64());
    try std.testing.expectEqual(@as(f32, 1.25), try value.getReal32());
    try std.testing.expectEqual(@as(f64, -2.5), try value.getReal64());
    try value.setSigned32(42);
    try value.setReal32(9.5);
    try value.setBoolean(false);
    try std.testing.expectEqual(@as(i32, 42), try value.getSigned32());
    try std.testing.expectEqual(@as(f32, 9.5), try value.getReal32());
    try std.testing.expect(!try value.getBoolean());
}

test "generated Builder reads Text Data enums and its current union" {
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var value = try Values.Builder.init(&arena);
    try std.testing.expectEqualStrings("tagged", try value.getTagged());
    try std.testing.expectEqualSlices(u8, &.{ 0, 127, 128, 255, 0 }, try value.getInlineData());
    try value.setTagged("changed");
    try std.testing.expectEqualStrings("changed", try value.getTagged());
    try std.testing.expectEqual(@as(u16, 0), value.whichOrdinal());
    try std.testing.expectEqual(.none, try value.which());
    var group = value.getDetails();
    try std.testing.expectEqual(.Archived, try group.getMode());
    try group.setMode(.Active);
    try std.testing.expectEqual(.Active, try group.getMode());
    var selected = value.initSelected();
    try selected.setName("chosen");
    try std.testing.expectEqual(.selected, try value.which());
    try std.testing.expectError(error.WrongUnionMember, value.getNone());
    try std.testing.expectEqualStrings("chosen", try selected.getName());
}

test "generated Builder asReader borrows explicit storage and validates pointer graph" {
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var value = try Values.Builder.init(&arena);
    try value.setTagged("borrowed");
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    const reader = try value.asReader(&storage);
    try std.testing.expectEqualStrings("borrowed", try reader.getTagged());
    try std.testing.expect((try reader.getTagged()).ptr == (try value.getTagged()).ptr);
    // Readers have expired before the mutation. Rebinding reuses storage safely.
    try value.setTagged("rebound");
    try std.testing.expectEqualStrings("rebound", try (try value.asReader(&storage)).getTagged());
    const pointer = try value._builder.getAnyPointer(0);
    std.mem.writeInt(u64, arena.segments.items[pointer.segment_id].items[pointer.pointer_pos..][0..8], 0xfffffffaffffffff, .little);
    try std.testing.expectError(error.InvalidPointer, value.asReader(&storage));
}

test "generated Text getters reject invalid UTF8 and absent wire terminators" {
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var value = try Values.Builder.init(&arena);
    try value.setTagged("\xff");
    try std.testing.expectError(error.InvalidUtf8, value.getTagged());
    var selected = value.initSelected();
    try selected.setName("\xff");
    try std.testing.expectError(error.InvalidUtf8, selected.getName());
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    const reader = try value.asReader(&storage);
    try std.testing.expectError(error.InvalidUtf8, reader.getTagged());
    try std.testing.expectError(error.InvalidUtf8, (try reader.getSelected()).getName());
}

test "generated mutable getters materialize independent defaults and retain edits" {
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var value = try Values.Builder.init(&arena);
    var record = try value.getRecord();
    try std.testing.expectEqualStrings("constant é", try record.getLabel());
    try record.setLabel("edited");
    try std.testing.expectEqualStrings("edited", try (try value.getRecord()).getLabel());
    var numbers = try value.getNumbers();
    try numbers.set(1, 123);
    var records = try value.getRecords();
    var first = try records.get(0);
    try std.testing.expectEqualStrings("first", try first.getLabel());
    try first.setLabel("list edit");
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    const reader = try value.asReader(&storage);
    try std.testing.expectEqual(@as(u64, 123), try (try reader.getNumbers()).get(1));
    try std.testing.expectEqualStrings("list edit", try (try (try reader.getRecords()).get(0)).getLabel());
    var other_arena = message.MessageBuilder.init(std.testing.allocator);
    defer other_arena.deinit();
    var other = try Values.Builder.init(&other_arena);
    try std.testing.expectEqualStrings("constant é", try (try other.getRecord()).getLabel());
}

test "generated mutable getters grow old layouts and retain unknown fields" {
    const Evolution = generated.scalars.Evolution;
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var value = try Evolution.Builder.init(&arena);
    // A newer producer stored unknown data but omitted the later pointer fields.
    var old = try value._builder.initStruct(3, 3, 0);
    old.writeU64(0, 7);
    old.writeU64(16, 0xdeadbeef);
    var child = try value.getSingle();
    try child.setLabel("grown");
    try std.testing.expectEqual(@as(u64, 7), try child.getValue());
    var old_list = try value._builder.writeStructList(0, 2, 3, 0);
    (try old_list.get(0)).writeU64(0, 11);
    (try old_list.get(1)).writeU64(0, 22);
    (try old_list.get(1)).writeU64(16, 0xcafe);
    var list = try value.getRecords();
    var first = try list.get(0);
    try first.setLabel("new field");
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    const reader = try value.asReader(&storage);
    const single = try reader.getSingle();
    try std.testing.expectEqualStrings("grown", try single.getLabel());
    try std.testing.expectEqual(@as(u64, 0xdeadbeef), single._reader.readU64(16));
    const records = try reader.getRecords();
    try std.testing.expectEqual(@as(u64, 22), try (try records.get(1)).getValue());
    try std.testing.expectEqual(@as(u64, 0xcafe), (try records.get(1))._reader.readU64(16));
    try std.testing.expectEqualStrings("new field", try (try records.get(0)).getLabel());
}

test "generated typed copy setters deep copy and support self-copy" {
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var value = try Values.Builder.init(&arena);
    var record = try value.getRecord();
    try record.setLabel("source");
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    try value.setRecord(try (try value.asReader(&storage)).getRecord());
    try std.testing.expectEqualStrings("source", try (try value.getRecord()).getLabel());
    // Source readers borrow the same arena; allocation during copy must not stale them.
    try value.setRecords(try (try value.asReader(&storage)).getRecords());
    try value.setNumbers(try (try value.asReader(&storage)).getNumbers());
    var other_arena = message.MessageBuilder.init(std.testing.allocator);
    defer other_arena.deinit();
    var other = try Values.Builder.init(&other_arena);
    const source = try value.asReader(&storage);
    try other.setRecord(try source.getRecord());
    try other.setRecords(try source.getRecords());
    try other.setNumbers(try source.getNumbers());
    var copied = try other.getRecord();
    try copied.setLabel("copy");
    try std.testing.expectEqualStrings("source", try (try value.getRecord()).getLabel());
    try std.testing.expectEqualStrings("copy", try (try other.getRecord()).getLabel());
    const other_reader = try other.asReader(&storage);
    try std.testing.expectEqual(@as(u64, 0x8000000000000000), try (try other_reader.getNumbers()).get(1));
    try std.testing.expectEqualStrings("first", try (try (try other_reader.getRecords()).get(0)).getLabel());
}

test "generated clear restores defaults and selects cleared union arms" {
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var value = try Values.Builder.init(&arena);
    try value.setHigh(3);
    try value.clearHigh();
    try std.testing.expectEqual(std.math.maxInt(u64), try value.getHigh());
    try value.setTagged("discard");
    try value.clearTagged();
    try std.testing.expect(!value.hasTagged());
    try std.testing.expectEqualStrings("tagged", try value.getTagged());
    var record = try value.getRecord();
    try record.setLabel("discard");
    try value.clearRecord();
    try std.testing.expect(!value.hasRecord());
    try std.testing.expectEqualStrings("constant é", try (try value.getRecord()).getLabel());
    var group = value.getDetails();
    try group.setEnabled(false);
    try value.clearDetails();
    try std.testing.expect(try value.getDetails().getEnabled());
    var selected = value.initSelected();
    try selected.setName("discard");
    try value.clearDetails();
    try std.testing.expectEqual(.selected, try value.which());
    try std.testing.expectEqualStrings("discard", try (try value.getSelected()).getName());
    try value.clearSelected();
    try std.testing.expectEqual(.selected, try value.which());
    try std.testing.expectEqualStrings("", try (try value.getSelected()).getName());
    try value.clearNone();
    try std.testing.expectEqual(.none, try value.which());
}

test "generated constrained pointer setters reject wrong kinds before mutation" {
    const Pointers = generated.brands.Pointers;
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var value = try Pointers.Builder.init(&arena);
    try std.testing.expectError(error.InvalidPointer, value.setStructureText("bad"));
    try std.testing.expectError(error.InvalidPointer, value.setStructureData("bad"));
    try std.testing.expectError(error.InvalidPointer, value.setStructureCapability(.{ .id = 1 }));
    try std.testing.expectError(error.InvalidPointer, value.setListCapability(.{ .id = 1 }));
    try std.testing.expect(!value.hasStructure());
    try std.testing.expect(!value.hasList());
    try value.setAnyText("source");
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    try std.testing.expectError(error.InvalidPointer, value.setStructure(try (try value.asReader(&storage)).getAny()));
    try value.setList(try (try value.asReader(&storage)).getAny());
    try std.testing.expectEqual(@as(u32, 7), (try (try value.getList()).getU8List()).len());
    try value.setAny(try (try value.asReader(&storage)).getList());
    try std.testing.expectEqualStrings("source", try (try (try value.asReader(&storage)).getAny()).getTextStrict());
    try value.clearList();
    try std.testing.expect(!value.hasList());
}

test "generated list copy setters cover scalar pointer nested and enum lists" {
    var source_arena = message.MessageBuilder.init(std.testing.allocator);
    defer source_arena.deinit();
    var source = try Scalars.Builder.init(&source_arena);
    try (try source.initBooleans(2)).set(1, true);
    try (try source.initSigned(1)).set(0, -12);
    try (try source.initReals(1)).set(0, 2.5);
    try (try source.initTexts(1)).set(0, "text");
    try (try source.initBlobs(1)).set(0, "\x00\xff");
    try (try source.initChoices(1)).setOrdinal(0, 65500);
    const nested = try source.initNested(1);
    try (try nested.initU32List(0, 1)).set(0, 44);
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    const reader = try source.asReader(&storage);
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var value = try Scalars.Builder.init(&arena);
    try value.setBooleans(try reader.getBooleans());
    try value.setSigned(try reader.getSigned());
    try value.setReals(try reader.getReals());
    try value.setTexts(try reader.getTexts());
    try value.setBlobs(try reader.getBlobs());
    try value.setChoices(try reader.getChoices());
    try value.setNested(try reader.getNested());
    const copied = try value.asReader(&storage);
    try std.testing.expect(try (try copied.getBooleans()).get(1));
    try std.testing.expectEqual(@as(i16, -12), try (try copied.getSigned()).get(0));
    try std.testing.expectEqual(@as(f64, 2.5), try (try copied.getReals()).get(0));
    try std.testing.expectEqualStrings("text", try (try copied.getTexts()).get(0));
    try std.testing.expectEqualSlices(u8, "\x00\xff", try (try copied.getBlobs()).get(0));
    try std.testing.expectEqual(@as(u16, 65500), try (try copied.getChoices()).getOrdinal(0));
    try std.testing.expectEqual(@as(u32, 44), try (try (try copied.getNested()).getU32List(0)).get(0));
}

test "generated strict Text reads require terminators for fields groups and lists" {
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var value = try Values.Builder.init(&arena);
    try value.setTagged("a");
    const bytes = try value.getTagged();
    @constCast(bytes.ptr)[bytes.len] = 'x';
    try std.testing.expectError(error.InvalidTextPointer, value.getTagged());
    var group = value.initSelected();
    try group.setName("a");
    const group_bytes = try group.getName();
    @constCast(group_bytes.ptr)[group_bytes.len] = 'x';
    try std.testing.expectError(error.InvalidTextPointer, group.getName());
    var list_arena = message.MessageBuilder.init(std.testing.allocator);
    defer list_arena.deinit();
    var list_root = try Scalars.Builder.init(&list_arena);
    try (try list_root.initTexts(1)).set(0, "a");
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    const text = try (try (try list_root.asReader(&storage)).getTexts()).get(0);
    @constCast(text.ptr)[text.len] = 'x';
    try std.testing.expectError(error.InvalidTextPointer, (try (try list_root.asReader(&storage)).getTexts()).get(0));
}

fn copyWithAllocationFailures(allocator: std.mem.Allocator) !void {
    const Evolution = generated.scalars.Evolution;
    var source_arena = message.MessageBuilder.init(allocator);
    defer source_arena.deinit();
    var source = try Evolution.Builder.init(&source_arena);
    var physical = try source._builder.initStruct(3, 3, 3);
    physical.writeU64(0, 71);
    physical.writeU64(16, 0x1234abcd);
    try physical.writeText(0, "known source");
    try physical.writeText(2, "unknown source");
    var source_storage = capnp.generated_helpers.ReaderStorage.init(allocator);
    defer source_storage.deinit();
    const reader = try source.asReader(&source_storage);
    var arena = message.MessageBuilder.init(allocator);
    defer arena.deinit();
    var value = try Evolution.Builder.init(&arena);
    var old = try value.initSingle();
    try old.setValue(99);
    try old.setLabel("retained on failure");
    const pointer = try value._builder.getAnyPointer(3);
    const original_word = std.mem.readInt(u64, arena.segments.items[pointer.segment_id].items[pointer.pointer_pos..][0..8], .little);
    value.setSingle(try reader.getSingle()) catch |err| {
        try std.testing.expectEqual(original_word, std.mem.readInt(u64, arena.segments.items[pointer.segment_id].items[pointer.pointer_pos..][0..8], .little));
        try std.testing.expectEqual(@as(u64, 99), capnp.generated_helpers.scalarReader(try pointer.getStruct()).readU64(0));
        return err;
    };
    var storage = capnp.generated_helpers.ReaderStorage.init(allocator);
    defer storage.deinit();
    const copied = try (try value.asReader(&storage)).getSingle();
    try std.testing.expectEqual(@as(u64, 71), try copied.getValue());
    try std.testing.expectEqual(@as(u64, 0x1234abcd), copied._reader.readU64(16));
    try std.testing.expectEqualStrings("unknown source", try copied._reader.readTextStrict(2));
}

test "generated copy setters retain unknown fields and roll back every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, copyWithAllocationFailures, .{});
}

test "generated list copy through old scalar and pointer types retains unknown struct fields" {
    for (0..3) |encoding| {
        var source_arena = message.MessageBuilder.init(std.testing.allocator);
        defer source_arena.deinit();
        var source = try Scalars.Builder.init(&source_arena);
        const landing = if (encoding == 0) 0 else try source_arena.createSegment();
        const content = if (encoding < 2) landing else try source_arena.createSegment();
        const physical = try source._builder.writeStructListInSegments(1, 2, 2, 2, landing, content);
        for (0..2) |i| {
            const element = try physical.get(@intCast(i));
            element.writeU64(0, 20 + i);
            element.writeU64(8, 0x1000 + i);
            try element.writeText(0, "known pointer");
            try element.writeText(1, "unknown pointer");
        }
        var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
        defer storage.deinit();
        const reader = try source.asReader(&storage);
        var arena = message.MessageBuilder.init(std.testing.allocator);
        defer arena.deinit();
        var copied = try Scalars.Builder.init(&arena);
        try copied.setSigned(try reader.getSigned());
        try copied.setTexts(try reader._reader.readTextListStrict(1));
        var copied_storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
        defer copied_storage.deinit();
        const result = try copied.asReader(&copied_storage);
        for ([_]usize{ 1, 3 }) |field| {
            const list = try result._reader.readStructList(field);
            const second = try list.get(1);
            try std.testing.expectEqual(@as(u64, 21), second.readU64(0));
            try std.testing.expectEqual(@as(u64, 0x1001), second.readU64(8));
            try std.testing.expectEqualStrings("unknown pointer", try second.readTextStrict(1));
        }
        // Void carries no value bits, but copying the list still preserves its
        // original objects, as it does in the reference implementation.
        var void_arena = message.MessageBuilder.init(std.testing.allocator);
        defer void_arena.deinit();
        const void_root = try void_arena.allocateStruct(0, 1);
        try capnp.generated_helpers.setList(try void_root.getAnyPointer(0), try reader._reader.readVoidList(1));
        var void_storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
        defer void_storage.deinit();
        try void_storage.bind(&void_arena);
        const void_list = try (try void_storage.reader(void_root)).readStructList(0);
        try std.testing.expectEqual(@as(u64, 0x1001), (try void_list.get(1)).readU64(8));
        try std.testing.expectEqualStrings("unknown pointer", try (try void_list.get(1)).readTextStrict(1));
    }
}

fn expectListCopyRetainsUnknowns(source: anytype) !void {
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    const root = try arena.allocateStruct(0, 1);
    try capnp.generated_helpers.setList(try root.getAnyPointer(0), source);
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    try storage.bind(&arena);
    const list = try (try storage.reader(root)).readStructList(0);
    try std.testing.expectEqual(@as(u64, 99), (try list.get(1)).readU64(8));
    try std.testing.expectEqualStrings("retained", try (try list.get(1)).readTextStrict(1));
}

test "nested and type-erased list readers retain copy provenance across casts" {
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    const root = try arena.allocateStruct(0, 1);
    const outer = try root.writePointerList(0, 1);
    const list = try outer.initStructList(0, 2, 2, 2);
    const second = try list.get(1);
    second.writeU64(0, 7);
    second.writeU64(8, 99);
    try second.writeText(0, "first pointer");
    try second.writeText(1, "retained");
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    try storage.bind(&arena);
    const nested = try (try storage.reader(root)).readPointerList(0);
    const pointer = message.AnyPointerReader{
        .message = nested.message,
        .segment_id = nested.segment_id,
        .pointer_pos = nested.elements_offset,
        .pointer_word = std.mem.readInt(u64, nested.message.segments[nested.segment_id][nested.elements_offset..][0..8], .little),
    };
    const any = try message.AnyListReader.wrap(pointer);
    inline for (.{
        try nested.getI8List(0),
        try nested.getF32List(0),
        try nested.getF64List(0),
        try nested.getTextListStrict(0),
        try nested.getPointerList(0),
        try nested.getVoidList(0),
        try nested.getStructList(0),
        try pointer.getPointerList(),
        try any.getI8List(),
        try any.getF32List(),
        try any.getF64List(),
        try any.getTextListStrict(),
        try any.getPointerList(),
        try any.getVoidList(),
        try any.getStructList(),
    }) |source| try expectListCopyRetainsUnknowns(source);
}

test "generated Text list constants and pointer defaults expose strict readers" {
    const constant = try generated.brands.textList.get();
    try std.testing.expect(@TypeOf(constant) == message.StrictTextListReader);
    try std.testing.expectEqualStrings("first", try constant.get(0));
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var value = try generated.brands.Brands.Builder.init(&arena);
    var pointers = try value.getPointers();
    try (try (try pointers.getList()).getTextList()).set(0, "changed");
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    const copied = try (try pointers.asReader(&storage)).getList();
    const strict = try (try message.AnyListReader.wrap(copied)).getTextListStrict();
    try std.testing.expectEqualStrings("changed", try strict.get(0));
    try std.testing.expectEqualStrings("first", try constant.get(0));
}

test "generated null and empty list copies initialize borrowed provenance" {
    var source_arena = message.MessageBuilder.init(std.testing.allocator);
    defer source_arena.deinit();
    var source = try Scalars.Builder.init(&source_arena);
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    const reader = try source.asReader(&storage);
    const signed = try reader.getSigned();
    const reals = try reader.getReals();
    const texts = try reader.getTexts();
    const booleans = try reader.getBooleans();
    const nested = try reader.getNested();
    try std.testing.expect(signed.source_list == null);
    try std.testing.expect(reals.source_list == null);
    try std.testing.expect(texts.source_list == null);
    try std.testing.expect(booleans.source_list == null);
    // Recursive typed list codecs use their own empty-reader constructor.
    const empty_signed = message.typed_list_helpers.ScalarListCodec(.int16).empty(nested);
    const empty_void = message.typed_list_helpers.ScalarListCodec(.void).empty(nested);
    try std.testing.expect(empty_signed.source_list == null);
    try std.testing.expect(empty_void.source_list == null);
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var value = try Scalars.Builder.init(&arena);
    try value.setSigned(signed);
    try value.setReals(reals);
    try value.setTexts(texts);
    try value.setBooleans(booleans);
    try value.setNested(nested);
    try value.setBlobs(try reader.getBlobs());
    try value.setChoices(try reader.getChoices());
    try value.setSigned(empty_signed);
    try capnp.generated_helpers.setList(try value._builder.getAnyPointer(0), empty_void);
    const result = try value.asReader(&storage);
    try std.testing.expectEqual(@as(u32, 0), (try result.getSigned()).len());
    try std.testing.expectEqual(@as(u32, 0), (try result.getTexts()).len());
    try std.testing.expectEqual(@as(u32, 0), (try result._reader.readVoidList(0)).len());
}
