const std = @import("std");
const testing = std.testing;
const capnpc = @import("capnpc-zig");
const message = capnpc.message;

// Test based on Cap'n Proto union semantics
//
// In Cap'n Proto, unions are represented as:
// - A discriminant field (u16) that indicates which field is set
// - The union fields themselves, which can be primitives or pointers
//
// Example schema:
// struct Shape {
//   area @0 :Float64;
//
//   perimeter :union {
//     circle @1 :Float64;      # discriminant = 0
//     square @2 :Float64;      # discriminant = 1
//     rectangle @3 :Rectangle; # discriminant = 2
//   }
// }

const Shape = struct {
    // Union discriminant values
    pub const PerimeterTag = enum(u16) {
        circle = 0,
        square = 1,
        rectangle = 2,
    };

    pub const Reader = struct {
        _reader: message.StructReader,

        pub fn init(msg: *const message.Message) !Reader {
            const root = try msg.getRootStruct();
            return .{ ._reader = root };
        }

        pub fn getArea(self: Reader) f64 {
            return @bitCast(self._reader.readU64(0));
        }

        pub fn whichPerimeter(self: Reader) PerimeterTag {
            const discriminant = self._reader.readUnionDiscriminant(8);
            return @enumFromInt(discriminant);
        }

        pub fn getCircle(self: Reader) f64 {
            return @bitCast(self._reader.readU64(16));
        }

        pub fn getSquare(self: Reader) f64 {
            return @bitCast(self._reader.readU64(16));
        }

        pub fn getRectangle(self: Reader) ![]const u8 {
            return try self._reader.readText(0);
        }
    };

    pub const Builder = struct {
        _builder: message.StructBuilder,

        pub fn init(msg: *message.MessageBuilder) !Builder {
            // Shape has 3 data words (area: f64, discriminant: u16 + padding, union data: f64)
            // and 1 pointer word (for rectangle if needed)
            const builder = try msg.allocateStruct(3, 1);
            return .{ ._builder = builder };
        }

        pub fn setArea(self: *Builder, value: f64) void {
            self._builder.writeU64(0, @bitCast(value));
        }

        pub fn setCircle(self: *Builder, radius: f64) void {
            self._builder.writeUnionDiscriminant(8, @intFromEnum(PerimeterTag.circle));
            self._builder.writeU64(16, @bitCast(radius));
        }

        pub fn setSquare(self: *Builder, side: f64) void {
            self._builder.writeUnionDiscriminant(8, @intFromEnum(PerimeterTag.square));
            self._builder.writeU64(16, @bitCast(side));
        }

        pub fn setRectangle(self: *Builder, description: []const u8) !void {
            self._builder.writeUnionDiscriminant(8, @intFromEnum(PerimeterTag.rectangle));
            try self._builder.writeText(0, description);
        }
    };
};

test "Union: circle variant" {
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var shape_builder = try Shape.Builder.init(&msg_builder);
    shape_builder.setArea(78.54);
    shape_builder.setCircle(5.0);

    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const shape_reader = try Shape.Reader.init(&msg);

    try testing.expectApproxEqAbs(@as(f64, 78.54), shape_reader.getArea(), 0.01);
    try testing.expectEqual(Shape.PerimeterTag.circle, shape_reader.whichPerimeter());
    try testing.expectApproxEqAbs(@as(f64, 5.0), shape_reader.getCircle(), 0.01);
}

test "Union: square variant" {
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var shape_builder = try Shape.Builder.init(&msg_builder);
    shape_builder.setArea(100.0);
    shape_builder.setSquare(10.0);

    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const shape_reader = try Shape.Reader.init(&msg);

    try testing.expectApproxEqAbs(@as(f64, 100.0), shape_reader.getArea(), 0.01);
    try testing.expectEqual(Shape.PerimeterTag.square, shape_reader.whichPerimeter());
    try testing.expectApproxEqAbs(@as(f64, 10.0), shape_reader.getSquare(), 0.01);
}

test "Union: rectangle variant with text" {
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var shape_builder = try Shape.Builder.init(&msg_builder);
    shape_builder.setArea(200.0);
    try shape_builder.setRectangle("10x20 rectangle");

    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const shape_reader = try Shape.Reader.init(&msg);

    try testing.expectApproxEqAbs(@as(f64, 200.0), shape_reader.getArea(), 0.01);
    try testing.expectEqual(Shape.PerimeterTag.rectangle, shape_reader.whichPerimeter());
    try testing.expectEqualStrings("10x20 rectangle", try shape_reader.getRectangle());
}

test "Union: switching between variants" {
    // Test that we can create a shape with one variant, then change it to another
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var shape_builder = try Shape.Builder.init(&msg_builder);
    shape_builder.setArea(50.0);

    // First set it as a circle
    shape_builder.setCircle(3.0);

    // Then change it to a square (this should update the discriminant)
    shape_builder.setSquare(7.07);

    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const shape_reader = try Shape.Reader.init(&msg);

    // Should read as square, not circle
    try testing.expectEqual(Shape.PerimeterTag.square, shape_reader.whichPerimeter());
    try testing.expectApproxEqAbs(@as(f64, 7.07), shape_reader.getSquare(), 0.01);
}

// Test a simpler union with just primitive types
const Status = struct {
    pub const Tag = enum(u16) {
        idle = 0,
        running = 1,
        error_code = 2,
    };

    pub const Reader = struct {
        _reader: message.StructReader,

        pub fn init(msg: *const message.Message) !Reader {
            const root = try msg.getRootStruct();
            return .{ ._reader = root };
        }

        pub fn which(self: Reader) Tag {
            return @enumFromInt(self._reader.readUnionDiscriminant(0));
        }

        pub fn getRunning(self: Reader) u32 {
            return self._reader.readU32(4);
        }

        pub fn getErrorCode(self: Reader) u32 {
            return self._reader.readU32(4);
        }
    };

    pub const Builder = struct {
        _builder: message.StructBuilder,

        pub fn init(msg: *message.MessageBuilder) !Builder {
            const builder = try msg.allocateStruct(1, 0);
            return .{ ._builder = builder };
        }

        pub fn setIdle(self: *Builder) void {
            self._builder.writeUnionDiscriminant(0, @intFromEnum(Tag.idle));
        }

        pub fn setRunning(self: *Builder, progress: u32) void {
            self._builder.writeUnionDiscriminant(0, @intFromEnum(Tag.running));
            self._builder.writeU32(4, progress);
        }

        pub fn setErrorCode(self: *Builder, code: u32) void {
            self._builder.writeUnionDiscriminant(0, @intFromEnum(Tag.error_code));
            self._builder.writeU32(4, code);
        }
    };
};

test "Union: simple primitive union - idle" {
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var status_builder = try Status.Builder.init(&msg_builder);
    status_builder.setIdle();

    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const status_reader = try Status.Reader.init(&msg);
    try testing.expectEqual(Status.Tag.idle, status_reader.which());
}

test "Union: simple primitive union - running" {
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var status_builder = try Status.Builder.init(&msg_builder);
    status_builder.setRunning(75);

    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const status_reader = try Status.Reader.init(&msg);
    try testing.expectEqual(Status.Tag.running, status_reader.which());
    try testing.expectEqual(@as(u32, 75), status_reader.getRunning());
}

test "Union: simple primitive union - error" {
    var msg_builder = message.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var status_builder = try Status.Builder.init(&msg_builder);
    status_builder.setErrorCode(404);

    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const status_reader = try Status.Reader.init(&msg);
    try testing.expectEqual(Status.Tag.error_code, status_reader.which());
    try testing.expectEqual(@as(u32, 404), status_reader.getErrorCode());
}
