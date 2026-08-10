const std = @import("std");

/// Generic typed list helpers used by generated Cap'n Proto code.
///
/// These provide thin wrappers around the low-level list reader/builder types,
/// adding type-safe enum conversions and struct wrapping. Generated code
/// references these instead of emitting identical helper implementations
/// in every struct.
pub fn define(
    comptime MessageModule: type,
) type {
    return struct {
        pub fn EnumListReader(comptime EnumType: type) type {
            return struct {
                _list: MessageModule.U16ListReader,

                pub fn len(self: @This()) u32 {
                    return self._list.len();
                }

                pub fn get(self: @This(), index: u32) !EnumType {
                    return std.enums.fromInt(EnumType, try self.getOrdinal(index)) orelse return error.InvalidEnumValue;
                }

                /// Return the logical wire ordinal without requiring it to be a
                /// currently-known member of `EnumType`.
                pub fn getOrdinal(self: @This(), index: u32) !u16 {
                    return try self._list.get(index);
                }

                pub fn raw(self: @This()) MessageModule.U16ListReader {
                    return self._list;
                }
            };
        }

        pub fn EnumListBuilder(comptime EnumType: type) type {
            return struct {
                _list: MessageModule.U16ListBuilder,

                pub fn len(self: @This()) u32 {
                    return self._list.len();
                }

                pub fn set(self: @This(), index: u32, value: EnumType) !void {
                    try self.setOrdinal(index, @backingInt(value));
                }

                /// Store a logical wire ordinal, including values added by a
                /// newer version of the schema.
                pub fn setOrdinal(self: @This(), index: u32, value: u16) !void {
                    try self._list.set(index, value);
                }

                pub fn raw(self: @This()) MessageModule.U16ListBuilder {
                    return self._list;
                }
            };
        }

        pub fn StructListReader(comptime StructType: type) type {
            return struct {
                _list: MessageModule.StructListReader,

                pub fn len(self: @This()) u32 {
                    return self._list.len();
                }

                pub fn get(self: @This(), index: u32) !StructType.Reader {
                    const item = try self._list.get(index);
                    return StructType.Reader.wrap(item);
                }

                pub fn raw(self: @This()) MessageModule.StructListReader {
                    return self._list;
                }
            };
        }

        pub fn StructListBuilder(comptime StructType: type) type {
            return struct {
                _list: MessageModule.StructListBuilder,

                pub fn len(self: @This()) u32 {
                    return self._list.len();
                }

                pub fn get(self: @This(), index: u32) !StructType.Builder {
                    const item = try self._list.get(index);
                    return StructType.Builder.wrap(item);
                }

                pub fn raw(self: @This()) MessageModule.StructListBuilder {
                    return self._list;
                }
            };
        }

        pub const DataListReader = struct {
            _list: MessageModule.PointerListReader,

            pub fn len(self: @This()) u32 {
                return self._list.len();
            }

            pub fn get(self: @This(), index: u32) ![]const u8 {
                return try self._list.getData(index);
            }

            pub fn raw(self: @This()) MessageModule.PointerListReader {
                return self._list;
            }
        };

        pub const DataListBuilder = struct {
            _list: MessageModule.PointerListBuilder,

            pub fn len(self: @This()) u32 {
                return self._list.len();
            }

            pub fn set(self: @This(), index: u32, value: []const u8) !void {
                try self._list.setData(index, value);
            }

            pub fn setNull(self: @This(), index: u32) !void {
                try self._list.setNull(index);
            }

            pub fn raw(self: @This()) MessageModule.PointerListBuilder {
                return self._list;
            }
        };

        pub const CapabilityListReader = struct {
            _list: MessageModule.PointerListReader,

            pub fn len(self: @This()) u32 {
                return self._list.len();
            }

            pub fn get(self: @This(), index: u32) !MessageModule.Capability {
                return try self._list.getCapability(index);
            }

            pub fn raw(self: @This()) MessageModule.PointerListReader {
                return self._list;
            }
        };

        pub const CapabilityListBuilder = struct {
            _list: MessageModule.PointerListBuilder,

            pub fn len(self: @This()) u32 {
                return self._list.len();
            }

            pub fn set(self: @This(), index: u32, cap: MessageModule.Capability) !void {
                try self._list.setCapability(index, cap);
            }

            pub fn setNull(self: @This(), index: u32) !void {
                try self._list.setNull(index);
            }

            pub fn raw(self: @This()) MessageModule.PointerListBuilder {
                return self._list;
            }
        };
    };
}

test "enum list ordinal access preserves unknown values" {
    const FakeMessage = struct {
        pub const U16ListReader = struct {
            values: []const u16,

            pub fn len(self: @This()) u32 {
                return @intCast(self.values.len);
            }

            pub fn get(self: @This(), index: u32) !u16 {
                if (index >= self.values.len) return error.IndexOutOfBounds;
                return self.values[index];
            }
        };

        pub const U16ListBuilder = struct {
            values: []u16,

            pub fn len(self: @This()) u32 {
                return @intCast(self.values.len);
            }

            pub fn set(self: @This(), index: u32, value: u16) !void {
                if (index >= self.values.len) return error.IndexOutOfBounds;
                self.values[index] = value;
            }
        };

        // The remaining types are referenced only by lazy declarations in the
        // generic helper namespace and are not instantiated by this test.
        pub const StructListReader = struct {};
        pub const StructListBuilder = struct {};
        pub const PointerListReader = struct {};
        pub const PointerListBuilder = struct {};
        pub const Capability = struct { id: u32 };
    };
    const State = enum(u16) { ready = 1 };
    const Helpers = define(FakeMessage);

    const source = [_]u16{ 1, 77 };
    const reader = Helpers.EnumListReader(State){ ._list = .{ .values = &source } };
    try std.testing.expectEqual(State.ready, try reader.get(0));
    try std.testing.expectEqual(@as(u16, 77), try reader.getOrdinal(1));
    try std.testing.expectError(error.InvalidEnumValue, reader.get(1));

    var destination = [_]u16{ 0, 0 };
    const builder = Helpers.EnumListBuilder(State){ ._list = .{ .values = &destination } };
    try builder.set(0, .ready);
    try builder.setOrdinal(1, 77);
    try std.testing.expectEqualSlices(u16, &source, &destination);
}
