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
                    return std.meta.intToEnum(EnumType, try self._list.get(index)) catch return error.InvalidEnumValue;
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
                    try self._list.set(index, @intFromEnum(value));
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
