//! The header gate of the native C ABI: `include/capnp_core.h` against
//! `abi.zig`, through translate-c (`capnp_core_h`) and as text
//! (`capnp_core_h_text`). Moved out of abi.zig's own tests (capnp-swift
//! handoff H7), so the shipped abi.zig imports no test-only module.
//!
//!   - every `pub export fn capnp_*` in abi.zig has a prototype in the header
//!     with the same calling convention, arity, scalar widths and struct
//!     layouts, and every `capnp_*(` prototype in the header is exported;
//!   - `CAPNP_CORE_ABI_VERSION` equals `abi.abi_version`;
//!   - the header's `CAPNP_*` constants equal the Zig values.
//!
//! Collected by the `capnpc-zig-core` test root (src/lib_core.zig), whose
//! build module carries both header imports (build/native.zig).

const std = @import("std");
const abi = @import("abi.zig");
const effects = @import("effects.zig");

const testing = std.testing;

test "capnp_core.h constants match the Zig values" {
    const c = @import("capnp_core_h");
    try testing.expectEqual(abi.CAPNP_OK, @as(i32, c.CAPNP_OK));
    try testing.expectEqual(abi.CAPNP_E_INVAL, @as(i32, c.CAPNP_E_INVAL));
    try testing.expectEqual(abi.CAPNP_E_BAD_ID, @as(i32, c.CAPNP_E_BAD_ID));
    try testing.expectEqual(abi.CAPNP_E_BUSY, @as(i32, c.CAPNP_E_BUSY));
    try testing.expectEqual(abi.CAPNP_E_CLOSED, @as(i32, c.CAPNP_E_CLOSED));
    try testing.expectEqual(abi.CAPNP_E_LIMIT, @as(i32, c.CAPNP_E_LIMIT));
    try testing.expectEqual(abi.CAPNP_E_PROTOCOL, @as(i32, c.CAPNP_E_PROTOCOL));
    try testing.expectEqual(abi.CAPNP_E_NOMEM, @as(i32, c.CAPNP_E_NOMEM));
    try testing.expectEqual(abi.CAPNP_E_INTERNAL, @as(i32, c.CAPNP_E_INTERNAL));
    try testing.expectEqual(abi.CAPNP_FRAMING_SEGMENT_TABLE, @as(u8, c.CAPNP_FRAMING_SEGMENT_TABLE));
    try testing.expectEqual(abi.CAPNP_FRAMING_U32_LE, @as(u8, c.CAPNP_FRAMING_U32_LE));
    // Cap kinds, effect kinds and return kinds are the ordinals effects.zig uses.
    try testing.expectEqual(@backingInt(effects.CapKind.none), @as(u8, c.CAPNP_CAP_NONE));
    try testing.expectEqual(@backingInt(effects.CapKind.import), @as(u8, c.CAPNP_CAP_IMPORT));
    try testing.expectEqual(@backingInt(effects.CapKind.@"export"), @as(u8, c.CAPNP_CAP_EXPORT));
    try testing.expectEqual(@backingInt(effects.CapKind.promised), @as(u8, c.CAPNP_CAP_PROMISED));
    try testing.expectEqual(@backingInt(effects.Kind.out_frame), @as(u8, c.CAPNP_EFFECT_OUT_FRAME));
    try testing.expectEqual(@backingInt(effects.Kind.close_requested), @as(u8, c.CAPNP_EFFECT_CLOSE_REQUESTED));
    try testing.expectEqual(@backingInt(effects.Kind.@"return"), @as(u8, c.CAPNP_EFFECT_RETURN));
    try testing.expectEqual(@backingInt(effects.Kind.inbound_call), @as(u8, c.CAPNP_EFFECT_INBOUND_CALL));
    try testing.expectEqual(@backingInt(effects.Kind.export_dropped), @as(u8, c.CAPNP_EFFECT_EXPORT_DROPPED));
    try testing.expectEqual(@backingInt(effects.Kind.event), @as(u8, c.CAPNP_EFFECT_EVENT));
    try testing.expectEqual(@backingInt(effects.ReturnKind.results), @as(u8, c.CAPNP_RETURN_RESULTS));
    try testing.expectEqual(@backingInt(effects.ReturnKind.exception), @as(u8, c.CAPNP_RETURN_EXCEPTION));
    try testing.expectEqual(@backingInt(effects.ReturnKind.canceled), @as(u8, c.CAPNP_RETURN_CANCELED));
    try testing.expectEqual(@backingInt(effects.ReturnKind.disconnected), @as(u8, c.CAPNP_RETURN_DISCONNECTED));
    // The two shared structs have the header's layout (the drift test below
    // checks them again through every prototype that names them).
    try testing.expectEqual(@sizeOf(c.capnp_cap), @sizeOf(abi.capnp_cap));
    try testing.expectEqual(@sizeOf(c.capnp_effect), @sizeOf(abi.capnp_effect));
    try testing.expectEqual(@sizeOf(c.capnp_conn_opts), @sizeOf(abi.capnp_conn_opts));
}

// Header-drift gate: every `pub export fn capnp_*` in abi.zig has a prototype
// in `capnp_core.h` with the same arity and scalar widths (via translate-c),
// and every function the header declares is exported there. The header's
// `CAPNP_CORE_ABI_VERSION` must equal `abi_version`.
test "capnp_core.h matches the Zig exports" {
    const c = @import("capnp_core_h");
    const this = abi;

    try testing.expectEqual(abi.abi_version, @as(u32, c.CAPNP_CORE_ABI_VERSION));

    var zig_names: [256][]const u8 = undefined;
    var n_zig: usize = 0;
    inline for (@typeInfo(this).@"struct".decl_names) |name| {
        if (comptime !std.mem.startsWith(u8, name, "capnp_")) continue;
        const ZigFn = @TypeOf(@field(this, name));
        if (@typeInfo(ZigFn) != .@"fn") continue;
        if (!@hasDecl(c, name)) {
            std.debug.print("export {s} has no prototype in capnp_core.h\n", .{name});
            return error.HeaderMissingPrototype;
        }
        try expectSameCShape(name, ZigFn, @TypeOf(@field(c, name)));
        if (n_zig == zig_names.len) return error.TooManyExportsForDriftTest;
        zig_names[n_zig] = name;
        n_zig += 1;
    }

    // Reverse direction: scan the header text for `capnp_xxx(` prototypes.
    const header = @embedFile("capnp_core_h_text");
    var it = HeaderFnNames{ .src = header };
    var n_header: usize = 0;
    while (it.next()) |name| {
        n_header += 1;
        for (zig_names[0..n_zig]) |zn| {
            if (std.mem.eql(u8, zn, name)) break;
        } else {
            std.debug.print("capnp_core.h declares {s} but abi.zig does not export it\n", .{name});
            return error.HeaderExtraPrototype;
        }
    }
    try testing.expectEqual(n_zig, n_header);
}

fn expectSameCShape(name: []const u8, comptime ZigFn: type, comptime CFn: type) !void {
    const z = @typeInfo(ZigFn).@"fn";
    const h = @typeInfo(CFn).@"fn";
    if (!std.meta.eql(z.attrs.@"callconv", h.attrs.@"callconv")) {
        std.debug.print("{s}: calling convention differs from capnp_core.h\n", .{name});
        return error.HeaderCallconvMismatch;
    }
    if (z.param_types.len != h.param_types.len) {
        std.debug.print("{s}: {d} params in Zig, {d} in capnp_core.h\n", .{ name, z.param_types.len, h.param_types.len });
        return error.HeaderArityMismatch;
    }
    inline for (z.param_types, h.param_types, 0..) |zp, hp, i| {
        if (comptime !sameCShape(zp.?, hp.?, 0)) {
            std.debug.print("{s}: param {d} is {s} in Zig, {s} in capnp_core.h\n", .{ name, i, @typeName(zp.?), @typeName(hp.?) });
            return error.HeaderParamMismatch;
        }
    }
    if (comptime !sameCShape(z.return_type.?, h.return_type.?, 0)) {
        std.debug.print("{s}: returns {s} in Zig, {s} in capnp_core.h\n", .{ name, @typeName(z.return_type.?), @typeName(h.return_type.?) });
        return error.HeaderReturnMismatch;
    }
}

/// C-compatibility of a Zig type `Z` and its translate-c twin `H`: ints agree
/// on size and signedness; pointers agree on what they point at (function
/// pointers by signature, structs by layout, opaque handles by opaqueness);
/// optional pointers match plain ones (C has no non-null pointers); structs
/// agree field by field and offset by offset; `noreturn` matches `void`.
fn sameCShape(comptime Z: type, comptime H: type, comptime depth: u8) bool {
    if (depth > 6) return true; // self-referential layouts: stop descending
    if (Z == noreturn) return H == void or H == noreturn;
    if (Z == void or H == void) return Z == H;
    if (comptime pointee(Z)) |zc| {
        const hc = comptime pointee(H) orelse return false;
        return samePointee(zc, hc, depth + 1);
    }
    if (comptime pointee(H) != null) return false;
    const zi = @typeInfo(Z);
    const hi = @typeInfo(H);
    return switch (zi) {
        .int => |a| hi == .int and hi.int.bits == a.bits and hi.int.signedness == a.signedness,
        .@"enum" => |e| sameCShape(e.tag_type, H, depth + 1),
        .bool => (hi == .bool or hi == .int) and @sizeOf(Z) == @sizeOf(H),
        .float => |f| hi == .float and hi.float.bits == f.bits,
        .@"struct" => sameStruct(Z, H, depth + 1),
        else => Z == H,
    };
}

fn pointee(comptime T: type) ?type {
    return switch (@typeInfo(T)) {
        .pointer => |p| p.child,
        .optional => |o| switch (@typeInfo(o.child)) {
            .pointer => |p| p.child,
            else => null,
        },
        else => null,
    };
}

fn samePointee(comptime Z: type, comptime H: type, comptime depth: u8) bool {
    const zi = @typeInfo(Z);
    const hi = @typeInfo(H);
    if (zi == .@"fn" or hi == .@"fn") {
        if (zi != .@"fn" or hi != .@"fn") return false;
        const z = zi.@"fn";
        const h = hi.@"fn";
        if (!std.meta.eql(z.attrs.@"callconv", h.attrs.@"callconv")) return false;
        if (z.param_types.len != h.param_types.len) return false;
        for (z.param_types, h.param_types) |zp, hp| {
            if (!sameCShape(zp.?, hp.?, depth + 1)) return false;
        }
        return sameCShape(z.return_type.?, h.return_type.?, depth + 1);
    }
    if (zi == .@"opaque" or hi == .@"opaque") return zi == .@"opaque" and hi == .@"opaque";
    return sameCShape(Z, H, depth + 1);
}

fn sameStruct(comptime Z: type, comptime H: type, comptime depth: u8) bool {
    if (@typeInfo(H) != .@"struct") return false;
    if (@sizeOf(Z) != @sizeOf(H) or @alignOf(Z) != @alignOf(H)) return false;
    const z = @typeInfo(Z).@"struct";
    const h = @typeInfo(H).@"struct";
    if (z.field_names.len != h.field_names.len) return false;
    for (z.field_names, h.field_names, z.field_types, h.field_types) |zn, hn, zt, ht| {
        if (@offsetOf(Z, zn) != @offsetOf(H, hn)) return false;
        if (!sameCShape(zt, ht, depth + 1)) return false;
    }
    return true;
}

/// Yields `capnp_*` identifiers that are immediately followed by `(` in the
/// header, skipping comments. Function-pointer typedefs (`(*capnp_x)(`) and
/// macros are not matched.
const HeaderFnNames = struct {
    src: []const u8,
    i: usize = 0,

    fn next(self: *HeaderFnNames) ?[]const u8 {
        const s = self.src;
        while (self.i < s.len) {
            if (std.mem.startsWith(u8, s[self.i..], "/*")) {
                const end = std.mem.indexOfPos(u8, s, self.i + 2, "*/") orelse s.len;
                self.i = @min(end + 2, s.len);
                continue;
            }
            if (std.mem.startsWith(u8, s[self.i..], "//")) {
                self.i = std.mem.indexOfScalarPos(u8, s, self.i, '\n') orelse s.len;
                continue;
            }
            const prev_is_ident = self.i > 0 and isIdent(s[self.i - 1]);
            if (!prev_is_ident and std.mem.startsWith(u8, s[self.i..], "capnp_")) {
                const start = self.i;
                while (self.i < s.len and isIdent(s[self.i])) self.i += 1;
                var j = self.i;
                while (j < s.len and (s[j] == ' ' or s[j] == '\t')) j += 1;
                if (j < s.len and s[j] == '(') return s[start..self.i];
                continue;
            }
            self.i += 1;
        }
        return null;
    }

    fn isIdent(ch: u8) bool {
        return std.ascii.isAlphanumeric(ch) or ch == '_';
    }
};
