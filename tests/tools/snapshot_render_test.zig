//! Tests for tools/snapshot_render.zig, the walker and renderer that the
//! snapshot gates share.
//!
//! The module must stand alone (it never imports capnpc-zig), so these tests
//! walk small fixture namespaces declared here, and this root imports only
//! `snapshot-render`.

const std = @import("std");
const render = @import("snapshot-render");
const testing = std.testing;

/// One of each declaration kind the walker renders.
const fx = struct {
    pub const Kind = enum(u8) { alpha = 1, beta = 2 };
    pub const Open = enum(u8) { known = 0, _ };
    pub const Options = struct {
        depth: u32 = 4,
        label: ?u16 = 7,
        kind: Kind = .beta,
    };
    pub const Shape = union(enum) { circle: f32, none: void };

    pub const interface_id: u64 = 0xc8cb212fcd9f5691;
    pub const ordinal: u16 = 3;
    pub const is_streaming = false;
    pub const kind: Kind = .beta;
    pub const unnamed: Open = @fromBackingInt(9);
    pub const literal = .gamma;
    pub const ratio: f32 = 0.5;
    pub const maybe: ?u32 = null;
    pub const some: ?u32 = 12;
    pub const label = "persist\"ent";
    pub const slice_label: []const u8 = "abc";
    pub const bytes: [3]u8 = .{ 'x', 'y', 'z' };
    pub const edge_label: [render.max_const_string_bytes]u8 = @splat('y');
    pub const long_label: [render.max_const_string_bytes + 1]u8 = @splat('x');
    pub const defaults: Options = .{};
    pub const Handler = *const fn (u32) error{ Zeta, Alpha }!void;
    // `@typeName` already sorts an explicit error set, so only a typedef of a
    // fn with an INFERRED set shows that `renderTypedef` expands the set.
    pub const InferredFn = @TypeOf(inferred);
    pub const InferredHandler = *const InferredFn;
    pub const std_root = std;
    pub const builtin_root = @import("builtin");

    pub fn inferred(x: u8) !void {
        if (x == 0) return error.Zeta;
        if (x == 1) return error.Alpha;
    }

    pub const Api = struct {
        pub fn open(options: Options) error{ Closed, Busy }!void {
            _ = options;
        }
        pub fn peek() u32 {
            return 0;
        }
        pub const Inner = struct {
            pub fn deep() void {}
        };
    };

    pub const Hidden = struct {
        pub fn secret() void {}
    };
};

const types_only = render.Snapshot(.{ .root = fx, .root_path = "fx", .max_depth = 4 });
const with_values = render.Snapshot(.{ .root = fx, .root_path = "fx", .max_depth = 4, .render_const_values = true });

fn hasLine(entries: []const render.Entry, line: []const u8) bool {
    for (entries) |entry| {
        if (std.mem.eql(u8, entry.line, line)) return true;
    }
    return false;
}

fn hasPathPrefix(entries: []const render.Entry, path_prefix: []const u8) bool {
    for (entries) |entry| {
        if (std.mem.startsWith(u8, entry.path, path_prefix)) return true;
    }
    return false;
}

fn expectLine(entries: []const render.Entry, line: []const u8) !void {
    if (hasLine(entries, line)) return;
    std.debug.print("missing line: {s}\n", .{line});
    return error.TestExpectedLine;
}

fn expectNoLine(entries: []const render.Entry, line: []const u8) !void {
    if (!hasLine(entries, line)) return;
    std.debug.print("unexpected line: {s}\n", .{line});
    return error.TestUnexpectedLine;
}

fn containsLine(lines: []const []const u8, line: []const u8) bool {
    for (lines) |candidate| {
        if (std.mem.eql(u8, candidate, line)) return true;
    }
    return false;
}

test "render_const_values off: a const renders its type only" {
    const e = types_only.entries;
    try expectLine(e, "fx.ordinal: const u16");
    try expectLine(e, "fx.interface_id: const u64");
    try expectLine(e, "fx.is_streaming: const bool");
    try expectLine(e, "fx.label: const *const [11:0]u8");
    try expectLine(e, "fx.kind: const " ++ @typeName(fx.Kind));
    for (e) |entry| {
        if (std.mem.indexOf(u8, entry.line, ": const ") == null) continue;
        if (std.mem.indexOf(u8, entry.line, " = ") != null) {
            std.debug.print("const line renders a value: {s}\n", .{entry.line});
            return error.TestUnexpectedValue;
        }
    }
}

test "render_const_values on: scalar consts render their values" {
    const e = with_values.entries;
    try expectLine(e, "fx.interface_id: const u64 = 14468694717054801553");
    try expectLine(e, "fx.ordinal: const u16 = 3");
    try expectLine(e, "fx.is_streaming: const bool = false");
    try expectLine(e, "fx.kind: const " ++ @typeName(fx.Kind) ++ " = .beta");
    try expectLine(e, "fx.unnamed: const " ++ @typeName(fx.Open) ++ " = @fromBackingInt(9)");
    try expectLine(e, "fx.literal: const " ++ @typeName(@TypeOf(fx.literal)) ++ " = .gamma");
    try expectLine(e, "fx.ratio: const f32 = 0.5");
    try expectLine(e, "fx.maybe: const ?u32 = null");
    try expectLine(e, "fx.some: const ?u32 = 12");
    try expectLine(e, "fx.label: const *const [11:0]u8 = \"persist\\\"ent\"");
    try expectLine(e, "fx.slice_label: const []const u8 = \"abc\"");
    try expectLine(e, "fx.bytes: const [3]u8 = \"xyz\"");
    try expectLine(e, "fx.edge_label: const [64]u8 = \"" ++ &fx.edge_label ++ "\"");
    // Past the string limit, and for aggregates, only the type renders.
    try expectLine(e, "fx.long_label: const [65]u8");
    try expectLine(e, "fx.defaults: const " ++ @typeName(fx.Options));
    // Values change const lines only.
    try expectLine(e, "fx.Options.depth: field u32 = 4");
    try testing.expectEqual(types_only.entries.len, with_values.entries.len);
}

test "the walker renders containers, fields, variants, enumerants, typedefs and error sets" {
    const e = types_only.entries;
    try expectLine(e, "fx.Options: struct");
    try expectLine(e, "fx.Options.depth: field u32 = 4");
    try expectLine(e, "fx.Options.label: field ?u16 = 7");
    try expectLine(e, "fx.Options.kind: field " ++ @typeName(fx.Kind) ++ " = .beta");
    try expectLine(e, "fx.Shape: union");
    try expectLine(e, "fx.Shape.circle: variant f32");
    try expectLine(e, "fx.Shape.none: variant void");
    try expectLine(e, "fx.Kind: enum");
    try expectLine(e, "fx.Kind.alpha: enumerant = 1");
    try expectLine(e, "fx.Kind.beta: enumerant = 2");
    // Error sets are expanded and sorted, for fns, fn typedefs and
    // fn-pointer typedefs.
    try expectLine(e, "fx.Handler: type = *const fn (u32) error{Alpha,Zeta}!void");
    try expectLine(e, "fx.inferred: fn (u8) error{Alpha,Zeta}!void");
    try expectLine(e, "fx.InferredFn: type = fn (u8) error{Alpha,Zeta}!void");
    try expectLine(e, "fx.InferredHandler: type = *const fn (u8) error{Alpha,Zeta}!void");
    try expectLine(e, "fx.Api.open: fn (" ++ @typeName(fx.Options) ++ ") error{Busy,Closed}!void");
    try expectLine(e, "fx.Api.peek: fn () u32");
}

test "the default descend skips the std and builtin roots" {
    try expectLine(types_only.entries, "fx.std_root: struct");
    try expectLine(types_only.entries, "fx.builtin_root: struct");
    try testing.expect(!hasPathPrefix(types_only.entries, "fx.std_root."));
    try testing.expect(!hasPathPrefix(types_only.entries, "fx.builtin_root."));
    // Zig 0.17 names std's own files without the `std.` prefix (`mem`,
    // `mem.Allocator`), so `foreignType` cannot recognize a nested std type
    // by name. Pinned here so the limit documented in the module stays true.
    try testing.expectEqualStrings("mem.Allocator", @typeName(std.mem.Allocator));
    try testing.expect(!render.foreignType(std.mem.Allocator));
}

fn skipHidden(comptime T: type) bool {
    return T != fx.Hidden and render.descendUnlessForeign(T);
}

test "descend decides which containers are walked" {
    const skipping = render.Snapshot(.{ .root = fx, .root_path = "fx", .max_depth = 4, .descend = skipHidden });
    try expectLine(skipping.entries, "fx.Hidden: struct");
    try expectNoLine(skipping.entries, "fx.Hidden.secret: fn () void");
    try expectLine(types_only.entries, "fx.Hidden.secret: fn () void");
}

test "max_depth stops the walk below the limit" {
    const one = render.Snapshot(.{ .root = fx, .root_path = "fx", .max_depth = 1 });
    // A container at the limit keeps its own line and its fields...
    try expectLine(one.entries, "fx.Api: struct");
    try expectLine(one.entries, "fx.Options.depth: field u32 = 4");
    // ...but its declarations are not walked.
    try testing.expect(!hasPathPrefix(one.entries, "fx.Api."));

    const two = render.Snapshot(.{ .root = fx, .root_path = "fx", .max_depth = 2 });
    try expectLine(two.entries, "fx.Api.Inner: struct");
    try testing.expect(!hasPathPrefix(two.entries, "fx.Api.Inner."));
    try expectLine(types_only.entries, "fx.Api.Inner.deep: fn () void");
}

const tiered = render.Snapshot(.{
    .root = fx,
    .root_path = "fx",
    .max_depth = 4,
    .stable_rules = &.{ render.prefix("fx.Api"), render.exact("fx.ordinal") },
    .experimental_overrides = &.{render.exact("fx.Api.peek")},
});

test "tiers: Stable only by rule, and an override wins over a Stable prefix" {
    try testing.expect(comptime tiered.tierIsStable("fx.Api"));
    try testing.expect(comptime tiered.tierIsStable("fx.Api.Inner.deep"));
    try testing.expect(comptime tiered.tierIsStable("fx.ordinal"));
    // An exact rule does not sweep in members; a prefix stops at a segment.
    try testing.expect(comptime !tiered.tierIsStable("fx.ordinal.x"));
    try testing.expect(comptime !tiered.tierIsStable("fx.Apix"));
    try testing.expect(comptime !tiered.tierIsStable("fx.Api.peek"));
    try testing.expect(comptime !tiered.tierIsStable("fx.kind"));

    try testing.expect(containsLine(tiered.stable_lines, "fx.Api: struct"));
    try testing.expect(containsLine(tiered.stable_lines, "fx.ordinal: const u16"));
    try testing.expect(!containsLine(tiered.stable_lines, "fx.Api.peek: fn () u32"));
    try testing.expect(containsLine(tiered.experimental_lines, "fx.Api.peek: fn () u32"));
    try testing.expectEqual(
        tiered.entries.len,
        tiered.stable_lines.len + tiered.experimental_lines.len,
    );
    try testing.expectEqualStrings("", tiered.dead_rules);
}

/// The closure check's fixture: one declaration per branch of the check.
/// Under `closure` below, `cx.Api` is Stable by prefix and two `cx.Hidden`
/// methods are Stable by exact rule. `Config`, `Handle` and `Hidden` itself
/// are Experimental. The two shared types sit at a Stable and an
/// Experimental path each, in opposite walk orders.
const cx = struct {
    /// Reached first at an Experimental path, then at `cx.Api.EarlyAlias`.
    pub const SharedEarly = struct {};
    pub const Config = struct {};
    pub const Handle = struct {};

    pub const Api = struct {
        pub const EarlyAlias = SharedEarly;
        /// Reached first at this Stable path, then at `cx.LateAlias`.
        pub const SharedLate = struct {};

        /// Violation: an Experimental parameter.
        pub fn open(config: Config) void {
            _ = config;
        }
        /// Violation: an Experimental return type, behind `!` and `*`.
        pub fn handle() error{Closed}!*Handle {
            return error.Closed;
        }
        /// Generic: skipped, although its concrete parameter and return
        /// type are Experimental.
        pub fn write(writer: anytype, config: Config) Config {
            _ = writer;
            return config;
        }
        /// Both types are Stable, because a Stable path reaches each.
        pub fn share(early: SharedEarly, late: SharedLate) void {
            _ = early;
            _ = late;
        }
    };

    pub const Hidden = struct {
        /// Stable (exact rule): its own Experimental type is exempt.
        pub fn secret(self: *Hidden) void {
            _ = self;
        }
        /// Stable (exact rule): its own Experimental type is exempt.
        pub fn init() Hidden {
            return .{};
        }
        /// Experimental: not checked.
        pub fn tweak(config: Config) Handle {
            _ = config;
            return .{};
        }
    };

    pub const LateAlias = Api.SharedLate;
};

const closure = render.Snapshot(.{
    .root = cx,
    .root_path = "cx",
    .max_depth = 4,
    .stable_rules = &.{
        render.prefix("cx.Api"),
        render.exact("cx.Hidden.secret"),
        render.exact("cx.Hidden.init"),
    },
});

/// Takes the violations as a runtime slice, so a miss fails here as a test
/// failure rather than as a compile error on a comptime index.
fn hasViolation(violations: []const render.Violation, decl: []const u8, offender: []const u8, role: []const u8) bool {
    for (violations) |v| {
        if (std.mem.eql(u8, v.decl, decl) and
            std.mem.eql(u8, v.offender, offender) and
            std.mem.eql(u8, v.role, role)) return true;
    }
    return false;
}

fn expectViolation(violations: []const render.Violation, decl: []const u8, offender: []const u8, role: []const u8) !void {
    if (hasViolation(violations, decl, offender, role)) return;
    std.debug.print("missing violation: {s} names {s} ({s})\n", .{ decl, offender, role });
    return error.TestExpectedViolation;
}

fn expectNoViolation(violations: []const render.Violation, decl: []const u8) !void {
    for (violations) |v| {
        if (!std.mem.eql(u8, v.decl, decl)) continue;
        std.debug.print("unexpected violation: {s} names {s} ({s})\n", .{ v.decl, v.offender, v.role });
        return error.TestUnexpectedViolation;
    }
}

test "closure fixture: the tiers the closure tests rely on" {
    try testing.expectEqualStrings("", closure.dead_rules);
    try testing.expect(comptime closure.tierIsStable("cx.Hidden.secret"));
    try testing.expect(comptime closure.tierIsStable("cx.Hidden.init"));
    try testing.expect(comptime !closure.tierIsStable("cx.Hidden.tweak"));
    try testing.expectEqual(@as(?bool, true), comptime closure.tierOfType(cx.Api));
    try testing.expectEqual(@as(?bool, false), comptime closure.tierOfType(cx.Hidden));
    try testing.expectEqual(@as(?bool, false), comptime closure.tierOfType(cx.Config));
    try testing.expectEqual(@as(?bool, false), comptime closure.tierOfType(*const cx.Handle));
    try testing.expectEqual(@as(?bool, null), comptime closure.tierOfType(u32));
}

test "closure: a Stable fn that takes an Experimental type is a violation" {
    try expectViolation(closure.closure_violations, "cx.Api.open", @typeName(cx.Config), "parameter");
}

test "closure: a Stable fn that returns an Experimental type is a violation" {
    try expectViolation(closure.closure_violations, "cx.Api.handle", @typeName(cx.Handle), "return");
}

test "closure: a method that takes or returns its own enclosing type is exempt" {
    try expectNoViolation(closure.closure_violations, "cx.Hidden.secret");
    try expectNoViolation(closure.closure_violations, "cx.Hidden.init");
}

test "closure: an Experimental fn is not checked" {
    try expectNoViolation(closure.closure_violations, "cx.Hidden.tweak");
}

test "closure: a generic fn is skipped" {
    try expectNoViolation(closure.closure_violations, "cx.Api.write");
}

test "closure: any Stable path that reaches a type makes it Stable" {
    try testing.expectEqual(@as(?bool, true), comptime closure.tierOfType(cx.SharedEarly));
    try testing.expectEqual(@as(?bool, true), comptime closure.tierOfType(cx.Api.SharedLate));
    try expectNoViolation(closure.closure_violations, "cx.Api.share");
}

test "closure: exactly the expected violations, and none once their types are Stable" {
    try testing.expectEqual(@as(usize, 2), closure.closure_violations.len);

    const closed = render.Snapshot(.{
        .root = cx,
        .root_path = "cx",
        .max_depth = 4,
        .stable_rules = &.{
            render.prefix("cx.Api"),
            render.exact("cx.Hidden.secret"),
            render.exact("cx.Hidden.init"),
            render.prefix("cx.Config"),
            render.prefix("cx.Handle"),
        },
    });
    try testing.expectEqual(@as(usize, 0), closed.closure_violations.len);
}

test "dead rules are listed in rule order" {
    const dead = render.Snapshot(.{
        .root = fx,
        .root_path = "fx",
        .max_depth = 4,
        .stable_rules = &.{ render.exact("fx.ordinal"), render.exact("fx.nope") },
        .experimental_overrides = &.{render.prefix("fx.gone")},
    });
    try testing.expectEqualStrings("\n  stable_rules: fx.nope\n  experimental_overrides: fx.gone", dead.dead_rules);
}

test "normalizeLine collapses anonymous-type counters and platform spellings" {
    const gpa = testing.allocator;
    const cases = [_]struct { in: []const u8, out: []const u8 }{
        .{ .in = "a.b__struct_4242.c: field u8", .out = "a.b__struct_*.c: field u8" },
        .{ .in = "x: field os.linux.sockaddr.in", .out = "x: field posix.sockaddr.in" },
        .{ .in = "x: field c.sockaddr__struct_77", .out = "x: field posix.sockaddr" },
        .{ .in = "x: field u8", .out = "x: field u8" },
    };
    for (cases) |case| {
        const got = try render.normalizeLine(gpa, case.in);
        defer gpa.free(got);
        try testing.expectEqualStrings(case.out, got);
    }
}

test "renderSnapshot sorts normalized lines under the header" {
    const gpa = testing.allocator;
    const text = try render.renderSnapshot(gpa, &.{ "b: const u8", "a__struct_9: struct" }, "# h\n");
    defer gpa.free(text);
    try testing.expectEqualStrings("# h\na__struct_*: struct\nb: const u8\n", text);
}
