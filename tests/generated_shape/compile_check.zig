//! Analysis-only compile of one generated-shape corpus entry
//! (build/generated_shape.zig). The shape walker reads signatures only, so a
//! function body that does not compile would pass it. This root takes the
//! address of every non-generic function the entry declares, which makes the
//! compiler analyze each body. The build names the step for the entry, so a
//! failure says which one.

const std = @import("std");
const entry = @import("entry");

/// The file-level names of the entry (`kvstore`, ...). A generated type's
/// `@typeName` starts with one of them.
const stems: []const []const u8 = blk: {
    var out: []const []const u8 = &.{};
    for (std.meta.declarations(entry)) |name| out = out ++ [_][]const u8{name};
    break :blk out;
};

/// A type this check walks into: one the entry declares, or a generic
/// instance (`Apply(...)`, `generic.Method(...)`), whose bodies exist only
/// once instantiated. Other runtime and std types are not walked.
fn ownType(comptime T: type) bool {
    const name = @typeName(T);
    if (std.mem.indexOfScalar(u8, name, '(') != null) return true;
    for (stems) |stem| {
        if (std.mem.startsWith(u8, name, stem ++ ".") or std.mem.eql(u8, name, stem)) return true;
    }
    return false;
}

fn isContainer(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .@"struct", .@"enum", .@"union", .@"opaque" => true,
        else => false,
    };
}

fn contains(comptime seen: []const type, comptime T: type) bool {
    for (seen) |S| if (S == T) return true;
    return false;
}

fn analyze(comptime T: type, comptime seen: *[]const type) void {
    if (contains(seen.*, T)) return;
    seen.* = seen.* ++ [_]type{T};
    for (std.meta.declarations(T)) |name| {
        const D = @field(T, name);
        const DType = @TypeOf(D);
        if (DType == type) {
            if (isContainer(D) and ownType(D)) analyze(D, seen);
        } else if (@typeInfo(DType) == .@"fn" and !@typeInfo(DType).@"fn".is_generic) {
            _ = &@field(T, name);
        }
    }
}

comptime {
    @setEvalBranchQuota(10_000_000);
    var seen: []const type = &.{};
    for (stems) |stem| analyze(@field(entry, stem), &seen);
}
