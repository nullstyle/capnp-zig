//! In-process `zig fmt --check` for generated source.
const std = @import("std");

/// The first line where `source` differs from what `zig fmt` renders.
pub const Mismatch = struct {
    /// 1-based line number in `source`.
    line: usize,
    generated: []const u8,
    formatted: []const u8,
    rendered: []u8,

    pub fn deinit(self: Mismatch, allocator: std.mem.Allocator) void {
        allocator.free(self.rendered);
    }
};

/// Returns null when `source` is exactly what `zig fmt` renders for it, and
/// `error.GeneratedSourceDoesNotParse` when it is not valid Zig. A returned
/// `Mismatch` borrows `source` and must be freed with `deinit`.
pub fn fmtMismatch(allocator: std.mem.Allocator, source: []const u8) !?Mismatch {
    const terminated = try allocator.dupeSentinel(u8, source, 0);
    defer allocator.free(terminated);
    var tree = try std.zig.Ast.parse(allocator, terminated, .{});
    defer tree.deinit(allocator);
    if (tree.errors.len != 0) return error.GeneratedSourceDoesNotParse;
    const rendered = try tree.renderAlloc(allocator);
    if (std.mem.eql(u8, rendered, source)) {
        allocator.free(rendered);
        return null;
    }

    var line: usize = 1;
    var start: usize = 0;
    var index: usize = 0;
    while (index < rendered.len and index < source.len and rendered[index] == source[index]) : (index += 1) {
        if (source[index] == '\n') {
            line += 1;
            start = index + 1;
        }
    }
    const generated_end = std.mem.indexOfScalarPos(u8, source, index, '\n') orelse source.len;
    const formatted_end = std.mem.indexOfScalarPos(u8, rendered, index, '\n') orelse rendered.len;
    return .{
        .line = line,
        .generated = source[start..generated_end],
        .formatted = rendered[start..formatted_end],
        .rendered = rendered,
    };
}

/// Fails unless `source` is exactly what `zig fmt` renders for it. capnpc-zig
/// output is committed and consumed as the plugin writes it, so the generator
/// itself has to emit zig fmt's layout. `name` labels the failure.
pub fn expectFmtClean(allocator: std.mem.Allocator, name: []const u8, source: []const u8) !void {
    const mismatch = fmtMismatch(allocator, source) catch |err| {
        if (err == error.GeneratedSourceDoesNotParse) std.debug.print("{s}: generated source does not parse as Zig\n", .{name});
        return err;
    } orelse return;
    defer mismatch.deinit(allocator);
    std.debug.print(
        "{s}:{d}: generated source is not zig fmt clean\n  generated: {s}\n  zig fmt:   {s}\n",
        .{ name, mismatch.line, mismatch.generated, mismatch.formatted },
    );
    return error.GeneratedSourceNotFmtClean;
}

test "fmtMismatch accepts formatted source and pinpoints a slip" {
    const allocator = std.testing.allocator;
    try std.testing.expectEqual(null, try fmtMismatch(allocator, "const a = 1;\n"));

    const mismatch = (try fmtMismatch(allocator, "const a = 1;\nconst b =2;\n")).?;
    defer mismatch.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), mismatch.line);
    try std.testing.expectEqualStrings("const b =2;", mismatch.generated);
    try std.testing.expectEqualStrings("const b = 2;", mismatch.formatted);

    try std.testing.expectError(error.GeneratedSourceDoesNotParse, fmtMismatch(allocator, "const a = ;\n"));
}
