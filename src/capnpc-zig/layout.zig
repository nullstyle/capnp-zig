//! Layout helpers that keep generated source in the shape `zig fmt` renders,
//! so plugin output passes `zig fmt --check` without a formatting pass.
//!
//! `writer` is always the generator's `ArrayListWriter`: these helpers look
//! back at what is already written, which a streaming writer cannot do.
const std = @import("std");

/// Ends a member list by dropping trailing blank lines.
///
/// Member emitters finish every declaration with a blank line, which separates
/// it from the next member. `zig fmt` drops the blank line in front of a
/// closing brace and at the end of a file, so the code that ends the list
/// removes the last one instead of each emitter knowing whether it wrote the
/// final member.
pub fn endMembers(writer: anytype) void {
    const list = writer.list;
    while (std.mem.endsWith(u8, list.items, "\n\n")) list.items.len -= 1;
}

/// Closes a container: ends its member list, then writes `indent` and
/// `closer` (such as `"};\n\n"`). A container with no members is joined to
/// its opening line, as zig fmt renders it: `pub const VTable = struct {};`.
pub fn closeBlock(writer: anytype, indent: []const u8, closer: []const u8) !void {
    endMembers(writer);
    const list = writer.list;
    if (std.mem.endsWith(u8, list.items, "{\n")) {
        list.items.len -= 1;
    } else {
        try writer.writeAll(indent);
    }
    try writer.writeAll(closer);
}

/// Writes `data` as a one-line array initializer, `open` (such as `[_]u8{`)
/// through the closing brace. zig fmt pads the braces only around two or
/// more elements: `[_]u8{}`, `[_]u8{0x01}`, `[_]u8{ 0x01, 0x02 }`.
pub fn writeByteArray(writer: anytype, open: []const u8, data: []const u8) !void {
    try writer.writeAll(open);
    const padded = data.len > 1;
    if (padded) try writer.writeByte(' ');
    for (data, 0..) |byte, i| {
        if (i != 0) try writer.writeAll(", ");
        try writer.print("0x{X:0>2}", .{byte});
    }
    if (padded) try writer.writeByte(' ');
    try writer.writeByte('}');
}
