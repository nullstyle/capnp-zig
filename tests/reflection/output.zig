const std = @import("std");
var directory: ?[]const u8 = null;

pub fn configure(init: std.process.Init) !?[]u8 {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const value = args.next() orelse return error.MissingOutputDirectory;
    if (std.mem.eql(u8, value, "--no-files")) return null;
    const path = try init.gpa.dupe(u8, value);
    errdefer init.gpa.free(path);
    try std.Io.Dir.cwd().createDirPath(init.io, path);
    directory = path;
    return path;
}

pub fn write(init: std.process.Init, name: []const u8, bytes: []const u8) !void {
    const destination = directory orelse return;
    const path = try std.fs.path.join(init.gpa, &.{ destination, name });
    defer init.gpa.free(path);
    const file = try std.Io.Dir.cwd().createFile(init.io, path, .{});
    defer file.close(init.io);
    try file.writeStreamingAll(init.io, bytes);
}
