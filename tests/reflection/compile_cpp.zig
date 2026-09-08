//! The oracle links to the system Cap'n Proto shared library. Use the native
//! C++ compiler so its exception ABI matches that library's C++ runtime.
const std = @import("std");

fn command(init: std.process.Init, argv: []const []const u8) ![]u8 {
    const result = try std.process.run(init.gpa, init.io, .{ .argv = argv });
    defer init.gpa.free(result.stderr);
    if (result.term != .exited or result.term.exited != 0) {
        defer init.gpa.free(result.stdout);
        std.debug.print("{s}: {s}\n", .{ argv[0], result.stderr });
        return error.CommandFailed;
    }
    return result.stdout;
}

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const source = args.next() orelse return error.MissingSource;
    const output = args.next() orelse return error.MissingOutput;
    const includes = try command(init, &.{ "pkg-config", "--variable=includedir", "capnp" });
    defer init.gpa.free(includes);
    const libraries = try command(init, &.{ "pkg-config", "--variable=libdir", "capnp" });
    defer init.gpa.free(libraries);
    const result = try command(init, &.{ "c++", "-std=c++20", "-I", std.mem.trim(u8, includes, " \r\n"), source, "-L", std.mem.trim(u8, libraries, " \r\n"), "-lcapnp", "-lkj", "-pthread", "-o", output });
    defer init.gpa.free(result);
}
