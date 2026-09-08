//! Generates fresh bindings from a reference compiler request. The fixture is
//! checked in so normal Zig tests need neither capnp nor another language host.
const std = @import("std");
const capnpc = @import("capnpc-zig");

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const output = args.next() orelse return error.MissingOutputDirectory;
    const bytes = @embedFile("request.bin");
    const request = try capnpc.request.parseCodeGeneratorRequest(init.gpa, bytes);
    defer capnpc.request.freeCodeGeneratorRequest(init.gpa, request);
    var generator = try capnpc.codegen.Generator.init(init.gpa, request.nodes);
    defer generator.deinit();
    try generator.setSchemaRequest(bytes);
    for (request.requested_files) |file| {
        const code = try generator.generateFile(file);
        defer init.gpa.free(code);
        const name = try std.fmt.allocPrint(init.gpa, "{s}.zig", .{file.filename[0 .. file.filename.len - ".capnp".len]});
        defer init.gpa.free(name);
        const path = try std.fs.path.join(init.gpa, &.{ output, name });
        defer init.gpa.free(path);
        if (std.fs.path.dirname(path)) |parent| try std.Io.Dir.cwd().createDirPath(init.io, parent);
        const destination = try std.Io.Dir.cwd().createFile(init.io, path, .{});
        defer destination.close(init.io);
        try destination.writeStreamingAll(init.io, code);
    }
    const path = try std.fs.path.join(init.gpa, &.{ output, "root.zig" });
    defer init.gpa.free(path);
    const root = try std.Io.Dir.cwd().createFile(init.io, path, .{});
    defer root.close(init.io);
    try root.writeStreamingAll(init.io,
        \\pub const values = @import("values.zig");
        \\pub const brands = @import("nested/brands.zig");
        \\pub const scalars = @import("reflection.zig");
        \\pub const helpers = @import("helper-names.zig");
        \\
    );
}
