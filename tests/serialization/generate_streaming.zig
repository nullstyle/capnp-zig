//! Fresh generated RPC fuzz bindings from the reference compiler fixture.
const std = @import("std");
const capnp = @import("capnpc-zig");
pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const directory = args.next() orelse return error.MissingOutputDirectory;
    const bytes = @embedFile("support/streaming-request.bin");
    const request = try capnp.request.parseCodeGeneratorRequest(init.gpa, bytes);
    defer capnp.request.freeCodeGeneratorRequest(init.gpa, request);
    var generator = try capnp.codegen.Generator.init(init.gpa, request.nodes);
    defer generator.deinit();
    try generator.setSchemaRequest(bytes);
    const code = try generator.generateFile(request.requested_files[0]);
    defer init.gpa.free(code);
    try std.Io.Dir.cwd().createDirPath(init.io, directory);
    const path = try std.fs.path.join(init.gpa, &.{ directory, "generated.zig" });
    defer init.gpa.free(path);
    var output = try std.Io.Dir.cwd().createFile(init.io, path, .{});
    defer output.close(init.io);
    try output.writeStreamingAll(init.io, code);
}
