//! Fd passing between capnp-zig and the C++ reference (sprint item 15):
//! `zig build test-rpc-fd-cpp`. Linux only: CI runs it in the
//! reflection-conformance job, which builds the pinned C++ reference.
//! Other targets compile it and skip (macOS: see the end of this comment).
//!
//! Ports the reference's "send FD over RPC" and "FD per message limit"
//! (`c++/src/capnp/rpc-twoparty-test.c++`) to a C++ <-> Zig connection,
//! both directions:
//! - fills of 1 MiB, 64 KiB, 8 KiB and 0 bytes, up to 2 fds per message:
//!   both write ends arrive, the server writes through them, and the
//!   returned capability's fd (a pipelined one, on the C++ client) reads
//!   back what the server wrote;
//! - 1 fd per message: the second capability arrives without its fd
//!   (`secondFdPresent == false`).
//!
//! The test generates the `tests/test_schemas/rpc_fd.capnp` binding, builds
//! the Zig endpoint (`rpc_unix_fd_cpp_endpoint.zig`: the real
//! `tcp.Connection` and `Peer`, `Connection.enableFdPassing`) and the C++
//! driver (`rpc_unix_fd_cpp_driver.cpp`) against the reference found by
//! `pkg-config capnp`, then runs the driver. Both sides check every pipe
//! for its data and EOF, and the endpoint checks that its fd table is back
//! at its baseline after teardown.
//!
//! Not on macOS: there the reference itself loses fds. `TwoPartyVatNetwork`
//! reads through kj's `BufferedMessageStream`, which reads in bulk and gives
//! a read's fds to the message that holds the read's last byte
//! (`serialize-async.c++`). Linux ends a read that takes fds at the end of
//! their segment, so that rule holds. macOS anchors them to the read's first
//! byte ("Bulk-read fd anchor" in docs/rpc-unix-sockets.md): when a frame
//! with fds and the next frame arrive in one read, kj gives the fds to the
//! next frame, and the capability arrives without its fd. Measured with this
//! test's driver and endpoint against Homebrew capnp 1.5.0, 16 runs at a
//! time: 5% to 44% of runs failed, each on the C++ receiving side; on Linux
//! arm64 none did ("Fd passing" in docs/rpc-unix-sockets.md).

const std = @import("std");
const builtin = @import("builtin");
const capnp = @import("capnpc-zig");
const cli = @import("capnp-cli");

/// The library reads `-Dfd-passing` from this module (see the fixture).
const build_options_arg = "-Mcapnp_build_options=tests/fixtures/capnp_build_options.zig";

fn run(argv: []const []const u8) ![]u8 {
    const result = try std.process.run(std.testing.allocator, std.testing.io, .{ .argv = argv });
    defer std.testing.allocator.free(result.stderr);
    if (result.term != .exited or result.term.exited != 0) {
        defer std.testing.allocator.free(result.stdout);
        std.debug.print("command {s}:\n{s}\n{s}\n", .{ argv[0], result.stdout, result.stderr });
        return error.FdInteropCommandFailed;
    }
    return result.stdout;
}

fn writeFile(dir: std.Io.Dir, path: []const u8, bytes: []const u8) !void {
    var file = try dir.createFile(std.testing.io, path, .{});
    defer file.close(std.testing.io);
    try file.writeStreamingAll(std.testing.io, bytes);
}

test "C++ reference and capnp-zig pass fds on capabilities over AF_UNIX, in both directions" {
    // Linux only. Windows has no AF_UNIX transport, and on macOS the C++
    // reference drops fds on its own receiving side (see the top of this
    // file), so the test would be flaky there.
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const schema = "tests/test_schemas/rpc_fd.capnp";
    const compiled = try cli.run(allocator, io, &.{ "compile", "-o-", "--src-prefix=tests/test_schemas", schema }, .{ .missing = .required });
    defer allocator.free(compiled.stdout);
    defer allocator.free(compiled.stderr);
    try std.testing.expect(compiled.term == .exited and compiled.term.exited == 0);
    const request = try capnp.request.parseCodeGeneratorRequest(allocator, compiled.stdout);
    defer capnp.request.freeCodeGeneratorRequest(allocator, request);
    var generator = try capnp.codegen.Generator.init(allocator, request.nodes);
    defer generator.deinit();
    const generated = try generator.generateFile(request.requested_files[0]);
    defer allocator.free(generated);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeFile(tmp.dir, "generated.zig", generated);
    try writeFile(tmp.dir, "endpoint.zig", @embedFile("rpc_unix_fd_cpp_endpoint.zig"));
    try writeFile(tmp.dir, "driver.cpp", @embedFile("rpc_unix_fd_cpp_driver.cpp"));
    const directory = try tmp.dir.realPathFileAlloc(io, ".", allocator);
    defer allocator.free(directory);

    const endpoint = try std.fs.path.join(allocator, &.{ directory, "endpoint" });
    defer allocator.free(endpoint);
    const endpoint_source = try std.fs.path.join(allocator, &.{ directory, "endpoint.zig" });
    defer allocator.free(endpoint_source);
    const lib = try std.Io.Dir.cwd().realPathFileAlloc(io, "src/lib.zig", allocator);
    defer allocator.free(lib);
    const root_arg = try std.fmt.allocPrint(allocator, "-Mroot={s}", .{endpoint_source});
    defer allocator.free(root_arg);
    const lib_arg = try std.fmt.allocPrint(allocator, "-Mcapnpc-zig={s}", .{lib});
    defer allocator.free(lib_arg);
    const emit_arg = try std.fmt.allocPrint(allocator, "-femit-bin={s}", .{endpoint});
    defer allocator.free(emit_arg);
    allocator.free(try run(&.{ "zig", "build-exe", "-lc", "-ODebug", "--dep", "capnpc-zig", root_arg, "--dep", "capnpc-zig", "--dep", "capnp_build_options", lib_arg, build_options_arg, emit_arg }));

    const cpp_output = try std.fmt.allocPrint(allocator, "c++:{s}", .{directory});
    defer allocator.free(cpp_output);
    allocator.free(try run(&.{ "capnp", "compile", "-o", cpp_output, "--src-prefix=tests/test_schemas", schema }));
    const includes = try run(&.{ "pkg-config", "--variable=includedir", "capnp" });
    defer allocator.free(includes);
    const libraries = try run(&.{ "pkg-config", "--variable=libdir", "capnp" });
    defer allocator.free(libraries);
    const cpp_source = try std.fs.path.join(allocator, &.{ directory, "driver.cpp" });
    defer allocator.free(cpp_source);
    const cpp_schema = try std.fs.path.join(allocator, &.{ directory, "rpc_fd.capnp.c++" });
    defer allocator.free(cpp_schema);
    const driver = try std.fs.path.join(allocator, &.{ directory, "driver" });
    defer allocator.free(driver);
    var environment = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer environment.deinit();
    const compiler = environment.get("CXX") orelse "c++";
    allocator.free(try run(&.{ compiler, "-std=c++23", "-I", std.mem.trim(u8, includes, " \r\n"), cpp_source, cpp_schema, "-L", std.mem.trim(u8, libraries, " \r\n"), "-lcapnp-rpc", "-lcapnp", "-lkj-async", "-lkj", "-pthread", "-o", driver }));

    const output = try run(&.{ driver, endpoint });
    defer allocator.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "C++ <-> Zig fd passing over AF_UNIX passed") != null);
}
