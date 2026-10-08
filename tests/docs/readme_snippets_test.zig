//! The Zig code blocks of README.md, compiled and run.
//!
//! README.md shows each whole program in tests/docs/readme/ under a
//! `<!-- verbatim-file: ... -->` marker, and the Unix-socket excerpt below
//! under a `<!-- verbatim: ... -->` marker. `zig build docs-smoke` fails when
//! a README block and its file differ by one character, and when a README
//! Zig block has no marker. This file runs the programs against the full
//! `capnpc-zig` module and the REAL generated examples/addressbook.zig (wired
//! up in build/build_impl.zig), so a README program that stops compiling or
//! working fails `zig build test-docs-snippets`.

const std = @import("std");
const capnpc = @import("capnpc-zig");

const testing = std.testing;

const library = @import("readme/library.zig");
const generated = @import("readme/generated.zig");
const io_backend = @import("readme/io_backend.zig");

/// What `start.zig` hands a `main(init: std.process.Init)`, for a test. The
/// README programs read only `gpa` and `io`; `testing.allocator` fails the
/// test on a leak. The fields left undefined must stay unread.
fn testInit(arena: *std.heap.ArenaAllocator) std.process.Init {
    return .{
        .minimal = undefined,
        .arena = arena,
        .gpa = testing.allocator,
        .io = testing.io,
        .environ_map = undefined,
        .preopens = undefined,
    };
}

test "README: As a Library" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    try library.main(testInit(&arena));
}

test "README: Generated Code Example" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    try generated.main(testInit(&arena));
}

test "README: Switchable Io Backend" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    try io_backend.main(testInit(&arena));
}

/// README "Unix-Domain Sockets". Compiled, not run: the README names a path
/// under /run. tests/docs/rpc_unix_snippets_test.zig runs `listen` and
/// `connect` over a real socket file.
fn unixSockets(gpa: std.mem.Allocator, io: std.Io) !void {
    var listener = try capnpc.rpc.transport.unix.listen(gpa, io, "/run/myapp/rpc.sock", .{});
    defer listener.close(); // removes the socket file, releases its lock
    const session = try capnpc.rpc.transport.unix.connect(gpa, io, "/run/myapp/rpc.sock", .{});
    defer session.deinit();
}

test "README: Unix-Domain Sockets compiles" {
    // Taking the address makes the compiler analyze the body.
    _ = &unixSockets;
}
