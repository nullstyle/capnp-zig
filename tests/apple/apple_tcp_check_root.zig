//! Root of the TCP-transport static libraries that `zig build check-ios`
//! builds for `aarch64-ios` and `aarch64-maccatalyst`: the full
//! `capnpc-zig` module, with the TCP transport's read path named.
//!
//! Fd passing is never compiled in on those targets, so the transport
//! refuses an AF_UNIX socket there: it reads the socket family with
//! `getsockname` and fails every read of one (see "AF_UNIX sockets without
//! fd passing" on `tcp.Transport`). That code path exists only on Darwin
//! targets without fd passing, and no lane runs them, so this root keeps it
//! compiling. A static library never links, so no Apple SDK is needed.
//!
//! The std overrides are those of `apple_check_root.zig`; iOS needs them at
//! Zig 0.17.0.
const std = @import("std");
const capnpc = @import("capnpc-zig");

const tcp = capnpc.rpc.transport.tcp;

fn trapPanic(msg: []const u8, ra: ?usize) noreturn {
    _ = msg;
    _ = ra;
    @trap();
}
pub const panic = std.debug.FullPanic(trapPanic);

fn noLog(comptime level: std.log.Level, comptime scope: @EnumLiteral(), comptime fmt: []const u8, args: anytype) void {
    _ = level;
    _ = scope;
    _ = fmt;
    _ = args;
}
pub const std_options: std.Options = .{ .logFn = noLog };
pub const std_options_debug_io: std.Io = std.Io.failing;

/// A transport on `fd` that reads once, with and without a deadline.
/// Returns 1 when the transport refused the socket (`unix_refused`), 0 when
/// it did not, and 2 when it could not be made.
export fn capnp_apple_check_tcp_read(fd: c_int) u32 {
    var transport = tcp.Transport.initWithOptions(std.heap.c_allocator, std.Io.failing, .{ .handle = fd }, .{
        .read_buffer_size = 64,
    }) catch return 2;
    defer transport.deinit();
    _ = transport.read() catch {};
    _ = transport.readTimeout(.none) catch {};
    return @intFromBool(transport.unix_refused);
}
