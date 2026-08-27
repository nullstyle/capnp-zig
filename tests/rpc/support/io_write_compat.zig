//! Socket-write compatibility for tests across the std.Io net move.
//!
//! Zig deleted the `netWrite`/`netRead` `Io.VTable` entries and replaced
//! them with `net_write`/`net_read` `Operation` variants around
//! 0.17.0-dev.1786. Production code selects between the two in
//! `rpc.transport.tcp.stream`'s `ioWrite`; tests need the same choice in
//! two shapes — a plain write helper, and a FAKE Io that intercepts
//! writes — so the suite builds on both sides of the move.
//!
//! Selection keys on the OPERATION's presence, never on a version number.

const std = @import("std");
const net = std.Io.net;

/// True when this std routes socket writes through `Operation.net_write`
/// (post-move) rather than the `Io.VTable.netWrite` entry (pre-move).
pub const uses_operation = @hasField(std.Io.Operation, "net_write");

/// One socket write. Returns bytes accepted.
pub fn write(io: std.Io, handle: net.Socket.Handle, bytes: []const u8) net.Stream.Writer.Error!usize {
    if (bytes.len == 0) return 0;
    const pattern: []const u8 = &.{};
    const data: [1][]const u8 = .{pattern};
    if (comptime uses_operation) {
        const result = try io.operate(.{ .net_write = .{
            .socket_handle = handle,
            .header = bytes,
            .data = &data,
            .splat = 0,
        } });
        return result.net_write;
    }
    return io.vtable.netWrite(io.userdata, handle, bytes, &data, 0);
}

/// Write every byte or fail. `error.BrokenPipe` on a zero-length accept.
pub fn writeAll(io: std.Io, handle: net.Socket.Handle, bytes: []const u8) !void {
    var offset: usize = 0;
    while (offset < bytes.len) {
        const n = write(io, handle, bytes[offset..]) catch return error.WriteFailed;
        if (n == 0) return error.BrokenPipe;
        offset += n;
    }
}

/// Install a socket-write interceptor into a copied `Io.VTable`, in
/// whichever shape this std uses, plus no-op shutdown/close.
///
/// `hook` receives the vtable `userdata` untouched, so callers keep using
/// their own state pointer (or the real testing userdata) exactly as they
/// did with a hand-assigned `netWrite`.
///
/// Only writes, shutdown, and close are serviced. Every other operation
/// traps — the same constraint the hand-rolled fakes always had (they left
/// the real vtable pointed at foreign userdata), now explicit.
pub fn installWriteHook(vtable: *std.Io.VTable, comptime hook: WriteHook) void {
    const Shim = struct {
        fn operate(userdata: ?*anyopaque, operation: std.Io.Operation) std.Io.Cancelable!std.Io.Operation.Result {
            switch (operation) {
                .net_write => |op| return .{ .net_write = hook(userdata, op.header, op.data) },
                else => unreachable, // interceptor services writes only
            }
        }
        fn netWrite(
            userdata: ?*anyopaque,
            _: net.Socket.Handle,
            header: []const u8,
            data: []const []const u8,
            _: usize,
        ) net.Stream.Writer.Error!usize {
            return hook(userdata, header, data);
        }
        fn netShutdown(_: ?*anyopaque, _: net.Socket.Handle, _: net.ShutdownHow) net.ShutdownError!void {}
        fn netClose(_: ?*anyopaque, _: []const net.Socket) void {}
    };
    if (comptime uses_operation) {
        vtable.operate = Shim.operate;
    } else {
        vtable.netWrite = Shim.netWrite;
    }
    vtable.netShutdown = Shim.netShutdown;
    vtable.netClose = Shim.netClose;
}

/// Error set a write hook may return.
///
/// Post-move this is the OPERATION's error set, deliberately narrower
/// than `Stream.Writer.Error`: `Cancelable` lives on `operate`'s own
/// error, not inside `Operation.Result.net_write`. Pre-move the vtable
/// entry wants exactly `Stream.Writer.Error`.
pub const HookError = if (uses_operation)
    std.Io.Operation.NetWrite.Error
else
    net.Stream.Writer.Error;

/// Interceptor callback: return bytes "written" or an error.
pub const WriteHook = fn (userdata: ?*anyopaque, header: []const u8, data: []const []const u8) HookError!usize;

/// Total length of a header + data-chunk write, the value a hook that
/// accepts everything returns.
pub fn writtenLen(header: []const u8, data: []const []const u8) usize {
    var len: usize = header.len;
    for (data) |chunk| len += chunk.len;
    return len;
}
