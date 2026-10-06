//! `-Dfd-passing=false` on Linux and macOS: what a build without fd passing
//! does with AF_UNIX sockets.
//!
//! Such a build has no fd closer, so it has no safe way to read an AF_UNIX
//! socket (a plain read leaks the fds a peer attaches on macOS and closes
//! them on the reading thread on Linux). So:
//!
//! - `rpc.transport.unix` is off: `unix.supported` is false, and `listen`
//!   and `connect` return `error.UnixSocketsUnsupported`.
//! - The TCP transport refuses an AF_UNIX socket it is handed
//!   (`Connection.init`, `Listener.initFd`): its first read fails with
//!   `error.Unexpected` after a `.resource_rejection` event whose `err` is
//!   `error.UnixSocketsUnsupported`. TCP sockets read as before.
//!
//! These cases run only in that build (`zig build -Dfd-passing=false test`
//! on Linux or macOS). With fd passing on, the last case checks the other
//! side of the switch: the same socket reads in drain mode.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const support = @import("fd_test_support.zig");
const io_write_compat = @import("io-write-compat");

const testing = std.testing;
const fd_io = support.fd_io;
const events = support.events;
const unix = capnpc.rpc.transport.unix;
const tcp = capnpc.rpc.transport.tcp;
const Connection = tcp.Connection;

/// This build compiled fd passing out on a target that has it.
const fd_passing_off = (support.is_linux or support.is_macos) and !fd_io.supported;

fn onMessage(conn: *Connection, _: []const u8) anyerror!void {
    const inbox: *support.Inbox = @ptrCast(@alignCast(conn.context().?));
    inbox.frames += 1;
}

fn onError(conn: *Connection, err: anyerror) void {
    const inbox: *support.Inbox = @ptrCast(@alignCast(conn.context().?));
    inbox.errors += 1;
    inbox.last_error = err;
}

fn onClose(_: *Connection) void {}

test "-Dfd-passing=false: unix.listen and unix.connect return UnixSocketsUnsupported" {
    if (comptime !fd_passing_off) return error.SkipZigTest;
    try testing.expect(!unix.supported);
    try testing.expectError(error.UnixSocketsUnsupported, unix.listen(testing.allocator, testing.io, "/tmp/capnp-fd-off.sock", .{}));
    try testing.expectError(error.UnixSocketsUnsupported, unix.connect(testing.allocator, testing.io, "/tmp/capnp-fd-off.sock", .{}));
}

test "-Dfd-passing=false: a Connection on an AF_UNIX socket fails its first read and dispatches nothing" {
    if (comptime !fd_passing_off) return error.SkipZigTest;
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        var pipes = try support.Pipes.open(1);
        defer pipes.closeAll();

        // A valid frame with a pipe write end attached, as a hostile local
        // peer would send it.
        const frame = try support.buildFrame(testing.allocator, 0xF00D);
        defer testing.allocator.free(frame);
        try support.sendWithFds(sp[0], frame, pipes.writers());
        pipes.closeWriters();
        // EOF after the frame, so a transport that did read it plainly
        // would still end its run (and this test fail, not hang).
        support.closeFd(sp[0]);

        var inbox: support.Inbox = .{};
        var recorder: support.Recorder = .{};
        var conn = try Connection.init(testing.allocator, testing.io, .{ .handle = sp[1] }, .{
            .observer = recorder.observer(),
        });
        conn.start(&inbox, onMessage, onError, onClose);
        conn.run();
        conn.deinit();

        try testing.expectEqual(@as(usize, 0), inbox.frames);
        try testing.expectEqual(@as(usize, 1), inbox.errors);
        try testing.expectEqual(@as(?anyerror, error.Unexpected), inbox.last_error);
        try testing.expectEqual(@as(usize, 1), recorder.countErr(error.UnixSocketsUnsupported));
        try testing.expectEqual(events.Source.unix, recorder.attached_fds[0].source);
        try testing.expectEqual(@as(?usize, 0), recorder.attached_fds[0].limit);
        // Nothing read the frame, so the attached fd never entered this
        // process: the kernel closed it with the socket.
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "-Dfd-passing=false: Transport refuses an AF_UNIX socket on every read, and reads a TCP socket" {
    if (comptime !fd_passing_off) return error.SkipZigTest;
    {
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        var recorder: support.Recorder = .{};
        var transport = try tcp.Transport.initWithOptions(testing.allocator, testing.io, .{ .handle = sp[1] }, .{
            .read_buffer_size = 64,
            .observer = recorder.observer(),
        });
        defer transport.deinit();
        try testing.expect(transport.unix_refused);
        try testing.expectEqual(events.Source.unix, transport.source);
        try testing.expectError(error.Unexpected, transport.read());
        try testing.expectError(error.Unexpected, transport.readTimeout(.{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }));
        try testing.expectEqual(@as(usize, 2), recorder.countErr(error.UnixSocketsUnsupported));
    }
    {
        const pair = try tcp.createLoopbackSocketPair(testing.io);
        defer tcp.closeFd(testing.io, pair[0]);
        var recorder: support.Recorder = .{};
        var transport = try tcp.Transport.initWithOptions(testing.allocator, testing.io, pair[1], .{
            .read_buffer_size = 64,
            .observer = recorder.observer(),
        });
        defer transport.deinit();
        try testing.expect(!transport.unix_refused);
        try testing.expectEqual(events.Source.tcp, transport.source);
        const sent = "ping";
        try io_write_compat.writeAll(testing.io, pair[0].handle, sent);
        const n = try transport.read();
        try testing.expectEqualStrings(sent, transport.read_buf[0..n]);
        try testing.expectEqual(@as(usize, 0), recorder.attached_count);
    }
}

test "with fd passing on, the same AF_UNIX socket reads in drain mode" {
    if (comptime !(support.supported and fd_io.supported)) return error.SkipZigTest;
    const sp = try support.socketPair();
    defer support.closeFd(sp[0]);
    var transport = try tcp.Transport.init(testing.allocator, testing.io, .{ .handle = sp[1] }, 64);
    defer transport.deinit();
    try testing.expect(!transport.unix_refused);
    try testing.expect(transport.drain != null);
    try testing.expectEqual(events.Source.unix, transport.source);
}
