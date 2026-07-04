const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const rpc = capnpc.rpc;
const protocol = rpc.wire.protocol;
const HostPeer = rpc.integration.host_peer.HostPeer;
const TcpConnection = rpc.transport.tcp.Connection;

fn buildAbortFrame(allocator: std.mem.Allocator, reason: []const u8) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildAbort(reason);
    return builder.finish();
}

fn buildInvalidMessageTagFrame(allocator: std.mem.Allocator) ![]const u8 {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(1, 1);
    root.writeUnionDiscriminant(0, 0xffff);
    return builder.toBytes();
}

const tcp = capnpc.rpc.transport.tcp;

/// Portable connected pair: loopback TCP via std.Io, so these suites run
/// on every platform (POSIX socketpair does not exist on Windows).
fn createSocketPair() ![2]tcp.SocketFd {
    return tcp.createLoopbackSocketPair(std.testing.io);
}

fn closeFd(socket: tcp.SocketFd) void {
    tcp.closeFd(std.testing.io, socket);
}

fn writeAll(socket: tcp.SocketFd, bytes: []const u8) !void {
    const io = std.testing.io;
    const pattern: []const u8 = &.{};
    const data: [1][]const u8 = .{pattern};
    var offset: usize = 0;
    while (offset < bytes.len) {
        const n = io.vtable.netWrite(io.userdata, socket.handle, bytes[offset..], &data, 0) catch
            return error.WriteFailed;
        if (n == 0) return error.BrokenPipe;
        offset += n;
    }
}

fn oversizedSegmentHeader() [8]u8 {
    var header: [8]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 0, .little);
    std.mem.writeInt(u32, header[4..8], @intCast(rpc.wire.framing.Framer.max_frame_words + 1), .little);
    return header;
}

fn containsDisclosureMarker(reason: []const u8) bool {
    const markers = [_][]const u8{
        "/Users/",
        "/home/",
        "/private/",
        "/tmp/",
        "src/",
        ".zig:",
        "stack trace",
        "Stack trace",
        "panicked at",
        "TruncatedMessage",
        "InvalidFrame",
    };
    for (markers) |marker| {
        if (std.mem.indexOf(u8, reason, marker) != null) return true;
    }
    return false;
}

test "raw TCP client malformed segment header closes without dispatch" {
    const allocator = std.testing.allocator;

    const pair = try createSocketPair();
    defer closeFd(pair[1]);

    const State = struct {
        message_count: usize = 0,
        error_count: usize = 0,
        close_count: usize = 0,
        last_error: ?anyerror = null,
        closing_seen_in_error: bool = false,

        fn onMessage(conn: *TcpConnection, _: []const u8) !void {
            const state: *@This() = @ptrCast(@alignCast(conn.ctx.?));
            state.message_count += 1;
        }

        fn onError(conn: *TcpConnection, err: anyerror) void {
            const state: *@This() = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
            state.closing_seen_in_error = conn.transport.isClosing();
        }

        fn onClose(conn: *TcpConnection) void {
            const state: *@This() = @ptrCast(@alignCast(conn.ctx.?));
            state.close_count += 1;
        }
    };

    const Runner = struct {
        fn run(conn: *TcpConnection) void {
            conn.run();
        }
    };

    var conn = try TcpConnection.init(allocator, std.testing.io, pair[0], .{});
    defer conn.deinit();

    var state = State{};
    conn.start(&state, State.onMessage, State.onError, State.onClose);

    const thread = try std.Thread.spawn(.{}, Runner.run, .{&conn});
    const bad_header = [_]u8{ 0xff, 0xff, 0xff, 0xff };
    try writeAll(pair[1], &bad_header);
    thread.join();

    try std.testing.expectEqual(@as(usize, 0), state.message_count);
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), state.last_error);
    try std.testing.expect(state.closing_seen_in_error);
    try std.testing.expectEqual(@as(usize, 1), state.close_count);
    try std.testing.expect(conn.transport.isClosing());
}

test "raw TCP client valid frame before malformed frame dispatches only the valid frame" {
    const allocator = std.testing.allocator;

    // pair[1] is closed explicitly mid-test (the EOF signal); no defer —
    // std.Io.Threaded treats a double close as a use-after-free bug.
    const pair = try createSocketPair();

    const State = struct {
        allocator: std.mem.Allocator,
        message_count: usize = 0,
        error_count: usize = 0,
        close_count: usize = 0,
        last_error: ?anyerror = null,
        last_tag: ?protocol.MessageTag = null,

        fn onMessage(conn: *TcpConnection, frame: []const u8) !void {
            const state: *@This() = @ptrCast(@alignCast(conn.ctx.?));
            var decoded = try protocol.DecodedMessage.init(state.allocator, frame);
            defer decoded.deinit();
            state.message_count += 1;
            state.last_tag = decoded.tag;
        }

        fn onError(conn: *TcpConnection, err: anyerror) void {
            const state: *@This() = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
        }

        fn onClose(conn: *TcpConnection) void {
            const state: *@This() = @ptrCast(@alignCast(conn.ctx.?));
            state.close_count += 1;
        }
    };

    const Runner = struct {
        fn run(conn: *TcpConnection) void {
            conn.run();
        }
    };

    const good_frame = try buildAbortFrame(allocator, "raw-frame-security");
    defer allocator.free(good_frame);
    const bad_header = oversizedSegmentHeader();

    var stream = std.ArrayList(u8).empty;
    defer stream.deinit(allocator);
    try stream.appendSlice(allocator, good_frame);
    try stream.appendSlice(allocator, &bad_header);
    try stream.appendSlice(allocator, good_frame);

    var conn = try TcpConnection.init(allocator, std.testing.io, pair[0], .{});
    defer conn.deinit();

    var state = State{ .allocator = allocator };
    conn.start(&state, State.onMessage, State.onError, State.onClose);

    const thread = try std.Thread.spawn(.{}, Runner.run, .{&conn});
    try writeAll(pair[1], stream.items);
    closeFd(pair[1]);
    thread.join();

    try std.testing.expectEqual(@as(usize, 1), state.message_count);
    try std.testing.expectEqual(protocol.MessageTag.abort, state.last_tag.?);
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.FrameTooLarge), state.last_error);
    try std.testing.expectEqual(@as(usize, 1), state.close_count);
    try std.testing.expect(conn.transport.isClosing());
}

test "raw host peer invalid top-level tag yields unimplemented not abort" {
    const allocator = std.testing.allocator;

    var host = HostPeer.init(allocator);
    defer host.deinit();
    host.start(null, null, null);

    const invalid_tag = try buildInvalidMessageTagFrame(allocator);
    defer allocator.free(invalid_tag);
    try host.pushFrame(invalid_tag);

    const frame = host.popOutgoingFrame() orelse return error.ExpectedFrame;
    defer host.freeFrame(frame);

    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();

    try std.testing.expectEqual(protocol.MessageTag.unimplemented, decoded.tag);
    const unimplemented = try decoded.asUnimplemented();
    try std.testing.expectEqual(@as(?protocol.MessageTag, null), unimplemented.message_tag);
}

test "raw host peer malformed complete frame emits sanitized abort" {
    const allocator = std.testing.allocator;

    var host = HostPeer.init(allocator);
    defer host.deinit();
    host.start(null, null, null);

    try std.testing.expectError(error.TruncatedMessage, host.pushFrame(&[_]u8{}));

    const frame = host.popOutgoingFrame() orelse return error.ExpectedFrame;
    defer host.freeFrame(frame);

    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();

    try std.testing.expectEqual(protocol.MessageTag.abort, decoded.tag);
    const abort = try decoded.asAbort();
    try std.testing.expectEqualStrings(HostPeer.ErrorDisclosurePolicy.default_generic_reason, abort.exception.reason);
    try std.testing.expect(!containsDisclosureMarker(abort.exception.reason));
}

test "raw QUIC length-delimited client rejects empty oversized budget and OOM frames" {
    const allocator = std.testing.allocator;

    var empty = rpc.transport.quic.LengthDelimitedFramer.initWithOptions(allocator, .{
        .max_message_bytes = 8,
        .max_buffered_bytes = 12,
    });
    defer empty.deinit();

    var empty_prefix: [4]u8 = @splat(0);
    try empty.push(&empty_prefix);
    try std.testing.expectError(error.InvalidFrame, empty.popFrame());
    empty.reset();

    var oversized_prefix: [4]u8 = undefined;
    std.mem.writeInt(u32, &oversized_prefix, 9, .little);
    try empty.push(&oversized_prefix);
    try std.testing.expectError(error.FrameTooLarge, empty.popFrame());
    empty.reset();

    var budget = rpc.transport.quic.LengthDelimitedFramer.initWithOptions(allocator, .{
        .max_message_bytes = 8,
        .max_buffered_bytes = 4,
    });
    defer budget.deinit();

    var prefix: [4]u8 = undefined;
    std.mem.writeInt(u32, &prefix, 8, .little);
    try budget.push(&prefix);
    try std.testing.expectEqual(@as(usize, 4), budget.buffer.items.len);
    try std.testing.expectError(error.FrameTooLarge, budget.push(&[_]u8{1}));
    try std.testing.expectEqual(@as(usize, 4), budget.buffer.items.len);

    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 1 });
    var oom = rpc.transport.quic.LengthDelimitedFramer.init(failing.allocator(), 1024);
    defer oom.deinit();

    var encoded: [7]u8 = undefined;
    std.mem.writeInt(u32, encoded[0..4], 3, .little);
    @memcpy(encoded[4..7], "abc");

    try oom.push(&encoded);
    try std.testing.expectError(error.OutOfMemory, oom.popFrame());
    oom.reset();
    try std.testing.expectEqual(@as(usize, 0), oom.buffer.items.len);
}
