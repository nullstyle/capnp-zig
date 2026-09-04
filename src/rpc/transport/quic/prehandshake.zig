//! Pre-handshake stream buffering for ALPN-routed hosts of embedded
//! sessions.
//!
//! The gap: an `EmbeddedSession` is created at handshake completion, when
//! the negotiated ALPN first tells the host WHICH protocol owns the
//! connection — but a resumed (0-RTT) dial can push stream bytes before
//! the handshake finishes. A host that wants to accept those dials buffers
//! the early stream events per connection and replays them into the
//! winning seat at handshake time. This is that buffer.
//!
//! Events are recorded in arrival order and replayed in the same order —
//! per-stream FIFO, which is exactly the ordering the `quic.app.Driver`
//! itself guarantees for hook delivery. The buffer is bounded: a peer that
//! sprays pre-handshake bytes beyond the cap should be closed (the same
//! policy the embedded seat applies to its unconsumed stream bytes).
//! Replay is duck-typed over `onStreamOpen` / `onStreamData` /
//! `onStreamEnd`, so seats of any protocol work unchanged — the buffer is
//! itself protocol-agnostic and does not import the engines.

const std = @import("std");

pub const max_total_bytes_default: usize = 512 * 1024;

pub const Error = error{
    /// The pre-handshake cap was exceeded. The host should close the
    /// connection: the peer is misbehaving before it is even routed.
    PrehandshakeBufferFull,
    OutOfMemory,
};

const Event = union(enum) {
    open: struct { id: u64, bidi: bool },
    data: struct { id: u64, bytes: []u8 },
    end: struct { id: u64, kind: EndKind },
};

pub const EndKind = enum { fin, reset, reaped };

/// Bounded, ordered recording of pre-handshake stream events.
pub const Buffer = struct {
    allocator: std.mem.Allocator,
    events: std.ArrayListUnmanaged(Event) = .empty,
    total_bytes: usize = 0,
    max_total_bytes: usize = max_total_bytes_default,

    pub fn init(allocator: std.mem.Allocator) Buffer {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Buffer) void {
        for (self.events.items) |event| switch (event) {
            .data => |data| self.allocator.free(data.bytes),
            else => {},
        };
        self.events.deinit(self.allocator);
        // Reset so a deinit after replayInto's internal cleanup (or any
        // second deinit) is a safe no-op.
        self.events = .empty;
        self.total_bytes = 0;
    }

    /// Record a stream open. Idempotent per id (the Driver itself is).
    pub fn recordOpen(self: *Buffer, id: u64, bidi: bool) Error!void {
        for (self.events.items) |event| switch (event) {
            .open => |open| if (open.id == id) return,
            else => {},
        };
        try self.events.append(self.allocator, .{ .open = .{ .id = id, .bidi = bidi } });
    }

    pub fn recordData(self: *Buffer, id: u64, chunk: []const u8) Error!void {
        if (chunk.len == 0) return;
        if (self.total_bytes + chunk.len > self.max_total_bytes) {
            return Error.PrehandshakeBufferFull;
        }
        const bytes = try self.allocator.dupe(u8, chunk);
        self.total_bytes += chunk.len;
        try self.events.append(self.allocator, .{ .data = .{ .id = id, .bytes = bytes } });
    }

    pub fn recordEnd(self: *Buffer, id: u64, kind: EndKind) Error!void {
        try self.events.append(self.allocator, .{ .end = .{ .id = id, .kind = kind } });
    }

    /// Whether anything was recorded (a host can skip replay otherwise).
    pub fn isEmpty(self: *const Buffer) bool {
        return self.events.items.len == 0;
    }

    /// Replay every recorded event into `seat`, in arrival order, then drop
    /// the recordings. `mapEnd` converts the recorded end kind into the
    /// seat's own stream-end type, so replay works for any seat.
    /// Tolerate seats whose stream-end hook returns void instead of an
    /// error union (the capnp embedded session's does).
    fn awaitResult(result: anytype) !void {
        const info = @typeInfo(@TypeOf(result));
        if (info == .error_union) {
            return result;
        }
    }

    pub fn replayInto(self: *Buffer, seat: anytype, mapEnd: anytype) !void {
        for (self.events.items) |event| {
            switch (event) {
                .open => |open| try seat.onStreamOpen(open.id, open.bidi),
                .data => |data| try seat.onStreamData(data.id, data.bytes),
                .end => |end| try awaitResult(seat.onStreamEnd(end.id, mapEnd(end.kind))),
            }
        }
        self.deinit();
    }
};

// -- tests --------------------------------------------------------------------

const StreamEnd = union(enum) { fin, reset, reaped };

const TypedRecorder = struct {
    log: std.ArrayListUnmanaged(u8) = .empty,
    allocator: std.mem.Allocator,

    fn deinit(self: *TypedRecorder) void {
        self.log.deinit(self.allocator);
    }

    fn line(self: *TypedRecorder, comptime fmt: []const u8, args: anytype) !void {
        const bytes = try std.fmt.allocPrint(self.allocator, fmt, args);
        defer self.allocator.free(bytes);
        try self.log.appendSlice(self.allocator, bytes);
    }

    pub fn onStreamOpen(self: *TypedRecorder, id: u64, bidi: bool) !void {
        try self.line("open {d} {s}\n", .{ id, if (bidi) "bidi" else "uni" });
    }

    pub fn onStreamData(self: *TypedRecorder, id: u64, chunk: []const u8) !void {
        try self.line("data {d} {s}\n", .{ id, chunk });
    }

    pub fn onStreamEnd(self: *TypedRecorder, id: u64, end: StreamEnd) !void {
        try self.line("end {d} {s}\n", .{ id, @tagName(end) });
    }
};

test "prehandshake buffer replays in arrival order" {
    const allocator = std.testing.allocator;
    var buffer = Buffer.init(allocator);
    defer buffer.deinit();

    try buffer.recordOpen(0, true);
    try buffer.recordData(0, "Boot");
    try buffer.recordData(0, "strap");
    try buffer.recordOpen(2, false);
    try buffer.recordData(2, "side");
    try buffer.recordEnd(2, .fin);

    var recorder = TypedRecorder{ .allocator = allocator };
    defer recorder.deinit();
    try buffer.replayInto(&recorder, struct {
        fn map(kind: EndKind) StreamEnd {
            return switch (kind) {
                .fin => .fin,
                .reset => .reset,
                .reaped => .reaped,
            };
        }
    }.map);

    try std.testing.expectEqualStrings(
        \\open 0 bidi
        \\data 0 Boot
        \\data 0 strap
        \\open 2 uni
        \\data 2 side
        \\end 2 fin
        \\
    , recorder.log.items);
    try std.testing.expect(buffer.isEmpty());
}

test "prehandshake buffer enforces its cap" {
    const allocator = std.testing.allocator;
    var buffer = Buffer.init(allocator);
    defer buffer.deinit();
    buffer.max_total_bytes = 8;

    try buffer.recordData(0, "12345678");
    try std.testing.expectError(Error.PrehandshakeBufferFull, buffer.recordData(0, "9"));
}

test "prehandshake buffer open is idempotent per id" {
    const allocator = std.testing.allocator;
    var buffer = Buffer.init(allocator);
    defer buffer.deinit();

    try buffer.recordOpen(0, true);
    try buffer.recordOpen(0, true);
    try buffer.recordOpen(4, false);
    try std.testing.expectEqual(@as(usize, 2), buffer.events.items.len);
}
