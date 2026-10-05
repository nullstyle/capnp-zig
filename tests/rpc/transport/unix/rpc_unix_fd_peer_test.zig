//! Fd passing through the Peer (sprint item 12): `Peer.setExportFd`,
//! `Peer.importFd`, and the side tables in `src/rpc/peer/peer_fds.zig`,
//! outside the frozen cap table.
//!
//! Two halves:
//! - The Peer seam, driven frame by frame through a fake `TransportBinding`
//!   that hands out fds the way a transport does (`take_frame_fd`) and
//!   records what the peer sends (`send_with_fds`). Every fd it "receives"
//!   is a dup of a pipe write end the test owns, so a pipe that reads EOF
//!   once the test closes its own write end proves every copy was closed.
//! - Real sockets: two peers over `unix.listen`/`unix.connect` sessions
//!   pass a pipe write end and write through it; TCP leaves `attachedFd` at
//!   0xff, and so does an AF_UNIX connection without fd passing on; a raw
//!   AF_UNIX client checks that only the attached fd ever crosses the
//!   socket while the server's wake socketpair is open.
//!
//! Linux and macOS run it; other targets compile it and skip.

const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");
const support = @import("fd_test_support.zig");

const protocol = capnpc.rpc.wire.protocol;
const framing = capnpc.rpc.wire.framing;
const cap_table = capnpc.rpc.caps.table;
const rpc_peer = capnpc.rpc.peer;
const Peer = rpc_peer.Peer;
const FdHandle = rpc_peer.FdHandle;
const tcp = capnpc.rpc.transport.tcp;
const unix = capnpc.rpc.transport.unix;
const events = capnpc.rpc.events;

const posix = support.posix;
const sys = support.sys;
const Fd = support.Fd;
const testing = std.testing;
const supported = support.supported;

const iface_id: u64 = 0x9a1f_2c3d_4e5f_6071;

// ---------------------------------------------------------------------------
// Pipes and fds
// ---------------------------------------------------------------------------

fn dupFd(fd: Fd) !Fd {
    return @intCast(try support.check(sys.dup(fd), "dup"));
}

/// The file `fd` refers to, as its inode number: a received copy of a pipe
/// write end has the write end's.
fn inodeOf(fd: Fd) !u64 {
    if (comptime support.is_linux) {
        const linux = std.os.linux;
        var stx: linux.Statx = undefined;
        const rc = linux.statx(fd, "", linux.AT.EMPTY_PATH, .{ .INO = true }, &stx);
        if (linux.errno(rc) != .SUCCESS) return error.SyscallFailed;
        return stx.ino;
    } else if (comptime support.is_macos) {
        var st: std.c.Stat = undefined;
        _ = try support.check(std.c.fstat(fd, &st), "fstat");
        return st.ino;
    } else {
        return error.Unsupported;
    }
}

/// Reads exactly `expected` from `read_end` within `support.closed_wait_ms`.
fn expectPipeData(read_end: Fd, expected: []const u8) !void {
    var buf: [64]u8 = undefined;
    var got: usize = 0;
    const start = support.nowNs();
    while (got < expected.len) {
        if (support.msSince(start) >= support.closed_wait_ms) return error.PipeDataMissing;
        var pfd = [1]posix.pollfd{.{ .fd = read_end, .events = posix.POLL.IN, .revents = 0 }};
        const rc = sys.poll(&pfd, 1, 50);
        if (posix.errno(rc) != .SUCCESS or rc == 0) continue;
        const n = try support.check(sys.read(read_end, buf[got..].ptr, expected.len - got), "read");
        if (n == 0) return error.PipeClosedEarly;
        got += n;
    }
    try testing.expectEqualStrings(expected, buf[0..got]);
}

fn writeAll(fd: Fd, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) off += try support.check(sys.write(fd, bytes[off..].ptr, bytes.len - off), "write");
}

/// Every copy of pipe `i`'s write end is closed: the read end sees EOF.
fn expectClosed(pipes: *const support.Pipes, i: usize) !void {
    if (!support.pipeWritersClosed(pipes.read_ends[i], support.closed_wait_ms)) {
        std.debug.print("pipe {d}: a copy of its write end is still open\n", .{i});
        return error.AttachedFdStillOpen;
    }
}

/// Some copy of pipe `i`'s write end is still open.
fn expectOpen(pipes: *const support.Pipes, i: usize) !void {
    if (support.pipeWritersClosed(pipes.read_ends[i], support.still_open_wait_ms)) {
        std.debug.print("pipe {d}: every copy of its write end is closed\n", .{i});
        return error.AttachedFdClosedEarly;
    }
}

// ---------------------------------------------------------------------------
// The fake transport: a TransportBinding with the fd hooks
// ---------------------------------------------------------------------------

const Sent = struct {
    bytes: []u8,
    fds: [8]Fd = @splat(-1),
    fd_count: usize = 0,
};

const FakeTransport = struct {
    allocator: std.mem.Allocator,
    max_outbound_fds: u8 = 253,
    /// The fds of the frame being dispatched; -1 once taken.
    frame_fds: [8]Fd = @splat(-1),
    frame_fd_count: usize = 0,
    sent: std.ArrayListUnmanaged(Sent) = .empty,
    /// When set, every send with fds fails with it and sends nothing, as
    /// `Connection.sendFrameWithFds` does on an fd error.
    fds_error: ?anyerror = null,
    /// How many times the peer closed the transport.
    closes: usize = 0,

    fn binding(self: *FakeTransport) rpc_peer.TransportBinding {
        var b = rpc_peer.TransportBinding.init(self, null, send, close, null);
        b.send_with_fds = sendWithFds;
        b.take_frame_fd = take;
        b.max_outbound_fds = maxOutboundFds;
        return b;
    }

    fn maxOutboundFds(ctx: *anyopaque) u8 {
        const self: *FakeTransport = @ptrCast(@alignCast(ctx));
        return self.max_outbound_fds;
    }

    fn close(ctx: *anyopaque) void {
        const self: *FakeTransport = @ptrCast(@alignCast(ctx));
        self.closes += 1;
    }

    fn deinit(self: *FakeTransport) void {
        for (self.sent.items) |s| self.allocator.free(s.bytes);
        self.sent.deinit(self.allocator);
    }

    fn record(self: *FakeTransport, frame: []const u8, fds: []const FdHandle) !void {
        var s: Sent = .{ .bytes = try self.allocator.dupe(u8, frame) };
        errdefer self.allocator.free(s.bytes);
        for (fds, 0..) |h, i| {
            // Borrowed: the fd must be open while the transport sends it.
            try testing.expect(support.isOpen(h.fd));
            s.fds[i] = h.fd;
        }
        s.fd_count = fds.len;
        try self.sent.append(self.allocator, s);
    }

    fn send(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *FakeTransport = @ptrCast(@alignCast(ctx));
        try self.record(frame, &.{});
    }

    fn sendWithFds(ctx: *anyopaque, frame: []const u8, fds: []const FdHandle) anyerror!void {
        const self: *FakeTransport = @ptrCast(@alignCast(ctx));
        try testing.expect(fds.len != 0);
        if (self.fds_error) |err| return err;
        try self.record(frame, fds);
    }

    fn take(ctx: *anyopaque, index: u8) ?FdHandle {
        const self: *FakeTransport = @ptrCast(@alignCast(ctx));
        if (index >= self.frame_fd_count) return null;
        const fd = self.frame_fds[index];
        if (fd < 0) return null;
        self.frame_fds[index] = -1;
        return .{ .fd = fd };
    }

    /// Dispatch `frame` with a dup of each of `attach` as its fds, as a
    /// transport would. Afterwards every fd the peer did not take is closed
    /// (the real transport hands them to the closer).
    fn deliver(self: *FakeTransport, peer: *Peer, frame: []const u8, attach: []const Fd) !void {
        std.debug.assert(attach.len <= self.frame_fds.len);
        for (attach, 0..) |fd, i| self.frame_fds[i] = try dupFd(fd);
        self.frame_fd_count = attach.len;
        defer {
            for (self.frame_fds[0..self.frame_fd_count]) |*fd| {
                if (fd.* >= 0) support.closeFd(fd.*);
                fd.* = -1;
            }
            self.frame_fd_count = 0;
        }
        try peer.handleFrame(frame);
    }

    fn last(self: *const FakeTransport) Sent {
        return self.sent.items[self.sent.items.len - 1];
    }
};

/// One cap descriptor of a hand-built inbound frame.
const Desc = struct {
    kind: enum { sender_hosted, sender_promise, receiver_hosted, third_party },
    id: u32,
    fd: ?u8 = null,
};

fn writeDescs(list: anytype, descs: []const Desc) !void {
    for (descs, 0..) |d, i| {
        var b = try list.get(@intCast(i));
        switch (d.kind) {
            .sender_hosted => try b.setSenderHosted(d.id),
            .sender_promise => try b.setSenderPromise(d.id),
            .receiver_hosted => try b.setReceiverHosted(d.id),
            .third_party => {
                var third = try b.initThirdPartyHosted();
                try third.setVineId(d.id);
                try third.setIdNull();
            },
        }
        if (d.fd) |index| try b.setAttachedFd(index);
    }
}

/// A Call to export `target` whose params carry `descs` (content: a list of
/// every cap).
fn buildCall(gpa: std.mem.Allocator, question_id: u32, target: u32, descs: []const Desc) ![]const u8 {
    var mb = protocol.MessageBuilder.init(gpa);
    defer mb.deinit();
    var call = try mb.beginCall(question_id, iface_id, 0);
    try call.setTargetImportedCap(target);
    var payload = try call.payloadTyped();
    const caps = try (try payload.initContent()).initPointerList(@intCast(descs.len));
    for (0..descs.len) |i| try caps.setCapability(@intCast(i), .{ .id = @intCast(i) });
    try writeDescs(try call.initCapTableTyped(@intCast(descs.len)), descs);
    return mb.finish();
}

/// A Return (results) for question `answer_id` carrying `descs`.
fn buildReturn(gpa: std.mem.Allocator, answer_id: u32, descs: []const Desc) ![]const u8 {
    var mb = protocol.MessageBuilder.init(gpa);
    defer mb.deinit();
    var ret = try mb.beginReturn(answer_id, .results);
    var payload = try ret.payloadTyped();
    try (try payload.initContent()).setCapability(.{ .id = 0 });
    try writeDescs(try ret.initCapTableTyped(@intCast(descs.len)), descs);
    return mb.finish();
}

fn buildResolve(gpa: std.mem.Allocator, promise_id: u32, desc: Desc) ![]const u8 {
    var mb = protocol.MessageBuilder.init(gpa);
    defer mb.deinit();
    try mb.buildResolveCap(promise_id, .{
        .tag = switch (desc.kind) {
            .sender_hosted => .senderHosted,
            .sender_promise => .senderPromise,
            .receiver_hosted => .receiverHosted,
            .third_party => unreachable,
        },
        .id = desc.id,
        .attached_fd = desc.fd,
    });
    return mb.finish();
}

/// A Finish for `question_id` that keeps the result caps.
fn buildFinish(gpa: std.mem.Allocator, question_id: u32) ![]const u8 {
    var mb = protocol.MessageBuilder.init(gpa);
    defer mb.deinit();
    try mb.buildFinish(question_id, false, false);
    return mb.finish();
}

fn buildRelease(gpa: std.mem.Allocator, id: u32, count: u32) ![]const u8 {
    var mb = protocol.MessageBuilder.init(gpa);
    defer mb.deinit();
    try mb.buildRelease(id, count);
    return mb.finish();
}

/// The bootstrap export's handler: for each cap of the call it records the
/// import id and `importFd`, writes `tag` through every fd it got, keeps
/// the caps when `retain` is set, and returns an empty struct.
const Recorder = struct {
    retain: bool = false,
    tag: u8 = 'x',
    calls: usize = 0,
    ids: [8]u32 = @splat(0),
    fds: [8]?Fd = @splat(null),
    count: usize = 0,

    fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *Recorder = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        self.count = 0;
        var index: u32 = 0;
        while (index < caps.len()) : (index += 1) {
            switch (try caps.get(index)) {
                .imported => |imported| {
                    self.ids[self.count] = imported.id;
                    const handle = peer.importFd(imported.id);
                    self.fds[self.count] = if (handle) |h| h.fd else null;
                    if (handle) |h| try writeAll(h.fd, &.{self.tag});
                    if (self.retain) try @constCast(caps).retainIndex(index);
                },
                else => {
                    self.ids[self.count] = std.math.maxInt(u32);
                    self.fds[self.count] = null;
                },
            }
            self.count += 1;
        }
        try peer.sendReturnEmptyStruct(call.question_id);
    }
};

/// A detached peer on a `FakeTransport`, with the recorder as bootstrap.
const SeamPeer = struct {
    fake: FakeTransport,
    peer: Peer,
    recorder: Recorder = .{},
    bootstrap_id: u32 = undefined,

    fn init(self: *SeamPeer, gpa: std.mem.Allocator, max_outbound_fds: u8) !void {
        self.* = .{
            .fake = .{ .allocator = gpa, .max_outbound_fds = max_outbound_fds },
            .peer = Peer.initDetached(gpa),
        };
        self.peer.attachTransportBinding(self.fake.binding());
        self.bootstrap_id = try self.peer.setBootstrap(.{ .ctx = &self.recorder, .on_call = Recorder.onCall });
    }

    fn deinit(self: *SeamPeer) void {
        self.peer.deinit();
        self.fake.deinit();
    }

    fn deliverCall(self: *SeamPeer, question_id: u32, descs: []const Desc, attach: []const Fd) !void {
        const frame = try buildCall(self.fake.allocator, question_id, self.bootstrap_id, descs);
        defer self.fake.allocator.free(frame);
        try self.fake.deliver(&self.peer, frame, attach);
    }

    fn deliver(self: *SeamPeer, frame: []const u8, attach: []const Fd) !void {
        defer self.fake.allocator.free(frame);
        try self.fake.deliver(&self.peer, frame, attach);
    }
};

// ---------------------------------------------------------------------------
// Inbound: which descriptor gets which fd
// ---------------------------------------------------------------------------

test "inbound Call: a senderHosted fd belongs to its import while the handler runs, and closes with it" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();

    try seam.deliverCall(0, &.{.{ .kind = .sender_hosted, .id = 7, .fd = 0 }}, pipes.writers());
    try testing.expectEqual(@as(usize, 1), seam.recorder.calls);
    try testing.expectEqual(@as(u32, 7), seam.recorder.ids[0]);
    try testing.expect(seam.recorder.fds[0] != null);
    // The handler wrote through the fd: it is the pipe.
    try expectPipeData(pipes.read_ends[0], "x");
    // Not retained: the import was released after dispatch, and its fd with it.
    try testing.expect(!seam.peer.caps.hasImport(7));
    try testing.expect(seam.peer.importFd(7) == null);
    pipes.closeWriters();
    try expectClosed(&pipes, 0);
}

test "inbound: a duplicate fd index goes to the first descriptor only" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();

    try seam.deliverCall(0, &.{
        .{ .kind = .sender_hosted, .id = 7, .fd = 0 },
        .{ .kind = .sender_hosted, .id = 8, .fd = 0 },
    }, pipes.writers());
    try testing.expect(seam.recorder.fds[0] != null);
    try testing.expect(seam.recorder.fds[1] == null);
    try expectPipeData(pipes.read_ends[0], "x");
    pipes.closeWriters();
    try expectClosed(&pipes, 0);
}

test "inbound: an attachedFd past the frame's fds attaches nothing, and the fd is closed" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();
    seam.recorder.retain = true;

    try seam.deliverCall(0, &.{.{ .kind = .sender_hosted, .id = 7, .fd = 3 }}, pipes.writers());
    try testing.expect(seam.recorder.fds[0] == null);
    try testing.expect(seam.peer.caps.hasImport(7));
    try testing.expect(seam.peer.importFd(7) == null);
    pipes.closeWriters();
    try expectClosed(&pipes, 0);
}

test "inbound: a second fd for the same import is closed, in one frame and in a later one; the first stays" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(3);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();
    seam.recorder.retain = true;

    // One frame: import 7 twice, with pipes 0 and 1.
    try seam.deliverCall(0, &.{
        .{ .kind = .sender_hosted, .id = 7, .fd = 0 },
        .{ .kind = .sender_hosted, .id = 7, .fd = 1 },
    }, pipes.writers()[0..2]);
    try testing.expectEqual(seam.recorder.fds[0], seam.recorder.fds[1]);
    // Both descriptors got pipe 0's fd: each wrote one byte through it.
    try expectPipeData(pipes.read_ends[0], "xx");

    // A later frame: import 7 again, with pipe 2.
    seam.recorder.tag = 'y';
    try seam.deliverCall(1, &.{.{ .kind = .sender_hosted, .id = 7, .fd = 0 }}, pipes.writers()[2..3]);
    try expectPipeData(pipes.read_ends[0], "y");

    pipes.closeWriters();
    try expectClosed(&pipes, 1);
    try expectClosed(&pipes, 2);
    try expectOpen(&pipes, 0);
    // Three wire refs on import 7; releasing them all closes pipe 0's fd.
    try seam.peer.releaseImport(7, 3);
    try testing.expect(!seam.peer.caps.hasImport(7));
    try expectClosed(&pipes, 0);
}

const EventLog = struct {
    over_limit: usize = 0,

    fn onEvent(ctx: *anyopaque, event: events.Event) void {
        const self: *EventLog = @ptrCast(@alignCast(ctx));
        switch (event) {
            .resource_rejection => |r| {
                if (r.resource == .attached_fds and r.err == error.ImportedFdsOverLimit) self.over_limit += 1;
            },
            else => {},
        }
    }
};

test "inbound: past max_live_imported_fds the capability arrives without its fd, the fd is closed, and an event says so" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(2);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();
    seam.recorder.retain = true;
    var log: EventLog = .{};
    seam.peer.setObserver(events.Observer.init(&log, EventLog.onEvent));
    seam.peer.setMaxLiveImportedFds(1);

    try seam.deliverCall(0, &.{
        .{ .kind = .sender_hosted, .id = 7, .fd = 0 },
        .{ .kind = .sender_hosted, .id = 8, .fd = 1 },
    }, pipes.writers());
    try testing.expect(seam.recorder.fds[0] != null);
    try testing.expect(seam.recorder.fds[1] == null);
    try testing.expect(seam.peer.caps.hasImport(8));
    try testing.expectEqual(@as(usize, 1), log.over_limit);
    pipes.closeWriters();
    try expectClosed(&pipes, 1);
    try expectOpen(&pipes, 0);
    try seam.peer.releaseImport(7, 1);
    try expectClosed(&pipes, 0);
}

test "inbound: thirdPartyHosted and receiverHosted descriptors never keep an fd" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(2);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();
    seam.recorder.retain = true;

    try seam.deliverCall(0, &.{
        .{ .kind = .third_party, .id = 9, .fd = 0 },
        .{ .kind = .receiver_hosted, .id = seam.bootstrap_id, .fd = 1 },
    }, pipes.writers());
    // The thirdPartyHosted cap arrives as an import of its vine, without fd.
    try testing.expectEqual(@as(u32, 9), seam.recorder.ids[0]);
    try testing.expect(seam.recorder.fds[0] == null);
    try testing.expect(seam.peer.caps.hasImport(9));
    try testing.expect(seam.peer.importFd(9) == null);
    pipes.closeWriters();
    try expectClosed(&pipes, 0);
    try expectClosed(&pipes, 1);
}

const BootstrapReturn = struct {
    import_id: ?u32 = null,
    fd: ?Fd = null,

    fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *BootstrapReturn = @ptrCast(@alignCast(ctx));
        const payload = ret.results orelse return error.NoResults;
        const cap = try payload.content.getCapability();
        switch (try caps.resolveCapability(cap)) {
            .imported => |imported| {
                self.import_id = imported.id;
                if (peer.importFd(imported.id)) |h| self.fd = h.fd;
                try @constCast(caps).retainCapability(cap);
            },
            else => return error.NotAnImport,
        }
    }
};

test "inbound Return: a result cap's fd belongs to its import until the import is released" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();

    var got: BootstrapReturn = .{};
    const question_id = try seam.peer.sendBootstrap(&got, BootstrapReturn.onReturn);
    try seam.deliver(try buildReturn(gpa, question_id, &.{.{ .kind = .sender_hosted, .id = 5, .fd = 0 }}), pipes.writers());
    try testing.expectEqual(@as(?u32, 5), got.import_id);
    const fd = got.fd orelse return error.NoFd;
    try testing.expectEqual(try inodeOf(pipes.write_ends[0]), try inodeOf(fd));
    try testing.expectEqual(fd, seam.peer.importFd(5).?.fd);

    pipes.closeWriters();
    try expectOpen(&pipes, 0);
    try seam.peer.releaseImport(5, 1);
    try testing.expect(seam.peer.importFd(5) == null);
    try expectClosed(&pipes, 0);
}

test "inbound Resolve: a promise import gives no fd until it resolves, then the fd of what it resolved to" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(2);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();
    seam.recorder.retain = true;

    // A promise that came with an fd of its own (pipe 0): still null.
    try seam.deliverCall(0, &.{.{ .kind = .sender_promise, .id = 5, .fd = 0 }}, pipes.writers()[0..1]);
    try testing.expect(seam.recorder.fds[0] == null);
    try testing.expect(seam.peer.importFd(5) == null);

    // It resolves to import 6 with pipe 1.
    try seam.deliver(try buildResolve(gpa, 5, .{ .kind = .sender_hosted, .id = 6, .fd = 0 }), pipes.writers()[1..2]);
    const via_promise = seam.peer.importFd(5) orelse return error.NoFd;
    const direct = seam.peer.importFd(6) orelse return error.NoFd;
    try testing.expectEqual(direct.fd, via_promise.fd);
    try testing.expectEqual(try inodeOf(pipes.write_ends[1]), try inodeOf(direct.fd));

    pipes.closeWriters();
    try expectOpen(&pipes, 0);
    try expectOpen(&pipes, 1);
    // Releasing the promise releases its resolution too: both fds close.
    try seam.peer.releaseImport(5, 1);
    try expectClosed(&pipes, 0);
    try expectClosed(&pipes, 1);
}

test "inbound Resolve for a promise this peer does not hold keeps no fd" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();

    try seam.deliver(try buildResolve(gpa, 40, .{ .kind = .sender_hosted, .id = 41, .fd = 0 }), pipes.writers());
    try testing.expect(seam.peer.importFd(41) == null);
    pipes.closeWriters();
    try expectClosed(&pipes, 0);
}

/// A handler that, while its transport frame dispatches, feeds the peer a
/// loopback Call (to `inner`) whose descriptor names fd index 0.
const Nester = struct {
    inner: u32 = 0,
    gpa: std.mem.Allocator,

    fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *Nester = @ptrCast(@alignCast(ctx));
        const nested = try buildCall(self.gpa, 0x100, self.inner, &.{.{ .kind = .sender_hosted, .id = 8, .fd = 0 }});
        defer self.gpa.free(nested);
        try peer.handleLoopbackFrame(nested);
        try peer.sendReturnEmptyStruct(call.question_id);
    }
};

test "inbound: a frame the peer feeds itself cannot take the fds of the transport frame being dispatched" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();
    seam.recorder.retain = true;

    var nester = Nester{ .inner = seam.bootstrap_id, .gpa = gpa };
    const outer = try seam.peer.addExport(.{ .ctx = &nester, .on_call = Nester.onCall });
    // The transport frame carries one fd that none of its descriptors names.
    const frame = try buildCall(gpa, 0, outer, &.{.{ .kind = .sender_hosted, .id = 7 }});
    try seam.deliver(frame, pipes.writers());
    try testing.expectEqual(@as(usize, 1), seam.recorder.calls);
    try testing.expectEqual(@as(u32, 8), seam.recorder.ids[0]);
    try testing.expect(seam.recorder.fds[0] == null);
    try testing.expect(seam.peer.importFd(8) == null);
    pipes.closeWriters();
    try expectClosed(&pipes, 0);
}

// ---------------------------------------------------------------------------
// Release: every path that drops an import closes its fd
// ---------------------------------------------------------------------------

/// The Release frames the peer sent for import `id`, summed.
fn releasedCount(gpa: std.mem.Allocator, fake: *const FakeTransport, id: u32) !u32 {
    var total: u32 = 0;
    for (fake.sent.items) |s| {
        var decoded = try protocol.DecodedMessage.init(gpa, s.bytes);
        defer decoded.deinit();
        if (decoded.tag != .release) continue;
        const release = try decoded.asRelease();
        if (release.id == id) total += release.reference_count;
    }
    return total;
}

test "close hook: a handoff-pinned import keeps its fd past its last wire ref, until the unpin" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();
    seam.recorder.retain = true;

    try seam.deliverCall(0, &.{.{ .kind = .sender_hosted, .id = 7, .fd = 0 }}, pipes.writers());
    pipes.closeWriters();
    // The handler wrote through the fd; read that byte, so the checks below
    // see only whether a write end is open.
    try expectPipeData(pipes.read_ends[0], "x");
    try seam.peer.noteHandoffImportPin(7);
    try seam.peer.releaseImport(7, 1);
    // The pin withholds the Release, so the remote cannot reuse the id yet.
    try testing.expectEqual(@as(u32, 0), try releasedCount(gpa, &seam.fake, 7));
    try testing.expect(seam.peer.caps.hasImport(7));
    try expectOpen(&pipes, 0);
    try seam.peer.releaseHandoffImportPin(7);
    try testing.expectEqual(@as(u32, 1), try releasedCount(gpa, &seam.fake, 7));
    try testing.expect(!seam.peer.caps.hasImport(7));
    try expectClosed(&pipes, 0);
}

test "close hook: an import pinned by a resolved promise export closes its fd with its last wire ref, since the Release lets the remote reuse the id" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();
    seam.recorder.retain = true;

    try seam.deliverCall(0, &.{.{ .kind = .sender_hosted, .id = 7, .fd = 0 }}, pipes.writers());
    pipes.closeWriters();
    try expectPipeData(pipes.read_ends[0], "x");
    const promise_id = try seam.peer.addPromiseExport();
    try seam.peer.resolvePromiseExportToImport(promise_id, 7);
    try expectOpen(&pipes, 0);
    try seam.peer.releaseImport(7, 1);
    // The promise pin keeps the entry (the promise still routes to it), but
    // the Release for the last wire ref went out: the fd goes with it.
    try testing.expectEqual(@as(u32, 1), try releasedCount(gpa, &seam.fake, 7));
    try testing.expect(seam.peer.caps.hasImport(7));
    try testing.expect(seam.peer.importFd(7) == null);
    try expectClosed(&pipes, 0);
    // The remote held the promise once; its Release destroys the promise
    // export, which drops its pin on import 7.
    try seam.peer.noteExportRef(promise_id);
    try seam.deliver(try buildRelease(gpa, promise_id, 1), &.{});
    try testing.expect(!seam.peer.caps.hasImport(7));
}

test "close hook: an import under a handoff pin and a promise pin keeps its fd while the Release is withheld, and closes it when the unpin sends the Release" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();
    seam.recorder.retain = true;

    try seam.deliverCall(0, &.{.{ .kind = .sender_hosted, .id = 7, .fd = 0 }}, pipes.writers());
    pipes.closeWriters();
    try expectPipeData(pipes.read_ends[0], "x");
    try seam.peer.noteHandoffImportPin(7);
    const promise_id = try seam.peer.addPromiseExport();
    try seam.peer.resolvePromiseExportToImport(promise_id, 7);
    try seam.peer.releaseImport(7, 1);
    try testing.expectEqual(@as(u32, 0), try releasedCount(gpa, &seam.fake, 7));
    try expectOpen(&pipes, 0);
    // The unpin sends the withheld Release; the promise pin keeps the entry.
    try seam.peer.releaseHandoffImportPin(7);
    try testing.expectEqual(@as(u32, 1), try releasedCount(gpa, &seam.fake, 7));
    try testing.expect(seam.peer.caps.hasImport(7));
    try expectClosed(&pipes, 0);
}

test "inbound: after the Release of a promise-pinned import, a new capability that reuses its id gets its own fd, not the old one" {
    // The remote may give a released export id to a new capability at once
    // (C++ does). A promise pin keeps our import entry for that id after
    // the Release, so the new capability lands on the same entry: its fd
    // must not lose to the old capability's.
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(2);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();
    seam.recorder.retain = true;

    // Capability A arrives as import 7 with pipe 0. The app forwards a
    // promise export to it, then drops its own reference.
    try seam.deliverCall(0, &.{.{ .kind = .sender_hosted, .id = 7, .fd = 0 }}, pipes.writers()[0..1]);
    const promise_id = try seam.peer.addPromiseExport();
    try seam.peer.resolvePromiseExportToImport(promise_id, 7);
    try seam.peer.releaseImport(7, 1);
    try testing.expectEqual(@as(u32, 1), try releasedCount(gpa, &seam.fake, 7));

    // The remote reuses export id 7 for capability B, with pipe 1.
    seam.recorder.tag = 'B';
    try seam.deliverCall(1, &.{.{ .kind = .sender_hosted, .id = 7, .fd = 0 }}, pipes.writers()[1..2]);
    const got = seam.recorder.fds[0] orelse return error.NoFdForTheNewCapability;
    try testing.expectEqual(try inodeOf(pipes.write_ends[1]), try inodeOf(got));
    pipes.closeWriters();
    // A's data went to A's pipe, B's to B's; A's fd is closed.
    try expectPipeData(pipes.read_ends[0], "x");
    try expectClosed(&pipes, 0);
    try expectPipeData(pipes.read_ends[1], "B");
    try expectOpen(&pipes, 1);
}

test "close hook: Peer.deinit closes the fd of every import still live" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(2);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    seam.recorder.retain = true;
    seam.deliverCall(0, &.{
        .{ .kind = .sender_hosted, .id = 7, .fd = 0 },
        .{ .kind = .sender_hosted, .id = 8, .fd = 1 },
    }, pipes.writers()) catch |err| {
        seam.deinit();
        return err;
    };
    pipes.closeWriters();
    try expectOpen(&pipes, 0);
    try expectOpen(&pipes, 1);
    seam.deinit();
    try expectClosed(&pipes, 0);
    try expectClosed(&pipes, 1);
}

// ---------------------------------------------------------------------------
// Outbound: which descriptor carries which fd
// ---------------------------------------------------------------------------

/// Answers its call with every export in `exports` (content: a list of caps).
const CapReturner = struct {
    exports: [4]u32 = undefined,
    count: usize = 0,

    fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *CapReturner = @ptrCast(@alignCast(ctx));
        try peer.sendReturnResults(call.question_id, self, build);
    }

    fn build(ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        const self: *CapReturner = @ptrCast(@alignCast(ctx));
        var payload = try ret.payloadTyped();
        const list = try (try payload.initContent()).initPointerList(@intCast(self.count));
        for (self.exports[0..self.count], 0..) |id, i| try list.setCapability(@intCast(i), .{ .id = id });
    }
};

const Noop = struct {
    fn onCall(_: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        try peer.sendReturnEmptyStruct(call.question_id);
    }
};

/// The attachedFd of each descriptor of the Return in `sent` (null = 0xff).
fn returnFdIndexes(gpa: std.mem.Allocator, sent: Sent, out: []?u8) !usize {
    var decoded = try protocol.DecodedMessage.init(gpa, sent.bytes);
    defer decoded.deinit();
    const ret = try decoded.asReturn();
    const list = (ret.results orelse return error.NoResults).cap_table orelse return 0;
    var i: u32 = 0;
    while (i < list.len()) : (i += 1) {
        out[i] = (try protocol.CapDescriptor.fromReader(try list.get(i))).attached_fd;
    }
    return list.len();
}

test "outbound Return: each senderHosted export with an fd gets its own index, in order" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(2);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();

    var noop: u8 = 0;
    var returner: CapReturner = .{};
    const caller = try seam.peer.addExport(.{ .ctx = &returner, .on_call = CapReturner.onCall });
    const w0 = try seam.peer.addExport(.{ .ctx = &noop, .on_call = Noop.onCall });
    const plain = try seam.peer.addExport(.{ .ctx = &noop, .on_call = Noop.onCall });
    const w1 = try seam.peer.addExport(.{ .ctx = &noop, .on_call = Noop.onCall });
    try seam.peer.setExportFd(w0, .{ .fd = pipes.write_ends[0] });
    try seam.peer.setExportFd(w1, .{ .fd = pipes.write_ends[1] });
    returner.exports = .{ w0, plain, w1, w0 };
    returner.count = 4;

    const frame = try buildCall(gpa, 0, caller, &.{});
    defer gpa.free(frame);
    try seam.fake.deliver(&seam.peer, frame, &.{});

    const sent = seam.fake.last();
    var indexes: [4]?u8 = undefined;
    // The encoder lists each export once: w0, plain, w1.
    try testing.expectEqual(@as(usize, 3), try returnFdIndexes(gpa, sent, &indexes));
    try testing.expectEqual(@as(?u8, 0), indexes[0]);
    try testing.expectEqual(@as(?u8, null), indexes[1]);
    try testing.expectEqual(@as(?u8, 1), indexes[2]);
    try testing.expectEqual(@as(usize, 2), sent.fd_count);
    try testing.expectEqual(pipes.write_ends[0], sent.fds[0]);
    try testing.expectEqual(pipes.write_ends[1], sent.fds[1]);
}

test "outbound: on a binding that carries no fds (TCP, QUIC), setExportFd leaves attachedFd at 0xff" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 0);
    defer seam.deinit();

    var noop: u8 = 0;
    var returner: CapReturner = .{};
    const caller = try seam.peer.addExport(.{ .ctx = &returner, .on_call = CapReturner.onCall });
    const w = try seam.peer.addExport(.{ .ctx = &noop, .on_call = Noop.onCall });
    try seam.peer.setExportFd(w, .{ .fd = pipes.write_ends[0] });
    returner.exports[0] = w;
    returner.count = 1;

    const frame = try buildCall(gpa, 0, caller, &.{});
    defer gpa.free(frame);
    try seam.fake.deliver(&seam.peer, frame, &.{});
    var indexes: [1]?u8 = undefined;
    try testing.expectEqual(@as(usize, 1), try returnFdIndexes(gpa, seam.fake.last(), &indexes));
    try testing.expectEqual(@as(?u8, null), indexes[0]);
    try testing.expectEqual(@as(usize, 0), seam.fake.last().fd_count);
}

test "outbound Resolve: a promise export resolved to an export with an fd carries it" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();

    var noop: u8 = 0;
    const w = try seam.peer.addExport(.{ .ctx = &noop, .on_call = Noop.onCall });
    try seam.peer.setExportFd(w, .{ .fd = pipes.write_ends[0] });
    const promise_id = try seam.peer.addPromiseExport();
    try seam.peer.resolvePromiseExportToExport(promise_id, w);

    const sent = seam.fake.last();
    var decoded = try protocol.DecodedMessage.init(gpa, sent.bytes);
    defer decoded.deinit();
    const resolve = try decoded.asResolve();
    try testing.expectEqual(promise_id, resolve.promise_id);
    try testing.expectEqual(@as(?u8, 0), resolve.cap.?.attached_fd);
    try testing.expectEqual(@as(usize, 1), sent.fd_count);
    try testing.expectEqual(pipes.write_ends[0], sent.fds[0]);
}

test "outbound Bootstrap Return: a bootstrap export with an fd carries it" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();
    try seam.peer.setExportFd(seam.bootstrap_id, .{ .fd = pipes.write_ends[0] });

    var mb = protocol.MessageBuilder.init(gpa);
    defer mb.deinit();
    try mb.buildBootstrap(3);
    try seam.deliver(try mb.finish(), &.{});

    const sent = seam.fake.last();
    var indexes: [1]?u8 = undefined;
    try testing.expectEqual(@as(usize, 1), try returnFdIndexes(gpa, sent, &indexes));
    try testing.expectEqual(@as(?u8, 0), indexes[0]);
    try testing.expectEqual(@as(usize, 1), sent.fd_count);
    try testing.expectEqual(pipes.write_ends[0], sent.fds[0]);
    var decoded = try protocol.DecodedMessage.init(gpa, sent.bytes);
    defer decoded.deinit();
    const ret = try decoded.asReturn();
    try testing.expectEqual(@as(u32, 3), ret.answer_id);
    const cap = try protocol.CapDescriptor.fromReader(try ret.results.?.cap_table.?.get(0));
    try testing.expectEqual(seam.bootstrap_id, cap.id.?);
}

fn deliverBootstrap(seam: *SeamPeer, question_id: u32) !void {
    var mb = protocol.MessageBuilder.init(seam.fake.allocator);
    defer mb.deinit();
    try mb.buildBootstrap(question_id);
    try seam.deliver(try mb.finish(), &.{});
}

test "outbound Bootstrap Return: an fd error answers with an exception Return and the connection stays up" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();
    try seam.peer.setExportFd(seam.bootstrap_id, .{ .fd = pipes.write_ends[0] });
    const refs_before = seam.peer.exports.get(seam.bootstrap_id).?.ref_count;

    // An fd error refuses only this Return (`Connection.sendFrameWithFds`):
    // the Bootstrap gets an exception instead, typed for a retry when the
    // fds lacked room.
    const Case = struct { err: anyerror, kind: protocol.ExceptionType };
    const cases = [_]Case{
        .{ .err = error.FdQueueFull, .kind = .overloaded },
        .{ .err = error.ProcessFdQuotaExceeded, .kind = .overloaded },
        .{ .err = error.InvalidFd, .kind = .failed },
    };
    for (cases, 0..) |case, i| {
        const question_id: u32 = @intCast(10 + i);
        const sent_before = seam.fake.sent.items.len;
        seam.fake.fds_error = case.err;
        try deliverBootstrap(&seam, question_id);

        try testing.expectEqual(sent_before + 1, seam.fake.sent.items.len);
        const sent = seam.fake.last();
        try testing.expectEqual(@as(usize, 0), sent.fd_count);
        var decoded = try protocol.DecodedMessage.init(gpa, sent.bytes);
        defer decoded.deinit();
        const ret = try decoded.asReturn();
        try testing.expectEqual(question_id, ret.answer_id);
        try testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
        try testing.expectEqualStrings(@errorName(case.err), ret.exception.?.reason);
        try testing.expectEqual(case.kind, ret.exception.?.kind());
        // Nothing went out naming the export: its ref is undone, and no
        // results answer is recorded.
        try testing.expectEqual(refs_before, seam.peer.exports.get(seam.bootstrap_id).?.ref_count);
        try testing.expect(!seam.peer.resolved_answers.contains(question_id));
        try testing.expectEqual(@as(usize, 0), seam.fake.closes);
    }

    // A send error that is not about the fds still leaves dispatch, as for
    // a frame without fds (a `Connection` reports it to `on_error`, and the
    // sessions close on that).
    seam.fake.fds_error = error.BrokenPipe;
    try testing.expectError(error.BrokenPipe, deliverBootstrap(&seam, 20));
    try testing.expectEqual(refs_before, seam.peer.exports.get(seam.bootstrap_id).?.ref_count);
}

test "outbound Bootstrap Return: after an fd error, the next Bootstrap carries the fd" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();
    try seam.peer.setExportFd(seam.bootstrap_id, .{ .fd = pipes.write_ends[0] });

    seam.fake.fds_error = error.FdQueueFull;
    try deliverBootstrap(&seam, 1);
    seam.fake.fds_error = null;
    try deliverBootstrap(&seam, 2);

    const sent = seam.fake.last();
    var indexes: [1]?u8 = undefined;
    try testing.expectEqual(@as(usize, 1), try returnFdIndexes(gpa, sent, &indexes));
    try testing.expectEqual(@as(?u8, 0), indexes[0]);
    try testing.expectEqual(@as(usize, 1), sent.fd_count);
    try testing.expectEqual(pipes.write_ends[0], sent.fds[0]);
    try testing.expectEqual(@as(usize, 0), seam.fake.closes);
}

test "outbound: clearExportFd and a released export stop attaching; setExportFd checks its arguments" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();

    var noop: u8 = 0;
    var returner: CapReturner = .{};
    const caller = try seam.peer.addExport(.{ .ctx = &returner, .on_call = CapReturner.onCall });
    const w = try seam.peer.addExport(.{ .ctx = &noop, .on_call = Noop.onCall });
    try testing.expectError(error.UnknownExport, seam.peer.setExportFd(9999, .{ .fd = pipes.write_ends[0] }));
    try testing.expectError(error.InvalidFd, seam.peer.setExportFd(w, .{ .fd = -1 }));
    try seam.peer.setExportFd(w, .{ .fd = pipes.write_ends[0] });
    seam.peer.clearExportFd(w);
    returner.exports[0] = w;
    returner.count = 1;
    const frame = try buildCall(gpa, 0, caller, &.{});
    defer gpa.free(frame);
    try seam.fake.deliver(&seam.peer, frame, &.{});
    try testing.expectEqual(@as(usize, 0), seam.fake.last().fd_count);

    // Released by the remote (after it finished the call that returned it):
    // the export goes, and its fd entry with it.
    try seam.peer.setExportFd(w, .{ .fd = pipes.write_ends[0] });
    try seam.deliver(try buildFinish(gpa, 0), &.{});
    try seam.deliver(try buildRelease(gpa, w, 1), &.{});
    try testing.expect(!seam.peer.exports.contains(w));
    try testing.expectError(error.UnknownExport, seam.peer.setExportFd(w, .{ .fd = pipes.write_ends[0] }));
}

test "outbound: a send_frame_override never carries fds" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    var seam: SeamPeer = undefined;
    try seam.init(gpa, 253);
    defer seam.deinit();
    seam.peer.setSendFrameOverride(&seam.fake, FakeTransport.send);

    var noop: u8 = 0;
    var returner: CapReturner = .{};
    const caller = try seam.peer.addExport(.{ .ctx = &returner, .on_call = CapReturner.onCall });
    const w = try seam.peer.addExport(.{ .ctx = &noop, .on_call = Noop.onCall });
    try seam.peer.setExportFd(w, .{ .fd = pipes.write_ends[0] });
    returner.exports[0] = w;
    returner.count = 1;
    const frame = try buildCall(gpa, 0, caller, &.{});
    defer gpa.free(frame);
    try seam.fake.deliver(&seam.peer, frame, &.{});
    var indexes: [1]?u8 = undefined;
    try testing.expectEqual(@as(usize, 1), try returnFdIndexes(gpa, seam.fake.last(), &indexes));
    try testing.expectEqual(@as(?u8, null), indexes[0]);
    try testing.expectEqual(@as(usize, 0), seam.fake.last().fd_count);
}

// ---------------------------------------------------------------------------
// Real sockets
// ---------------------------------------------------------------------------

/// Server side of the socket tests: the bootstrap's method 0 returns a new
/// export carrying `pipe_w`.
const PipeServer = struct {
    pipe_w: Fd,
    returner: CapReturner = .{},
    noop: u8 = 0,

    fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *PipeServer = @ptrCast(@alignCast(ctx));
        const w = try peer.addExport(.{ .ctx = &self.noop, .on_call = Noop.onCall });
        try peer.setExportFd(w, .{ .fd = self.pipe_w });
        self.returner.exports[0] = w;
        self.returner.count = 1;
        try CapReturner.onCall(&self.returner, peer, call, caps);
    }
};

/// Client side: bootstrap, call method 0, write `message` through the fd of
/// the returned cap, release it, close.
const PipeClient = struct {
    peer: *Peer = undefined,
    message: []const u8 = "ping",
    descriptor_fd: ?u8 = 0xee,
    had_fd: bool = false,
    wrote: bool = false,
    failed: ?anyerror = null,

    fn close(self: *PipeClient) void {
        if (!self.peer.isAttachedTransportClosing()) self.peer.closeAttachedTransport();
    }

    fn onBootstrap(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *PipeClient = @ptrCast(@alignCast(ctx));
        errdefer self.close();
        const payload = ret.results orelse return error.NoResults;
        const cap = try payload.content.getCapability();
        const target = try caps.resolveCapability(cap);
        _ = try peer.sendCallResolved(target, iface_id, 0, self, null, onCallReturn);
    }

    fn onCallReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *PipeClient = @ptrCast(@alignCast(ctx));
        defer self.close();
        self.run(peer, ret, caps) catch |err| {
            self.failed = err;
        };
    }

    fn run(self: *PipeClient, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) !void {
        const payload = ret.results orelse return error.NoResults;
        const list = payload.cap_table orelse return error.NoCapTable;
        self.descriptor_fd = (try protocol.CapDescriptor.fromReader(try list.get(0))).attached_fd;
        const caps_list = try payload.content.getPointerList();
        const cap = try caps_list.getCapability(0);
        const import_id = switch (try caps.resolveCapability(cap)) {
            .imported => |imported| imported.id,
            else => return error.NotAnImport,
        };
        try @constCast(caps).retainCapability(cap);
        if (peer.importFd(import_id)) |h| {
            self.had_fd = true;
            try writeAll(h.fd, self.message);
            self.wrote = true;
        }
        try peer.releaseImport(import_id, 1);
        try testing.expect(peer.importFd(import_id) == null);
    }
};

test "unix sessions: the server attaches a pipe write end to a returned cap, the client writes through importFd, the server reads it" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    const io = testing.io;
    try support.waitCloserIdle(support.closed_wait_ms);
    const baseline = support.FdSnapshot.take();

    // A private (0700) directory under /tmp, removed with everything in it.
    var dir_buf: [64]u8 = undefined;
    const dir = try std.fmt.bufPrint(&dir_buf, "/tmp/czfp-{d}", .{sys.getpid()});
    std.Io.Dir.cwd().deleteTree(testing.io, dir) catch {};
    var dir_z: [65]u8 = undefined;
    @memcpy(dir_z[0..dir.len], dir);
    dir_z[dir.len] = 0;
    _ = try support.check(sys.mkdir(@ptrCast(&dir_z), 0o700), "mkdir");
    defer std.Io.Dir.cwd().deleteTree(testing.io, dir) catch {};
    var path_buf: [96]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/s", .{dir});

    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();

    const fd_passing: unix.FdPassing = .{ .max_fds_per_message = 4, .max_live_imported_fds = 8 };
    var listener = try unix.listen(gpa, io, path, .{ .fd_passing = fd_passing });
    var listener_open = true;
    defer if (listener_open) listener.close();

    const Server = struct {
        listener: *tcp.Listener,
        handler: PipeServer,
        err: ?anyerror = null,
        fn main(self: *@This()) void {
            var session = tcp.ServerSession.accept(testing.allocator, self.listener, .{}) catch |err| {
                self.err = err;
                return;
            };
            defer session.deinit();
            _ = session.peer.setBootstrap(.{ .ctx = &self.handler, .on_call = PipeServer.onCall }) catch |err| {
                self.err = err;
                return;
            };
            session.run();
        }
    };
    var server = Server{ .listener = &listener, .handler = .{ .pipe_w = pipes.write_ends[0] } };
    const server_thread = try std.Thread.spawn(.{}, Server.main, .{&server});

    var client = PipeClient{};
    const session = unix.connect(gpa, io, path, .{ .fd_passing = fd_passing }) catch |err| {
        listener.close();
        server_thread.join();
        return err;
    };
    {
        // On every exit, a failed send too: stop the client (which ends the
        // server's session), join the server, then free both ends.
        defer {
            session.requestStop();
            server_thread.join();
            session.deinit();
            listener.close();
            listener_open = false;
        }
        client.peer = &session.peer;
        _ = try session.peer.sendBootstrap(&client, PipeClient.onBootstrap);
        session.run();
    }

    try testing.expectEqual(@as(?anyerror, null), server.err);
    try testing.expectEqual(@as(?anyerror, null), client.failed);
    try testing.expectEqual(@as(?u8, 0), client.descriptor_fd);
    try testing.expect(client.had_fd);
    try testing.expect(client.wrote);
    try expectPipeData(pipes.read_ends[0], "ping");
    // The server's own write end is the last copy: the sender's dup and the
    // client's received fd are closed.
    pipes.closeWriters();
    try expectClosed(&pipes, 0);
    pipes.closeAll();
    try support.waitCloserIdle(support.closed_wait_ms);
    try support.expectBackAtBaseline(baseline);
}

test "TCP: setExportFd leaves attachedFd at 0xff and the client gets no fd" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    const io = testing.io;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();

    const pair = try tcp.createLoopbackSocketPair(io);
    var server_conn = tcp.Connection.init(gpa, io, pair[0], .{}) catch |err| {
        tcp.closeFd(io, pair[0]);
        tcp.closeFd(io, pair[1]);
        return err;
    };
    defer server_conn.deinit();
    var client_conn = tcp.Connection.init(gpa, io, pair[1], .{}) catch |err| {
        tcp.closeFd(io, pair[1]);
        return err;
    };
    defer client_conn.deinit();
    try testing.expectEqual(@as(u8, 0), server_conn.maxOutboundFds());
    try testing.expectError(error.FdPassingUnsupported, server_conn.enableFdPassing(4));

    var handler = PipeServer{ .pipe_w = pipes.write_ends[0] };
    var server_peer = Peer.init(gpa, &server_conn);
    defer {
        // After the server thread is joined (below).
        server_peer.adoptOwnerThread();
        server_conn.adoptOwnerThread();
        _ = server_peer.takeAttachedConnection(*tcp.Connection);
        server_peer.deinit();
    }
    _ = try server_peer.setBootstrap(.{ .ctx = &handler, .on_call = PipeServer.onCall });
    var client_peer = Peer.init(gpa, &client_conn);
    defer {
        _ = client_peer.takeAttachedConnection(*tcp.Connection);
        client_peer.deinit();
    }

    const Runner = struct {
        conn: *tcp.Connection,
        peer: *Peer,
        fn main(self: *@This()) void {
            self.peer.adoptOwnerThread();
            self.conn.adoptOwnerThread();
            self.peer.start(null, null, null);
            self.conn.run();
        }
    };
    var runner = Runner{ .conn = &server_conn, .peer = &server_peer };
    const server_thread = try std.Thread.spawn(.{}, Runner.main, .{&runner});
    // On every exit: the client's close normally ended the server's loop
    // already; a failed check ends it here.
    defer {
        server_conn.requestClose();
        server_thread.join();
    }

    var client = PipeClient{ .peer = &client_peer };
    client_peer.start(null, null, null);
    _ = try client_peer.sendBootstrap(&client, PipeClient.onBootstrap);
    client_conn.run();

    try testing.expectEqual(@as(?anyerror, null), client.failed);
    try testing.expectEqual(@as(?u8, null), client.descriptor_fd);
    try testing.expect(!client.had_fd);
}

const ConnErrors = struct {
    var count: usize = 0;
    fn onError(_: *tcp.Connection, _: anyerror) void {
        count += 1;
    }
};

test "Connection.sendFrameWithFds: an fd error refuses the message and leaves the connection up" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    const io = testing.io;
    const pair = try support.socketPair();
    defer support.closeFd(pair[1]);
    var conn = tcp.Connection.init(gpa, io, .{ .handle = pair[0] }, .{}) catch |err| {
        support.closeFd(pair[0]);
        return err;
    };
    defer conn.deinit();
    ConnErrors.count = 0;
    conn.on_error = ConnErrors.onError;
    const frame = try support.buildFrame(gpa, 7);
    defer gpa.free(frame);

    // Fd passing off: an AF_UNIX connection sends no fds (as C++), and
    // refusing them is no connection error either.
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    try testing.expectError(error.FdPassingUnsupported, conn.sendFrameWithFds(frame, &.{.{ .fd = pipes.write_ends[0] }}));
    try testing.expectEqual(@as(usize, 0), ConnErrors.count);
    try testing.expectEqual(@as(u8, 0), conn.maxOutboundFds());
    try conn.enableFdPassing(4);
    try testing.expectEqual(@as(u8, 253), conn.maxOutboundFds());
    try conn.transport.startWriter();

    // Not an open fd: the queue's dup fails, and only this message is refused.
    try testing.expectError(error.InvalidFd, conn.sendFrameWithFds(frame, &.{.{ .fd = 4000 }}));
    try testing.expectEqual(@as(usize, 0), ConnErrors.count);
    try testing.expect(!conn.isClosing());
    try conn.sendFrame(frame);
}

/// A raw AF_UNIX client: sends frames, and reads frames while collecting
/// every fd that arrives on the socket.
const RawClient = struct {
    sock: Fd,
    framer: framing.Framer,
    fds: [16]Fd = @splat(-1),
    fd_count: usize = 0,

    fn send(self: *RawClient, frame: []const u8) !void {
        try support.sendWithFds(self.sock, frame, &.{});
    }

    /// The next frame (owned by the caller).
    fn next(self: *RawClient) ![]const u8 {
        var buf: [4096]u8 = undefined;
        var control: [fd_passing_control_bytes]u8 align(std.Io.net.cmsg_align) = undefined;
        while (true) {
            if (try self.framer.popFrame()) |frame| return frame;
            var got: [16]Fd = undefined;
            const r = try unix.fd_io.recvWithFds(self.sock, &buf, &control, &got);
            if (r.data_len == 0) return error.EndOfStream;
            for (got[0..r.fd_count]) |fd| {
                if (self.fd_count < self.fds.len) {
                    self.fds[self.fd_count] = fd;
                    self.fd_count += 1;
                } else support.closeFd(fd);
            }
            try self.framer.push(buf[0..r.data_len]);
        }
    }

    fn closeFds(self: *RawClient) void {
        for (self.fds[0..self.fd_count]) |fd| support.closeFd(fd);
        self.fd_count = 0;
    }
};

const fd_passing_control_bytes = 256;

fn onWakeNoop(_: *tcp.Connection) void {}

/// What a raw AF_UNIX client saw of the Return to its call (`rawPipeCall`).
const RawSeen = struct {
    /// The returned cap's `attachedFd` (null = 0xff).
    attached_fd: ?u8,
    /// How many fds crossed the socket, over both Returns.
    fd_count: usize,
    /// The first fd that crossed is a copy of the pipe's write end.
    is_pipe: bool,
    /// The server's outbound fd limit, from its connection and from its
    /// peer's binding: before `fd_passing` is applied, and after.
    conn_limit: [2]u8,
    peer_limit: [2]u8,
};

/// A server `Peer` on one end of an AF_UNIX socketpair, with its wake
/// socketpair open and `PipeServer` (which attaches a pipe write end to the
/// cap it returns) as bootstrap. A raw client on the other end bootstraps
/// and calls method 0. With `fd_passing` set, the server's connection turns
/// fd passing on after the `Peer` is attached, before it runs: the peer
/// reads the connection's outbound limit per frame, not once at attach.
fn rawPipeCall(gpa: std.mem.Allocator, fd_passing: ?u8) !RawSeen {
    const io = testing.io;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();

    const pair = try support.socketPair();
    var server_conn = tcp.Connection.init(gpa, io, .{ .handle = pair[0] }, .{}) catch |err| {
        support.closeFd(pair[0]);
        support.closeFd(pair[1]);
        return err;
    };
    defer server_conn.deinit();
    defer support.closeFd(pair[1]);
    try server_conn.enableWake(onWakeNoop);

    var handler = PipeServer{ .pipe_w = pipes.write_ends[0] };
    var server_peer = Peer.init(gpa, &server_conn);
    defer {
        // Runs after the server thread is joined (below): take the peer
        // and connection back to this thread and tear the peer down.
        server_peer.adoptOwnerThread();
        server_conn.adoptOwnerThread();
        _ = server_peer.takeAttachedConnection(*tcp.Connection);
        server_peer.deinit();
    }
    _ = try server_peer.setBootstrap(.{ .ctx = &handler, .on_call = PipeServer.onCall });

    var conn_limit: [2]u8 = undefined;
    var peer_limit: [2]u8 = undefined;
    conn_limit[0] = server_conn.maxOutboundFds();
    peer_limit[0] = server_peer.transport.outboundFdLimit();
    if (fd_passing) |max| try server_conn.enableFdPassing(max);
    conn_limit[1] = server_conn.maxOutboundFds();
    peer_limit[1] = server_peer.transport.outboundFdLimit();

    const Runner = struct {
        conn: *tcp.Connection,
        peer: *Peer,
        fn main(self: *@This()) void {
            self.peer.adoptOwnerThread();
            self.conn.adoptOwnerThread();
            self.peer.start(null, null, null);
            self.conn.run();
        }
    };
    var runner = Runner{ .conn = &server_conn, .peer = &server_peer };
    const server_thread = try std.Thread.spawn(.{}, Runner.main, .{&runner});
    // On every exit, also a failed check: end the session, join the thread.
    defer {
        _ = sys.shutdown(pair[1], posix.SHUT.RDWR);
        server_thread.join();
    }

    var raw = RawClient{ .sock = pair[1], .framer = framing.Framer.init(gpa) };
    defer raw.framer.deinit();
    defer raw.closeFds();

    // Bootstrap, then call method 0 on it.
    {
        var mb = protocol.MessageBuilder.init(gpa);
        defer mb.deinit();
        try mb.buildBootstrap(0);
        const bytes = try mb.finish();
        defer gpa.free(bytes);
        try raw.send(bytes);
    }
    const boot_cap = blk: {
        const frame = try raw.next();
        defer gpa.free(frame);
        var decoded = try protocol.DecodedMessage.init(gpa, frame);
        defer decoded.deinit();
        const ret = try decoded.asReturn();
        const list = ret.results.?.cap_table.?;
        break :blk (try protocol.CapDescriptor.fromReader(try list.get(0))).id.?;
    };
    {
        const bytes = try buildCall(gpa, 1, boot_cap, &.{});
        defer gpa.free(bytes);
        try raw.send(bytes);
    }
    const attached_fd = blk: {
        const frame = try raw.next();
        defer gpa.free(frame);
        var decoded = try protocol.DecodedMessage.init(gpa, frame);
        defer decoded.deinit();
        const ret = try decoded.asReturn();
        try testing.expectEqual(@as(u32, 1), ret.answer_id);
        const list = ret.results.?.cap_table.?;
        break :blk (try protocol.CapDescriptor.fromReader(try list.get(0))).attached_fd;
    };
    const is_pipe = raw.fd_count != 0 and try inodeOf(pipes.write_ends[0]) == try inodeOf(raw.fds[0]);
    return .{
        .attached_fd = attached_fd,
        .fd_count = raw.fd_count,
        .is_pipe = is_pipe,
        .conn_limit = conn_limit,
        .peer_limit = peer_limit,
    };
}

test "the wake socketpair's fds never cross the socket: only the attached fd does" {
    if (comptime !supported) return error.SkipZigTest;
    const seen = try rawPipeCall(testing.allocator, 4);
    try testing.expectEqual(@as(?u8, 0), seen.attached_fd);
    // Exactly one fd crossed, and it is the pipe, not a wake fd.
    try testing.expectEqual(@as(usize, 1), seen.fd_count);
    try testing.expect(seen.is_pipe);
    // Fd passing came on after the peer attached; the peer saw it.
    try testing.expectEqual([2]u8{ 0, 253 }, seen.conn_limit);
    try testing.expectEqual([2]u8{ 0, 253 }, seen.peer_limit);
}

test "an AF_UNIX connection without fd passing on sends no fds: attachedFd stays 0xff" {
    if (comptime !supported) return error.SkipZigTest;
    // A receiver that did not ask for fds still gets them installed on
    // macOS, so a sender holds them back unless its own connection opted
    // in, as C++ does.
    const seen = try rawPipeCall(testing.allocator, null);
    try testing.expectEqual(@as(?u8, null), seen.attached_fd);
    try testing.expectEqual(@as(usize, 0), seen.fd_count);
    try testing.expectEqual([2]u8{ 0, 0 }, seen.conn_limit);
    try testing.expectEqual([2]u8{ 0, 0 }, seen.peer_limit);
}

// ---------------------------------------------------------------------------
// Targets without fd passing
// ---------------------------------------------------------------------------

test "setExportFd returns FdPassingUnsupported and importFd null where fd passing is compiled out" {
    if (comptime supported) return error.SkipZigTest;
    var peer = Peer.initDetached(testing.allocator);
    defer peer.deinit();
    var noop: u8 = 0;
    const id = try peer.addExport(.{ .ctx = &noop, .on_call = Noop.onCall });
    try testing.expectError(error.FdPassingUnsupported, peer.setExportFd(id, .{ .fd = {} }));
    try testing.expect(peer.importFd(0) == null);
}
