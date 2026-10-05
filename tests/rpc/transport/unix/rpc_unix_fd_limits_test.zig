//! Limits and fault injection for fd passing (sprint item 13 of
//! docs/sprint-plan-2026-10-04.md).
//!
//! - The process fd budget (`fd_io.budget`): one count of every fd fd
//!   passing keeps alive (frame fds, imports, sent dups, the closer's
//!   queues) and one limit. Over it a received fd goes to the closer with an
//!   event and the connection stays; a send gets a backpressure error.
//! - The per-connection live-fd cap (`FdPassing.max_live_imported_fds`)
//!   together with the budget: N connections that each try to fill their cap
//!   cannot fill the fd table, and `accept` still works.
//! - Injected faults, none of which may leak an fd or a budget unit: OOM at
//!   every allocation of the Peer's fd paths and of the closer's queues,
//!   EMFILE while a sent fd is dup'd, a peer torn down with live imports,
//!   and Linux ETOOMANYREFS (typed backpressure; the connection stays). The
//!   writer error mid-batch, and EMFILE on the receive side, are pinned in
//!   `rpc_unix_fd_send_test.zig`, `rpc_unix_fd_drain_test.zig` and
//!   `rpc_unix_fd_boundary_test.zig`; every suite's baseline check
//!   (`support.expectBackAtBaseline`) now covers the budget too.
//!
//! Linux and macOS run it; other targets compile it and skip. Tests that
//! shrink the fd table do it themselves, with `setrlimit`, and restore it.

const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");
const support = @import("fd_test_support.zig");

const testing = std.testing;
const posix = support.posix;
const sys = support.sys;
const fd_io = support.fd_io;
const budget = fd_io.budget;
const events = support.events;
const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const rpc_peer = capnpc.rpc.peer;
const Peer = rpc_peer.Peer;
const FdHandle = rpc_peer.FdHandle;
const tcp = capnpc.rpc.transport.tcp;
const unix = capnpc.rpc.transport.unix;
const Transport = tcp.Transport;
const Fd = support.Fd;
const supported = support.supported;

const iface_id: u64 = 0x5eed_f00d_0013_0013;

/// Starts the closer threads (they open no fds) and waits until they are
/// idle, so a baseline taken next holds nothing in flight.
fn warmUp() !void {
    try fd_io.closer.ensureStarted();
    try support.waitCloserIdle(5000);
}

/// Reads from `transport` until `len` bytes (one whole frame, with fd
/// passing on) came in.
fn readFrame(transport: *Transport, len: usize) !void {
    var got: usize = 0;
    while (got < len) {
        const n = try transport.read();
        if (n == 0) return error.UnexpectedEndOfStream;
        got += n;
    }
}

/// A message larger than any AF_UNIX socket buffer, so a writer blocks in it
/// until the peer reads or closes.
const blocking_len = 16 * 1024 * 1024;

/// Waits until the writer took every queued item into its batch.
fn waitBatchTaken(transport: *Transport) !void {
    const start = support.nowNs();
    while (true) {
        const stats = transport.queueStats();
        if (stats.items == 0 and stats.bytes != 0) return;
        if (support.msSince(start) >= 5000) return error.WriterStalled;
        support.sleepMs(2);
    }
}

/// Reads exactly `want` bytes from the raw peer socket `sock`, closes every
/// fd that comes with them, and returns how many came.
fn readAndCloseFds(sock: Fd, want: usize) !usize {
    var data: [64 * 1024]u8 = undefined;
    var control: [fd_io.controlSpace(fd_io.max_fds_per_read)]u8 align(8) = undefined;
    var fds: [fd_io.max_fds_per_read]Fd = undefined;
    var bytes: usize = 0;
    var n_fds: usize = 0;
    while (bytes < want) {
        var pfd = [1]posix.pollfd{.{ .fd = sock, .events = posix.POLL.IN, .revents = 0 }};
        if (try support.check(sys.poll(&pfd, 1, 5000), "poll") == 0) return error.PeerReadTimedOut;
        const got = try fd_io.recvWithFds(sock, data[0..@min(data.len, want - bytes)], &control, &fds);
        if (got.data_len == 0) return error.UnexpectedEndOfStream;
        for (fds[0..got.fd_count]) |fd| support.closeFd(fd);
        n_fds += got.fd_count;
        bytes += got.data_len;
    }
    return n_fds;
}

/// True when `sock` has bytes to read within `timeout_ms`.
fn hasData(sock: Fd, timeout_ms: i32) bool {
    var pfd = [1]posix.pollfd{.{ .fd = sock, .events = posix.POLL.IN, .revents = 0 }};
    const rc = sys.poll(&pfd, 1, timeout_ms);
    return posix.errno(rc) == .SUCCESS and rc != 0;
}

/// The soft RLIMIT_NOFILE, saved to be restored, and a way to leave exactly
/// `free` fd numbers usable.
const FdTable = struct {
    saved: posix.rlimit,
    fillers: [1024]Fd = undefined,
    n_fillers: usize = 0,

    fn save() !FdTable {
        return .{ .saved = try posix.getrlimit(.NOFILE) };
    }

    /// Set the soft limit just above the highest open fd, fill every hole
    /// below it with dups of `any_fd`, then free the top `free` fillers: the
    /// next `free` fds the process opens fit, the one after gets EMFILE.
    fn leave(self: *FdTable, free: usize, any_fd: Fd) !void {
        var lowered = self.saved;
        lowered.cur = support.FdSnapshot.take().highest() + 1 + free;
        // Grow the fd table past the new limit first. macOS (27, measured)
        // grows it in steps and refuses a step that would pass the soft
        // limit: a limit between its size and the next step gives EMFILE
        // early (a fresh process at limit 40 stops at 25 open fds).
        const high: Fd = @intCast(lowered.cur + 64);
        _ = try support.check(sys.dup2(any_fd, high), "dup2");
        support.closeFd(high);
        try posix.setrlimit(.NOFILE, lowered);
        while (self.n_fillers < self.fillers.len) {
            const rc = sys.dup(any_fd);
            if (posix.errno(rc) != .SUCCESS) break;
            self.fillers[self.n_fillers] = @intCast(rc);
            self.n_fillers += 1;
        }
        if (self.n_fillers < free or self.n_fillers == self.fillers.len) {
            std.debug.print("fd table setup: limit {d}, {d} fillers for {d} free\n", .{ lowered.cur, self.n_fillers, free });
            return error.FdTableSetupFailed;
        }
        for (0..free) |_| {
            self.n_fillers -= 1;
            support.closeFd(self.fillers[self.n_fillers]);
        }
    }

    fn restore(self: *FdTable) void {
        for (self.fillers[0..self.n_fillers]) |fd| support.closeFd(fd);
        self.n_fillers = 0;
        posix.setrlimit(.NOFILE, self.saved) catch |err| {
            std.debug.print("could not restore RLIMIT_NOFILE: {t}\n", .{err});
        };
    }
};

/// Serializes every call into `child`. `std.testing.FailingAllocator` is not
/// thread-safe, and a transport's writer thread frees what its queue held
/// while the test thread allocates.
const LockedAllocator = struct {
    child: std.mem.Allocator,
    mu: std.atomic.Mutex = .unlocked,

    fn allocator(self: *LockedAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = resize,
            .remap = remap,
            .free = free,
        } };
    }

    fn lock(ctx: *anyopaque) *LockedAllocator {
        const self: *LockedAllocator = @ptrCast(@alignCast(ctx));
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        return self;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self = lock(ctx);
        defer self.mu.unlock();
        return self.child.rawAlloc(len, alignment, ret_addr);
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self = lock(ctx);
        defer self.mu.unlock();
        return self.child.rawResize(memory, alignment, new_len, ret_addr);
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self = lock(ctx);
        defer self.mu.unlock();
        return self.child.rawRemap(memory, alignment, new_len, ret_addr);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self = lock(ctx);
        defer self.mu.unlock();
        self.child.rawFree(memory, alignment, ret_addr);
    }
};

/// Records `.backpressure` and `.resource_rejection` events on
/// `.attached_fds`. Thread-safe: a writer thread emits some of them.
const SharedRecorder = struct {
    mu: std.atomic.Mutex = .unlocked,
    backpressure: [32]events.BackpressureEvent = undefined,
    n_backpressure: usize = 0,
    rejections: [32]events.ResourceRejectionEvent = undefined,
    n_rejections: usize = 0,

    fn observer(self: *SharedRecorder) events.Observer {
        return events.Observer.init(self, onEvent);
    }

    fn lock(self: *SharedRecorder) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
    }

    fn onEvent(ctx: *anyopaque, event: events.Event) void {
        const self: *SharedRecorder = @ptrCast(@alignCast(ctx));
        self.lock();
        defer self.mu.unlock();
        switch (event) {
            .backpressure => |b| if (b.resource == .attached_fds and self.n_backpressure < self.backpressure.len) {
                self.backpressure[self.n_backpressure] = b;
                self.n_backpressure += 1;
            },
            .resource_rejection => |r| if (r.resource == .attached_fds and self.n_rejections < self.rejections.len) {
                self.rejections[self.n_rejections] = r;
                self.n_rejections += 1;
            },
            else => {},
        }
    }

    fn backpressureWith(self: *SharedRecorder, err: anyerror) ?events.BackpressureEvent {
        self.lock();
        defer self.mu.unlock();
        for (self.backpressure[0..self.n_backpressure]) |b| {
            if (b.err == err) return b;
        }
        return null;
    }

    fn countBackpressure(self: *SharedRecorder, err: anyerror) usize {
        self.lock();
        defer self.mu.unlock();
        var n: usize = 0;
        for (self.backpressure[0..self.n_backpressure]) |b| {
            if (b.err == err) n += 1;
        }
        return n;
    }

    fn countRejections(self: *SharedRecorder, err: anyerror) usize {
        self.lock();
        defer self.mu.unlock();
        var n: usize = 0;
        for (self.rejections[0..self.n_rejections]) |r| {
            if (r.err == err) n += 1;
        }
        return n;
    }

    /// Waits until a `.backpressure` event with `err` came in.
    fn waitBackpressure(self: *SharedRecorder, err: anyerror, timeout_ms: i64) !events.BackpressureEvent {
        const start = support.nowNs();
        while (true) {
            if (self.backpressureWith(err)) |b| return b;
            if (support.msSince(start) >= timeout_ms) return error.EventMissing;
            support.sleepMs(2);
        }
    }
};

// ---------------------------------------------------------------------------
// The budget itself
// ---------------------------------------------------------------------------

test "budget: the default limit is a quarter of the soft RLIMIT_NOFILE, at least 16, and the limit is read once" {
    if (comptime !supported) {
        try testing.expectEqual(@as(usize, 0), budget.limit());
        return error.SkipZigTest;
    }
    const in_force = budget.limit();
    var table = try FdTable.save();
    defer table.restore();
    var changed = table.saved;
    changed.cur = @min(table.saved.max, 1024);
    try posix.setrlimit(.NOFILE, changed);
    try testing.expectEqual(@as(usize, @intCast(changed.cur / 4)), budget.defaultLimit());
    changed.cur = 40;
    try posix.setrlimit(.NOFILE, changed);
    try testing.expectEqual(budget.min_limit, budget.defaultLimit());
    // The limit in force was read at first use: a later RLIMIT_NOFILE does
    // not move it.
    try testing.expectEqual(in_force, budget.limit());
}

test "budget: tryAcquire counts all or nothing, acquireUpTo what fits, acquire past the limit" {
    if (comptime !supported) return error.SkipZigTest;
    try warmUp();
    const base = budget.inUse();
    const limit = support.BudgetLimit.set(base + 5);
    defer limit.restore();

    try testing.expect(budget.tryAcquire(3));
    try testing.expect(!budget.tryAcquire(3));
    try testing.expectEqual(base + 3, budget.inUse());
    try testing.expectEqual(@as(usize, 2), budget.acquireUpTo(4));
    try testing.expectEqual(@as(usize, 0), budget.acquireUpTo(1));
    try testing.expect(!budget.tryAcquire(1));
    try testing.expect(budget.tryAcquire(0));
    // Fds that already exist count past the limit.
    budget.acquire(2);
    try testing.expectEqual(base + 7, budget.inUse());
    try testing.expectEqual(@as(usize, 0), budget.acquireUpTo(1));
    budget.release(7);
    try testing.expectEqual(base, budget.inUse());
}

// ---------------------------------------------------------------------------
// Over the budget: receive and send degrade, connections stay
// ---------------------------------------------------------------------------

test "over the process fd budget a frame arrives without the fds that do not fit; they are closed and the connection stays" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        var pipes = try support.Pipes.open(5);
        defer pipes.closeAll();
        const frame = try support.buildFrame(gpa, 13);
        defer gpa.free(frame);

        var recorder: SharedRecorder = .{};
        var transport = Transport.initWithOptions(gpa, testing.io, .{ .handle = sp[1] }, .{
            .read_buffer_size = 64 * 1024,
            .observer = recorder.observer(),
        }) catch |err| {
            support.closeFd(sp[1]);
            return err;
        };
        var transport_live = true;
        defer if (transport_live) transport.deinit();
        try transport.enableFdPassing(.{ .max_fds_per_message = 8 });

        // The budget holds fds of other connections (imports, say): room
        // for exactly two more. (The closer's queue stays far below the
        // limit, which would end the connection: see `fd_closer`.)
        const base = budget.inUse();
        const limit = support.BudgetLimit.set(base + 16);
        defer limit.restore();
        var held_elsewhere: usize = 14;
        budget.acquire(held_elsewhere);
        defer budget.release(held_elsewhere);
        const full = base + 16;

        // A: three fds, two fit. The third goes to the closer at once.
        try support.sendWithFds(sp[0], frame, pipes.writers()[0..3]);
        try readFrame(&transport, frame.len);
        try testing.expectEqual(@as(usize, 2), transport.frameFdCount());
        // The third counts until the closer has closed it.
        try support.expectBudgetInUse(full);
        try testing.expectEqual(@as(usize, 1), recorder.countRejections(error.FdBudgetExceeded));
        const event = recorder.rejections[0];
        try testing.expectEqual(events.Source.unix, event.source);
        try testing.expectEqual(@as(?usize, full), event.limit);
        try testing.expect((event.attempted orelse 0) > full);
        try testing.expect(!transport.isClosing());

        // The app takes one: it leaves the budget.
        var taken: Fd = transport.takeFrameFd(0) orelse return error.FdMissing;
        defer if (taken >= 0) support.closeFd(taken);
        try testing.expectEqual(full - 1, budget.inUse());

        // B: no fds. Reading it hands A's untaken fd to the closer.
        try support.sendWithFds(sp[0], frame, &.{});
        try readFrame(&transport, frame.len);
        try testing.expectEqual(@as(usize, 0), transport.frameFdCount());
        try support.expectBudgetInUse(full - 2);

        // C: two more fds held elsewhere fill the budget: C keeps none, and
        // still arrives.
        budget.acquire(2);
        held_elsewhere += 2;
        try support.sendWithFds(sp[0], frame, pipes.writers()[3..5]);
        try readFrame(&transport, frame.len);
        try testing.expectEqual(@as(usize, 0), transport.frameFdCount());
        try testing.expectEqual(@as(usize, 2), recorder.countRejections(error.FdBudgetExceeded));
        budget.release(2);
        held_elsewhere -= 2;
        // C's fds count until the closer has closed them.
        try support.expectBudgetInUse(full - 2);

        // D: room again; it keeps its fd.
        try support.sendWithFds(sp[0], frame, pipes.writers()[0..1]);
        try readFrame(&transport, frame.len);
        try testing.expectEqual(@as(usize, 1), transport.frameFdCount());
        try testing.expect(!transport.isClosing());

        // Every fd the transport did not keep or hand out is closed now:
        // A's third and second (released), and both of C's.
        pipes.closeWriters();
        try pipes.expectClosed(&.{ 1, 2, 3, 4 });
        // Pipe 0: D holds a copy until teardown, and so does the test (A's
        // copy, taken).
        support.closeFd(taken);
        taken = -1;
        try pipes.expectOpen(&.{0});
        transport.deinit();
        transport_live = false;
        try pipes.expectClosed(&.{0});
    }
    try support.expectBackAtBaseline(before);
}

test "one budget for every kind: fds a frame keeps leave less room for sends, and sent dups less for frames" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var pipes = try support.Pipes.open(2);
        defer pipes.closeAll();
        const frame = try support.buildFrame(gpa, 31);
        defer gpa.free(frame);
        const big = try gpa.alloc(u8, blocking_len);
        defer gpa.free(big);
        @memset(big, 0x13);

        // R receives with fd passing on.
        const r_sp = try support.socketPair();
        defer support.closeFd(r_sp[0]);
        var r_events: SharedRecorder = .{};
        var r = Transport.initWithOptions(gpa, testing.io, .{ .handle = r_sp[1] }, .{
            .read_buffer_size = 64 * 1024,
            .observer = r_events.observer(),
        }) catch |err| {
            support.closeFd(r_sp[1]);
            return err;
        };
        defer r.deinit();
        try r.enableFdPassing(.{ .max_fds_per_message = 4 });

        // R2 receives too. (R's own next read would release R's fds first.)
        const r2_sp = try support.socketPair();
        defer support.closeFd(r2_sp[0]);
        var r2_events: SharedRecorder = .{};
        var r2 = Transport.initWithOptions(gpa, testing.io, .{ .handle = r2_sp[1] }, .{
            .read_buffer_size = 64 * 1024,
            .observer = r2_events.observer(),
        }) catch |err| {
            support.closeFd(r2_sp[1]);
            return err;
        };
        defer r2.deinit();
        try r2.enableFdPassing(.{ .max_fds_per_message = 4 });

        // S sends; its writer blocks inside `big`, so every dup it queues
        // stays held.
        const s_sp = try support.socketPair();
        defer support.closeFd(s_sp[0]);
        var s_events: SharedRecorder = .{};
        var s = Transport.initWithOptions(gpa, testing.io, .{ .handle = s_sp[1] }, .{
            .read_buffer_size = 64,
            .observer = s_events.observer(),
        }) catch |err| {
            support.closeFd(s_sp[1]);
            return err;
        };
        var s_live = true;
        defer if (s_live) s.deinit();
        try s.startWriter();
        try s.enqueueWrite(big);
        try waitBatchTaken(&s);

        // Room for three fds; the rest of the budget is held elsewhere (and
        // the closer's queue stays far below the limit).
        const base = budget.inUse();
        const limit = support.BudgetLimit.set(base + 16);
        defer limit.restore();
        const elsewhere = 13;
        budget.acquire(elsewhere);
        defer budget.release(elsewhere);
        const full = base + 16;

        // R keeps two: one fd of room left for S.
        const w = pipes.write_ends[0];
        try support.sendWithFds(r_sp[0], frame, &.{ w, w });
        try readFrame(&r, frame.len);
        try testing.expectEqual(@as(usize, 2), r.frameFdCount());
        try s.enqueueWriteWithFds("one", &.{w});
        try testing.expectError(error.FdQueueFull, s.enqueueWriteWithFds("two", &.{w}));
        const refusal = s_events.backpressureWith(error.FdBudgetExceeded) orelse return error.EventMissing;
        try testing.expectEqual(@as(?usize, full), refusal.limit);
        try testing.expectEqual(@as(?usize, 1), refusal.attempted_bytes);
        try testing.expectEqual(full, budget.inUse());
        // A send without fds still goes, and S stays up.
        try s.enqueueWrite("plain");
        try testing.expect(!s.isClosing());

        // S's dup and R's two fill the budget: R2's frame keeps none, and
        // still arrives.
        try support.sendWithFds(r2_sp[0], frame, &.{w});
        try readFrame(&r2, frame.len);
        try testing.expectEqual(@as(usize, 0), r2.frameFdCount());
        try testing.expectEqual(@as(usize, 1), r2_events.countRejections(error.FdBudgetExceeded));
        try testing.expect(!r2.isClosing());
        try support.expectBudgetInUse(full);

        // R's next read (a frame without fds) hands its two to the closer:
        // then S has room for two.
        try support.sendWithFds(r_sp[0], frame, &.{});
        try readFrame(&r, frame.len);
        try support.expectBudgetInUse(full - 2);
        try s.enqueueWriteWithFds("two", &.{ w, w });
        try testing.expectEqual(full, budget.inUse());
        try testing.expect(!r.isClosing());

        pipes.closeWriters();
        s.deinit();
        s_live = false;
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

// ---------------------------------------------------------------------------
// The Peer seam: imports count, and OOM at every allocation
// ---------------------------------------------------------------------------

/// A `TransportBinding` that hands the Peer fds the way a transport does
/// (`take_frame_fd`) and accepts every send. The fds it hands out are dups
/// of the fds the test attaches; the ones the Peer does not take are closed
/// after dispatch (a real transport hands them to the closer).
const FakeBinding = struct {
    frame_fds: [8]Fd = @splat(-1),
    frame_fd_count: usize = 0,
    sends: usize = 0,
    fd_sends: usize = 0,

    fn binding(self: *FakeBinding) rpc_peer.TransportBinding {
        var b = rpc_peer.TransportBinding.init(self, null, send, close, null);
        b.send_with_fds = sendWithFds;
        b.take_frame_fd = take;
        b.max_outbound_fds = maxOutboundFds;
        return b;
    }

    fn maxOutboundFds(_: *anyopaque) u8 {
        return 253;
    }

    fn close(_: *anyopaque) void {}

    fn send(ctx: *anyopaque, _: []const u8) anyerror!void {
        const self: *FakeBinding = @ptrCast(@alignCast(ctx));
        self.sends += 1;
    }

    fn sendWithFds(ctx: *anyopaque, _: []const u8, fds: []const FdHandle) anyerror!void {
        const self: *FakeBinding = @ptrCast(@alignCast(ctx));
        // Borrowed: every fd must be open while the transport sends it.
        for (fds) |h| if (!support.isOpen(h.fd)) return error.FdNotOpen;
        self.fd_sends += 1;
    }

    fn take(ctx: *anyopaque, index: u8) ?FdHandle {
        const self: *FakeBinding = @ptrCast(@alignCast(ctx));
        if (index >= self.frame_fd_count) return null;
        const fd = self.frame_fds[index];
        if (fd < 0) return null;
        self.frame_fds[index] = -1;
        return .{ .fd = fd };
    }

    fn deliver(self: *FakeBinding, peer: *Peer, frame: []const u8, attach: []const Fd) !void {
        std.debug.assert(attach.len <= self.frame_fds.len);
        self.frame_fd_count = 0;
        defer {
            for (self.frame_fds[0..self.frame_fd_count]) |*fd| {
                if (fd.* >= 0) support.closeFd(fd.*);
                fd.* = -1;
            }
            self.frame_fd_count = 0;
        }
        for (attach, 0..) |fd, i| {
            self.frame_fds[i] = @intCast(try support.check(sys.dup(fd), "dup"));
            self.frame_fd_count = i + 1;
        }
        try peer.handleFrame(frame);
    }
};

/// A Call to `target` whose params carry one `senderHosted` cap per entry
/// of `ids`, the i-th with `attachedFd = i`.
fn buildFdCall(gpa: std.mem.Allocator, question_id: u32, target: u32, ids: []const u32) ![]const u8 {
    var mb = protocol.MessageBuilder.init(gpa);
    defer mb.deinit();
    var call = try mb.beginCall(question_id, iface_id, 0);
    try call.setTargetImportedCap(target);
    try fillFdCaps(&call, ids);
    return mb.finish();
}

/// The params of `call`: a list of `ids.len` caps, the i-th a
/// `senderHosted` descriptor for `ids[i]` with `attachedFd = i`.
fn fillFdCaps(call: *protocol.CallBuilder, ids: []const u32) !void {
    var payload = try call.payloadTyped();
    const caps = try (try payload.initContent()).initPointerList(@intCast(ids.len));
    for (0..ids.len) |i| try caps.setCapability(@intCast(i), .{ .id = @intCast(i) });
    var list = try call.initCapTableTyped(@intCast(ids.len));
    for (ids, 0..) |id, i| {
        var d = try list.get(@intCast(i));
        try d.setSenderHosted(id);
        try d.setAttachedFd(@intCast(i));
    }
}

/// The bootstrap of the seam tests: keeps every cap of a call, counts the
/// ones that came with an fd, and answers with `answer` (an export with an
/// fd), when set.
const SeamHandler = struct {
    imports_with_fd: usize = 0,
    answer: ?u32 = null,

    fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *SeamHandler = @ptrCast(@alignCast(ctx));
        var index: u32 = 0;
        while (index < caps.len()) : (index += 1) {
            switch (try caps.get(index)) {
                .imported => |imported| {
                    try @constCast(caps).retainIndex(index);
                    if (peer.importFd(imported.id) != null) self.imports_with_fd += 1;
                },
                else => {},
            }
        }
        if (self.answer != null) {
            try peer.sendReturnResults(call.question_id, self, build);
        } else {
            try peer.sendReturnEmptyStruct(call.question_id);
        }
    }

    fn build(ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        const self: *SeamHandler = @ptrCast(@alignCast(ctx));
        var payload = try ret.payloadTyped();
        try (try payload.initContent()).setCapability(.{ .id = self.answer.? });
    }
};

const Noop = struct {
    fn onCall(_: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        try peer.sendReturnEmptyStruct(call.question_id);
    }
};

/// The Peer's fd paths, end to end, on `allocator`: `setExportFd`, an
/// inbound Call whose two caps bring fds (adopted into the import table),
/// a Return that carries the export's fd, the release of one import, and
/// `Peer.deinit` with the other still live. Any step may fail (OOM sweeps).
fn seamScenario(allocator: std.mem.Allocator, pipe_w: Fd) !void {
    var fake: FakeBinding = .{};
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.attachTransportBinding(fake.binding());

    var handler: SeamHandler = .{};
    const bootstrap = try peer.setBootstrap(.{ .ctx = &handler, .on_call = SeamHandler.onCall });
    var noop: u8 = 0;
    const with_fd = try peer.addExport(.{ .ctx = &noop, .on_call = Noop.onCall });
    try peer.setExportFd(with_fd, .{ .fd = pipe_w });
    handler.answer = with_fd;

    const call = try buildFdCall(allocator, 0, bootstrap, &.{ 10, 11 });
    defer allocator.free(call);
    try fake.deliver(&peer, call, &.{ pipe_w, pipe_w });
    try peer.releaseImport(10, 1);
}

test "imports count against the budget: one unit per imported fd, until the import is released or the peer is gone" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var pipes = try support.Pipes.open(1);
        defer pipes.closeAll();
        const base = budget.inUse();

        var fake: FakeBinding = .{};
        var peer = Peer.initDetached(gpa);
        var peer_live = true;
        defer if (peer_live) peer.deinit();
        peer.attachTransportBinding(fake.binding());
        var handler: SeamHandler = .{};
        const bootstrap = try peer.setBootstrap(.{ .ctx = &handler, .on_call = SeamHandler.onCall });

        const call = try buildFdCall(gpa, 0, bootstrap, &.{ 10, 11 });
        defer gpa.free(call);
        try fake.deliver(&peer, call, &.{ pipes.write_ends[0], pipes.write_ends[0] });
        try testing.expectEqual(@as(usize, 2), handler.imports_with_fd);
        try testing.expectEqual(base + 2, budget.inUse());

        try peer.releaseImport(10, 1);
        try testing.expect(peer.importFd(10) == null);
        try support.expectBudgetInUse(base + 1);

        // Torn down with import 11 live: its fd and its unit go too.
        peer.deinit();
        peer_live = false;
        try support.expectBudgetInUse(base);
        pipes.closeWriters();
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "OOM at every allocation of the Peer's fd paths leaks no fd, no byte and no budget unit" {
    if (comptime !supported) return error.SkipZigTest;
    try warmUp();
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    const before = support.FdSnapshot.take();

    // `std.testing.checkAllAllocationFailures` needs every failure to come
    // back as `error.OutOfMemory`; the Peer deliberately swallows some (an fd
    // it cannot adopt stays with the transport, a handler's OOM becomes an
    // exception Return). So this sweeps every allocation index itself and
    // checks each run on its own.
    var fail_index: usize = 0;
    var failed_runs: usize = 0;
    while (true) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(testing.allocator, .{ .fail_index = fail_index });
        seamScenario(failing.allocator(), pipes.write_ends[0]) catch {};
        if (failing.allocated_bytes != failing.freed_bytes) {
            std.debug.print("allocation {d} failed: {d} bytes leaked\n", .{ fail_index, failing.allocated_bytes - failing.freed_bytes });
            return error.MemoryLeakDetected;
        }
        support.expectBackAtBaseline(before) catch |err| {
            std.debug.print("allocation {d} failed: the fd table or the budget is off\n", .{fail_index});
            return err;
        };
        if (!failing.has_induced_failure) break;
        failed_runs += 1;
    }
    // The scenario allocates on every fd path (measured: 32 allocations);
    // a sweep much shorter than that misses some.
    try testing.expect(failed_runs >= 20);
}

test "a failed closer-queue allocation at any point of the Peer's fd paths leaks nothing" {
    if (comptime !supported) return error.SkipZigTest;
    try warmUp();
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    const before = support.FdSnapshot.take();
    defer _ = fd_io.closer.injectAllocationFailure(null);

    var nth: usize = 0;
    while (true) : (nth += 1) {
        _ = fd_io.closer.injectAllocationFailure(nth);
        seamScenario(testing.allocator, pipes.write_ends[0]) catch {};
        const fired = !fd_io.closer.allocationFailureArmed();
        _ = fd_io.closer.injectAllocationFailure(null);
        support.expectBackAtBaseline(before) catch |err| {
            std.debug.print("closer allocation {d} failed: the fd table or the budget is off\n", .{nth});
            return err;
        };
        if (!fired) break;
    }
    // Each of the two adoptions reserves a closer slot: both injections
    // fired (measured: 2).
    try testing.expect(nth >= 2);
}

// ---------------------------------------------------------------------------
// The transport's fd paths under closer-queue allocation failures
// ---------------------------------------------------------------------------

/// A transport's fd paths, end to end: drain state, fd passing, a frame
/// with fds (one taken, one released), a frame past the per-message cap,
/// a queued fd send, and teardown. Any step may fail.
fn transportScenario(gpa: std.mem.Allocator, pipe_w: Fd) !void {
    const sp = try support.socketPair();
    defer support.closeFd(sp[0]);
    var transport = Transport.init(gpa, testing.io, .{ .handle = sp[1] }, 64 * 1024) catch |err| {
        support.closeFd(sp[1]);
        return err;
    };
    defer transport.deinit();
    try transport.enableFdPassing(.{ .max_fds_per_message = 2 });

    const frame = try support.buildFrame(gpa, 7);
    defer gpa.free(frame);
    try support.sendWithFds(sp[0], frame, &.{ pipe_w, pipe_w });
    try support.sendWithFds(sp[0], frame, &.{ pipe_w, pipe_w, pipe_w });
    try readFrame(&transport, frame.len);
    if (transport.takeFrameFd(0)) |fd| support.closeFd(fd);
    try readFrame(&transport, frame.len);

    try transport.startWriter();
    try transport.enqueueWriteWithFds("fd", &.{pipe_w});
    _ = try readAndCloseFds(sp[0], 2);
}

test "a failed closer-queue allocation at any point of a transport's fd paths leaks nothing" {
    if (comptime !supported) return error.SkipZigTest;
    try warmUp();
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    const before = support.FdSnapshot.take();
    defer _ = fd_io.closer.injectAllocationFailure(null);

    var nth: usize = 0;
    var errors_seen: usize = 0;
    while (true) : (nth += 1) {
        _ = fd_io.closer.injectAllocationFailure(nth);
        transportScenario(testing.allocator, pipes.write_ends[0]) catch {
            errors_seen += 1;
        };
        const fired = !fd_io.closer.allocationFailureArmed();
        _ = fd_io.closer.injectAllocationFailure(null);
        support.expectBackAtBaseline(before) catch |err| {
            std.debug.print("closer allocation {d} failed: the fd table or the budget is off\n", .{nth});
            return err;
        };
        if (!fired) break;
    }
    // Drain state (three lanes), fd passing, a read's top-up and the queued
    // send each reserve: every one of those injections fired and failed the
    // scenario (measured: 6 and 6).
    try testing.expect(nth >= 5);
    try testing.expect(errors_seen >= 5);
}

test "OOM at every allocation of a transport's fd paths leaks no fd and no budget unit" {
    if (comptime !supported) return error.SkipZigTest;
    try warmUp();
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    const before = support.FdSnapshot.take();

    const Run = struct {
        fn run(allocator: std.mem.Allocator, pipe_w: Fd, baseline: support.FdSnapshot) !void {
            // The writer thread frees what the queue held.
            var locked: LockedAllocator = .{ .child = allocator };
            transportScenario(locked.allocator(), pipe_w) catch |err| {
                // Every failed run must leave nothing behind before the next.
                try support.expectBackAtBaseline(baseline);
                return err;
            };
            try support.expectBackAtBaseline(baseline);
        }
    };
    try testing.checkAllAllocationFailures(testing.allocator, Run.run, .{ pipes.write_ends[0], before });
}

// ---------------------------------------------------------------------------
// N connections at their per-connection cap; accept still works
// ---------------------------------------------------------------------------

var dir_counter: std.atomic.Value(u32) = .init(0);

/// A private (0700) directory under /tmp, removed with everything in it.
const TestDir = struct {
    buf: [64]u8 = undefined,
    dir: []const u8 = &.{},

    fn init(self: *TestDir) !void {
        const n = dir_counter.fetchAdd(1, .monotonic);
        self.dir = try std.fmt.bufPrint(&self.buf, "/tmp/czfl-{d}-{d}", .{ sys.getpid(), n });
        std.Io.Dir.cwd().deleteTree(testing.io, self.dir) catch {};
        var z: [65]u8 = undefined;
        @memcpy(z[0..self.dir.len], self.dir);
        z[self.dir.len] = 0;
        _ = try support.check(sys.mkdir(@ptrCast(&z), 0o700), "mkdir");
    }

    fn deinit(self: *TestDir) void {
        std.Io.Dir.cwd().deleteTree(testing.io, self.dir) catch {};
    }
};

fn rawConnect(path: []const u8) !Fd {
    const fd: Fd = @intCast(try support.check(sys.socket(posix.AF.UNIX, posix.SOCK.STREAM, 0), "socket"));
    errdefer support.closeFd(fd);
    var addr: posix.sockaddr.un = .{ .family = posix.AF.UNIX, .path = undefined };
    @memset(&addr.path, 0);
    @memcpy(addr.path[0..path.len], path);
    _ = try support.check(sys.connect(fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.un)), "connect");
    return fd;
}

/// The server's bootstrap: keeps every cap a call brings (so each import
/// and its fd stay), and records how many came with an fd.
const Keeper = struct {
    calls: std.atomic.Value(usize) = .init(0),
    kept: [16]std.atomic.Value(usize) = @splat(.init(0)),

    fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *Keeper = @ptrCast(@alignCast(ctx));
        var with_fd: usize = 0;
        var index: u32 = 0;
        while (index < caps.len()) : (index += 1) {
            switch (try caps.get(index)) {
                .imported => |imported| {
                    try @constCast(caps).retainIndex(index);
                    if (peer.importFd(imported.id) != null) with_fd += 1;
                },
                else => {},
            }
        }
        try peer.sendReturnEmptyStruct(call.question_id);
        // The test sends one call at a time and waits for it here.
        const n = self.calls.load(.acquire);
        self.kept[n].store(with_fd, .release);
        self.calls.store(n + 1, .release);
    }

    fn waitCalls(self: *Keeper, want: usize, sessions: []const Session) !void {
        const start = support.nowNs();
        while (self.calls.load(.acquire) < want) {
            for (sessions) |*s| if (s.done.load(.acquire)) {
                std.debug.print("a server session ended early: {?t}\n", .{s.err});
                return error.SessionEnded;
            };
            if (support.msSince(start) >= 5000) return error.CallNeverDispatched;
            support.sleepMs(2);
        }
    }
};

/// One server session on its own thread: accept, set the bootstrap, run
/// until the client goes away.
const Session = struct {
    listener: *tcp.Listener = undefined,
    keeper: *Keeper = undefined,
    recorder: support.Recorder = .{},
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
    thread: ?std.Thread = null,

    fn main(self: *Session) void {
        defer self.done.store(true, .release);
        var session = tcp.ServerSession.accept(testing.allocator, self.listener, .{ .observer = self.recorder.observer() }) catch |err| {
            self.err = err;
            return;
        };
        defer session.deinit();
        _ = session.peer.setBootstrap(.{ .ctx = self.keeper, .on_call = Keeper.onCall }) catch |err| {
            self.err = err;
            return;
        };
        session.run();
    }
};

/// Bootstrap (question 0), then a call pipelined on it (question 1) that
/// brings `fd_count` caps, each with its own fd.
fn sendKeepCall(gpa: std.mem.Allocator, client: Fd, pipe_w: Fd, fd_count: usize) !void {
    var boot = protocol.MessageBuilder.init(gpa);
    defer boot.deinit();
    try boot.buildBootstrap(0);
    const boot_frame = try boot.finish();
    defer gpa.free(boot_frame);
    try support.sendWithFds(client, boot_frame, &.{});

    var ids: [8]u32 = undefined;
    for (ids[0..fd_count], 0..) |*id, i| id.* = @intCast(i);
    var mb = protocol.MessageBuilder.init(gpa);
    defer mb.deinit();
    var call = try mb.beginCall(1, iface_id, 0);
    try call.setTargetPromisedAnswer(0);
    try fillFdCaps(&call, ids[0..fd_count]);
    const call_frame = try mb.finish();
    defer gpa.free(call_frame);
    var fds: [8]Fd = undefined;
    @memset(fds[0..fd_count], pipe_w);
    try support.sendWithFds(client, call_frame, fds[0..fd_count]);
}

test "N connections at their per-connection fd cap: the budget holds the total, and accept still works" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const n_conns = 6;
        const cap = 4;
        // One more fd than the cap: the per-connection cap refuses it.
        const per_call = cap + 1;
        // Room for two connections' worth of imports.
        const room = 2 * cap;

        var dir: TestDir = .{};
        try dir.init();
        defer dir.deinit();
        var path_buf: [96]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "{s}/s", .{dir.dir});
        const p = try support.pipePair();
        defer support.closeFd(p[0]);
        var pipe_w: Fd = p[1];
        defer if (pipe_w >= 0) support.closeFd(pipe_w);

        var listener = try unix.listen(gpa, testing.io, path, .{
            .fd_passing = .{ .max_fds_per_message = 8, .max_live_imported_fds = cap },
        });
        var listener_open = true;
        defer if (listener_open) listener.close();

        var keeper: Keeper = .{};
        var sessions: [n_conns + 1]Session = @splat(.{});
        var clients: [n_conns + 1]Fd = @splat(-1);
        var table = try FdTable.save();
        // On every exit: the clients go, so every session ends, and the
        // listener closes, which wakes a session still parked in accept;
        // then the fd table comes back.
        defer {
            for (clients) |c| if (c >= 0) support.closeFd(c);
            if (listener_open) listener.close();
            listener_open = false;
            for (&sessions) |*s| if (s.thread) |t| t.join();
            table.restore();
        }

        const base = budget.inUse();
        const limit = support.BudgetLimit.set(base + room);
        defer limit.restore();

        var before_first: support.FdSnapshot = undefined;
        for (0..n_conns + 1) |i| {
            if (i == 0) before_first = support.FdSnapshot.take();
            sessions[i] = .{ .listener = &listener, .keeper = &keeper };
            sessions[i].thread = try std.Thread.spawn(.{}, Session.main, .{&sessions[i]});
            // A full fd table shows up as a session that could not be set
            // up (its accept or its Connection failed): say which.
            errdefer {
                support.sleepMs(100);
                for (sessions[0 .. i + 1], 0..) |*s, k| {
                    if (s.done.load(.acquire)) std.debug.print("session {d} ended: {?t}\n", .{ k, s.err });
                }
            }
            clients[i] = try rawConnect(path);
            try sendKeepCall(gpa, clients[i], pipe_w, per_call);
            try keeper.waitCalls(i + 1, sessions[0 .. i + 1]);
            try support.waitCloserIdle(support.closed_wait_ms);
            if (i == 0) {
                // What one connection costs here (client socket, server
                // socket, wake pair: measured, not assumed), then a table
                // with room for the rest of the connections, the rest of
                // the budget and one call's fds in flight, plus a margin.
                // Without the budget the imports alone (5 more connections
                // at the cap: 20 fds) would not fit.
                var added: [32]Fd = undefined;
                const per_conn = support.FdSnapshot.take().added(before_first, &added) - cap;
                const margin = 6;
                try table.leave(n_conns * per_conn + (room - cap) + per_call + margin, p[0]);
            }
        }

        // The first two connections reached their cap (the fifth fd refused
        // by it); after that the budget held: the other connections, and
        // the one accepted at full budget, kept none, and all stayed up.
        var kept: [n_conns + 1]usize = undefined;
        for (&kept, 0..) |*k, i| k.* = keeper.kept[i].load(.acquire);
        errdefer std.debug.print("fds kept per connection: {any}\n", .{kept});
        try testing.expectEqualSlices(usize, &.{ cap, cap, 0, 0, 0, 0, 0 }, &kept);
        try testing.expectEqual(base + room, budget.inUse());
        for (&sessions) |*s| try testing.expect(!s.done.load(.acquire));

        // Teardown: every session ends, every import's fd is closed.
        for (&clients) |*c| {
            support.closeFd(c.*);
            c.* = -1;
        }
        for (&sessions) |*s| {
            if (s.thread) |t| t.join();
            s.thread = null;
        }
        table.restore();
        try support.waitCloserIdle(support.closed_wait_ms);
        try support.expectBudgetInUse(base);

        try testing.expectEqual(@as(usize, 1), sessions[0].recorder.countErr(error.ImportedFdsOverLimit));
        try testing.expectEqual(@as(usize, 1), sessions[1].recorder.countErr(error.FdBudgetExceeded));
        for (sessions[2..]) |*s| {
            try testing.expectEqual(@as(?anyerror, null), s.err);
            try testing.expectEqual(@as(usize, 1), s.recorder.countErr(error.FdBudgetExceeded));
            try testing.expectEqual(@as(usize, 0), s.recorder.countErr(error.ImportedFdsOverLimit));
        }
        support.closeFd(pipe_w);
        pipe_w = -1;
        try testing.expect(support.pipeWritersClosed(p[0], support.closed_wait_ms));
    }
    try support.expectBackAtBaseline(before);
}

// ---------------------------------------------------------------------------
// EMFILE and ETOOMANYREFS on the send side
// ---------------------------------------------------------------------------

test "EMFILE while dup'ing a sent fd refuses that message, closes the dups made, and the connection stays" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var pipes = try support.Pipes.open(3);
        defer pipes.closeAll();
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        var recorder: SharedRecorder = .{};
        var transport = Transport.initWithOptions(gpa, testing.io, .{ .handle = sp[1] }, .{
            .read_buffer_size = 64,
            .observer = recorder.observer(),
        }) catch |err| {
            support.closeFd(sp[1]);
            return err;
        };
        defer transport.deinit();
        try transport.startWriter();
        const base = budget.inUse();

        {
            var table = try FdTable.save();
            defer table.restore();
            // Room for two dups: the third fails with EMFILE.
            try table.leave(2, pipes.read_ends[0]);
            try testing.expectError(error.ProcessFdQuotaExceeded, transport.enqueueWriteWithFds("three", pipes.writers()));
            try testing.expectEqual(@as(usize, 0), transport.queueStats().fds);
            try testing.expect(!transport.isClosing());
            // The two dups made went to the closer, their units with them.
            try support.waitLaneIdle(.sent, support.closed_wait_ms);
            try testing.expectEqual(base, budget.inUse());
            // Still at the limit, a message without fds goes.
            try transport.enqueueWrite("plain");
            try testing.expectEqual(@as(usize, 0), try readAndCloseFds(sp[0], "plain".len));
        }

        // With room again, the same message goes with its fds.
        try transport.enqueueWriteWithFds("three", pipes.writers());
        try testing.expectEqual(@as(usize, 3), try readAndCloseFds(sp[0], "three".len));
        pipes.closeWriters();
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "Linux ETOOMANYREFS: a direct send is refused with FdQueueFull, a queued one goes without its fds, and the connection stays" {
    if (comptime !support.is_linux) return error.SkipZigTest;
    const gpa = testing.allocator;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var parked_pipe = try support.Pipes.open(1);
        defer parked_pipe.closeAll();
        var pipes = try support.Pipes.open(1);
        defer pipes.closeAll();
        const w = pipes.write_ends[0];

        // The in-flight limit is the sender's soft RLIMIT_NOFILE: lower it to
        // a little above what is open, then park more fds than that in
        // flight on a socket nobody reads.
        var table = try FdTable.save();
        defer table.restore();
        var lowered = table.saved;
        lowered.cur = @min(table.saved.max, support.FdSnapshot.take().highest() + 64);
        try posix.setrlimit(.NOFILE, lowered);
        const parked = try support.socketPair();
        var parked_open = true;
        defer if (parked_open) {
            support.closeFd(parked[0]);
            support.closeFd(parked[1]);
        };
        const many: [fd_io.max_fds_per_send]Fd = @splat(parked_pipe.write_ends[0]);
        var in_flight: usize = 0;
        const refused = while (in_flight <= lowered.cur + 2 * many.len) {
            fd_io.sendWithFds(parked[0], "p", &many) catch |err| switch (err) {
                error.TooManyFdsInFlight => break true,
                else => return err,
            };
            in_flight += many.len;
        } else false;
        if (!refused) {
            // Root and CAP_SYS_RESOURCE may have any number in flight.
            std.debug.print("skipped: {d} fds in flight and no ETOOMANYREFS (privileged user)\n", .{in_flight});
            return error.SkipZigTest;
        }

        var recorder: SharedRecorder = .{};
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        var transport = Transport.initWithOptions(gpa, testing.io, .{ .handle = sp[1] }, .{
            .read_buffer_size = 64,
            .observer = recorder.observer(),
        }) catch |err| {
            support.closeFd(sp[1]);
            return err;
        };
        defer transport.deinit();

        // Before startWriter the send is direct: refused, nothing sent.
        try testing.expectError(error.FdQueueFull, transport.enqueueWriteWithFds("direct", &.{w}));
        const direct = recorder.backpressureWith(error.TooManyFdsInFlight) orelse return error.EventMissing;
        try testing.expectEqual(@as(?usize, 1), direct.attempted_bytes);
        try testing.expectEqual(@as(?usize, null), direct.limit);
        try testing.expect(!hasData(sp[0], 50));

        // From the write queue: the writer sends it without its fd.
        try transport.startWriter();
        try transport.enqueueWriteWithFds("queued", &.{w});
        try testing.expectEqual(@as(usize, 0), try readAndCloseFds(sp[0], "queued".len));
        _ = try recorder.waitBackpressure(error.TooManyFdsInFlight, 2000);
        try testing.expectEqual(@as(usize, 2), recorder.countBackpressure(error.TooManyFdsInFlight));
        try testing.expect(!transport.isClosing());

        // Once the parked fds are gone, fds go out again.
        support.closeFd(parked[0]);
        support.closeFd(parked[1]);
        parked_open = false;
        try transport.enqueueWriteWithFds("again", &.{w});
        try testing.expectEqual(@as(usize, 1), try readAndCloseFds(sp[0], "again".len));
        pipes.closeWriters();
        try support.waitLaneIdle(.sent, support.closed_wait_ms);
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}
