const std = @import("std");
const quic_zig = @import("quic_zig");

const endpoint_mod = @import("endpoint.zig");
const native_framer = @import("native_framer.zig");
const quic_options = @import("options.zig");

const Role = endpoint_mod.Role;
const NativeOptions = quic_options.NativeOptions;
const NativeControlFramer = native_framer.ControlFramer;

/// Narrow callback surface used by the native engine to avoid depending on the
/// concrete `Connection` type. `Connection` owns lifecycle and callbacks; the
/// engine owns native stream state.
pub const Owner = struct {
    ptr: *anyopaque,
    allocator: std.mem.Allocator,
    role: Role,
    max_message_bytes: usize,
    stream_read_buf: []u8,
    is_closing: *const fn (ptr: *anyopaque) bool,
    dispatch_rpc_frame: *const fn (ptr: *anyopaque, frame: []const u8) anyerror!void,
    terminate_frame_error: *const fn (ptr: *anyopaque, err: anyerror) void,
    deinit_requested: *const fn (ptr: *anyopaque) bool,
};

pub const NativeQueuedKind = enum {
    inline_rpc,
    data_rpc,
};

pub const NativeQueuedFrame = struct {
    sequence: u64,
    bytes: []u8,
    kind: NativeQueuedKind,
    control: ?[]u8 = null,
    control_offset: usize = 0,
    data_offset: usize = 0,
    stream_id: ?u64 = null,
    stream_finished: bool = false,

    fn free(self: *NativeQueuedFrame, allocator: std.mem.Allocator) void {
        if (self.control) |control| allocator.free(control);
        allocator.free(self.bytes);
        self.* = undefined;
    }
};

pub const NativeOutboundQueue = struct {
    mu: std.atomic.Mutex = .unlocked,
    items: std.ArrayListUnmanaged(NativeQueuedFrame) = .empty,
    head: usize = 0,
    queued_items: usize = 0,
    queued_bytes: usize = 0,
    queued_data_items: usize = 0,
    queued_data_bytes: usize = 0,
    max_items: usize = quic_options.default_max_outbound_queue_items,
    max_bytes: usize = quic_options.default_max_outbound_queue_bytes,
    inline_threshold: usize = quic_options.default_native_inline_frame_threshold,
    max_control_frame_bytes: usize = quic_options.default_native_max_control_frame_bytes,
    max_pending_data_streams: usize = quic_options.default_native_max_pending_data_streams,
    max_pending_data_bytes: usize = quic_options.default_native_max_pending_data_bytes,
    next_sequence: u64 = 0,
    next_uni_stream_id: u64 = 2,
    closed: bool = false,
    flushing: bool = false,

    pub fn init(
        role: Role,
        max_items: usize,
        max_bytes: usize,
        options: NativeOptions,
    ) NativeOutboundQueue {
        return .{
            .max_items = max_items,
            .max_bytes = max_bytes,
            .inline_threshold = options.inline_frame_threshold,
            .max_control_frame_bytes = options.max_control_frame_bytes,
            .max_pending_data_streams = options.max_pending_data_streams,
            .max_pending_data_bytes = options.max_pending_data_bytes,
            .next_uni_stream_id = switch (role) {
                .client => 2,
                .server => 3,
            },
        };
    }

    fn lock(self: *NativeOutboundQueue) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn enqueue(
        self: *NativeOutboundQueue,
        allocator: std.mem.Allocator,
        frame: []const u8,
    ) error{ BrokenPipe, OutboundQueueFull, OutOfMemory }!void {
        self.lock();
        defer self.mu.unlock();
        if (self.closed) return error.BrokenPipe;

        const kind: NativeQueuedKind = if (frame.len <= self.inline_threshold) .inline_rpc else .data_rpc;
        const new_bytes = std.math.add(usize, self.queued_bytes, frame.len) catch return error.OutboundQueueFull;
        if (self.queued_items >= self.max_items or new_bytes > self.max_bytes) {
            return error.OutboundQueueFull;
        }
        if (kind == .data_rpc) {
            const new_data_items = std.math.add(usize, self.queued_data_items, 1) catch return error.OutboundQueueFull;
            const new_data_bytes = std.math.add(usize, self.queued_data_bytes, frame.len) catch return error.OutboundQueueFull;
            if (new_data_items > self.max_pending_data_streams or new_data_bytes > self.max_pending_data_bytes) {
                return error.OutboundQueueFull;
            }
        }

        const bytes = allocator.alloc(u8, frame.len) catch return error.OutOfMemory;
        errdefer allocator.free(bytes);
        std.mem.copyForwards(u8, bytes, frame);

        self.items.append(allocator, .{
            .sequence = self.next_sequence,
            .bytes = bytes,
            .kind = kind,
        }) catch return error.OutOfMemory;
        self.next_sequence +%= 1;
        self.queued_items += 1;
        self.queued_bytes = new_bytes;
        if (kind == .data_rpc) {
            self.queued_data_items += 1;
            self.queued_data_bytes += frame.len;
        }
    }

    pub fn close(self: *NativeOutboundQueue) void {
        self.lock();
        self.closed = true;
        self.mu.unlock();
    }

    pub fn isEmpty(self: *NativeOutboundQueue) bool {
        self.lock();
        defer self.mu.unlock();
        return self.queued_items == 0;
    }

    pub fn drain(self: *NativeOutboundQueue, allocator: std.mem.Allocator) void {
        self.lock();
        defer self.mu.unlock();
        for (self.items.items[self.head..]) |*item| item.free(allocator);
        self.items.deinit(allocator);
        self.items = .empty;
        self.head = 0;
        self.queued_items = 0;
        self.queued_bytes = 0;
        self.queued_data_items = 0;
        self.queued_data_bytes = 0;
    }

    pub fn flush(
        self: *NativeOutboundQueue,
        allocator: std.mem.Allocator,
        conn: *quic_zig.Connection,
    ) !void {
        if (!self.beginFlush()) return;
        defer self.endFlush();

        while (true) {
            var item = self.takeFront() orelse return;
            const complete = self.flushItem(allocator, conn, &item) catch |err| {
                self.requeueFront(allocator, item);
                return err;
            };
            if (!complete) {
                self.requeueFront(allocator, item);
                return;
            }
            self.releaseItem(allocator, item);
        }
    }

    fn beginFlush(self: *NativeOutboundQueue) bool {
        self.lock();
        defer self.mu.unlock();
        if (self.flushing) return false;
        self.flushing = true;
        return true;
    }

    fn endFlush(self: *NativeOutboundQueue) void {
        self.lock();
        self.flushing = false;
        self.mu.unlock();
    }

    fn flushItem(
        self: *NativeOutboundQueue,
        allocator: std.mem.Allocator,
        conn: *quic_zig.Connection,
        item: *NativeQueuedFrame,
    ) !bool {
        if (item.kind == .data_rpc) {
            if (!try self.flushDataStream(conn, item)) return false;
        }
        if (item.control == null) {
            item.control = switch (item.kind) {
                .inline_rpc => try native_framer.encodeInlineRpc(
                    allocator,
                    item.sequence,
                    item.bytes,
                    self.max_control_frame_bytes,
                ),
                .data_rpc => try native_framer.encodeDataRpc(
                    allocator,
                    item.sequence,
                    item.stream_id orelse return error.InvalidFrame,
                    item.bytes.len,
                    self.max_control_frame_bytes,
                ),
            };
        }
        return try writeQueuedControl(conn, item);
    }

    fn flushDataStream(
        self: *NativeOutboundQueue,
        conn: *quic_zig.Connection,
        item: *NativeQueuedFrame,
    ) !bool {
        if (item.stream_id == null) {
            const stream_id = self.next_uni_stream_id;
            _ = conn.openUni(stream_id) catch |err| switch (err) {
                error.StreamLimitExceeded => return false,
                else => return err,
            };
            item.stream_id = stream_id;
            self.next_uni_stream_id +%= 4;
        }

        const stream_id = item.stream_id.?;
        while (item.data_offset < item.bytes.len) {
            const written = conn.streamWrite(stream_id, item.bytes[item.data_offset..]) catch |err| switch (err) {
                error.StreamNotFound => return error.InvalidFrame,
                else => return err,
            };
            if (written == 0) return false;
            item.data_offset += written;
        }
        if (!item.stream_finished) {
            try conn.streamFinish(stream_id);
            item.stream_finished = true;
        }
        return true;
    }

    fn writeQueuedControl(conn: *quic_zig.Connection, item: *NativeQueuedFrame) !bool {
        const control = item.control orelse return error.InvalidFrame;
        while (item.control_offset < control.len) {
            const written = conn.streamWrite(quic_options.baseline_stream_id, control[item.control_offset..]) catch |err| switch (err) {
                error.StreamNotFound => return false,
                else => return err,
            };
            if (written == 0) return false;
            item.control_offset += written;
        }
        return true;
    }

    fn takeFront(self: *NativeOutboundQueue) ?NativeQueuedFrame {
        self.lock();
        defer self.mu.unlock();
        if (self.head >= self.items.items.len) return null;
        const item = self.items.items[self.head];
        self.head += 1;
        return item;
    }

    fn requeueFront(
        self: *NativeOutboundQueue,
        allocator: std.mem.Allocator,
        item: NativeQueuedFrame,
    ) void {
        var free_item = false;

        self.lock();
        if (self.closed) {
            self.subtractItemCounts(item);
            free_item = true;
        } else {
            std.debug.assert(self.head > 0);
            self.head -= 1;
            self.items.items[self.head] = item;
        }
        self.mu.unlock();

        if (free_item) {
            var owned = item;
            owned.free(allocator);
        }
    }

    fn releaseItem(
        self: *NativeOutboundQueue,
        allocator: std.mem.Allocator,
        item: NativeQueuedFrame,
    ) void {
        self.lock();
        self.subtractItemCounts(item);
        self.compactIfNeeded();
        self.mu.unlock();

        var owned = item;
        owned.free(allocator);
    }

    fn subtractItemCounts(self: *NativeOutboundQueue, item: NativeQueuedFrame) void {
        self.queued_items -= 1;
        self.queued_bytes -= item.bytes.len;
        if (item.kind == .data_rpc) {
            self.queued_data_items -= 1;
            self.queued_data_bytes -= item.bytes.len;
        }
    }

    fn compactIfNeeded(self: *NativeOutboundQueue) void {
        if (self.head == 0) return;
        if (self.head == self.items.items.len) {
            self.items.clearRetainingCapacity();
            self.head = 0;
            return;
        }
        if (self.head < 32 or self.head < self.items.items.len - self.head) return;

        const live = self.items.items.len - self.head;
        std.mem.copyForwards(
            NativeQueuedFrame,
            self.items.items[0..live],
            self.items.items[self.head..],
        );
        self.items.items.len = live;
        self.head = 0;
    }
};

pub const NativePendingData = struct {
    sequence: u64,
    stream_id: u64,
    bytes: []u8,
    offset: usize = 0,
};

fn isNativeFrameError(err: anyerror) bool {
    return switch (err) {
        error.InvalidFrame, error.FrameTooLarge, error.OutOfMemory => true,
        else => false,
    };
}

/// Stream engine for the QUIC-native control/data stream transport mode.
///
/// The engine keeps native wire state local to native mode: control preface,
/// hello sequencing, ordered control frames, one-shot data streams, and the
/// native outbound queue.
pub const NativeEngine = struct {
    control_ready: bool = false,
    preamble: [native_framer.preface.len + native_framer.encodedHelloLen()]u8 = undefined,
    preamble_len: usize = 0,
    preamble_offset: usize = 0,
    received_preface_len: usize = 0,
    hello_received: bool = false,
    inbound: NativeControlFramer,
    outbound: NativeOutboundQueue,
    next_in_sequence: u64 = 0,
    pending_data: ?NativePendingData = null,

    pub fn init(
        allocator: std.mem.Allocator,
        role: Role,
        max_message_bytes: usize,
        max_outbound_queue_items: usize,
        max_outbound_queue_bytes: usize,
        native_options: NativeOptions,
    ) NativeEngine {
        return .{
            .inbound = NativeControlFramer.init(allocator, .{
                .max_control_frame_bytes = native_options.max_control_frame_bytes,
                .max_rpc_frame_bytes = max_message_bytes,
            }),
            .outbound = NativeOutboundQueue.init(
                role,
                max_outbound_queue_items,
                max_outbound_queue_bytes,
                native_options,
            ),
        };
    }

    pub fn deinit(self: *NativeEngine, allocator: std.mem.Allocator) void {
        self.inbound.deinit();
        self.outbound.drain(allocator);
        if (self.pending_data) |pending| allocator.free(pending.bytes);
        self.pending_data = null;
    }

    pub fn close(self: *NativeEngine) void {
        self.outbound.close();
    }

    pub fn resetInbound(self: *NativeEngine, allocator: std.mem.Allocator) void {
        self.inbound.reset();
        if (self.pending_data) |pending| allocator.free(pending.bytes);
        self.pending_data = null;
    }

    pub fn enqueue(
        self: *NativeEngine,
        allocator: std.mem.Allocator,
        frame: []const u8,
    ) !void {
        try self.outbound.enqueue(allocator, frame);
    }

    pub fn outboundEmpty(self: *NativeEngine) bool {
        return self.outbound.isEmpty();
    }

    pub fn hasImmediateWork(
        self: *NativeEngine,
        role: Role,
        conn: *quic_zig.Connection,
    ) bool {
        if (self.control_ready) return true;
        return role == .client and conn.handshakeDone();
    }

    pub fn service(
        self: *NativeEngine,
        owner: Owner,
        conn: *quic_zig.Connection,
    ) !void {
        if (!try self.ensureControlStream(owner.role, conn)) return;
        if (!try self.flushPreamble(conn)) return;
        try self.outbound.flush(owner.allocator, conn);
        self.readControlStream(owner, conn) catch |err| {
            if (isNativeFrameError(err)) {
                owner.terminate_frame_error(owner.ptr, err);
                return;
            }
            return err;
        };
        self.processControlFrames(owner, conn) catch |err| {
            if (isNativeFrameError(err)) {
                owner.terminate_frame_error(owner.ptr, err);
                return;
            }
            return err;
        };
    }

    fn ensureControlStream(
        self: *NativeEngine,
        role: Role,
        conn: *quic_zig.Connection,
    ) !bool {
        if (self.control_ready) return true;
        if (conn.stream(quic_options.baseline_stream_id) != null) {
            self.control_ready = true;
            return true;
        }
        if (role == .client) {
            if (!conn.handshakeDone()) return false;
            _ = conn.openBidi(quic_options.baseline_stream_id) catch |err| switch (err) {
                error.StreamAlreadyOpen => {},
                else => return err,
            };
            self.control_ready = true;
            return true;
        }
        return false;
    }

    fn flushPreamble(
        self: *NativeEngine,
        conn: *quic_zig.Connection,
    ) !bool {
        if (self.preamble_offset == self.preamble_len and self.preamble_len != 0) return true;
        if (self.preamble_len == 0) {
            @memcpy(self.preamble[0..native_framer.preface.len], native_framer.preface);
            const hello_len = try native_framer.encodeHello(self.preamble[native_framer.preface.len..]);
            self.preamble_len = native_framer.preface.len + hello_len;
        }

        while (self.preamble_offset < self.preamble_len) {
            const remaining = self.preamble[self.preamble_offset..self.preamble_len];
            const written = conn.streamWrite(quic_options.baseline_stream_id, remaining) catch |err| switch (err) {
                error.StreamNotFound => return false,
                else => return err,
            };
            if (written == 0) return false;
            self.preamble_offset += written;
        }
        return true;
    }

    fn readControlStream(
        self: *NativeEngine,
        owner: Owner,
        conn: *quic_zig.Connection,
    ) !void {
        while (!owner.is_closing(owner.ptr)) {
            const n = conn.streamRead(quic_options.baseline_stream_id, owner.stream_read_buf) catch |err| switch (err) {
                error.StreamNotFound => return,
                else => return err,
            };
            if (n == 0) return;
            try self.pushControlBytes(owner.stream_read_buf[0..n]);
        }
    }

    fn pushControlBytes(self: *NativeEngine, bytes: []const u8) !void {
        var remaining = bytes;
        if (self.received_preface_len < native_framer.preface.len) {
            const need = native_framer.preface.len - self.received_preface_len;
            const take = @min(need, remaining.len);
            const prefix_start = self.received_preface_len;
            const prefix_end = prefix_start + take;
            if (!std.mem.eql(u8, remaining[0..take], native_framer.preface[prefix_start..prefix_end])) {
                return error.InvalidFrame;
            }
            self.received_preface_len = prefix_end;
            remaining = remaining[take..];
        }
        if (self.received_preface_len == native_framer.preface.len and remaining.len > 0) {
            try self.inbound.push(remaining);
        }
    }

    pub fn processControlFrames(
        self: *NativeEngine,
        owner: Owner,
        conn: *quic_zig.Connection,
    ) !void {
        while (!owner.is_closing(owner.ptr)) {
            if (self.pending_data != null) {
                if (!try self.readPendingData(owner, conn)) return;
                continue;
            }

            const frame = (try self.inbound.popFrame()) orelse return;
            switch (frame) {
                .hello => |hello| {
                    if (self.hello_received) return error.InvalidFrame;
                    if (hello.version != native_framer.version) return error.InvalidFrame;
                    self.hello_received = true;
                },
                .inline_rpc => |inline_frame| {
                    defer frame.deinit(owner.allocator);
                    try self.ensureRpcSequence(inline_frame.sequence);
                    self.next_in_sequence +%= 1;
                    try owner.dispatch_rpc_frame(owner.ptr, inline_frame.frame);
                    if (owner.deinit_requested(owner.ptr)) return;
                },
                .data_rpc => |data| {
                    try self.ensureRpcSequence(data.sequence);
                    try self.startPendingData(owner, data);
                    if (!try self.readPendingData(owner, conn)) return;
                },
            }
        }
    }

    fn ensureRpcSequence(self: *NativeEngine, sequence: u64) !void {
        if (!self.hello_received) return error.InvalidFrame;
        if (sequence != self.next_in_sequence) return error.InvalidFrame;
    }

    pub fn startPendingData(
        self: *NativeEngine,
        owner: Owner,
        data: native_framer.DataRpc,
    ) !void {
        if (self.pending_data != null) return error.InvalidFrame;
        if (!isPeerInitiatedUniStreamId(owner.role, data.stream_id)) return error.InvalidFrame;
        if (data.length == 0 or data.length > owner.max_message_bytes) return error.FrameTooLarge;
        if (data.length > self.outbound.max_pending_data_bytes) return error.FrameTooLarge;

        const bytes = try owner.allocator.alloc(u8, data.length);
        errdefer owner.allocator.free(bytes);
        self.pending_data = .{
            .sequence = data.sequence,
            .stream_id = data.stream_id,
            .bytes = bytes,
        };
    }

    fn readPendingData(
        self: *NativeEngine,
        owner: Owner,
        conn: *quic_zig.Connection,
    ) !bool {
        if (self.pending_data == null) return true;
        var pending = &self.pending_data.?;
        const stream = conn.stream(pending.stream_id) orelse return false;
        if (stream.recv.final_size) |final_size| {
            if (final_size != pending.bytes.len) return error.InvalidFrame;
        }

        while (pending.offset < pending.bytes.len) {
            const n = conn.streamRead(pending.stream_id, pending.bytes[pending.offset..]) catch |err| switch (err) {
                error.StreamNotFound => return false,
                else => return err,
            };
            if (n == 0) break;
            pending.offset += n;
            if (stream.recv.final_size) |final_size| {
                if (final_size != pending.bytes.len) return error.InvalidFrame;
            }
        }

        if (pending.offset < pending.bytes.len) return false;
        if (stream.recv.final_size == null) return false;

        const bytes = pending.bytes;
        self.pending_data = null;
        defer owner.allocator.free(bytes);
        self.next_in_sequence +%= 1;
        try owner.dispatch_rpc_frame(owner.ptr, bytes);
        return true;
    }
};

fn isPeerInitiatedUniStreamId(role: Role, stream_id: u64) bool {
    const is_uni = (stream_id & 0b10) != 0;
    if (!is_uni) return false;
    const client_initiated = (stream_id & 0b01) == 0;
    return client_initiated != (role == .client);
}
