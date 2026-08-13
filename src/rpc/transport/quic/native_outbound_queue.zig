const std = @import("std");
const quic_zig = @import("quic");

const events = @import("../../events.zig");
const endpoint_mod = @import("endpoint.zig");
const native_framer = @import("native_framer.zig");
const options = @import("options.zig");

const Role = endpoint_mod.Role;

pub const QueuedKind = enum {
    inline_rpc,
    data_rpc,
};

pub const QueuedFrame = struct {
    sequence: u64,
    bytes: []u8,
    kind: QueuedKind,
    control: ?[]u8 = null,
    control_offset: usize = 0,
    data_offset: usize = 0,
    stream_id: ?u64 = null,
    stream_finished: bool = false,

    fn free(self: *QueuedFrame, allocator: std.mem.Allocator) void {
        if (self.control) |control| allocator.free(control);
        allocator.free(self.bytes);
        self.* = undefined;
    }
};

pub const OutboundQueue = struct {
    mu: std.atomic.Mutex = .unlocked,
    items: std.ArrayListUnmanaged(QueuedFrame) = .empty,
    head: usize = 0,
    queued_items: usize = 0,
    queued_bytes: usize = 0,
    queued_data_items: usize = 0,
    queued_data_bytes: usize = 0,
    max_items: usize = options.default_max_outbound_queue_items,
    max_bytes: usize = options.default_max_outbound_queue_bytes,
    inline_threshold: usize = options.default_native_inline_frame_threshold,
    max_control_frame_bytes: usize = options.default_native_max_control_frame_bytes,
    max_pending_data_streams: usize = options.default_native_max_pending_data_streams,
    max_pending_data_bytes: usize = options.default_native_max_pending_data_bytes,
    next_sequence: u64 = 0,
    next_uni_stream_id: u64 = 2,
    closed: bool = false,
    flushing: bool = false,

    pub fn init(
        role: Role,
        max_items: usize,
        max_bytes: usize,
        native_options: options.NativeOptions,
    ) OutboundQueue {
        return .{
            .max_items = max_items,
            .max_bytes = max_bytes,
            .inline_threshold = native_options.inline_frame_threshold,
            .max_control_frame_bytes = native_options.max_control_frame_bytes,
            .max_pending_data_streams = native_options.max_pending_data_streams,
            .max_pending_data_bytes = native_options.max_pending_data_bytes,
            .next_uni_stream_id = switch (role) {
                .client => 2,
                .server => 3,
            },
        };
    }

    fn lock(self: *OutboundQueue) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn enqueue(
        self: *OutboundQueue,
        allocator: std.mem.Allocator,
        frame: []const u8,
    ) error{ BrokenPipe, OutboundQueueFull, OutOfMemory }!void {
        self.lock();
        defer self.mu.unlock();
        if (self.closed) return error.BrokenPipe;

        const kind: QueuedKind = if (frame.len <= self.inline_threshold) .inline_rpc else .data_rpc;
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

    pub fn close(self: *OutboundQueue) void {
        self.lock();
        self.closed = true;
        self.mu.unlock();
    }

    pub fn isEmpty(self: *OutboundQueue) bool {
        self.lock();
        defer self.mu.unlock();
        return self.queued_items == 0;
    }

    pub fn drain(self: *OutboundQueue, allocator: std.mem.Allocator) void {
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
        self: *OutboundQueue,
        allocator: std.mem.Allocator,
        conn: *quic_zig.Connection,
        observer: ?events.Observer,
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
            self.releaseItem(allocator, item, observer);
        }
    }

    fn beginFlush(self: *OutboundQueue) bool {
        self.lock();
        defer self.mu.unlock();
        if (self.flushing) return false;
        self.flushing = true;
        return true;
    }

    fn endFlush(self: *OutboundQueue) void {
        self.lock();
        self.flushing = false;
        self.mu.unlock();
    }

    fn flushItem(
        self: *OutboundQueue,
        allocator: std.mem.Allocator,
        conn: *quic_zig.Connection,
        item: *QueuedFrame,
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
        self: *OutboundQueue,
        conn: *quic_zig.Connection,
        item: *QueuedFrame,
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

        const stream_id = item.stream_id orelse return error.InvalidFrame;
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

    fn writeQueuedControl(conn: *quic_zig.Connection, item: *QueuedFrame) !bool {
        const control = item.control orelse return error.InvalidFrame;
        while (item.control_offset < control.len) {
            const written = conn.streamWrite(options.baseline_stream_id, control[item.control_offset..]) catch |err| switch (err) {
                error.StreamNotFound => return false,
                else => return err,
            };
            if (written == 0) return false;
            item.control_offset += written;
        }
        return true;
    }

    fn takeFront(self: *OutboundQueue) ?QueuedFrame {
        self.lock();
        defer self.mu.unlock();
        if (self.head >= self.items.items.len) return null;
        const item = self.items.items[self.head];
        self.head += 1;
        return item;
    }

    fn requeueFront(
        self: *OutboundQueue,
        allocator: std.mem.Allocator,
        item: QueuedFrame,
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
        self: *OutboundQueue,
        allocator: std.mem.Allocator,
        item: QueuedFrame,
        observer: ?events.Observer,
    ) void {
        self.lock();
        self.subtractItemCounts(item);
        self.compactIfNeeded();
        self.mu.unlock();

        var owned = item;
        events.emitFrame(observer, .quic_native, .unknown, .sent, owned.bytes.len);
        owned.free(allocator);
    }

    fn subtractItemCounts(self: *OutboundQueue, item: QueuedFrame) void {
        self.queued_items -= 1;
        self.queued_bytes -= item.bytes.len;
        if (item.kind == .data_rpc) {
            self.queued_data_items -= 1;
            self.queued_data_bytes -= item.bytes.len;
        }
    }

    fn compactIfNeeded(self: *OutboundQueue) void {
        if (self.head == 0) return;
        if (self.head == self.items.items.len) {
            self.items.clearRetainingCapacity();
            self.head = 0;
            return;
        }
        if (self.head < 32 or self.head < self.items.items.len - self.head) return;

        const live = self.items.items.len - self.head;
        std.mem.copyForwards(
            QueuedFrame,
            self.items.items[0..live],
            self.items.items[self.head..],
        );
        self.items.items.len = live;
        self.head = 0;
    }
};
