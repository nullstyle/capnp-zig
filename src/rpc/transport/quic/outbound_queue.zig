const std = @import("std");
const quic_zig = @import("quic_zig");

const length_framer = @import("length_framer.zig");
const options = @import("options.zig");

const baseline_stream_id = options.baseline_stream_id;

const QueuedWrite = struct {
    bytes: []u8,
    offset: usize = 0,
};

pub const OutboundQueue = struct {
    mu: std.atomic.Mutex = .unlocked,
    items: std.ArrayListUnmanaged(QueuedWrite) = .empty,
    head: usize = 0,
    queued_items: usize = 0,
    queued_bytes: usize = 0,
    max_items: usize = options.default_max_outbound_queue_items,
    max_bytes: usize = options.default_max_outbound_queue_bytes,
    closed: bool = false,
    flushing: bool = false,

    pub fn init(max_items: usize, max_bytes: usize) OutboundQueue {
        return .{
            .max_items = max_items,
            .max_bytes = max_bytes,
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
        if (self.closed) {
            return error.BrokenPipe;
        }
        const encoded_len = std.math.add(usize, length_framer.length_prefix_bytes, frame.len) catch return error.OutboundQueueFull;
        const new_bytes = std.math.add(usize, self.queued_bytes, encoded_len) catch {
            return error.OutboundQueueFull;
        };
        if (self.queued_items >= self.max_items or new_bytes > self.max_bytes) {
            return error.OutboundQueueFull;
        }

        const bytes = allocator.alloc(u8, encoded_len) catch return error.OutOfMemory;
        errdefer allocator.free(bytes);
        std.mem.writeInt(u32, bytes[0..length_framer.length_prefix_bytes], @intCast(frame.len), .little);
        std.mem.copyForwards(u8, bytes[length_framer.length_prefix_bytes..], frame);
        self.items.append(allocator, .{ .bytes = bytes }) catch {
            return error.OutOfMemory;
        };
        self.queued_items += 1;
        self.queued_bytes = new_bytes;
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
        for (self.items.items[self.head..]) |item| allocator.free(item.bytes);
        self.items.deinit(allocator);
        self.items = .empty;
        self.head = 0;
        self.queued_items = 0;
        self.queued_bytes = 0;
    }

    pub fn flush(
        self: *OutboundQueue,
        allocator: std.mem.Allocator,
        conn: *quic_zig.Connection,
    ) !void {
        if (!self.beginFlush()) return;
        defer self.endFlush();

        while (true) {
            var item = self.takeFront() orelse return;
            while (true) {
                const remaining = item.bytes[item.offset..];
                if (remaining.len == 0) {
                    const item_len = item.bytes.len;
                    allocator.free(item.bytes);
                    self.releaseItem(item_len);
                    break;
                }

                const written = conn.streamWrite(baseline_stream_id, remaining) catch |err| switch (err) {
                    error.StreamNotFound => {
                        self.requeueFront(allocator, item);
                        return;
                    },
                    else => {
                        self.requeueFront(allocator, item);
                        return err;
                    },
                };
                if (written == 0) {
                    self.requeueFront(allocator, item);
                    return;
                }

                item.offset += written;
            }
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

    fn takeFront(self: *OutboundQueue) ?QueuedWrite {
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
        item: QueuedWrite,
    ) void {
        var free_item = false;

        self.lock();
        if (self.closed) {
            self.queued_items -= 1;
            self.queued_bytes -= item.bytes.len;
            free_item = true;
        } else {
            std.debug.assert(self.head > 0);
            self.head -= 1;
            self.items.items[self.head] = item;
        }
        self.mu.unlock();

        if (free_item) allocator.free(item.bytes);
    }

    fn releaseItem(self: *OutboundQueue, item_len: usize) void {
        self.lock();
        self.queued_items -= 1;
        self.queued_bytes -= item_len;
        self.compactIfNeeded();
        self.mu.unlock();
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
            QueuedWrite,
            self.items.items[0..live],
            self.items.items[self.head..],
        );
        self.items.items.len = live;
        self.head = 0;
    }
};
