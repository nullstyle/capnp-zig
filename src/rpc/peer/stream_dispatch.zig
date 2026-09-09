//! Experimental per-server streaming delivery. Queued calls own their frame
//! and parameter capabilities until dispatch, cancellation, or peer teardown.
const std = @import("std");
const message = @import("../../serialization/message.zig");
const protocol = @import("../wire/protocol.zig");
const table = @import("../caps/table.zig");

pub const Limits = struct {
    max_calls: usize = 64,
    max_bytes: usize = 1024 * 1024,
};

fn framedSize(msg: *const message.Message) !usize {
    var size: usize = ((msg.segments.len + 2) & ~@as(usize, 1)) * 4;
    for (msg.segments) |segment| size = try std.math.add(usize, size, segment.len);
    return size;
}
fn copyFrame(allocator: std.mem.Allocator, msg: *const message.Message, size: usize) ![]u8 {
    const frame = try allocator.alloc(u8, size);
    const header_size = ((msg.segments.len + 2) & ~@as(usize, 1)) * 4;
    @memset(frame[0..header_size], 0);
    std.mem.writeInt(u32, frame[0..4], @intCast(msg.segments.len - 1), .little);
    var offset = header_size;
    for (msg.segments, 0..) |segment, index| {
        std.mem.writeInt(u32, frame[(index + 1) * 4 ..][0..4], @intCast(segment.len / 8), .little);
        @memcpy(frame[offset..][0..segment.len], segment);
        offset += segment.len;
    }
    return frame;
}

pub fn Registry(comptime Peer: type) type {
    return struct {
        const Self = @This();
        pub const DispatchFn = *const fn (*anyopaque, *Peer, protocol.Call, *const table.InboundCapTable) anyerror!void;
        const Pending = struct {
            id: u32,
            frame: []u8,
            caps: table.InboundCapTable,
        };
        const Active = struct { id: u32, bytes: usize, deferred: bool = false, token: u64 = 0 };
        const Queue = struct {
            ctx: *anyopaque,
            dispatch: DispatchFn,
            active: ?Active = null,
            dispatching: bool = false,
            failed: bool = false,
            pending: std.ArrayList(Pending) = .empty,
        };
        queues: std.AutoHashMapUnmanaged(usize, *Queue) = .empty,
        limits: Limits = .{},
        outstanding_calls: usize = 0,
        outstanding_bytes: usize = 0,
        next_token: u64 = 0,

        fn release(self: *Self, size: usize) void {
            std.debug.assert(self.outstanding_calls != 0 and self.outstanding_bytes >= size);
            self.outstanding_calls -= 1;
            self.outstanding_bytes -= size;
        }
        fn admit(self: *Self, size: usize) !void {
            if (self.limits.max_bytes != 0 and size > self.limits.max_bytes) return error.StreamCallTooLarge;
            const bytes = std.math.add(usize, self.outstanding_bytes, size) catch return error.StreamByteLimitExceeded;
            if (self.limits.max_calls != 0 and self.outstanding_calls >= self.limits.max_calls) return error.StreamInFlightLimitExceeded;
            if (self.limits.max_bytes != 0 and bytes > self.limits.max_bytes) return error.StreamByteLimitExceeded;
            self.outstanding_calls = try std.math.add(usize, self.outstanding_calls, 1);
            self.outstanding_bytes = bytes;
        }
        fn releasePending(peer: *Peer, pending: *Pending) void {
            for (pending.caps.entries) |cap| if (cap == .exported) peer.releaseHandoffHeldExport(cap.exported.id);
            peer.releaseInboundCaps(&pending.caps) catch {};
            pending.caps.deinit();
            peer.allocator.free(pending.frame);
        }
        fn queue(self: *Self, peer: *Peer, q: *Queue, call: protocol.Call, caps: *const table.InboundCapTable, size: usize) !void {
            const frame = try copyFrame(peer.allocator, call.params.content.message, size);
            errdefer peer.allocator.free(frame);
            var owned = try caps.clone();
            errdefer owned.deinit();
            // Transfer the already-noted wire import refs only after all fallible
            // preparation; local exports need an independent hold across delay.
            var held: usize = 0;
            errdefer for (owned.entries[0..held]) |cap| {
                if (cap == .exported) peer.releaseHandoffHeldExport(cap.exported.id);
            };
            for (owned.entries) |cap| {
                if (cap == .exported) try peer.noteHandoffExportRef(cap.exported.id);
                held += 1;
            }
            @memset(owned.retained, false);
            try q.pending.append(peer.allocator, .{ .id = call.question_id, .frame = frame, .caps = owned });
            @memset(caps.retained, true);
            _ = self;
        }
        pub fn dispatch(self: *Self, peer: *Peer, ctx: *anyopaque, handler: DispatchFn, call: protocol.Call, caps: *const table.InboundCapTable) !void {
            peer.enterStreamingOperation();
            defer peer.leaveStreamingOperation();
            const key = @intFromPtr(ctx);
            const size = try framedSize(call.params.content.message);
            if (!self.queues.contains(key) and self.limits.max_calls != 0 and self.queues.count() >= self.limits.max_calls) return error.StreamInFlightLimitExceeded;
            const slot = try self.queues.getOrPut(peer.allocator, key);
            const q = if (slot.found_existing) slot.value_ptr.* else blk: {
                const created = peer.allocator.create(Queue) catch |err| {
                    _ = self.queues.remove(key);
                    return err;
                };
                created.* = .{ .ctx = ctx, .dispatch = handler };
                slot.value_ptr.* = created;
                break :blk created;
            };
            errdefer self.maybeRemove(peer, key, q);
            if (q.failed) return error.StreamingCallFailed;
            try self.admit(size);
            if (q.active != null or q.dispatching) {
                self.queue(peer, q, call, caps, size) catch |err| {
                    self.release(size);
                    return err;
                };
                return;
            }
            q.active = .{ .id = call.question_id, .bytes = size };
            q.dispatching = true;
            handler(ctx, peer, call, caps) catch |err| {
                q.dispatching = false;
                self.fail(peer, q);
                return err;
            };
            q.dispatching = false;
            self.releaseSynchronous(q, call.question_id);
            self.drain(peer, key, q);
        }
        fn releaseSynchronous(self: *Self, q: *Queue, id: u32) void {
            if (q.active) |active| if (active.id == id and !active.deferred) {
                q.active = null;
                self.release(active.bytes);
            };
        }
        pub fn begin(self: *Self, ctx: *anyopaque, id: u32) !u64 {
            const q = self.queues.get(@intFromPtr(ctx)) orelse return error.StreamingCallClosed;
            const active = if (q.active) |*a| a else return error.StreamingCallClosed;
            if (active.id != id or active.deferred) return error.StreamingCallClosed;
            const token = std.math.add(u64, self.next_token, 1) catch return error.StreamSequenceExhausted;
            self.next_token = token;
            active.deferred = true;
            active.token = token;
            return token;
        }
        pub fn complete(self: *Self, peer: *Peer, ctx: *anyopaque, id: u32, token: u64, reason: ?[]const u8) !void {
            peer.enterStreamingOperation();
            defer peer.leaveStreamingOperation();
            const key = @intFromPtr(ctx);
            const q = self.queues.get(key) orelse return error.StreamingCallClosed;
            const active = q.active orelse return error.StreamingCallClosed;
            if (active.id != id or active.token != token or !active.deferred or q.failed) return error.StreamingCallClosed;
            const was_dispatching = q.dispatching;
            q.dispatching = true;
            const result = if (reason) |text| peer.sendReturnException(id, text) else peer.sendReturnEmptyStruct(id);
            q.dispatching = was_dispatching;
            result catch |err| {
                self.fail(peer, q);
                return err;
            };
            if (q.active) |current| if (current.id == id) {
                q.active = null;
                self.release(current.bytes);
            };
            if (reason != null) self.fail(peer, q);
            if (!q.dispatching) self.drain(peer, key, q);
        }
        fn fail(self: *Self, peer: *Peer, q: *Queue) void {
            q.failed = true;
            if (q.active) |active| {
                q.active = null;
                self.release(active.bytes);
            }
            // Detach each entry before callbacks: a Return/Release may reenter.
            while (q.pending.items.len != 0) {
                var item = q.pending.orderedRemove(0);
                self.release(item.frame.len);
                peer.sendReturnException(item.id, "stream failed") catch {};
                releasePending(peer, &item);
            }
        }
        fn drain(self: *Self, peer: *Peer, key: usize, q: *Queue) void {
            while (!q.failed and q.active == null and q.pending.items.len != 0 and !peer.is_shutting_down) {
                var item = q.pending.orderedRemove(0);
                defer releasePending(peer, &item);
                var decoded = protocol.DecodedMessage.init(peer.allocator, item.frame) catch {
                    self.release(item.frame.len);
                    peer.sendReturnException(item.id, "stream decode failed") catch {};
                    self.fail(peer, q);
                    break;
                };
                defer decoded.deinit();
                const call = decoded.asCall() catch unreachable;
                q.active = .{ .id = item.id, .bytes = item.frame.len };
                q.dispatching = true;
                q.dispatch(q.ctx, peer, call, &item.caps) catch |err| {
                    q.dispatching = false;
                    peer.sendReturnException(item.id, @errorName(err)) catch {};
                    self.fail(peer, q);
                    break;
                };
                q.dispatching = false;
                self.releaseSynchronous(q, item.id);
            }
            self.maybeRemove(peer, key, q);
        }
        fn maybeRemove(self: *Self, peer: *Peer, key: usize, q: *Queue) void {
            if (!q.failed and !q.dispatching and q.active == null and q.pending.items.len == 0) {
                _ = self.queues.remove(key);
                q.pending.deinit(peer.allocator);
                peer.allocator.destroy(q);
            }
        }
        pub fn cancel(self: *Self, peer: *Peer, id: u32) void {
            peer.enterStreamingOperation();
            defer peer.leaveStreamingOperation();
            var it = self.queues.iterator();
            while (it.next()) |entry| {
                const q = entry.value_ptr.*;
                if (q.active) |active| if (active.id == id) {
                    self.fail(peer, q);
                    peer.sendReturnException(id, "stream canceled") catch {};
                    return;
                };
                for (q.pending.items, 0..) |item, index| if (item.id == id) {
                    var removed = q.pending.orderedRemove(index);
                    self.release(removed.frame.len);
                    releasePending(peer, &removed);
                    peer.sendReturnException(id, "stream canceled") catch {};
                    return;
                };
            }
        }
        pub fn deinit(self: *Self, peer: *Peer) void {
            var queues = self.queues;
            self.queues = .empty;
            self.outstanding_calls = 0;
            self.outstanding_bytes = 0;
            var it = queues.valueIterator();
            while (it.next()) |q_ptr| {
                const q = q_ptr.*;
                for (q.pending.items) |*item| releasePending(peer, item);
                q.pending.deinit(peer.allocator);
                peer.allocator.destroy(q);
            }
            queues.deinit(peer.allocator);
        }
    };
}
