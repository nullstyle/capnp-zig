const std = @import("std");
const quic_zig = @import("quic_zig");

const endpoint_mod = @import("endpoint.zig");
const native_framer = @import("native_framer.zig");

const Role = endpoint_mod.Role;

pub const PendingData = struct {
    sequence: u64,
    stream_id: u64,
    bytes: []u8,
    offset: usize = 0,
    /// Absolute wall-clock deadline (microseconds) by which the stream must
    /// make forward progress, or `null` when the completion deadline is
    /// disabled. Re-armed on every read that delivers bytes.
    deadline_us: ?u64 = null,
};

pub fn reset(
    allocator: std.mem.Allocator,
    pending_data: *?PendingData,
) void {
    if (pending_data.*) |pending| allocator.free(pending.bytes);
    pending_data.* = null;
}

fn deadlineFrom(now_us: u64, completion_deadline_us: ?u64) ?u64 {
    const window = completion_deadline_us orelse return null;
    return now_us +| window;
}

pub fn start(
    allocator: std.mem.Allocator,
    role: Role,
    max_message_bytes: usize,
    max_pending_data_bytes: usize,
    pending_data: *?PendingData,
    data: native_framer.DataRpc,
    now_us: u64,
    completion_deadline_us: ?u64,
) !void {
    if (pending_data.* != null) return error.InvalidFrame;
    if (!isPeerInitiatedUniStreamId(role, data.stream_id)) return error.InvalidFrame;
    if (data.length == 0 or data.length > max_message_bytes) return error.FrameTooLarge;
    if (data.length > max_pending_data_bytes) return error.FrameTooLarge;

    const bytes = try allocator.alloc(u8, data.length);
    errdefer allocator.free(bytes);
    pending_data.* = .{
        .sequence = data.sequence,
        .stream_id = data.stream_id,
        .bytes = bytes,
        .deadline_us = deadlineFrom(now_us, completion_deadline_us),
    };
}

pub fn readComplete(
    pending_data: *?PendingData,
    conn: *quic_zig.Connection,
    now_us: u64,
    completion_deadline_us: ?u64,
) !?[]u8 {
    var pending = if (pending_data.*) |*pending| pending else return null;

    // Not yet complete when the stream has not been opened at all. The
    // completion deadline still applies so a peer that announces a data stream
    // then never opens (or drips into) it cannot pin the session open.
    const stream = conn.stream(pending.stream_id) orelse
        return incompleteOrTimeout(pending, false, now_us, completion_deadline_us);
    if (stream.recv.final_size) |final_size| {
        if (final_size != pending.bytes.len) return error.InvalidFrame;
    }

    var made_progress = false;
    while (pending.offset < pending.bytes.len) {
        const n = conn.streamRead(pending.stream_id, pending.bytes[pending.offset..]) catch |err| switch (err) {
            error.StreamNotFound => return incompleteOrTimeout(pending, made_progress, now_us, completion_deadline_us),
            else => return err,
        };
        if (n == 0) break;
        pending.offset += n;
        made_progress = true;
        if (stream.recv.final_size) |final_size| {
            if (final_size != pending.bytes.len) return error.InvalidFrame;
        }
    }

    if (pending.offset < pending.bytes.len or stream.recv.final_size == null) {
        return incompleteOrTimeout(pending, made_progress, now_us, completion_deadline_us);
    }

    const bytes = pending.bytes;
    pending_data.* = null;
    return bytes;
}

/// Handle an incomplete read: re-arm the stall window when the stream made
/// forward progress, otherwise abort once the completion deadline has elapsed
/// so a peer that announces a data stream then drips or withholds bytes cannot
/// pin the session open indefinitely. Returns null (still pending) unless the
/// deadline fired, in which case it returns `error.DataStreamTimeout`.
fn incompleteOrTimeout(
    pending: *PendingData,
    made_progress: bool,
    now_us: u64,
    completion_deadline_us: ?u64,
) !?[]u8 {
    if (made_progress) {
        pending.deadline_us = deadlineFrom(now_us, completion_deadline_us);
    } else if (pending.deadline_us) |deadline| {
        if (now_us >= deadline) return error.DataStreamTimeout;
    }
    return null;
}

fn isPeerInitiatedUniStreamId(role: Role, stream_id: u64) bool {
    const is_uni = (stream_id & 0b10) != 0;
    if (!is_uni) return false;
    const client_initiated = (stream_id & 0b01) == 0;
    return client_initiated != (role == .client);
}
