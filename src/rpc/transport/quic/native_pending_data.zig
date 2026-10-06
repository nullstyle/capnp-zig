const std = @import("std");
const quic_zig = @import("quic");

const endpoint_mod = @import("endpoint.zig");
const native_framer = @import("native_framer.zig");

const Role = endpoint_mod.Role;

const log = std.log.scoped(.rpc_quic_transport);

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

/// Read the announced data stream as far as it goes. Returns the whole
/// message once every announced byte is in hand, null while bytes are still
/// to come, and an error when the stream can no longer complete it.
///
/// `conn.stream(id)` gives a view with `recv.final_size` (null until the
/// stream ends) and `recv.reset` (null unless the peer reset the stream,
/// else a value with `error_code`). quic-zig's own stream has both; so has
/// the embedded seat's buffered view. When there is no view,
/// `conn.streamRecvEnd(id)` and `conn.streamRecvWasReaped(id)` (quic-zig's
/// `Connection` functions) tell an ended stream from one that is not open
/// yet.
pub fn readComplete(
    pending_data: *?PendingData,
    conn: anytype,
    now_us: u64,
    completion_deadline_us: ?u64,
) !?[]u8 {
    var pending = if (pending_data.*) |*pending| pending else return null;

    const stream = conn.stream(pending.stream_id) orelse
        return settleWithoutStream(pending_data, pending, conn, now_us, completion_deadline_us);
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

    if (pending.offset < pending.bytes.len) {
        // A reset stream gives no more bytes: quic-zig drops the unread ones
        // when the RESET_STREAM arrives. Fail now, not at the deadline.
        if (stream.recv.reset) |reset_info| {
            log.debug("native data stream {d} reset by the peer (code {d}) after {d} of {d} bytes", .{
                pending.stream_id, reset_info.error_code, pending.offset, pending.bytes.len,
            });
            return error.DataStreamReset;
        }
        return incompleteOrTimeout(pending, made_progress, now_us, completion_deadline_us);
    }
    if (stream.recv.final_size == null) {
        return incompleteOrTimeout(pending, made_progress, now_us, completion_deadline_us);
    }

    return take(pending_data, pending);
}

/// The announced data stream has no view: it is not open yet, or quic-zig
/// reclaimed it in a `tick` (its stream GC frees a stream once the receive
/// half has ended, also before this read when its FIN or RESET arrived
/// alone). No byte can come from a reclaimed stream, so settle the frame
/// now. Do not wait for the completion deadline.
fn settleWithoutStream(
    pending_data: *?PendingData,
    pending: *PendingData,
    conn: anytype,
    now_us: u64,
    completion_deadline_us: ?u64,
) !?[]u8 {
    if (conn.streamRecvEnd(pending.stream_id)) |end| {
        // quic-zig still knows how the stream ended. Judge it as the live
        // stream: a final size other than the announced length fails the
        // frame, and all the announced bytes complete it.
        if (end.final_size != pending.bytes.len) return error.InvalidFrame;
        if (pending.offset == pending.bytes.len) return take(pending_data, pending);
        // Bytes are missing, and none can come: the peer reset the stream
        // (quic-zig dropped the unread bytes), or this side stopped it.
        if (end.reset_code) |code| {
            log.debug("native data stream {d} reset by the peer (code {d}) after {d} of {d} bytes", .{
                pending.stream_id, code, pending.offset, pending.bytes.len,
            });
        } else {
            log.debug("native data stream {d} ended unclean (stopped: {}) after {d} of {d} bytes", .{
                pending.stream_id, end.stopped, pending.offset, pending.bytes.len,
            });
        }
        return error.DataStreamReset;
    }
    if (conn.streamRecvWasReaped(pending.stream_id)) {
        // quic-zig reclaimed the stream and no longer knows how it ended
        // (its note of the end is gone). The length came on the control
        // stream, so all the announced bytes complete the frame. With bytes
        // missing the stream is cut: fail now.
        if (pending.offset == pending.bytes.len) return take(pending_data, pending);
        log.debug("native data stream {d} ended with its end unknown after {d} of {d} bytes", .{
            pending.stream_id, pending.offset, pending.bytes.len,
        });
        return error.DataStreamReset;
    }
    // Not open yet. The completion deadline still applies, so a peer that
    // announces a data stream and then never opens it cannot keep the
    // session open.
    return incompleteOrTimeout(pending, false, now_us, completion_deadline_us);
}

/// Give the completed message to the caller, who frees it. `pending` is the
/// payload of `pending_data`, which this clears.
fn take(pending_data: *?PendingData, pending: *PendingData) []u8 {
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
