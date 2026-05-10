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
};

pub fn reset(
    allocator: std.mem.Allocator,
    pending_data: *?PendingData,
) void {
    if (pending_data.*) |pending| allocator.free(pending.bytes);
    pending_data.* = null;
}

pub fn start(
    allocator: std.mem.Allocator,
    role: Role,
    max_message_bytes: usize,
    max_pending_data_bytes: usize,
    pending_data: *?PendingData,
    data: native_framer.DataRpc,
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
    };
}

pub fn readComplete(
    pending_data: *?PendingData,
    conn: *quic_zig.Connection,
) !?[]u8 {
    var pending = if (pending_data.*) |*pending| pending else return null;
    const stream = conn.stream(pending.stream_id) orelse return null;
    if (stream.recv.final_size) |final_size| {
        if (final_size != pending.bytes.len) return error.InvalidFrame;
    }

    while (pending.offset < pending.bytes.len) {
        const n = conn.streamRead(pending.stream_id, pending.bytes[pending.offset..]) catch |err| switch (err) {
            error.StreamNotFound => return null,
            else => return err,
        };
        if (n == 0) break;
        pending.offset += n;
        if (stream.recv.final_size) |final_size| {
            if (final_size != pending.bytes.len) return error.InvalidFrame;
        }
    }

    if (pending.offset < pending.bytes.len) return null;
    if (stream.recv.final_size == null) return null;

    const bytes = pending.bytes;
    pending_data.* = null;
    return bytes;
}

fn isPeerInitiatedUniStreamId(role: Role, stream_id: u64) bool {
    const is_uni = (stream_id & 0b10) != 0;
    if (!is_uni) return false;
    const client_initiated = (stream_id & 0b01) == 0;
    return client_initiated != (role == .client);
}
