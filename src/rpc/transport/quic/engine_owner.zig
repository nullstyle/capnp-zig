const std = @import("std");

const baseline_engine = @import("baseline_engine.zig");
const endpoint_mod = @import("endpoint.zig");
const native_engine = @import("native_engine.zig");

const Role = endpoint_mod.Role;

/// Shared callback surface that can be projected into either QUIC stream
/// engine's owner view.
pub const Owner = struct {
    ptr: *anyopaque,
    allocator: std.mem.Allocator,
    role: Role,
    max_message_bytes: usize,
    stream_read_buf: []u8,
    is_closing: *const fn (ptr: *anyopaque) bool,
    callbacks_ready: *const fn (ptr: *anyopaque) bool,
    dispatch_rpc_frame: *const fn (ptr: *anyopaque, frame: []const u8) anyerror!void,
    terminate_frame_error: *const fn (ptr: *anyopaque, err: anyerror) void,
    deinit_requested: *const fn (ptr: *anyopaque) bool,

    pub fn baseline(self: Owner) baseline_engine.Owner {
        return .{
            .ptr = self.ptr,
            .allocator = self.allocator,
            .role = self.role,
            .stream_read_buf = self.stream_read_buf,
            .is_closing = self.is_closing,
            .callbacks_ready = self.callbacks_ready,
            .dispatch_rpc_frame = self.dispatch_rpc_frame,
            .terminate_frame_error = self.terminate_frame_error,
            .deinit_requested = self.deinit_requested,
        };
    }

    pub fn native(self: Owner) native_engine.Owner {
        return .{
            .ptr = self.ptr,
            .allocator = self.allocator,
            .role = self.role,
            .max_message_bytes = self.max_message_bytes,
            .stream_read_buf = self.stream_read_buf,
            .is_closing = self.is_closing,
            .dispatch_rpc_frame = self.dispatch_rpc_frame,
            .terminate_frame_error = self.terminate_frame_error,
            .deinit_requested = self.deinit_requested,
        };
    }
};
