const std = @import("std");
const quic_zig = @import("quic");

const events = @import("../../events.zig");
const endpoint_mod = @import("endpoint.zig");
const native_framer = @import("native_framer.zig");
const native_outbound_queue = @import("native_outbound_queue.zig");
const native_pending_data = @import("native_pending_data.zig");
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
    observer: ?events.Observer,
    max_message_bytes: usize,
    stream_read_buf: []u8,
    is_closing: *const fn (ptr: *anyopaque) bool,
    dispatch_rpc_frame: *const fn (ptr: *anyopaque, frame: []const u8) anyerror!void,
    terminate_frame_error: *const fn (ptr: *anyopaque, err: anyerror) void,
    deinit_requested: *const fn (ptr: *anyopaque) bool,
};

pub const NativeQueuedKind = native_outbound_queue.QueuedKind;
pub const NativeQueuedFrame = native_outbound_queue.QueuedFrame;
pub const NativeOutboundQueue = native_outbound_queue.OutboundQueue;
pub const NativePendingData = native_pending_data.PendingData;

fn isNativeFrameError(err: anyerror) bool {
    return switch (err) {
        error.InvalidFrame, error.FrameTooLarge, error.OutOfMemory, error.DataStreamTimeout => true,
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
    /// Wall-clock stall budget for an announced inbound data stream, or `null`
    /// when disabled. See `NativeOptions.data_stream_completion_deadline_us`.
    data_stream_completion_deadline_us: ?u64 = null,

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
            .data_stream_completion_deadline_us = native_options.data_stream_completion_deadline_us,
        };
    }

    pub fn deinit(self: *NativeEngine, allocator: std.mem.Allocator) void {
        self.inbound.deinit();
        self.outbound.drain(allocator);
        native_pending_data.reset(allocator, &self.pending_data);
    }

    pub fn close(self: *NativeEngine) void {
        self.outbound.close();
    }

    pub fn resetInbound(self: *NativeEngine, allocator: std.mem.Allocator) void {
        self.inbound.reset();
        native_pending_data.reset(allocator, &self.pending_data);
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
        now_us: u64,
    ) !void {
        if (!try self.ensureControlStream(owner.role, conn)) return;
        if (!try self.flushPreamble(conn)) return;
        try self.outbound.flush(owner.allocator, conn, owner.observer);
        self.readControlStream(owner, conn) catch |err| {
            if (isNativeFrameError(err)) {
                owner.terminate_frame_error(owner.ptr, err);
                return;
            }
            return err;
        };
        self.processControlFrames(owner, conn, now_us) catch |err| {
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
        now_us: u64,
    ) !void {
        while (!owner.is_closing(owner.ptr)) {
            if (self.pending_data != null) {
                if (!try self.readPendingData(owner, conn, now_us)) return;
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
                    try self.startPendingData(owner, data, now_us);
                    if (!try self.readPendingData(owner, conn, now_us)) return;
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
        now_us: u64,
    ) !void {
        try native_pending_data.start(
            owner.allocator,
            owner.role,
            owner.max_message_bytes,
            self.outbound.max_pending_data_bytes,
            &self.pending_data,
            data,
            now_us,
            self.data_stream_completion_deadline_us,
        );
    }

    fn readPendingData(
        self: *NativeEngine,
        owner: Owner,
        conn: *quic_zig.Connection,
        now_us: u64,
    ) !bool {
        const bytes = (try native_pending_data.readComplete(
            &self.pending_data,
            conn,
            now_us,
            self.data_stream_completion_deadline_us,
        )) orelse {
            return self.pending_data == null;
        };
        defer owner.allocator.free(bytes);
        self.next_in_sequence +%= 1;
        try owner.dispatch_rpc_frame(owner.ptr, bytes);
        return true;
    }
};
