const std = @import("std");
const quic_zig = @import("quic");

const events = @import("../../events.zig");
const endpoint_mod = @import("endpoint.zig");
const length_framer = @import("length_framer.zig");
const outbound_queue = @import("outbound_queue.zig");
const quic_options = @import("options.zig");

const Role = endpoint_mod.Role;
const LengthDelimitedFramer = length_framer.LengthDelimitedFramer;
const OutboundQueue = outbound_queue.OutboundQueue;

/// Narrow callback surface used by the baseline engine to avoid depending on
/// the concrete `Connection` type. `Connection` owns lifecycle and callbacks;
/// the engine owns stream 0 readiness and length-delimited frame state.
pub const Owner = struct {
    ptr: *anyopaque,
    allocator: std.mem.Allocator,
    role: Role,
    observer: ?events.Observer,
    stream_read_buf: []u8,
    is_closing: *const fn (ptr: *anyopaque) bool,
    callbacks_ready: *const fn (ptr: *anyopaque) bool,
    dispatch_rpc_frame: *const fn (ptr: *anyopaque, frame: []const u8) anyerror!void,
    terminate_frame_error: *const fn (ptr: *anyopaque, err: anyerror) void,
    deinit_requested: *const fn (ptr: *anyopaque) bool,
};

/// Stream engine for the conservative length-delimited QUIC transport mode.
pub const BaselineEngine = struct {
    ready: bool = false,
    inbound: LengthDelimitedFramer,
    outbound: OutboundQueue = .{},
    /// Resumed-dial (0-RTT) posture: open stream 0 before the handshake
    /// completes so queued frames ride early data. quic-zig requeues the
    /// staged bytes verbatim at 1-RTT if the server rejects 0-RTT, so
    /// this can cost a round trip but never data. False for servers and
    /// fresh dials.
    early_open: bool = false,

    pub fn init(
        allocator: std.mem.Allocator,
        max_message_bytes: usize,
        max_outbound_queue_items: usize,
        max_outbound_queue_bytes: usize,
        early_open: bool,
    ) BaselineEngine {
        return .{
            .inbound = LengthDelimitedFramer.init(allocator, max_message_bytes),
            .outbound = OutboundQueue.init(max_outbound_queue_items, max_outbound_queue_bytes),
            .early_open = early_open,
        };
    }

    pub fn deinit(self: *BaselineEngine, allocator: std.mem.Allocator) void {
        self.outbound.drain(allocator);
        self.inbound.deinit();
    }

    pub fn close(self: *BaselineEngine) void {
        self.outbound.close();
    }

    pub fn resetInbound(self: *BaselineEngine) void {
        self.inbound.reset();
    }

    pub fn enqueue(
        self: *BaselineEngine,
        allocator: std.mem.Allocator,
        frame: []const u8,
    ) !void {
        try self.outbound.enqueue(allocator, frame);
    }

    pub fn outboundEmpty(self: *BaselineEngine) bool {
        return self.outbound.isEmpty();
    }

    pub fn hasImmediateWork(
        self: *BaselineEngine,
        role: Role,
        conn: *quic_zig.Connection,
    ) bool {
        if (self.ready) return true;
        return role == .client and (conn.handshakeDone() or self.early_open);
    }

    pub fn service(
        self: *BaselineEngine,
        owner: Owner,
        conn: *quic_zig.Connection,
    ) !void {
        if (!try self.ensureStream(owner.role, conn)) return;
        try self.outbound.flush(owner.allocator, conn, owner.observer);
        // Frames can buffer in the inbound framer while callbacks are not
        // yet bound (0-RTT data read during the handshake, before `start`).
        // `readStream` only dispatches after reading NEW bytes, so without
        // this pass a frame that arrived early and saw no follow-up
        // traffic would sit parsed-but-undelivered forever.
        try self.dispatchAvailableFrames(owner);
        try self.readStream(owner, conn);
    }

    fn ensureStream(
        self: *BaselineEngine,
        role: Role,
        conn: *quic_zig.Connection,
    ) !bool {
        if (self.ready) return true;
        if (conn.stream(quic_options.baseline_stream_id) != null) {
            self.ready = true;
            return true;
        }
        if (role == .client) {
            // A resumed dial (`early_open`) opens the stream pre-handshake
            // so the flush below sends the queued frames as 0-RTT.
            if (!conn.handshakeDone() and !self.early_open) return false;
            _ = conn.openBidi(quic_options.baseline_stream_id) catch |err| switch (err) {
                error.StreamAlreadyOpen => {},
                else => return err,
            };
            self.ready = true;
            return true;
        }
        return false;
    }

    fn readStream(
        self: *BaselineEngine,
        owner: Owner,
        conn: *quic_zig.Connection,
    ) !void {
        while (!owner.is_closing(owner.ptr)) {
            const n = conn.streamRead(quic_options.baseline_stream_id, owner.stream_read_buf) catch |err| switch (err) {
                error.StreamNotFound => return,
                else => return err,
            };
            if (n == 0) return;
            try self.inbound.push(owner.stream_read_buf[0..n]);
            try self.dispatchAvailableFrames(owner);
        }
    }

    pub fn dispatchAvailableFrames(
        self: *BaselineEngine,
        owner: Owner,
    ) !void {
        while (true) {
            if (!owner.callbacks_ready(owner.ptr)) return;
            const frame = self.inbound.popFrame() catch |err| {
                owner.terminate_frame_error(owner.ptr, err);
                return;
            };
            const bytes = frame orelse return;
            defer owner.allocator.free(bytes);
            try owner.dispatch_rpc_frame(owner.ptr, bytes);
            if (owner.deinit_requested(owner.ptr)) return;
        }
    }
};
