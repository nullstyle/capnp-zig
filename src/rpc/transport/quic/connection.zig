const std = @import("std");
const builtin = @import("builtin");
const nullq = @import("nullq");

const length_framer = @import("length_framer.zig");
const nullq_adapter = @import("nullq_adapter.zig");
const quic_options = @import("options.zig");
const outbound_queue = @import("outbound_queue.zig");

const log = std.log.scoped(.rpc_quic_transport);
const Net = std.Io.net;

const ClientOptions = quic_options.ClientOptions;
const ServerOptions = quic_options.ServerOptions;
const LengthDelimitedFramer = length_framer.LengthDelimitedFramer;
const OutboundQueue = outbound_queue.OutboundQueue;

const Role = enum { client, server };

const Endpoint = union(Role) {
    client: nullq.Client,
    server: ServerEndpoint,
};

const ServerEndpoint = struct {
    server: nullq.Server,
    slot: ?*nullq.Server.Slot = null,
};

/// A single vat-to-vat Cap'n Proto RPC session over QUIC.
///
/// Client-side instances own one `nullq.Client` and one UDP socket. Server-side
/// instances own one `nullq.Server` configured for one active QUIC connection by
/// default; this intentionally models the first QUIC design step as "one QUIC
/// connection equals one authenticated vat session". A future multi-client
/// listener can fan out `nullq.Server.Slot`s to multiple `Connection`s without
/// changing the peer-facing send/start/close shape.
pub const Connection = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    role: Role,
    socket: Net.Socket,
    endpoint: Endpoint,
    remote_addr: ?Net.IpAddress = null,
    start_timestamp: std.Io.Timestamp,
    receive_timeout: std.Io.Duration,
    udp_rx_buf: []u8,
    udp_tx_buf: []u8,
    stream_read_buf: []u8,
    max_message_bytes: usize,
    inbound: LengthDelimitedFramer,
    outbound: OutboundQueue = .{},
    close_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    socket_closed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    baseline_ready: bool = false,

    ctx: ?*anyopaque = null,
    on_message: ?*const fn (conn: *Connection, frame: []const u8) anyerror!void = null,
    on_error: ?*const fn (conn: *Connection, err: anyerror) void = null,
    on_close: ?*const fn (conn: *Connection) void = null,
    callback_depth: usize = 0,
    deinit_requested: bool = false,
    deinitialized: bool = false,

    owner_thread_id: ?std.Thread.Id = null,

    pub fn initClient(
        allocator: std.mem.Allocator,
        io: std.Io,
        options: ClientOptions,
    ) !Connection {
        if (options.alpn_protocols.len == 0) return error.InvalidConfig;
        if (options.udp_rx_buffer_size == 0 or
            options.udp_tx_buffer_size == 0 or
            options.stream_read_buffer_size == 0 or
            options.max_message_bytes == 0 or
            options.max_outbound_queue_items == 0 or
            options.max_outbound_queue_bytes == 0)
        {
            return error.InvalidConfig;
        }

        const local_addr = options.local_addr orelse nullq_adapter.defaultClientBindAddress(options.remote_addr);
        const socket = try Net.IpAddress.bind(&local_addr, io, .{
            .mode = .dgram,
            .protocol = .udp,
        });
        errdefer socket.close(io);

        var client = try nullq.Client.connect(.{
            .allocator = allocator,
            .server_name = options.server_name,
            .alpn_protocols = options.alpn_protocols,
            .transport_params = options.transport_params,
            .ca_pem = options.ca_pem,
        });
        errdefer client.deinit();

        return try initCommon(
            allocator,
            io,
            .client,
            socket,
            .{ .client = client },
            options.receive_timeout,
            options.udp_rx_buffer_size,
            options.udp_tx_buffer_size,
            options.stream_read_buffer_size,
            options.max_message_bytes,
            options.max_outbound_queue_items,
            options.max_outbound_queue_bytes,
            options.remote_addr,
        );
    }

    pub fn initServer(
        allocator: std.mem.Allocator,
        io: std.Io,
        options: ServerOptions,
    ) !Connection {
        const server_config = try quic_options.nullqServerConfigFromOptions(allocator, options);

        const socket = try Net.IpAddress.bind(&options.listen_addr, io, .{
            .mode = .dgram,
            .protocol = .udp,
        });
        errdefer socket.close(io);

        var server = try nullq.Server.init(server_config);
        errdefer server.deinit();

        return try initCommon(
            allocator,
            io,
            .server,
            socket,
            .{ .server = .{ .server = server } },
            options.receive_timeout,
            options.udp_rx_buffer_size,
            options.udp_tx_buffer_size,
            options.stream_read_buffer_size,
            options.max_message_bytes,
            options.max_outbound_queue_items,
            options.max_outbound_queue_bytes,
            null,
        );
    }

    fn initCommon(
        allocator: std.mem.Allocator,
        io: std.Io,
        role: Role,
        socket: Net.Socket,
        endpoint: Endpoint,
        receive_timeout: std.Io.Duration,
        udp_rx_buffer_size: usize,
        udp_tx_buffer_size: usize,
        stream_read_buffer_size: usize,
        max_message_bytes: usize,
        max_outbound_queue_items: usize,
        max_outbound_queue_bytes: usize,
        remote_addr: ?Net.IpAddress,
    ) !Connection {
        const udp_rx_buf = try allocator.alloc(u8, udp_rx_buffer_size);
        errdefer allocator.free(udp_rx_buf);
        const udp_tx_buf = try allocator.alloc(u8, udp_tx_buffer_size);
        errdefer allocator.free(udp_tx_buf);
        const stream_read_buf = try allocator.alloc(u8, stream_read_buffer_size);
        errdefer allocator.free(stream_read_buf);

        return .{
            .allocator = allocator,
            .io = io,
            .role = role,
            .socket = socket,
            .endpoint = endpoint,
            .remote_addr = remote_addr,
            .start_timestamp = std.Io.Timestamp.now(io, .awake),
            .receive_timeout = receive_timeout,
            .udp_rx_buf = udp_rx_buf,
            .udp_tx_buf = udp_tx_buf,
            .stream_read_buf = stream_read_buf,
            .max_message_bytes = max_message_bytes,
            .inbound = LengthDelimitedFramer.init(allocator, max_message_bytes),
            .outbound = OutboundQueue.init(max_outbound_queue_items, max_outbound_queue_bytes),
            .owner_thread_id = if (comptime builtin.target.os.tag == .freestanding) null else std.Thread.getCurrentId(),
        };
    }

    pub fn deinit(self: *Connection) void {
        if (self.deinitialized) return;
        if (self.callback_depth != 0) {
            self.deinit_requested = true;
            self.requestClose();
            return;
        }
        self.deinitNow(true);
    }

    fn deinitNow(self: *Connection, comptime check_affinity: bool) void {
        if (self.deinitialized) return;
        if (check_affinity) self.assertThreadAffinity();
        self.deinitialized = true;
        self.deinit_requested = false;
        _ = self.close_requested.swap(true, .acq_rel);
        self.outbound.close();
        self.outbound.drain(self.allocator);
        self.inbound.deinit();
        self.ctx = null;
        self.on_message = null;
        self.on_error = null;
        self.on_close = null;
        if (!self.socket_closed.swap(true, .acq_rel)) {
            self.socket.close(self.io);
        }
        switch (self.endpoint) {
            .client => |*client| client.deinit(),
            .server => |*server| server.server.deinit(),
        }
        self.allocator.free(self.udp_rx_buf);
        self.allocator.free(self.udp_tx_buf);
        self.allocator.free(self.stream_read_buf);
    }

    pub fn start(
        self: *Connection,
        ctx: *anyopaque,
        on_message: *const fn (conn: *Connection, frame: []const u8) anyerror!void,
        on_error: *const fn (conn: *Connection, err: anyerror) void,
        on_close: *const fn (conn: *Connection) void,
    ) void {
        self.assertThreadAffinity();
        self.ctx = ctx;
        self.on_message = on_message;
        self.on_error = on_error;
        self.on_close = on_close;
    }

    pub fn run(self: *Connection) void {
        while (!self.close_requested.load(.acquire)) {
            self.step() catch |err| {
                log.debug("QUIC connection step failed: {}", .{err});
                self.invokeOnError(err);
                self.close();
                break;
            };
            if (self.activeQuicConn()) |conn| {
                if (conn.isClosed() and self.outbound.isEmpty()) {
                    _ = self.close_requested.swap(true, .acq_rel);
                }
            }
        }

        self.flushCloseDatagram();
        self.outbound.close();
        if (self.on_close) |cb| self.invokeCloseCallback(cb);
        self.completeDeferredDeinit();
    }

    pub fn sendFrame(self: *Connection, frame: []const u8) !void {
        if (self.close_requested.load(.acquire)) return error.BrokenPipe;
        if (frame.len == 0 or frame.len > self.max_message_bytes) return error.FrameTooLarge;
        if (frame.len > std.math.maxInt(u32)) return error.FrameTooLarge;

        try self.outbound.enqueue(self.allocator, frame);
    }

    pub fn close(self: *Connection) void {
        _ = self.close_requested.swap(true, .acq_rel);
        self.outbound.close();
        if (self.activeQuicConn()) |conn| {
            conn.close(false, 0, "");
        }
    }

    pub fn requestClose(self: *Connection) void {
        _ = self.close_requested.swap(true, .acq_rel);
        self.outbound.close();
    }

    pub fn isClosing(self: *const Connection) bool {
        return self.close_requested.load(.acquire);
    }

    pub fn getAddress(self: *const Connection) Net.IpAddress {
        return self.socket.address;
    }

    pub fn assertThreadAffinity(self: *const Connection) void {
        if (comptime builtin.target.os.tag == .freestanding) return;
        if (builtin.mode == .Debug) {
            const owner = self.owner_thread_id orelse return;
            const current = std.Thread.getCurrentId();
            if (current != owner) {
                @panic("QUIC Connection method called from wrong thread");
            }
        }
    }

    fn step(self: *Connection) !void {
        var now_us = self.nowUs();
        try self.receiveOne(now_us);

        now_us = self.nowUs();
        try self.advanceActive();
        try self.serviceBaselineStream();
        try self.drainOutgoingDatagrams(now_us);

        now_us = self.nowUs();
        try self.tickActive(now_us);
        try self.serviceBaselineStream();
        try self.drainOutgoingDatagrams(now_us);

        self.reapServerIfClosed();
    }

    fn flushCloseDatagram(self: *Connection) void {
        const conn = self.activeQuicConn() orelse return;
        if (!conn.isClosed()) {
            conn.close(false, 0, "");
        }
        self.drainOutgoingDatagrams(self.nowUs()) catch |err| {
            log.debug("failed to flush QUIC close datagram: {}", .{err});
        };
    }

    fn receiveOne(self: *Connection, now_us: u64) !void {
        const msg = self.socket.receiveTimeout(self.io, self.udp_rx_buf, .{
            .duration = .{
                .raw = self.receive_timeout,
                .clock = .awake,
            },
        }) catch |err| switch (err) {
            error.Timeout => return,
            else => return err,
        };
        if (msg.flags.trunc) return error.DatagramTooLarge;

        switch (self.endpoint) {
            .client => |*client| {
                const remote = self.remote_addr.?;
                if (!Net.IpAddress.eql(&msg.from, &remote)) return;
                try client.conn.handle(msg.data, nullq_adapter.ipAddressToPathAddress(msg.from), now_us);
            },
            .server => |*server| {
                const from = nullq_adapter.ipAddressToPathAddress(msg.from);
                _ = try server.server.feed(msg.data, from, now_us);
                self.setServerSlotIfAccepted();
                try self.drainStatelessResponses(server);
            },
        }
    }

    fn advanceActive(self: *Connection) !void {
        const conn = self.activeQuicConn() orelse return;
        if (!conn.handshakeDone()) {
            try conn.advance();
        }
    }

    fn tickActive(self: *Connection, now_us: u64) !void {
        const conn = self.activeQuicConn() orelse return;
        try conn.tick(now_us);
    }

    fn serviceBaselineStream(self: *Connection) !void {
        const conn = self.activeQuicConn() orelse return;
        if (!try self.ensureBaselineStream(conn)) return;
        try self.outbound.flush(self.allocator, conn);
        try self.readBaselineStream(conn);
    }

    fn ensureBaselineStream(self: *Connection, conn: *nullq.Connection) !bool {
        if (self.baseline_ready) return true;
        if (conn.stream(quic_options.baseline_stream_id) != null) {
            self.baseline_ready = true;
            return true;
        }
        if (self.role == .client) {
            if (!conn.handshakeDone()) return false;
            _ = conn.openBidi(quic_options.baseline_stream_id) catch |err| switch (err) {
                error.StreamAlreadyOpen => {},
                else => return err,
            };
            self.baseline_ready = true;
            return true;
        }
        return false;
    }

    fn readBaselineStream(self: *Connection, conn: *nullq.Connection) !void {
        while (!self.close_requested.load(.acquire)) {
            const n = conn.streamRead(quic_options.baseline_stream_id, self.stream_read_buf) catch |err| switch (err) {
                error.StreamNotFound => return,
                else => return err,
            };
            if (n == 0) return;
            try self.inbound.push(self.stream_read_buf[0..n]);
            try self.dispatchAvailableFrames();
        }
    }

    fn dispatchAvailableFrames(self: *Connection) !void {
        while (true) {
            if (self.on_message == null or self.on_error == null) return;
            const frame = self.inbound.popFrame() catch |err| {
                if (err == error.OutOfMemory) {
                    self.terminateFrameError(err);
                    return;
                }
                self.terminateFrameError(err);
                return;
            };
            const bytes = frame orelse return;
            defer self.allocator.free(bytes);
            self.invokeMessageCallback(self.on_message.?, bytes) catch |err| {
                self.invokeOnError(err);
                return;
            };
            if (self.deinit_requested) return;
        }
    }

    fn terminateFrameError(self: *Connection, err: anyerror) void {
        log.debug("fatal QUIC frame error, closing connection: {}", .{err});
        _ = self.close_requested.swap(true, .acq_rel);
        self.outbound.close();
        self.inbound.reset();
        self.on_message = null;
        const on_error = self.on_error;
        self.on_error = null;
        if (on_error) |cb| self.invokeErrorCallback(cb, err);
    }

    fn drainOutgoingDatagrams(self: *Connection, now_us: u64) !void {
        switch (self.endpoint) {
            .client => |*client| {
                while (try client.conn.pollDatagram(self.udp_tx_buf, now_us)) |out| {
                    const dest = if (out.to) |addr|
                        nullq_adapter.pathAddressToIpAddress(addr) orelse self.remote_addr.?
                    else
                        self.remote_addr.?;
                    try self.socket.send(self.io, &dest, self.udp_tx_buf[0..out.len]);
                }
            },
            .server => |*server| {
                const slot = server.slot orelse return;
                while (try slot.conn.pollDatagram(self.udp_tx_buf, now_us)) |out| {
                    const target = if (out.to) |addr| addr else slot.peer_addr orelse continue;
                    const dest = nullq_adapter.pathAddressToIpAddress(target) orelse continue;
                    try self.socket.send(self.io, &dest, self.udp_tx_buf[0..out.len]);
                }
            },
        }
    }

    fn drainStatelessResponses(self: *Connection, server: *ServerEndpoint) !void {
        while (server.server.drainStatelessResponse()) |response| {
            const dest = nullq_adapter.pathAddressToIpAddress(response.dst) orelse continue;
            try self.socket.send(self.io, &dest, response.slice());
        }
    }

    fn activeQuicConn(self: *Connection) ?*nullq.Connection {
        return switch (self.endpoint) {
            .client => |*client| client.conn,
            .server => |*server| if (server.slot) |slot| slot.conn else null,
        };
    }

    fn setServerSlotIfAccepted(self: *Connection) void {
        switch (self.endpoint) {
            .client => return,
            .server => |*server| {
                if (server.slot != null) return;
                const slots = server.server.iterator();
                if (slots.len > 0) server.slot = slots[0];
            },
        }
    }

    fn reapServerIfClosed(self: *Connection) void {
        switch (self.endpoint) {
            .client => return,
            .server => |*server| {
                if (server.slot) |slot| {
                    if (!slot.conn.isClosed()) return;
                    const reaped = server.server.reap();
                    if (reaped > 0) {
                        server.slot = null;
                        _ = self.close_requested.swap(true, .acq_rel);
                    }
                } else {
                    _ = server.server.reap();
                }
            },
        }
    }

    fn nowUs(self: *Connection) u64 {
        const now = std.Io.Timestamp.now(self.io, .awake);
        const delta = self.start_timestamp.durationTo(now).toMicroseconds();
        if (delta <= 0) return 0;
        return @intCast(delta);
    }

    fn invokeOnError(self: *Connection, err: anyerror) void {
        const cb = self.on_error orelse return;
        self.invokeErrorCallback(cb, err);
    }

    fn invokeErrorCallback(
        self: *Connection,
        cb: *const fn (conn: *Connection, callback_err: anyerror) void,
        err: anyerror,
    ) void {
        self.callback_depth += 1;
        defer self.callback_depth -= 1;
        cb(self, err);
    }

    fn invokeMessageCallback(
        self: *Connection,
        cb: *const fn (conn: *Connection, frame: []const u8) anyerror!void,
        frame: []const u8,
    ) !void {
        self.callback_depth += 1;
        defer self.callback_depth -= 1;
        try cb(self, frame);
    }

    fn invokeCloseCallback(
        self: *Connection,
        cb: *const fn (conn: *Connection) void,
    ) void {
        self.callback_depth += 1;
        defer self.callback_depth -= 1;
        cb(self);
    }

    fn completeDeferredDeinit(self: *Connection) void {
        if (self.deinit_requested and self.callback_depth == 0) {
            self.deinitNow(false);
        }
    }
};

test "QUIC dispatch treats malformed frames as terminal" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    var state = Harness.State{};
    var udp_rx_buf: [0]u8 = .{};
    var udp_tx_buf: [0]u8 = .{};
    var stream_read_buf: [0]u8 = .{};
    var conn = Connection{
        .allocator = std.testing.allocator,
        .io = std.testing.io,
        .role = .client,
        .socket = undefined,
        .endpoint = undefined,
        .start_timestamp = std.Io.Timestamp.now(std.testing.io, .awake),
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
        .udp_rx_buf = &udp_rx_buf,
        .udp_tx_buf = &udp_tx_buf,
        .stream_read_buf = &stream_read_buf,
        .max_message_bytes = 1024,
        .inbound = LengthDelimitedFramer.init(std.testing.allocator, 1024),
        .outbound = OutboundQueue.init(4, 1024),
        .ctx = &state,
        .on_message = Harness.onMessage,
        .on_error = Harness.onError,
    };
    defer conn.inbound.deinit();
    defer conn.outbound.drain(std.testing.allocator);

    const bad_frame = [_]u8{ 0, 0, 0, 0 };
    try conn.inbound.push(&bad_frame);
    try conn.dispatchAvailableFrames();

    try std.testing.expect(conn.isClosing());
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), state.last_error);
    try std.testing.expectError(error.BrokenPipe, conn.sendFrame("late"));
}

test "QUIC dispatch treats inbound frame allocation OOM as terminal" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var state = Harness.State{};
    var udp_rx_buf: [0]u8 = .{};
    var udp_tx_buf: [0]u8 = .{};
    var stream_read_buf: [0]u8 = .{};
    var conn = Connection{
        .allocator = failing.allocator(),
        .io = std.testing.io,
        .role = .client,
        .socket = undefined,
        .endpoint = undefined,
        .start_timestamp = std.Io.Timestamp.now(std.testing.io, .awake),
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
        .udp_rx_buf = &udp_rx_buf,
        .udp_tx_buf = &udp_tx_buf,
        .stream_read_buf = &stream_read_buf,
        .max_message_bytes = 1024,
        .inbound = LengthDelimitedFramer.init(failing.allocator(), 1024),
        .outbound = OutboundQueue.init(4, 1024),
        .ctx = &state,
        .on_message = Harness.onMessage,
        .on_error = Harness.onError,
    };
    defer conn.inbound.deinit();
    defer conn.outbound.drain(failing.allocator());

    var encoded: [7]u8 = undefined;
    std.mem.writeInt(u32, encoded[0..4], 3, .little);
    @memcpy(encoded[4..7], "abc");

    try conn.inbound.push(&encoded);
    try conn.dispatchAvailableFrames();

    try std.testing.expect(conn.isClosing());
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), state.last_error);
    try std.testing.expectEqual(@as(usize, 0), conn.inbound.buffer.items.len);
    try std.testing.expectError(error.BrokenPipe, conn.sendFrame("late"));
}

test "QUIC deinit requested from error callback is deferred without panic" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            close_count: usize = 0,
            deinit_seen_in_error: bool = false,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, _: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            conn.deinit();
            state.deinit_seen_in_error = conn.deinit_requested;
        }

        fn onClose(conn: *Connection) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.close_count += 1;
        }
    };

    var state = Harness.State{};
    var udp_rx_buf: [0]u8 = .{};
    var udp_tx_buf: [0]u8 = .{};
    var stream_read_buf: [0]u8 = .{};
    var conn = Connection{
        .allocator = std.testing.allocator,
        .io = std.testing.io,
        .role = .client,
        .socket = undefined,
        .endpoint = undefined,
        .start_timestamp = std.Io.Timestamp.now(std.testing.io, .awake),
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
        .udp_rx_buf = &udp_rx_buf,
        .udp_tx_buf = &udp_tx_buf,
        .stream_read_buf = &stream_read_buf,
        .max_message_bytes = 1024,
        .inbound = LengthDelimitedFramer.init(std.testing.allocator, 1024),
        .outbound = OutboundQueue.init(4, 1024),
        .ctx = &state,
        .on_message = Harness.onMessage,
        .on_error = Harness.onError,
        .on_close = Harness.onClose,
    };
    defer if (!conn.deinitialized) conn.inbound.deinit();
    defer if (!conn.deinitialized) conn.outbound.drain(std.testing.allocator);

    conn.terminateFrameError(error.InvalidFrame);
    if (conn.on_close) |cb| conn.invokeCloseCallback(cb);

    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(usize, 1), state.close_count);
    try std.testing.expect(state.deinit_seen_in_error);
    try std.testing.expect(conn.deinit_requested);
}
