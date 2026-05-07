const std = @import("std");
const builtin = @import("builtin");
const nullq = @import("nullq");

const framing = @import("../level0/framing.zig");

const log = std.log.scoped(.rpc_quic_transport);
const Net = std.Io.net;

/// Native QUIC ALPN for Cap'n Proto RPC over nullq.
pub const alpn = "capnp-rpc/1";

/// The first client-initiated bidirectional stream carries the baseline RPC
/// byte stream. This keeps the initial QUIC transport isomorphic to the TCP
/// transport above the framing boundary while reserving additional streams for
/// later QUIC-native payload modes.
pub const baseline_stream_id: u64 = 0;

pub const default_udp_rx_buffer_size: usize = 64 * 1024;
pub const default_udp_tx_buffer_size: usize = 1500;
pub const default_stream_read_buffer_size: usize = 64 * 1024;
pub const default_max_message_bytes: usize = framing.Framer.max_frame_words * 8;
pub const default_max_outbound_queue_items: usize = 1024;
pub const default_max_outbound_queue_bytes: usize = default_max_message_bytes + length_prefix_bytes;

const length_prefix_bytes: usize = 4;

pub fn defaultTransportParams() nullq.tls.TransportParams {
    return .{
        .max_idle_timeout_ms = 30_000,
        .initial_max_data = 16 * 1024 * 1024,
        .initial_max_stream_data_bidi_local = 1 << 20,
        .initial_max_stream_data_bidi_remote = 1 << 20,
        .initial_max_stream_data_uni = 1 << 20,
        .initial_max_streams_bidi = 16,
        .initial_max_streams_uni = 4,
        .active_connection_id_limit = 4,
    };
}

pub const ClientOptions = struct {
    /// UDP local bind address. When null, an ephemeral unspecified address is
    /// chosen with the same address family as `remote_addr`.
    local_addr: ?Net.IpAddress = null,
    remote_addr: Net.IpAddress,
    server_name: []const u8,
    alpn_protocols: []const []const u8 = &.{alpn},
    transport_params: nullq.tls.TransportParams = defaultTransportParams(),
    receive_timeout: std.Io.Duration = std.Io.Duration.fromMilliseconds(5),
    udp_rx_buffer_size: usize = default_udp_rx_buffer_size,
    udp_tx_buffer_size: usize = default_udp_tx_buffer_size,
    stream_read_buffer_size: usize = default_stream_read_buffer_size,
    max_message_bytes: usize = default_max_message_bytes,
    max_outbound_queue_items: usize = default_max_outbound_queue_items,
    max_outbound_queue_bytes: usize = default_max_outbound_queue_bytes,
    ca_pem: ?[]const u8 = null,
};

pub const ServerOptions = struct {
    listen_addr: Net.IpAddress,
    tls_cert_pem: []const u8,
    tls_key_pem: []const u8,
    alpn_protocols: []const []const u8 = &.{alpn},
    transport_params: nullq.tls.TransportParams = defaultTransportParams(),
    max_concurrent_connections: u32 = 1,
    receive_timeout: std.Io.Duration = std.Io.Duration.fromMilliseconds(5),
    udp_rx_buffer_size: usize = default_udp_rx_buffer_size,
    udp_tx_buffer_size: usize = default_udp_tx_buffer_size,
    stream_read_buffer_size: usize = default_stream_read_buffer_size,
    max_message_bytes: usize = default_max_message_bytes,
    max_outbound_queue_items: usize = default_max_outbound_queue_items,
    max_outbound_queue_bytes: usize = default_max_outbound_queue_bytes,
};

const Role = enum { client, server };

const Endpoint = union(Role) {
    client: nullq.Client,
    server: ServerEndpoint,
};

const ServerEndpoint = struct {
    server: nullq.Server,
    slot: ?*nullq.Server.Slot = null,
};

const QueuedWrite = struct {
    bytes: []u8,
    offset: usize = 0,
};

const OutboundQueue = struct {
    mu: std.atomic.Mutex = .unlocked,
    items: std.ArrayListUnmanaged(QueuedWrite) = .empty,
    head: usize = 0,
    queued_items: usize = 0,
    queued_bytes: usize = 0,
    max_items: usize = default_max_outbound_queue_items,
    max_bytes: usize = default_max_outbound_queue_bytes,
    closed: bool = false,
    flushing: bool = false,

    fn init(max_items: usize, max_bytes: usize) OutboundQueue {
        return .{
            .max_items = max_items,
            .max_bytes = max_bytes,
        };
    }

    fn lock(self: *OutboundQueue) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
    }

    fn enqueue(
        self: *OutboundQueue,
        allocator: std.mem.Allocator,
        frame: []const u8,
    ) error{ BrokenPipe, OutboundQueueFull, OutOfMemory }!void {
        self.lock();
        defer self.mu.unlock();
        if (self.closed) {
            return error.BrokenPipe;
        }
        const encoded_len = std.math.add(usize, length_prefix_bytes, frame.len) catch return error.OutboundQueueFull;
        const new_bytes = std.math.add(usize, self.queued_bytes, encoded_len) catch {
            return error.OutboundQueueFull;
        };
        if (self.queued_items >= self.max_items or new_bytes > self.max_bytes) {
            return error.OutboundQueueFull;
        }

        const bytes = allocator.alloc(u8, encoded_len) catch return error.OutOfMemory;
        errdefer allocator.free(bytes);
        std.mem.writeInt(u32, bytes[0..length_prefix_bytes], @intCast(frame.len), .little);
        std.mem.copyForwards(u8, bytes[length_prefix_bytes..], frame);
        self.items.append(allocator, .{ .bytes = bytes }) catch {
            return error.OutOfMemory;
        };
        self.queued_items += 1;
        self.queued_bytes = new_bytes;
    }

    fn close(self: *OutboundQueue) void {
        self.lock();
        self.closed = true;
        self.mu.unlock();
    }

    fn isEmpty(self: *OutboundQueue) bool {
        self.lock();
        defer self.mu.unlock();
        return self.queued_items == 0;
    }

    fn drain(self: *OutboundQueue, allocator: std.mem.Allocator) void {
        self.lock();
        defer self.mu.unlock();
        for (self.items.items[self.head..]) |item| allocator.free(item.bytes);
        self.items.deinit(allocator);
        self.items = .empty;
        self.head = 0;
        self.queued_items = 0;
        self.queued_bytes = 0;
    }

    fn flush(
        self: *OutboundQueue,
        allocator: std.mem.Allocator,
        conn: *nullq.Connection,
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

/// A u32 little-endian length-delimited message framer used inside the QUIC
/// baseline stream. The payload itself is still one complete Cap'n Proto RPC
/// message encoded by the existing peer/protocol layer.
pub const LengthDelimitedFramer = struct {
    pub const Options = struct {
        max_message_bytes: usize,
        max_buffered_bytes: ?usize = null,
    };

    allocator: std.mem.Allocator,
    buffer: std.ArrayList(u8),
    expected_len: ?usize = null,
    max_message_bytes: usize,
    max_buffered_bytes: usize,

    pub fn init(allocator: std.mem.Allocator, max_message_bytes: usize) LengthDelimitedFramer {
        return initWithOptions(allocator, .{ .max_message_bytes = max_message_bytes });
    }

    pub fn initWithOptions(allocator: std.mem.Allocator, options: Options) LengthDelimitedFramer {
        return .{
            .allocator = allocator,
            .buffer = .empty,
            .max_message_bytes = options.max_message_bytes,
            .max_buffered_bytes = options.max_buffered_bytes orelse length_prefix_bytes + options.max_message_bytes,
        };
    }

    pub fn deinit(self: *LengthDelimitedFramer) void {
        self.buffer.deinit(self.allocator);
        self.expected_len = null;
    }

    pub fn push(self: *LengthDelimitedFramer, data: []const u8) !void {
        if (data.len == 0) return;
        try self.ensureAppendBudget(data.len);
        try self.buffer.appendSlice(self.allocator, data);
    }

    pub fn reset(self: *LengthDelimitedFramer) void {
        self.buffer.items.len = 0;
        self.expected_len = null;
    }

    pub fn popFrame(self: *LengthDelimitedFramer) !?[]u8 {
        try self.updateExpected();
        const len = self.expected_len orelse return null;
        const total = length_prefix_bytes + len;
        if (self.buffer.items.len < total) return null;

        const frame = try self.allocator.alloc(u8, len);
        std.mem.copyForwards(u8, frame, self.buffer.items[length_prefix_bytes..total]);

        const remaining = self.buffer.items.len - total;
        if (remaining > 0) {
            std.mem.copyForwards(u8, self.buffer.items[0..remaining], self.buffer.items[total..]);
        }
        self.buffer.items.len = remaining;
        self.expected_len = null;
        return frame;
    }

    fn updateExpected(self: *LengthDelimitedFramer) !void {
        if (self.expected_len != null) return;
        if (self.buffer.items.len < length_prefix_bytes) return;

        const raw_len = std.mem.readInt(u32, self.buffer.items[0..length_prefix_bytes], .little);
        if (raw_len == 0) return error.InvalidFrame;
        const len: usize = @intCast(raw_len);
        if (len > self.max_message_bytes) return error.FrameTooLarge;
        self.expected_len = len;
    }

    fn ensureAppendBudget(self: *const LengthDelimitedFramer, data_len: usize) !void {
        const next = std.math.add(usize, self.buffer.items.len, data_len) catch return error.FrameTooLarge;
        if (next > self.max_buffered_bytes) return error.FrameTooLarge;
    }
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
    socket_closed: bool = false,
    baseline_ready: bool = false,

    ctx: ?*anyopaque = null,
    on_message: ?*const fn (conn: *Connection, frame: []const u8) anyerror!void = null,
    on_error: ?*const fn (conn: *Connection, err: anyerror) void = null,
    on_close: ?*const fn (conn: *Connection) void = null,
    in_error_callback: bool = false,

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

        const local_addr = options.local_addr orelse defaultClientBindAddress(options.remote_addr);
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
        if (options.alpn_protocols.len == 0) return error.InvalidConfig;
        if (options.max_concurrent_connections != 1) return error.InvalidConfig;
        if (options.udp_rx_buffer_size == 0 or
            options.udp_tx_buffer_size == 0 or
            options.stream_read_buffer_size == 0 or
            options.max_message_bytes == 0 or
            options.max_outbound_queue_items == 0 or
            options.max_outbound_queue_bytes == 0)
        {
            return error.InvalidConfig;
        }

        const socket = try Net.IpAddress.bind(&options.listen_addr, io, .{
            .mode = .dgram,
            .protocol = .udp,
        });
        errdefer socket.close(io);

        var server = try nullq.Server.init(.{
            .allocator = allocator,
            .tls_cert_pem = options.tls_cert_pem,
            .tls_key_pem = options.tls_key_pem,
            .alpn_protocols = options.alpn_protocols,
            .transport_params = options.transport_params,
            .max_concurrent_connections = options.max_concurrent_connections,
        });
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
        self.assertThreadAffinity();
        if (self.in_error_callback) {
            @panic("Connection.deinit() must not be called from on_error callback; defer cleanup to on_close");
        }
        self.close_requested.store(true, .release);
        self.outbound.close();
        self.outbound.drain(self.allocator);
        self.inbound.deinit();
        self.ctx = null;
        self.on_message = null;
        self.on_error = null;
        self.on_close = null;
        if (!self.socket_closed) {
            self.socket.close(self.io);
            self.socket_closed = true;
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
                    self.close_requested.store(true, .release);
                }
            }
        }

        self.flushCloseDatagram();
        self.outbound.close();
        if (self.on_close) |cb| cb(self);
    }

    pub fn sendFrame(self: *Connection, frame: []const u8) !void {
        if (self.close_requested.load(.acquire)) return error.BrokenPipe;
        if (frame.len == 0 or frame.len > self.max_message_bytes) return error.FrameTooLarge;
        if (frame.len > std.math.maxInt(u32)) return error.FrameTooLarge;

        try self.outbound.enqueue(self.allocator, frame);
    }

    pub fn close(self: *Connection) void {
        self.close_requested.store(true, .release);
        self.outbound.close();
        if (self.activeQuicConn()) |conn| {
            conn.close(false, 0, "");
        }
    }

    pub fn requestClose(self: *Connection) void {
        self.close_requested.store(true, .release);
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
                try client.conn.handle(msg.data, ipAddressToPathAddress(msg.from), now_us);
            },
            .server => |*server| {
                const from = ipAddressToPathAddress(msg.from);
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
        if (conn.stream(baseline_stream_id) != null) {
            self.baseline_ready = true;
            return true;
        }
        if (self.role == .client) {
            if (!conn.handshakeDone()) return false;
            _ = conn.openBidi(baseline_stream_id) catch |err| switch (err) {
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
            const n = conn.streamRead(baseline_stream_id, self.stream_read_buf) catch |err| switch (err) {
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
                    self.invokeOnError(err);
                    return;
                }
                self.terminateFrameError(err);
                return;
            };
            const bytes = frame orelse return;
            defer self.allocator.free(bytes);
            self.on_message.?(self, bytes) catch |err| {
                self.invokeOnError(err);
                return;
            };
        }
    }

    fn terminateFrameError(self: *Connection, err: anyerror) void {
        log.debug("fatal QUIC frame error, closing connection: {}", .{err});
        self.close_requested.store(true, .release);
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
                        pathAddressToIpAddress(addr) orelse self.remote_addr.?
                    else
                        self.remote_addr.?;
                    try self.socket.send(self.io, &dest, self.udp_tx_buf[0..out.len]);
                }
            },
            .server => |*server| {
                const slot = server.slot orelse return;
                while (try slot.conn.pollDatagram(self.udp_tx_buf, now_us)) |out| {
                    const target = if (out.to) |addr| addr else slot.peer_addr orelse continue;
                    const dest = pathAddressToIpAddress(target) orelse continue;
                    try self.socket.send(self.io, &dest, self.udp_tx_buf[0..out.len]);
                }
            },
        }
    }

    fn drainStatelessResponses(self: *Connection, server: *ServerEndpoint) !void {
        while (server.server.drainStatelessResponse()) |response| {
            const dest = pathAddressToIpAddress(response.dst) orelse continue;
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
                        self.close_requested.store(true, .release);
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
        self.in_error_callback = true;
        defer self.in_error_callback = false;
        cb(self, err);
    }
};

fn defaultClientBindAddress(remote_addr: Net.IpAddress) Net.IpAddress {
    return switch (remote_addr) {
        .ip4 => .{ .ip4 = .unspecified(0) },
        .ip6 => .{ .ip6 = .unspecified(0) },
    };
}

pub fn ipAddressToPathAddress(addr: Net.IpAddress) nullq.conn.path.Address {
    var out: nullq.conn.path.Address = .{};
    switch (addr) {
        .ip4 => |ip4| {
            out.bytes[0] = 4;
            @memcpy(out.bytes[1..5], &ip4.bytes);
            std.mem.writeInt(u16, out.bytes[5..7], ip4.port, .big);
        },
        .ip6 => |ip6| {
            out.bytes[0] = 6;
            @memcpy(out.bytes[1..17], &ip6.bytes);
            std.mem.writeInt(u16, out.bytes[17..19], ip6.port, .big);
            out.bytes[19] = @truncate(ip6.flow >> 16);
            out.bytes[20] = @truncate(ip6.flow >> 8);
            out.bytes[21] = @truncate(ip6.flow);
        },
    }
    return out;
}

pub fn pathAddressToIpAddress(addr: nullq.conn.path.Address) ?Net.IpAddress {
    switch (addr.bytes[0]) {
        4 => {
            var ip4_bytes: [4]u8 = undefined;
            @memcpy(&ip4_bytes, addr.bytes[1..5]);
            const port = std.mem.readInt(u16, addr.bytes[5..7], .big);
            return .{ .ip4 = .{ .bytes = ip4_bytes, .port = port } };
        },
        6 => {
            var ip6_bytes: [16]u8 = undefined;
            @memcpy(&ip6_bytes, addr.bytes[1..17]);
            const port = std.mem.readInt(u16, addr.bytes[17..19], .big);
            const flow: u32 = (@as(u32, addr.bytes[19]) << 16) |
                (@as(u32, addr.bytes[20]) << 8) |
                @as(u32, addr.bytes[21]);
            return .{ .ip6 = .{
                .bytes = ip6_bytes,
                .port = port,
                .flow = flow,
            } };
        },
        else => return null,
    }
}

test "LengthDelimitedFramer assembles fragmented and coalesced frames" {
    var framer = LengthDelimitedFramer.init(std.testing.allocator, 1024);
    defer framer.deinit();

    var bytes: [16]u8 = @splat(0);
    std.mem.writeInt(u32, bytes[0..4], 3, .little);
    @memcpy(bytes[4..7], "abc");
    std.mem.writeInt(u32, bytes[7..11], 5, .little);
    @memcpy(bytes[11..16], "hello");

    try framer.push(bytes[0..2]);
    try std.testing.expect(try framer.popFrame() == null);
    try framer.push(bytes[2..9]);

    const first = (try framer.popFrame()).?;
    defer std.testing.allocator.free(first);
    try std.testing.expectEqualStrings("abc", first);
    try std.testing.expect(try framer.popFrame() == null);

    try framer.push(bytes[9..]);
    const second = (try framer.popFrame()).?;
    defer std.testing.allocator.free(second);
    try std.testing.expectEqualStrings("hello", second);
    try std.testing.expect(try framer.popFrame() == null);
}

test "LengthDelimitedFramer rejects oversized frames" {
    var framer = LengthDelimitedFramer.init(std.testing.allocator, 2);
    defer framer.deinit();

    var bytes: [4]u8 = @splat(0);
    std.mem.writeInt(u32, bytes[0..4], 3, .little);
    try framer.push(&bytes);
    try std.testing.expectError(error.FrameTooLarge, framer.popFrame());
}

test "LengthDelimitedFramer rejects buffered byte budget before append" {
    var framer = LengthDelimitedFramer.initWithOptions(std.testing.allocator, .{
        .max_message_bytes = 1024,
        .max_buffered_bytes = 4,
    });
    defer framer.deinit();

    try framer.push(&[_]u8{ 1, 2, 3, 4 });
    try std.testing.expectEqual(@as(usize, 4), framer.buffer.items.len);

    try std.testing.expectError(error.FrameTooLarge, framer.push(&[_]u8{5}));
    try std.testing.expectEqual(@as(usize, 4), framer.buffer.items.len);
}

test "LengthDelimitedFramer budget rejection does not allocate" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var framer = LengthDelimitedFramer.initWithOptions(failing.allocator(), .{
        .max_message_bytes = 1024,
        .max_buffered_bytes = 0,
    });
    defer framer.deinit();

    try std.testing.expectError(error.FrameTooLarge, framer.push(&[_]u8{1}));
    try std.testing.expectEqual(@as(usize, 0), framer.buffer.items.len);
}

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

test "QUIC path address round-trips IPv4" {
    const addr: Net.IpAddress = .{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = 7001,
    } };
    const path_addr = ipAddressToPathAddress(addr);
    const round_trip = pathAddressToIpAddress(path_addr).?;
    try std.testing.expect(round_trip == .ip4);
    try std.testing.expectEqual(addr.ip4.port, round_trip.ip4.port);
    try std.testing.expectEqualSlices(u8, &addr.ip4.bytes, &round_trip.ip4.bytes);
}
