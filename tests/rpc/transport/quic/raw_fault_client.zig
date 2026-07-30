const std = @import("std");
const capnpc = @import("capnpc-zig");
const quic_zig = @import("quic_zig");

const quic = capnpc.rpc.transport.quic;
const loopback = @import("loopback_test_support.zig");

const raw_client_rx_buffer_size: usize = 64 * 1024;
const raw_client_tx_buffer_size: usize = 1500;

const RawFaultClient = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    socket: std.Io.net.Socket,
    remote_addr: std.Io.net.IpAddress,
    client: quic_zig.Client,
    start_timestamp: std.Io.Timestamp,
    rx_buf: []u8,
    tx_buf: []u8,

    fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        remote_addr: std.Io.net.IpAddress,
    ) !RawFaultClient {
        const local_addr = quic.defaultClientBindAddress(remote_addr);
        const socket = try std.Io.net.IpAddress.bind(&local_addr, io, .{
            .mode = .dgram,
            .protocol = .udp,
        });
        errdefer socket.close(io);

        var client = try quic_zig.Client.connect(.{
            .allocator = allocator,
            .server_name = "localhost",
            .insecure_skip_verify = true,
            .alpn_protocols = &.{quic.alpn},
            .transport_params = quic.defaultTransportParams(),
        });
        errdefer client.deinit();

        const rx_buf = try allocator.alloc(u8, raw_client_rx_buffer_size);
        errdefer allocator.free(rx_buf);
        const tx_buf = try allocator.alloc(u8, raw_client_tx_buffer_size);
        errdefer allocator.free(tx_buf);

        return .{
            .allocator = allocator,
            .io = io,
            .socket = socket,
            .remote_addr = remote_addr,
            .client = client,
            .start_timestamp = std.Io.Timestamp.now(io, .awake),
            .rx_buf = rx_buf,
            .tx_buf = tx_buf,
        };
    }

    fn deinit(self: *RawFaultClient) void {
        self.client.deinit();
        self.socket.close(self.io);
        self.allocator.free(self.rx_buf);
        self.allocator.free(self.tx_buf);
    }

    fn waitForHandshake(self: *RawFaultClient, server_state: *const loopback.QuicEndpointState) !void {
        var waited_ms: u64 = 0;
        while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
            try self.step(std.Io.Duration.fromMilliseconds(1));
            if (self.client.conn.handshakeDone()) return;
            if (server_state.errors.load(.acquire) > 0) return error.QuicLoopbackUnexpectedServerError;
            loopback.sleepMs(loopback.loopback_poll_ms);
        }
        return error.QuicLoopbackTimedOut;
    }

    fn waitForServerError(self: *RawFaultClient, server_state: *const loopback.QuicEndpointState) !void {
        var waited_ms: u64 = 0;
        while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
            try self.step(std.Io.Duration.fromMilliseconds(1));
            if (server_state.errors.load(.acquire) > 0) return;
            loopback.sleepMs(loopback.loopback_poll_ms);
        }
        return error.QuicLoopbackTimedOut;
    }

    fn ensureControlStream(self: *RawFaultClient) !void {
        _ = self.client.conn.openBidi(quic.baseline_stream_id) catch |err| switch (err) {
            error.StreamAlreadyOpen => return,
            else => return err,
        };
    }

    fn ensureUniStream(self: *RawFaultClient, stream_id: u64) !void {
        _ = self.client.conn.openUni(stream_id) catch |err| switch (err) {
            error.StreamAlreadyOpen => return,
            else => return err,
        };
    }

    fn writeAll(self: *RawFaultClient, stream_id: u64, bytes: []const u8) !void {
        var offset: usize = 0;
        while (offset < bytes.len) {
            const written = try self.client.conn.streamWrite(stream_id, bytes[offset..]);
            if (written == 0) {
                try self.step(std.Io.Duration.zero);
                loopback.sleepMs(loopback.loopback_poll_ms);
                continue;
            }
            offset += written;
            try self.drainOutgoing(self.nowUs());
        }
    }

    fn step(self: *RawFaultClient, receive_timeout: std.Io.Duration) !void {
        var now_us = self.nowUs();
        try self.client.conn.advance();
        try self.drainOutgoing(now_us);

        now_us = self.nowUs();
        const msg = self.socket.receiveTimeout(self.io, self.rx_buf, .{
            .duration = .{
                .raw = receive_timeout,
                .clock = .awake,
            },
        }) catch |err| switch (err) {
            error.Timeout => null,
            else => return err,
        };
        if (msg) |received| {
            if (std.Io.net.IpAddress.eql(&received.from, &self.remote_addr)) {
                try self.client.conn.handle(
                    received.data,
                    quic.ipAddressToPathAddress(received.from),
                    now_us,
                );
            }
        }

        now_us = self.nowUs();
        try self.client.conn.advance();
        try self.drainOutgoing(now_us);

        now_us = self.nowUs();
        try self.client.conn.tick(now_us);
        try self.drainOutgoing(now_us);
    }

    fn drainOutgoing(self: *RawFaultClient, now_us: u64) !void {
        while (try self.client.conn.pollDatagram(self.tx_buf, now_us)) |out| {
            const dest = if (out.to) |addr|
                quic.pathAddressToIpAddress(addr) orelse self.remote_addr
            else
                self.remote_addr;
            try self.socket.send(self.io, &dest, self.tx_buf[0..out.len]);
        }
    }

    fn nowUs(self: *RawFaultClient) u64 {
        const now = std.Io.Timestamp.now(self.io, .awake);
        const delta = self.start_timestamp.durationTo(now).toMicroseconds();
        if (delta <= 0) return 0;
        return @intCast(delta);
    }
};

pub const RawNativeFault = enum {
    malformed_preface,
    malformed_hello,
    malformed_control,
    unknown_control_tag,
    oversized_control_frame,
    data_final_size_mismatch,
    data_budget_violation,
};

fn injectRawNativeFault(
    allocator: std.mem.Allocator,
    client: *RawFaultClient,
    fault: RawNativeFault,
) !void {
    try client.ensureControlStream();
    switch (fault) {
        .malformed_preface => {
            try client.writeAll(quic.baseline_stream_id, "wrong-native-preface");
        },
        .malformed_hello => {
            var bytes: [quic.native.preface.len + quic.native.encodedHelloLen()]u8 = undefined;
            @memcpy(bytes[0..quic.native.preface.len], quic.native.preface);
            const hello_len = try quic.native.encodeHello(bytes[quic.native.preface.len..]);
            bytes[quic.native.preface.len + hello_len - 1] = 1;
            try client.writeAll(quic.baseline_stream_id, &bytes);
        },
        .malformed_control => {
            var bytes: [quic.native.preface.len + quic.native.encodedHelloLen() + quic.native.length_prefix_bytes]u8 = undefined;
            @memcpy(bytes[0..quic.native.preface.len], quic.native.preface);
            const hello_len = try quic.native.encodeHello(bytes[quic.native.preface.len..]);
            @memset(bytes[quic.native.preface.len + hello_len ..], 0);
            try client.writeAll(quic.baseline_stream_id, &bytes);
        },
        .unknown_control_tag => {
            var control: [quic.native.length_prefix_bytes + quic.native.common_header_bytes]u8 = undefined;
            std.mem.writeInt(u32, control[0..quic.native.length_prefix_bytes], quic.native.common_header_bytes, .little);
            control[quic.native.length_prefix_bytes] = 0xff;
            @memset(control[quic.native.length_prefix_bytes + 1 ..], 0);
            try writeNativePreambleAndControl(client, &control);
        },
        .oversized_control_frame => {
            const payload_len = quic.native.rpc_header_bytes + 128;
            const control = try allocator.alloc(u8, quic.native.length_prefix_bytes + payload_len);
            defer allocator.free(control);
            std.mem.writeInt(u32, control[0..quic.native.length_prefix_bytes], @intCast(payload_len), .little);
            control[quic.native.length_prefix_bytes] = @backingInt(quic.native.ControlFrameTag.inline_rpc);
            @memset(control[quic.native.length_prefix_bytes + 1 .. quic.native.length_prefix_bytes + quic.native.rpc_header_bytes], 0);
            @memset(control[quic.native.length_prefix_bytes + quic.native.rpc_header_bytes ..], 0xa5);
            try writeNativePreambleAndControl(client, control);
        },
        .data_final_size_mismatch => {
            const data_rpc = try quic.native.encodeDataRpc(
                allocator,
                0,
                2,
                8,
                quic.default_native_max_control_frame_bytes,
            );
            defer allocator.free(data_rpc);

            try writeNativePreambleAndControl(client, data_rpc);
            try client.ensureUniStream(2);
            try client.writeAll(2, "tiny");
            try client.client.conn.streamFinish(2);
            try client.drainOutgoing(client.nowUs());
        },
        .data_budget_violation => {
            const data_rpc = try quic.native.encodeDataRpc(
                allocator,
                0,
                2,
                8,
                quic.default_native_max_control_frame_bytes,
            );
            defer allocator.free(data_rpc);

            try writeNativePreambleAndControl(client, data_rpc);
        },
    }
}

fn writeNativePreambleAndControl(client: *RawFaultClient, control: []const u8) !void {
    var hello: [quic.native.encodedHelloLen()]u8 = undefined;
    const hello_len = try quic.native.encodeHello(&hello);
    try client.writeAll(quic.baseline_stream_id, quic.native.preface);
    try client.writeAll(quic.baseline_stream_id, hello[0..hello_len]);
    try client.writeAll(quic.baseline_stream_id, control);
}

pub fn runRawNativeFaultCase(
    fault: RawNativeFault,
    native_options: quic.NativeOptions,
    expected_err: anyerror,
) !void {
    const allocator = std.testing.allocator;

    var server = try quic.Connection.initServer(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    });
    defer server.deinit();

    const server_addr = server.getAddress();
    try std.testing.expect(server_addr == .ip4);
    try std.testing.expect(server_addr.ip4.port != 0);

    var server_state = loopback.QuicEndpointState{};
    server.start(&server_state, loopback.rejectUnexpectedQuicMessage, loopback.recordQuicError, loopback.recordQuicClose);

    var raw_client = try RawFaultClient.init(allocator, std.testing.io, server_addr);
    defer raw_client.deinit();

    var server_thread = try std.Thread.spawn(.{}, loopback.runQuicConnection, .{&server});
    var joined = false;
    defer if (!joined) {
        server.requestClose();
        server_thread.join();
    };

    try raw_client.waitForHandshake(&server_state);
    try injectRawNativeFault(allocator, &raw_client, fault);
    try raw_client.waitForServerError(&server_state);

    server.requestClose();
    server_thread.join();
    joined = true;

    try std.testing.expectEqual(@as(usize, 0), server_state.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), server_state.errors.load(.acquire));
    try std.testing.expectEqual(@as(?anyerror, expected_err), server_state.last_error);
    const status = server.closeStatus() orelse return error.QuicLoopbackMissingCloseStatus;
    try std.testing.expectEqual(quic.ApplicationCloseCode.frame_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, expected_err), status.err);
    try std.testing.expect(server.isClosing());
}
