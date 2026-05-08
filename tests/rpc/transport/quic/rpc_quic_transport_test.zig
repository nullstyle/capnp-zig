const std = @import("std");
const capnpc = @import("capnpc-zig");

const quic = capnpc.rpc.quic;

fn testListenAddr() std.Io.net.IpAddress {
    return .{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = 0,
    } };
}

fn captureServerLog(_: ?*anyopaque, _: quic.ServerLogEvent) void {}

test "quic transport exposes native Cap'n Proto RPC ALPN" {
    try std.testing.expectEqualStrings("capnp-rpc/1", quic.alpn);
    try std.testing.expectEqual(@as(u64, 0), quic.baseline_stream_id);
    try std.testing.expect(quic.default_max_outbound_queue_items > 0);
    try std.testing.expect(quic.default_max_outbound_queue_bytes > quic.default_max_message_bytes);
}

test "quic server options propagate nullq hardening controls" {
    const retry_key: quic.ServerRetryTokenKey = @splat(0x11);
    const new_token_key: quic.ServerNewTokenKey = @splat(0x22);
    var log_user_data: u8 = 0;

    const config = try quic.nullqServerConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .local_cid_len = 12,
        .log_callback = captureServerLog,
        .log_user_data = &log_user_data,
        .max_initials_per_source_per_window = 32,
        .source_rate_window_us = 123_000,
        .source_rate_table_capacity = 256,
        .max_vn_per_source_per_window = 7,
        .retry_token_key = retry_key,
        .retry_token_lifetime_us = 456_000,
        .retry_state_table_capacity = 64,
        .new_token_key = new_token_key,
        .new_token_lifetime_us = 789_000,
        .enable_0rtt = true,
        .reveal_close_reason_on_wire = true,
        .max_connection_memory = 4 * 1024 * 1024,
        .max_datagrams_per_window = 100,
        .max_bytes_per_window = 64 * 1024,
        .listener_rate_window_us = 42_000,
        .max_bytes_per_source_per_second = 32 * 1024,
        .max_log_events_per_source_per_window = 5,
    });

    try std.testing.expectEqual(@as(u8, 12), config.local_cid_len);
    try std.testing.expect(config.log_callback != null);
    try std.testing.expectEqual(@intFromPtr(&log_user_data), @intFromPtr(config.log_user_data.?));
    try std.testing.expectEqual(@as(?u32, 32), config.max_initials_per_source_per_window);
    try std.testing.expectEqual(@as(u64, 123_000), config.source_rate_window_us);
    try std.testing.expectEqual(@as(u32, 256), config.source_rate_table_capacity);
    try std.testing.expectEqual(@as(?u32, 7), config.max_vn_per_source_per_window);
    try std.testing.expectEqual(retry_key, config.retry_token_key.?);
    try std.testing.expectEqual(@as(u64, 456_000), config.retry_token_lifetime_us);
    try std.testing.expectEqual(@as(u32, 64), config.retry_state_table_capacity);
    try std.testing.expectEqual(new_token_key, config.new_token_key.?);
    try std.testing.expectEqual(@as(u64, 789_000), config.new_token_lifetime_us);
    try std.testing.expect(config.enable_0rtt);
    try std.testing.expect(config.reveal_close_reason_on_wire);
    try std.testing.expectEqual(@as(u64, 4 * 1024 * 1024), config.max_connection_memory);
    try std.testing.expectEqual(@as(?u32, 100), config.max_datagrams_per_window);
    try std.testing.expectEqual(@as(?u64, 64 * 1024), config.max_bytes_per_window);
    try std.testing.expectEqual(@as(u64, 42_000), config.listener_rate_window_us);
    try std.testing.expectEqual(@as(?u64, 32 * 1024), config.max_bytes_per_source_per_second);
    try std.testing.expectEqual(@as(?u32, 5), config.max_log_events_per_source_per_window);
}

test "quic production hardening preset enables retry and rate gates" {
    const retry_key: quic.ServerRetryTokenKey = @splat(0x33);
    const new_token_key: quic.ServerNewTokenKey = @splat(0x44);

    const options = quic.withProductionServerHardening(.{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .enable_0rtt = true,
        .reveal_close_reason_on_wire = true,
    }, .{
        .retry_token_key = retry_key,
        .new_token_key = new_token_key,
    });

    try std.testing.expectEqual(retry_key, options.retry_token_key.?);
    try std.testing.expectEqual(new_token_key, options.new_token_key.?);
    try std.testing.expectEqual(@as(?u32, 32), options.max_initials_per_source_per_window);
    try std.testing.expect(options.max_datagrams_per_window.? > 0);
    try std.testing.expect(options.max_bytes_per_window.? > 0);
    try std.testing.expect(options.max_bytes_per_source_per_second.? > 0);
    try std.testing.expect(!options.enable_0rtt);
    try std.testing.expect(!options.reveal_close_reason_on_wire);

    const config = try quic.nullqServerConfigFromOptions(std.testing.allocator, options);
    try std.testing.expectEqual(retry_key, config.retry_token_key.?);
    try std.testing.expectEqual(new_token_key, config.new_token_key.?);
    try std.testing.expectEqual(options.max_initials_per_source_per_window, config.max_initials_per_source_per_window);
    try std.testing.expectEqual(options.max_datagrams_per_window, config.max_datagrams_per_window);
    try std.testing.expectEqual(options.max_bytes_per_window, config.max_bytes_per_window);
}

test "quic length-delimited framer handles fragmented and coalesced payloads" {
    var framer = quic.LengthDelimitedFramer.init(std.testing.allocator, 1024);
    defer framer.deinit();

    var bytes: [16]u8 = @splat(0);
    std.mem.writeInt(u32, bytes[0..4], 3, .little);
    @memcpy(bytes[4..7], "abc");
    std.mem.writeInt(u32, bytes[7..11], 5, .little);
    @memcpy(bytes[11..16], "hello");

    try framer.push(bytes[0..5]);
    try std.testing.expect(try framer.popFrame() == null);
    try framer.push(bytes[5..]);

    const first = (try framer.popFrame()).?;
    defer std.testing.allocator.free(first);
    try std.testing.expectEqualStrings("abc", first);

    const second = (try framer.popFrame()).?;
    defer std.testing.allocator.free(second);
    try std.testing.expectEqualStrings("hello", second);
    try std.testing.expect(try framer.popFrame() == null);
}

test "quic path address conversion round-trips IPv4" {
    const addr: std.Io.net.IpAddress = .{ .ip4 = .{
        .bytes = .{ 10, 20, 30, 40 },
        .port = 4321,
    } };

    const path_addr = quic.ipAddressToPathAddress(addr);
    const round_trip = quic.pathAddressToIpAddress(path_addr).?;

    try std.testing.expect(round_trip == .ip4);
    try std.testing.expectEqual(addr.ip4.port, round_trip.ip4.port);
    try std.testing.expectEqualSlices(u8, &addr.ip4.bytes, &round_trip.ip4.bytes);
}

test "quic client outbound queue enforces item and byte bounds" {
    const remote_addr: std.Io.net.IpAddress = .{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = 4433,
    } };

    var item_limited = try quic.Connection.initClient(std.testing.allocator, std.testing.io, .{
        .remote_addr = remote_addr,
        .server_name = "localhost",
        .max_outbound_queue_items = 1,
        .max_outbound_queue_bytes = 1024,
    });
    defer item_limited.deinit();

    try item_limited.sendFrame("abc");
    try std.testing.expectError(error.OutboundQueueFull, item_limited.sendFrame("def"));

    var byte_limited = try quic.Connection.initClient(std.testing.allocator, std.testing.io, .{
        .remote_addr = remote_addr,
        .server_name = "localhost",
        .max_outbound_queue_items = 4,
        .max_outbound_queue_bytes = 8,
    });
    defer byte_limited.deinit();

    try byte_limited.sendFrame("abcd");
    try std.testing.expectError(error.OutboundQueueFull, byte_limited.sendFrame("e"));
}

test "quic server rejects multi-connection option until fanout exists" {
    try std.testing.expectError(error.InvalidConfig, quic.nullqServerConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .max_concurrent_connections = 2,
    }));
}

test "quic server options reject unusable hardening limits" {
    try std.testing.expectError(error.InvalidConfig, quic.nullqServerConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .local_cid_len = 0,
    }));

    try std.testing.expectError(error.InvalidConfig, quic.nullqServerConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .max_datagrams_per_window = 0,
    }));

    try std.testing.expectError(error.InvalidConfig, quic.nullqServerConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .retry_token_key = @splat(0x55),
        .retry_state_table_capacity = 0,
    }));
}
