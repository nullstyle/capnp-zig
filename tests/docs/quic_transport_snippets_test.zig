const std = @import("std");
const capnpc = @import("capnpc-zig");

const quic = capnpc.rpc.transport.quic;
const rpc_events = capnpc.rpc.events;

const Net = std.Io.net;

const server_cert_pem = "test certificate fixture";
const server_key_pem = "test private key fixture";

fn loopbackAddr(port: u16) Net.IpAddress {
    return .{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port,
    } };
}

fn initServerOptions(
    listen_addr: Net.IpAddress,
    observer: ?rpc_events.Observer,
) quic.ServerOptions {
    return .{
        .listen_addr = listen_addr,
        .tls_cert_pem = server_cert_pem,
        .tls_key_pem = server_key_pem,
        .mode = .native,
        .native = .{
            .inline_frame_threshold = 64 * 1024,
            .max_control_frame_bytes = quic.default_native_max_control_frame_bytes,
            .max_pending_data_streams = 16,
            .max_pending_data_bytes = quic.default_native_max_pending_data_bytes,
        },
        .observer = observer,
    };
}

fn initClientOptions(
    server_addr: Net.IpAddress,
    observer: ?rpc_events.Observer,
) quic.ClientOptions {
    return .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .mode = .native,
        .native = .{
            .inline_frame_threshold = 64 * 1024,
            .max_control_frame_bytes = quic.default_native_max_control_frame_bytes,
            .max_pending_data_streams = 16,
            .max_pending_data_bytes = quic.default_native_max_pending_data_bytes,
        },
        .observer = observer,
    };
}

test "quic transport guide native mode snippets use the public options surface" {
    comptime {
        if (!quic.enabled) @compileError("QUIC transport snippets require -Dquic=true");
        _ = quic.Connection;
        _ = quic.Server;
        _ = quic.ServerSession;
        _ = quic.Listener;
        _ = quic.NativeOptions;
        _ = quic.TransportMode;
    }

    var event_count: usize = 0;
    const ObserverCtx = struct {
        fn onEvent(ctx: *anyopaque, _: rpc_events.Event) void {
            const count: *usize = @ptrCast(@alignCast(ctx));
            count.* += 1;
        }
    };
    const observer = rpc_events.Observer.init(&event_count, ObserverCtx.onEvent);

    const server_options = initServerOptions(loopbackAddr(7000), observer);
    const client_options = initClientOptions(loopbackAddr(7000), observer);

    try std.testing.expectEqual(quic.TransportMode.native, server_options.mode);
    try std.testing.expectEqual(quic.TransportMode.native, client_options.mode);
    try std.testing.expectEqualStrings(quic.alpn, server_options.alpn_protocols[0]);
    try std.testing.expectEqualStrings(quic.alpn, client_options.alpn_protocols[0]);
    try std.testing.expect(server_options.native.max_control_frame_bytes >= server_options.native.inline_frame_threshold);
    try std.testing.expect(client_options.native.max_control_frame_bytes >= client_options.native.inline_frame_threshold);
    try std.testing.expect(server_options.observer != null);
    try std.testing.expect(client_options.observer != null);

    const server_config = try quic.serverConfigFromOptions(std.testing.allocator, server_options);
    try std.testing.expectEqual(quic.compatibility_max_concurrent_sessions, server_config.max_concurrent_connections);
    try std.testing.expectEqualStrings(quic.alpn, server_config.alpn_protocols[0]);
}

test "quic transport guide server fanout and hardening snippets avoid network setup" {
    const retry_key: quic.ServerRetryTokenKey = @splat(0x33);
    const new_token_key: quic.ServerNewTokenKey = @splat(0x44);

    const fanout_options = quic.ServerOptions{
        .listen_addr = loopbackAddr(7001),
        .tls_cert_pem = server_cert_pem,
        .tls_key_pem = server_key_pem,
        .max_concurrent_connections = 4,
        .mode = .native,
        .native = .{},
    };

    try std.testing.expectEqual(@as(u32, 4), fanout_options.max_concurrent_connections);
    try std.testing.expectEqual(quic.TransportMode.native, fanout_options.mode);

    const hardened_options = quic.withProductionServerHardening(fanout_options, .{
        .retry_token_key = retry_key,
        .new_token_key = new_token_key,
    });

    try std.testing.expectEqual(retry_key, hardened_options.retry_token_key.?);
    try std.testing.expectEqual(new_token_key, hardened_options.new_token_key.?);
    // Production hardening pins every bandwidth/flood ceiling to an EXPLICIT
    // cap rather than `.default`, because `.default` for the listener and
    // bandwidth limiters resolves to "off" (the right ceiling is
    // deployment-specific). `.resolve(0)` therefore has to yield a real cap.
    try std.testing.expect(hardened_options.initial_source_rate_limit.resolve(0).? > 0);
    try std.testing.expect(hardened_options.listener_datagram_rate_limit.resolve(0).? > 0);
    try std.testing.expect(hardened_options.listener_byte_rate_limit.resolve(0).? > 0);
    try std.testing.expect(hardened_options.source_byte_rate_limit.resolve(0).? > 0);
    try std.testing.expect(hardened_options.early_data == .disabled);
    try std.testing.expect(!hardened_options.reveal_close_reason_on_wire);

    const server_config = try quic.serverConfigFromOptions(std.testing.allocator, hardened_options);
    try std.testing.expectEqual(@as(u32, 4), server_config.max_concurrent_connections);
    try std.testing.expectEqual(retry_key, server_config.retry_token_key.?);
    try std.testing.expectEqual(new_token_key, server_config.new_token_key.?);
}
