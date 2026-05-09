const std = @import("std");
const capnpc = @import("capnpc-zig");

const quic = capnpc.rpc.transport.quic;

const Net = std.Io.net;

fn loopbackAddr() Net.IpAddress {
    return .{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = 4433,
    } };
}

fn expectQuicDecl(comptime name: []const u8) void {
    if (!@hasDecl(quic, name)) {
        @compileError("missing capnpc.rpc.transport.quic." ++ name);
    }
}

test "QUIC public API is discoverable from capnpc.rpc.transport.quic" {
    comptime {
        expectQuicDecl("Connection");
        expectQuicDecl("Server");
        expectQuicDecl("ServerSession");
        expectQuicDecl("ClientOptions");
        expectQuicDecl("ServerOptions");
        expectQuicDecl("NativeOptions");
        expectQuicDecl("TransportMode");
        expectQuicDecl("alpn");
        expectQuicDecl("serverConfigFromOptions");

        _ = quic.Connection;
        _ = quic.Server;
        _ = quic.ServerSession;
        _ = quic.ClientOptions;
        _ = quic.ServerOptions;
        _ = quic.NativeOptions;
        _ = quic.TransportMode;
        _ = quic.serverConfigFromOptions;
    }

    try std.testing.expectEqualStrings("capnp-rpc/1", quic.alpn);

    const client_options = quic.ClientOptions{
        .remote_addr = loopbackAddr(),
        .server_name = "localhost",
    };
    const server_options = quic.ServerOptions{
        .listen_addr = loopbackAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
    };
    const native_options = quic.NativeOptions{};

    try std.testing.expectEqual(quic.TransportMode.baseline, client_options.mode);
    try std.testing.expectEqual(quic.TransportMode.baseline, server_options.mode);
    try std.testing.expect(native_options.max_control_frame_bytes >= native_options.inline_frame_threshold);

    const server_config = try quic.serverConfigFromOptions(std.testing.allocator, server_options);
    try std.testing.expectEqual(@as(u32, 1), server_config.max_concurrent_connections);
    try std.testing.expectEqualStrings(quic.alpn, server_config.alpn_protocols[0]);
}
