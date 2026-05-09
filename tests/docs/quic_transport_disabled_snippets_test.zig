const std = @import("std");
const capnpc = @import("capnpc-zig");

const quic = capnpc.rpc.transport.quic;

test "quic transport guide snippets keep default builds dependency-free" {
    comptime {
        if (quic.enabled) @compileError("disabled QUIC docs fixture must run without -Dquic=true");
        _ = quic.Connection;
        _ = quic.LengthDelimitedFramer;
        _ = quic.NativeControlFramer;
        _ = quic.NativeOptions;
        _ = quic.TransportMode;
    }

    try std.testing.expect(!quic.enabled);
    try std.testing.expectEqualStrings("capnp-rpc/1", quic.alpn);
    try std.testing.expectEqual(@as(u64, 0), quic.baseline_stream_id);

    const native_options = quic.NativeOptions{
        .inline_frame_threshold = 64 * 1024,
        .max_control_frame_bytes = quic.default_native_max_control_frame_bytes,
        .max_pending_data_streams = 16,
        .max_pending_data_bytes = quic.default_native_max_pending_data_bytes,
    };

    try std.testing.expectEqual(quic.TransportMode.native, .native);
    try std.testing.expect(native_options.max_control_frame_bytes >= native_options.inline_frame_threshold);
    try std.testing.expect(native_options.max_pending_data_streams > 0);
    try std.testing.expect(native_options.max_pending_data_bytes > 0);
}
