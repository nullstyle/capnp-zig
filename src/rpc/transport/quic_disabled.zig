const framer = @import("./quic/length_framer.zig");
const native_framer = @import("./quic/native_framer.zig");

pub const enabled = false;
pub const unavailable_message =
    "capnpc-zig QUIC support is disabled; build with -Dquic=true to enable the quic-zig-backed transport";

pub const Error = error{QuicSupportDisabled};

pub const alpn = "capnp-rpc/1";
pub const baseline_stream_id: u64 = 0;
pub const default_native_inline_frame_threshold: usize = 64 * 1024;
pub const default_native_max_control_frame_bytes: usize = native_framer.rpc_header_bytes + default_native_inline_frame_threshold;
pub const default_native_max_pending_data_streams: usize = 16;
pub const default_native_max_pending_data_bytes: usize = 8 * 1024 * 1024 * 8;

pub const LengthDelimitedFramer = framer.LengthDelimitedFramer;
pub const NativeControlFramer = native_framer.ControlFramer;
pub const NativeControlFrame = native_framer.ControlFrame;
pub const length_prefix_bytes = framer.length_prefix_bytes;
pub const native = native_framer;

pub const TransportMode = enum {
    baseline,
    native,
};

pub const NativeOptions = struct {
    inline_frame_threshold: usize = default_native_inline_frame_threshold,
    max_control_frame_bytes: usize = default_native_max_control_frame_bytes,
    max_pending_data_streams: usize = default_native_max_pending_data_streams,
    max_pending_data_bytes: usize = default_native_max_pending_data_bytes,
};

pub fn requireEnabled() void {
    @compileError(unavailable_message);
}

pub const Connection = struct {
    pub fn initClient(_: anytype, _: anytype, _: anytype) noreturn {
        @compileError(unavailable_message);
    }

    pub fn initServer(_: anytype, _: anytype, _: anytype) noreturn {
        @compileError(unavailable_message);
    }
};
