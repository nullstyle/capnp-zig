const framer = @import("./quic/length_framer.zig");

pub const enabled = false;
pub const unavailable_message =
    "capnpc-zig QUIC support is disabled; build with -Dquic=true to enable the nullq-backed transport";

pub const Error = error{QuicSupportDisabled};

pub const alpn = "capnp-rpc/1";
pub const baseline_stream_id: u64 = 0;

pub const LengthDelimitedFramer = framer.LengthDelimitedFramer;
pub const length_prefix_bytes = framer.length_prefix_bytes;

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
