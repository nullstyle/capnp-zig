//! The only socket-level timed UDP receive used by the QUIC runtime.
//!
//! Windows must use `udp_receive_bridge.Bridge`: `std.Io`'s timed socket
//! receive is not supported by the hosted Windows backend. Keeping the call
//! behind this compile-time tripwire makes an accidental Windows use a build
//! failure instead of `error.ConcurrencyUnavailable` at runtime.

const std = @import("std");
const builtin = @import("builtin");

pub fn receive(
    socket: *const std.Io.net.Socket,
    io: std.Io,
    buffer: []u8,
    duration: std.Io.Duration,
) std.Io.net.Socket.ReceiveTimeoutError!std.Io.net.IncomingMessage {
    if (comptime builtin.target.os.tag == .windows) {
        @compileError("Windows QUIC UDP receives must use udp_receive_bridge.Bridge");
    }
    return socket.receiveTimeout(io, buffer, .{
        .duration = .{
            .raw = duration,
            .clock = .awake,
        },
    });
}
