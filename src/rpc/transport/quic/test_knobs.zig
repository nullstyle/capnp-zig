//! TEST-ONLY switches for the owned QUIC loops.
//!
//! The switches exist only in a test build (`builtin.is_test`). In any other
//! build each query is comptime false, the loops compile the extra work out,
//! and no product code can turn a switch on. Tests reach them through
//! `rpc.transport.quic.testing.knobs`.

const std = @import("std");
const builtin = @import("builtin");

const State = if (builtin.is_test) struct {
    var tick_before_service = std.atomic.Value(bool).init(false);
} else struct {};

/// The stream-end trap order: a QUIC `tick` between the receive and the first
/// service pass.
///
/// quic-zig's `Connection.tick` frees a stream once its receive half has
/// ended (`gcClosedStreams`). When the transport has read every byte of a
/// stream and its FIN or RESET then arrives alone, a tick before the service
/// pass removes the stream, and the next read gets `StreamNotFound`. The
/// owned loops service before they tick. With this switch on, the client loop
/// (`connection_loop.stepOnce`) and the fanout server
/// (`Server.stepSessionAt`) also tick right after the receive, so a test can
/// prove that the transport survives the wrong order.
pub inline fn tickBeforeService() bool {
    return if (builtin.is_test) State.tick_before_service.load(.acquire) else false;
}

/// Set the trap order on or off. A test that sets it must set it off again
/// (`defer knobs.setTickBeforeService(false)`).
pub fn setTickBeforeService(on: bool) void {
    if (!builtin.is_test) @compileError("test_knobs: tests only");
    State.tick_before_service.store(on, .release);
}
