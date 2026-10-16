//! TEST-ONLY switches for the owned QUIC loops.
//!
//! The switches exist only in a test build (`builtin.is_test`). In any other
//! build each query is comptime false, the loops compile the extra work out,
//! and no product code can turn a switch on. Tests reach them through
//! `rpc.transport.quic.testing.knobs`.

const std = @import("std");
const builtin = @import("builtin");

const PauseState = enum(u8) { off, armed, paused };

const State = if (builtin.is_test) struct {
    var tick_before_service = std.atomic.Value(bool).init(false);
    var pause_after_publish = std.atomic.Value(PauseState).init(.off);
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

/// The longest time that `pauseAfterPublish` waits. A broken test that never
/// ends the pause then fails, and does not hang.
pub const pause_limit_ms: u64 = 10_000;

/// The WarmRedialClient setup gap: a pause in `runGeneration` after it
/// publishes the generation's connection (`current_conn`) and before it
/// enqueues the Bootstrap and Restore frames. A `requestStop` in this gap
/// closes the connection before the first frame goes out.
///
/// The pause is one-shot. Only the first generation that gets here after
/// `setPauseAfterPublish(true)` pauses. It waits until the test calls
/// `setPauseAfterPublish(false)`, or for `pause_limit_ms`.
pub inline fn pauseAfterPublish(io: std.Io) void {
    if (builtin.is_test) pauseAfterPublishSlow(io);
}

fn pauseAfterPublishSlow(io: std.Io) void {
    if (State.pause_after_publish.cmpxchgStrong(.armed, .paused, .acq_rel, .acquire) != null) return;
    var waited_ms: u64 = 0;
    while (State.pause_after_publish.load(.acquire) == .paused and waited_ms < pause_limit_ms) : (waited_ms += 1) {
        std.Io.sleep(io, std.Io.Duration.fromMilliseconds(1), .awake) catch break;
    }
    _ = State.pause_after_publish.cmpxchgStrong(.paused, .off, .acq_rel, .acquire);
}

/// Arm the setup-gap pause (true), or disarm it and end a pause that is in
/// progress (false). A test that arms it must disarm it again
/// (`defer knobs.setPauseAfterPublish(false)`).
pub fn setPauseAfterPublish(on: bool) void {
    if (!builtin.is_test) @compileError("test_knobs: tests only");
    State.pause_after_publish.store(if (on) .armed else .off, .release);
}

/// True while a generation waits in the setup-gap pause.
pub fn pausedAfterPublish() bool {
    if (!builtin.is_test) @compileError("test_knobs: tests only");
    return State.pause_after_publish.load(.acquire) == .paused;
}
