const pending_calls = @import("./pending_calls.zig");

pub const queuePendingCall = pending_calls.queuePendingCall;
pub const deinitPendingCallOwnedFrame = pending_calls.deinitPendingCallOwnedFrame;
pub const deinitPendingCallOwnedFrameForPeerFn = pending_calls.deinitPendingCallOwnedFrameForPeerFn;
pub const recordResolvedAnswer = pending_calls.recordResolvedAnswer;
pub const replayResolvedPromiseExport = pending_calls.replayResolvedPromiseExport;
