const peer_control = @import("./peer_control.zig");

pub const FinishOps = peer_control.FinishOps;
pub const clearFinishQuestionState = peer_control.clearFinishQuestionState;
pub const forwardTailFinishIfNeeded = peer_control.forwardTailFinishIfNeeded;
pub const handleResolvedAnswerCleanup = peer_control.handleResolvedAnswerCleanup;
pub const handleFinish = peer_control.handleFinish;
pub const handleFinishWithOps = peer_control.handleFinishWithOps;
pub const takeResolvedAnswerFrameForPeer = peer_control.takeResolvedAnswerFrameForPeer;
pub const takeResolvedAnswerFrameForPeerFn = peer_control.takeResolvedAnswerFrameForPeerFn;
pub const freeOwnedFrameForPeer = peer_control.freeOwnedFrameForPeer;
pub const freeOwnedFrameForPeerFn = peer_control.freeOwnedFrameForPeerFn;
