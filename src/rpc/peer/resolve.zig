const peer_control = @import("./peer_control.zig");

pub const ResolveOps = peer_control.ResolveOps;
pub const handleResolve = peer_control.handleResolve;
pub const handleResolveWithOps = peer_control.handleResolveWithOps;
pub const hasKnownResolvePromiseForPeer = peer_control.hasKnownResolvePromiseForPeer;
pub const hasKnownResolvePromiseForPeerFn = peer_control.hasKnownResolvePromiseForPeerFn;
pub const resolveCapDescriptorForPeer = peer_control.resolveCapDescriptorForPeer;
pub const resolveCapDescriptorForPeerFn = peer_control.resolveCapDescriptorForPeerFn;
pub const allocateEmbargoIdForPeer = peer_control.allocateEmbargoIdForPeer;
pub const allocateEmbargoIdForPeerFn = peer_control.allocateEmbargoIdForPeerFn;
pub const rememberPendingEmbargoForPeer = peer_control.rememberPendingEmbargoForPeer;
pub const rememberPendingEmbargoForPeerFn = peer_control.rememberPendingEmbargoForPeerFn;
pub const forgetPendingEmbargoForPeer = peer_control.forgetPendingEmbargoForPeer;
pub const forgetPendingEmbargoForPeerFn = peer_control.forgetPendingEmbargoForPeerFn;
