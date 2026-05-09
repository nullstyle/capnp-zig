const return_routing = @import("./return_routing.zig");
const return_send = @import("./return_send.zig");

pub const clearSendResultsToThirdPartyForPeer = return_routing.clearSendResultsToThirdPartyForPeer;
pub const clearSendResultsRoutingForPeer = return_routing.clearSendResultsRoutingForPeer;
pub const noteSendResultsToYourselfForPeer = return_routing.noteSendResultsToYourselfForPeer;
pub const noteSendResultsToThirdPartyForPeer = return_routing.noteSendResultsToThirdPartyForPeer;

pub const sendReturnFrameWithLoopbackForPeer = return_send.sendReturnFrameWithLoopbackForPeer;
pub const noteOutboundReturnCapRefsForPeer = return_send.noteOutboundReturnCapRefsForPeer;
pub const rollbackOutboundReturnCapRefsForPeer = return_send.rollbackOutboundReturnCapRefsForPeer;
