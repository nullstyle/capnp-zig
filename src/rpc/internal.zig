//! Deprecated compatibility alias for the previous `rpc._internal` surface.
//!
//! New in-tree tests should use `rpc.testing`. This alias intentionally mirrors
//! only the narrow test-support facade and will not grow into a public API.

const testing = @import("./testing.zig");

pub const payload_remap = testing.payload_remap;
pub const peer_cleanup = testing.peer_cleanup;
pub const peer_control = testing.peer_control;
pub const peer_dispatch = testing.peer_dispatch;
pub const peer_embargo_accepts = testing.peer_embargo_accepts;
pub const peer_return_send_helpers = testing.peer_return_send_helpers;
pub const peer_transport_callbacks = testing.peer_transport_callbacks;
pub const peer_transport_state = testing.peer_transport_state;
