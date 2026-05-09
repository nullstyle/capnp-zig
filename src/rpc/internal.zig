//! Deprecated compatibility alias for the previous `rpc._internal` surface.
//!
//! New in-tree tests should use `rpc.testing`. This alias intentionally mirrors
//! only the narrow test-support facade and will not grow into a public API.

const testing = @import("./testing.zig");

pub const payload_remap = testing.payload_remap;
pub const peer_bootstrap = testing.peer_bootstrap;
pub const peer_cap_lifecycle = testing.peer_cap_lifecycle;
pub const peer_cleanup = testing.peer_cleanup;
pub const peer_disembargo = testing.peer_disembargo;
pub const peer_dispatch = testing.peer_dispatch;
pub const peer_embargo_accepts = testing.peer_embargo_accepts;
pub const peer_finish = testing.peer_finish;
pub const peer_provide_accept_join = testing.peer_provide_accept_join;
pub const peer_resolve = testing.peer_resolve;
pub const peer_return_send_helpers = testing.peer_return_send_helpers;
pub const peer_third_party = testing.peer_third_party;
pub const peer_transport_callbacks = testing.peer_transport_callbacks;
pub const peer_transport_state = testing.peer_transport_state;
