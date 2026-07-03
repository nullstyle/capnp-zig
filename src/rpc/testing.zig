//! Deliberately unstable RPC test-support facade.
//!
//! This module exists for capnpc-zig's own RPC tests and compatibility
//! fixtures. It may change or disappear without semver guarantees; application
//! code should import the public `rpc` modules instead.

pub const payload_remap = @import("./caps/payload_remap.zig");
pub const peer_bootstrap = @import("./peer/bootstrap.zig");
pub const peer_cap_lifecycle = @import("./peer/peer_cap_lifecycle.zig");
pub const peer_cleanup = @import("./peer/peer_cleanup.zig");
pub const peer_disembargo = @import("./peer/disembargo.zig");
pub const peer_dispatch = @import("./peer/dispatch.zig");
pub const peer_embargo_accepts = @import("./peer/peer_embargo_accepts.zig");
pub const peer_finish = @import("./peer/finish.zig");
pub const peer_provide_accept_join = @import("./peer/provide_accept_join.zig");
pub const peer_provide_join_orchestration = @import("./peer/provide/peer_provide_join_orchestration.zig");
pub const peer_resolve = @import("./peer/resolve.zig");
pub const peer_return_dispatch = @import("./peer/return/peer_return_dispatch.zig");
pub const peer_return_orchestration = @import("./peer/return/peer_return_orchestration.zig");
pub const peer_return_send_helpers = @import("./promises/return_send_helpers.zig");
pub const peer_third_party = @import("./peer/third_party.zig");
pub const peer_transport_callbacks = @import("./peer/peer_transport_callbacks.zig");
pub const peer_transport_state = @import("./peer/peer_transport_state.zig");
