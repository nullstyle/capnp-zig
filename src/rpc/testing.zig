//! Deliberately unstable RPC test-support facade.
//!
//! This module exists for capnpc-zig's own RPC tests and compatibility
//! fixtures. It may change or disappear without semver guarantees; application
//! code should import the public `rpc` modules instead.

pub const payload_remap = @import("./caps/payload_remap.zig");
pub const peer_cleanup = @import("./peer/peer_cleanup.zig");
pub const peer_control = @import("./peer/peer_control.zig");
pub const peer_dispatch = @import("./peer/dispatch.zig");
pub const peer_embargo_accepts = @import("./peer/peer_embargo_accepts.zig");
pub const peer_return_send_helpers = @import("./promises/return_send_helpers.zig");
pub const peer_transport_callbacks = @import("./peer/peer_transport_callbacks.zig");
pub const peer_transport_state = @import("./peer/peer_transport_state.zig");
