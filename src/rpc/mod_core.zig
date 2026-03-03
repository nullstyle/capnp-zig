// Reduced RPC export surface for environments without POSIX I/O (e.g. wasm32-freestanding).
// Excludes level2 transport/runtime modules that depend on OS sockets and threads.
// Used by lib_core.zig; see mod.zig for the full export surface.

pub const framing = @import("level0/framing.zig");
pub const protocol = @import("level0/protocol.zig");
pub const cap_table = @import("level0/cap_table.zig");
pub const promise_pipeline = @import("common/promise_pipeline.zig");
pub const cap_pointer = @import("common/cap_pointer.zig");
pub const peer = @import("level3/peer.zig");
pub const host_peer = @import("integration/host_peer.zig");
pub const generated = struct {
    pub const rpc = @import("gen/capnp/rpc.zig");
    pub const persistent = @import("gen/capnp/persistent.zig");
};
