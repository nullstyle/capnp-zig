// Reduced RPC export surface for environments without POSIX I/O (e.g. wasm32-freestanding).
// Excludes concrete transport/runtime modules that depend on OS sockets and threads.
// Used by lib_core.zig; see mod.zig for the full export surface.

pub const framing = @import("./wire/framing.zig");
pub const protocol = @import("./wire/protocol.zig");
pub const cap_table = @import("./caps/table.zig");
pub const promise_pipeline = @import("./promises/pipeline.zig");
pub const cap_pointer = @import("./caps/cap_pointer.zig");
pub const transport_binding = @import("./transport/binding.zig");
pub const peer = @import("./peer/mod.zig");
pub const host_peer = @import("./integration/host_peer.zig");
pub const testing = @import("./testing.zig");
pub const generated = struct {
    pub const rpc = @import("./gen/capnp/rpc.zig");
    pub const persistent = @import("./gen/capnp/persistent.zig");
};
