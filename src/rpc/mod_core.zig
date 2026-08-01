// Reduced RPC export surface for environments without OS socket I/O (e.g. wasm32-freestanding).
// Excludes concrete transport/runtime modules that depend on OS sockets and threads.
// Used by lib_core.zig; see mod.zig for the full export surface.

const base = @import("./mod_base.zig");

pub const wire = base.wire;
pub const caps = base.caps;
pub const promises = base.promises;
pub const events = base.events;
pub const time = base.time;
pub const transport = base.Transport(@import("./transport/quic_disabled.zig"), false);
pub const peer = base.peer;

// The L3 vat surface (provision index, host, join/vat networks) is pure logic:
// nothing under src/rpc/vat/ imports a transport, socket, or thread. It belongs
// in the core surface, and its absence here was an oversight rather than a
// design decision -- it left `capnpc-zig-core` and the wasm build unable to
// reach `rpc.vat.*` at all. Verified to compile for wasm32-freestanding.
pub const vat = base.vat;
pub const integration = base.Integration(false);
pub const generated = base.generated;
pub const testing = base.testing;
