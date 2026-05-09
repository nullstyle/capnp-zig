// Reduced RPC export surface for environments without POSIX I/O (e.g. wasm32-freestanding).
// Excludes concrete transport/runtime modules that depend on OS sockets and threads.
// Used by lib_core.zig; see mod.zig for the full export surface.

const base = @import("./mod_base.zig");

pub const wire = base.wire;
pub const caps = base.caps;
pub const promises = base.promises;
pub const transport = base.Transport(@import("./transport/quic_disabled.zig"), false);
pub const peer = base.peer;
pub const integration = base.Integration(false);
pub const generated = base.generated;
pub const testing = base.testing;
