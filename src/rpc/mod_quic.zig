const base = @import("./mod_base.zig");

pub const framing = base.framing;
pub const transport = base.transport;
pub const connection = base.connection;
pub const runtime = base.runtime;
pub const host_peer = base.host_peer;
pub const protocol = base.protocol;
pub const cap_table = base.cap_table;
pub const promise_pipeline = base.promise_pipeline;
pub const cap_pointer = base.cap_pointer;
pub const transport_binding = base.transport_binding;
pub const peer = base.peer;
pub const stream_state = base.stream_state;
pub const worker_pool = base.worker_pool;
pub const generated = base.generated;
pub const testing = base.testing;

/// Deprecated compatibility alias for old test imports. Use `rpc.testing`.
pub const _internal = base._internal;

pub const quic = @import("./transport/quic/mod.zig");
