const transport_binding = @import("../transport/binding.zig");

pub const callbacks = @import("./peer_transport_callbacks.zig");
pub const state = @import("./peer_transport_state.zig");

/// Peer-facing transport callback bundle specialized for a concrete Peer type.
pub fn Binding(comptime PeerType: type) type {
    return transport_binding.Binding(PeerType);
}
