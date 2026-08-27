//! Server-side 0-RTT dispatch posture: what may EXECUTE off RPC frames that
//! arrived in early data, before the handshake completes.
//!
//! With `early_data = .without_replay_protection`, a replayed first flight
//! could otherwise re-execute calls. A replay can never COMPLETE a
//! handshake, so `.hold_until_handshake` (the default) buffers everything
//! until the handshake lands — replay-safe, round trip still banked.
//! `.restore_only` additionally executes the longest idempotent PREFIX of
//! the early flight (Bootstrap frames and Calls on the pinned Restorer
//! interface) before the handshake: the warm-restore pattern's whole early
//! flight is exactly that prefix, so a resumed client's restore answers
//! without waiting for handshake completion. Prefix-only is what keeps
//! frame order intact: dispatch stops at the first non-qualifying frame,
//! which waits (with everything behind it) for the handshake.

const std = @import("std");
const protocol = @import("../../wire/protocol.zig");

pub const Mode = enum {
    /// Buffer every early-data frame until the handshake completes.
    hold_until_handshake,
    /// Execute the idempotent prefix (Bootstrap + Restorer calls) early;
    /// hold everything from the first other frame on.
    restore_only,
};

/// The pinned Restorer interface id — the vat-level restore convention.
/// Kept equal to `rpc.peer.persistence.restorer_interface_id` (a spec-pinned
/// value; the transport layer must not import the peer layer, so the
/// constant is mirrored here and equality is asserted in the internal QUIC
/// test suite).
pub const restorer_interface_id: u64 = 0xac47e3f6453b50f3;

/// True when `frame` may execute inside the 0-RTT replay window under
/// `.restore_only`: a Bootstrap, or a Call on the Restorer interface
/// (restore is idempotent by convention). Anything unparseable is NOT
/// qualified — it holds until the handshake, where the peer's own decode
/// path produces the proper error handling.
pub fn frameQualifies(allocator: std.mem.Allocator, frame: []const u8) bool {
    var decoded = protocol.DecodedMessage.init(allocator, frame) catch return false;
    defer decoded.deinit();
    return switch (decoded.tag) {
        .bootstrap => true,
        .call => blk: {
            const call = decoded.asCall() catch break :blk false;
            break :blk call.interface_id == restorer_interface_id;
        },
        else => false,
    };
}
