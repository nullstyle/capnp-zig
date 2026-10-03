//! Refusal of peer-opened streams that have no place in this transport.
//!
//! The RPC transport uses a fixed set of streams: the client's
//! bidirectional stream 0 (both modes) and, in native mode only, one-shot
//! unidirectional data streams opened by either side. A peer can open
//! any other stream id, and before this module nothing here ever read,
//! finished, or reset one.
//!
//! Since quic v0.24.0 that matters: a peer's stream limit is a WINDOW of
//! streams open at once, and an id comes back only when its stream is
//! fully closed (both directions). A stream we never answer and never
//! finish keeps its place in the window for the life of the connection,
//! and its unread bytes stay charged to the connection's flow-control
//! window. So every unexpected stream is refused, completely: STOP_SENDING
//! ends the half the peer sends on (quic then reads and drops what still
//! arrives) and, for a bidirectional stream, RESET_STREAM ends our half.
//! STOP_SENDING alone would leave a bidirectional stream half open here.

const quic_zig = @import("quic");

const close = @import("close.zig");
const endpoint_mod = @import("endpoint.zig");
const options = @import("options.zig");

const Role = endpoint_mod.Role;
const TransportMode = options.TransportMode;

/// Application error code carried by the STOP_SENDING and RESET_STREAM of
/// a refused stream: the peer used a stream this protocol has no use for.
/// The connection itself stays up.
pub const refusal_code: u64 = @backingInt(close.ApplicationCloseCode.protocol_error);

/// Whether a stream the PEER opened has a place in the protocol, seen from
/// an endpoint with `role` in `mode`. RFC 9000 §2.1: bit 1 of the id is the
/// direction (0 bidirectional, 1 unidirectional).
pub fn expected(role: Role, mode: TransportMode, stream_id: u64) bool {
    const bidi = stream_id & 0b10 == 0;
    if (bidi) {
        // Only the client's stream 0, the RPC (baseline) or control
        // (native) stream. A server never opens a bidirectional stream.
        return role == .server and stream_id == options.baseline_stream_id;
    }
    // Unidirectional: native data streams. Baseline mode uses none.
    return mode == .native;
}

/// Refuse one peer stream on both halves. `conn` is a `*quic_zig.Connection`
/// (anytype so tests can drive it with a fake). Errors are ignored on
/// purpose: quic refuses to name a stream that is already closed, and
/// either way the stream is not ours to keep.
pub fn refuse(conn: anytype, stream_id: u64, bidi: bool) void {
    conn.streamStopSending(stream_id, refusal_code) catch {};
    if (bidi) conn.streamReset(stream_id, refusal_code) catch {};
}

/// Drain `conn`'s event queue and refuse every peer stream that
/// `expected` rejects. Loop-thread only, once per service pass.
///
/// Nothing else in the owned-loop transports consumes `pollEvent`: close
/// causes come from the sticky `closeEvent()`, and the other events
/// (flow_blocked, connection_ids_needed, datagram acks, ...) are bounded
/// queues this transport never read. Peer stream opens are surfaced by a
/// watermark, so none is lost however late this runs.
pub fn refuseUnexpected(conn: *quic_zig.Connection, role: Role, mode: TransportMode) void {
    while (conn.pollEvent()) |event| switch (event) {
        .stream_opened => |info| if (!expected(role, mode, info.stream_id)) refuse(conn, info.stream_id, info.bidi),
        else => {},
    };
}
