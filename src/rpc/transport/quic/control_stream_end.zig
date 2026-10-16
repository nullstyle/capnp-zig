const quic_options = @import("options.zig");

/// How the receive half of stream 0, the ordered control stream, stands
/// when a read finds no bytes. Both stream engines act on it (see
/// "Wire constants (frozen)" in docs/quic-transport.md):
///
/// - `clean`: the session ends with close code `normal` and cause
///   `peer_close`, after every whole frame before the FIN has been
///   dispatched. A partial frame at the FIN is a `frame_error` close.
/// - `cut`: the session ends at once with close code `normal` and cause
///   `transport_error`. Bytes of stream 0 were lost, and the E-order
///   contract has no way to go on without them.
pub const ControlEnd = enum {
    /// More bytes can come, or bytes wait to be read.
    open,
    /// A clean FIN, and every byte up to it has been read.
    clean,
    /// The peer reset stream 0, this side stopped it, or it ended and
    /// quic-zig cannot say how.
    cut,
};

/// Classify the end of stream 0 on `conn`: quic-zig's `Connection` in the
/// owned loops, or the embedded seat's buffered view, whose `stream` view
/// counts the bytes the seat holds and not the bytes its host read. Read
/// the live stream first. Only when it is gone, ask quic-zig's note of the
/// end (`streamRecvEnd`).
pub fn classify(conn: anytype) ControlEnd {
    const id = quic_options.baseline_stream_id;
    if (conn.stream(id)) |s| {
        if (s.recv.reset != null or s.recv_stopped) return .cut;
        const final_size = s.recv.final_size orelse return .open;
        return if (s.recv.read_offset >= final_size) .clean else .open;
    }
    if (conn.streamRecvEnd(id)) |end| {
        if (end.stopped or end.reset_code != null) return .cut;
        // quic-zig frees a stream once both of its halves end, also with
        // bytes still unread. Those bytes are gone.
        return if (end.fin_seen and end.read_offset >= end.final_size) .clean else .cut;
    }
    // Freed, and the note of its end is gone: treat it as cut.
    if (conn.streamRecvWasReaped(id)) return .cut;
    return .open;
}
