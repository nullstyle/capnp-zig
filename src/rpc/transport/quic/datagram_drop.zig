//! The single policy for an oversized (truncated) inbound UDP datagram.
//!
//! UDP is unauthenticated and spoofable, so a single oversized datagram from
//! any host must not tear down the endpoint — and, for a fanout server, every
//! session on it. Treat it as a per-datagram fault: drop it and keep serving.
//! Socket-fatal errors still propagate at the call sites.
//!
//! This module exists because the policy was previously written twice and the
//! two copies disagreed: the single-connection loop dropped and continued
//! while `Server`/`Listener` returned `error.DatagramTooLarge`, which
//! `Server.run` turns into a full server close. Every receive path now routes
//! its truncation arm through `report`, so a future divergence has to be a
//! deliberate edit here rather than an omission over there.

const std = @import("std");

const events = @import("../../events.zig");
const endpoint_mod = @import("endpoint.zig");
const quic_options = @import("options.zig");

const log = std.log.scoped(.rpc_quic);

/// Log and publish one dropped oversized datagram. Call sites own the counter
/// and the "keep serving" control flow; this owns how the drop is *reported*.
///
/// The drop is reported twice on purpose. A `warn` log serves operators
/// reading logs, and a redacted `resource_rejection` event serves operators
/// reading metrics — which matters more here than for most faults, because a
/// legitimate peer whose datagram exceeds `udp_rx_buffer_size` is
/// indistinguishable on the wire from an attacker's spoofed one, and both are
/// now silent to the application. An rx buffer sized under the peers' path MTU
/// is a misconfiguration, and this event is how it stops being invisible.
pub fn report(
    observer: ?events.Observer,
    source: events.Source,
    role: events.Role,
    rx_buf_len: usize,
) void {
    log.warn("dropping truncated UDP datagram (exceeds {d}-byte rx buffer)", .{rx_buf_len});
    events.emitResourceRejection(
        observer,
        source,
        role,
        .udp_datagram_bytes,
        // Neither platform can report the datagram's true size: POSIX hands
        // back only the prefix that fit, and Windows discards the payload
        // outright. Report the ceiling that was exceeded rather than invent an
        // `attempted` from the truncated length.
        null,
        rx_buf_len,
        error.DatagramTooLarge,
    );
}

/// Log one receive dropped because of remote-provoked ICMP feedback.
///
/// Same policy as `report` — remote-influenced, socket still usable, do not
/// tear down the endpoint — but deliberately NOT the same reporting shape.
/// `report` hardcodes `.udp_datagram_bytes` and `error.DatagramTooLarge`,
/// and both are wrong here: nothing exceeded a buffer, and no datagram was
/// even delivered. Emitting a `resource_rejection` would name a resource that
/// was never exhausted, which is worse than the omission.
///
/// This is not silent to applications: the caller sets
/// `ReceiveResult.dropped_datagram`, which `connection_loop` propagates as
/// `StepResult.dropped_datagram`. If these ever need to be distinguished from
/// truncation in metrics, that wants its own event kind rather than a
/// borrowed one.
pub fn reportPeerFault(err: anyerror) void {
    log.warn("dropping UDP receive after transient peer fault: {t}", .{err});
}

pub fn eventSource(mode: quic_options.TransportMode) events.Source {
    return switch (mode) {
        .baseline => .quic_baseline,
        .native => .quic_native,
    };
}

pub fn eventRole(role: endpoint_mod.Role) events.Role {
    return switch (role) {
        .client => .client,
        .server => .server,
    };
}
