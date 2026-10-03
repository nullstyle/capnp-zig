const std = @import("std");
const capnpc = @import("capnpc-zig");
const common = @import("common.zig");

comptime {
    if (!capnpc.rpc.transport.quic.enabled) {
        @compileError("the QUIC consumer resolved the non-QUIC package root");
    }
    _ = capnpc.rpc.transport.quic.Connection;
    _ = capnpc.canonical;
}

const rpc = capnpc.rpc;
const quic = rpc.transport.quic;
const Peer = rpc.peer.Peer;

// QUIC-root surface beyond common.zig's shared set. See common.zig for why
// each line takes an `&`, and RELEASING.md for the rule that keeps this list
// current. Both consumers embed capnp-rpc/1 seats in their own quic-zig
// driver and also dial out on owned connections.
comptime {
    _ = &quic.Connection.initClient; // mruby-quic (--capnp-dial), capnp-qmsg-demo
    _ = &quic.Connection.start; // capnp-qmsg-demo (raw capnp client)
    _ = &quic.Connection.run; // capnp-qmsg-demo (runCapnpClientLoop)
    _ = &quic.Connection.sendFrame; // capnp-qmsg-demo
    _ = &quic.Connection.requestClose; // mruby-quic, capnp-qmsg-demo
    _ = &quic.Connection.isClosing; // mruby-quic (dial liveness)
    _ = &quic.Connection.activeQuicConnection; // mruby-quic (dial liveness)
    _ = &quic.Connection.deinit; // mruby-quic, capnp-qmsg-demo

    _ = &quic.Listener.init; // capnp-qmsg-demo (capnp-only listener)
    _ = &quic.Listener.receiveOne; // capnp-qmsg-demo
    _ = &quic.Listener.drainSessionDatagrams; // capnp-qmsg-demo
    _ = &quic.Listener.tick; // capnp-qmsg-demo
    _ = &quic.Listener.reapClosedSessions; // capnp-qmsg-demo
    _ = &quic.Listener.nowUs; // capnp-qmsg-demo
    _ = &quic.Listener.getAddress; // capnp-qmsg-demo
    _ = &quic.Listener.droppedDatagramCount; // capnp-qmsg-demo
    _ = &quic.Listener.deinit; // capnp-qmsg-demo
    _ = &quic.Session.fromSlot; // capnp-qmsg-demo

    _ = &quic.isCapnpSessionAlpn; // mruby-quic, capnp-qmsg-demo (shared-endpoint ALPN routing)
    _ = &quic.EmbeddedSession.create; // mruby-quic, capnp-qmsg-demo
    _ = &quic.EmbeddedSession.destroy; // mruby-quic, capnp-qmsg-demo
    _ = &quic.EmbeddedSession.start; // capnp-qmsg-demo (echo seat without a Peer)
    _ = &quic.EmbeddedSession.context; // capnp-qmsg-demo
    _ = &quic.EmbeddedSession.sendFrame; // capnp-qmsg-demo
    _ = &quic.EmbeddedSession.service; // mruby-quic, capnp-qmsg-demo
    _ = &quic.EmbeddedSession.requestClose; // mruby-quic, capnp-qmsg-demo
    _ = &quic.EmbeddedSession.notifyDisconnected; // mruby-quic, capnp-qmsg-demo
    _ = &quic.EmbeddedSession.onStreamOpen; // capnp-qmsg-demo (driver stream hooks)
    _ = &quic.EmbeddedSession.onStreamData; // capnp-qmsg-demo
    _ = &quic.EmbeddedSession.onStreamEnd; // capnp-qmsg-demo
    _ = &quic.prehandshake.Buffer.init; // mruby-quic, capnp-qmsg-demo (0-RTT pre-seat buffer)
    _ = &quic.prehandshake.Buffer.recordOpen; // mruby-quic, capnp-qmsg-demo
    _ = &quic.prehandshake.Buffer.recordData; // mruby-quic, capnp-qmsg-demo
    _ = &quic.prehandshake.Buffer.recordEnd; // mruby-quic, capnp-qmsg-demo
    _ = &quic.prehandshake.Buffer.isEmpty; // mruby-quic
    _ = &quic.prehandshake.Buffer.deinit; // mruby-quic, capnp-qmsg-demo

    _ = &Peer.disableThreadAffinity; // mruby-quic, capnp-qmsg-demo
    _ = &Peer.start; // mruby-quic, capnp-qmsg-demo
    _ = &Peer.deinit; // mruby-quic, capnp-qmsg-demo
    _ = &Peer.setBootstrap; // mruby-quic, capnp-qmsg-demo
    _ = &Peer.sendBootstrap; // mruby-quic, capnp-qmsg-demo
    _ = &Peer.sendCall; // capnp-qmsg-demo
    _ = &Peer.sendCallResolved; // mruby-quic (CAPNP.call)
    _ = &Peer.sendReturnResults; // mruby-quic, capnp-qmsg-demo
    _ = &Peer.sendReturnException; // mruby-quic
    _ = &Peer.sendReturnEmptyStruct; // mruby-quic
    _ = &Peer.sendAccept; // capnp-qmsg-demo (L3 pickup)
    _ = &Peer.releaseImport; // capnp-qmsg-demo
    _ = &Peer.attachProvisionIndex; // capnp-qmsg-demo (shared ProvisionIndex)
    _ = &Peer.attachVatNetwork; // capnp-qmsg-demo (QmsgVatNetwork)
    _ = &Peer.attachJoinNetwork; // capnp-qmsg-demo (QmsgJoinNetwork)
    _ = &rpc.peer.ProvisionIndex.init; // capnp-qmsg-demo
    _ = &rpc.peer.ProvisionIndex.stats; // capnp-qmsg-demo
    _ = &rpc.peer.ProvisionIndex.deinit; // capnp-qmsg-demo

    _ = &rpc.caps.table.InboundCapTable.resolveCapability; // capnp-qmsg-demo (call handlers)
    _ = &rpc.caps.table.InboundCapTable.retainCapability; // capnp-qmsg-demo (call handlers)
    _ = &rpc.events.Observer.init; // mruby-quic, capnp-qmsg-demo (endpoint_metrics)
    _ = &rpc.vat.network.VatNetwork(Peer).init; // capnp-qmsg-demo (QmsgVatNetwork)
    _ = &rpc.vat.network.encodeNonceToken; // capnp-qmsg-demo (QmsgVatNetwork)
    _ = &rpc.vat.join.JoinNetwork(Peer).init; // capnp-qmsg-demo (QmsgJoinNetwork)
    _ = &rpc.vat.join.encodeJoinResult; // capnp-qmsg-demo (QmsgJoinNetwork)
    _ = &rpc.vat.join.decodeJoinResult; // capnp-qmsg-demo (QmsgJoinNetwork)
    _ = &rpc.wire.protocol.MessageBuilder.init; // capnp-qmsg-demo (raw capnp client frames)

    _ = &forceQuicGenerics;
}

/// Never called. Taking its address forces its body through analysis, which
/// instantiates the generic `Peer.init` and `Buffer.replayInto` with the
/// concrete types the QUIC consumers pass. `_ = &Peer.init;` alone would not.
fn forceQuicGenerics(
    allocator: std.mem.Allocator,
    conn: *quic.Connection,
    seat: *quic.EmbeddedSession,
    pending: *quic.prehandshake.Buffer,
) !void {
    var dialed = Peer.init(allocator, conn); // mruby-quic (--capnp-dial), capnp-qmsg-demo
    dialed.deinit();
    var seated = Peer.init(allocator, seat); // mruby-quic, capnp-qmsg-demo (Peer over an EmbeddedSession)
    seated.deinit();
    try pending.replayInto(seat, streamEnd); // mruby-quic, capnp-qmsg-demo (0-RTT replay)
}

fn streamEnd(kind: quic.prehandshake.EndKind) quic.quic_app.StreamEnd {
    return switch (kind) {
        .fin => .fin,
        .reset => .reset,
        .reaped => .reaped,
    };
}

pub fn main() !void {
    try common.exerciseSerialization();
}
