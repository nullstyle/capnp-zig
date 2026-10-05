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
    _ = &quic.Connection.stepOnce; // mruby-quic (dial pump: stepOnce(.poll)), capnp-qmsg-demo (tests)
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
    _ = &rpc.wire.protocol.MessageBuilder.init; // capnp-qmsg-demo (buildBootstrapFrame)
    _ = &rpc.wire.protocol.MessageBuilder.buildBootstrap; // capnp-qmsg-demo (buildBootstrapFrame)
    _ = &rpc.wire.protocol.MessageBuilder.finish; // capnp-qmsg-demo (buildBootstrapFrame)
    _ = &rpc.wire.protocol.MessageBuilder.deinit; // capnp-qmsg-demo (buildBootstrapFrame)

    // Call and Return build callbacks, and reading a Return's payload.
    _ = &rpc.wire.protocol.CallBuilder.payloadTyped; // mruby-quic (buildOutboundParams)
    _ = &rpc.wire.protocol.ReturnBuilder.payloadTyped; // mruby-quic (ReplyCtx.build), capnp-qmsg-demo (tests)
    _ = &rpc.wire.protocol.ReturnBuilder.initCapTableTyped; // mruby-quic (ReplyCtx.build)
    _ = &rpc.wire.protocol.PayloadBuilder.initContent; // mruby-quic (buildOutboundParams, ReplyCtx.build), capnp-qmsg-demo (tests)
    _ = &capnpc.message.Message.getRootAnyPointer; // mruby-quic (ReplyCtx.build)
    _ = &capnpc.message.AnyPointerBuilder.initStruct; // mruby-quic (buildOutboundParams)
    _ = &capnpc.message.AnyPointerReader.getStruct; // mruby-quic (dialCallReturn), capnp-qmsg-demo (tests)
    _ = &capnpc.message.AnyPointerReader.getCapability; // mruby-quic (dialBootstrapReturn), capnp-qmsg-demo (tests)
    _ = &capnpc.message.AnyPointerReader.getData; // capnp-qmsg-demo (QmsgVatNetwork.connectToIntroduced)

    // capnp-qmsg-demo's L3, join and warm-dial tests (src/*_test.zig) wire
    // detached peers and quic.Server sessions together by hand.
    _ = &Peer.initDetached; // capnp-qmsg-demo (tests)
    _ = &Peer.handleFrame; // capnp-qmsg-demo (tests)
    _ = &Peer.setSendFrameOverride; // capnp-qmsg-demo (tests)
    _ = &Peer.setHandoffPickupHandler; // capnp-qmsg-demo (warm-dial test)
    _ = &Peer.addPromiseExport; // capnp-qmsg-demo (warm-dial test)
    _ = &Peer.resolvePromiseExportToThirdParty; // capnp-qmsg-demo (warm-dial test)
    _ = &Peer.sendProvide; // capnp-qmsg-demo (L3 tests)
    _ = &Peer.sendFinishForHost; // capnp-qmsg-demo (join test)
    _ = &rpc.caps.table.CapTable.hasImport; // capnp-qmsg-demo (tests: Peer.caps)
    _ = &rpc.wire.protocol.DecodedMessage.init; // capnp-qmsg-demo (L3 test frame routing)
    _ = &rpc.wire.protocol.DecodedMessage.deinit; // capnp-qmsg-demo (L3 test frame routing)
    _ = &rpc.wire.protocol.DecodedMessage.asBootstrap; // capnp-qmsg-demo (L3 test frame routing)
    _ = &rpc.wire.protocol.DecodedMessage.asCall; // capnp-qmsg-demo (L3 test frame routing)
    _ = &rpc.wire.protocol.DecodedMessage.asReturn; // capnp-qmsg-demo (L3 test frame routing)
    _ = &rpc.wire.protocol.DecodedMessage.asFinish; // capnp-qmsg-demo (L3 test frame routing)
    _ = &rpc.wire.protocol.DecodedMessage.asAccept; // capnp-qmsg-demo (L3 test frame routing)
    _ = &quic.Server.init; // capnp-qmsg-demo (full-stack L3 and warm-dial tests)
    _ = &quic.Server.run; // capnp-qmsg-demo (tests)
    _ = &quic.Server.stepOnce; // capnp-qmsg-demo (tests)
    _ = &quic.Server.getAddress; // capnp-qmsg-demo (tests)
    _ = &quic.Server.sessionCount; // capnp-qmsg-demo (tests)
    _ = &quic.Server.sessionAt; // capnp-qmsg-demo (tests)
    _ = &quic.Server.requestClose; // capnp-qmsg-demo (tests)
    _ = &quic.Server.deinit; // capnp-qmsg-demo (tests)
    // Not listed: `Peer.test_hooks` (the join test's
    // sendJoinExperimentalRetainedResult). It exists only when
    // `builtin.is_test`, so this executable cannot reference it.

    _ = &forceQuicGenerics;
}

// Unix-domain transport (Experimental, sprint items 7 and 9). No downstream
// uses it yet. Forced so the full roots keep exporting it and its bodies compile on
// every consumer target (stubs where AF_UNIX is unsupported).
comptime {
    const unix = capnpc.rpc.transport.unix;
    _ = &unix.listen; // release sentinel (no downstream yet)
    _ = &unix.connect; // release sentinel (no downstream yet)
    _ = &capnpc.rpc.transport.tcp.Listener.unixPath; // release sentinel (no downstream yet)
    _ = &capnpc.rpc.integration.WorkerPool.initListener; // release sentinel (no downstream yet; sprint item 9)
}

/// Never called. Taking its address forces its body through analysis, which
/// instantiates the generic `Peer.init` and `Buffer.replayInto` with the
/// concrete types the QUIC consumers pass. `_ = &Peer.init;` alone would not.
fn forceQuicGenerics(
    allocator: std.mem.Allocator,
    conn: *quic.Connection,
    seat: *quic.EmbeddedSession,
    server_session: *quic.ServerSession,
    pending: *quic.prehandshake.Buffer,
) !void {
    var dialed = Peer.init(allocator, conn); // mruby-quic (--capnp-dial), capnp-qmsg-demo
    dialed.deinit();
    var seated = Peer.init(allocator, seat); // mruby-quic, capnp-qmsg-demo (Peer over an EmbeddedSession)
    seated.deinit();
    var served = Peer.init(allocator, server_session); // capnp-qmsg-demo (tests: Peer over quic.Server.sessionAt)
    served.deinit();
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
