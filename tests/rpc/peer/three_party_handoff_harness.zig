const std = @import("std");
const capnpc = @import("capnpc-zig");

const Peer = capnpc.rpc.peer.Peer;

pub fn expectImport(peer: *const Peer, import_id: u32) !void {
    try std.testing.expect(peer.caps.hasImport(import_id));
}

pub fn expectNoImport(peer: *const Peer, import_id: u32) !void {
    try std.testing.expect(!peer.caps.hasImport(import_id));
}

pub fn expectNoProvideState(peer: *const Peer) !void {
    try std.testing.expectEqual(@as(usize, 0), peer.provides_by_question.count());
    try std.testing.expectEqual(@as(usize, 0), peer.provides_by_key.count());
}

pub fn expectOutboundProvideCoupled(
    recipient_peer: *const Peer,
    provide_peer: *const Peer,
    vine_id: u32,
    provide_question_id: u32,
    provided_import_id: ?u32,
) !void {
    const outbound = recipient_peer.outbound_provides.get(vine_id) orelse return error.MissingOutboundProvide;
    try std.testing.expectEqual(provide_peer, outbound.provide_peer orelse return error.MissingProvidePeer);
    try std.testing.expectEqual(provide_question_id, outbound.provide_question_id);
    try std.testing.expectEqual(provided_import_id, outbound.provided_import_id);

    for (provide_peer.coupled_vines.items) |link| {
        if (link.recipient_peer == recipient_peer and link.vine_id == vine_id) return;
    }
    return error.MissingCoupledVineBacklink;
}

pub fn expectNoOutboundProvide(recipient_peer: *const Peer, vine_id: u32) !void {
    try std.testing.expect(!recipient_peer.outbound_provides.contains(vine_id));
    try std.testing.expect(recipient_peer.caps.getThirdPartyHosted(vine_id) == null);
}

pub fn expectNoCrossPeerProxyLinks(peer: *const Peer) !void {
    try std.testing.expectEqual(@as(usize, 0), peer.cross_peer_proxy_links.items.len);
}
