//! QuicVatNetwork seam tests: provision-ticket codec, pool redemption, and
//! the byte-identity invariant (`to_await` == Accept completion) that VatC's
//! provide table relies on. Transport-free — the QUIC three-vat e2e lives in
//! tests/rpc/transport/quic/rpc_quic_peer_test.zig.

const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const vat_network = capnpc.rpc.vat.network;
const quic_network = capnpc.rpc.vat.quic_network;
const Peer = capnpc.rpc.peer.Peer;

const QuicNet = quic_network.QuicVatNetwork(Peer);
const AddrHint = quic_network.AddrHint;

const test_hints = [_]AddrHint{
    AddrHint.ip4(.{ 127, 0, 0, 1 }, 4433),
    AddrHint.ip6(.{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 }, 4434),
};

test "quic vat network ticket codec round-trips and is deterministic" {
    const allocator = std.testing.allocator;

    const dcid = [_]u8{ 0xd0, 0xd1, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6, 0xd7 };
    const nonce: [quic_network.nonce_len]u8 = @splat(0xa0);
    const ticket_a = try quic_network.encodeTicket(allocator, &dcid, &nonce, "vat-c", &test_hints);
    defer allocator.free(ticket_a);
    const ticket_b = try quic_network.encodeTicket(allocator, &dcid, &nonce, "vat-c", &test_hints);
    defer allocator.free(ticket_b);
    try std.testing.expectEqualSlices(u8, ticket_a, ticket_b);

    var msg = try message.Message.init(allocator, ticket_a, .{});
    defer msg.deinit();
    const view = try quic_network.decodeTicket(try msg.getRootAnyPointer());
    try std.testing.expectEqual(quic_network.ticket_version, view.version);
    try std.testing.expectEqualSlices(u8, &dcid, view.dcid);
    try std.testing.expectEqualSlices(u8, &nonce, view.nonce);
    try std.testing.expectEqualStrings("vat-c", view.vat_key);
    try std.testing.expectEqual(@as(usize, 0), view.reset_token.len);

    var hints_buf: [quic_network.max_addr_hints]AddrHint = undefined;
    const hints = try view.parseHints(&hints_buf);
    try std.testing.expectEqual(@as(usize, 2), hints.len);
    try std.testing.expectEqualSlices(u8, test_hints[0].slice(), hints[0].slice());
    try std.testing.expectEqual(@as(u16, 4433), hints[0].port);
    try std.testing.expectEqualSlices(u8, test_hints[1].slice(), hints[1].slice());
    try std.testing.expectEqual(@as(u16, 4434), hints[1].port);

    const await_a = try quic_network.encodeAwaitToken(allocator, &nonce, &dcid);
    defer allocator.free(await_a);
    const await_b = try quic_network.encodeAwaitToken(allocator, &nonce, &dcid);
    defer allocator.free(await_b);
    try std.testing.expectEqualSlices(u8, await_a, await_b);
}

test "quic vat network encodeTicket validates its inputs" {
    const allocator = std.testing.allocator;
    const nonce: [quic_network.nonce_len]u8 = @splat(1);
    const dcid: [8]u8 = @splat(2);

    try std.testing.expectError(
        error.InvalidDcid,
        quic_network.encodeTicket(allocator, dcid[0..7], &nonce, "vat-c", &.{}),
    );
    try std.testing.expectError(
        error.InvalidNonce,
        quic_network.encodeTicket(allocator, &dcid, nonce[0..15], "vat-c", &.{}),
    );
    try std.testing.expectError(
        error.InvalidVatKey,
        quic_network.encodeTicket(allocator, &dcid, &nonce, "", &.{}),
    );
}

test "quic vat network decodeTicket rejects malformed and foreign tokens" {
    const allocator = std.testing.allocator;

    // A loopback nonce token (Data-rooted) must fail cleanly, not crash.
    const loopback_token = try vat_network.encodeNonceToken(allocator, "some-nonce");
    defer allocator.free(loopback_token);
    var loopback_msg = try message.Message.init(allocator, loopback_token, .{});
    defer loopback_msg.deinit();
    try std.testing.expectError(
        error.MalformedTicket,
        quic_network.decodeTicket(try loopback_msg.getRootAnyPointer()),
    );

    // A future ticket version is refused, never misread.
    const bad_version = blk: {
        var builder = message.MessageBuilder.init(allocator);
        defer builder.deinit();
        const root = try builder.initRootAnyPointer();
        const ticket = try root.initStruct(1, 5);
        ticket.writeU16(0, quic_network.ticket_version + 1);
        const bytes = try builder.toBytes();
        break :blk @constCast(bytes);
    };
    defer allocator.free(bad_version);
    var bad_version_msg = try message.Message.init(allocator, bad_version, .{});
    defer bad_version_msg.deinit();
    try std.testing.expectError(
        error.UnsupportedTicketVersion,
        quic_network.decodeTicket(try bad_version_msg.getRootAnyPointer()),
    );

    // Right version, out-of-bounds fields: short DCID, then garbage hints.
    const nonce: [quic_network.nonce_len]u8 = @splat(3);
    const cases = [_]struct { dcid_len: usize, hints: []const u8 }{
        .{ .dcid_len = 7, .hints = "" }, // DCID under the RFC 9000 floor
        .{ .dcid_len = 8, .hints = "\x05\x00\x01ab" }, // hint family neither 4 nor 16
        .{ .dcid_len = 8, .hints = "\x04\x00\x01ab" }, // hint truncated mid-address
    };
    for (cases) |case| {
        const dcid_bytes: [8]u8 = @splat(4);
        const bad = blk: {
            var builder = message.MessageBuilder.init(allocator);
            defer builder.deinit();
            const root = try builder.initRootAnyPointer();
            const ticket = try root.initStruct(1, 5);
            ticket.writeU16(0, quic_network.ticket_version);
            try ticket.writeData(0, dcid_bytes[0..case.dcid_len]);
            try ticket.writeData(1, &nonce);
            try ticket.writeData(2, "vat-c");
            try ticket.writeData(3, &.{});
            try ticket.writeData(4, case.hints);
            const bytes = try builder.toBytes();
            break :blk @constCast(bytes);
        };
        defer allocator.free(bad);
        var bad_msg = try message.Message.init(allocator, bad, .{});
        defer bad_msg.deinit();
        try std.testing.expectError(
            error.MalformedTicket,
            quic_network.decodeTicket(try bad_msg.getRootAnyPointer()),
        );
    }
}

test "quic vat network mint->connect resolves the pool and upholds byte identity" {
    const allocator = std.testing.allocator;

    var third_vat_peer = Peer.initDetached(allocator);
    defer third_vat_peer.deinit();
    var host_peer = Peer.initDetached(allocator);
    defer host_peer.deinit();

    var net = try QuicNet.init(allocator, .{ .seed = @splat(7) });
    defer net.deinit();
    try net.addVat("vat-c", &test_hints);
    try net.registerPeer("vat-c", &third_vat_peer);

    const network = net.network();
    var introduction = try network.mintIntroduction(&host_peer, "vat-c");
    defer introduction.deinit(allocator);

    // The ticket names the vat and carries a full-length CSPRNG mint.
    var contact_msg = try message.Message.init(allocator, introduction.to_contact, .{});
    defer contact_msg.deinit();
    const contact = try contact_msg.getRootAnyPointer();
    const view = try quic_network.decodeTicket(contact);
    try std.testing.expectEqual(quic_network.minted_dcid_len, view.dcid.len);
    try std.testing.expectEqual(quic_network.nonce_len, view.nonce.len);
    try std.testing.expectEqualStrings("vat-c", view.vat_key);
    try std.testing.expectEqualSlices(u8, introduction.nonce, view.nonce);

    var introduced = try network.connectToIntroduced(contact);
    defer introduced.deinit(allocator);
    try std.testing.expectEqual(&third_vat_peer, introduced.peer);
    // The invariant VatC's provide table depends on.
    try std.testing.expectEqualSlices(u8, introduction.to_await, introduced.completion);
}

test "quic vat network mint and connect error paths" {
    const allocator = std.testing.allocator;

    // No entropy decision fails closed, never a guessable default.
    try std.testing.expectError(error.EntropyUnavailable, QuicNet.init(allocator, .{}));

    var third_vat_peer = Peer.initDetached(allocator);
    defer third_vat_peer.deinit();
    var host_peer = Peer.initDetached(allocator);
    defer host_peer.deinit();

    var net = try QuicNet.init(allocator, .{ .seed = @splat(7) });
    defer net.deinit();

    // Minting to an undeclared vat fails.
    try std.testing.expectError(
        error.UnknownVat,
        net.network().mintIntroduction(&host_peer, "vat-c"),
    );

    try net.addVat("vat-c", &test_hints);
    try std.testing.expectError(error.DuplicateVat, net.addVat("vat-c", &test_hints));

    // A ticket for a vat with no pooled peer redeems to NoPathToVat.
    var introduction = try net.network().mintIntroduction(&host_peer, "vat-c");
    defer introduction.deinit(allocator);
    var contact_msg = try message.Message.init(allocator, introduction.to_contact, .{});
    defer contact_msg.deinit();
    const contact = try contact_msg.getRootAnyPointer();
    try std.testing.expectError(error.NoPathToVat, net.network().connectToIntroduced(contact));

    try net.registerPeer("vat-c", &third_vat_peer);
    try std.testing.expectError(error.DuplicatePeer, net.registerPeer("vat-c", &third_vat_peer));

    var introduced = try net.network().connectToIntroduced(contact);
    introduced.deinit(allocator);

    // Unregistering stops new redemptions.
    try std.testing.expect(net.unregisterPeer("vat-c"));
    try std.testing.expect(!net.unregisterPeer("vat-c"));
    try std.testing.expectError(error.NoPathToVat, net.network().connectToIntroduced(contact));

    // The loopback network fails cleanly on our struct-rooted ticket.
    var loopback = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer loopback.deinit();
    const loopback_result = loopback.network().connectToIntroduced(contact);
    try std.testing.expect(std.meta.isError(loopback_result));
}

fn quicVatNetworkIntroductionOomImpl(allocator: std.mem.Allocator) !void {
    var third_vat_peer = Peer.initDetached(allocator);
    defer third_vat_peer.deinit();
    var host_peer = Peer.initDetached(allocator);
    defer host_peer.deinit();

    var net = try QuicNet.init(allocator, .{ .seed = @splat(7) });
    defer net.deinit();
    try net.addVat("vat-c", &test_hints);
    try net.registerPeer("vat-c", &third_vat_peer);

    const network = net.network();
    var introduction = try network.mintIntroduction(&host_peer, "vat-c");
    defer introduction.deinit(allocator);

    var contact_msg = try message.Message.init(allocator, introduction.to_contact, .{});
    defer contact_msg.deinit();
    const contact = try contact_msg.getRootAnyPointer();

    var introduced = try network.connectToIntroduced(contact);
    defer introduced.deinit(allocator);
    try std.testing.expectEqual(&third_vat_peer, introduced.peer);
    try std.testing.expectEqualSlices(u8, introduction.to_await, introduced.completion);
}

test "quic vat network introduction path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        quicVatNetworkIntroductionOomImpl,
        .{},
    );
}
