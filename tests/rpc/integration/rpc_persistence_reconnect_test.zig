const std = @import("std");
const capnpc = @import("capnpc-zig");

const cap_table = capnpc.rpc.caps.table;
const HostPeer = capnpc.rpc.integration.host_peer.HostPeer;
const peer_impl = capnpc.rpc.peer;
const persistence = peer_impl.persistence;
const Peer = peer_impl.Peer;
const protocol = capnpc.rpc.wire.protocol;
const rpc_time = capnpc.rpc.time;

fn pumpAll(src: *HostPeer, dst: *HostPeer) !void {
    while (src.popOutgoingFrame()) |frame| {
        errdefer src.freeFrame(frame);
        try dst.pushFrame(frame);
        src.freeFrame(frame);
    }
}

fn pumpBothWays(a: *HostPeer, b: *HostPeer) !void {
    var rounds: usize = 0;
    while (a.pendingOutgoingCount() > 0 or b.pendingOutgoingCount() > 0) {
        try pumpAll(a, b);
        try pumpAll(b, a);
        rounds += 1;
        if (rounds > 16) return error.PumpDidNotSettle;
    }
}

/// The persistent capability: a service that survives connections. Answers
/// `echo_interface_id` method 0 with an empty struct.
const EchoService = struct {
    calls_handled: usize = 0,

    const echo_interface_id: u64 = 0xec40_1122_3344_5566;

    fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
        _ = caps;
        const self: *EchoService = @ptrCast(@alignCast(ctx));
        if (call.interface_id != echo_interface_id or call.method_id != 0) {
            return peer.sendReturnException(call.question_id, "unknown method");
        }
        self.calls_handled += 1;
        try peer.sendReturnEmptyStruct(call.question_id);
    }
};

/// Application state that outlives any single connection: the bootstrap
/// directory hands out the echo capability, produces sturdy refs for it,
/// and rehydrates it on a fresh connection.
const Directory = struct {
    service: *EchoService,

    const get_interface_id: u64 = 0xd1f0_0000_0000_0001;
    const sturdy_ref = "sturdy:echo/1";

    fn exportEcho(self: *Directory, peer: *Peer) !u32 {
        const export_id = try peer.addExport(.{ .ctx = self.service, .on_call = EchoService.onCall });
        try peer.setPersistentExport(export_id, self, onSave);
        return export_id;
    }

    fn onBootstrapCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
        _ = caps;
        const self: *Directory = @ptrCast(@alignCast(ctx));
        if (call.interface_id != get_interface_id or call.method_id != 0) {
            return peer.sendReturnException(call.question_id, "unknown method");
        }
        const export_id = try self.exportEcho(peer);
        var build_ctx = persistence.CapabilityReturnContext{ .cap_id = export_id };
        try peer.sendReturnResults(call.question_id, &build_ctx, persistence.buildCapabilityReturn);
    }

    fn onSave(
        ctx: *anyopaque,
        peer: *Peer,
        export_id: u32,
        seal_for: ?capnpc.message.AnyPointerReader,
    ) anyerror![]u8 {
        _ = ctx;
        _ = export_id;
        _ = seal_for;
        return peer.allocator.dupe(u8, sturdy_ref);
    }

    fn onRestore(ctx: *anyopaque, peer: *Peer, ref: []const u8) anyerror!peer_impl.RestoreOutcome {
        const self: *Directory = @ptrCast(@alignCast(ctx));
        if (!std.mem.eql(u8, ref, sturdy_ref)) return .unknown;
        // Re-export, re-marked persistent so the restored cap can be saved
        // again on this connection.
        return .{ .existing = try self.exportEcho(peer) };
    }

    fn serveOn(self: *Directory, peer: *Peer) !void {
        _ = try peer.setBootstrap(.{ .ctx = self, .on_call = onBootstrapCall });
        try peer.setRestorer(self, onRestore);
    }
};

const BootstrapRecorder = struct {
    imported_id: ?u32 = null,

    fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (ret.tag != .results) return error.TestUnexpectedResult;
        const payload = ret.results orelse return error.TestUnexpectedResult;
        const cap = try payload.content.getCapability();
        var mutable_caps = caps.*;
        try mutable_caps.retainCapability(cap);
        switch (try caps.resolveCapability(cap)) {
            .imported => |imported| self.imported_id = imported.id,
            else => return error.TestUnexpectedResult,
        }
    }
};

/// Records the capability returned by Directory.get() (results struct with
/// the cap at pointer 0).
const GetEchoRecorder = struct {
    imported_id: ?u32 = null,

    fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (ret.tag != .results) return error.TestUnexpectedResult;
        const payload = ret.results orelse return error.TestUnexpectedResult;
        const results_struct = try payload.content.getStruct();
        const field = try results_struct.readAnyPointer(0);
        const cap = try field.getCapability();
        var mutable_caps = caps.*;
        try mutable_caps.retainCapability(cap);
        switch (try caps.resolveCapability(cap)) {
            .imported => |imported| self.imported_id = imported.id,
            else => return error.TestUnexpectedResult,
        }
    }
};

const SaveRecorder = struct {
    allocator: std.mem.Allocator,
    sturdy_ref: ?[]u8 = null,

    fn onResponse(ctx: *anyopaque, _: *Peer, response: peer_impl.SaveResponse) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        switch (response) {
            .sturdy_ref => |bytes| self.sturdy_ref = try self.allocator.dupe(u8, bytes),
            else => return error.TestUnexpectedResult,
        }
    }

    fn deinit(self: *@This()) void {
        if (self.sturdy_ref) |bytes| self.allocator.free(bytes);
    }
};

const RestoreRecorder = struct {
    imported_id: ?u32 = null,

    fn onResponse(ctx: *anyopaque, _: *Peer, response: peer_impl.RestoreResponse) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        switch (response) {
            .cap => |resolved| switch (resolved) {
                .imported => |imported| self.imported_id = imported.id,
                else => return error.TestUnexpectedResult,
            },
            else => return error.TestUnexpectedResult,
        }
    }
};

const EchoCallRecorder = struct {
    ok_returns: usize = 0,
    exception_count: usize = 0,
    saw_deadline_reason: bool = false,

    fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        _ = caps;
        const self: *@This() = @ptrCast(@alignCast(ctx));
        switch (ret.tag) {
            .results => self.ok_returns += 1,
            .exception => {
                self.exception_count += 1;
                if (ret.exception) |exception| {
                    if (std.mem.eql(u8, exception.reason, "deadline exceeded")) {
                        self.saw_deadline_reason = true;
                    }
                }
            },
            else => return error.TestUnexpectedResult,
        }
    }
};

fn buildEmptyParams(ctx: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
    _ = ctx;
    var payload = try call.payloadTyped();
    const any = try payload.initContent();
    _ = try any.initStruct(0, 0);
    _ = try call.initCapTableTyped(0);
}

test "save, drop mid-flight, reconnect, restore, resume calling" {
    const allocator = std.testing.allocator;

    // Application state that outlives both connections.
    var service = EchoService{};
    var directory = Directory{ .service = &service };
    var saved = SaveRecorder{ .allocator = allocator };
    defer saved.deinit();

    // ---- Session 1: bootstrap, fetch the echo cap, save a sturdy ref,
    // then lose the connection with a call in flight. ----
    {
        var clock = rpc_time.TestClock{};

        var server = HostPeer.init(allocator);
        defer server.deinit();
        server.start(null, null);
        try directory.serveOn(&server.peer);

        var client = HostPeer.init(allocator);
        defer client.deinit();
        client.start(null, null);
        client.peer.setClock(clock.clock());
        client.peer.setTimeouts(.{ .default_call_timeout_ms = 100 });

        var bootstrap_recorder = BootstrapRecorder{};
        _ = try client.peer.sendBootstrap(&bootstrap_recorder, BootstrapRecorder.onReturn);
        try pumpBothWays(&client, &server);
        const directory_id = bootstrap_recorder.imported_id orelse return error.TestUnexpectedResult;

        var get_recorder = GetEchoRecorder{};
        _ = try client.peer.sendCall(
            directory_id,
            Directory.get_interface_id,
            0,
            &get_recorder,
            buildEmptyParams,
            GetEchoRecorder.onReturn,
        );
        try pumpBothWays(&client, &server);
        const echo_id = get_recorder.imported_id orelse return error.TestUnexpectedResult;

        _ = try client.peer.sendSave(echo_id, &saved, SaveRecorder.onResponse);
        try pumpBothWays(&client, &server);
        try std.testing.expectEqualStrings(Directory.sturdy_ref, saved.sturdy_ref orelse return error.TestUnexpectedResult);
        try std.testing.expectEqual(@as(u64, 1), server.peer.stats().saves_served);

        // The capability works on this connection.
        var echo_recorder = EchoCallRecorder{};
        _ = try client.peer.sendCall(echo_id, EchoService.echo_interface_id, 0, &echo_recorder, buildEmptyParams, EchoCallRecorder.onReturn);
        try pumpBothWays(&client, &server);
        try std.testing.expectEqual(@as(usize, 1), echo_recorder.ok_returns);
        try std.testing.expectEqual(@as(usize, 1), service.calls_handled);

        // Connection drops with a call in flight: the frame is never pumped
        // and the deadline machinery delivers the exception locally.
        _ = try client.peer.sendCall(echo_id, EchoService.echo_interface_id, 0, &echo_recorder, buildEmptyParams, EchoCallRecorder.onReturn);
        clock.advanceMs(101);
        try std.testing.expectEqual(@as(usize, 1), client.peer.checkDeadlines());
        try std.testing.expectEqual(@as(usize, 1), echo_recorder.exception_count);
        try std.testing.expect(echo_recorder.saw_deadline_reason);
    }

    // ---- Session 2: fresh peers, same application state. Restore the
    // sturdy ref and resume calling. ----
    {
        var server = HostPeer.init(allocator);
        defer server.deinit();
        server.start(null, null);
        try directory.serveOn(&server.peer);

        var client = HostPeer.init(allocator);
        defer client.deinit();
        client.start(null, null);

        var bootstrap_recorder = BootstrapRecorder{};
        _ = try client.peer.sendBootstrap(&bootstrap_recorder, BootstrapRecorder.onReturn);
        try pumpBothWays(&client, &server);
        const directory_id = bootstrap_recorder.imported_id orelse return error.TestUnexpectedResult;

        const sturdy_ref = saved.sturdy_ref orelse return error.TestUnexpectedResult;
        var restore_recorder = RestoreRecorder{};
        _ = try client.peer.sendRestore(directory_id, sturdy_ref, &restore_recorder, RestoreRecorder.onResponse);
        try pumpBothWays(&client, &server);
        const echo_id = restore_recorder.imported_id orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u64, 1), server.peer.stats().restores_served);

        // The restored capability answers calls again.
        var echo_recorder = EchoCallRecorder{};
        _ = try client.peer.sendCall(echo_id, EchoService.echo_interface_id, 0, &echo_recorder, buildEmptyParams, EchoCallRecorder.onReturn);
        try pumpBothWays(&client, &server);
        try std.testing.expectEqual(@as(usize, 1), echo_recorder.ok_returns);
        try std.testing.expectEqual(@as(usize, 2), service.calls_handled);

        // And it is persistent again: saving it yields the same sturdy ref.
        var resaved = SaveRecorder{ .allocator = allocator };
        defer resaved.deinit();
        _ = try client.peer.sendSave(echo_id, &resaved, SaveRecorder.onResponse);
        try pumpBothWays(&client, &server);
        try std.testing.expectEqualStrings(Directory.sturdy_ref, resaved.sturdy_ref orelse return error.TestUnexpectedResult);
    }
}

test "restore against a peer without a restorer yields an exception" {
    const allocator = std.testing.allocator;

    var service = EchoService{};
    var directory = Directory{ .service = &service };

    var server = HostPeer.init(allocator);
    defer server.deinit();
    server.start(null, null);
    // Bootstrap only — no restorer installed.
    _ = try server.peer.setBootstrap(.{ .ctx = &directory, .on_call = Directory.onBootstrapCall });

    var client = HostPeer.init(allocator);
    defer client.deinit();
    client.start(null, null);

    var bootstrap_recorder = BootstrapRecorder{};
    _ = try client.peer.sendBootstrap(&bootstrap_recorder, BootstrapRecorder.onReturn);
    try pumpBothWays(&client, &server);
    const directory_id = bootstrap_recorder.imported_id orelse return error.TestUnexpectedResult;

    const ExceptionRecorder = struct {
        exception_count: usize = 0,

        fn onResponse(ctx: *anyopaque, _: *Peer, response: peer_impl.RestoreResponse) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            switch (response) {
                .exception => self.exception_count += 1,
                else => return error.TestUnexpectedResult,
            }
        }
    };
    var restore_recorder = ExceptionRecorder{};
    _ = try client.peer.sendRestore(directory_id, "sturdy:echo/1", &restore_recorder, ExceptionRecorder.onResponse);
    try pumpBothWays(&client, &server);
    try std.testing.expectEqual(@as(usize, 1), restore_recorder.exception_count);
}
