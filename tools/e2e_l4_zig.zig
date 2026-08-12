const std = @import("std");
const capnpc = @import("capnpc-zig");
const io_backend_options = @import("io_backend_options");

const rpc = capnpc.rpc;
const message = capnpc.message;
const protocol = rpc.wire.protocol;
const cap_table = rpc.caps.table;
const tcp = rpc.transport.tcp;
const vat_join = rpc.vat.join;
const Peer = rpc.peer.Peer;
const ClientSession = tcp.ClientSession;
const ServerSession = tcp.ServerSession;

const number_interface_id: u64 = 0x4c34_4a4f_494e_0001;
const get_number_method_id: u16 = 0;
const joined_value: u32 = 0x4c4a_0004;

fn sleepMs(io: std.Io, ms: u64) void {
    const duration: std.Io.Clock.Duration = .{
        .raw = .{ .nanoseconds = @as(i96, @intCast(ms)) * std.time.ns_per_ms },
        .clock = .awake,
    };
    duration.sleep(io) catch {};
}

const Tap = struct {
    test_num: usize = 0,
    failures: usize = 0,

    fn ok(self: *Tap, pass: bool, desc: []const u8) void {
        self.test_num += 1;
        if (pass) {
            std.debug.print("ok {d} - {s}\n", .{ self.test_num, desc });
        } else {
            std.debug.print("not ok {d} - {s}\n", .{ self.test_num, desc });
            self.failures += 1;
        }
    }
};

const NumberService = struct {
    value: u32,
    calls: u32 = 0,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (call.interface_id != number_interface_id or call.method_id != get_number_method_id) {
            return error.UnexpectedMethod;
        }
        self.calls += 1;

        const BuildCtx = struct {
            n: u32,

            fn build(build_ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const build_ctx: *const @This() = @ptrCast(@alignCast(build_ctx_ptr));
                var payload = try ret.payloadTyped();
                const any = try payload.initContent();
                const results = try any.initStruct(1, 0);
                results.writeU32(0, build_ctx.n);
            }
        };
        var build_ctx = BuildCtx{ .n = self.value };
        try peer.sendReturnResults(call.question_id, &build_ctx, BuildCtx.build);
    }
};

const ServerCtx = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    listener: *tcp.Listener,
    join_network: rpc.peer.JoinNetwork,
    number: NumberService = .{ .value = joined_value },
    session: ?*ServerSession = null,
    accepted: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    ready: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn main(self: *@This()) void {
        var session = ServerSession.accept(self.allocator, self.listener, .{
            // Deliberately tiny deterministic lease/quota for the attacker
            // phase. The legitimate two-part completion still fits exactly
            // after expiry reclaims the attacker's partial bucket.
            .conn = .{ .tick_interval_ms = 10 },
            .join_timeout_ms = 50,
            .limits = .{
                .max_join_parts_per_join = 3,
                .max_pending_join_records = 4,
            },
        }) catch {
            self.failed.store(true, .release);
            self.accepted.store(true, .release);
            self.done.store(true, .release);
            return;
        };
        self.session = session;
        self.accepted.store(true, .release);

        while (!self.ready.load(.acquire)) sleepMs(self.io, 1);
        if (self.failed.load(.acquire)) {
            session.deinit();
            self.done.store(true, .release);
            return;
        }

        _ = session.peer.setBootstrap(.{ .ctx = &self.number, .on_call = NumberService.onCall }) catch {
            self.failed.store(true, .release);
            session.deinit();
            self.done.store(true, .release);
            return;
        };
        session.peer.attachJoinNetwork(self.join_network) catch {
            self.failed.store(true, .release);
            session.deinit();
            self.done.store(true, .release);
            return;
        };
        session.run();
        session.deinit();
        self.done.store(true, .release);
    }

    fn waitAccepted(self: *@This()) !*ServerSession {
        var spins: usize = 0;
        while (!self.accepted.load(.acquire)) : (spins += 1) {
            if (spins > 10_000) return error.ServerAcceptTimedOut;
            sleepMs(self.io, 1);
        }
        if (self.failed.load(.acquire)) return error.ServerAcceptFailed;
        return self.session orelse error.MissingServerSession;
    }
};

const ClientApp = struct {
    allocator: std.mem.Allocator,
    session: *ClientSession,
    join_network: rpc.peer.JoinNetwork,
    bootstrap_import_id: ?u32 = null,
    accept_import_id: ?u32 = null,
    join_id: u32 = 0x4c4a_1001,
    attacker_join_id: u32 = 0x4c4a_10a0,
    quota_join_id: u32 = 0x4c4a_10a1,
    attacker_timeouts: u32 = 0,
    quota_rejected: bool = false,
    legitimate_started: bool = false,
    joined: std.ArrayList(vat_join.Joined(Peer)) = .empty,
    number_result: ?u32 = null,
    failed: bool = false,
    failure: ?anyerror = null,

    fn deinit(self: *@This()) void {
        for (self.joined.items) |*joined| joined.deinit(self.allocator);
        self.joined.deinit(self.allocator);
    }

    fn fail(self: *@This(), err: anyerror) anyerror {
        self.failed = true;
        self.failure = err;
        self.session.close();
        return err;
    }

    fn sendJoinPartFor(
        self: *@This(),
        join_id: u32,
        part_count: u16,
        part_num: u16,
        callback: rpc.peer.QuestionCallback,
    ) !void {
        const target = self.bootstrap_import_id orelse return error.MissingBootstrapImport;
        const bytes = try vat_join.encodeJoinKeyPart(self.allocator, join_id, part_count, part_num);
        defer self.allocator.free(bytes);
        var key_msg = try message.Message.init(self.allocator, bytes, .{});
        defer key_msg.deinit();
        _ = try self.session.peer.sendJoinExperimental(
            .{ .tag = .importedCap, .imported_cap = target, .promised_answer = null },
            try key_msg.getRootAnyPointer(),
            self,
            callback,
        );
    }

    fn sendJoinPart(self: *@This(), part_count: u16, part_num: u16) !void {
        try self.sendJoinPartFor(self.join_id, part_count, part_num, ClientApp.onJoinReturn);
    }

    fn maybeStartLegitimateJoin(self: *@This()) !void {
        if (self.legitimate_started or self.attacker_timeouts != 2 or !self.quota_rejected) return;
        self.legitimate_started = true;
        try self.sendJoinPart(2, 0);
        try self.sendJoinPart(2, 1);
    }

    fn maybeAccept(self: *@This()) !void {
        if (self.accept_import_id != null) return;
        if (self.joined.items.len != 2) return;

        const first = &self.joined.items[0];
        const second = &self.joined.items[1];
        if (first.peer != second.peer or !std.mem.eql(u8, first.provision, second.provision)) {
            return self.fail(error.JoinResultMismatch);
        }

        var provision_msg = try message.Message.initUnvalidated(self.allocator, first.provision);
        defer provision_msg.deinit();
        _ = try first.peer.sendAccept(
            try provision_msg.getRootAnyPointer(),
            null,
            self,
            ClientApp.onAcceptReturn,
        );
    }

    fn onBootstrap(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return self.fail(error.UnexpectedBootstrapReturn);
        const payload = ret.results orelse return self.fail(error.MissingBootstrapPayload);
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.bootstrap_import_id = switch (resolved) {
            .imported => |imported| imported.id,
            else => return self.fail(error.ExpectedImportedCap),
        };

        _ = peer;
        // Three aggregate records (bucket + two parts) occupy the tiny quota
        // without completing. A sibling bucket is refused, then the short TTL
        // expires both attacker answers and the legitimate handoff proceeds.
        try self.sendJoinPartFor(self.attacker_join_id, 3, 0, ClientApp.onAttackerReturn);
        try self.sendJoinPartFor(self.attacker_join_id, 3, 1, ClientApp.onAttackerReturn);
        try self.sendJoinPartFor(self.quota_join_id, 2, 0, ClientApp.onQuotaReturn);
    }

    fn onAttackerReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .exception or
            ret.exception == null or
            !std.mem.eql(u8, ret.exception.?.reason, "join unavailable"))
        {
            return self.fail(error.UnexpectedAttackerJoinReturn);
        }
        self.attacker_timeouts += 1;
        try self.maybeStartLegitimateJoin();
    }

    fn onQuotaReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .exception or
            ret.exception == null or
            !std.mem.eql(u8, ret.exception.?.reason, "join unavailable"))
        {
            return self.fail(error.UnexpectedQuotaJoinReturn);
        }
        self.quota_rejected = true;
        try self.maybeStartLegitimateJoin();
    }

    fn onJoinReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return self.fail(error.UnexpectedJoinReturn);
        const payload = ret.results orelse return self.fail(error.MissingJoinPayload);
        const decoded = try vat_join.decodeJoinResult(payload.content);
        if (!decoded.succeeded or decoded.join_id != self.join_id) {
            return self.fail(error.JoinResultMismatch);
        }
        const joined = try self.join_network.connectJoined(self.allocator, payload.content);
        errdefer {
            var rollback = joined;
            rollback.deinit(self.allocator);
        }
        try self.joined.append(self.allocator, joined);
        try self.maybeAccept();
    }

    fn onAcceptReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return self.fail(error.UnexpectedAcceptReturn);
        const payload = ret.results orelse return self.fail(error.MissingAcceptPayload);
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.accept_import_id = switch (resolved) {
            .imported => |imported| imported.id,
            else => return self.fail(error.ExpectedImportedCap),
        };

        _ = try peer.sendCall(
            self.accept_import_id.?,
            number_interface_id,
            get_number_method_id,
            self,
            null,
            ClientApp.onNumberReturn,
        );
    }

    fn onNumberReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return self.fail(error.UnexpectedNumberReturn);
        const payload = ret.results orelse return self.fail(error.MissingNumberPayload);
        const content = try payload.content.getStruct();
        self.number_result = content.readU32(0);

        if (self.accept_import_id) |import_id| {
            try peer.releaseImport(import_id, 1);
            self.accept_import_id = null;
        }
        if (self.bootstrap_import_id) |import_id| {
            try peer.releaseImport(import_id, 1);
            self.bootstrap_import_id = null;
        }
        self.session.close();
    }
};

fn runScenario(allocator: std.mem.Allocator, io: std.Io, tap: *Tap) !void {
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    const listen = try tcp.createListenSocket(io, address, 1, false);
    var listener = tcp.Listener.initFd(allocator, io, .{ .handle = listen.socket.handle }, .{});
    defer listener.close();

    var join_net = vat_join.AddressedJoinNetwork(Peer).init(allocator);
    defer join_net.deinit();

    var server_ctx = ServerCtx{
        .allocator = allocator,
        .io = io,
        .listener = &listener,
        .join_network = join_net.network(),
    };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.main, .{&server_ctx});
    var server_thread_joined = false;
    defer if (!server_thread_joined) {
        server_ctx.failed.store(true, .release);
        server_ctx.ready.store(true, .release);
        if (server_ctx.session) |server_session| server_session.requestStop();
        listener.close();
        server_thread.join();
    };

    const session = try ClientSession.connect(allocator, io, listen.socket.address, .{});
    defer session.deinit();

    const server_session = try server_ctx.waitAccepted();
    try join_net.registerDirectPeer(
        &server_session.peer,
        &session.peer,
        "tcp://127.0.0.1:l4-e2e",
    );
    server_ctx.ready.store(true, .release);

    var app = ClientApp{
        .allocator = allocator,
        .session = session,
        .join_network = join_net.network(),
    };
    defer app.deinit();

    _ = try session.peer.sendBootstrap(&app, ClientApp.onBootstrap);
    session.run();
    server_thread.join();
    server_thread_joined = true;

    tap.ok(!app.failed, "L4 Zig TCP scenario completed without callback failure");
    tap.ok(app.quota_rejected, "attacker exhausted the small aggregate Join quota");
    tap.ok(app.attacker_timeouts == 2, "short TTL expired both attacker Join parts");
    tap.ok(app.legitimate_started, "quota recovery admitted the legitimate Join");
    tap.ok(app.joined.items.len == 2, "client consumed two JoinResult payloads");
    tap.ok(app.number_result == joined_value, "accepted Join cap returned the hosted number");
    tap.ok(server_ctx.number.calls == 1, "joined capability invocation reached the TCP server exactly once");
    tap.ok(join_net.registry.count() == 0, "addressed Join provision registry drained");
    tap.ok(server_ctx.done.load(.acquire), "server session unwound after client close");
}

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    const backend_kind = capnpc.io_backend.parseKind(io_backend_options.kind) orelse {
        std.debug.print("invalid -Dio-backend selector: {s}\n", .{io_backend_options.kind});
        return error.InvalidIoBackend;
    };
    var backend = try capnpc.io_backend.Backend.init(backend_kind, init.gpa, init.io);
    defer backend.deinit();
    const io = backend.io();

    var tap = Tap{};
    runScenario(allocator, io, &tap) catch |err| {
        std.debug.print("# L4 Zig TCP scenario failed: {}\n", .{err});
        tap.ok(false, "L4 Zig TCP scenario completes without harness error");
    };

    std.debug.print("l4-zig total: {d} pass, {d} fail\n", .{ tap.test_num - tap.failures, tap.failures });
    if (tap.failures != 0) return error.L4ZigE2EFailed;
}
