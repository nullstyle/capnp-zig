const std = @import("std");
const capnpc = @import("capnpc-zig");
const io_backend_options = @import("io_backend_options");

const rpc = capnpc.rpc;
const message = capnpc.message;
const protocol = rpc.wire.protocol;
const HostPeer = rpc.integration.HostPeer;
const Peer = rpc.peer.Peer;
const InboundCapTable = rpc.caps.table.InboundCapTable;
const Framer = rpc.wire.framing.Framer;
const l3 = @import("l3_l4_interop");

const Allocator = std.mem.Allocator;

const cxx_number_value: u32 = 4242;
const introducer_interface_id: u64 = 0x6c33_6c34_696e_7472;
const get_promise_method_id: u16 = 0;
const pump_timeout_ms: i64 = 10_000;

const Scenario = enum(u16) {
    happy = 0,
    bad_contact_fallback = 1,
    malformed_completion = 2,
    rejected_completion = 3,
    reject_await = 4,
    drop_after_provide = 5,
    duplicate_accept = 6,
    number_exception = 7,

    fn name(self: Scenario) []const u8 {
        return switch (self) {
            .happy => "happy path",
            .bad_contact_fallback => "bad contact fallback",
            .malformed_completion => "malformed completion",
            .rejected_completion => "rejected completion",
            .reject_await => "rejected await",
            .drop_after_provide => "drop after Provide",
            .duplicate_accept => "duplicate Accept",
            .number_exception => "accepted cap exception",
        };
    }

    fn token(self: Scenario, ordinal: u64) u64 {
        return (@as(u64, @intFromEnum(self)) << 48) | ordinal;
    }
};

const scenarios = [_]Scenario{
    .happy,
    .bad_contact_fallback,
    .malformed_completion,
    .rejected_completion,
    .reject_await,
    .drop_after_provide,
    .duplicate_accept,
    .number_exception,
};

const CliArgs = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 4000,
};

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

fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
    return @ptrCast(@alignCast(ctx));
}

fn milliTimestamp(io: std.Io) i64 {
    return std.Io.Timestamp.now(io, .awake).toMilliseconds();
}

fn usage() void {
    std.debug.print(
        \\Usage: e2e-l3-cpp [--host 127.0.0.1] [--port 4000]
        \\
    , .{});
}

fn parseArgs(allocator: Allocator, args: std.process.Args) !CliArgs {
    var out = CliArgs{};
    var host_text: []const u8 = out.host;

    var args_iter = try std.process.Args.Iterator.initAllocator(args, allocator);
    defer args_iter.deinit();
    _ = args_iter.skip();
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            return error.HelpRequested;
        }
        if (std.mem.eql(u8, arg, "--host")) {
            host_text = args_iter.next() orelse return error.MissingArgValue;
            continue;
        }
        if (std.mem.eql(u8, arg, "--port")) {
            const port_str = args_iter.next() orelse return error.MissingArgValue;
            out.port = try std.fmt.parseInt(u16, port_str, 10);
            continue;
        }
        return error.InvalidOption;
    }

    out.host = try allocator.dupe(u8, host_text);
    return out;
}

fn encodeAwaitToken(allocator: Allocator, token: u64) ![]u8 {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try builder.initRootAnyPointer();
    const token_struct = try root.initStruct(1, 0);
    token_struct.writeU64(0, token);
    const bytes = try builder.toBytes();
    return @constCast(bytes);
}

fn encodeCompletionToken(allocator: Allocator, token: u64) ![]u8 {
    return encodeAwaitToken(allocator, token);
}

fn encodeContactToken(allocator: Allocator, token: u64, scenario: Scenario) ![]u8 {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try builder.initRootAnyPointer();
    const contact = try root.initStruct(1, 2);
    contact.writeU64(0, token);
    const host = if (scenario == .bad_contact_fallback) "not-cpp-host" else "cpp-host";
    try contact.writeText(0, host);
    try contact.writeText(1, "zig-orchestrator");
    const bytes = try builder.toBytes();
    return @constCast(bytes);
}

fn decodeContactToken(contact_any: message.AnyPointerReader) !u64 {
    const contact = try contact_any.getStruct();
    const host = try contact.readText(0);
    if (!std.mem.eql(u8, host, "cpp-host")) return error.UnexpectedContactHost;
    return contact.readU64(0);
}

const InteropVatNetwork = struct {
    const Net = rpc.peer.VatNetwork;
    const Introduced = Net.IntroducedType;

    allocator: Allocator,
    a_to_c: *Peer,
    scenario: Scenario,
    next_token: u64 = 1,
    last_token: ?u64 = null,

    fn init(allocator: Allocator, a_to_c: *Peer, scenario: Scenario) InteropVatNetwork {
        return .{
            .allocator = allocator,
            .a_to_c = a_to_c,
            .scenario = scenario,
        };
    }

    fn network(self: *InteropVatNetwork) Net {
        return Net.init(self, mintIntroduction, connectToIntroduced);
    }

    fn mintIntroduction(
        ctx: *anyopaque,
        host_peer: *Peer,
        recipient_hint: []const u8,
    ) anyerror!rpc.vat.network.Introduction {
        _ = host_peer;
        _ = recipient_hint;
        const self: *InteropVatNetwork = castCtx(*InteropVatNetwork, ctx);
        const token = self.scenario.token(self.next_token);
        self.next_token += 1;
        self.last_token = token;

        const nonce = try self.allocator.alloc(u8, @sizeOf(u64));
        errdefer self.allocator.free(nonce);
        std.mem.writeInt(u64, nonce[0..8], token, .little);

        const to_await = try encodeAwaitToken(self.allocator, token);
        errdefer self.allocator.free(to_await);
        const to_contact = try encodeContactToken(self.allocator, token, self.scenario);
        errdefer self.allocator.free(to_contact);

        return .{
            .to_await = to_await,
            .to_contact = to_contact,
            .nonce = nonce,
        };
    }

    fn connectToIntroduced(
        ctx: *anyopaque,
        contact: message.AnyPointerReader,
    ) anyerror!Introduced {
        const self: *InteropVatNetwork = castCtx(*InteropVatNetwork, ctx);
        const token = try decodeContactToken(contact);
        const completion = switch (self.scenario) {
            .malformed_completion => try encodeCompletionToken(self.allocator, token),
            .rejected_completion => try encodeCompletionToken(self.allocator, 0),
            else => try encodeCompletionToken(self.allocator, token),
        };
        return .{
            .peer = self.a_to_c,
            .completion = completion,
        };
    }
};

const TcpPeer = struct {
    allocator: Allocator,
    socket: rpc.transport.tcp.SocketFd,
    host: HostPeer,
    framer: Framer,
    closed: bool = false,

    fn connect(allocator: Allocator, io: std.Io, host_text: []const u8, port: u16) !TcpPeer {
        const address = try std.Io.net.IpAddress.parse(host_text, port);
        const stream = try std.Io.net.IpAddress.connect(&address, io, .{ .mode = .stream, .protocol = .tcp });
        const socket = rpc.transport.tcp.SocketFd{ .handle = stream.socket.handle };
        rpc.transport.tcp.runtime.setTcpNoDelay(socket);

        return .{
            .allocator = allocator,
            .socket = socket,
            .host = HostPeer.init(allocator),
            .framer = Framer.init(allocator),
        };
    }

    fn deinit(self: *TcpPeer, io: std.Io) void {
        if (!self.closed) {
            rpc.transport.tcp.closeFd(io, self.socket);
            self.closed = true;
        }
        self.framer.deinit();
        self.host.deinit();
    }

    fn flush(self: *TcpPeer, io: std.Io) !bool {
        if (self.closed) return false;
        var progressed = false;
        while (self.host.popOutgoingFrame()) |frame| {
            defer self.host.freeFrame(frame);
            try writeAll(io, self.socket, frame);
            progressed = true;
        }
        return progressed;
    }

    fn readReady(self: *TcpPeer) !bool {
        var buf: [16 * 1024]u8 = undefined;
        while (true) {
            const rc = std.posix.system.read(self.socket.handle, &buf, buf.len);
            switch (std.posix.errno(rc)) {
                .SUCCESS => {
                    if (rc == 0) {
                        self.closed = true;
                        return error.PeerDisconnected;
                    }
                    const n: usize = @intCast(rc);
                    try self.framer.push(buf[0..n]);
                    while (try self.framer.popFrame()) |frame| {
                        defer self.allocator.free(frame);
                        try self.host.pushFrame(frame);
                    }
                    return true;
                },
                .INTR => continue,
                .AGAIN => return false,
                else => return error.ReadFailed,
            }
        }
    }
};

fn writeAll(io: std.Io, socket: rpc.transport.tcp.SocketFd, bytes: []const u8) !void {
    const pattern: []const u8 = &.{};
    const data: [1][]const u8 = .{pattern};
    var offset: usize = 0;
    while (offset < bytes.len) {
        const n = io.vtable.netWrite(io.userdata, socket.handle, bytes[offset..], &data, 0) catch
            return error.WriteFailed;
        if (n == 0) return error.BrokenPipe;
        offset += n;
    }
}

const Pump = struct {
    io: std.Io,
    a_to_b: *HostPeer,
    b_to_a: *HostPeer,
    a_to_c: *TcpPeer,
    b_to_c: *TcpPeer,
    allow_a_to_c_disconnect: bool = false,
    allow_b_to_c_disconnect: bool = false,

    fn drainLocal(src: *HostPeer, dst: *HostPeer) !bool {
        var progressed = false;
        while (src.popOutgoingFrame()) |frame| {
            defer src.freeFrame(frame);
            try dst.pushFrame(frame);
            progressed = true;
        }
        return progressed;
    }

    fn drainQueued(self: *Pump) !bool {
        var progressed = false;
        var spins: usize = 0;
        while (true) {
            var round = false;
            round = try drainLocal(self.a_to_b, self.b_to_a) or round;
            round = try drainLocal(self.b_to_a, self.a_to_b) or round;
            round = try self.a_to_c.flush(self.io) or round;
            round = try self.b_to_c.flush(self.io) or round;
            if (!round) break;
            progressed = true;
            spins += 1;
            if (spins > 1024) return error.PumpSpinLimit;
        }
        return progressed;
    }

    fn step(self: *Pump, timeout_ms: i32) !bool {
        if (try self.drainQueued()) return true;

        var fds = [_]std.posix.pollfd{
            .{
                .fd = self.b_to_c.socket.handle,
                .events = if (self.b_to_c.closed) 0 else std.posix.POLL.IN,
                .revents = 0,
            },
            .{
                .fd = self.a_to_c.socket.handle,
                .events = if (self.a_to_c.closed) 0 else std.posix.POLL.IN,
                .revents = 0,
            },
        };
        if (fds[0].events == 0 and fds[1].events == 0) return false;
        while (true) {
            const rc = std.posix.system.poll(&fds, fds.len, timeout_ms);
            switch (std.posix.errno(rc)) {
                .SUCCESS => break,
                .INTR => continue,
                else => return error.PollFailed,
            }
        }

        var progressed = false;
        if (fds[0].revents != 0) {
            progressed = (self.b_to_c.readReady() catch |err| switch (err) {
                error.PeerDisconnected => if (self.allow_b_to_c_disconnect) true else return err,
                else => return err,
            }) or progressed;
        }
        if (fds[1].revents != 0) {
            progressed = (self.a_to_c.readReady() catch |err| switch (err) {
                error.PeerDisconnected => if (self.allow_a_to_c_disconnect) true else return err,
                else => return err,
            }) or progressed;
        }
        if (progressed) _ = try self.drainQueued();
        return progressed;
    }

    fn runFor(self: *Pump, duration_ms: i64) !void {
        const start = milliTimestamp(self.io);
        while (milliTimestamp(self.io) - start < duration_ms) {
            _ = try self.step(10);
        }
    }
};

const NumberBootstrapProbe = struct {
    client: ?l3.Number.Client = null,

    fn onBootstrap(ctx_ptr: *anyopaque, _: *Peer, response: l3.Number.BootstrapResponse) anyerror!void {
        const self: *NumberBootstrapProbe = castCtx(*NumberBootstrapProbe, ctx_ptr);
        self.client = try response.unwrap();
    }
};

const Introducer = struct {
    promise_export_id: ?u32 = null,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const InboundCapTable,
    ) anyerror!void {
        const self: *Introducer = castCtx(*Introducer, ctx_ptr);
        if (call.interface_id != introducer_interface_id or call.method_id != get_promise_method_id) {
            return error.UnexpectedIntroducerMethod;
        }
        const promise_id = try peer.addPromiseExport();
        self.promise_export_id = promise_id;

        const PromiseReturnCtx = struct {
            promise_id: u32,

            fn build(bctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const bself: *const @This() = castCtx(*const @This(), bctx);
                var payload = try ret.payloadTyped();
                const any = try payload.initContent();
                try any.setCapability(.{ .id = bself.promise_id });
            }
        };

        var ret_ctx = PromiseReturnCtx{ .promise_id = promise_id };
        try peer.sendReturnResults(call.question_id, &ret_ctx, PromiseReturnCtx.build);
    }
};

const IntroducerProbe = struct {
    import_id: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const InboundCapTable,
    ) anyerror!void {
        const self: *IntroducerProbe = castCtx(*IntroducerProbe, ctx_ptr);
        if (ret.tag != .results) return error.UnexpectedBootstrapReturn;
        const payload = ret.results orelse return error.MissingBootstrapPayload;
        var mutable_caps: *InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.IntroducerNotImported,
        };
    }
};

const PromiseCall = struct {
    import_id: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const InboundCapTable,
    ) anyerror!void {
        const self: *PromiseCall = castCtx(*PromiseCall, ctx_ptr);
        if (ret.tag != .results) return error.UnexpectedPromiseReturn;
        const payload = ret.results orelse return error.MissingPromisePayload;
        var mutable_caps: *InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.PromiseNotImported,
        };
    }
};

const PickupHandler = struct {
    expected_promise_id: u32,
    expected_accept_peer: *Peer,
    accepted_id: ?u32 = null,
    return_count: usize = 0,
    accepted_on_third_peer: bool = false,
    saw_exception: bool = false,

    fn onPickup(
        ctx_ptr: *anyopaque,
        _: *Peer,
        promise_id: u32,
        accept_peer: *Peer,
        ret: protocol.Return,
        accept_caps: *const InboundCapTable,
    ) anyerror!void {
        const self: *PickupHandler = castCtx(*PickupHandler, ctx_ptr);
        self.return_count += 1;
        if (promise_id != self.expected_promise_id) return error.UnexpectedPromiseId;
        self.accepted_on_third_peer = accept_peer == self.expected_accept_peer;
        if (ret.tag == .exception) {
            self.saw_exception = true;
            return;
        }
        if (ret.tag != .results) return error.UnexpectedAcceptReturn;
        const payload = ret.results orelse return error.MissingAcceptPayload;
        var mutable_caps: *InboundCapTable = @constCast(accept_caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.accepted_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.AcceptedNumberNotImported,
        };
    }
};

const NumberCall = struct {
    result: ?u32 = null,
    return_count: usize = 0,
    saw_remote_exception: bool = false,
    saw_disconnect: bool = false,
    saw_unexpected: bool = false,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        response: l3.Number.GetNumber.Response,
        _: *const InboundCapTable,
    ) anyerror!void {
        const self: *NumberCall = castCtx(*NumberCall, ctx_ptr);
        self.return_count += 1;
        const results = response.unwrap() catch |err| switch (err) {
            error.RemoteException => {
                self.saw_remote_exception = true;
                return;
            },
            error.Disconnected => {
                self.saw_disconnect = true;
                return;
            },
            else => {
                self.saw_unexpected = true;
                return;
            },
        };
        self.result = try results.getN();
    }
};

const RawAcceptCall = struct {
    return_count: usize = 0,
    accepted_id: ?u32 = null,
    saw_exception: bool = false,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const InboundCapTable,
    ) anyerror!void {
        const self: *RawAcceptCall = castCtx(*RawAcceptCall, ctx_ptr);
        self.return_count += 1;
        if (ret.tag == .exception) {
            self.saw_exception = true;
            return;
        }
        if (ret.tag != .results) return error.UnexpectedDuplicateAcceptReturn;
        const payload = ret.results orelse return error.MissingDuplicateAcceptPayload;
        var mutable_caps: *InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.accepted_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.DuplicateAcceptNotImported,
        };
    }
};

fn pumpUntilNumberClient(pump: *Pump, probe: *NumberBootstrapProbe) !void {
    const start = milliTimestamp(pump.io);
    while (probe.client == null) {
        if (milliTimestamp(pump.io) - start > pump_timeout_ms) return error.Timeout;
        _ = try pump.step(25);
    }
}

fn pumpUntilIntroducer(pump: *Pump, probe: *IntroducerProbe) !void {
    const start = milliTimestamp(pump.io);
    while (probe.import_id == null) {
        if (milliTimestamp(pump.io) - start > pump_timeout_ms) return error.Timeout;
        _ = try pump.step(25);
    }
}

fn pumpUntilPromise(pump: *Pump, call: *PromiseCall, introducer: *Introducer) !void {
    const start = milliTimestamp(pump.io);
    while (call.import_id == null or introducer.promise_export_id == null) {
        if (milliTimestamp(pump.io) - start > pump_timeout_ms) return error.Timeout;
        _ = try pump.step(25);
    }
}

fn pumpUntilPickup(pump: *Pump, pickup: *PickupHandler) !void {
    const start = milliTimestamp(pump.io);
    while (pickup.return_count == 0) {
        if (milliTimestamp(pump.io) - start > pump_timeout_ms) return error.Timeout;
        _ = try pump.step(25);
    }
}

fn pumpUntilNoOutboundProvide(pump: *Pump, peer: *Peer, vine_id: u32) !void {
    const start = milliTimestamp(pump.io);
    while (peer.outbound_provides.contains(vine_id)) {
        if (milliTimestamp(pump.io) - start > pump_timeout_ms) return error.Timeout;
        _ = try pump.step(25);
    }
}

fn pumpUntilNumberResult(pump: *Pump, call: *NumberCall) !void {
    const start = milliTimestamp(pump.io);
    while (call.return_count == 0) {
        if (milliTimestamp(pump.io) - start > pump_timeout_ms) return error.Timeout;
        _ = try pump.step(25);
    }
}

fn pumpUntilRawAccept(pump: *Pump, call: *RawAcceptCall) !void {
    const start = milliTimestamp(pump.io);
    while (call.return_count == 0) {
        if (milliTimestamp(pump.io) - start > pump_timeout_ms) return error.Timeout;
        _ = try pump.step(25);
    }
}

fn buildJoinProbe(allocator: Allocator) !u32 {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try builder.initRootAnyPointer();
    const key_struct = try root.initStruct(1, 0);
    var key = l3.JoinKeyPart.Builder.wrap(key_struct);
    try key.setJoinId(0x1020_3040);
    try key.setPartCount(2);
    try key.setPartNum(1);

    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();
    const any = try msg.getRootAnyPointer();
    const reader = l3.JoinKeyPart.Reader.wrap(try any.getStruct());
    if ((try reader.getPartCount()) != 2 or (try reader.getPartNum()) != 1) return error.InvalidJoinProbe;
    return try reader.getJoinId();
}

const JoinResultProbe = struct {
    join_id: u32,
    succeeded: bool,
    cap_id: u32,
};

fn buildJoinResultProbe(allocator: Allocator) !JoinResultProbe {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try builder.initRootAnyPointer();
    const result_struct = try root.initStruct(1, 1);
    var result = l3.JoinResult.Builder.wrap(result_struct);
    try result.setJoinId(0x5060_7080);
    try result.setSucceeded(true);
    try result.setCapCapability(.{ .id = 17 });

    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();
    const any = try msg.getRootAnyPointer();
    const reader = l3.JoinResult.Reader.wrap(try any.getStruct());
    const cap_any = try reader.getCap();
    const cap = try cap_any.getCapability();
    return .{
        .join_id = try reader.getJoinId(),
        .succeeded = try reader.getSucceeded(),
        .cap_id = cap.id,
    };
}

fn expectTransientDrain(
    tap: *Tap,
    scenario: Scenario,
    a_to_b: *HostPeer,
    b_to_a: *HostPeer,
    a_to_c: *TcpPeer,
    b_to_c: *TcpPeer,
    promise_import_id: u32,
) void {
    const promise_peer_drained =
        !a_to_b.peer.resolved_imports.contains(promise_import_id) and
        a_to_b.peer.pending_embargoes.count() == 0 and
        a_to_b.peer.pending_accepts_by_embargo.count() == 0 and
        a_to_b.peer.pending_accept_embargo_by_question.count() == 0;
    const introducer_drained =
        b_to_a.peer.outbound_provides.count() == 0 and
        b_to_a.peer.pending_third_party_awaits.count() == 0;
    const accept_peer_drained =
        a_to_c.host.peer.pending_accepts_by_embargo.count() == 0 and
        a_to_c.host.peer.pending_accept_embargo_by_question.count() == 0 and
        a_to_c.host.peer.questions.count() == 0;
    const provide_peer_drained =
        scenario == .drop_after_provide or b_to_c.host.peer.questions.count() == 0;

    tap.ok(
        promise_peer_drained and introducer_drained and accept_peer_drained and provide_peer_drained,
        "Provide/Accept/vine/embargo transient state drains",
    );
}

fn invokeDirectNumber(
    pump: *Pump,
    client: l3.Number.Client,
    expect_exception: bool,
    success_desc: []const u8,
    tap: *Tap,
) !void {
    var number_call = NumberCall{};
    _ = try client.callGetNumber(&number_call, null, NumberCall.onReturn);
    try pumpUntilNumberResult(pump, &number_call);
    if (expect_exception) {
        tap.ok(
            number_call.return_count == 1 and number_call.saw_remote_exception and number_call.result == null,
            "direct A-C Number.getNumber reports the expected C++ exception",
        );
    } else {
        tap.ok(
            number_call.return_count == 1 and number_call.result == cxx_number_value,
            success_desc,
        );
    }
}

fn sendDuplicateAccept(
    allocator: Allocator,
    pump: *Pump,
    token: u64,
    tap: *Tap,
) !void {
    const completion = try encodeCompletionToken(allocator, token);
    defer allocator.free(completion);
    var completion_msg = try message.Message.initUnvalidated(allocator, completion);
    defer completion_msg.deinit();
    const provision = try completion_msg.getRootAnyPointer();

    var duplicate = RawAcceptCall{};
    _ = try pump.a_to_c.host.peer.sendAccept(provision, null, &duplicate, RawAcceptCall.onReturn);
    try pumpUntilRawAccept(pump, &duplicate);
    if (duplicate.accepted_id) |id| {
        l3.Number.Client.init(&pump.a_to_c.host.peer, id).release();
    }
    tap.ok(
        duplicate.return_count == 1 and duplicate.saw_exception and duplicate.accepted_id == null,
        "duplicate Accept is rejected without yielding a second cap",
    );
}

fn runScenario(allocator: Allocator, io: std.Io, args: CliArgs, tap: *Tap, scenario: Scenario) !void {
    std.debug.print("# scenario: {s}\n", .{scenario.name()});

    var b_to_c = try TcpPeer.connect(allocator, io, args.host, args.port);
    b_to_c.host.start(null, null, null);
    var a_to_c = try TcpPeer.connect(allocator, io, args.host, args.port);
    a_to_c.host.start(null, null, null);

    var a_to_b = HostPeer.init(allocator);
    a_to_b.start(null, null, null);
    var b_to_a = HostPeer.init(allocator);
    defer b_to_c.deinit(io);
    defer a_to_b.deinit();
    defer b_to_a.deinit();
    defer a_to_c.deinit(io);
    b_to_a.start(null, null, null);

    var vat_network = InteropVatNetwork.init(allocator, &a_to_c.host.peer, scenario);
    b_to_a.peer.attachVatNetwork(vat_network.network());
    a_to_b.peer.attachVatNetwork(vat_network.network());

    var pump = Pump{
        .io = io,
        .a_to_b = &a_to_b,
        .b_to_a = &b_to_a,
        .a_to_c = &a_to_c,
        .b_to_c = &b_to_c,
        .allow_b_to_c_disconnect = scenario == .drop_after_provide,
    };

    var number_probe = NumberBootstrapProbe{};
    _ = try l3.Number.Client.fromBootstrap(&b_to_c.host.peer, &number_probe, NumberBootstrapProbe.onBootstrap);
    try pumpUntilNumberClient(&pump, &number_probe);
    var cxx_number = number_probe.client orelse return error.CxxNumberBootstrapFailed;
    var cxx_number_owned = true;
    defer if (cxx_number_owned) cxx_number.release();
    tap.ok(true, "B bootstraps the C++ Number capability");

    var introducer = Introducer{};
    _ = try b_to_a.peer.setBootstrap(.{ .ctx = &introducer, .on_call = Introducer.onCall });

    var introducer_probe = IntroducerProbe{};
    _ = try a_to_b.peer.sendBootstrap(&introducer_probe, IntroducerProbe.onReturn);
    try pumpUntilIntroducer(&pump, &introducer_probe);
    const introducer_import_id = introducer_probe.import_id orelse return error.IntroducerBootstrapFailed;
    tap.ok(true, "A bootstraps B's Zig introducer");

    var promise_call = PromiseCall{};
    _ = try a_to_b.peer.sendCall(
        introducer_import_id,
        introducer_interface_id,
        get_promise_method_id,
        &promise_call,
        null,
        PromiseCall.onReturn,
    );
    try pumpUntilPromise(&pump, &promise_call, &introducer);
    const promise_import_id = promise_call.import_id orelse return error.PromiseNotImported;
    const promise_export_id = introducer.promise_export_id orelse return error.PromiseNotExported;
    tap.ok(promise_import_id == promise_export_id, "A imports B's unresolved promise");

    var pickup = PickupHandler{
        .expected_promise_id = promise_import_id,
        .expected_accept_peer = &a_to_c.host.peer,
    };
    a_to_b.peer.setHandoffPickupHandler(&pickup, PickupHandler.onPickup);

    const b_network = b_to_a.peer.vat_network orelse return error.NoVatNetworkOnB;
    var introduction = try b_network.mintIntroduction(&b_to_a.peer, "zig-recipient");
    defer introduction.deinit(allocator);

    var await_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
    defer await_msg.deinit();
    const recipient = try await_msg.getRootAnyPointer();

    const provided_target = protocol.MessageTarget{
        .tag = .importedCap,
        .imported_cap = cxx_number.cap_id,
        .promised_answer = null,
    };

    const handle = try b_to_a.peer.resolvePromiseExportToThirdParty(
        promise_import_id,
        &b_to_c.host.peer,
        provided_target,
        recipient,
        introduction.to_contact,
    );

    var accepted_number: ?l3.Number.Client = null;

    switch (scenario) {
        .bad_contact_fallback => {
            const fallback_number = l3.Number.Client.init(&a_to_b.peer, promise_import_id);
            try invokeDirectNumber(&pump, fallback_number, false, "vine-proxy Number.getNumber returns the C++ value exactly once", tap);
            tap.ok(pickup.return_count == 0 and pickup.accepted_id == null, "bad contact falls back to the vine proxy without auto-pickup");
        },
        .malformed_completion, .rejected_completion, .reject_await => {
            try pumpUntilPickup(&pump, &pickup);
            tap.ok(
                pickup.return_count == 1 and pickup.saw_exception and pickup.accepted_id == null,
                "failed C++ pickup returns one deterministic exception",
            );
            try pumpUntilNoOutboundProvide(&pump, &b_to_a.peer, handle.vine_id);
            tap.ok(!b_to_a.peer.exports.contains(handle.vine_id), "handoff vine drains after failed pickup");
        },
        .number_exception => {
            try pumpUntilPickup(&pump, &pickup);
            const accepted_id = pickup.accepted_id orelse return error.AutoPickupFailed;
            accepted_number = l3.Number.Client.init(&a_to_c.host.peer, accepted_id);
            tap.ok(
                pickup.return_count == 1 and pickup.accepted_on_third_peer and a_to_c.host.peer.caps.hasImport(accepted_id),
                "A auto-picks up a direct A-C cap",
            );
            try pumpUntilNoOutboundProvide(&pump, &b_to_a.peer, handle.vine_id);
            tap.ok(!b_to_a.peer.exports.contains(handle.vine_id), "handoff vine drains after pickup");
            try invokeDirectNumber(&pump, accepted_number.?, true, "unused success path", tap);
        },
        .duplicate_accept => {
            try pumpUntilPickup(&pump, &pickup);
            const accepted_id = pickup.accepted_id orelse return error.AutoPickupFailed;
            accepted_number = l3.Number.Client.init(&a_to_c.host.peer, accepted_id);
            tap.ok(
                pickup.return_count == 1 and pickup.accepted_on_third_peer and a_to_c.host.peer.caps.hasImport(accepted_id),
                "A auto-picks up a direct A-C cap",
            );
            try sendDuplicateAccept(allocator, &pump, vat_network.last_token orelse return error.MissingL3Token, tap);
            try pumpUntilNoOutboundProvide(&pump, &b_to_a.peer, handle.vine_id);
            tap.ok(!b_to_a.peer.exports.contains(handle.vine_id), "handoff vine drains after duplicate Accept rejection");
            try invokeDirectNumber(&pump, accepted_number.?, false, "direct A-C Number.getNumber returns the C++ value exactly once", tap);
        },
        .happy, .drop_after_provide => {
            try pumpUntilPickup(&pump, &pickup);
            const accepted_id = pickup.accepted_id orelse return error.AutoPickupFailed;
            accepted_number = l3.Number.Client.init(&a_to_c.host.peer, accepted_id);
            tap.ok(
                pickup.return_count == 1 and pickup.accepted_on_third_peer and a_to_c.host.peer.caps.hasImport(accepted_id),
                "A auto-picks up a direct A-C cap",
            );
            try pumpUntilNoOutboundProvide(&pump, &b_to_a.peer, handle.vine_id);
            tap.ok(!b_to_a.peer.exports.contains(handle.vine_id), "handoff vine drains after pickup");
            try invokeDirectNumber(&pump, accepted_number.?, false, "direct A-C Number.getNumber returns the C++ value exactly once", tap);
        },
    }

    if (accepted_number) |client| client.release();
    try a_to_b.peer.releaseImport(promise_import_id, 1);
    try a_to_b.peer.releaseImport(introducer_import_id, 1);
    if (cxx_number_owned) {
        cxx_number.release();
        cxx_number_owned = false;
    }
    try pump.runFor(150);
    if (scenario == .bad_contact_fallback) {
        try pumpUntilNoOutboundProvide(&pump, &b_to_a.peer, handle.vine_id);
    }

    expectTransientDrain(tap, scenario, &a_to_b, &b_to_a, &a_to_c, &b_to_c, promise_import_id);
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

    const args = parseArgs(allocator, init.minimal.args) catch |err| switch (err) {
        error.HelpRequested => {
            usage();
            return;
        },
        error.InvalidOption,
        error.InvalidCharacter,
        error.Overflow,
        error.MissingArgValue,
        => {
            usage();
            return err;
        },
        else => return err,
    };
    defer allocator.free(args.host);

    var tap = Tap{};
    for (scenarios) |scenario| {
        runScenario(allocator, io, args, &tap, scenario) catch |err| {
            std.debug.print("# scenario failed: {s}: {}\n", .{ scenario.name(), err });
            tap.ok(false, "scenario completes without harness error");
        };
    }

    const join_probe_id = try buildJoinProbe(allocator);
    tap.ok(join_probe_id == 0x1020_3040, "L4 JoinKeyPart shape remains consumable on the Zig receive-side path");
    const join_result_probe = try buildJoinResultProbe(allocator);
    tap.ok(
        join_result_probe.join_id == 0x5060_7080 and join_result_probe.succeeded and join_result_probe.cap_id == 17,
        "L4 JoinResult fixture shape remains consumable by Zig",
    );

    std.debug.print("1..{d}\n", .{tap.test_num});
    if (tap.failures > 0) return error.TestFailed;
}
