const std = @import("std");
const capnpc = @import("capnpc-zig");
const rd = @import("resolve_disembargo");

const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const Peer = capnpc.rpc.peer.Peer;

const Reflector = rd.Reflector;
const CallSequence = rd.CallSequence;

// Capability pass-back through the GENERATED client API, Zig to Zig.
//
// A peer's export ids and its import ids are two independent spaces that both
// start at 0: an import id is the id the REMOTE chose for its export. So in an
// ordinary two-way session a peer soon holds export N and import N at once.
// A capability pointer written into a payload carries only the id, and the
// outbound encoder resolves a bare id to the local export first. These tests
// pin that a Client the runtime produced for an import (bootstrap, `resolveX`)
// still goes back out as the REMOTE's capability (`receiverHosted`), and that
// a capability which comes back home resolves to a usable local Client.

// -- In-process wire ---------------------------------------------------------

const Dir = enum { client_to_server, server_to_client };

const max_recorded_caps = 4;

/// One decoded outbound frame: the message tag, its question/answer id, and,
/// for a Call or a results Return, the payload's cap-table descriptors.
const WireEvent = struct {
    dir: Dir,
    tag: protocol.MessageTag,
    id: u32 = 0,
    method_id: u16 = 0,
    release_id: u32 = 0,
    cap_count: usize = 0,
    cap_tags: [max_recorded_caps]protocol.CapDescriptorTag = undefined,
    cap_ids: [max_recorded_caps]u32 = undefined,

    fn recordCaps(self: *WireEvent, payload: protocol.Payload) !void {
        const list = payload.cap_table orelse return;
        const n = @min(list.len(), max_recorded_caps);
        var index: u32 = 0;
        while (index < n) : (index += 1) {
            const desc = try protocol.CapDescriptor.fromReader(try list.get(index));
            self.cap_tags[index] = desc.tag;
            self.cap_ids[index] = desc.id orelse 0;
        }
        self.cap_count = n;
    }
};

/// Cross-wires two detached peers: each send-frame override records the frame
/// and synchronously hands it to the other peer. Declared before the peers so
/// their teardown frames are recorded but no longer forwarded.
const Wire = struct {
    allocator: std.mem.Allocator,
    events: std.ArrayList(WireEvent) = .empty,
    client_peer: ?*Peer = null,
    server_peer: ?*Peer = null,
    forwarding: bool = true,

    fn deinit(self: *Wire) void {
        self.events.deinit(self.allocator);
    }

    fn record(self: *Wire, dir: Dir, frame: []const u8) !void {
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        var event = WireEvent{ .dir = dir, .tag = decoded.tag };
        switch (decoded.tag) {
            .call => {
                const call = try decoded.asCall();
                event.id = call.question_id;
                event.method_id = call.method_id;
                try event.recordCaps(call.params);
            },
            .@"return" => {
                const ret = try decoded.asReturn();
                event.id = ret.answer_id;
                if (ret.results) |results| try event.recordCaps(results);
            },
            .release => event.release_id = (try decoded.asRelease()).id,
            else => {},
        }
        try self.events.append(self.allocator, event);
    }

    fn clientSend(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Wire = @ptrCast(@alignCast(ctx_ptr));
        try self.record(.client_to_server, frame);
        if (self.forwarding) {
            if (self.server_peer) |peer| try peer.handleFrame(frame);
        }
    }

    fn serverSend(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Wire = @ptrCast(@alignCast(ctx_ptr));
        try self.record(.server_to_client, frame);
        if (self.forwarding) {
            if (self.client_peer) |peer| try peer.handleFrame(frame);
        }
    }

    /// The last Call the client sent for `method`.
    fn lastCall(self: *const Wire, method: u16) ?WireEvent {
        var index = self.events.items.len;
        while (index > 0) {
            index -= 1;
            const event = self.events.items[index];
            if (event.dir == .client_to_server and event.tag == .call and event.method_id == method) return event;
        }
        return null;
    }

    /// The server's Return for `answer_id`.
    fn serverReturn(self: *const Wire, answer_id: u32) ?WireEvent {
        for (self.events.items) |event| {
            if (event.dir == .server_to_client and event.tag == .@"return" and event.id == answer_id) return event;
        }
        return null;
    }

    fn countReleases(self: *const Wire, dir: Dir, id: u32) usize {
        var n: usize = 0;
        for (self.events.items) |event| {
            if (event.dir == dir and event.tag == .release and event.release_id == id) n += 1;
        }
        return n;
    }
};

// -- A CallSequence implementation -------------------------------------------

/// getNumber returns `base + calls`, so every instance answers with numbers
/// of its own and a test can tell which capability a call reached.
const Counter = struct {
    base: u32,
    calls: u32 = 0,
    server: CallSequence.Server = undefined,

    fn bind(self: *Counter) void {
        self.server = .{ .ctx = self, .vtable = .{ .getNumber = getNumber } };
    }

    fn getNumber(
        ctx_ptr: *anyopaque,
        _: *Peer,
        _: CallSequence.GetNumberParams.Reader,
        results: *CallSequence.GetNumberResults.Builder,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *Counter = @ptrCast(@alignCast(ctx_ptr));
        try results.setN(self.base + self.calls);
        self.calls += 1;
    }
};

// -- The server (Reflector) ----------------------------------------------------

const ReflectMode = enum {
    /// Export `home` and return it as `promise`.
    return_home,
    /// Resolve `target` with the generated resolver and hand it straight back
    /// as `promise` with the generated setter.
    echo_target,
};

const InvokeMode = enum {
    /// Record what `cb` is in the inbound cap table; do not touch it.
    inspect,
    /// Resolve `cb` with the generated resolver and call getNumber on it.
    resolve_and_call,
};

const ServerState = struct {
    home: Counter = .{ .base = 1000 },
    home_export_id: ?u32 = null,
    reflect_mode: ReflectMode = .return_home,
    invoke_mode: InvokeMode = .inspect,
    /// `echo_target`: what `target` was in reflect's inbound cap table.
    reflect_target: ?cap_table.ResolvedCap = null,
    /// `echo_target`: the error the generated resolver returned, if any.
    echo_resolve_error: ?anyerror = null,
    /// What `cb` was in invokeCap's inbound cap table.
    invoke_cb: ?cap_table.ResolvedCap = null,
    /// `resolve_and_call`: the error the generated resolver returned, if any.
    invoke_resolve_error: ?anyerror = null,
    /// `resolve_and_call`: the value getNumber returned through the resolved
    /// Client.
    invoke_cb_n: ?u32 = null,
    invoke_cb_client: ?CallSequence.Client = null,
    server: Reflector.Server = undefined,

    fn bind(self: *ServerState) void {
        self.home.bind();
        self.server = .{ .ctx = self, .vtable = .{
            .reflect = reflect,
            .resolveNow = resolveNow,
            .invokeCap = invokeCap,
            .disconnectNow = disconnectNow,
        } };
    }

    fn reflect(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        params: Reflector.ReflectParams.Reader,
        results: *Reflector.ReflectResults.Builder,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ServerState = @ptrCast(@alignCast(ctx_ptr));
        switch (self.reflect_mode) {
            .return_home => {
                const id = self.home_export_id orelse blk: {
                    const fresh = try CallSequence.exportServer(peer, &self.home.server);
                    self.home_export_id = fresh;
                    break :blk fresh;
                };
                try results.setPromiseCapability(.{ .id = id });
            },
            .echo_target => {
                self.reflect_target = try caps.resolveCapability(try params.getTarget());
                const target = params.resolveTarget(peer, caps) catch |err| {
                    self.echo_resolve_error = err;
                    return err;
                };
                try results.setPromiseClient(target);
            },
        }
    }

    fn invokeCap(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        params: Reflector.InvokeCapParams.Reader,
        results: *Reflector.InvokeCapResults.Builder,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ServerState = @ptrCast(@alignCast(ctx_ptr));
        self.invoke_cb = try caps.resolveCapability(try params.getCb());
        switch (self.invoke_mode) {
            .inspect => try results.setObserved(0),
            .resolve_and_call => {
                const cb = params.resolveCb(peer, caps) catch |err| {
                    self.invoke_resolve_error = err;
                    return err;
                };
                self.invoke_cb_client = cb;
                _ = try cb.callGetNumber(self, null, onCbReturn);
                try results.setObserved(self.invoke_cb_n orelse 0xFFFF_FFFF);
            },
        }
    }

    fn onCbReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        response: CallSequence.GetNumber.Response,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ServerState = @ptrCast(@alignCast(ctx_ptr));
        const results = try response.unwrap();
        self.invoke_cb_n = try results.getN();
    }

    fn resolveNow(
        _: *anyopaque,
        _: *Peer,
        _: Reflector.ResolveNowParams.Reader,
        _: *Reflector.ResolveNowResults.Builder,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        return error.UnexpectedMethodCall;
    }

    fn disconnectNow(
        _: *anyopaque,
        _: *Peer,
        _: Reflector.DisconnectNowParams.Reader,
        _: *Reflector.DisconnectNowResults.Builder,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        return error.UnexpectedMethodCall;
    }
};

// -- The client ------------------------------------------------------------------

const ClientState = struct {
    reflector: ?Reflector.Client = null,
    /// What `reflect` sends as `target`.
    target: ?CallSequence.Client = null,
    /// What `invokeCap` sends as `cb` (a capability pointer written by the
    /// generated `setCbClient`), unless `cb_raw` is set.
    cb: ?CallSequence.Client = null,
    /// A raw capability for `cb`, written with `setCbCapability`.
    cb_raw: ?u32 = null,
    /// reflect's `promise`, through the generated resolver.
    promise: ?CallSequence.Client = null,
    promise_resolve_error: ?anyerror = null,
    /// reflect's `promise`, as the inbound cap table resolved it.
    promise_raw: ?cap_table.ResolvedCap = null,
    reflect_exception: bool = false,
    invoke_observed: ?u32 = null,
    invoke_exception: bool = false,

    fn onBootstrap(ctx_ptr: *anyopaque, _: *Peer, response: Reflector.BootstrapResponse) anyerror!void {
        const self: *ClientState = @ptrCast(@alignCast(ctx_ptr));
        self.reflector = try response.unwrap();
    }

    fn buildReflect(ctx_ptr: *anyopaque, params: *Reflector.ReflectParams.Builder) anyerror!void {
        const self: *ClientState = @ptrCast(@alignCast(ctx_ptr));
        try params.setTargetClient(self.target.?);
    }

    fn onReflectReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        response: Reflector.Reflect.Response,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ClientState = @ptrCast(@alignCast(ctx_ptr));
        const results = response.unwrap() catch {
            self.reflect_exception = true;
            return;
        };
        self.promise_raw = try caps.resolveCapability(try results.getPromise());
        self.promise = results.resolvePromise(peer, caps) catch |err| blk: {
            self.promise_resolve_error = err;
            break :blk null;
        };
    }

    fn buildInvokeCap(ctx_ptr: *anyopaque, params: *Reflector.InvokeCapParams.Builder) anyerror!void {
        const self: *ClientState = @ptrCast(@alignCast(ctx_ptr));
        if (self.cb_raw) |id| {
            try params.setCbCapability(.{ .id = id });
        } else {
            try params.setCbClient(self.cb.?);
        }
    }

    fn onInvokeCapReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        response: Reflector.InvokeCap.Response,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ClientState = @ptrCast(@alignCast(ctx_ptr));
        const results = response.unwrap() catch {
            self.invoke_exception = true;
            return;
        };
        self.invoke_observed = try results.getObserved();
    }
};

// -- Fixture ---------------------------------------------------------------------

/// Two peers on one in-process wire. The CLIENT exports two CallSequences of
/// its own (ids 0 and 1) before it bootstraps, the way a bidirectional app
/// exports callbacks; the server's bootstrap Reflector then lands as the
/// client's import 0, and the first capability the server exports after it
/// lands as the client's import 1. Both ids exist in the client's export
/// space too, which is the collision these tests are about.
const Fixture = struct {
    wire: Wire,
    server_peer: Peer,
    client_peer: Peer,
    server: ServerState = .{},
    client: ClientState = .{},
    decoy0: Counter = .{ .base = 500 },
    decoy1: Counter = .{ .base = 900 },

    fn init(self: *Fixture, allocator: std.mem.Allocator) !void {
        self.* = .{
            .wire = .{ .allocator = allocator },
            .server_peer = Peer.initDetached(allocator),
            .client_peer = Peer.initDetached(allocator),
        };
        self.server_peer.disableThreadAffinity();
        self.client_peer.disableThreadAffinity();
        self.wire.client_peer = &self.client_peer;
        self.wire.server_peer = &self.server_peer;
        self.client_peer.setSendFrameOverride(&self.wire, Wire.clientSend);
        self.server_peer.setSendFrameOverride(&self.wire, Wire.serverSend);

        self.server.bind();
        _ = try Reflector.setBootstrap(&self.server_peer, &self.server.server);

        self.decoy0.bind();
        self.decoy1.bind();
        const decoy0_id = try CallSequence.exportServer(&self.client_peer, &self.decoy0.server);
        const decoy1_id = try CallSequence.exportServer(&self.client_peer, &self.decoy1.server);
        try std.testing.expectEqual(@as(u32, 0), decoy0_id);
        try std.testing.expectEqual(@as(u32, 1), decoy1_id);
        // The client names its own export 0 as reflect's `target` through a
        // Client built with `Client.init`, the way tests/e2e/zig/main_client.zig
        // does: such a Client records no id space, so the encoder classifies
        // the bare id and a local export wins.
        self.client.target = CallSequence.Client.init(&self.client_peer, decoy0_id);

        _ = try Reflector.Client.fromBootstrap(&self.client_peer, &self.client, ClientState.onBootstrap);
        const reflector = self.client.reflector orelse return error.BootstrapDidNotResolve;
        // Precondition: the bootstrap import collides with the client's export 0.
        try std.testing.expectEqual(@as(u32, 0), reflector.cap_id);
        try std.testing.expect(self.client_peer.caps.hasExport(0));
        try std.testing.expect(self.client_peer.caps.hasImport(0));
    }

    fn deinit(self: *Fixture) void {
        self.wire.forwarding = false;
        self.client_peer.deinit();
        self.server_peer.deinit();
        self.wire.deinit();
    }

    fn reflect(self: *Fixture, options: capnpc.rpc.peer.CallOptions) !u32 {
        const reflector = self.client.reflector.?;
        return reflector.callReflectWithOptions(&self.client, ClientState.buildReflect, ClientState.onReflectReturn, options);
    }

    fn invokeCap(self: *Fixture) !u32 {
        const reflector = self.client.reflector.?;
        return reflector.callInvokeCap(&self.client, ClientState.buildInvokeCap, ClientState.onInvokeCapReturn);
    }
};

fn expectExported(expected_id: u32, actual: ?cap_table.ResolvedCap) !void {
    const cap = actual orelse return error.CapabilityNotRecorded;
    switch (cap) {
        .exported => |exported| try std.testing.expectEqual(expected_id, exported.id),
        else => {
            std.debug.print("expected .exported{{ .id = {d} }}, got {any}\n", .{ expected_id, cap });
            return error.TestExpectedExportedCapability;
        },
    }
}

// -- Pass-back: an import goes back out as the remote's capability ---------------

test "a client passes a server-issued capability back and the server receives its own export" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    // reflect: the server exports `home` (its export 1) and returns it.
    _ = try fx.reflect(.{});
    const home_id = fx.server.home_export_id orelse return error.HomeNotExported;
    try std.testing.expectEqual(@as(u32, 1), home_id);
    const home = fx.client.promise orelse return error.PromiseNotResolved;
    try std.testing.expectEqual(home_id, home.cap_id);
    // Precondition: the client's import of `home` collides with its own
    // export 1 (decoy1).
    try std.testing.expect(fx.client_peer.caps.hasImport(home_id));
    try std.testing.expect(fx.client_peer.caps.hasExport(home_id));

    // invokeCap(cb = home), written by the generated setCbClient.
    fx.client.cb = home;
    _ = try fx.invokeCap();

    // On the wire the cap is the server's own capability...
    const call = fx.wire.lastCall(Reflector.InvokeCap.ordinal) orelse return error.MissingInvokeCapCall;
    try std.testing.expectEqual(@as(usize, 1), call.cap_count);
    try std.testing.expectEqual(protocol.CapDescriptorTag.receiverHosted, call.cap_tags[0]);
    try std.testing.expectEqual(home_id, call.cap_ids[0]);
    // ...so the server sees its export, not an import of the client's decoy1.
    try expectExported(home_id, fx.server.invoke_cb);
    try std.testing.expectEqual(@as(?u32, 0), fx.client.invoke_observed);
    try std.testing.expectEqual(@as(u32, 0), fx.decoy1.calls);
}

test "a server hands a client's capability back and the client receives its own export" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    // reflect(target = decoy0, the client's export 0): the server resolves
    // `target` (its import 0, colliding with its bootstrap export 0) with the
    // generated resolver and returns it with the generated setter.
    fx.server.reflect_mode = .echo_target;
    const question_id = try fx.reflect(.{});
    try std.testing.expectEqual(@as(?anyerror, null), fx.server.echo_resolve_error);
    try std.testing.expect(fx.server_peer.caps.hasExport(0));
    try std.testing.expect(fx.server_peer.caps.hasImport(0));

    const ret = fx.wire.serverReturn(question_id) orelse return error.MissingReflectReturn;
    try std.testing.expectEqual(@as(usize, 1), ret.cap_count);
    try std.testing.expectEqual(protocol.CapDescriptorTag.receiverHosted, ret.cap_tags[0]);
    try std.testing.expectEqual(@as(u32, 0), ret.cap_ids[0]);
    // The client gets decoy0 back, not an import of the server's Reflector.
    try expectExported(0, fx.client.promise_raw);
}

// -- Coming home: resolveX accepts the peer's own export ---------------------------

test "resolveX turns a capability that came home into a callable local Client" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    _ = try fx.reflect(.{});
    const home_id = fx.server.home_export_id orelse return error.HomeNotExported;
    fx.client.cb = fx.client.promise orelse return error.PromiseNotResolved;

    // The server resolves `cb` with the generated resolveCb and calls it.
    fx.server.invoke_mode = .resolve_and_call;
    _ = try fx.invokeCap();

    try std.testing.expectEqual(@as(?anyerror, null), fx.server.invoke_resolve_error);
    try expectExported(home_id, fx.server.invoke_cb);
    const cb = fx.server.invoke_cb_client orelse return error.ResolveCbDidNotRun;
    try std.testing.expectEqual(home_id, cb.cap_id);
    try std.testing.expectEqual(capnpc.rpc.peer.ClientOrigin.exported, cb.origin);
    // getNumber reached the server's own `home` (base 1000), locally: no
    // client decoy ran and the call never touched the wire.
    try std.testing.expectEqual(@as(u32, 1), fx.server.home.calls);
    try std.testing.expectEqual(@as(?u32, 1000), fx.server.invoke_cb_n);
    try std.testing.expectEqual(@as(?u32, 1000), fx.client.invoke_observed);
    try std.testing.expectEqual(@as(u32, 0), fx.decoy0.calls);
    try std.testing.expectEqual(@as(u32, 0), fx.decoy1.calls);
    for (fx.wire.events.items) |event| {
        if (event.dir == .server_to_client and event.tag == .call) return error.LocalCallWentOnTheWire;
    }

    // release() on a local Client owns no import, so it sends nothing.
    const releases_before = fx.wire.countReleases(.server_to_client, home_id);
    cb.release();
    try std.testing.expectEqual(releases_before, fx.wire.countReleases(.server_to_client, home_id));
}

test "release() on a local Client leaves an import with the same id alone" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    _ = try fx.reflect(.{});
    const home_id = fx.server.home_export_id orelse return error.HomeNotExported;

    // invokeCap(cb = the client's decoy1, its export 1): the server resolves
    // it to an import it keeps, so the server now holds export 1 (`home`)
    // and import 1 (decoy1) at once.
    fx.client.cb = CallSequence.Client.init(&fx.client_peer, 1);
    fx.server.invoke_mode = .resolve_and_call;
    _ = try fx.invokeCap();
    try std.testing.expectEqual(@as(?u32, 900), fx.client.invoke_observed);
    try std.testing.expectEqual(capnpc.rpc.peer.ClientOrigin.imported, fx.server.invoke_cb_client.?.origin);
    try std.testing.expectEqual(home_id, fx.server.invoke_cb_client.?.cap_id);
    try std.testing.expect(fx.server_peer.caps.hasExport(home_id));
    try std.testing.expect(fx.server_peer.caps.hasImport(home_id));

    // A local Client for `home` owns no import: its release() must not spend
    // the server's reference to decoy1.
    const local = CallSequence.Client{ .peer = &fx.server_peer, .cap_id = home_id, .origin = .exported };
    local.release();
    try std.testing.expectEqual(@as(usize, 0), fx.wire.countReleases(.server_to_client, home_id));
    try std.testing.expect(fx.server_peer.caps.hasImport(home_id));
}

test "resolveX resolves a receiverAnswer param whose answer returned to a local Client" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    // reflect, keeping the answer open (no Finish) so a later call can still
    // name it.
    const reflect_question = try fx.reflect(.{ .result_lifetime = .retained });
    const home_id = fx.server.home_export_id orelse return error.HomeNotExported;

    // invokeCap(cb = receiverAnswer(reflect, [promise])): a pipelined
    // capability in params, the shape the C++ reference sends.
    const ops = [_]protocol.PromisedAnswerOp{.{ .tag = .getPointerField, .pointer_index = 0 }};
    fx.client.cb_raw = try fx.client_peer.caps.noteReceiverAnswerOps(reflect_question, &ops);
    fx.server.invoke_mode = .resolve_and_call;
    _ = try fx.invokeCap();

    const call = fx.wire.lastCall(Reflector.InvokeCap.ordinal) orelse return error.MissingInvokeCapCall;
    try std.testing.expectEqual(protocol.CapDescriptorTag.receiverAnswer, call.cap_tags[0]);
    try std.testing.expectEqual(@as(?anyerror, null), fx.server.invoke_resolve_error);
    const cb = fx.server.invoke_cb_client orelse return error.ResolveCbDidNotRun;
    try std.testing.expectEqual(home_id, cb.cap_id);
    try std.testing.expectEqual(capnpc.rpc.peer.ClientOrigin.exported, cb.origin);
    try std.testing.expectEqual(@as(?u32, 1000), fx.client.invoke_observed);
    try std.testing.expectEqual(@as(u32, 0), fx.decoy1.calls);

    try fx.client_peer.finishRetainedQuestion(reflect_question, true);
}

test "resolveX hands a local Client back out as the peer's own capability" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    // First reflect: the client imports the server's `home`.
    _ = try fx.reflect(.{});
    const home_id = fx.server.home_export_id orelse return error.HomeNotExported;
    const home = fx.client.promise orelse return error.PromiseNotResolved;

    // Second reflect(target = home): the server resolves `target` (its own
    // export, come home) and returns it with the generated setter.
    fx.client.target = home;
    fx.client.promise = null;
    fx.client.promise_raw = null;
    fx.server.reflect_mode = .echo_target;
    const question_id = try fx.reflect(.{});
    try expectExported(home_id, fx.server.reflect_target);
    try std.testing.expectEqual(@as(?anyerror, null), fx.server.echo_resolve_error);

    const ret = fx.wire.serverReturn(question_id) orelse return error.MissingReflectReturn;
    try std.testing.expectEqual(@as(usize, 1), ret.cap_count);
    try std.testing.expectEqual(protocol.CapDescriptorTag.senderHosted, ret.cap_tags[0]);
    try std.testing.expectEqual(home_id, ret.cap_ids[0]);
    const again = fx.client.promise orelse return error.PromiseNotResolved;
    try std.testing.expectEqual(home_id, again.cap_id);
    try std.testing.expectEqual(capnpc.rpc.peer.ClientOrigin.imported, again.origin);
}

test "resolveX fails closed on a receiverAnswer param that resolves to an import" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    // reflect(target = decoy0) with the echo server: the open answer's
    // `promise` is the server's IMPORT of the client's decoy0.
    fx.server.reflect_mode = .echo_target;
    const reflect_question = try fx.reflect(.{ .result_lifetime = .retained });

    const ops = [_]protocol.PromisedAnswerOp{.{ .tag = .getPointerField, .pointer_index = 0 }};
    fx.client.cb_raw = try fx.client_peer.caps.noteReceiverAnswerOps(reflect_question, &ops);
    fx.server.invoke_mode = .resolve_and_call;
    _ = try fx.invokeCap();

    // A Client for that import would own no reference of its own, so its
    // release() would spend a reference someone else holds: refuse it.
    try std.testing.expectEqual(@as(?anyerror, error.UnexpectedCapabilityType), fx.server.invoke_resolve_error);
    try std.testing.expect(fx.client.invoke_exception);
    try std.testing.expectEqual(@as(u32, 0), fx.decoy0.calls);

    try fx.client_peer.finishRetainedQuestion(reflect_question, true);
}

test "a local Client refuses pipelined calls instead of sending them to the wrong vat" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    // The server's own bootstrap export, held as a local Client. A local
    // call's question never reaches the wire, so a call pipelined on its
    // answer has nowhere correct to go.
    const local = Reflector.Client{ .peer = &fx.server_peer, .cap_id = 0, .origin = .exported };
    const events_before = fx.wire.events.items.len;
    try std.testing.expectError(
        error.LocalCapabilityPipelineUnsupported,
        local.callReflectPipelined(&fx.client, null, ClientState.onReflectReturn),
    );
    try std.testing.expectEqual(events_before, fx.wire.events.items.len);
}

test "a Builder reads back the capability a generated setXClient wrote" {
    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();
    var builder = capnpc.message.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    var params = try Reflector.InvokeCapParams.Builder.init(&builder);

    // An import is written with its id space attached (an in-builder
    // intermediate the outbound encoder resolves); the Builder still reads
    // the id back.
    try params.setCbClient(.{ .peer = &peer, .cap_id = 7, .origin = .imported });
    try std.testing.expectEqual(@as(u32, 7), (try params.getCb()).id);
    try params.setCbClient(CallSequence.Client.init(&peer, 9));
    try std.testing.expectEqual(@as(u32, 9), (try params.getCb()).id);
}

/// Builds invokeCap params whose `cb` is `cb` (or the raw capability
/// `cb_raw`), and records what the local call observed.
const LocalParamCtx = struct {
    cb: ?CallSequence.Client = null,
    cb_raw: ?u32 = null,
    returned: bool = false,
    observed: ?u32 = null,
    exception: bool = false,

    fn build(ctx_ptr: *anyopaque, params: *Reflector.InvokeCapParams.Builder) anyerror!void {
        const self: *LocalParamCtx = @ptrCast(@alignCast(ctx_ptr));
        if (self.cb_raw) |id| {
            try params.setCbCapability(.{ .id = id });
        } else {
            try params.setCbClient(self.cb.?);
        }
    }

    fn onReturn(ctx_ptr: *anyopaque, _: *Peer, response: Reflector.InvokeCap.Response, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *LocalParamCtx = @ptrCast(@alignCast(ctx_ptr));
        self.returned = true;
        const results = response.unwrap() catch {
            self.exception = true;
            return;
        };
        self.observed = try results.getObserved();
    }
};

test "a local Client call hands its handler the peer's own export and import from its params" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    _ = try fx.reflect(.{});
    const home_id = fx.server.home_export_id orelse return error.HomeNotExported;
    // invokeCap(cb = the client's decoy1): the server keeps its import of
    // decoy1, so it holds export 1 (`home`) and import 1 at once.
    fx.client.cb = CallSequence.Client.init(&fx.client_peer, 1);
    fx.server.invoke_mode = .resolve_and_call;
    _ = try fx.invokeCap();
    const decoy1_import = fx.server.invoke_cb_client orelse return error.ResolveCbDidNotRun;
    try std.testing.expectEqual(capnpc.rpc.peer.ClientOrigin.imported, decoy1_import.origin);
    try std.testing.expectEqual(home_id, decoy1_import.cap_id);
    try std.testing.expect(fx.server_peer.caps.hasExport(home_id));
    try std.testing.expect(fx.server_peer.caps.hasImport(home_id));
    const decoy1_calls = fx.decoy1.calls;

    // The server calls its own Reflector (export 0) locally with
    // cb = its own `home`. The handler resolves `home` to a local Client and
    // reaches `home` (base 1000), not the client's decoy1. Nothing touches
    // the wire.
    const local_reflector = Reflector.Client{ .peer = &fx.server_peer, .cap_id = 0, .origin = .exported };
    var home_ctx = LocalParamCtx{ .cb = .{ .peer = &fx.server_peer, .cap_id = home_id, .origin = .exported } };
    const events_before = fx.wire.events.items.len;
    _ = try local_reflector.callInvokeCap(&home_ctx, LocalParamCtx.build, LocalParamCtx.onReturn);
    try expectExported(home_id, fx.server.invoke_cb);
    try std.testing.expectEqual(capnpc.rpc.peer.ClientOrigin.exported, fx.server.invoke_cb_client.?.origin);
    try std.testing.expectEqual(@as(?u32, 1000), home_ctx.observed);
    try std.testing.expectEqual(@as(u32, 1), fx.server.home.calls);
    try std.testing.expectEqual(decoy1_calls, fx.decoy1.calls);
    try std.testing.expectEqual(events_before, fx.wire.events.items.len);
    try std.testing.expectEqual(@as(u32, 1), fx.server_peer.caps.imports.get(home_id).?.ref_count);

    // cb = the server's import of decoy1: the handler gets an import Client
    // and its call goes to the client's decoy1 (base 900).
    var import_ctx = LocalParamCtx{ .cb = decoy1_import };
    _ = try local_reflector.callInvokeCap(&import_ctx, LocalParamCtx.build, LocalParamCtx.onReturn);
    const kept = fx.server.invoke_cb_client orelse return error.ResolveCbDidNotRun;
    try std.testing.expectEqual(capnpc.rpc.peer.ClientOrigin.imported, kept.origin);
    try std.testing.expectEqual(home_id, kept.cap_id);
    try std.testing.expectEqual(@as(?u32, 900 + decoy1_calls), import_ctx.observed);
    try std.testing.expectEqual(decoy1_calls + 1, fx.decoy1.calls);
    try std.testing.expectEqual(@as(u32, 1), fx.server.home.calls);

    // The handler's Client owns a reference the client never granted:
    // releasing it sends nothing, and the server keeps its own reference.
    const releases_before = fx.wire.countReleases(.server_to_client, home_id);
    kept.release();
    try std.testing.expectEqual(releases_before, fx.wire.countReleases(.server_to_client, home_id));
    try std.testing.expectEqual(@as(u32, 1), fx.server_peer.caps.imports.get(home_id).?.ref_count);
    // The server's own reference is the one the client counts.
    decoy1_import.release();
    try std.testing.expectEqual(releases_before + 1, fx.wire.countReleases(.server_to_client, home_id));
    try std.testing.expect(!fx.server_peer.caps.hasImport(home_id));
}

test "a local Client call with a pipelined capability in its params fails closed" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    fx.server.invoke_mode = .resolve_and_call;
    // A promise on one of the server's own questions (a `receiverAnswer`
    // once encoded). The loopback receiver is the server itself, which would
    // read it as one of its ANSWERS, a different id space: the local call
    // refuses it before anything is dispatched.
    const ops = [_]protocol.PromisedAnswerOp{.{ .tag = .getPointerField, .pointer_index = 0 }};
    const pipelined_id = try fx.server_peer.caps.noteReceiverAnswerOps(5, &ops);
    const local_reflector = Reflector.Client{ .peer = &fx.server_peer, .cap_id = 0, .origin = .exported };
    var ctx = LocalParamCtx{ .cb_raw = pipelined_id };
    const events_before = fx.wire.events.items.len;
    try std.testing.expectError(
        error.LoopbackPromisedCapabilityUnsupported,
        local_reflector.callInvokeCap(&ctx, LocalParamCtx.build, LocalParamCtx.onReturn),
    );
    try std.testing.expect(!ctx.returned);
    try std.testing.expectEqual(@as(?cap_table.ResolvedCap, null), fx.server.invoke_cb);
    try std.testing.expectEqual(events_before, fx.wire.events.items.len);
    try std.testing.expect(fx.server_peer.caps.hasReceiverAnswer(pipelined_id));
    try std.testing.expectEqual(@as(usize, 0), fx.server_peer.loopback_questions.count());
}

test "resolveX fails with PromiseUnresolved for a receiverAnswer whose answer has not returned" {
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    // invokeCap params whose `cb` is receiverAnswer(question 5, [field 0]),
    // decoded the way an inbound Call is. This peer has no answer 5.
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(1, Reflector.interface_id, Reflector.InvokeCap.ordinal);
    try call.setTargetImportedCap(0);
    var payload = try call.payloadTyped();
    var content = try payload.initContent();
    var params = Reflector.InvokeCapParams.Builder.wrap(try content.initStruct(0, 1));
    try params.setCbCapability(.{ .id = 0 });
    var cap_list = try call.initCapTableTyped(1);
    const ops = [_]protocol.PromisedAnswerOp{.{ .tag = .getPointerField, .pointer_index = 0 }};
    try protocol.CapDescriptor.writeReceiverAnswer((try cap_list.get(0))._builder, 5, &ops);
    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    const inbound_call = try decoded.asCall();
    var caps = try cap_table.InboundCapTable.init(allocator, inbound_call.params.cap_table, &peer.caps);
    defer caps.deinit();
    const reader = Reflector.InvokeCapParams.Reader.wrap(try inbound_call.params.content.getStruct());

    try std.testing.expectError(error.PromiseUnresolved, reader.resolveCb(&peer, &caps));
}
