//! Loopback calls that carry capabilities, through the Stable raw API:
//! `Peer.sendCallResolved` with an `.exported` target, and the
//! resolved-import fast path of `Peer.sendCall`.
//!
//! A loopback call never leaves the process. This peer's own encoder writes
//! the Call (and later its Return), so every capability descriptor in it is
//! written from OUR side: `senderHosted`/`senderPromise` name one of our
//! exports, `receiverHosted` one of our imports. The loopback must read them
//! back the same way. Read as if the remote had sent them, our export N comes
//! back as an import N, which names whatever the REMOTE exports as N.
//!
//! Export ids and import ids are independent spaces that both start at 0, so
//! the fixture below holds export 0 and import 0 at once on the server. The
//! tests pin that a handler (and a Return callback) receives our export as
//! `.exported`, our import as `.imported`, that calling each reaches the
//! right object, and that a loopback-only reference never sends a Release,
//! Finish or Disembargo to the remote.

const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const descriptors = cap_table.descriptors;
const Peer = capnpc.rpc.peer.Peer;

const interface_id: u64 = 0xd1ce_10ab_0c0f_fee1;
/// `number() -> (n :UInt32)`: every Counter answers `base + calls`.
const number_method: u16 = 0;
/// `probe(caps...) -> (caps...)`: params and results are a struct whose
/// pointer fields are capabilities.
const probe_method: u16 = 1;

const max_caps = 4;

fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
    return @ptrCast(@alignCast(ctx));
}

// -- In-process wire -----------------------------------------------------------

const Dir = enum { client_to_server, server_to_client };

const WireEvent = struct {
    dir: Dir,
    tag: protocol.MessageTag,
    id: u32 = 0,
    release_count: u32 = 0,
};

const QueuedFrame = struct {
    dir: Dir,
    bytes: []u8,
};

/// Cross-wires two detached peers: each send-frame override records the
/// frame and hands it to the other peer, synchronously unless `queueing` is
/// set, in which case it waits for `flush` the way an async transport would.
const Wire = struct {
    allocator: std.mem.Allocator,
    events: std.ArrayList(WireEvent) = .empty,
    client_peer: ?*Peer = null,
    server_peer: ?*Peer = null,
    forwarding: bool = true,
    queueing: bool = false,
    queue: std.ArrayList(QueuedFrame) = .empty,

    fn deinit(self: *Wire) void {
        for (self.queue.items) |queued| self.allocator.free(queued.bytes);
        self.queue.deinit(self.allocator);
        self.events.deinit(self.allocator);
    }

    fn deliver(self: *Wire, dir: Dir, frame: []const u8) !void {
        if (!self.forwarding) return;
        if (self.queueing) {
            const bytes = try self.allocator.dupe(u8, frame);
            errdefer self.allocator.free(bytes);
            try self.queue.append(self.allocator, .{ .dir = dir, .bytes = bytes });
            return;
        }
        const peer = switch (dir) {
            .client_to_server => self.server_peer,
            .server_to_client => self.client_peer,
        };
        if (peer) |target| try target.handleFrame(frame);
    }

    /// Deliver queued frames in order, including any their delivery queues,
    /// then go back to synchronous delivery.
    fn flush(self: *Wire) !void {
        while (self.queue.items.len > 0) {
            const queued = self.queue.orderedRemove(0);
            defer self.allocator.free(queued.bytes);
            const peer = switch (queued.dir) {
                .client_to_server => self.server_peer,
                .server_to_client => self.client_peer,
            };
            if (peer) |target| try target.handleFrame(queued.bytes);
        }
        self.queueing = false;
    }

    fn record(self: *Wire, dir: Dir, frame: []const u8) !void {
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        var event = WireEvent{ .dir = dir, .tag = decoded.tag };
        switch (decoded.tag) {
            .call => event.id = (try decoded.asCall()).question_id,
            .@"return" => event.id = (try decoded.asReturn()).answer_id,
            .finish => event.id = (try decoded.asFinish()).question_id,
            .resolve => event.id = (try decoded.asResolve()).promise_id,
            .release => {
                const release = try decoded.asRelease();
                event.id = release.id;
                event.release_count = release.reference_count;
            },
            else => {},
        }
        try self.events.append(self.allocator, event);
    }

    fn clientSend(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const self = castCtx(*Wire, ctx_ptr);
        try self.record(.client_to_server, frame);
        try self.deliver(.client_to_server, frame);
    }

    fn serverSend(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const self = castCtx(*Wire, ctx_ptr);
        try self.record(.server_to_client, frame);
        try self.deliver(.server_to_client, frame);
    }

    /// Frames `dir` carried from event index `since` on.
    fn countSince(self: *const Wire, dir: Dir, since: usize) usize {
        var n: usize = 0;
        for (self.events.items[since..]) |event| {
            if (event.dir == dir) n += 1;
        }
        return n;
    }

    /// The reference count all of `dir`'s Release frames for `id` spent.
    fn releasedTotal(self: *const Wire, dir: Dir, id: u32) u32 {
        var n: u32 = 0;
        for (self.events.items) |event| {
            if (event.dir == dir and event.tag == .release and event.id == id) n += event.release_count;
        }
        return n;
    }

    fn dump(self: *const Wire, since: usize) void {
        for (self.events.items[since..]) |event| {
            std.debug.print("  {s} {s} id={d} count={d}\n", .{ @tagName(event.dir), @tagName(event.tag), event.id, event.release_count });
        }
    }
};

// -- Capabilities in a payload -------------------------------------------------

/// One capability pointer to write. `origin` attaches the id space to the
/// pointer, the way a generated `setXClient` does; null writes a plain
/// pointer, which the encoder classifies by id (a local export wins).
const CapRef = struct {
    origin: ?protocol.CapDescriptorTag,
    id: u32,
};

/// Writes a struct whose pointer fields are the given capabilities.
const CapList = struct {
    refs: []const CapRef,

    fn write(self: *const CapList, payload_in: protocol.PayloadBuilder) !void {
        var payload = payload_in;
        var content = try payload.initContent();
        const fields = try content.initStruct(0, @intCast(self.refs.len));
        for (self.refs, 0..) |ref, index| {
            const any = try fields.getAnyPointer(index);
            if (ref.origin) |origin| {
                try any.setCapabilityOriginTagged(descriptors.originCodeForTag(origin), ref.id);
            } else {
                try any.setCapability(.{ .id = ref.id });
            }
        }
    }

    fn buildReturn(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        const self = castCtx(*const CapList, ctx_ptr);
        try self.write(try ret.payloadTyped());
    }
};

/// What a handler or Return callback does with the capabilities it receives.
const CapAction = enum {
    /// Record them; do not touch them.
    inspect,
    /// Record them and call `number` on each.
    call_each,
    /// Record them and retain every `.imported` one (the receiver then owns
    /// that reference and must release it).
    retain_imports,
};

/// The capabilities one payload delivered, and what calling each returned.
const Received = struct {
    len: usize = 0,
    caps: [max_caps]cap_table.ResolvedCap = undefined,
    reached: [max_caps]NumberCall = @splat(.{}),
    retained_imports: [max_caps]?u32 = @splat(null),

    fn take(
        self: *Received,
        peer: *Peer,
        content: capnpc.message.AnyPointerReader,
        caps: *const cap_table.InboundCapTable,
        action: CapAction,
    ) !void {
        const fields = try content.getStruct();
        var index: usize = 0;
        while (index < fields.pointer_count and index < max_caps) : (index += 1) {
            const cap = try fields.readCapability(index);
            const resolved = try caps.resolveCapability(cap);
            self.caps[index] = resolved;
            switch (action) {
                .inspect => {},
                .call_each => _ = try peer.sendCallResolved(
                    resolved,
                    interface_id,
                    number_method,
                    &self.reached[index],
                    null,
                    NumberCall.onReturn,
                ),
                .retain_imports => switch (resolved) {
                    .imported => |imported| {
                        var owned = caps.*;
                        try owned.retainCapability(cap);
                        self.retained_imports[index] = imported.id;
                    },
                    else => {},
                },
            }
        }
        self.len = index;
    }
};

// -- Servers -------------------------------------------------------------------

/// `number()` returns `base + calls`, so a test can tell which object a call
/// reached.
const Counter = struct {
    base: u32,
    calls: u32 = 0,

    fn exported(self: *Counter) capnpc.rpc.peer.Export {
        return .{ .ctx = self, .on_call = onCall };
    }

    fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        const self = castCtx(*Counter, ctx_ptr);
        if (call.method_id != number_method) return error.UnexpectedMethod;
        var reply = NumberReply{ .n = self.base + self.calls };
        self.calls += 1;
        try peer.sendReturnResults(call.question_id, &reply, NumberReply.build);
    }
};

const NumberReply = struct {
    n: u32,

    fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        const self = castCtx(*const NumberReply, ctx_ptr);
        var payload = try ret.payloadTyped();
        var content = try payload.initContent();
        const fields = try content.initStruct(1, 0);
        fields.writeU32(0, self.n);
    }
};

const NumberCall = struct {
    n: ?u32 = null,
    exception: bool = false,

    fn onReturn(ctx_ptr: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self = castCtx(*NumberCall, ctx_ptr);
        if (ret.tag != .results) {
            self.exception = true;
            return;
        }
        const results = ret.results orelse return error.MissingResults;
        self.n = (try results.content.getStruct()).readU32(0);
    }
};

/// `probe(caps...)`: records the capabilities in its params, acts on them,
/// and returns `results` as its own result capabilities.
const Probe = struct {
    action: CapAction = .inspect,
    results: []const CapRef = &.{},
    calls: u32 = 0,
    params: Received = .{},

    fn exported(self: *Probe) capnpc.rpc.peer.Export {
        return .{ .ctx = self, .on_call = onCall };
    }

    fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self = castCtx(*Probe, ctx_ptr);
        if (call.method_id != probe_method) return error.UnexpectedMethod;
        self.calls += 1;
        self.params = .{};
        try self.params.take(peer, call.params.content, caps, self.action);
        var reply = CapList{ .refs = self.results };
        try peer.sendReturnResults(call.question_id, &reply, CapList.buildReturn);
    }
};

/// A `probe` call: the capabilities it sends, and the caller's view of its
/// Return. One context serves both the build and the Return callback.
const ProbeCall = struct {
    params: []const CapRef = &.{},
    action: CapAction = .inspect,
    returned: bool = false,
    /// Every Return the callback saw; a call gets exactly one.
    returns: u32 = 0,
    exception: bool = false,
    results: Received = .{},

    fn onReturn(ctx_ptr: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self = castCtx(*ProbeCall, ctx_ptr);
        self.returned = true;
        self.returns += 1;
        if (ret.tag != .results) {
            self.exception = true;
            return;
        }
        const results = ret.results orelse return error.MissingResults;
        try self.results.take(peer, results.content, caps, self.action);
    }

    fn build(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const self = castCtx(*const ProbeCall, ctx_ptr);
        const list = CapList{ .refs = self.params };
        try list.write(try call.payloadTyped());
    }
};

const BootstrapCapture = struct {
    import_id: ?u32 = null,

    fn onReturn(ctx_ptr: *anyopaque, _: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self = castCtx(*BootstrapCapture, ctx_ptr);
        if (ret.tag != .results) return error.UnexpectedBootstrapReturn;
        const results = ret.results orelse return error.MissingResults;
        const cap = try results.content.getCapability();
        switch (try caps.resolveCapability(cap)) {
            .imported => |imported| {
                var owned = caps.*;
                try owned.retainCapability(cap);
                self.import_id = imported.id;
            },
            else => return error.UnexpectedBootstrapCapability,
        }
    }
};

// -- Fixture -------------------------------------------------------------------

/// Two peers on one in-process wire. The SERVER exports `home` (id 0) and its
/// bootstrap `probe` (id 1). The CLIENT's bootstrap is `decoy`; the server
/// bootstraps the client, so `decoy` lands as the server's import 0. The
/// server then holds export 0 and import 0 at once.
const Fixture = struct {
    wire: Wire,
    server: Peer,
    client: Peer,
    home: Counter = .{ .base = 1000 },
    solo: Counter = .{ .base = 2000 },
    decoy: Counter = .{ .base = 900 },
    probe: Probe = .{},
    home_id: u32 = undefined,
    probe_id: u32 = undefined,
    decoy_export_id: u32 = undefined,
    /// The server's import of the client's `decoy`.
    decoy_import_id: u32 = undefined,

    fn init(self: *Fixture, allocator: std.mem.Allocator) !void {
        self.* = .{
            .wire = .{ .allocator = allocator },
            .server = Peer.initDetached(allocator),
            .client = Peer.initDetached(allocator),
        };
        self.server.disableThreadAffinity();
        self.client.disableThreadAffinity();
        self.wire.client_peer = &self.client;
        self.wire.server_peer = &self.server;
        self.client.setSendFrameOverride(&self.wire, Wire.clientSend);
        self.server.setSendFrameOverride(&self.wire, Wire.serverSend);

        self.home_id = try self.server.addExport(self.home.exported());
        self.probe_id = try self.server.setBootstrap(self.probe.exported());
        self.decoy_export_id = try self.client.setBootstrap(self.decoy.exported());

        var boot = BootstrapCapture{};
        _ = try self.server.sendBootstrap(&boot, BootstrapCapture.onReturn);
        self.decoy_import_id = boot.import_id orelse return error.BootstrapDidNotResolve;

        // Precondition: the server's import of `decoy` collides with its own
        // export `home`.
        try std.testing.expectEqual(self.home_id, self.decoy_import_id);
        try std.testing.expect(self.server.caps.hasExport(self.home_id));
        try std.testing.expect(self.server.caps.hasImport(self.decoy_import_id));
        try self.expectBaselineRefs();
    }

    fn deinit(self: *Fixture) void {
        self.wire.forwarding = false;
        self.client.deinit();
        self.server.deinit();
        self.wire.deinit();
    }

    /// The server's own loopback call to `probe` with `params`.
    fn callProbe(self: *Fixture, params: []const CapRef, caller: *ProbeCall) !u32 {
        caller.params = params;
        return self.server.sendCallResolved(
            .{ .exported = .{ .id = self.probe_id } },
            interface_id,
            probe_method,
            caller,
            ProbeCall.build,
            ProbeCall.onReturn,
        );
    }

    /// Reference counts as the fixture set them up: the server holds one
    /// wire reference on `decoy` (the client counts it on its export), and
    /// no wire reference on its own exports.
    fn expectBaselineRefs(self: *Fixture) !void {
        try std.testing.expectEqual(@as(u32, 1), self.server.caps.imports.get(self.decoy_import_id).?.ref_count);
        try std.testing.expectEqual(@as(u32, 1), self.client.exports.get(self.decoy_export_id).?.ref_count);
        try std.testing.expectEqual(@as(u32, 0), self.server.exports.get(self.home_id).?.ref_count);
    }
};

fn expectExported(expected_id: u32, actual: cap_table.ResolvedCap) !void {
    switch (actual) {
        .exported => |exported| try std.testing.expectEqual(expected_id, exported.id),
        else => {
            std.debug.print("expected .exported{{ .id = {d} }}, got {any}\n", .{ expected_id, actual });
            return error.TestExpectedExportedCapability;
        },
    }
}

fn expectImported(expected_id: u32, actual: cap_table.ResolvedCap) !void {
    switch (actual) {
        .imported => |imported| try std.testing.expectEqual(expected_id, imported.id),
        else => {
            std.debug.print("expected .imported{{ .id = {d} }}, got {any}\n", .{ expected_id, actual });
            return error.TestExpectedImportedCapability;
        },
    }
}

fn expectNoFramesSince(wire: *const Wire, since: usize) !void {
    const sent = wire.countSince(.server_to_client, since);
    if (sent != 0) {
        std.debug.print("the loopback sent {d} frame(s) to the remote:\n", .{sent});
        wire.dump(since);
        return error.TestLoopbackReachedTheWire;
    }
}

// -- (a) Params: our export and our import with colliding ids ------------------

test "a loopback call hands the handler our export and our import, and each call reaches the right object" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    fx.probe.action = .call_each;
    var caller = ProbeCall{};
    _ = try fx.callProbe(&.{
        .{ .origin = .senderHosted, .id = fx.home_id },
        .{ .origin = .receiverHosted, .id = fx.decoy_import_id },
    }, &caller);

    try std.testing.expect(caller.returned);
    try std.testing.expect(!caller.exception);
    try std.testing.expectEqual(@as(usize, 2), fx.probe.params.len);
    // Our export `home` is a local capability...
    try expectExported(fx.home_id, fx.probe.params.caps[0]);
    try std.testing.expectEqual(@as(?u32, 1000), fx.probe.params.reached[0].n);
    // ...and our import is the client's `decoy`, reached over the wire.
    try expectImported(fx.decoy_import_id, fx.probe.params.caps[1]);
    try std.testing.expectEqual(@as(?u32, 900), fx.probe.params.reached[1].n);
    try std.testing.expectEqual(@as(u32, 1), fx.home.calls);
    try std.testing.expectEqual(@as(u32, 1), fx.decoy.calls);
    try fx.expectBaselineRefs();
}

// -- (b) Params: nothing reaches the remote; refcounts unchanged ---------------

test "a loopback call with capabilities in its params sends nothing to the remote and takes no import" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    // `solo` is an export with no import of the same id: read as the
    // remote's, it would become a brand-new (phantom) import.
    const solo_id = try fx.server.addExport(fx.solo.exported());
    try std.testing.expect(!fx.server.caps.hasImport(solo_id));

    const before = fx.wire.events.items.len;
    var caller = ProbeCall{};
    _ = try fx.callProbe(&.{
        .{ .origin = .senderHosted, .id = fx.home_id },
        .{ .origin = .receiverHosted, .id = fx.decoy_import_id },
        .{ .origin = null, .id = solo_id },
    }, &caller);

    try std.testing.expect(caller.returned);
    try std.testing.expect(!caller.exception);
    // No Call, Release, Finish or Disembargo went to the client.
    try expectNoFramesSince(&fx.wire, before);
    try std.testing.expect(!fx.server.caps.hasImport(solo_id));
    try std.testing.expectEqual(@as(u32, 0), fx.server.exports.get(solo_id).?.ref_count);
    try fx.expectBaselineRefs();
    try expectExported(fx.home_id, fx.probe.params.caps[0]);
    try expectImported(fx.decoy_import_id, fx.probe.params.caps[1]);
    try expectExported(solo_id, fx.probe.params.caps[2]);
}

test "a handler that keeps a loopback import param owns a reference the remote never granted" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    fx.probe.action = .retain_imports;
    const before = fx.wire.events.items.len;
    var caller = ProbeCall{};
    _ = try fx.callProbe(&.{
        .{ .origin = .receiverHosted, .id = fx.decoy_import_id },
    }, &caller);
    try std.testing.expect(caller.returned);
    try std.testing.expectEqual(@as(?u32, fx.decoy_import_id), fx.probe.params.retained_imports[0]);
    try expectNoFramesSince(&fx.wire, before);
    // The client still counts exactly the one reference it granted.
    try std.testing.expectEqual(@as(u32, 1), fx.client.exports.get(fx.decoy_export_id).?.ref_count);

    // Two holders now: the bootstrap's original reference and the handler's.
    // Whichever releases first sends nothing; the last one sends the single
    // Release the client is owed.
    try fx.server.releaseImport(fx.decoy_import_id, 1);
    try std.testing.expectEqual(@as(u32, 0), fx.wire.releasedTotal(.server_to_client, fx.decoy_import_id));
    try std.testing.expect(fx.server.caps.hasImport(fx.decoy_import_id));
    try fx.server.releaseImport(fx.decoy_import_id, 1);
    try std.testing.expectEqual(@as(u32, 1), fx.wire.releasedTotal(.server_to_client, fx.decoy_import_id));
    try std.testing.expect(!fx.server.caps.hasImport(fx.decoy_import_id));
    try std.testing.expectEqual(@as(u32, 0), fx.client.exports.get(fx.decoy_export_id).?.ref_count);
}

// -- (c) Results: our export and our import come back to the caller ------------

test "loopback results hand the caller our export and our import, and each call reaches the right object" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    const results = [_]CapRef{
        .{ .origin = .senderHosted, .id = fx.home_id },
        .{ .origin = .receiverHosted, .id = fx.decoy_import_id },
    };
    fx.probe.results = &results;
    var caller = ProbeCall{ .action = .call_each };
    _ = try fx.callProbe(&.{}, &caller);

    try std.testing.expect(caller.returned);
    try std.testing.expect(!caller.exception);
    try std.testing.expectEqual(@as(usize, 2), caller.results.len);
    try expectExported(fx.home_id, caller.results.caps[0]);
    try std.testing.expectEqual(@as(?u32, 1000), caller.results.reached[0].n);
    try expectImported(fx.decoy_import_id, caller.results.caps[1]);
    try std.testing.expectEqual(@as(?u32, 900), caller.results.reached[1].n);
    try fx.expectBaselineRefs();
}

test "loopback results send nothing to the remote and keep refcounts balanced" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    const solo_id = try fx.server.addExport(fx.solo.exported());
    const results = [_]CapRef{
        .{ .origin = .senderHosted, .id = fx.home_id },
        .{ .origin = .receiverHosted, .id = fx.decoy_import_id },
        .{ .origin = null, .id = solo_id },
    };
    fx.probe.results = &results;
    const before = fx.wire.events.items.len;
    var caller = ProbeCall{};
    _ = try fx.callProbe(&.{}, &caller);

    try std.testing.expect(caller.returned);
    try expectNoFramesSince(&fx.wire, before);
    try std.testing.expect(!fx.server.caps.hasImport(solo_id));
    try std.testing.expectEqual(@as(u32, 0), fx.server.exports.get(solo_id).?.ref_count);
    try fx.expectBaselineRefs();
    try expectExported(fx.home_id, caller.results.caps[0]);
    try expectImported(fx.decoy_import_id, caller.results.caps[1]);
    try expectExported(solo_id, caller.results.caps[2]);
}

test "a caller that keeps a loopback import result releases it without over-releasing" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    const results = [_]CapRef{.{ .origin = .receiverHosted, .id = fx.decoy_import_id }};
    fx.probe.results = &results;
    const before = fx.wire.events.items.len;
    var caller = ProbeCall{ .action = .retain_imports };
    _ = try fx.callProbe(&.{}, &caller);
    try std.testing.expectEqual(@as(?u32, fx.decoy_import_id), caller.results.retained_imports[0]);
    try expectNoFramesSince(&fx.wire, before);

    try fx.server.releaseImport(fx.decoy_import_id, 1);
    try fx.server.releaseImport(fx.decoy_import_id, 1);
    try std.testing.expectEqual(@as(u32, 1), fx.wire.releasedTotal(.server_to_client, fx.decoy_import_id));
    try std.testing.expect(!fx.server.caps.hasImport(fx.decoy_import_id));
    try std.testing.expectEqual(@as(u32, 0), fx.client.exports.get(fx.decoy_export_id).?.ref_count);
}

// -- (d) Promise caps in loopback params -----------------------------------------

test "a promise export in loopback params stays our promise and its call reaches the resolution" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    const promise_id = try fx.server.addPromiseExport();
    try std.testing.expect(!fx.server.caps.hasImport(promise_id));

    fx.probe.action = .call_each;
    const before = fx.wire.events.items.len;
    var caller = ProbeCall{};
    // A plain pointer: the encoder classifies the id as our promise export
    // and writes `senderPromise`.
    _ = try fx.callProbe(&.{.{ .origin = null, .id = promise_id }}, &caller);
    try std.testing.expect(caller.returned);
    try expectExported(promise_id, fx.probe.params.caps[0]);
    // The handler's call parks on the unresolved promise.
    try std.testing.expectEqual(@as(?u32, null), fx.probe.params.reached[0].n);
    try std.testing.expect(!fx.probe.params.reached[0].exception);
    try expectNoFramesSince(&fx.wire, before);
    try std.testing.expect(!fx.server.caps.hasImport(promise_id));
    try std.testing.expectEqual(@as(u32, 0), fx.server.exports.get(promise_id).?.ref_count);

    // Resolving replays the parked call onto `home`. (Resolving a promise
    // export also announces it with a Resolve, which the client, holding no
    // such promise, drops by releasing the reference it carried.)
    const resolve_at = fx.wire.events.items.len;
    try fx.server.resolvePromiseExportToExport(promise_id, fx.home_id);
    try std.testing.expectEqual(@as(?u32, 1000), fx.probe.params.reached[0].n);
    for (fx.wire.events.items[resolve_at..]) |event| {
        if (event.dir == .server_to_client and event.tag != .resolve) {
            fx.wire.dump(resolve_at);
            return error.TestLoopbackReachedTheWire;
        }
    }
    try fx.expectBaselineRefs();
}

test "a pipelined capability in loopback params fails closed before the handler runs" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    // A promise on one of OUR questions to the client. A loopback receiver
    // would read `receiverAnswer` as a promise on one of ITS answers, a
    // different id space: refuse instead of guessing.
    const ops = [_]protocol.PromisedAnswerOp{.{ .tag = .getPointerField, .pointer_index = 0 }};
    const pipelined_id = try fx.server.caps.noteReceiverAnswerOps(7, &ops);

    const before = fx.wire.events.items.len;
    const questions_before = fx.server.questions.count();
    var caller = ProbeCall{};
    try std.testing.expectError(
        error.LoopbackPromisedCapabilityUnsupported,
        fx.callProbe(&.{.{ .origin = .receiverAnswer, .id = pipelined_id }}, &caller),
    );
    try std.testing.expectEqual(@as(u32, 0), fx.probe.calls);
    try std.testing.expect(!caller.returned);
    try std.testing.expectEqual(questions_before, fx.server.questions.count());
    try std.testing.expectEqual(@as(usize, 0), fx.server.loopback_questions.count());
    // The pipelined capability is still ours to send elsewhere.
    try std.testing.expect(fx.server.caps.hasReceiverAnswer(pipelined_id));
    try expectNoFramesSince(&fx.wire, before);
    try fx.expectBaselineRefs();
}

test "a pipelined capability in loopback results fails the call with an exception" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    const ops = [_]protocol.PromisedAnswerOp{.{ .tag = .getPointerField, .pointer_index = 0 }};
    const pipelined_id = try fx.server.caps.noteReceiverAnswerOps(7, &ops);
    const results = [_]CapRef{.{ .origin = .receiverAnswer, .id = pipelined_id }};
    fx.probe.results = &results;

    const before = fx.wire.events.items.len;
    var caller = ProbeCall{};
    _ = try fx.callProbe(&.{}, &caller);
    // The handler ran, but its Return could not carry the capability: the
    // caller gets an exception instead.
    try std.testing.expectEqual(@as(u32, 1), fx.probe.calls);
    try std.testing.expect(caller.returned);
    try std.testing.expect(caller.exception);
    try std.testing.expect(fx.server.caps.hasReceiverAnswer(pipelined_id));
    try std.testing.expectEqual(@as(usize, 0), fx.server.loopback_questions.count());
    try expectNoFramesSince(&fx.wire, before);
    try fx.expectBaselineRefs();
}

// -- The resolved-import fast path of sendCall -----------------------------------

test "sendCall on an import that resolved to our own export delivers loopback params the same way" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();

    // The client hands the server a promise, then resolves it to the server's
    // own `probe` (an import on the client). The server's import of that
    // promise now resolves to its export `probe`: `sendCall` on it takes the
    // loopback.
    var client_boot = BootstrapCapture{};
    _ = try fx.client.sendBootstrap(&client_boot, BootstrapCapture.onReturn);
    const probe_import_on_client = client_boot.import_id orelse return error.BootstrapDidNotResolve;
    const client_promise = try fx.client.addPromiseExport();

    fx.probe.action = .retain_imports;
    var hand_over = ProbeCall{ .params = &.{.{ .origin = null, .id = client_promise }} };
    _ = try fx.client.sendCall(
        probe_import_on_client,
        interface_id,
        probe_method,
        &hand_over,
        ProbeCall.build,
        ProbeCall.onReturn,
    );
    const promise_import = fx.probe.params.retained_imports[0] orelse return error.PromiseNotHandedOver;
    // Resolve over a queued wire, as on a real transport: the server's
    // Disembargo must find the resolution it stores after sending it.
    fx.wire.queueing = true;
    try fx.client.resolvePromiseExportToImport(client_promise, probe_import_on_client);
    try fx.wire.flush();
    const resolved = fx.server.resolved_imports.get(promise_import) orelse return error.PromiseNotResolved;
    try std.testing.expect(!resolved.embargoed);
    try expectExported(fx.probe_id, resolved.cap.?);

    fx.probe.action = .call_each;
    var caller = ProbeCall{ .params = &.{
        .{ .origin = .senderHosted, .id = fx.home_id },
        .{ .origin = .receiverHosted, .id = fx.decoy_import_id },
    } };
    const calls_before = fx.wire.events.items.len;
    _ = try fx.server.sendCall(promise_import, interface_id, probe_method, &caller, ProbeCall.build, ProbeCall.onReturn);

    try std.testing.expect(caller.returned);
    try std.testing.expect(!caller.exception);
    try expectExported(fx.home_id, fx.probe.params.caps[0]);
    try std.testing.expectEqual(@as(?u32, 1000), fx.probe.params.reached[0].n);
    try expectImported(fx.decoy_import_id, fx.probe.params.caps[1]);
    try std.testing.expectEqual(@as(?u32, 900), fx.probe.params.reached[1].n);
    // The probe call itself went nowhere; only the handler's call to the
    // client's `decoy` (Call out, Finish after its Return) used the wire.
    var probe_calls_on_wire: usize = 0;
    for (fx.wire.events.items[calls_before..]) |event| {
        if (event.dir == .server_to_client and event.tag == .release) return error.TestLoopbackReleasedOnTheWire;
        if (event.dir == .server_to_client and event.tag == .call) probe_calls_on_wire += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), probe_calls_on_wire);
    try fx.expectBaselineRefs();
}

// -- (e) Capabilities the peer does not hold -----------------------------------
//
// An origin-tagged capability pointer is never checked against the cap table
// when it is written: a generated local Client (origin `.exported`) kept past
// its export's removal and written with `setXClient` names an export that no
// longer exists. The loopback encoder refuses such a descriptor itself, the
// way `onOutboundCap` refuses one for the wire, before the call is
// dispatched or the Return leaves the handler. Otherwise only the loopback
// decode notices, after the Return has consumed the loopback marker, and
// the exception that follows goes to the remote under the loopback answer
// id.

/// An id that names neither an export nor an import on the server.
const stale_id: u32 = 77;

fn expectStaleId(fx: *const Fixture) !void {
    try std.testing.expect(!fx.server.caps.hasExport(stale_id));
    try std.testing.expect(!fx.server.caps.hasImport(stale_id));
}

/// A loopback call that failed: the caller saw exactly one Return, an
/// exception; nothing reached the remote; no question or loopback marker is
/// left; and the fixture's references are as they were.
fn expectFailedLocally(fx: *Fixture, caller: *const ProbeCall, questions_before: usize, events_before: usize) !void {
    try std.testing.expectEqual(@as(u32, 1), caller.returns);
    try std.testing.expect(caller.exception);
    try expectNoFramesSince(&fx.wire, events_before);
    try std.testing.expectEqual(questions_before, fx.server.questions.count());
    try std.testing.expectEqual(@as(usize, 0), fx.server.loopback_questions.count());
    try fx.expectBaselineRefs();
}

/// A loopback call whose params name a capability the peer does not hold
/// fails synchronously, before the handler runs.
fn expectParamsRefused(origin: protocol.CapDescriptorTag, expected: anyerror) !void {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();
    try expectStaleId(&fx);

    const before = fx.wire.events.items.len;
    const questions_before = fx.server.questions.count();
    var caller = ProbeCall{};
    try std.testing.expectError(expected, fx.callProbe(&.{.{ .origin = origin, .id = stale_id }}, &caller));
    try std.testing.expectEqual(@as(u32, 0), fx.probe.calls);
    try std.testing.expectEqual(@as(u32, 0), caller.returns);
    try std.testing.expectEqual(questions_before, fx.server.questions.count());
    try std.testing.expectEqual(@as(usize, 0), fx.server.loopback_questions.count());
    try expectNoFramesSince(&fx.wire, before);
    try fx.expectBaselineRefs();
}

test "a loopback call whose params name an export the peer does not hold fails before dispatch" {
    try expectParamsRefused(.senderHosted, error.UnknownExport);
}

test "a loopback call whose params name an import the peer does not hold fails before dispatch" {
    try expectParamsRefused(.receiverHosted, error.UnknownImport);
}

/// A handler whose loopback results name a capability the peer does not
/// hold: its `sendReturnResults` fails, the handler lets the error out of
/// `on_call`, and the caller receives an exception Return.
fn expectResultsRefused(origin: protocol.CapDescriptorTag, queued_wire: bool) !void {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();
    try expectStaleId(&fx);

    const results = [_]CapRef{.{ .origin = origin, .id = stale_id }};
    fx.probe.results = &results;
    const before = fx.wire.events.items.len;
    const questions_before = fx.server.questions.count();
    // A queued wire holds what the server sends until `flush`, as a real
    // transport would: nothing may be waiting there either.
    fx.wire.queueing = queued_wire;
    var caller = ProbeCall{};
    _ = try fx.callProbe(&.{}, &caller);
    try std.testing.expectEqual(@as(usize, 0), fx.wire.queue.items.len);
    try fx.wire.flush();
    try std.testing.expectEqual(@as(u32, 1), fx.probe.calls);
    try expectFailedLocally(&fx, &caller, questions_before, before);
}

test "a loopback Return whose results name an export the peer does not hold fails the call locally" {
    try expectResultsRefused(.senderHosted, false);
}

test "a loopback Return whose results name an export the peer does not hold fails the call locally over a queued wire" {
    try expectResultsRefused(.senderHosted, true);
}

test "a loopback Return whose results name an import the peer does not hold fails the call locally" {
    try expectResultsRefused(.receiverHosted, false);
}

/// Answers each call later, from the test, instead of inside `on_call`.
const DeferredProbe = struct {
    pending: ?u32 = null,

    fn exported(self: *DeferredProbe) capnpc.rpc.peer.Export {
        return .{ .ctx = self, .on_call = onCall };
    }

    fn onCall(ctx_ptr: *anyopaque, _: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        const self = castCtx(*DeferredProbe, ctx_ptr);
        self.pending = call.question_id;
    }
};

test "an async handler whose loopback results name a stale export gets the error and settles the call itself" {
    var fx: Fixture = undefined;
    try fx.init(std.testing.allocator);
    defer fx.deinit();
    try expectStaleId(&fx);

    var deferred = DeferredProbe{};
    const deferred_id = try fx.server.addExport(deferred.exported());
    const before = fx.wire.events.items.len;
    const questions_before = fx.server.questions.count();
    var caller = ProbeCall{};
    _ = try fx.server.sendCallResolved(
        .{ .exported = .{ .id = deferred_id } },
        interface_id,
        probe_method,
        &caller,
        ProbeCall.build,
        ProbeCall.onReturn,
    );
    const answer_id = deferred.pending orelse return error.TestHandlerDidNotRun;

    var reply = CapList{ .refs = &.{.{ .origin = .senderHosted, .id = stale_id }} };
    try std.testing.expectError(error.UnknownExport, fx.server.sendReturnResults(answer_id, &reply, CapList.buildReturn));
    // The call is still open, and still a loopback one...
    try std.testing.expectEqual(@as(u32, 0), caller.returns);
    try std.testing.expect(fx.server.loopback_questions.contains(answer_id));
    // ...so the handler's own exception Return reaches the caller here.
    try fx.server.sendReturnException(answer_id, "results unavailable");
    try expectFailedLocally(&fx, &caller, questions_before, before);
}
