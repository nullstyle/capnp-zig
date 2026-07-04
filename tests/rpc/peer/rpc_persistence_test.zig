const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.caps.table;
const events = capnpc.rpc.events;
const rpc_time = capnpc.rpc.time;
const persistence = peer_impl.persistence;
const Peer = peer_impl.Peer;

const Capture = struct {
    allocator: std.mem.Allocator,
    frames: std.ArrayList([]u8),

    fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
        const copy = try ctx.allocator.alloc(u8, frame.len);
        std.mem.copyForwards(u8, copy, frame);
        try ctx.frames.append(ctx.allocator, copy);
    }

    fn deinit(self: *@This()) void {
        for (self.frames.items) |frame| self.allocator.free(frame);
        self.frames.deinit(self.allocator);
    }
};

const EventRecorder = struct {
    persistent_export_pressure: usize = 0,
    sturdy_ref_rejections: usize = 0,

    fn onEvent(ctx: *anyopaque, event: events.Event) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        switch (event) {
            .pressure => |pressure| {
                if (pressure.resource == .persistent_exports) self.persistent_export_pressure += 1;
            },
            .resource_rejection => |rejection| {
                if (rejection.resource == .sturdy_ref_bytes) self.sturdy_ref_rejections += 1;
            },
            else => {},
        }
    }

    fn observer(self: *@This()) events.Observer {
        return events.Observer.init(self, onEvent);
    }
};

/// Application-side export handler that records the calls it receives and
/// answers each with an empty struct.
const AppHandler = struct {
    calls: usize = 0,
    last_interface_id: u64 = 0,

    fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
        _ = caps;
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        self.last_interface_id = call.interface_id;
        try peer.sendReturnEmptyStruct(call.question_id);
    }
};

const SaveRegistry = struct {
    sturdy_ref: []const u8,
    saves: usize = 0,
    saw_seal_for: bool = false,
    fail_with: ?anyerror = null,

    fn onSave(
        ctx: *anyopaque,
        peer: *Peer,
        export_id: u32,
        seal_for: ?capnpc.message.AnyPointerReader,
    ) anyerror![]u8 {
        _ = export_id;
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (self.fail_with) |err| return err;
        self.saves += 1;
        self.saw_seal_for = seal_for != null;
        const copy = try peer.allocator.alloc(u8, self.sturdy_ref.len);
        std.mem.copyForwards(u8, copy, self.sturdy_ref);
        return copy;
    }
};

const Restorer = struct {
    known_ref: []const u8,
    handler: *AppHandler,
    /// When set, answer `.existing` with this export instead of hosting a
    /// fresh one.
    existing_export_id: ?u32 = null,

    fn onRestore(ctx: *anyopaque, peer: *Peer, sturdy_ref: []const u8) anyerror!peer_impl.RestoreOutcome {
        _ = peer;
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (!std.mem.eql(u8, sturdy_ref, self.known_ref)) return .unknown;
        if (self.existing_export_id) |id| return .{ .existing = id };
        return .{ .host = .{ .ctx = self.handler, .on_call = AppHandler.onCall } };
    }
};

fn buildCallFrame(
    allocator: std.mem.Allocator,
    question_id: u32,
    target_export_id: u32,
    interface_id: u64,
    method_id: u16,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(question_id, interface_id, method_id);
    try call.setTargetImportedCap(target_export_id);
    try persistence.writeEmptySaveParams(&call);
    _ = try call.initCapTableTyped(0);
    return builder.finish();
}

fn buildSaveCallFrame(allocator: std.mem.Allocator, question_id: u32, target_export_id: u32) ![]const u8 {
    return buildCallFrame(
        allocator,
        question_id,
        target_export_id,
        persistence.persistent_interface_id,
        persistence.save_method_id,
    );
}

fn buildRestoreCallFrame(
    allocator: std.mem.Allocator,
    question_id: u32,
    target_export_id: u32,
    sturdy_ref: []const u8,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(question_id, persistence.restorer_interface_id, persistence.restore_method_id);
    try call.setTargetImportedCap(target_export_id);
    try persistence.writeRestoreParams(&call, sturdy_ref);
    _ = try call.initCapTableTyped(0);
    return builder.finish();
}

/// Expect frame `index` to be an exception Return for `answer_id` whose
/// reason equals `reason`.
fn expectExceptionReturn(capture: *Capture, index: usize, answer_id: u32, reason: []const u8) !void {
    var decoded = try protocol.DecodedMessage.init(capture.allocator, capture.frames.items[index]);
    defer decoded.deinit();
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(answer_id, ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const exception = ret.exception orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(reason, exception.reason);
}

test "save call on a persistent export returns the sturdy ref payload" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var app = AppHandler{};
    var registry = SaveRegistry{ .sturdy_ref = "ref:bootstrap/1" };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const export_id = try peer.setBootstrap(.{ .ctx = &app, .on_call = AppHandler.onCall });
    try peer.setPersistentExport(export_id, &registry, SaveRegistry.onSave);
    try std.testing.expectEqual(@as(u32, 1), peer.stats().persistent_exports);

    const frame = try buildSaveCallFrame(allocator, 42, export_id);
    defer allocator.free(frame);
    try peer.handleFrame(frame);

    try std.testing.expectEqual(@as(usize, 1), registry.saves);
    try std.testing.expect(!registry.saw_seal_for);
    try std.testing.expectEqual(@as(usize, 0), app.calls);
    try std.testing.expectEqual(@as(u64, 1), peer.stats().saves_served);

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 42), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
    const payload = ret.results orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("ref:bootstrap/1", try persistence.readSturdyRefResult(payload));
}

test "save handler error becomes an exception Return" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var app = AppHandler{};
    var registry = SaveRegistry{ .sturdy_ref = "", .fail_with = error.SaveRefused };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const export_id = try peer.setBootstrap(.{ .ctx = &app, .on_call = AppHandler.onCall });
    try peer.setPersistentExport(export_id, &registry, SaveRegistry.onSave);

    const frame = try buildSaveCallFrame(allocator, 7, export_id);
    defer allocator.free(frame);
    try peer.handleFrame(frame);

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    try expectExceptionReturn(&capture, 0, 7, "SaveRefused");
    try std.testing.expectEqual(@as(u64, 0), peer.stats().saves_served);
}

test "oversized save payload is rejected with an exception" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var event_recorder = EventRecorder{};
    var app = AppHandler{};
    var registry = SaveRegistry{ .sturdy_ref = "way too long" };

    var peer = Peer.initDetachedWithLimits(allocator, .{ .max_sturdy_ref_bytes = 4 });
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setObserver(event_recorder.observer());

    const export_id = try peer.setBootstrap(.{ .ctx = &app, .on_call = AppHandler.onCall });
    try peer.setPersistentExport(export_id, &registry, SaveRegistry.onSave);

    const frame = try buildSaveCallFrame(allocator, 8, export_id);
    defer allocator.free(frame);
    try peer.handleFrame(frame);

    try expectExceptionReturn(&capture, 0, 8, "sturdy ref too large");
    try std.testing.expectEqual(@as(usize, 1), event_recorder.sturdy_ref_rejections);
}

test "non-persistence interfaces fall through to the original handler" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var app = AppHandler{};
    var registry = SaveRegistry{ .sturdy_ref = "ref:x" };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const export_id = try peer.setBootstrap(.{ .ctx = &app, .on_call = AppHandler.onCall });
    try peer.setPersistentExport(export_id, &registry, SaveRegistry.onSave);

    const app_interface_id: u64 = 0x1234_5678_9abc_def0;
    const frame = try buildCallFrame(allocator, 9, export_id, app_interface_id, 3);
    defer allocator.free(frame);
    try peer.handleFrame(frame);

    try std.testing.expectEqual(@as(usize, 1), app.calls);
    try std.testing.expectEqual(app_interface_id, app.last_interface_id);
    try std.testing.expectEqual(@as(usize, 0), registry.saves);
}

test "save call on an unmarked export reaches the app handler" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var app = AppHandler{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const export_id = try peer.setBootstrap(.{ .ctx = &app, .on_call = AppHandler.onCall });

    const frame = try buildSaveCallFrame(allocator, 10, export_id);
    defer allocator.free(frame);
    try peer.handleFrame(frame);

    try std.testing.expectEqual(@as(usize, 1), app.calls);
    try std.testing.expectEqual(persistence.persistent_interface_id, app.last_interface_id);
}

test "unknown method on the persistent interface is rejected" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var app = AppHandler{};
    var registry = SaveRegistry{ .sturdy_ref = "ref:x" };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const export_id = try peer.setBootstrap(.{ .ctx = &app, .on_call = AppHandler.onCall });
    try peer.setPersistentExport(export_id, &registry, SaveRegistry.onSave);

    const frame = try buildCallFrame(allocator, 11, export_id, persistence.persistent_interface_id, 9);
    defer allocator.free(frame);
    try peer.handleFrame(frame);

    try expectExceptionReturn(&capture, 0, 11, "unknown method");
    try std.testing.expectEqual(@as(usize, 0), app.calls);
}

test "clearPersistentExport restores the original handler" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var app = AppHandler{};
    var registry = SaveRegistry{ .sturdy_ref = "ref:x" };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const export_id = try peer.setBootstrap(.{ .ctx = &app, .on_call = AppHandler.onCall });
    try peer.setPersistentExport(export_id, &registry, SaveRegistry.onSave);
    peer.clearPersistentExport(export_id);
    try std.testing.expectEqual(@as(u32, 0), peer.stats().persistent_exports);

    const frame = try buildSaveCallFrame(allocator, 12, export_id);
    defer allocator.free(frame);
    try peer.handleFrame(frame);

    try std.testing.expectEqual(@as(usize, 1), app.calls);
    try std.testing.expectEqual(@as(usize, 0), registry.saves);
}

test "persistent export registry enforces its budget and emits pressure" {
    const allocator = std.testing.allocator;

    var event_recorder = EventRecorder{};
    var app = AppHandler{};
    var registry = SaveRegistry{ .sturdy_ref = "ref:x" };

    var peer = Peer.initDetachedWithLimits(allocator, .{ .max_persistent_exports = 1 });
    defer peer.deinit();
    peer.setObserver(event_recorder.observer());

    const first = try peer.addExport(.{ .ctx = &app, .on_call = AppHandler.onCall });
    const second = try peer.addExport(.{ .ctx = &app, .on_call = AppHandler.onCall });

    try peer.setPersistentExport(first, &registry, SaveRegistry.onSave);
    try std.testing.expectEqual(@as(usize, 1), event_recorder.persistent_export_pressure);
    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer.setPersistentExport(second, &registry, SaveRegistry.onSave),
    );
    try std.testing.expectEqual(@as(u32, 1), peer.stats().persistent_exports);
}

test "setRestorer requires a bootstrap export" {
    const allocator = std.testing.allocator;

    var app = AppHandler{};
    var restorer = Restorer{ .known_ref = "ref:x", .handler = &app };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    try std.testing.expectError(
        error.BootstrapNotConfigured,
        peer.setRestorer(&restorer, Restorer.onRestore),
    );
}

test "restorer hosts a fresh export for a known sturdy ref" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var app = AppHandler{};
    var restored_app = AppHandler{};
    var restorer = Restorer{ .known_ref = "ref:echo", .handler = &restored_app };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const bootstrap_id = try peer.setBootstrap(.{ .ctx = &app, .on_call = AppHandler.onCall });
    try peer.setRestorer(&restorer, Restorer.onRestore);

    const frame = try buildRestoreCallFrame(allocator, 21, bootstrap_id, "ref:echo");
    defer allocator.free(frame);
    try peer.handleFrame(frame);

    try std.testing.expectEqual(@as(u64, 1), peer.stats().restores_served);
    try std.testing.expectEqual(@as(u32, 2), peer.stats().exports);

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
    const payload = ret.results orelse return error.TestUnexpectedResult;
    const cap_list = payload.cap_table orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 1), cap_list.len());

    // The restored capability answers calls through its own handler.
    const echo_frame = try buildCallFrame(allocator, 22, bootstrap_id + 1, 0xabcd, 0);
    defer allocator.free(echo_frame);
    try peer.handleFrame(echo_frame);
    try std.testing.expectEqual(@as(usize, 1), restored_app.calls);
    try std.testing.expectEqual(@as(usize, 0), app.calls);
}

test "restorer can re-expose an existing export" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var app = AppHandler{};
    var shared = AppHandler{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const bootstrap_id = try peer.setBootstrap(.{ .ctx = &app, .on_call = AppHandler.onCall });
    const shared_id = try peer.addExport(.{ .ctx = &shared, .on_call = AppHandler.onCall });
    var restorer = Restorer{ .known_ref = "ref:shared", .handler = &shared, .existing_export_id = shared_id };
    try peer.setRestorer(&restorer, Restorer.onRestore);

    const exports_before = peer.stats().exports;
    const frame = try buildRestoreCallFrame(allocator, 23, bootstrap_id, "ref:shared");
    defer allocator.free(frame);
    try peer.handleFrame(frame);

    try std.testing.expectEqual(exports_before, peer.stats().exports);
    try std.testing.expectEqual(@as(u64, 1), peer.stats().restores_served);
}

test "unknown sturdy ref and oversized refs are rejected" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var event_recorder = EventRecorder{};
    var app = AppHandler{};
    var restorer = Restorer{ .known_ref = "ref:echo", .handler = &app };

    var peer = Peer.initDetachedWithLimits(allocator, .{ .max_sturdy_ref_bytes = 16 });
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setObserver(event_recorder.observer());

    const bootstrap_id = try peer.setBootstrap(.{ .ctx = &app, .on_call = AppHandler.onCall });
    try peer.setRestorer(&restorer, Restorer.onRestore);

    const unknown_frame = try buildRestoreCallFrame(allocator, 31, bootstrap_id, "ref:other");
    defer allocator.free(unknown_frame);
    try peer.handleFrame(unknown_frame);
    try expectExceptionReturn(&capture, 0, 31, "unknown sturdy ref");

    const oversized_frame = try buildRestoreCallFrame(allocator, 32, bootstrap_id, "ref:waaaaaay-too-long");
    defer allocator.free(oversized_frame);
    try peer.handleFrame(oversized_frame);
    try expectExceptionReturn(&capture, 1, 32, "sturdy ref too large");
    try std.testing.expectEqual(@as(usize, 1), event_recorder.sturdy_ref_rejections);
    try std.testing.expectEqual(@as(u64, 0), peer.stats().restores_served);
}

const SaveResponseRecorder = struct {
    allocator: std.mem.Allocator,
    sturdy_ref: ?[]u8 = null,
    exception_reason: ?[]u8 = null,
    other: ?protocol.ReturnTag = null,

    fn onResponse(ctx: *anyopaque, peer: *Peer, response: peer_impl.SaveResponse) anyerror!void {
        _ = peer;
        const self: *@This() = @ptrCast(@alignCast(ctx));
        switch (response) {
            .sturdy_ref => |bytes| self.sturdy_ref = try self.allocator.dupe(u8, bytes),
            .exception => |exception| self.exception_reason = try self.allocator.dupe(u8, exception.reason),
            .other => |tag| self.other = tag,
        }
    }

    fn deinit(self: *@This()) void {
        if (self.sturdy_ref) |bytes| self.allocator.free(bytes);
        if (self.exception_reason) |bytes| self.allocator.free(bytes);
    }
};

const RestoreResponseRecorder = struct {
    allocator: std.mem.Allocator,
    cap: ?cap_table.ResolvedCap = null,
    exception_reason: ?[]u8 = null,

    fn onResponse(ctx: *anyopaque, peer: *Peer, response: peer_impl.RestoreResponse) anyerror!void {
        _ = peer;
        const self: *@This() = @ptrCast(@alignCast(ctx));
        switch (response) {
            .cap => |resolved| self.cap = resolved,
            .exception => |exception| self.exception_reason = try self.allocator.dupe(u8, exception.reason),
            .other => {},
        }
    }

    fn deinit(self: *@This()) void {
        if (self.exception_reason) |bytes| self.allocator.free(bytes);
    }
};

fn buildSaveResultsReturnFrame(allocator: std.mem.Allocator, answer_id: u32, sturdy_ref: []const u8) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var ret = try builder.beginReturn(answer_id, .results);
    var build_ctx = persistence.SturdyRefReturnContext{ .sturdy_ref = sturdy_ref };
    try persistence.buildSaveResultsReturn(&build_ctx, &ret);
    _ = try ret.initCapTableTyped(0);
    return builder.finish();
}

fn buildRestoreResultsReturnFrame(allocator: std.mem.Allocator, answer_id: u32, hosted_export_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var ret = try builder.beginReturn(answer_id, .results);
    var payload = try ret.payloadTyped();
    const any = try payload.initContent();
    const results = try any.initStruct(0, 1);
    const field = try results.getAnyPointer(0);
    try field.setCapability(.{ .id = 0 });
    var cap_list = try ret.initCapTableTyped(1);
    var entry = try cap_list.get(0);
    try entry.setSenderHosted(hosted_export_id);
    return builder.finish();
}

fn buildExceptionReturnFrame(allocator: std.mem.Allocator, answer_id: u32, reason: []const u8) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var ret = try builder.beginReturn(answer_id, .exception);
    try ret.setException(reason);
    return builder.finish();
}

test "sendSave emits a Persistent.save call and surfaces the sturdy ref" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = SaveResponseRecorder{ .allocator = allocator };
    defer recorder.deinit();

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const question_id = try peer.sendSave(77, &recorder, SaveResponseRecorder.onResponse);

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    {
        var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
        defer decoded.deinit();
        const call = try decoded.asCall();
        try std.testing.expectEqual(persistence.persistent_interface_id, call.interface_id);
        try std.testing.expectEqual(persistence.save_method_id, call.method_id);
        try std.testing.expectEqual(protocol.MessageTargetTag.importedCap, call.target.tag);
        try std.testing.expectEqual(@as(?u32, 77), call.target.imported_cap);
    }

    const return_frame = try buildSaveResultsReturnFrame(allocator, question_id, "ref:wire");
    defer allocator.free(return_frame);
    try peer.handleFrame(return_frame);

    const sturdy_ref = recorder.sturdy_ref orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("ref:wire", sturdy_ref);
}

test "sendSave surfaces an exception response" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = SaveResponseRecorder{ .allocator = allocator };
    defer recorder.deinit();

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const question_id = try peer.sendSave(5, &recorder, SaveResponseRecorder.onResponse);
    const return_frame = try buildExceptionReturnFrame(allocator, question_id, "not persistent");
    defer allocator.free(return_frame);
    try peer.handleFrame(return_frame);

    const reason = recorder.exception_reason orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("not persistent", reason);
}

test "sendSave deadline expiry delivers the exception via cancel machinery" {
    const allocator = std.testing.allocator;

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = SaveResponseRecorder{ .allocator = allocator };
    defer recorder.deinit();

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setClock(clock.clock());
    peer.setTimeouts(.{ .default_call_timeout_ms = 10 });

    _ = try peer.sendSave(6, &recorder, SaveResponseRecorder.onResponse);
    clock.advanceMs(11);
    try std.testing.expectEqual(@as(usize, 1), peer.checkDeadlines());

    const reason = recorder.exception_reason orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("deadline exceeded", reason);
}

test "outstanding sendSave question is cleaned up on peer deinit" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = SaveResponseRecorder{ .allocator = allocator };
    defer recorder.deinit();

    var peer = Peer.initDetached(allocator);
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    _ = try peer.sendSave(8, &recorder, SaveResponseRecorder.onResponse);
    // No Return is ever delivered; deinit must release the question context.
    peer.deinit();
}

test "sendRestore round-trips and retains the restored import" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = RestoreResponseRecorder{ .allocator = allocator };
    defer recorder.deinit();

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const question_id = try peer.sendRestore(3, "sturdy:echo", &recorder, RestoreResponseRecorder.onResponse);

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    {
        var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
        defer decoded.deinit();
        const call = try decoded.asCall();
        try std.testing.expectEqual(persistence.restorer_interface_id, call.interface_id);
        try std.testing.expectEqual(persistence.restore_method_id, call.method_id);
        try std.testing.expectEqual(@as(?u32, 3), call.target.imported_cap);
        try std.testing.expectEqualStrings("sturdy:echo", try persistence.readSturdyRefParam(call.params));
    }

    const return_frame = try buildRestoreResultsReturnFrame(allocator, question_id, 99);
    defer allocator.free(return_frame);
    try peer.handleFrame(return_frame);

    const resolved = recorder.cap orelse return error.TestUnexpectedResult;
    switch (resolved) {
        .imported => |imported| try std.testing.expectEqual(@as(u32, 99), imported.id),
        else => return error.TestUnexpectedResult,
    }
    // The import was retained for the application.
    try std.testing.expect(peer.caps.imports.contains(99));
}

test "sendRestore surfaces an exception response" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = RestoreResponseRecorder{ .allocator = allocator };
    defer recorder.deinit();

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const question_id = try peer.sendRestore(4, "sturdy:gone", &recorder, RestoreResponseRecorder.onResponse);
    const return_frame = try buildExceptionReturnFrame(allocator, question_id, "unknown sturdy ref");
    defer allocator.free(return_frame);
    try peer.handleFrame(return_frame);

    const reason = recorder.exception_reason orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("unknown sturdy ref", reason);
    try std.testing.expectEqual(@as(?cap_table.ResolvedCap, null), recorder.cap);
}

test "released persistent export drops its persistence state" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var app = AppHandler{};
    var shared = AppHandler{};
    var registry = SaveRegistry{ .sturdy_ref = "ref:x" };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const bootstrap_id = try peer.setBootstrap(.{ .ctx = &app, .on_call = AppHandler.onCall });
    // A plain (non-bootstrap) export so a Release can fully remove it.
    const shared_id = try peer.addExport(.{ .ctx = &shared, .on_call = AppHandler.onCall });
    try peer.setPersistentExport(shared_id, &registry, SaveRegistry.onSave);
    var restorer = Restorer{ .known_ref = "ref:x", .handler = &shared, .existing_export_id = shared_id };
    try peer.setRestorer(&restorer, Restorer.onRestore);
    try std.testing.expectEqual(@as(u32, 2), peer.stats().persistent_exports);

    // Serving a restore hands the remote a reference to the shared export.
    const restore_frame = try buildRestoreCallFrame(allocator, 50, bootstrap_id, "ref:x");
    defer allocator.free(restore_frame);
    try peer.handleFrame(restore_frame);

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildRelease(shared_id, 1);
    const release_frame = try builder.finish();
    defer allocator.free(release_frame);
    try peer.handleFrame(release_frame);

    // The Release spends the wire ref, but the still-unfinished restore
    // answer (question 50) pins the export as a pipeline target, so the
    // entry (and its persistence state) survives until the Finish.
    try std.testing.expect(peer.exports.contains(shared_id));
    try std.testing.expectEqual(@as(u32, 2), peer.stats().persistent_exports);

    var finish_builder = protocol.MessageBuilder.init(allocator);
    defer finish_builder.deinit();
    try finish_builder.buildFinish(50, false, false);
    const finish_frame = try finish_builder.finish();
    defer allocator.free(finish_frame);
    try peer.handleFrame(finish_frame);

    try std.testing.expect(!peer.exports.contains(shared_id));
    try std.testing.expectEqual(@as(u32, 1), peer.stats().persistent_exports);
}
