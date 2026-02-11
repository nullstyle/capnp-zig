const std = @import("std");
const capnpc = @import("capnpc-zig");

const cap_table = capnpc.rpc.cap_table;
const message = capnpc.message;
const protocol = capnpc.rpc.protocol;
const peer_control = capnpc.rpc._internal.peer_control;

const adoptThirdPartyAnswer = peer_control.adoptThirdPartyAnswer;
const allocateEmbargoIdForPeerFn = peer_control.allocateEmbargoIdForPeerFn;
const captureAnyPointerPayloadForPeerFn = peer_control.captureAnyPointerPayloadForPeerFn;
const clearResolvedImportEmbargoForPeerFn = peer_control.clearResolvedImportEmbargoForPeerFn;
const freeOwnedFrameForPeerFn = peer_control.freeOwnedFrameForPeerFn;
const handleBootstrap = peer_control.handleBootstrap;
const handleFinish = peer_control.handleFinish;
const handleUnimplementedQuestionForPeerFn = peer_control.handleUnimplementedQuestionForPeerFn;
const hasKnownResolvePromiseForPeerFn = peer_control.hasKnownResolvePromiseForPeerFn;
const noteCallSendResultsForPeerFn = peer_control.noteCallSendResultsForPeerFn;
const rememberPendingEmbargoForPeerFn = peer_control.rememberPendingEmbargoForPeerFn;
const resolveCapDescriptorForPeerFn = peer_control.resolveCapDescriptorForPeerFn;
const resolveProvideImportedCapForPeerFn = peer_control.resolveProvideImportedCapForPeerFn;
const resolveProvidePromisedAnswerForPeerFn = peer_control.resolveProvidePromisedAnswerForPeerFn;
const resolveProvideTargetForPeerFn = peer_control.resolveProvideTargetForPeerFn;
const setForwardedCallThirdPartyFromPayloadForPeerFn = peer_control.setForwardedCallThirdPartyFromPayloadForPeerFn;
const takePendingEmbargoPromiseForPeerFn = peer_control.takePendingEmbargoPromiseForPeerFn;
const takeResolvedAnswerFrameForPeerFn = peer_control.takeResolvedAnswerFrameForPeerFn;

test "peer_control resolve/disembargo peer helper factories operate on peer state" {
    const FakeResolvedImport = struct {
        cap: ?cap_table.ResolvedCap = null,
        embargo_id: ?u32 = null,
        embargoed: bool = false,
    };

    const FakePeer = struct {
        caps: cap_table.CapTable,
        pending_embargoes: std.AutoHashMap(u32, u32),
        resolved_imports: std.AutoHashMap(u32, FakeResolvedImport),
        next_embargo_id: u32 = 0,
    };

    var peer = FakePeer{
        .caps = cap_table.CapTable.init(std.testing.allocator),
        .pending_embargoes = std.AutoHashMap(u32, u32).init(std.testing.allocator),
        .resolved_imports = std.AutoHashMap(u32, FakeResolvedImport).init(std.testing.allocator),
    };
    defer {
        peer.pending_embargoes.deinit();
        peer.resolved_imports.deinit();
        peer.caps.deinit();
    }

    const has_known = hasKnownResolvePromiseForPeerFn(FakePeer);
    try std.testing.expect(!has_known(&peer, 7));
    try peer.caps.noteImport(7);
    try std.testing.expect(has_known(&peer, 7));

    const resolve_descriptor = resolveCapDescriptorForPeerFn(FakePeer);
    const resolved_none = try resolve_descriptor(&peer, .{ .tag = .none });
    try std.testing.expect(resolved_none == .none);

    const alloc_embargo_id = allocateEmbargoIdForPeerFn(FakePeer);
    const remember_pending = rememberPendingEmbargoForPeerFn(FakePeer);
    const take_pending = takePendingEmbargoPromiseForPeerFn(FakePeer);
    const clear_embargo = clearResolvedImportEmbargoForPeerFn(FakePeer);

    const first_id = alloc_embargo_id(&peer);
    const second_id = alloc_embargo_id(&peer);
    try std.testing.expectEqual(@as(u32, 0), first_id);
    try std.testing.expectEqual(@as(u32, 1), second_id);
    try remember_pending(&peer, first_id, 41);
    try remember_pending(&peer, second_id, 42);
    try std.testing.expectEqual(@as(?u32, 41), take_pending(&peer, first_id));
    try std.testing.expectEqual(@as(?u32, null), take_pending(&peer, first_id));
    try std.testing.expectEqual(@as(?u32, 42), take_pending(&peer, second_id));

    peer.next_embargo_id = std.math.maxInt(u32);
    try std.testing.expectEqual(std.math.maxInt(u32), alloc_embargo_id(&peer));
    try std.testing.expectEqual(@as(u32, 0), alloc_embargo_id(&peer));

    try peer.resolved_imports.put(9, .{
        .cap = .none,
        .embargo_id = 123,
        .embargoed = true,
    });
    clear_embargo(&peer, 9);
    const cleared = peer.resolved_imports.get(9) orelse return error.MissingResolvedImport;
    try std.testing.expect(!cleared.embargoed);
    try std.testing.expectEqual(@as(?u32, null), cleared.embargo_id);

    clear_embargo(&peer, 12345);
}

test "peer_control noteCallSendResultsForPeerFn routes to yourself and third-party handlers" {
    const State = struct {
        yourself_calls: usize = 0,
        third_party_calls: usize = 0,

        fn noteYourself(self: *@This(), answer_id: u32) !void {
            _ = answer_id;
            self.yourself_calls += 1;
        }

        fn noteThirdParty(self: *@This(), answer_id: u32, maybe_ptr: ?message.AnyPointerReader) !void {
            _ = answer_id;
            _ = maybe_ptr;
            self.third_party_calls += 1;
        }
    };

    const note_send_results = noteCallSendResultsForPeerFn(
        State,
        State.noteYourself,
        State.noteThirdParty,
    );

    var state = State{};
    try note_send_results(&state, .{
        .question_id = 1,
        .target = .{
            .tag = .imported_cap,
            .imported_cap = 0,
            .promised_answer = null,
        },
        .interface_id = 0,
        .method_id = 0,
        .params = .{
            .content = undefined,
            .cap_table = null,
        },
        .send_results_to = .{
            .tag = .caller,
            .third_party = null,
        },
        .allow_third_party_tail = false,
        .no_promise_pipelining = false,
        .only_promise_pipeline = false,
    });
    try std.testing.expectEqual(@as(usize, 0), state.yourself_calls);
    try std.testing.expectEqual(@as(usize, 0), state.third_party_calls);

    try note_send_results(&state, .{
        .question_id = 2,
        .target = .{
            .tag = .imported_cap,
            .imported_cap = 0,
            .promised_answer = null,
        },
        .interface_id = 0,
        .method_id = 0,
        .params = .{
            .content = undefined,
            .cap_table = null,
        },
        .send_results_to = .{
            .tag = .yourself,
            .third_party = null,
        },
        .allow_third_party_tail = false,
        .no_promise_pipelining = false,
        .only_promise_pipeline = false,
    });
    try std.testing.expectEqual(@as(usize, 1), state.yourself_calls);
    try std.testing.expectEqual(@as(usize, 0), state.third_party_calls);

    try note_send_results(&state, .{
        .question_id = 3,
        .target = .{
            .tag = .imported_cap,
            .imported_cap = 0,
            .promised_answer = null,
        },
        .interface_id = 0,
        .method_id = 0,
        .params = .{
            .content = undefined,
            .cap_table = null,
        },
        .send_results_to = .{
            .tag = .third_party,
            .third_party = null,
        },
        .allow_third_party_tail = false,
        .no_promise_pipelining = false,
        .only_promise_pipeline = false,
    });
    try std.testing.expectEqual(@as(usize, 1), state.yourself_calls);
    try std.testing.expectEqual(@as(usize, 1), state.third_party_calls);
}

test "peer_control forwarded third-party/capture helper factories use peer allocator and payload fields" {
    const FakePeer = struct {
        allocator: std.mem.Allocator,
        capture_calls: usize = 0,
        saw_non_null_ptr: bool = false,
    };
    const Hooks = struct {
        fn capture(allocator: std.mem.Allocator, ptr: ?message.AnyPointerReader) !?[]u8 {
            _ = ptr;
            return try allocator.dupe(u8, "captured");
        }
    };

    var peer = FakePeer{
        .allocator = std.testing.allocator,
    };

    var third_party_builder = message.MessageBuilder.init(std.testing.allocator);
    defer third_party_builder.deinit();
    const third_party_root = try third_party_builder.initRootAnyPointer();
    try third_party_root.setText("destination");
    const third_party_payload = try third_party_builder.toBytes();
    defer std.testing.allocator.free(third_party_payload);

    const capture_any = captureAnyPointerPayloadForPeerFn(FakePeer, Hooks.capture);
    var third_party_msg = try message.Message.init(std.testing.allocator, third_party_payload);
    defer third_party_msg.deinit();
    const third_party_ptr = try third_party_msg.getRootAnyPointer();
    const captured = try capture_any(&peer, third_party_ptr);
    defer std.testing.allocator.free(captured.?);
    try std.testing.expectEqualStrings("captured", captured.?);

    var call_builder_msg = protocol.MessageBuilder.init(std.testing.allocator);
    defer call_builder_msg.deinit();
    var call_builder = try call_builder_msg.beginCall(77, 0xAA, 2);
    try call_builder.setTargetImportedCap(0);
    _ = try call_builder.payloadBuilder();

    const set_third_party = setForwardedCallThirdPartyFromPayloadForPeerFn(FakePeer);
    try set_third_party(&peer, &call_builder, third_party_payload);

    const encoded = try call_builder_msg.finish();
    defer std.testing.allocator.free(encoded);

    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, encoded);
    defer decoded.deinit();
    const call = try decoded.asCall();
    try std.testing.expectEqual(protocol.SendResultsToTag.third_party, call.send_results_to.tag);
    const payload_ptr = call.send_results_to.third_party orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualStrings("destination", try payload_ptr.getText());
}

test "peer_control provide-target helper factories resolve imported and promised targets with peer state" {
    const FakeExportEntry = struct {
        is_promise: bool = false,
        resolved: ?cap_table.ResolvedCap = null,
    };
    const FakePeer = struct {
        exports: std.AutoHashMap(u32, FakeExportEntry),
        promised_mode: enum { none_resolved, imported_resolved, failure } = .none_resolved,

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .exports = std.AutoHashMap(u32, FakeExportEntry).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            self.exports.deinit();
        }

        fn resolvePromisedAnswer(self: *@This(), promised: protocol.PromisedAnswer) !cap_table.ResolvedCap {
            _ = promised;
            return switch (self.promised_mode) {
                .none_resolved => .none,
                .imported_resolved => .{ .imported = .{ .id = 77 } },
                .failure => error.TestExpectedError,
            };
        }
    };

    var peer = FakePeer.init(std.testing.allocator);
    defer peer.deinit();

    const resolve_imported = resolveProvideImportedCapForPeerFn(FakePeer);
    const resolve_promised = resolveProvidePromisedAnswerForPeerFn(FakePeer, FakePeer.resolvePromisedAnswer);
    const resolve_target = resolveProvideTargetForPeerFn(
        FakePeer,
        resolveProvideImportedCapForPeerFn(FakePeer),
        resolveProvidePromisedAnswerForPeerFn(FakePeer, FakePeer.resolvePromisedAnswer),
    );

    try std.testing.expectError(error.UnknownExport, resolve_imported(&peer, 1));

    try peer.exports.put(2, .{ .is_promise = false, .resolved = null });
    const exported = try resolve_imported(&peer, 2);
    switch (exported) {
        .exported => |cap| try std.testing.expectEqual(@as(u32, 2), cap.id),
        else => return error.TestExpectedEqual,
    }

    try peer.exports.put(3, .{ .is_promise = true, .resolved = null });
    try std.testing.expectError(error.PromiseUnresolved, resolve_imported(&peer, 3));

    try peer.exports.put(3, .{ .is_promise = true, .resolved = .none });
    try std.testing.expectError(error.PromiseBroken, resolve_imported(&peer, 3));

    try peer.exports.put(3, .{ .is_promise = true, .resolved = .{ .imported = .{ .id = 41 } } });
    const promised_import = try resolve_imported(&peer, 3);
    switch (promised_import) {
        .imported => |cap| try std.testing.expectEqual(@as(u32, 41), cap.id),
        else => return error.TestExpectedEqual,
    }

    const promised = protocol.PromisedAnswer{
        .question_id = 9,
        .transform = .{ .list = null },
    };
    try std.testing.expectError(error.PromisedAnswerMissing, resolve_promised(&peer, promised));

    peer.promised_mode = .imported_resolved;
    const promised_resolved = try resolve_promised(&peer, promised);
    switch (promised_resolved) {
        .imported => |cap| try std.testing.expectEqual(@as(u32, 77), cap.id),
        else => return error.TestExpectedEqual,
    }

    peer.promised_mode = .failure;
    try std.testing.expectError(error.TestExpectedError, resolve_promised(&peer, promised));

    const imported_target = try resolve_target(&peer, .{
        .tag = .imported_cap,
        .imported_cap = 2,
        .promised_answer = null,
    });
    switch (imported_target) {
        .exported => |cap| try std.testing.expectEqual(@as(u32, 2), cap.id),
        else => return error.TestExpectedEqual,
    }

    try std.testing.expectError(
        error.MissingCallTarget,
        resolve_target(&peer, .{
            .tag = .imported_cap,
            .imported_cap = null,
            .promised_answer = null,
        }),
    );
    try std.testing.expectError(
        error.MissingPromisedAnswer,
        resolve_target(&peer, .{
            .tag = .promised_answer,
            .imported_cap = null,
            .promised_answer = null,
        }),
    );
}

test "peer_control handleBootstrap sends exception when bootstrap export is not configured" {
    const State = struct {
        send_return_exception_calls: usize = 0,
        exception_question_id: u32 = 0,
        exception_reason: ?[]const u8 = null,
        note_export_ref_calls: usize = 0,
        send_frame_calls: usize = 0,
        record_resolved_answer_calls: usize = 0,
        allocator: std.mem.Allocator,

        fn noteExportRef(state: *@This(), export_id: u32) !void {
            _ = export_id;
            state.note_export_ref_calls += 1;
        }

        fn sendReturnException(state: *@This(), question_id: u32, reason: []const u8) !void {
            state.send_return_exception_calls += 1;
            state.exception_question_id = question_id;
            state.exception_reason = reason;
        }

        fn sendFrame(state: *@This(), frame: []const u8) !void {
            _ = frame;
            state.send_frame_calls += 1;
        }

        fn recordResolvedAnswer(state: *@This(), question_id: u32, frame: []u8) !void {
            _ = question_id;
            state.record_resolved_answer_calls += 1;
            state.allocator.free(frame);
        }
    };

    var state = State{ .allocator = std.testing.allocator };
    try handleBootstrap(
        State,
        &state,
        std.testing.allocator,
        .{
            .question_id = 91,
            .deprecated_object = null,
        },
        null,
        State.noteExportRef,
        State.sendReturnException,
        State.sendFrame,
        State.recordResolvedAnswer,
    );

    try std.testing.expectEqual(@as(usize, 1), state.send_return_exception_calls);
    try std.testing.expectEqual(@as(u32, 91), state.exception_question_id);
    try std.testing.expectEqualStrings("bootstrap not configured", state.exception_reason orelse "");
    try std.testing.expectEqual(@as(usize, 0), state.note_export_ref_calls);
    try std.testing.expectEqual(@as(usize, 0), state.send_frame_calls);
    try std.testing.expectEqual(@as(usize, 0), state.record_resolved_answer_calls);
}

test "peer_control handleBootstrap sends frame and records resolved bootstrap answer" {
    const State = struct {
        allocator: std.mem.Allocator,
        note_export_ref_calls: usize = 0,
        noted_export_id: ?u32 = null,
        send_return_exception_calls: usize = 0,
        send_frame_calls: usize = 0,
        sent_frame: ?[]u8 = null,
        record_resolved_answer_calls: usize = 0,
        recorded_question_id: ?u32 = null,
        recorded_frame: ?[]u8 = null,

        fn deinit(state: *@This()) void {
            if (state.sent_frame) |bytes| state.allocator.free(bytes);
            if (state.recorded_frame) |bytes| state.allocator.free(bytes);
        }

        fn noteExportRef(state: *@This(), export_id: u32) !void {
            state.note_export_ref_calls += 1;
            state.noted_export_id = export_id;
        }

        fn sendReturnException(state: *@This(), question_id: u32, reason: []const u8) !void {
            _ = question_id;
            _ = reason;
            state.send_return_exception_calls += 1;
        }

        fn sendFrame(state: *@This(), frame: []const u8) !void {
            state.send_frame_calls += 1;
            const copy = try state.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            state.sent_frame = copy;
        }

        fn recordResolvedAnswer(state: *@This(), question_id: u32, frame: []u8) !void {
            state.record_resolved_answer_calls += 1;
            state.recorded_question_id = question_id;
            state.recorded_frame = frame;
        }
    };

    var state = State{
        .allocator = std.testing.allocator,
    };
    defer state.deinit();

    try handleBootstrap(
        State,
        &state,
        std.testing.allocator,
        .{
            .question_id = 7,
            .deprecated_object = null,
        },
        1234,
        State.noteExportRef,
        State.sendReturnException,
        State.sendFrame,
        State.recordResolvedAnswer,
    );

    try std.testing.expectEqual(@as(usize, 1), state.note_export_ref_calls);
    try std.testing.expectEqual(@as(?u32, 1234), state.noted_export_id);
    try std.testing.expectEqual(@as(usize, 0), state.send_return_exception_calls);
    try std.testing.expectEqual(@as(usize, 1), state.send_frame_calls);
    try std.testing.expectEqual(@as(usize, 1), state.record_resolved_answer_calls);
    try std.testing.expectEqual(@as(?u32, 7), state.recorded_question_id);
    try std.testing.expect(state.sent_frame != null);
    try std.testing.expect(state.recorded_frame != null);
    try std.testing.expectEqualSlices(u8, state.sent_frame.?, state.recorded_frame.?);
}

test "peer_control frame helper factories take and free resolved answer frame" {
    const FakeResolvedAnswer = struct {
        frame: []u8,
    };

    const FakePeer = struct {
        allocator: std.mem.Allocator,
        resolved_answers: std.AutoHashMap(u32, FakeResolvedAnswer),
    };

    var peer = FakePeer{
        .allocator = std.testing.allocator,
        .resolved_answers = std.AutoHashMap(u32, FakeResolvedAnswer).init(std.testing.allocator),
    };
    defer peer.resolved_answers.deinit();

    const frame = try std.testing.allocator.alloc(u8, 3);
    frame[0] = 1;
    frame[1] = 2;
    frame[2] = 3;
    try peer.resolved_answers.put(55, .{ .frame = frame });

    const take_frame = takeResolvedAnswerFrameForPeerFn(FakePeer);
    const free_frame = freeOwnedFrameForPeerFn(FakePeer);

    const removed = take_frame(&peer, 55) orelse return error.MissingResolvedImport;
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 2, 3 }, removed);
    try std.testing.expectEqual(@as(usize, 0), peer.resolved_answers.count());
    free_frame(&peer, removed);

    try std.testing.expectEqual(@as(?[]u8, null), take_frame(&peer, 55));
}

test "peer_control handleFinish runs clear, tail-forward, and resolved cleanup" {
    const State = struct {
        expected_question_id: u32,
        clear_calls: usize = 0,
        tail_question_id: ?u32 = null,
        send_finish_calls: usize = 0,
        last_finish_question_id: u32 = 0,
        last_finish_release_result_caps: bool = true,
        resolved_frame: ?[]u8 = null,
        release_caps_calls: usize = 0,
        free_frame_calls: usize = 0,
    };

    const Hooks = struct {
        fn noteClear(state: *State, question_id: u32) void {
            std.testing.expectEqual(state.expected_question_id, question_id) catch unreachable;
            state.clear_calls += 1;
        }

        fn removeSendResultsToYourself(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearSendResultsToThirdParty(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearProvide(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearPendingJoinQuestion(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearPendingAcceptQuestion(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn takeForwardedTailQuestion(state: *State, question_id: u32) ?u32 {
            std.testing.expectEqual(state.expected_question_id, question_id) catch unreachable;
            return state.tail_question_id;
        }

        fn sendFinish(state: *State, question_id: u32, release_result_caps: bool) !void {
            state.send_finish_calls += 1;
            state.last_finish_question_id = question_id;
            state.last_finish_release_result_caps = release_result_caps;
        }

        fn takeResolvedAnswerFrame(state: *State, question_id: u32) ?[]u8 {
            std.testing.expectEqual(state.expected_question_id, question_id) catch unreachable;
            const frame = state.resolved_frame;
            state.resolved_frame = null;
            return frame;
        }

        fn releaseCapsForFrame(state: *State, frame: []const u8) !void {
            state.release_caps_calls += 1;
            try std.testing.expectEqualSlices(u8, &[_]u8{ 9, 8, 7 }, frame);
        }

        fn freeFrame(state: *State, frame: []u8) void {
            std.testing.expectEqualSlices(u8, &[_]u8{ 9, 8, 7 }, frame) catch unreachable;
            state.free_frame_calls += 1;
        }
    };

    var frame_storage = [_]u8{ 9, 8, 7 };
    var state = State{
        .expected_question_id = 51,
        .tail_question_id = 88,
        .resolved_frame = frame_storage[0..],
    };

    try handleFinish(
        State,
        &state,
        51,
        true,
        Hooks.removeSendResultsToYourself,
        Hooks.clearSendResultsToThirdParty,
        Hooks.clearProvide,
        Hooks.clearPendingJoinQuestion,
        Hooks.clearPendingAcceptQuestion,
        Hooks.takeForwardedTailQuestion,
        Hooks.sendFinish,
        Hooks.takeResolvedAnswerFrame,
        Hooks.releaseCapsForFrame,
        Hooks.freeFrame,
    );

    try std.testing.expectEqual(@as(usize, 5), state.clear_calls);
    try std.testing.expectEqual(@as(usize, 1), state.send_finish_calls);
    try std.testing.expectEqual(@as(u32, 88), state.last_finish_question_id);
    try std.testing.expectEqual(false, state.last_finish_release_result_caps);
    try std.testing.expectEqual(@as(usize, 1), state.release_caps_calls);
    try std.testing.expectEqual(@as(usize, 1), state.free_frame_calls);
}

test "peer_control handleFinish skips optional tail and resolved cleanup when absent" {
    const State = struct {
        expected_question_id: u32,
        clear_calls: usize = 0,
        send_finish_calls: usize = 0,
        release_caps_calls: usize = 0,
        free_frame_calls: usize = 0,
    };

    const Hooks = struct {
        fn noteClear(state: *State, question_id: u32) void {
            std.testing.expectEqual(state.expected_question_id, question_id) catch unreachable;
            state.clear_calls += 1;
        }

        fn removeSendResultsToYourself(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearSendResultsToThirdParty(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearProvide(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearPendingJoinQuestion(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearPendingAcceptQuestion(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn takeForwardedTailQuestion(state: *State, question_id: u32) ?u32 {
            std.testing.expectEqual(state.expected_question_id, question_id) catch unreachable;
            return null;
        }

        fn sendFinish(state: *State, question_id: u32, release_result_caps: bool) !void {
            _ = question_id;
            _ = release_result_caps;
            state.send_finish_calls += 1;
        }

        fn takeResolvedAnswerFrame(state: *State, question_id: u32) ?[]u8 {
            std.testing.expectEqual(state.expected_question_id, question_id) catch unreachable;
            return null;
        }

        fn releaseCapsForFrame(state: *State, frame: []const u8) !void {
            _ = frame;
            state.release_caps_calls += 1;
        }

        fn freeFrame(state: *State, frame: []u8) void {
            _ = frame;
            state.free_frame_calls += 1;
        }
    };

    var state = State{ .expected_question_id = 19 };
    try handleFinish(
        State,
        &state,
        19,
        true,
        Hooks.removeSendResultsToYourself,
        Hooks.clearSendResultsToThirdParty,
        Hooks.clearProvide,
        Hooks.clearPendingJoinQuestion,
        Hooks.clearPendingAcceptQuestion,
        Hooks.takeForwardedTailQuestion,
        Hooks.sendFinish,
        Hooks.takeResolvedAnswerFrame,
        Hooks.releaseCapsForFrame,
        Hooks.freeFrame,
    );

    try std.testing.expectEqual(@as(usize, 5), state.clear_calls);
    try std.testing.expectEqual(@as(usize, 0), state.send_finish_calls);
    try std.testing.expectEqual(@as(usize, 0), state.release_caps_calls);
    try std.testing.expectEqual(@as(usize, 0), state.free_frame_calls);
}

test "peer_control adoptThirdPartyAnswer rejects invalid adopted answer id" {
    const Question = struct { marker: u32 };
    const State = struct {
        abort_reason: ?[]const u8 = null,
    };

    const Hooks = struct {
        fn hasQuestion(_: *State, _: u32) bool {
            return false;
        }

        fn hasAdopted(_: *State, _: u32) bool {
            return false;
        }

        fn sendAbort(state: *State, reason: []const u8) !void {
            state.abort_reason = reason;
        }

        fn putQuestion(_: *State, _: u32, _: Question) !void {
            return error.TestUnexpectedResult;
        }

        fn removeQuestion(_: *State, _: u32) void {}

        fn putAdopted(_: *State, _: u32, _: u32) !void {
            return error.TestUnexpectedResult;
        }

        fn removeAdopted(_: *State, _: u32) void {}

        fn takePendingReturn(_: *State, _: u32) ?[]u8 {
            return null;
        }

        fn freeFrame(_: *State, _: []u8) void {}

        fn handlePendingReturn(_: *State, _: []const u8) !void {
            return error.TestUnexpectedResult;
        }
    };

    var state = State{};
    const err = adoptThirdPartyAnswer(
        State,
        Question,
        &state,
        55,
        1,
        .{ .marker = 123 },
        Hooks.hasQuestion,
        Hooks.hasAdopted,
        Hooks.sendAbort,
        Hooks.putQuestion,
        Hooks.removeQuestion,
        Hooks.putAdopted,
        Hooks.removeAdopted,
        Hooks.takePendingReturn,
        Hooks.freeFrame,
        Hooks.handlePendingReturn,
    );
    try std.testing.expectError(error.InvalidThirdPartyAnswerId, err);
    try std.testing.expectEqualStrings("invalid thirdPartyAnswer answerId", state.abort_reason orelse "");
}

test "peer_control adoptThirdPartyAnswer stores mapping and replays pending return frame" {
    const Question = struct { marker: u32 };
    const State = struct {
        has_question: bool = false,
        has_adopted: bool = false,
        put_question_calls: usize = 0,
        put_adopted_calls: usize = 0,
        remove_question_calls: usize = 0,
        remove_adopted_calls: usize = 0,
        stored_answer_id: u32 = 0,
        stored_question: Question = .{ .marker = 0 },
        stored_adopted_answer_id: u32 = 0,
        stored_original_question_id: u32 = 0,
        pending_frame: ?[]u8 = null,
        handled_pending_frames: usize = 0,
        freed_pending_frames: usize = 0,
        abort_reason: ?[]const u8 = null,
    };

    const Hooks = struct {
        fn hasQuestion(state: *State, answer_id: u32) bool {
            _ = answer_id;
            return state.has_question;
        }

        fn hasAdopted(state: *State, answer_id: u32) bool {
            _ = answer_id;
            return state.has_adopted;
        }

        fn sendAbort(state: *State, reason: []const u8) !void {
            state.abort_reason = reason;
        }

        fn putQuestion(state: *State, answer_id: u32, question: Question) !void {
            state.has_question = true;
            state.put_question_calls += 1;
            state.stored_answer_id = answer_id;
            state.stored_question = question;
        }

        fn removeQuestion(state: *State, _: u32) void {
            state.has_question = false;
            state.remove_question_calls += 1;
        }

        fn putAdopted(state: *State, adopted_answer_id: u32, question_id: u32) !void {
            state.has_adopted = true;
            state.put_adopted_calls += 1;
            state.stored_adopted_answer_id = adopted_answer_id;
            state.stored_original_question_id = question_id;
        }

        fn removeAdopted(state: *State, _: u32) void {
            state.has_adopted = false;
            state.remove_adopted_calls += 1;
        }

        fn takePendingReturn(state: *State, _: u32) ?[]u8 {
            const frame = state.pending_frame;
            state.pending_frame = null;
            return frame;
        }

        fn freeFrame(state: *State, frame: []u8) void {
            std.testing.expectEqualSlices(u8, &[_]u8{ 0xDE, 0xAD }, frame) catch unreachable;
            state.freed_pending_frames += 1;
        }

        fn handlePendingReturn(state: *State, frame: []const u8) !void {
            try std.testing.expectEqualSlices(u8, &[_]u8{ 0xDE, 0xAD }, frame);
            state.handled_pending_frames += 1;
        }
    };

    var pending_storage = [_]u8{ 0xDE, 0xAD };
    var state = State{
        .pending_frame = pending_storage[0..],
    };
    const adopted_answer_id: u32 = 0x4000_0101;
    const original_question_id: u32 = 77;
    const adopted_question = Question{ .marker = 999 };

    try adoptThirdPartyAnswer(
        State,
        Question,
        &state,
        original_question_id,
        adopted_answer_id,
        adopted_question,
        Hooks.hasQuestion,
        Hooks.hasAdopted,
        Hooks.sendAbort,
        Hooks.putQuestion,
        Hooks.removeQuestion,
        Hooks.putAdopted,
        Hooks.removeAdopted,
        Hooks.takePendingReturn,
        Hooks.freeFrame,
        Hooks.handlePendingReturn,
    );

    try std.testing.expectEqual(@as(usize, 1), state.put_question_calls);
    try std.testing.expectEqual(@as(usize, 1), state.put_adopted_calls);
    try std.testing.expectEqual(adopted_answer_id, state.stored_answer_id);
    try std.testing.expectEqual(adopted_answer_id, state.stored_adopted_answer_id);
    try std.testing.expectEqual(original_question_id, state.stored_original_question_id);
    try std.testing.expectEqual(adopted_question.marker, state.stored_question.marker);
    try std.testing.expectEqual(@as(usize, 0), state.remove_question_calls);
    try std.testing.expectEqual(@as(usize, 0), state.remove_adopted_calls);
    try std.testing.expectEqual(@as(usize, 1), state.handled_pending_frames);
    try std.testing.expectEqual(@as(usize, 1), state.freed_pending_frames);
    try std.testing.expect(state.abort_reason == null);
}

test "peer_control handleUnimplementedQuestionForPeerFn builds exception return" {
    const State = struct {
        calls: usize = 0,
        answer_id: u32 = 0,
        tag: protocol.ReturnTag = .canceled,
        reason: []const u8 = "",
        frame_len: usize = 0,
    };

    const Hooks = struct {
        fn onReturn(state: *State, frame: []const u8, ret: protocol.Return) !void {
            state.calls += 1;
            state.answer_id = ret.answer_id;
            state.tag = ret.tag;
            state.reason = ret.exception.?.reason;
            state.frame_len = frame.len;
        }
    };

    var state = State{};
    const callback = handleUnimplementedQuestionForPeerFn(State, Hooks.onReturn);
    try callback(&state, 44);

    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expectEqual(@as(u32, 44), state.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, state.tag);
    try std.testing.expectEqual(@as(usize, 0), state.frame_len);
    try std.testing.expectEqualStrings("unimplemented", state.reason);
}

test "peer_control handleUnimplementedQuestionForPeerFn ignores unknown question return error" {
    const State = struct {
        calls: usize = 0,
    };

    const Hooks = struct {
        fn onReturn(state: *State, frame: []const u8, ret: protocol.Return) !void {
            _ = frame;
            _ = ret;
            state.calls += 1;
            return error.UnknownQuestion;
        }
    };

    var state = State{};
    const callback = handleUnimplementedQuestionForPeerFn(State, Hooks.onReturn);
    try callback(&state, 55);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
}
