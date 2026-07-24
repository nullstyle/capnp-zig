const std = @import("std");
const capnpc = @import("capnpc-zig");

const cap_table = capnpc.rpc.caps.table;
const message = capnpc.message;
const protocol = capnpc.rpc.wire.protocol;
const peer_bootstrap = capnpc.rpc.testing.peer_bootstrap;
const peer_disembargo = capnpc.rpc.testing.peer_disembargo;
const peer_embargo_accepts = capnpc.rpc.testing.peer_embargo_accepts;
const peer_finish = capnpc.rpc.testing.peer_finish;
const peer_provide_accept_join = capnpc.rpc.testing.peer_provide_accept_join;
const peer_provide_join_orchestration = capnpc.rpc.testing.peer_provide_join_orchestration;
const peer_resolve = capnpc.rpc.testing.peer_resolve;
const peer_return_dispatch = capnpc.rpc.testing.peer_return_dispatch;
const peer_return_orchestration = capnpc.rpc.testing.peer_return_orchestration;
const peer_third_party = capnpc.rpc.testing.peer_third_party;

const allocateEmbargoIdForPeerFn = peer_resolve.allocateEmbargoIdForPeerFn;
const captureAnyPointerPayloadForPeerFn = peer_third_party.captureAnyPointerPayloadForPeerFn;
const clearResolvedImportEmbargoForPeerFn = peer_disembargo.clearResolvedImportEmbargoForPeerFn;
const freeOwnedFrameForPeerFn = peer_finish.freeOwnedFrameForPeerFn;
const forgetPendingEmbargoForPeerFn = peer_resolve.forgetPendingEmbargoForPeerFn;
const handleBootstrap = peer_bootstrap.handleBootstrap;
const handleFinish = peer_finish.handleFinish;
const handleUnimplementedQuestionForPeerFn = peer_bootstrap.handleUnimplementedQuestionForPeerFn;
const hasKnownResolvePromiseForPeerFn = peer_resolve.hasKnownResolvePromiseForPeerFn;
const noteCallSendResultsForPeerFn = peer_third_party.noteCallSendResultsForPeerFn;
const rememberPendingEmbargoForPeerFn = peer_resolve.rememberPendingEmbargoForPeerFn;
const resolveCapDescriptorForPeerFn = peer_resolve.resolveCapDescriptorForPeerFn;
const resolveProvideImportedCapForPeerFn = peer_provide_accept_join.resolveProvideImportedCapForPeerFn;
const resolveProvidePromisedAnswerForPeerFn = peer_provide_accept_join.resolveProvidePromisedAnswerForPeerFn;
const resolveProvideTargetForPeerFn = peer_provide_accept_join.resolveProvideTargetForPeerFn;
const setForwardedCallThirdPartyFromPayloadForPeerFn = peer_third_party.setForwardedCallThirdPartyFromPayloadForPeerFn;
const takePendingEmbargoPromiseForPeerFn = peer_disembargo.takePendingEmbargoPromiseForPeerFn;
const takeResolvedAnswerFrameForPeerFn = peer_finish.takeResolvedAnswerFrameForPeerFn;

test "peer resolve/disembargo helper factories operate on peer state" {
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
    const forget_pending = forgetPendingEmbargoForPeerFn(FakePeer);
    const take_pending = takePendingEmbargoPromiseForPeerFn(FakePeer);
    const clear_embargo = clearResolvedImportEmbargoForPeerFn(FakePeer);

    const first_id = try alloc_embargo_id(&peer);
    const second_id = try alloc_embargo_id(&peer);
    try std.testing.expectEqual(@as(u32, 0), first_id);
    try std.testing.expectEqual(@as(u32, 1), second_id);
    try remember_pending(&peer, first_id, 41);
    try remember_pending(&peer, second_id, 42);
    forget_pending(&peer, second_id);
    try std.testing.expectEqual(@as(?u32, null), take_pending(&peer, second_id));
    try remember_pending(&peer, second_id, 42);
    try std.testing.expectEqual(@as(?u32, 41), take_pending(&peer, first_id));
    try std.testing.expectEqual(@as(?u32, null), take_pending(&peer, first_id));
    try std.testing.expectEqual(@as(?u32, 42), take_pending(&peer, second_id));

    peer.next_embargo_id = std.math.maxInt(u32);
    try std.testing.expectEqual(std.math.maxInt(u32), try alloc_embargo_id(&peer));
    try std.testing.expectEqual(@as(u32, 0), try alloc_embargo_id(&peer));

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

test "peer third-party noteCallSendResultsForPeerFn routes to yourself and third-party handlers" {
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
            .tag = .importedCap,
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
            .tag = .importedCap,
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
            .tag = .importedCap,
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
            .tag = .thirdParty,
            .third_party = null,
        },
        .allow_third_party_tail = false,
        .no_promise_pipelining = false,
        .only_promise_pipeline = false,
    });
    try std.testing.expectEqual(@as(usize, 1), state.yourself_calls);
    try std.testing.expectEqual(@as(usize, 1), state.third_party_calls);
}

test "peer third-party capture helper factories use peer allocator and payload fields" {
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
    var third_party_msg = try message.Message.init(std.testing.allocator, third_party_payload, .{});
    defer third_party_msg.deinit();
    const third_party_ptr = try third_party_msg.getRootAnyPointer();
    const captured = try capture_any(&peer, third_party_ptr);
    defer std.testing.allocator.free(captured.?);
    try std.testing.expectEqualStrings("captured", captured.?);

    var call_builder_msg = protocol.MessageBuilder.init(std.testing.allocator);
    defer call_builder_msg.deinit();
    var call_builder = try call_builder_msg.beginCall(77, 0xAA, 2);
    try call_builder.setTargetImportedCap(0);
    _ = try call_builder.payloadTyped();

    const set_third_party = setForwardedCallThirdPartyFromPayloadForPeerFn(FakePeer);
    try set_third_party(&peer, &call_builder, third_party_payload);

    const encoded = try call_builder_msg.finish();
    defer std.testing.allocator.free(encoded);

    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, encoded);
    defer decoded.deinit();
    const call = try decoded.asCall();
    try std.testing.expectEqual(protocol.SendResultsToTag.thirdParty, call.send_results_to.tag);
    const payload_ptr = call.send_results_to.third_party orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualStrings("destination", try payload_ptr.getText());
}

test "peer provide-target helper factories resolve imported and promised targets with peer state" {
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
        .tag = .importedCap,
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
            .tag = .importedCap,
            .imported_cap = null,
            .promised_answer = null,
        }),
    );
    try std.testing.expectError(
        error.MissingPromisedAnswer,
        resolve_target(&peer, .{
            .tag = .promisedAnswer,
            .imported_cap = null,
            .promised_answer = null,
        }),
    );
}

test "peer bootstrap sends exception when bootstrap export is not configured" {
    const State = struct {
        send_return_exception_calls: usize = 0,
        exception_question_id: u32 = 0,
        exception_reason: ?[]const u8 = null,
        note_export_ref_calls: usize = 0,
        rollback_export_ref_calls: usize = 0,
        send_and_record_calls: usize = 0,

        fn questionIdInUse(state: *@This(), question_id: u32) !bool {
            _ = state;
            _ = question_id;
            return false;
        }

        fn noteExportRef(state: *@This(), export_id: u32) !void {
            _ = export_id;
            state.note_export_ref_calls += 1;
        }

        fn rollbackExportRef(state: *@This(), export_id: u32) void {
            _ = export_id;
            state.rollback_export_ref_calls += 1;
        }

        fn sendReturnException(state: *@This(), question_id: u32, reason: []const u8) !void {
            state.send_return_exception_calls += 1;
            state.exception_question_id = question_id;
            state.exception_reason = reason;
        }

        fn sendAndRecordReturn(state: *@This(), question_id: u32, frame: []const u8) !void {
            _ = question_id;
            _ = frame;
            state.send_and_record_calls += 1;
        }
    };

    var state = State{};
    try handleBootstrap(
        State,
        &state,
        std.testing.allocator,
        .{
            .question_id = 91,
            .deprecated_object = null,
        },
        null,
        State.questionIdInUse,
        State.noteExportRef,
        State.rollbackExportRef,
        State.sendReturnException,
        State.sendAndRecordReturn,
    );

    try std.testing.expectEqual(@as(usize, 1), state.send_return_exception_calls);
    try std.testing.expectEqual(@as(u32, 91), state.exception_question_id);
    try std.testing.expectEqualStrings("bootstrap not configured", state.exception_reason orelse "");
    try std.testing.expectEqual(@as(usize, 0), state.note_export_ref_calls);
    try std.testing.expectEqual(@as(usize, 0), state.rollback_export_ref_calls);
    try std.testing.expectEqual(@as(usize, 0), state.send_and_record_calls);
}

test "peer bootstrap delivers and records through the combined send hook" {
    const State = struct {
        allocator: std.mem.Allocator,
        note_export_ref_calls: usize = 0,
        noted_export_id: ?u32 = null,
        rollback_export_ref_calls: usize = 0,
        send_return_exception_calls: usize = 0,
        send_and_record_calls: usize = 0,
        recorded_question_id: ?u32 = null,
        recorded_frame: ?[]u8 = null,

        fn deinit(state: *@This()) void {
            if (state.recorded_frame) |bytes| state.allocator.free(bytes);
        }

        fn questionIdInUse(state: *@This(), question_id: u32) !bool {
            _ = state;
            _ = question_id;
            return false;
        }

        fn noteExportRef(state: *@This(), export_id: u32) !void {
            state.note_export_ref_calls += 1;
            state.noted_export_id = export_id;
        }

        fn rollbackExportRef(state: *@This(), export_id: u32) void {
            _ = export_id;
            state.rollback_export_ref_calls += 1;
        }

        fn sendReturnException(state: *@This(), question_id: u32, reason: []const u8) !void {
            _ = question_id;
            _ = reason;
            state.send_return_exception_calls += 1;
        }

        fn sendAndRecordReturn(state: *@This(), question_id: u32, frame: []const u8) !void {
            state.send_and_record_calls += 1;
            state.recorded_question_id = question_id;
            const copy = try state.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            state.recorded_frame = copy;
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
        State.questionIdInUse,
        State.noteExportRef,
        State.rollbackExportRef,
        State.sendReturnException,
        State.sendAndRecordReturn,
    );

    try std.testing.expectEqual(@as(usize, 1), state.note_export_ref_calls);
    try std.testing.expectEqual(@as(?u32, 1234), state.noted_export_id);
    try std.testing.expectEqual(@as(usize, 0), state.rollback_export_ref_calls);
    try std.testing.expectEqual(@as(usize, 0), state.send_return_exception_calls);
    // Exactly one delivery+record for the bootstrap question, handed a
    // decodable Return frame whose descriptor names the bootstrap export.
    try std.testing.expectEqual(@as(usize, 1), state.send_and_record_calls);
    try std.testing.expectEqual(@as(?u32, 7), state.recorded_question_id);
    try std.testing.expect(state.recorded_frame != null);
    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, state.recorded_frame.?);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 7), ret.answer_id);
}

test "peer bootstrap rejects a question id already in use without a second return" {
    const State = struct {
        note_export_ref_calls: usize = 0,
        rollback_export_ref_calls: usize = 0,
        send_return_exception_calls: usize = 0,
        send_and_record_calls: usize = 0,

        fn questionIdInUse(state: *@This(), question_id: u32) !bool {
            _ = state;
            _ = question_id;
            return true;
        }

        fn noteExportRef(state: *@This(), export_id: u32) !void {
            _ = export_id;
            state.note_export_ref_calls += 1;
        }

        fn rollbackExportRef(state: *@This(), export_id: u32) void {
            _ = export_id;
            state.rollback_export_ref_calls += 1;
        }

        fn sendReturnException(state: *@This(), question_id: u32, reason: []const u8) !void {
            _ = question_id;
            _ = reason;
            state.send_return_exception_calls += 1;
        }

        fn sendAndRecordReturn(state: *@This(), question_id: u32, frame: []const u8) !void {
            _ = question_id;
            _ = frame;
            state.send_and_record_calls += 1;
        }
    };

    var state = State{};
    try std.testing.expectError(error.DuplicateQuestionId, handleBootstrap(
        State,
        &state,
        std.testing.allocator,
        .{
            .question_id = 42,
            .deprecated_object = null,
        },
        1234,
        State.questionIdInUse,
        State.noteExportRef,
        State.rollbackExportRef,
        State.sendReturnException,
        State.sendAndRecordReturn,
    ));

    // No second Return, no export ref bump, and no cached answer to poison.
    try std.testing.expectEqual(@as(usize, 0), state.note_export_ref_calls);
    try std.testing.expectEqual(@as(usize, 0), state.rollback_export_ref_calls);
    try std.testing.expectEqual(@as(usize, 0), state.send_return_exception_calls);
    try std.testing.expectEqual(@as(usize, 0), state.send_and_record_calls);
}

test "peer bootstrap rolls back the export ref when delivery fails" {
    const State = struct {
        note_export_ref_calls: usize = 0,
        rollback_export_ref_calls: usize = 0,
        rolled_back_export_id: ?u32 = null,
        send_and_record_calls: usize = 0,

        fn questionIdInUse(state: *@This(), question_id: u32) !bool {
            _ = state;
            _ = question_id;
            return false;
        }

        fn noteExportRef(state: *@This(), export_id: u32) !void {
            _ = export_id;
            state.note_export_ref_calls += 1;
        }

        fn rollbackExportRef(state: *@This(), export_id: u32) void {
            state.rollback_export_ref_calls += 1;
            state.rolled_back_export_id = export_id;
        }

        fn sendReturnException(state: *@This(), question_id: u32, reason: []const u8) !void {
            _ = state;
            _ = question_id;
            _ = reason;
        }

        fn sendAndRecordReturn(state: *@This(), question_id: u32, frame: []const u8) !void {
            _ = question_id;
            _ = frame;
            state.send_and_record_calls += 1;
            return error.TransportGone;
        }
    };

    var state = State{};
    try std.testing.expectError(error.TransportGone, handleBootstrap(
        State,
        &state,
        std.testing.allocator,
        .{
            .question_id = 7,
            .deprecated_object = null,
        },
        1234,
        State.questionIdInUse,
        State.noteExportRef,
        State.rollbackExportRef,
        State.sendReturnException,
        State.sendAndRecordReturn,
    ));

    // Any error out of the delivery+record hook means the frame was not
    // delivered: the remote never received the descriptor, so the ref bump is
    // rolled back and no resolved answer was recorded.
    try std.testing.expectEqual(@as(usize, 1), state.note_export_ref_calls);
    try std.testing.expectEqual(@as(usize, 1), state.send_and_record_calls);
    try std.testing.expectEqual(@as(usize, 1), state.rollback_export_ref_calls);
    try std.testing.expectEqual(@as(?u32, 1234), state.rolled_back_export_id);
}

test "peer finish frame helper factories take and free resolved answer frame" {
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

test "peer finish runs clear, tail-forward, and resolved cleanup" {
    const State = struct {
        expected_question_id: u32,
        clear_calls: usize = 0,
        tail_question_id: ?u32 = null,
        send_finish_calls: usize = 0,
        last_finish_question_id: u32 = 0,
        last_finish_release_result_caps: bool = true,
        resolved_frame: ?[]u8 = null,
        release_answer_caps_calls: usize = 0,
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

        fn releaseAnswerCapsForFrame(state: *State, frame: []const u8) !void {
            state.release_answer_caps_calls += 1;
            try std.testing.expectEqualSlices(u8, &[_]u8{ 9, 8, 7 }, frame);
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
        Hooks.releaseAnswerCapsForFrame,
        Hooks.releaseCapsForFrame,
        Hooks.freeFrame,
    );

    try std.testing.expectEqual(@as(usize, 5), state.clear_calls);
    try std.testing.expectEqual(@as(usize, 1), state.send_finish_calls);
    try std.testing.expectEqual(@as(u32, 88), state.last_finish_question_id);
    try std.testing.expectEqual(false, state.last_finish_release_result_caps);
    // The answer-held refs are released on every Finish of a recorded answer;
    // the wire refs additionally because releaseResultCaps was set.
    try std.testing.expectEqual(@as(usize, 1), state.release_answer_caps_calls);
    try std.testing.expectEqual(@as(usize, 1), state.release_caps_calls);
    try std.testing.expectEqual(@as(usize, 1), state.free_frame_calls);
}

test "peer finish skips optional tail and resolved cleanup when absent" {
    const State = struct {
        expected_question_id: u32,
        clear_calls: usize = 0,
        send_finish_calls: usize = 0,
        release_answer_caps_calls: usize = 0,
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

        fn releaseAnswerCapsForFrame(state: *State, frame: []const u8) !void {
            _ = frame;
            state.release_answer_caps_calls += 1;
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
        Hooks.releaseAnswerCapsForFrame,
        Hooks.releaseCapsForFrame,
        Hooks.freeFrame,
    );

    try std.testing.expectEqual(@as(usize, 5), state.clear_calls);
    try std.testing.expectEqual(@as(usize, 0), state.send_finish_calls);
    try std.testing.expectEqual(@as(usize, 0), state.release_answer_caps_calls);
    try std.testing.expectEqual(@as(usize, 0), state.release_caps_calls);
    try std.testing.expectEqual(@as(usize, 0), state.free_frame_calls);
}

test "peer bootstrap handleUnimplementedQuestionForPeerFn builds exception return" {
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

test "peer bootstrap handleUnimplementedQuestionForPeerFn ignores unknown question return error" {
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

const EmbargoTestPendingAccept = struct {
    answer_id: u32,
    provided_question_id: u32,
};

fn deinitEmbargoTestMaps(
    allocator: std.mem.Allocator,
    pending_accepts_by_embargo: *std.StringHashMap(std.ArrayList(EmbargoTestPendingAccept)),
    pending_accept_embargo_by_question: *std.AutoHashMap(u32, []u8),
) void {
    var it = pending_accepts_by_embargo.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        entry.value_ptr.deinit(allocator);
    }
    pending_accepts_by_embargo.deinit();
    // Values here borrow keys already freed above; only the table needs deinit.
    pending_accept_embargo_by_question.deinit();
}

test "peer embargo release unlinks all borrowed question refs before a mid-fanout send failure" {
    // BUG 1 regression: releaseEmbargoedAccepts frees the shared embargo key in
    // its defer. Every value in pending_accept_embargo_by_question borrows that
    // key. If send_return_exception errors partway through the fan-out, the
    // `try` propagates and breaks the send loop; any answer id whose borrowed
    // reference had not yet been unlinked would then dangle into the freed key.
    // The fix unlinks all queued answer ids up front, so the question map is
    // fully cleared regardless of where the send loop aborts.
    const ProvideEntry = struct { target: u32 };
    const State = struct {
        provided_calls: usize = 0,
        exception_calls: usize = 0,
    };
    const Hooks = struct {
        fn sendProvided(state: *State, answer_id: u32, entry: *const ProvideEntry) !void {
            _ = answer_id;
            _ = entry;
            state.provided_calls += 1;
            // Force the fan-out to fail so send_return_exception runs.
            return error.TestProvidedSendFailed;
        }

        fn sendException(state: *State, answer_id: u32, reason: []const u8) !void {
            _ = answer_id;
            _ = reason;
            state.exception_calls += 1;
            // Exception send also fails: the `try` in releaseEmbargoedAccepts
            // propagates and breaks the loop before the remaining entries are
            // processed by the send loop.
            return error.TestExceptionSendFailed;
        }
    };

    const allocator = std.testing.allocator;

    var pending_accepts_by_embargo = std.StringHashMap(std.ArrayList(EmbargoTestPendingAccept)).init(allocator);
    var pending_accept_embargo_by_question = std.AutoHashMap(u32, []u8).init(allocator);
    defer deinitEmbargoTestMaps(
        allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
    );

    var provides_by_question = std.AutoHashMap(u32, ProvideEntry).init(allocator);
    defer provides_by_question.deinit();
    try provides_by_question.put(500, .{ .target = 1 });
    try provides_by_question.put(501, .{ .target = 2 });

    // Two accepts under the same embargo share one heap-allocated key.
    try peer_embargo_accepts.queueEmbargoedAccept(
        EmbargoTestPendingAccept,
        allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
        60,
        500,
        "shared-embargo",
    );
    try peer_embargo_accepts.queueEmbargoedAccept(
        EmbargoTestPendingAccept,
        allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
        61,
        501,
        "shared-embargo",
    );

    try std.testing.expectEqual(@as(usize, 2), pending_accept_embargo_by_question.count());

    var state = State{};
    try std.testing.expectError(error.TestExceptionSendFailed, peer_embargo_accepts.releaseEmbargoedAccepts(
        State,
        EmbargoTestPendingAccept,
        ProvideEntry,
        &state,
        allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
        &provides_by_question,
        "shared-embargo",
        Hooks.sendProvided,
        Hooks.sendException,
    ));

    // The send loop aborted after the first entry, but no borrowed reference to
    // the now-freed key survives: the whole question map was unlinked up front.
    try std.testing.expectEqual(@as(usize, 0), pending_accept_embargo_by_question.count());
    try std.testing.expect(!pending_accept_embargo_by_question.contains(60));
    try std.testing.expect(!pending_accept_embargo_by_question.contains(61));
    // The embargo bucket (and its key allocation) was also removed by release.
    try std.testing.expectEqual(@as(usize, 0), pending_accepts_by_embargo.count());
}

test "peer join orchestration budget rejection frees no ProvideTarget" {
    // BUG 2 regression: a promised-answer Join heap-allocates a ProvideTarget in
    // make_target. When ensure_join_budget rejected the join AFTER that
    // allocation with no errdefer covering the target, the clone leaked. The fix
    // enforces the budget before make_target runs, so a rejection allocates
    // nothing. This test uses the testing allocator so a leak fails the test.
    const JoinKeyPart = struct {
        join_id: u32,
        part_count: u16,
        part_num: u16,
    };
    // ProvideTarget owns a heap allocation, mirroring cloned transform ops.
    const ProvideTarget = struct {
        ops: []u8,
    };
    const JoinPartEntry = struct {
        question_id: u32,
        target: ProvideTarget,
    };
    const JoinState = struct {
        part_count: u16,
        parts: std.AutoHashMap(u16, JoinPartEntry),

        fn init(allocator: std.mem.Allocator, part_count: u16) @This() {
            return .{
                .part_count = part_count,
                .parts = std.AutoHashMap(u16, JoinPartEntry).init(allocator),
            };
        }

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            _ = allocator;
            self.parts.deinit();
        }
    };
    const PendingJoinQuestion = struct {
        join_id: u32,
        part_num: u16,
    };
    const State = struct {
        allocator: std.mem.Allocator,
        make_target_calls: usize = 0,
        budget_calls: usize = 0,
        exception_calls: usize = 0,
    };
    const Hooks = struct {
        fn sendAbort(state: *State, reason: []const u8) !void {
            _ = state;
            _ = reason;
            unreachable;
        }

        fn resolveTarget(state: *State, target: protocol.MessageTarget) !cap_table.ResolvedCap {
            _ = state;
            _ = target;
            return .{ .exported = .{ .id = 1 } };
        }

        fn makeTarget(state: *State, resolved: cap_table.ResolvedCap) !ProvideTarget {
            _ = resolved;
            state.make_target_calls += 1;
            // Heap-allocate to model cloned transform ops. If this is reached
            // and then abandoned without a free, the testing allocator flags it.
            return .{ .ops = try state.allocator.dupe(u8, "cloned-ops") };
        }

        fn deinitTarget(target: *ProvideTarget, allocator: std.mem.Allocator) void {
            allocator.free(target.ops);
        }

        fn initJoinState(allocator: std.mem.Allocator, part_count: u16) JoinState {
            return JoinState.init(allocator, part_count);
        }

        fn deinitJoinState(state: *JoinState, allocator: std.mem.Allocator) void {
            state.deinit(allocator);
        }

        fn ensureJoinBudget(state: *State, join_key_part: JoinKeyPart, question_id: u32) !void {
            _ = join_key_part;
            _ = question_id;
            state.budget_calls += 1;
            return error.PeerLimitExceeded;
        }

        fn completeJoin(state: *State, join_id: u32) !void {
            _ = state;
            _ = join_id;
            unreachable;
        }

        fn sendReturnException(state: *State, question_id: u32, reason: []const u8) !void {
            _ = question_id;
            _ = reason;
            state.exception_calls += 1;
        }
    };

    const allocator = std.testing.allocator;

    var pending_joins = std.AutoHashMap(u32, JoinState).init(allocator);
    defer {
        var it = pending_joins.valueIterator();
        while (it.next()) |join_state| join_state.deinit(allocator);
        pending_joins.deinit();
    }
    var pending_join_questions = std.AutoHashMap(u32, PendingJoinQuestion).init(allocator);
    defer pending_join_questions.deinit();

    // Build a valid key_part payload so parseJoinKeyPart succeeds and the
    // budget check is reached: [join_id:u32, part_count:u16, part_num:u16].
    var key_part_builder = message.MessageBuilder.init(allocator);
    defer key_part_builder.deinit();
    const key_part_root = try key_part_builder.initRootAnyPointer();
    var key_part_struct = try key_part_root.initStruct(1, 0);
    key_part_struct.writeU32(0, 0x99);
    key_part_struct.writeU16(4, 1);
    key_part_struct.writeU16(6, 0);
    const key_part_bytes = try key_part_builder.toBytes();
    defer allocator.free(key_part_bytes);
    var key_part_msg = try message.Message.init(allocator, key_part_bytes, .{});
    defer key_part_msg.deinit();
    const key_part_ptr = try key_part_msg.getRootAnyPointer();

    var state = State{ .allocator = allocator };

    // A promised-answer target reaches make_target; the budget must reject it.
    try std.testing.expectError(error.PeerLimitExceeded, peer_provide_join_orchestration.handleJoin(
        State,
        JoinKeyPart,
        JoinState,
        PendingJoinQuestion,
        ProvideTarget,
        &state,
        allocator,
        .{
            .question_id = 8,
            .target = .{
                .tag = .importedCap,
                .imported_cap = 1,
                .promised_answer = null,
            },
            .key_part = key_part_ptr,
        },
        &pending_joins,
        &pending_join_questions,
        Hooks.sendAbort,
        Hooks.resolveTarget,
        Hooks.makeTarget,
        Hooks.deinitTarget,
        Hooks.initJoinState,
        Hooks.deinitJoinState,
        Hooks.ensureJoinBudget,
        Hooks.completeJoin,
        Hooks.sendReturnException,
    ));

    // The budget runs before make_target, so no ProvideTarget was allocated.
    try std.testing.expectEqual(@as(usize, 1), state.budget_calls);
    try std.testing.expectEqual(@as(usize, 0), state.make_target_calls);
    try std.testing.expectEqual(@as(usize, 0), pending_joins.count());
    try std.testing.expectEqual(@as(usize, 0), pending_join_questions.count());
}

test "peer return restore helpers degrade gracefully when the map has no spare slot" {
    // BUG 3 regression: restoreQuestionForReturnForPeer and
    // restoreAdoptedAnswerOriginal previously used putAssumeCapacity. A
    // re-entrant callback during dispatch could insert into the same map and
    // consume the slot freed by the preceding remove, so a later restore ran
    // without capacity and tripped the capacity assertion. The helpers now use a
    // fallible put and grow on demand. Force a zero-spare-capacity map so the
    // restore must allocate, and confirm it succeeds without assertion.
    const Question = struct { marker: u32 };
    const FakePeer = struct {
        questions: std.AutoHashMap(u32, Question),
        adopted_third_party_answers: std.AutoHashMap(u32, u32),
    };

    const allocator = std.testing.allocator;
    var peer = FakePeer{
        .questions = std.AutoHashMap(u32, Question).init(allocator),
        .adopted_third_party_answers = std.AutoHashMap(u32, u32).init(allocator),
    };
    defer {
        peer.questions.deinit();
        peer.adopted_third_party_answers.deinit();
    }

    // Drive the questions map to exactly its available capacity so a restore has
    // no spare slot and must grow (the scenario that broke putAssumeCapacity).
    var next: u32 = 0;
    while (peer.questions.unmanaged.available > 0) : (next += 1) {
        try peer.questions.put(next, .{ .marker = next });
    }
    try std.testing.expectEqual(@as(u32, 0), peer.questions.unmanaged.available);

    const restore_question = peer_return_orchestration.restoreQuestionForReturnForPeerFn(FakePeer, Question);
    restore_question(&peer, 9999, .{ .marker = 42 });
    const restored = peer.questions.get(9999) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 42), restored.marker);

    // Same for the adopted-answer map.
    var next_adopted: u32 = 0;
    while (peer.adopted_third_party_answers.unmanaged.available > 0) : (next_adopted += 1) {
        try peer.adopted_third_party_answers.put(next_adopted, next_adopted);
    }
    try std.testing.expectEqual(@as(u32, 0), peer.adopted_third_party_answers.unmanaged.available);

    const restore_adopted = peer_return_dispatch.restoreAdoptedAnswerOriginalForPeerFn(FakePeer);
    restore_adopted(&peer, 8888, 77);
    const restored_adopted = peer.adopted_third_party_answers.get(8888) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 77), restored_adopted);
}
