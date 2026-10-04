const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const protocol = capnpc.rpc.wire.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.caps.table;
const events = capnpc.rpc.events;
const rpc_time = capnpc.rpc.time;
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

    /// Decode the captured frame at `index` and return it as a Finish.
    fn decodeFinish(self: *@This(), index: usize) !protocol.Finish {
        var decoded = try protocol.DecodedMessage.init(self.allocator, self.frames.items[index]);
        defer decoded.deinit();
        return decoded.asFinish();
    }
};

const ReturnRecorder = struct {
    return_count: usize = 0,
    exception_count: usize = 0,
    saw_deadline_reason: bool = false,
    saw_shutdown_reason: bool = false,

    fn onReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        _ = peer;
        _ = inbound_caps;
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        self.return_count += 1;
        if (ret.tag == .exception) {
            self.exception_count += 1;
            if (ret.exception) |ex| {
                if (std.mem.eql(u8, ex.reason, "deadline exceeded")) self.saw_deadline_reason = true;
                if (std.mem.eql(u8, ex.reason, "peer shutting down")) self.saw_shutdown_reason = true;
            }
        }
    }
};

const EventRecorder = struct {
    call_deadline_timeouts: usize = 0,
    shutdown_drain_timeouts: usize = 0,
    idle_timeouts: usize = 0,
    parked_accept_timeouts: usize = 0,
    join_timeouts: usize = 0,
    backpressure_events: usize = 0,
    last_timeout_question_id: ?u32 = null,
    cancel_failures: usize = 0,
    last_cancel_failure: ?events.CancelFailureEvent = null,
    /// `cancel_failures` at the moment the last `.timeout` event arrived.
    cancel_failures_at_last_timeout: usize = 0,

    fn onEvent(ctx_ptr: *anyopaque, event: events.Event) void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        switch (event) {
            .timeout => |t| {
                self.cancel_failures_at_last_timeout = self.cancel_failures;
                switch (t.kind) {
                    .call_deadline => {
                        self.call_deadline_timeouts += 1;
                        self.last_timeout_question_id = t.question_id;
                    },
                    .shutdown_drain => self.shutdown_drain_timeouts += 1,
                    .idle_connection => self.idle_timeouts += 1,
                    .parked_accept => self.parked_accept_timeouts += 1,
                    .join => self.join_timeouts += 1,
                }
            },
            .backpressure => self.backpressure_events += 1,
            .cancel_failure => |f| {
                self.cancel_failures += 1;
                self.last_cancel_failure = f;
            },
            else => {},
        }
    }

    fn observer(self: *@This()) events.Observer {
        return events.Observer.init(self, onEvent);
    }
};

/// Fake transport binding that records sends and close requests, and can be
/// configured to fail every send with a write-queue error.
const FakeTransport = struct {
    send_count: usize = 0,
    close_count: usize = 0,
    fail_sends_with: ?anyerror = null,

    fn sendFn(ctx: *anyopaque, frame: []const u8) anyerror!void {
        _ = frame;
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (self.fail_sends_with) |err| return err;
        self.send_count += 1;
    }

    fn closeFn(ctx: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.close_count += 1;
    }

    fn isClosingFn(ctx: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        return self.close_count > 0;
    }

    fn attach(self: *@This(), peer: *Peer) void {
        peer.attachTransport(self, null, sendFn, closeFn, isClosingFn);
    }
};

const ShutdownFlag = struct {
    var fired: bool = false;
    fn onComplete(peer: *Peer) void {
        _ = peer;
        fired = true;
    }
};

fn buildRemoteExceptionReturn(allocator: std.mem.Allocator, answer_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var ret = try builder.beginReturn(answer_id, .exception);
    try ret.setException("remote says no");
    return builder.finish();
}

fn buildRemoteExceptionReturnNoFinish(allocator: std.mem.Allocator, answer_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var ret = try builder.beginReturn(answer_id, .exception);
    try ret.setException("remote says no");
    ret.setNoFinishNeeded(true);
    return builder.finish();
}

test "retained call withholds Finish until explicit retry-safe completion" {
    const allocator = std.testing.allocator;

    var fake = FakeTransport{};
    var recorder = ReturnRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    fake.attach(&peer);

    const question_id = try peer.sendCallWithOptions(
        77,
        0x1234,
        5,
        &recorder,
        null,
        ReturnRecorder.onReturn,
        .{ .result_lifetime = .retained },
    );
    try std.testing.expectEqual(@as(usize, 1), peer.stats().retained_questions);
    try std.testing.expectEqual(@as(usize, 1), fake.send_count);
    try std.testing.expectError(
        error.RetainedQuestionPending,
        peer.finishRetainedQuestion(question_id, false),
    );

    const ret_frame = try buildRemoteExceptionReturn(allocator, question_id);
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    try std.testing.expectEqual(@as(usize, 1), recorder.return_count);
    try std.testing.expectEqual(@as(u32, 0), peer.questions.count());
    try std.testing.expectEqual(@as(usize, 1), peer.stats().retained_questions);
    // The Return itself did not trigger automatic Finish.
    try std.testing.expectEqual(@as(usize, 1), fake.send_count);

    // A failed control send leaves the retained answer retryable.
    fake.fail_sends_with = error.TestFinishSendFailed;
    try std.testing.expectError(
        error.TestFinishSendFailed,
        peer.finishRetainedQuestion(question_id, false),
    );
    try std.testing.expectEqual(@as(usize, 1), peer.stats().retained_questions);

    fake.fail_sends_with = null;
    try peer.finishRetainedQuestion(question_id, false);
    try std.testing.expectEqual(@as(usize, 0), peer.stats().retained_questions);
    try std.testing.expectEqual(@as(usize, 2), fake.send_count);
    try std.testing.expectError(
        error.UnknownRetainedQuestion,
        peer.finishRetainedQuestion(question_id, false),
    );
}

test "retained noFinishNeeded result retires without a Finish" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = ReturnRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const question_id = try peer.sendCallWithOptions(
        88,
        0x5678,
        3,
        &recorder,
        null,
        ReturnRecorder.onReturn,
        .{ .result_lifetime = .retained },
    );
    const ret_frame = try buildRemoteExceptionReturnNoFinish(allocator, question_id);
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    try std.testing.expectEqual(@as(usize, 1), recorder.return_count);
    try std.testing.expectEqual(@as(usize, 0), peer.stats().retained_questions);
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    try std.testing.expectError(
        error.UnknownRetainedQuestion,
        peer.finishRetainedQuestion(question_id, false),
    );
}

test "retained Finish forwards releaseResultCaps" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = ReturnRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const question_id = try peer.sendCallWithOptions(
        91,
        0xABCD,
        4,
        &recorder,
        null,
        ReturnRecorder.onReturn,
        .{ .result_lifetime = .retained },
    );
    const ret_frame = try buildRemoteExceptionReturn(allocator, question_id);
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);
    try peer.finishRetainedQuestion(question_id, true);

    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);
    const finish = try capture.decodeFinish(1);
    try std.testing.expectEqual(question_id, finish.question_id);
    try std.testing.expect(finish.release_result_caps);
}

test "retained question limit, pressure, cancel, and close refund gauges" {
    const allocator = std.testing.allocator;

    const RetainedEvents = struct {
        pressure: usize = 0,
        rejection: usize = 0,

        fn onEvent(ctx_ptr: *anyopaque, event: events.Event) void {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            switch (event) {
                .pressure => |pressure| {
                    if (pressure.resource == .retained_questions) self.pressure += 1;
                },
                .resource_rejection => |rejection| {
                    if (rejection.resource == .retained_questions) self.rejection += 1;
                },
                else => {},
            }
        }
    };

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = ReturnRecorder{};
    var retained_events = RetainedEvents{};

    var peer = Peer.initDetachedWithLimits(allocator, .{ .max_retained_questions = 1 });
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setObserver(events.Observer.init(&retained_events, RetainedEvents.onEvent));

    const question_id = try peer.sendCallWithOptions(
        1,
        2,
        3,
        &recorder,
        null,
        ReturnRecorder.onReturn,
        .{ .result_lifetime = .retained },
    );
    try std.testing.expectEqual(@as(usize, 1), retained_events.pressure);
    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer.sendCallWithOptions(
            2,
            2,
            3,
            &recorder,
            null,
            ReturnRecorder.onReturn,
            .{ .result_lifetime = .retained },
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), retained_events.rejection);

    try peer.cancelQuestion(question_id, "cancel retained");
    try std.testing.expectEqual(@as(usize, 0), peer.stats().retained_questions);

    _ = try peer.sendCallWithOptions(
        3,
        2,
        3,
        &recorder,
        null,
        ReturnRecorder.onReturn,
        .{ .result_lifetime = .retained },
    );
    peer.notifyTransportClosed();
    try std.testing.expectEqual(@as(usize, 0), peer.stats().retained_questions);
    peer.notifyTransportClosed();
    try std.testing.expectEqual(@as(usize, 0), peer.stats().retained_questions);
}

test "retained registration precedes synchronous Return and survives trailing send error" {
    const allocator = std.testing.allocator;

    const SyncTransport = struct {
        allocator: std.mem.Allocator,
        peer: *Peer = undefined,
        callback_count: usize = 0,
        finish_count: usize = 0,
        callback_question_id: ?u32 = null,
        fail_after_return: bool = true,

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
            defer decoded.deinit();
            switch (decoded.tag) {
                .call => {
                    const call = try decoded.asCall();
                    const ret_frame = try buildRemoteExceptionReturn(self.allocator, call.question_id);
                    defer self.allocator.free(ret_frame);
                    try self.peer.handleFrame(ret_frame);
                    if (self.fail_after_return) return error.TestTrailingSendError;
                },
                .finish => self.finish_count += 1,
                else => {},
            }
        }

        fn onReturn(
            ctx_ptr: *anyopaque,
            peer: *Peer,
            ret: protocol.Return,
            inbound_caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = peer;
            _ = inbound_caps;
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            self.callback_count += 1;
            self.callback_question_id = ret.answer_id;
            // The retained entry must already be callback-visible.
            try std.testing.expectEqual(@as(usize, 1), self.peer.stats().retained_questions);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    var sync = SyncTransport{ .allocator = allocator };
    sync.peer = &peer;
    peer.setSendFrameOverride(&sync, SyncTransport.onFrame);

    try std.testing.expectError(
        error.TestTrailingSendError,
        peer.sendCallWithOptions(
            55,
            0x9999,
            1,
            &sync,
            null,
            SyncTransport.onReturn,
            .{ .result_lifetime = .retained },
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), sync.callback_count);
    const question_id = sync.callback_question_id orelse return error.MissingQuestionId;
    try std.testing.expectEqual(@as(usize, 1), peer.stats().retained_questions);

    sync.fail_after_return = false;
    try peer.finishRetainedQuestion(question_id, false);
    try std.testing.expectEqual(@as(usize, 1), sync.finish_count);
    try std.testing.expectEqual(@as(usize, 0), peer.stats().retained_questions);
}

test "deadline expiry cancels question, sends Finish, delivers exception" {
    const allocator = std.testing.allocator;

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = ReturnRecorder{};
    var event_recorder = EventRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setObserver(event_recorder.observer());
    peer.setClock(clock.clock());
    peer.setTimeouts(.{ .default_call_timeout_ms = 100 });

    const question_id = try peer.sendBootstrap(&recorder, ReturnRecorder.onReturn);
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len); // bootstrap

    // Before the deadline nothing happens.
    clock.advanceMs(99);
    try std.testing.expectEqual(@as(usize, 0), peer.checkDeadlines());
    try std.testing.expectEqual(@as(usize, 0), recorder.return_count);

    // At the deadline the question is cancelled.
    clock.advanceMs(1);
    try std.testing.expectEqual(@as(usize, 1), peer.checkDeadlines());

    try std.testing.expectEqual(@as(usize, 1), recorder.return_count);
    try std.testing.expectEqual(@as(usize, 1), recorder.exception_count);
    try std.testing.expect(recorder.saw_deadline_reason);

    // A Finish with releaseResultCaps went out for the question.
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);
    const finish = try capture.decodeFinish(1);
    try std.testing.expectEqual(question_id, finish.question_id);
    try std.testing.expect(finish.release_result_caps);

    // Timeout event was emitted with the question id.
    try std.testing.expectEqual(@as(usize, 1), event_recorder.call_deadline_timeouts);
    try std.testing.expectEqual(@as(?u32, question_id), event_recorder.last_timeout_question_id);

    // The entry stays in the table (cancelled) to absorb the late Return,
    // and a second sweep does not cancel it again.
    try std.testing.expectEqual(@as(u32, 1), peer.questions.count());
    clock.advanceMs(1000);
    try std.testing.expectEqual(@as(usize, 0), peer.checkDeadlines());
    try std.testing.expectEqual(@as(usize, 1), recorder.return_count);
}

test "late Return for a cancelled question is absorbed silently" {
    const allocator = std.testing.allocator;

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = ReturnRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setClock(clock.clock());
    peer.setTimeouts(.{ .default_call_timeout_ms = 10 });

    const question_id = try peer.sendBootstrap(&recorder, ReturnRecorder.onReturn);
    clock.advanceMs(20);
    try std.testing.expectEqual(@as(usize, 1), peer.checkDeadlines());
    try std.testing.expectEqual(@as(usize, 1), recorder.return_count);
    try std.testing.expectEqual(@as(u32, 1), peer.questions.count());

    // The remote's (late) Return arrives: consumed without re-dispatch.
    const late_return = try buildRemoteExceptionReturn(allocator, question_id);
    defer allocator.free(late_return);
    try peer.handleFrame(late_return);

    try std.testing.expectEqual(@as(usize, 1), recorder.return_count);
    try std.testing.expectEqual(@as(u32, 0), peer.questions.count());
}

test "no clock means deadlines are inert" {
    const allocator = std.testing.allocator;

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = ReturnRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setTimeouts(.{ .default_call_timeout_ms = 1 });

    _ = try peer.sendBootstrap(&recorder, ReturnRecorder.onReturn);
    try std.testing.expectEqual(@as(usize, 0), peer.checkDeadlines());
    try std.testing.expectEqual(@as(usize, 0), recorder.return_count);
}

test "setQuestionDeadline and clearQuestionDeadline control individual questions" {
    const allocator = std.testing.allocator;

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = ReturnRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setClock(clock.clock());
    // No default timeout: questions start with no deadline.
    const question_id = try peer.sendBootstrap(&recorder, ReturnRecorder.onReturn);

    clock.advanceMs(10_000);
    try std.testing.expectEqual(@as(usize, 0), peer.checkDeadlines());

    try peer.setQuestionDeadline(question_id, 50);
    clock.advanceMs(49);
    try std.testing.expectEqual(@as(usize, 0), peer.checkDeadlines());

    try peer.clearQuestionDeadline(question_id);
    clock.advanceMs(10_000);
    try std.testing.expectEqual(@as(usize, 0), peer.checkDeadlines());

    try peer.setQuestionDeadline(question_id, 5);
    clock.advanceMs(5);
    try std.testing.expectEqual(@as(usize, 1), peer.checkDeadlines());
    try std.testing.expect(recorder.saw_deadline_reason);

    try std.testing.expectError(error.QuestionCancelled, peer.setQuestionDeadline(question_id, 5));
    try std.testing.expectError(error.UnknownQuestion, peer.setQuestionDeadline(question_id + 1, 5));
}

test "explicit cancelQuestion delivers exception once and is idempotent" {
    const allocator = std.testing.allocator;

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = ReturnRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setClock(clock.clock());

    const question_id = try peer.sendBootstrap(&recorder, ReturnRecorder.onReturn);

    try peer.cancelQuestion(question_id, "deadline exceeded");
    try std.testing.expectEqual(@as(usize, 1), recorder.exception_count);

    try peer.cancelQuestion(question_id, "deadline exceeded");
    try std.testing.expectEqual(@as(usize, 1), recorder.exception_count);

    try std.testing.expectError(error.UnknownQuestion, peer.cancelQuestion(question_id + 7, "x"));
}

test "shutdown drain deadline force-cancels stragglers and completes shutdown" {
    const allocator = std.testing.allocator;

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = ReturnRecorder{};
    var event_recorder = EventRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setObserver(event_recorder.observer());
    peer.setClock(clock.clock());
    peer.setTimeouts(.{ .shutdown_drain_timeout_ms = 50 });

    _ = try peer.sendBootstrap(&recorder, ReturnRecorder.onReturn);
    _ = try peer.sendBootstrap(&recorder, ReturnRecorder.onReturn);

    ShutdownFlag.fired = false;
    peer.shutdown(ShutdownFlag.onComplete);
    try std.testing.expect(!ShutdownFlag.fired);
    try std.testing.expect(peer.is_shutting_down);

    // New calls are refused during the drain.
    try std.testing.expectError(
        error.PeerShuttingDown,
        peer.sendBootstrap(&recorder, ReturnRecorder.onReturn),
    );

    clock.advanceMs(49);
    try std.testing.expectEqual(@as(usize, 0), peer.checkDeadlines());
    try std.testing.expect(!ShutdownFlag.fired);

    clock.advanceMs(1);
    try std.testing.expectEqual(@as(usize, 2), peer.checkDeadlines());

    try std.testing.expectEqual(@as(usize, 2), recorder.exception_count);
    try std.testing.expect(recorder.saw_shutdown_reason);
    try std.testing.expectEqual(@as(u32, 0), peer.questions.count());
    try std.testing.expect(ShutdownFlag.fired);
    try std.testing.expectEqual(@as(usize, 1), event_recorder.shutdown_drain_timeouts);
}

test "shutdown completes normally when the drain finishes before the bound" {
    const allocator = std.testing.allocator;

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = ReturnRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setClock(clock.clock());
    peer.setTimeouts(.{ .shutdown_drain_timeout_ms = 1000 });

    const question_id = try peer.sendBootstrap(&recorder, ReturnRecorder.onReturn);

    ShutdownFlag.fired = false;
    peer.shutdown(ShutdownFlag.onComplete);
    try std.testing.expect(!ShutdownFlag.fired);

    // The Return arrives before the drain bound: shutdown completes without
    // any force-cancel.
    const ret_frame = try buildRemoteExceptionReturn(allocator, question_id);
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    try std.testing.expect(ShutdownFlag.fired);
    try std.testing.expectEqual(@as(usize, 1), recorder.return_count);
}

test "user call sends surface write-queue backpressure without closing transport" {
    const allocator = std.testing.allocator;

    var fake = FakeTransport{ .fail_sends_with = error.WriteQueueFull };
    var recorder = ReturnRecorder{};
    var event_recorder = EventRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    fake.attach(&peer);
    peer.setObserver(event_recorder.observer());

    try std.testing.expectError(
        error.WriteQueueFull,
        peer.sendBootstrap(&recorder, ReturnRecorder.onReturn),
    );

    // The question was rolled back and the transport stays open.
    try std.testing.expectEqual(@as(u32, 0), peer.questions.count());
    try std.testing.expectEqual(@as(usize, 0), fake.close_count);
    try std.testing.expectEqual(@as(usize, 0), event_recorder.backpressure_events);
}

test "control frame backpressure escalates to transport close" {
    const allocator = std.testing.allocator;

    var fake = FakeTransport{ .fail_sends_with = error.WriteQueueFull };
    var event_recorder = EventRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    fake.attach(&peer);
    peer.setObserver(event_recorder.observer());

    // A host-driven Finish is a control frame: enqueue overflow must emit a
    // peer-level backpressure event and initiate close.
    try std.testing.expectError(error.WriteQueueFull, peer.sendFinishForHost(7, true, false));

    try std.testing.expectEqual(@as(usize, 1), event_recorder.backpressure_events);
    try std.testing.expectEqual(@as(usize, 1), fake.close_count);
}

test "cancelQuestion still delivers the local exception when the Finish send fails" {
    const allocator = std.testing.allocator;

    var fake = FakeTransport{};
    var recorder = ReturnRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    fake.attach(&peer);

    const question_id = try peer.sendBootstrap(&recorder, ReturnRecorder.onReturn);
    try std.testing.expectEqual(@as(usize, 1), fake.send_count);

    fake.fail_sends_with = error.WriteQueueFull;
    try peer.cancelQuestion(question_id, "deadline exceeded");

    try std.testing.expectEqual(@as(usize, 1), recorder.exception_count);
    try std.testing.expect(recorder.saw_deadline_reason);
    // Control-class escalation closed the transport.
    try std.testing.expectEqual(@as(usize, 1), fake.close_count);
}

test "call latency event is emitted on Return when a clock is present" {
    const allocator = std.testing.allocator;

    const LatencyRecorder = struct {
        latency_events: usize = 0,
        last_ns: u64 = 0,
        last_question_id: u32 = 0,

        fn onEvent(ctx_ptr: *anyopaque, event: events.Event) void {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            switch (event) {
                .call_latency => |l| {
                    self.latency_events += 1;
                    self.last_ns = l.nanoseconds;
                    self.last_question_id = l.question_id;
                },
                else => {},
            }
        }
    };

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = ReturnRecorder{};
    var latency = LatencyRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setObserver(events.Observer.init(&latency, LatencyRecorder.onEvent));
    peer.setClock(clock.clock());

    const question_id = try peer.sendBootstrap(&recorder, ReturnRecorder.onReturn);

    clock.advanceMs(7);
    const ret_frame = try buildRemoteExceptionReturn(allocator, question_id);
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    try std.testing.expectEqual(@as(usize, 1), latency.latency_events);
    try std.testing.expectEqual(question_id, latency.last_question_id);
    try std.testing.expectEqual(@as(u64, 7 * std.time.ns_per_ms), latency.last_ns);
}

test "peer stats snapshot tracks questions, cancellations, and queued state" {
    const allocator = std.testing.allocator;

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = ReturnRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setClock(clock.clock());

    var stats = peer.stats();
    try std.testing.expectEqual(@as(u32, 0), stats.outbound_questions);
    try std.testing.expectEqual(@as(u32, 0), stats.cancelled_questions);
    try std.testing.expectEqual(@as(usize, 0), stats.parked_accepts);
    try std.testing.expectEqual(@as(usize, 0), stats.parked_accept_bytes);

    const q1 = try peer.sendBootstrap(&recorder, ReturnRecorder.onReturn);
    _ = try peer.sendBootstrap(&recorder, ReturnRecorder.onReturn);

    stats = peer.stats();
    try std.testing.expectEqual(@as(u32, 2), stats.outbound_questions);
    try std.testing.expectEqual(@as(u32, 0), stats.cancelled_questions);

    try peer.cancelQuestion(q1, "deadline exceeded");
    stats = peer.stats();
    try std.testing.expectEqual(@as(u32, 2), stats.outbound_questions);
    try std.testing.expectEqual(@as(u32, 1), stats.cancelled_questions);
    try std.testing.expectEqual(@as(usize, 0), stats.pending_queued_calls);
}

test "outbound question pressure event fires at 80% of the budget" {
    const allocator = std.testing.allocator;

    const PressureRecorder = struct {
        pressure_events: usize = 0,
        last_current: usize = 0,
        last_limit: usize = 0,

        fn onEvent(ctx_ptr: *anyopaque, event: events.Event) void {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            switch (event) {
                .pressure => |p| {
                    if (p.resource == .outbound_questions) {
                        self.pressure_events += 1;
                        self.last_current = p.current;
                        self.last_limit = p.limit;
                    }
                },
                else => {},
            }
        }
    };

    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var recorder = ReturnRecorder{};
    var pressure = PressureRecorder{};

    var peer = Peer.initDetachedWithLimits(allocator, .{ .max_outbound_questions = 10 });
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setObserver(events.Observer.init(&pressure, PressureRecorder.onEvent));

    // Threshold for limit 10 is 8: the eighth question crosses it, once.
    var i: usize = 0;
    while (i < 9) : (i += 1) {
        _ = try peer.sendBootstrap(&recorder, ReturnRecorder.onReturn);
    }
    try std.testing.expectEqual(@as(usize, 1), pressure.pressure_events);
    try std.testing.expectEqual(@as(usize, 8), pressure.last_current);
    try std.testing.expectEqual(@as(usize, 10), pressure.last_limit);
}

// -- Deadline-cancel failures are observer events, not on_error -------------
//
// A wire Return whose question callback fails is handed to `on_error` (the
// nonfatal seam in `dispatchQuestionReturn`), and the Stable sessions close
// on `on_error`. The deadline sweep synthesizes an exception Return locally;
// a callback failing on it (the `try response.unwrap()` idiom returns
// `error.CallTimedOut`) or the cancel itself failing used to vanish into a
// debug log. These pin the chosen behavior: the failure is VISIBLE as a
// `.cancel_failure` observer event, and `on_error` is NOT called, so one
// timed-out call never ends a session.

/// Records `on_error` reports.
const PeerErrorRecorder = struct {
    count: usize = 0,
    last: ?anyerror = null,

    fn onError(ctx: ?*anyopaque, _: *Peer, err: anyerror) void {
        const self: *@This() = @ptrCast(@alignCast(ctx.?));
        self.count += 1;
        self.last = err;
    }
};

/// A question callback that always fails with `fail_with`.
const FailingReturn = struct {
    fail_with: anyerror,
    return_count: usize = 0,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        _: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        self.return_count += 1;
        return self.fail_with;
    }
};

test "deadline-cancel callback failure is an observer event, not on_error (a wire Return's still is)" {
    const allocator = std.testing.allocator;

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var errors = PeerErrorRecorder{};
    var event_recorder = EventRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setObserver(event_recorder.observer());
    peer.setClock(clock.clock());
    peer.start(&errors, PeerErrorRecorder.onError, null);

    // Contrast: the wire Return path reports a failing callback to on_error,
    // and emits no cancel_failure event.
    var wire = FailingReturn{ .fail_with = error.TestCallbackFailed };
    const wire_id = try peer.sendBootstrap(&wire, FailingReturn.onReturn);
    const ret_frame = try buildRemoteExceptionReturn(allocator, wire_id);
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);
    try std.testing.expectEqual(@as(usize, 1), wire.return_count);
    try std.testing.expectEqual(@as(usize, 1), errors.count);
    try std.testing.expectEqual(@as(?anyerror, error.TestCallbackFailed), errors.last);
    try std.testing.expectEqual(@as(usize, 0), event_recorder.cancel_failures);

    // The deadline sweep synthesizes an exception Return; its callback
    // failure is reported to the observer only.
    errors = .{};
    peer.setTimeouts(.{ .default_call_timeout_ms = 100 });
    var timed = FailingReturn{ .fail_with = error.CallTimedOut };
    const timed_id = try peer.sendBootstrap(&timed, FailingReturn.onReturn);
    clock.advanceMs(100);
    try std.testing.expectEqual(@as(usize, 1), peer.checkDeadlines());
    try std.testing.expectEqual(@as(usize, 1), timed.return_count);
    try std.testing.expectEqual(@as(usize, 1), event_recorder.call_deadline_timeouts);
    try std.testing.expectEqual(@as(?u32, timed_id), event_recorder.last_timeout_question_id);
    try std.testing.expectEqual(@as(usize, 1), event_recorder.cancel_failures);
    const failure = event_recorder.last_cancel_failure.?;
    try std.testing.expectEqual(events.Source.peer, failure.source);
    try std.testing.expectEqual(events.TimeoutKind.call_deadline, failure.kind);
    try std.testing.expectEqual(timed_id, failure.question_id);
    try std.testing.expectEqual(@as(anyerror, error.CallTimedOut), failure.err);
    // The .timeout event came first, then the failure.
    try std.testing.expectEqual(@as(usize, 0), event_recorder.cancel_failures_at_last_timeout);
    try std.testing.expectEqual(@as(usize, 0), errors.count);

    // An explicit cancelQuestion keeps log-only behavior: its caller is on
    // the stack, so neither the observer nor on_error hears about it.
    var explicit = FailingReturn{ .fail_with = error.TestCallbackFailed };
    const explicit_id = try peer.sendBootstrap(&explicit, FailingReturn.onReturn);
    try peer.cancelQuestion(explicit_id, "caller gave up");
    try std.testing.expectEqual(@as(usize, 1), explicit.return_count);
    try std.testing.expectEqual(@as(usize, 1), event_recorder.cancel_failures);
    try std.testing.expectEqual(@as(usize, 0), errors.count);
}

test "a deadline cancel that itself fails is an observer event, not on_error" {
    const allocator = std.testing.allocator;

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var errors = PeerErrorRecorder{};
    var event_recorder = EventRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setObserver(event_recorder.observer());
    peer.setClock(clock.clock());
    peer.setTimeouts(.{ .default_call_timeout_ms = 100 });
    peer.start(&errors, PeerErrorRecorder.onError, null);

    // An OOM from the callback propagates out of the cancel itself (it is
    // never swallowed as a callback failure), into the sweep's own catch.
    var oom = FailingReturn{ .fail_with = error.OutOfMemory };
    const oom_id = try peer.sendBootstrap(&oom, FailingReturn.onReturn);
    clock.advanceMs(100);
    _ = peer.checkDeadlines();
    try std.testing.expectEqual(@as(usize, 1), oom.return_count);
    try std.testing.expectEqual(@as(usize, 1), event_recorder.cancel_failures);
    const failure = event_recorder.last_cancel_failure.?;
    try std.testing.expectEqual(events.TimeoutKind.call_deadline, failure.kind);
    try std.testing.expectEqual(oom_id, failure.question_id);
    try std.testing.expectEqual(@as(anyerror, error.OutOfMemory), failure.err);
    try std.testing.expectEqual(@as(usize, 0), errors.count);

    // The cancelled entry absorbs later sweeps without a second report.
    clock.advanceMs(1000);
    try std.testing.expectEqual(@as(usize, 0), peer.checkDeadlines());
    try std.testing.expectEqual(@as(usize, 1), event_recorder.cancel_failures);
    try std.testing.expectEqual(@as(usize, 0), errors.count);
}

test "drain-bound force-cancel callback failures are observer events, not on_error" {
    const allocator = std.testing.allocator;

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var errors = PeerErrorRecorder{};
    var event_recorder = EventRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setObserver(event_recorder.observer());
    peer.setClock(clock.clock());
    peer.setTimeouts(.{ .shutdown_drain_timeout_ms = 50 });
    peer.start(&errors, PeerErrorRecorder.onError, null);

    var first = FailingReturn{ .fail_with = error.TestCallbackFailed };
    var second = FailingReturn{ .fail_with = error.TestCallbackFailed };
    _ = try peer.sendBootstrap(&first, FailingReturn.onReturn);
    _ = try peer.sendBootstrap(&second, FailingReturn.onReturn);

    ShutdownFlag.fired = false;
    peer.shutdown(ShutdownFlag.onComplete);
    clock.advanceMs(50);
    try std.testing.expectEqual(@as(usize, 2), peer.checkDeadlines());
    try std.testing.expectEqual(@as(usize, 1), event_recorder.shutdown_drain_timeouts);
    try std.testing.expectEqual(@as(usize, 1), first.return_count);
    try std.testing.expectEqual(@as(usize, 1), second.return_count);
    try std.testing.expectEqual(@as(usize, 2), event_recorder.cancel_failures);
    const failure = event_recorder.last_cancel_failure.?;
    try std.testing.expectEqual(events.TimeoutKind.shutdown_drain, failure.kind);
    try std.testing.expectEqual(@as(anyerror, error.TestCallbackFailed), failure.err);
    try std.testing.expectEqual(@as(usize, 0), errors.count);
    try std.testing.expect(ShutdownFlag.fired);
}

test "a drain-bound force-cancel that itself fails (OOM) is an observer event, not on_error" {
    const allocator = std.testing.allocator;

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var errors = PeerErrorRecorder{};
    var event_recorder = EventRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setObserver(event_recorder.observer());
    peer.setClock(clock.clock());
    peer.setTimeouts(.{ .shutdown_drain_timeout_ms = 50 });
    peer.start(&errors, PeerErrorRecorder.onError, null);

    // An OOM from the callback propagates out of the force-cancel's
    // delivery (it is never swallowed as a callback failure), into the
    // drain sweep's own catch: the drain-bound twin of the per-question
    // deadline test above.
    var oom = FailingReturn{ .fail_with = error.OutOfMemory };
    const oom_id = try peer.sendBootstrap(&oom, FailingReturn.onReturn);

    ShutdownFlag.fired = false;
    peer.shutdown(ShutdownFlag.onComplete);
    clock.advanceMs(50);
    try std.testing.expectEqual(@as(usize, 1), peer.checkDeadlines());
    try std.testing.expectEqual(@as(usize, 1), oom.return_count);
    try std.testing.expectEqual(@as(usize, 1), event_recorder.shutdown_drain_timeouts);
    try std.testing.expectEqual(@as(usize, 1), event_recorder.cancel_failures);
    const failure = event_recorder.last_cancel_failure.?;
    try std.testing.expectEqual(events.Source.peer, failure.source);
    try std.testing.expectEqual(events.TimeoutKind.shutdown_drain, failure.kind);
    try std.testing.expectEqual(oom_id, failure.question_id);
    try std.testing.expectEqual(@as(anyerror, error.OutOfMemory), failure.err);
    // The .timeout event came first, then the failure.
    try std.testing.expectEqual(@as(usize, 0), event_recorder.cancel_failures_at_last_timeout);
    try std.testing.expectEqual(@as(usize, 0), errors.count);
    // The failed delivery still removed the question, so the drain finished.
    try std.testing.expectEqual(@as(u32, 0), peer.questions.count());
    try std.testing.expect(ShutdownFlag.fired);
}

// -- One question-id space per cancellation ----------------------------------
//
// A `.cancel_failure` event names the question with the id the `.timeout`
// event for the same cancellation carries: the questions-table (wire) id.
// The two id spaces differ only for a retained call redirected by
// `awaitFromThirdParty`, whose open answer is the adopted third-party answer
// id while its caller still addresses it by the id the send returned. That
// is the case below. Both emit sites are covered: a failing callback (the
// delivery path) and an OOM that escapes the cancel (the sweep's own catch).

fn expectRedirectedCancelFailureMatchesTimeoutId(fail_with: anyerror) !void {
    const allocator = std.testing.allocator;

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var errors = PeerErrorRecorder{};
    var event_recorder = EventRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setObserver(event_recorder.observer());
    peer.setClock(clock.clock());
    peer.setTimeouts(.{ .default_call_timeout_ms = 100 });
    peer.start(&errors, PeerErrorRecorder.onError, null);

    var redirected = FailingReturn{ .fail_with = fail_with };
    const logical_id = try peer.sendCallWithOptions(
        7,
        0x5155_4943,
        7,
        &redirected,
        null,
        FailingReturn.onReturn,
        .{ .result_lifetime = .retained },
    );
    const adopted_id: u32 = 0x4000_0077;

    var completion_builder = message.MessageBuilder.init(allocator);
    defer completion_builder.deinit();
    const completion_root = try completion_builder.initRootAnyPointer();
    try completion_root.setText("deadline-redirect-completion");
    const completion_bytes = try completion_builder.toBytes();
    defer allocator.free(completion_bytes);
    var completion_message = try message.Message.init(allocator, completion_bytes, .{});
    defer completion_message.deinit();
    const completion = try completion_message.getRootAnyPointer();

    var await_builder = protocol.MessageBuilder.init(allocator);
    defer await_builder.deinit();
    var await_ret = try await_builder.beginReturn(logical_id, .awaitFromThirdParty);
    try await_ret.setAcceptFromThirdParty(completion);
    const await_frame = try await_builder.finish();
    defer allocator.free(await_frame);
    try peer.handleFrame(await_frame);

    var answer_builder = protocol.MessageBuilder.init(allocator);
    defer answer_builder.deinit();
    try answer_builder.buildThirdPartyAnswer(adopted_id, completion);
    const answer_frame = try answer_builder.finish();
    defer allocator.free(answer_frame);
    try peer.handleFrame(answer_frame);

    // The question now waits under the adopted wire id, still carrying the
    // deadline it was sent with.
    try std.testing.expect(peer.questions.contains(adopted_id));
    try std.testing.expect(!peer.questions.contains(logical_id));
    try std.testing.expectEqual(@as(usize, 0), redirected.return_count);

    clock.advanceMs(100);
    _ = peer.checkDeadlines();
    try std.testing.expectEqual(@as(usize, 1), redirected.return_count);
    try std.testing.expectEqual(@as(usize, 1), event_recorder.call_deadline_timeouts);
    try std.testing.expectEqual(@as(?u32, adopted_id), event_recorder.last_timeout_question_id);
    try std.testing.expectEqual(@as(usize, 1), event_recorder.cancel_failures);
    const failure = event_recorder.last_cancel_failure.?;
    try std.testing.expectEqual(events.TimeoutKind.call_deadline, failure.kind);
    try std.testing.expectEqual(event_recorder.last_timeout_question_id.?, failure.question_id);
    try std.testing.expectEqual(fail_with, failure.err);
    try std.testing.expectEqual(@as(usize, 0), errors.count);
}

test "a redirected question's cancel_failure from a failing callback carries the timeout event's id" {
    try expectRedirectedCancelFailureMatchesTimeoutId(error.CallTimedOut);
}

test "a redirected question's cancel_failure from a failed cancel (OOM) carries the timeout event's id" {
    try expectRedirectedCancelFailureMatchesTimeoutId(error.OutOfMemory);
}

/// A question callback that re-enters the peer and grows the questions map
/// (forcing rehashes) before it fails, as a callback that issues follow-up
/// calls and then propagates `unwrap()`'s error does.
const ReentrantFailingReturn = struct {
    recorder: *ReturnRecorder,
    return_count: usize = 0,
    new_calls: usize = 0,

    fn onReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        _: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        self.return_count += 1;
        for (0..8) |_| {
            _ = try peer.sendBootstrap(self.recorder, ReturnRecorder.onReturn);
            self.new_calls += 1;
        }
        return error.CallTimedOut;
    }
};

test "failing deadline-cancel callbacks may re-enter the peer and grow the questions map mid-sweep" {
    const allocator = std.testing.allocator;

    var clock = rpc_time.TestClock{};
    var capture = Capture{ .allocator = allocator, .frames = .empty };
    defer capture.deinit();
    var late = ReturnRecorder{};
    var errors = PeerErrorRecorder{};
    var event_recorder = EventRecorder{};

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);
    peer.setObserver(event_recorder.observer());
    peer.setClock(clock.clock());
    peer.setTimeouts(.{ .default_call_timeout_ms = 100 });
    peer.start(&errors, PeerErrorRecorder.onError, null);

    var expiring: [8]ReentrantFailingReturn = undefined;
    for (&expiring) |*cb| {
        cb.* = .{ .recorder = &late };
        _ = try peer.sendBootstrap(cb, ReentrantFailingReturn.onReturn);
    }

    clock.advanceMs(100);
    // Every callback sends new calls, rehashing the map the sweep collected
    // its expired ids from, and then fails. The sweep must cancel exactly the
    // 8 expired questions, each exactly once, report each failure once, and
    // touch none of the calls issued during it.
    try std.testing.expectEqual(@as(usize, 8), peer.checkDeadlines());
    for (expiring) |cb| {
        try std.testing.expectEqual(@as(usize, 1), cb.return_count);
        try std.testing.expectEqual(@as(usize, 8), cb.new_calls);
    }
    try std.testing.expectEqual(@as(usize, 8), event_recorder.cancel_failures);
    try std.testing.expectEqual(@as(usize, 0), errors.count);
    try std.testing.expectEqual(@as(usize, 0), late.return_count);
    try std.testing.expectEqual(@as(u32, 8 + 64), peer.questions.count());
}
