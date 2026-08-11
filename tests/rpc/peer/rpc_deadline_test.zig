const std = @import("std");
const capnpc = @import("capnpc-zig");

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
    backpressure_events: usize = 0,
    last_timeout_question_id: ?u32 = null,

    fn onEvent(ctx_ptr: *anyopaque, event: events.Event) void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        switch (event) {
            .timeout => |t| {
                switch (t.kind) {
                    .call_deadline => {
                        self.call_deadline_timeouts += 1;
                        self.last_timeout_question_id = t.question_id;
                    },
                    .shutdown_drain => self.shutdown_drain_timeouts += 1,
                    .idle_connection => self.idle_timeouts += 1,
                    .parked_accept => self.parked_accept_timeouts += 1,
                }
            },
            .backpressure => self.backpressure_events += 1,
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
