const std = @import("std");

/// Redacted, transport-general RPC event observer.
///
/// Event payloads carry only metadata: source, role, lifecycle phase, frame
/// sizes, resource classes, message tags, and error names. They deliberately do
/// not carry frame bytes, QUIC datagrams, application sideband payloads,
/// certificates, keys, or wire close reason text.
pub const Observer = struct {
    ctx: *anyopaque,
    on_event: *const fn (ctx: *anyopaque, event: Event) void,

    pub fn init(
        ctx: *anyopaque,
        on_event: *const fn (ctx: *anyopaque, event: Event) void,
    ) Observer {
        return .{
            .ctx = ctx,
            .on_event = on_event,
        };
    }

    pub inline fn notify(self: Observer, event: Event) void {
        self.on_event(self.ctx, event);
    }
};

pub const Source = enum {
    tcp,
    quic_baseline,
    quic_native,
    peer,
    host_peer,
    worker_pool,
};

pub const Role = enum {
    unknown,
    client,
    server,
};

pub const ConnectionPhase = enum {
    initialized,
    accepted,
    started,
    closing,
    closed,
    rejected,
};

pub const FrameStage = enum {
    enqueued,
    sent,
    received,
};

pub const Resource = enum {
    frame_bytes,
    write_queue_items,
    write_queue_bytes,
    outbound_queue_items,
    outbound_queue_bytes,
    outbound_data_streams,
    outbound_data_bytes,
    host_outbound_frames,
    host_outbound_bytes,
    host_call_frames,
    host_call_bytes,
    peer_state,
};

pub const Event = union(enum) {
    connection: ConnectionEvent,
    frame: FrameEvent,
    backpressure: BackpressureEvent,
    resource_rejection: ResourceRejectionEvent,
    protocol_error: ProtocolErrorEvent,
    close: CloseEvent,
    timeout: TimeoutEvent,
};

pub const ConnectionEvent = struct {
    source: Source,
    role: Role = .unknown,
    phase: ConnectionPhase,
};

pub const FrameEvent = struct {
    source: Source,
    role: Role = .unknown,
    stage: FrameStage,
    bytes: usize,
};

pub const BackpressureEvent = struct {
    source: Source,
    role: Role = .unknown,
    resource: Resource,
    attempted_bytes: ?usize = null,
    limit: ?usize = null,
    err: anyerror,
};

pub const ResourceRejectionEvent = struct {
    source: Source,
    role: Role = .unknown,
    resource: Resource,
    attempted: ?usize = null,
    limit: ?usize = null,
    err: anyerror,
};

pub const ProtocolErrorEvent = struct {
    source: Source,
    role: Role = .unknown,
    err: anyerror,
    message_tag: ?[]const u8 = null,
};

pub const CloseEvent = struct {
    source: Source,
    role: Role = .unknown,
    err: ?anyerror = null,
};

pub const TimeoutKind = enum {
    /// An outbound call exceeded its deadline and was cancelled.
    call_deadline,
    /// A connection saw no traffic for longer than its idle limit and was reaped.
    idle_connection,
    /// A graceful shutdown drain exceeded its bound; remaining in-flight
    /// questions were force-cancelled.
    shutdown_drain,
};

pub const TimeoutEvent = struct {
    source: Source,
    role: Role = .unknown,
    kind: TimeoutKind,
    /// Question ID for `call_deadline`; null for connection-scoped kinds.
    question_id: ?u32 = null,
};

pub inline fn emit(observer: ?Observer, event: Event) void {
    if (observer) |obs| obs.notify(event);
}

pub inline fn emitConnection(
    observer: ?Observer,
    source: Source,
    role: Role,
    phase: ConnectionPhase,
) void {
    emit(observer, .{ .connection = .{
        .source = source,
        .role = role,
        .phase = phase,
    } });
}

pub inline fn emitFrame(
    observer: ?Observer,
    source: Source,
    role: Role,
    stage: FrameStage,
    bytes: usize,
) void {
    emit(observer, .{ .frame = .{
        .source = source,
        .role = role,
        .stage = stage,
        .bytes = bytes,
    } });
}

pub inline fn emitBackpressure(
    observer: ?Observer,
    source: Source,
    role: Role,
    resource: Resource,
    attempted_bytes: ?usize,
    limit: ?usize,
    err: anyerror,
) void {
    emit(observer, .{ .backpressure = .{
        .source = source,
        .role = role,
        .resource = resource,
        .attempted_bytes = attempted_bytes,
        .limit = limit,
        .err = err,
    } });
}

pub inline fn emitResourceRejection(
    observer: ?Observer,
    source: Source,
    role: Role,
    resource: Resource,
    attempted: ?usize,
    limit: ?usize,
    err: anyerror,
) void {
    emit(observer, .{ .resource_rejection = .{
        .source = source,
        .role = role,
        .resource = resource,
        .attempted = attempted,
        .limit = limit,
        .err = err,
    } });
}

pub inline fn emitProtocolError(
    observer: ?Observer,
    source: Source,
    role: Role,
    err: anyerror,
    message_tag: ?[]const u8,
) void {
    emit(observer, .{ .protocol_error = .{
        .source = source,
        .role = role,
        .err = err,
        .message_tag = message_tag,
    } });
}

pub inline fn emitClose(
    observer: ?Observer,
    source: Source,
    role: Role,
    err: ?anyerror,
) void {
    emit(observer, .{ .close = .{
        .source = source,
        .role = role,
        .err = err,
    } });
}

pub inline fn emitTimeout(
    observer: ?Observer,
    source: Source,
    role: Role,
    kind: TimeoutKind,
    question_id: ?u32,
) void {
    emit(observer, .{ .timeout = .{
        .source = source,
        .role = role,
        .kind = kind,
        .question_id = question_id,
    } });
}

test "observer receives redacted frame metadata only" {
    const State = struct {
        count: usize = 0,
        last: ?Event = null,

        fn onEvent(ctx: *anyopaque, event: Event) void {
            const state: *@This() = @ptrCast(@alignCast(ctx));
            state.count += 1;
            state.last = event;
        }
    };

    var state = State{};
    const observer = Observer.init(&state, State.onEvent);

    emitFrame(observer, .tcp, .server, .received, 128);

    try std.testing.expectEqual(@as(usize, 1), state.count);
    const event = state.last orelse return error.ExpectedEvent;
    try std.testing.expect(event == .frame);
    try std.testing.expectEqual(Source.tcp, event.frame.source);
    try std.testing.expectEqual(Role.server, event.frame.role);
    try std.testing.expectEqual(FrameStage.received, event.frame.stage);
    try std.testing.expectEqual(@as(usize, 128), event.frame.bytes);
}

test "null observer is a no-op" {
    emitConnection(null, .peer, .unknown, .started);
    emitClose(null, .peer, .unknown, null);
}
