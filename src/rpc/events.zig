const std = @import("std");

/// Redacted, transport-general RPC event observer.
///
/// Event payloads carry only metadata: source, role, lifecycle phase, frame
/// sizes, resource classes, message tags, and error names. They deliberately do
/// not carry frame bytes, QUIC datagrams, application sideband payloads,
/// certificates, keys, or wire close reason text.
///
/// ## Threading contract
///
/// Observer callbacks are invoked synchronously on the thread that owns the
/// emitting component — for connections and sessions, the owner/run-loop
/// thread; for `Peer`/`HostPeer`, the peer's owner thread. Cross-thread entry
/// points (`requestClose`, `wake`, cross-thread `close` on QUIC server
/// sessions) never invoke the observer directly: lifecycle events they
/// trigger (`.closing`) are deferred to the loop thread, which emits them
/// when it observes the request. Observers may therefore rely on:
///
/// - Callbacks for one connection never run concurrently, and its events are
///   totally ordered: `.closing`, when it fires, precedes the terminal
///   `.close`/`.closed` pair.
/// - `.closing` fires at most once per connection, and only for a local
///   deliberate close (`close`/`requestClose`), not for remote EOF or error
///   teardown.
/// - A deferred `.closing` needs a running loop to observe the request: if
///   the loop never runs, or has already exited, the event is dropped.
///
/// Callbacks run inline in transport loops, so they should be cheap and must
/// not call back into the emitting connection.
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
    outbound_questions,
    /// Caller-owned outbound answers deliberately kept open after Return.
    retained_questions,
    active_inbound_questions,
    queued_calls,
    queued_call_bytes,
    resolved_imports,
    persistent_exports,
    sturdy_ref_bytes,
    /// Parked Accept entries held while waiting for a matching Provide.
    parked_accepts,
    /// Attributable recipient-token and embargo bytes held by parked Accepts.
    parked_accept_bytes,
    /// Aggregate live L4 Join lifecycle records on one peer.
    join_records,
    /// Inbound Join key parts retained by incomplete Join buckets.
    join_parts,
    /// Canonically-owned direct-Accept provision bytes for hosted JoinResult.
    join_accept_bytes,
    /// One inbound UDP datagram measured against a QUIC endpoint's
    /// `udp_rx_buffer_size`. Rejected per-datagram — dropped while the
    /// endpoint and every session on it keep running — because UDP is
    /// unauthenticated and any host can send one.
    udp_datagram_bytes,
};

/// Transport-agnostic typed close cause — the "death certificate" a
/// transport hands the peer layer when a connection dies. Today every
/// cause collapses into one untyped disconnect; this enum is how a
/// transport that KNOWS more (QUIC's CloseEvent) says so. `.unknown` is
/// the compatible default for transports that cannot distinguish causes
/// (TCP today), so absence of plumbing never misreports a cause.
pub const DisconnectCause = enum {
    /// No typed cause was available.
    unknown,
    /// We closed the connection locally and cleanly.
    local_close,
    /// The remote endpoint closed the connection (its CONNECTION_CLOSE
    /// or equivalent reached us).
    peer_close,
    /// The transport's idle timer expired — nothing heard from the
    /// remote; says nothing about whether it is alive.
    idle_timeout,
    /// A QUIC stateless reset matched an installed token: PROOF the
    /// remote endpoint lost its connection state (crash-restart class)
    /// while its host is still reachable. The strongest restore signal
    /// the durable-caps ladder has.
    stateless_reset,
    /// A local transport-layer error terminated the connection.
    transport_error,
};

pub const Event = union(enum) {
    connection: ConnectionEvent,
    frame: FrameEvent,
    backpressure: BackpressureEvent,
    resource_rejection: ResourceRejectionEvent,
    protocol_error: ProtocolErrorEvent,
    close: CloseEvent,
    timeout: TimeoutEvent,
    pressure: PressureEvent,
    call_latency: CallLatencyEvent,
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
    /// An inbound Accept waited past the vat's parked-Accept deadline.
    parked_accept,
    /// An inbound L4 Join phase exceeded its local lease.
    join,
};

pub const TimeoutEvent = struct {
    source: Source,
    role: Role = .unknown,
    kind: TimeoutKind,
    /// Outbound question ID for `call_deadline`; null for every other kind.
    question_id: ?u32 = null,
    /// Inbound answer ID for `parked_accept` and `join`; null for every other
    /// kind. No Join key, target, provision, or address enters this event.
    answer_id: ?u32 = null,
};

/// Early-warning signal: a bounded resource crossed 80% of its budget.
/// Emitted once per upward crossing (not on every insertion above the
/// threshold), so consumers can alert without rate-limiting.
pub const PressureEvent = struct {
    source: Source,
    role: Role = .unknown,
    resource: Resource,
    current: usize,
    limit: usize,
};

/// Wall time between sending a Call and dispatching its Return, measured
/// on the peer's monotonic clock. Only emitted when the peer has a clock.
pub const CallLatencyEvent = struct {
    source: Source,
    role: Role = .unknown,
    question_id: u32,
    nanoseconds: u64,
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

/// Emit a redacted parked-Accept expiry. The answer id is enough to correlate
/// the lifecycle locally; recipient tokens, embargoes, and frame bytes never
/// enter the event API.
pub inline fn emitParkedAcceptTimeout(
    observer: ?Observer,
    answer_id: u32,
) void {
    emit(observer, .{ .timeout = .{
        .source = .peer,
        .role = .unknown,
        .kind = .parked_accept,
        .answer_id = answer_id,
    } });
}

/// Emit a redacted L4 Join expiry. The inbound answer id is the only protocol
/// value exposed; key parts, capabilities, provisions, and addresses stay out
/// of the operability surface.
pub inline fn emitJoinTimeout(observer: ?Observer, answer_id: u32) void {
    emit(observer, .{ .timeout = .{
        .source = .peer,
        .role = .unknown,
        .kind = .join,
        .answer_id = answer_id,
    } });
}

/// 80% pressure threshold for a budget. Integer-exact: `limit - limit/5`.
pub inline fn pressureThreshold(limit: usize) usize {
    return limit - limit / 5;
}

/// Emit a pressure event iff this change crossed the 80% threshold of
/// `limit` from below. Stateless crossing detection: callers pass the
/// resource's occupancy before and after the insertion.
pub inline fn emitPressureCrossing(
    observer: ?Observer,
    source: Source,
    role: Role,
    resource: Resource,
    prev: usize,
    current: usize,
    limit: usize,
) void {
    if (observer == null or limit == 0) return;
    const threshold = pressureThreshold(limit);
    if (prev < threshold and current >= threshold) {
        emit(observer, .{ .pressure = .{
            .source = source,
            .role = role,
            .resource = resource,
            .current = current,
            .limit = limit,
        } });
    }
}

pub inline fn emitCallLatency(
    observer: ?Observer,
    source: Source,
    role: Role,
    question_id: u32,
    nanoseconds: u64,
) void {
    emit(observer, .{ .call_latency = .{
        .source = source,
        .role = role,
        .question_id = question_id,
        .nanoseconds = nanoseconds,
    } });
}

test "emitPressureCrossing fires exactly on the upward 80% crossing" {
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

    const limit: usize = 100;
    try std.testing.expectEqual(@as(usize, 80), pressureThreshold(limit));

    // Below threshold: no event.
    emitPressureCrossing(observer, .peer, .unknown, .outbound_questions, 78, 79, limit);
    try std.testing.expectEqual(@as(usize, 0), state.count);

    // Crossing: exactly one event.
    emitPressureCrossing(observer, .peer, .unknown, .outbound_questions, 79, 80, limit);
    try std.testing.expectEqual(@as(usize, 1), state.count);
    try std.testing.expect(state.last.? == .pressure);
    try std.testing.expectEqual(@as(usize, 80), state.last.?.pressure.current);

    // Already above: no repeat.
    emitPressureCrossing(observer, .peer, .unknown, .outbound_questions, 80, 81, limit);
    try std.testing.expectEqual(@as(usize, 1), state.count);

    // Byte-style jump across the threshold also fires.
    emitPressureCrossing(observer, .peer, .unknown, .queued_call_bytes, 10, 95, limit);
    try std.testing.expectEqual(@as(usize, 2), state.count);
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
