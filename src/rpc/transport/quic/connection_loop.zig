const std = @import("std");
const quic_zig = @import("quic_zig");

const datagram_io = @import("datagram_io.zig");
const endpoint_mod = @import("endpoint.zig");
const engine_owner = @import("engine_owner.zig");
const mode_router = @import("mode_router.zig");
const scheduler = @import("scheduler.zig");
const wake_mod = @import("wake.zig");

const log = std.log.scoped(.rpc_quic_transport);

pub const StepMode = scheduler.StepMode;
pub const StepResult = scheduler.StepResult;

pub const Owner = struct {
    ptr: *anyopaque,
    io: std.Io,
    role: endpoint_mod.Role,
    udp_rx_buf: []u8,
    udp_tx_buf: []u8,
    wake: *wake_mod.Handle,

    driver: *const fn (ptr: *anyopaque) endpoint_mod.EndpointDriver,
    active_quic_conn: *const fn (ptr: *anyopaque) ?*quic_zig.Connection,
    receive_timeout: *const fn (ptr: *anyopaque) std.Io.Duration,
    selected_mode: *const fn (ptr: *anyopaque) mode_router.Router,
    selected_outbound_empty: *const fn (ptr: *anyopaque) bool,
    engine_owner: *const fn (ptr: *anyopaque) engine_owner.Owner,
    close_requested: *const fn (ptr: *anyopaque) bool,
    request_close: *const fn (ptr: *anyopaque) void,
    terminate_internal_error: *const fn (ptr: *anyopaque, err: anyerror) void,
    flush_close_datagram: *const fn (ptr: *anyopaque) void,
    close_engines: *const fn (ptr: *anyopaque) void,
    invoke_close_callback: *const fn (ptr: *anyopaque) void,
    complete_deferred_deinit: *const fn (ptr: *anyopaque) void,
    reap_server_if_closed: *const fn (ptr: *anyopaque) void,
    now_us: *const fn (ptr: *anyopaque) u64,
};

pub fn run(owner: Owner) void {
    while (!owner.close_requested(owner.ptr)) {
        _ = stepOnce(owner, .wait) catch |err| {
            log.debug("QUIC connection step failed: {}", .{err});
            owner.terminate_internal_error(owner.ptr, err);
            break;
        };
        if (owner.active_quic_conn(owner.ptr)) |conn| {
            if (conn.isClosed() and owner.selected_outbound_empty(owner.ptr)) {
                owner.request_close(owner.ptr);
            }
        }
    }

    owner.flush_close_datagram(owner.ptr);
    owner.close_engines(owner.ptr);
    owner.invoke_close_callback(owner.ptr);
    owner.complete_deferred_deinit(owner.ptr);
}

pub fn stepOnce(owner: Owner, mode: StepMode) !StepResult {
    var now_us = owner.now_us(owner.ptr);
    const next_deadline_us = nextTimerDeadlineUs(owner, now_us);
    const waited_for = scheduler.receiveWaitDuration(.{
        .mode = mode,
        .receive_timeout = owner.receive_timeout(owner.ptr),
        .now_us = now_us,
        .next_deadline_us = next_deadline_us,
        .immediate_work = hasImmediateWork(owner),
        .wake_supported = owner.wake.isSupported(),
    });
    var result = StepResult{
        .waited_for = waited_for,
        .next_deadline_us = next_deadline_us,
    };
    const receive_result = try datagram_io.receiveOne(.{
        .io = owner.io,
        .driver = owner.driver(owner.ptr),
        .wake = owner.wake,
        .rx_buf = owner.udp_rx_buf,
        .now_us = now_us,
        .wait_duration = waited_for,
    });
    result.received_datagram = receive_result.received_datagram;
    result.wake_drained = receive_result.wake_drained;

    now_us = owner.now_us(owner.ptr);
    try advanceActive(owner);
    try serviceModeStreams(owner);
    try datagram_io.drainOutgoingDatagrams(owner.driver(owner.ptr), owner.udp_tx_buf, now_us);

    now_us = owner.now_us(owner.ptr);
    try tickActive(owner, now_us);
    try serviceModeStreams(owner);
    try datagram_io.drainOutgoingDatagrams(owner.driver(owner.ptr), owner.udp_tx_buf, now_us);

    owner.reap_server_if_closed(owner.ptr);
    result.closed = isTransportDrainedClosed(owner);
    if (result.closed) {
        owner.request_close(owner.ptr);
    }
    return result;
}

fn hasImmediateWork(owner: Owner) bool {
    if (owner.wake.isRequested()) return true;
    if (owner.active_quic_conn(owner.ptr)) |conn| {
        if (conn.canSend()) return true;
        if (!owner.selected_outbound_empty(owner.ptr)) {
            return owner.selected_mode(owner.ptr).hasImmediateWork(owner.role, conn);
        }
    }
    return false;
}

fn nextTimerDeadlineUs(owner: Owner, now_us: u64) ?u64 {
    const conn = owner.active_quic_conn(owner.ptr) orelse return null;
    const deadline = conn.nextTimerDeadline(now_us) orelse return null;
    return deadline.at_us;
}

fn isTransportDrainedClosed(owner: Owner) bool {
    const conn = owner.active_quic_conn(owner.ptr) orelse return false;
    return conn.isClosed() and owner.selected_outbound_empty(owner.ptr);
}

fn advanceActive(owner: Owner) !void {
    const conn = owner.active_quic_conn(owner.ptr) orelse return;
    if (!conn.handshakeDone()) {
        try conn.advance();
    }
}

fn tickActive(owner: Owner, now_us: u64) !void {
    const conn = owner.active_quic_conn(owner.ptr) orelse return;
    try conn.tick(now_us);
}

fn serviceModeStreams(owner: Owner) !void {
    const conn = owner.active_quic_conn(owner.ptr) orelse return;
    try owner.selected_mode(owner.ptr).service(owner.engine_owner(owner.ptr), conn);
}
