const std = @import("std");

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
};

pub fn run(owner: Owner) void {
    while (!owner.close_requested(owner.ptr)) {
        _ = stepOnce(owner, .wait) catch |err| {
            log.debug("QUIC connection step failed: {}", .{err});
            owner.terminate_internal_error(owner.ptr, err);
            break;
        };
        if (owner.driver(owner.ptr).quicConnection()) |conn| {
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
    const driver = owner.driver(owner.ptr);
    var now_us = driver.nowUs();
    const next_deadline_us = nextTimerDeadlineUs(driver, now_us);
    const waited_for = scheduler.receiveWaitDuration(.{
        .mode = mode,
        .receive_timeout = driver.receiveTimeout(),
        .now_us = now_us,
        .next_deadline_us = next_deadline_us,
        .immediate_work = hasImmediateWork(owner, driver),
        .wake_supported = owner.wake.isSupported(),
    });
    var result = StepResult{
        .waited_for = waited_for,
        .next_deadline_us = next_deadline_us,
    };
    const receive_result = try datagram_io.receiveOne(.{
        .io = owner.io,
        .driver = driver,
        .wake = owner.wake,
        .rx_buf = owner.udp_rx_buf,
        .now_us = now_us,
        .wait_duration = waited_for,
    });
    result.received_datagram = receive_result.received_datagram;
    result.wake_drained = receive_result.wake_drained;

    now_us = driver.nowUs();
    try advanceActive(driver);
    try serviceModeStreams(owner, driver);
    try datagram_io.drainOutgoingDatagrams(driver, owner.udp_tx_buf, now_us);

    now_us = driver.nowUs();
    try tickActive(driver, now_us);
    try serviceModeStreams(owner, driver);
    try datagram_io.drainOutgoingDatagrams(driver, owner.udp_tx_buf, now_us);

    if (driver.reapClosed()) {
        owner.request_close(owner.ptr);
    }
    result.closed = isTransportDrainedClosed(owner, driver);
    if (result.closed) {
        owner.request_close(owner.ptr);
    }
    return result;
}

fn hasImmediateWork(owner: Owner, driver: endpoint_mod.EndpointDriver) bool {
    if (owner.wake.isRequested()) return true;
    if (driver.quicConnection()) |conn| {
        if (conn.canSend()) return true;
        if (!owner.selected_outbound_empty(owner.ptr)) {
            return owner.selected_mode(owner.ptr).hasImmediateWork(owner.role, conn);
        }
    }
    return false;
}

fn nextTimerDeadlineUs(driver: endpoint_mod.EndpointDriver, now_us: u64) ?u64 {
    const conn = driver.quicConnection() orelse return null;
    const deadline = conn.nextTimerDeadline(now_us) orelse return null;
    return deadline.at_us;
}

fn isTransportDrainedClosed(owner: Owner, driver: endpoint_mod.EndpointDriver) bool {
    const conn = driver.quicConnection() orelse return false;
    return conn.isClosed() and owner.selected_outbound_empty(owner.ptr);
}

fn advanceActive(driver: endpoint_mod.EndpointDriver) !void {
    const conn = driver.quicConnection() orelse return;
    if (!conn.handshakeDone()) {
        try conn.advance();
    }
}

fn tickActive(driver: endpoint_mod.EndpointDriver, now_us: u64) !void {
    const conn = driver.quicConnection() orelse return;
    try conn.tick(now_us);
}

fn serviceModeStreams(owner: Owner, driver: endpoint_mod.EndpointDriver) !void {
    const conn = driver.quicConnection() orelse return;
    try owner.selected_mode(owner.ptr).service(owner.engine_owner(owner.ptr), conn);
}
