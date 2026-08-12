const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_quic);

const endpoint_mod = @import("endpoint.zig");
const non_windows_receive = @import("non_windows_receive.zig");
const udp_receive_bridge = @import("udp_receive_bridge.zig");
const wake_mod = @import("wake.zig");

pub const ReceiveResult = struct {
    received_datagram: bool = false,
    wake_drained: bool = false,
    /// A datagram was received but dropped as a per-datagram fault (e.g.
    /// truncation) rather than processed. The endpoint stays alive.
    dropped_datagram: bool = false,
};

pub const ReceiveInput = struct {
    io: std.Io,
    driver: endpoint_mod.EndpointDriver,
    wake: *wake_mod.Handle,
    receive_bridge: *udp_receive_bridge.Bridge,
    rx_buf: []u8,
    now_us: u64,
    wait_duration: std.Io.Duration,
};

pub fn receiveOne(input: ReceiveInput) !ReceiveResult {
    var result = ReceiveResult{};
    if (input.wake.consumeRequested()) {
        if (comptime builtin.target.os.tag == .windows) {
            input.receive_bridge.consumeWake(input.io);
        }
        result.wake_drained = true;
        return result;
    }

    if (comptime builtin.target.os.tag == .windows) {
        return receiveOneWindows(input);
    }

    var receive_timeout = input.wait_duration;
    if (try waitForUdpOrWake(input.driver, input.wake, input.wait_duration)) |poll_result| {
        result.wake_drained = poll_result.wake_drained;
        if (!poll_result.socket_ready) return result;
        receive_timeout = std.Io.Duration.zero;
    }

    const socket = input.driver.activeSocket();
    const msg = non_windows_receive.receive(socket, input.io, input.rx_buf, receive_timeout) catch |err| switch (err) {
        error.Timeout => return result,
        else => return err,
    };
    if (msg.flags.trunc) {
        // A datagram larger than rx_buf was truncated. UDP is unauthenticated
        // and spoofable, so a single oversized datagram from any host must not
        // tear down the endpoint (and, for a fanout server, every session on
        // it). Treat it as a per-datagram fault: drop it and keep serving.
        // Socket-fatal errors still propagate via the switch above.
        log.warn("dropping truncated UDP datagram (exceeds {d}-byte rx buffer)", .{input.rx_buf.len});
        result.dropped_datagram = true;
        return result;
    }
    result.received_datagram = true;

    _ = try input.driver.handleDatagram(msg.data, msg.from, input.now_us);
    return result;
}

fn receiveOneWindows(input: ReceiveInput) !ReceiveResult {
    var result = ReceiveResult{};
    const received = try input.receive_bridge.receive(
        input.io,
        input.driver.activeSocket().*,
        input.rx_buf,
        input.wait_duration,
    );
    switch (received) {
        .timeout => return result,
        .wake => {
            result.wake_drained = input.wake.consumeRequested();
            return result;
        },
        .datagram => |msg| {
            if (msg.flags.trunc) {
                log.warn("dropping truncated UDP datagram (exceeds {d}-byte rx buffer)", .{input.rx_buf.len});
                result.dropped_datagram = true;
                return result;
            }
            result.received_datagram = true;
            _ = try input.driver.handleDatagram(msg.data, msg.from, input.now_us);
            return result;
        },
    }
}

pub fn drainOutgoingDatagrams(
    driver: endpoint_mod.EndpointDriver,
    tx_buf: []u8,
    now_us: u64,
) !void {
    try driver.drainOutgoingDatagrams(tx_buf, now_us);
}

fn waitForUdpOrWake(
    driver: endpoint_mod.EndpointDriver,
    wake: *wake_mod.Handle,
    wait_duration: std.Io.Duration,
) !?wake_mod.PollResult {
    const socket = driver.activeSocket();
    return try wake.waitForSocket(socket.handle, wait_duration);
}
