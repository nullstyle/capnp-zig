const std = @import("std");

const endpoint_mod = @import("endpoint.zig");
const wake_mod = @import("wake.zig");

pub const ReceiveResult = struct {
    received_datagram: bool = false,
    wake_drained: bool = false,
};

pub const ReceiveInput = struct {
    io: std.Io,
    driver: endpoint_mod.EndpointDriver,
    wake: *wake_mod.Handle,
    rx_buf: []u8,
    now_us: u64,
    wait_duration: std.Io.Duration,
};

pub fn receiveOne(input: ReceiveInput) !ReceiveResult {
    var result = ReceiveResult{};
    if (input.wake.consumeRequested()) {
        result.wake_drained = true;
        return result;
    }

    var receive_timeout = input.wait_duration;
    if (try waitForUdpOrWake(input.driver, input.wake, input.wait_duration)) |poll_result| {
        result.wake_drained = poll_result.wake_drained;
        if (!poll_result.socket_ready) return result;
        receive_timeout = std.Io.Duration.zero;
    }

    const socket = input.driver.activeSocket();
    const msg = socket.receiveTimeout(input.io, input.rx_buf, .{
        .duration = .{
            .raw = receive_timeout,
            .clock = .awake,
        },
    }) catch |err| switch (err) {
        error.Timeout => return result,
        else => return err,
    };
    if (msg.flags.trunc) return error.DatagramTooLarge;
    result.received_datagram = true;

    _ = try input.driver.handleDatagram(msg.data, msg.from, input.now_us);
    return result;
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
