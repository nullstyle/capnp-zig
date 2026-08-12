const std = @import("std");
const builtin = @import("builtin");

const datagram_drop = @import("datagram_drop.zig");
const endpoint_mod = @import("endpoint.zig");
const events = @import("../../events.zig");
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
    /// Reporting context for a dropped oversized datagram. Deliberately not
    /// defaulted: a wrong `source`/`role` is worse than a compile error, and
    /// the drop is otherwise invisible to the application.
    observer: ?events.Observer,
    source: events.Source,
    role: events.Role,
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
        // A datagram larger than rx_buf was truncated. Per-datagram fault:
        // drop it and keep serving (see datagram_drop). Socket-fatal errors
        // still propagate via the switch above.
        datagram_drop.report(input.observer, input.source, input.role, input.rx_buf.len);
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
        .truncated => {
            // Same per-datagram fault the POSIX path treats as non-fatal.
            // Socket-fatal errors still propagate through the `try` above.
            datagram_drop.report(input.observer, input.source, input.role, input.rx_buf.len);
            result.dropped_datagram = true;
            return result;
        },
        .datagram => |msg| {
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
