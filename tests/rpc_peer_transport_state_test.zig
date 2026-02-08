const std = @import("std");
const capnpc = @import("capnpc-zig");

const transport_state = capnpc.rpc.peer_transport_state;

test "peer_transport_state attach/detach toggles transport presence" {
    const StartFn = *const fn (*anyopaque, *u8) void;
    const SendFn = *const fn (*anyopaque, []const u8) anyerror!void;
    const CloseFn = *const fn (*anyopaque) void;
    const IsClosingFn = *const fn (*anyopaque) bool;

    const State = struct {
        transport_ctx: ?*anyopaque = null,
        transport_start: ?StartFn = null,
        transport_send: ?SendFn = null,
        transport_close: ?CloseFn = null,
        transport_is_closing: ?IsClosingFn = null,
    };

    const Hooks = struct {
        fn start(ctx: *anyopaque, peer: *u8) void {
            _ = ctx;
            _ = peer;
        }

        fn send(ctx: *anyopaque, bytes: []const u8) !void {
            _ = ctx;
            _ = bytes;
        }

        fn close(ctx: *anyopaque) void {
            _ = ctx;
        }

        fn isClosing(ctx: *anyopaque) bool {
            _ = ctx;
            return false;
        }
    };

    var ctx: u8 = 0;
    var state = State{};
    try std.testing.expect(!transport_state.hasAttachedTransportForPeer(State, &state));

    transport_state.attachTransportForPeer(
        State,
        StartFn,
        SendFn,
        CloseFn,
        IsClosingFn,
        &state,
        @ptrCast(&ctx),
        Hooks.start,
        Hooks.send,
        Hooks.close,
        Hooks.isClosing,
    );
    try std.testing.expect(transport_state.hasAttachedTransportForPeer(State, &state));

    transport_state.detachTransportForPeer(State, &state);
    try std.testing.expect(!transport_state.hasAttachedTransportForPeer(State, &state));
}

test "peer_transport_state close/isClosing dispatches through registered callbacks" {
    const StartFn = *const fn (*anyopaque, *u8) void;
    const SendFn = *const fn (*anyopaque, []const u8) anyerror!void;
    const CloseFn = *const fn (*anyopaque) void;
    const IsClosingFn = *const fn (*anyopaque) bool;

    const Ctx = struct {
        close_calls: usize = 0,
        closing: bool = false,
    };
    const State = struct {
        transport_ctx: ?*anyopaque = null,
        transport_start: ?StartFn = null,
        transport_send: ?SendFn = null,
        transport_close: ?CloseFn = null,
        transport_is_closing: ?IsClosingFn = null,
    };

    const Hooks = struct {
        fn close(ctx_ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ctx_ptr));
            ctx.close_calls += 1;
        }

        fn isClosing(ctx_ptr: *anyopaque) bool {
            const ctx: *Ctx = @ptrCast(@alignCast(ctx_ptr));
            return ctx.closing;
        }
    };

    var ctx = Ctx{};
    var state = State{};
    transport_state.attachTransportForPeer(
        State,
        StartFn,
        SendFn,
        CloseFn,
        IsClosingFn,
        &state,
        @ptrCast(&ctx),
        null,
        null,
        Hooks.close,
        Hooks.isClosing,
    );

    try std.testing.expect(!transport_state.isAttachedTransportClosingForPeer(State, &state));
    transport_state.closeAttachedTransportForPeer(State, &state);
    try std.testing.expectEqual(@as(usize, 1), ctx.close_calls);

    ctx.closing = true;
    try std.testing.expect(transport_state.isAttachedTransportClosingForPeer(State, &state));
}

test "peer_transport_state get/take attached connection returns typed pointer and detaches" {
    const StartFn = *const fn (*anyopaque, *u8) void;
    const SendFn = *const fn (*anyopaque, []const u8) anyerror!void;
    const CloseFn = *const fn (*anyopaque) void;
    const IsClosingFn = *const fn (*anyopaque) bool;

    const Conn = struct { id: u32 };
    const State = struct {
        transport_ctx: ?*anyopaque = null,
        transport_start: ?StartFn = null,
        transport_send: ?SendFn = null,
        transport_close: ?CloseFn = null,
        transport_is_closing: ?IsClosingFn = null,
    };
    const Hooks = struct {
        fn detach(state: *State) void {
            transport_state.detachTransportForPeer(State, state);
        }
    };

    var conn = Conn{ .id = 42 };
    var state = State{
        .transport_ctx = @ptrCast(&conn),
    };

    const attached = transport_state.getAttachedConnectionForPeer(State, *Conn, &state) orelse return error.MissingConn;
    try std.testing.expectEqual(@as(u32, 42), attached.id);

    const taken = transport_state.takeAttachedConnectionForPeer(State, *Conn, &state, Hooks.detach) orelse return error.MissingConn;
    try std.testing.expectEqual(@as(u32, 42), taken.id);
    try std.testing.expect(state.transport_ctx == null);
}
