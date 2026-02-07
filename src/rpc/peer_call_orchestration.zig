const std = @import("std");
const cap_table = @import("cap_table.zig");
const peer_call_targets = @import("peer_call_targets.zig");
const protocol = @import("protocol.zig");

pub fn routeCallTarget(call: protocol.Call) !union(enum) {
    imported: u32,
    promised: protocol.PromisedAnswer,
} {
    return switch (call.target.tag) {
        .imported_cap => .{ .imported = call.target.imported_cap orelse return error.MissingCallTarget },
        .promised_answer => .{ .promised = call.target.promised_answer orelse return error.MissingCallTarget },
    };
}

pub fn handleResolvedExportedCall(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    call: protocol.Call,
    inbound_caps: *const InboundCapsType,
    has_export: bool,
    is_promise: bool,
    resolved: ?cap_table.ResolvedCap,
    handler_ctx: ?*anyopaque,
    handler_on_call: ?*const fn (*anyopaque, *PeerType, protocol.Call, *const InboundCapsType) anyerror!void,
    note_call_send_results: *const fn (*PeerType, protocol.Call) anyerror!void,
    handle_resolved_call: *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    try note_call_send_results(peer, call);

    if (!has_export) {
        try send_return_exception(peer, call.question_id, "unknown promised capability");
        return;
    }

    if (is_promise) {
        const next = resolved orelse {
            try send_return_exception(peer, call.question_id, "promised capability unresolved");
            return;
        };
        if (next == .none) {
            try send_return_exception(peer, call.question_id, "promise broken");
            return;
        }
        try handle_resolved_call(peer, call, inbound_caps, next);
        return;
    }

    const on_call = handler_on_call orelse {
        try send_return_exception(peer, call.question_id, "missing promised capability handler");
        return;
    };
    const ctx = handler_ctx orelse {
        try send_return_exception(peer, call.question_id, "missing promised capability handler");
        return;
    };
    on_call(ctx, peer, call, inbound_caps) catch |err| {
        try send_return_exception(peer, call.question_id, @errorName(err));
    };
}

pub fn dispatchImportedTargetPlan(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    call: protocol.Call,
    inbound_caps: *const InboundCapsType,
    target_plan: peer_call_targets.ImportedTargetPlan,
    handler_ctx: ?*anyopaque,
    handler_on_call: ?*const fn (*anyopaque, *PeerType, protocol.Call, *const InboundCapsType) anyerror!void,
    note_call_send_results: *const fn (*PeerType, protocol.Call) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    handle_resolved_call: *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap) anyerror!void,
) !void {
    switch (target_plan) {
        .promise_broken => {
            try send_return_exception(peer, call.question_id, "promise broken");
        },
        .handle_resolved => |resolved| {
            try handle_resolved_call(peer, call, inbound_caps, resolved);
        },
        .call_handler => {
            try note_call_send_results(peer, call);
            const on_call = handler_on_call orelse {
                try send_return_exception(peer, call.question_id, "missing export handler");
                return;
            };
            const ctx = handler_ctx orelse {
                try send_return_exception(peer, call.question_id, "missing export handler");
                return;
            };
            on_call(ctx, peer, call, inbound_caps) catch |err| {
                try send_return_exception(peer, call.question_id, @errorName(err));
            };
        },
        .missing_export_handler => {
            try note_call_send_results(peer, call);
            try send_return_exception(peer, call.question_id, "missing export handler");
        },
        .unknown_capability, .queue_promise_export => unreachable,
    }
}

test "peer_call_orchestration routeCallTarget enforces required target payloads" {
    const imported = protocol.Call{
        .question_id = 1,
        .target = .{
            .tag = .imported_cap,
            .imported_cap = 77,
            .promised_answer = null,
        },
        .interface_id = 0,
        .method_id = 0,
        .params = .{
            .content = undefined,
            .cap_table = null,
        },
        .send_results_to = .{
            .tag = .caller,
            .third_party = null,
        },
    };
    const imported_target = try routeCallTarget(imported);
    try std.testing.expectEqual(@as(u32, 77), imported_target.imported);

    const bad_imported = protocol.Call{
        .question_id = 1,
        .target = .{
            .tag = .imported_cap,
            .imported_cap = null,
            .promised_answer = null,
        },
        .interface_id = 0,
        .method_id = 0,
        .params = .{
            .content = undefined,
            .cap_table = null,
        },
        .send_results_to = .{
            .tag = .caller,
            .third_party = null,
        },
    };
    try std.testing.expectError(error.MissingCallTarget, routeCallTarget(bad_imported));
}

test "peer_call_orchestration dispatchImportedTargetPlan invokes handler and note-send-results" {
    const InboundCaps = struct {};
    const State = struct {
        note_calls: usize = 0,
        handler_calls: usize = 0,
    };
    const Hooks = struct {
        fn note(state: *State, call: protocol.Call) !void {
            _ = call;
            state.note_calls += 1;
        }

        fn sendException(state: *State, answer_id: u32, reason: []const u8) !void {
            _ = state;
            _ = answer_id;
            _ = reason;
            return error.TestUnexpectedResult;
        }

        fn handleResolved(state: *State, call: protocol.Call, inbound: *const InboundCaps, resolved: cap_table.ResolvedCap) !void {
            _ = state;
            _ = call;
            _ = inbound;
            _ = resolved;
            return error.TestUnexpectedResult;
        }

        fn onCall(ctx: *anyopaque, state: *State, call: protocol.Call, inbound: *const InboundCaps) !void {
            _ = call;
            _ = inbound;
            const marker: *u32 = @ptrCast(@alignCast(ctx));
            marker.* += 1;
            state.handler_calls += 1;
        }
    };

    var marker: u32 = 0;
    var state = State{};
    const call = protocol.Call{
        .question_id = 5,
        .target = .{
            .tag = .imported_cap,
            .imported_cap = 1,
            .promised_answer = null,
        },
        .interface_id = 0,
        .method_id = 0,
        .params = .{
            .content = undefined,
            .cap_table = null,
        },
        .send_results_to = .{
            .tag = .caller,
            .third_party = null,
        },
    };
    const inbound = InboundCaps{};

    try dispatchImportedTargetPlan(
        State,
        InboundCaps,
        &state,
        call,
        &inbound,
        .call_handler,
        &marker,
        Hooks.onCall,
        Hooks.note,
        Hooks.sendException,
        Hooks.handleResolved,
    );

    try std.testing.expectEqual(@as(usize, 1), state.note_calls);
    try std.testing.expectEqual(@as(usize, 1), state.handler_calls);
    try std.testing.expectEqual(@as(u32, 1), marker);
}
