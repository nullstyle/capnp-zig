const std = @import("std");
const cap_table = @import("../../../level0/cap_table.zig");
const peer_call_targets = @import("peer_call_targets.zig");
const protocol = @import("../../../level0/protocol.zig");

pub fn routeCallTarget(call: protocol.Call) !union(enum) {
    imported: u32,
    promised: protocol.PromisedAnswer,
} {
    return switch (call.target.tag) {
        .importedCap => .{ .imported = call.target.imported_cap orelse return error.MissingCallTarget },
        .promisedAnswer => .{ .promised = call.target.promised_answer orelse return error.MissingCallTarget },
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
        .unknown_capability => {
            try note_call_send_results(peer, call);
            try send_return_exception(peer, call.question_id, "unknown capability");
        },
        .queue_promise_export => unreachable, // filtered by handleCallImportedTargetForPeer before dispatch
    }
}

pub fn handleResolvedExportedCallForPeer(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    call: protocol.Call,
    inbound_caps: *const InboundCapsType,
    export_id: u32,
    note_call_send_results: *const fn (*PeerType, protocol.Call) anyerror!void,
    handle_resolved_call: *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    const exported_entry = peer.exports.getEntry(export_id) orelse {
        try send_return_exception(peer, call.question_id, "unknown promised capability");
        return;
    };
    const handler = exported_entry.value_ptr.handler;

    try handleResolvedExportedCall(
        PeerType,
        InboundCapsType,
        peer,
        call,
        inbound_caps,
        true,
        exported_entry.value_ptr.is_promise,
        exported_entry.value_ptr.resolved,
        if (handler) |h| h.ctx else null,
        if (handler) |h| h.on_call else null,
        note_call_send_results,
        handle_resolved_call,
        send_return_exception,
    );
}

pub fn handleResolvedExportedCallForPeerFn(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    comptime note_call_send_results: *const fn (*PeerType, protocol.Call) anyerror!void,
    comptime handle_resolved_call: *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap) anyerror!void,
    comptime send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
) *const fn (*PeerType, protocol.Call, *const InboundCapsType, u32) anyerror!void {
    return struct {
        fn call(peer: *PeerType, rpc_call: protocol.Call, inbound_caps: *const InboundCapsType, export_id: u32) anyerror!void {
            try handleResolvedExportedCallForPeer(
                PeerType,
                InboundCapsType,
                peer,
                rpc_call,
                inbound_caps,
                export_id,
                note_call_send_results,
                handle_resolved_call,
                send_return_exception,
            );
        }
    }.call;
}

pub fn handleCallImportedTargetForPeer(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    frame: []const u8,
    call: protocol.Call,
    export_id: u32,
    queue_promise_export_call: *const fn (*PeerType, u32, []const u8, InboundCapsType) anyerror!void,
    release_inbound_caps: *const fn (*PeerType, *InboundCapsType) anyerror!void,
    report_nonfatal_error: *const fn (*PeerType, anyerror) void,
    note_call_send_results: *const fn (*PeerType, protocol.Call) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    handle_resolved_call: *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap) anyerror!void,
) !void {
    var inbound_caps = try InboundCapsType.init(peer.allocator, call.params.cap_table, &peer.caps);
    const exported_entry = peer.exports.getEntry(export_id);
    const target_plan = peer_call_targets.planImportedTarget(
        exported_entry != null,
        if (exported_entry) |entry| entry.value_ptr.is_promise else false,
        if (exported_entry) |entry| entry.value_ptr.resolved else null,
        if (exported_entry) |entry| entry.value_ptr.handler != null else false,
    );

    switch (target_plan) {
        .unknown_capability => {
            inbound_caps.deinit();
            try send_return_exception(peer, call.question_id, "unknown capability");
            return;
        },
        .queue_promise_export => {
            // Queue ownership transfers inbound caps and frame bytes to pending state.
            try queue_promise_export_call(peer, export_id, frame, inbound_caps);
            return;
        },
        else => {},
    }

    defer inbound_caps.deinit();
    defer release_inbound_caps(peer, &inbound_caps) catch |err| {
        report_nonfatal_error(peer, err);
    };

    const handler = if (exported_entry) |entry| entry.value_ptr.handler else null;
    try dispatchImportedTargetPlan(
        PeerType,
        InboundCapsType,
        peer,
        call,
        &inbound_caps,
        target_plan,
        if (handler) |h| h.ctx else null,
        if (handler) |h| h.on_call else null,
        note_call_send_results,
        send_return_exception,
        handle_resolved_call,
    );
}

pub fn handleCallImportedTargetForPeerFn(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    comptime queue_promise_export_call: *const fn (*PeerType, u32, []const u8, InboundCapsType) anyerror!void,
    comptime release_inbound_caps: *const fn (*PeerType, *InboundCapsType) anyerror!void,
    comptime report_nonfatal_error: *const fn (*PeerType, anyerror) void,
    comptime note_call_send_results: *const fn (*PeerType, protocol.Call) anyerror!void,
    comptime send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    comptime handle_resolved_call: *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap) anyerror!void,
) *const fn (*PeerType, []const u8, protocol.Call, u32) anyerror!void {
    return struct {
        fn call(peer: *PeerType, frame: []const u8, call_msg: protocol.Call, export_id: u32) anyerror!void {
            try handleCallImportedTargetForPeer(
                PeerType,
                InboundCapsType,
                peer,
                frame,
                call_msg,
                export_id,
                queue_promise_export_call,
                release_inbound_caps,
                report_nonfatal_error,
                note_call_send_results,
                send_return_exception,
                handle_resolved_call,
            );
        }
    }.call;
}

pub fn handleCallPromisedTargetForPeer(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    frame: []const u8,
    call: protocol.Call,
    promised: protocol.PromisedAnswer,
    resolve_promised_answer: *const fn (*PeerType, protocol.PromisedAnswer) anyerror!cap_table.ResolvedCap,
    has_unresolved_promise_export: *const fn (*PeerType, u32) bool,
    queue_promised_call: *const fn (*PeerType, u32, []const u8, InboundCapsType) anyerror!void,
    queue_promise_export_call: *const fn (*PeerType, u32, []const u8, InboundCapsType) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    handle_resolved_call: *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap) anyerror!void,
    release_inbound_caps: *const fn (*PeerType, *InboundCapsType) anyerror!void,
    report_nonfatal_error: *const fn (*PeerType, anyerror) void,
) !void {
    var inbound_caps = try InboundCapsType.init(peer.allocator, call.params.cap_table, &peer.caps);
    const target_plan = peer_call_targets.planPromisedTarget(
        PeerType,
        peer,
        promised,
        resolve_promised_answer,
        has_unresolved_promise_export,
    );

    switch (target_plan) {
        .queue_promised_call => {
            // Queue ownership transfers inbound caps and frame bytes to pending state.
            try queue_promised_call(peer, promised.question_id, frame, inbound_caps);
            return;
        },
        .queue_export_promise => |export_id| {
            // Queue ownership transfers inbound caps and frame bytes to pending state.
            try queue_promise_export_call(peer, export_id, frame, inbound_caps);
            return;
        },
        .send_exception => |err| {
            inbound_caps.deinit();
            try send_return_exception(peer, call.question_id, @errorName(err));
            return;
        },
        .handle_resolved => |resolved| {
            defer inbound_caps.deinit();
            defer release_inbound_caps(peer, &inbound_caps) catch |err| {
                report_nonfatal_error(peer, err);
            };
            try handle_resolved_call(peer, call, &inbound_caps, resolved);
        },
    }
}

pub fn handleCallPromisedTargetForPeerFn(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    comptime resolve_promised_answer: *const fn (*PeerType, protocol.PromisedAnswer) anyerror!cap_table.ResolvedCap,
    comptime has_unresolved_promise_export: *const fn (*PeerType, u32) bool,
    comptime queue_promised_call: *const fn (*PeerType, u32, []const u8, InboundCapsType) anyerror!void,
    comptime queue_promise_export_call: *const fn (*PeerType, u32, []const u8, InboundCapsType) anyerror!void,
    comptime send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    comptime handle_resolved_call: *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap) anyerror!void,
    comptime release_inbound_caps: *const fn (*PeerType, *InboundCapsType) anyerror!void,
    comptime report_nonfatal_error: *const fn (*PeerType, anyerror) void,
) *const fn (*PeerType, []const u8, protocol.Call, protocol.PromisedAnswer) anyerror!void {
    return struct {
        fn call(peer: *PeerType, frame: []const u8, call_msg: protocol.Call, promised: protocol.PromisedAnswer) anyerror!void {
            try handleCallPromisedTargetForPeer(
                PeerType,
                InboundCapsType,
                peer,
                frame,
                call_msg,
                promised,
                resolve_promised_answer,
                has_unresolved_promise_export,
                queue_promised_call,
                queue_promise_export_call,
                send_return_exception,
                handle_resolved_call,
                release_inbound_caps,
                report_nonfatal_error,
            );
        }
    }.call;
}

pub fn handleCallForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    frame: []const u8,
    call: protocol.Call,
    handle_imported_target: *const fn (*PeerType, []const u8, protocol.Call, u32) anyerror!void,
    handle_promised_target: *const fn (*PeerType, []const u8, protocol.Call, protocol.PromisedAnswer) anyerror!void,
) !void {
    const target = try routeCallTarget(call);
    switch (target) {
        .imported => |export_id| {
            try handle_imported_target(peer, frame, call, export_id);
        },
        .promised => |promised| {
            try handle_promised_target(peer, frame, call, promised);
        },
    }
}

test "peer_call_orchestration routeCallTarget enforces required target payloads" {
    const imported = protocol.Call{
        .question_id = 1,
        .target = .{
            .tag = .importedCap,
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
            .tag = .importedCap,
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
            .tag = .importedCap,
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

test "peer_call_orchestration handleResolvedExportedCallForPeerFn reports unknown promised capability export" {
    const State = struct {
        const Self = @This();
        const InboundCaps = struct {};
        const Handler = struct {
            ctx: *anyopaque,
            on_call: *const fn (*anyopaque, *Self, protocol.Call, *const InboundCaps) anyerror!void,
        };
        const ExportEntry = struct {
            handler: ?Handler = null,
            is_promise: bool = false,
            resolved: ?cap_table.ResolvedCap = null,
        };

        exports: std.AutoHashMap(u32, ExportEntry),
        exception_calls: usize = 0,
        last_question_id: u32 = 0,
        last_reason: []const u8 = "",

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .exports = std.AutoHashMap(u32, ExportEntry).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            self.exports.deinit();
        }
    };
    const InboundCaps = State.InboundCaps;

    const Hooks = struct {
        fn note(state: *State, call: protocol.Call) !void {
            _ = state;
            _ = call;
            return error.TestUnexpectedResult;
        }

        fn handleResolved(state: *State, call: protocol.Call, inbound: *const InboundCaps, resolved: cap_table.ResolvedCap) !void {
            _ = state;
            _ = call;
            _ = inbound;
            _ = resolved;
            return error.TestUnexpectedResult;
        }

        fn sendException(state: *State, question_id: u32, reason: []const u8) !void {
            state.exception_calls += 1;
            state.last_question_id = question_id;
            state.last_reason = reason;
        }
    };

    var state = State.init(std.testing.allocator);
    defer state.deinit();

    const call = protocol.Call{
        .question_id = 41,
        .target = .{
            .tag = .importedCap,
            .imported_cap = 9,
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

    const handle_exported = handleResolvedExportedCallForPeerFn(
        State,
        InboundCaps,
        Hooks.note,
        Hooks.handleResolved,
        Hooks.sendException,
    );
    try handle_exported(&state, call, &inbound, 999);

    try std.testing.expectEqual(@as(usize, 1), state.exception_calls);
    try std.testing.expectEqual(@as(u32, 41), state.last_question_id);
    try std.testing.expectEqualStrings("unknown promised capability", state.last_reason);
}

test "peer_call_orchestration handleCallForPeer routes imported and promised targets" {
    const State = struct {
        imported_calls: usize = 0,
        promised_calls: usize = 0,
    };
    const Hooks = struct {
        fn imported(state: *State, frame: []const u8, call: protocol.Call, export_id: u32) !void {
            _ = frame;
            _ = call;
            _ = export_id;
            state.imported_calls += 1;
        }

        fn promised(state: *State, frame: []const u8, call: protocol.Call, promised_answer: protocol.PromisedAnswer) !void {
            _ = frame;
            _ = call;
            _ = promised_answer;
            state.promised_calls += 1;
        }
    };

    var state = State{};

    try handleCallForPeer(
        State,
        &state,
        &.{},
        .{
            .question_id = 1,
            .target = .{
                .tag = .importedCap,
                .imported_cap = 7,
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
        },
        Hooks.imported,
        Hooks.promised,
    );

    try handleCallForPeer(
        State,
        &state,
        &.{},
        .{
            .question_id = 2,
            .target = .{
                .tag = .promisedAnswer,
                .imported_cap = null,
                .promised_answer = .{
                    .question_id = 9,
                    .transform = .{ .segment = &.{}, .byte_offset = 0 },
                },
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
        },
        Hooks.imported,
        Hooks.promised,
    );

    try std.testing.expectEqual(@as(usize, 1), state.imported_calls);
    try std.testing.expectEqual(@as(usize, 1), state.promised_calls);
}
