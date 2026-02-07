const std = @import("std");
const cap_table = @import("cap_table.zig");
const protocol = @import("protocol.zig");

pub const ImportedTargetPlan = union(enum) {
    unknown_capability,
    queue_promise_export,
    promise_broken,
    handle_resolved: cap_table.ResolvedCap,
    call_handler,
    missing_export_handler,
};

pub fn planImportedTarget(
    has_export: bool,
    is_promise: bool,
    resolved: ?cap_table.ResolvedCap,
    has_handler: bool,
) ImportedTargetPlan {
    if (!has_export) return .unknown_capability;
    if (is_promise) {
        const resolved_cap = resolved orelse return .queue_promise_export;
        if (resolved_cap == .none) return .promise_broken;
        return .{ .handle_resolved = resolved_cap };
    }
    return if (has_handler) .call_handler else .missing_export_handler;
}

pub const PromisedTargetPlan = union(enum) {
    queue_promised_call,
    queue_export_promise: u32,
    handle_resolved: cap_table.ResolvedCap,
    send_exception: anyerror,
};

pub fn planPromisedTarget(
    comptime PeerType: type,
    peer: *PeerType,
    promised: protocol.PromisedAnswer,
    resolve_promised_answer: *const fn (*PeerType, protocol.PromisedAnswer) anyerror!cap_table.ResolvedCap,
    has_unresolved_promise_export: *const fn (*PeerType, u32) bool,
) PromisedTargetPlan {
    const resolved = resolve_promised_answer(peer, promised) catch |err| {
        if (err == error.PromiseUnresolved) return .queue_promised_call;
        return .{ .send_exception = err };
    };

    if (resolved == .exported) {
        const export_id = resolved.exported.id;
        if (has_unresolved_promise_export(peer, export_id)) {
            return .{ .queue_export_promise = export_id };
        }
    }

    return .{ .handle_resolved = resolved };
}

test "peer_call_targets imported target planning covers all branches" {
    try std.testing.expectEqual(
        ImportedTargetPlan.unknown_capability,
        planImportedTarget(false, false, null, false),
    );
    try std.testing.expectEqual(
        ImportedTargetPlan.queue_promise_export,
        planImportedTarget(true, true, null, false),
    );
    try std.testing.expectEqual(
        ImportedTargetPlan.promise_broken,
        planImportedTarget(true, true, .none, false),
    );

    const resolved = planImportedTarget(
        true,
        true,
        .{ .imported = .{ .id = 42 } },
        false,
    );
    switch (resolved) {
        .handle_resolved => |cap| switch (cap) {
            .imported => |imported| try std.testing.expectEqual(@as(u32, 42), imported.id),
            else => return error.TestExpectedEqual,
        },
        else => return error.TestExpectedEqual,
    }

    try std.testing.expectEqual(
        ImportedTargetPlan.call_handler,
        planImportedTarget(true, false, null, true),
    );
    try std.testing.expectEqual(
        ImportedTargetPlan.missing_export_handler,
        planImportedTarget(true, false, null, false),
    );
}

test "peer_call_targets promised target planning handles unresolved, exception, queue-export and resolved" {
    const FakePeer = struct {
        mode: enum {
            unresolved,
            failure,
            exported_unresolved,
            exported_resolved,
            imported_resolved,
        },
    };

    const Hooks = struct {
        fn resolvePromisedAnswer(peer: *FakePeer, promised: protocol.PromisedAnswer) !cap_table.ResolvedCap {
            _ = promised;
            return switch (peer.mode) {
                .unresolved => error.PromiseUnresolved,
                .failure => error.TestExpectedError,
                .exported_unresolved, .exported_resolved => .{ .exported = .{ .id = 9 } },
                .imported_resolved => .{ .imported = .{ .id = 11 } },
            };
        }

        fn hasUnresolvedPromiseExport(peer: *FakePeer, export_id: u32) bool {
            return peer.mode == .exported_unresolved and export_id == 9;
        }
    };

    const promised = protocol.PromisedAnswer{
        .question_id = 1,
        .transform = .{ .segment = &.{}, .byte_offset = 0 },
    };

    {
        var peer = FakePeer{ .mode = .unresolved };
        const plan = planPromisedTarget(
            FakePeer,
            &peer,
            promised,
            Hooks.resolvePromisedAnswer,
            Hooks.hasUnresolvedPromiseExport,
        );
        try std.testing.expectEqual(PromisedTargetPlan.queue_promised_call, plan);
    }

    {
        var peer = FakePeer{ .mode = .failure };
        const plan = planPromisedTarget(
            FakePeer,
            &peer,
            promised,
            Hooks.resolvePromisedAnswer,
            Hooks.hasUnresolvedPromiseExport,
        );
        switch (plan) {
            .send_exception => |err| try std.testing.expectEqual(error.TestExpectedError, err),
            else => return error.TestExpectedEqual,
        }
    }

    {
        var peer = FakePeer{ .mode = .exported_unresolved };
        const plan = planPromisedTarget(
            FakePeer,
            &peer,
            promised,
            Hooks.resolvePromisedAnswer,
            Hooks.hasUnresolvedPromiseExport,
        );
        switch (plan) {
            .queue_export_promise => |export_id| try std.testing.expectEqual(@as(u32, 9), export_id),
            else => return error.TestExpectedEqual,
        }
    }

    {
        var peer = FakePeer{ .mode = .exported_resolved };
        const plan = planPromisedTarget(
            FakePeer,
            &peer,
            promised,
            Hooks.resolvePromisedAnswer,
            Hooks.hasUnresolvedPromiseExport,
        );
        switch (plan) {
            .handle_resolved => |cap| switch (cap) {
                .exported => |exported| try std.testing.expectEqual(@as(u32, 9), exported.id),
                else => return error.TestExpectedEqual,
            },
            else => return error.TestExpectedEqual,
        }
    }

    {
        var peer = FakePeer{ .mode = .imported_resolved };
        const plan = planPromisedTarget(
            FakePeer,
            &peer,
            promised,
            Hooks.resolvePromisedAnswer,
            Hooks.hasUnresolvedPromiseExport,
        );
        switch (plan) {
            .handle_resolved => |cap| switch (cap) {
                .imported => |imported| try std.testing.expectEqual(@as(u32, 11), imported.id),
                else => return error.TestExpectedEqual,
            },
            else => return error.TestExpectedEqual,
        }
    }
}
