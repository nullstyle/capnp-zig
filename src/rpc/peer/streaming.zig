//! Experimental generated streaming plumbing. The ordinary call sender owns
//! wire encoding/capability effects; this adapter reserves the final encoded
//! frame before the same transport commit.
const std = @import("std");
const table = @import("../caps/table.zig");
const protocol = @import("../wire/protocol.zig");
const sender = @import("call/peer_call_sender.zig");
const state = @import("state.zig");
const StreamState = @import("../transport/stream_state.zig").StreamState;

pub fn Streaming(comptime Peer: type) type {
    return struct {
        const BuildFn = *const fn (*anyopaque, *protocol.CallBuilder) anyerror!void;
        const Callback = *const fn (*anyopaque, *Peer, protocol.Return, *const table.InboundCapTable) anyerror!void;
        const Adapter = struct {
            peer: *Peer,
            reservation: *StreamState.Reservation,

            fn allocate(self: *@This(), ctx: *anyopaque, callback: Callback) !u32 {
                return self.peer.allocateQuestionNoRestore(ctx, callback);
            }
            fn allocateLoopback(self: *@This(), ctx: *anyopaque, callback: Callback) !u32 {
                return self.peer.allocateLoopbackQuestionNoRestore(ctx, callback);
            }
            fn remove(self: *@This(), id: u32) void {
                self.peer.removeQuestion(id);
            }
            fn record(self: *@This(), id: u32, entries: []const table.OutboundEntry) !void {
                try self.peer.recordQuestionParamExports(id, entries);
            }
            fn send(self: *@This(), builder: *protocol.MessageBuilder) !void {
                const bytes = try builder.finish();
                const allocator = self.peer.allocator;
                defer allocator.free(bytes);
                try self.reservation.reserveBytes(bytes.len);
                // A synchronous Return may destroy the reservation's owner.
                // Never access it after calling the transport.
                try self.peer.sendFrame(bytes);
            }
            fn loopback(self: *@This(), bytes: []const u8) !void {
                try self.reservation.reserveBytes(bytes.len);
                try self.peer.handleLoopbackFrame(bytes);
            }
        };

        pub fn send(
            peer: *Peer,
            target_id: u32,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?BuildFn,
            callback: Callback,
            reservation: *StreamState.Reservation,
        ) !u32 {
            peer.assertThreadAffinity();
            if (peer.is_shutting_down) return error.PeerShuttingDown;
            peer.enterStreamingOperation();
            defer peer.leaveStreamingOperation();
            var adapter = Adapter{ .peer = peer, .reservation = reservation };
            const resolved: table.ResolvedCap = if (peer.resolved_imports.get(target_id)) |entry|
                if (!entry.embargoed and entry.cap != null) entry.cap.? else .{ .imported = .{ .id = target_id } }
            else
                .{ .imported = .{ .id = target_id } };
            return switch (resolved) {
                .imported => |cap| sender.sendCallToImport(
                    Adapter,
                    BuildFn,
                    Callback,
                    peer.allocator,
                    &peer.caps,
                    peer,
                    Peer.onOutboundCap,
                    Peer.rollbackOutboundCap,
                    &adapter,
                    cap.id,
                    interface_id,
                    method_id,
                    ctx,
                    build,
                    callback,
                    Adapter.allocate,
                    Adapter.remove,
                    Adapter.record,
                    Adapter.send,
                ),
                .exported => |cap| blk: {
                    try Peer.ensureCountLimit(false, peer.loopback_questions.count(), peer.limits.max_loopback_questions);
                    break :blk sender.sendCallToExport(
                        Adapter,
                        state.Question(Callback),
                        BuildFn,
                        Callback,
                        peer.allocator,
                        &peer.caps,
                        peer,
                        Peer.onOutboundCap,
                        Peer.rollbackOutboundCap,
                        &adapter,
                        &peer.questions,
                        &peer.loopback_questions,
                        cap.id,
                        interface_id,
                        method_id,
                        ctx,
                        build,
                        callback,
                        Adapter.allocateLoopback,
                        Adapter.remove,
                        Adapter.loopback,
                    );
                },
                .promised => |answer| sender.sendCallPromised(
                    Adapter,
                    BuildFn,
                    Callback,
                    peer.allocator,
                    &peer.caps,
                    peer,
                    Peer.onOutboundCap,
                    Peer.rollbackOutboundCap,
                    &adapter,
                    answer,
                    interface_id,
                    method_id,
                    ctx,
                    build,
                    callback,
                    Adapter.allocate,
                    Adapter.remove,
                    Adapter.record,
                    Adapter.send,
                ),
                .none => error.NullCapability,
            };
        }
    };
}
