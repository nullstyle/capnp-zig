const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const protocol = capnpc.rpc.protocol;
const helpers = capnpc.rpc.peer_return_send_helpers;

test "peer_return_send_helpers clearSendResultsRoutingForPeer clears maps and frees payload" {
    const State = struct {
        allocator: std.mem.Allocator,
        send_results_to_yourself: std.AutoHashMap(u32, void),
        send_results_to_third_party: std.AutoHashMap(u32, ?[]u8),

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .send_results_to_yourself = std.AutoHashMap(u32, void).init(allocator),
                .send_results_to_third_party = std.AutoHashMap(u32, ?[]u8).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            var it = self.send_results_to_third_party.valueIterator();
            while (it.next()) |value| {
                if (value.*) |payload| self.allocator.free(payload);
            }
            self.send_results_to_third_party.deinit();
            self.send_results_to_yourself.deinit();
        }
    };

    const Hooks = struct {
        fn clearThirdParty(state: *State, answer_id: u32) void {
            helpers.clearSendResultsToThirdPartyForPeer(State, state, answer_id);
        }
    };

    var state = State.init(std.testing.allocator);
    defer state.deinit();

    _ = try state.send_results_to_yourself.getOrPut(5);
    const payload = try std.testing.allocator.alloc(u8, 3);
    @memcpy(payload, "abc");
    try state.send_results_to_third_party.put(5, payload);

    helpers.clearSendResultsRoutingForPeer(State, &state, 5, Hooks.clearThirdParty);

    try std.testing.expect(!state.send_results_to_yourself.contains(5));
    try std.testing.expect(!state.send_results_to_third_party.contains(5));
}

test "peer_return_send_helpers sendReturnFrameWithLoopbackForPeer dispatches by loopback marker" {
    const State = struct {
        allocator: std.mem.Allocator,
        loopback_questions: std.AutoHashMap(u32, void),
        sent_count: usize = 0,
        delivered_count: usize = 0,

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .loopback_questions = std.AutoHashMap(u32, void).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            self.loopback_questions.deinit();
        }
    };

    const Hooks = struct {
        fn deliver(state: *State, bytes: []const u8) !void {
            _ = bytes;
            state.delivered_count += 1;
        }

        fn send(state: *State, bytes: []const u8) !void {
            _ = bytes;
            state.sent_count += 1;
        }
    };

    var state = State.init(std.testing.allocator);
    defer state.deinit();

    try helpers.sendReturnFrameWithLoopbackForPeer(
        State,
        &state,
        10,
        "frame-a",
        Hooks.deliver,
        Hooks.send,
    );
    try std.testing.expectEqual(@as(usize, 1), state.sent_count);
    try std.testing.expectEqual(@as(usize, 0), state.delivered_count);

    _ = try state.loopback_questions.getOrPut(11);
    try helpers.sendReturnFrameWithLoopbackForPeer(
        State,
        &state,
        11,
        "frame-b",
        Hooks.deliver,
        Hooks.send,
    );
    try std.testing.expectEqual(@as(usize, 1), state.sent_count);
    try std.testing.expectEqual(@as(usize, 1), state.delivered_count);
}

test "peer_return_send_helpers noteOutboundReturnCapRefsForPeer tracks sender refs" {
    const State = struct {
        allocator: std.mem.Allocator,
        noted: std.ArrayList(u32),

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .noted = std.ArrayList(u32){},
            };
        }

        fn deinit(self: *@This()) void {
            self.noted.deinit(self.allocator);
        }
    };

    const Hooks = struct {
        fn note(state: *State, id: u32) !void {
            try state.noted.append(state.allocator, id);
        }
    };

    var builder = protocol.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();

    var ret_builder = try builder.beginReturn(77, .results);
    var any = try ret_builder.getResultsAnyPointer();
    try any.setNull();
    var cap_table = try ret_builder.initCapTable(3);
    protocol.CapDescriptor.writeSenderHosted(try cap_table.get(0), 1001);
    protocol.CapDescriptor.writeSenderPromise(try cap_table.get(1), 1002);
    protocol.CapDescriptor.writeReceiverHosted(try cap_table.get(2), 2001);
    const frame = try builder.finish();
    defer std.testing.allocator.free(frame);

    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, frame);
    defer decoded.deinit();
    const ret = try decoded.asReturn();

    var state = State.init(std.testing.allocator);
    defer state.deinit();

    try helpers.noteOutboundReturnCapRefsForPeer(State, &state, ret, Hooks.note);
    try std.testing.expectEqual(@as(usize, 2), state.noted.items.len);
    try std.testing.expectEqual(@as(u32, 1001), state.noted.items[0]);
    try std.testing.expectEqual(@as(u32, 1002), state.noted.items[1]);

    const invalid = protocol.Return{
        .answer_id = 88,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .results,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    try std.testing.expectError(
        error.InvalidReturnSemantics,
        helpers.noteOutboundReturnCapRefsForPeer(State, &state, invalid, Hooks.note),
    );
}

test "peer_return_send_helpers noteSendResults helpers update and replace routing payloads" {
    const State = struct {
        allocator: std.mem.Allocator,
        send_results_to_yourself: std.AutoHashMap(u32, void),
        send_results_to_third_party: std.AutoHashMap(u32, ?[]u8),

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .send_results_to_yourself = std.AutoHashMap(u32, void).init(allocator),
                .send_results_to_third_party = std.AutoHashMap(u32, ?[]u8).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            var it = self.send_results_to_third_party.valueIterator();
            while (it.next()) |value| {
                if (value.*) |payload| self.allocator.free(payload);
            }
            self.send_results_to_third_party.deinit();
            self.send_results_to_yourself.deinit();
        }
    };

    const Hooks = struct {
        fn clearThirdParty(state: *State, answer_id: u32) void {
            helpers.clearSendResultsToThirdPartyForPeer(State, state, answer_id);
        }

        fn captureA(allocator: std.mem.Allocator, ptr: ?message.AnyPointerReader) !?[]u8 {
            _ = ptr;
            const out = try allocator.alloc(u8, 1);
            out[0] = 'a';
            return out;
        }

        fn captureB(allocator: std.mem.Allocator, ptr: ?message.AnyPointerReader) !?[]u8 {
            _ = ptr;
            const out = try allocator.alloc(u8, 1);
            out[0] = 'b';
            return out;
        }
    };

    var state = State.init(std.testing.allocator);
    defer state.deinit();

    const payload = try std.testing.allocator.alloc(u8, 3);
    @memcpy(payload, "old");
    try state.send_results_to_third_party.put(9, payload);
    try helpers.noteSendResultsToYourselfForPeer(State, &state, 9, Hooks.clearThirdParty);
    try std.testing.expect(state.send_results_to_yourself.contains(9));
    try std.testing.expect(!state.send_results_to_third_party.contains(9));

    try helpers.noteSendResultsToThirdPartyForPeer(State, &state, 9, null, Hooks.captureA);
    try std.testing.expect(!state.send_results_to_yourself.contains(9));
    const a_entry = state.send_results_to_third_party.get(9) orelse return error.MissingThirdPartyEntry;
    const a_payload = a_entry orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualSlices(u8, "a", a_payload);

    try helpers.noteSendResultsToThirdPartyForPeer(State, &state, 9, null, Hooks.captureB);
    const b_entry = state.send_results_to_third_party.get(9) orelse return error.MissingThirdPartyEntry;
    const b_payload = b_entry orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualSlices(u8, "b", b_payload);
}
