const std = @import("std");
const abi = @import("capnp-wasm-host-abi");
const capnpc = @import("capnpc-zig");
const rpc_fixtures = @import("rpc-fixture-tool");

const protocol = capnpc.rpc.protocol;

fn toAbiPtr(ptr: anytype) abi.AbiPtr {
    return @intCast(@intFromPtr(ptr));
}

fn hasP0BExports(comptime ModuleType: type) bool {
    return @hasDecl(ModuleType, "capnp_wasm_abi_min_version") and
        @hasDecl(ModuleType, "capnp_wasm_abi_max_version") and
        @hasDecl(ModuleType, "capnp_wasm_feature_flags_lo") and
        @hasDecl(ModuleType, "capnp_wasm_feature_flags_hi") and
        @hasDecl(ModuleType, "capnp_error_take");
}

fn hasP0CExports(comptime ModuleType: type) bool {
    return @hasDecl(ModuleType, "capnp_peer_outbound_count") and
        @hasDecl(ModuleType, "capnp_peer_outbound_bytes") and
        @hasDecl(ModuleType, "capnp_peer_has_uncommitted_pop") and
        @hasDecl(ModuleType, "capnp_peer_set_limits") and
        @hasDecl(ModuleType, "capnp_peer_get_limits");
}

fn hasP0AExports(comptime ModuleType: type) bool {
    return @hasDecl(ModuleType, "capnp_peer_pop_host_call") and
        @hasDecl(ModuleType, "capnp_peer_free_host_call_frame") and
        @hasDecl(ModuleType, "capnp_peer_respond_host_call_results") and
        @hasDecl(ModuleType, "capnp_peer_respond_host_call_exception");
}

fn hasHostCallReturnFrameExport(comptime ModuleType: type) bool {
    return @hasDecl(ModuleType, "capnp_peer_respond_host_call_return_frame");
}

fn hasP1BExports(comptime ModuleType: type) bool {
    return @hasDecl(ModuleType, "capnp_peer_send_finish") and
        @hasDecl(ModuleType, "capnp_peer_send_release");
}

fn hasP1AExports(comptime ModuleType: type) bool {
    return @hasDecl(ModuleType, "capnp_schema_manifest_json");
}

fn hasBootstrapStubIdExport(comptime ModuleType: type) bool {
    return @hasDecl(ModuleType, "capnp_peer_set_bootstrap_stub_with_id");
}

fn buildUnknownCallFrame(allocator: std.mem.Allocator, question_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var call = try builder.beginCall(question_id, 0x1234, 9);
    try call.setTargetImportedCap(777);
    _ = try call.initCapTableTyped(0);

    return builder.finish();
}

fn buildBootstrapFrame(allocator: std.mem.Allocator, question_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildBootstrap(question_id);
    return builder.finish();
}

fn popOutFrameCopy(allocator: std.mem.Allocator, peer: u32) ![]u8 {
    var out_ptr: abi.AbiPtr = 0;
    var out_len: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_pop_out_frame(
        peer,
        toAbiPtr(&out_ptr),
        toAbiPtr(&out_len),
    ));
    try std.testing.expect(out_ptr != 0);
    try std.testing.expect(out_len > 0);

    const out_src: [*]const u8 = @ptrFromInt(@as(usize, @intCast(out_ptr)));
    const out_copy = try allocator.alloc(u8, out_len);
    std.mem.copyForwards(u8, out_copy, out_src[0..out_len]);
    abi.capnp_peer_pop_commit(peer);
    return out_copy;
}

fn extractBootstrapImportId(allocator: std.mem.Allocator, server: u32) !u32 {
    const bootstrap = try buildBootstrapFrame(allocator, 1);
    defer allocator.free(bootstrap);

    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_push_frame(
        server,
        toAbiPtr(bootstrap.ptr),
        @intCast(bootstrap.len),
    ));

    const bootstrap_return_frame = try popOutFrameCopy(allocator, server);
    defer allocator.free(bootstrap_return_frame);

    var decoded_bootstrap = try protocol.DecodedMessage.init(allocator, bootstrap_return_frame);
    defer decoded_bootstrap.deinit();
    const bootstrap_ret = try decoded_bootstrap.asReturn();
    const bootstrap_payload = bootstrap_ret.results orelse return error.MissingBootstrapPayload;
    const bootstrap_cap = try bootstrap_payload.content.getCapability();
    const bootstrap_cap_table = bootstrap_payload.cap_table orelse return error.MissingBootstrapCapTable;
    const bootstrap_desc_reader = try bootstrap_cap_table.get(bootstrap_cap.id);
    const bootstrap_desc = try protocol.CapDescriptor.fromReader(bootstrap_desc_reader);
    try std.testing.expectEqual(protocol.CapDescriptorTag.senderHosted, bootstrap_desc.tag);
    return bootstrap_desc.id orelse return error.MissingBootstrapImportId;
}

const PendingHostCall = struct {
    question_id: u32,
    frame_ptr: abi.AbiPtr,
    frame_len: u32,
    bootstrap_export_id: u32,
};

fn queuePendingHostCall(
    allocator: std.mem.Allocator,
    server: u32,
    question_id: u32,
    interface_id: u64,
    method_id: u16,
) !PendingHostCall {
    const bootstrap_import_id = try extractBootstrapImportId(allocator, server);

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(question_id, interface_id, method_id);
    try call.setTargetImportedCap(bootstrap_import_id);
    _ = try call.initCapTableTyped(0);

    const call_frame = try call_builder.finish();
    defer allocator.free(call_frame);

    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_push_frame(
        server,
        toAbiPtr(call_frame.ptr),
        @intCast(call_frame.len),
    ));

    var call_q: u32 = 0;
    var call_iface: u64 = 0;
    var call_method: u16 = 0;
    var call_ptr: abi.AbiPtr = 0;
    var call_len: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_pop_host_call(
        server,
        toAbiPtr(&call_q),
        toAbiPtr(&call_iface),
        toAbiPtr(&call_method),
        toAbiPtr(&call_ptr),
        toAbiPtr(&call_len),
    ));
    try std.testing.expectEqual(question_id, call_q);
    try std.testing.expectEqual(interface_id, call_iface);
    try std.testing.expectEqual(method_id, call_method);
    try std.testing.expect(call_ptr != 0);
    try std.testing.expect(call_len > 0);

    return .{
        .question_id = call_q,
        .frame_ptr = call_ptr,
        .frame_len = call_len,
        .bootstrap_export_id = bootstrap_import_id,
    };
}

fn buildReturnResultsFrameWithCapAndFlags(
    allocator: std.mem.Allocator,
    answer_id: u32,
    export_id: u32,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(answer_id, .results);
    ret.setReleaseParamCaps(false);
    ret.setNoFinishNeeded(true);

    var out_any_payload = try ret.payloadTyped();

    var out_any = try out_any_payload.initContent();

    try out_any.setCapability(.{ .id = 0 });

    var cap_table = try ret.initCapTableTyped(1);

    const desc = try cap_table.get(0);
    protocol.CapDescriptor.writeSenderHosted(desc, export_id);

    return builder.finish();
}

fn buildReturnExceptionFrame(allocator: std.mem.Allocator, answer_id: u32, reason: []const u8) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(answer_id, .exception);
    try ret.setException(reason);
    return builder.finish();
}

fn runAbiOutboundCase(
    allocator: std.mem.Allocator,
    inbound: []const u8,
    with_bootstrap_stub: bool,
) ![]u8 {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const peer = abi.capnp_peer_new();
    defer abi.capnp_peer_free(peer);
    if (peer == 0) return error.TestUnexpectedResult;

    if (with_bootstrap_stub) {
        if (abi.capnp_peer_set_bootstrap_stub(peer) != 1) return error.TestUnexpectedResult;
    }

    if (abi.capnp_peer_push_frame(peer, toAbiPtr(inbound.ptr), @intCast(inbound.len)) != 1) return error.TestUnexpectedResult;

    var out_ptr: abi.AbiPtr = 0;
    var out_len: u32 = 0;
    if (abi.capnp_peer_pop_out_frame(peer, toAbiPtr(&out_ptr), toAbiPtr(&out_len)) != 1) return error.TestUnexpectedResult;
    if (out_len == 0 or out_ptr == 0) return error.TestUnexpectedResult;

    const out_src: [*]const u8 = @ptrFromInt(@as(usize, @intCast(out_ptr)));
    const out_copy = try allocator.alloc(u8, out_len);
    std.mem.copyForwards(u8, out_copy, out_src[0..out_len]);
    abi.capnp_peer_pop_commit(peer);
    return out_copy;
}

test "wasm host ABI exposes complete P0-B export set" {
    try std.testing.expect(hasP0BExports(abi));

    const PartialMissingErrorTake = struct {
        pub fn capnp_wasm_abi_min_version() u32 {
            return 1;
        }
        pub fn capnp_wasm_abi_max_version() u32 {
            return 1;
        }
        pub fn capnp_wasm_feature_flags_lo() u32 {
            return 0;
        }
        pub fn capnp_wasm_feature_flags_hi() u32 {
            return 0;
        }
    };
    try std.testing.expect(!hasP0BExports(PartialMissingErrorTake));

    const PartialMissingFlags = struct {
        pub fn capnp_wasm_abi_min_version() u32 {
            return 1;
        }
        pub fn capnp_wasm_abi_max_version() u32 {
            return 1;
        }
        pub fn capnp_error_take(_: abi.AbiPtr, _: abi.AbiPtr, _: abi.AbiPtr) u32 {
            return 0;
        }
    };
    try std.testing.expect(!hasP0BExports(PartialMissingFlags));
}

test "wasm host ABI exposes complete P0-C export set" {
    try std.testing.expect(hasP0CExports(abi));

    const PartialMissingGetLimits = struct {
        pub fn capnp_peer_outbound_count(_: u32) u32 {
            return 0;
        }
        pub fn capnp_peer_outbound_bytes(_: u32) u32 {
            return 0;
        }
        pub fn capnp_peer_has_uncommitted_pop(_: u32) u32 {
            return 0;
        }
        pub fn capnp_peer_set_limits(_: u32, _: u32, _: u32) u32 {
            return 0;
        }
    };
    try std.testing.expect(!hasP0CExports(PartialMissingGetLimits));
}

test "wasm host ABI exposes complete P0-A export set" {
    try std.testing.expect(hasP0AExports(abi));

    const PartialMissingRespondException = struct {
        pub fn capnp_peer_pop_host_call(_: u32, _: abi.AbiPtr, _: abi.AbiPtr, _: abi.AbiPtr, _: abi.AbiPtr, _: abi.AbiPtr) u32 {
            return 0;
        }
        pub fn capnp_peer_respond_host_call_results(_: u32, _: u32, _: abi.AbiPtr, _: u32) u32 {
            return 0;
        }
    };
    try std.testing.expect(!hasP0AExports(PartialMissingRespondException));
}

test "wasm host ABI exposes host-call raw return-frame export" {
    try std.testing.expect(hasHostCallReturnFrameExport(abi));

    const Missing = struct {
        pub fn capnp_peer_respond_host_call_results(_: u32, _: u32, _: abi.AbiPtr, _: u32) u32 {
            return 0;
        }
    };
    try std.testing.expect(!hasHostCallReturnFrameExport(Missing));
}

test "wasm host ABI exposes complete P1-B export set" {
    try std.testing.expect(hasP1BExports(abi));

    const PartialMissingRelease = struct {
        pub fn capnp_peer_send_finish(_: u32, _: u32, _: u32, _: u32) u32 {
            return 0;
        }
    };
    try std.testing.expect(!hasP1BExports(PartialMissingRelease));
}

test "wasm host ABI exposes complete P1-A export set" {
    try std.testing.expect(hasP1AExports(abi));

    const Empty = struct {};
    try std.testing.expect(!hasP1AExports(Empty));
}

test "wasm host ABI exposes bootstrap-stub identity export" {
    try std.testing.expect(hasBootstrapStubIdExport(abi));

    const Missing = struct {
        pub fn capnp_peer_set_bootstrap_stub(_: u32) u32 {
            return 0;
        }
    };
    try std.testing.expect(!hasBootstrapStubIdExport(Missing));
}

test "wasm host ABI reports min/max version and feature flags" {
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_wasm_abi_version());
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_wasm_abi_min_version());
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_wasm_abi_max_version());

    const flags_lo = abi.capnp_wasm_feature_flags_lo();
    const flags_hi = abi.capnp_wasm_feature_flags_hi();
    const flags: u64 = (@as(u64, flags_hi) << 32) | flags_lo;

    try std.testing.expect((flags & (@as(u64, 1) << 0)) != 0);
    try std.testing.expect((flags & (@as(u64, 1) << 1)) != 0);
    try std.testing.expect((flags & (@as(u64, 1) << 2)) != 0);
    try std.testing.expect((flags & (@as(u64, 1) << 3)) != 0);
    try std.testing.expect((flags & (@as(u64, 1) << 4)) != 0);
    try std.testing.expect((flags & (@as(u64, 1) << 5)) != 0);
    try std.testing.expect((flags & (@as(u64, 1) << 6)) != 0);
    try std.testing.expect((flags & (@as(u64, 1) << 7)) != 0);
    try std.testing.expect((flags & (@as(u64, 1) << 8)) != 0);
}

test "host call raw return frame accepts results with cap table and flags" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const server = abi.capnp_peer_new();
    defer abi.capnp_peer_free(server);
    try std.testing.expect(server != 0);

    const pending = try queuePendingHostCall(std.testing.allocator, server, 2, 0xA100, 3);
    defer {
        std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_free_host_call_frame(
            server,
            pending.frame_ptr,
            pending.frame_len,
        )) catch unreachable;
    }

    const return_frame = try buildReturnResultsFrameWithCapAndFlags(
        std.testing.allocator,
        pending.question_id,
        pending.bootstrap_export_id,
    );
    defer std.testing.allocator.free(return_frame);

    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_respond_host_call_return_frame(
        server,
        toAbiPtr(return_frame.ptr),
        @intCast(return_frame.len),
    ));

    const outbound = try popOutFrameCopy(std.testing.allocator, server);
    defer std.testing.allocator.free(outbound);
    try std.testing.expectEqualSlices(u8, return_frame, outbound);

    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, outbound);
    defer decoded.deinit();
    const ret = try decoded.asReturn();

    try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
    try std.testing.expect(!ret.release_param_caps);
    try std.testing.expect(ret.no_finish_needed);

    const payload = ret.results orelse return error.MissingPayload;
    const cap = try payload.content.getCapability();
    try std.testing.expectEqual(@as(u32, 0), cap.id);
    const cap_table = payload.cap_table orelse return error.MissingCapTable;
    try std.testing.expectEqual(@as(u32, 1), cap_table.len());
    const desc_reader = try cap_table.get(0);
    const desc = try protocol.CapDescriptor.fromReader(desc_reader);
    try std.testing.expectEqual(protocol.CapDescriptorTag.senderHosted, desc.tag);
    try std.testing.expectEqual(pending.bootstrap_export_id, desc.id orelse return error.MissingDescriptorId);
}

test "host call raw return frame accepts exception" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const server = abi.capnp_peer_new();
    defer abi.capnp_peer_free(server);
    try std.testing.expect(server != 0);

    const pending = try queuePendingHostCall(std.testing.allocator, server, 3, 0xA200, 4);
    defer {
        std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_free_host_call_frame(
            server,
            pending.frame_ptr,
            pending.frame_len,
        )) catch unreachable;
    }

    const reason = "return exception";
    const return_frame = try buildReturnExceptionFrame(std.testing.allocator, pending.question_id, reason);
    defer std.testing.allocator.free(return_frame);

    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_respond_host_call_return_frame(
        server,
        toAbiPtr(return_frame.ptr),
        @intCast(return_frame.len),
    ));

    const outbound = try popOutFrameCopy(std.testing.allocator, server);
    defer std.testing.allocator.free(outbound);
    try std.testing.expectEqualSlices(u8, return_frame, outbound);

    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, outbound);
    defer decoded.deinit();
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings(reason, ex.reason);
}

test "host call raw return frame rejects non-Return frames" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const server = abi.capnp_peer_new();
    defer abi.capnp_peer_free(server);
    try std.testing.expect(server != 0);

    const pending = try queuePendingHostCall(std.testing.allocator, server, 4, 0xA300, 5);
    defer {
        std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_free_host_call_frame(
            server,
            pending.frame_ptr,
            pending.frame_len,
        )) catch unreachable;
    }

    const non_return = try buildUnknownCallFrame(std.testing.allocator, 999);
    defer std.testing.allocator.free(non_return);

    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_respond_host_call_return_frame(
        server,
        toAbiPtr(non_return.ptr),
        @intCast(non_return.len),
    ));
    try std.testing.expectEqual(@as(u32, 10), abi.capnp_last_error_code());
}

test "host call raw return frame rejects malformed truncated frames" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const server = abi.capnp_peer_new();
    defer abi.capnp_peer_free(server);
    try std.testing.expect(server != 0);

    const pending = try queuePendingHostCall(std.testing.allocator, server, 5, 0xA400, 6);
    defer {
        std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_free_host_call_frame(
            server,
            pending.frame_ptr,
            pending.frame_len,
        )) catch unreachable;
    }

    const valid = try buildReturnExceptionFrame(std.testing.allocator, pending.question_id, "truncated");
    defer std.testing.allocator.free(valid);
    try std.testing.expect(valid.len > 8);

    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_respond_host_call_return_frame(
        server,
        toAbiPtr(valid.ptr),
        @intCast(valid.len - 3),
    ));
    try std.testing.expectEqual(@as(u32, 10), abi.capnp_last_error_code());
}

test "host call raw return frame rejects unknown and stale answerId" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const server = abi.capnp_peer_new();
    defer abi.capnp_peer_free(server);
    try std.testing.expect(server != 0);

    const pending = try queuePendingHostCall(std.testing.allocator, server, 6, 0xA500, 7);
    defer {
        std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_free_host_call_frame(
            server,
            pending.frame_ptr,
            pending.frame_len,
        )) catch unreachable;
    }

    const unknown_frame = try buildReturnExceptionFrame(std.testing.allocator, pending.question_id + 77, "unknown");
    defer std.testing.allocator.free(unknown_frame);

    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_respond_host_call_return_frame(
        server,
        toAbiPtr(unknown_frame.ptr),
        @intCast(unknown_frame.len),
    ));
    try std.testing.expectEqual(@as(u32, 10), abi.capnp_last_error_code());

    const valid_frame = try buildReturnExceptionFrame(std.testing.allocator, pending.question_id, "stale");
    defer std.testing.allocator.free(valid_frame);

    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_respond_host_call_return_frame(
        server,
        toAbiPtr(valid_frame.ptr),
        @intCast(valid_frame.len),
    ));

    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_respond_host_call_return_frame(
        server,
        toAbiPtr(valid_frame.ptr),
        @intCast(valid_frame.len),
    ));
    try std.testing.expectEqual(@as(u32, 10), abi.capnp_last_error_code());
}

test "host call raw return frame keeps pending call after invalid frame" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const server = abi.capnp_peer_new();
    defer abi.capnp_peer_free(server);
    try std.testing.expect(server != 0);

    const pending = try queuePendingHostCall(std.testing.allocator, server, 7, 0xA600, 8);
    defer {
        std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_free_host_call_frame(
            server,
            pending.frame_ptr,
            pending.frame_len,
        )) catch unreachable;
    }

    const malformed = [_]u8{ 0x01, 0x02, 0x03, 0x04 };
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_respond_host_call_return_frame(
        server,
        toAbiPtr(&malformed),
        malformed.len,
    ));
    try std.testing.expectEqual(@as(u32, 10), abi.capnp_last_error_code());

    const valid_frame = try buildReturnExceptionFrame(std.testing.allocator, pending.question_id, "after invalid");
    defer std.testing.allocator.free(valid_frame);

    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_respond_host_call_return_frame(
        server,
        toAbiPtr(valid_frame.ptr),
        @intCast(valid_frame.len),
    ));
}

test "capnp_error_take drains and clears current error state" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_push_frame(9999, 0, 0));
    try std.testing.expectEqual(@as(u32, 3), abi.capnp_last_error_code());

    var code: u32 = 0;
    var msg_ptr: abi.AbiPtr = 0;
    var msg_len: u32 = 0;

    try std.testing.expectEqual(@as(u32, 1), abi.capnp_error_take(
        toAbiPtr(&code),
        toAbiPtr(&msg_ptr),
        toAbiPtr(&msg_len),
    ));

    try std.testing.expectEqual(@as(u32, 3), code);
    try std.testing.expect(msg_ptr != 0);
    try std.testing.expect(msg_len > 0);

    const msg_bytes: [*]const u8 = @ptrFromInt(@as(usize, @intCast(msg_ptr)));
    const msg = msg_bytes[0..msg_len];
    try std.testing.expect(std.mem.eql(u8, msg, "unknown peer handle"));

    try std.testing.expectEqual(@as(u32, 0), abi.capnp_last_error_code());
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_last_error_len());
}

test "capnp_error_take returns empty snapshot when no error is present" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    var code: u32 = 99;
    var msg_ptr: abi.AbiPtr = 1;
    var msg_len: u32 = 99;

    try std.testing.expectEqual(@as(u32, 0), abi.capnp_error_take(
        toAbiPtr(&code),
        toAbiPtr(&msg_ptr),
        toAbiPtr(&msg_len),
    ));

    try std.testing.expectEqual(@as(u32, 0), code);
    try std.testing.expectEqual(@as(abi.AbiPtr, 0), msg_ptr);
    try std.testing.expectEqual(@as(u32, 0), msg_len);
}

test "capnp_error_take validates output pointers" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    var code: u32 = 0;
    var msg_ptr: abi.AbiPtr = 0;
    var msg_len: u32 = 0;

    try std.testing.expectEqual(@as(u32, 0), abi.capnp_error_take(
        0,
        toAbiPtr(&msg_ptr),
        toAbiPtr(&msg_len),
    ));
    try std.testing.expectEqual(@as(u32, 2), abi.capnp_last_error_code());

    abi.capnp_clear_error();
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_error_take(
        toAbiPtr(&code),
        0,
        toAbiPtr(&msg_len),
    ));
    try std.testing.expectEqual(@as(u32, 2), abi.capnp_last_error_code());

    abi.capnp_clear_error();
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_error_take(
        toAbiPtr(&code),
        toAbiPtr(&msg_ptr),
        0,
    ));
    try std.testing.expectEqual(@as(u32, 2), abi.capnp_last_error_code());
}

test "peer outbound introspection and limits exports work" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const peer = abi.capnp_peer_new();
    defer abi.capnp_peer_free(peer);
    try std.testing.expect(peer != 0);

    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_outbound_count(peer));
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_outbound_bytes(peer));
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_has_uncommitted_pop(peer));

    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_set_limits(peer, 3, 4096));
    var limit_count: u32 = 0;
    var limit_bytes: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_get_limits(
        peer,
        toAbiPtr(&limit_count),
        toAbiPtr(&limit_bytes),
    ));
    try std.testing.expectEqual(@as(u32, 3), limit_count);
    try std.testing.expectEqual(@as(u32, 4096), limit_bytes);

    const inbound = try buildUnknownCallFrame(std.testing.allocator, 9);
    defer std.testing.allocator.free(inbound);
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_push_frame(peer, toAbiPtr(inbound.ptr), @intCast(inbound.len)));

    const queued = abi.capnp_peer_outbound_count(peer);
    try std.testing.expect(queued > 0);
    try std.testing.expect(abi.capnp_peer_outbound_bytes(peer) > 0);
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_has_uncommitted_pop(peer));

    var out_ptr: abi.AbiPtr = 0;
    var out_len: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_pop_out_frame(peer, toAbiPtr(&out_ptr), toAbiPtr(&out_len)));
    try std.testing.expect(out_ptr != 0);
    try std.testing.expect(out_len > 0);
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_has_uncommitted_pop(peer));

    abi.capnp_peer_pop_commit(peer);
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_has_uncommitted_pop(peer));
}

test "host call bridge exports can pop inbound calls and send exception responses" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const server = abi.capnp_peer_new();
    defer abi.capnp_peer_free(server);
    try std.testing.expect(server != 0);

    const bootstrap = try buildBootstrapFrame(std.testing.allocator, 1);
    defer std.testing.allocator.free(bootstrap);
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_push_frame(server, toAbiPtr(bootstrap.ptr), @intCast(bootstrap.len)));

    var bootstrap_ptr: abi.AbiPtr = 0;
    var bootstrap_len: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_pop_out_frame(server, toAbiPtr(&bootstrap_ptr), toAbiPtr(&bootstrap_len)));
    const bootstrap_return: [*]const u8 = @ptrFromInt(@as(usize, @intCast(bootstrap_ptr)));
    const bootstrap_return_frame = try std.testing.allocator.alloc(u8, bootstrap_len);
    defer std.testing.allocator.free(bootstrap_return_frame);
    std.mem.copyForwards(u8, bootstrap_return_frame, bootstrap_return[0..bootstrap_len]);
    abi.capnp_peer_pop_commit(server);

    var decoded_bootstrap = try protocol.DecodedMessage.init(std.testing.allocator, bootstrap_return_frame);
    defer decoded_bootstrap.deinit();
    const bootstrap_ret = try decoded_bootstrap.asReturn();
    const bootstrap_payload = bootstrap_ret.results orelse return error.MissingBootstrapPayload;
    const bootstrap_cap = try bootstrap_payload.content.getCapability();
    const bootstrap_cap_table = bootstrap_payload.cap_table orelse return error.MissingBootstrapCapTable;
    const bootstrap_desc_reader = try bootstrap_cap_table.get(bootstrap_cap.id);
    const bootstrap_desc = try protocol.CapDescriptor.fromReader(bootstrap_desc_reader);
    try std.testing.expectEqual(protocol.CapDescriptorTag.senderHosted, bootstrap_desc.tag);
    const bootstrap_import_id = bootstrap_desc.id orelse return error.MissingBootstrapImportId;

    var call_builder = protocol.MessageBuilder.init(std.testing.allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(2, 0x9999, 5);
    try call.setTargetImportedCap(bootstrap_import_id);
    _ = try call.initCapTableTyped(0);

    const call_frame = try call_builder.finish();
    defer std.testing.allocator.free(call_frame);
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_push_frame(server, toAbiPtr(call_frame.ptr), @intCast(call_frame.len)));

    var call_q: u32 = 0;
    var call_iface: u64 = 0;
    var call_method: u16 = 0;
    var call_ptr: abi.AbiPtr = 0;
    var call_len: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_pop_host_call(
        server,
        toAbiPtr(&call_q),
        toAbiPtr(&call_iface),
        toAbiPtr(&call_method),
        toAbiPtr(&call_ptr),
        toAbiPtr(&call_len),
    ));
    try std.testing.expectEqual(@as(u32, 2), call_q);
    try std.testing.expectEqual(@as(u64, 0x9999), call_iface);
    try std.testing.expectEqual(@as(u16, 5), call_method);
    try std.testing.expect(call_ptr != 0);
    try std.testing.expect(call_len > 0);

    const reason = "bridge exception";
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_respond_host_call_exception(
        server,
        call_q,
        toAbiPtr(reason.ptr),
        @intCast(reason.len),
    ));
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_free_host_call_frame(
        server,
        call_ptr,
        call_len,
    ));

    var ret_ptr: abi.AbiPtr = 0;
    var ret_len: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_pop_out_frame(server, toAbiPtr(&ret_ptr), toAbiPtr(&ret_len)));
    const ret_bytes: [*]const u8 = @ptrFromInt(@as(usize, @intCast(ret_ptr)));
    const ret_copy = try std.testing.allocator.alloc(u8, ret_len);
    defer std.testing.allocator.free(ret_copy);
    std.mem.copyForwards(u8, ret_copy, ret_bytes[0..ret_len]);
    abi.capnp_peer_pop_commit(server);

    var decoded_ret = try protocol.DecodedMessage.init(std.testing.allocator, ret_copy);
    defer decoded_ret.deinit();
    const ret = try decoded_ret.asReturn();
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings(reason, ex.reason);
}

test "host call frame release export supports repeated pop/free cycles" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const server = abi.capnp_peer_new();
    defer abi.capnp_peer_free(server);
    try std.testing.expect(server != 0);

    const bootstrap = try buildBootstrapFrame(std.testing.allocator, 1);
    defer std.testing.allocator.free(bootstrap);
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_push_frame(server, toAbiPtr(bootstrap.ptr), @intCast(bootstrap.len)));

    var bootstrap_ptr: abi.AbiPtr = 0;
    var bootstrap_len: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_pop_out_frame(server, toAbiPtr(&bootstrap_ptr), toAbiPtr(&bootstrap_len)));
    const bootstrap_return: [*]const u8 = @ptrFromInt(@as(usize, @intCast(bootstrap_ptr)));
    const bootstrap_return_frame = try std.testing.allocator.alloc(u8, bootstrap_len);
    defer std.testing.allocator.free(bootstrap_return_frame);
    std.mem.copyForwards(u8, bootstrap_return_frame, bootstrap_return[0..bootstrap_len]);
    abi.capnp_peer_pop_commit(server);

    var decoded_bootstrap = try protocol.DecodedMessage.init(std.testing.allocator, bootstrap_return_frame);
    defer decoded_bootstrap.deinit();
    const bootstrap_ret = try decoded_bootstrap.asReturn();
    const bootstrap_payload = bootstrap_ret.results orelse return error.MissingBootstrapPayload;
    const bootstrap_cap = try bootstrap_payload.content.getCapability();
    const bootstrap_cap_table = bootstrap_payload.cap_table orelse return error.MissingBootstrapCapTable;
    const bootstrap_desc_reader = try bootstrap_cap_table.get(bootstrap_cap.id);
    const bootstrap_desc = try protocol.CapDescriptor.fromReader(bootstrap_desc_reader);
    try std.testing.expectEqual(protocol.CapDescriptorTag.senderHosted, bootstrap_desc.tag);
    const bootstrap_import_id = bootstrap_desc.id orelse return error.MissingBootstrapImportId;

    const reason = "cycle exception";
    const cycle_count: usize = 16;
    var idx: usize = 0;
    while (idx < cycle_count) : (idx += 1) {
        var call_builder = protocol.MessageBuilder.init(std.testing.allocator);
        defer call_builder.deinit();
        var call = try call_builder.beginCall(@intCast(100 + idx), 0xAA00 + idx, @intCast(10 + idx));
        try call.setTargetImportedCap(bootstrap_import_id);
        _ = try call.initCapTableTyped(0);

        const call_frame = try call_builder.finish();
        defer std.testing.allocator.free(call_frame);
        try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_push_frame(server, toAbiPtr(call_frame.ptr), @intCast(call_frame.len)));

        var call_q: u32 = 0;
        var call_iface: u64 = 0;
        var call_method: u16 = 0;
        var call_ptr: abi.AbiPtr = 0;
        var call_len: u32 = 0;
        try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_pop_host_call(
            server,
            toAbiPtr(&call_q),
            toAbiPtr(&call_iface),
            toAbiPtr(&call_method),
            toAbiPtr(&call_ptr),
            toAbiPtr(&call_len),
        ));
        try std.testing.expectEqual(@as(u32, @intCast(100 + idx)), call_q);
        try std.testing.expectEqual(@as(u64, 0xAA00 + idx), call_iface);
        try std.testing.expectEqual(@as(u16, @intCast(10 + idx)), call_method);
        try std.testing.expect(call_ptr != 0);
        try std.testing.expect(call_len > 0);

        try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_respond_host_call_exception(
            server,
            call_q,
            toAbiPtr(reason.ptr),
            @intCast(reason.len),
        ));
        try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_free_host_call_frame(
            server,
            call_ptr,
            call_len,
        ));

        var ret_ptr: abi.AbiPtr = 0;
        var ret_len: u32 = 0;
        try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_pop_out_frame(
            server,
            toAbiPtr(&ret_ptr),
            toAbiPtr(&ret_len),
        ));
        const ret_bytes: [*]const u8 = @ptrFromInt(@as(usize, @intCast(ret_ptr)));
        const ret_copy = try std.testing.allocator.alloc(u8, ret_len);
        defer std.testing.allocator.free(ret_copy);
        std.mem.copyForwards(u8, ret_copy, ret_bytes[0..ret_len]);
        abi.capnp_peer_pop_commit(server);

        var decoded_ret = try protocol.DecodedMessage.init(std.testing.allocator, ret_copy);
        defer decoded_ret.deinit();
        const ret = try decoded_ret.asReturn();
        try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
        const ex = ret.exception orelse return error.MissingException;
        try std.testing.expectEqualStrings(reason, ex.reason);
    }
}

test "rpc fixture generation matches wasm-host bridge behavior for bootstrap flows" {
    const bootstrap_inbound = try buildBootstrapFrame(std.testing.allocator, 1);
    defer std.testing.allocator.free(bootstrap_inbound);

    const fixture_bootstrap = try rpc_fixtures.runCase(std.testing.allocator, bootstrap_inbound, false);
    defer std.testing.allocator.free(fixture_bootstrap);
    const wasm_bootstrap = try runAbiOutboundCase(std.testing.allocator, bootstrap_inbound, false);
    defer std.testing.allocator.free(wasm_bootstrap);
    try std.testing.expectEqualSlices(u8, fixture_bootstrap, wasm_bootstrap);

    const fixture_bootstrap_stub = try rpc_fixtures.runCase(std.testing.allocator, bootstrap_inbound, true);
    defer std.testing.allocator.free(fixture_bootstrap_stub);
    const wasm_bootstrap_stub = try runAbiOutboundCase(std.testing.allocator, bootstrap_inbound, true);
    defer std.testing.allocator.free(wasm_bootstrap_stub);
    try std.testing.expectEqualSlices(u8, fixture_bootstrap_stub, wasm_bootstrap_stub);

    const call_bootstrap_fixture = try rpc_fixtures.makeCallToBootstrapFixture(std.testing.allocator);
    defer {
        std.testing.allocator.free(call_bootstrap_fixture.inbound);
        std.testing.allocator.free(call_bootstrap_fixture.outbound);
    }

    const wasm_call_bootstrap = try runAbiOutboundCase(
        std.testing.allocator,
        call_bootstrap_fixture.inbound,
        true,
    );
    defer std.testing.allocator.free(wasm_call_bootstrap);
    try std.testing.expectEqualSlices(u8, call_bootstrap_fixture.outbound, wasm_call_bootstrap);
}

test "bootstrap stub identity export returns stable installed export id" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const peer = abi.capnp_peer_new();
    defer abi.capnp_peer_free(peer);
    try std.testing.expect(peer != 0);

    var export_id_first: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_set_bootstrap_stub_with_id(
        peer,
        toAbiPtr(&export_id_first),
    ));
    try std.testing.expect(export_id_first != 0);

    var export_id_second: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_set_bootstrap_stub_with_id(
        peer,
        toAbiPtr(&export_id_second),
    ));
    try std.testing.expectEqual(export_id_first, export_id_second);

    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_set_bootstrap_stub(peer));

    var export_id_third: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_set_bootstrap_stub_with_id(
        peer,
        toAbiPtr(&export_id_third),
    ));
    try std.testing.expectEqual(export_id_first, export_id_third);
}

test "bootstrap stub identity export validates arguments" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const peer = abi.capnp_peer_new();
    defer abi.capnp_peer_free(peer);
    try std.testing.expect(peer != 0);

    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_set_bootstrap_stub_with_id(peer, 0));
    try std.testing.expectEqual(@as(u32, 2), abi.capnp_last_error_code());

    abi.capnp_clear_error();
    var export_id: u32 = 0;
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_set_bootstrap_stub_with_id(9999, toAbiPtr(&export_id)));
    try std.testing.expectEqual(@as(u32, 3), abi.capnp_last_error_code());
}

test "lifecycle helper exports send release and finish frames" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const peer = abi.capnp_peer_new();
    defer abi.capnp_peer_free(peer);
    try std.testing.expect(peer != 0);

    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_send_release(peer, 77, 3));
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_send_finish(peer, 91, 1, 1));
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_last_error_code());

    var release_ptr: abi.AbiPtr = 0;
    var release_len: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_pop_out_frame(
        peer,
        toAbiPtr(&release_ptr),
        toAbiPtr(&release_len),
    ));
    const release_src: [*]const u8 = @ptrFromInt(@as(usize, @intCast(release_ptr)));
    const release_copy = try std.testing.allocator.alloc(u8, release_len);
    defer std.testing.allocator.free(release_copy);
    std.mem.copyForwards(u8, release_copy, release_src[0..release_len]);
    abi.capnp_peer_pop_commit(peer);

    var decoded_release = try protocol.DecodedMessage.init(std.testing.allocator, release_copy);
    defer decoded_release.deinit();
    const release = try decoded_release.asRelease();
    try std.testing.expectEqual(@as(u32, 77), release.id);
    try std.testing.expectEqual(@as(u32, 3), release.reference_count);

    var finish_ptr: abi.AbiPtr = 0;
    var finish_len: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_pop_out_frame(
        peer,
        toAbiPtr(&finish_ptr),
        toAbiPtr(&finish_len),
    ));
    const finish_src: [*]const u8 = @ptrFromInt(@as(usize, @intCast(finish_ptr)));
    const finish_copy = try std.testing.allocator.alloc(u8, finish_len);
    defer std.testing.allocator.free(finish_copy);
    std.mem.copyForwards(u8, finish_copy, finish_src[0..finish_len]);
    abi.capnp_peer_pop_commit(peer);

    var decoded_finish = try protocol.DecodedMessage.init(std.testing.allocator, finish_copy);
    defer decoded_finish.deinit();
    const finish = try decoded_finish.asFinish();
    try std.testing.expectEqual(@as(u32, 91), finish.question_id);
    try std.testing.expect(finish.release_result_caps);
    try std.testing.expect(finish.require_early_cancellation);
}

test "lifecycle helper exports validate finish bool flags" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const peer = abi.capnp_peer_new();
    defer abi.capnp_peer_free(peer);
    try std.testing.expect(peer != 0);

    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_send_finish(peer, 1, 2, 0));
    try std.testing.expectEqual(@as(u32, 2), abi.capnp_last_error_code());

    abi.capnp_clear_error();
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_send_finish(peer, 1, 0, 2));
    try std.testing.expectEqual(@as(u32, 2), abi.capnp_last_error_code());
}

test "schema manifest export returns deterministic valid json" {
    const ManifestSerdeEntry = struct {
        id: u64,
        type_name: []const u8,
        to_json_export: []const u8,
        from_json_export: []const u8,
    };
    const Manifest = struct {
        schema: []const u8,
        module: []const u8,
        serde: []const ManifestSerdeEntry,
    };

    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    var out_ptr_a: abi.AbiPtr = 0;
    var out_len_a: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_schema_manifest_json(
        toAbiPtr(&out_ptr_a),
        toAbiPtr(&out_len_a),
    ));
    try std.testing.expect(out_ptr_a != 0);
    try std.testing.expect(out_len_a > 0);

    var out_ptr_b: abi.AbiPtr = 0;
    var out_len_b: u32 = 0;
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_schema_manifest_json(
        toAbiPtr(&out_ptr_b),
        toAbiPtr(&out_len_b),
    ));
    try std.testing.expect(out_ptr_b != 0);
    try std.testing.expect(out_len_b > 0);

    const bytes_a_ptr: [*]const u8 = @ptrFromInt(@as(usize, @intCast(out_ptr_a)));
    const bytes_b_ptr: [*]const u8 = @ptrFromInt(@as(usize, @intCast(out_ptr_b)));
    const bytes_a = bytes_a_ptr[0..out_len_a];
    const bytes_b = bytes_b_ptr[0..out_len_b];

    try std.testing.expectEqualStrings(bytes_a, bytes_b);

    var parsed = try std.json.parseFromSlice(Manifest, std.testing.allocator, bytes_a, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("tests/test_schemas/example.capnp", parsed.value.schema);
    try std.testing.expectEqualStrings("example", parsed.value.module);
    try std.testing.expect(parsed.value.serde.len > 0);
    try std.testing.expectEqualStrings("Person", parsed.value.serde[0].type_name);
    try std.testing.expectEqualStrings("capnp_example_person_to_json", parsed.value.serde[0].to_json_export);
    try std.testing.expectEqualStrings("capnp_example_person_from_json", parsed.value.serde[0].from_json_export);

    abi.capnp_buf_free(out_ptr_a, out_len_a);
    abi.capnp_buf_free(out_ptr_b, out_len_b);
}

test "schema manifest export validates output pointers" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    var out_len: u32 = 0;
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_schema_manifest_json(0, toAbiPtr(&out_len)));
    try std.testing.expectEqual(@as(u32, 2), abi.capnp_last_error_code());

    abi.capnp_clear_error();
    var out_ptr: abi.AbiPtr = 0;
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_schema_manifest_json(toAbiPtr(&out_ptr), 0));
    try std.testing.expectEqual(@as(u32, 2), abi.capnp_last_error_code());
}

test "wasm host ABI supports peer reinit after free" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const first = abi.capnp_peer_new();
    try std.testing.expect(first != 0);
    abi.capnp_peer_free(first);
    // Double-free should be a harmless no-op.
    abi.capnp_peer_free(first);

    const second = abi.capnp_peer_new();
    defer abi.capnp_peer_free(second);
    try std.testing.expect(second != 0);
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_last_error_code());
}

test "capnp_peer_free_host_call_frame rejects double free" {
    abi.capnp_clear_error();
    defer abi.capnp_clear_error();

    const server = abi.capnp_peer_new();
    defer abi.capnp_peer_free(server);
    try std.testing.expect(server != 0);

    const pending = try queuePendingHostCall(std.testing.allocator, server, 700, 0xCAFE, 1);
    try std.testing.expectEqual(@as(u32, 1), abi.capnp_peer_free_host_call_frame(
        server,
        pending.frame_ptr,
        pending.frame_len,
    ));

    abi.capnp_clear_error();
    try std.testing.expectEqual(@as(u32, 0), abi.capnp_peer_free_host_call_frame(
        server,
        pending.frame_ptr,
        pending.frame_len,
    ));
    try std.testing.expectEqual(@as(u32, 2), abi.capnp_last_error_code());
}
