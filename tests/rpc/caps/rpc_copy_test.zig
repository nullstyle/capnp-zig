const std = @import("std");
const capnp = @import("capnpc-zig");
const message = capnp.message;
const helpers = capnp.generated_helpers;
const protocol = capnp.rpc.wire.protocol;
const caps = capnp.rpc.caps.table;

const Mapper = struct {
    calls: usize = 0,
    fail: bool = false,

    fn map(self: *Mapper, inbound: *const caps.InboundCapTable, index: u32) anyerror!?caps.payload_remap.RemappedCap {
        self.calls += 1;
        if (self.fail) return error.MappingFailed;
        return switch (try inbound.get(index)) {
            .none => null,
            .exported => |cap| .{ .origin_code = caps.descriptors.originCodeForTag(.senderHosted), .cap_id = cap.id },
            else => error.UnexpectedCapability,
        };
    }
};

fn copyCase(allocator: std.mem.Allocator, fail_mapping: bool) !void {
    const fixed = std.testing.allocator;
    var source = protocol.MessageBuilder.init(fixed);
    defer source.deinit();
    var call = try source.beginCall(1, 1, 1);
    try call.setTargetImportedCap(1);
    var payload = try call.payloadTyped();
    const pointer = try payload.initContent();
    try pointer.setCapability(.{ .id = 0 });
    const bytes = try source.finish();
    defer fixed.free(bytes);
    var decoded = try protocol.DecodedMessage.init(fixed, bytes);
    defer decoded.deinit();
    var entries = [_]caps.ResolvedCap{.{ .exported = .{ .id = 42 } }};
    var retained = [_]bool{false};
    const inbound = caps.InboundCapTable{ .allocator = fixed, .entries = &entries, .retained = &retained };
    var target = protocol.MessageBuilder.init(allocator);
    defer target.deinit();
    var target_call = try target.beginCall(2, 1, 1);
    try target_call.setTargetImportedCap(1);
    var target_payload = try target_call.payloadTyped();
    try (try target_payload.initContent()).setText("old value");
    var mapper = Mapper{ .fail = fail_mapping };
    caps.payload_remap.clonePayloadWithRemappedCaps(Mapper, allocator, &mapper, target_call.call.builder, target_payload, (try decoded.asCall()).params, &inbound, Mapper.map) catch |err| {
        var view = helpers.ReaderStorage.init(fixed);
        defer view.deinit();
        try view.bind(target_call.call.builder);
        const saved = try view.reader(target_payload._builder);
        try std.testing.expectEqualStrings("old value", try (try saved.readAnyPointer(protocol.PAYLOAD_CONTENT_PTR)).getTextStrict());
        if (fail_mapping and err == error.MappingFailed) return;
        return err;
    };
    try std.testing.expect(!fail_mapping);
    try std.testing.expectEqual(@as(usize, 1), mapper.calls);
    var table = caps.CapTable.init(fixed);
    defer table.deinit();
    try table.noteExport(42);
    try caps.encodeCallPayloadCaps(&table, &target_call, null, null, null);
    const result = try target.finish();
    defer allocator.free(result);
    var result_decoded = try protocol.DecodedMessage.init(fixed, result);
    defer result_decoded.deinit();
    const result_payload = (try result_decoded.asCall()).params;
    try std.testing.expectEqual(@as(u32, 0), (try result_payload.content.getCapability()).id);
    const descriptor = try protocol.CapDescriptor.fromReader(try result_payload.cap_table.?.get(0));
    try std.testing.expectEqual(protocol.CapDescriptorTag.senderHosted, descriptor.tag);
    try std.testing.expectEqual(@as(?u32, 42), descriptor.id);
}

test "RPC-aware copy remaps distinct capability tables and preserves destination on mapper failure" {
    try copyCase(std.testing.allocator, false);
    try copyCase(std.testing.allocator, true);
}

test "RPC-aware copy preserves destination through every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, copyCase, .{false});
}
