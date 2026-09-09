const std = @import("std");
const capnp = @import("capnpc-zig");
const g = @import("generated.zig");
const rpc = capnp.rpc;
const TextService = g.Service.Apply(.{ .T = capnp.generic.Text });
const DataService = g.Service.Apply(.{ .T = capnp.generic.Data });
const Link = struct {
    fn send(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const peer: *rpc.peer.Peer = @ptrCast(@alignCast(ctx));
        try peer.handleFrame(bytes);
    }
};
const State = struct {
    text_calls: usize = 0,
    data_calls: usize = 0,
    fn text(ctx: *anyopaque, _: *rpc.peer.Peer, params: TextService.Echo.Params.Reader, result: *TextService.Echo.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try std.testing.expectEqualStrings("typed text", try params.getValue());
        try result.setValue(try params.getValue());
        self.text_calls += 1;
    }
    fn data(ctx: *anyopaque, _: *rpc.peer.Peer, params: DataService.Echo.Params.Reader, result: *DataService.Echo.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try std.testing.expectEqualSlices(u8, &.{ 0xff, 0, 3 }, try params.getValue());
        try result.setValue(try params.getValue());
        self.data_calls += 1;
    }
    fn textBuild(_: *anyopaque, params: *TextService.Echo.Params.Builder) anyerror!void {
        try params.setValue("typed text");
    }
    fn dataBuild(_: *anyopaque, params: *DataService.Echo.Params.Builder) anyerror!void {
        try params.setValue(&.{ 0xff, 0, 3 });
    }
    fn textReturn(_: *anyopaque, _: *rpc.peer.Peer, response: TextService.Echo.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        try std.testing.expectEqualStrings("typed text", try (try response.unwrap()).getValue());
    }
    fn dataReturn(_: *anyopaque, _: *rpc.peer.Peer, response: DataService.Echo.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        try std.testing.expectEqualSlices(u8, &.{ 0xff, 0, 3 }, try (try response.unwrap()).getValue());
    }
};
test "two simultaneous concrete RPC bindings preserve typed parameters and results" {
    var client_peer = rpc.peer.Peer.initDetached(std.testing.allocator);
    defer client_peer.deinit();
    var server_peer = rpc.peer.Peer.initDetached(std.testing.allocator);
    defer server_peer.deinit();
    client_peer.setSendFrameOverride(&server_peer, Link.send);
    server_peer.setSendFrameOverride(&client_peer, Link.send);
    var state = State{};
    var text_server = TextService.ServerAdapter(.{ .echo = State.text }).init(&state);
    var data_server = DataService.ServerAdapter(.{ .echo = State.data }).init(&state);
    const text_id = try text_server.exportServer(&server_peer);
    const data_id = try data_server.exportServer(&server_peer);
    _ = try TextService.Client.init(&client_peer, text_id).callEcho(&state, State.textBuild, State.textReturn);
    _ = try DataService.Client.init(&client_peer, data_id).callEcho(&state, State.dataBuild, State.dataReturn);
    try std.testing.expectEqual(@as(usize, 1), state.text_calls);
    try std.testing.expectEqual(@as(usize, 1), state.data_calls);
    try std.testing.expectEqual(g.Service.interface_id, TextService.interface_id);
    try std.testing.expectEqual(@as(u16, 0), TextService.Echo.ordinal);
    try std.testing.expect(TextService.Echo.Params.Reader != DataService.Echo.Params.Reader);
}

const IdentityText = g.Factory.Apply(.{}).Identity.Apply(.{ .T = capnp.generic.Text });
const IdentityData = g.Factory.Apply(.{}).Identity.Apply(.{ .T = capnp.generic.Data });
const IdentityState = struct {
    calls: usize = 0,
    fn handle(ctx: *anyopaque, _: *rpc.peer.Peer, params: g.Factory.Identity.Params.Reader, results: *g.Factory.Identity.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try results.setValue(try params.getValue());
        self.calls += 1;
    }
    fn textBuild(_: *anyopaque, params: *IdentityText.Params.Builder) anyerror!void {
        try params.setValue("method text");
    }
    fn textReturn(_: *anyopaque, _: *rpc.peer.Peer, response: IdentityText.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        try std.testing.expectEqualStrings("method text", try (try response.unwrap()).getValue());
    }
    fn dataBuild(_: *anyopaque, params: *IdentityData.Params.Builder) anyerror!void {
        try params.setValue(&.{ 0xff, 0, 4 });
    }
    fn dataReturn(_: *anyopaque, _: *rpc.peer.Peer, response: IdentityData.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        try std.testing.expectEqualSlices(u8, &.{ 0xff, 0, 4 }, try (try response.unwrap()).getValue());
    }
};
test "caller method bindings have separate types with ordinary erased server dispatch" {
    var client_peer = rpc.peer.Peer.initDetached(std.testing.allocator);
    defer client_peer.deinit();
    var server_peer = rpc.peer.Peer.initDetached(std.testing.allocator);
    defer server_peer.deinit();
    client_peer.setSendFrameOverride(&server_peer, Link.send);
    server_peer.setSendFrameOverride(&client_peer, Link.send);
    var state = IdentityState{};
    var server = g.Factory.Apply(.{}).ServerAdapter(.{ .identity = IdentityState.handle }).init(&state);
    const id = try server.exportServer(&server_peer);
    const client = g.Factory.Apply(.{}).Client.init(&client_peer, id);
    _ = try client.callIdentity(.{ .T = capnp.generic.Text }, &state, IdentityState.textBuild, IdentityState.textReturn);
    _ = try client.callIdentity(.{ .T = capnp.generic.Data }, &state, IdentityState.dataBuild, IdentityState.dataReturn);
    try std.testing.expectEqual(@as(usize, 2), state.calls);
    try std.testing.expectEqual(@as(u16, 1), IdentityText.ordinal);
    try std.testing.expect(IdentityText.Params.Reader != IdentityData.Params.Reader);
}

const Factory = g.Factory.Apply(.{});
const PipelineCapture = struct {
    calls: usize = 0,
    fn send(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        var decoded = try rpc.wire.protocol.DecodedMessage.init(std.testing.allocator, bytes);
        defer decoded.deinit();
        if (decoded.tag != .call) return;
        const call = try decoded.asCall();
        try std.testing.expectEqual(g.Service.interface_id, call.interface_id);
        try std.testing.expectEqual(@as(u16, 0), call.method_id);
        const target = call.target.promised_answer.?;
        try std.testing.expectEqual(@as(u32, 91), target.question_id);
        try std.testing.expectEqual(@as(u32, 4), target.transform.len());
        for ([_]u16{ 0, 1, 1, 0 }, 0..) |expected, i| try std.testing.expectEqual(expected, (try target.transform.get(@intCast(i))).pointer_index);
        self.calls += 1;
    }
};
test "generic capability paths retain recursive applications before the parent reply" {
    var peer = rpc.peer.Peer.initDetached(std.testing.allocator);
    defer peer.deinit();
    var capture = PipelineCapture{};
    peer.setSendFrameOverride(&capture, PipelineCapture.send);
    const pipeline = Factory.GetService.Results.Pipeline{ .peer = &peer, .question_id = 91 };
    const box = try (try (try pipeline.getBox()).getNext()).getNext();
    const client = try box.getValue();
    _ = try client.callEcho(&capture, State.textBuild, State.textReturn);
    try std.testing.expectEqual(@as(usize, 1), capture.calls);
    var deep = try pipeline.getBox();
    for (0..62) |_| deep = try deep.getNext();
    _ = try deep.getValue();
    try std.testing.expectError(error.PipelineDepthLimit, (try deep.getNext()).getValue());
}

const external = @import("generic_rpc_external.zig");
const Inherited = struct {
    calls: usize = 0,
    fn text(ctx: *anyopaque, _: *rpc.peer.Peer, params: g.TextChild.Apply(.{}).Echo.Params.Reader, results: *g.TextChild.Apply(.{}).Echo.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try results.setValue(try params.getValue());
        self.calls += 1;
    }
    fn build(_: *anyopaque, params: *g.TextChild.Apply(.{}).Echo.Params.Builder) anyerror!void {
        try params.setValue("inherited");
    }
    fn onReturn(_: *anyopaque, _: *rpc.peer.Peer, result: g.TextChild.Apply(.{}).Echo.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        try std.testing.expectEqualStrings("inherited", try (try result.unwrap()).getValue());
    }
};
test "imported superclass bindings survive multiple ancestors and equivalent diamonds" {
    const Child = g.TextChild.Apply(.{});
    try std.testing.expect(Child.Echo.Params.Reader == external.Parent.Apply(.{ .T = capnp.generic.Text }).Echo.Params.Reader);
    try std.testing.expect(g.DataChild.Apply(.{}).Echo.Params.Reader == external.Parent.Apply(.{ .T = capnp.generic.Data }).Echo.Params.Reader);
    try std.testing.expect(g.Diamond.Apply(.{}).Echo.Params.Reader == Child.Echo.Params.Reader);
    var client_peer = rpc.peer.Peer.initDetached(std.testing.allocator);
    defer client_peer.deinit();
    var server_peer = rpc.peer.Peer.initDetached(std.testing.allocator);
    defer server_peer.deinit();
    client_peer.setSendFrameOverride(&server_peer, Link.send);
    server_peer.setSendFrameOverride(&client_peer, Link.send);
    var state = Inherited{};
    var server = Child.ServerAdapter(.{ .echo = Inherited.text }).init(&state);
    const id = try server.exportServer(&server_peer);
    _ = try Child.Client.init(&client_peer, id).callEcho(&state, Inherited.build, Inherited.onReturn);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
}

test "caller bindings compose pointer lists and recursive mutable data views" {
    const Type = g.Box.Apply(.{ .T = capnp.generic.List(capnp.generic.Text) });
    var message = capnp.message.MessageBuilder.init(std.testing.allocator);
    defer message.deinit();
    var root = Type.Builder.wrap(try message.allocateStruct(0, 2));
    const values = try root.initValue(2);
    try values.set(0, "list alpha");
    try values.set(1, "list beta");
    var next = try root.initNext();
    const child = try next.initValue(1);
    try child.set(0, "child");
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    const reader = try root.asReader(&storage);
    try std.testing.expectEqualStrings("list beta", try (try reader.getValue()).get(1));
    try std.testing.expectEqualStrings("child", try (try (try reader.getNext()).getValue()).get(0));
}

test "typed applications preserve pointer defaults and union selection during reopening" {
    const Type = g.Defaults.Apply(.{});
    var message = capnp.message.MessageBuilder.init(std.testing.allocator);
    defer message.deinit();
    var root = Type.Builder.wrap(try message.allocateStruct(1, 1));
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    try std.testing.expectEqualStrings("typed default", try (try (try root.asReader(&storage)).getChoice()).getValue());
    var choice = try root.getChoice();
    try std.testing.expectEqualStrings("typed default", try choice.getValue());
    try choice.setValue("reopened");
    try root.setPlain("alternate");
    try std.testing.expectError(error.WrongUnionMember, root.getChoice());
    var selected = try root.initChoice();
    try selected.setValue("selected");
    try std.testing.expectEqualStrings("selected", try (try (try root.asReader(&storage)).getChoice()).getValue());
}

test "accepted conflicting ancestor bindings require an explicit typed ancestor view" {
    const Type = g.Conflicting.Apply(.{});
    try std.testing.expect(!@hasDecl(Type.Client, "callEcho"));
    var peer = rpc.peer.Peer.initDetached(std.testing.allocator);
    defer peer.deinit();
    const client = Type.Client.init(&peer, 19);
    const text = client.asAncestor(g.ConflictText.Apply(.{}));
    const data = client.asAncestor(g.ConflictData.Apply(.{}));
    try std.testing.expectEqual(@as(u32, 19), text.raw.cap_id);
    try std.testing.expectEqual(@as(u32, 19), data.raw.cap_id);
    try std.testing.expect(@TypeOf(text) != @TypeOf(data));
    _ = Type.Raw.Client.callEcho;
}

const NamedText = g.NamedMethods.Apply(.{}).Identity.Apply(.{ .U = capnp.generic.Text });
const NamedState = struct {
    returned: bool = false,
    fn handle(_: *anyopaque, _: *rpc.peer.Peer, params: g.Box.Reader, result: *g.Box.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        try result.setValue(try params.getValue());
    }
    fn build(_: *anyopaque, params: *NamedText.Params.Builder) anyerror!void {
        try params.setValue("named parameter");
    }
    fn onReturn(ctx: *anyopaque, _: *rpc.peer.Peer, response: NamedText.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try std.testing.expectEqualStrings("named parameter", try (try response.unwrap()).getValue());
        self.returned = true;
    }
};
test "method-local applications bind named parameter and result structs" {
    var client_peer = rpc.peer.Peer.initDetached(std.testing.allocator);
    defer client_peer.deinit();
    var server_peer = rpc.peer.Peer.initDetached(std.testing.allocator);
    defer server_peer.deinit();
    client_peer.setSendFrameOverride(&server_peer, Link.send);
    server_peer.setSendFrameOverride(&client_peer, Link.send);
    var state = NamedState{};
    var server = g.NamedMethods.Apply(.{}).ServerAdapter(.{ .identity = NamedState.handle }).init(&state);
    const id = try server.exportServer(&server_peer);
    _ = try g.NamedMethods.Apply(.{}).Client.init(&client_peer, id).callIdentity(.{ .U = capnp.generic.Text }, &state, NamedState.build, NamedState.onReturn);
    try std.testing.expect(state.returned);
}

test "applied constrained pointer setters preserve destination and union on rejection" {
    var source = capnp.message.MessageBuilder.init(std.testing.allocator);
    defer source.deinit();
    var source_root = g.Box.Apply(.{ .T = capnp.generic.Text }).Builder.wrap(try source.allocateStruct(0, 2));
    try source_root.setValue("wrong pointer kind");
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    const source_reader = (try source_root.asReader(&storage)).raw();
    var destination = capnp.message.MessageBuilder.init(std.testing.allocator);
    defer destination.deinit();
    var root = g.Constraints.Apply(.{ .T = capnp.generic.Text }).Builder.wrap(try destination.allocateStruct(1, 1));
    try root.setOther("retained");
    try std.testing.expectError(error.InvalidPointer, root.setRecord(try source_reader.getValue()));
    try std.testing.expectEqualStrings("retained", try root.getOther());
}

const PlainCapture = struct {
    calls: usize = 0,
    fn send(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        var decoded = try rpc.wire.protocol.DecodedMessage.init(std.testing.allocator, bytes);
        defer decoded.deinit();
        if (decoded.tag != .call) return;
        const call = try decoded.asCall();
        try std.testing.expectEqual(g.PlainService.interface_id, call.interface_id);
        try std.testing.expectEqual(@as(u16, 0), call.method_id);
        const target = call.target.promised_answer.?;
        try std.testing.expectEqual(@as(u32, 71), target.question_id);
        try std.testing.expectEqual(@as(u32, 3), target.transform.len());
        for (0..3) |i| try std.testing.expectEqual(@as(u16, 0), (try target.transform.get(@intCast(i))).pointer_index);
        self.calls += 1;
    }
    fn build(_: *anyopaque, params: *g.PlainService.Apply(.{}).Echo.Params.Builder) anyerror!void {
        try params.setValue("through plain holder");
    }
    fn onReturn(_: *anyopaque, _: *rpc.peer.Peer, _: g.PlainService.Apply(.{}).Echo.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {}
};
test "generic pipelines compose ordinary struct and capability applications" {
    var peer = rpc.peer.Peer.initDetached(std.testing.allocator);
    defer peer.deinit();
    var capture = PlainCapture{};
    peer.setSendFrameOverride(&capture, PlainCapture.send);
    const root = g.HolderFactory.Apply(.{}).Get.Results.Pipeline{ .peer = &peer, .question_id = 71 };
    const client = try (try (try root.getBox()).getValue()).getCapability();
    _ = try client.callEcho(&capture, PlainCapture.build, PlainCapture.onReturn);
    try std.testing.expectEqual(@as(usize, 1), capture.calls);
}
