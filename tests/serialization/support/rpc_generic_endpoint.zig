const std = @import("std");
const capnp = @import("capnpc-zig");
const g = @import("generated.zig");
const rpc = capnp.rpc;
const Peer = rpc.peer.Peer;
extern "c" fn read(c_int, [*]u8, usize) isize;
extern "c" fn write(c_int, [*]const u8, usize) isize;
extern "c" fn close(c_int) c_int;
fn require(ok: bool) !void {
    if (!ok) return error.GenericInteropMismatch;
}
const Link = struct {
    fd: c_int,
    live: bool = true,
    promised_calls: usize = 0,
    fn send(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (!self.live) return;
        var offset: usize = 0;
        while (offset < bytes.len) {
            const n = write(self.fd, bytes[offset..].ptr, bytes.len - offset);
            if (n <= 0) return error.StreamWriteFailed;
            offset += @intCast(n);
        }
    }
    fn readAll(self: *@This(), out: []u8) !void {
        var offset: usize = 0;
        while (offset < out.len) {
            const n = read(self.fd, out[offset..].ptr, out.len - offset);
            if (n <= 0) return error.StreamReadFailed;
            offset += @intCast(n);
        }
    }
    fn receive(self: *@This(), peer: *Peer) !void {
        var first: [8]u8 = undefined;
        try self.readAll(&first);
        const count: usize = @as(usize, std.mem.readInt(u32, first[0..4], .little)) + 1;
        try require(count <= 512);
        const header_size = ((count + 2) & ~@as(usize, 1)) * 4;
        const header = try peer.allocator.alloc(u8, header_size);
        defer peer.allocator.free(header);
        @memcpy(header[0..8], &first);
        try self.readAll(header[8..]);
        var size = header_size;
        for (0..count) |i| size += @as(usize, std.mem.readInt(u32, header[(i + 1) * 4 ..][0..4], .little)) * 8;
        try require(size <= 2 * 1024 * 1024);
        const frame = try peer.allocator.alloc(u8, size);
        defer peer.allocator.free(frame);
        @memcpy(frame[0..header_size], header);
        try self.readAll(frame[header_size..]);
        var decoded = try rpc.wire.protocol.DecodedMessage.init(peer.allocator, frame);
        defer decoded.deinit();
        if (decoded.tag == .call and (try decoded.asCall()).target.tag == .promisedAnswer) self.promised_calls += 1;
        try peer.handleFrame(frame);
    }
};
fn echoEndpoint(comptime Type: type, peer: *Peer, link: *Link, serving: bool, expected: []const u8) !void {
    const State = struct {
        expected: []const u8,
        done: bool = false,
        client: ?Type.Client = null,
        fn handler(ctx: *anyopaque, _: *Peer, params: Type.Echo.Params.Reader, result: *Type.Echo.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try require(std.mem.eql(u8, self.expected, try params.getValue()));
            try result.setValue(try params.getValue());
            self.done = true;
        }
        fn bootstrap(ctx: *anyopaque, _: *Peer, response: Type.Raw.BootstrapResponse) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.client = .{ .raw = try response.unwrap() };
        }
        fn build(ctx: *anyopaque, params: *Type.Echo.Params.Builder) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try params.setValue(self.expected);
        }
        fn onResult(ctx: *anyopaque, _: *Peer, response: Type.Echo.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try require(std.mem.eql(u8, self.expected, try (try response.unwrap()).getValue()));
            self.done = true;
        }
    };
    var state = State{ .expected = expected };
    if (serving) {
        var server = Type.ServerAdapter(.{ .echo = State.handler }).init(&state);
        _ = try Type.Raw.setBootstrap(peer, &server.raw);
        while (!state.done) try link.receive(peer);
    } else {
        _ = try Type.Raw.Client.fromBootstrap(peer, &state, State.bootstrap);
        while (state.client == null) try link.receive(peer);
        defer state.client.?.release();
        _ = try state.client.?.callEcho(&state, State.build, State.onResult);
        while (!state.done) try link.receive(peer);
    }
}
const Factory = g.Factory.Apply(.{});
const Identity = Factory.Identity.Apply(.{ .T = capnp.generic.Data });
const MethodState = struct {
    done: bool = false,
    client: ?Factory.Client = null,
    fn handler(ctx: *anyopaque, _: *Peer, params: g.Factory.Identity.Params.Reader, result: *g.Factory.Identity.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try result.setValue(try params.getValue());
        self.done = true;
    }
    fn bootstrap(ctx: *anyopaque, _: *Peer, response: g.Factory.BootstrapResponse) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.client = .{ .raw = try response.unwrap() };
    }
    fn build(_: *anyopaque, params: *Identity.Params.Builder) anyerror!void {
        try params.setValue(&.{ 0xff, 0, 7 });
    }
    fn onResult(ctx: *anyopaque, _: *Peer, response: Identity.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try require(std.mem.eql(u8, &.{ 0xff, 0, 7 }, try (try response.unwrap()).getValue()));
        self.done = true;
    }
};
fn methodEndpoint(peer: *Peer, link: *Link, serving: bool) !void {
    var state = MethodState{};
    if (serving) {
        var server = Factory.ServerAdapter(.{ .identity = MethodState.handler }).init(&state);
        _ = try g.Factory.setBootstrap(peer, &server.raw);
        while (!state.done) try link.receive(peer);
    } else {
        _ = try g.Factory.Client.fromBootstrap(peer, &state, MethodState.bootstrap);
        while (state.client == null) try link.receive(peer);
        defer state.client.?.release();
        _ = try state.client.?.callIdentity(.{ .T = capnp.generic.Data }, &state, MethodState.build, MethodState.onResult);
        while (!state.done) try link.receive(peer);
    }
}
const TextService = g.Service.Apply(.{ .T = capnp.generic.Text });
const PipelineState = struct {
    pending: ?g.Factory.GetService.ReturnSender = null,
    leaf_id: u32 = 0,
    parent_done: bool = false,
    child_done: bool = false,
    client: ?Factory.Client = null,
    fn forbidden(_: *anyopaque, _: *Peer, _: g.Factory.GetService.Params.Reader, _: *g.Factory.GetService.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        return error.ExpectedDeferred;
    }
    fn deferParent(ctx: *anyopaque, _: *Peer, _: g.Factory.GetService.Params.Reader, _: *const rpc.caps.table.InboundCapTable, sender: g.Factory.GetService.ReturnSender) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.pending = sender;
    }
    fn parentBuild(ctx: *anyopaque, ret: *rpc.wire.protocol.ReturnBuilder) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        var payload = try ret.payloadTyped();
        var pointer = try payload.initContent();
        var results = Factory.GetService.Results.Builder.wrap(try pointer.initStruct(0, 1));
        var box = try results.initBox();
        try box.setValue(.{ .id = self.leaf_id });
    }
    fn leaf(ctx: *anyopaque, _: *Peer, params: TextService.Echo.Params.Reader, result: *TextService.Echo.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try require(std.mem.eql(u8, "pipelined generic", try params.getValue()));
        try result.setValue(try params.getValue());
        self.child_done = true;
    }
    fn bootstrap(ctx: *anyopaque, _: *Peer, response: g.Factory.BootstrapResponse) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.client = .{ .raw = try response.unwrap() };
    }
    fn parentResult(ctx: *anyopaque, _: *Peer, response: Factory.GetService.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        _ = try response.unwrap();
        self.parent_done = true;
    }
    fn leafBuild(_: *anyopaque, params: *TextService.Echo.Params.Builder) anyerror!void {
        try params.setValue("pipelined generic");
    }
    fn leafResult(ctx: *anyopaque, _: *Peer, response: TextService.Echo.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try require(std.mem.eql(u8, "pipelined generic", try (try response.unwrap()).getValue()));
        self.child_done = true;
    }
};
fn pipelineEndpoint(peer: *Peer, link: *Link, serving: bool) !void {
    var state = PipelineState{};
    if (serving) {
        var leaf = TextService.ServerAdapter(.{ .echo = PipelineState.leaf }).init(&state);
        state.leaf_id = try leaf.exportServer(peer);
        var server = g.Factory.Server{ .ctx = &state, .vtable = .{ .getService = PipelineState.forbidden, .getService_deferred = PipelineState.deferParent, .identity = MethodState.handler } };
        _ = try g.Factory.setBootstrap(peer, &server);
        while (!state.child_done) {
            try link.receive(peer);
            if (state.pending) |sender| if (link.promised_calls > 0) {
                try require(!state.child_done);
                state.pending = null;
                try sender.sendResults(&state, PipelineState.parentBuild);
            };
        }
        try require(link.promised_calls > 0);
    } else {
        _ = try g.Factory.Client.fromBootstrap(peer, &state, PipelineState.bootstrap);
        while (state.client == null) try link.receive(peer);
        defer state.client.?.release();
        const parent = try state.client.?.callGetServicePipelined(&state, null, PipelineState.parentResult);
        _ = try (try (try parent.getBox()).getValue()).callEcho(&state, PipelineState.leafBuild, PipelineState.leafResult);
        try require(!state.parent_done);
        while (!state.child_done or !state.parent_done) try link.receive(peer);
    }
}
pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const fd = try std.fmt.parseInt(c_int, args.next() orelse return error.MissingFd, 10);
    defer _ = close(fd);
    const serving = std.mem.eql(u8, args.next() orelse return error.MissingMode, "server");
    const binding = args.next() orelse return error.MissingBinding;
    var link = Link{ .fd = fd };
    var allocator = std.heap.DebugAllocator(.{}){};
    defer if (allocator.deinit() != .ok) @panic("generic RPC endpoint leaked");
    var peer = Peer.initDetached(allocator.allocator());
    peer.setSendFrameOverride(&link, Link.send);
    defer {
        link.live = false;
        peer.deinit();
    }
    if (std.mem.eql(u8, binding, "text")) try echoEndpoint(g.Service.Apply(.{ .T = capnp.generic.Text }), &peer, &link, serving, "generic interop") else if (std.mem.eql(u8, binding, "data")) try echoEndpoint(g.Service.Apply(.{ .T = capnp.generic.Data }), &peer, &link, serving, &.{ 0xff, 0, 7 }) else if (std.mem.eql(u8, binding, "inherited")) try echoEndpoint(g.TextChild.Apply(.{}), &peer, &link, serving, "generic interop") else if (std.mem.eql(u8, binding, "pipeline")) try pipelineEndpoint(&peer, &link, serving) else try methodEndpoint(&peer, &link, serving);
}
