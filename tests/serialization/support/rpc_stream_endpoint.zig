const std = @import("std");
const capnp = @import("capnpc-zig");
const g = @import("generated.zig");
const rpc = capnp.rpc;
const Peer = rpc.peer.Peer;
extern "c" fn read(c_int, [*]u8, usize) isize;
extern "c" fn write(c_int, [*]const u8, usize) isize;
extern "c" fn close(c_int) c_int;
fn require(ok: bool) !void {
    if (!ok) return error.StreamInteropMismatch;
}
const Link = struct {
    fd: c_int,
    live: bool = true,
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
        try peer.handleFrame(frame);
    }
};
const ServerState = struct {
    count: u32 = 0,
    total: u32 = 0,
    ack: ?g.TestStreaming.DoStreamI.StreamReturnSender = null,
    done: bool = false,
    fn forbidden(_: *anyopaque, _: *Peer, _: g.TestStreaming.DoStreamI.Params.Reader, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        return error.ExpectedDeferredDispatch;
    }
    fn push(ctx: *anyopaque, _: *Peer, params: g.TestStreaming.DoStreamI.Params.Reader, _: *const rpc.caps.table.InboundCapTable, ack: g.TestStreaming.DoStreamI.StreamReturnSender) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        const value = try params.getI();
        try require(value == self.count + 1);
        self.count += 1;
        self.total += value;
        self.ack = ack;
    }
    fn j(_: *anyopaque, _: *Peer, _: g.TestStreaming.DoStreamJ.Params.Reader, _: *const rpc.caps.table.InboundCapTable) anyerror!void {}
    fn finish(ctx: *anyopaque, _: *Peer, _: g.TestStreaming.FinishStream.Params.Reader, result: *g.TestStreaming.FinishStream.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try require(self.count == 2);
        try result.setTotalI(self.total);
        self.done = true;
    }
};
fn serve(peer: *Peer, link: *Link) !void {
    var state = ServerState{};
    var server = g.TestStreaming.Server{ .ctx = &state, .vtable = .{ .doStreamI = ServerState.forbidden, .doStreamI_deferred = ServerState.push, .doStreamJ = ServerState.j, .finishStream = ServerState.finish } };
    _ = try g.TestStreaming.setBootstrap(peer, &server);
    peer.streaming.limits.max_calls = 3;
    peer.streaming.limits.max_bytes = 4096;
    while (!state.done) {
        try link.receive(peer);
        try require(peer.streaming.outstanding_calls <= 3 and peer.streaming.outstanding_bytes <= 4096);
        if (peer.streaming.outstanding_calls == 3) {
            try require(state.count == 1 and !state.done);
            try state.ack.?.send();
            try require(state.count == 2 and !state.done);
            try state.ack.?.send();
        }
    }
    try require(peer.streaming.outstanding_calls == 0 and peer.streaming.outstanding_bytes == 0);
}
const ClientState = struct {
    client: ?g.TestStreaming.Client = null,
    next: u32 = 0,
    ready: bool = false,
    done: bool = false,
    fn bootstrap(ctx: *anyopaque, _: *Peer, response: g.TestStreaming.BootstrapResponse) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.client = try response.unwrap();
    }
    fn build(ctx: *anyopaque, params: *g.TestStreaming.DoStreamI.Params.Builder) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.next += 1;
        try params.setI(self.next);
    }
    fn readiness(ctx: *anyopaque, err: ?anyerror) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.ready = err == null;
    }
    fn finish(ctx: *anyopaque, _: *Peer, response: g.TestStreaming.FinishStream.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try require(try (try response.unwrap()).getTotalI() == 3);
        self.done = true;
    }
};
fn consume(peer: *Peer, link: *Link) !void {
    var state = ClientState{};
    _ = try g.TestStreaming.Client.fromBootstrap(peer, &state, ClientState.bootstrap);
    while (state.client == null) try link.receive(peer);
    defer state.client.?.release();
    var client = g.TestStreaming.StreamClient.init(state.client.?);
    client.stream.max_in_flight = 2;
    client.stream.max_in_flight_bytes = 4096;
    try client.callDoStreamI(&state, ClientState.build);
    try client.callDoStreamI(&state, ClientState.build);
    if (client.callDoStreamI(&state, ClientState.build)) |_| return error.ExpectedPressure else |err| try require(err == error.StreamInFlightLimitExceeded);
    try require(state.next == 2);
    client.whenStreamingReady(0, &state, ClientState.readiness);
    try require(!state.ready);
    _ = try client.callFinishStream(&state, null, ClientState.finish);
    while (!state.done) try link.receive(peer);
    try require(state.ready and client.stream.in_flight == 0 and client.stream.in_flight_bytes == 0);
}
pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const fd = try std.fmt.parseInt(c_int, args.next() orelse return error.MissingFd, 10);
    defer _ = close(fd);
    const mode = args.next() orelse return error.MissingMode;
    var link = Link{ .fd = fd };
    var peer = Peer.initDetached(init.gpa);
    peer.setSendFrameOverride(&link, Link.send);
    defer {
        link.live = false;
        peer.deinit();
    }
    if (std.mem.eql(u8, mode, "server")) try serve(&peer, &link) else try consume(&peer, &link);
}
