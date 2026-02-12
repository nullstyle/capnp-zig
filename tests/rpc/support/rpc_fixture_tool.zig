const std = @import("std");
const capnpc = @import("capnpc-zig-core");

const host_peer_mod = capnpc.rpc.host_peer;
const protocol = capnpc.rpc.protocol;
const peer_mod = capnpc.rpc.peer;
const cap_table = capnpc.rpc.cap_table;

const BootstrapStubHandler = struct {
    fn onCall(
        ctx: *anyopaque,
        called_peer: *peer_mod.Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        _ = ctx;
        _ = inbound_caps;
        try called_peer.sendReturnException(call.question_id, "bootstrap stub");
    }
};

var bootstrap_stub_ctx: u8 = 0;

pub const FramePair = struct {
    inbound: []u8,
    outbound: []u8,

    pub fn deinit(self: *FramePair, allocator: std.mem.Allocator) void {
        allocator.free(self.inbound);
        allocator.free(self.outbound);
    }
};

fn dupBytes(allocator: std.mem.Allocator, bytes: []const u8) ![]u8 {
    const copy = try allocator.alloc(u8, bytes.len);
    std.mem.copyForwards(u8, copy, bytes);
    return copy;
}

pub fn runCase(allocator: std.mem.Allocator, inbound: []const u8, with_bootstrap_stub: bool) ![]u8 {
    var host = host_peer_mod.HostPeer.init(allocator);
    defer host.deinit();
    host.start(null, null);
    try host.enableHostCallBridge();

    if (with_bootstrap_stub) {
        _ = try host.peer.setBootstrap(.{
            .ctx = &bootstrap_stub_ctx,
            .on_call = BootstrapStubHandler.onCall,
        });
    }

    try host.pushFrame(inbound);
    const out = host.popOutgoingFrame() orelse return error.MissingOutbound;
    defer host.freeFrame(out);

    return dupBytes(allocator, out);
}

pub fn makeCallToBootstrapFixture(allocator: std.mem.Allocator) !FramePair {
    var host = host_peer_mod.HostPeer.init(allocator);
    defer host.deinit();
    host.start(null, null);
    try host.enableHostCallBridge();

    const export_id = try host.peer.setBootstrap(.{
        .ctx = &bootstrap_stub_ctx,
        .on_call = BootstrapStubHandler.onCall,
    });

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(2, 0x1234, 9);
    try call.setTargetImportedCap(export_id);
    _ = try call.initCapTableTyped(0);

    const inbound = try call_builder.finish();
    defer allocator.free(inbound);

    try host.pushFrame(inbound);
    const out = host.popOutgoingFrame() orelse return error.MissingOutbound;
    defer host.freeFrame(out);

    return .{
        .inbound = try dupBytes(allocator, inbound),
        .outbound = try dupBytes(allocator, out),
    };
}
