const std = @import("std");
const capnpc = @import("capnpc-zig");

const tcp = capnpc.rpc.transport.tcp;
const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const peer_impl = capnpc.rpc.peer;
const Peer = peer_impl.Peer;
const ClientSession = tcp.ClientSession;

// Lifecycle coverage for ClientSession: the one blessed connect/run/close/
// deinit ordering that replaces the hand-rolled (and twice-divergent)
// consumer recipes. Servers here are minimal on purpose — a listen socket
// whose backlog completes the TCP handshake is enough for connect(), and an
// accept-then-drop thread is enough to drive the EOF/terminal path.

const Counters = struct {
    errors: usize = 0,
    closes: usize = 0,

    fn onError(ctx: ?*anyopaque, _: *ClientSession, _: anyerror) void {
        const self: *Counters = @ptrCast(@alignCast(ctx.?));
        self.errors += 1;
    }
    fn onClose(ctx: ?*anyopaque, _: *ClientSession) void {
        const self: *Counters = @ptrCast(@alignCast(ctx.?));
        self.closes += 1;
    }
};

fn listenLoopback(io: std.Io, backlog: u31) !struct { fd: tcp.SocketFd, address: std.Io.net.IpAddress } {
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    const server = try tcp.createListenSocket(io, address, backlog, false);
    return .{ .fd = .{ .handle = server.socket.handle }, .address = server.socket.address };
}

test "close before run: run returns, on_close fires exactly once, fromPeer recovers" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const server = try listenLoopback(io, 64);
    defer tcp.closeFd(io, server.fd);

    var counters = Counters{};
    const session = try tcp.connect(allocator, io, server.address, .{
        .ctx = &counters,
        .on_error = Counters.onError,
        .on_close = Counters.onClose,
    });

    try std.testing.expectEqual(session, ClientSession.fromPeer(&session.peer));

    session.close();
    session.close(); // idempotent
    session.run();

    try std.testing.expectEqual(@as(usize, 1), counters.closes);
    session.deinit();
}

test "requestStop before run: run returns without traffic, deinit leak-free" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const server = try listenLoopback(io, 64);
    defer tcp.closeFd(io, server.fd);

    var counters = Counters{};
    const session = try tcp.connect(allocator, io, server.address, .{
        .ctx = &counters,
        .on_error = Counters.onError,
        .on_close = Counters.onClose,
    });

    session.requestStop();
    session.run();

    try std.testing.expectEqual(@as(usize, 1), counters.closes);
    session.deinit();
}

const AcceptDrop = struct {
    listener: *tcp.Listener,

    fn main(self: AcceptDrop) void {
        const conn = self.listener.accept() catch return;
        const allocator = conn.allocator;
        // Drop the server end immediately: the client observes EOF.
        conn.close();
        conn.deinit();
        allocator.destroy(conn);
    }
};

const DisconnectWaiter = struct {
    session: *ClientSession = undefined,
    fired: usize = 0,
    disconnects: usize = 0,

    fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *DisconnectWaiter = @ptrCast(@alignCast(ctx));
        self.fired += 1;
        if (ret.tag == .exception) {
            if (ret.exception) |ex| {
                if (std.mem.eql(u8, ex.reason, peer_impl.disconnected_reason)) self.disconnects += 1;
            }
        }
        // Closing from a question callback (inside run()) must be legal and
        // idempotent with the close already in progress.
        self.session.close();
    }
};

test "server EOF fails the in-flight question with Disconnected; close-from-callback is safe" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    const server = try tcp.createListenSocket(io, address, 1, false);
    var listener = tcp.Listener.initFd(allocator, io, .{ .handle = server.socket.handle }, .{});
    defer listener.close();

    const accept_thread = try std.Thread.spawn(.{}, AcceptDrop.main, .{AcceptDrop{ .listener = &listener }});

    var counters = Counters{};
    const session = try tcp.connect(allocator, io, server.socket.address, .{
        .ctx = &counters,
        .on_error = Counters.onError,
        .on_close = Counters.onClose,
    });

    var waiter = DisconnectWaiter{ .session = session };
    _ = try session.peer.sendBootstrap(&waiter, DisconnectWaiter.onReturn);

    session.run();
    accept_thread.join();

    // The dropped transport resolved the outstanding bootstrap exactly once
    // with the Disconnected terminal before on_close fired.
    try std.testing.expectEqual(@as(usize, 1), waiter.fired);
    try std.testing.expectEqual(@as(usize, 1), waiter.disconnects);
    try std.testing.expectEqual(@as(usize, 1), counters.closes);
    session.deinit();
}

test "connect never leaks under allocation failure" {
    const io = std.testing.io;

    const server = try listenLoopback(io, 64);
    defer tcp.closeFd(io, server.fd);

    var fail_index: usize = 0;
    while (fail_index < 24) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
        const session = tcp.connect(failing.allocator(), io, server.address, .{}) catch continue;
        // Injection index beyond connect's allocations: tear down cleanly.
        session.close();
        session.run();
        session.deinit();
    }
    // std.testing.allocator (backing the failing wrapper) reports any leak
    // at test end, whichever allocation the injection hit.
}
