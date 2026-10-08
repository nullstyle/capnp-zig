//! Root of the embedder static libraries that `zig build check-ios` and
//! `zig build check-fd-passing-off-symbols` build: `capnpc-zig-core` for the
//! iOS device and simulator targets, and for macOS with fd passing compiled
//! out (`-Dfd-passing=false`). A static library never links, so no Apple
//! SDK is needed, and nothing here runs on those targets.
//!
//! The std overrides are the ones every iOS embedder needs at Zig 0.17.0:
//! any reference to `std.Io.Threaded.io()` fails to compile for iOS, tvOS,
//! watchOS and visionOS, and the default panic handler, the default log
//! function and `std.debug` reach it through `std.Options.debug_io`
//! (docs/upstream/handoff-zig-fork-ios-nullfile.md). capnp-swift's root
//! makes the same three overrides.
//!
//! `zig build test` also runs the tests at the end on the host, so the
//! exported functions are known to work, not only to compile.
const std = @import("std");
const builtin = @import("builtin");
const core = @import("capnpc-zig-core");
const build_options = @import("capnp_build_options");

const rpc = core.rpc;
const Peer = rpc.peer.Peer;

/// The static libraries link libc and allocate with `std.heap.c_allocator`,
/// as an embedder's do. The host test links no libc, so it compiles for
/// every CI cross target (Zig cannot provide a libc for some of them, for
/// example powerpc64-linux-gnu).
const embedder_allocator = if (builtin.link_libc) std.heap.c_allocator else std.heap.page_allocator;

// The native C ABI capnp-swift ships from these libraries (src/native/,
// handoff H7): referencing it emits the `capnp_*` symbols, so `check-ios`
// compiles them for every target above and `check-fd-passing-off-symbols`
// reads them. abi.zig takes its allocator from the root.
comptime {
    _ = core.native.abi;
}
pub const capnp_core_allocator: std.mem.Allocator = embedder_allocator;

fn trapPanic(msg: []const u8, ra: ?usize) noreturn {
    _ = msg;
    _ = ra;
    @trap();
}
pub const panic = std.debug.FullPanic(trapPanic);

fn noLog(comptime level: std.log.Level, comptime scope: @EnumLiteral(), comptime fmt: []const u8, args: anytype) void {
    _ = level;
    _ = scope;
    _ = fmt;
    _ = args;
}
pub const std_options: std.Options = .{ .logFn = noLog };
pub const std_options_debug_io: std.Io = std.Io.failing;

// The fd passing gate must follow both the target and `-Dfd-passing`: on
// only for Linux and macOS, and only with the option on.
comptime {
    const os = builtin.target.os.tag;
    const expect_fd = build_options.fd_passing and (os == .linux or os == .macos);
    if ((@FieldType(rpc.peer.FdHandle, "fd") != void) != expect_fd)
        @compileError("fd passing gate disagrees with the target and -Dfd-passing");
}

/// The sans-IO calls an embedder makes on a peer with no transport
/// (capnp-swift's shim). Returns 1.
export fn capnp_apple_check_peer() u32 {
    var peer = Peer.initDetached(embedder_allocator);
    defer peer.deinit();
    peer.disableThreadAffinity();
    _ = peer.checkDeadlines();
    peer.handleFrame(&.{}) catch {};
    _ = peer.importFd(0);
    peer.notifyTransportClosed();
    return 1;
}

/// Two peers that pass frames through memory: one bootstraps the other,
/// and the bootstrap capability comes back. Returns 1 on success, 0 on any
/// failure.
export fn capnp_apple_check_loopback() u32 {
    const ok = loopbackBootstrap(embedder_allocator) catch return 0;
    return @intFromBool(ok);
}

/// Frames one peer sent, in order, for the other peer to read. A peer
/// never reads a frame inside its partner's `send`: the pump reads them
/// after the call that sent them returns.
const Wire = struct {
    allocator: std.mem.Allocator,
    frames: std.ArrayList([]u8) = .empty,

    fn send(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Wire = @ptrCast(@alignCast(ctx));
        const copy = try self.allocator.dupe(u8, frame);
        errdefer self.allocator.free(copy);
        try self.frames.append(self.allocator, copy);
    }

    /// Delivers every queued frame to `to`. Returns whether any was.
    fn deliver(self: *Wire, to: *Peer) !bool {
        if (self.frames.items.len == 0) return false;
        while (self.frames.items.len != 0) {
            const frame = self.frames.orderedRemove(0);
            defer self.allocator.free(frame);
            try to.handleFrame(frame);
        }
        return true;
    }

    fn deinit(self: *Wire) void {
        for (self.frames.items) |frame| self.allocator.free(frame);
        self.frames.deinit(self.allocator);
    }
};

const Bootstrapped = struct {
    returned: bool = false,
    results: bool = false,

    fn onReturn(ctx: *anyopaque, peer: *Peer, ret: rpc.wire.protocol.Return, caps: *const rpc.caps.table.InboundCapTable) anyerror!void {
        _ = peer;
        _ = caps;
        const self: *Bootstrapped = @ptrCast(@alignCast(ctx));
        self.returned = true;
        self.results = ret.tag == .results;
    }
};

const Service = struct {
    fn onCall(ctx: *anyopaque, peer: *Peer, call: rpc.wire.protocol.Call, caps: *const rpc.caps.table.InboundCapTable) anyerror!void {
        _ = ctx;
        _ = peer;
        _ = call;
        _ = caps;
    }
};

fn loopbackBootstrap(allocator: std.mem.Allocator) !bool {
    var to_server: Wire = .{ .allocator = allocator };
    defer to_server.deinit();
    var to_client: Wire = .{ .allocator = allocator };
    defer to_client.deinit();

    var client = Peer.initDetached(allocator);
    defer client.deinit();
    client.disableThreadAffinity();
    client.attachTransport(&to_server, null, Wire.send, null, null);

    var server = Peer.initDetached(allocator);
    defer server.deinit();
    server.disableThreadAffinity();
    server.attachTransport(&to_client, null, Wire.send, null, null);

    var service: u8 = 0;
    _ = try server.setBootstrap(.{ .ctx = &service, .on_call = Service.onCall });

    var bootstrapped: Bootstrapped = .{};
    _ = try client.sendBootstrap(&bootstrapped, Bootstrapped.onReturn);

    // Bootstrap, Return, Finish, and whatever else either side answers.
    var rounds: usize = 0;
    while (rounds < 16) : (rounds += 1) {
        const a = try to_server.deliver(&server);
        const b = try to_client.deliver(&client);
        if (!a and !b) break;
    }
    _ = client.checkDeadlines();
    _ = server.checkDeadlines();
    return bootstrapped.returned and bootstrapped.results;
}

test "the check library's exported functions work on the host" {
    try std.testing.expect(try loopbackBootstrap(std.testing.allocator));
    try std.testing.expectEqual(@as(u32, 1), capnp_apple_check_peer());
}
