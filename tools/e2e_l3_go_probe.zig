const std = @import("std");

const Allocator = std.mem.Allocator;
const max_file_bytes = 512 * 1024;

const Tap = struct {
    test_num: usize = 0,
    failures: usize = 0,

    fn ok(self: *Tap, pass: bool, desc: []const u8) void {
        self.test_num += 1;
        if (pass) {
            std.debug.print("ok {d} - {s}\n", .{ self.test_num, desc });
        } else {
            std.debug.print("not ok {d} - {s}\n", .{ self.test_num, desc });
            self.failures += 1;
        }
    }
};

fn readRepoFile(allocator: Allocator, io: std.Io, path: []const u8) ![]u8 {
    return std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_file_bytes));
}

fn containsAll(haystack: []const u8, needles: []const []const u8) bool {
    for (needles) |needle| {
        if (std.mem.indexOf(u8, haystack, needle) == null) return false;
    }
    return true;
}

fn containsAny(haystack: []const u8, needles: []const []const u8) bool {
    for (needles) |needle| {
        if (std.mem.indexOf(u8, haystack, needle) != null) return true;
    }
    return false;
}

fn sliceBetween(haystack: []const u8, start_marker: []const u8, end_marker: []const u8) ![]const u8 {
    const start = std.mem.indexOf(u8, haystack, start_marker) orelse return error.MissingStartMarker;
    const after_start = haystack[start + start_marker.len ..];
    const end = std.mem.indexOf(u8, after_start, end_marker) orelse return error.MissingEndMarker;
    return after_start[0..end];
}

fn probeGoL3RuntimeSurface(allocator: Allocator, io: std.Io, tap: *Tap) !void {
    const network_go = try readRepoFile(allocator, io, "vendor/ext/go-capnp/rpc/network.go");
    defer allocator.free(network_go);
    const rpc_go = try readRepoFile(allocator, io, "vendor/ext/go-capnp/rpc/rpc.go");
    defer allocator.free(rpc_go);

    tap.ok(
        containsAll(network_go, &.{
            "type Network3PH interface",
            "Introduce(provider *Conn, recipient *Conn) (IntroductionInfo, error)",
            "Forward(from *Conn, destination *Conn, info ThirdPartyToContact) (ThirdPartyToContact, error)",
            "CompleteThirdParty(ctx context.Context, conn *Conn, completion ThirdPartyCompletion) (any, error)",
            "AwaitThirdParty(ctx context.Context, conn *Conn, await ThirdPartyToAwait, value any)",
        }),
        "Go vendored Network3PH interface exposes the expected Level-3 hook names",
    );

    const accept_provide = try sliceBetween(
        rpc_go,
        "case rpccp.Message_Which_accept, rpccp.Message_Which_provide:",
        "default:",
    );
    tap.ok(
        containsAll(accept_provide, &.{ "if c.network != nil", "panic(\"TODO: 3PH\")" }),
        "Go inbound Accept/Provide runtime path is still TODO",
    );

    const third_party_hosted = try sliceBetween(
        rpc_go,
        "case rpccp.CapDescriptor_Which_thirdPartyHosted:",
        "case rpccp.CapDescriptor_Which_receiverHosted:",
    );
    tap.ok(
        containsAll(third_party_hosted, &.{ "if c.network == nil", "panic(\"TODO: 3PH\")" }),
        "Go thirdPartyHosted pickup falls back without Network and is TODO with Network",
    );

    const await_from_third_party = try sliceBetween(
        rpc_go,
        "case rpccp.Return_Which_awaitFromThirdParty:",
        "default:",
    );
    tap.ok(
        containsAll(await_from_third_party, &.{ "TODO: 3PH", "allowThirdPartyTailCall = false" }),
        "Go awaitFromThirdParty Return runtime path is still TODO",
    );

    const disembargo_accept = try sliceBetween(
        rpc_go,
        "case rpccp.Disembargo_context_Which_accept:",
        "default:",
    );
    tap.ok(
        containsAll(disembargo_accept, &.{ "if c.network != nil", "panic(\"TODO: 3PH\")" }),
        "Go accept-context Disembargo runtime path is still TODO",
    );

    const is_local_client = try sliceBetween(
        rpc_go,
        "func (c *lockedConn) isLocalClient(client capnp.Client) bool",
        "if _, ok := bv.(error); ok {",
    );
    tap.ok(
        containsAny(is_local_client, &.{
            "Might have to do more refactoring re: what to do in this case",
            "panic(\"TODO: 3PH\")",
        }),
        "Go same-network embargo/locality logic still has 3PH TODO guards",
    );
}

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);

    var tap = Tap{};
    try probeGoL3RuntimeSurface(gpa.allocator(), init.io, &tap);

    std.debug.print("1..{d}\n", .{tap.test_num});
    if (tap.failures > 0) return error.TestFailed;
}
