//! Generated Builder mutators spell `message.BuildError` / `message.CopyError`.
//!
//! slcp kept four public entry points out of its Stable tier because generated
//! `initX` signatures were inferred (`!T`) and could widen to `anyerror`
//! whenever a builder primitive routed through an `anyerror`-typed pointer
//! maker. These tests pin the named sets on the committed kvstore bindings
//! (examples/kvstore/gen/kvstore.zig) by type identity, so each regression
//! turns them red:
//!
//!  - the plugin stops spelling the set: the inferred set is a different type
//!    and the equality assertions fail;
//!  - a builder primitive regresses to `anyerror`: the generated body no longer
//!    coerces into the spelled set and this file fails to compile.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const kvstore = @import("kvstore");
const message = capnpc.message;

fn ErrorSetOf(comptime FnType: type) type {
    const ret = @typeInfo(FnType).@"fn".return_type.?;
    return @typeInfo(ret).error_union.error_set;
}

test "kvstore initChanges/initOps/initResults return message.BuildError" {
    // Both the method-scoped (`WriteBatch.Params`) and the file-scoped
    // (`WriteBatchParams`) spellings name the same generated Builder.
    const cases = .{
        @TypeOf(kvstore.KvClientNotifier.KeysChanged.Params.Builder.initChanges),
        @TypeOf(kvstore.KvClientNotifier.KeysChangedParams.Builder.initChanges),
        @TypeOf(kvstore.KvStore.WriteBatch.Params.Builder.initOps),
        @TypeOf(kvstore.KvStore.WriteBatchParams.Builder.initOps),
        @TypeOf(kvstore.KvStore.WriteBatch.Results.Builder.initResults),
        @TypeOf(kvstore.KvStore.WriteBatchResults.Builder.initResults),
    };
    inline for (cases) |FnType| {
        try std.testing.expect(ErrorSetOf(FnType) == message.BuildError);
    }
}

test "kvstore init methods build through the spelled set" {
    // Calling the methods analyzes their bodies, which is where a builder
    // primitive regressing to `anyerror` fails to coerce into BuildError.
    var builder = message.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    var params = try kvstore.KvStore.WriteBatchParams.Builder.init(&builder);
    const ops = try params.initOps(2);
    try std.testing.expectEqual(@as(u32, 2), ops.len());

    var results_builder = message.MessageBuilder.init(std.testing.allocator);
    defer results_builder.deinit();
    var results = try kvstore.KvStore.WriteBatchResults.Builder.init(&results_builder);
    const written = try results.initResults(3);
    try std.testing.expectEqual(@as(u32, 3), written.len());

    var notify_builder = message.MessageBuilder.init(std.testing.allocator);
    defer notify_builder.deinit();
    var notify = try kvstore.KvClientNotifier.KeysChangedParams.Builder.init(&notify_builder);
    const changes = notify.initChanges(1) catch |err| switch (err) {
        // Exhaustive over the spelled set: this switch stops compiling if the
        // generated signature ever widens past message.BuildError.
        error.OutOfMemory,
        error.TooManySegments,
        error.InvalidSegmentId,
        error.OutOfBounds,
        error.IndexOutOfBounds,
        error.PointerIndexOutOfBounds,
        error.OffsetOutOfRange,
        error.FarPointerOffsetTooLarge,
        error.InvalidPointer,
        error.ElementCountTooLarge,
        error.ListTooLarge,
        error.TextTooLong,
        => return err,
    };
    try std.testing.expectEqual(@as(u32, 1), changes.len());
}

test "kvstore copy setters return message.CopyError" {
    const cases = .{
        @TypeOf(kvstore.KvClientNotifier.KeysChangedParams.Builder.setChanges),
        @TypeOf(kvstore.KvStore.WriteBatchParams.Builder.setOps),
        @TypeOf(kvstore.KvStore.GetResults.Builder.setEntry),
        @TypeOf(kvstore.WriteOpResult.Builder.setPut),
    };
    inline for (cases) |FnType| {
        try std.testing.expect(ErrorSetOf(FnType) == message.CopyError);
    }
}

const Tally = struct { build: usize = 0, copy: usize = 0, other: usize = 0 };

fn isMutatorName(comptime name: []const u8) bool {
    inline for (.{ "init", "set", "clear" }) |prefix| {
        if (name.len > prefix.len and std.mem.startsWith(u8, name, prefix) and
            std.ascii.isUpper(name[prefix.len])) return true;
    }
    return false;
}

/// Walk every `Builder` the bindings declare (and the views nested in it) and
/// classify each `initX`/`setX`/`clearX` by its error set. `setXServer`
/// exports through the Peer, so it legitimately keeps an inferred set.
fn tallyMutators(comptime T: type, comptime in_builder: bool, comptime depth: usize, tally: *Tally) void {
    if (depth > 6) return;
    inline for (comptime std.meta.declarations(T)) |name| {
        const D = @field(T, name);
        if (@TypeOf(D) == type) {
            switch (@typeInfo(D)) {
                .@"struct" => tallyMutators(D, in_builder or std.mem.eql(u8, name, "Builder"), depth + 1, tally),
                else => {},
            }
        } else if (in_builder and @typeInfo(@TypeOf(D)) == .@"fn" and comptime isMutatorName(name)) {
            if (comptime std.mem.endsWith(u8, name, "Server")) continue;
            // Taking the address forces the body to be analyzed. Zig checks a
            // spelled set against the body only then, so without this a
            // primitive regressing to `anyerror` would go unnoticed here.
            std.mem.doNotOptimizeAway(&D);
            const E = ErrorSetOf(@TypeOf(D));
            if (E == message.BuildError) {
                tally.build += 1;
            } else if (E == message.CopyError) {
                tally.copy += 1;
            } else {
                std.debug.print("unspelled generated mutator: {s}.{s}\n", .{ @typeName(T), name });
                tally.other += 1;
            }
        }
    }
}

test "every generated kvstore Builder mutator spells a named error set" {
    @setEvalBranchQuota(200_000);
    var tally: Tally = .{};
    tallyMutators(kvstore, false, 0, &tally);
    try std.testing.expectEqual(@as(usize, 0), tally.other);
    // Guard against a vacuous pass if the walk stops finding Builders.
    try std.testing.expect(tally.build >= 100);
    try std.testing.expect(tally.copy >= 10);
}
