//! Generated Builder mutators spell `message.BuildError` / `message.CopyError`
//! where they can allocate, and stay precise where they cannot.
//!
//! slcp kept four public entry points out of its Stable tier because generated
//! `initX` signatures were inferred (`!T`) and could widen to `anyerror`
//! whenever a builder primitive routed through an `anyerror`-typed pointer
//! maker. These tests pin the contract on the committed kvstore bindings
//! (examples/kvstore/gen/kvstore.zig) and on the generated rpc.capnp bindings
//! that the Stable `rpc.wire.protocol` builders wrap, by type identity:
//!
//!  - `initX`, the text/data/capability setters: exactly `message.BuildError`;
//!  - copy setters (`setX` from a Reader): exactly `message.CopyError`;
//!  - scalar setters: an empty error set, as before the named sets existed;
//!  - `clearX` and `setXNull`: never the whole `BuildError`, only the
//!    pointer-slot errors their body can produce.
//!
//! Each regression turns them red: the plugin stops spelling a set, spells it
//! on a setter that cannot allocate, or a builder primitive regresses to
//! `anyerror` (the generated body no longer coerces into the spelled set and
//! this file fails to compile).

const std = @import("std");
const capnpc = @import("capnpc-zig");
const kvstore = @import("kvstore");
const message = capnpc.message;
const rpc_capnp = capnpc.rpc.generated.rpc;

fn ErrorSetOf(comptime FnType: type) type {
    const ret = @typeInfo(FnType).@"fn".return_type.?;
    return @typeInfo(ret).error_union.error_set;
}

fn errorNames(comptime E: type) []const [:0]const u8 {
    return @typeInfo(E).error_set.error_names orelse @compileError(@typeName(E) ++ " is anyerror");
}

/// True when every error in `Sub` is also in `Super`.
fn isSubsetOf(comptime Sub: type, comptime Super: type) bool {
    for (errorNames(Sub)) |sub| {
        const found = for (errorNames(Super)) |super| {
            if (std.mem.eql(u8, sub, super)) break true;
        } else false;
        if (!found) return false;
    }
    return true;
}

fn sameErrors(comptime A: type, comptime B: type) bool {
    return isSubsetOf(A, B) and isSubsetOf(B, A);
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

test "scalar setters and clearX keep the error sets they had before the named sets" {
    // Scalar setters write fixed-size data into space the message already
    // holds: no error at all, including union members (whose discriminant
    // write cannot fail), Void members and enums.
    const scalar_cases = .{
        @TypeOf(kvstore.Entry.Builder.setVersion),
        @TypeOf(kvstore.BackupInfo.Builder.setTimestamp),
        @TypeOf(kvstore.KvStore.GetResults.Builder.setFound),
        @TypeOf(kvstore.WriteOp.Builder.setDelete), // Void union member
        @TypeOf(kvstore.WriteOpResult.Builder.setDelete), // Bool union member
        @TypeOf(kvstore.Entry.Builder.clearVersion),
        // The Stable rpc.wire.protocol builders wrap these; spelling
        // BuildError on them widened ReturnBuilder.setTakeFromOtherQuestion
        // from error{InvalidReturnTag} to thirteen errors.
        @TypeOf(rpc_capnp.Return.Builder.setTakeFromOtherQuestion),
        @TypeOf(rpc_capnp.Return.Builder.setReleaseParamCaps),
        @TypeOf(rpc_capnp.Call.Builder.setQuestionId),
        @TypeOf(rpc_capnp.Exception.Builder.setType), // enum
    };
    inline for (scalar_cases) |FnType| {
        try std.testing.expectEqual(@as(usize, 0), errorNames(ErrorSetOf(FnType)).len);
    }

    // Nulling a pointer never allocates: only the pointer-slot errors.
    const pointer_slot_errors = error{ InvalidSegmentId, OutOfBounds, PointerIndexOutOfBounds };
    const null_cases = .{
        @TypeOf(rpc_capnp.Payload.Builder.setContentNull),
        @TypeOf(kvstore.Entry.Builder.clearKey),
        @TypeOf(kvstore.KvStore.SubscribeParams.Builder.clearNotifier),
    };
    inline for (null_cases) |FnType| {
        try std.testing.expect(comptime sameErrors(ErrorSetOf(FnType), pointer_slot_errors));
    }
}

/// What a generated mutator may return, decided from its name and the type of
/// the value it writes.
const Contract = enum {
    /// `initX`, text/data setters, capability setters: exactly BuildError.
    build,
    /// `setX` from a Reader: exactly CopyError.
    copy,
    /// Scalar `setX`: no error at all.
    scalar,
    /// `clearX` and `setXNull`: a strict subset of BuildError.
    no_alloc,
};

fn isMutatorName(comptime name: []const u8) bool {
    inline for (.{ "init", "set", "clear" }) |prefix| {
        if (name.len > prefix.len and std.mem.startsWith(u8, name, prefix) and
            std.ascii.isUpper(name[prefix.len])) return true;
    }
    return false;
}

fn isScalar(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .void, .bool, .int, .float, .@"enum" => true,
        else => false,
    };
}

fn contractOf(comptime name: []const u8, comptime FnType: type) Contract {
    if (std.mem.startsWith(u8, name, "init")) return .build;
    if (std.mem.startsWith(u8, name, "clear") or std.mem.endsWith(u8, name, "Null")) return .no_alloc;
    const param_types = @typeInfo(FnType).@"fn".param_types;
    const Value = param_types[param_types.len - 1].?;
    if (isScalar(Value)) return .scalar;
    if (Value == []const u8 or Value == message.Capability) return .build;
    // `setXClient(client: Iface.Client)` writes a capability pointer.
    if (@typeInfo(Value) == .@"struct" and @hasField(Value, "cap_id")) return .build;
    return .copy;
}

fn honors(comptime contract: Contract, comptime E: type) bool {
    return switch (contract) {
        .build => E == message.BuildError,
        .copy => E == message.CopyError,
        .scalar => errorNames(E).len == 0,
        .no_alloc => isSubsetOf(E, message.BuildError) and
            errorNames(E).len < errorNames(message.BuildError).len,
    };
}

const Tally = struct {
    build: usize = 0,
    copy: usize = 0,
    scalar: usize = 0,
    no_alloc: usize = 0,
    broken: usize = 0,
};

/// Walk every `Builder` the bindings declare (and the views nested in it) and
/// check each `initX`/`setX`/`clearX` against its contract. `setXServer`
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
            const contract = comptime contractOf(name, @TypeOf(D));
            if (comptime honors(contract, ErrorSetOf(@TypeOf(D)))) {
                @field(tally, @tagName(contract)) += 1;
            } else {
                std.debug.print("{s}.{s} breaks the .{s} error-set contract: {s}\n", .{
                    @typeName(T), name, @tagName(contract), @typeName(ErrorSetOf(@TypeOf(D))),
                });
                tally.broken += 1;
            }
        }
    }
}

test "every generated kvstore Builder mutator honors its error-set contract" {
    @setEvalBranchQuota(400_000);
    var tally: Tally = .{};
    tallyMutators(kvstore, false, 0, &tally);
    try std.testing.expectEqual(@as(usize, 0), tally.broken);
    // Guard against a vacuous pass if the walk stops finding Builders
    // (32 / 17 / 31 / 59 when this was written).
    try std.testing.expect(tally.build >= 30);
    try std.testing.expect(tally.copy >= 15);
    try std.testing.expect(tally.scalar >= 30);
    try std.testing.expect(tally.no_alloc >= 50);
}

test "every generated rpc.capnp Builder mutator honors its error-set contract" {
    @setEvalBranchQuota(1_000_000);
    var tally: Tally = .{};
    tallyMutators(rpc_capnp, false, 0, &tally);
    try std.testing.expectEqual(@as(usize, 0), tally.broken);
    // 74 / 39 / 42 / 97 when this was written.
    try std.testing.expect(tally.build >= 70);
    try std.testing.expect(tally.copy >= 35);
    try std.testing.expect(tally.scalar >= 40);
    try std.testing.expect(tally.no_alloc >= 90);
}
