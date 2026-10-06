const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");
const capnp_build_options = @import("capnp_build_options");

// Release sentinels: each package root must carry the new sprint surface, not
// merely compile an older baseline. This file is imported by all three clean-
// room consumers.
comptime {
    _ = capnpc.message.PointerListReader.isNull;
    _ = capnpc.message.PointerListBuilder.initTextList;
    _ = capnpc.message.typed_list_helpers.NestedListReader;
    _ = capnpc.rpc.peer.CallOptions;
}

// The package exports its `-Dfd-passing` options module, so a consumer can
// read the option without making a second options module with the same
// contents (two modules may not own one file). The gate follows it: fd
// passing on Linux and macOS only, and only with the option on.
comptime {
    const os = builtin.target.os.tag;
    const expect_fd = capnp_build_options.fd_passing and (os == .linux or os == .macos);
    if ((@FieldType(capnpc.rpc.peer.FdHandle, "fd") != void) != expect_fd)
        @compileError("capnp_build_options.fd_passing disagrees with the fd passing gate");
}

// Downstream surface, shared by every root.
//
// Zig analyzes lazily: a function body is type-checked only when something
// references it, and naming a namespace or a type does not. A consumer that
// never references a function will build even if that function no longer
// compiles. v0.18.0 shipped that way: `Transport.read` used std's
// `net.Stream.read`, which does not compile on tagged Zig 0.17.0, and this
// gate stayed green because no root referenced the TCP transport.
//
// Each `_ = &f;` below forces one function body through semantic analysis.
// Bare `_ = f;` is not enough. A generic function is analyzed only when it is
// called with concrete arguments, so those go through a never-called `force*`
// function instead. The comment on each line names the downstream that uses
// the API. When a downstream starts using an API that is not listed, add it
// here or to its root's list before the next tag (RELEASING.md, section 1).
//
// Consumers by root: default = slcp-zig, bucketlist-zig, qmsg (`-Dcapnp`);
// core = prollytree-zig; quic = mruby-quic, capnp-qmsg-demo.
comptime {
    const message = capnpc.message;
    _ = &message.MessageBuilder.init; // slcp, bucketlist, prollytree, qmsg, capnp-qmsg-demo, mruby-quic
    _ = &message.MessageBuilder.deinit; // slcp, bucketlist, prollytree, qmsg, capnp-qmsg-demo, mruby-quic
    _ = &message.MessageBuilder.allocateStruct; // slcp, bucketlist, prollytree, qmsg, capnp-qmsg-demo, mruby-quic
    _ = &message.MessageBuilder.toBytes; // slcp, bucketlist, capnp-qmsg-demo, mruby-quic
    _ = &message.MessageBuilder.toPackedBytes; // qmsg (codec_capnp.encode)
    _ = &message.Message.init; // slcp, bucketlist
    _ = &message.Message.initFlat; // prollytree (format/capnp.readWithScratch)
    _ = &message.Message.initPacked; // qmsg (codec_capnp.decode)
    _ = &message.Message.initUnvalidated; // mruby-quic
    _ = &message.Message.deinit; // slcp, bucketlist, prollytree, qmsg, mruby-quic
    _ = &message.Message.getRootStruct; // slcp, bucketlist, prollytree, qmsg, capnp-qmsg-demo
    _ = &message.cloneAnyPointer; // mruby-quic
    // Generated bindings (slcp src/gen, bucketlist) call the StructReader and
    // StructBuilder accessors; prollytree and qmsg call them by hand.
    refAllFunctions(message.StructReader); // slcp, bucketlist, prollytree, qmsg (generated + hand-written readers)
    refAllFunctions(message.StructBuilder); // slcp, bucketlist, prollytree, qmsg (generated + hand-written builders)

    // The list values those accessors return have bodies of their own, which
    // walking StructReader and StructBuilder does not reach.
    _ = &message.U32ListReader.len; // prollytree (format/capnp.copyOffsets)
    _ = &message.U32ListReader.get; // prollytree (format/capnp.copyOffsets)
    _ = &message.U32ListBuilder.set; // prollytree (format/capnp offset writers)
    const typed = message.typed_list_helpers;
    _ = &typed.DataListReader.len; // slcp (generated Data lists: validators, votes, values)
    _ = &typed.DataListReader.get; // slcp (generated Data lists)
    _ = &typed.DataListBuilder.set; // slcp (generated Data lists: engine/emit.writeValueList)
    _ = &forceSerializationGenerics; // slcp, bucketlist (generated struct lists)

    const canonical = capnpc.canonical;
    _ = &canonical.isCanonical; // slcp
    _ = &canonical.canonicalizeFlat; // slcp
    _ = &canonical.canonicalizeFlatFromBuilder; // slcp, prollytree (format/capnp.serialize)

    const Framer = capnpc.rpc.wire.framing.Framer;
    _ = &Framer.initWithOptions; // slcp (node/overlay.zig: every consensus-network frame)
    _ = &Framer.push; // slcp
    _ = &Framer.popFrame; // slcp
    _ = &Framer.reset; // slcp
    _ = &Framer.deinit; // slcp
}

/// Never called. Taking its address forces its body through analysis, which
/// instantiates the generic list helpers the way capnpc-zig's generated
/// bindings do. `_ = &typed.StructListReader;` alone would not.
fn forceSerializationGenerics(
    reader: capnpc.message.StructReader,
    struct_list: capnpc.message.StructListReader,
    struct_list_builder: capnpc.message.StructListBuilder,
) !void {
    const message = capnpc.message;
    const typed = message.typed_list_helpers;
    _ = reader.emptyList(message.PointerListReader); // slcp (generated Data-list getters on a null pointer)
    const items: typed.StructListReader(GeneratedStruct) = .{ ._list = struct_list };
    _ = items.len(); // slcp (node/wire, engine/qset), bucketlist (proofs_wire)
    _ = try items.get(0); // slcp (node/wire, engine/qset), bucketlist (proofs_wire)
    const out: typed.StructListBuilder(GeneratedStruct) = .{ ._list = struct_list_builder };
    _ = try out.get(0); // slcp (node/wire), bucketlist (proofs_wire)
}

/// The shape capnpc-zig generates for a struct, which the typed list helpers
/// wrap: slcp's QuorumSet and Envelope, bucketlist's Step and ChainLevel.
const GeneratedStruct = struct {
    pub const Reader = struct {
        _reader: capnpc.message.StructReader,
        pub fn wrap(reader: capnpc.message.StructReader) Reader {
            return .{ ._reader = reader };
        }
    };
    pub const Builder = struct {
        _builder: capnpc.message.StructBuilder,
        pub fn wrap(builder: capnpc.message.StructBuilder) Builder {
            return .{ ._builder = builder };
        }
    };
};

/// `_ = &` every function declared directly on `T`. A generic method stays
/// uninstantiated (taking its address does not pick a type), so callers
/// still cover those with a concrete call.
pub fn refAllFunctions(comptime T: type) void {
    inline for (comptime std.meta.declarations(T)) |name| {
        if (@typeInfo(@TypeOf(@field(T, name))) == .@"fn") {
            _ = &@field(T, name);
        }
    }
}

pub fn exerciseSerialization() !void {
    try exerciseReflection();

    var builder = capnpc.message.MessageBuilder.init(std.heap.page_allocator);
    defer builder.deinit();

    var root = try builder.allocateStruct(1, 1);
    root.writeU32(0, 0xdecafbad);
    try root.writeText(0, "consumer");

    const bytes = try builder.toBytes();
    defer std.heap.page_allocator.free(bytes);

    var parsed = try capnpc.message.Message.init(std.heap.page_allocator, bytes, .{});
    defer parsed.deinit();
    const reader = try parsed.getRootStruct();
    if (reader.readU32(0) != 0xdecafbad) return error.ConsumerRoundTripFailed;
    if (!std.mem.eql(u8, try reader.readText(0), "consumer")) return error.ConsumerRoundTripFailed;
}

// Prove the filtered package carries both regenerated binary descriptors and
// the reflection runtime through the default, core, and QUIC module roots.
fn exerciseReflection() !void {
    const allocator = std.heap.page_allocator;
    const schema_ref = capnpc.rpc.wire.protocol.PayloadBuilder.capnpSchema;
    const registry = try schema_ref.load(allocator);
    defer registry.deinit();
    const descriptor = try schema_ref.resolve(registry);
    if (descriptor.id() != schema_ref.id) return error.ConsumerReflectionFailed;
    _ = try descriptor.raw();
    const payload_schema = try descriptor.asStruct();

    var builder = capnpc.message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const payload = try capnpc.reflection.DynamicStruct.Builder.init(payload_schema, &builder);
    _ = try payload.initList("capTable", 0);
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    var parsed = try capnpc.message.Message.init(allocator, bytes, .{});
    defer parsed.deinit();
    const reader = try capnpc.reflection.DynamicStruct.Reader.init(payload_schema, &parsed);
    if (try (try reader.get("capTable")).list.len() != 0) return error.ConsumerReflectionFailed;
}
