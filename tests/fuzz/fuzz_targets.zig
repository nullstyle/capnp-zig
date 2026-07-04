//! Coverage-guided fuzz targets for the untrusted-input surfaces.
//!
//! Run deterministically (smoke) as part of `zig build test-fuzz`; run with
//! coverage-guided input generation via `zig build test-fuzz --fuzz`.
//!
//! Each target treats *any* error from the API under test as a correct
//! rejection — the properties being checked are "no crash, no hang, no
//! leak" plus a few read-side probes that must stay in bounds whenever the
//! parser accepts an input.

const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const framing = capnpc.rpc.wire.framing;
const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const Peer = capnpc.rpc.peer.Peer;
const request_reader = capnpc.request;
const Generator = capnpc.codegen.Generator;

const max_fuzz_input = 8 * 1024;

/// Validated wire-message parse plus reader probes on accepted inputs.
fn fuzzMessageInit(_: void, smith: *std.testing.Smith) anyerror!void {
    var buf: [max_fuzz_input]u8 = undefined;
    const len = smith.slice(&buf);
    const bytes = buf[0..len];

    var msg = message.Message.init(std.testing.allocator, bytes, .{}) catch return;
    defer msg.deinit();

    // Accepted messages must survive root traversal and primitive reads.
    const root = msg.getRootStruct() catch return;
    _ = root.readU64(0);
    _ = root.readText(0) catch {};
}

/// Packed decode: unpack budget enforcement plus validated parse of the
/// unpacked bytes.
fn fuzzPackedDecode(_: void, smith: *std.testing.Smith) anyerror!void {
    var buf: [max_fuzz_input]u8 = undefined;
    const len = smith.slice(&buf);
    const bytes = buf[0..len];

    var msg = message.Message.initPacked(std.testing.allocator, bytes, .{}) catch return;
    defer msg.deinit();
    const root = msg.getRootStruct() catch return;
    _ = root.readU32(0);
}

/// Stream framer: arbitrary chunk boundaries must never produce an
/// out-of-bounds frame or leak buffered bytes.
fn fuzzFramer(_: void, smith: *std.testing.Smith) anyerror!void {
    var framer = framing.Framer.init(std.testing.allocator);
    defer framer.deinit();

    var chunk_buf: [512]u8 = undefined;
    while (!smith.eosWeightedSimple(7, 1)) {
        const chunk_len = smith.slice(&chunk_buf);
        framer.push(chunk_buf[0..chunk_len]) catch {
            framer.reset();
            continue;
        };
        while (true) {
            const frame = framer.popFrame() catch {
                framer.reset();
                break;
            };
            if (frame) |bytes| {
                std.testing.allocator.free(bytes);
            } else break;
        }
    }
}

/// RPC frame dispatch on a detached peer: arbitrary frames must be either
/// dispatched or rejected without corrupting peer state across a sequence
/// of frames.
fn fuzzPeerHandleFrame(_: void, smith: *std.testing.Smith) anyerror!void {
    const Sink = struct {
        fn onFrame(_: *anyopaque, _: []const u8) anyerror!void {}
    };

    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();
    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, Sink.onFrame);

    var frame_buf: [max_fuzz_input]u8 = undefined;
    while (!smith.eosWeightedSimple(3, 1)) {
        const frame_len = smith.slice(&frame_buf);
        peer.handleFrame(frame_buf[0..frame_len]) catch {};
    }
}

/// Fill a Payload cap table with `count` fuzzed CapDescriptors spanning every
/// variant (none / senderHosted / senderPromise / receiverHosted /
/// receiverAnswer), with ids chosen from a small range so they overlap the
/// peer's seeded export/import/answer state — the exact conditions under which
/// an id-space confusion (e.g. the export/import collision class) would surface.
fn fuzzCapTable(cap_list: anytype, count: u32, smith: *std.testing.Smith) !void {
    var list = cap_list;
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        var desc = try list.get(i);
        switch (smith.valueRangeAtMost(u8, 0, 4)) {
            0 => try desc.setNone({}),
            1 => try desc.setSenderHosted(smith.valueRangeAtMost(u32, 0, 8)),
            2 => try desc.setSenderPromise(smith.valueRangeAtMost(u32, 0, 8)),
            3 => try desc.setReceiverHosted(smith.valueRangeAtMost(u32, 0, 8)),
            else => {
                var ops: [4]protocol.PromisedAnswerOp = undefined;
                const n: usize = smith.valueRangeAtMost(u8, 0, ops.len);
                for (ops[0..n]) |*op| op.* = .{
                    .tag = if (smith.boolWeighted(1, 1)) .getPointerField else .noop,
                    .pointer_index = smith.value(u16),
                };
                try protocol.CapDescriptor.writeReceiverAnswer(desc._builder, smith.valueRangeAtMost(u32, 0, 8), ops[0..n]);
            },
        }
    }
}

/// Build a well-formed but adversarial Call: a hostile target (importedCap or
/// a PromisedAnswer with a fuzzed transform-op path), fuzzed send-results-to
/// routing, a fuzzed cap table, and — for cap-bearing payloads — a params
/// content capability pointer at a (possibly out-of-range) cap-table index, so
/// forward/pipeline cap remapping is exercised.
fn buildHostileCall(builder: *protocol.MessageBuilder, smith: *std.testing.Smith) !void {
    var call = try builder.beginCall(
        smith.valueRangeAtMost(u32, 0, 8),
        smith.value(u64),
        smith.value(u16),
    );

    if (smith.boolWeighted(1, 1)) {
        try call.setTargetImportedCap(smith.valueRangeAtMost(u32, 0, 8));
    } else {
        var ops: [8]protocol.PromisedAnswerOp = undefined;
        const n: usize = smith.valueRangeAtMost(u8, 0, ops.len);
        for (ops[0..n]) |*op| op.* = .{
            .tag = if (smith.boolWeighted(1, 1)) .getPointerField else .noop,
            .pointer_index = smith.value(u16),
        };
        try call.setTargetPromisedAnswerWithOps(smith.valueRangeAtMost(u32, 0, 8), ops[0..n]);
    }

    switch (smith.valueRangeAtMost(u8, 0, 2)) {
        0 => call.setSendResultsToCaller(),
        1 => call.setSendResultsToYourself(),
        else => try call.setSendResultsToThirdPartyNull(),
    }

    const cap_count = smith.valueRangeAtMost(u32, 0, 6);
    var payload = try call.payloadTyped();
    if (cap_count > 0 and smith.boolWeighted(1, 1)) {
        var content = try payload.initContent();
        // May index one past the end to fuzz the bounds check.
        try content.setCapability(.{ .id = smith.valueRangeAtMost(u32, 0, cap_count) });
    }
    try fuzzCapTable(try payload.initCapTable(cap_count), cap_count, smith);
}

/// Build an adversarial Return: any discriminant, and for `.results` a fuzzed
/// cap table (drives cap decoding on the answer path plus PromisedAnswer
/// resolution against whatever the peer has cached).
fn buildHostileReturn(builder: *protocol.MessageBuilder, smith: *std.testing.Smith) !void {
    const tags = [_]protocol.ReturnTag{
        .results, .exception, .canceled, .resultsSentElsewhere, .takeFromOtherQuestion, .awaitFromThirdParty,
    };
    const tag = tags[smith.index(tags.len)];
    var ret = try builder.beginReturn(smith.valueRangeAtMost(u32, 0, 8), tag);
    if (tag == .results) {
        const cap_count = smith.valueRangeAtMost(u32, 0, 6);
        try fuzzCapTable(try ret.initCapTableTyped(cap_count), cap_count, smith);
    }
}

/// STRUCTURE-AWARE peer dispatch fuzzing. Unlike `fuzzPeerHandleFrame` (random
/// bytes, which almost always fail at decode), this feeds a sequence of
/// well-formed Call/Return frames with hostile cap tables and transform paths,
/// so the fuzzer reaches the actual dispatch: cap-descriptor resolution, call
/// target routing, PromisedAnswer transform walking, forward/pipeline cap
/// remapping, and answer/return lifecycle. A real export handler resolves
/// answers so pipelined targets have a payload to walk. Property: no crash,
/// hang, or leak across the sequence, whatever the peer accepts or rejects.
fn fuzzPeerStructuredFrame(_: void, smith: *std.testing.Smith) anyerror!void {
    const H = struct {
        fn onCall(_: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
            // Resolve the answer so a later pipelined Call targeting this
            // question walks its transform against a real results payload.
            peer.sendReturnEmptyStruct(call.question_id) catch {};
        }
        fn onFrame(_: *anyopaque, _: []const u8) anyerror!void {}
    };

    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();
    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, H.onFrame);

    // A live export (so importedCap targets dispatch to a real handler) plus a
    // couple of imports (so receiverHosted/import ids hit live entries).
    _ = peer.addExport(.{ .ctx = &sink, .on_call = H.onCall }) catch {};
    peer.caps.noteImport(2) catch {};
    peer.caps.noteImport(3) catch {};

    var frames: u8 = 0;
    while (frames < 32 and !smith.eosWeightedSimple(3, 1)) : (frames += 1) {
        var builder = protocol.MessageBuilder.init(std.testing.allocator);
        defer builder.deinit();
        if (smith.boolWeighted(1, 1))
            buildHostileCall(&builder, smith) catch continue
        else
            buildHostileReturn(&builder, smith) catch continue;
        const frame = builder.finish() catch continue;
        defer std.testing.allocator.free(frame);
        peer.handleFrame(frame) catch {};
    }
}

/// The compiler plugin's untrusted-input surface: a hostile capnp toolchain
/// (or a crafted CodeGeneratorRequest piped to stdin) must never crash, hang,
/// leak, or blow the codegen budget. Parse random bytes as a request and run
/// the generator over every requested file with tight caps.
fn fuzzCodeGeneratorRequest(_: void, smith: *std.testing.Smith) anyerror!void {
    var buf: [max_fuzz_input]u8 = undefined;
    const len = smith.slice(&buf);
    const bytes = buf[0..len];

    const request = request_reader.parseCodeGeneratorRequest(std.testing.allocator, bytes) catch return;
    defer request_reader.freeCodeGeneratorRequest(std.testing.allocator, request);

    var generator = Generator.init(std.testing.allocator, request.nodes) catch return;
    defer generator.deinit();
    // Tight budget so a hostile schema cannot amplify a tiny input into
    // unbounded generation work/output during fuzzing.
    generator.setCodegenBudget(.{
        .max_nodes = 1024,
        .max_imports = 1024,
        .max_fields = 4096,
        .max_name_bytes = 256 * 1024,
        .max_default_bytes = 256 * 1024,
        .max_manifest_bytes = 256 * 1024,
        .max_output_bytes = 1024 * 1024,
    });

    for (request.requested_files) |requested_file| {
        const output = generator.generateFile(requested_file) catch continue;
        std.testing.allocator.free(output);
    }
}

test "fuzz: Message.init validated parse" {
    try std.testing.fuzz({}, fuzzMessageInit, .{});
}

test "fuzz: CodeGeneratorRequest parse + generate" {
    try std.testing.fuzz({}, fuzzCodeGeneratorRequest, .{});
}

test "fuzz: packed decode" {
    try std.testing.fuzz({}, fuzzPackedDecode, .{});
}

test "fuzz: stream framer chunking" {
    try std.testing.fuzz({}, fuzzFramer, .{});
}

test "fuzz: peer frame dispatch" {
    try std.testing.fuzz({}, fuzzPeerHandleFrame, .{});
}

test "fuzz: peer structured frame dispatch" {
    try std.testing.fuzz({}, fuzzPeerStructuredFrame, .{});
}
