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
const ProvisionIndex = capnpc.rpc.peer.ProvisionIndex;
const vat_join = capnpc.rpc.vat.join;
const quic_wire = capnpc.rpc.transport.quic;
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

/// Flat (table-less single-segment) validated decode plus reader probes —
/// `Message.initFlat` is an untrusted-input surface: downstream consensus
/// consumers decode peer-supplied canonical bytes with it directly.
fn fuzzFlatDecode(_: void, smith: *std.testing.Smith) anyerror!void {
    var buf: [max_fuzz_input]u8 = undefined;
    const len = smith.slice(&buf);
    const bytes = buf[0..len];

    var msg = message.Message.initFlat(std.testing.allocator, bytes, .{}) catch return;
    defer msg.deinit();
    const root = msg.getRootStruct() catch return;
    _ = root.readU64(0);
    _ = root.readText(0) catch {};
}

/// Warm sturdy-ref envelope decode ({ticket, NEW_TOKEN}): applications
/// persist these bytes and feed them back after restarts, so the decoder
/// is an untrusted-input surface. Any error is a correct rejection; a
/// successful decode must round-trip byte-identically.
fn fuzzWarmStateDecode(_: void, smith: *std.testing.Smith) anyerror!void {
    var buf: [max_fuzz_input]u8 = undefined;
    const len = smith.slice(&buf);
    const bytes = buf[0..len];

    const decoded = quic_wire.warm_state.decode(bytes) catch return;
    const re = try quic_wire.warm_state.encode(std.testing.allocator, decoded.ticket, decoded.token);
    defer std.testing.allocator.free(re);
    if (!std.mem.eql(u8, re, bytes)) return error.WarmStateNotCanonical;
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

/// A fuzzed MessageTarget: an importedCap at a small id (overlapping seeded
/// imports/exports) or a PromisedAnswer with a short transform path. Returned
/// by value; the PromisedAnswer variant carries a null transform list because
/// the builder path (`buildProvidePromisedAnswerWithOps`) takes ops directly —
/// here the value is only used for the importedCap arm of Provide/Join.
fn fuzzImportedTarget(smith: *std.testing.Smith) protocol.MessageTarget {
    return .{
        .tag = .importedCap,
        .imported_cap = smith.valueRangeAtMost(u32, 0, 8),
        .promised_answer = null,
    };
}

/// Optionally hand back a small, well-formed AnyPointer reader (the shared
/// fixture) or null — so recipient/provision/completion/key_part fields
/// exercise both the present and absent decode arms without fabricating wire
/// bytes per frame.
fn maybeAnyPointer(fixture: message.AnyPointerReader, smith: *std.testing.Smith) ?message.AnyPointerReader {
    return if (smith.boolWeighted(1, 2)) fixture else null;
}

/// Build a fuzzed embargo byte slice for Accept/Disembargo, bounded so hostile
/// inputs cannot balloon the provision index's attributable-byte budget.
fn fuzzEmbargoBytes(buf: []u8, smith: *std.testing.Smith) []const u8 {
    const n: usize = smith.valueRangeAtMost(u16, 0, @intCast(buf.len));
    for (buf[0..n]) |*b| b.* = smith.value(u8);
    return buf[0..n];
}

/// STRUCTURE-AWARE L3/L4 dispatch fuzzing. `fuzzPeerStructuredFrame` only emits
/// Call and Return, so the 3-party handoff / Join surface (Provide, Accept,
/// Join, ThirdPartyAnswer, Resolve, Disembargo, Release, Finish, Bootstrap,
/// Abort) was reachable only through random bytes that essentially never
/// decode. This target seeds the peer with the state those handlers gate on — a
/// live export, imports, an attached provision index (small limits so hostile
/// embargo/park bytes stay bounded), an attached loopback Join network, and a
/// well-formed Provide preamble so a matching Accept can hit the live-provision
/// path — then feeds a weighted mix of hostile frames across all message types.
/// Property: no crash, hang, or leak, and full state drain on deinit, whatever
/// the peer accepts or rejects.
fn fuzzPeerL3Frame(_: void, smith: *std.testing.Smith) anyerror!void {
    const H = struct {
        fn onCall(_: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
            peer.sendReturnEmptyStruct(call.question_id) catch {};
        }
        fn onFrame(_: *anyopaque, _: []const u8) anyerror!void {}
    };

    // A shared, well-formed AnyPointer fixture (a tiny struct's root) reused as
    // recipient/provision/completion/key_part. Built once; its backing message
    // outlives every frame that clones from it.
    var fixture_builder = protocol.MessageBuilder.init(std.testing.allocator);
    defer fixture_builder.deinit();
    {
        var seed_call = fixture_builder.beginCall(0, 0, 0) catch return;
        seed_call.setTargetImportedCap(0) catch return;
        seed_call.setSendResultsToCaller();
        _ = seed_call.payloadTyped() catch return;
    }
    const fixture_frame = fixture_builder.finish() catch return;
    defer std.testing.allocator.free(fixture_frame);
    var fixture_msg = message.Message.initUnvalidated(std.testing.allocator, fixture_frame) catch return;
    defer fixture_msg.deinit();
    const fixture = fixture_msg.getRootAnyPointer() catch return;

    // Small provision-index limits: a hostile Accept storm must not amplify a
    // tiny fuzz input into large parked-accept memory.
    var index = ProvisionIndex.init(std.testing.allocator, .{
        .max_provisions = 32,
        .max_provision_key_bytes = 4096,
        .max_parked_accepts = 32,
        .max_parked_accepts_per_peer = 16,
        .max_parked_accept_bytes = 8192,
        .max_parked_accept_bytes_per_peer = 4096,
    });
    index.disableThreadAffinity();
    defer index.deinit();

    var join_net = vat_join.LoopbackJoinNetwork(Peer).init(std.testing.allocator);
    defer join_net.deinit();

    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();
    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, H.onFrame);
    const export_id = peer.addExport(.{ .ctx = &sink, .on_call = H.onCall }) catch return;
    peer.caps.noteImport(2) catch {};
    peer.caps.noteImport(3) catch {};
    peer.attachProvisionIndex(&index) catch {};
    join_net.registerDirectPeer(&peer, &peer) catch {};
    peer.attachJoinNetwork(join_net.network()) catch {};

    // Deterministic Provide preamble (question 7, target = the live export) so
    // the peer holds at least one live provision for a fuzzed Accept to match.
    // The reachability self-check fails loudly if a future refactor closes the
    // gate (e.g. renames the provide bookkeeping or changes target resolution)
    // so this target cannot silently rot into "random bytes that never
    // provide". Seeded once, before the hostile loop.
    {
        var preamble = protocol.MessageBuilder.init(std.testing.allocator);
        defer preamble.deinit();
        preamble.buildProvide(7, .{ .tag = .importedCap, .imported_cap = export_id, .promised_answer = null }, fixture) catch return;
        const frame = preamble.finish() catch return;
        defer std.testing.allocator.free(frame);
        peer.handleFrame(frame) catch {};
    }
    std.debug.assert(peer.provides_by_question.count() > 0);

    var embargo_buf: [32]u8 = undefined;
    var frames: u8 = 0;
    while (frames < 48 and !smith.eosWeightedSimple(3, 1)) : (frames += 1) {
        var builder = protocol.MessageBuilder.init(std.testing.allocator);
        defer builder.deinit();

        const ok = switch (smith.valueRangeAtMost(u8, 0, 13)) {
            0 => builder.buildBootstrap(smith.valueRangeAtMost(u32, 0, 8)),
            1 => builder.buildFinish(smith.valueRangeAtMost(u32, 0, 8), smith.boolWeighted(1, 1), smith.boolWeighted(1, 3)),
            2 => builder.buildRelease(smith.valueRangeAtMost(u32, 0, 8), smith.value(u32)),
            3 => builder.buildResolveCap(smith.valueRangeAtMost(u32, 0, 8), .{
                .tag = .senderHosted,
                .id = smith.valueRangeAtMost(u32, 0, 8),
            }),
            4 => builder.buildResolveException(smith.valueRangeAtMost(u32, 0, 8), "fuzz"),
            5 => builder.buildDisembargoSenderLoopback(fuzzImportedTarget(smith), smith.value(u32)),
            6 => builder.buildDisembargoReceiverLoopback(fuzzImportedTarget(smith), smith.value(u32)),
            7 => builder.buildDisembargoAccept(fuzzImportedTarget(smith), fuzzEmbargoBytes(&embargo_buf, smith)),
            8 => builder.buildProvide(smith.valueRangeAtMost(u32, 0, 8), fuzzImportedTarget(smith), maybeAnyPointer(fixture, smith)),
            9 => builder.buildAccept(
                smith.valueRangeAtMost(u32, 0, 8),
                maybeAnyPointer(fixture, smith),
                if (smith.boolWeighted(1, 1)) fuzzEmbargoBytes(&embargo_buf, smith) else null,
            ),
            10 => builder.buildJoin(smith.valueRangeAtMost(u32, 0, 8), fuzzImportedTarget(smith), maybeAnyPointer(fixture, smith)),
            11 => builder.buildThirdPartyAnswer(smith.valueRangeAtMost(u32, 0, 8), maybeAnyPointer(fixture, smith)),
            else => builder.buildAbortTyped("fuzz", .failed),
        };
        ok catch continue;

        const frame = builder.finish() catch continue;
        defer std.testing.allocator.free(frame);
        peer.handleFrame(frame) catch {};
    }
}

/// QUIC length-delimited framer at arbitrary chunk boundaries: like `fuzzFramer`
/// for the TCP stream framer, but for the QUIC transport's frame codec, which
/// is never reached by the default `test-fuzz` root otherwise (the `-Dquic=true`
/// build swaps the library root, and `test-fuzz` runs without it). The framer
/// itself is std-only and re-exported under both roots, so it fuzzes here with
/// no QUIC dependency.
fn fuzzQuicLengthFramer(_: void, smith: *std.testing.Smith) anyerror!void {
    var framer = quic_wire.LengthDelimitedFramer.initWithOptions(std.testing.allocator, .{
        .max_message_bytes = 4096,
        .max_buffered_bytes = 16 * 1024,
    });
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

/// QUIC native control framer: decodes the hello/inline_rpc/data_rpc control
/// stream. A weighted arm prefixes the valid preface so a share of inputs get
/// past the version gate into real frame parsing instead of bouncing at the
/// preface check.
fn fuzzQuicNativeControlFramer(_: void, smith: *std.testing.Smith) anyerror!void {
    var framer = quic_wire.NativeControlFramer.init(std.testing.allocator, .{
        .max_control_frame_bytes = 4096,
        .max_rpc_frame_bytes = 4096,
        .max_buffered_bytes = 16 * 1024,
    });
    defer framer.deinit();

    if (smith.boolWeighted(1, 1)) {
        framer.push(quic_wire.native.preface) catch return;
    }

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
            if (frame) |f| {
                f.deinit(std.testing.allocator);
            } else break;
        }
    }
}

const persistence = capnpc.rpc.peer.persistence;

/// Restore/sturdy-ref decode fuzzing. Sturdy-ref bytes are attacker-controlled
/// by definition (a remote peer names whatever it likes in `restore`), and the
/// restore path — payload decode via readSturdyRefParam, the restorer hook, and
/// RestoreOutcome handling (unknown / existing / host) — was reachable only
/// through random bytes that essentially never hit the restorer interface id.
/// This seeds a bootstrap export plus a restorer hook whose outcome is driven
/// by the fuzzed ref, then feeds restore Calls with both well-formed and
/// malformed payloads. Property: no crash, hang, or leak; full state drain.
fn fuzzPersistenceRestore(_: void, smith: *std.testing.Smith) anyerror!void {
    const Restorer = struct {
        existing_id: u32,
        fn onRestore(ctx: *anyopaque, _: *Peer, sturdy_ref: []const u8) anyerror!capnpc.rpc.peer.RestoreOutcome {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            // Drive every RestoreOutcome arm off the fuzzed ref's first byte so
            // the existing/host/unknown handling all gets exercised.
            if (sturdy_ref.len == 0) return .unknown;
            return switch (sturdy_ref[0] & 0x3) {
                0 => .unknown,
                1 => .{ .existing = self.existing_id },
                else => .{ .host = .{ .ctx = self, .on_call = struct {
                    fn onCall(_: *anyopaque, p: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
                        p.sendReturnEmptyStruct(call.question_id) catch {};
                    }
                }.onCall } },
            };
        }
        fn onFrame(_: *anyopaque, _: []const u8) anyerror!void {}
        fn onCall(_: *anyopaque, p: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
            p.sendReturnEmptyStruct(call.question_id) catch {};
        }
    };

    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();
    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, Restorer.onFrame);

    const bootstrap_id = peer.setBootstrap(.{ .ctx = &sink, .on_call = Restorer.onCall }) catch return;
    // A second live export id for the `.existing` outcome to re-expose.
    const existing_id = peer.addExport(.{ .ctx = &sink, .on_call = Restorer.onCall }) catch return;
    var restorer = Restorer{ .existing_id = existing_id };
    peer.setRestorer(&restorer, Restorer.onRestore) catch return;

    var ref_buf: [256]u8 = undefined;
    var frames: u8 = 0;
    while (frames < 32 and !smith.eosWeightedSimple(3, 1)) : (frames += 1) {
        var builder = protocol.MessageBuilder.init(std.testing.allocator);
        defer builder.deinit();

        const qid = smith.valueRangeAtMost(u32, 0, 8);
        // Target the bootstrap export most of the time (the only one that
        // routes to the restorer), a wrong id occasionally.
        const target: u32 = if (smith.boolWeighted(4, 1)) bootstrap_id else smith.valueRangeAtMost(u32, 0, 8);
        var call = builder.beginCall(qid, persistence.restorer_interface_id, persistence.restore_method_id) catch continue;
        call.setTargetImportedCap(target) catch continue;

        switch (smith.valueRangeAtMost(u8, 0, 3)) {
            0 => {
                // Well-formed Data of fuzzed length/content.
                const n: usize = smith.valueRangeAtMost(u16, 0, @intCast(ref_buf.len));
                for (ref_buf[0..n]) |*b| b.* = smith.value(u8);
                persistence.writeRestoreParams(&call, ref_buf[0..n]) catch continue;
            },
            1 => {
                // Malformed: content struct present but the sturdyRef pointer
                // is left null (MissingSturdyRef path).
                var payload = call.payloadTyped() catch continue;
                const any = payload.initContent() catch continue;
                _ = any.initStruct(0, 1) catch continue;
            },
            else => {
                // Malformed: no content at all.
                _ = call.payloadTyped() catch continue;
            },
        }
        _ = call.initCapTableTyped(0) catch continue;

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

test "fuzz: flat (table-less) validated decode" {
    try std.testing.fuzz({}, fuzzFlatDecode, .{});
}

test "fuzz: warm sturdy-ref envelope decode" {
    try std.testing.fuzz({}, fuzzWarmStateDecode, .{});
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

test "fuzz: peer L3/L4 frame dispatch" {
    try std.testing.fuzz({}, fuzzPeerL3Frame, .{});
}

test "fuzz: quic length-delimited framer chunking" {
    try std.testing.fuzz({}, fuzzQuicLengthFramer, .{});
}

test "fuzz: quic native control framer chunking" {
    try std.testing.fuzz({}, fuzzQuicNativeControlFramer, .{});
}

test "fuzz: persistence restore / sturdy-ref decode" {
    try std.testing.fuzz({}, fuzzPersistenceRestore, .{});
}
