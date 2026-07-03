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
