//! docs/build-integration.md. Its canonical `build.zig` is the codegen
//! consumer's, tests/package_consumer/codegen/build.zig, byte for byte
//! (`zig build docs-smoke` enforces that), and package-preflight runs that
//! consumer from the filtered release archive.
//!
//! Here the same recipe runs against this checkout (see build/build_impl.zig):
//! the plugin reads the consumer's checked-in CodeGeneratorRequest on stdin,
//! writes through `--output-dir=` into a cached directory, and the consumer's
//! own code runs against the result. If the plugin flag, the request fixture
//! or the generated API shape breaks, this fails on every `zig build check`.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const codegen_consumer = @import("codegen_consumer");

const testing = std.testing;

test "pinned-plugin recipe: generated code compiles and round-trips against the runtime" {
    try codegen_consumer.exercise(testing.allocator);
}

test "generated modules can compile against the documented capnpc-zig import" {
    comptime {
        _ = capnpc.message.Message;
        _ = capnpc.message.MessageBuilder;
        _ = capnpc.message.StructReader;
        _ = capnpc.message.StructBuilder;
        _ = capnpc.schema.Node;
        _ = capnpc.reader;
        _ = capnpc.codegen.Generator;
        _ = capnpc.codegen.TypeGenerator;
        _ = capnpc.request.parseCodeGeneratorRequest;
        _ = capnpc.schema_validation;
        _ = capnpc.reflection.SchemaRef;
    }
}
