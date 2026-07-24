const std = @import("std");
const capnpc = @import("capnpc-zig");

const testing = std.testing;

const BuildIntegrationSnippet = struct {
    pub const dependency_name = "capnpc_zig";
    pub const core_module_name = "capnpc-zig-core";
    pub const runtime_import_name = "capnpc-zig";
    pub const generated_import_name = "addressbook";
    pub const app_root_path = "src/main.zig";
    pub const schema_path = "schema/addressbook.capnp";
    // `-ozig:gen` writes under the `gen/` output dir, preserving the schema
    // path: schema/addressbook.capnp -> gen/schema/addressbook.zig.
    pub const generated_root_path = "gen/schema/addressbook.zig";
    pub const codegen_output_arg = "-ozig:gen";
    pub const codegen_argv = [_][]const u8{
        "capnpc",
        codegen_output_arg,
        schema_path,
    };
};

const CapnpOutputArg = struct {
    language: []const u8,
    directory: []const u8,
};

fn parseCapnpOutputArg(arg: []const u8) !CapnpOutputArg {
    if (!std.mem.startsWith(u8, arg, "-o")) return error.InvalidCapnpOutputArg;

    const payload = arg[2..];
    const separator = std.mem.indexOfScalar(u8, payload, ':') orelse return error.InvalidCapnpOutputArg;
    if (separator == 0 or separator == payload.len - 1) return error.InvalidCapnpOutputArg;

    return .{
        .language = payload[0..separator],
        .directory = payload[separator + 1 ..],
    };
}

/// The generated path is the output dir (`-o<plugin>:<dir>`) joined with the
/// schema path, extension swapped `.capnp` -> `.zig`. For `-ozig:gen` and
/// `schema/addressbook.capnp` that is `gen/schema/addressbook.zig`.
fn pathFollowsCapnpToZigConvention(output_arg: []const u8, schema_path: []const u8, generated_path: []const u8) bool {
    const capnp_ext = ".capnp";
    const zig_ext = ".zig";

    if (!std.mem.endsWith(u8, schema_path, capnp_ext)) return false;

    // Output dir is the text after the `:` in `-o<plugin>:<dir>`.
    const colon = std.mem.lastIndexOfScalar(u8, output_arg, ':') orelse return false;
    const out_dir = output_arg[colon + 1 ..];
    if (out_dir.len == 0) return false;

    const stem = schema_path[0 .. schema_path.len - capnp_ext.len];
    if (!std.mem.startsWith(u8, generated_path, out_dir)) return false;
    var rest = generated_path[out_dir.len..];
    if (rest.len == 0 or rest[0] != '/') return false;
    rest = rest[1..];
    return rest.len == stem.len + zig_ext.len and
        std.mem.eql(u8, rest[0..stem.len], stem) and
        std.mem.eql(u8, rest[stem.len..], zig_ext);
}

test "build integration snippet keeps dependency module and import names current" {
    try testing.expectEqualStrings("capnpc_zig", BuildIntegrationSnippet.dependency_name);
    try testing.expectEqualStrings("capnpc-zig-core", BuildIntegrationSnippet.core_module_name);
    try testing.expectEqualStrings("capnpc-zig", BuildIntegrationSnippet.runtime_import_name);
    try testing.expectEqualStrings("addressbook", BuildIntegrationSnippet.generated_import_name);
    try testing.expectEqualStrings("src/main.zig", BuildIntegrationSnippet.app_root_path);
}

test "build integration snippet models capnp codegen command without running it" {
    try testing.expectEqual(@as(usize, 3), BuildIntegrationSnippet.codegen_argv.len);
    try testing.expectEqualStrings("capnpc", BuildIntegrationSnippet.codegen_argv[0]);
    try testing.expectEqualStrings("-ozig:gen", BuildIntegrationSnippet.codegen_argv[1]);
    try testing.expectEqualStrings("schema/addressbook.capnp", BuildIntegrationSnippet.codegen_argv[2]);

    const output = try parseCapnpOutputArg(BuildIntegrationSnippet.codegen_argv[1]);
    try testing.expectEqualStrings("zig", output.language);
    try testing.expectEqualStrings("gen", output.directory);
}

test "build integration snippet keeps generated path convention current" {
    try testing.expect(pathFollowsCapnpToZigConvention(
        BuildIntegrationSnippet.codegen_output_arg,
        BuildIntegrationSnippet.schema_path,
        BuildIntegrationSnippet.generated_root_path,
    ));
    try testing.expectEqualStrings("gen/schema/addressbook.zig", BuildIntegrationSnippet.generated_root_path);
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
    }
}
