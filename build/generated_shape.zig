//! Generated-shape gate (`generated-shape`, `check-generated-shape`).
//!
//! The corpus is a set of committed CodeGeneratorRequests under
//! tests/generated_shape/requests/ (`just gen` writes them; the git diff in
//! `just check-generated` keeps them current). The build runs the host plugin
//! on each request, once per profile that uses it, so no schema compiler is
//! needed here. tools/generated_shape.zig walks every generated file and
//! renders docs/generated-shape.txt (Stable) and
//! docs/generated-shape-experimental.txt.
//!
//! Each corpus entry also gets an analysis-only compile
//! (`generated-shape-<profile>-<request>`) that references every function of
//! the generated files, so code that does not compile fails under a step
//! named for its entry. The walker alone only analyzes signatures.

const std = @import("std");

pub const Profile = enum {
    full,
    compact,
    no_reflection,

    /// The first path segment of the profile's snapshot lines.
    pub fn label(profile: Profile) []const u8 {
        return switch (profile) {
            .full => "full",
            .compact => "compact",
            .no_reflection => "no-reflection",
        };
    }

    fn pluginArgs(profile: Profile) []const []const u8 {
        return switch (profile) {
            .full => &.{},
            .compact => &.{"--api-profile=compact"},
            .no_reflection => &.{"--no-reflection"},
        };
    }
};

/// One committed request: `tests/generated_shape/requests/<name>.request.bin`.
/// `files` are the stems of the files the plugin writes for it (one per
/// requested schema file), in the order the walk should list them.
pub const Request = struct {
    name: []const u8,
    files: []const []const u8,
    profiles: []const Profile = &.{.full},
};

/// The corpus. `just gen` (recipe `gen-shape-requests`) writes one request per
/// row; keep the two lists in step. The tool fails when a committed request
/// is missing here.
pub const requests = [_]Request{
    .{ .name = "addressbook", .files = &.{"addressbook"}, .profiles = &.{ .full, .no_reflection } },
    .{ .name = "kvstore", .files = &.{"kvstore"}, .profiles = &.{ .full, .compact, .no_reflection } },
    .{ .name = "pingpong", .files = &.{"pingpong"} },
    .{ .name = "persistent", .files = &.{"persistent"} },
    .{ .name = "rpc_inherited_paths", .files = &.{ "rpc_inherited_paths", "rpc_inherited_external" } },
    .{ .name = "inherited_method_collision", .files = &.{"inherited_method_collision"} },
    .{ .name = "streaming", .files = &.{"streaming"} },
    .{ .name = "rpc_pipeline_paths", .files = &.{"rpc_pipeline_paths"} },
    .{ .name = "nested_interfaces", .files = &.{"nested_interfaces"} },
    .{ .name = "generic_rpc", .files = &.{ "generic_rpc", "generic_rpc_external" }, .profiles = &.{ .full, .compact } },
    .{ .name = "enum_evolution_v1", .files = &.{"enum_evolution_v1"} },
    .{ .name = "union_member_guard_runtime", .files = &.{"union_member_guard_runtime"} },
    .{ .name = "defaults", .files = &.{"defaults"} },
    .{ .name = "nested_collisions", .files = &.{"nested_collisions"} },
    .{ .name = "nested_interface_collisions", .files = &.{"nested_interface_collisions"} },
    .{ .name = "zig_field_names", .files = &.{"zig_field_names"} },
    .{ .name = "runtime_guard_names", .files = &.{ "runtime_guard_names", "runtime_abi" } },
    .{ .name = "edge_codegen", .files = &.{"edge_codegen"} },
    .{ .name = "brand_application_edge_cases", .files = &.{"brand_application_edge_cases"} },
    .{ .name = "brand_cross_file", .files = &.{ "brand_cross_file", "brand_imported" } },
    .{ .name = "brand_list_specialization", .files = &.{"brand_list_specialization"} },
    .{ .name = "brand_pointer_fidelity", .files = &.{"brand_pointer_fidelity"} },
    .{ .name = "generic_collections", .files = &.{"generic_collections"} },
    .{ .name = "generic_recursive", .files = &.{"generic_recursive"} },
    .{ .name = "rpc_nested", .files = &.{"rpc_nested"} },
    .{ .name = "annotations", .files = &.{"annotations"} },
};

pub const Options = struct {
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    /// The capnpc-zig module the generated files import.
    lib_module: *std.Build.Module,
    snapshot_render_module: *std.Build.Module,
    /// The plugin, built for the host so cross-target builds can still run it.
    plugin: *std.Build.Step.Compile,
};

fn moduleName(b: *std.Build, profile: Profile, request: []const u8) []const u8 {
    return b.fmt("shape-{s}-{s}", .{ profile.label(), request });
}

pub fn add(b: *std.Build, options: Options) void {
    const instances_module = b.createModule(.{
        .root_source_file = b.path("tests/generated_shape/instances.zig"),
        .target = options.target,
        .optimize = options.optimize,
        .imports = &.{.{ .name = "capnpc-zig", .module = options.lib_module }},
    });

    var corpus_source: std.ArrayList(u8) = .empty;
    const w = struct {
        fn print(list: *std.ArrayList(u8), gpa: std.mem.Allocator, comptime fmt: []const u8, args: anytype) void {
            list.print(gpa, fmt, args) catch @panic("OOM");
        }
    }.print;
    w(&corpus_source, b.allocator,
        \\//! Written by build/generated_shape.zig: the corpus the walker reads.
        \\pub const Entry = struct {{ profile: []const u8, request: []const u8, root: type }};
        \\pub const instances = @import("shape-instances");
        \\pub const requests = [_][]const u8{{
        \\
    , .{});
    for (requests) |request| w(&corpus_source, b.allocator, "    \"{s}\",\n", .{request.name});
    w(&corpus_source, b.allocator, "}};\npub const entries = [_]Entry{{\n", .{});

    const corpus_files = b.addWriteFiles();
    const corpus_module = b.createModule(.{
        .target = options.target,
        .optimize = options.optimize,
    });
    corpus_module.addImport("shape-instances", instances_module);

    const check_step = b.step(
        "check-generated-shape",
        "Fail when the generated-code shape drifts from docs/generated-shape*.txt",
    );
    const write_step = b.step(
        "generated-shape",
        "Regenerate docs/generated-shape.txt and docs/generated-shape-experimental.txt",
    );

    for (requests) |request| {
        const request_path = b.path(b.fmt("tests/generated_shape/requests/{s}.request.bin", .{request.name}));
        for (request.profiles) |profile| {
            const run = b.addRunArtifact(options.plugin);
            run.setName(b.fmt("generate shape {s} {s}", .{ profile.label(), request.name }));
            run.setStdIn(.{ .lazy_path = request_path });
            run.addArgs(profile.pluginArgs());
            const generated = run.addPrefixedOutputDirectoryArg("--output-dir=", "capnp-gen");

            // The plugin's directory plus a root that names each file, so a
            // request that writes several files is one module.
            var root_source: std.ArrayList(u8) = .empty;
            for (request.files) |stem| {
                w(&root_source, b.allocator, "pub const {s} = @import(\"{s}.zig\");\n", .{ stem, stem });
            }
            const files = b.addWriteFiles();
            _ = files.addCopyDirectory(generated, "", .{});
            const root = files.add("shape_root.zig", root_source.items);
            const name = moduleName(b, profile, request.name);
            const module = b.createModule(.{
                .root_source_file = root,
                .target = options.target,
                .optimize = options.optimize,
                .imports = &.{.{ .name = "capnpc-zig", .module = options.lib_module }},
            });
            corpus_module.addImport(name, module);
            instances_module.addImport(name, module);
            w(&corpus_source, b.allocator, "    .{{ .profile = \"{s}\", .request = \"{s}\", .root = @import(\"{s}\") }},\n", .{ profile.label(), request.name, name });

            // Analysis only (nothing asks for the binary): every generated
            // function body must compile, and a failure names this entry.
            const analysis = b.addObject(.{
                .name = b.fmt("generated-shape-{s}-{s}", .{ profile.label(), request.name }),
                .root_module = b.createModule(.{
                    .root_source_file = b.path("tests/generated_shape/compile_check.zig"),
                    .target = options.target,
                    .optimize = options.optimize,
                    .imports = &.{.{ .name = "entry", .module = module }},
                }),
            });
            check_step.dependOn(&analysis.step);
            write_step.dependOn(&analysis.step);
        }
    }
    w(&corpus_source, b.allocator, "}};\n", .{});
    corpus_module.root_source_file = corpus_files.add("corpus.zig", corpus_source.items);

    // The instances' function bodies get the same analysis.
    const instances_analysis = b.addObject(.{
        .name = "generated-shape-instances",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/generated_shape/compile_check.zig"),
            .target = options.target,
            .optimize = options.optimize,
            .imports = &.{.{ .name = "entry", .module = instances_module }},
        }),
    });
    check_step.dependOn(&instances_analysis.step);
    write_step.dependOn(&instances_analysis.step);

    const tool = b.addExecutable(.{
        .name = "generated-shape",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/generated_shape.zig"),
            .target = options.target,
            .optimize = options.optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = options.lib_module },
                .{ .name = "snapshot-render", .module = options.snapshot_render_module },
                .{ .name = "generated-shape-corpus", .module = corpus_module },
            },
        }),
    });

    // Never cached: the tool reads and writes docs/generated-shape*.txt and
    // reads docs/api-snapshot.txt and the request directory, none of which
    // the build graph tracks (see `run_api_snapshot_write` in build_impl.zig).
    const run_write = b.addRunArtifact(tool);
    run_write.addArg("--write");
    // `zig build generated-shape -- --dump <file>` also lists every line
    // with its tier and kind, even when a gate fails.
    run_write.addPassthruArgs();
    run_write.setCwd(b.path("."));
    run_write.has_side_effects = true;
    write_step.dependOn(&run_write.step);

    const run_check = b.addRunArtifact(tool);
    run_check.addArg("--check");
    run_check.setCwd(b.path("."));
    run_check.has_side_effects = true;
    check_step.dependOn(&run_check.step);
}
