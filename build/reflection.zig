//! Development reflection gates. Fixture generation always uses the host
//! generator; emitted bindings and runtime checks use the requested target.
const std = @import("std");
const helpers = @import("helpers.zig");

fn runtime(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) *std.Build.Module {
    const module = b.createModule(.{ .root_source_file = b.path("src/lib_core.zig"), .target = target, .optimize = optimize });
    module.addImport("capnpc-zig", module);
    return module;
}

fn bindings(b: *std.Build, source: std.Build.LazyPath, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, core: *std.Build.Module) *std.Build.Module {
    return b.createModule(.{ .root_source_file = source.path(b, "root.zig"), .target = target, .optimize = optimize, .imports = &.{.{ .name = "capnpc-zig", .module = core }} });
}

fn consumer(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, core: *std.Build.Module, generated: *std.Build.Module) *std.Build.Step.Compile {
    const exe = b.addExecutable(.{ .name = "reflection-consumer", .root_module = b.createModule(.{
        .root_source_file = b.path("tests/reflection/consumer.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{ .{ .name = "capnpc-zig", .module = core }, .{ .name = "generated", .module = generated } },
    }) });
    helpers.registered_test_compile_steps.append(b.allocator, &exe.step) catch @panic("OOM");
    return exe;
}

pub fn add(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, core: *std.Build.Module) *std.Build.Step {
    const host_core = runtime(b, b.graph.host, optimize);
    const generator = b.addExecutable(.{ .name = "reflection-generate", .root_module = b.createModule(.{
        .root_source_file = b.path("tests/reflection/generate.zig"),
        .target = b.graph.host,
        .optimize = optimize,
        .imports = &.{.{ .name = "capnpc-zig", .module = host_core }},
    }) });
    const generation = b.addRunArtifact(generator);
    const source = generation.addOutputDirectoryArg("generated");
    const generated = bindings(b, source, target, optimize, core);
    const executable = consumer(b, target, optimize, core, generated);
    const run = b.addRunArtifact(executable);
    const output = run.addOutputDirectoryArg("reflection-output");
    const registry = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = b.path("tests/reflection/registry_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "capnpc-zig", .module = core }},
    }), .filters = &.{"registry"} });
    helpers.registered_test_compile_steps.append(b.allocator, &registry.step) catch @panic("OOM");
    const helper_names = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = b.path("tests/reflection/helper_names_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{ .{ .name = "capnpc-zig", .module = core }, .{ .name = "generated", .module = generated } },
    }) });
    helpers.registered_test_compile_steps.append(b.allocator, &helper_names.step) catch @panic("OOM");
    const step = b.step("test-reflection", "Run generated reflection, schema ownership, helper-name and evolution regressions");
    step.dependOn(&run.step);
    step.dependOn(helpers.addLibTest(b, "tests/reflection/wire_test.zig", target, optimize, core));
    step.dependOn(&b.addRunArtifact(registry).step);
    step.dependOn(&b.addRunArtifact(helper_names).step);

    const wasm_target = b.resolveTargetQuery(.{ .cpu_arch = .wasm32, .os_tag = .wasi });
    const wasm_core = runtime(b, wasm_target, optimize);
    const wasm_generated = bindings(b, source, wasm_target, optimize, wasm_core);
    const wasm_exe = consumer(b, wasm_target, optimize, wasm_core, wasm_generated);
    const run_wasm = b.addSystemCommand(&.{ "wasmtime", "run" });
    run_wasm.addFileArg(wasm_exe.getEmittedBin());
    run_wasm.addArg("--no-files");
    const wasm_step = b.step("test-reflection-wasi", "Run reflection consumer in WASI (requires Wasmtime)");
    wasm_step.dependOn(&run_wasm.step);
    const wasm_registry = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = b.path("tests/reflection/registry_test.zig"),
        .target = wasm_target,
        .optimize = optimize,
        .imports = &.{.{ .name = "capnpc-zig", .module = wasm_core }},
    }), .filters = &.{"registry"} });
    helpers.registered_test_compile_steps.append(b.allocator, &wasm_registry.step) catch @panic("OOM");
    const run_wasm_registry = b.addSystemCommand(&.{ "wasmtime", "run" });
    run_wasm_registry.addFileArg(wasm_registry.getEmittedBin());
    wasm_step.dependOn(&run_wasm_registry.step);

    // This is an explicit optional gate: requesting it requires development
    // headers/libraries via pkg-config, rather than silently skipping C++.
    const cpp_builder = b.addExecutable(.{ .name = "reflection-cpp-build", .root_module = b.createModule(.{
        .root_source_file = b.path("tests/reflection/compile_cpp.zig"),
        .target = b.graph.host,
        .optimize = optimize,
    }) });
    const compile_cpp = b.addRunArtifact(cpp_builder);
    compile_cpp.addFileArg(b.path("tests/reflection/oracle.c++"));
    const oracle = compile_cpp.addOutputFileArg("reflection-cpp-oracle");
    const run_oracle = std.Build.Step.Run.create(b, "run C++ reflection oracle");
    run_oracle.addFileArg(oracle);
    run_oracle.addFileArg(b.path("tests/reflection/request.bin"));
    run_oracle.addFileArg(output.path(b, "schema.bin"));
    run_oracle.addFileArg(output.path(b, "values.bin"));
    run_oracle.addFileArg(output.path(b, "scalars.bin"));
    run_oracle.addDirectoryArg(output);
    const cpp_step = b.step("test-reflection-cpp", "Validate every descriptor and evolved message with C++ (requires pkg-config capnp/kj)");
    cpp_step.dependOn(&run_oracle.step);
    return step;
}
