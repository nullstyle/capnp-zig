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

fn generatedTest(b: *std.Build, path: []const u8, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, core: *std.Build.Module, generated: *std.Build.Module) *std.Build.Step.Compile {
    const exe = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = b.path(path),
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
    const performance = b.addExecutable(.{ .name = "reflection-performance", .root_module = b.createModule(.{
        .root_source_file = b.path("tests/reflection/performance.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{ .{ .name = "capnpc-zig", .module = core }, .{ .name = "generated", .module = generated }, .{ .name = "alloc-counter", .module = b.createModule(.{ .root_source_file = b.path("bench/alloc_counter.zig"), .target = target, .optimize = optimize }) } },
    }) });
    b.step("bench-reflection", "Measure registry load and generated/dynamic read/copy costs").dependOn(&b.addRunArtifact(performance).step);
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
    const dynamic_failure = helpers.addLibTest(b, "tests/reflection/dynamic_failure_test.zig", target, optimize, core);
    step.dependOn(dynamic_failure);
    b.step("test-dynamic-failure", "Run dynamic mutation ownership and query regressions").dependOn(dynamic_failure);
    const copy_limits = helpers.addLibTest(b, "tests/reflection/copy_limits_test.zig", target, optimize, core);
    step.dependOn(copy_limits);
    b.step("test-copy-limits", "Run bounded copy work/allocation regressions").dependOn(copy_limits);
    const reflection_fuzz = generatedTest(b, "tests/reflection/fuzz_test.zig", target, optimize, core, generated);
    const reflection_fuzz_run = b.addRunArtifact(reflection_fuzz);
    step.dependOn(&reflection_fuzz_run.step);
    const fuzz_filter = b.option([]const u8, "reflection-fuzz-filter", "Select one reflection fuzz target");
    const selected_reflection_fuzz = generatedTest(b, "tests/reflection/fuzz_test.zig", target, optimize, core, generated);
    if (fuzz_filter) |filter| selected_reflection_fuzz.filters = b.allocator.dupe([]const u8, &.{filter}) catch @panic("OOM");
    b.step("test-fuzz-reflection", "Run reflection mutation and schema fuzz targets").dependOn(&b.addRunArtifact(selected_reflection_fuzz).step);
    step.dependOn(&run.step);
    step.dependOn(helpers.addLibTest(b, "tests/reflection/wire_test.zig", target, optimize, core));
    step.dependOn(&b.addRunArtifact(registry).step);
    step.dependOn(&b.addRunArtifact(helper_names).step);
    const generated_builder = generatedTest(b, "tests/reflection/generated_builder_test.zig", target, optimize, core, generated);
    const run_generated_builder = b.addRunArtifact(generated_builder);
    step.dependOn(&run_generated_builder.step);
    b.step("test-generated-builder", "Run generated Builder mutation, copying and reader views").dependOn(&run_generated_builder.step);
    const wire_validation = helpers.addLibTest(b, "tests/serialization/double_far_validation_test.zig", target, optimize, core);
    step.dependOn(wire_validation);
    b.step("test-wire-validation", "Run canonical double-far struct validation regressions").dependOn(wire_validation);
    const evolution = helpers.addLibTest(b, "tests/serialization/builder_evolution_test.zig", target, optimize, core);
    step.dependOn(evolution);
    b.step("test-builder-evolution", "Run evolved list mutation and strict Text regressions").dependOn(evolution);

    const wasm_target = b.resolveTargetQuery(.{ .cpu_arch = .wasm32, .os_tag = .wasi });
    const wasm_core = runtime(b, wasm_target, optimize);
    const wasm_generated = bindings(b, source, wasm_target, optimize, wasm_core);
    const wasm_exe = consumer(b, wasm_target, optimize, wasm_core, wasm_generated);
    const run_wasm = b.addSystemCommand(&.{ "wasmtime", "run" });
    run_wasm.addFileArg(wasm_exe.getEmittedBin());
    run_wasm.addArg("--no-files");
    const wasm_step = b.step("test-reflection-wasi", "Run reflection consumer in WASI (requires Wasmtime)");
    const wasm_dynamic_failure = generatedTest(b, "tests/reflection/dynamic_failure_test.zig", wasm_target, optimize, wasm_core, wasm_generated);
    const run_wasm_dynamic_failure = b.addSystemCommand(&.{ "wasmtime", "run" });
    run_wasm_dynamic_failure.addFileArg(wasm_dynamic_failure.getEmittedBin());
    wasm_step.dependOn(&run_wasm_dynamic_failure.step);
    b.step("test-dynamic-failure-wasi", "Run dynamic mutation ownership and queries in WASI").dependOn(&run_wasm_dynamic_failure.step);
    const wasm_copy_limits = generatedTest(b, "tests/reflection/copy_limits_test.zig", wasm_target, optimize, wasm_core, wasm_generated);
    const run_wasm_copy_limits = b.addSystemCommand(&.{ "wasmtime", "run" });
    run_wasm_copy_limits.addFileArg(wasm_copy_limits.getEmittedBin());
    wasm_step.dependOn(&run_wasm_copy_limits.step);
    b.step("test-copy-limits-wasi", "Run bounded copy work/allocation regressions in WASI").dependOn(&run_wasm_copy_limits.step);
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
    const wasm_builder = generatedTest(b, "tests/reflection/generated_builder_test.zig", wasm_target, optimize, wasm_core, wasm_generated);
    const run_wasm_builder = b.addSystemCommand(&.{ "wasmtime", "run" });
    run_wasm_builder.addFileArg(wasm_builder.getEmittedBin());
    wasm_step.dependOn(&run_wasm_builder.step);
    b.step("test-generated-builder-wasi", "Run generated Builder regressions in WASI").dependOn(&run_wasm_builder.step);
    const wasm_validation = generatedTest(b, "tests/serialization/double_far_validation_test.zig", wasm_target, optimize, wasm_core, wasm_generated);
    const run_wasm_validation = b.addSystemCommand(&.{ "wasmtime", "run" });
    run_wasm_validation.addFileArg(wasm_validation.getEmittedBin());
    wasm_step.dependOn(&run_wasm_validation.step);
    b.step("test-wire-validation-wasi", "Run double-far validation regressions in WASI").dependOn(&run_wasm_validation.step);
    const wasm_evolution = generatedTest(b, "tests/serialization/builder_evolution_test.zig", wasm_target, optimize, wasm_core, wasm_generated);
    const run_wasm_evolution = b.addSystemCommand(&.{ "wasmtime", "run" });
    run_wasm_evolution.addFileArg(wasm_evolution.getEmittedBin());
    wasm_step.dependOn(&run_wasm_evolution.step);
    b.step("test-builder-evolution-wasi", "Run evolved list mutation and strict Text in WASI").dependOn(&run_wasm_evolution.step);

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
    const oracle_ablation = std.Build.Step.Run.create(b, "reject injected C++ oracle mismatch");
    oracle_ablation.addFileArg(oracle);
    oracle_ablation.addFileArg(b.path("tests/reflection/request.bin"));
    oracle_ablation.addFileArg(output.path(b, "schema.bin"));
    oracle_ablation.addFileArg(output.path(b, "values.bin"));
    oracle_ablation.addFileArg(output.path(b, "scalars.bin"));
    oracle_ablation.addDirectoryArg(output);
    oracle_ablation.addArg("--inject-mismatch");
    oracle_ablation.expectExitCode(2);
    b.step("test-reflection-oracle-ablation", "Prove the C++ oracle rejects injected mismatches").dependOn(&oracle_ablation.step);
    return step;
}
