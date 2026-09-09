#!/usr/bin/env python3
"""Measure the same pinned schemas and consumer against two runtime source trees.

Run from the repository root with `mise exec -- python3 ...`. The baseline tree
must contain a pristine src/ at the comparison revision. This script never
writes to either source tree or to vendor/; artifacts belong under .zig-cache/.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import statistics
import subprocess
import sys
import time


def digest_tree(root):
    digest = hashlib.sha256()
    for path in sorted((root / "src").rglob("*")):
        if path.is_file():
            digest.update(path.relative_to(root).as_posix().encode() + b"\0")
            digest.update(path.read_bytes())
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-root", type=Path, required=True)
    parser.add_argument("--baseline-revision", default="68ad72f")
    parser.add_argument("--output", type=Path, default=Path(".zig-cache/parity-sprint/performance"))
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.repeats < 2:
        parser.error("at least two repeated samples are required")
    current = Path.cwd().resolve()
    baseline = args.baseline_root.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    zig = str(Path(shutil.which("zig")).resolve())
    capnp = str(Path(shutil.which("capnp")).resolve())
    commands = []

    def run(command, cwd=current, data=None):
        start = time.perf_counter_ns()
        cpu_before = resource.getrusage(resource.RUSAGE_CHILDREN)
        result = subprocess.run(command, cwd=cwd, input=data, capture_output=True)
        elapsed = time.perf_counter_ns() - start
        cpu_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        cpu_ns = int((cpu_after.ru_utime + cpu_after.ru_stime - cpu_before.ru_utime - cpu_before.ru_stime) * 1e9)
        record = {"argv": list(map(str, command)), "cwd": str(cwd), "elapsed_ns": elapsed, "cpu_ns": cpu_ns, "exit_code": result.returncode}
        if result.stderr:
            record["stderr"] = result.stderr.decode(errors="replace")
        commands.append(record)
        (output / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
        if result.returncode:
            sys.stderr.write(result.stderr.decode(errors="replace"))
            raise RuntimeError(f"command failed ({result.returncode}): {command}")
        return result.stdout, elapsed

    def version(command):
        return run(command)[0].decode().strip()

    roots = {"baseline": baseline, "current": current}
    initial_digests = {name: digest_tree(root) for name, root in roots.items()}
    metadata = {
        "baseline_revision": args.baseline_revision,
        "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "current_base_revision": version(["git", "rev-parse", "HEAD"]),
        "host": platform.platform(), "machine": platform.machine(),
        "optimize": "ReleaseSafe", "strip": False,
        "python": sys.version, "zig": version([zig, "version"]),
        "capnp": version([capnp, "--version"]),
        "source_digests": initial_digests,
        "benchmark_source_sha256": hashlib.sha256((current / "tests/reflection/performance.zig").read_bytes()).hexdigest(),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "fixture_request_sha256": hashlib.sha256((current / "tests/reflection/request.bin").read_bytes()).hexdigest(),
        "reference_gitlinks": version(["git", "submodule", "status", "vendor/ext/capnproto"]),
        "timing_contract": "first process invocation and median repeated process invocations; no OS cache purge. First consumer compile uses a new local cache, shared default global Zig cache; warm compile repeats the same command. Runtime samples alternate baseline/current."
    }
    if platform.system() == "Darwin":
        metadata["hardware"] = {key: version(["sysctl", "-n", key]) for key in
                                ("machdep.cpu.brand_string", "hw.physicalcpu", "hw.logicalcpu", "hw.memsize")}
    schemas = {
        "small": {"source": "tests/reflection/schemas/helper-names.capnp", "prefix": "tests/reflection/schemas", "include": "tests/reflection/schemas/include", "generated": "helper-names.zig", "type": "Payload", "setter": "setNumber", "getter": "getNumber"},
        "large": {"source": "vendor/ext/capnproto/c++/src/capnp/test.capnp", "prefix": "vendor/ext/capnproto/c++/src", "include": "vendor/ext/capnproto/c++/src", "generated": "capnp/test.zig", "type": "TestAllTypes", "setter": "setInt64Field", "getter": "getInt64Field"},
    }
    requests = {}
    for name, schema in schemas.items():
        request, _ = run([capnp, "compile", "-o-", "--src-prefix=" + schema["prefix"], "-I" + schema["include"], schema["source"]])
        (output / f"{name}-request.bin").write_bytes(request)
        requests[name] = request
        schema["request_bytes"] = len(request)
        schema["request_sha256"] = hashlib.sha256(request).hexdigest()
    metadata["schemas"] = schemas
    results = []
    runtime_exes = {}
    for version_name, runtime_root in roots.items():
        directory = output / version_name
        directory.mkdir(exist_ok=True)
        generator = directory / "capnpc-zig"
        run([zig, "build-exe", "-OReleaseSafe", str(runtime_root / "src/main.zig"), "-femit-bin=" + str(generator)])
        for schema_name, schema in schemas.items():
            for profile in ("full", "compact"):
                for reflection in (True, False):
                    config = directory / f"{schema_name}-{profile}-{'reflection' if reflection else 'plain'}"
                    config.mkdir(exist_ok=True)
                    generated_dir = config / "generated"
                    generated_dir.mkdir(exist_ok=True)
                    command = [str(generator), "--api-profile=" + profile, "--reflection" if reflection else "--no-reflection"]
                    _, first_generation = run(command, generated_dir, requests[schema_name])
                    repeated = [run(command, generated_dir, requests[schema_name])[1] for _ in range(args.repeats)]
                    source_bytes = sum(path.stat().st_size for path in generated_dir.rglob("*.zig"))
                    # Both profiles support explicit Reader/Builder wire wrappers.
                    # Reflection-on intentionally loads the embedded registry so
                    # binary-size differences measure linked reflection support.
                    consumer = config / "consumer.zig"
                    consumer.write_text('''const std = @import("std");
const capnp = @import("capnpc-zig");
const T = @import("generated").TYPE;
pub fn main(init: std.process.Init) !void {
    var arena = capnp.message.MessageBuilder.init(init.gpa);
    defer arena.deinit();
    var value = T.Builder{ ._builder = try arena.allocateStruct(64, 64) };
    try value.SETTER(123);
    const bytes = try arena.toBytes();
    defer init.gpa.free(bytes);
    var decoded = try capnp.message.Message.init(init.gpa, bytes, .{});
    defer decoded.deinit();
    const reader = T.Reader{ ._reader = try decoded.getRootStruct() };
    if (try reader.GETTER() != 123) return error.WrongValue;
    if (@hasDecl(T, "capnpSchema")) {
        const registry = try T.capnpSchema.load(init.gpa);
        defer registry.deinit();
        if ((try T.capnpSchema.resolve(registry)).id() != T.capnpSchema.id) return error.WrongSchema;
    }
}
'''.replace("TYPE", schema["type"]).replace("SETTER", schema["setter"]).replace("GETTER", schema["getter"]))
                    executable = config / "consumer"
                    compile_command = [zig, "build-exe", "-OReleaseSafe", "--cache-dir", str(config / ("cache-" + str(time.time_ns()))), "--dep", "capnpc-zig", "--dep", "generated", "-Mroot=" + str(consumer), "--dep", "capnpc-zig", "-Mcapnpc-zig=" + str(runtime_root / "src/lib_core.zig"), "--dep", "capnpc-zig", "-Mgenerated=" + str(generated_dir / schema["generated"]), "-femit-bin=" + str(executable)]
                    _, first_compile = run(compile_command)
                    warm_compile = [run(compile_command)[1] for _ in range(args.repeats)]
                    run([str(executable)])
                    results.append({"version": version_name, "schema": schema_name, "profile": profile, "reflection": reflection, "generated_source_bytes": source_bytes, "binary_bytes": executable.stat().st_size, "generation_first_ns": first_generation, "generation_repeated_ns": repeated, "generation_repeated_median_ns": statistics.median(repeated), "compile_first_ns": first_compile, "compile_repeated_ns": warm_compile, "compile_repeated_median_ns": statistics.median(warm_compile)})
                    (output / "generation.json").write_text(json.dumps(results, indent=2) + "\n")
                    print(f"{version_name} {schema_name} {profile} reflection={reflection}: verified", flush=True)
        fixture_generator = directory / "fixture-generator"
        run([zig, "build-exe", "-OReleaseSafe", "--dep", "capnpc-zig", "-Mroot=" + str(current / "tests/reflection/generate.zig"), "--dep", "capnpc-zig", "-Mcapnpc-zig=" + str(runtime_root / "src/lib_core.zig"), "-femit-bin=" + str(fixture_generator)])
        bindings = directory / "fixtures"
        run([str(fixture_generator), str(bindings)])
        executable = directory / "runtime-performance"
        run([zig, "build-exe", "-OReleaseSafe", "--dep", "capnpc-zig", "--dep", "generated", "--dep", "alloc-counter", "-Mroot=" + str(current / "tests/reflection/performance.zig"), "--dep", "capnpc-zig", "-Mcapnpc-zig=" + str(runtime_root / "src/lib_core.zig"), "--dep", "capnpc-zig", "-Mgenerated=" + str(bindings / "root.zig"), "-Malloc-counter=" + str(current / "bench/alloc_counter.zig"), "-femit-bin=" + str(executable)])
        runtime_exes[version_name] = executable
    samples = []
    for repetition in range(args.repeats):
        for version_name, executable in runtime_exes.items():
            stdout, _ = run([str(executable)])
            for line in stdout.decode().splitlines():
                samples.append({"version": version_name, "repetition": repetition, **json.loads(line)})
    expected_checksums = {}
    for sample in samples:
        key = (sample["case"], sample["size"])
        if key in expected_checksums and sample["checksum"] != expected_checksums[key]:
            raise RuntimeError(f"runtime checksum mismatch: {key}")
        expected_checksums[key] = sample["checksum"]
    if len(results) != 16 or len(samples) != args.repeats * 20 or len(expected_checksums) != 10:
        raise RuntimeError("performance matrix coverage is incomplete")
    (output / "runtime.json").write_text(json.dumps(samples, indent=2) + "\n")
    metadata["final_source_digests"] = {name: digest_tree(root) for name, root in roots.items()}
    metadata["sources_unchanged_during_run"] = initial_digests == metadata["final_source_digests"] and metadata["benchmark_source_sha256"] == hashlib.sha256((current / "tests/reflection/performance.zig").read_bytes()).hexdigest()
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print("Performance receipts:", output, flush=True)
    if not metadata["sources_unchanged_during_run"]:
        print("Source tree changed during measurements; rerun after source freeze for final acceptance.", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
