//! Finite fuzz runs with positive, target-specific evidence. Exit code and wall
//! time alone cannot establish that any target executed.
const std = @import("std");
const builtin = @import("builtin");
pub const Report = struct { before: u64, after: u64, iterations: u64 };

pub fn validateReport(output: []const u8, expected: []const u8, minimum: u64, process_succeeded: bool) !Report {
    if (!process_succeeded) return error.FuzzCommandFailed;
    var reports: usize = 0;
    var target: ?[]const u8 = null;
    var before: ?u64 = null;
    var after: ?u64 = null;
    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \r\t");
        if (std.mem.eql(u8, line, "======= FUZZING REPORT =======")) {
            reports += 1;
            if (reports > 1) return error.MultipleFuzzReports;
        } else if (std.mem.startsWith(u8, line, "Fuzz test: \"")) {
            if (reports != 1 or target != null) return error.MalformedFuzzReport;
            const rest = line["Fuzz test: \"".len..];
            const end = std.mem.indexOfScalar(u8, rest, '"') orelse return error.MalformedFuzzReport;
            target = rest[0..end];
        } else if (std.mem.startsWith(u8, line, "Runs: ")) {
            if (reports != 1 or before != null) return error.MalformedFuzzReport;
            const values = line["Runs: ".len..];
            const arrow = std.mem.indexOf(u8, values, " -> ") orelse return error.MalformedFuzzReport;
            before = std.fmt.parseInt(u64, values[0..arrow], 10) catch return error.MalformedFuzzReport;
            after = std.fmt.parseInt(u64, values[arrow + 4 ..], 10) catch return error.MalformedFuzzReport;
        }
    }
    if (reports == 0) return error.MissingFuzzReport;
    const name = target orelse return error.MalformedFuzzReport;
    if (!std.mem.eql(u8, name, expected)) return error.WrongFuzzTarget;
    const start = before orelse return error.MalformedFuzzReport;
    const end = after orelse return error.MalformedFuzzReport;
    if (end < start) return error.MalformedFuzzReport;
    const iterations = end - start;
    if (iterations == 0 or iterations < minimum) return error.InsufficientFuzzActivity;
    return .{ .before = start, .after = end, .iterations = iterations };
}
const fixture =
    \\======= FUZZING REPORT =======
    \\Step: run test
    \\Fuzz test: "fixture.test.fuzz: mutation" (abcdef01)
    \\Runs: 100 -> 125
    \\Unique runs: 20 -> 23
    \\==============================
;
test "fuzz evidence accepts only measured activity for the requested target" {
    const report = try validateReport(fixture, "fixture.test.fuzz: mutation", 25, true);
    try std.testing.expectEqual(@as(u64, 25), report.iterations);
}
test "fuzz evidence rejects no activity and missing reports" {
    try std.testing.expectError(error.MissingFuzzReport, validateReport("compiling for 10 minutes", "fixture.test.fuzz: mutation", 1, true));
    try std.testing.expectError(error.InsufficientFuzzActivity, validateReport(fixture, "fixture.test.fuzz: mutation", 26, true));
    try std.testing.expectError(error.InsufficientFuzzActivity, validateReport("======= FUZZING REPORT =======\nFuzz test: \"fixture.test.fuzz: mutation\" (1234)\nRuns: 125 -> 125\n", "fixture.test.fuzz: mutation", 1, true));
}
test "fuzz evidence rejects oracle mismatch wrong target failure and ambiguous reports" {
    try std.testing.expectError(error.WrongFuzzTarget, validateReport(fixture, "fixture.test.fuzz: other", 1, true));
    try std.testing.expectError(error.FuzzCommandFailed, validateReport(fixture, "fixture.test.fuzz: mutation", 1, false));
    try std.testing.expectError(error.FuzzCommandFailed, validateReport(fixture ++ "\nerror: oracle mismatch", "fixture.test.fuzz: mutation", 1, false));
    try std.testing.expectError(error.MultipleFuzzReports, validateReport(fixture ++ "\n" ++ fixture, "fixture.test.fuzz: mutation", 1, true));
}

const Suite = struct { source: []const u8, step: []const u8, filter: []const u8 };
const suites = [_]Suite{
    .{ .source = "tests/fuzz/fuzz_targets.zig", .step = "test-fuzz-target", .filter = "fuzz-filter" },
    .{ .source = "tests/reflection/fuzz_test.zig", .step = "test-fuzz-reflection", .filter = "reflection-fuzz-filter" },
    .{ .source = "tests/fuzz/generated_rpc_test.zig", .step = "test-fuzz-generated-rpc", .filter = "generated-rpc-fuzz-filter" },
};
const Target = struct {
    suite: Suite,
    name: []const u8,
    report_name: []const u8,
    source_sha256: []const u8,
};
fn targets(allocator: std.mem.Allocator, io: std.Io, filter: ?[]const u8) ![]const Target {
    var result = std.ArrayList(Target).empty;
    for (suites) |suite| {
        const source = try std.Io.Dir.cwd().readFileAlloc(io, suite.source, allocator, .limited(8 * 1024 * 1024));
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(source, &digest, .{});
        const hash = try allocator.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
        const filename = std.fs.path.basename(suite.source);
        const module = filename[0 .. filename.len - ".zig".len];
        var lines = std.mem.splitScalar(u8, source, '\n');
        while (lines.next()) |line| {
            const prefix = "test \"fuzz: ";
            if (!std.mem.startsWith(u8, line, prefix)) continue;
            const rest = line["test \"".len..];
            const end = std.mem.indexOfScalar(u8, rest, '"') orelse return error.MalformedFuzzDeclaration;
            const name = rest[0..end];
            if (filter) |selected| if (std.mem.indexOf(u8, name, selected) == null) continue;
            const report_name = try std.fmt.allocPrint(allocator, "{s}.test.{s}", .{ module, name });
            try result.append(allocator, .{ .suite = suite, .name = name, .report_name = report_name, .source_sha256 = hash });
        }
    }
    if (result.items.len == 0) return error.NoMatchingFuzzTargets;
    return result.toOwnedSlice(allocator);
}
fn writeFile(io: std.Io, path: []const u8, bytes: []const u8) !void {
    var file = try std.Io.Dir.cwd().createFile(io, path, .{});
    defer file.close(io);
    try file.writeStreamingAll(io, bytes);
}
fn writeJson(allocator: std.mem.Allocator, io: std.Io, path: []const u8, value: anytype) !void {
    const json = try std.json.Stringify.valueAlloc(allocator, value, .{ .whitespace = .indent_2 });
    try writeFile(io, path, json);
}
fn metadataCommand(allocator: std.mem.Allocator, io: std.Io, argv: []const []const u8) ![]const u8 {
    const result = try std.process.run(allocator, io, .{
        .argv = argv,
        .stdout_limit = .limited(1024 * 1024),
        .stderr_limit = .limited(1024 * 1024),
        .timeout = .{ .deadline = std.Io.Clock.Timestamp.fromNow(io, .{ .raw = std.Io.Duration.fromSeconds(30), .clock = .awake }) },
    });
    if (result.term != .exited or result.term.exited != 0) return error.MetadataCommandFailed;
    return std.mem.trim(u8, result.stdout, " \r\n");
}
const Receipt = struct {
    source: []const u8,
    source_sha256: []const u8,
    target: []const u8,
    report_name: []const u8,
    argv: []const []const u8,
    requested_iterations: u64,
    seed: u32,
    elapsed_ms: i64,
    status: enum { passed, failed },
    failure: ?[]const u8,
    report: ?Report,
    process_term: ?std.process.Child.Term,
    stdout_path: []const u8,
    stderr_path: []const u8,
    crash_input_path: ?[]const u8 = null,
    corpus_path: []const u8 = ".zig-cache/f",
    coverage_path: []const u8 = ".zig-cache/v",
};
pub fn main(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(init.gpa);
    defer arena.deinit();
    const allocator = arena.allocator();
    const io = init.io;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    _ = args.next();
    var runs: u64 = 10000;
    var seed: u32 = 0x6ca9b3d1;
    var filter: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.startsWith(u8, arg, "--runs=")) {
            runs = try std.fmt.parseInt(u64, arg["--runs=".len..], 10);
            if (runs == 0) return error.PositiveRunCountRequired;
        } else if (std.mem.startsWith(u8, arg, "--filter=")) {
            filter = arg["--filter=".len..];
        } else if (std.mem.startsWith(u8, arg, "--seed=")) {
            seed = try std.fmt.parseInt(u32, arg["--seed=".len..], 0);
        } else return error.UnknownArgument;
    }
    const selected = try targets(allocator, io, filter);
    const revision = try metadataCommand(allocator, io, &.{ "git", "rev-parse", "HEAD" });
    const status = try metadataCommand(allocator, io, &.{ "git", "status", "--porcelain=v1", "--untracked-files=normal" });
    const zig = try metadataCommand(allocator, io, &.{ "zig", "version" });
    const started = std.Io.Timestamp.now(io, .real).toNanoseconds();
    const directory = try std.fmt.allocPrint(allocator, ".zig-cache/fuzz-evidence/run-{d}", .{started});
    try std.Io.Dir.cwd().createDirPath(io, directory);
    const manifest = try std.fs.path.join(allocator, &.{ directory, "manifest.json" });
    try writeJson(allocator, io, manifest, .{
        .revision = revision,
        .dirty = status.len != 0,
        .git_status = status,
        .zig_version = zig,
        .os = @tagName(builtin.os.tag),
        .arch = @tagName(builtin.cpu.arch),
        .requested_iterations_per_target = runs,
        .seed = seed,
        .targets = selected,
        .corpus_path = ".zig-cache/f",
        .coverage_path = ".zig-cache/v",
        .working_directory = try std.Io.Dir.cwd().realPathFileAlloc(io, ".", allocator),
    });
    var receipts = std.ArrayList(Receipt).empty;
    var failed: usize = 0;
    for (selected, 0..) |target, index| {
        const prefix = try std.fmt.allocPrint(allocator, "{s}/{d:0>2}", .{ directory, index });
        const stdout_path = try std.fmt.allocPrint(allocator, "{s}.stdout.log", .{prefix});
        const stderr_path = try std.fmt.allocPrint(allocator, "{s}.stderr.log", .{prefix});
        const filter_arg = try std.fmt.allocPrint(allocator, "-D{s}={s}", .{ target.suite.filter, target.name });
        const runs_arg = try std.fmt.allocPrint(allocator, "--fuzz={d}", .{runs});
        const seed_arg = try std.fmt.allocPrint(allocator, "--seed=0x{x}", .{seed});
        const argv = try allocator.dupe([]const u8, &.{ "zig", "build", target.suite.step, filter_arg, runs_arg, seed_arg, "-j1", "--summary", "all" });
        const start_ms = std.Io.Timestamp.now(io, .awake).toMilliseconds();
        std.debug.print("[{d}/{d}] {s}\n", .{ index + 1, selected.len, target.name });
        var receipt = Receipt{
            .source = target.suite.source,
            .source_sha256 = target.source_sha256,
            .target = target.name,
            .report_name = target.report_name,
            .argv = argv,
            .requested_iterations = runs,
            .seed = seed,
            .elapsed_ms = 0,
            .status = .failed,
            .failure = null,
            .report = null,
            .process_term = null,
            .stdout_path = stdout_path,
            .stderr_path = stderr_path,
        };
        const result = std.process.run(allocator, io, .{
            .argv = argv,
            .stdout_limit = .limited(1024 * 1024),
            .stderr_limit = .limited(1024 * 1024),
            .timeout = .{ .deadline = std.Io.Clock.Timestamp.fromNow(io, .{ .raw = std.Io.Duration.fromSeconds(300), .clock = .awake }) },
        }) catch |err| {
            receipt.failure = @errorName(err);
            receipt.elapsed_ms = std.Io.Timestamp.now(io, .awake).toMilliseconds() - start_ms;
            try writeFile(io, stdout_path, "");
            try writeFile(io, stderr_path, try std.fmt.allocPrint(allocator, "process did not yield a complete fuzz report: {s}\n", .{@errorName(err)}));
            try receipts.append(allocator, receipt);
            try writeJson(allocator, io, try std.fmt.allocPrint(allocator, "{s}.json", .{prefix}), receipt);
            std.debug.print("  FAILED: {s}\n", .{@errorName(err)});
            failed += 1;
            continue;
        };
        receipt.elapsed_ms = std.Io.Timestamp.now(io, .awake).toMilliseconds() - start_ms;
        receipt.process_term = result.term;
        try writeFile(io, stdout_path, result.stdout);
        try writeFile(io, stderr_path, result.stderr);
        const combined = try std.mem.concat(allocator, u8, &.{ result.stdout, "\n", result.stderr });
        // Zig reuses f/crash for later failures. Preserve this target's input
        // before continuing with an independent target.
        if (std.mem.indexOf(u8, combined, "input saved to '") != null) {
            const crash = std.Io.Dir.cwd().readFileAlloc(io, ".zig-cache/f/crash", allocator, .limited(64 * 1024 * 1024)) catch null;
            if (crash) |bytes| {
                receipt.crash_input_path = try std.fmt.allocPrint(allocator, "{s}.crash.bin", .{prefix});
                try writeFile(io, receipt.crash_input_path.?, bytes);
            }
        }
        if (validateReport(combined, target.report_name, runs, result.term == .exited and result.term.exited == 0)) |report| {
            receipt.status = .passed;
            receipt.report = report;
            std.debug.print("  {d} iterations in {d} ms\n", .{ report.iterations, receipt.elapsed_ms });
        } else |err| {
            receipt.failure = @errorName(err);
            failed += 1;
            std.debug.print("  FAILED: {s} ({s})\n", .{ @errorName(err), stderr_path });
        }
        try receipts.append(allocator, receipt);
        try writeJson(allocator, io, try std.fmt.allocPrint(allocator, "{s}.json", .{prefix}), receipt);
    }
    try writeJson(allocator, io, try std.fs.path.join(allocator, &.{ directory, "summary.json" }), .{ .targets = selected.len, .failed = failed, .receipts = receipts.items });
    std.debug.print("Fuzz evidence: {s} ({d}/{d} passed)\n", .{ directory, selected.len - failed, selected.len });
    if (failed != 0) return error.FuzzEvidenceRejected;
}

test "fuzz evidence harness rejects a real successful child with no fuzz activity" {
    const result = try std.process.run(std.testing.allocator, std.testing.io, .{ .argv = &.{ "zig", "version" } });
    defer std.testing.allocator.free(result.stdout);
    defer std.testing.allocator.free(result.stderr);
    try std.testing.expect(result.term == .exited and result.term.exited == 0);
    try std.testing.expectError(error.MissingFuzzReport, validateReport(result.stdout, "fixture.test.fuzz: mutation", 1, true));
}
test "fuzz evidence harness rejects deliberately injected oracle failure" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    const result = try std.process.run(std.testing.allocator, std.testing.io, .{ .argv = &.{ "sh", "-c", "printf '%s\\n' 'error: injected oracle mismatch'; exit 1" } });
    defer std.testing.allocator.free(result.stdout);
    defer std.testing.allocator.free(result.stderr);
    try std.testing.expect(result.term == .exited and result.term.exited == 1);
    const output = try std.mem.concat(std.testing.allocator, u8, &.{ fixture, "\n", result.stdout });
    defer std.testing.allocator.free(output);
    try std.testing.expectError(error.FuzzCommandFailed, validateReport(output, "fixture.test.fuzz: mutation", 1, result.term == .exited and result.term.exited == 0));
}
