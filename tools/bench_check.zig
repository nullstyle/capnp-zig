const std = @import("std");

const Case = struct {
    name: []const u8,
    binary: []const u8,
    args: []const []const u8,
    metric: []const u8 = "ns_per_iter",
    baseline: f64,
    max_regression_pct: ?f64 = null,
    // Direction of "worse". Latency/alloc metrics regress when they grow
    // (the default). Throughput metrics (e.g. calls_per_sec) regress when
    // they shrink; set this true so the gate bounds the downside instead.
    higher_is_better: bool = false,
    // Measure and report, but never fail the gate. For metrics that a shared
    // CI runner cannot measure reliably — a serialized round-trip's latency
    // percentiles are dominated by hypervisor/neighbor scheduling, not by our
    // code, so enforcing them produces random red builds that train everyone
    // to ignore the gate. Advisory cases still print their numbers (as [WARN]
    // when out of band) so a real regression remains visible, and they stay
    // enforceable on a quiet machine via `--enforce-advisory`.
    advisory: bool = false,
    // Free text the checker ignores: where and how this baseline was taken.
    // A number whose provenance is unknown cannot be re-taken comparably.
    note: ?[]const u8 = null,
};

const Baselines = struct {
    max_regression_pct: f64 = 30.0,
    // Free text the checker ignores; see `Case.note`.
    note: ?[]const u8 = null,
    cases: []const Case,
};

const Options = struct {
    baseline_path: []const u8 = "bench/baselines.json",
    max_regression_pct_override: ?f64 = null,
    // Promote advisory cases back to hard failures. Use on a quiet machine
    // (local runs, a dedicated bench host) where latency percentiles are
    // actually measurable.
    enforce_advisory: bool = false,
};

fn printUsage() void {
    std.debug.print(
        \\Usage: zig build bench-check -- [options]
        \\  --baseline PATH   Baseline JSON path (default: bench/baselines.json)
        \\  --max-reg-pct N   Override max regression percent for all cases
        \\  --enforce-advisory  Fail on advisory cases too (quiet machines only)
        \\  -h, --help        Show this help
        \\
    , .{});
}

fn parseF64(arg: []const u8) !f64 {
    return std.fmt.parseFloat(f64, arg);
}

fn parseArgs(iter: *std.process.Args.Iterator) !?Options {
    var opts = Options{};
    _ = iter.skip(); // skip program name

    while (iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--baseline")) {
            opts.baseline_path = iter.next() orelse return error.InvalidArgument;
            continue;
        }
        if (std.mem.eql(u8, arg, "--max-reg-pct")) {
            opts.max_regression_pct_override = try parseF64(iter.next() orelse return error.InvalidArgument);
            continue;
        }
        if (std.mem.eql(u8, arg, "--enforce-advisory")) {
            opts.enforce_advisory = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            printUsage();
            return null;
        }
        return error.InvalidArgument;
    }

    return opts;
}

fn metricFromJson(root: std.json.Value, metric: []const u8) !f64 {
    if (root != .object) return error.InvalidBenchmarkOutput;
    const value = root.object.get(metric) orelse return error.MissingMetric;
    return switch (value) {
        .float => value.float,
        .integer => @as(f64, @floatFromInt(value.integer)),
        else => error.InvalidMetricType,
    };
}

/// One benchmark invocation's result. Several cases usually read different
/// metrics from the same command line (p50, p99, calls/sec, allocations), so
/// each distinct command runs ONCE per bench-check and every case sharing it
/// reads the same JSON. Before this, each case re-ran its binary: the RPC
/// sequential bench ran three times to produce three numbers from one line.
const Run = struct {
    binary: []const u8,
    args: []const []const u8,
    outcome: Outcome,

    const Outcome = union(enum) {
        ok: std.json.Parsed(std.json.Value),
        /// Exited non-zero; its output was printed when it ran.
        command_failed,
        /// Could not spawn it, or its stdout was not JSON.
        harness_error: anyerror,
    };

    fn matches(self: *const Run, case: Case) bool {
        if (!std.mem.eql(u8, self.binary, case.binary)) return false;
        if (self.args.len != case.args.len) return false;
        for (self.args, case.args) |a, b| {
            if (!std.mem.eql(u8, a, b)) return false;
        }
        return true;
    }
};

fn runBenchmark(allocator: std.mem.Allocator, io: std.Io, case: Case) Run.Outcome {
    return runBenchmarkInner(allocator, io, case) catch |err| .{ .harness_error = err };
}

fn runBenchmarkInner(allocator: std.mem.Allocator, io: std.Io, case: Case) !Run.Outcome {
    const argv = try allocator.alloc([]const u8, case.args.len + 1);
    defer allocator.free(argv);
    argv[0] = case.binary;
    for (case.args, 0..) |arg, idx| argv[idx + 1] = arg;

    const result = try std.process.run(allocator, io, .{
        .argv = argv,
        .stdout_limit = .limited(8 * 1024 * 1024),
        .stderr_limit = .limited(8 * 1024 * 1024),
    });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    if (!(result.term == .exited and result.term.exited == 0)) {
        // Printed once per command; each case reading it reports its own [FAIL].
        std.debug.print("{s}: benchmark command failed ({any})\n", .{ case.binary, result.term });
        if (result.stdout.len > 0) std.debug.print("stdout:\n{s}\n", .{result.stdout});
        if (result.stderr.len > 0) std.debug.print("stderr:\n{s}\n", .{result.stderr});
        return .command_failed;
    }

    // alloc_always: the parsed value outlives `result.stdout`.
    return .{ .ok = try std.json.parseFromSlice(std.json.Value, allocator, result.stdout, .{
        .allocate = .alloc_always,
    }) };
}

fn checkCase(
    root: std.json.Value,
    case: Case,
    default_max_regression_pct: f64,
    override_max_regression_pct: ?f64,
    // True when a miss actually fails the gate. Only affects the printed
    // label, so an out-of-band advisory case under --enforce-advisory reads
    // [FAIL] (which is what it does) rather than [WARN].
    gates: bool,
) !bool {
    const actual = try metricFromJson(root, case.metric);

    const max_pct = override_max_regression_pct orelse case.max_regression_pct orelse default_max_regression_pct;
    if (case.higher_is_better) {
        // Throughput: the regression is a drop below baseline, so the bound
        // is a floor (baseline scaled down by the allowed percentage).
        const allowed = case.baseline * (1.0 - (max_pct / 100.0));
        const pass = actual >= allowed;
        std.debug.print(
            "[{s}] {s}: {s}={d:.2} baseline={d:.2} allowed>={d:.2} (-{d:.1}%)\n",
            .{
                if (pass) "PASS" else if (gates) "FAIL" else "WARN",
                case.name,
                case.metric,
                actual,
                case.baseline,
                allowed,
                max_pct,
            },
        );
        return pass;
    }

    const allowed = case.baseline * (1.0 + (max_pct / 100.0));
    const pass = actual <= allowed;

    std.debug.print(
        "[{s}] {s}: {s}={d:.2} baseline={d:.2} allowed<={d:.2} (+{d:.1}%)\n",
        .{
            if (pass) "PASS" else if (gates) "FAIL" else "WARN",
            case.name,
            case.metric,
            actual,
            case.baseline,
            allowed,
            max_pct,
        },
    );
    return pass;
}

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();
    const io = init.io;

    // initAllocator is the cross-platform form; plain init is a compile
    // error on Windows. The iterator outlives parsing because
    // --baseline captures a slice of its buffer.
    var args_iter = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args_iter.deinit();
    const opts = (parseArgs(&args_iter) catch |err| {
        std.debug.print("Argument error: {s}\n", .{@errorName(err)});
        printUsage();
        return;
    }) orelse return;

    const baseline_bytes = try std.Io.Dir.cwd().readFileAlloc(io, opts.baseline_path, allocator, .limited(4 * 1024 * 1024));
    defer allocator.free(baseline_bytes);

    const parsed = try std.json.parseFromSlice(Baselines, allocator, baseline_bytes, .{});
    defer parsed.deinit();
    const baselines = parsed.value;

    if (baselines.cases.len == 0) {
        std.debug.print("No benchmark cases in {s}\n", .{opts.baseline_path});
        return error.NoBenchmarkCases;
    }

    var runs: std.ArrayList(Run) = .empty;
    defer {
        for (runs.items) |*run| switch (run.outcome) {
            .ok => |*json| json.deinit(),
            .command_failed, .harness_error => {},
        };
        runs.deinit(allocator);
    }

    var failures: usize = 0;
    var advisories: usize = 0;
    for (baselines.cases) |case| {
        // An advisory case never fails the gate unless --enforce-advisory.
        // A harness error (could not run / exited non-zero / could not
        // parse) always fails, advisory or not: that is a broken benchmark,
        // not a noisy metric.
        const gates = !case.advisory or opts.enforce_advisory;
        const run: *const Run = for (runs.items) |*run| {
            if (run.matches(case)) break run;
        } else blk: {
            try runs.append(allocator, .{
                .binary = case.binary,
                .args = case.args,
                .outcome = runBenchmark(allocator, io, case),
            });
            break :blk &runs.items[runs.items.len - 1];
        };
        const root = switch (run.outcome) {
            .ok => |json| json.value,
            .command_failed => {
                failures += 1;
                std.debug.print("[FAIL] {s}: benchmark command failed\n", .{case.name});
                continue;
            },
            .harness_error => |err| {
                failures += 1;
                std.debug.print("[FAIL] {s}: {s}\n", .{ case.name, @errorName(err) });
                continue;
            },
        };
        const pass = checkCase(
            root,
            case,
            baselines.max_regression_pct,
            opts.max_regression_pct_override,
            gates,
        ) catch |err| {
            failures += 1;
            std.debug.print("[FAIL] {s}: {s}\n", .{ case.name, @errorName(err) });
            continue;
        };
        if (!pass) {
            if (gates) {
                failures += 1;
            } else {
                advisories += 1;
            }
        }
    }

    if (advisories != 0) {
        std.debug.print(
            "{d} advisory case(s) outside their band (not gating; re-run with --enforce-advisory on a quiet machine)\n",
            .{advisories},
        );
    }

    if (failures != 0) {
        std.debug.print("Benchmark regression check failed: {d} case(s)\n", .{failures});
        return error.BenchmarkRegression;
    }

    std.debug.print("Benchmark regression check passed: {d} case(s)\n", .{baselines.cases.len});
}
