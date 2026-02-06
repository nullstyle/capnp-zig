const std = @import("std");

const Case = struct {
    name: []const u8,
    binary: []const u8,
    args: []const []const u8,
    metric: []const u8 = "ns_per_iter",
    baseline: f64,
    max_regression_pct: ?f64 = null,
};

const Baselines = struct {
    max_regression_pct: f64 = 30.0,
    cases: []const Case,
};

const Options = struct {
    baseline_path: []const u8 = "bench/baselines.json",
    max_regression_pct_override: ?f64 = null,
};

fn printUsage() void {
    std.debug.print(
        \\Usage: zig build bench-check -- [options]
        \\  --baseline PATH   Baseline JSON path (default: bench/baselines.json)
        \\  --max-reg-pct N   Override max regression percent for all cases
        \\  -h, --help        Show this help
        \\
    , .{});
}

fn parseF64(arg: []const u8) !f64 {
    return std.fmt.parseFloat(f64, arg);
}

fn parseArgs(allocator: std.mem.Allocator) !?Options {
    var opts = Options{};
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--baseline")) {
            i += 1;
            if (i >= args.len) return error.InvalidArgument;
            opts.baseline_path = args[i];
            continue;
        }
        if (std.mem.eql(u8, arg, "--max-reg-pct")) {
            i += 1;
            if (i >= args.len) return error.InvalidArgument;
            opts.max_regression_pct_override = try parseF64(args[i]);
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

fn runCase(
    allocator: std.mem.Allocator,
    case: Case,
    default_max_regression_pct: f64,
    override_max_regression_pct: ?f64,
) !bool {
    const argv = try allocator.alloc([]const u8, case.args.len + 1);
    defer allocator.free(argv);
    argv[0] = case.binary;
    for (case.args, 0..) |arg, idx| argv[idx + 1] = arg;

    const result = try std.process.Child.run(.{
        .allocator = allocator,
        .argv = argv,
        .max_output_bytes = 8 * 1024 * 1024,
    });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    if (!(result.term == .Exited and result.term.Exited == 0)) {
        std.debug.print("[FAIL] {s}: benchmark command failed\n", .{case.name});
        if (result.stdout.len > 0) std.debug.print("stdout:\n{s}\n", .{result.stdout});
        if (result.stderr.len > 0) std.debug.print("stderr:\n{s}\n", .{result.stderr});
        return false;
    }

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, result.stdout, .{});
    defer parsed.deinit();
    const actual = try metricFromJson(parsed.value, case.metric);

    const max_pct = override_max_regression_pct orelse case.max_regression_pct orelse default_max_regression_pct;
    const allowed = case.baseline * (1.0 + (max_pct / 100.0));
    const pass = actual <= allowed;

    std.debug.print(
        "[{s}] {s}: {s}={d:.2} baseline={d:.2} allowed<={d:.2} (+{d:.1}%)\n",
        .{
            if (pass) "PASS" else "FAIL",
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

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const opts = (parseArgs(allocator) catch |err| {
        std.debug.print("Argument error: {s}\n", .{@errorName(err)});
        printUsage();
        return;
    }) orelse return;

    const baseline_bytes = try std.fs.cwd().readFileAlloc(allocator, opts.baseline_path, 4 * 1024 * 1024);
    defer allocator.free(baseline_bytes);

    const parsed = try std.json.parseFromSlice(Baselines, allocator, baseline_bytes, .{});
    defer parsed.deinit();
    const baselines = parsed.value;

    if (baselines.cases.len == 0) {
        std.debug.print("No benchmark cases in {s}\n", .{opts.baseline_path});
        return error.NoBenchmarkCases;
    }

    var failures: usize = 0;
    for (baselines.cases) |case| {
        const pass = runCase(
            allocator,
            case,
            baselines.max_regression_pct,
            opts.max_regression_pct_override,
        ) catch |err| {
            failures += 1;
            std.debug.print("[FAIL] {s}: {s}\n", .{ case.name, @errorName(err) });
            continue;
        };
        if (!pass) failures += 1;
    }

    if (failures != 0) {
        std.debug.print("Benchmark regression check failed: {d} case(s)\n", .{failures});
        return error.BenchmarkRegression;
    }

    std.debug.print("Benchmark regression check passed: {d} case(s)\n", .{baselines.cases.len});
}
