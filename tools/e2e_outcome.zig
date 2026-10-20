//! The outcome of one e2e case, from its client's TAP output and exit code.
//!
//! tools/e2e_runner.zig records one status per case:
//! - `PASS(n)`: the client exited 0, printed n `ok` lines and no `not ok`.
//! - `SKIP(...)`: not run (the runner decides this before the run).
//! - `XFAIL(id,f/t)`: a known limit (tools/e2e_known_limits.zig). Exactly
//!   its listed checks failed, every other check passed, the plan line
//!   matches, and the client exited 1.
//! - `XPASS(id: desc; ...)`: a listed check of a known limit passed. This
//!   fails the gate, so a fix must also remove its rows and update the docs.
//! - `FAIL(...)`: anything else.
//!
//! `Tally` counts a run's statuses and decides its gate: FAIL and XPASS
//! fail it. The runner takes its verdict only from `Tally.gate`.
//!
//! The runner's own tests live here: `zig build test` runs this file as a
//! test root (step `test-e2e-outcome`).

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const known_limits = @import("e2e_known_limits.zig");
pub const KnownLimit = known_limits.KnownLimit;

/// The test lines of a TAP stream. Descriptions are slices of the output.
pub const Tap = struct {
    passed: std.ArrayList([]const u8) = .empty,
    failed: std.ArrayList([]const u8) = .empty,
    bailed: bool = false,
    /// N of the `1..N` plan line, when there is one.
    plan: ?usize = null,

    pub fn deinit(self: *Tap, allocator: Allocator) void {
        self.passed.deinit(allocator);
        self.failed.deinit(allocator);
    }

    pub fn total(self: *const Tap) usize {
        return self.passed.items.len + self.failed.items.len;
    }
};

/// Read the `ok`, `not ok`, `Bail out!` and `1..N` lines of `output`. Every
/// other line (diagnostics, YAML blocks, logs) is ignored.
pub fn evalTap(allocator: Allocator, output: []const u8) Allocator.Error!Tap {
    var tap: Tap = .{};
    errdefer tap.deinit(allocator);
    var it = std.mem.splitScalar(u8, output, '\n');
    while (it.next()) |raw| {
        const line = std.mem.trimEnd(u8, raw, " \t\r");
        if (std.mem.startsWith(u8, line, "Bail out!")) {
            tap.bailed = true;
        } else if (std.mem.startsWith(u8, line, "ok ")) {
            try tap.passed.append(allocator, description(line["ok ".len..]));
        } else if (std.mem.startsWith(u8, line, "not ok ")) {
            try tap.failed.append(allocator, description(line["not ok ".len..]));
        } else if (std.mem.startsWith(u8, line, "1..")) {
            const digits = line["1..".len..];
            const end = std.mem.indexOfNone(u8, digits, "0123456789") orelse digits.len;
            tap.plan = std.fmt.parseInt(usize, digits[0..end], 10) catch null;
        }
    }
    return tap;
}

/// The description after a test line's status: `3 - text` gives `text`.
fn description(rest: []const u8) []const u8 {
    var text = std.mem.trimStart(u8, rest, " ");
    text = std.mem.trimStart(u8, text, "0123456789");
    text = std.mem.trimStart(u8, text, " ");
    if (std.mem.startsWith(u8, text, "- ")) text = text["- ".len..];
    return text;
}

/// A case's place in the matrix.
pub const Case = struct {
    client: []const u8,
    server: []const u8,
    transport: []const u8,
    schema: []const u8,
};

/// The known limit of `case` in `limits`, if it has one.
pub fn findLimit(limits: []const KnownLimit, case: Case) ?*const KnownLimit {
    for (limits) |*limit| {
        if (std.mem.eql(u8, limit.client, case.client) and
            std.mem.eql(u8, limit.server, case.server) and
            std.mem.eql(u8, limit.transport, case.transport) and
            std.mem.eql(u8, limit.schema, case.schema)) return limit;
    }
    return null;
}

fn contains(list: []const []const u8, item: []const u8) bool {
    for (list) |entry| {
        if (std.mem.eql(u8, entry, item)) return true;
    }
    return false;
}

fn countOf(list: []const []const u8, item: []const u8) usize {
    var n: usize = 0;
    for (list) |entry| {
        if (std.mem.eql(u8, entry, item)) n += 1;
    }
    return n;
}

/// The status of one run. `limit` is the case's known limit, if any.
/// The caller owns the returned text.
pub fn status(allocator: Allocator, exit_code: i32, tap: *const Tap, limit: ?*const KnownLimit) Allocator.Error![]u8 {
    const fail_count = tap.failed.items.len;
    const run_total = tap.total();
    if (limit == null and exit_code == 0 and fail_count == 0 and run_total > 0 and !tap.bailed) {
        return std.fmt.allocPrint(allocator, "PASS({d})", .{run_total});
    }

    if (tap.bailed) {
        if (exit_code == 0) return allocator.dupe(u8, "FAIL(BAIL)");
        return std.fmt.allocPrint(allocator, "FAIL(exit={d},BAIL)", .{exit_code});
    }

    if (run_total == 0) {
        if (exit_code == 0) return allocator.dupe(u8, "FAIL(NO_TESTS)");
        return std.fmt.allocPrint(allocator, "FAIL(exit={d},NO_TESTS)", .{exit_code});
    }

    const known = limit orelse return plainFail(allocator, exit_code, fail_count, run_total, null);

    // A failure the limit does not list, or a listed one twice.
    for (tap.failed.items) |desc| {
        if (!contains(known.checks, desc) or countOf(tap.failed.items, desc) > 1) {
            return plainFail(allocator, exit_code, fail_count, run_total, .{ "unexpected", desc });
        }
    }

    // A listed check that passed: the limit may be fixed.
    var xpassed: std.ArrayList(u8) = .empty;
    defer xpassed.deinit(allocator);
    for (known.checks) |check| {
        if (contains(tap.passed.items, check)) {
            if (xpassed.items.len > 0) try xpassed.appendSlice(allocator, "; ");
            try xpassed.appendSlice(allocator, check);
        }
    }
    if (xpassed.items.len > 0) {
        return std.fmt.allocPrint(allocator, "XPASS({s}: {s})", .{ known.id, xpassed.items });
    }

    // A listed check that the client never reported: the run is incomplete.
    for (known.checks) |check| {
        if (!contains(tap.failed.items, check)) {
            return plainFail(allocator, exit_code, fail_count, run_total, .{ "missing", check });
        }
    }

    // Exactly the listed checks failed. The run must also be complete and
    // end the way a client ends on a failed check.
    if (tap.plan == null or tap.plan.? != run_total) {
        return plainFail(allocator, exit_code, fail_count, run_total, .{ "plan", "the 1..N line is missing or does not match" });
    }
    if (exit_code != 1) {
        return plainFail(allocator, exit_code, fail_count, run_total, .{ "exit", "a known limit needs exit code 1" });
    }
    return std.fmt.allocPrint(allocator, "XFAIL({s},{d}/{d})", .{ known.id, fail_count, run_total });
}

fn plainFail(allocator: Allocator, exit_code: i32, fail_count: usize, run_total: usize, why: ?struct { []const u8, []const u8 }) Allocator.Error![]u8 {
    if (why) |w| {
        if (exit_code == 0) return std.fmt.allocPrint(allocator, "FAIL({d}/{d},{s}: {s})", .{ fail_count, run_total, w[0], w[1] });
        return std.fmt.allocPrint(allocator, "FAIL(exit={d},{d}/{d},{s}: {s})", .{ exit_code, fail_count, run_total, w[0], w[1] });
    }
    if (exit_code == 0) return std.fmt.allocPrint(allocator, "FAIL({d}/{d})", .{ fail_count, run_total });
    return std.fmt.allocPrint(allocator, "FAIL(exit={d},{d}/{d})", .{ exit_code, fail_count, run_total });
}

/// How the summary counts a status.
pub const Bucket = enum {
    pass,
    skip,
    xfail,
    xpass,
    fail,

    /// XPASS fails the gate like FAIL.
    pub fn failsGate(self: Bucket) bool {
        return self == .xpass or self == .fail;
    }
};

pub fn bucket(status_text: []const u8) Bucket {
    if (std.mem.startsWith(u8, status_text, "PASS(")) return .pass;
    if (std.mem.startsWith(u8, status_text, "SKIP(")) return .skip;
    if (std.mem.startsWith(u8, status_text, "XFAIL(")) return .xfail;
    if (std.mem.startsWith(u8, status_text, "XPASS(")) return .xpass;
    return .fail;
}

/// The statuses of one run, counted by bucket. The runner prints these
/// counts, writes them to its summary JSON, and takes its gate verdict from
/// `gate`.
pub const Tally = struct {
    pass: usize = 0,
    skip: usize = 0,
    xfail: usize = 0,
    xpass: usize = 0,
    fail: usize = 0,
    /// Statuses whose bucket fails the gate (`Bucket.failsGate`).
    gate_failures: usize = 0,

    pub fn add(self: *Tally, status_text: []const u8) void {
        const b = bucket(status_text);
        switch (b) {
            .pass => self.pass += 1,
            .skip => self.skip += 1,
            .xfail => self.xfail += 1,
            .xpass => self.xpass += 1,
            .fail => self.fail += 1,
        }
        if (b.failsGate()) self.gate_failures += 1;
    }

    /// The run's gate: error.E2EFailed when any status fails it (FAIL or
    /// XPASS).
    pub fn gate(self: Tally) error{E2EFailed}!void {
        if (self.gate_failures > 0) return error.E2EFailed;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

const check_mint = "check(pipelined mint().token) called the server's own Token";
const check_fail = "check(pipelined mintFail().token) fails with mintFail()'s exception";

const test_limit: KnownLimit = .{
    .id = "test-limit",
    .client = "cpp",
    .server = "zig",
    .transport = "tcp",
    .schema = "pipelined_params_deferred",
    .checks = &.{ check_mint, check_fail },
    .reason = "test",
    .link = "test",
};

fn expectStatus(expected: []const u8, exit_code: i32, output: []const u8, limit: ?*const KnownLimit) !void {
    var tap = try evalTap(testing.allocator, output);
    defer tap.deinit(testing.allocator);
    const got = try status(testing.allocator, exit_code, &tap, limit);
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(expected, got);
}

/// The C++ reference client against the pipelined_params_deferred Zig
/// server, as `just docker-client` printed it on 2026-10-20 (the plan line
/// comes last).
const cpp_xfail_output =
    \\[entrypoint] Starting RPC client...
    \\# check exception: remote exception: PromiseUnresolved
    \\not ok 1 - check(pipelined mint().token) called the server's own Token
    \\ok 2 - mint() returns results
    \\ok 3 - mintFail() fails with its exception
    \\# check(pipelined mintFail().token) exception: remote exception: PromiseUnresolved
    \\not ok 4 - check(pipelined mintFail().token) fails with mintFail()'s exception
    \\1..4
    \\
    \\error: recipe `docker-client` failed on line 28 with exit code 1
    \\
;

test "evalTap reads descriptions, the plan and bail-outs, and ignores other lines" {
    const output =
        "TAP version 14\r\n" ++
        "1..4\r\n" ++
        "ok 1 - first check\r\n" ++
        "not ok 2 - second check  \r\n" ++
        "  ---\n" ++
        "  message: not ok inside YAML\n" ++
        "  ...\n" ++
        "  # not ok in a diagnostic\n" ++
        "debug(rpc_peer): ok 9 - a log line\n" ++
        "ok 3 third check without a dash\n" ++
        "not ok 4 - fourth # TODO kept as part of the text\n";
    var tap = try evalTap(testing.allocator, output);
    defer tap.deinit(testing.allocator);
    try testing.expectEqual(@as(?usize, 4), tap.plan);
    try testing.expect(!tap.bailed);
    try testing.expectEqual(@as(usize, 2), tap.passed.items.len);
    try testing.expectEqualStrings("first check", tap.passed.items[0]);
    try testing.expectEqualStrings("third check without a dash", tap.passed.items[1]);
    try testing.expectEqual(@as(usize, 2), tap.failed.items.len);
    try testing.expectEqualStrings("second check", tap.failed.items[0]);
    try testing.expectEqualStrings("fourth # TODO kept as part of the text", tap.failed.items[1]);

    var bail = try evalTap(testing.allocator, "ok 1 - a\nBail out! KJ exception\n");
    defer bail.deinit(testing.allocator);
    try testing.expect(bail.bailed);
    try testing.expectEqual(@as(?usize, null), bail.plan);
}

test "status without a known limit keeps the PASS and FAIL texts" {
    try expectStatus("PASS(2)", 0, "ok 1 - a\nok 2 - b\n1..2\n", null);
    // No plan line is needed for PASS (the runner never required one).
    try expectStatus("PASS(1)", 0, "ok 1 - a\n", null);
    try expectStatus("FAIL(exit=1,1/2)", 1, "ok 1 - a\nnot ok 2 - b\n1..2\n", null);
    try expectStatus("FAIL(1/2)", 0, "ok 1 - a\nnot ok 2 - b\n", null);
    try expectStatus("FAIL(exit=1,BAIL)", 1, "Bail out! boom\n", null);
    try expectStatus("FAIL(BAIL)", 0, "ok 1 - a\nBail out! boom\n", null);
    try expectStatus("FAIL(exit=124,NO_TESTS)", 124, "", null);
    try expectStatus("FAIL(NO_TESTS)", 0, "hello\n", null);
    // A clean TAP stream with a nonzero exit is still a failure.
    try expectStatus("FAIL(exit=1,0/1)", 1, "ok 1 - a\n1..1\n", null);
}

test "status is XFAIL when exactly the listed checks fail" {
    try expectStatus("XFAIL(test-limit,2/4)", 1, cpp_xfail_output, &test_limit);
    // A leading plan line (the Rust client) counts the same.
    try expectStatus("XFAIL(test-limit,2/4)", 1, "1..4\nnot ok 1 - " ++ check_mint ++ "\nok 2 - m\nok 3 - f\nnot ok 4 - " ++ check_fail ++ "\n", &test_limit);
    try testing.expectEqual(Bucket.xfail, bucket("XFAIL(test-limit,2/4)"));
    try testing.expect(!bucket("XFAIL(test-limit,2/4)").failsGate());
}

test "status is XPASS, a gate failure, when a listed check passes" {
    const one_fixed = "ok 1 - " ++ check_mint ++ "\nok 2 - m\nok 3 - f\nnot ok 4 - " ++ check_fail ++ "\n1..4\n";
    try expectStatus("XPASS(test-limit: " ++ check_mint ++ ")", 1, one_fixed, &test_limit);
    const all_fixed = "ok 1 - " ++ check_mint ++ "\nok 2 - m\nok 3 - f\nok 4 - " ++ check_fail ++ "\n1..4\n";
    try expectStatus("XPASS(test-limit: " ++ check_mint ++ "; " ++ check_fail ++ ")", 0, all_fixed, &test_limit);
    try testing.expectEqual(Bucket.xpass, bucket("XPASS(test-limit: x)"));
    try testing.expect(bucket("XPASS(test-limit: x)").failsGate());
}

test "status is FAIL when anything else fails, or the run is incomplete" {
    // An unlisted check fails too.
    try expectStatus(
        "FAIL(exit=1,3/4,unexpected: mint() returns results)",
        1,
        "not ok 1 - " ++ check_mint ++ "\nnot ok 2 - mint() returns results\nok 3 - f\nnot ok 4 - " ++ check_fail ++ "\n1..4\n",
        &test_limit,
    );
    // An unlisted failure wins over an XPASS.
    try expectStatus(
        "FAIL(exit=1,1/4,unexpected: mint() returns results)",
        1,
        "ok 1 - " ++ check_mint ++ "\nnot ok 2 - mint() returns results\nok 3 - f\nok 4 - " ++ check_fail ++ "\n1..4\n",
        &test_limit,
    );
    // A listed check fails twice.
    try expectStatus(
        "FAIL(exit=1,3/3,unexpected: " ++ check_mint ++ ")",
        1,
        "not ok 1 - " ++ check_mint ++ "\nnot ok 2 - " ++ check_mint ++ "\nnot ok 3 - " ++ check_fail ++ "\n1..3\n",
        &test_limit,
    );
    // The client stopped before it reported a listed check.
    try expectStatus(
        "FAIL(exit=1,1/3,missing: " ++ check_fail ++ ")",
        1,
        "not ok 1 - " ++ check_mint ++ "\nok 2 - m\nok 3 - f\n",
        &test_limit,
    );
    // No plan line, or a plan that does not match the lines.
    try expectStatus(
        "FAIL(exit=1,2/4,plan: the 1..N line is missing or does not match)",
        1,
        "not ok 1 - " ++ check_mint ++ "\nok 2 - m\nok 3 - f\nnot ok 4 - " ++ check_fail ++ "\n",
        &test_limit,
    );
    try expectStatus(
        "FAIL(exit=1,2/4,plan: the 1..N line is missing or does not match)",
        1,
        "1..5\nnot ok 1 - " ++ check_mint ++ "\nok 2 - m\nok 3 - f\nnot ok 4 - " ++ check_fail ++ "\n",
        &test_limit,
    );
    // A timeout (124), a signal or a clean exit is not how a client ends on
    // a failed check.
    try expectStatus("FAIL(exit=124,2/4,exit: a known limit needs exit code 1)", 124, cpp_xfail_output, &test_limit);
    try expectStatus("FAIL(exit=137,2/4,exit: a known limit needs exit code 1)", 137, cpp_xfail_output, &test_limit);
    try expectStatus("FAIL(2/4,exit: a known limit needs exit code 1)", 0, cpp_xfail_output, &test_limit);
    // Bail-outs and empty runs stay what they were.
    try expectStatus("FAIL(exit=1,BAIL)", 1, "not ok 1 - " ++ check_mint ++ "\nBail out! KJ exception\n", &test_limit);
    try expectStatus("FAIL(exit=124,NO_TESTS)", 124, "", &test_limit);
    // A known-limit case that passes completely is an XPASS, not a PASS.
    try expectStatus(
        "XPASS(test-limit: " ++ check_mint ++ "; " ++ check_fail ++ ")",
        0,
        "ok 1 - " ++ check_mint ++ "\nok 2 - " ++ check_fail ++ "\n1..2\n",
        &test_limit,
    );
}

test "bucket sorts every status the runner writes" {
    try testing.expectEqual(Bucket.pass, bucket("PASS(5)"));
    try testing.expectEqual(Bucket.skip, bucket("SKIP(unix: reference harness TCP-only)"));
    try testing.expectEqual(Bucket.fail, bucket("FAIL(exit=1,1/5)"));
    try testing.expectEqual(Bucket.fail, bucket("FAIL(server-start:ServerStartTimeout)"));
    // Anything unknown fails the gate rather than passing it.
    try testing.expectEqual(Bucket.fail, bucket("PASSED"));
    try testing.expectEqual(Bucket.fail, bucket(""));
    try testing.expect(!Bucket.pass.failsGate());
    try testing.expect(!Bucket.skip.failsGate());
    try testing.expect(Bucket.fail.failsGate());
}

fn tallyOf(statuses: []const []const u8) Tally {
    var tally: Tally = .{};
    for (statuses) |status_text| tally.add(status_text);
    return tally;
}

test "the gate fails on FAIL or XPASS, and passes PASS, SKIP and XFAIL" {
    // Known limits that still fail, with passes and skips: the gate passes.
    const green = tallyOf(&.{
        "PASS(5)",
        "SKIP(unix: reference harness TCP-only)",
        "XFAIL(h9-pending-answer,2/4)",
        "XFAIL(h9-pending-answer,2/4)",
    });
    try green.gate();
    try testing.expectEqual(@as(usize, 1), green.pass);
    try testing.expectEqual(@as(usize, 1), green.skip);
    try testing.expectEqual(@as(usize, 2), green.xfail);
    try testing.expectEqual(@as(usize, 0), green.xpass);
    try testing.expectEqual(@as(usize, 0), green.fail);
    // Nothing ran, so nothing failed.
    try tallyOf(&.{}).gate();

    // A fixed known limit fails the gate on its own.
    const fixed = tallyOf(&.{"XPASS(h9-pending-answer: " ++ check_mint ++ ")"});
    try testing.expectEqual(@as(usize, 1), fixed.xpass);
    try testing.expectError(error.E2EFailed, fixed.gate());
    try testing.expectError(error.E2EFailed, tallyOf(&.{ "PASS(5)", "XFAIL(h9-pending-answer,2/4)", "XPASS(h9-pending-answer: x)" }).gate());
    // So does a FAIL, and a status the runner does not know.
    const failed = tallyOf(&.{ "PASS(5)", "FAIL(exit=1,1/5)" });
    try testing.expectEqual(@as(usize, 1), failed.fail);
    try testing.expectError(error.E2EFailed, failed.gate());
    try testing.expectError(error.E2EFailed, tallyOf(&.{"PASSED"}).gate());
}

test "the runner takes its gate verdict only from Tally.gate" {
    // The gate tests above prove nothing if the runner decides on its own.
    // writeSummary ends with this one call, and no other runner code
    // returns the gate error.
    const runner = @embedFile("e2e_runner.zig");
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, runner, "try tally.gate();"));
    try testing.expectEqual(@as(usize, 0), std.mem.count(u8, runner, "E2EFailed"));
}

test "findLimit matches the whole case" {
    const limits = [_]KnownLimit{test_limit};
    const case: Case = .{ .client = "cpp", .server = "zig", .transport = "tcp", .schema = "pipelined_params_deferred" };
    try testing.expectEqual(@as(?*const KnownLimit, &limits[0]), findLimit(&limits, case));
    var other = case;
    other.transport = "unix";
    try testing.expectEqual(@as(?*const KnownLimit, null), findLimit(&limits, other));
    other = case;
    other.client = "go";
    try testing.expectEqual(@as(?*const KnownLimit, null), findLimit(&limits, other));
    other = case;
    other.schema = "pipelined_params";
    try testing.expectEqual(@as(?*const KnownLimit, null), findLimit(&limits, other));
    other = .{ .client = "zig", .server = "cpp", .transport = "tcp", .schema = "pipelined_params_deferred" };
    try testing.expectEqual(@as(?*const KnownLimit, null), findLimit(&limits, other));
}

test "the committed known-limits list is well formed" {
    const backends = [_][]const u8{ "cpp", "go", "python", "rust" };
    try testing.expect(known_limits.list.len > 0);
    for (known_limits.list, 0..) |limit, i| {
        try testing.expect(limit.id.len > 0);
        try testing.expect(limit.reason.len > 0);
        try testing.expect(limit.link.len > 0);
        try testing.expect(limit.schema.len > 0);
        try testing.expect(std.mem.eql(u8, limit.transport, "tcp") or std.mem.eql(u8, limit.transport, "unix"));
        // The runner always has Zig on one side and a reference on the other.
        const zig_client = std.mem.eql(u8, limit.client, "zig");
        const zig_server = std.mem.eql(u8, limit.server, "zig");
        try testing.expect(zig_client != zig_server);
        try testing.expect(contains(&backends, if (zig_client) limit.server else limit.client));
        try testing.expect(limit.checks.len > 0);
        for (limit.checks, 0..) |check, j| {
            try testing.expect(check.len > 0);
            try testing.expect(std.mem.indexOfScalar(u8, check, '\n') == null);
            try testing.expectEqual(@as(usize, 1), countOf(limit.checks, check));
            _ = j;
        }
        // One row per case: findLimit returns the first match only.
        const case: Case = .{ .client = limit.client, .server = limit.server, .transport = limit.transport, .schema = limit.schema };
        try testing.expectEqual(@as(?*const KnownLimit, &known_limits.list[i]), findLimit(&known_limits.list, case));
    }
}

/// The Go, Python and Rust reference clients in the same run (TAP and
/// diagnostic lines; the docker and just lines are left out).
const go_xfail_output =
    \\# check error: cap_passing.capnp:TokenHost.check: PromiseUnresolved
    \\not ok 1 - check(pipelined mint().token) called the server's own Token
    \\ok 2 - mint() returns results
    \\ok 3 - mintFail() fails with its exception
    \\# check(pipelined mintFail().token) error: cap_passing.capnp:TokenHost.check: PromiseUnresolved
    \\not ok 4 - check(pipelined mintFail().token) fails with mintFail()'s exception
    \\1..4
    \\
;
const python_xfail_output =
    \\# pipelined_params tests
    \\not ok 1 - check(pipelined mint().token) called the server's own Token
    \\  # capnp.lib.capnp.KjException: (remote):0: failed: remote exception: PromiseUnresolved
    \\ok 2 - mint() returns results
    \\ok 3 - mintFail() fails with its exception
    \\# check(pipelined mintFail().token) error: (remote):0: failed: remote exception: PromiseUnresolved
    \\stack: ffff8837c048 ffff8836da40 ffff882368e0 ffff882387e0
    \\not ok 4 - check(pipelined mintFail().token) fails with mintFail()'s exception
    \\  # (remote):0: failed: remote exception: PromiseUnresolved
    \\1..4
    \\
;
const rust_xfail_output =
    \\TAP version 14
    \\1..4
    \\not ok 1 - check(pipelined mint().token) called the server's own Token
    \\  ---
    \\  message: Failed: remote exception: PromiseUnresolved
    \\  ...
    \\ok 2 - mint() returns results
    \\ok 3 - mintFail() fails with its exception
    \\# check(pipelined mintFail().token) error: Failed: remote exception: PromiseUnresolved
    \\not ok 4 - check(pipelined mintFail().token) fails with mintFail()'s exception
    \\  ---
    \\  message: Failed: remote exception: PromiseUnresolved
    \\  ...
    \\Error: "Some tests failed"
    \\
;

test "the committed list turns each recorded reference run into XFAIL" {
    const runs = [_]struct { client: []const u8, output: []const u8 }{
        .{ .client = "cpp", .output = cpp_xfail_output },
        .{ .client = "go", .output = go_xfail_output },
        .{ .client = "python", .output = python_xfail_output },
        .{ .client = "rust", .output = rust_xfail_output },
    };
    for (runs) |run| {
        const case: Case = .{ .client = run.client, .server = "zig", .transport = "tcp", .schema = "pipelined_params_deferred" };
        const limit = findLimit(&known_limits.list, case) orelse return error.TestExpectedKnownLimit;
        try expectStatus("XFAIL(h9-pending-answer,2/4)", 1, run.output, limit);
    }
}
