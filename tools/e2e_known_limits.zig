//! The known limits of the cross-implementation e2e matrix
//! (tools/e2e_runner.zig): cases that fail on purpose, each with the exact
//! TAP checks that fail.
//!
//! The runner records such a case as XFAIL when exactly these checks fail,
//! with the expected text in the client output, and every other check
//! passes. Any other failure stays FAIL. A listed check that passes is
//! XPASS, which fails the gate like FAIL: when a fix lands, remove its rows
//! here and update the docs that the `link` names.
//! tools/e2e_outcome.zig holds the logic and its tests.

/// One case of the matrix that is expected to fail.
pub const KnownLimit = struct {
    /// Names the limit. One limit can have a row per client and transport.
    id: []const u8,
    /// "zig", or a reference backend: "cpp", "go", "python" or "rust".
    client: []const u8,
    server: []const u8,
    /// "tcp" or "unix" (the runner's `--transport`).
    transport: []const u8,
    /// The runner's `--schema` value for the case.
    schema: []const u8,
    /// The description of every check that fails, as the client prints it
    /// after `not ok N - `. The case is XFAIL only when exactly these fail.
    checks: []const []const u8,
    /// Text that the client's output must contain for XFAIL: the exception
    /// that the listed checks fail with. Without it the case is FAIL, so
    /// another fault that fails the same checks is not taken for the limit.
    expect_output: []const u8,
    reason: []const u8,
    /// Where the limit is documented.
    link: []const u8,
};

/// A pipelined param that names an answer the server has not returned yet
/// stays `.promised`, so the server's resolveToken fails with
/// error.PromiseUnresolved and check() returns that exception.
const pending_answer_checks = [_][]const u8{
    "check(pipelined mint().token) called the server's own Token",
    "check(pipelined mintFail().token) fails with mintFail()'s exception",
};
const pending_answer_output = "PromiseUnresolved";
const pending_answer_reason = "The Peer resolves a receiverAnswer param only after the named answer has returned. This server answers mint() and mintFail() after the check() that names them, so the param stays .promised.";
const pending_answer_link = "docs/supported-surface.md (\"A pipelined capability in call params resolves only once its answer has returned\")";

pub const list = [_]KnownLimit{
    .{ .id = "h9-pending-answer", .client = "cpp", .server = "zig", .transport = "tcp", .schema = "pipelined_params_deferred", .checks = &pending_answer_checks, .expect_output = pending_answer_output, .reason = pending_answer_reason, .link = pending_answer_link },
    .{ .id = "h9-pending-answer", .client = "go", .server = "zig", .transport = "tcp", .schema = "pipelined_params_deferred", .checks = &pending_answer_checks, .expect_output = pending_answer_output, .reason = pending_answer_reason, .link = pending_answer_link },
    .{ .id = "h9-pending-answer", .client = "python", .server = "zig", .transport = "tcp", .schema = "pipelined_params_deferred", .checks = &pending_answer_checks, .expect_output = pending_answer_output, .reason = pending_answer_reason, .link = pending_answer_link },
    .{ .id = "h9-pending-answer", .client = "rust", .server = "zig", .transport = "tcp", .schema = "pipelined_params_deferred", .checks = &pending_answer_checks, .expect_output = pending_answer_output, .reason = pending_answer_reason, .link = pending_answer_link },
    .{ .id = "h9-pending-answer", .client = "cpp", .server = "zig", .transport = "unix", .schema = "pipelined_params_deferred", .checks = &pending_answer_checks, .expect_output = pending_answer_output, .reason = pending_answer_reason, .link = pending_answer_link },
};
