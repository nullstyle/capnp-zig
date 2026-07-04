//! CI honesty gate against SILENT SKIPS.
//!
//! Many codegen/interop tests `return error.SkipZigTest` when a committed
//! fixture is absent (their `access(path) catch SkipZigTest` guards, meant for
//! genuinely-optional external inputs). That is convenient, but it means a
//! rename or accidental delete of a checked-in fixture turns whole suites
//! green-by-skip instead of failing — and Zig's build test runner folds
//! per-test skips into the step summary, so the regression is easy to miss.
//!
//! Committed fixtures are ALWAYS expected to exist. This gate asserts the
//! fixtures that gate whole suites are present, so a rename/move fails loudly
//! here (locally and in CI) instead of silently disabling coverage elsewhere.
//! When a fixture is intentionally renamed, this list is updated in the same
//! change — the diff makes the coverage move explicit.

const std = @import("std");

test "CI honesty: committed fixtures gating skip-guarded suites are present" {
    const io = std.testing.io;

    // Each entry gates at least one suite that would otherwise skip silently
    // if the file went missing.
    const fixtures = [_][]const u8{
        // codegen runtime / golden / collision suites
        "tests/test_schemas/nested_collisions.capnp",
        "tests/test_schemas/nested_interfaces.capnp",
        "tests/test_schemas/streaming.capnp",
        "tests/test_schemas/defaults.capnp",
        "tests/test_schemas/annotations.capnp",
        "tests/test_schemas/edge_codegen.capnp",
        // e2e schema set
        "tests/e2e/schemas/game_world.capnp",
    };
    // NOTE: vendored interop fixtures (vendor/ext/*) are git submodules and may
    // legitimately be unfetched, so they are NOT asserted here.

    var missing: usize = 0;
    for (fixtures) |path| {
        std.Io.Dir.cwd().access(io, path, .{}) catch |err| {
            std.debug.print(
                "CI HONESTY FAILURE: committed fixture missing: {s} ({}). A suite that " ++
                    "gates on this path would silently skip.\n",
                .{ path, err },
            );
            missing += 1;
        };
    }
    try std.testing.expectEqual(@as(usize, 0), missing);
}
