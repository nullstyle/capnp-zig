//! `check-evented-canary`: an expected-fail compile of `std.Io.Evented`.
//!
//! `src/io_backend.zig` hard-codes `evented_available = false` because no
//! `std.Io.Evented` compiles at the pinned Zig 0.17.0. Nothing would notice
//! the day one does, so the selector would stay disabled after the reason
//! went away. This step compiles `tools/evented_canary.zig`, which references
//! the backend, and SUCCEEDS ONLY WHILE that compile fails with the known
//! std defect. It goes red when:
//!
//!  - `std.Io.Evented` compiles: flip `evented_available` (and re-check the
//!    socket vtable, see docs/stability.md) and retarget this canary; or
//!  - it still fails, but for a different reason: read the new error, update
//!    `expected_error` and the fork handoff, and keep the flag off.
//!
//! Self-contained on purpose: one call from build_impl.zig registers it.

const std = @import("std");

/// The defect pinned in docs/upstream/handoff-zig-fork-evented-processreplacepath.md.
/// Matched as a line suffix, so the std path and line number may move.
pub const expected_error = "error: no field named 'processReplacePath' in struct 'Io.VTable'";

pub fn register(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) void {
    const step = b.step(
        "check-evented-canary",
        "Expected-fail canary: green only while std.Io.Evented fails to compile with the known std defect",
    );
    if (!exposesEvented(target.result)) {
        const fail = b.addFail("check-evented-canary needs a target where Zig exposes std.Io.Evented (Linux or Darwin on aarch64/x86_64/riscv64); pass a supported -Dtarget such as -Dtarget=x86_64-linux (`just check-evented` does this on Windows).");
        step.dependOn(&fail.step);
        return;
    }
    const canary = b.addObject(.{
        .name = "evented-canary",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/evented_canary.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    canary.expect_errors = .{ .contains = expected_error };
    step.dependOn(&canary.step);
}

/// Mirrors `std.Io.Evented`'s own selection (std/Io.zig): a fiber-capable
/// arch, then Uring on Linux and Dispatch on Darwin. The BSDs' Kqueue is left
/// out because this canary pins the Uring/Dispatch defect.
fn exposesEvented(t: std.Target) bool {
    const fiber = switch (t.cpu.arch) {
        .aarch64, .riscv64, .x86_64 => true,
        else => false,
    };
    return fiber and (t.os.tag == .linux or t.os.tag.isDarwin());
}
