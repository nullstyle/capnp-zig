//! Release drift hook: `just check-release-drift <prev-tag> [version]`, or
//! `zig build release-drift -- --prev <ref> [--version X.Y.Z] [--head <ref>]`.
//!
//! `check-api` and `check-generated-shape` prove that the committed snapshot
//! files match the tree. They cannot say whether a release declares what the
//! files did since the previous release. This tool diffs the five surface
//! snapshots between that release and now, and holds the release to the
//! semver table in RELEASING.md:
//!
//!   docs/api-snapshot.txt                    Stable: the library, frozen
//!   docs/generated-shape.txt                 Stable: generated code, frozen
//!   docs/api-snapshot-experimental.txt       Experimental
//!   docs/api-snapshot-experimental-quic.txt  Experimental (the -Dquic root)
//!   docs/generated-shape-experimental.txt    Experimental
//!
//! It FAILS when:
//!   * a Stable file removes or changes a line, and the release's CHANGELOG
//!     section has no `### Breaking` entry (a changed line is one removal
//!     plus one addition);
//!   * any of the five files changed, and the bump is a patch;
//!   * the version goes backwards.
//! It WARNS when an Experimental file loses lines and the `### Breaking`
//! entry names nothing Experimental, when a `### Breaking` entry has no
//! Migration paragraph, and when the CHANGELOG section is missing.
//!
//! A file that does not exist at the previous release counts as empty, so
//! all its lines are added. Header lines (`#`) and blank lines are not
//! surface, and line order does not matter.
//!
//! The release's CHANGELOG section is `## [<version>]` when one exists, and
//! `## [Unreleased]` otherwise. The version is `--version`, else the
//! `.version` in build.zig.zon. When it equals the previous release's (a
//! preflight run before the version sweep), the bump is not known yet: the
//! tool checks `[Unreleased]` and prints the bump the drift needs.
//!
//! The new side is the working tree, or `--head <ref>` to replay a past
//! release (`--prev v0.18.0 --head v0.19.0`). The previous side always
//! comes from `git show <prev>:<path>`, so the tool runs from the
//! repository root (the build step sets that).

const std = @import("std");

pub const Tier = enum { stable, experimental };

pub const Snapshot = struct { path: []const u8, tier: Tier };

/// The files the hook reads. A Stable file is a frozen contract; the
/// Experimental ones are kept current but not frozen.
pub const snapshots = [_]Snapshot{
    .{ .path = "docs/api-snapshot.txt", .tier = .stable },
    .{ .path = "docs/generated-shape.txt", .tier = .stable },
    .{ .path = "docs/api-snapshot-experimental.txt", .tier = .experimental },
    .{ .path = "docs/api-snapshot-experimental-quic.txt", .tier = .experimental },
    .{ .path = "docs/generated-shape-experimental.txt", .tier = .experimental },
};

/// How many removed lines of each Stable file the report lists.
pub const max_listed_removals = 12;

// ---------------------------------------------------------------------------
// The line diff.
// ---------------------------------------------------------------------------

pub const LineDiff = struct {
    /// Lines of the new text that the old text lacks.
    added: usize = 0,
    /// Lines of the old text that the new text lacks. A changed line counts
    /// here once and in `added` once.
    removed: usize = 0,
    /// The first removed lines, in the old text's order. Slices of the old
    /// text; the slice itself is owned.
    removed_sample: []const []const u8 = &.{},

    pub fn deinit(self: LineDiff, gpa: std.mem.Allocator) void {
        gpa.free(self.removed_sample);
    }
};

pub const DiffError = std.mem.Allocator.Error;

/// The surface lines of a snapshot: no header comments, no blank lines, and
/// no trailing CR.
const SurfaceLines = struct {
    rest: std.mem.SplitIterator(u8, .scalar),

    fn init(text: []const u8) SurfaceLines {
        return .{ .rest = std.mem.splitScalar(u8, text, '\n') };
    }

    fn next(self: *SurfaceLines) ?[]const u8 {
        while (self.rest.next()) |raw| {
            const line = std.mem.trimEnd(u8, raw, "\r");
            if (line.len == 0 or line[0] == '#') continue;
            return line;
        }
        return null;
    }
};

/// Compares two snapshot texts as multisets of surface lines, so order,
/// headers, blank lines and line ends do not count.
pub fn diffLines(gpa: std.mem.Allocator, old: []const u8, new: []const u8, max_listed: usize) DiffError!LineDiff {
    var unmatched: std.StringHashMapUnmanaged(usize) = .empty;
    defer unmatched.deinit(gpa);

    var new_lines = SurfaceLines.init(new);
    while (new_lines.next()) |line| {
        const slot = try unmatched.getOrPut(gpa, line);
        slot.value_ptr.* = if (slot.found_existing) slot.value_ptr.* + 1 else 1;
    }

    var sample: std.ArrayList([]const u8) = .empty;
    errdefer sample.deinit(gpa);
    var removed: usize = 0;
    var old_lines = SurfaceLines.init(old);
    while (old_lines.next()) |line| {
        if (unmatched.getPtr(line)) |count| {
            if (count.* > 0) {
                count.* -= 1;
                continue;
            }
        }
        removed += 1;
        if (sample.items.len < max_listed) try sample.append(gpa, line);
    }

    var added: usize = 0;
    var it = unmatched.valueIterator();
    while (it.next()) |count| added += count.*;

    return .{ .added = added, .removed = removed, .removed_sample = try sample.toOwnedSlice(gpa) };
}

// ---------------------------------------------------------------------------
// Versions.
// ---------------------------------------------------------------------------

/// `none` is a release that has not bumped the version yet.
pub const Bump = enum { none, patch, minor, major };

pub const BumpError = error{VersionWentBackwards};

/// Classifies `prev` -> `new` by its numbers. Pre-release and build tags are
/// ignored: the hook classifies release lines, not candidates.
pub fn classifyBump(prev: std.SemanticVersion, new: std.SemanticVersion) BumpError!Bump {
    if (new.major != prev.major) return if (new.major > prev.major) .major else error.VersionWentBackwards;
    if (new.minor != prev.minor) return if (new.minor > prev.minor) .minor else error.VersionWentBackwards;
    if (new.patch != prev.patch) return if (new.patch > prev.patch) .patch else error.VersionWentBackwards;
    return .none;
}

/// The `.version = "..."` string of a build.zig.zon, or null.
pub fn zonVersion(text: []const u8) ?[]const u8 {
    const key = ".version = \"";
    const start = (std.mem.indexOf(u8, text, key) orelse return null) + key.len;
    const len = std.mem.indexOfScalar(u8, text[start..], '"') orelse return null;
    return text[start..][0..len];
}

// ---------------------------------------------------------------------------
// The CHANGELOG.
// ---------------------------------------------------------------------------

pub const Changelog = struct {
    /// The section the release is checked against: a version, or
    /// "Unreleased". Borrowed from the caller.
    section: []const u8,
    /// False when the CHANGELOG has no such section.
    found: bool = false,
    /// The section has a `### Breaking` heading with an entry under it.
    breaking: bool = false,
    /// The Breaking entries mention "Experimental": a Breaking
    /// (Experimental) entry, as `rpc.events.Event` gaining a variant was.
    breaking_experimental: bool = false,
    /// The Breaking entries carry a Migration paragraph.
    breaking_migration: bool = false,
};

/// The body of `## [<name>]` (the lines below its heading, up to the next
/// `## ` heading), or null.
fn sectionBody(text: []const u8, name: []const u8) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, text, '\n');
    var body_start: ?usize = null;
    while (lines.next()) |raw| {
        const line = std.mem.trimEnd(u8, raw, "\r");
        const line_start = @intFromPtr(raw.ptr) - @intFromPtr(text.ptr);
        if (body_start) |start| {
            if (std.mem.startsWith(u8, line, "## ")) return text[start..line_start];
            continue;
        }
        if (!std.mem.startsWith(u8, line, "## [")) continue;
        const rest = line["## [".len..];
        if (rest.len > name.len and std.mem.startsWith(u8, rest, name) and rest[name.len] == ']') {
            body_start = @min(text.len, line_start + raw.len + 1);
        }
    }
    if (body_start) |start| return text[start..];
    return null;
}

/// The text under a section's `### Breaking` heading, up to the next `###`
/// heading, or null.
fn breakingBody(section: []const u8) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, section, '\n');
    var body_start: ?usize = null;
    while (lines.next()) |raw| {
        const line = std.mem.trimEnd(u8, raw, "\r");
        const line_start = @intFromPtr(raw.ptr) - @intFromPtr(section.ptr);
        if (body_start) |start| {
            if (std.mem.startsWith(u8, line, "### ")) return section[start..line_start];
            continue;
        }
        if (std.mem.startsWith(u8, line, "### Breaking")) {
            body_start = @min(section.len, line_start + raw.len + 1);
        }
    }
    if (body_start) |start| return section[start..];
    return null;
}

/// Reads the release's section: `## [<version>]` when it exists, else
/// `## [Unreleased]`.
pub fn readChangelog(text: []const u8, version: ?[]const u8) Changelog {
    var name: []const u8 = "Unreleased";
    var body = sectionBody(text, name);
    if (version) |v| {
        if (sectionBody(text, v)) |own| {
            name = v;
            body = own;
        }
    }
    var result: Changelog = .{ .section = name, .found = body != null };
    const breaking = breakingBody(body orelse return result) orelse return result;
    result.breaking = std.mem.trim(u8, breaking, " \t\r\n").len > 0;
    result.breaking_experimental = result.breaking and std.mem.indexOf(u8, breaking, "Experimental") != null;
    result.breaking_migration = result.breaking and std.mem.indexOf(u8, breaking, "Migration") != null;
    return result;
}

// ---------------------------------------------------------------------------
// The rules.
// ---------------------------------------------------------------------------

pub const FileDrift = struct {
    path: []const u8,
    tier: Tier,
    /// The file did not exist at the previous release; its lines all count
    /// as added.
    absent_at_prev: bool,
    diff: LineDiff,
};

pub const Kind = enum {
    /// A Stable file removed or changed a line, and the section has no
    /// `### Breaking` entry.
    stable_removal_without_breaking,
    /// A snapshot file changed, and the bump is a patch.
    patch_with_drift,
    /// An Experimental file lost lines, and no Breaking entry names
    /// anything Experimental.
    experimental_removal_without_breaking_entry,
    /// A `### Breaking` entry without a Migration paragraph.
    breaking_without_migration,
    /// The CHANGELOG has no section for this release.
    changelog_section_missing,
    /// The version is not bumped yet, and the files drifted.
    bump_pending,
};

pub const Severity = enum { fail, warn, note };

pub fn severity(kind: Kind) Severity {
    return switch (kind) {
        .stable_removal_without_breaking, .patch_with_drift => .fail,
        .experimental_removal_without_breaking_entry, .breaking_without_migration, .changelog_section_missing => .warn,
        .bump_pending => .note,
    };
}

pub const Finding = struct {
    kind: Kind,
    /// The snapshot file, for a per-file finding.
    path: ?[]const u8 = null,
};

/// Applies the rules. Per-file findings come first, in `drifts` order.
/// The caller frees the result.
pub fn evaluate(gpa: std.mem.Allocator, bump: Bump, drifts: []const FileDrift, changelog: Changelog) std.mem.Allocator.Error![]Finding {
    var findings: std.ArrayList(Finding) = .empty;
    errdefer findings.deinit(gpa);

    var any_drift = false;
    for (drifts) |d| {
        if (d.diff.added + d.diff.removed > 0) any_drift = true;
        if (d.diff.removed == 0) continue;
        switch (d.tier) {
            .stable => if (!changelog.breaking) {
                try findings.append(gpa, .{ .kind = .stable_removal_without_breaking, .path = d.path });
            },
            .experimental => if (!changelog.breaking_experimental) {
                try findings.append(gpa, .{ .kind = .experimental_removal_without_breaking_entry, .path = d.path });
            },
        }
    }
    if (any_drift) switch (bump) {
        .patch => try findings.append(gpa, .{ .kind = .patch_with_drift }),
        .none => try findings.append(gpa, .{ .kind = .bump_pending }),
        .minor, .major => {},
    };
    if (changelog.breaking and !changelog.breaking_migration) {
        try findings.append(gpa, .{ .kind = .breaking_without_migration });
    }
    if (!changelog.found) try findings.append(gpa, .{ .kind = .changelog_section_missing });
    return findings.toOwnedSlice(gpa);
}

// ---------------------------------------------------------------------------
// The command line: git and file reads, then the report.
// ---------------------------------------------------------------------------

const max_file_bytes = 64 * 1024 * 1024;

const Git = struct {
    gpa: std.mem.Allocator,
    io: std.Io,

    const Result = struct { ok: bool, stdout: []u8 };

    /// Runs git. A failure's stderr is printed unless `quiet` (a probe whose
    /// failure is an answer, not an error).
    fn run(git: Git, argv: []const []const u8, quiet: bool) !Result {
        const result = try std.process.run(git.gpa, git.io, .{
            .argv = argv,
            .stdout_limit = .limited(max_file_bytes),
            .stderr_limit = .limited(1024 * 1024),
        });
        defer git.gpa.free(result.stderr);
        const ok = result.term == .exited and result.term.exited == 0;
        if (!ok and !quiet and result.stderr.len > 0) std.debug.print("{s}", .{result.stderr});
        return .{ .ok = ok, .stdout = result.stdout };
    }

    fn verifyRef(git: Git, ref: []const u8) !void {
        const spec = try std.fmt.allocPrint(git.gpa, "{s}^{{commit}}", .{ref});
        defer git.gpa.free(spec);
        const result = try git.run(&.{ "git", "rev-parse", "--verify", "--quiet", spec }, false);
        defer git.gpa.free(result.stdout);
        if (!result.ok) {
            std.debug.print("release-drift: `{s}` is not a commit or tag in this repository\n", .{ref});
            return error.UnknownRef;
        }
    }

    /// The file at `ref`, or null when `ref` has no such path.
    fn show(git: Git, ref: []const u8, path: []const u8) !?[]u8 {
        const spec = try std.fmt.allocPrint(git.gpa, "{s}:{s}", .{ ref, path });
        defer git.gpa.free(spec);
        const exists = try git.run(&.{ "git", "cat-file", "-e", spec }, true);
        git.gpa.free(exists.stdout);
        if (!exists.ok) return null;
        const result = try git.run(&.{ "git", "show", spec }, false);
        if (!result.ok) {
            git.gpa.free(result.stdout);
            return error.GitShowFailed;
        }
        return result.stdout;
    }
};

/// The new side: the working tree, or a git ref.
const Head = union(enum) {
    tree,
    ref: []const u8,

    fn label(head: Head) []const u8 {
        return switch (head) {
            .tree => "the working tree",
            .ref => |r| r,
        };
    }

    /// The file, or null when it does not exist at `--head`. A file missing
    /// from the working tree is an error: the run is in the wrong
    /// directory, or a snapshot was deleted by hand.
    fn read(head: Head, git: Git, path: []const u8) !?[]u8 {
        switch (head) {
            .ref => |r| return git.show(r, path),
            .tree => return std.Io.Dir.cwd().readFileAlloc(git.io, path, git.gpa, .limited(max_file_bytes)) catch |err| {
                std.debug.print("release-drift: cannot read {s} ({t}); run from the repository root\n", .{ path, err });
                return err;
            },
        }
    }
};

fn usage() error{InvalidArgument} {
    std.debug.print(
        \\usage: release-drift --prev <tag> [--version X.Y.Z] [--head <ref>]
        \\  --prev     the previous release (a tag or any commit)
        \\  --version  the version being released (default: build.zig.zon)
        \\  --head     read the new side from a ref, not the working tree
        \\
    , .{});
    return error.InvalidArgument;
}

fn parseVersion(text: []const u8, what: []const u8) !std.SemanticVersion {
    return std.SemanticVersion.parse(text) catch {
        std.debug.print("release-drift: {s} `{s}` is not a semantic version\n", .{ what, text });
        return error.InvalidVersion;
    };
}

fn bumpName(bump: Bump) []const u8 {
    return switch (bump) {
        .none => "no bump yet",
        .patch => "patch bump",
        .minor => "minor bump",
        .major => "major bump",
    };
}

fn yesNo(value: bool) []const u8 {
    return if (value) "yes" else "no";
}

pub fn main(init: std.process.Init) !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    defer _ = debug_allocator.deinit();
    const gpa = debug_allocator.allocator();
    const git: Git = .{ .gpa = gpa, .io = init.io };

    var prev: ?[]const u8 = null;
    var version_arg: ?[]const u8 = null;
    var head: Head = .tree;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa);
    defer args.deinit();
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--prev")) {
            prev = args.next() orelse return usage();
        } else if (std.mem.eql(u8, arg, "--version")) {
            version_arg = args.next() orelse return usage();
        } else if (std.mem.eql(u8, arg, "--head")) {
            head = .{ .ref = args.next() orelse return usage() };
        } else {
            std.debug.print("release-drift: unknown argument `{s}`\n", .{arg});
            return usage();
        }
    }
    const prev_ref = prev orelse return usage();
    if (prev_ref.len == 0) return usage();
    try git.verifyRef(prev_ref);
    switch (head) {
        .tree => {},
        .ref => |r| try git.verifyRef(r),
    }

    // Versions: the previous release's manifest, and --version or ours.
    const prev_zon = try git.show(prev_ref, "build.zig.zon") orelse {
        std.debug.print("release-drift: {s} has no build.zig.zon\n", .{prev_ref});
        return error.NoManifest;
    };
    defer gpa.free(prev_zon);
    const prev_version_text = zonVersion(prev_zon) orelse return error.NoManifestVersion;
    const head_zon = try head.read(git, "build.zig.zon") orelse return error.NoManifest;
    defer gpa.free(head_zon);
    const version_text = version_arg orelse (zonVersion(head_zon) orelse return error.NoManifestVersion);
    const bump = classifyBump(
        try parseVersion(prev_version_text, "the previous version"),
        try parseVersion(version_text, "the release version"),
    ) catch |err| {
        std.debug.print("release-drift: FAIL the version goes backwards: {s} ({s}) -> {s}\n", .{ prev_ref, prev_version_text, version_text });
        return err;
    };

    // The five files on both sides.
    var texts: [snapshots.len * 2]?[]u8 = @splat(null);
    defer for (texts) |t| if (t) |bytes| gpa.free(bytes);
    var drifts: [snapshots.len]FileDrift = undefined;
    var diffed: usize = 0;
    defer for (drifts[0..diffed]) |d| d.diff.deinit(gpa);
    for (snapshots, 0..) |snapshot, i| {
        texts[2 * i] = try git.show(prev_ref, snapshot.path);
        texts[2 * i + 1] = try head.read(git, snapshot.path);
        drifts[i] = .{
            .path = snapshot.path,
            .tier = snapshot.tier,
            .absent_at_prev = texts[2 * i] == null,
            .diff = try diffLines(gpa, texts[2 * i] orelse "", texts[2 * i + 1] orelse "", max_listed_removals),
        };
        diffed += 1;
    }

    const changelog_owned = try head.read(git, "CHANGELOG.md");
    defer if (changelog_owned) |text| gpa.free(text);
    const changelog_text = changelog_owned orelse "";
    // Before the sweep the version is still the previous release's, whose
    // section is history: check [Unreleased].
    const changelog = readChangelog(changelog_text, if (bump == .none) null else version_text);

    const findings = try evaluate(gpa, bump, &drifts, changelog);
    defer gpa.free(findings);

    // The report.
    std.debug.print("release-drift: {s} ({s}) -> {s} ({s}): {s}\n", .{
        prev_ref, prev_version_text, head.label(), version_text, bumpName(bump),
    });
    for (drifts, 0..) |d, i| {
        const absent_at_head = texts[2 * i + 1] == null;
        std.debug.print("  {s: <40} {s: <12} +{d} -{d}{s}\n", .{
            d.path,
            if (d.tier == .stable) "Stable" else "Experimental",
            d.diff.added,
            d.diff.removed,
            if (d.absent_at_prev and absent_at_head)
                " (absent on both sides)"
            else if (d.absent_at_prev)
                " (new since the previous release)"
            else if (absent_at_head)
                " (deleted since the previous release)"
            else
                "",
        });
    }
    if (changelog.found) {
        std.debug.print("  CHANGELOG [{s}]: ### Breaking: {s} (names Experimental: {s}; Migration: {s})\n", .{
            changelog.section, yesNo(changelog.breaking), yesNo(changelog.breaking_experimental), yesNo(changelog.breaking_migration),
        });
    } else {
        std.debug.print("  CHANGELOG [{s}]: no such section\n", .{changelog.section});
    }

    var fail_count: usize = 0;
    var warn_count: usize = 0;
    for (findings) |f| {
        switch (severity(f.kind)) {
            .fail => fail_count += 1,
            .warn => warn_count += 1,
            .note => {},
        }
        const tag = switch (severity(f.kind)) {
            .fail => "FAIL",
            .warn => "WARN",
            .note => "NOTE",
        };
        switch (f.kind) {
            .stable_removal_without_breaking => {
                const d = for (drifts) |entry| {
                    if (std.mem.eql(u8, entry.path, f.path.?)) break entry;
                } else unreachable;
                std.debug.print(
                    "{s} {s} removes or changes {d} Stable line(s), and CHANGELOG [{s}] has no `### Breaking` entry. A frozen contract moved: add a Breaking entry with a Migration paragraph and bump the minor version (RELEASING.md), or restore the lines.\n",
                    .{ tag, f.path.?, d.diff.removed, changelog.section },
                );
                for (d.diff.removed_sample) |line| std.debug.print("      - {s}\n", .{line});
                if (d.diff.removed > d.diff.removed_sample.len) {
                    std.debug.print("      ... and {d} more (git diff {s} -- {s})\n", .{ d.diff.removed - d.diff.removed_sample.len, prev_ref, f.path.? });
                }
            },
            .patch_with_drift => std.debug.print(
                "{s} {s} -> {s} is a patch bump, but the snapshot files changed. Any surface change, on any tier, is a minor bump (RELEASING.md).\n",
                .{ tag, prev_version_text, version_text },
            ),
            .experimental_removal_without_breaking_entry => std.debug.print(
                "{s} {s} loses lines, and no `### Breaking` entry in CHANGELOG [{s}] names anything Experimental. If a consumer could see the change, add a Breaking (Experimental) entry with a Migration note.\n",
                .{ tag, f.path.?, changelog.section },
            ),
            .breaking_without_migration => std.debug.print(
                "{s} CHANGELOG [{s}] has a `### Breaking` entry with no Migration paragraph (RELEASING.md).\n",
                .{ tag, changelog.section },
            ),
            .changelog_section_missing => std.debug.print(
                "{s} CHANGELOG.md has no `## [{s}]` section.\n",
                .{ tag, changelog.section },
            ),
            .bump_pending => std.debug.print(
                "{s} build.zig.zon still says {s}: the snapshot files changed, so this release needs at least a minor bump. Pass the version to check it now: `just check-release-drift {s} X.Y.Z`.\n",
                .{ tag, version_text, prev_ref },
            ),
        }
    }

    if (fail_count > 0) {
        std.debug.print("release-drift: FAILED ({d} failure(s), {d} warning(s))\n", .{ fail_count, warn_count });
        std.process.exit(1);
    }
    std.debug.print("release-drift: OK ({d} warning(s))\n", .{warn_count});
}
