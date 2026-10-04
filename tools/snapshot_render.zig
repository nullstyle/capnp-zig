//! Comptime declaration walker and line renderer for surface snapshots.
//!
//! tools/api_snapshot.zig uses it to snapshot the library's public API (the
//! three docs/api-snapshot*.txt files). It is also built for a generated-shape
//! snapshot (the surface of generated bindings, with const values rendered),
//! and it is the seam another package can reuse to snapshot its own
//! surface: this file imports only
//! `std`, never `capnpc-zig`. The build creates it as the `snapshot-render`
//! module with no imports, so an `@import("capnpc-zig")` here fails to
//! compile.
//!
//! A gate describes its surface with a `Config` table and reads the results
//! off `Snapshot(config)`:
//!
//!     const render = @import("snapshot-render");
//!     const snap = render.Snapshot(.{
//!         .root = @import("my-lib"),
//!         .root_path = "my-lib",
//!         .max_depth = 8,
//!         .stable_rules = &.{render.prefix("my-lib.wire")},
//!     });
//!     // snap.entries, snap.stable_lines, snap.experimental_lines,
//!     // snap.closure_violations, snap.dead_rules
//!     const text = try render.renderSnapshot(gpa, snap.stable_lines, header);
//!
//! Every reachable declaration renders as one `<path>: <description>` line.
//! Each container the walk descends into also gets one line per field,
//! union variant or enumerant. `renderSnapshot` normalizes and sorts the
//! lines, so the output does not depend on declaration order or on the
//! compiler's anonymous-type counters.
//!
//! Tier contract: a path is Stable ONLY when a `stable_rules` entry matches
//! it and no `experimental_overrides` entry does. Every other path is
//! Experimental, so a new declaration is never frozen by accident.
//!
//! Limits, shared by every gate that uses this module:
//!   * `anytype` parameters stay unresolved, so such a signature pins only
//!     its arity, and the closure check skips it.
//!   * A generic function's inferred error set cannot be resolved, so its
//!     line keeps the opaque `@typeName` rendering.
//!   * A `pub var` in the walked surface is a compile error: the walker
//!     loads every declaration at comptime.
//!   * The default `descend` recognizes std by name only. Zig 0.17 names
//!     std's own files without a `std.` prefix (`mem.Allocator`), so a
//!     re-exported std container below the `std` root IS walked. The
//!     library re-exports none. A surface that does should pass its own
//!     `descend`, for example one that accepts only its own types.

const std = @import("std");

// ---------------------------------------------------------------------------
// Tier rules.
//
// A rule matches on the declaration PATH (the text left of the first ": " in a
// rendered line), never on the signature. Two match kinds:
//
//   .prefix — path equals the rule OR begins with `rule ++ "."`. Freezes a
//             whole subtree (a module or a type and all its members).
//   .exact  — path equals the rule exactly. Freezes ONE symbol without
//             dragging in its siblings or an enclosing container's other
//             members.
// ---------------------------------------------------------------------------

pub const MatchKind = enum { prefix, exact };
pub const Rule = struct { kind: MatchKind, path: []const u8 };

/// A rule that matches `path` and everything under it.
pub fn prefix(path: []const u8) Rule {
    return .{ .kind = .prefix, .path = path };
}

/// A rule that matches `path` only.
pub fn exact(path: []const u8) Rule {
    return .{ .kind = .exact, .path = path };
}

pub fn matchesRule(comptime path: []const u8, comptime rules: []const Rule) bool {
    inline for (rules) |rule| {
        if (ruleMatches(rule, path)) return true;
    }
    return false;
}

fn ruleMatches(comptime rule: Rule, comptime path: []const u8) bool {
    switch (rule.kind) {
        .exact => return std.mem.eql(u8, path, rule.path),
        .prefix => {
            if (std.mem.eql(u8, path, rule.path)) return true;
            return path.len > rule.path.len and
                std.mem.startsWith(u8, path, rule.path) and
                path[rule.path.len] == '.';
        },
    }
}

// ---------------------------------------------------------------------------
// The configuration table.
// ---------------------------------------------------------------------------

pub const Config = struct {
    /// The container the walk starts from: a module (`@import("x")`) or any
    /// namespace type.
    root: type,
    /// The first path segment of every line, standing for `root`.
    root_path: []const u8,
    /// How many container levels below `root` the walk descends. A
    /// container at the limit still gets its own line and its field lines,
    /// but its declarations are not walked.
    max_depth: usize,
    /// Paths that are Stable. Every other path is Experimental.
    stable_rules: []const Rule = &.{},
    /// Paths that are Experimental even when a Stable rule matches them.
    /// These are checked first.
    experimental_overrides: []const Rule = &.{},
    /// Which containers the walk descends into, for their fields and their
    /// declarations. A container it does not descend into still gets its
    /// own line. The default skips the std and builtin roots (see the
    /// module limits).
    descend: fn (comptime type) bool = descendUnlessForeign,
    /// When true, a scalar const declaration also renders its value:
    /// `Persistent.interface_id: const u64 = 14468694717054801553`. Ints,
    /// floats, bools, enums, enum literals, optionals of those, and strings
    /// of up to `max_const_string_bytes` bytes render. Other consts (and
    /// longer strings) render their type only, as they do when this is
    /// false. The library snapshots keep it false so their files do not
    /// change. A generated-shape snapshot sets it, so a changed interface
    /// id, method ordinal or schema constant moves a line.
    render_const_values: bool = false,
};

/// The default `Config.descend`: walk into everything `foreignType` does not
/// name as std or builtin (re-exports that are not ours).
pub fn descendUnlessForeign(comptime T: type) bool {
    return !foreignType(T);
}

/// The longest string const whose value `render_const_values` spells out.
pub const max_const_string_bytes = 64;

// ---------------------------------------------------------------------------
// Type predicates.
// ---------------------------------------------------------------------------

pub fn isContainer(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .@"struct", .@"enum", .@"union", .@"opaque" => true,
        else => false,
    };
}

pub fn containerKind(comptime T: type) []const u8 {
    return switch (@typeInfo(T)) {
        .@"struct" => "struct",
        .@"enum" => "enum",
        .@"union" => "union",
        .@"opaque" => "opaque",
        else => unreachable,
    };
}

/// True for types that are not ours (std re-exports etc.). On Zig 0.17 this
/// matches only the `std` and `builtin` roots: `@typeName` of a nested std
/// type has no `std.` prefix (see the module limits).
pub fn foreignType(comptime T: type) bool {
    const name = @typeName(T);
    return std.mem.startsWith(u8, name, "std.") or
        std.mem.startsWith(u8, name, "builtin.") or
        std.mem.eql(u8, name, "std") or
        std.mem.eql(u8, name, "builtin");
}

pub fn contains(comptime seen: []const type, comptime T: type) bool {
    for (seen) |S| {
        if (S == T) return true;
    }
    return false;
}

/// Strip the wrappers a signature puts around a nominal type.
pub fn peel(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .pointer => |pi| peel(pi.child),
        .optional => |oi| peel(oi.child),
        .error_union => |eu| peel(eu.payload),
        else => T,
    };
}

// ---------------------------------------------------------------------------
// Renderers.
// ---------------------------------------------------------------------------

/// Render a function signature, expanding any inferred error set.
///
/// `@typeName` renders an inferred error set as the self-referential expression
/// `@typeInfo(@typeInfo(@TypeOf(f)).@"fn".return_type.?).error_union.error_set`,
/// which is IDENTICAL no matter what the set contains — 325 of the frozen lines
/// rendered that way, so adding, removing, or renaming an error on nearly half
/// the Stable surface passed the gate unchanged while breaking every consumer's
/// `catch |err| switch (err)`. Expanding the set to a sorted `error{...}` list
/// makes those changes visible.
///
/// `anytype` parameters remain unpinned: they are genuinely unresolved until
/// instantiation, so a signature containing one pins only its arity. That
/// residual hole is documented in docs/supported-surface.md rather than hidden.
pub fn renderErrorSet(comptime E: type) []const u8 {
    const info = @typeInfo(E).error_set;
    const names = info.error_names orelse return "anyerror";

    comptime var sorted: []const []const u8 = &.{};
    for (names) |name| sorted = sorted ++ [_][]const u8{name};
    // Insertion sort: the rendered set must not depend on declaration order.
    comptime var i: usize = 1;
    inline while (i < sorted.len) : (i += 1) {
        comptime var j = i;
        inline while (j > 0 and std.mem.lessThan(u8, sorted[j], sorted[j - 1])) : (j -= 1) {
            const swapped = sorted[j - 1];
            var next: []const []const u8 = sorted[0 .. j - 1];
            next = next ++ [_][]const u8{sorted[j]} ++ [_][]const u8{swapped};
            if (j + 1 < sorted.len) next = next ++ sorted[j + 1 ..];
            sorted = next;
        }
    }

    comptime var out: []const u8 = "error{";
    for (sorted, 0..) |name, idx| {
        if (idx != 0) out = out ++ ",";
        out = out ++ name;
    }
    return out ++ "}";
}

pub fn renderFnType(comptime FnType: type) []const u8 {
    const fn_info = @typeInfo(FnType).@"fn";
    // A GENERIC function's inferred error set cannot be resolved here: it
    // depends on the instantiation, and asking for it is a compile error
    // ("cannot resolve inferred error set of generic function type"). Those
    // lines keep the opaque rendering and stay unpinned; the count is recorded
    // in docs/supported-surface.md so the residual hole is a known quantity
    // rather than a surprise.
    if (fn_info.is_generic) return @typeName(FnType);
    const ret = fn_info.return_type orelse return @typeName(FnType);
    switch (@typeInfo(ret)) {
        .error_union => |eu| {
            // Rebuild the signature with the expanded set. The parameter list is
            // taken verbatim from @typeName so `anytype`/`comptime` render
            // exactly as before and only the error set changes.
            const full = @typeName(FnType);
            const open = std.mem.indexOfScalar(u8, full, '(') orelse return full;
            // Match the parameter list's OWN closing paren by depth. A plain
            // lastIndexOf(')') lands inside the rendered return type — which for
            // an inferred error set is itself a paren-heavy
            // `@typeInfo(@typeInfo(@TypeOf(f)).@"fn".return_type.?)` expression —
            // and splices that fragment into the output.
            comptime var depth: usize = 0;
            comptime var close: ?usize = null;
            inline for (full[open..], open..) |ch, idx| {
                if (ch == '(') depth += 1;
                if (ch == ')') {
                    depth -= 1;
                    if (depth == 0) {
                        close = idx;
                        break;
                    }
                }
            }
            const close_idx = close orelse return full;
            const params = full[open .. close_idx + 1];
            return "fn " ++ params ++ " " ++ renderErrorSet(eu.error_set) ++ "!" ++ @typeName(eu.payload);
        },
        else => return @typeName(FnType),
    }
}

/// Render a typedef (a `type` declaration that is not a container).
///
/// A typedef whose value is a function (or a pointer to one) is still a
/// signature consumers code against, so its error set is expanded too, rather
/// than leaving the opaque @typeName rendering.
pub fn renderTypedef(comptime D: type) []const u8 {
    return switch (@typeInfo(D)) {
        .@"fn" => renderFnType(D),
        .pointer => |pi| if (@typeInfo(pi.child) == .@"fn")
            "*const " ++ renderFnType(pi.child)
        else
            @typeName(D),
        else => @typeName(D),
    };
}

/// Render a field's default-value initializer, or "" when it has none.
///
/// The default VALUE matters, not just its presence: changing
/// `Connection.Options.read_buffer_size` from one number to another is a
/// behavior change for every consumer who relied on it, and a name-only
/// snapshot could not see it. Values are rendered for the scalar kinds where a
/// default is meaningful and comparable; anything else records that a default
/// exists without trying to spell it, which still pins presence.
pub fn defaultSuffix(
    comptime FieldType: type,
    comptime attrs: std.builtin.Type.Struct.FieldAttributes,
) []const u8 {
    const value = attrs.defaultValue(FieldType) orelse return "";
    return " = " ++ renderValue(FieldType, value);
}

/// Render a comptime-known default. Optionals are unwrapped rather than reported
/// as merely present: `default_call_timeout_ms: ?u64 = 30000` is a number
/// consumers depend on, and collapsing it to "<non-null default>" would let a
/// 30s → 60s change pass the gate. Aggregates render as `<default>` — their own
/// fields are pinned separately by their own snapshot lines.
pub fn renderValue(comptime T: type, comptime value: T) []const u8 {
    return switch (@typeInfo(T)) {
        .int, .comptime_int => std.fmt.comptimePrint("{d}", .{value}),
        .float, .comptime_float => std.fmt.comptimePrint("{d}", .{value}),
        .bool => if (value) "true" else "false",
        .@"enum" => "." ++ @tagName(value),
        .void => "{}",
        .optional => |oi| if (value) |inner| renderValue(oi.child, inner) else "null",
        else => "<default>",
    };
}

/// Render the value of a const declaration for `render_const_values`, or
/// null when the value is not a scalar this renderer spells.
///
/// Unlike `renderValue`, aggregates return null instead of `<default>`: a
/// const line already names its type, and a placeholder would add nothing.
/// An enum value with no tag (a non-exhaustive enum) renders as
/// `@fromBackingInt(n)`. A string is a `[]const u8`, a pointer to a `u8` array,
/// or a `u8` array. It renders as a Zig string literal when it has at most
/// `max_const_string_bytes` bytes; a longer one returns null.
pub fn renderConstValue(comptime T: type, comptime value: T) ?[]const u8 {
    return switch (@typeInfo(T)) {
        .int, .comptime_int, .float, .comptime_float, .bool, .void => renderValue(T, value),
        .@"enum" => if (std.enums.tagName(T, value)) |tag|
            "." ++ tag
        else
            std.fmt.comptimePrint("@fromBackingInt({d})", .{@backingInt(value)}),
        .enum_literal => "." ++ @tagName(value),
        .optional => |oi| if (value) |inner| renderConstValue(oi.child, inner) else "null",
        .pointer => |pi| switch (pi.size) {
            .slice => if (pi.child == u8) renderString(value) else null,
            .one => switch (@typeInfo(pi.child)) {
                .array => |ai| if (ai.child == u8) renderString(value) else null,
                else => null,
            },
            else => null,
        },
        .array => |ai| if (ai.child == u8) renderString(&value) else null,
        else => null,
    };
}

fn renderString(comptime bytes: []const u8) ?[]const u8 {
    if (bytes.len > max_const_string_bytes) return null;
    return std.fmt.comptimePrint("\"{f}\"", .{std.zig.fmtString(bytes)});
}

/// A rendered declaration line plus the path that produced it (kept so the
/// categorizer can route lines after they are collected).
pub const Entry = struct { path: []const u8, line: []const u8 };

/// Emit one line per field/enumerant of a container.
///
/// The walker enumerates DECLARATIONS only, so before this every frozen struct
/// was pinned by name alone: removing a field from `PeerLimits`, reordering a
/// union, or changing a default was invisible to `check-api`. Fields render
/// under the container's path, so the existing tier rules route them — a
/// `.prefix` rule (a frozen module) sweeps its types' fields into the contract,
/// while a `.exact` rule (e.g. `Peer` itself) deliberately does not, keeping 73
/// fields of internal peer state out of the frozen surface.
pub fn fieldEntries(
    comptime T: type,
    comptime path: []const u8,
    comptime entries: *[]const Entry,
) void {
    switch (@typeInfo(T)) {
        .@"struct" => |info| {
            for (info.field_names, info.field_types, info.field_attrs) |name, FieldType, attrs| {
                const fpath = path ++ "." ++ name;
                entries.* = entries.* ++ [_]Entry{.{
                    .path = fpath,
                    .line = fpath ++ ": field " ++ @typeName(FieldType) ++ defaultSuffix(FieldType, attrs),
                }};
            }
        },
        .@"union" => |info| {
            for (info.field_names, info.field_types) |name, FieldType| {
                const fpath = path ++ "." ++ name;
                entries.* = entries.* ++ [_]Entry{.{
                    .path = fpath,
                    .line = fpath ++ ": variant " ++ @typeName(FieldType),
                }};
            }
        },
        .@"enum" => |info| {
            for (info.field_names, info.field_values) |name, value| {
                const fpath = path ++ "." ++ name;
                entries.* = entries.* ++ [_]Entry{.{
                    .path = fpath,
                    .line = fpath ++ ": enumerant = " ++ std.fmt.comptimePrint("{d}", .{value}),
                }};
            }
        },
        else => {},
    }
}

// ---------------------------------------------------------------------------
// The configured walk.
// ---------------------------------------------------------------------------

/// A container type the walk reached, plus whether any path reaching it is
/// Stable. Re-exports mean one type can sit at several paths; reachable via a
/// Stable path is what puts it in the contract.
pub const TypeTier = struct { ty: type, stable: bool };

/// A Stable function whose signature names an Experimental type.
pub const Violation = struct { decl: []const u8, offender: []const u8, role: []const u8 };

/// The snapshot of the surface `config` describes. Every result is a
/// comptime constant, computed on first use.
pub fn Snapshot(comptime config: Config) type {
    return struct {
        /// True when `path` is Stable under the config's rules.
        pub fn tierIsStable(comptime path: []const u8) bool {
            // Explicit Experimental overrides win over any Stable prefix.
            if (matchesRule(path, config.experimental_overrides)) return false;
            return matchesRule(path, config.stable_rules);
        }

        fn walk(
            comptime T: type,
            comptime path: []const u8,
            comptime depth: usize,
            comptime seen: *[]const type,
            comptime entries_out: *[]const Entry,
        ) void {
            if (depth >= config.max_depth) return;
            if (contains(seen.*, T)) return;
            seen.* = seen.* ++ [_]type{T};

            for (std.meta.declarations(T)) |decl_name| {
                const decl_path = path ++ "." ++ decl_name;
                const D = @field(T, decl_name);
                const DType = @TypeOf(D);

                if (DType == type) {
                    if (isContainer(D)) {
                        entries_out.* = entries_out.* ++ [_]Entry{.{ .path = decl_path, .line = decl_path ++ ": " ++ containerKind(D) }};
                        if (config.descend(D)) {
                            fieldEntries(D, decl_path, entries_out);
                            walk(D, decl_path, depth + 1, seen, entries_out);
                        }
                    } else {
                        entries_out.* = entries_out.* ++ [_]Entry{.{ .path = decl_path, .line = decl_path ++ ": type = " ++ renderTypedef(D) }};
                    }
                } else if (@typeInfo(DType) == .@"fn") {
                    entries_out.* = entries_out.* ++ [_]Entry{.{ .path = decl_path, .line = decl_path ++ ": " ++ renderFnType(DType) }};
                } else {
                    const value_suffix: []const u8 = if (config.render_const_values)
                        if (renderConstValue(DType, D)) |value| " = " ++ value else ""
                    else
                        "";
                    entries_out.* = entries_out.* ++ [_]Entry{.{ .path = decl_path, .line = decl_path ++ ": const " ++ @typeName(DType) ++ value_suffix }};
                }
            }
        }

        /// Every rendered line, in walk order, with its path.
        pub const entries: []const Entry = blk: {
            @setEvalBranchQuota(20_000_000);
            var seen: []const type = &.{};
            var out: []const Entry = &.{};
            walk(config.root, config.root_path, 0, &seen, &out);
            break :blk out;
        };

        /// The lines whose paths are Stable, unsorted.
        pub const stable_lines: []const []const u8 = blk: {
            @setEvalBranchQuota(20_000_000);
            var stable: []const []const u8 = &.{};
            for (entries) |entry| {
                if (tierIsStable(entry.path)) {
                    stable = stable ++ [_][]const u8{entry.line};
                }
            }
            break :blk stable;
        };

        /// The lines whose paths are Experimental, unsorted.
        pub const experimental_lines: []const []const u8 = blk: {
            @setEvalBranchQuota(20_000_000);
            var experimental: []const []const u8 = &.{};
            for (entries) |entry| {
                if (!tierIsStable(entry.path)) {
                    experimental = experimental ++ [_][]const u8{entry.line};
                }
            }
            break :blk experimental;
        };

        fn ruleMatchesAnyDeclaration(comptime rule: Rule) bool {
            for (entries) |entry| {
                if (ruleMatches(rule, entry.path)) return true;
            }
            return false;
        }

        /// Every rule that matches no rendered path, one per line, as
        /// `"\n  stable_rules: <path>"` or
        /// `"\n  experimental_overrides: <path>"`. Empty when every rule is
        /// live.
        ///
        /// A rule for a symbol that was never there (or has since been
        /// renamed) is silent: it documents a contract nobody can rely on, and
        /// it would mask a typo in a future promotion. A gate turns a
        /// non-empty value into a compile error.
        pub const dead_rules: []const u8 = blk: {
            @setEvalBranchQuota(40_000_000);
            var dead: []const u8 = "";
            for (config.stable_rules) |rule| {
                if (!ruleMatchesAnyDeclaration(rule)) dead = dead ++ "\n  stable_rules: " ++ rule.path;
            }
            for (config.experimental_overrides) |rule| {
                if (!ruleMatchesAnyDeclaration(rule)) dead = dead ++ "\n  experimental_overrides: " ++ rule.path;
            }
            break :blk dead;
        };

        fn collectTypes(
            comptime T: type,
            comptime path: []const u8,
            comptime depth: usize,
            comptime seen: *[]const type,
            comptime out: *[]const TypeTier,
        ) void {
            if (depth >= config.max_depth) return;
            if (contains(seen.*, T)) return;
            seen.* = seen.* ++ [_]type{T};

            for (std.meta.declarations(T)) |decl_name| {
                const decl_path = path ++ "." ++ decl_name;
                const D = @field(T, decl_name);
                if (@TypeOf(D) != type) continue;
                if (!isContainer(D)) continue;
                out.* = out.* ++ [_]TypeTier{.{ .ty = D, .stable = tierIsStable(decl_path) }};
                if (config.descend(D)) collectTypes(D, decl_path, depth + 1, seen, out);
            }
        }

        /// Every container type the walk reached, with its tier.
        pub const type_tiers: []const TypeTier = blk: {
            @setEvalBranchQuota(40_000_000);
            var seen: []const type = &.{};
            var out: []const TypeTier = &.{};
            collectTypes(config.root, config.root_path, 0, &seen, &out);
            break :blk out;
        };

        /// The tier of the nominal type inside `T`, or `null` when that type
        /// is not part of the walked surface (std type, primitive, ...).
        pub fn tierOfType(comptime T: type) ?bool {
            const P = peel(T);
            var found: ?bool = null;
            for (type_tiers) |entry| {
                if (entry.ty == P) {
                    if (entry.stable) return true; // any Stable path wins
                    found = false;
                }
            }
            return found;
        }

        /// Walk again, this time checking each Stable function's signature. The check
        /// has to happen inside the walk: that is the only place a declaration and its
        /// snapshot path are both in hand.
        fn collectClosure(
            comptime T: type,
            comptime path: []const u8,
            comptime depth: usize,
            comptime seen: *[]const type,
            comptime out: *[]const Violation,
        ) void {
            if (depth >= config.max_depth) return;
            if (contains(seen.*, T)) return;
            seen.* = seen.* ++ [_]type{T};

            for (std.meta.declarations(T)) |decl_name| {
                const decl_path = path ++ "." ++ decl_name;
                const D = @field(T, decl_name);
                const DType = @TypeOf(D);

                if (DType == type) {
                    if (isContainer(D) and config.descend(D)) {
                        collectClosure(D, decl_path, depth + 1, seen, out);
                    }
                    continue;
                }
                if (@typeInfo(DType) != .@"fn") continue;
                if (!tierIsStable(decl_path)) continue;

                const fn_info = @typeInfo(DType).@"fn";
                if (fn_info.is_generic) continue;

                // A method that takes or returns its OWN enclosing type is not a
                // closure violation. `ServerSession.run(self: *ServerSession)` is the
                // frozen method of a type deliberately frozen only at `.accept` and its
                // lifecycle — the receiver is the same declaration cluster, not an
                // unfrozen dependency a consumer must obtain elsewhere.
                for (fn_info.param_types) |maybe_pt| {
                    const PT = maybe_pt orelse continue;
                    if (peel(PT) == T) continue;
                    if (tierOfType(PT)) |is_stable| {
                        if (!is_stable) out.* = out.* ++ [_]Violation{.{
                            .decl = decl_path,
                            .offender = @typeName(peel(PT)),
                            .role = "parameter",
                        }};
                    }
                }
                if (fn_info.return_type) |RT| {
                    if (peel(RT) != T) {
                        if (tierOfType(RT)) |is_stable| {
                            if (!is_stable) out.* = out.* ++ [_]Violation{.{
                                .decl = decl_path,
                                .offender = @typeName(peel(RT)),
                                .role = "return",
                            }};
                        }
                    }
                }
            }
        }

        /// Every Stable function whose parameter or return type is an
        /// Experimental type of the walked surface, in walk order. Generic
        /// functions are skipped: an `anytype` parameter has no type to
        /// check until instantiation.
        pub const closure_violations: []const Violation = blk: {
            @setEvalBranchQuota(40_000_000);
            var seen: []const type = &.{};
            var out: []const Violation = &.{};
            collectClosure(config.root, config.root_path, 0, &seen, &out);
            break :blk out;
        };
    };
}

// ---------------------------------------------------------------------------
// Runtime rendering: normalize, sort, join.
// ---------------------------------------------------------------------------

fn lessThan(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

pub const PlatformAlias = struct { from: []const u8, to: []const u8 };

/// std type spellings that name the SAME logical type with a different path
/// per platform. `@typeName` reports the resolved declaration, so without this
/// the rendered surface differs by the OS that generated it and no staleness
/// gate can run in CI.
///
/// Concretely: `std.posix.sockaddr` resolves through translate-c on macOS
/// (`c.sockaddr__struct_*`, after the numeric suffix is normalized above) and
/// through `os.linux.sockaddr` on Linux. That is a property of std, not of our
/// API — `SockAddrStorage` names `std.posix.sockaddr` on every platform — so
/// the snapshot canonicalizes it rather than freezing one OS's spelling.
/// Longest/base forms come first: replacing the base rewrites the `.in` and
/// `.in6` members with it.
pub const platform_type_aliases = [_]PlatformAlias{
    .{ .from = "c.sockaddr__struct_*", .to = "posix.sockaddr" },
    .{ .from = "os.linux.sockaddr", .to = "posix.sockaddr" },
    .{ .from = "os.darwin.sockaddr", .to = "posix.sockaddr" },
    .{ .from = "os.windows.ws2_32.sockaddr", .to = "posix.sockaddr" },
};

/// Rewrite every `platform_type_aliases` spelling in `line`. Takes ownership
/// of `line` and returns a slice the caller owns.
pub fn canonicalizePlatformTypes(allocator: std.mem.Allocator, line: []u8) std.mem.Allocator.Error![]u8 {
    var current = line;
    for (platform_type_aliases) |alias| {
        if (std.mem.indexOf(u8, current, alias.from) == null) continue;
        const size = std.mem.replacementSize(u8, current, alias.from, alias.to);
        const next = try allocator.alloc(u8, size);
        _ = std.mem.replace(u8, current, alias.from, alias.to, next);
        allocator.free(current);
        current = next;
    }
    return current;
}

/// Copy `line` with every `__struct_<digits>` collapsed to `__struct_*`, then
/// canonicalize platform type spellings (`canonicalizePlatformTypes`).
/// Those suffixes are compiler-assigned anonymous-type counters: they shift
/// whenever unrelated code changes and differ between targets, so keeping
/// them verbatim would make the snapshot churn without any API change.
pub fn normalizeLine(allocator: std.mem.Allocator, line: []const u8) std.mem.Allocator.Error![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    const marker = "__struct_";
    var rest = line;
    while (std.mem.indexOf(u8, rest, marker)) |idx| {
        var end = idx + marker.len;
        while (end < rest.len and std.ascii.isDigit(rest[end])) end += 1;
        try out.appendSlice(allocator, rest[0 .. idx + marker.len]);
        try out.append(allocator, '*');
        rest = rest[end..];
    }
    try out.appendSlice(allocator, rest);
    return canonicalizePlatformTypes(allocator, try out.toOwnedSlice(allocator));
}

/// Render a snapshot file: `header`, then every line normalized
/// (`normalizeLine`), sorted bytewise and newline-terminated. The caller owns
/// the result.
pub fn renderSnapshot(
    allocator: std.mem.Allocator,
    lines: []const []const u8,
    header: []const u8,
) std.mem.Allocator.Error![]u8 {
    const normalized = try allocator.alloc([]u8, lines.len);
    var normalized_count: usize = 0;
    defer {
        for (normalized[0..normalized_count]) |line| allocator.free(line);
        allocator.free(normalized);
    }
    for (lines) |line| {
        normalized[normalized_count] = try normalizeLine(allocator, line);
        normalized_count += 1;
    }
    std.mem.sort([]u8, normalized[0..normalized_count], {}, lessThan);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, header);
    for (normalized[0..normalized_count]) |line| {
        try out.appendSlice(allocator, line);
        try out.append(allocator, '\n');
    }
    return out.toOwnedSlice(allocator);
}
