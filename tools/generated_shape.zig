//! Generated-shape snapshot gate — the shape of code the plugin generates.
//!
//! `check-api` freezes the library's own surface, but it cannot see the code
//! a consumer generates from a schema: a renamed `callXFromY`, a getter that
//! loses `error.WrongUnionMember`, or a changed `interface_id` all leave it
//! green. This tool walks generated bindings for a fixed corpus of schemas
//! and renders them to two files:
//!
//!   docs/generated-shape.txt              — STABLE generated shape, frozen.
//!   docs/generated-shape-experimental.txt — everything else, checked for
//!                                           staleness only.
//!
//!   zig build generated-shape         # regenerate both files
//!   zig build check-generated-shape   # fail on any drift in either file
//!
//! The corpus is a set of committed CodeGeneratorRequests
//! (tests/generated_shape/requests/, written by `just gen`). The build runs
//! the plugin on each request once per profile (full, compact,
//! no-reflection; build/generated_shape.zig), and hands the generated files
//! to this tool as modules. tests/generated_shape/instances.zig instantiates
//! a set of generic types, because a generic's members exist only once it is
//! instantiated.
//!
//! Lines are `<profile>.<file>.<decl path>: <description>`, rendered by the
//! shared walker (tools/snapshot_render.zig) with const values on, so an
//! interface id, a method ordinal or a schema constant is pinned by value.
//! Each generated type renders once, at the path that declares it; a
//! re-export renders as `alias <type>`. Runtime type names are rewritten to
//! their public capnpc-zig paths (`capnpc-zig.message.StructBuilder`, not the
//! instantiated `serialization.message.struct_builder.define(...)`); a type
//! public at several paths gets the shortest, then the bytewise smallest. So
//! the files do not move when the runtime reorganizes its private files.
//!
//! Tiers (owner decision D1(a), docs/sprint-plan-2026-10-04.md): a line is
//! Stable when a `stable_families` rule matches it and no
//! `experimental_overrides` rule does; a container that declares a Stable
//! line is Stable too, unless an override matches it. Every other line is
//! Experimental, and so is every line under `<profile>.instances`.
//! `zig build generated-shape -- --dump <file>` lists every line with its
//! tier and kind. The gate fails when:
//!   * either file drifts from the live render (`--check`);
//!   * a Stable line names an Experimental type: a runtime declaration that
//!     is not in docs/api-snapshot.txt, or a generated type whose own line is
//!     Experimental (the closure rule; `--write` refuses too);
//!   * a tier rule matches no line;
//!   * the census finds no example of a feature the corpus must cover;
//!   * a committed request is not in the corpus;
//!   * the walk reaches its depth limit (a compile error naming the path).
//! A corpus entry whose generated code does not compile fails the build
//! under a step named `generated-shape-<profile>-<request>`.
//!
//! Generated signatures spell the runtime's error sets, so a change to an
//! error set in the runtime (message, rpc) moves generated-shape lines too.
//! That is intended: it changes what consumers' generated code returns.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const render = @import("snapshot-render");
const corpus = @import("generated-shape-corpus");

/// How deep the walk descends below a generated file. Reaching it with an
/// unwalked container is a compile error, never a silent stop.
const max_depth = 16;

/// The library walk that maps runtime type names to public paths. Same depth
/// as the API snapshot (tools/api_snapshot.zig).
const library_depth = 8;

const stable_path_default = "docs/generated-shape.txt";
const experimental_path_default = "docs/generated-shape-experimental.txt";
const api_snapshot_path = "docs/api-snapshot.txt";
const requests_dir = "tests/generated_shape/requests";
const request_suffix = ".request.bin";

// ---------------------------------------------------------------------------
// Tier rules (D1(a)).
//
// A rule matches a line's path BELOW its generated file (`KvStore.Client.init`
// for `full.kvstore.KvStore.Client.init`) and the line's kind. Path globs
// split on `.`: `**` matches any number of segments (zero included), and `*`
// inside a segment matches any run of characters within it.
// ---------------------------------------------------------------------------

const Kind = enum { @"struct", @"enum", @"union", @"opaque", alias, @"fn", field, variant, enumerant, typedef, @"const" };

const all_kinds = std.enums.values(Kind)[0..].*;

const Rule = struct {
    glob: []const u8,
    kinds: []const Kind,
    /// When set, the rule also requires this text in the line's
    /// description: an override for "every member that names type X".
    names: ?[]const u8 = null,
    /// Why the rule exists; printed when it matches nothing.
    why: []const u8,
};

const stable_families = [_]Rule{
    // Reader/Builder get/set/init/has/clear/which/wrap.
    .{ .glob = "**.Reader", .kinds = &.{.@"struct"}, .why = "Reader family" },
    .{ .glob = "**.Builder", .kinds = &.{.@"struct"}, .why = "Builder family" },
    .{ .glob = "**.Reader.get*", .kinds = &.{.@"fn"}, .why = "Reader getters" },
    .{ .glob = "**.Reader.has*", .kinds = &.{.@"fn"}, .why = "Reader has" },
    .{ .glob = "**.Reader.which", .kinds = &.{.@"fn"}, .why = "Reader which" },
    .{ .glob = "**.Reader.init", .kinds = &.{.@"fn"}, .why = "Reader init" },
    .{ .glob = "**.Reader.wrap", .kinds = &.{.@"fn"}, .why = "Reader wrap" },
    .{ .glob = "**.Builder.get*", .kinds = &.{.@"fn"}, .why = "Builder getters" },
    .{ .glob = "**.Builder.set*", .kinds = &.{.@"fn"}, .why = "Builder setters" },
    .{ .glob = "**.Builder.init*", .kinds = &.{.@"fn"}, .why = "Builder init and initX" },
    .{ .glob = "**.Builder.has*", .kinds = &.{.@"fn"}, .why = "Builder has" },
    .{ .glob = "**.Builder.clear*", .kinds = &.{.@"fn"}, .why = "Builder clear" },
    .{ .glob = "**.Builder.which", .kinds = &.{.@"fn"}, .why = "Builder which" },
    .{ .glob = "**.Builder.wrap", .kinds = &.{.@"fn"}, .why = "Builder wrap" },
    // WhichTag, enums (schema enums, `Method`) and consts, with their values.
    .{ .glob = "**", .kinds = &.{ .@"enum", .enumerant }, .why = "enums and their enumerants" },
    .{ .glob = "**", .kinds = &.{.@"const"}, .why = "consts" },
    // Client init/release/fromBootstrap/callX/callXPipelined.
    .{ .glob = "**.Client", .kinds = &.{.@"struct"}, .why = "Client" },
    .{ .glob = "**.Client.init", .kinds = &.{.@"fn"}, .why = "Client init" },
    .{ .glob = "**.Client.release", .kinds = &.{.@"fn"}, .why = "Client release" },
    .{ .glob = "**.Client.fromBootstrap", .kinds = &.{.@"fn"}, .why = "Client fromBootstrap" },
    .{ .glob = "**.Client.call*", .kinds = &.{.@"fn"}, .why = "Client calls" },
    // PipelinedClient calls.
    .{ .glob = "**.PipelinedClient", .kinds = &.{.@"struct"}, .why = "PipelinedClient" },
    .{ .glob = "**.PipelinedClient.call*", .kinds = &.{.@"fn"}, .why = "PipelinedClient calls" },
    // Server, the VTable fields and the Method enum (an enum, above).
    .{ .glob = "**.Server", .kinds = &.{.@"struct"}, .why = "Server" },
    .{ .glob = "**.Server.*", .kinds = &.{.field}, .why = "Server fields" },
    .{ .glob = "**.VTable", .kinds = &.{.@"struct"}, .why = "VTable" },
    .{ .glob = "**.VTable.*", .kinds = &.{.field}, .why = "VTable fields" },
    // Response with unwrap. `BootstrapResponse` is the bootstrap call's
    // Response: `Client.fromBootstrap` is Stable and its callback receives
    // one, so it is in the family (and `BootstrapCallback` with `Callback`).
    .{ .glob = "**.Response", .kinds = &.{.@"union"}, .why = "Response" },
    .{ .glob = "**.Response.*", .kinds = &.{.variant}, .why = "Response variants" },
    .{ .glob = "**.Response.unwrap", .kinds = &.{.@"fn"}, .why = "Response unwrap" },
    .{ .glob = "**.BootstrapResponse", .kinds = &.{.@"union"}, .why = "BootstrapResponse" },
    .{ .glob = "**.BootstrapResponse.*", .kinds = &.{.variant}, .why = "BootstrapResponse variants" },
    .{ .glob = "**.BootstrapResponse.unwrap", .kinds = &.{.@"fn"}, .why = "BootstrapResponse unwrap" },
    // The Handler/Callback/BuildFn typedefs.
    .{ .glob = "**.Handler", .kinds = &.{.typedef}, .why = "Handler typedef" },
    .{ .glob = "**.Callback", .kinds = &.{.typedef}, .why = "Callback typedef" },
    .{ .glob = "**.BootstrapCallback", .kinds = &.{.typedef}, .why = "BootstrapCallback typedef" },
    .{ .glob = "**.BuildFn", .kinds = &.{.typedef}, .why = "BuildFn typedef" },
};

/// Lines a Stable family matches that stay Experimental. Two groups:
///
/// Closure carve-outs (owner default for D1: a Stable-family member that
/// names an Experimental type stays Experimental rather than freezing that
/// type). Each names the type that keeps it out.
///
/// Scope: generator metadata that is a `const` but not a schema constant or
/// a wire constant (interface id, ordinal, is_streaming).
const experimental_overrides = [_]Rule{
    // Closure: `capnpc-zig.reflection.SchemaRef` is Experimental.
    .{ .glob = "**.capnpSchema", .kinds = &.{.@"const"}, .names = "capnpc-zig.reflection.SchemaRef", .why = "capnpSchema names reflection.SchemaRef" },
    // Closure: `callXWithOptions` takes `capnpc-zig.rpc.peer.CallOptions`.
    .{ .glob = "**.Client.call*", .kinds = &.{.@"fn"}, .names = "capnpc-zig.rpc.peer.CallOptions", .why = "Client callXWithOptions names rpc.peer.CallOptions" },
    .{ .glob = "**.PipelinedClient.call*", .kinds = &.{.@"fn"}, .names = "capnpc-zig.rpc.peer.CallOptions", .why = "PipelinedClient callXWithOptions names rpc.peer.CallOptions" },
    // Closure: the deferred-handler VTable fields name the generated
    // `ReturnSender` / `StreamReturnSender`, which no family covers.
    .{ .glob = "**.VTable.*_deferred", .kinds = &.{.field}, .names = "ReturnSender", .why = "VTable x_deferred names ReturnSender" },
    // Closure: a streaming method's Response carries the runtime's
    // `rpc.generated.stream.StreamResult`, which is Experimental.
    .{ .glob = "**.Response.*", .kinds = &.{ .variant, .@"fn" }, .names = "capnpc-zig.rpc.generated.stream.StreamResult", .why = "streaming Response names rpc.generated.stream.StreamResult" },
    // Closure: `callXPipelined` returns the generated `XPipeline` struct,
    // which no family covers.
    .{ .glob = "**.Client.call*Pipelined", .kinds = &.{.@"fn"}, .why = "Client callXPipelined returns the generated XPipeline" },
    // Scope: brand views (`Reader.brands()` / `Builder.brands()` and the
    // `Brands` namespace they return) are not a family; their Reader and
    // Builder getters only look like the Reader/Builder family by name.
    .{ .glob = "**.Reader.Brands.**", .kinds = &all_kinds, .why = "Reader brand views" },
    .{ .glob = "**.Builder.Brands.**", .kinds = &all_kinds, .why = "Builder brand views" },
    // Scope: reflection and manifest payloads (their accessors and
    // `capnpSchema` are Experimental too).
    .{ .glob = "CAPNP_SCHEMA_REQUEST", .kinds = &.{.@"const"}, .why = "reflection request bytes" },
    .{ .glob = "CAPNP_SCHEMA_MANIFEST_JSON", .kinds = &.{.@"const"}, .why = "schema manifest JSON" },
    // Scope: annotation metadata (anonymous struct values).
    .{ .glob = "**.targets", .kinds = &.{.@"const"}, .why = "annotation targets" },
    .{ .glob = "**.*_annotations", .kinds = &.{.@"const"}, .why = "annotation uses" },
};

// ---------------------------------------------------------------------------
// The comptime walks.
// ---------------------------------------------------------------------------

const library = render.Snapshot(.{
    .root = capnpc,
    .root_path = "capnpc-zig",
    .max_depth = library_depth,
});

fn declNames(comptime T: type) []const []const u8 {
    var out: []const []const u8 = &.{};
    for (std.meta.declarations(T)) |name| out = out ++ [_][]const u8{name};
    return out;
}

/// The file stems of every corpus entry in `profile`.
fn profileStems(comptime profile: []const u8) []const []const u8 {
    var out: []const []const u8 = &.{};
    for (corpus.entries) |entry| {
        if (std.mem.eql(u8, entry.profile, profile)) out = out ++ declNames(entry.root);
    }
    return out;
}

fn startsWithStem(comptime name: []const u8, comptime stems: []const []const u8) bool {
    for (stems) |stem| {
        if (std.mem.eql(u8, name, stem)) return true;
        if (std.mem.startsWith(u8, name, stem ++ ".")) return true;
    }
    return false;
}

fn mentionsStem(comptime name: []const u8, comptime stems: []const []const u8) bool {
    for (stems) |stem| {
        var start: usize = 0;
        while (std.mem.indexOfPos(u8, name, start, stem ++ ".")) |idx| {
            if (idx == 0 or !isIdentByte(name[idx - 1])) return true;
            start = idx + 1;
        }
    }
    return false;
}

/// The walk policy for one profile: a generated type renders at the path
/// that declares it (`<profile>.<@typeName>`) and as an alias everywhere
/// else; a generic instance that involves a generated type renders where it
/// is reached; a runtime or std type is pinned by name and not walked.
fn Policy(comptime profile: []const u8) type {
    return struct {
        const stems = profileStems(profile);

        fn aliasTarget(comptime T: type, comptime path: []const u8) ?[]const u8 {
            const name = @typeName(T);
            if (std.mem.indexOfScalar(u8, name, '(') != null and
                (startsWithStem(name, stems) or mentionsStem(name, stems)))
            {
                return null;
            }
            if (startsWithStem(name, stems)) {
                return if (std.mem.eql(u8, path, profile ++ "." ++ name)) null else name;
            }
            return name;
        }

        fn Walk(comptime root: type, comptime root_path: []const u8) type {
            return render.Snapshot(.{
                .root = root,
                .root_path = root_path,
                .max_depth = max_depth,
                .render_const_values = true,
                .on_depth_limit = .fail,
                .alias_target = aliasTarget,
            });
        }
    };
}

/// One walked root: a generated file, or a profile's instance namespace.
const Walked = struct {
    profile: []const u8,
    /// `<profile>.<file>` or `<profile>.instances`.
    root_path: []const u8,
    request: []const u8,
    instances: bool,
    entries: []const render.Entry,
    containers: []const render.ContainerName,
};

const walks: []const Walked = blk: {
    @setEvalBranchQuota(100_000_000);
    var out: []const Walked = &.{};
    for (corpus.entries) |entry| {
        const P = Policy(entry.profile);
        for (declNames(entry.root)) |stem| {
            const root_path = entry.profile ++ "." ++ stem;
            const W = P.Walk(@field(entry.root, stem), root_path);
            out = out ++ [_]Walked{.{
                .profile = entry.profile,
                .root_path = root_path,
                .request = entry.request,
                .instances = false,
                .entries = W.entries,
                .containers = W.container_names,
            }};
        }
    }
    for (declNames(corpus.instances)) |profile| {
        const P = Policy(profile);
        const root_path = profile ++ ".instances";
        const W = P.Walk(@field(corpus.instances, profile), root_path);
        out = out ++ [_]Walked{.{
            .profile = profile,
            .root_path = root_path,
            .request = "instances",
            .instances = true,
            .entries = W.entries,
            .containers = W.container_names,
        }};
    }
    break :blk out;
};

/// The first `@typeName` segment of every capnpc-zig source root
/// (`serialization`, `rpc`, ...). A library container whose name starts with
/// one is a runtime type; a re-exported std type is not.
const runtime_roots: []const []const u8 = blk: {
    var out: []const []const u8 = &.{};
    for (std.meta.declarations(capnpc)) |name| {
        const D = @field(capnpc, name);
        if (@TypeOf(D) != type) continue;
        const type_name = @typeName(D);
        const end = std.mem.indexOfAny(u8, type_name, ".(") orelse type_name.len;
        const root = type_name[0..end];
        const seen = for (out) |existing| {
            if (std.mem.eql(u8, existing, root)) break true;
        } else false;
        if (!seen) out = out ++ [_][]const u8{root};
    }
    break :blk out;
};

// ---------------------------------------------------------------------------
// Name scanning shared by the rewriter and the closure check.
// ---------------------------------------------------------------------------

fn isIdentByte(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

fn isIdentStart(c: u8) bool {
    return std.ascii.isAlphabetic(c) or c == '_' or c == '@';
}

/// True when a qualified name can start at `text[i]`: not in the middle of
/// an identifier, a dotted path or `capnpc-zig`.
fn isNameStart(text: []const u8, i: usize) bool {
    if (!isIdentStart(text[i])) return false;
    if (i == 0) return true;
    const prev = text[i - 1];
    return !(isIdentByte(prev) or prev == '.' or prev == '-' or prev == '"' or prev == '@');
}

/// One segment: an identifier (`capnpc-zig` and `@"quoted"` included), then
/// an optional balanced `(...)`. Returns the end, or null when no segment
/// starts at `i`.
fn segmentEnd(text: []const u8, i: usize) ?usize {
    var j = i;
    if (j + 1 < text.len and text[j] == '@' and text[j + 1] == '"') {
        j += 2;
        while (j < text.len and text[j] != '"') : (j += 1) {
            if (text[j] == '\\') j += 1;
        }
        if (j >= text.len) return null;
        j += 1;
    } else {
        if (j >= text.len or !(std.ascii.isAlphabetic(text[j]) or text[j] == '_')) return null;
        while (j < text.len and (isIdentByte(text[j]) or text[j] == '-')) j += 1;
    }
    if (j < text.len and text[j] == '(') {
        var depth: usize = 0;
        var k = j;
        while (k < text.len) : (k += 1) {
            switch (text[k]) {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if (depth == 0) return k + 1;
                },
                else => {},
            }
        }
        return null;
    }
    return j;
}

/// The end of every dotted prefix of the qualified name at `start`, shortest
/// first: `a`, `a.b`, `a.b(x)`, ...
fn candidateEnds(text: []const u8, start: usize, ends: *std.ArrayList(usize), gpa: std.mem.Allocator) !void {
    ends.clearRetainingCapacity();
    var i = start;
    while (segmentEnd(text, i)) |end| {
        try ends.append(gpa, end);
        if (end < text.len and text[end] == '.' and end + 1 < text.len) {
            i = end + 1;
        } else break;
    }
}

/// The dotted run of plain identifiers after `end` (`.Method` in
/// `capnpc-zig.generic.Method(...)`), without any `(...)`.
fn trailingMembers(text: []const u8, end: usize) []const u8 {
    var j = end;
    while (j + 1 < text.len and text[j] == '.' and (std.ascii.isAlphabetic(text[j + 1]) or text[j + 1] == '_')) {
        j += 1;
        while (j < text.len and isIdentByte(text[j])) j += 1;
    }
    return text[end..j];
}

// ---------------------------------------------------------------------------
// Runtime type rewriting.
// ---------------------------------------------------------------------------

const RuntimeType = struct {
    /// The public path the snapshot prints.
    path: []const u8,
    /// Every public path the walk reached the type at.
    aliases: std.ArrayList([]const u8) = .empty,
};

/// A runtime declaration a line names: by public path, or by its raw
/// `@typeName` when no public path reaches it (`public == false`).
const RuntimeRef = struct { name: []const u8, stable: bool, public: bool = true };

const Rewriter = struct {
    types: std.StringHashMapUnmanaged(RuntimeType) = .empty,
    stable_api: *const std.StringHashMapUnmanaged(void),

    fn init(gpa: std.mem.Allocator, stable_api: *const std.StringHashMapUnmanaged(void)) !Rewriter {
        var self: Rewriter = .{ .stable_api = stable_api };
        for (library.container_names) |container| {
            if (!isRuntimeName(container.type_name)) continue;
            const slot = try self.types.getOrPut(gpa, container.type_name);
            if (!slot.found_existing) slot.value_ptr.* = .{ .path = container.path };
            try slot.value_ptr.aliases.append(gpa, container.path);
            // The shortest path wins, then the bytewise smallest, so the
            // choice does not depend on walk order.
            const best = slot.value_ptr.path;
            if (container.path.len < best.len or
                (container.path.len == best.len and std.mem.lessThan(u8, container.path, best)))
            {
                slot.value_ptr.path = container.path;
            }
        }
        return self;
    }

    fn isRuntimeName(name: []const u8) bool {
        for (runtime_roots) |root| {
            if (std.mem.eql(u8, name, root)) return true;
            if (name.len > root.len and std.mem.startsWith(u8, name, root) and (name[root.len] == '.' or name[root.len] == '(')) return true;
        }
        return false;
    }

    fn startsWithRuntimeRoot(text: []const u8, i: usize) bool {
        for (runtime_roots) |root| {
            if (std.mem.startsWith(u8, text[i..], root)) return true;
        }
        return false;
    }

    /// True when the qualified name at `i` is inside a runtime root
    /// (`serialization.x`, `rpc.mod.Y`), not merely spelled like one
    /// (`rpc_inherited_paths.Z`).
    fn isRuntimeNameAt(text: []const u8, i: usize) bool {
        for (runtime_roots) |root| {
            const end = i + root.len;
            if (end < text.len and std.mem.startsWith(u8, text[i..], root) and (text[end] == '.' or text[end] == '(')) return true;
        }
        return false;
    }

    fn isStable(self: *const Rewriter, runtime_type: *const RuntimeType, members: []const u8, scratch: std.mem.Allocator) !bool {
        for (runtime_type.aliases.items) |alias| {
            const full = try std.mem.concat(scratch, u8, &.{ alias, members });
            defer scratch.free(full);
            if (self.stable_api.contains(full)) return true;
        }
        return false;
    }

    /// Rewrite every runtime type name in `text` to its public path, and
    /// append the declarations it names to `refs`.
    fn rewrite(
        self: *const Rewriter,
        arena: std.mem.Allocator,
        text: []const u8,
        refs: ?*std.ArrayList(RuntimeRef),
        ends: *std.ArrayList(usize),
    ) ![]const u8 {
        var out: std.ArrayList(u8) = .empty;
        var i: usize = 0;
        while (i < text.len) {
            if (isNameStart(text, i) and startsWithRuntimeRoot(text, i)) {
                try candidateEnds(text, i, ends, arena);
                var matched: ?usize = null;
                var n = ends.items.len;
                while (n > 0) : (n -= 1) {
                    const end = ends.items[n - 1];
                    if (self.types.getPtr(text[i..end])) |runtime_type| {
                        const members = trailingMembers(text, end);
                        try out.appendSlice(arena, runtime_type.path);
                        if (refs) |list| try list.append(arena, .{
                            .name = try std.mem.concat(arena, u8, &.{ runtime_type.path, members }),
                            .stable = try self.isStable(runtime_type, members, arena),
                        });
                        matched = end;
                        break;
                    }
                }
                if (matched) |end| {
                    i = end;
                    continue;
                }
                if (isRuntimeNameAt(text, i) and ends.items.len != 0) {
                    // A runtime type no public path reaches: consumers
                    // cannot name it, and its private file path would move
                    // the snapshot on any runtime refactor.
                    const end = ends.items[ends.items.len - 1];
                    if (refs) |list| try list.append(arena, .{ .name = text[i..end], .stable = false, .public = false });
                }
            }
            try out.append(arena, text[i]);
            i += 1;
        }
        return out.toOwnedSlice(arena);
    }
};

// ---------------------------------------------------------------------------
// Lines, tiers, closure.
// ---------------------------------------------------------------------------

const Line = struct {
    walk: *const Walked,
    path: []const u8,
    /// The path below the generated file (`KvStore.Client.init`).
    rel: []const u8,
    kind: Kind,
    /// The rewritten `<path>: <description>` line.
    text: []const u8,
    description: []const u8,
    stable: bool,
    /// An `experimental_overrides` rule matched it.
    overridden: bool = false,
    runtime_refs: []const RuntimeRef,
};

fn kindOf(description: []const u8) Kind {
    const exact = [_]struct { []const u8, Kind }{
        .{ "struct", .@"struct" }, .{ "enum", .@"enum" }, .{ "union", .@"union" }, .{ "opaque", .@"opaque" },
    };
    for (exact) |pair| if (std.mem.eql(u8, description, pair[0])) return pair[1];
    const prefixes = [_]struct { []const u8, Kind }{
        .{ "alias ", .alias },     .{ "fn ", .@"fn" },            .{ "field ", .field },
        .{ "variant ", .variant }, .{ "enumerant ", .enumerant }, .{ "type = ", .typedef },
        .{ "const ", .@"const" },
    };
    for (prefixes) |pair| if (std.mem.startsWith(u8, description, pair[0])) return pair[1];
    std.debug.panic("generated-shape: unknown line kind: {s}", .{description});
}

/// Glob match on `.`-separated segments; see the tier rules.
fn globMatch(glob: []const u8, path: []const u8) bool {
    var glob_segments: [32][]const u8 = undefined;
    var path_segments: [64][]const u8 = undefined;
    const g = splitPath(glob, &glob_segments);
    const p = splitPath(path, &path_segments);
    return matchSegments(g, p);
}

/// Split a path on `.` outside `(...)` and `@"..."`.
fn splitPath(path: []const u8, buffer: [][]const u8) [][]const u8 {
    var count: usize = 0;
    var start: usize = 0;
    var depth: usize = 0;
    var quoted = false;
    var i: usize = 0;
    while (i < path.len) : (i += 1) {
        const c = path[i];
        if (quoted) {
            if (c == '\\') i += 1 else if (c == '"') quoted = false;
            continue;
        }
        switch (c) {
            '"' => quoted = true,
            '(' => depth += 1,
            ')' => depth -|= 1,
            '.' => if (depth == 0) {
                buffer[count] = path[start..i];
                count += 1;
                start = i + 1;
            },
            else => {},
        }
    }
    buffer[count] = path[start..];
    return buffer[0 .. count + 1];
}

fn matchSegments(glob: []const []const u8, path: []const []const u8) bool {
    if (glob.len == 0) return path.len == 0;
    if (std.mem.eql(u8, glob[0], "**")) {
        var skip: usize = 0;
        while (skip <= path.len) : (skip += 1) {
            if (matchSegments(glob[1..], path[skip..])) return true;
        }
        return false;
    }
    if (path.len == 0) return false;
    return segmentMatch(glob[0], path[0]) and matchSegments(glob[1..], path[1..]);
}

/// `*` matches any run of characters within one segment.
fn segmentMatch(pattern: []const u8, segment: []const u8) bool {
    const star = std.mem.indexOfScalar(u8, pattern, '*') orelse return std.mem.eql(u8, pattern, segment);
    const head = pattern[0..star];
    if (!std.mem.startsWith(u8, segment, head)) return false;
    const rest_pattern = pattern[star + 1 ..];
    var offset = head.len;
    while (offset <= segment.len) : (offset += 1) {
        if (segmentMatch(rest_pattern, segment[offset..])) return true;
    }
    return false;
}

fn ruleMatches(rule: Rule, line: *const Line) bool {
    if (rule.names) |names| {
        if (std.mem.indexOf(u8, line.description, names) == null) return false;
    }
    for (rule.kinds) |kind| {
        if (kind == line.kind) return globMatch(rule.glob, line.rel);
    }
    return false;
}

fn isContainerKind(kind: Kind) bool {
    return switch (kind) {
        .@"struct", .@"enum", .@"union", .@"opaque" => true,
        else => false,
    };
}

/// The path of the container that declares `path`, or null at the root.
fn parentPath(path: []const u8) ?[]const u8 {
    var buffer: [64][]const u8 = undefined;
    const segments = splitPath(path, &buffer);
    if (segments.len < 2) return null;
    const last = segments[segments.len - 1];
    return path[0 .. path.len - last.len - 1];
}

const Violation = struct { line: *const Line, offender: []const u8, why: []const u8 };

/// The generated container types of one profile, keyed by their rewritten
/// `@typeName`, valued by their line.
const GeneratedTypes = std.StringHashMapUnmanaged(*const Line);

fn collectGeneratedRefs(
    arena: std.mem.Allocator,
    line: *const Line,
    types: *const GeneratedTypes,
    stems: []const []const u8,
    ends: *std.ArrayList(usize),
    out: *std.ArrayList(Violation),
) !void {
    const text = line.description;
    var i: usize = 0;
    while (i < text.len) : (i += 1) {
        if (!isNameStart(text, i)) continue;
        const stem_hit = for (stems) |stem| {
            if (std.mem.startsWith(u8, text[i..], stem) and
                (i + stem.len == text.len or text[i + stem.len] == '.'))
                break true;
        } else false;
        if (!stem_hit) continue;
        try candidateEnds(text, i, ends, arena);
        if (ends.items.len == 0) continue;
        // The whole qualified name must be a walked container. A prefix
        // match would credit `Foo.targets__struct_9` to `Foo`.
        const end = ends.items[ends.items.len - 1];
        const name = text[i..end];
        if (types.get(name)) |target| {
            if (!target.stable) try out.append(arena, .{ .line = line, .offender = target.path, .why = "generated type is Experimental" });
        } else {
            try out.append(arena, .{ .line = line, .offender = name, .why = "generated type has no line (not pub, or not walked)" });
        }
        i = end - 1;
    }
}

// ---------------------------------------------------------------------------
// Census: features the corpus must exercise.
// ---------------------------------------------------------------------------

const Census = struct {
    name: []const u8,
    predicate: *const fn (*const Line) bool,
};

fn lastSegment(path: []const u8) []const u8 {
    var buffer: [64][]const u8 = undefined;
    const segments = splitPath(path, &buffer);
    return segments[segments.len - 1];
}

fn parentSegment(path: []const u8) []const u8 {
    var buffer: [64][]const u8 = undefined;
    const segments = splitPath(path, &buffer);
    return if (segments.len >= 2) segments[segments.len - 2] else "";
}

fn isHexSuffix(name: []const u8) bool {
    const underscore = std.mem.lastIndexOfScalar(u8, name, '_') orelse return false;
    const hex = name[underscore + 1 ..];
    if (hex.len < 8) return false;
    for (hex) |c| if (!std.ascii.isHex(c)) return false;
    return true;
}

const census = [_]Census{
    .{ .name = "non-generic Client (one `release` each)", .predicate = struct {
        fn f(l: *const Line) bool {
            return !l.walk.instances and l.kind == .@"fn" and std.mem.eql(u8, lastSegment(l.rel), "release") and std.mem.eql(u8, parentSegment(l.rel), "Client");
        }
    }.f },
    .{ .name = "StreamClient", .predicate = struct {
        fn f(l: *const Line) bool {
            return l.kind == .@"struct" and std.mem.eql(u8, lastSegment(l.rel), "StreamClient");
        }
    }.f },
    .{ .name = "PipelinedClient", .predicate = struct {
        fn f(l: *const Line) bool {
            return l.kind == .@"struct" and std.mem.eql(u8, lastSegment(l.rel), "PipelinedClient");
        }
    }.f },
    .{ .name = "inherited `From` call family", .predicate = struct {
        fn f(l: *const Line) bool {
            const name = lastSegment(l.rel);
            return l.kind == .@"fn" and std.mem.eql(u8, parentSegment(l.rel), "Client") and
                std.mem.startsWith(u8, name, "call") and std.mem.indexOf(u8, name, "From") != null;
        }
    }.f },
    .{ .name = "WhichTag", .predicate = struct {
        fn f(l: *const Line) bool {
            return l.kind == .@"enum" and std.mem.eql(u8, lastSegment(l.rel), "WhichTag");
        }
    }.f },
    .{ .name = "guarded group getter", .predicate = struct {
        fn f(l: *const Line) bool {
            return l.kind == .@"fn" and std.mem.eql(u8, parentSegment(l.rel), "Reader") and
                std.mem.startsWith(u8, lastSegment(l.rel), "get") and
                std.mem.indexOf(u8, l.description, "WrongUnionMember") != null and
                std.mem.endsWith(u8, l.description, ".Reader");
        }
    }.f },
    .{ .name = "const with a rendered value", .predicate = struct {
        fn f(l: *const Line) bool {
            return l.kind == .@"const" and std.mem.indexOf(u8, l.description, " = ") != null;
        }
    }.f },
    .{ .name = "Apply instance", .predicate = struct {
        fn f(l: *const Line) bool {
            return l.walk.instances;
        }
    }.f },
    .{ .name = "compact entry", .predicate = struct {
        fn f(l: *const Line) bool {
            return std.mem.eql(u8, l.walk.profile, "compact") and !l.walk.instances;
        }
    }.f },
    .{ .name = "no-reflection entry", .predicate = struct {
        fn f(l: *const Line) bool {
            return std.mem.eql(u8, l.walk.profile, "no-reflection");
        }
    }.f },
    .{ .name = "collision-renamed decl", .predicate = struct {
        fn f(l: *const Line) bool {
            return isHexSuffix(lastSegment(l.rel));
        }
    }.f },
    .{
        .name = "escaped field name (written @\"...\" in the source)",
        .predicate = struct {
            fn f(l: *const Line) bool {
                // Paths carry the raw name (`WhichTag.error`), so test whether
                // Zig source must quote it.
                return !std.zig.isValidId(lastSegment(l.rel));
            }
        }.f,
    },
};

// ---------------------------------------------------------------------------
// Files.
// ---------------------------------------------------------------------------

const stable_header =
    \\# STABLE generated-code shape — the FROZEN contract for generated bindings.
    \\# Generated by `zig build generated-shape`; `zig build check-generated-shape`
    \\# fails on any drift. A changed or removed line here changes code that
    \\# consumers generate. Generated signatures spell the runtime's error
    \\# sets, so a runtime error-set change moves lines here too. The corpus,
    \\# tiers and rules are in tools/generated_shape.zig and
    \\# build/generated_shape.zig.
    \\
;

const experimental_header =
    \\# EXPERIMENTAL generated-code shape — not frozen, but kept current.
    \\# Generated by `zig build generated-shape`; `zig build check-generated-shape`
    \\# fails when this file is stale. Do not rely on these lines across
    \\# releases; only docs/generated-shape.txt is a contract.
    \\
;

fn readStableApi(gpa: std.mem.Allocator, io: std.Io) !std.StringHashMapUnmanaged(void) {
    const text = std.Io.Dir.cwd().readFileAlloc(io, api_snapshot_path, gpa, .limited(64 * 1024 * 1024)) catch |err| {
        std.debug.print("generated-shape: cannot read {s} ({}); it gives the tier of runtime types\n", .{ api_snapshot_path, err });
        return err;
    };
    var set: std.StringHashMapUnmanaged(void) = .empty;
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        if (line.len == 0 or line[0] == '#') continue;
        const colon = std.mem.indexOf(u8, line, ": ") orelse continue;
        try set.put(gpa, line[0..colon], {});
    }
    return set;
}

fn checkRequests(io: std.Io) !usize {
    var dir = std.Io.Dir.cwd().openDir(io, requests_dir, .{ .iterate = true }) catch |err| {
        std.debug.print("generated-shape: cannot open {s} ({})\n", .{ requests_dir, err });
        return err;
    };
    defer dir.close(io);
    var problems: usize = 0;
    var it = dir.iterate();
    while (try it.next(io)) |file| {
        if (!std.mem.endsWith(u8, file.name, request_suffix)) continue;
        const name = file.name[0 .. file.name.len - request_suffix.len];
        const known = for (corpus.requests) |request| {
            if (std.mem.eql(u8, request, name)) break true;
        } else false;
        if (!known) {
            std.debug.print("generated-shape: {s}/{s} is not in the corpus (build/generated_shape.zig `requests`)\n", .{ requests_dir, file.name });
            problems += 1;
        }
    }
    return problems;
}

fn writeFile(io: std.Io, path: []const u8, contents: []const u8) !void {
    var file = try std.Io.Dir.cwd().createFile(io, path, .{});
    defer file.close(io);
    try file.writeStreamingAll(io, contents);
}

/// Diff `rendered` against `path`; print up to 25 drifting lines.
fn diffAndReport(gpa: std.mem.Allocator, io: std.Io, path: []const u8, rendered: []const u8) !bool {
    const existing = std.Io.Dir.cwd().readFileAlloc(io, path, gpa, .limited(64 * 1024 * 1024)) catch |err| {
        std.debug.print("generated-shape: cannot read {s} ({}); run `zig build generated-shape`\n", .{ path, err });
        return false;
    };
    defer gpa.free(existing);
    if (std.mem.eql(u8, existing, rendered)) return true;

    // Report removed and added lines rather than positional drift: one
    // inserted line would otherwise make every later line differ.
    var old_set: std.StringHashMapUnmanaged(void) = .empty;
    defer old_set.deinit(gpa);
    var new_set: std.StringHashMapUnmanaged(void) = .empty;
    defer new_set.deinit(gpa);
    var it = std.mem.splitScalar(u8, existing, '\n');
    while (it.next()) |line| try old_set.put(gpa, line, {});
    it = std.mem.splitScalar(u8, rendered, '\n');
    while (it.next()) |line| try new_set.put(gpa, line, {});

    var removed: usize = 0;
    var added: usize = 0;
    const max_reported = 25;
    it = std.mem.splitScalar(u8, existing, '\n');
    while (it.next()) |line| {
        if (new_set.contains(line)) continue;
        removed += 1;
        if (removed <= max_reported) std.debug.print("generated-shape: {s}: - {s}\n", .{ path, line });
    }
    it = std.mem.splitScalar(u8, rendered, '\n');
    while (it.next()) |line| {
        if (old_set.contains(line)) continue;
        added += 1;
        if (added <= max_reported) std.debug.print("generated-shape: {s}: + {s}\n", .{ path, line });
    }
    std.debug.print("generated-shape: {s}: {d} line(s) removed, {d} added\n", .{ path, removed, added });
    return false;
}

pub fn main(init: std.process.Init) !void {
    var arena_state: std.heap.ArenaAllocator = .init(init.gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const io = init.io;

    var mode: enum { check, write } = .check;
    var stable_path: []const u8 = stable_path_default;
    var experimental_path: []const u8 = experimental_path_default;
    var dump_path: ?[]const u8 = null;
    var iter = try std.process.Args.Iterator.initAllocator(init.minimal.args, arena);
    defer iter.deinit();
    _ = iter.skip();
    while (iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--write")) {
            mode = .write;
        } else if (std.mem.eql(u8, arg, "--check")) {
            mode = .check;
        } else if (std.mem.eql(u8, arg, "--path")) {
            stable_path = iter.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--experimental-path")) {
            experimental_path = iter.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--dump")) {
            dump_path = iter.next() orelse return error.InvalidArgument;
        } else {
            std.debug.print("generated-shape: unknown argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }

    const stable_api = try readStableApi(arena, io);
    const rewriter = try Rewriter.init(arena, &stable_api);
    var ends: std.ArrayList(usize) = .empty;

    // Render, rewrite and tier every line.
    var lines: std.ArrayList(Line) = .empty;
    for (walks) |*walk| {
        for (walk.entries) |entry| {
            var refs: std.ArrayList(RuntimeRef) = .empty;
            const text = try rewriter.rewrite(arena, entry.line, &refs, &ends);
            const colon = std.mem.indexOf(u8, text, ": ").?;
            const description = text[colon + 2 ..];
            const rel = if (entry.path.len > walk.root_path.len) entry.path[walk.root_path.len + 1 ..] else "";
            try lines.append(arena, .{
                .walk = walk,
                .path = entry.path,
                .rel = rel,
                .kind = kindOf(description),
                .text = text,
                .description = description,
                .stable = false,
                .runtime_refs = refs.items,
            });
        }
    }

    var family_hits: [stable_families.len]usize = @splat(0);
    var override_hits: [experimental_overrides.len]usize = @splat(0);
    for (lines.items) |*line| {
        if (line.walk.instances) continue;
        var family = false;
        for (stable_families, &family_hits) |rule, *hits| {
            if (ruleMatches(rule, line)) {
                hits.* += 1;
                family = true;
            }
        }
        // An override also keeps a container out of the enclosing-container
        // rule below, but it only counts as live when it overrides a family.
        var overridden = false;
        for (experimental_overrides, &override_hits) |rule, *hits| {
            if (ruleMatches(rule, line)) {
                if (family) hits.* += 1;
                overridden = true;
            }
        }
        line.overridden = overridden;
        line.stable = family and !overridden;
    }

    // A container that declares a Stable line is Stable too: the families
    // name members, and a member is used through its container's path
    // (`addressbook.Person` in `StructListReader(addressbook.Person)`).
    var line_by_path: std.StringHashMapUnmanaged(*Line) = .empty;
    for (lines.items) |*line| try line_by_path.put(arena, line.path, line);
    for (lines.items) |*line| {
        if (!line.stable) continue;
        var path = line.path;
        while (parentPath(path)) |parent_path| : (path = parent_path) {
            const parent = line_by_path.get(parent_path) orelse break;
            if (!isContainerKind(parent.kind) or parent.overridden) break;
            parent.stable = true;
        }
    }

    var failures: usize = 0;

    if (dump_path) |path| {
        var dump: std.ArrayList(u8) = .empty;
        for (lines.items) |line| {
            try dump.print(arena, "{s} {s} {s}\n", .{ if (line.stable) "S" else "E", @tagName(line.kind), line.text });
        }
        try writeFile(io, path, dump.items);
    }

    // Every rule must match a line.
    for (stable_families, family_hits) |rule, hits| {
        if (hits != 0) continue;
        std.debug.print("generated-shape: stable_families rule matches no line: {s} ({s})\n", .{ rule.glob, rule.why });
        failures += 1;
    }
    for (experimental_overrides, override_hits) |rule, hits| {
        if (hits != 0) continue;
        std.debug.print("generated-shape: experimental_overrides rule matches no Stable-family line: {s} ({s})\n", .{ rule.glob, rule.why });
        failures += 1;
    }

    // Every runtime type a line names must have a public path.
    var private_runtime: usize = 0;
    for (lines.items) |*line| {
        for (line.runtime_refs) |ref| {
            if (ref.public) continue;
            if (private_runtime == 0) std.debug.print("generated-shape: generated code names runtime types that no public capnpc-zig path reaches (raise library_depth, or make the type public):\n", .{});
            private_runtime += 1;
            std.debug.print("  {s}\n    names {s}\n", .{ line.path, ref.name });
        }
    }
    if (private_runtime != 0) failures += 1;

    // The closure rule.
    var generated_types: std.StringHashMapUnmanaged(GeneratedTypes) = .empty;
    for (walks) |*walk| {
        const slot = try generated_types.getOrPut(arena, walk.profile);
        if (!slot.found_existing) slot.value_ptr.* = .empty;
        for (walk.containers) |container| {
            const line = line_by_path.get(container.path) orelse continue;
            const name = try rewriter.rewrite(arena, container.type_name, null, &ends);
            const type_slot = try slot.value_ptr.getOrPut(arena, name);
            if (!type_slot.found_existing) type_slot.value_ptr.* = line;
        }
    }
    var profile_stems: std.StringHashMapUnmanaged(std.ArrayList([]const u8)) = .empty;
    inline for (corpus.entries) |entry| {
        const slot = try profile_stems.getOrPut(arena, entry.profile);
        if (!slot.found_existing) slot.value_ptr.* = .empty;
        inline for (comptime declNames(entry.root)) |stem| try slot.value_ptr.append(arena, stem);
    }
    var violations: std.ArrayList(Violation) = .empty;
    for (lines.items) |*line| {
        if (!line.stable) continue;
        for (line.runtime_refs) |ref| {
            if (ref.public and !ref.stable) try violations.append(arena, .{ .line = line, .offender = ref.name, .why = "runtime declaration is not in docs/api-snapshot.txt" });
        }
        const stems = profile_stems.get(line.walk.profile).?;
        try collectGeneratedRefs(arena, line, &generated_types.get(line.walk.profile).?, stems.items, &ends, &violations);
    }
    if (violations.items.len != 0) {
        std.debug.print(
            "generated-shape: {d} Stable line(s) name an Experimental type. Each is a decision: promote the type,\n" ++
                "or add the line to `experimental_overrides` in tools/generated_shape.zig.\n",
            .{violations.items.len},
        );
        for (violations.items) |v| std.debug.print("  {s}\n    names {s} ({s})\n", .{ v.line.path, v.offender, v.why });
        failures += 1;
    }

    // What each override keeps Experimental, for the reviewer.
    std.debug.print("generated-shape: experimental_overrides (family lines kept Experimental):\n", .{});
    for (experimental_overrides, override_hits) |rule, hits| {
        std.debug.print("  {d:>6}  {s}\n", .{ hits, rule.why });
    }

    // The census.
    std.debug.print("generated-shape: census:\n", .{});
    for (census) |item| {
        var count: usize = 0;
        for (lines.items) |*line| {
            if (item.predicate(line)) count += 1;
        }
        std.debug.print("  {d:>6}  {s}\n", .{ count, item.name });
        if (count == 0) {
            std.debug.print("generated-shape: the corpus has no {s}; a corpus entry is missing or a generator change removed the feature\n", .{item.name});
            failures += 1;
        }
    }

    failures += try checkRequests(io);

    var stable_lines: std.ArrayList([]const u8) = .empty;
    var experimental_lines: std.ArrayList([]const u8) = .empty;
    for (lines.items) |line| {
        if (line.stable) try stable_lines.append(arena, line.text) else try experimental_lines.append(arena, line.text);
    }
    const stable_rendered = try render.renderSnapshot(arena, stable_lines.items, stable_header);
    const experimental_rendered = try render.renderSnapshot(arena, experimental_lines.items, experimental_header);

    switch (mode) {
        .write => {
            // Never refreeze around a failed rule.
            if (failures != 0) return error.GeneratedShapeGateFailed;
            try writeFile(io, stable_path, stable_rendered);
            try writeFile(io, experimental_path, experimental_rendered);
            std.debug.print("generated-shape: wrote {d} stable lines to {s}, {d} experimental lines to {s}\n", .{
                stable_lines.items.len, stable_path, experimental_lines.items.len, experimental_path,
            });
        },
        .check => {
            // Report drift even when a rule above failed, so one run shows
            // everything a change moved.
            const stable_ok = try diffAndReport(init.gpa, io, stable_path, stable_rendered);
            const experimental_ok = try diffAndReport(init.gpa, io, experimental_path, experimental_rendered);
            if (!stable_ok) std.debug.print(
                "generated-shape: the STABLE generated shape changed. Code consumers generate will change with it. Review it\n" ++
                    "(a runtime error-set change moves generated signatures too), then run `zig build generated-shape` and commit.\n",
                .{},
            );
            if (!experimental_ok) std.debug.print(
                "generated-shape: {s} is stale. It is not frozen; run `zig build generated-shape` and commit it.\n",
                .{experimental_path},
            );
            if (failures != 0) return error.GeneratedShapeGateFailed;
            if (!stable_ok) return error.GeneratedShapeDrift;
            if (!experimental_ok) return error.GeneratedShapeExperimentalDrift;
            std.debug.print("generated-shape: OK ({d} stable lines frozen, {d} experimental lines current)\n", .{
                stable_lines.items.len, experimental_lines.items.len,
            });
        },
    }
}
