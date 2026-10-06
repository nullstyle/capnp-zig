//! `zig build check-fd-passing-off-symbols` reads `nm` output of an embedder
//! static library with this tool and fails on a forbidden symbol.
//!
//!   archive-symbols --nm <file> [rules...]
//!   archive-symbols --skip <reason>
//!
//! Rules (each may repeat):
//!   --forbid-undefined <name>          an import of exactly <name>
//!   --forbid-undefined-prefix <prefix> an import whose name starts so
//!   --forbid-function-containing <text>
//!                                      a defined function (a text symbol,
//!                                      `T` or `t`) whose name has <text>
//!   --min-symbols <n>                  fail when fewer symbols were read,
//!                                      so an empty or unreadable listing
//!                                      can never pass
//!
//! The input is plain `nm` output (BSD or GNU format): `<addr> <type> <name>`
//! for a defined symbol, `<type> <name>` for an undefined one (type `U`),
//! and `<member>:` headers between an archive's members. `--skip` prints
//! the reason and succeeds; the build uses it where the host has no Mach-O
//! `nm`.

const std = @import("std");

const max_file_bytes = 64 * 1024 * 1024;

const Rule = union(enum) {
    undefined_exact: []const u8,
    undefined_prefix: []const u8,
    function_containing: []const u8,
};

const Symbol = struct {
    /// The `nm` type letter: `U` for an import, `T`/`t` for a function.
    kind: u8,
    name: []const u8,

    fn defined(symbol: Symbol) bool {
        return symbol.kind != 'U' and symbol.kind != 'u';
    }

    fn function(symbol: Symbol) bool {
        return symbol.kind == 'T' or symbol.kind == 't';
    }
};

/// One `nm` line as a symbol, or null for a header, a blank line or a line
/// it cannot read.
fn parseLine(line: []const u8) ?Symbol {
    var fields: [3][]const u8 = undefined;
    var count: usize = 0;
    var it = std.mem.tokenizeAny(u8, line, " \t\r");
    while (it.next()) |field| {
        if (count == fields.len) return null;
        fields[count] = field;
        count += 1;
    }
    switch (count) {
        2 => {
            if (fields[0].len != 1) return null;
            const kind = fields[0][0];
            if (kind != 'U' and kind != 'u') return null;
            return .{ .kind = kind, .name = fields[1] };
        },
        3 => {
            if (fields[1].len != 1) return null;
            return .{ .kind = fields[1][0], .name = fields[2] };
        },
        else => return null,
    }
}

fn violates(rule: Rule, symbol: Symbol) bool {
    return switch (rule) {
        .undefined_exact => |name| !symbol.defined() and std.mem.eql(u8, symbol.name, name),
        .undefined_prefix => |prefix| !symbol.defined() and std.mem.startsWith(u8, symbol.name, prefix),
        .function_containing => |text| symbol.function() and std.mem.indexOf(u8, symbol.name, text) != null,
    };
}

fn describe(rule: Rule) []const u8 {
    return switch (rule) {
        .undefined_exact, .undefined_prefix => "forbidden import",
        .function_containing => "forbidden function",
    };
}

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();
    const io = init.io;

    var rules: std.ArrayList(Rule) = .empty;
    defer rules.deinit(allocator);
    var nm_path: ?[]const u8 = null;
    var min_symbols: usize = 1;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.skip();
    while (args.next()) |arg| {
        const value = args.next() orelse {
            std.debug.print("archive-symbols: {s} needs a value\n", .{arg});
            return error.InvalidArgument;
        };
        if (std.mem.eql(u8, arg, "--skip")) {
            std.debug.print("SKIPPED: {s}\n", .{value});
            return;
        } else if (std.mem.eql(u8, arg, "--nm")) {
            nm_path = value;
        } else if (std.mem.eql(u8, arg, "--forbid-undefined")) {
            try rules.append(allocator, .{ .undefined_exact = value });
        } else if (std.mem.eql(u8, arg, "--forbid-undefined-prefix")) {
            try rules.append(allocator, .{ .undefined_prefix = value });
        } else if (std.mem.eql(u8, arg, "--forbid-function-containing")) {
            try rules.append(allocator, .{ .function_containing = value });
        } else if (std.mem.eql(u8, arg, "--min-symbols")) {
            min_symbols = std.fmt.parseInt(usize, value, 10) catch {
                std.debug.print("archive-symbols: --min-symbols takes a number, not {s}\n", .{value});
                return error.InvalidArgument;
            };
        } else {
            std.debug.print("archive-symbols: unknown argument {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    const path = nm_path orelse {
        std.debug.print("archive-symbols: pass --nm <file> or --skip <reason>\n", .{});
        return error.InvalidArgument;
    };

    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_file_bytes));
    defer allocator.free(bytes);

    var symbols: usize = 0;
    var imports: usize = 0;
    var failures: usize = 0;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |line| {
        const symbol = parseLine(line) orelse continue;
        symbols += 1;
        if (!symbol.defined()) imports += 1;
        for (rules.items) |rule| {
            if (!violates(rule, symbol)) continue;
            failures += 1;
            std.debug.print("[FAIL] {s}: {s}\n", .{ describe(rule), symbol.name });
        }
    }

    if (symbols < min_symbols) {
        std.debug.print("archive-symbols: read {d} symbol(s), expected at least {d}: is the nm output empty?\n", .{ symbols, min_symbols });
        return error.TooFewSymbols;
    }
    if (failures != 0) {
        std.debug.print("archive-symbols: {d} forbidden symbol(s) among {d} ({d} imports)\n", .{ failures, symbols, imports });
        return error.ForbiddenSymbols;
    }
    std.debug.print("archive-symbols: {d} symbol(s) ({d} imports), none forbidden by {d} rule(s)\n", .{ symbols, imports, rules.items.len });
}

test "parseLine reads defined, undefined and header lines" {
    const function = parseLine("0000000000002230 t _rpc.transport.unix.fd_closer.trim").?;
    try std.testing.expect(function.defined() and function.function());
    try std.testing.expectEqualStrings("_rpc.transport.unix.fd_closer.trim", function.name);
    const data = parseLine("0000000000000010 s _rpc.transport.unix.fd_closer.supported").?;
    try std.testing.expect(data.defined() and !data.function());
    const imported = parseLine("                 U ___ulock_wait2").?;
    try std.testing.expect(!imported.defined());
    try std.testing.expectEqualStrings("___ulock_wait2", imported.name);
    try std.testing.expect(parseLine("root_zcu.o:") == null);
    try std.testing.expect(parseLine("") == null);
}

test "rules match only their kind of symbol" {
    const imported: Symbol = .{ .kind = 'U', .name = "___ulock_wake" };
    const function: Symbol = .{ .kind = 'T', .name = "___ulock_wake" };
    const data: Symbol = .{ .kind = 'S', .name = "___ulock_wake" };
    try std.testing.expect(violates(.{ .undefined_prefix = "___ulock_" }, imported));
    try std.testing.expect(!violates(.{ .undefined_prefix = "___ulock_" }, function));
    try std.testing.expect(violates(.{ .undefined_exact = "___ulock_wake" }, imported));
    try std.testing.expect(!violates(.{ .undefined_exact = "___ulock_wai" }, imported));
    try std.testing.expect(violates(.{ .function_containing = "ulock" }, function));
    try std.testing.expect(!violates(.{ .function_containing = "ulock" }, data));
    try std.testing.expect(!violates(.{ .function_containing = "ulock" }, imported));
}
