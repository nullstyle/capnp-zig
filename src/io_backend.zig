//! Switchable `std.Io` backend selection.
//!
//! capnpc-zig's RPC runtime is polymorphic over `std.Io`: every entry point
//! that opens sockets, schedules work, or runs the connection loop accepts a
//! `std.Io` value. This module centralises the choice of which concrete
//! backend to construct so that swapping concrete backends is a single-line
//! change rather than a sweep through every `main`.
//!
//! The project currently builds against Zig 0.17-dev. On platforms where Zig
//! exposes `std.Io.Evented`, the `.evented` selector constructs and owns that
//! backend. On platforms where `std.Io.Evented == void`, initialization returns
//! `error.EventedBackendUnsupported`.
//!
//! Typical usage:
//!
//! ```zig
//! pub fn main(init: std.process.Init) !void {
//!     var backend = try io_backend.Backend.init(.process_init, init.gpa, init.io);
//!     defer backend.deinit();
//!     const io = backend.io();
//!     // ... pass `io` to rpc.transport.tcp.Listener / rpc.transport.tcp.Connection / etc.
//! }
//! ```

const std = @import("std");

/// Selects which `std.Io` backend to construct.
pub const Kind = enum {
    /// Use the `std.Io` provided by `std.process.Init` (the language
    /// default). Zig may change the concrete default in future releases.
    process_init,

    /// Explicitly construct a fresh `std.Io.Threaded`. Useful when you want
    /// to control the thread pool sizing or run multiple isolated I/O
    /// instances in the same process.
    threaded,

    /// Construct `std.Io.Evented` on platforms where Zig exposes it. On
    /// unsupported platforms (`std.Io.Evented == void`), initialization returns
    /// `error.EventedBackendUnsupported`.
    evented,
};

const EventedState = if (std.Io.Evented == void) void else struct {
    allocator: std.mem.Allocator,
    instance: *std.Io.Evented,
};

/// Platform-specific Evented options. This is `void` when Evented is not
/// available in Zig's standard library for the target.
pub const EventedInitOptions = if (std.Io.Evented == void) void else std.Io.Evented.InitOptions;

const EventedInitError = if (std.Io.Evented == void) error{} else eventedInitError(std.Io.Evented);
const evented_deinit_available = if (std.Io.Evented == void) false else std.Io.Evented != std.Io.Dispatch;

fn eventedInitError(comptime Evented: type) type {
    const Return = @typeInfo(@TypeOf(Evented.init)).@"fn".return_type orelse
        @compileError("std.Io.Evented.init must return an error union");
    return @typeInfo(Return).error_union.error_set;
}

/// Errors returned by `Backend.init`.
pub const InitError = error{
    /// Zig's standard library does not provide `std.Io.Evented` for this
    /// target.
    EventedBackendUnsupported,
} || std.mem.Allocator.Error || EventedInitError;

/// Owns the concrete backend storage. Call `.io()` to obtain a `std.Io`
/// suitable for passing into the RPC runtime (`rpc.transport.tcp.Listener`,
/// `rpc.transport.tcp.Connection`, `rpc.transport.tcp.Transport`, etc.).
pub const Backend = union(Kind) {
    process_init: std.Io,
    threaded: std.Io.Threaded,
    evented: EventedState,

    /// Construct a backend of the requested kind.
    ///
    /// - For `.process_init`, `default_io` is returned as-is (typically
    ///   `init.io` from `std.process.Init`).
    /// - For `.threaded`, a fresh `std.Io.Threaded` is constructed with
    ///   default options using `gpa`.
    /// - For `.evented`, constructs an owned `std.Io.Evented` when available,
    ///   or returns `error.EventedBackendUnsupported` when Zig exposes
    ///   `std.Io.Evented` as `void` for the target.
    pub fn init(
        kind: Kind,
        gpa: std.mem.Allocator,
        default_io: std.Io,
    ) InitError!Backend {
        return switch (kind) {
            .process_init => fromInit(default_io),
            .threaded => initThreaded(gpa, .{}),
            .evented => initEvented(gpa, defaultEventedOptions()),
        };
    }

    /// Wrap an existing `std.Io` (typically `std.process.Init.io`). The
    /// caller retains ownership of the underlying backend.
    pub fn fromInit(borrowed_io: std.Io) Backend {
        return .{ .process_init = borrowed_io };
    }

    /// Construct a fresh `std.Io.Threaded` with caller-controlled options.
    pub fn initThreaded(
        gpa: std.mem.Allocator,
        options: std.Io.Threaded.InitOptions,
    ) Backend {
        return .{ .threaded = std.Io.Threaded.init(gpa, options) };
    }

    /// Construct a fresh `std.Io.Evented` with caller-controlled options.
    pub fn initEvented(
        gpa: std.mem.Allocator,
        options: EventedInitOptions,
    ) InitError!Backend {
        if (comptime std.Io.Evented == void) {
            return error.EventedBackendUnsupported;
        } else {
            const instance = try gpa.create(std.Io.Evented);
            errdefer gpa.destroy(instance);
            try instance.init(gpa, options);
            return .{ .evented = .{ .allocator = gpa, .instance = instance } };
        }
    }

    /// Tear down any storage owned by this `Backend`. Safe to call for any
    /// variant; backends that borrow (`.process_init`) are no-ops.
    pub fn deinit(self: *Backend) void {
        switch (self.*) {
            .process_init => {},
            .threaded => |*t| t.deinit(),
            .evented => |state| {
                if (comptime std.Io.Evented != void) {
                    // Zig master (through 0.17.0-dev.813) exposes Dispatch as
                    // Evented on Darwin, but Dispatch.deinit does not compile
                    // in std because it passes a pointer-to-array to
                    // Allocator.free. Other Evented backends use their normal
                    // deinitializer here.
                    if (comptime evented_deinit_available) state.instance.deinit();
                    state.allocator.destroy(state.instance);
                }
            },
        }
    }

    /// Obtain the `std.Io` value to pass into capnpc-zig's RPC runtime.
    pub fn io(self: *Backend) std.Io {
        return switch (self.*) {
            .process_init => |existing| existing,
            .threaded => |*t| t.io(),
            .evented => |state| {
                if (comptime std.Io.Evented == void) unreachable;
                return state.instance.io();
            },
        };
    }
};

fn defaultEventedOptions() EventedInitOptions {
    if (comptime std.Io.Evented == void) return {};
    return .{};
}

/// Parse a backend kind from a textual selector. Accepted spellings (case
/// insensitive): `process_init` / `process-init` / `default`, `threaded`,
/// `evented`. Returns `null` for anything else.
pub fn parseKind(text: []const u8) ?Kind {
    if (std.ascii.eqlIgnoreCase(text, "process_init") or
        std.ascii.eqlIgnoreCase(text, "process-init") or
        std.ascii.eqlIgnoreCase(text, "default"))
    {
        return .process_init;
    }
    if (std.ascii.eqlIgnoreCase(text, "threaded")) return .threaded;
    if (std.ascii.eqlIgnoreCase(text, "evented")) return .evented;
    return null;
}

test "fromInit borrows the provided std.Io" {
    var backend = Backend.fromInit(std.testing.io);
    defer backend.deinit();
    const a = backend.io();
    const b = std.testing.io;
    try std.testing.expectEqual(a.userdata, b.userdata);
    try std.testing.expectEqual(a.vtable, b.vtable);
}

test "initThreaded constructs an owned Threaded backend" {
    var backend = Backend.initThreaded(std.testing.allocator, .{});
    defer backend.deinit();
    const io = backend.io();
    try std.testing.expect(io.userdata == @as(*anyopaque, @ptrCast(&backend.threaded)));
}

test "init dispatches by Kind" {
    var pi = try Backend.init(.process_init, std.testing.allocator, std.testing.io);
    defer pi.deinit();
    try std.testing.expectEqual(Kind.process_init, std.meta.activeTag(pi));

    var t = try Backend.init(.threaded, std.testing.allocator, std.testing.io);
    defer t.deinit();
    try std.testing.expectEqual(Kind.threaded, std.meta.activeTag(t));
}

test "init dispatches evented when supported" {
    if (comptime std.Io.Evented == void) {
        try std.testing.expectError(
            error.EventedBackendUnsupported,
            Backend.init(.evented, std.testing.allocator, std.testing.io),
        );
    } else {
        const allocator = if (comptime evented_deinit_available)
            std.testing.allocator
        else
            std.heap.page_allocator;
        var backend = try Backend.init(.evented, allocator, std.testing.io);
        defer backend.deinit();
        try std.testing.expectEqual(Kind.evented, std.meta.activeTag(backend));

        const io = backend.io();
        try std.testing.expect(io.userdata == @as(*anyopaque, @ptrCast(backend.evented.instance)));
    }
}

test "parseKind accepts canonical and friendly spellings" {
    try std.testing.expectEqual(@as(?Kind, .process_init), parseKind("process_init"));
    try std.testing.expectEqual(@as(?Kind, .process_init), parseKind("process-init"));
    try std.testing.expectEqual(@as(?Kind, .process_init), parseKind("default"));
    try std.testing.expectEqual(@as(?Kind, .threaded), parseKind("THREADED"));
    try std.testing.expectEqual(@as(?Kind, .evented), parseKind("Evented"));
    try std.testing.expectEqual(@as(?Kind, null), parseKind("nonsense"));
}
