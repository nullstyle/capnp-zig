//! Switchable `std.Io` backend selection.
//!
//! capnpc-zig's RPC runtime is polymorphic over `std.Io`: every entry point
//! that opens sockets, schedules work, or runs the connection loop accepts a
//! `std.Io` value. This module centralises the choice of which concrete
//! backend to construct so that swapping (for example) `std.Io.Threaded` for
//! `std.Io.Evented` once it ships upstream is a single-line change rather
//! than a sweep through every `main`.
//!
//! Typical usage:
//!
//! ```zig
//! pub fn main(init: std.process.Init) !void {
//!     var backend = try io_backend.Backend.init(.process_init, init.gpa, init.io);
//!     defer backend.deinit();
//!     const io = backend.io();
//!     // ... pass `io` to rpc.runtime.Listener / rpc.connection.Connection / etc.
//! }
//! ```

const std = @import("std");

/// Selects which `std.Io` backend to construct.
pub const Kind = enum {
    /// Use the `std.Io` provided by `std.process.Init` (the language
    /// default). In Zig 0.16 this is `std.Io.Threaded`; Zig may change the
    /// default in future releases.
    process_init,

    /// Explicitly construct a fresh `std.Io.Threaded`. Useful when you want
    /// to control the thread pool sizing or run multiple isolated I/O
    /// instances in the same process.
    threaded,

    /// Reserved for `std.Io.Evented` once it lands upstream. Selecting this
    /// today returns `error.EventedBackendNotImplemented`. Once Zig ships
    /// the evented backend, the implementation here will be filled in and
    /// callers that already select `.evented` will start using it.
    evented,
};

/// Errors returned by `Backend.init`.
pub const InitError = error{
    /// `std.Io.Evented` is not yet available in the standard library.
    EventedBackendNotImplemented,
};

/// Owns the concrete backend storage. Call `.io()` to obtain a `std.Io`
/// suitable for passing into the RPC runtime (`rpc.runtime.Listener`,
/// `rpc.connection.Connection`, `rpc.transport.Transport`, etc.).
pub const Backend = union(Kind) {
    process_init: std.Io,
    threaded: std.Io.Threaded,
    evented: void,

    /// Construct a backend of the requested kind.
    ///
    /// - For `.process_init`, `default_io` is returned as-is (typically
    ///   `init.io` from `std.process.Init`).
    /// - For `.threaded`, a fresh `std.Io.Threaded` is constructed with
    ///   default options using `gpa`.
    /// - For `.evented`, returns `error.EventedBackendNotImplemented`
    ///   until Zig ships the evented backend.
    pub fn init(
        kind: Kind,
        gpa: std.mem.Allocator,
        default_io: std.Io,
    ) InitError!Backend {
        return switch (kind) {
            .process_init => fromInit(default_io),
            .threaded => initThreaded(gpa, .{}),
            .evented => error.EventedBackendNotImplemented,
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

    /// Tear down any storage owned by this `Backend`. Safe to call for any
    /// variant; backends that borrow (`.process_init`) are no-ops.
    pub fn deinit(self: *Backend) void {
        switch (self.*) {
            .process_init => {},
            .threaded => |*t| t.deinit(),
            .evented => {},
        }
    }

    /// Obtain the `std.Io` value to pass into capnpc-zig's RPC runtime.
    pub fn io(self: *Backend) std.Io {
        return switch (self.*) {
            .process_init => |existing| existing,
            .threaded => |*t| t.io(),
            .evented => unreachable,
        };
    }
};

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

test "init returns error for evented" {
    try std.testing.expectError(
        error.EventedBackendNotImplemented,
        Backend.init(.evented, std.testing.allocator, std.testing.io),
    );
}

test "parseKind accepts canonical and friendly spellings" {
    try std.testing.expectEqual(@as(?Kind, .process_init), parseKind("process_init"));
    try std.testing.expectEqual(@as(?Kind, .process_init), parseKind("process-init"));
    try std.testing.expectEqual(@as(?Kind, .process_init), parseKind("default"));
    try std.testing.expectEqual(@as(?Kind, .threaded), parseKind("THREADED"));
    try std.testing.expectEqual(@as(?Kind, .evented), parseKind("Evented"));
    try std.testing.expectEqual(@as(?Kind, null), parseKind("nonsense"));
}
