//! Cap'n Proto RPC over an AF_UNIX stream socket at a filesystem path:
//! `rpc.transport.unix.listen` and `rpc.transport.unix.connect`.
//!
//! Experimental. Linux and Darwin only (`supported`). On every other target,
//! Windows included, both calls return `error.UnixSocketsUnsupported`.
//!
//! Both reuse the TCP stack, which runs over any stream socket:
//! - `listen` returns a `tcp.Listener`, so the frozen `ServerSession.accept`
//!   (and `Listener.accept`) serve it unchanged.
//! - `connect` returns a `*tcp.ClientSession`, wired exactly like
//!   `tcp.connect`.
//!
//! By default every connection reads in drain mode (`fd_io`): fds a peer
//! attaches are closed off the reader thread, never kept. With
//! `fd_passing.max_fds_per_message > 0` (`ListenOptions`, `ConnectOptions`)
//! the connection keeps up to that many per message, and the `Peer`
//! attaches them to the capabilities they came with (`Peer.importFd`).
//! The same option turns sending on: only then does an export given an fd
//! with `Peer.setExportFd` carry it. A connection in drain mode sends no
//! fds, as in C++, because a receiver that did not ask for them still gets
//! them installed on macOS. Connections report `events.Source.unix`.
//!
//! ## The socket file
//!
//! - **Paths only.** A leading NUL (a Linux abstract name) returns
//!   `AbstractNameUnsupported`. An empty path, or one with a NUL inside it,
//!   returns `BadPathName`.
//! - **Length.** `path.len` must be shorter than `sun_path` (104 bytes on
//!   Darwin, 108 on Linux), so the kernel always gets a NUL-terminated
//!   name. A longer path returns `NameTooLong`.
//! - **Lock.** `listen` opens `<path>.lock` (`O_NOFOLLOW`, mode 0600) and
//!   takes `flock(LOCK_EX | LOCK_NB)` before it touches the socket file. It
//!   holds the lock until `Listener.close`. If another listener holds it,
//!   `listen` returns `AddressInUse`. The lock file is never removed:
//!   removing it would let two servers lock two different files. The lock
//!   fd is close-on-exec, but a child that forks without exec shares it and
//!   keeps the path locked until it exits.
//! - **Stale files.** A server that dies leaves its socket file behind.
//!   With `reclaim_stale = false` (the default), `bind` then fails and
//!   `listen` returns `AddressInUse`. With `true`, `listen` (holding the
//!   lock, so no live server built with `listen` owns the path) removes the
//!   file if it is a socket, then binds. A file of any other type is left
//!   alone, and `listen` returns `AddressInUse`. `listen` never probes with a
//!   connect: a server between its `bind` and its `listen` refuses connects
//!   too, so a refused connect does not prove the file stale. The lock only
//!   protects servers that take it, so every server on a path must be built
//!   with `listen`.
//! - **Permissions.** Connecting needs write permission on the socket
//!   file. `listen` sets `socket_mode` (default 0600) after `bind` and
//!   before `listen`, so no client can connect while the file still has the
//!   umask's mode. Right after `bind`, just before the `chmod`, it records
//!   the file's (dev, ino); after the `chmod` it checks that the path still
//!   names that file (`SocketPathChanged` otherwise). `chmod` follows a
//!   symbolic link, and the checks narrow, but cannot close, the window in
//!   which another user who can write the directory swaps the path. Put the
//!   socket in a private directory (mode 0700) that you own: then nobody
//!   else can swap it.
//! - **Close.** `Listener.close` removes the path only while it still
//!   names this listener's file (same dev and ino), then closes the socket
//!   and releases the lock. A relative path resolves against the current
//!   directory at each step: after a `chdir`, `close` finds a different file
//!   (or none) and leaves the socket file in place.
//! - **Flags.** The listening and client sockets are close-on-exec (std
//!   already makes accepted sockets close-on-exec). No `TCP_NODELAY`: these
//!   are not TCP sockets.
//!
//! ## Connect timeout
//!
//! On Linux a blocking connect to a listener whose backlog is full waits
//! until the server accepts. A non-blocking connect returns EAGAIN at once,
//! and `poll` on that socket reports it writable, so neither can wait for
//! room. `connect` therefore makes a blocking connect bounded by
//! `SO_SNDTIMEO` (`connect_timeout_ms`), which Linux applies to AF_UNIX
//! connects, and resets it to 0 before the socket carries any data. macOS
//! never waits: a full backlog refuses the connect (`ConnectionRefused`).
//! So on macOS `connect` does not touch `SO_SNDTIMEO` at all: XNU refuses
//! every socket option (EINVAL) once the socket is fully shut down, and a
//! server that accepts and closes at once does that between the connect
//! and the reset.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const log = std.log.scoped(.rpc_unix);

const fd_io = @import("fd_io.zig");
const fd_passing_mod = @import("../fd_passing.zig");
const runtime = @import("../tcp/runtime.zig");
const client = @import("../tcp/client.zig");
const client_wiring = @import("../tcp/client_wiring.zig");
const connection_mod = @import("../tcp/connection.zig");

const Listener = runtime.Listener;
const ClientSession = client.ClientSession;
const Connection = connection_mod.Connection;
const Fd = fd_io.Fd;

/// True where `listen` and `connect` work: Linux and Darwin.
pub const supported: bool = fd_io.supported;

const is_linux = builtin.target.os.tag == .linux;

/// Bytes a `SocketFile` keeps for its path: Linux's `sun_path` length. Darwin's
/// is 104. One size on every target keeps `tcp.Listener`'s layout (and its
/// API snapshot line) the same everywhere.
const path_capacity = 108;

/// `sun_path`'s length on this target. A path must be shorter.
const sun_path_len: usize = if (supported) @typeInfo(@FieldType(posix.sockaddr.un, "path")).array.len else path_capacity;

comptime {
    std.debug.assert(sun_path_len <= path_capacity);
}

const lock_suffix = ".lock";

/// Options for `listen`.
pub const ListenOptions = struct {
    /// Options for every `Connection` that `Listener.accept` creates.
    /// `ServerSession.accept` takes its own `ServeOptions` instead.
    conn: Connection.Options = .{},
    /// The kernel's queue of connections not yet accepted.
    backlog: u31 = 128,
    /// Permission bits of the socket file, set before the socket listens.
    /// Clients need write permission to connect. Bits outside 0o777 return
    /// `InvalidSocketMode`.
    socket_mode: u32 = 0o600,
    /// Remove a socket file that a server which is gone left at the path.
    /// See "Stale files" in the module doc.
    reclaim_stale: bool = false,
    /// Fd passing on every accepted connection (`Listener.accept` and
    /// `ServerSession.accept`). The default keeps no received fd and sends
    /// none.
    /// `ServerSession.accept` also gives its peer `max_live_imported_fds`;
    /// a `Peer` you build on a `Listener.accept` connection takes it from
    /// `Peer.setMaxLiveImportedFds` (default 64).
    fd_passing: FdPassing = .{},
};

/// Fd passing on one connection; see `rpc.transport.unix.FdPassing`.
pub const FdPassing = fd_passing_mod.FdPassing;

/// Every way `listen` fails.
pub const ListenError = error{
    /// The path starts with a NUL byte (a Linux abstract name).
    AbstractNameUnsupported,
    AccessDenied,
    /// Another listener holds the path's lock, or a file is at the path
    /// (see "Stale files" in the module doc).
    AddressInUse,
    /// The path is empty or has a NUL byte inside it.
    BadPathName,
    /// A directory in the path does not exist.
    FileNotFound,
    /// `socket_mode` has bits outside 0o777.
    InvalidSocketMode,
    /// `path.len` is not shorter than `sun_path` (104 on Darwin, 108 on
    /// Linux).
    NameTooLong,
    NoSpaceLeft,
    /// A component of the path is not a directory.
    NotDir,
    ProcessFdQuotaExceeded,
    ReadOnlyFileSystem,
    /// Between `bind` and `listen` the path stopped naming the socket file
    /// this call created.
    SocketPathChanged,
    /// `<path>.lock` is a symbolic link, or the path has too many of them.
    SymLinkLoop,
    SystemFdQuotaExceeded,
    SystemResources,
    Unexpected,
    UnixSocketsUnsupported,
};

/// Options for `connect`.
pub const ConnectOptions = struct {
    /// Everything `tcp.connect` takes: connection options, timeouts,
    /// limits, observer and callbacks.
    session: client.ConnectOptions = .{},
    /// How long the connect itself may wait (Linux waits while the
    /// server's backlog is full; macOS never waits). 0 acts as 1 ms. Null
    /// waits without a bound.
    connect_timeout_ms: ?u64 = 30_000,
    /// Fd passing on the connection. The default keeps no received fd and
    /// sends none.
    fd_passing: FdPassing = .{},
};

/// Every way `connect` fails.
pub const ConnectError = error{
    /// The path starts with a NUL byte (a Linux abstract name).
    AbstractNameUnsupported,
    /// No write permission on the socket file, or no search permission on
    /// a directory in the path.
    AccessDenied,
    /// The path is empty or has a NUL byte inside it.
    BadPathName,
    Canceled,
    /// Nothing listens at the path, the file is not a stream socket, or
    /// (macOS) the listener's backlog is full.
    ConnectionRefused,
    /// Nothing is at the path.
    FileNotFound,
    /// `path.len` is not shorter than `sun_path` (104 on Darwin, 108 on
    /// Linux).
    NameTooLong,
    /// A component of the path is not a directory.
    NotDir,
    OutOfMemory,
    ProcessFdQuotaExceeded,
    SymLinkLoop,
    SystemFdQuotaExceeded,
    SystemResources,
    /// `connect_timeout_ms` passed (Linux: the backlog stayed full).
    Timeout,
    Unexpected,
    UnixSocketsUnsupported,
};

/// The socket file a `tcp.Listener` from `listen` owns: its path, its
/// identity, and the held lock. Internal state of the listener (read the
/// path with `Listener.unixPath`).
pub const SocketFile = struct {
    path_buf: [path_capacity]u8,
    path_len: u8,
    /// Device and inode of the file `bind` created.
    dev: u64,
    ino: u64,
    /// The open `<path>.lock` with the flock held; -1 once released.
    lock_fd: Fd,

    /// The path `listen` bound.
    pub fn path(self: *const SocketFile) []const u8 {
        return self.path_buf[0..self.path_len];
    }

    /// Remove the path if it still names the file `bind` created. Holding
    /// the lock while this runs keeps every other `listen` off the path.
    pub fn unlinkIfOurs(self: *const SocketFile) void {
        if (comptime !supported) return;
        var z: [path_capacity + 1]u8 = undefined;
        const path_z = nulTerminate(&z, self.path());
        const id = statPath(path_z) catch return;
        if (id.dev != self.dev or id.ino != self.ino) return;
        switch (posix.errno(posix.system.unlink(path_z))) {
            .SUCCESS, .NOENT => {},
            else => |err| log.debug("unlink of the socket file failed: errno {d}", .{@backingInt(err)}),
        }
    }

    /// Close the lock file, which releases the flock. Idempotent.
    pub fn releaseLock(self: *SocketFile) void {
        if (comptime !supported) return;
        if (self.lock_fd < 0) return;
        closeRaw(self.lock_fd);
        self.lock_fd = -1;
    }
};

/// Bind and listen on an AF_UNIX stream socket at `path`. See the module
/// doc for the lock, stale files, permissions and what `close` removes.
///
/// The returned `tcp.Listener` accepts with `Listener.accept`,
/// `Listener.acceptFd` or `ServerSession.accept`. `Listener.unixPath`
/// returns `path`; `getAddress` has no meaning for it (it returns 0.0.0.0:0).
/// Call `close` exactly as for a TCP listener.
pub fn listen(
    gpa: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    options: ListenOptions,
) ListenError!Listener {
    if (comptime !supported) return error.UnixSocketsUnsupported;
    if (options.socket_mode & ~@as(u32, 0o777) != 0) return error.InvalidSocketMode;
    try validatePath(path);

    var file: SocketFile = .{
        .path_buf = undefined,
        .path_len = @intCast(path.len),
        .dev = 0,
        .ino = 0,
        .lock_fd = -1,
    };
    @memcpy(file.path_buf[0..path.len], path);

    var path_z_buf: [path_capacity + 1]u8 = undefined;
    const path_z = nulTerminate(&path_z_buf, path);
    var lock_z_buf: [path_capacity + lock_suffix.len + 1]u8 = undefined;
    @memcpy(lock_z_buf[0..path.len], path);
    @memcpy(lock_z_buf[path.len..][0..lock_suffix.len], lock_suffix);
    lock_z_buf[path.len + lock_suffix.len] = 0;
    const lock_z: [*:0]const u8 = @ptrCast(&lock_z_buf);

    file.lock_fd = try openLock(lock_z);
    errdefer file.releaseLock();
    try takeLock(file.lock_fd);

    const fd = try openSocket(ListenError);
    errdefer closeRaw(fd);

    if (options.reclaim_stale) try removeStaleSocket(path_z);

    var addr = sockaddrFor(path);
    try bindSocket(fd, &addr);
    // Record the identity of the file bind created. This is also the check
    // before the chmod. If it cannot be read, the path already names
    // something else (or nothing): leave it alone.
    const id = statPath(path_z) catch |err| return switch (err) {
        error.FileNotFound, error.NotDir => error.SocketPathChanged,
        else => |e| e,
    };
    file.dev = id.dev;
    file.ino = id.ino;
    errdefer file.unlinkIfOurs();

    // Before `listen`: until then a connect is refused, so no client can
    // reach the socket while it has the umask's mode.
    try chmodPath(path_z, options.socket_mode);
    try expectSameFile(path_z, &file);

    try listenSocket(fd, options.backlog);

    var listener = Listener.initFd(gpa, io, .{ .handle = fd }, options.conn);
    listener.unix_socket = file;
    listener.fd_passing = options.fd_passing;
    return listener;
}

/// Connect to the AF_UNIX stream socket at `path` and return a live
/// session, exactly as `tcp.connect` does for TCP (same lifecycle, same
/// callbacks). See "Connect timeout" in the module doc.
pub fn connect(
    gpa: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    options: ConnectOptions,
) ConnectError!*ClientSession {
    if (comptime !supported) return error.UnixSocketsUnsupported;
    try validatePath(path);

    const fd = try openSocket(ConnectError);
    {
        errdefer closeRaw(fd);
        var addr = sockaddrFor(path);
        try connectSocket(fd, &addr, options.connect_timeout_ms);
    }
    // `wire` owns the socket from here, on success and on error.
    return client_wiring.wireWithFdPassing(gpa, io, .{ .handle = fd }, options.session, options.fd_passing);
}

// ---------------------------------------------------------------------------
// Path checks
// ---------------------------------------------------------------------------

fn validatePath(path: []const u8) error{ AbstractNameUnsupported, BadPathName, NameTooLong }!void {
    if (path.len == 0) return error.BadPathName;
    if (path[0] == 0) return error.AbstractNameUnsupported;
    if (path.len >= sun_path_len) return error.NameTooLong;
    if (std.mem.indexOfScalar(u8, path, 0) != null) return error.BadPathName;
}

/// `path` copied into `buf` with a NUL after it. `path.len < buf.len`.
fn nulTerminate(buf: *[path_capacity + 1]u8, path: []const u8) [*:0]const u8 {
    @memcpy(buf[0..path.len], path);
    buf[path.len] = 0;
    return @ptrCast(buf);
}

fn sockaddrFor(path: []const u8) posix.sockaddr.un {
    var addr: posix.sockaddr.un = .{ .family = posix.AF.UNIX, .path = undefined };
    @memset(&addr.path, 0);
    @memcpy(addr.path[0..path.len], path);
    return addr;
}

// ---------------------------------------------------------------------------
// File identity
// ---------------------------------------------------------------------------

const FileId = struct { dev: u64, ino: u64, is_socket: bool };

const StatError = error{
    AccessDenied,
    FileNotFound,
    NameTooLong,
    NotDir,
    SymLinkLoop,
    SystemResources,
    Unexpected,
};

/// `lstat`: the identity of whatever `path` names, not following a final
/// symbolic link.
fn statPath(path_z: [*:0]const u8) StatError!FileId {
    const linux = std.os.linux;
    while (true) {
        var dev: u64 = undefined;
        var ino: u64 = undefined;
        var mode: u32 = undefined;
        const err = if (is_linux) err: {
            // Raw statx on Linux with or without libc: std has no fstatat
            // there, and musl may lack the statx wrapper.
            var stx: linux.Statx = undefined;
            const rc = linux.statx(linux.AT.FDCWD, path_z, linux.AT.SYMLINK_NOFOLLOW, .{ .TYPE = true, .INO = true }, &stx);
            dev = (@as(u64, stx.dev_major) << 32) | stx.dev_minor;
            ino = stx.ino;
            mode = stx.mode;
            break :err linux.errno(rc);
        } else err: {
            var st: std.c.Stat = undefined;
            const rc = std.c.fstatat(std.c.AT.FDCWD, path_z, &st, std.c.AT.SYMLINK_NOFOLLOW);
            dev = @as(u32, @bitCast(st.dev));
            ino = st.ino;
            mode = st.mode;
            break :err posix.errno(rc);
        };
        switch (err) {
            .SUCCESS => return .{ .dev = dev, .ino = ino, .is_socket = mode & 0o170000 == 0o140000 },
            .INTR => continue,
            .ACCES => return error.AccessDenied,
            .NOENT => return error.FileNotFound,
            .NAMETOOLONG => return error.NameTooLong,
            .NOTDIR => return error.NotDir,
            .LOOP => return error.SymLinkLoop,
            .NOMEM => return error.SystemResources,
            else => |e| return unexpected("stat", e),
        }
    }
}

fn expectSameFile(path_z: [*:0]const u8, file: *const SocketFile) ListenError!void {
    const id = statPath(path_z) catch |err| return switch (err) {
        error.FileNotFound, error.NotDir => error.SocketPathChanged,
        else => |e| e,
    };
    if (id.dev != file.dev or id.ino != file.ino) return error.SocketPathChanged;
}

// ---------------------------------------------------------------------------
// Syscalls
// ---------------------------------------------------------------------------

fn unexpected(what: []const u8, err: posix.E) error{Unexpected} {
    // Log the number, never the tag: `posix.E` does not name every errno.
    log.debug("{s} failed: errno {d}", .{ what, @backingInt(err) });
    return error.Unexpected;
}

fn closeRaw(fd: Fd) void {
    _ = posix.system.close(fd);
}

fn openLock(lock_z: [*:0]const u8) ListenError!Fd {
    while (true) {
        // NONBLOCK: a FIFO planted at the lock path must not hang the open.
        const rc = posix.system.openat(posix.AT.FDCWD, lock_z, .{
            .ACCMODE = .RDONLY,
            .CREAT = true,
            .NOFOLLOW = true,
            .CLOEXEC = true,
            .NONBLOCK = true,
        }, @as(posix.mode_t, 0o600));
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .ACCES, .PERM => return error.AccessDenied,
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            .LOOP => return error.SymLinkLoop,
            .NAMETOOLONG => return error.NameTooLong,
            .ROFS => return error.ReadOnlyFileSystem,
            .NOSPC, .DQUOT => return error.NoSpaceLeft,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NFILE => return error.SystemFdQuotaExceeded,
            .NOMEM => return error.SystemResources,
            else => |e| return unexpected("open of the lock file", e),
        }
    }
}

fn takeLock(lock_fd: Fd) ListenError!void {
    while (true) {
        switch (posix.errno(posix.system.flock(lock_fd, posix.LOCK.EX | posix.LOCK.NB))) {
            .SUCCESS => return,
            .INTR => continue,
            .AGAIN => return error.AddressInUse,
            .NOLCK => return error.SystemResources,
            else => |e| return unexpected("flock", e),
        }
    }
}

/// A close-on-exec AF_UNIX stream socket. `E` is the caller's error set.
fn openSocket(comptime E: type) E!Fd {
    const flags: u32 = posix.SOCK.STREAM | if (is_linux) posix.SOCK.CLOEXEC else 0;
    const fd: Fd = while (true) {
        const rc = posix.system.socket(posix.AF.UNIX, flags, 0);
        switch (posix.errno(rc)) {
            .SUCCESS => break @intCast(rc),
            .INTR => continue,
            .AFNOSUPPORT, .PROTONOSUPPORT => return error.UnixSocketsUnsupported,
            .ACCES => return error.AccessDenied,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NFILE => return error.SystemFdQuotaExceeded,
            .NOBUFS, .NOMEM => return error.SystemResources,
            else => |e| return unexpected("socket", e),
        }
    };
    if (!is_linux) {
        // Darwin has no SOCK_CLOEXEC.
        errdefer closeRaw(fd);
        while (true) {
            switch (posix.errno(posix.system.fcntl(fd, posix.F.SETFD, @as(usize, posix.FD_CLOEXEC)))) {
                .SUCCESS => break,
                .INTR => continue,
                else => |e| return unexpected("fcntl(FD_CLOEXEC)", e),
            }
        }
    }
    return fd;
}

fn removeStaleSocket(path_z: [*:0]const u8) ListenError!void {
    const id = statPath(path_z) catch |err| switch (err) {
        error.FileNotFound => return,
        else => |e| return e,
    };
    // Only a socket is a stale server's file. Anything else stays, and
    // bind reports AddressInUse.
    if (!id.is_socket) return;
    switch (posix.errno(posix.system.unlink(path_z))) {
        .SUCCESS, .NOENT => {},
        .ACCES, .PERM => return error.AccessDenied,
        .ROFS => return error.ReadOnlyFileSystem,
        .NOTDIR => return error.NotDir,
        .LOOP => return error.SymLinkLoop,
        .NOMEM => return error.SystemResources,
        else => |e| return unexpected("unlink of a stale socket file", e),
    }
}

fn bindSocket(fd: Fd, addr: *const posix.sockaddr.un) ListenError!void {
    switch (posix.errno(posix.system.bind(fd, @ptrCast(addr), @sizeOf(posix.sockaddr.un)))) {
        .SUCCESS => {},
        .ADDRINUSE => return error.AddressInUse,
        .ACCES, .PERM => return error.AccessDenied,
        .NOENT => return error.FileNotFound,
        .NOTDIR => return error.NotDir,
        .LOOP => return error.SymLinkLoop,
        .NAMETOOLONG => return error.NameTooLong,
        .ROFS => return error.ReadOnlyFileSystem,
        .NOSPC, .DQUOT => return error.NoSpaceLeft,
        .NOBUFS, .NOMEM => return error.SystemResources,
        else => |e| return unexpected("bind", e),
    }
}

fn chmodPath(path_z: [*:0]const u8, mode: u32) ListenError!void {
    while (true) {
        switch (posix.errno(posix.system.chmod(path_z, @intCast(mode)))) {
            .SUCCESS => return,
            .INTR => continue,
            .ACCES, .PERM => return error.AccessDenied,
            .NOENT, .NOTDIR => return error.SocketPathChanged,
            .LOOP => return error.SymLinkLoop,
            .ROFS => return error.ReadOnlyFileSystem,
            .NOMEM => return error.SystemResources,
            else => |e| return unexpected("chmod", e),
        }
    }
}

fn listenSocket(fd: Fd, backlog: u31) ListenError!void {
    switch (posix.errno(posix.system.listen(fd, backlog))) {
        .SUCCESS => {},
        .ADDRINUSE => return error.AddressInUse,
        .NOBUFS, .NOMEM => return error.SystemResources,
        else => |e| return unexpected("listen", e),
    }
}

/// `SO_SNDTIMEO`'s value. Linux: two `long`s (the classic ABI, which
/// `SO_SNDTIMEO` keeps on 32-bit targets too). Darwin: libc's `timeval`.
const SendTimeout = if (is_linux) extern struct { sec: c_long, usec: c_long } else posix.timeval;

/// Whether `connect` bounds its wait with `SO_SNDTIMEO`. Only Linux waits
/// in an AF_UNIX connect. Darwin refuses a full backlog at once, so the
/// option bounds nothing there, and touching it after the connect can fail:
/// XNU refuses every socket option (EINVAL) once the socket is fully shut
/// down, which a server that accepts and closes at once causes.
const connect_uses_send_timeout = is_linux;

fn setSendTimeout(fd: Fd, ms: u64) error{Unexpected}!void {
    switch (sendTimeoutErrno(fd, ms)) {
        .SUCCESS => {},
        else => |e| return unexpected("setsockopt(SO_SNDTIMEO)", e),
    }
}

fn sendTimeoutErrno(fd: Fd, ms: u64) posix.E {
    const sec = @min(ms / 1000, std.math.maxInt(i32));
    const usec = (ms % 1000) * 1000;
    const tv: SendTimeout = .{ .sec = @intCast(sec), .usec = @intCast(usec) };
    const bytes = std.mem.asBytes(&tv);
    return posix.errno(posix.system.setsockopt(fd, posix.SOL.SOCKET, posix.SO.SNDTIMEO, bytes, bytes.len));
}

fn nowMs() u64 {
    var ts: posix.timespec = undefined;
    _ = posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * 1000 + @as(u64, @intCast(ts.nsec)) / std.time.ns_per_ms;
}

fn connectSocket(fd: Fd, addr: *const posix.sockaddr.un, timeout_ms: ?u64) ConnectError!void {
    const deadline: ?u64 = if (timeout_ms) |ms| nowMs() + @max(ms, 1) else null;
    while (true) {
        if (deadline) |d| {
            const now = nowMs();
            if (now >= d) return error.Timeout;
            if (connect_uses_send_timeout) try setSendTimeout(fd, d - now);
        }
        const rc = posix.system.connect(fd, @ptrCast(addr), @sizeOf(posix.sockaddr.un));
        switch (posix.errno(rc)) {
            .SUCCESS, .ISCONN => break,
            // A signal: try again with what is left of the deadline.
            .INTR => continue,
            // Linux: SO_SNDTIMEO ran out while the backlog stayed full.
            .AGAIN, .INPROGRESS, .TIMEDOUT => return error.Timeout,
            .NOENT => return error.FileNotFound,
            .CONNREFUSED => return error.ConnectionRefused,
            // The file is not a socket (macOS; Linux says CONNREFUSED), or
            // is a socket of another type. `fd` itself is a socket.
            .NOTSOCK, .PROTOTYPE => return error.ConnectionRefused,
            .ACCES, .PERM => return error.AccessDenied,
            .NOTDIR => return error.NotDir,
            .LOOP => return error.SymLinkLoop,
            .NAMETOOLONG => return error.NameTooLong,
            .NOBUFS, .NOMEM => return error.SystemResources,
            else => |e| return unexpected("connect", e),
        }
    }
    try finishConnect(fd, deadline != null);
}

/// Undo what `connectSocket` set for the wait. `timed`: it had a deadline.
/// The transport writes through std.Io, which treats EAGAIN as a bug, so
/// the send timeout must not outlive the connect. Darwin: nothing to undo.
/// Internal; `pub` only for the tests (`rpc.testing.unix_socket`).
pub fn finishConnect(fd: Fd, timed: bool) error{Unexpected}!void {
    if (comptime !connect_uses_send_timeout) return;
    if (timed) try clearSendTimeout(fd);
}

/// Set `SO_SNDTIMEO` back to 0 (no bound). EINVAL is not a failure here:
/// XNU returns it for every option once the socket is fully shut down (the
/// peer closed after the connect), and a write on such a socket fails with
/// EPIPE at once, so no bound is left to hit. The only other EINVAL, a
/// wrong option size, would already have failed the set before the connect.
/// Internal; `pub` only for the tests (`rpc.testing.unix_socket`).
pub fn clearSendTimeout(fd: Fd) error{Unexpected}!void {
    if (comptime !supported) return;
    switch (sendTimeoutErrno(fd, 0)) {
        .SUCCESS, .INVAL => {},
        else => |e| return unexpected("setsockopt(SO_SNDTIMEO, 0)", e),
    }
}
