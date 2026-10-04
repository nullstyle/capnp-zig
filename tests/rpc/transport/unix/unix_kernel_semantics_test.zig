//! FD-0: the kernel semantics of SCM_RIGHTS over AF_UNIX stream sockets.
//!
//! Item 5 of docs/sprint-plan-2026-10-04.md. This suite calls the kernel
//! directly (raw `posix.system` calls, no capnp-zig code). It pins, per OS,
//! each fd-passing behaviour that the Unix transport (item 6) and the
//! fd-passing branch (items 10-13) depend on.
//!
//! The per-OS expectations are one table, `linux_expect` and `macos_expect`
//! below. Each field names the design decision that depends on it. If a CI
//! image moves to a kernel that behaves differently, this suite goes red and
//! names the behaviour. Re-check that decision before you change a field.
//!
//! Linux and macOS run the kernel tests. Other targets skip them (Windows has
//! no SCM_RIGHTS), but every target compiles this file. The top-level
//! `comptime` block checks std's `cmsghdr` layout against the fixed cmsg
//! bytes on every Linux and macOS target that compiles the suite. That
//! includes the compile-only big-endian powerpc64 job, which never runs it.
//!
//! The suite must stay well below the macOS default soft fd limit (256). The
//! tests that need more fds raise RLIMIT_NOFILE themselves and restore it.

const std = @import("std");
const builtin = @import("builtin");

const posix = std.posix;
const sys = posix.system;
const net = std.Io.net;
const cmsg = net.cmsg;
const cmsg_align = net.cmsg_align;
const testing = std.testing;

const os_tag = builtin.os.tag;
const is_linux = os_tag == .linux;
const is_macos = os_tag == .macos;
/// The two kernels this suite pins. Every other target skips the kernel
/// tests.
const supported = is_linux or is_macos;

// ---------------------------------------------------------------------------
// The per-OS expectation table
// ---------------------------------------------------------------------------

const KernelExpect = struct {
    /// `MSG_CMSG_CLOEXEC` exists, so `recvmsg` can mark received fds
    /// close-on-exec atomically. Where it does not exist, item 6 calls
    /// `fcntl(FD_CLOEXEC)` right after `recvmsg` (a documented race window).
    has_msg_cmsg_cloexec: bool,
    /// A received fd has FD_CLOEXEC set. The suite receives with
    /// `MSG_CMSG_CLOEXEC` wherever that flag exists.
    received_fd_cloexec: bool,
    /// A stream `recvmsg` that has already copied plain bytes stops BEFORE a
    /// segment that carries fds (macOS). Linux copies on into that segment
    /// and stops after it. So with bulk reads the fd anchors to the FIRST
    /// byte of a read on macOS and to its LAST bytes on Linux. This is why
    /// D2 picks exact-boundary reads (item 11): only they give one rule.
    stops_before_fd_segment: bool,
    /// A read with no control buffer (plain `read`, `recvmsg` with no
    /// control, std's `net_read` with an empty control buffer: today's
    /// `Transport.read`) still installs the fds into the receiver's fd
    /// table, where nothing can find and close them (macOS). Linux closes
    /// them inside the read. Item 6's drain mode exists because of this.
    no_control_read_installs_fds: bool,
    /// When the control buffer is too small, the kernel still installs every
    /// fd, including the ones whose numbers did not fit (macOS). Linux closes
    /// the ones that do not fit. Item 6 sizes its buffer to 512 slots.
    truncation_installs_all_fds: bool,
    /// A truncated header keeps its full `cmsg_len`, longer than the bytes
    /// present (macOS). Linux shrinks it to the fds it copied. std's
    /// `cmsg.Iterator` drops such a header, so item 6 uses its own clamped
    /// parser.
    truncated_cmsg_len_is_full: bool,
    /// The largest fd count that one `sendmsg` accepts. One more gives
    /// EINVAL. Item 10 rejects more than 253 before the syscall.
    max_fds_per_sendmsg: usize,
    /// At RLIMIT_NOFILE, `recvmsg` fails with EMFILE and installs nothing;
    /// the retry returns the data with no fds and no MSG_CTRUNC (macOS).
    /// Linux delivers the fds that fit and sets MSG_CTRUNC. Item 6 emits the
    /// drop event on the first EMFILE and retries once.
    emfile_fails_recvmsg: bool,
    /// The final close of a received TCP socket that lingers with unsent
    /// data blocks the closing thread for the linger time. With no control
    /// buffer, Linux does that close inside `recvmsg`, on the reader thread.
    /// This is why item 6 closes received fds on a closer thread, never on
    /// the reader or Peer thread. macOS blocks too, once the unsent data is
    /// really stuck: its receive-buffer autotuning drains a single fill
    /// pass, and its SO_LINGER counts clock ticks, not seconds. A probe
    /// that missed either measured 0 ms there.
    final_close_lingers: bool,
};

const linux_expect: KernelExpect = .{
    .has_msg_cmsg_cloexec = true,
    .received_fd_cloexec = true,
    .stops_before_fd_segment = false,
    .no_control_read_installs_fds = false,
    .truncation_installs_all_fds = false,
    .truncated_cmsg_len_is_full = false,
    .max_fds_per_sendmsg = 253,
    .emfile_fails_recvmsg = false,
    .final_close_lingers = true,
};

const macos_expect: KernelExpect = .{
    .has_msg_cmsg_cloexec = false,
    .received_fd_cloexec = false,
    .stops_before_fd_segment = true,
    .no_control_read_installs_fds = true,
    .truncation_installs_all_fds = true,
    .truncated_cmsg_len_is_full = true,
    .max_fds_per_sendmsg = 254,
    .emfile_fails_recvmsg = true,
    .final_close_lingers = true,
};

const expect: KernelExpect = if (is_linux) linux_expect else macos_expect;

// ---------------------------------------------------------------------------
// Fixed cmsg bytes (the byte-order proof)
// ---------------------------------------------------------------------------

/// The cmsg layout the kernel writes for one ABI.
const Layout = struct {
    endian: std.builtin.Endian,
    /// Width of `cmsg_len` as the kernel writes it: 8 bytes on 64-bit Linux
    /// (musl's padded `socklen_t` gives the same bytes), else 4.
    len_width: usize,
    /// The CMSG_ALIGN unit.
    alignment: usize,
    sol_socket: i32,

    /// CMSG_LEN(0).
    fn headerLen(layout: Layout) usize {
        return std.mem.alignForward(usize, layout.len_width + 8, layout.alignment);
    }
};

const scm_rights_value: i32 = 1;

/// Every byte of each fd differs, so a decode in the wrong byte order gives
/// a different number (7 becomes 0x07000000).
const fixture_fds = [_]i32{ 7, 0x0102, 0x01_0203 };

const Fixture = struct {
    name: []const u8,
    layout: Layout,
    /// One SCM_RIGHTS cmsg carrying `fixture_fds`, padded to
    /// CMSG_SPACE(12), written out by hand.
    bytes: []const u8,
};

const fixtures = [_]Fixture{
    .{
        .name = "linux 64-bit little-endian (x86_64, aarch64)",
        .layout = .{ .endian = .little, .len_width = 8, .alignment = 8, .sol_socket = 1 },
        .bytes = &.{
            0x1c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cmsg_len = CMSG_LEN(12) = 28
            0x01, 0x00, 0x00, 0x00, // cmsg_level = SOL_SOCKET
            0x01, 0x00, 0x00, 0x00, // cmsg_type = SCM_RIGHTS
            0x07, 0x00, 0x00, 0x00, // fd 7
            0x02, 0x01, 0x00, 0x00, // fd 0x0102
            0x03, 0x02, 0x01, 0x00, // fd 0x010203
            0x00, 0x00, 0x00, 0x00, // pad to CMSG_SPACE(12) = 32
        },
    },
    .{
        .name = "linux 64-bit big-endian (powerpc64)",
        .layout = .{ .endian = .big, .len_width = 8, .alignment = 8, .sol_socket = 1 },
        .bytes = &.{
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1c, // cmsg_len = 28
            0x00, 0x00, 0x00, 0x01, // cmsg_level = SOL_SOCKET
            0x00, 0x00, 0x00, 0x01, // cmsg_type = SCM_RIGHTS
            0x00, 0x00, 0x00, 0x07, // fd 7
            0x00, 0x00, 0x01, 0x02, // fd 0x0102
            0x00, 0x01, 0x02, 0x03, // fd 0x010203
            0x00, 0x00, 0x00, 0x00, // pad to 32
        },
    },
    .{
        .name = "linux 32-bit little-endian (x86, arm)",
        .layout = .{ .endian = .little, .len_width = 4, .alignment = 4, .sol_socket = 1 },
        .bytes = &.{
            0x18, 0x00, 0x00, 0x00, // cmsg_len = CMSG_LEN(12) = 24
            0x01, 0x00, 0x00, 0x00, // cmsg_level = SOL_SOCKET
            0x01, 0x00, 0x00, 0x00, // cmsg_type = SCM_RIGHTS
            0x07, 0x00, 0x00, 0x00, // fd 7
            0x02, 0x01, 0x00, 0x00, // fd 0x0102
            0x03, 0x02, 0x01, 0x00, // fd 0x010203 (CMSG_SPACE(12) = 24, no pad)
        },
    },
    .{
        .name = "macOS (arm64, x86_64)",
        .layout = .{ .endian = .little, .len_width = 4, .alignment = 4, .sol_socket = 0xffff },
        .bytes = &.{
            0x18, 0x00, 0x00, 0x00, // cmsg_len = CMSG_LEN(12) = 24
            0xff, 0xff, 0x00, 0x00, // cmsg_level = SOL_SOCKET (0xffff)
            0x01, 0x00, 0x00, 0x00, // cmsg_type = SCM_RIGHTS
            0x07, 0x00, 0x00, 0x00, // fd 7
            0x02, 0x01, 0x00, 0x00, // fd 0x0102
            0x03, 0x02, 0x01, 0x00, // fd 0x010203
        },
    },
};

/// The fixture for the target this suite is compiled for, or null when no
/// fixture covers it.
fn nativeFixture() ?Fixture {
    if (is_macos) return fixtures[3];
    if (!is_linux or posix.SOL.SOCKET != 1) return null;
    const endian = builtin.cpu.arch.endian();
    return switch (@sizeOf(usize)) {
        8 => if (endian == .little) fixtures[0] else fixtures[1],
        4 => if (endian == .little) fixtures[2] else null,
        else => null,
    };
}

const native_layout: Layout = .{
    .endian = builtin.cpu.arch.endian(),
    .len_width = if (is_linux and @sizeOf(usize) == 8) 8 else 4,
    .alignment = cmsg_align,
    .sol_socket = if (supported) posix.SOL.SOCKET else 0,
};

/// The clamped SCM_RIGHTS parser that item 6's `fd_io` must implement. It
/// clamps each `cmsg_len` to the bytes present (macOS keeps the full length
/// on truncation) and reads fd ints in `layout.endian`. It returns how many
/// fds the buffer carries; the first `out.len` of them land in `out`.
fn decodeRights(control: []const u8, layout: Layout, out: []i32) usize {
    const header_len = layout.headerLen();
    var count: usize = 0;
    var offset: usize = 0;
    while (offset + header_len <= control.len) {
        const header = control[offset..];
        const cmsg_len: usize = switch (layout.len_width) {
            8 => @intCast(std.mem.readInt(u64, header[0..8], layout.endian)),
            else => std.mem.readInt(u32, header[0..4], layout.endian),
        };
        if (cmsg_len < header_len) break;
        const level = std.mem.readInt(i32, header[layout.len_width..][0..4], layout.endian);
        const kind = std.mem.readInt(i32, header[layout.len_width + 4 ..][0..4], layout.endian);
        const usable = @min(cmsg_len, control.len - offset);
        if (level == layout.sol_socket and kind == scm_rights_value) {
            var i: usize = 0;
            while (i < (usable - header_len) / 4) : (i += 1) {
                const at = offset + header_len + i * 4;
                const fd = std.mem.readInt(i32, control[at..][0..4], layout.endian);
                if (count < out.len) out[count] = fd;
                count += 1;
            }
        }
        // A header that runs to (or past) the end is the last one. Checking
        // first keeps a hostile `cmsg_len` from overflowing `offset`.
        if (cmsg_len >= control.len - offset) break;
        offset += std.mem.alignForward(usize, cmsg_len, layout.alignment);
    }
    return count;
}

// std's `cmsghdr` must lay the fixed bytes out exactly, on every Linux and
// macOS target that compiles this file. This runs at compile time, so the
// compile-only powerpc64 job checks the big-endian fixture too.
comptime {
    if (supported) {
        if (nativeFixture()) |fixture| {
            @setEvalBranchQuota(10_000);
            const layout = fixture.layout;
            if (layout.endian != native_layout.endian or
                layout.len_width != native_layout.len_width or
                layout.alignment != cmsg_align or
                layout.sol_socket != posix.SOL.SOCKET or
                posix.SCM.RIGHTS != scm_rights_value or
                cmsg.len(0) != layout.headerLen() or
                cmsg.space(fixture_fds.len * 4) != fixture.bytes.len)
            {
                @compileError("FD-0: the fixed cmsg fixture '" ++ fixture.name ++ "' no longer matches std's cmsg layout for this target");
            }
            var header = std.mem.zeroes(posix.cmsghdr);
            header.len = cmsg.len(fixture_fds.len * 4);
            header.level = posix.SOL.SOCKET;
            header.type = posix.SCM.RIGHTS;
            const header_bytes = std.mem.toBytes(header);
            if (!std.mem.eql(u8, &header_bytes, fixture.bytes[0..@sizeOf(posix.cmsghdr)])) {
                @compileError("FD-0: std's cmsghdr bytes differ from the fixture '" ++ fixture.name ++ "'");
            }
            for (fixture_fds, 0..) |fd, i| {
                const at = layout.headerLen() + i * 4;
                if (!std.mem.eql(u8, &std.mem.toBytes(@as(posix.fd_t, fd)), fixture.bytes[at..][0..4])) {
                    @compileError("FD-0: native fd bytes differ from the fixture '" ++ fixture.name ++ "'");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Raw syscall helpers (Linux without libc, Linux with glibc for the TSan
// lane, and macOS libc)
// ---------------------------------------------------------------------------

fn ival(rc: anytype) isize {
    return switch (@typeInfo(@TypeOf(rc)).int.signedness) {
        .signed => @intCast(rc),
        .unsigned => @bitCast(rc),
    };
}

/// Returns the non-negative result of a syscall, or prints the errno number
/// and fails.
fn check(rc: anytype, what: []const u8) error{SyscallFailed}!usize {
    const err = posix.errno(rc);
    if (err != .SUCCESS) {
        std.debug.print("FD-0: {s} failed with errno {d}\n", .{ what, @backingInt(err) });
        return error.SyscallFailed;
    }
    return @intCast(ival(rc));
}

fn closeFd(fd: posix.fd_t) void {
    _ = sys.close(fd);
}

fn socketPair() ![2]posix.fd_t {
    var fds: [2]posix.fd_t = undefined;
    _ = try check(sys.socketpair(posix.AF.UNIX, posix.SOCK.STREAM, 0, &fds), "socketpair");
    return fds;
}

fn pipePair() ![2]posix.fd_t {
    var fds: [2]posix.fd_t = undefined;
    _ = try check(sys.pipe(&fds), "pipe");
    return fds;
}

fn fdFlags(fd: posix.fd_t) ?usize {
    const rc = if (is_linux and !builtin.link_libc)
        sys.fcntl(fd, posix.F.GETFD, 0)
    else
        sys.fcntl(fd, posix.F.GETFD);
    if (posix.errno(rc) != .SUCCESS) return null;
    return @intCast(ival(rc));
}

fn setNonBlocking(fd: posix.fd_t, on: bool) !void {
    const nonblock: usize = @as(u32, @bitCast(posix.O{ .NONBLOCK = true }));
    const get = if (is_linux and !builtin.link_libc)
        sys.fcntl(fd, posix.F.GETFL, 0)
    else
        sys.fcntl(fd, posix.F.GETFL);
    const old = try check(get, "fcntl(F_GETFL)");
    const new = if (on) old | nonblock else old & ~nonblock;
    const set = if (is_linux and !builtin.link_libc)
        sys.fcntl(fd, posix.F.SETFL, new)
    else
        sys.fcntl(fd, posix.F.SETFL, @as(c_int, @intCast(new)));
    _ = try check(set, "fcntl(F_SETFL)");
}

fn isOpen(fd: posix.fd_t) bool {
    return fdFlags(fd) != null;
}

fn writeByte(fd: posix.fd_t, byte: u8) !void {
    const one = [1]u8{byte};
    const n = try check(sys.write(fd, &one, 1), "write");
    try testing.expectEqual(@as(usize, 1), n);
}

/// True when every write end of the pipe whose read end is `read_end` is
/// closed: the read end polls ready within `timeout_ms` and reads EOF. The
/// pipe must hold no unread data.
fn pipeWritersClosed(read_end: posix.fd_t, timeout_ms: i32) bool {
    var pfd = [1]posix.pollfd{.{ .fd = read_end, .events = posix.POLL.IN, .revents = 0 }};
    const rc = sys.poll(&pfd, 1, timeout_ms);
    if (posix.errno(rc) != .SUCCESS or ival(rc) <= 0) return false;
    var byte: [1]u8 = undefined;
    const n = sys.read(read_end, &byte, 1);
    return posix.errno(n) == .SUCCESS and ival(n) == 0;
}

/// How long a negative check waits for a pipe writer that must stay open.
const still_open_wait_ms: i32 = 50;
/// How long a positive check waits for a pipe writer that must be closed.
const closed_wait_ms: i32 = 1000;

fn nowNs() i96 {
    return std.Io.Clock.awake.now(testing.io).nanoseconds;
}

fn msSince(start_ns: i96) i64 {
    return @intCast(@divFloor(nowNs() - start_ns, std.time.ns_per_ms));
}

/// Enough room for one cmsg with 300 fds, above both kernels' limits.
const max_control_fds = 300;

fn buildRights(buf: []align(cmsg_align) u8, fds: []const posix.fd_t) []align(cmsg_align) u8 {
    const data_len = fds.len * @sizeOf(posix.fd_t);
    const total = cmsg.space(data_len);
    @memset(buf[0..total], 0);
    const header: *align(cmsg_align) posix.cmsghdr = @ptrCast(buf.ptr);
    header.len = @intCast(cmsg.len(@intCast(data_len)));
    header.level = posix.SOL.SOCKET;
    header.type = posix.SCM.RIGHTS;
    @memcpy(cmsg.data(header)[0..data_len], std.mem.sliceAsBytes(fds));
    return buf[0..total];
}

const SendOutcome = union(enum) {
    sent: usize,
    failed: posix.E,
};

/// One raw `sendmsg`. A non-empty `fds` goes out as one SCM_RIGHTS cmsg.
fn sendRights(sock: posix.fd_t, bytes: []const u8, fds: []const posix.fd_t, flags: u32) SendOutcome {
    var control_buf: [cmsg.space(max_control_fds * @sizeOf(posix.fd_t))]u8 align(cmsg_align) = undefined;
    const control: []const u8 = if (fds.len == 0) &.{} else buildRights(&control_buf, fds);
    var iov = [1]posix.iovec_const{.{ .base = bytes.ptr, .len = bytes.len }};
    const msg: posix.msghdr_const = .{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = 1,
        .control = if (control.len == 0) null else control.ptr,
        .controllen = @intCast(control.len),
        .flags = 0,
    };
    while (true) {
        const rc = sys.sendmsg(sock, &msg, flags);
        switch (posix.errno(rc)) {
            .SUCCESS => return .{ .sent = @intCast(ival(rc)) },
            .INTR => continue,
            else => |err| return .{ .failed = err },
        }
    }
}

/// Sends all of `bytes`, with `fds` on the first chunk only.
fn sendAll(sock: posix.fd_t, bytes: []const u8, fds: []const posix.fd_t) !void {
    var offset: usize = 0;
    var first = true;
    while (offset < bytes.len) {
        switch (sendRights(sock, bytes[offset..], if (first) fds else &.{}, 0)) {
            .sent => |n| offset += n,
            .failed => |err| {
                std.debug.print("FD-0: sendmsg failed with errno {d}\n", .{@backingInt(err)});
                return error.SyscallFailed;
            },
        }
        first = false;
    }
}

const Received = struct {
    len: usize,
    control_len: usize,
    flags: u32,

    fn truncated(r: Received) bool {
        return r.flags & posix.MSG.CTRUNC != 0;
    }
};

const RecvOutcome = union(enum) {
    received: Received,
    failed: posix.E,
};

/// One raw `recvmsg`. An empty `control` passes no control buffer at all.
fn recvRaw(sock: posix.fd_t, buf: []u8, control: []align(cmsg_align) u8, flags: u32) RecvOutcome {
    var iov = [1]posix.iovec{.{ .base = buf.ptr, .len = buf.len }};
    var msg: posix.msghdr = .{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = 1,
        .control = if (control.len == 0) null else control.ptr,
        .controllen = @intCast(control.len),
        .flags = 0,
    };
    while (true) {
        const rc = sys.recvmsg(sock, &msg, flags);
        switch (posix.errno(rc)) {
            .SUCCESS => return .{ .received = .{
                .len = @intCast(ival(rc)),
                .control_len = @intCast(msg.controllen),
                .flags = msg.flags,
            } },
            .INTR => continue,
            else => |err| return .{ .failed = err },
        }
    }
}

fn recvOk(sock: posix.fd_t, buf: []u8, control: []align(cmsg_align) u8, flags: u32) !Received {
    return switch (recvRaw(sock, buf, control, flags)) {
        .received => |r| r,
        .failed => |err| {
            std.debug.print("FD-0: recvmsg failed with errno {d}\n", .{@backingInt(err)});
            return error.SyscallFailed;
        },
    };
}

/// The receive flags this suite uses: `MSG_CMSG_CLOEXEC` where it exists.
const recv_flags: u32 = if (@hasDecl(posix.MSG, "CMSG_CLOEXEC")) posix.MSG.CMSG_CLOEXEC else 0;

/// Decodes the fds in `control[0..r.control_len]` with the native layout.
fn receivedFds(control: []const u8, r: Received, out: []i32) usize {
    return decodeRights(control[0..r.control_len], native_layout, out);
}

fn closeAll(fds: []const i32) void {
    for (fds) |fd| closeFd(fd);
}

/// Which fds below `max_scanned_fd` are open. Fds are allocated lowest
/// first, and this suite never holds more than a few hundred.
const max_scanned_fd = 4096;

const FdSnapshot = struct {
    open: std.StaticBitSet(max_scanned_fd),

    fn take() FdSnapshot {
        var snapshot: FdSnapshot = .{ .open = .empty };
        var fd: usize = 0;
        while (fd < max_scanned_fd) : (fd += 1) {
            if (isOpen(@intCast(fd))) snapshot.open.set(fd);
        }
        return snapshot;
    }

    /// The fds open in `after` that were closed in `before`. Returns the
    /// count; the first `out.len` land in `out`.
    fn added(after: FdSnapshot, before: FdSnapshot, out: []posix.fd_t) usize {
        var count: usize = 0;
        var it = after.open.iterator(.{});
        while (it.next()) |fd| {
            if (before.open.isSet(fd)) continue;
            if (count < out.len) out[count] = @intCast(fd);
            count += 1;
        }
        return count;
    }

    fn highest(snapshot: FdSnapshot) usize {
        return snapshot.open.findLastSet() orelse 0;
    }
};

/// Fails the test when `body` left an fd open (or closed one it did not
/// own). Used around every case, so the suite cannot leak into the next one.
fn expectNoFdDelta(before: FdSnapshot) !void {
    const after = FdSnapshot.take();
    var leaked: [16]posix.fd_t = undefined;
    const n_leaked = after.added(before, &leaked);
    var lost: [16]posix.fd_t = undefined;
    const n_lost = before.added(after, &lost);
    if (n_leaked != 0 or n_lost != 0) {
        std.debug.print("FD-0: fd table changed: {d} new fd(s) {any}, {d} closed fd(s) {any}\n", .{
            n_leaked, leaked[0..@min(n_leaked, leaked.len)], n_lost, lost[0..@min(n_lost, lost.len)],
        });
        return error.FdTableChanged;
    }
}

/// Raises the soft RLIMIT_NOFILE to at least `want` (never lowers it) and
/// remembers the old value.
const FdHeadroom = struct {
    saved: posix.rlimit,

    fn ensure(want: u64) !FdHeadroom {
        const saved = try posix.getrlimit(.NOFILE);
        if (saved.cur < want) {
            var raised = saved;
            raised.cur = @min(want, saved.max);
            try posix.setrlimit(.NOFILE, raised);
        }
        return .{ .saved = saved };
    }

    fn restore(self: FdHeadroom) void {
        posix.setrlimit(.NOFILE, self.saved) catch |err| {
            std.debug.print("FD-0: could not restore RLIMIT_NOFILE: {t}\n", .{err});
        };
    }
};

// ---------------------------------------------------------------------------
// Fixed bytes: decode
// ---------------------------------------------------------------------------

test "FD-0 fixed cmsg bytes decode to the same fd numbers in each ABI's byte order" {
    // Pure computation: no kernel, so every target runs it.
    for (fixtures) |fixture| {
        var out: [8]i32 = undefined;
        const n = decodeRights(fixture.bytes, fixture.layout, &out);
        testing.expectEqualSlices(i32, &fixture_fds, out[0..n]) catch |err| {
            std.debug.print("FD-0: fixture '{s}' decoded to {any}\n", .{ fixture.name, out[0..n] });
            return err;
        };

        // The fixture has teeth: the other byte order reads other numbers
        // (or rejects the header outright).
        var swapped = fixture.layout;
        swapped.endian = if (fixture.layout.endian == .little) .big else .little;
        var wrong: [8]i32 = undefined;
        const n_wrong = decodeRights(fixture.bytes, swapped, &wrong);
        try testing.expect(n_wrong != fixture_fds.len or !std.mem.eql(i32, &fixture_fds, wrong[0..n_wrong]));
    }

    // A truncated buffer keeps the fds that fit (the clamp), here the first.
    const darwin = fixtures[3];
    var out: [8]i32 = undefined;
    const n = decodeRights(darwin.bytes[0 .. darwin.layout.headerLen() + 4], darwin.layout, &out);
    try testing.expectEqualSlices(i32, fixture_fds[0..1], out[0..n]);
}

test "FD-0 std builds exactly the native fixed cmsg bytes" {
    if (!supported) return error.SkipZigTest;
    const fixture = nativeFixture() orelse return error.SkipZigTest;
    var fds: [fixture_fds.len]posix.fd_t = undefined;
    for (&fds, fixture_fds) |*dst, src| dst.* = src;
    var buf: [64]u8 align(cmsg_align) = undefined;
    try testing.expectEqualSlices(u8, fixture.bytes, buildRights(&buf, &fds));
}

// ---------------------------------------------------------------------------
// Round trip and CLOEXEC
// ---------------------------------------------------------------------------

test "FD-0 fd round trip: the kernel header matches the fixture, fds map in order, CLOEXEC per OS" {
    if (!supported) return error.SkipZigTest;
    const before = FdSnapshot.take();
    {
        try testing.expectEqual(expect.has_msg_cmsg_cloexec, @hasDecl(posix.MSG, "CMSG_CLOEXEC"));

        const sp = try socketPair();
        defer closeFd(sp[0]);
        defer closeFd(sp[1]);
        var pipes: [3][2]posix.fd_t = undefined;
        for (&pipes) |*p| p.* = try pipePair();
        defer for (pipes) |p| closeFd(p[0]);

        const writers = [3]posix.fd_t{ pipes[0][1], pipes[1][1], pipes[2][1] };
        try sendAll(sp[0], "X", &writers);
        for (writers) |w| closeFd(w);

        var data: [8]u8 = undefined;
        var control: [cmsg.space(16 * @sizeOf(posix.fd_t))]u8 align(cmsg_align) = undefined;
        const r = try recvOk(sp[1], &data, &control, recv_flags);
        try testing.expectEqual(@as(usize, 1), r.len);
        try testing.expect(!r.truncated());

        // The kernel's header bytes are the fixed fixture's header bytes.
        if (nativeFixture()) |fixture| {
            try testing.expectEqual(fixture.bytes.len, r.control_len);
            const header_len = native_layout.headerLen();
            try testing.expectEqualSlices(u8, fixture.bytes[0..header_len], control[0..header_len]);
        }

        var got: [8]i32 = undefined;
        const n = receivedFds(&control, r, &got);
        try testing.expectEqual(@as(usize, 3), n);
        defer closeAll(got[0..n]);

        for (got[0..n], 0..) |fd, i| {
            const fd_flags = fdFlags(fd) orelse return error.ReceivedFdNotOpen;
            try testing.expectEqual(expect.received_fd_cloexec, fd_flags & posix.FD_CLOEXEC != 0);
            // Received fd i is pipe i's writer: a byte written through it
            // comes out of pipe i's read end.
            try writeByte(fd, 'a' + @as(u8, @intCast(i)));
            var byte: [1]u8 = undefined;
            const got_n = try check(sys.read(pipes[i][0], &byte, 1), "read pipe");
            try testing.expectEqual(@as(usize, 1), got_n);
            try testing.expectEqual('a' + @as(u8, @intCast(i)), byte[0]);
        }
    }
    try expectNoFdDelta(before);
}

// ---------------------------------------------------------------------------
// Attribution
// ---------------------------------------------------------------------------

const Chunk = struct { len: usize, fds: usize };

fn fill(buf: []u8, byte: u8) []u8 {
    @memset(buf, byte);
    return buf;
}

test "FD-0 bulk reads anchor an fd to the read's last bytes on Linux and its first byte on macOS" {
    if (!supported) return error.SkipZigTest;
    const before = FdSnapshot.take();
    // A(100) plain, B(100) + fd, C(100) plain, all queued before the first
    // read. The only difference between the cases is the read size.
    const Case = struct { read_size: usize, linux: []const Chunk, macos: []const Chunk };
    const cases = [_]Case{
        .{
            .read_size = 1000,
            .linux = &.{ .{ .len = 200, .fds = 1 }, .{ .len = 100, .fds = 0 } },
            .macos = &.{ .{ .len = 100, .fds = 0 }, .{ .len = 200, .fds = 1 } },
        },
        .{
            .read_size = 150,
            .linux = &.{ .{ .len = 150, .fds = 1 }, .{ .len = 150, .fds = 0 } },
            .macos = &.{ .{ .len = 100, .fds = 0 }, .{ .len = 150, .fds = 1 }, .{ .len = 50, .fds = 0 } },
        },
    };
    for (cases) |case| {
        const sp = try socketPair();
        defer closeFd(sp[0]);
        defer closeFd(sp[1]);
        const p = try pipePair();
        defer closeFd(p[0]);
        defer closeFd(p[1]);

        var segment: [100]u8 = undefined;
        try sendAll(sp[0], fill(&segment, 'A'), &.{});
        try sendAll(sp[0], fill(&segment, 'B'), &.{p[1]});
        try sendAll(sp[0], fill(&segment, 'C'), &.{});

        var observed: [8]Chunk = undefined;
        var n_observed: usize = 0;
        var total: usize = 0;
        var data: [1000]u8 = undefined;
        while (total < 300 and n_observed < observed.len) {
            var control: [cmsg.space(16 * @sizeOf(posix.fd_t))]u8 align(cmsg_align) = undefined;
            const r = try recvOk(sp[1], data[0..case.read_size], &control, recv_flags);
            if (r.len == 0) return error.UnexpectedEof;
            var got: [16]i32 = undefined;
            const n_fds = receivedFds(&control, r, &got);
            closeAll(got[0..@min(n_fds, got.len)]);
            observed[n_observed] = .{ .len = r.len, .fds = n_fds };
            n_observed += 1;
            total += r.len;
        }
        // `stops_before_fd_segment` picks the row; flipping it turns this red.
        const want = if (expect.stops_before_fd_segment) case.macos else case.linux;
        testing.expectEqualSlices(Chunk, want, observed[0..n_observed]) catch |err| {
            std.debug.print("FD-0: read size {d}: reads {any}\n", .{ case.read_size, observed[0..n_observed] });
            return err;
        };
    }
    try expectNoFdDelta(before);
}

/// Which `recvmsg` of an exact read saw the fds.
const PartFds = struct {
    calls: usize = 0,
    first_call: usize = 0,
    later_calls: usize = 0,

    fn total(p: PartFds) usize {
        return p.first_call + p.later_calls;
    }
};

/// Reads exactly `buf.len` bytes. No `recvmsg` crosses the end of `buf`, so
/// no read crosses a frame boundary. Closes every fd it receives.
fn readPartExact(sock: posix.fd_t, buf: []u8) !PartFds {
    var part: PartFds = .{};
    var got_len: usize = 0;
    while (got_len < buf.len) {
        var control: [cmsg.space(16 * @sizeOf(posix.fd_t))]u8 align(cmsg_align) = undefined;
        const r = try recvOk(sock, buf[got_len..], &control, recv_flags);
        if (r.len == 0) return error.UnexpectedEof;
        var fds: [16]i32 = undefined;
        const n_fds = receivedFds(&control, r, &fds);
        closeAll(fds[0..@min(n_fds, fds.len)]);
        if (part.calls == 0) part.first_call += n_fds else part.later_calls += n_fds;
        part.calls += 1;
        got_len += r.len;
    }
    return part;
}

const frame_header_len = 8;

const FrameFds = struct {
    header: PartFds,
    body: PartFds,

    fn total(f: FrameFds) usize {
        return f.header.total() + f.body.total();
    }
};

/// Reads one frame of `frame_len` bytes the way item 11 will: the header
/// first, then the body, each with exact reads.
fn readFrame(sock: posix.fd_t, frame_len: usize, scratch: []u8) !FrameFds {
    const header = try readPartExact(sock, scratch[0..frame_header_len]);
    const body = try readPartExact(sock, scratch[0 .. frame_len - frame_header_len]);
    return .{ .header = header, .body = body };
}

test "FD-0 exact-boundary reads put each fd in the frame whose bytes carried it (E1-E4, E3b)" {
    if (!supported) return error.SkipZigTest;
    const before = FdSnapshot.take();
    const scratch = try testing.allocator.alloc(u8, 256 * 1024);
    defer testing.allocator.free(scratch);
    var segment: [100]u8 = undefined;
    // On Linux an exact read that reaches an fd-carrying segment takes the
    // fd on that same call; on macOS the call stops before the segment and
    // the next call takes it. The frame is the same either way.
    const fd_on_first_call = !expect.stops_before_fd_segment;

    { // E1: A | B+fd | C
        const sp = try socketPair();
        defer closeFd(sp[0]);
        defer closeFd(sp[1]);
        const p = try pipePair();
        defer closeFd(p[0]);
        defer closeFd(p[1]);
        try sendAll(sp[0], fill(&segment, 'A'), &.{});
        try sendAll(sp[0], fill(&segment, 'B'), &.{p[1]});
        try sendAll(sp[0], fill(&segment, 'C'), &.{});
        try testing.expectEqual(@as(usize, 0), (try readFrame(sp[1], 100, scratch)).total());
        const b = try readFrame(sp[1], 100, scratch);
        try testing.expectEqual(@as(usize, 1), b.header.first_call);
        try testing.expectEqual(@as(usize, 1), b.total());
        try testing.expectEqual(@as(usize, 0), (try readFrame(sp[1], 100, scratch)).total());
    }
    { // E2: B+1fd | D+2fd, back to back
        const sp = try socketPair();
        defer closeFd(sp[0]);
        defer closeFd(sp[1]);
        const p = try pipePair();
        defer closeFd(p[0]);
        defer closeFd(p[1]);
        try sendAll(sp[0], fill(&segment, 'B'), &.{p[1]});
        try sendAll(sp[0], fill(&segment, 'D'), &.{ p[0], p[1] });
        const b = try readFrame(sp[1], 100, scratch);
        try testing.expectEqual(@as(usize, 1), b.total());
        try testing.expectEqual(@as(usize, 1), b.header.first_call);
        const d = try readFrame(sp[1], 100, scratch);
        try testing.expectEqual(@as(usize, 2), d.total());
        try testing.expectEqual(@as(usize, 2), d.header.first_call);
    }
    { // E3: hostile, fd attached mid-frame (A[0..50] plain, A[50..100]+fd), then C
        const sp = try socketPair();
        defer closeFd(sp[0]);
        defer closeFd(sp[1]);
        const p = try pipePair();
        defer closeFd(p[0]);
        defer closeFd(p[1]);
        const a = fill(&segment, 'A');
        try sendAll(sp[0], a[0..50], &.{});
        try sendAll(sp[0], a[50..100], &.{p[1]});
        try sendAll(sp[0], fill(&segment, 'C'), &.{});
        const frame_a = try readFrame(sp[1], 100, scratch);
        try testing.expectEqual(@as(usize, 1), frame_a.total());
        try testing.expectEqual(@as(usize, 1), frame_a.body.total());
        try testing.expectEqual(@as(usize, @intFromBool(fd_on_first_call)), frame_a.body.first_call);
        try testing.expectEqual(@as(usize, 0), (try readFrame(sp[1], 100, scratch)).total());
    }
    { // E3b: hostile, fd attached at header byte 4 (A[0..4], A[4..100]+fd)
        const sp = try socketPair();
        defer closeFd(sp[0]);
        defer closeFd(sp[1]);
        const p = try pipePair();
        defer closeFd(p[0]);
        defer closeFd(p[1]);
        const a = fill(&segment, 'A');
        try sendAll(sp[0], a[0..4], &.{});
        try sendAll(sp[0], a[4..100], &.{p[1]});
        const frame_a = try readFrame(sp[1], 100, scratch);
        try testing.expectEqual(@as(usize, 1), frame_a.total());
        try testing.expectEqual(@as(usize, 1), frame_a.header.total());
        try testing.expectEqual(@as(usize, @intFromBool(fd_on_first_call)), frame_a.header.first_call);
    }
    { // E4: large B (256 KiB) + fd, sent in partial chunks by a writer thread, then C
        const sp = try socketPair();
        defer closeFd(sp[0]);
        defer closeFd(sp[1]);
        const p = try pipePair();
        defer closeFd(p[0]);
        defer closeFd(p[1]);
        const big = try testing.allocator.alloc(u8, 256 * 1024);
        defer testing.allocator.free(big);
        @memset(big, 'B');
        const Writer = struct {
            fn run(sock: posix.fd_t, bytes: []const u8, fd: posix.fd_t, result: *?anyerror) void {
                sendAll(sock, bytes, &.{fd}) catch |err| {
                    result.* = err;
                    return;
                };
                var tail: [100]u8 = @splat('C');
                sendAll(sock, &tail, &.{}) catch |err| {
                    result.* = err;
                };
            }
        };
        var writer_result: ?anyerror = null;
        const writer = try std.Thread.spawn(.{}, Writer.run, .{ sp[0], big, p[1], &writer_result });
        const frame_b = readFrame(sp[1], big.len, scratch);
        const frame_c = readFrame(sp[1], 100, scratch);
        // If a read failed, the writer may be parked on a full socket:
        // shut ours down so its send fails and the join returns.
        if (std.meta.isError(frame_b) or std.meta.isError(frame_c)) _ = sys.shutdown(sp[1], posix.SHUT.RDWR);
        writer.join();
        if (writer_result) |err| return err;
        try testing.expectEqual(@as(usize, 1), (try frame_b).total());
        try testing.expectEqual(@as(usize, 1), (try frame_b).header.first_call);
        try testing.expectEqual(@as(usize, 0), (try frame_c).total());
    }
    try expectNoFdDelta(before);
}

// ---------------------------------------------------------------------------
// Fds the receiver did not ask for
// ---------------------------------------------------------------------------

test "FD-0 a read with no control buffer leaks the fd on macOS (T4) and closes it on Linux" {
    if (!supported) return error.SkipZigTest;
    const before = FdSnapshot.take();
    const Mode = enum { plain_read, recvmsg_no_control, std_net_read_empty_control };
    for ([_]Mode{ .plain_read, .recvmsg_no_control, .std_net_read_empty_control }) |mode| {
        const sp = try socketPair();
        defer closeFd(sp[0]);
        defer closeFd(sp[1]);
        const p = try pipePair();
        defer closeFd(p[0]);
        try sendAll(sp[0], "Q", &.{p[1]});
        closeFd(p[1]);

        const pre_read = FdSnapshot.take();
        var data: [8]u8 = undefined;
        const n: usize = switch (mode) {
            .plain_read => try check(sys.read(sp[1], &data, data.len), "read"),
            .recvmsg_no_control => (try recvOk(sp[1], &data, &.{}, 0)).len,
            // Today's `Transport.read`: std's `net_read` with no control.
            .std_net_read_empty_control => blk: {
                var bufs: [1][]u8 = .{&data};
                const r = try (try testing.io.operate(.{ .net_read = .{
                    .socket_handle = sp[1],
                    .data = &bufs,
                } })).net_read;
                break :blk r.data_len;
            },
        };
        try testing.expectEqual(@as(usize, 1), n);

        var leaked: [4]posix.fd_t = undefined;
        const n_leaked = FdSnapshot.take().added(pre_read, &leaked);
        defer closeAll(leaked[0..@min(n_leaked, leaked.len)]);
        testing.expectEqual(@as(usize, @intFromBool(expect.no_control_read_installs_fds)), n_leaked) catch |err| {
            std.debug.print("FD-0: mode {t}: {d} fd(s) installed\n", .{ mode, n_leaked });
            return err;
        };
        if (expect.no_control_read_installs_fds) {
            // The leaked fd is the pipe's writer: it keeps the pipe open
            // until we find it and close it.
            try testing.expect(!pipeWritersClosed(p[0], still_open_wait_ms));
            closeFd(leaked[0]);
            leaked[0] = -1;
            try testing.expect(pipeWritersClosed(p[0], closed_wait_ms));
        } else {
            try testing.expect(pipeWritersClosed(p[0], closed_wait_ms));
        }
    }
    try expectNoFdDelta(before);
}

test "FD-0 a truncated control buffer: macOS installs every fd (T3), Linux closes the ones that do not fit" {
    if (!supported) return error.SkipZigTest;
    const before = FdSnapshot.take();
    {
        const sp = try socketPair();
        defer closeFd(sp[0]);
        defer closeFd(sp[1]);
        var pipes: [3][2]posix.fd_t = undefined;
        for (&pipes) |*p| p.* = try pipePair();
        defer for (pipes) |p| closeFd(p[0]);
        const writers = [3]posix.fd_t{ pipes[0][1], pipes[1][1], pipes[2][1] };
        try sendAll(sp[0], "Z", &writers);
        for (writers) |w| closeFd(w);

        // Room for one fd. On 64-bit Linux the 8-byte alignment of
        // CMSG_SPACE leaves room for two.
        const room = cmsg.space(@sizeOf(posix.fd_t));
        const want_visible = (room - cmsg.len(0)) / @sizeOf(posix.fd_t);
        var control: [cmsg.space(16 * @sizeOf(posix.fd_t))]u8 align(cmsg_align) = undefined;
        const pre_read = FdSnapshot.take();
        var data: [8]u8 = undefined;
        const r = try recvOk(sp[1], &data, control[0..room], recv_flags);
        try testing.expectEqual(@as(usize, 1), r.len);
        try testing.expect(r.truncated());
        try testing.expectEqual(room, r.control_len);

        // macOS leaves cmsg_len at the full length; Linux shrinks it.
        const header: *align(cmsg_align) posix.cmsghdr = @ptrCast(&control);
        const want_cmsg_len: usize = if (expect.truncated_cmsg_len_is_full)
            cmsg.len(writers.len * @sizeOf(posix.fd_t))
        else
            cmsg.len(@intCast(want_visible * @sizeOf(posix.fd_t)));
        try testing.expectEqual(want_cmsg_len, @as(usize, @intCast(header.len)));

        // std's `cmsg.Iterator` drops a header whose `cmsg_len` runs past the
        // buffer, so on macOS it cannot even see the visible fd
        // (docs/upstream/handoff-zig-fork-scm-rights.md). The clamped parser
        // sees it on both.
        var it: cmsg.Iterator = .{ .control = control[0..r.control_len] };
        try testing.expectEqual(!expect.truncated_cmsg_len_is_full, it.next() != null);

        var visible: [8]i32 = undefined;
        const n_visible = receivedFds(&control, r, &visible);
        try testing.expectEqual(want_visible, n_visible);
        closeAll(visible[0..n_visible]);

        var installed: [8]posix.fd_t = undefined;
        const n_installed = FdSnapshot.take().added(pre_read, &installed);
        defer closeAll(installed[0..@min(n_installed, installed.len)]);
        const want_installed: usize = if (expect.truncation_installs_all_fds) writers.len - n_visible else 0;
        try testing.expectEqual(want_installed, n_installed);

        // Pipe i is closed iff its writer was visible (we closed it) or the
        // kernel closed it. The hidden ones stay open on macOS.
        for (pipes, 0..) |p, i| {
            const want_closed = i < n_visible or !expect.truncation_installs_all_fds;
            const wait = if (want_closed) closed_wait_ms else still_open_wait_ms;
            testing.expectEqual(want_closed, pipeWritersClosed(p[0], wait)) catch |err| {
                std.debug.print("FD-0: pipe {d} of {d}, {d} visible\n", .{ i, pipes.len, n_visible });
                return err;
            };
        }
        // Closing the fds that nobody could see closes the hidden writers.
        closeAll(installed[0..@min(n_installed, installed.len)]);
        for (installed[0..@min(n_installed, installed.len)]) |*fd| fd.* = -1;
        for (pipes) |p| try testing.expect(pipeWritersClosed(p[0], closed_wait_ms));
    }
    try expectNoFdDelta(before);
}

test "FD-0 one recvmsg never merges two sends that carry fds, even with a 512-slot buffer" {
    if (!supported) return error.SkipZigTest;
    const headroom = try FdHeadroom.ensure(2048);
    defer headroom.restore();
    const before = FdSnapshot.take();
    {
        const sp = try socketPair();
        defer closeFd(sp[0]);
        defer closeFd(sp[1]);
        const p = try pipePair();
        defer closeFd(p[0]);
        defer closeFd(p[1]);

        const per_send = expect.max_fds_per_sendmsg;
        var many: [max_control_fds]posix.fd_t = undefined;
        @memset(&many, p[1]);
        const sends = 3;
        for (0..sends) |i| try sendAll(sp[0], &.{'a' + @as(u8, @intCast(i))}, many[0..per_send]);

        const control = try testing.allocator.alignedAlloc(u8, .fromByteUnits(cmsg_align), cmsg.space(512 * @sizeOf(posix.fd_t)));
        defer testing.allocator.free(control);
        const got = try testing.allocator.alloc(i32, 512);
        defer testing.allocator.free(got);
        for (0..sends) |i| {
            var data: [64]u8 = undefined;
            const r = try recvOk(sp[1], &data, control, recv_flags);
            const n = receivedFds(control, r, got);
            closeAll(got[0..@min(n, got.len)]);
            testing.expectEqual(Chunk{ .len = 1, .fds = per_send }, Chunk{ .len = r.len, .fds = n }) catch |err| {
                std.debug.print("FD-0: recvmsg {d}: {d} byte(s), {d} fds, CTRUNC={}\n", .{ i, r.len, n, r.truncated() });
                return err;
            };
            try testing.expect(!r.truncated());
            try testing.expectEqual('a' + @as(u8, @intCast(i)), data[0]);
        }
    }
    try expectNoFdDelta(before);
}

test "FD-0 the per-sendmsg fd limit is 253 on Linux and 254 on macOS" {
    if (!supported) return error.SkipZigTest;
    const headroom = try FdHeadroom.ensure(2048);
    defer headroom.restore();
    const before = FdSnapshot.take();
    {
        const sp = try socketPair();
        defer closeFd(sp[0]);
        defer closeFd(sp[1]);
        const p = try pipePair();
        defer closeFd(p[0]);
        defer closeFd(p[1]);
        var many: [max_control_fds]posix.fd_t = undefined;
        @memset(&many, p[1]);
        const limit = expect.max_fds_per_sendmsg;

        // At the limit: accepted, and every fd arrives.
        switch (sendRights(sp[0], "M", many[0..limit], 0)) {
            .sent => |n| try testing.expectEqual(@as(usize, 1), n),
            .failed => |err| {
                std.debug.print("FD-0: {d} fds in one sendmsg failed with errno {d}\n", .{ limit, @backingInt(err) });
                return error.LimitRejected;
            },
        }
        const control = try testing.allocator.alignedAlloc(u8, .fromByteUnits(cmsg_align), cmsg.space(512 * @sizeOf(posix.fd_t)));
        defer testing.allocator.free(control);
        const got = try testing.allocator.alloc(i32, 512);
        defer testing.allocator.free(got);
        var data: [8]u8 = undefined;
        const r = try recvOk(sp[1], &data, control, recv_flags);
        const n = receivedFds(control, r, got);
        closeAll(got[0..@min(n, got.len)]);
        try testing.expectEqual(limit, n);

        // One more: EINVAL, and nothing is sent.
        switch (sendRights(sp[0], "M", many[0 .. limit + 1], 0)) {
            .sent => return error.LimitNotEnforced,
            .failed => |err| try testing.expectEqual(posix.E.INVAL, err),
        }
    }
    try expectNoFdDelta(before);
}

test "FD-0 EMFILE: Linux delivers a partial list with CTRUNC; macOS fails the first recvmsg and drops the fds" {
    if (!supported) return error.SkipZigTest;
    const before = FdSnapshot.take();
    {
        const sp = try socketPair();
        defer closeFd(sp[0]);
        defer closeFd(sp[1]);
        var pipes: [5][2]posix.fd_t = undefined;
        for (&pipes) |*p| p.* = try pipePair();
        defer for (pipes) |p| closeFd(p[0]);
        var writers: [5]posix.fd_t = undefined;
        for (&writers, pipes) |*w, p| w.* = p[1];
        try sendAll(sp[0], "ABCD", &writers);
        for (writers) |w| closeFd(w);

        // Leave exactly two free fd slots below the soft limit: set it just
        // above the highest open fd, fill every hole, then free two.
        const saved = try posix.getrlimit(.NOFILE);
        const highest = FdSnapshot.take().highest();
        var lowered = saved;
        lowered.cur = highest + 1 + 2;
        try posix.setrlimit(.NOFILE, lowered);
        defer posix.setrlimit(.NOFILE, saved) catch {};
        var fillers: [1024]posix.fd_t = undefined;
        var n_fillers: usize = 0;
        defer closeAll(fillers[0..n_fillers]);
        while (n_fillers < fillers.len) {
            const rc = sys.dup(sp[0]);
            if (posix.errno(rc) != .SUCCESS) break;
            fillers[n_fillers] = @intCast(ival(rc));
            n_fillers += 1;
        }
        try testing.expect(n_fillers >= 2 and n_fillers < fillers.len);
        closeFd(fillers[n_fillers - 1]);
        closeFd(fillers[n_fillers - 2]);
        n_fillers -= 2;
        const free_slots = 2;

        const pre_read = FdSnapshot.take();
        var data: [16]u8 = undefined;
        var control: [cmsg.space(512 * @sizeOf(posix.fd_t))]u8 align(cmsg_align) = undefined;
        var got: [8]i32 = undefined;
        switch (recvRaw(sp[1], &data, &control, recv_flags)) {
            .received => |r| {
                if (expect.emfile_fails_recvmsg) return error.ExpectedEmfile;
                // Linux: the data, the fds that fit, and MSG_CTRUNC. The
                // kernel closed the rest inside recvmsg.
                try testing.expectEqual(@as(usize, 4), r.len);
                try testing.expect(r.truncated());
                const n = receivedFds(&control, r, &got);
                closeAll(got[0..@min(n, got.len)]);
                try testing.expectEqual(@as(usize, free_slots), n);
            },
            .failed => |err| {
                if (!expect.emfile_fails_recvmsg) {
                    std.debug.print("FD-0: recvmsg at the fd limit failed with errno {d}\n", .{@backingInt(err)});
                    return error.UnexpectedRecvFailure;
                }
                // macOS: EMFILE, and nothing was installed.
                try testing.expectEqual(posix.E.MFILE, err);
                var installed: [8]posix.fd_t = undefined;
                try testing.expectEqual(@as(usize, 0), FdSnapshot.take().added(pre_read, &installed));
                // The retry, at the same limit, returns the data with no
                // fds and no MSG_CTRUNC: the kernel dropped them.
                const r = try recvOk(sp[1], &data, &control, recv_flags);
                try testing.expectEqual(@as(usize, 4), r.len);
                try testing.expectEqual(@as(usize, 0), r.control_len);
                try testing.expect(!r.truncated());
            },
        }
        // Either way no writer leaked: the kernel closed every fd that did
        // not reach us.
        for (pipes) |p| try testing.expect(pipeWritersClosed(p[0], closed_wait_ms));
    }
    try expectNoFdDelta(before);
}

// ---------------------------------------------------------------------------
// Blocking close
// ---------------------------------------------------------------------------

/// A TCP client socket whose final close lingers: a peer that never reads
/// leaves unsent data queued, and SO_LINGER is on.
const LingeringSocket = struct {
    listener: posix.fd_t,
    server_side: posix.fd_t,
    client: posix.fd_t,

    const linger_seconds = 1;
    var chunk: [64 * 1024]u8 = @splat(0xab);

    fn open() !LingeringSocket {
        const listener: posix.fd_t = @intCast(try check(sys.socket(posix.AF.INET, posix.SOCK.STREAM, 0), "socket"));
        errdefer closeFd(listener);
        var addr: posix.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f00_0001) };
        _ = try check(sys.bind(listener, @ptrCast(&addr), @sizeOf(posix.sockaddr.in)), "bind");
        _ = try check(sys.listen(listener, 1), "listen");
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
        _ = try check(sys.getsockname(listener, @ptrCast(&addr), &addr_len), "getsockname");
        const client: posix.fd_t = @intCast(try check(sys.socket(posix.AF.INET, posix.SOCK.STREAM, 0), "socket"));
        errdefer closeFd(client);
        _ = try check(sys.connect(client, @ptrCast(&addr), @sizeOf(posix.sockaddr.in)), "connect");
        const server_side: posix.fd_t = @intCast(try check(sys.accept(listener, null, null), "accept"));
        errdefer closeFd(server_side);

        // Fill the send queue: the server side never reads. O_NONBLOCK, not
        // MSG_DONTWAIT: Darwin's send path ignores MSG_DONTWAIT and blocks.
        // One pass is not enough on macOS: receive-buffer autotuning then
        // drains the queue, the FIN goes out and the close does not linger.
        // So refill until two passes 10 ms apart add nothing.
        try setNonBlocking(client, true);
        var queued = try fillSendQueue(client);
        try testing.expect(queued > 0);
        var quiet_passes: usize = 0;
        var passes: usize = 0;
        while (quiet_passes < 2) : (passes += 1) {
            if (passes == 200) return error.SendQueueNeverSettled;
            try std.Io.sleep(testing.io, .fromMilliseconds(10), .awake);
            const more = try fillSendQueue(client);
            queued += more;
            quiet_passes = if (more == 0) quiet_passes + 1 else 0;
        }
        try setNonBlocking(client, false);

        // Darwin's SO_LINGER counts clock ticks; SO_LINGER_SEC counts
        // seconds, as Linux's SO_LINGER does.
        const linger_opt = if (is_macos) posix.SO.LINGER_SEC else posix.SO.LINGER;
        const lg: posix.linger = .{ .onoff = 1, .linger = linger_seconds };
        _ = try check(sys.setsockopt(client, posix.SOL.SOCKET, linger_opt, std.mem.asBytes(&lg), @sizeOf(posix.linger)), "setsockopt(SO_LINGER)");
        return .{ .listener = listener, .server_side = server_side, .client = client };
    }

    /// Sends on the non-blocking `client` until EAGAIN; returns the bytes sent.
    fn fillSendQueue(client: posix.fd_t) !usize {
        var queued: usize = 0;
        while (queued < 64 * 1024 * 1024) {
            switch (sendRights(client, &chunk, &.{}, 0)) {
                .sent => |n| queued += n,
                .failed => |err| {
                    if (err == .AGAIN) return queued;
                    std.debug.print("FD-0: filling the TCP send queue failed with errno {d}\n", .{@backingInt(err)});
                    return error.SyscallFailed;
                },
            }
        }
        return error.SendQueueNeverFilled;
    }

    fn deinit(self: *LingeringSocket) void {
        if (self.client >= 0) closeFd(self.client);
        closeFd(self.server_side);
        closeFd(self.listener);
    }
};

/// A blocked close takes at least this long (the linger time is 1000 ms).
const blocked_min_ms = 700;
/// A close that does not block finishes well within this.
const unblocked_max_ms = 500;

test "FD-0 the final close of a received lingering socket blocks the closing thread (Linux: inside recvmsg with no control buffer)" {
    if (!supported) return error.SkipZigTest;
    const before = FdSnapshot.take();
    { // With a control buffer the fd reaches us; our close is the final one.
        var lingering = try LingeringSocket.open();
        defer lingering.deinit();
        const sp = try socketPair();
        defer closeFd(sp[0]);
        defer closeFd(sp[1]);
        try sendAll(sp[0], "L", &.{lingering.client});
        closeFd(lingering.client);
        lingering.client = -1;

        var data: [8]u8 = undefined;
        var control: [cmsg.space(16 * @sizeOf(posix.fd_t))]u8 align(cmsg_align) = undefined;
        const recv_start = nowNs();
        const r = try recvOk(sp[1], &data, &control, recv_flags);
        const recv_ms = msSince(recv_start);
        var got: [4]i32 = undefined;
        try testing.expectEqual(@as(usize, 1), receivedFds(&control, r, &got));

        const close_start = nowNs();
        closeFd(got[0]);
        const close_ms = msSince(close_start);
        errdefer std.debug.print("FD-0: recvmsg took {d} ms, close took {d} ms\n", .{ recv_ms, close_ms });
        try testing.expect(recv_ms < unblocked_max_ms);
        if (expect.final_close_lingers) {
            try testing.expect(close_ms >= blocked_min_ms);
        } else {
            try testing.expect(close_ms < unblocked_max_ms);
        }
    }
    { // With no control buffer (today's read path): on Linux the kernel does
        // the final close inside recvmsg, on the reader thread.
        var lingering = try LingeringSocket.open();
        defer lingering.deinit();
        const sp = try socketPair();
        defer closeFd(sp[0]);
        defer closeFd(sp[1]);
        try sendAll(sp[0], "L", &.{lingering.client});
        closeFd(lingering.client);
        lingering.client = -1;

        const pre_read = FdSnapshot.take();
        var data: [8]u8 = undefined;
        const recv_start = nowNs();
        _ = try recvOk(sp[1], &data, &.{}, 0);
        const recv_ms = msSince(recv_start);
        var leaked: [4]posix.fd_t = undefined;
        const n_leaked = FdSnapshot.take().added(pre_read, &leaked);
        try testing.expectEqual(@as(usize, @intFromBool(expect.no_control_read_installs_fds)), n_leaked);
        var close_ms: i64 = 0;
        if (n_leaked == 1) {
            const close_start = nowNs();
            closeFd(leaked[0]);
            close_ms = msSince(close_start);
        }
        errdefer std.debug.print("FD-0: no-control recvmsg took {d} ms, close of the leaked fd took {d} ms\n", .{ recv_ms, close_ms });
        if (expect.final_close_lingers and !expect.no_control_read_installs_fds) {
            try testing.expect(recv_ms >= blocked_min_ms);
        } else {
            try testing.expect(recv_ms < unblocked_max_ms);
        }
        if (expect.final_close_lingers and expect.no_control_read_installs_fds) {
            try testing.expect(close_ms >= blocked_min_ms);
        } else {
            try testing.expect(close_ms < unblocked_max_ms);
        }
    }
    try expectNoFdDelta(before);
}
