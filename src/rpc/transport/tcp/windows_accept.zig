//! The transport's own TCP accept on Windows. `Listener.accept`,
//! `Listener.acceptFd` (and so `ServerSession.accept`) and the workers of a
//! `WorkerPool` accept through it there, instead of through std's
//! `net.Server.accept`.
//!
//! std's (0.17.0) `netAcceptWindows` waits for a connection with
//! `IOCTL_AFD_WAIT_FOR_LISTEN` and treats a wait that ends STATUS_CANCELLED
//! as `unreachable`. A close of the listening handle ends a pending wait
//! that way, so `Listener.close` with a thread parked in accept aborted the
//! process: a panic in Debug and ReleaseSafe, undefined behavior in
//! ReleaseFast and ReleaseSmall. Windows CI run 38918354154 showed it for
//! all three accept entry points
//! (tests/rpc/transport/tcp/rpc_listener_close_test.zig).
//!
//! `accept` takes the same AFD steps as std: wait, open an AFD socket, and
//! accept the connection into it, or give the connection back with
//! `IOCTL_AFD_DEFER_ACCEPT` when a later step fails. It differs only in a
//! cancelled wait: once the caller's `closing` flag is set, the accept
//! returns `error.ListenerClosed`; until then, it posts the wait again.
//!
//! `stopWaits` is the closing side. The caller sets `closing` first and
//! counts the threads inside `accept` (`in_flight`). `stopWaits` cancels
//! every pending I/O request on the handle until that count is 0, and only
//! then does the caller close the handle. So no accept is left waiting on a
//! closed handle, and no accept that read `closing` before it was set can
//! post its wait on a closed handle, or on a handle value that Windows has
//! given to another object since.

const std = @import("std");
const builtin = @import("builtin");
const net = std.Io.net;
const log = std.log.scoped(.rpc_runtime);

/// Every error `accept` returns: `net.Server.AcceptError`, which std's
/// accept returns, and `error.ListenerClosed`.
pub const Error = net.Server.AcceptError || error{ListenerClosed};

/// What the final status of one `IOCTL_AFD_WAIT_FOR_LISTEN` means to
/// `accept`. `closing` is the caller's flag, read after the wait ended.
const WaitOutcome = enum {
    /// A connection is waiting to be accepted.
    connection,
    /// The wait was cancelled after the caller started to close:
    /// `error.ListenerClosed`.
    closed,
    /// The wait was cancelled, but the caller is not closing: post it
    /// again.
    wait_again,
    /// `error.SystemResources`, as std maps it.
    system_resources,
    /// Any other status: `error.Unexpected`, as std maps it.
    unexpected,
};

fn waitOutcome(status: std.os.windows.NTSTATUS, closing: bool) WaitOutcome {
    return switch (status) {
        .SUCCESS => .connection,
        .CANCELLED => if (closing) .closed else .wait_again,
        .INSUFFICIENT_RESOURCES => .system_resources,
        else => .unexpected,
    };
}

/// How long `accept` waits before it posts a cancelled wait again, from the
/// second one in a row on. The first one goes at once. The wait keeps a
/// handle that something keeps cancelling from using a whole core.
const repost_wait: std.Io.Duration = .fromMilliseconds(1);

/// The output of `IOCTL_AFD_WAIT_FOR_LISTEN`, laid out as std's
/// `netAcceptWindows` lays it out.
const ListenResponse = extern struct {
    Info: std.os.windows.AFD.LISTEN_RESPONSE_INFO,
    RemoteAddress: extern union {
        posix: std.Io.Threaded.PosixAddress,
        unix: std.posix.sockaddr.un,
    },
};

/// Accept one connection on `listen_handle`, an AFD listening socket, and
/// return the new socket's handle. `options` are the listener's
/// (`net.Server.options`). `closing` is the caller's close flag: the
/// listener's `close_requested`, or a `WorkerPool`'s `should_stop`.
///
/// The caller must count itself in the `in_flight` count it gives
/// `stopWaits`, and read `closing` after it did so, before it calls this.
///
/// It returns:
/// - the accepted socket. It may still be returned once `closing` is set,
///   if the connection came before the close cancelled the wait.
/// - `error.ListenerClosed`: a wait (or the accept step) was cancelled
///   after `closing` was set. A cancelled wait while `closing` is unset is
///   posted again (the first at once, each later one in a row after
///   `repost_wait`).
/// - `error.Canceled`: an Io cancellation of the calling task. As with
///   std's accept, a connection that the cancelled wait had taken is not
///   given back.
/// - the rest as std's (0.17.0) `netAcceptWindows` maps them.
pub fn accept(
    io: std.Io,
    listen_handle: net.Socket.Handle,
    options: net.Server.AcceptOptions,
    closing: *const std.atomic.Value(bool),
) Error!net.Socket.Handle {
    if (comptime builtin.os.tag != .windows) @compileError("windows_accept is Windows only");
    const windows = std.os.windows;
    var response: ListenResponse = undefined;
    var cancelled_in_a_row: u32 = 0;
    while (true) {
        const result = try io.operate(.{ .device_io_control = .{
            .file = .{ .handle = listen_handle, .flags = .{ .nonblocking = true } },
            .code = windows.IOCTL.AFD.WAIT_FOR_LISTEN,
            .out = std.mem.asBytes(&response),
        } });
        const status = result.device_io_control.u.Status;
        switch (waitOutcome(status, closing.load(.acquire))) {
            .connection => {},
            .closed => return error.ListenerClosed,
            .wait_again => {
                cancelled_in_a_row += 1;
                log.debug("Windows cancelled a listen wait while the listener is open; posting it again ({d} in a row)", .{cancelled_in_a_row});
                if (cancelled_in_a_row > 1) try io.sleep(repost_wait, .awake);
                continue;
            },
            .system_resources => return error.SystemResources,
            .unexpected => return windows.unexpectedStatus(status),
        }
        cancelled_in_a_row = 0;
        if (try take(io, listen_handle, options, &response, closing)) |accepted| return accepted;
    }
}

/// Accept the connection that a wait returned into a new AFD socket. Null
/// when Windows cancelled the accept step while `closing` is unset: the
/// connection was given back, and `accept` waits again.
fn take(
    io: std.Io,
    listen_handle: net.Socket.Handle,
    options: net.Server.AcceptOptions,
    response: *const ListenResponse,
    closing: *const std.atomic.Value(bool),
) Error!?net.Socket.Handle {
    const windows = std.os.windows;
    const sequence = response.Info.Sequence;
    const accept_handle = openSocket(io, response.RemoteAddress.posix.any.family, options) catch |err| {
        deferConnection(io, listen_handle, sequence);
        return err;
    };
    const info: windows.AFD.ACCEPT_INFO = .{
        .UseSAN = .FALSE,
        .Sequence = sequence,
        .AcceptHandle = accept_handle,
    };
    const result = io.operate(.{ .device_io_control = .{
        .file = .{ .handle = listen_handle, .flags = .{ .nonblocking = true } },
        .code = windows.IOCTL.AFD.ACCEPT,
        .in = std.mem.asBytes(&info),
    } }) catch |err| {
        windows.CloseHandle(accept_handle);
        deferConnection(io, listen_handle, sequence);
        return err;
    };
    const status = result.device_io_control.u.Status;
    if (status == .SUCCESS) return accept_handle;
    windows.CloseHandle(accept_handle);
    deferConnection(io, listen_handle, sequence);
    return switch (status) {
        .CANCELLED => if (closing.load(.acquire)) error.ListenerClosed else null,
        .INSUFFICIENT_RESOURCES => error.SystemResources,
        else => windows.unexpectedStatus(status),
    };
}

/// Give a connection that a wait returned back to the listener
/// (`IOCTL_AFD_DEFER_ACCEPT`, not rejected), as std does when its accept
/// step fails. Best effort: no Io cancellation interrupts it, and a status
/// other than success is only logged. When the listener is closing, the
/// close releases the connection anyway.
fn deferConnection(io: std.Io, listen_handle: net.Socket.Handle, sequence: std.os.windows.ULONG) void {
    const windows = std.os.windows;
    const protection = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(protection);
    const info: windows.AFD.DEFER_ACCEPT_INFO = .{ .Sequence = sequence, .Reject = .FALSE };
    const result = io.operate(.{ .device_io_control = .{
        .file = .{ .handle = listen_handle, .flags = .{ .nonblocking = true } },
        .code = windows.IOCTL.AFD.DEFER_ACCEPT,
        .in = std.mem.asBytes(&info),
    } }) catch |err| {
        log.debug("giving back a connection failed: {t}", .{err});
        return;
    };
    const status = result.device_io_control.u.Status;
    if (status != .SUCCESS) log.debug("giving back a connection failed: NTSTATUS 0x{x}", .{@backingInt(status)});
}

/// Open an AFD socket for an accepted connection, as std's (0.17.0) private
/// `openSocketAfd` does, and map its errors as `netAcceptWindows` does.
fn openSocket(
    io: std.Io,
    family: std.os.windows.ws2_32.ADDRESS_FAMILY,
    options: net.Server.AcceptOptions,
) Error!std.os.windows.HANDLE {
    const windows = std.os.windows;
    const mode, const protocol = std.Io.Threaded.posixSocketModeProtocol(family, options.mode, options.protocol) catch return error.Unexpected;
    var handle: windows.HANDLE = undefined;
    var iosb: windows.IO_STATUS_BLOCK = undefined;
    while (true) {
        try io.checkCancel();
        switch (windows.ntdll.NtCreateFile(
            &handle,
            .{
                .STANDARD = .{ .RIGHTS = .{ .WRITE_DAC = true }, .SYNCHRONIZE = true },
                .GENERIC = .{ .WRITE = true, .READ = true },
            },
            &.{
                .ObjectName = @constCast(&windows.UNICODE_STRING.init(
                    windows.AFD.DEVICE_NAME ++ .{ '\\', 'E', 'n', 'd', 'p', 'o', 'i', 'n', 't' },
                )),
            },
            &iosb,
            null,
            .{},
            .{ .READ = true, .WRITE = true },
            .OPEN_IF,
            .{ .IO = .ASYNCHRONOUS },
            &windows.AFD.OPEN_PACKET.FULL_EA_INFORMATION{ .Value = .{
                .EndpointType = .{
                    .CONNECTIONLESS = switch (options.mode) {
                        .stream, .seqpacket, .rdm => false,
                        .dgram, .raw => true,
                    },
                    .MESSAGEMODE = options.mode != .stream,
                    .RAW = options.mode == .raw,
                },
                .GroupID = 0,
                .AddressFamily = family,
                .SocketType = @bitCast(mode),
                .Protocol = @bitCast(protocol),
                .TransportDeviceNameLength = 0,
                .TransportDeviceName = undefined,
            } },
            @sizeOf(windows.AFD.OPEN_PACKET.FULL_EA_INFORMATION),
        )) {
            .SUCCESS => return handle,
            // std retries after its cancellation check; so does this.
            .CANCELLED => continue,
            // std maps both to errors that `netAcceptWindows` then turns
            // into `error.Unexpected`.
            .PROTOCOL_NOT_SUPPORTED, .NO_SUCH_FILE => return error.Unexpected,
            else => |status| return windows.unexpectedStatus(status),
        }
    }
}

/// The closing side of `accept`. The caller has set the `closing` flag that
/// its accepts read. This cancels every pending I/O request of this process
/// on `listen_handle` (`CancelIoEx` with no OVERLAPPED), again every
/// millisecond, until `in_flight` (the caller's count of threads inside
/// `accept`) is 0. Each cancelled wait then sees `closing` and returns
/// `error.ListenerClosed`. An accept that counted itself before `closing`
/// was set, but had not posted its wait yet, is cancelled by a later pass.
/// Then the caller may close the handle.
///
/// An Io cancellation of the calling task does not end it: the handle must
/// not be closed under a pending wait. It ends once every accept has
/// returned, which needs no network: no dial, and no free port.
pub fn stopWaits(io: std.Io, listen_handle: net.Socket.Handle, in_flight: *const std.atomic.Value(u32)) void {
    if (comptime builtin.os.tag != .windows) @compileError("windows_accept is Windows only");
    const protection = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(protection);
    while (in_flight.load(.seq_cst) != 0) {
        // FALSE with ERROR_NOT_FOUND when nothing is pending, for example
        // while an accept is between its count and its wait. The next pass
        // gets it.
        _ = CancelIoEx(listen_handle, null);
        io.sleep(.fromMilliseconds(1), .awake) catch {};
    }
}

/// kernel32's `CancelIoEx`. std (0.17.0) declares `NtCancelIoFileEx` with
/// a request pointer that cannot be null, and only a null request cancels
/// every request on the handle, whichever thread posted it.
extern "kernel32" fn CancelIoEx(hFile: std.os.windows.HANDLE, lpOverlapped: ?*anyopaque) callconv(.winapi) std.os.windows.BOOL;

test "a cancelled Windows listen wait ends the accept only once the listener is closing" {
    // Runs on every host: the mapping is pure. std's (0.17.0) own accept
    // treats CANCELLED as unreachable in both cases.
    try std.testing.expectEqual(WaitOutcome.closed, waitOutcome(.CANCELLED, true));
    try std.testing.expectEqual(WaitOutcome.wait_again, waitOutcome(.CANCELLED, false));
    // Every other status maps as std maps it, whether or not the listener
    // is closing: a connection that came first is still accepted.
    for ([_]bool{ false, true }) |closing| {
        try std.testing.expectEqual(WaitOutcome.connection, waitOutcome(.SUCCESS, closing));
        try std.testing.expectEqual(WaitOutcome.system_resources, waitOutcome(.INSUFFICIENT_RESOURCES, closing));
        try std.testing.expectEqual(WaitOutcome.unexpected, waitOutcome(.INVALID_HANDLE, closing));
    }
}
