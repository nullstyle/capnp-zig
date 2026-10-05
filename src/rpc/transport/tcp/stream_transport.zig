const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_transport);
const net = std.Io.net;
const events = @import("../../events.zig");
const framing = @import("../../wire/framing.zig");
const fd_io = @import("../unix/fd_io.zig");

/// Opaque platform-stable socket handle wrapper passed into the transport
/// layer. This is the canonical handle type in every public, handle-taking
/// entry point (`Connection.init`, `Listener.initFd`/`acceptFd`/`listenHandle`,
/// `Transport.init*`, `createLoopbackSocketPair`, and the `runtime` re-export).
///
/// It is a thin named wrapper so the raw platform handle never leaks into a
/// frozen signature: `std.Io.net.Socket.Handle` is an `i32` on POSIX and a
/// pointer (`*anyopaque`/`HANDLE`) on Windows. Wrapping it keeps the public
/// surface — and the `docs/api-snapshot.txt` gate built from it — byte-for-byte
/// identical on every platform. Treat `handle` as opaque; do not depend on its
/// concrete type across targets.
pub const SocketFd = struct {
    handle: net.Socket.Handle,
};

/// TCP transport layer with concurrent read/write support.
///
/// `Transport` owns a TCP socket handle and provides blocking
/// read operations on the reader thread and asynchronous writes via a
/// dedicated writer thread.
///
/// ## Concurrent architecture
///
/// Reads happen on the caller's thread (blocking `read()`). Writes are
/// enqueued via `enqueueWrite()` (thread-safe, non-blocking) and drained
/// by a dedicated writer thread started with `startWriter()`.
///
/// ## Shutdown sequence
///
/// Call `close()` to shut down the socket and signal the writer to stop.
/// After close, `read()` returns 0 and `enqueueWrite()` returns
/// `error.BrokenPipe`. Call `stopWriter()` to join the writer thread and
/// drain remaining queued writes. `deinit()` calls `stopWriter()` then
/// closes the fd if not already closed.
///
/// ## Cross-platform
///
/// Uses `std.Io` for all socket operations, supporting both POSIX and
/// Windows via the Io VTable abstraction.
///
/// ## AF_UNIX sockets: drain mode
///
/// On Linux and macOS, `initWithOptions` reads the socket family once (with
/// `getsockname`). On an AF_UNIX socket, and on any socket whose family it
/// cannot read, the transport runs in drain mode: a read waits until the
/// socket is readable, checks the closer's bound, then does one `recvmsg`
/// with a control buffer (`rpc.transport.unix.fd_io.recvWithFds`), and every
/// file descriptor the peer attached goes to the process-wide closer
/// (`fd_io.closer`, its `.received` lane), never closed on the reading
/// thread. Each read that brought fds emits a `.resource_rejection` event
/// (`resource = .attached_fds`), and the transport reports
/// `events.Source.unix`.
///
/// Fds still riding on unread messages are closed by the kernel inside the
/// final close of this socket, and on macOS already inside
/// `shutdown(SHUT_RD)`. So `deinit` closes the socket inline only on Linux
/// and only when its receive queue is empty after `shutdown(SHUT_RD)`;
/// otherwise the close goes to the closer's `.socket` lane, and on macOS
/// `shutdown` does its read half there too (the write half stays inline).
/// Neither ever waits for a close that blocks.
///
/// See `fd_io` for why, and for what is left: without drain mode, macOS
/// installs and leaks every fd a local peer sends, and Linux can stall the
/// reader on a lingering socket. TCP sockets read exactly as before, and so
/// does a handle that is not a socket at all (only a test's fake `Io` passes
/// one).
///
/// ## Sending fds (AF_UNIX)
///
/// `enqueueWriteWithFds` queues a message with fds attached. The queue holds
/// a close-on-exec dup of each fd, at most `max_queued_fds` at a time, and
/// the writer sends the message with one `fd_io.sendWithFds`. Every dup goes
/// to the closer's `.sent` lane once: after its send, after a failed send
/// (and for each later item of that batch, which is never sent), or when
/// `stopWriter` drains the queue. No thread of the transport closes a dup
/// itself, so `deinit` never waits for one.
///
/// Every dup counts against the process fd budget (`fd_io.budget`), from
/// before it is made until the closer has closed it. A message whose dups do
/// not fit is refused with `error.FdQueueFull` and a `.backpressure` event
/// (`err = error.FdBudgetExceeded`); messages without fds still go. While a
/// dup's close blocks, the dups behind it pile up to that budget, and then
/// every transport refuses fd messages until the closer catches up.
///
/// On Linux a send can also fail with ETOOMANYREFS: the user already has
/// `RLIMIT_NOFILE` fds in flight on AF_UNIX sockets. The writer then sends
/// that message without its fds (`attachedFd` past the message's fds means
/// no fd) and emits a `.backpressure` event (`err =
/// error.TooManyFdsInFlight`); the connection stays up.
///
/// ## Receiving fds (AF_UNIX): exact-boundary reads
///
/// `enableFdPassing`, called before the first read with
/// `max_fds_per_message > 0`, makes the transport keep the fds a peer
/// attaches to a message instead of closing them. It then reads one Cap'n
/// Proto frame at a time: the first 8 bytes of the header, the rest of the
/// segment table, then the body. Each `recvmsg` asks for at most the bytes
/// left in the current part, so no read crosses a frame boundary, and every
/// fd belongs to the frame whose bytes carried it. Bulk reads cannot give
/// that rule: they attach an fd to a read's last bytes on Linux and to its
/// first byte on macOS (FD-0). It costs about two `recvmsg` calls per frame,
/// on these connections only.
///
/// Each header is checked against the frozen `framing.Framer`'s limits
/// before the next part is read: at most `Framer.max_segment_count`
/// segments, `Framer.max_frame_words` words and `max_buffered_frame_bytes`
/// in all. So no byte of a body past the limits is read, and nothing past
/// them is buffered. One `read` returns at most the rest of one part.
///
/// The fds of a frame:
/// - It keeps at most `max_fds_per_message`; the extras go to the closer
///   (`error.AttachedFdsOverLimit`).
/// - Every fd it keeps counts against the process fd budget
///   (`fd_io.budget`) until it is taken or goes to the closer. It keeps only
///   the fds that fit; the rest go to the closer with a `.resource_rejection`
///   event (`err = error.FdBudgetExceeded`, `attempted` = the fds the budget
///   would count with them, `limit` = its limit). The frame still arrives,
///   and the connection stays.
/// - Fds that arrive in two batches (two `recvmsg` calls) within one frame
///   are a protocol error: every fd the transport holds goes to the closer,
///   and the connection ends.
/// - A batch the kernel cut short (MSG_CTRUNC: Linux at RLIMIT_NOFILE) or
///   dropped (EMFILE: macOS) leaves the frame with no fds. The fds of that
///   read go to the closer, an event reports it, and the connection stays.
///   A second EMFILE in a row ends the connection, as in drain mode.
/// - After the read that completes a frame, `frameFdCount` and
///   `takeFrameFd` give its fds, until the next read. `releaseFrameFds`
///   (the `Connection` calls it after dispatch), the next read and
///   `deinit` hand every fd nobody took to the closer. A taken fd leaves the
///   budget: it is the caller's.
/// - A frame that never completes (the stream ends or fails mid-frame)
///   gives its fds to the closer when reading ends, and at `deinit`.
///
/// No fd is closed on the reading thread: every one goes to the closer's
/// `.received` lane. A protocol error (a bad header, a frame too large, two
/// fd batches in one frame) emits a `.protocol_error` event naming the
/// cause (`error.InvalidFrame`, `error.FrameTooLarge` or
/// `error.MultipleAttachedFdBatches`), and `read` then fails with
/// `error.ConnectionResetByPeer`, on that call and every later one.
pub const Transport = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    fd: net.Socket.Handle,
    read_buf: []u8,
    observer: ?events.Observer = null,
    /// The `events.Source` this transport reports: `.unix` for an AF_UNIX
    /// socket, `.tcp` otherwise. Set by `initWithOptions`.
    source: events.Source = .tcp,
    /// Drain-mode state; null for a socket read the plain way (see the type
    /// doc). Owned by the transport and freed by `deinit`.
    drain: ?*FdDrain = null,
    close_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    fd_closed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    fd_mu: std.atomic.Mutex = .unlocked,

    // Write queue for concurrent write support
    write_queue: WriteQueue = .{},
    writer_thread: ?std.Thread = null,

    pub const default_max_queued_items: usize = 1024;
    pub const default_max_queued_bytes: usize = 64 * 1024 * 1024;
    /// The most fds one transport holds for sending at a time: the dups of
    /// every message queued or being written (`enqueueWriteWithFds`).
    pub const max_queued_fds: usize = 256;

    pub const ReadError = net.Stream.Reader.Error;
    pub const WriteError = net.Stream.Writer.Error;

    pub const EnqueueError = error{
        BrokenPipe,
        OutOfMemory,
        WriteQueueFull,
        WriteQueueBytesExceeded,
    };

    /// `enqueueWriteWithFds` failures: the `enqueueWrite` ones, plus the fd
    /// checks. On every failure nothing was queued and the caller still owns
    /// its fds.
    pub const EnqueueFdsError = EnqueueError || error{
        /// This transport cannot carry fds: its socket is not AF_UNIX, or
        /// fd passing is not compiled in for this target (only Linux and
        /// macOS have it).
        FdPassingUnsupported,
        /// More than `fd_io.max_fds_per_send` (253) fds in one message.
        TooManyFds,
        /// Fds with an empty message: a stream socket cannot carry them.
        FdsWithoutData,
        /// Backpressure: this message's fds would take the transport past
        /// `max_queued_fds`, or their dups do not fit in the process fd
        /// budget (`fd_io.budget`). Retry once the writer has sent what it
        /// holds and the closer has caught up. Before `startWriter` (a
        /// direct send, no dups) it means the kernel refused the send with
        /// ETOOMANYREFS (`fd_io.SendError.TooManyFdsInFlight`): nothing was
        /// sent.
        FdQueueFull,
        /// One of the fds is not open.
        InvalidFd,
        /// The process fd table has no room for the dups.
        ProcessFdQuotaExceeded,
        /// The closer thread that closes the dups could not start, or a
        /// direct send ran out of kernel buffers.
        SystemResources,
        Unexpected,
    };

    /// Point-in-time write queue occupancy, for metrics scraping.
    pub const QueueStats = struct {
        items: usize,
        bytes: usize,
        max_items: usize,
        max_bytes: usize,
        /// Dups of fds held for sending (queued plus being written).
        fds: usize = 0,
        max_fds: usize = 0,
    };

    pub const Options = struct {
        read_buffer_size: usize,
        write_queue_max_items: usize = default_max_queued_items,
        write_queue_max_bytes: usize = default_max_queued_bytes,
        observer: ?events.Observer = null,
    };

    /// The most fds one inbound message keeps, whatever
    /// `FdPassingOptions.max_fds_per_message` asks for: Linux's
    /// `SCM_MAX_FD`, the most one `sendmsg` carries there (`fd_io`).
    pub const max_fds_per_message_cap: u8 = @intCast(fd_io.max_fds_per_send);

    /// See `enableFdPassing`.
    pub const FdPassingOptions = struct {
        /// The most fds one inbound message keeps; the extras are closed.
        /// 0 turns fd passing off (drain mode: every fd closed). Values
        /// above `max_fds_per_message_cap` (253) count as 253.
        max_fds_per_message: u8,
        /// The frame limit each header is checked against before the rest
        /// of the frame is read: header plus body, in bytes. Give it the
        /// same value as the `framing.Framer` the bytes go to
        /// (`Connection.enableFdPassing` does).
        max_buffered_frame_bytes: usize = framing.Framer.default_max_buffered_bytes,
    };

    /// `enableFdPassing` failures. On each, nothing changed.
    pub const EnableFdPassingError = error{
        /// The socket is not AF_UNIX, or fd passing is not compiled in for
        /// this target (only Linux and macOS have it).
        FdPassingUnsupported,
        /// The transport has already read: fd passing must start at the
        /// first byte of the stream, so frames and their fds line up.
        AlreadyReading,
        /// The closer-queue reservation for the fds a frame holds, or the
        /// frame state, could not be allocated.
        OutOfMemory,
    };

    /// Thread-safe write queue. The queue state and condition wait use the
    /// same mutex so enqueue/close cannot signal between the writer's state
    /// check and its wait registration.
    ///
    /// An item may carry fds: the queue's own dups of the caller's fds
    /// (`F_DUPFD_CLOEXEC`), sent with the item's bytes. Every dup goes to
    /// the closer's `.sent` lane exactly once: after its item's send, after
    /// a failed or skipped send (`Transport.writerLoop`), or in `drain`
    /// (`stopWriter`). Never closed inline: once the app and the receiver
    /// have closed their copies, a dup's close is the final one and can
    /// block (see `fd_io.closer`).
    const WriteQueue = struct {
        mu: std.Io.Mutex = .init,
        cond: std.Io.Condition = .init,
        items: std.ArrayListUnmanaged(Item) = .empty,
        queued_bytes: usize = 0,
        in_flight_bytes: usize = 0,
        /// Dups held by queued items, and by the items of the batch the
        /// writer is sending. Both count against `max_fds`.
        queued_fds: usize = 0,
        in_flight_fds: usize = 0,
        max_items: usize = default_max_queued_items,
        max_bytes: usize = default_max_queued_bytes,
        max_fds: usize = max_queued_fds,
        /// `.sent`-lane capacity for every dup held, so no hand-off of a
        /// dup allocates or falls back to an inline close. It is also how
        /// the dups held here count against the process fd budget (each
        /// slot is one unit, `fd_io.closer.reserveSent`). Invariant (under
        /// `mu`):
        /// `fd_reservation.slots == queued_fds + in_flight_fds` (a failed
        /// enqueue trims what it reserved). Used only under `mu`.
        fd_reservation: fd_io.closer.Reservation = .{ .lane = .sent },
        closed: bool = false,

        const Item = struct {
            bytes: []u8,
            /// Owned dups; empty (and not allocated) for a plain write.
            fds: []fd_io.Fd = &.{},
        };

        /// Queue occupancy before and after a successful enqueue, for
        /// pressure-crossing emission outside the queue lock.
        const EnqueueOutcome = struct {
            prev_items: usize,
            items: usize,
            prev_bytes: usize,
            bytes: usize,
        };

        /// Copy bytes into the queue after checking the configured item and
        /// byte bounds. The copy is deliberately made while the queue lock is
        /// held so a full queue is rejected before allocating.
        fn enqueueCopy(self: *WriteQueue, io: std.Io, allocator: std.mem.Allocator, bytes: []const u8) EnqueueError!EnqueueOutcome {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);

            const accounted_bytes = try self.checkBoundsLocked(bytes.len);
            const data = allocator.dupe(u8, bytes) catch return error.OutOfMemory;
            errdefer allocator.free(data);
            self.items.append(allocator, .{ .bytes = data }) catch {
                return error.OutOfMemory;
            };
            return self.queuedLocked(io, accounted_bytes, data.len);
        }

        /// `enqueueCopy` for a message with fds: also checks the fd bounds
        /// (`max_fds` here, then the process fd budget, which fails with
        /// `error.FdBudgetExceeded`), and queues a dup of each fd. The dups
        /// are made last, so a failure before them leaves nothing to undo
        /// but frees and the reservation; a failed dup sends the dups made
        /// so far to the closer.
        fn enqueueCopyWithFds(
            self: *WriteQueue,
            io: std.Io,
            allocator: std.mem.Allocator,
            bytes: []const u8,
            fds: []const fd_io.Fd,
        ) (EnqueueFdsError || error{FdBudgetExceeded})!EnqueueOutcome {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);

            const accounted_bytes = try self.checkBoundsLocked(bytes.len);
            const held_fds = self.queued_fds + self.in_flight_fds;
            if (fds.len > self.max_fds or held_fds > self.max_fds - fds.len) {
                return error.FdQueueFull;
            }

            // The closer must exist before any dup does: dups are never
            // closed on the threads that use this queue.
            fd_io.closer.ensureStarted() catch return error.SystemResources;
            // Counts these dups against the process fd budget before any
            // exists; refused when they do not fit.
            fd_io.closer.reserveSent(&self.fd_reservation, held_fds + fds.len) catch |err| switch (err) {
                error.FdBudgetExceeded => return error.FdBudgetExceeded,
                error.OutOfMemory => return error.OutOfMemory,
                error.UnixSocketsUnsupported => return error.FdPassingUnsupported,
            };
            // Runs last on failure, after the dups made so far went to the
            // closer under this reservation: give back the rest of it.
            errdefer fd_io.closer.trim(&self.fd_reservation, held_fds);
            const data = allocator.dupe(u8, bytes) catch return error.OutOfMemory;
            errdefer allocator.free(data);
            const dups = allocator.alloc(fd_io.Fd, fds.len) catch return error.OutOfMemory;
            errdefer allocator.free(dups);
            self.items.ensureUnusedCapacity(allocator, 1) catch return error.OutOfMemory;

            var made: usize = 0;
            // Covered by the reservation above: cannot allocate or fail.
            errdefer fd_io.closer.handOffSent(&self.fd_reservation, dups[0..made]);
            while (made < fds.len) : (made += 1) {
                dups[made] = fd_io.dupCloexec(fds[made]) catch |err| return switch (err) {
                    error.InvalidFd => error.InvalidFd,
                    error.ProcessFdQuotaExceeded => error.ProcessFdQuotaExceeded,
                    error.Unexpected => error.Unexpected,
                    error.UnixSocketsUnsupported => error.FdPassingUnsupported,
                };
            }

            self.items.appendAssumeCapacity(.{ .bytes = data, .fds = dups });
            self.queued_fds += dups.len;
            return self.queuedLocked(io, accounted_bytes, data.len);
        }

        /// The item and byte bounds, under `mu`. Returns the bytes already
        /// accounted for (queued plus in flight).
        fn checkBoundsLocked(self: *WriteQueue, len: usize) EnqueueError!usize {
            if (self.closed) {
                return error.BrokenPipe;
            }
            if (self.items.items.len >= self.max_items) {
                return error.WriteQueueFull;
            }
            const accounted_bytes = self.queued_bytes + self.in_flight_bytes;
            if (len > self.max_bytes or accounted_bytes > self.max_bytes - len) {
                return error.WriteQueueBytesExceeded;
            }
            return accounted_bytes;
        }

        /// Account for the item just appended and wake the writer, under
        /// `mu`.
        fn queuedLocked(self: *WriteQueue, io: std.Io, accounted_bytes: usize, len: usize) EnqueueOutcome {
            self.queued_bytes += len;
            self.cond.signal(io);
            return .{
                .prev_items = self.items.items.len - 1,
                .items = self.items.items.len,
                .prev_bytes = accounted_bytes,
                .bytes = accounted_bytes + len,
            };
        }

        /// The writer is done with `item`'s dups (sent, failed or skipped):
        /// hand them to the closer and stop counting them.
        fn finishItemFds(self: *WriteQueue, io: std.Io, item: Item) void {
            if (item.fds.len == 0) return;
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);
            fd_io.closer.handOffSent(&self.fd_reservation, item.fds);
            self.in_flight_fds -= item.fds.len;
        }

        /// Snapshot of queue occupancy (queued plus in-flight bytes).
        fn stats(self: *WriteQueue, io: std.Io) QueueStats {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);
            return .{
                .items = self.items.items.len,
                .bytes = self.queued_bytes + self.in_flight_bytes,
                .max_items = self.max_items,
                .max_bytes = self.max_bytes,
                .fds = self.queued_fds + self.in_flight_fds,
                .max_fds = self.max_fds,
            };
        }

        /// Block until queued items are available or the queue is closed.
        /// Returns null when the queue is closed and empty.
        fn waitForBatch(self: *WriteQueue, io: std.Io) ?std.ArrayListUnmanaged(Item) {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);

            while (self.items.items.len == 0 and !self.closed) {
                self.cond.waitUncancelable(io, &self.mu);
            }
            if (self.items.items.len == 0) return null;

            const result = self.items;
            for (result.items) |item| {
                self.in_flight_bytes += item.bytes.len;
            }
            self.in_flight_fds += self.queued_fds;
            self.items = .empty;
            self.queued_bytes = 0;
            self.queued_fds = 0;
            return result;
        }

        /// Release byte budget for a batch after the writer has freed it.
        fn releaseBatchBytes(self: *WriteQueue, io: std.Io, batch_bytes: usize) void {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);
            self.in_flight_bytes -= batch_bytes;
        }

        /// Mark the queue as closed and wake the writer thread.
        /// Idempotent.
        fn close(self: *WriteQueue, io: std.Io) void {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);
            self.closed = true;
            self.cond.broadcast(io);
        }

        /// Free all remaining queued items and the backing storage, and
        /// hand their dups to the closer. Call only once no writer thread
        /// runs (`stopWriter` joins it first): the writer has handed off the
        /// dups of every batch it took. Idempotent — safe to call multiple
        /// times.
        fn drain(self: *WriteQueue, io: std.Io, allocator: std.mem.Allocator) void {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);
            for (self.items.items) |item| {
                fd_io.closer.handOffSent(&self.fd_reservation, item.fds);
                freeItem(allocator, item);
            }
            self.items.deinit(allocator);
            // Reinitialize to valid empty state. ArrayListUnmanaged.deinit
            // sets self.* = undefined, so a second drain would crash without
            // this reset.
            self.items = .empty;
            self.queued_bytes = 0;
            self.in_flight_bytes = 0;
            self.queued_fds = 0;
            self.in_flight_fds = 0;
            fd_io.closer.release(&self.fd_reservation);
        }

        fn freeItem(allocator: std.mem.Allocator, item: Item) void {
            if (item.fds.len != 0) allocator.free(item.fds);
            allocator.free(item.bytes);
        }
    };

    /// Create a transport wrapping the given socket handle.
    ///
    /// Allocates a read buffer of `read_buffer_size` bytes from `allocator`.
    /// The caller must later call `deinit` to free the buffer and close
    /// the socket (if not already closed).
    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        socket: SocketFd,
        read_buffer_size: usize,
    ) !Transport {
        return initWithOptions(allocator, io, socket, .{ .read_buffer_size = read_buffer_size });
    }

    /// Reads the socket family once (see "AF_UNIX sockets: drain mode" on
    /// the type). The only failure is an allocation: the read buffer, and in
    /// drain mode the drain state and its closer-queue reservation.
    pub fn initWithOptions(
        allocator: std.mem.Allocator,
        io: std.Io,
        socket: SocketFd,
        options: Options,
    ) !Transport {
        ignoreSigpipe();
        const buf = try allocator.alloc(u8, options.read_buffer_size);
        errdefer allocator.free(buf);
        var source: events.Source = .tcp;
        var drain: ?*FdDrain = null;
        if (comptime fd_io.supported) {
            switch (socketFamily(socket.handle)) {
                .ip, .not_socket => {},
                .unix => {
                    source = .unix;
                    drain = try FdDrain.create(allocator);
                },
                // recvmsg works on any stream socket, so an unreadable
                // family costs nothing but the drain state.
                .unknown => drain = try FdDrain.create(allocator),
            }
        }
        return .{
            .allocator = allocator,
            .io = io,
            .fd = socket.handle,
            .read_buf = buf,
            .observer = options.observer,
            .source = source,
            .drain = drain,
            .write_queue = .{
                .max_items = options.write_queue_max_items,
                .max_bytes = options.write_queue_max_bytes,
            },
        };
    }

    /// Release the read buffer. Stops the writer thread and closes the
    /// socket if not already closed. In drain mode a close that might
    /// dispose of unread fds happens on the closer (see the type doc), so
    /// this never blocks on it.
    pub fn deinit(self: *Transport) void {
        self.stopWriter();
        self.lockFd();
        defer self.fd_mu.unlock();
        if (!self.fd_closed.swap(true, .acq_rel)) {
            _ = self.close_requested.swap(true, .acq_rel);
            self.closeSocket();
        }
        if (self.drain) |drain| {
            if (comptime fd_io.supported) {
                if (drain.frames) |frames| {
                    // The fds of a frame never dispatched, or never taken.
                    // No events here: the observer may already be gone.
                    self.endFrames(drain, frames, .quiet);
                    self.allocator.destroy(frames);
                    drain.frames = null;
                }
            }
            drain.destroy(self.allocator);
            self.drain = null;
        }
        self.allocator.free(self.read_buf);
    }

    /// Keep the fds a peer attaches to inbound messages, at most
    /// `options.max_fds_per_message` per message, instead of closing them
    /// all (drain mode). See "Receiving fds" on the type. Call it before
    /// the first read, on the thread that reads. With
    /// `max_fds_per_message = 0` it turns fd passing off again (still
    /// before the first read).
    ///
    /// Only an AF_UNIX transport on Linux or macOS can do it. TCP and other
    /// targets return `error.FdPassingUnsupported`.
    pub fn enableFdPassing(self: *Transport, options: FdPassingOptions) EnableFdPassingError!void {
        if (comptime !fd_io.supported) return error.FdPassingUnsupported;
        if (self.source != .unix) return error.FdPassingUnsupported;
        const drain = self.drain orelse return error.FdPassingUnsupported;
        if (drain.reads_started) return error.AlreadyReading;
        const max_fds: usize = @min(options.max_fds_per_message, max_fds_per_message_cap);
        if (max_fds == 0) {
            if (drain.frames) |frames| {
                self.allocator.destroy(frames);
                drain.frames = null;
                fd_io.closer.trim(&drain.read_reservation, FdDrain.read_slots);
            }
            return;
        }
        // Room in the closer for the fds the transport may hold (the frame
        // being read and the last completed one), on top of one read's
        // worth: handing them off can then never allocate or fail.
        fd_io.closer.reserve(&drain.read_reservation, FdDrain.read_slots + FrameReader.heldSlots(max_fds)) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.UnixSocketsUnsupported => return error.FdPassingUnsupported,
        };
        const frames = drain.frames orelse blk: {
            const created = self.allocator.create(FrameReader) catch {
                fd_io.closer.trim(&drain.read_reservation, FdDrain.read_slots);
                return error.OutOfMemory;
            };
            drain.frames = created;
            break :blk created;
        };
        frames.* = .{
            .max_fds = max_fds,
            .max_frame_bytes = options.max_buffered_frame_bytes,
        };
        // A smaller cap than before keeps only what it needs.
        fd_io.closer.trim(&drain.read_reservation, drain.readSlots());
    }

    /// The most fds this transport keeps per inbound message
    /// (`enableFdPassing`): 0 in drain mode, on a socket that is not
    /// AF_UNIX, and where fd passing is not compiled in. It changes only
    /// before the first read.
    pub fn maxFdsPerMessage(self: *const Transport) u8 {
        if (comptime !fd_io.supported) return 0;
        const drain = self.drain orelse return 0;
        const frames = drain.frames orelse return 0;
        return @intCast(frames.max_fds);
    }

    /// The number of fds attached to the frame the last read completed:
    /// `takeFrameFd` takes indexes below it. 0 when fd passing is off, and
    /// once that frame's fds were released (`releaseFrameFds`, or the next
    /// read). On the thread that reads.
    pub fn frameFdCount(self: *const Transport) usize {
        if (comptime !fd_io.supported) return 0;
        const drain = self.drain orelse return 0;
        const frames = drain.frames orelse return 0;
        return frames.completed.count;
    }

    /// Take fd `index` (in the order the peer attached them) of the frame
    /// the last read completed. The caller owns it from now on and must
    /// close it; closing it off the reading thread (for example through
    /// `fd_io.closer.handOff`) keeps a close that blocks from stalling the
    /// reader. It no longer counts against the process fd budget. Null for
    /// an index out of range or already taken, and when fd passing is off.
    /// On the thread that reads, before its next read.
    pub fn takeFrameFd(self: *Transport, index: usize) ?fd_io.Fd {
        if (comptime !fd_io.supported) return null;
        const drain = self.drain orelse return null;
        const frames = drain.frames orelse return null;
        const slot = &frames.completed;
        if (index >= slot.count) return null;
        const fd = slot.fds[index];
        if (fd < 0) return null;
        slot.fds[index] = -1;
        fd_io.budget.release(1);
        return fd;
    }

    /// Hand every fd of the frame the last read completed that nobody took
    /// to the closer, with a `.resource_rejection` event
    /// (`error.AttachedFdsRejected`). `Connection` calls it once a frame
    /// is dispatched; the next read does it anyway. On the thread that
    /// reads. A no-op when fd passing is off.
    pub fn releaseFrameFds(self: *Transport) void {
        if (comptime !fd_io.supported) return;
        const drain = self.drain orelse return;
        const frames = drain.frames orelse return;
        self.releaseFds(drain, &frames.completed, .report);
    }

    /// The final close of the socket. The kernel closes any fds still
    /// riding on unread messages inside this close, and one of them may
    /// block, so in drain mode it runs inline only when nothing can be in
    /// flight (Linux, see `FdDrain.closeSocket`) and otherwise on the
    /// closer's `.socket` lane.
    fn closeSocket(self: *Transport) void {
        if (comptime fd_io.supported) {
            if (self.drain) |drain| {
                drain.closeSocket(self.io, self.fd);
                return;
            }
        }
        ioClose(self.io, self.fd);
    }

    /// Blocking read into the internal buffer. Returns the number of bytes
    /// read, or 0 on EOF or if the transport is closed.
    ///
    /// In drain mode (AF_UNIX) this waits until the socket is readable,
    /// then does one `recvmsg` with a control buffer, and every attached fd
    /// goes to the closer. Three conditions there close the connection with
    /// `error.SystemResources`, each after a `.resource_rejection` event that
    /// names the cause in `err`: the closer's `.received` lane already held
    /// as many fds that arrived uncounted (`fd_io.closer.admission`) as the
    /// process fd budget's limit when data arrived (nothing is read;
    /// `error.FdCloseQueueFull`), this read's fds pushed those past that
    /// limit (`error.FdCloseQueueFull`), or the process fd table
    /// stayed full for a read and its one retry
    /// (`error.ProcessFdQuotaExceeded` or `error.SystemFdQuotaExceeded`).
    ///
    /// With fd passing on (`enableFdPassing`) a read returns at most the
    /// rest of one part of one frame, and keeps that frame's fds (see
    /// "Receiving fds" on the type). The same three conditions end the
    /// connection, and so does a protocol error (`error.ConnectionResetByPeer`
    /// after a `.protocol_error` event).
    pub fn read(self: *Transport) ReadError!usize {
        if (self.close_requested.load(.acquire)) return 0;
        if (comptime fd_io.supported) {
            if (self.drain) |drain| return self.readDrain(drain);
        }
        var bufs: [1][]u8 = .{self.read_buf};
        return ioReadVec(self.io, self.fd, &bufs);
    }

    fn readDrain(self: *Transport, drain: *FdDrain) ReadError!usize {
        drain.reads_started = true;
        // A thread must exist before any fd can arrive: received fds are
        // never closed on this one.
        fd_io.closer.ensureStarted() catch |err| {
            log.debug("fd closer thread unavailable: {}", .{err});
            return self.failDrainRead(drain, error.SystemResources);
        };
        // Top the reservation back up, so handing this read's fds (and the
        // fds a frame holds) to the closer cannot allocate (or fail) while
        // this thread holds them.
        fd_io.closer.reserve(&drain.read_reservation, drain.readSlots()) catch
            return self.failDrainRead(drain, error.SystemResources);
        if (drain.frames) |frames| return self.readFramePart(drain, frames);

        // Wait for data first, then check the closer's bound, then take the
        // fds. A reader parked inside a blocking recvmsg would take the next
        // message's fds however full the closer is by then: an idle
        // connection would carry a check made when it went idle.
        if (!try self.waitDrainReadable()) return 0;
        const before = fd_io.closer.admission();
        if (before.full()) {
            // The closer is behind (a close that blocks). Read nothing: any
            // fds on the waiting message stay in flight in the kernel,
            // outside this process's fd table, until this socket's close
            // disposes of them (on the closer's socket lane).
            self.emitAttachedFds(before.pending, before.limit, error.FdCloseQueueFull);
            return error.SystemResources;
        }

        var fd_quota_retried = false;
        while (true) {
            const got = fd_io.recvWithFds(self.fd, self.read_buf, &drain.control, &drain.fds) catch |err| switch (err) {
                error.ProcessFdQuotaExceeded, error.SystemFdQuotaExceeded => {
                    // macOS fails the read and installs nothing. The retry
                    // returns the data, and the kernel drops the fds. A
                    // second failure closes the connection: never spin.
                    self.emitAttachedFds(null, null, err);
                    if (fd_quota_retried) return error.SystemResources;
                    fd_quota_retried = true;
                    continue;
                },
                error.ConnectionResetByPeer => return error.ConnectionResetByPeer,
                error.ConnectionTimedOut => return error.ConnectionTimedOut,
                error.SocketUnconnected => return error.SocketUnconnected,
                error.SystemResources => return error.SystemResources,
                error.Unexpected, error.UnixSocketsUnsupported => return error.Unexpected,
            };
            if (got.fd_count != 0) {
                const fds = drain.fds[0..got.fd_count];
                const admission = fd_io.closer.handOff(&drain.read_reservation, fds);
                self.emitAttachedFds(fds.len, 0, error.AttachedFdsRejected);
                if (admission.over_limit) {
                    self.emitAttachedFds(admission.pending, admission.limit, error.FdCloseQueueFull);
                    return error.SystemResources;
                }
            }
            if (got.control_truncated) {
                self.emitAttachedFds(got.fd_count, drain.fds.len, error.AttachedFdsTruncated);
            }
            return got.data_len;
        }
    }

    fn emitAttachedFds(self: *const Transport, attempted: ?usize, limit: ?usize, err: anyerror) void {
        events.emitResourceRejection(self.observer, self.source, .unknown, .attached_fds, attempted, limit, err);
    }

    /// A drain-mode read that ends the connection: with fd passing on, the
    /// fds the transport holds for frames go to the closer first.
    fn failDrainRead(self: *Transport, drain: *FdDrain, err: ReadError) ReadError {
        if (drain.frames) |frames| self.endFrames(drain, frames, .report);
        return err;
    }

    /// Fd passing: one exact-boundary read (see "Receiving fds" on the
    /// type). The closer is started and the reservation topped up.
    fn readFramePart(self: *Transport, drain: *FdDrain, frames: *FrameReader) ReadError!usize {
        // The consumer of the last completed frame is done with it.
        self.releaseFds(drain, &frames.completed, .report);
        if (frames.failure) |err| return err;
        // The one line that keeps a read inside the current part, and so
        // inside the current frame.
        const len = @min(frames.part_left, self.read_buf.len);
        if (len == 0) return 0;

        const readable = self.waitDrainReadable() catch |err| return self.failFrames(drain, frames, err);
        if (!readable) {
            // Closing: the frame being read will not complete.
            self.endFrames(drain, frames, .report);
            return 0;
        }
        const before = fd_io.closer.admission();
        if (before.full()) {
            self.emitAttachedFds(before.pending, before.limit, error.FdCloseQueueFull);
            return self.failFrames(drain, frames, error.SystemResources);
        }

        // Set once the kernel dropped this read's fds (EMFILE): the retry
        // returns the data of the same message, which keeps no fds.
        var quota_dropped = false;
        while (true) {
            const got = fd_io.recvWithFds(self.fd, self.read_buf[0..len], &drain.control, &drain.fds) catch |err| switch (err) {
                error.ProcessFdQuotaExceeded, error.SystemFdQuotaExceeded => {
                    // macOS fails the read and installs nothing; the retry
                    // returns the data, and the kernel drops the fds. A
                    // second failure ends the connection: never spin.
                    self.emitAttachedFds(null, null, err);
                    if (quota_dropped) return self.failFrames(drain, frames, error.SystemResources);
                    quota_dropped = true;
                    // Those fds were one batch of the frame being read.
                    frames.reading.batches += 1;
                    if (frames.reading.batches > 1) return self.frameViolation(drain, frames, error.MultipleAttachedFdBatches);
                    continue;
                },
                error.ConnectionResetByPeer => return self.failFrames(drain, frames, error.ConnectionResetByPeer),
                error.ConnectionTimedOut => return self.failFrames(drain, frames, error.ConnectionTimedOut),
                error.SocketUnconnected => return self.failFrames(drain, frames, error.SocketUnconnected),
                error.SystemResources => return self.failFrames(drain, frames, error.SystemResources),
                error.Unexpected, error.UnixSocketsUnsupported => return self.failFrames(drain, frames, error.Unexpected),
            };
            const fds = drain.fds[0..got.fd_count];
            if (quota_dropped) {
                // The retry after EMFILE. The frame keeps none of this
                // message's fds, even any a kernel still delivered.
                if (self.handOffRead(drain, frames, fds)) |err| return err;
            } else if (fds.len != 0 or got.control_truncated) {
                frames.reading.batches += 1;
                if (frames.reading.batches > 1) {
                    if (self.handOffRead(drain, frames, fds)) |err| return err;
                    return self.frameViolation(drain, frames, error.MultipleAttachedFdBatches);
                }
                if (got.control_truncated) {
                    // The kernel dropped some (Linux at RLIMIT_NOFILE): the
                    // frame keeps none.
                    if (self.handOffRead(drain, frames, fds)) |err| return err;
                    self.emitAttachedFds(got.fd_count, drain.fds.len, error.AttachedFdsTruncated);
                } else {
                    // The per-message cap, then the process fd budget: the
                    // frame keeps (and counts) only the fds that fit both.
                    const wanted = @min(fds.len, frames.max_fds);
                    const keep = fd_io.budget.acquireUpTo(wanted);
                    @memcpy(frames.reading.fds[0..keep], fds[0..keep]);
                    frames.reading.count = keep;
                    if (keep < fds.len) {
                        if (self.handOffRead(drain, frames, fds[keep..])) |err| return err;
                        if (wanted < fds.len) self.emitAttachedFds(fds.len, frames.max_fds, error.AttachedFdsOverLimit);
                        if (keep < wanted) {
                            // Over the budget: the frame arrives without
                            // these fds, and the connection stays.
                            self.emitAttachedFds(fd_io.budget.inUse() + (wanted - keep), fd_io.budget.limit(), error.FdBudgetExceeded);
                        }
                    }
                }
            }
            if (got.data_len == 0) {
                // End of stream: the frame being read never completes.
                self.endFrames(drain, frames, .report);
                return 0;
            }
            self.advanceFrames(drain, frames, self.read_buf[0..got.data_len]) catch |err|
                return self.frameViolation(drain, frames, err);
            return got.data_len;
        }
    }

    /// Hand fds of the current read that no frame keeps to the closer.
    /// Returns the error that ends the connection when that took the
    /// closer past its bound (as in drain mode), after the fds the
    /// transport holds went to the closer too.
    fn handOffRead(self: *Transport, drain: *FdDrain, frames: *FrameReader, fds: []const fd_io.Fd) ?ReadError {
        if (fds.len == 0) return null;
        const admission = fd_io.closer.handOff(&drain.read_reservation, fds);
        if (!admission.over_limit) return null;
        self.emitAttachedFds(admission.pending, admission.limit, error.FdCloseQueueFull);
        return self.failFrames(drain, frames, error.SystemResources);
    }

    /// A protocol error in fd-passing mode: report `cause`, give every fd
    /// the transport holds to the closer, and fail this read and every
    /// later one.
    fn frameViolation(self: *Transport, drain: *FdDrain, frames: *FrameReader, cause: anyerror) ReadError {
        log.debug("fd-passing read: protocol error {t}", .{cause});
        events.emitProtocolError(self.observer, self.source, .unknown, cause, null);
        return self.failFrames(drain, frames, error.ConnectionResetByPeer);
    }

    /// End reading for good with `err`: every fd the transport holds for
    /// frames goes to the closer, and every later read fails with `err`.
    fn failFrames(self: *Transport, drain: *FdDrain, frames: *FrameReader, err: ReadError) ReadError {
        self.endFrames(drain, frames, .report);
        frames.failure = err;
        return err;
    }

    /// Hand the fds of the frame being read and of the last completed one
    /// to the closer: neither will be dispatched.
    fn endFrames(self: *Transport, drain: *FdDrain, frames: *FrameReader, report: FdReport) void {
        self.releaseFds(drain, &frames.completed, report);
        self.releaseFds(drain, &frames.reading, report);
    }

    const FdReport = enum { report, quiet };

    /// Hand the fds in `slot` that nobody took to the closer, and empty it.
    /// They are counted in the process fd budget already; the closer gives
    /// their units back. Covered by the read reservation
    /// (`FrameReader.heldSlots`), so it cannot allocate or fail.
    fn releaseFds(self: *Transport, drain: *FdDrain, slot: *FrameFds, report: FdReport) void {
        var live: usize = 0;
        for (slot.fds[0..slot.count]) |fd| {
            if (fd < 0) continue;
            slot.fds[live] = fd;
            live += 1;
        }
        slot.count = 0;
        slot.batches = 0;
        if (live == 0) return;
        _ = fd_io.closer.handOffCounted(&drain.read_reservation, slot.fds[0..live]);
        if (report == .report) self.emitAttachedFds(live, 0, error.AttachedFdsRejected);
    }

    /// Move the part state over `bytes`, the next bytes of the stream,
    /// checking each header as it comes in: the segment count as soon as
    /// its 4 bytes are there, the frame's size when the header is complete.
    /// A read returns its bytes only after this, so a caller's framer never
    /// holds a bad segment count, nor a whole header of a frame past the
    /// limits. With the exact-boundary read `bytes` never runs past the
    /// current part; the loop still handles any split.
    fn advanceFrames(self: *Transport, drain: *FdDrain, frames: *FrameReader, bytes: []const u8) error{ InvalidFrame, FrameTooLarge }!void {
        var rest = bytes;
        while (rest.len != 0) {
            const n = @min(rest.len, frames.part_left);
            if (frames.part != .body) {
                const had = frames.header_len;
                @memcpy(frames.header[had..][0..n], rest[0..n]);
                frames.header_len += n;
                if (had < 4 and frames.header_len >= 4) try frames.checkSegmentCount();
            }
            frames.part_left -= n;
            rest = rest[n..];
            if (frames.part_left != 0) continue;
            switch (frames.part) {
                .head => try frames.endHead(),
                .segment_table => try frames.endHeader(),
                .body => {},
            }
            if (frames.part == .body and frames.part_left == 0) self.completeFrame(drain, frames);
        }
    }

    /// The last byte of a frame was read: its fds become the completed
    /// frame's, and the next frame starts.
    fn completeFrame(self: *Transport, drain: *FdDrain, frames: *FrameReader) void {
        // A consumer that read on without taking the previous frame's fds.
        self.releaseFds(drain, &frames.completed, .report);
        frames.completed = frames.reading;
        frames.reading = .{};
        frames.startFrame();
    }

    /// Drain mode: block until the socket is readable (or hung up). Returns
    /// false once the transport is closing. Where the read half of
    /// `shutdown` runs on the closer (`FdDrain.shutdown_off_thread`), the
    /// wait wakes every `FdDrain.wake_tick_ms` to look for a close request,
    /// so a reader notices `shutdown` even while that closer lane is stuck.
    fn waitDrainReadable(self: *Transport) ReadError!bool {
        var fds = [1]std.posix.pollfd{.{ .fd = self.fd, .events = std.posix.POLL.IN, .revents = 0 }};
        while (true) {
            if (self.close_requested.load(.acquire)) return false;
            const rc = std.posix.system.poll(&fds, 1, FdDrain.wake_tick_ms);
            switch (std.posix.errno(rc)) {
                .SUCCESS => if (rc != 0) return true,
                .INTR => {},
                .NOMEM => return error.SystemResources,
                else => |err| {
                    log.debug("poll failed: errno {d}", .{@backingInt(err)});
                    return error.Unexpected;
                },
            }
        }
    }

    /// Error set for `readTimeout`: a read plus the deadline's own outcome.
    pub const ReadTimeoutError = ReadError || std.Io.Timeout.Error || std.Io.ConcurrentError;

    /// Blocking read with a DEADLINE, into the internal buffer. Returns
    /// bytes read, 0 on EOF/closed, or `error.Timeout` when the deadline
    /// expires first.
    ///
    /// Use this instead of arming `SO_RCVTIMEO` on the raw fd: a timed-out
    /// `recv` returns EAGAIN, and `Io.Threaded`'s read path classifies
    /// EAGAIN as a programmer bug (`errnoBug`), so a sockopt deadline turns
    /// a normal timeout into a debug-build panic. The deadline belongs to
    /// the Io operation, not to the socket. Windows uses an owned AFD receive
    /// batch when concurrent network reads are unavailable; other backends
    /// race a cancellable read task against the deadline. Every timed path joins
    /// the read and preserves successful completions before returning.
    ///
    /// In drain mode the deadline is a `poll` before the drain-mode `read`.
    pub fn readTimeout(self: *Transport, timeout: std.Io.Timeout) ReadTimeoutError!usize {
        if (self.close_requested.load(.acquire)) return 0;
        if (comptime fd_io.supported) {
            if (self.drain) |drain| {
                try pollReadable(self.io, self.fd, timeout);
                return self.readDrain(drain);
            }
        }
        var bufs: [1][]u8 = .{self.read_buf};
        return ioReadVecTimeout(self.io, self.fd, &bufs, timeout);
    }

    /// Blocking write of all bytes. Retries partial writes until the
    /// entire buffer is sent. Used internally by the writer thread.
    pub fn write(self: *Transport, bytes: []const u8) WriteError!void {
        if (self.close_requested.load(.acquire)) return error.ConnectionResetByPeer;
        var offset: usize = 0;
        while (offset < bytes.len) {
            const n = ioWrite(self.io, self.fd, bytes[offset..]) catch |err| {
                return err;
            };
            if (n == 0) return error.ConnectionResetByPeer;
            offset += n;
        }
    }

    /// Enqueue bytes for asynchronous writing by the writer thread.
    /// Makes an owned copy of `bytes`. Called from the owner thread.
    /// The write queue itself is thread-safe via its internal mutex.
    /// If the async queue would exceed its item or byte bound, this returns
    /// `error.WriteQueueFull` or `error.WriteQueueBytesExceeded` without
    /// closing the transport.
    ///
    /// If the writer thread has not been started yet (i.e., before
    /// `startWriter()`), falls back to a synchronous blocking write.
    /// This supports the common pattern of sending initial messages
    /// (e.g., bootstrap) before entering the read loop.
    pub fn enqueueWrite(self: *Transport, bytes: []const u8) EnqueueError!void {
        if (self.writer_thread == null) {
            // Writer not started yet — write synchronously.
            self.write(bytes) catch return error.BrokenPipe;
            events.emitFrame(self.observer, self.source, .unknown, .sent, bytes.len);
            return;
        }
        const outcome = self.write_queue.enqueueCopy(self.io, self.allocator, bytes) catch |err| {
            self.emitEnqueueRejected(err, bytes.len, 0);
            return err;
        };
        self.emitEnqueued(outcome, bytes.len);
    }

    /// `enqueueWrite` for a message that carries fds: `fds` go to the peer
    /// with the first bytes of `bytes`, as one SCM_RIGHTS message. Only an
    /// AF_UNIX transport on Linux or macOS can send them; with no fds this
    /// is `enqueueWrite`.
    ///
    /// The caller keeps owning `fds` and may close them as soon as this
    /// returns: the queue holds its own dup of each (close-on-exec), and the
    /// closer thread closes the dups once the message is sent or dropped
    /// (see `fd_io.closer`, lane `.sent`). The dups count against
    /// `max_queued_fds` until then; a message whose fds would pass it is
    /// refused with `error.FdQueueFull`, and the transport stays open. They
    /// also count against the process fd budget (`fd_io.budget`) until the
    /// closer has closed them: a message whose dups do not fit is refused
    /// the same way, with a `.backpressure` event whose `err` is
    /// `error.FdBudgetExceeded` (`limit` = the budget's limit).
    ///
    /// Before `startWriter` this sends at once on the calling thread, with
    /// the caller's fds and no dups, like `enqueueWrite`. There a Linux
    /// ETOOMANYREFS refuses the message with `error.FdQueueFull` (nothing
    /// was sent) and a `.backpressure` event whose `err` is
    /// `error.TooManyFdsInFlight`. From the write queue the writer sends
    /// such a message without its fds instead (see the type doc).
    pub fn enqueueWriteWithFds(self: *Transport, bytes: []const u8, fds: []const fd_io.Fd) EnqueueFdsError!void {
        if (fds.len == 0) return self.enqueueWrite(bytes);
        if (comptime !fd_io.supported) return error.FdPassingUnsupported;
        if (self.source != .unix) return error.FdPassingUnsupported;
        if (fds.len > fd_io.max_fds_per_send) return error.TooManyFds;
        if (bytes.len == 0) return error.FdsWithoutData;
        if (self.writer_thread == null) {
            // Writer not started yet — send synchronously.
            try self.sendWithFdsNow(bytes, fds);
            events.emitFrame(self.observer, self.source, .unknown, .sent, bytes.len);
            return;
        }
        const outcome = self.write_queue.enqueueCopyWithFds(self.io, self.allocator, bytes, fds) catch |err| {
            self.emitEnqueueRejected(err, bytes.len, fds.len);
            return switch (err) {
                // The process fd budget: the same backpressure as this
                // transport's own cap; only the event tells them apart.
                error.FdBudgetExceeded => error.FdQueueFull,
                else => |e| e,
            };
        };
        self.emitEnqueued(outcome, bytes.len);
    }

    fn sendWithFdsNow(self: *Transport, bytes: []const u8, fds: []const fd_io.Fd) EnqueueFdsError!void {
        if (comptime !fd_io.supported) return error.FdPassingUnsupported;
        if (self.close_requested.load(.acquire)) return error.BrokenPipe;
        fd_io.sendWithFds(self.fd, bytes, fds) catch |err| return switch (err) {
            error.TooManyFds => error.TooManyFds,
            error.FdsWithoutData => error.FdsWithoutData,
            error.TooManyFdsInFlight => {
                self.emitFdsInFlight(fds.len);
                return error.FdQueueFull;
            },
            error.SystemResources => error.SystemResources,
            error.UnixSocketsUnsupported => error.FdPassingUnsupported,
            // Like `enqueueWrite`'s direct write: the connection is gone.
            error.BrokenPipe,
            error.ConnectionResetByPeer,
            error.SocketUnconnected,
            error.Unexpected,
            => error.BrokenPipe,
        };
    }

    /// Backpressure events for an enqueue the queue refused.
    fn emitEnqueueRejected(self: *const Transport, err: anyerror, len: usize, fd_count: usize) void {
        switch (err) {
            error.WriteQueueFull => events.emitBackpressure(
                self.observer,
                self.source,
                .unknown,
                .write_queue_items,
                len,
                self.write_queue.max_items,
                err,
            ),
            error.WriteQueueBytesExceeded => events.emitBackpressure(
                self.observer,
                self.source,
                .unknown,
                .write_queue_bytes,
                len,
                self.write_queue.max_bytes,
                err,
            ),
            error.FdQueueFull => events.emitBackpressure(
                self.observer,
                self.source,
                .unknown,
                .attached_fds,
                fd_count,
                self.write_queue.max_fds,
                err,
            ),
            error.FdBudgetExceeded => events.emitBackpressure(
                self.observer,
                self.source,
                .unknown,
                .attached_fds,
                fd_count,
                fd_io.budget.limit(),
                err,
            ),
            else => {},
        }
    }

    /// The `.backpressure` event for a Linux ETOOMANYREFS: the user already
    /// has `RLIMIT_NOFILE` fds in flight on AF_UNIX sockets (`limit` is
    /// not ours to report: null). `fd_count` = the message's fds.
    fn emitFdsInFlight(self: *const Transport, fd_count: usize) void {
        events.emitBackpressure(self.observer, self.source, .unknown, .attached_fds, fd_count, null, error.TooManyFdsInFlight);
    }

    /// Pressure and frame events for a queued message.
    fn emitEnqueued(self: *const Transport, outcome: WriteQueue.EnqueueOutcome, len: usize) void {
        events.emitPressureCrossing(
            self.observer,
            self.source,
            .unknown,
            .write_queue_items,
            outcome.prev_items,
            outcome.items,
            self.write_queue.max_items,
        );
        events.emitPressureCrossing(
            self.observer,
            self.source,
            .unknown,
            .write_queue_bytes,
            outcome.prev_bytes,
            outcome.bytes,
            self.write_queue.max_bytes,
        );
        events.emitFrame(self.observer, self.source, .unknown, .enqueued, len);
    }

    /// Point-in-time write queue occupancy. Takes the queue lock briefly;
    /// safe to call from the owner thread for metrics scraping.
    pub fn queueStats(self: *Transport) QueueStats {
        return self.write_queue.stats(self.io);
    }

    /// Spawn the dedicated writer thread. Call before entering the read loop.
    pub fn startWriter(self: *Transport) !void {
        self.writer_thread = try std.Thread.spawn(.{}, writerLoop, .{self});
    }

    /// Stop the writer thread and drain any remaining queued writes. The
    /// dups of unsent fds go to the closer. Idempotent — safe to call
    /// multiple times.
    pub fn stopWriter(self: *Transport) void {
        self.shutdown();
        if (self.writer_thread) |t| {
            t.join();
            self.writer_thread = null;
        }
        self.write_queue.drain(self.io, self.allocator);
    }

    /// Writer thread entry point. Blocks on the condition variable,
    /// drains the write queue in batches, and performs blocking writes, one
    /// item per send. Exits when the queue is closed or on write error.
    fn writerLoop(self: *Transport) void {
        while (true) {
            var batch = self.write_queue.waitForBatch(self.io) orelse break;
            var batch_bytes: usize = 0;
            for (batch.items) |item| {
                batch_bytes += item.bytes.len;
            }
            defer self.write_queue.releaseBatchBytes(self.io, batch_bytes);
            defer batch.deinit(self.allocator);

            var write_failed = false;
            for (batch.items) |item| {
                if (!write_failed) {
                    self.writeItem(item) catch |err| {
                        log.debug("writer thread write error: {}", .{err});
                        self.shutdown();
                        write_failed = true;
                    };
                    if (!write_failed) {
                        events.emitFrame(self.observer, self.source, .unknown, .sent, item.bytes.len);
                    }
                }
                // Sent, failed or skipped after a failure: the dups go to
                // the closer either way. A sent message holds the kernel's
                // own reference to each file.
                self.write_queue.finishItemFds(self.io, item);
                WriteQueue.freeItem(self.allocator, item);
            }
            if (write_failed) break;
        }
    }

    /// One queued item: a plain write, or one `sendWithFds` for an item
    /// that carries fds. A Linux ETOOMANYREFS sent nothing: the item goes
    /// again without its fds, after a backpressure event (see the type doc).
    fn writeItem(self: *Transport, item: WriteQueue.Item) (WriteError || fd_io.SendError)!void {
        if (item.fds.len == 0) return self.write(item.bytes);
        // Only `enqueueWriteWithFds` queues fds, and it refuses them there.
        if (comptime !fd_io.supported) return error.UnixSocketsUnsupported;
        if (self.close_requested.load(.acquire)) return error.ConnectionResetByPeer;
        fd_io.sendWithFds(self.fd, item.bytes, item.fds) catch |err| switch (err) {
            error.TooManyFdsInFlight => {
                self.emitFdsInFlight(item.fds.len);
                return fd_io.sendWithFds(self.fd, item.bytes, &.{});
            },
            else => return err,
        };
    }

    /// Shut down the socket for both reading and writing without closing
    /// the file descriptor. This unblocks any thread currently blocked in
    /// `read()` or `write()` on this socket. Safe to call from any thread.
    ///
    /// In drain mode on macOS the read half runs on the closer's `.socket`
    /// lane (see the type doc). A blocked reader wakes when that lane gets
    /// to it, at once unless it is stuck in a blocking close; then the
    /// reader notices on its next poll tick (250 ms).
    ///
    /// Also closes the write queue so the writer thread will exit.
    /// The owning thread should subsequently call `deinit()`.
    pub fn shutdown(self: *Transport) void {
        const already_closing = self.close_requested.swap(true, .acq_rel);
        self.write_queue.close(self.io);
        if (already_closing) return;

        self.lockFd();
        defer self.fd_mu.unlock();
        if (!self.fd_closed.load(.acquire)) {
            if (comptime fd_io.supported) {
                if (self.drain) |drain| {
                    drain.shutdownSocket(self.io, self.fd);
                    return;
                }
            }
            ioShutdown(self.io, self.fd);
        }
    }

    /// Shut down the socket and signal the writer to stop. Idempotent.
    /// The fd is not closed here — `deinit()` closes it after the writer
    /// thread has been joined.
    pub fn close(self: *Transport) void {
        const was_closing = self.close_requested.load(.acquire);
        if (!was_closing) {
            log.debug("transport close requested", .{});
        }
        self.shutdown();
    }

    /// Returns `true` if `close()` or `shutdown()` has been called.
    pub fn isClosing(self: *const Transport) bool {
        return self.close_requested.load(.acquire);
    }

    fn lockFd(self: *Transport) void {
        while (!self.fd_mu.tryLock()) std.atomic.spinLoopHint();
    }
};

/// Drain-mode state of one `Transport` (see "AF_UNIX sockets: drain mode" on
/// `Transport`). Created only where `fd_io.supported`.
const FdDrain = struct {
    control: [control_bytes]u8 align(8) = undefined,
    fds: [fd_io.max_fds_per_read]fd_io.Fd = undefined,
    /// Closer-queue capacity held for this transport, one reservation per
    /// thread that may use it, so a hand-off never allocates: one read's
    /// worth of fds in the `.received` lane (the reader thread; topped up
    /// before each read), and in the `.socket` lane the socket shutdown
    /// (`Transport.shutdown`, from any thread, once) and the socket close
    /// (`deinit`, once).
    read_reservation: fd_io.closer.Reservation = .{ .lane = .received },
    shutdown_reservation: fd_io.closer.Reservation = .{ .lane = .socket },
    close_reservation: fd_io.closer.Reservation = .{ .lane = .socket },
    /// Fd-passing state (`Transport.enableFdPassing`); null in drain mode.
    /// Owned by the transport.
    frames: ?*FrameReader = null,
    /// Set by the first read: fd passing can no longer start.
    reads_started: bool = false,

    const control_bytes = fd_io.controlSpace(fd_io.max_fds_per_read);
    const read_slots = fd_io.max_fds_per_read;

    /// The `.received` slots the reader keeps reserved: one read's worth,
    /// plus with fd passing on the fds the transport may hold for frames.
    fn readSlots(self: *const FdDrain) usize {
        const frames = self.frames orelse return read_slots;
        return read_slots + FrameReader.heldSlots(frames.max_fds);
    }

    /// Darwin disposes of the fds on unread messages inside
    /// `shutdown(SHUT_RD)`, on the calling thread, so the read half of the
    /// shutdown goes to the closer there. The write half stays inline: it
    /// cannot block, and it is what wakes a writer thread parked in a full
    /// send buffer (`deinit` joins that thread). Linux disposes of nothing
    /// in `shutdown` (measured: 0 ms), and an inline shutdown wakes a
    /// blocked reader at once.
    const shutdown_off_thread = builtin.target.os.tag.isDarwin();

    /// How often a drain-mode reader waiting for data looks for a close
    /// request (`Transport.waitDrainReadable`). Only where the read half of
    /// `shutdown` runs off-thread: that job can wait behind a `.socket` job
    /// that blocks, and the tick bounds how late the reader notices. -1
    /// (no tick) elsewhere: an inline shutdown wakes the reader at once.
    const wake_tick_ms: i32 = if (shutdown_off_thread) 250 else -1;

    /// Linux only: after `shutdown(SHUT_RD)` a peer can queue nothing more
    /// on an AF_UNIX stream socket (its send fails with EPIPE), and fds
    /// never travel without data bytes there (a zero-byte send queues
    /// nothing), so a receive queue that is empty then (`SIOCINQ` 0) holds no
    /// fds to dispose of, and the final close cannot block. Darwin cannot
    /// freeze the queue that way: its `shutdown(SHUT_RD)` disposes of the
    /// queued fds inline, and without it the peer's next send can land
    /// between an emptiness check and the close.
    const close_inline_when_empty = builtin.target.os.tag == .linux;

    fn create(allocator: std.mem.Allocator) error{OutOfMemory}!*FdDrain {
        const drain = try allocator.create(FdDrain);
        errdefer allocator.destroy(drain);
        drain.* = .{};
        errdefer drain.releaseAll();
        try reserve(&drain.read_reservation, read_slots);
        try reserve(&drain.shutdown_reservation, 1);
        try reserve(&drain.close_reservation, 1);
        return drain;
    }

    fn reserve(r: *fd_io.closer.Reservation, slots: usize) error{OutOfMemory}!void {
        fd_io.closer.reserve(r, slots) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            // Never on a target that creates drain state.
            error.UnixSocketsUnsupported => {},
        };
    }

    fn shutdownSocket(self: *FdDrain, io: std.Io, socket: net.Socket.Handle) void {
        if (comptime shutdown_off_thread) {
            io.vtable.netShutdown(io.userdata, socket, .send) catch {};
            fd_io.closer.handOffShutdown(&self.shutdown_reservation, socket);
        } else {
            ioShutdown(io, socket);
        }
    }

    /// The final close of the transport's socket. Inline only when nothing
    /// can be in flight on it (`close_inline_when_empty`); otherwise on the
    /// closer's `.socket` lane, away from the fds peers attached, so a
    /// received fd whose close blocks never holds this socket open.
    fn closeSocket(self: *FdDrain, io: std.Io, socket: net.Socket.Handle) void {
        if (comptime close_inline_when_empty) {
            if (receiveQueueSettledEmpty(socket)) {
                ioClose(io, socket);
                return;
            }
        }
        fd_io.closer.handOffSocketClose(&self.close_reservation, socket);
    }

    /// See `close_inline_when_empty`. Any failure answers false (the close
    /// then goes to the closer).
    fn receiveQueueSettledEmpty(socket: net.Socket.Handle) bool {
        const posix = std.posix;
        switch (posix.errno(posix.system.shutdown(socket, posix.SHUT.RD))) {
            // ENOTCONN: no peer, so nothing can arrive either.
            .SUCCESS, .NOTCONN => {},
            else => return false,
        }
        var unread: c_int = 0;
        const rc = if (builtin.link_libc)
            std.c.ioctl(socket, posix.T.FIONREAD, &unread)
        else
            std.os.linux.ioctl(socket, posix.T.FIONREAD, @intFromPtr(&unread));
        return posix.errno(rc) == .SUCCESS and unread == 0;
    }

    fn releaseAll(self: *FdDrain) void {
        fd_io.closer.release(&self.read_reservation);
        fd_io.closer.release(&self.shutdown_reservation);
        fd_io.closer.release(&self.close_reservation);
    }

    fn destroy(self: *FdDrain, allocator: std.mem.Allocator) void {
        self.releaseAll();
        allocator.destroy(self);
    }
};

/// The fds a peer attached to one frame (fd passing).
const FrameFds = struct {
    /// `fds[0..count]`, in the order the peer attached them. A slot whose
    /// fd was taken (`Transport.takeFrameFd`) holds -1.
    fds: [Transport.max_fds_per_message_cap]fd_io.Fd = undefined,
    count: usize = 0,
    /// `recvmsg` calls that brought fds for the frame, or whose fds the
    /// kernel dropped. More than one is a protocol error.
    batches: usize = 0,
};

/// Fd-passing state of one `Transport` (see "Receiving fds" on
/// `Transport`): where the exact-boundary reader is in the current frame,
/// and the fds of that frame and of the last completed one. Created by
/// `Transport.enableFdPassing`. Only the reading thread uses it, and
/// `deinit` after reading has stopped.
const FrameReader = struct {
    /// The most fds a frame keeps: 1 to `Transport.max_fds_per_message_cap`.
    max_fds: usize,
    /// Header plus body, in bytes, the largest frame read.
    max_frame_bytes: usize,
    part: Part = .head,
    /// Bytes of the current part still to read. Never 0 between reads.
    part_left: usize = head_len,
    /// The current frame's header so far: `header[0..header_len]`.
    header: [framing.Framer.max_header_bytes]u8 = undefined,
    header_len: usize = 0,
    /// The fds of the frame being read.
    reading: FrameFds = .{},
    /// The fds of the frame the last read completed, until taken or
    /// released.
    completed: FrameFds = .{},
    /// Set when reading ended for good (a protocol error, or the closer
    /// past its bound): every later read fails with it.
    failure: ?Transport.ReadError = null,

    /// A frame in three parts, each read without crossing its end.
    const Part = enum {
        /// The first 8 bytes: the segment count and the first segment's
        /// size. Every frame has them.
        head,
        /// The other segment sizes and the padding word, if any.
        segment_table,
        body,
    };

    const head_len = 8;

    /// Closer slots for the fds the transport may hold at once: the frame
    /// being read and the last completed one.
    fn heldSlots(max_fds: usize) usize {
        return 2 * max_fds;
    }

    fn startFrame(self: *FrameReader) void {
        self.part = .head;
        self.part_left = head_len;
        self.header_len = 0;
    }

    fn segmentCount(self: *const FrameReader) error{InvalidFrame}!usize {
        const raw = std.mem.readInt(u32, self.header[0..4], .little);
        const count = std.math.add(u32, raw, 1) catch return error.InvalidFrame;
        if (count > framing.Framer.max_segment_count) return error.InvalidFrame;
        return count;
    }

    /// The segment count's 4 bytes are in: check it (the frozen `Framer`'s
    /// rule) and the size of the header it implies.
    fn checkSegmentCount(self: *const FrameReader) error{ InvalidFrame, FrameTooLarge }!void {
        if (self.headerBytes(try self.segmentCount()) > self.max_frame_bytes) return error.FrameTooLarge;
    }

    /// Header bytes for `count` segments: the count, the sizes and the
    /// padding word that keeps the header a whole number of words.
    fn headerBytes(_: *const FrameReader, count: usize) usize {
        const padding: usize = if (count % 2 == 0) 4 else 0;
        return 4 + 4 * count + padding;
    }

    /// The head is in (its segment count already checked): read the rest
    /// of the segment table, or go on to the body.
    fn endHead(self: *FrameReader) error{ InvalidFrame, FrameTooLarge }!void {
        const header_bytes = self.headerBytes(try self.segmentCount());
        if (header_bytes > head_len) {
            self.part = .segment_table;
            self.part_left = header_bytes - head_len;
            return;
        }
        return self.endHeader();
    }

    /// The whole header is in: check the frame's size against the frozen
    /// `Framer`'s word limit and `max_frame_bytes`, before any byte of the
    /// body is read. Sets up the body (possibly empty).
    fn endHeader(self: *FrameReader) error{ InvalidFrame, FrameTooLarge }!void {
        const count = try self.segmentCount();
        var words: usize = 0;
        for (0..count) |i| {
            const size = std.mem.readInt(u32, self.header[4 + 4 * i ..][0..4], .little);
            words = std.math.add(usize, words, size) catch return error.InvalidFrame;
        }
        if (words > framing.Framer.max_frame_words) return error.FrameTooLarge;
        const body_bytes = std.math.mul(usize, words, 8) catch return error.InvalidFrame;
        const frame_bytes = std.math.add(usize, self.header_len, body_bytes) catch return error.InvalidFrame;
        if (frame_bytes > self.max_frame_bytes) return error.FrameTooLarge;
        self.part = .body;
        self.part_left = body_bytes;
    }
};

const SocketFamily = enum {
    ip,
    unix,
    /// A socket whose family `getsockname` did not report. Read in drain
    /// mode: `recvmsg` works on any stream socket.
    unknown,
    /// Not a socket, or not an open fd (ENOTSOCK, EBADF). `recvmsg` cannot
    /// read it, so it keeps the plain `std.Io` path; only a test's fake `Io`
    /// hands the transport such a handle.
    not_socket,
};

/// The address family of a socket, from `getsockname`.
fn socketFamily(handle: net.Socket.Handle) SocketFamily {
    var addr: std.posix.sockaddr.storage = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.storage);
    const rc = std.posix.system.getsockname(handle, @ptrCast(&addr), &len);
    switch (std.posix.errno(rc)) {
        .SUCCESS => {},
        .NOTSOCK, .BADF => return .not_socket,
        else => return .unknown,
    }
    const family_end = @offsetOf(std.posix.sockaddr.storage, "family") + @sizeOf(std.posix.sa_family_t);
    if (len < family_end) return .unknown;
    return switch (addr.family) {
        std.posix.AF.INET, std.posix.AF.INET6 => .ip,
        std.posix.AF.UNIX => .unix,
        else => .unknown,
    };
}

/// Wait with a deadline until `handle` is readable or hung up (drain-mode
/// `readTimeout`). The socket stays blocking; the deadline is the poll's.
fn pollReadable(io: std.Io, handle: net.Socket.Handle, timeout: std.Io.Timeout) Transport.ReadTimeoutError!void {
    const deadline = timeout.toDeadline(io);
    var fds = [1]std.posix.pollfd{.{ .fd = handle, .events = std.posix.POLL.IN, .revents = 0 }};
    while (true) {
        const remaining = deadline.toDurationFromNow(io) orelse return;
        const ns = remaining.raw.nanoseconds;
        if (ns <= 0) return error.Timeout;
        const ms_wanted = @divFloor(ns + std.time.ns_per_ms - 1, std.time.ns_per_ms);
        const ms: i32 = @intCast(@min(ms_wanted, std.math.maxInt(i32)));
        const rc = std.posix.system.poll(&fds, 1, ms);
        switch (std.posix.errno(rc)) {
            .SUCCESS => if (rc != 0) return,
            .INTR => {},
            .NOMEM => return error.SystemResources,
            else => |err| {
                log.debug("poll failed: errno {d}", .{@backingInt(err)});
                return error.Unexpected;
            },
        }
    }
}

/// Ignore SIGPIPE process-wide so that writing to a broken TCP connection
/// returns EPIPE instead of killing the process. Called from Transport.init;
/// the atomic guard ensures the sigaction call happens at most once.
/// This is standard practice for TCP servers/clients.
fn ignoreSigpipe() void {
    if (comptime builtin.target.os.tag == .windows or builtin.target.os.tag == .freestanding) return;
    const static = struct {
        var done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);
    };
    if (static.done.load(.acquire)) return;
    if (static.done.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return;
    const act = std.posix.Sigaction{
        .handler = .{ .handler = std.posix.SIG.IGN },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.PIPE, &act, null);
}

/// Write bytes to a socket handle via Io. Returns bytes written.
///
/// Version-adaptive: zig moved socket writes off the `Io.VTable` and onto
/// the `Operation` union (`netWrite` vtable entry deleted; `net_write`
/// operation added) around 0.17.0-dev.1786. Selecting on the OPERATION's
/// presence — not a version number — keeps one source tree building on
/// both sides of that move, which is what a downstream pinned to a
/// different dev snapshot actually needs. Same shape as `ioReadVec`'s
/// existing `net.Stream.read` preference. Both arms land in
/// `WriteError` exactly: `Stream.Writer.Error = NetWrite.Error ||
/// Cancelable`, and `operate` contributes the `Cancelable` half.
fn ioWrite(io: std.Io, fd: net.Socket.Handle, bytes: []const u8) Transport.WriteError!usize {
    if (bytes.len == 0) return 0;
    const pattern: []const u8 = &.{};
    const data: [1][]const u8 = .{pattern};
    if (comptime @hasField(std.Io.Operation, "net_write")) {
        const result = try io.operate(.{ .net_write = .{
            .socket_handle = fd,
            .header = bytes,
            .data = &data,
            .splat = 0,
        } });
        return result.net_write;
    }
    return io.vtable.netWrite(io.userdata, fd, bytes, &data, 0);
}

/// Submits the `net_read` operation directly rather than calling
/// `net.Stream.read`: at 0.17.0 that std wrapper destructures the
/// operation's new `ReadResult` struct as a tuple, so it stops compiling
/// the moment anything references it.
fn ioReadVec(io: std.Io, fd: net.Socket.Handle, bufs: [][]u8) Transport.ReadError!usize {
    if (comptime @hasField(std.Io.Operation, "net_read")) {
        const result = try io.operate(.{ .net_read = .{
            .socket_handle = fd,
            .data = bufs,
        } });
        return netReadLen(try result.net_read);
    }
    return io.vtable.netRead(io.userdata, fd, bufs);
}

/// Byte count of a completed `net_read`. zig 0.17.0 widened the result from
/// a bare `usize` to `net.Stream.ReadResult` (data plus control lengths).
/// Selecting on the result TYPE, as `ioWrite` selects on the operation,
/// keeps one source tree building on both shapes.
fn netReadLen(result: anytype) usize {
    return if (@TypeOf(result) == usize) result else result.data_len;
}

/// Shut down a socket for both reading and writing via Io. Ignores errors.
fn ioShutdown(io: std.Io, fd: net.Socket.Handle) void {
    io.vtable.netShutdown(io.userdata, fd, .both) catch {};
}

/// Close a socket handle via Io.
fn ioClose(io: std.Io, fd: net.Socket.Handle) void {
    // See runtime.closeFd: netClose now takes `[]const net.Socket`.
    const sockets = [_]net.Socket{.{ .handle = fd, .address = undefined }};
    io.vtable.netClose(io.userdata, &sockets);
}

/// Create a connected loopback TCP pair for tests. Unlike socketpair(2), this
/// works under the same std.Io socket implementation on Windows.
fn createSocketPair() ![2]net.Socket.Handle {
    const io = std.testing.io;
    const listen_addr: net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try net.IpAddress.listen(&listen_addr, io, .{
        .kernel_backlog = 1,
        .reuse_address = true,
    });
    defer server.socket.close(io);

    var connect_addr = server.socket.address;
    const client = try net.IpAddress.connect(&connect_addr, io, .{
        .mode = .stream,
        .protocol = .tcp,
    });
    errdefer client.socket.close(io);

    const accepted = try server.accept(io);
    return .{ client.socket.handle, accepted.socket.handle };
}

/// Vectored read with a deadline, via the Io operation's own timeout.
///
/// Prefer the backend's batched operation support. The pinned Windows
/// Threaded backend cannot submit concurrent net_read batches, so use an
/// owned AFD receive batch there to preserve completions racing cancellation.
fn ioReadVecTimeout(
    io: std.Io,
    fd: net.Socket.Handle,
    bufs: [][]u8,
    timeout: std.Io.Timeout,
) Transport.ReadTimeoutError!usize {
    if (timeout == .none) return ioReadVec(io, fd, bufs);
    const deadline = timeout.toDeadline(io);
    var storage: [1]std.Io.Operation.Storage = undefined;
    var batch: std.Io.Batch = .init(&storage);
    defer batch.cancel(io);
    batch.addAt(0, .{ .net_read = .{
        .socket_handle = fd,
        .data = bufs,
    } });
    batch.awaitConcurrent(io, deadline) catch |err| {
        // The pinned Windows backend releases a rejected net_read slot but
        // leaves its submitted head pointing at that now-unused slot. This
        // batch owns one operation: repair only that fully inactive state,
        // preserving backend userdata for the normal cancellation cleanup.
        if (err == error.ConcurrencyUnavailable and batch.unused.head != .none and
            batch.pending.head == .none and batch.completed.head == .none)
        {
            batch.submitted = .empty;
        }
        // An await error may leave pending work or completed reads. Join it
        // before reusing the buffer and preserve bytes already consumed.
        batch.cancel(io);
        if (batch.next()) |completion| return netReadLen(try completion.result.net_read);
        return switch (err) {
            error.ConcurrencyUnavailable => if (comptime builtin.os.tag == .windows)
                ioReadVecWindowsTimeout(io, fd, bufs, deadline)
            else
                ioReadVecTaskTimeout(io, fd, bufs, deadline),
            else => err,
        };
    };
    const completion = batch.next() orelse return error.Unexpected;
    return netReadLen(try completion.result.net_read);
}

fn ioReadVecWindowsTimeout(io: std.Io, fd: net.Socket.Handle, bufs: [][]u8, deadline: std.Io.Timeout) Transport.ReadTimeoutError!usize {
    const windows = std.os.windows;
    var vectors: [std.Io.Threaded.max_iovecs_len]windows.AFD.WSABUF(.@"var") = undefined;
    var vector_count: u32 = 0;
    buffers: for (bufs) |buf| {
        var remaining = buf;
        while (remaining.len != 0) {
            if (vector_count == vectors.len) break :buffers;
            const len: u32 = @intCast(@min(remaining.len, std.math.maxInt(u32)));
            vectors[vector_count] = .{ .buf = remaining.ptr, .len = len };
            vector_count += 1;
            remaining = remaining[len..];
        }
    }
    if (vector_count == 0) return 0;
    const receive: windows.AFD.RECV_INFO = .{
        .BufferArray = &vectors,
        .BufferCount = vector_count,
        .AfdFlags = .{ .NO_FAST_IO = true, .OVERLAPPED = true },
        .TdiFlags = .{ .NORMAL = true },
    };
    var storage: [1]std.Io.Operation.Storage = undefined;
    var batch: std.Io.Batch = .init(&storage);
    defer cancelWindowsReceive(io, &batch);
    batch.addAt(0, .{ .device_io_control = .{
        .file = .{ .handle = fd, .flags = .{ .nonblocking = true } },
        .code = windows.IOCTL.AFD.RECEIVE,
        .in = std.mem.asBytes(&receive),
    } });
    batch.awaitConcurrent(io, deadline) catch |err| {
        // Ordinary Windows netRead can return Canceled after AFD consumed
        // bytes. Batch cancellation instead retains the final successful
        // IOSB, and keeps receive/vectors alive until the APC has completed.
        cancelWindowsReceive(io, &batch);
        if (batch.next()) |completion| return windowsReadResult(completion.result.device_io_control);
        return err;
    };
    const completion = batch.next() orelse return error.Unexpected;
    return windowsReadResult(completion.result.device_io_control);
}

fn cancelWindowsReceive(io: std.Io, batch: *std.Io.Batch) void {
    if (batch.pending.head != .none) {
        // Pinned Threaded.batchCancel first waits indefinitely for an APC or
        // alert, before issuing NtCancelIoFileEx. A silent receive has neither
        // after its deadline. Wake that initial wait on the submitting thread;
        // cancellation still targets this batch and joins its final APCs.
        const windows = std.os.windows;
        const status = windows.ntdll.NtAlertThread(windows.GetCurrentThread());
        if (status != .SUCCESS) std.debug.panic("cannot wake Windows receive cancellation: NTSTATUS=0x{x}", .{@backingInt(status)});
    }
    batch.cancel(io);
}

fn windowsReadResult(iosb: std.os.windows.IO_STATUS_BLOCK) Transport.ReadError!usize {
    return switch (iosb.u.Status) {
        .SUCCESS => iosb.Information,
        .CANCELLED => error.Canceled,
        .INSUFFICIENT_RESOURCES => error.SystemResources,
        .CONNECTION_RESET => error.ConnectionResetByPeer,
        else => |status| std.os.windows.unexpectedStatus(status),
    };
}

fn ioReadVecTaskTimeout(io: std.Io, fd: net.Socket.Handle, bufs: [][]u8, deadline: std.Io.Timeout) Transport.ReadTimeoutError!usize {
    const Result = union(enum) {
        read: Transport.ReadError!usize,
        deadline: std.Io.Cancelable!void,
    };
    var completed: [2]Result = undefined;
    var tasks = std.Io.Select(Result).init(io, &completed);
    defer tasks.cancelDiscard();
    // Start the timer first: failure to assign either task must not leave a
    // read consuming bytes without a deadline or a caller to receive them.
    try tasks.concurrent(.deadline, std.Io.Timeout.sleep, .{ deadline, io });
    try tasks.concurrent(.read, ioReadVec, .{ io, fd, bufs });
    const selected = tasks.await() catch |err| {
        // Caller cancellation can race a successful read too. Join while the
        // result queue remains open so consumed bytes still reach the caller.
        while (tasks.cancel()) |pending| switch (pending) {
            .read => |read| return read,
            .deadline => {},
        };
        return err;
    };
    switch (selected) {
        .read => |result| return result,
        .deadline => |result| {
            try result;
            // Cancellation races completion. Preserve bytes (or EOF) if the
            // read finished before its cancellation was observed.
            while (tasks.cancel()) |pending| switch (pending) {
                .read => |read| return read catch |err| switch (err) {
                    error.Canceled => return error.Timeout,
                    else => return err,
                },
                .deadline => {},
            };
            return error.Timeout;
        },
    }
}

/// Read from a socket handle via Io into a buffer.
fn ioRead(io: std.Io, fd: net.Socket.Handle, buf: []u8) Transport.ReadError!usize {
    var bufs: [1][]u8 = .{buf};
    return ioReadVec(io, fd, &bufs);
}

test "transport init and deinit" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    try std.testing.expect(!transport.isClosing());
    transport.deinit();
}

test "transport read returns data written to peer" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    // Write data into the other end of the socketpair.
    _ = try ioWrite(std.testing.io, pair[1], "hello");
    const n = try transport.read();
    try std.testing.expectEqualStrings("hello", transport.read_buf[0..n]);
}

test "transport read returns 0 after close" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    transport.close();
    try std.testing.expect(transport.isClosing());
    const n = try transport.read();
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "transport write sends data to peer" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    try transport.write("world");

    var buf: [64]u8 = undefined;
    const n = try ioRead(std.testing.io, pair[1], &buf);
    try std.testing.expectEqualStrings("world", buf[0..n]);
}

test "transport write after close returns error" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    transport.close();
    try std.testing.expectError(error.ConnectionResetByPeer, transport.write("fail"));
}

test "transport read returns 0 on peer close (EOF)" {
    const pair = try createSocketPair();

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    // Close the peer end — transport should see EOF.
    ioClose(std.testing.io, pair[1]);
    const n = try transport.read();
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "transport isClosing tracks close state" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    try std.testing.expect(!transport.isClosing());
    transport.shutdown();
    try std.testing.expect(transport.isClosing());
}

test "transport close is idempotent" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    transport.close();
    transport.close(); // should not panic or double-close
    try std.testing.expect(transport.isClosing());
}

test "transport shutdown then deinit does not double-close" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    transport.shutdown();
    transport.deinit(); // should close fd and free buffer without error
}

test "transport shutdown after deinit observes closed fd guard" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    transport.deinit();
    transport.shutdown();
    try std.testing.expect(transport.isClosing());
}

test "transport enqueue write delivers data to peer" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    try transport.startWriter();

    try transport.enqueueWrite("hello");

    // Read from the peer end — the writer thread should have delivered it.
    var buf: [64]u8 = undefined;
    const n = try ioRead(std.testing.io, pair[1], &buf);
    try std.testing.expectEqualStrings("hello", buf[0..n]);

    transport.stopWriter();
}

test "transport enqueue write delivers multiple frames in order" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    try transport.startWriter();

    try transport.enqueueWrite("aaa");
    try transport.enqueueWrite("bbb");
    try transport.enqueueWrite("ccc");

    // Read all data from the peer end.
    var buf: [64]u8 = undefined;
    var total: usize = 0;
    while (total < 9) {
        const n = try ioRead(std.testing.io, pair[1], buf[total..]);
        if (n == 0) break;
        total += n;
    }
    try std.testing.expectEqualStrings("aaabbbccc", buf[0..total]);

    transport.stopWriter();
}

test "transport enqueue write after close returns error" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    transport.close();
    try std.testing.expectError(error.BrokenPipe, transport.enqueueWrite("fail"));
}
