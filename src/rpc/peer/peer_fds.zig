//! Fd passing in the peer (Experimental, Linux and macOS): the side tables
//! that tie file descriptors to capabilities (`CapDescriptor.attachedFd`,
//! `rpc.capnp:1112-1167`).
//!
//! They live here, outside the frozen cap table (`caps/table.zig`, whose
//! encoders, `InboundCapTable.init`, `ImportCap` and `CapTable` fields are
//! Stable), and `caps/table.zig` does not re-export this file. No root
//! exports it either: the API is `Peer.setExportFd`, `Peer.clearExportFd`,
//! `Peer.importFd` and `Peer.setMaxLiveImportedFds`.
//!
//! ## Outbound
//!
//! `Peer.setExportFd` attaches an fd to one of the peer's exports. The fd
//! stays borrowed: the app keeps it open while the export has it, and the
//! transport sends a dup of it. After a Call or Return is encoded, a pass
//! here walks the built cap table: each `senderHosted` descriptor whose
//! export has an fd gets `attachedFd` = the fd's index in the frame's fd
//! list, and the frame goes out with that list. A Resolve to such an export,
//! and the Bootstrap Return for a bootstrap export with an fd, carry it the
//! same way. The rules:
//! - only when the transport binding can carry fds (`outboundFdLimit`: 0 for
//!   TCP and QUIC, so their descriptors keep `attachedFd = 0xff`), and never
//!   through a `send_frame_override`. On an AF_UNIX connection it needs no
//!   option: a peer that does not take fds closes them (drain mode), as the
//!   spec expects of a receiver that did not ask (`rpc.capnp:1118-1124`);
//! - at most `max_fds_per_message_cap` (253) fds per frame; later
//!   descriptors go without one;
//! - each fd index on at most one descriptor (`rpc.capnp:1149-1150`): two
//!   exports with the same fd send it twice.
//! Frames that never reach the wire carry none: loopback calls and returns,
//! the self-loopback results frame, the source frame of an automatic
//! third-party route (relayed through the target peer's own encode), and
//! prebuilt (forwarded) Return frames. Fds through proxies are deferred.
//!
//! ## Inbound
//!
//! Each frame owns its fds: the transport holds them while the peer
//! dispatches it (`TransportBinding.take_frame_fd`) and closes every one
//! nobody took once dispatch returns. After `InboundCapTable.init` succeeds
//! for the frame's payload (Call params, Return results), and after a
//! Resolve's descriptor is resolved, a pass reads each descriptor's
//! `attachedFd` again:
//! - `senderHosted` and `senderPromise`: the fd moves into this table,
//!   owned by the import, only while that import has no fd. On a duplicate
//!   index the first descriptor wins (the second finds the fd taken); a
//!   second fd for the same import, in this frame or a later one, stays
//!   with the transport, which closes it.
//! - `thirdPartyHosted`, `receiverHosted` and `receiverAnswer`: never
//!   attached (`rpc.capnp:1137-1146` makes the third-party case optional; it
//!   is deferred), so the transport closes the fd.
//! - Past `max_live_imports` fds, the fd stays with the transport and a
//!   `.resource_rejection` event (`.attached_fds`,
//!   `error.ImportedFdsOverLimit`) reports it.
//! Only the transport frame being dispatched can give fds: a frame the peer
//! replays or delivers to itself (a stashed loopback Return, a buffered
//! third-party Return, a loopback Call) is a different decoded message, and
//! the pass skips it.
//!
//! ## Ownership and close
//!
//! An imported fd belongs to its import and is closed when the import
//! leaves `CapTable.imports`: the peer's release paths (`releaseImport`,
//! the inbound-cap release, the handoff unpin, the promise-pin release) and
//! `Peer.deinit` call `importReleased`, which hands the fd to the closer
//! thread (`fd_closer`, `.received` lane) when the import is gone. A
//! reservation in that lane covers every fd held here, so that hand-off
//! never allocates and never closes inline. In Debug builds every release
//! checks that each id in the table is still an import.
//!
//! `Peer.importFd` lends the fd: it stays valid until the import is
//! released (C++ `ClientHook::getFd`, `capability.h:278-288`). A promise
//! import gives null until it resolves, then the fd of what it resolved to.

const std = @import("std");
const builtin = @import("builtin");
const events = @import("../events.zig");
const message = @import("../../serialization/message.zig");
const protocol = @import("../wire/protocol.zig");
const fd_passing = @import("../transport/fd_passing.zig");
const closer = @import("../transport/unix/fd_closer.zig");
const peer_export_release = @import("./peer_export_release.zig");

comptime {
    std.debug.assert(closer.supported == fd_passing.supported);
}

pub const FdHandle = fd_passing.FdHandle;
const Fd = closer.Fd;

/// `Peer.setExportFd` failures. On each, nothing changed.
pub const SetExportFdError = error{
    /// The peer has no export with this id.
    UnknownExport,
    /// The fd is negative.
    InvalidFd,
    /// Fd passing is not compiled in for this target (only Linux and macOS
    /// have it).
    FdPassingUnsupported,
    OutOfMemory,
};

/// The built cap table could not be read back.
pub const AttachError = error{InvalidCapTable};

/// How many resolution steps `importFd` follows (a promise that resolved to
/// a promise that resolved ...).
const max_resolution_hops: usize = 16;

const ImportedFd = struct {
    fd: Fd,
    /// The import came as a `senderPromise`: `importFd` gives null for it.
    promise: bool,
};

/// The peer's fd side tables (`Peer.fds`).
pub const State = struct {
    /// Export id -> borrowed fd (the app's).
    exports: std.AutoHashMapUnmanaged(u32, Fd) = .empty,
    /// Import id -> owned fd. Every id is in `CapTable.imports`.
    imports: std.AutoHashMapUnmanaged(u32, ImportedFd) = .empty,
    /// `.received`-lane capacity for every fd in `imports`
    /// (`reservation.slots == imports.count()`).
    reservation: closer.Reservation = .{ .lane = .received },
    /// The most fds `imports` holds (`Peer.setMaxLiveImportedFds`).
    max_live_imports: u32 = fd_passing.default_max_live_imported_fds,
    /// The decoded transport frame being dispatched, while `Peer.handleFrame`
    /// runs; null otherwise and inside a loopback dispatch.
    inbound_msg: ?*const message.Message = null,
};

/// The fds one outbound frame carries, in `attachedFd` order.
pub const OutboundFds = struct {
    handles: [fd_passing.max_fds_per_message_cap]FdHandle = undefined,
    len: u8 = 0,

    pub fn slice(self: *const OutboundFds) []const FdHandle {
        return self.handles[0..self.len];
    }

    fn push(self: *OutboundFds, fd: Fd) u8 {
        const index = self.len;
        self.handles[index] = .{ .fd = fd };
        self.len += 1;
        return index;
    }
};

pub fn PeerFds(comptime Peer: type) type {
    return struct {
        const Send = peer_export_release.ExportRelease(Peer);

        // -- App API ----------------------------------------------------------

        pub fn setExportFd(peer: *Peer, export_id: u32, handle: FdHandle) SetExportFdError!void {
            if (comptime !fd_passing.supported) return error.FdPassingUnsupported;
            if (handle.fd < 0) return error.InvalidFd;
            if (!peer.exports.contains(export_id)) return error.UnknownExport;
            try peer.fds.exports.put(peer.allocator, export_id, handle.fd);
            debugCheck(peer);
        }

        pub fn clearExportFd(peer: *Peer, export_id: u32) void {
            _ = peer.fds.exports.remove(export_id);
        }

        pub fn importFd(peer: *const Peer, import_id: u32) ?FdHandle {
            if (comptime !fd_passing.supported) return null;
            var id = import_id;
            var hops: usize = 0;
            while (hops < max_resolution_hops) : (hops += 1) {
                if (peer.resolved_imports.get(id)) |resolved| {
                    // A promise that resolved: the fd of what it resolved
                    // to. Not while its embargo holds calls back.
                    if (resolved.embargoed) return null;
                    const cap = resolved.cap orelse return null;
                    switch (cap) {
                        .imported => |next| {
                            id = next.id;
                            continue;
                        },
                        .exported => |local| {
                            const fd = peer.fds.exports.get(local.id) orelse return null;
                            return .{ .fd = fd };
                        },
                        else => return null,
                    }
                }
                const entry = peer.fds.imports.get(id) orelse return null;
                // An unresolved promise: it may resolve to another
                // capability with another fd.
                if (entry.promise) return null;
                return .{ .fd = entry.fd };
            }
            return null;
        }

        pub fn setMaxLiveImportedFds(peer: *Peer, limit: u32) void {
            peer.fds.max_live_imports = limit;
        }

        // -- Outbound ---------------------------------------------------------

        /// The most fds one outbound frame may carry right now: 0 without a
        /// binding that carries fds, and with a `send_frame_override`.
        fn outboundLimit(peer: *const Peer) u8 {
            if (comptime !fd_passing.supported) return 0;
            if (peer.send_frame_override != null) return 0;
            return peer.transport.outboundFdLimit();
        }

        /// The post-encode pass: set `attachedFd` on every `senderHosted`
        /// descriptor of `payload`'s cap table whose export has an fd, and
        /// collect those fds in `out`.
        pub fn attachPayload(peer: *Peer, payload: ?protocol.PayloadBuilder, out: *OutboundFds) AttachError!void {
            if (comptime !fd_passing.supported) return;
            const limit = outboundLimit(peer);
            if (limit == 0 or peer.fds.exports.count() == 0) return;
            var builder = payload orelse return;
            if (!builder.hasCapTable()) return;
            const list = builder.getCapTable() catch return error.InvalidCapTable;
            var index: u32 = 0;
            while (index < list.len() and out.len < limit) : (index += 1) {
                var descriptor = list.get(index) catch return error.InvalidCapTable;
                const tag = descriptor.which() catch continue;
                if (tag != .senderHosted) continue;
                const export_id = descriptor.getSenderHosted() catch continue;
                const fd = peer.fds.exports.get(export_id) orelse continue;
                try descriptor.setAttachedFd(out.push(fd));
            }
        }

        /// The fd index a lone descriptor for export `export_id` carries
        /// (`tag` is the descriptor's: a Resolve's, or the Bootstrap
        /// Return's), with the fd pushed onto `out`; null for none.
        pub fn attachExport(peer: *Peer, tag: protocol.CapDescriptorTag, export_id: u32, out: *OutboundFds) ?u8 {
            if (comptime !fd_passing.supported) return null;
            if (tag != .senderHosted) return null;
            if (out.len >= outboundLimit(peer)) return null;
            const fd = peer.fds.exports.get(export_id) orelse return null;
            return out.push(fd);
        }

        /// The Bootstrap Return frame for `question_id`, as
        /// `bootstrap.buildBootstrapReturnFrame` builds it, when the
        /// bootstrap export has an fd to carry: then its descriptor names fd
        /// 0, which is pushed onto `out`. Null when there is no fd (send the
        /// prebuilt frame). The caller owns the frame.
        pub fn bootstrapReturnWithFd(peer: *Peer, question_id: u32, export_id: u32, out: *OutboundFds) !?[]const u8 {
            const index = attachExport(peer, .senderHosted, export_id, out) orelse return null;
            errdefer out.len = 0;
            return try buildBootstrapReturn(peer.allocator, question_id, export_id, index);
        }

        fn buildBootstrapReturn(allocator: std.mem.Allocator, question_id: u32, export_id: u32, fd_index: u8) ![]const u8 {
            var builder = protocol.MessageBuilder.init(allocator);
            defer builder.deinit();
            var ret = try builder.beginReturn(question_id, .results);
            var payload = try ret.payloadTyped();
            var any = try payload.initContent();
            try any.setCapability(.{ .id = 0 });
            var cap_list = try ret.initCapTableTyped(1);
            var entry = try cap_list.get(0);
            try entry.setSenderHosted(export_id);
            try entry.setAttachedFd(fd_index);
            return builder.finish();
        }

        /// The `send_call` hook of `call/peer_call_sender.zig` for a real
        /// peer: the post-encode pass on the call's params, then the send
        /// with the frame's fds.
        pub fn sendCall(peer: *Peer, builder: *protocol.MessageBuilder, call: *protocol.CallBuilder) anyerror!void {
            var fds: OutboundFds = .{};
            try attachPayload(peer, call.payload, &fds);
            const bytes = try builder.finish();
            defer peer.allocator.free(bytes);
            try Send.sendFrameWithFds(peer, bytes, fds.slice());
        }

        // -- Inbound ----------------------------------------------------------

        /// After `InboundCapTable.init` succeeded for `list` (a payload's cap
        /// table): move the fds of its `senderHosted` and `senderPromise`
        /// descriptors into the import table. Only for the transport frame
        /// being dispatched. Best effort: an fd it does not take stays with
        /// the transport, which closes it.
        pub fn adoptPayload(peer: *Peer, list_opt: ?message.StructListReader) void {
            if (comptime !fd_passing.supported) return;
            const list = list_opt orelse return;
            const inbound = peer.fds.inbound_msg orelse return;
            if (list.message != inbound) return;
            if (peer.transport.take_frame_fd == null) return;
            var index: u32 = 0;
            while (index < list.len()) : (index += 1) {
                const reader = list.get(index) catch return;
                const descriptor = protocol.CapDescriptor.fromReader(reader) catch continue;
                adoptDescriptor(peer, descriptor);
            }
        }

        /// After an inbound Resolve's descriptor was resolved: the same for
        /// its one descriptor. `handleResolve` runs only from the dispatch
        /// of the frame that carried it, so `inbound_msg` tells whether that
        /// frame came from the transport.
        pub fn adoptResolve(peer: *Peer, descriptor: protocol.CapDescriptor) void {
            if (comptime !fd_passing.supported) return;
            if (peer.fds.inbound_msg == null) return;
            if (peer.transport.take_frame_fd == null) return;
            adoptDescriptor(peer, descriptor);
        }

        fn adoptDescriptor(peer: *Peer, descriptor: protocol.CapDescriptor) void {
            const index = descriptor.attached_fd orelse return;
            switch (descriptor.tag) {
                .senderHosted, .senderPromise => {},
                // Never attached here; the transport closes the fd.
                .thirdPartyHosted, .receiverHosted, .receiverAnswer, .none => return,
            }
            const import_id = descriptor.id orelse return;
            const promise = descriptor.tag == .senderPromise;
            if (!peer.caps.hasImport(import_id)) return;
            // The first fd wins (`rpc.capnp:1137-1146`).
            if (peer.fds.imports.contains(import_id)) return;
            const live = peer.fds.imports.count();
            if (live >= peer.fds.max_live_imports) {
                events.emitResourceRejection(
                    peer.observer,
                    .peer,
                    .unknown,
                    .attached_fds,
                    live + 1,
                    peer.fds.max_live_imports,
                    error.ImportedFdsOverLimit,
                );
                return;
            }
            // Room first, so taking the fd cannot fail half way: a closer
            // slot for its eventual hand-off, and the map entry.
            closer.reserve(&peer.fds.reservation, live + 1) catch return;
            peer.fds.imports.ensureUnusedCapacity(peer.allocator, 1) catch {
                closer.trim(&peer.fds.reservation, live);
                return;
            };
            const handle = peer.transport.takeFrameFd(index) orelse {
                closer.trim(&peer.fds.reservation, live);
                return;
            };
            peer.fds.imports.putAssumeCapacityNoClobber(import_id, .{ .fd = handle.fd, .promise = promise });
        }

        // -- Release ----------------------------------------------------------

        /// Call after anything that may have removed import `import_id` from
        /// `CapTable.imports`: once it is gone, its fd goes to the closer.
        pub fn importReleased(peer: *Peer, import_id: u32) void {
            if (comptime !fd_passing.supported) return;
            if (!peer.caps.hasImport(import_id)) {
                if (peer.fds.imports.fetchRemove(import_id)) |removed| {
                    _ = closer.handOff(&peer.fds.reservation, &.{removed.value.fd});
                }
            }
            debugCheck(peer);
        }

        /// Call after export `export_id` left the export table: forget its
        /// (borrowed) fd, so an export that reuses the id starts without one.
        pub fn exportRemoved(peer: *Peer, export_id: u32) void {
            _ = peer.fds.exports.remove(export_id);
        }

        /// `Peer.deinit`: every imported fd goes to the closer.
        pub fn deinit(peer: *Peer) void {
            if (comptime fd_passing.supported) {
                var batch: [64]Fd = undefined;
                var n: usize = 0;
                var it = peer.fds.imports.valueIterator();
                while (it.next()) |entry| {
                    batch[n] = entry.fd;
                    n += 1;
                    if (n == batch.len) {
                        _ = closer.handOff(&peer.fds.reservation, batch[0..n]);
                        n = 0;
                    }
                }
                if (n != 0) _ = closer.handOff(&peer.fds.reservation, batch[0..n]);
                closer.release(&peer.fds.reservation);
            }
            peer.fds.imports.deinit(peer.allocator);
            peer.fds.exports.deinit(peer.allocator);
            peer.fds.imports = .empty;
            peer.fds.exports = .empty;
        }

        /// Debug builds: every id in the import table is still an import,
        /// and every id in the export table still an export. Skipped inside
        /// `Peer.deinit`, which frees the export table before the imports.
        fn debugCheck(peer: *const Peer) void {
            if (builtin.mode != .debug or peer.in_deinit) return;
            var imports = peer.fds.imports.keyIterator();
            while (imports.next()) |id| std.debug.assert(peer.caps.hasImport(id.*));
            var exports = peer.fds.exports.keyIterator();
            while (exports.next()) |id| std.debug.assert(peer.exports.contains(id.*));
            std.debug.assert(peer.fds.reservation.slots == peer.fds.imports.count() or !fd_passing.supported);
        }
    };
}
