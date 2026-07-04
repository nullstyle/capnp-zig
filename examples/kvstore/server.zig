const std = @import("std");
const capnpc = @import("capnpc-zig");
const store_mod = @import("store.zig");
const kvstore = @import("gen/kvstore.zig");

const message = capnpc.message;
const rpc = capnpc.rpc;
const Store = store_mod.Store;
const BackupInfo = kvstore.BackupInfo;
const KvStore = kvstore.KvStore;
const KvClientNotifier = kvstore.KvClientNotifier;

const Allocator = std.mem.Allocator;
var server_is_quiet: bool = false;

pub const std_options: std.Options = .{
    .logFn = serverLog,
};

fn serverLog(
    comptime level: std.log.Level,
    comptime scope: @EnumLiteral(),
    comptime format: []const u8,
    args: anytype,
) void {
    if (server_is_quiet and (level == .debug or level == .info)) return;
    std.log.defaultLog(level, scope, format, args);
}

const Limits = struct {
    const max_key_bytes: usize = 1024;
    const max_value_bytes: usize = 64 * 1024;
    const max_batch_ops: u32 = 256;
    const max_watch_keys: u32 = 512;
    const max_list_limit: u32 = 1024;
    const max_pending_notifications: usize = 1024;
};

const NotifyChange = union(enum) {
    put: struct {
        key: []const u8,
        value: []const u8,
        version: u64,
    },
    delete: struct {
        key: []const u8,
        found: bool,
    },
};

var g_service: ?*KvService = null;

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

fn validateKey(key: []const u8) !void {
    if (key.len > Limits.max_key_bytes) return error.KeyTooLarge;
}

fn validateValue(value: []const u8) !void {
    if (value.len > Limits.max_value_bytes) return error.ValueTooLarge;
}

fn validateListLimit(limit: u32) !void {
    if (limit > Limits.max_list_limit) return error.ListLimitTooLarge;
}

fn watchedKeysContain(watched_keys: []const []u8, candidate: []const u8) bool {
    for (watched_keys) |watched| {
        if (std.mem.eql(u8, watched, candidate)) return true;
    }
    return false;
}

fn notifyChangeKey(change: NotifyChange) []const u8 {
    return switch (change) {
        .put => |put| put.key,
        .delete => |del| del.key,
    };
}

fn notifyChangesIntersectWatched(watched_keys: []const []u8, changes: []const NotifyChange) bool {
    for (changes) |change| {
        if (watchedKeysContain(watched_keys, notifyChangeKey(change))) return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// KV service state
// ---------------------------------------------------------------------------

const PendingNotification = union(enum) {
    keys_changed: struct {
        /// Owned copies of keys and values (already filtered to watched keys).
        changes: []OwnedNotifyChange,
    },
    state_reset: struct {
        restored_backup_id: u32,
        next_version: u64,
    },
};

const OwnedNotifyChange = union(enum) {
    put: struct { key: []u8, value: []u8, version: u64 },
    delete: struct { key: []u8, found: bool },

    fn deinit(self: OwnedNotifyChange, allocator: Allocator) void {
        switch (self) {
            .put => |p| {
                allocator.free(p.key);
                allocator.free(p.value);
            },
            .delete => |d| allocator.free(d.key),
        }
    }
};

const KvService = struct {
    const Subscriber = struct {
        peer: *rpc.peer.Peer,
        conn: *rpc.transport.tcp.Connection,
        notifier: KvClientNotifier.Client,
        watched_keys: std.ArrayListUnmanaged([]u8) = .empty,
        /// Pending notifications queued from other worker threads.
        /// Protected by `pending_mu`.
        pending: std.ArrayListUnmanaged(PendingNotification) = .empty,
        pending_mu: std.atomic.Mutex = .unlocked,
    };

    allocator: Allocator,
    store: Store,
    server: KvStore.Server,
    subscribers: std.ArrayListUnmanaged(Subscriber),
    /// Protects all mutable state when accessed from multiple worker threads.
    mu: std.atomic.Mutex = .unlocked,

    fn init(allocator: Allocator, io: std.Io, db_path: []const u8, backup_dir: []const u8) !KvService {
        return .{
            .allocator = allocator,
            .store = try Store.open(allocator, io, db_path, backup_dir),
            .server = undefined,
            .subscribers = .empty,
        };
    }

    /// Must be called after the KvService is at its final memory location.
    fn bind(self: *KvService) void {
        self.server = .{
            .ctx = self,
            .vtable = .{
                .get = handleGet,
                .writeBatch = handleWriteBatch,
                .list = handleList,
                .subscribe = handleSubscribe,
                .setWatchedKeys = handleSetWatchedKeys,
                .createBackup = handleCreateBackup,
                .listBackups = handleListBackups,
                .restoreFromBackup = handleRestoreFromBackup,
            },
        };
    }

    fn deinit(self: *KvService) void {
        for (self.subscribers.items) |*subscriber| {
            self.clearWatchedKeys(&subscriber.watched_keys);
            subscriber.watched_keys.deinit(self.allocator);
            self.drainAndFreePending(&subscriber.pending);
            subscriber.pending.deinit(self.allocator);
        }
        self.subscribers.deinit(self.allocator);
        self.store.deinit();
    }

    fn addOrUpdateSubscriber(self: *KvService, peer: *rpc.peer.Peer, conn: *rpc.transport.tcp.Connection, notifier: KvClientNotifier.Client) !void {
        for (self.subscribers.items) |*subscriber| {
            if (subscriber.peer == peer) {
                subscriber.notifier = notifier;
                subscriber.conn = conn;
                return;
            }
        }

        try self.subscribers.append(self.allocator, .{
            .peer = peer,
            .conn = conn,
            .notifier = notifier,
        });
    }

    fn clearWatchedKeys(self: *KvService, watched_keys: *std.ArrayListUnmanaged([]u8)) void {
        for (watched_keys.items) |key| {
            self.allocator.free(key);
        }
        watched_keys.clearRetainingCapacity();
    }

    fn findSubscriber(self: *KvService, peer: *rpc.peer.Peer) ?*Subscriber {
        for (self.subscribers.items) |*subscriber| {
            if (subscriber.peer == peer) return subscriber;
        }
        return null;
    }

    fn setSubscriberWatchedKeys(self: *KvService, peer: *rpc.peer.Peer, watched_keys: message.TextListReader) !u32 {
        const subscriber = self.findSubscriber(peer) orelse return error.NotSubscribed;

        if (watched_keys.len() > Limits.max_watch_keys) return error.TooManyWatchedKeys;
        for (0..watched_keys.len()) |idx_usize| {
            const idx: u32 = @intCast(idx_usize);
            const key = try watched_keys.get(idx);
            if (key.len == 0) continue;
            try validateKey(key);
        }

        self.clearWatchedKeys(&subscriber.watched_keys);

        const count: u32 = watched_keys.len();
        for (0..count) |idx_usize| {
            const idx: u32 = @intCast(idx_usize);
            const key = try watched_keys.get(idx);
            if (key.len == 0) continue;

            var duplicate = false;
            for (subscriber.watched_keys.items) |existing| {
                if (std.mem.eql(u8, existing, key)) {
                    duplicate = true;
                    break;
                }
            }
            if (duplicate) continue;

            const owned = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned);
            try subscriber.watched_keys.append(self.allocator, owned);
        }

        return @intCast(subscriber.watched_keys.items.len);
    }

    fn removeSubscriber(self: *KvService, peer: *rpc.peer.Peer) void {
        var idx: usize = 0;
        while (idx < self.subscribers.items.len) {
            if (self.subscribers.items[idx].peer == peer) {
                var removed = self.subscribers.swapRemove(idx);
                self.clearWatchedKeys(&removed.watched_keys);
                removed.watched_keys.deinit(self.allocator);
                self.drainAndFreePending(&removed.pending);
                removed.pending.deinit(self.allocator);
            } else {
                idx += 1;
            }
        }
    }

    fn drainAndFreePending(self: *KvService, pending: *std.ArrayListUnmanaged(PendingNotification)) void {
        for (pending.items) |*notif| {
            switch (notif.*) {
                .keys_changed => |kc| {
                    for (kc.changes) |c| c.deinit(self.allocator);
                    self.allocator.free(kc.changes);
                },
                .state_reset => {},
            }
        }
        pending.clearRetainingCapacity();
    }

    /// Queue notifications for subscribers and wake their connections.
    /// The actual send happens on the subscriber's own worker thread
    /// via the connection's on_wake callback.
    fn notifySubscribers(self: *KvService, source_peer: *rpc.peer.Peer, changes: []const NotifyChange) void {
        if (changes.len == 0 or self.subscribers.items.len == 0) return;

        for (self.subscribers.items) |*subscriber| {
            if (subscriber.peer == source_peer) continue;

            const watched_keys = subscriber.watched_keys.items;
            if (!notifyChangesIntersectWatched(watched_keys, changes)) continue;

            while (!subscriber.pending_mu.tryLock()) {}
            const pending_full = subscriber.pending.items.len >= Limits.max_pending_notifications;
            subscriber.pending_mu.unlock();
            if (pending_full) {
                std.log.warn("dropping notification for subscriber: pending queue limit reached ({d})", .{Limits.max_pending_notifications});
                continue;
            }

            // Build owned copies of the relevant (filtered) changes.
            const owned_changes = self.buildOwnedKeysChanged(changes, watched_keys) catch |err| {
                std.log.warn("failed to build notification: {s}", .{@errorName(err)});
                continue;
            };

            // Push to subscriber's queue (thread-safe via spinlock).
            while (!subscriber.pending_mu.tryLock()) {}
            if (subscriber.pending.items.len >= Limits.max_pending_notifications) {
                subscriber.pending_mu.unlock();
                std.log.warn("dropping notification for subscriber: pending queue limit reached ({d})", .{Limits.max_pending_notifications});
                for (owned_changes) |c| c.deinit(self.allocator);
                self.allocator.free(owned_changes);
                continue;
            }
            subscriber.pending.append(self.allocator, .{
                .keys_changed = .{ .changes = owned_changes },
            }) catch |err| {
                subscriber.pending_mu.unlock();
                std.log.warn("failed to queue notification: {s}", .{@errorName(err)});
                for (owned_changes) |c| c.deinit(self.allocator);
                self.allocator.free(owned_changes);
                continue;
            };
            subscriber.pending_mu.unlock();

            // Wake the subscriber's connection so it drains the queue.
            subscriber.conn.wake();
        }
    }

    fn buildOwnedKeysChanged(
        self: *KvService,
        changes: []const NotifyChange,
        watched_keys: []const []u8,
    ) ![]OwnedNotifyChange {
        // Count matching changes.
        var count: usize = 0;
        for (changes) |change| {
            if (watchedKeysContain(watched_keys, notifyChangeKey(change))) count += 1;
        }

        const owned_changes = try self.allocator.alloc(OwnedNotifyChange, count);
        errdefer self.allocator.free(owned_changes);

        var allocated: usize = 0;
        errdefer for (owned_changes[0..allocated]) |c| c.deinit(self.allocator);

        for (changes) |change| {
            if (!watchedKeysContain(watched_keys, notifyChangeKey(change))) continue;
            owned_changes[allocated] = switch (change) {
                .put => |p| .{ .put = .{
                    .key = try self.allocator.dupe(u8, p.key),
                    .value = try self.allocator.dupe(u8, p.value),
                    .version = p.version,
                } },
                .delete => |d| .{ .delete = .{
                    .key = try self.allocator.dupe(u8, d.key),
                    .found = d.found,
                } },
            };
            allocated += 1;
        }

        return owned_changes;
    }

    /// Queue state-reset notifications for subscribers and wake their connections.
    fn notifyStateResetSubscribers(self: *KvService, source_peer: *rpc.peer.Peer, restored_backup_id: u32, next_version: u64) void {
        if (self.subscribers.items.len == 0) return;

        for (self.subscribers.items) |*subscriber| {
            if (subscriber.peer == source_peer) continue;

            // Push to subscriber's queue (thread-safe via spinlock).
            while (!subscriber.pending_mu.tryLock()) {}
            if (subscriber.pending.items.len >= Limits.max_pending_notifications) {
                subscriber.pending_mu.unlock();
                std.log.warn("dropping state reset notification for subscriber: pending queue limit reached ({d})", .{Limits.max_pending_notifications});
                continue;
            }
            subscriber.pending.append(self.allocator, .{
                .state_reset = .{
                    .restored_backup_id = restored_backup_id,
                    .next_version = next_version,
                },
            }) catch |err| {
                subscriber.pending_mu.unlock();
                std.log.warn("failed to queue state reset notification: {s}", .{@errorName(err)});
                continue;
            };
            subscriber.pending_mu.unlock();

            // Wake the subscriber's connection so it drains the queue.
            subscriber.conn.wake();
        }
    }
};

// ---------------------------------------------------------------------------
// Wake-pipe notification support
// ---------------------------------------------------------------------------

/// Context for building a keysChanged notification from owned data.
const WakeKeysChangedCtx = struct {
    changes: []OwnedNotifyChange,
};

/// Context for building a stateResetRequired notification.
const WakeStateResetCtx = struct {
    restored_backup_id: u32,
    next_version: u64,
};

fn buildWakeKeysChanged(
    ctx_ptr: *anyopaque,
    params: *KvClientNotifier.KeysChanged.Params.Builder,
) anyerror!void {
    const ctx: *const WakeKeysChangedCtx = @ptrCast(@alignCast(ctx_ptr));
    const count: u32 = @intCast(ctx.changes.len);
    var out_changes = try params.initChanges(count);
    for (ctx.changes, 0..) |change, idx| {
        var out_change = try out_changes.get(@intCast(idx));
        switch (change) {
            .put => |put| {
                try out_change.setKey(put.key);
                var out_entry = try out_change.initPut();
                try out_entry.setKey(put.key);
                try out_entry.setValue(put.value);
                try out_entry.setVersion(put.version);
            },
            .delete => |del| {
                try out_change.setKey(del.key);
                try out_change.setDelete(del.found);
            },
        }
    }
}

fn buildWakeStateReset(
    ctx_ptr: *anyopaque,
    params: *KvClientNotifier.StateResetRequired.Params.Builder,
) anyerror!void {
    const ctx: *const WakeStateResetCtx = @ptrCast(@alignCast(ctx_ptr));
    try params.setRestoredBackupId(ctx.restored_backup_id);
    try params.setNextVersion(ctx.next_version);
}

fn onWakeKeysChangedReturn(
    _: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvClientNotifier.KeysChanged.Response,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    _ = response.unwrap() catch |err| {
        std.log.debug("client notifier returned {s}", .{@errorName(err)});
        return;
    };
}

fn onWakeStateResetReturn(
    _: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvClientNotifier.StateResetRequired.Response,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    _ = response.unwrap() catch |err| {
        std.log.debug("client reset notifier returned {s}", .{@errorName(err)});
        return;
    };
}

/// Called on the connection's own thread when woken by another thread.
/// Drains the subscriber's pending notification queue and sends each
/// notification via the Peer (which is thread-affine to this thread).
fn onConnectionWake(conn: *rpc.transport.tcp.Connection) void {
    const svc = g_service orelse return;

    // Find the subscriber for this connection under the service lock.
    while (!svc.mu.tryLock()) {}

    var sub_ptr: ?*KvService.Subscriber = null;
    for (svc.subscribers.items) |*sub| {
        if (sub.conn == conn) {
            sub_ptr = sub;
            break;
        }
    }

    const sub = sub_ptr orelse {
        svc.mu.unlock();
        return;
    };

    // Drain pending queue under the subscriber's spinlock.
    while (!sub.pending_mu.tryLock()) {}
    const pending = svc.allocator.dupe(PendingNotification, sub.pending.items) catch {
        sub.pending_mu.unlock();
        svc.mu.unlock();
        return;
    };
    sub.pending.clearRetainingCapacity();
    sub.pending_mu.unlock();

    // Copy notifier reference so we can release the service lock before I/O.
    const notifier = sub.notifier;
    svc.mu.unlock();

    // Process each queued notification on this connection's thread.
    defer svc.allocator.free(pending);
    std.log.debug("draining {d} pending notification(s)", .{pending.len});
    for (pending) |notif| {
        switch (notif) {
            .keys_changed => |kc| {
                var ctx = WakeKeysChangedCtx{ .changes = kc.changes };
                _ = notifier.callKeysChanged(
                    @ptrCast(&ctx),
                    buildWakeKeysChanged,
                    onWakeKeysChangedReturn,
                ) catch |err| {
                    std.log.warn("failed to send keysChanged: {s}", .{@errorName(err)});
                };
                for (kc.changes) |c| c.deinit(svc.allocator);
                svc.allocator.free(kc.changes);
            },
            .state_reset => |sr| {
                var ctx = WakeStateResetCtx{
                    .restored_backup_id = sr.restored_backup_id,
                    .next_version = sr.next_version,
                };
                _ = notifier.callStateResetRequired(
                    @ptrCast(&ctx),
                    buildWakeStateReset,
                    onWakeStateResetReturn,
                ) catch |err| {
                    std.log.warn("failed to send stateResetRequired: {s}", .{@errorName(err)});
                };
            },
        }
    }
}

// ---------------------------------------------------------------------------
// RPC handlers
// ---------------------------------------------------------------------------

fn handleGet(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: KvStore.Get.Params.Reader,
    results: *KvStore.Get.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const key = try params.getKey();
    try validateKey(key);
    std.log.debug("GET key_bytes={d}", .{key.len});

    if (svc.store.get(key)) |record| {
        var out_entry = try results.initEntry();
        try out_entry.setKey(key);
        try out_entry.setValue(record.value);
        try out_entry.setVersion(record.version);
        try results.setFound(true);
    } else {
        try results.setFound(false);
    }
}

fn handleWriteBatch(
    ctx_ptr: *anyopaque,
    caller_peer: *rpc.peer.Peer,
    params: KvStore.WriteBatch.Params.Reader,
    results: *KvStore.WriteBatch.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const ops = try params.getOps();
    const op_count: u32 = ops.len();
    std.log.debug("WRITE BATCH ops={d}", .{op_count});
    if (op_count > Limits.max_batch_ops) return error.TooManyBatchOps;

    if (op_count == 0) {
        _ = try results.initResults(0);
        try results.setApplied(0);
        try results.setNextVersion(svc.store.nextVersion());
        return;
    }

    // Translate the inbound ops into store batch ops. The key/value slices
    // borrow the inbound Cap'n Proto message, which outlives this handler.
    const batch_ops = try svc.allocator.alloc(Store.BatchOp, op_count);
    defer svc.allocator.free(batch_ops);
    const outcomes = try svc.allocator.alloc(Store.BatchOutcome, op_count);
    defer svc.allocator.free(outcomes);

    for (0..op_count) |idx| {
        const op = try ops.get(@intCast(idx));
        const key = try op.getKey();
        try validateKey(key);
        switch (try op.which()) {
            .put => {
                const value = try op.getPut();
                try validateValue(value);
                batch_ops[idx] = .{ .put = .{ .key = key, .value = value } };
            },
            .delete => batch_ops[idx] = .{ .delete = key },
        }
    }

    try svc.store.applyBatch(batch_ops, outcomes);

    // Build the response list and the notification set from the applied ops.
    var notify_changes = std.ArrayListUnmanaged(NotifyChange).empty;
    defer notify_changes.deinit(svc.allocator);

    var out_results = try results.initResults(op_count);
    for (0..op_count) |idx| {
        var out = try out_results.get(@intCast(idx));
        switch (batch_ops[idx]) {
            .put => |p| {
                try out.setKey(p.key);
                var out_entry = try out.initPut();
                try out_entry.setKey(p.key);
                try out_entry.setValue(p.value);
                try out_entry.setVersion(outcomes[idx].put.version);
                try notify_changes.append(svc.allocator, .{ .put = .{
                    .key = p.key,
                    .value = p.value,
                    .version = outcomes[idx].put.version,
                } });
            },
            .delete => |key| {
                try out.setKey(key);
                try out.setDelete(outcomes[idx].delete.found);
                try notify_changes.append(svc.allocator, .{ .delete = .{
                    .key = key,
                    .found = outcomes[idx].delete.found,
                } });
            },
        }
    }

    try results.setApplied(op_count);
    try results.setNextVersion(svc.store.nextVersion());

    svc.notifySubscribers(caller_peer, notify_changes.items);
}

fn handleList(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: KvStore.List.Params.Reader,
    results: *KvStore.List.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const prefix = try params.getPrefix();
    const limit = try params.getLimit();
    try validateKey(prefix);
    try validateListLimit(limit);
    std.log.debug("LIST prefix_bytes={d} limit={d}", .{ prefix.len, limit });

    if (limit == 0) {
        _ = try results.initEntries(0);
        return;
    }

    const entries = try svc.store.list(svc.allocator, prefix, limit);
    defer svc.allocator.free(entries);

    var out_entries = try results.initEntries(@intCast(entries.len));
    for (entries, 0..) |entry, idx| {
        var out = try out_entries.get(@intCast(idx));
        try out.setKey(entry.key);
        try out.setValue(entry.value);
        try out.setVersion(entry.version);
    }
}

fn handleSubscribe(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: KvStore.Subscribe.Params.Reader,
    _: *KvStore.Subscribe.Results.Builder,
    caps: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const notifier = try params.resolveNotifier(peer, caps);
    const conn = peer.getAttachedConnection(*rpc.transport.tcp.Connection) orelse return error.NoPeerConnection;
    try svc.addOrUpdateSubscriber(peer, conn, notifier);
    std.log.debug("SUBSCRIBE", .{});
}

fn handleSetWatchedKeys(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: KvStore.SetWatchedKeys.Params.Reader,
    _: *KvStore.SetWatchedKeys.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const watched_keys = try params.getKeys();
    const count = try svc.setSubscriberWatchedKeys(peer, watched_keys);
    std.log.debug("SET WATCHED KEYS count={d}", .{count});
}

fn writeBackupInfo(builder: *BackupInfo.Builder, record: Store.BackupRecord) !void {
    try builder.setBackupId(record.backup_id);
    try builder.setTimestamp(record.timestamp);
    try builder.setSize(record.size);
    try builder.setNumFiles(record.num_files);
}

fn handleCreateBackup(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: KvStore.CreateBackup.Params.Reader,
    results: *KvStore.CreateBackup.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    // The store always flushes its log before snapshotting, so the
    // flushBeforeBackup request flag is accepted but has no additional effect.
    _ = try params.getFlushBeforeBackup();
    const outcome = try svc.store.createBackup();

    var backup = try results.initBackup();
    try writeBackupInfo(&backup, outcome.record);
    try results.setBackupCount(outcome.count);
}

fn handleListBackups(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: KvStore.ListBackups.Params.Reader,
    results: *KvStore.ListBackups.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const records = try svc.store.listBackups(svc.allocator);
    defer svc.allocator.free(records);

    var backups = try results.initBackups(@intCast(records.len));
    for (records, 0..) |record, idx| {
        var backup = try backups.get(@intCast(idx));
        try writeBackupInfo(&backup, record);
    }
}

fn handleRestoreFromBackup(
    ctx_ptr: *anyopaque,
    caller_peer: *rpc.peer.Peer,
    params: KvStore.RestoreFromBackup.Params.Reader,
    results: *KvStore.RestoreFromBackup.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const backup_id = try params.getBackupId();
    const keep_log_files = try params.getKeepLogFiles();

    const restored_backup_id = try svc.store.restoreFromBackup(backup_id, keep_log_files);
    try results.setRestoredBackupId(restored_backup_id);
    try results.setNextVersion(svc.store.nextVersion());
    svc.notifyStateResetSubscribers(caller_peer, restored_backup_id, svc.store.nextVersion());
}

// ---------------------------------------------------------------------------
// Peer lifecycle
// ---------------------------------------------------------------------------

fn onPeerError(_: ?*anyopaque, peer: *rpc.peer.Peer, err: anyerror) void {
    const last_tag: []const u8 = if (peer.last_inbound_tag) |tag| @tagName(tag) else "none";
    std.log.err("peer error: {s} (last_inbound_tag={s})", .{ @errorName(err), last_tag });
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}

fn onPeerClose(_: ?*anyopaque, peer: *rpc.peer.Peer) void {
    if (g_service) |svc| {
        while (!svc.mu.tryLock()) {}
        defer svc.mu.unlock();
        svc.removeSubscriber(peer);
    }

    std.log.debug("client disconnected", .{});
}

// ---------------------------------------------------------------------------
// Listener (WorkerPool)
// ---------------------------------------------------------------------------

fn onAccept(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, conn: *rpc.transport.tcp.Connection, _: u32) anyerror!rpc.integration.worker_pool.WorkerPool.AcceptDecision {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));

    // Enable cross-thread wake so notifications can be sent on this
    // connection's thread rather than the notifying worker's thread.
    conn.enableWake(onConnectionWake) catch |err| {
        std.log.err("failed to enable wake pipe: {s}", .{@errorName(err)});
    };

    _ = KvStore.setBootstrap(peer, &svc.server) catch |err| {
        std.log.err("failed to set bootstrap: {s}", .{@errorName(err)});
        return err;
    };

    peer.start(null, onPeerError, onPeerClose);
    std.log.debug("client connected", .{});
    return .accept;
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

const CliArgs = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 9000,
    db_path: []const u8 = "kvstore-data",
    backup_dir: []const u8 = "kvstore-backups",
    quiet: bool = false,
};

fn parseArgs(allocator: Allocator, args: std.process.Args) !CliArgs {
    var out = CliArgs{};
    var host_text: []const u8 = out.host;
    var db_path_text: []const u8 = out.db_path;
    var backup_dir_text: []const u8 = out.backup_dir;

    var args_iter = std.process.Args.Iterator.init(args);
    _ = args_iter.skip(); // skip program name
    var need_value: enum { none, host, port, db_path, backup_dir } = .none;
    while (args_iter.next()) |arg| {
        switch (need_value) {
            .host => {
                host_text = arg;
                need_value = .none;
                continue;
            },
            .port => {
                out.port = try std.fmt.parseInt(u16, arg, 10);
                need_value = .none;
                continue;
            },
            .db_path => {
                db_path_text = arg;
                need_value = .none;
                continue;
            },
            .backup_dir => {
                backup_dir_text = arg;
                need_value = .none;
                continue;
            },
            .none => {},
        }
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            return error.HelpRequested;
        }
        if (std.mem.eql(u8, arg, "--host")) {
            need_value = .host;
            continue;
        }
        if (std.mem.eql(u8, arg, "--port")) {
            need_value = .port;
            continue;
        }
        if (std.mem.eql(u8, arg, "--db-path")) {
            need_value = .db_path;
            continue;
        }
        if (std.mem.eql(u8, arg, "--backup-dir")) {
            need_value = .backup_dir;
            continue;
        }
        if (std.mem.eql(u8, arg, "--quiet")) {
            out.quiet = true;
            continue;
        }
    }
    if (need_value != .none) return error.MissingArgValue;

    out.host = try allocator.dupe(u8, host_text);
    errdefer allocator.free(out.host);

    out.db_path = try allocator.dupe(u8, db_path_text);
    errdefer allocator.free(out.db_path);

    out.backup_dir = try allocator.dupe(u8, backup_dir_text);

    return out;
}

fn usage() void {
    std.debug.print(
        \\Usage: kvstore-server [--host 127.0.0.1] [--port 9000] [--db-path kvstore-data] [--backup-dir kvstore-backups] [--quiet]
        \\  --quiet             suppress debug/info logs
        \\
    , .{});
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = parseArgs(allocator, init.minimal.args) catch |err| switch (err) {
        error.HelpRequested => {
            usage();
            return;
        },
        error.InvalidCharacter,
        error.Overflow,
        error.MissingArgValue,
        => {
            usage();
            return err;
        },
        else => return err,
    };
    defer allocator.free(args.host);
    defer allocator.free(args.db_path);
    defer allocator.free(args.backup_dir);
    server_is_quiet = args.quiet;

    var svc = try KvService.init(allocator, io, args.db_path, args.backup_dir);
    defer svc.deinit();
    svc.bind();
    g_service = &svc;
    defer g_service = null;

    const address = try std.Io.net.IpAddress.parse(args.host, args.port);

    var pool = try rpc.integration.worker_pool.WorkerPool.init(
        allocator,
        io,
        address,
        &svc,
        onAccept,
        .{ .concurrency = 4 },
    );
    defer pool.deinit();

    if (!server_is_quiet) {
        std.debug.print("READY on {s}:{d}\n", .{
            args.host,
            args.port,
        });
    }

    try pool.run();
}

test "kvstore server defaults bind localhost" {
    const args = CliArgs{};
    try std.testing.expectEqualStrings("127.0.0.1", args.host);

    const address = try std.Io.net.IpAddress.parse(args.host, args.port);
    try std.testing.expectEqual(@as(u16, 9000), address.ip4.port);
    try std.testing.expectEqualSlices(u8, &.{ 127, 0, 0, 1 }, address.ip4.bytes[0..]);
}

test "kvstore server rejects over-limit keys and values" {
    var key: [Limits.max_key_bytes + 1]u8 = undefined;
    @memset(key[0..], 'k');
    try std.testing.expectError(error.KeyTooLarge, validateKey(key[0..]));

    var value: [Limits.max_value_bytes + 1]u8 = undefined;
    @memset(value[0..], 'v');
    try std.testing.expectError(error.ValueTooLarge, validateValue(value[0..]));
}

test "kvstore server rejects over-limit list requests" {
    try std.testing.expectError(error.ListLimitTooLarge, validateListLimit(Limits.max_list_limit + 1));
    try validateListLimit(Limits.max_list_limit);
}
