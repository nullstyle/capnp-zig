const std = @import("std");
const capnpc = @import("capnpc-zig");
const rdb = @import("rocksdb");
const rocksdb = @import("rocksdb-zig");
const kvstore = @import("gen/kvstore.zig");

const message = capnpc.message;
const rpc = capnpc.rpc;
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

const user_key_prefix = "u:";
const next_version_meta_key = "m:next_version";
const encoded_version_size = @sizeOf(u64);

const StoredRecord = struct {
    version: u64,
    value: []const u8,
};

const BatchOutcome = union(enum) {
    put: struct {
        version: u64,
    },
    delete: struct {
        found: bool,
    },
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

const BackupRecord = struct {
    backup_id: u32,
    timestamp: i64,
    size: u64,
    num_files: u32,
};

var g_service: ?*KvService = null;

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

fn encodeVersion(version: u64) [encoded_version_size]u8 {
    var out: [encoded_version_size]u8 = undefined;
    std.mem.writeInt(u64, &out, version, .big);
    return out;
}

fn decodeVersion(encoded: []const u8) !u64 {
    if (encoded.len != encoded_version_size) return error.CorruptVersionEncoding;

    var buf: [encoded_version_size]u8 = undefined;
    std.mem.copyForwards(u8, buf[0..], encoded);
    return std.mem.readInt(u64, &buf, .big);
}

fn encodeRecord(allocator: Allocator, version: u64, value: []const u8) ![]u8 {
    const out = try allocator.alloc(u8, encoded_version_size + value.len);
    const version_bytes = encodeVersion(version);

    std.mem.copyForwards(u8, out[0..encoded_version_size], version_bytes[0..]);
    std.mem.copyForwards(u8, out[encoded_version_size..], value);

    return out;
}

fn decodeRecord(encoded: []const u8) !StoredRecord {
    if (encoded.len < encoded_version_size) return error.CorruptValueEncoding;
    return .{
        .version = try decodeVersion(encoded[0..encoded_version_size]),
        .value = encoded[encoded_version_size..],
    };
}

fn makeUserKey(allocator: Allocator, key: []const u8) ![]u8 {
    const out = try allocator.alloc(u8, user_key_prefix.len + key.len);
    std.mem.copyForwards(u8, out[0..user_key_prefix.len], user_key_prefix);
    std.mem.copyForwards(u8, out[user_key_prefix.len..], key);
    return out;
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

fn logRocksError(operation: []const u8, err: anyerror, err_data: ?rocksdb.Data) void {
    if (err_data) |data| {
        std.log.err("rocksdb {s} failed ({s}): {s}", .{ operation, @errorName(err), data.data });
    } else {
        std.log.err("rocksdb {s} failed ({s})", .{ operation, @errorName(err) });
    }
}

fn logRocksCStringError(operation: []const u8, err_ptr: ?[*:0]u8) void {
    if (err_ptr) |raw| {
        var data = rocksdb.Data{
            .data = std.mem.span(raw),
            .free = rdb.rocksdb_free,
        };
        defer data.deinit();
        std.log.err("rocksdb {s} failed: {s}", .{ operation, data.data });
    }
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
        conn: *rpc.connection.Connection,
        notifier: KvClientNotifier.Client,
        watched_keys: std.ArrayListUnmanaged([]u8) = .{},
        /// Pending notifications queued from other worker threads.
        /// Protected by `pending_mu`.
        pending: std.ArrayListUnmanaged(PendingNotification) = .{},
        pending_mu: std.atomic.Mutex = .unlocked,
    };

    allocator: Allocator,
    db_path: []const u8,
    backup_dir: []const u8,
    db: rocksdb.DB,
    default_cf: rocksdb.ColumnFamilyHandle,
    db_open: bool,
    next_version: u64,
    server: KvStore.Server,
    subscribers: std.ArrayListUnmanaged(Subscriber),
    /// Protects all mutable state when accessed from multiple worker threads.
    mu: std.atomic.Mutex = .unlocked,

    fn init(allocator: Allocator, db_path: []const u8, backup_dir: []const u8) !KvService {
        var err_data: ?rocksdb.Data = null;
        defer if (err_data) |data| data.deinit();

        var db, const column_families = rocksdb.DB.open(
            allocator,
            db_path,
            .{
                .create_if_missing = true,
                .create_missing_column_families = true,
            },
            null,
            false,
            &err_data,
        ) catch |err| {
            logRocksError("open", err, err_data);
            return err;
        };
        errdefer db.deinit();
        defer allocator.free(column_families);

        if (column_families.len == 0) return error.MissingDefaultColumnFamily;

        const default_cf = column_families[0].handle;
        db = db.withDefaultColumnFamily(default_cf);

        var service = KvService{
            .allocator = allocator,
            .db_path = db_path,
            .backup_dir = backup_dir,
            .db = db,
            .default_cf = default_cf,
            .db_open = true,
            .next_version = 1,
            .server = undefined,
            .subscribers = .{},
        };

        service.next_version = try service.loadNextVersion();

        return service;
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
        if (self.db_open) self.db.deinit();
    }

    fn loadNextVersion(self: *KvService) !u64 {
        var err_data: ?rocksdb.Data = null;
        defer if (err_data) |data| data.deinit();

        const maybe_encoded = self.db.get(null, next_version_meta_key, &err_data) catch |err| {
            logRocksError("get next_version", err, err_data);
            return err;
        };

        if (maybe_encoded) |encoded| {
            defer encoded.deinit();
            return decodeVersion(encoded.data);
        }

        return 1;
    }

    fn closeDatabase(self: *KvService) void {
        if (!self.db_open) return;
        self.db.deinit();
        self.db_open = false;
    }

    fn reopenDatabase(self: *KvService) !void {
        var err_data: ?rocksdb.Data = null;
        defer if (err_data) |data| data.deinit();

        var db, const column_families = rocksdb.DB.open(
            self.allocator,
            self.db_path,
            .{
                .create_if_missing = true,
                .create_missing_column_families = true,
            },
            null,
            false,
            &err_data,
        ) catch |err| {
            logRocksError("open (reopen)", err, err_data);
            return err;
        };
        errdefer db.deinit();
        defer self.allocator.free(column_families);

        if (column_families.len == 0) return error.MissingDefaultColumnFamily;

        const default_cf = column_families[0].handle;
        db = db.withDefaultColumnFamily(default_cf);

        self.db = db;
        self.default_cf = default_cf;
        self.db_open = true;
        self.next_version = try self.loadNextVersion();
    }

    fn openBackupEngine(self: *KvService) !*rdb.rocksdb_backup_engine_t {
        const options = rdb.rocksdb_options_create() orelse return error.RocksDBBackupOptionsCreateFailed;
        defer rdb.rocksdb_options_destroy(options);

        const backup_dir_z = try self.allocator.dupeZ(u8, self.backup_dir);
        defer self.allocator.free(backup_dir_z);

        // Create backup directory if it doesn't exist.
        _ = std.c.mkdir(backup_dir_z.ptr, 0o755);

        var err_ptr: ?[*:0]u8 = null;
        const engine = rdb.rocksdb_backup_engine_open(options, backup_dir_z.ptr, @ptrCast(&err_ptr));
        if (err_ptr != null) {
            logRocksCStringError("backup_engine_open", err_ptr);
            return error.RocksDBBackupEngineOpen;
        }

        return engine orelse error.RocksDBBackupEngineOpen;
    }

    fn readBackupRecords(self: *KvService, engine: *rdb.rocksdb_backup_engine_t) ![]BackupRecord {
        const info = rdb.rocksdb_backup_engine_get_backup_info(engine) orelse return error.RocksDBBackupInfoUnavailable;
        defer rdb.rocksdb_backup_engine_info_destroy(info);

        const count_raw = rdb.rocksdb_backup_engine_info_count(info);
        if (count_raw < 0) return error.RocksDBBackupInfoInvalidCount;
        if (count_raw == 0) return try self.allocator.alloc(BackupRecord, 0);

        const count: usize = @intCast(count_raw);
        const records = try self.allocator.alloc(BackupRecord, count);
        errdefer self.allocator.free(records);

        for (0..count) |idx_usize| {
            const idx: c_int = @intCast(idx_usize);
            records[idx_usize] = .{
                .backup_id = rdb.rocksdb_backup_engine_info_backup_id(info, idx),
                .timestamp = rdb.rocksdb_backup_engine_info_timestamp(info, idx),
                .size = rdb.rocksdb_backup_engine_info_size(info, idx),
                .num_files = rdb.rocksdb_backup_engine_info_number_files(info, idx),
            };
        }

        return records;
    }

    fn createBackup(self: *KvService, flush_before_backup: bool) !struct {
        backup: BackupRecord,
        backup_count: u32,
    } {
        const engine = try self.openBackupEngine();
        defer rdb.rocksdb_backup_engine_close(engine);

        var err_ptr: ?[*:0]u8 = null;
        rdb.rocksdb_backup_engine_create_new_backup_flush(
            engine,
            self.db.db,
            @intFromBool(flush_before_backup),
            @ptrCast(&err_ptr),
        );
        if (err_ptr != null) {
            logRocksCStringError("backup_engine_create_new_backup", err_ptr);
            return error.RocksDBCreateBackup;
        }

        const records = try self.readBackupRecords(engine);
        defer self.allocator.free(records);
        if (records.len == 0) return error.NoBackupsAvailable;

        return .{
            .backup = records[records.len - 1],
            .backup_count = @intCast(records.len),
        };
    }

    fn listBackups(self: *KvService) ![]BackupRecord {
        const engine = try self.openBackupEngine();
        defer rdb.rocksdb_backup_engine_close(engine);
        return self.readBackupRecords(engine);
    }

    fn restoreFromBackup(self: *KvService, requested_backup_id: u32, keep_log_files: bool) !u32 {
        const engine = try self.openBackupEngine();
        defer rdb.rocksdb_backup_engine_close(engine);

        const records = try self.readBackupRecords(engine);
        defer self.allocator.free(records);
        if (records.len == 0) return error.NoBackupsAvailable;

        var target_backup_id: u32 = requested_backup_id;
        if (target_backup_id == 0) {
            target_backup_id = records[records.len - 1].backup_id;
        } else {
            var found = false;
            for (records) |record| {
                if (record.backup_id == target_backup_id) {
                    found = true;
                    break;
                }
            }
            if (!found) return error.BackupNotFound;
        }

        const restore_options = rdb.rocksdb_restore_options_create() orelse return error.RocksDBRestoreOptionsCreateFailed;
        defer rdb.rocksdb_restore_options_destroy(restore_options);
        rdb.rocksdb_restore_options_set_keep_log_files(restore_options, @intFromBool(keep_log_files));

        const db_path_z = try self.allocator.dupeZ(u8, self.db_path);
        defer self.allocator.free(db_path_z);

        self.closeDatabase();

        var err_ptr: ?[*:0]u8 = null;
        if (requested_backup_id == 0) {
            rdb.rocksdb_backup_engine_restore_db_from_latest_backup(
                engine,
                db_path_z.ptr,
                db_path_z.ptr,
                restore_options,
                @ptrCast(&err_ptr),
            );
        } else {
            rdb.rocksdb_backup_engine_restore_db_from_backup(
                engine,
                db_path_z.ptr,
                db_path_z.ptr,
                restore_options,
                target_backup_id,
                @ptrCast(&err_ptr),
            );
        }

        if (err_ptr != null) {
            logRocksCStringError("backup_engine_restore_db", err_ptr);
            self.reopenDatabase() catch |reopen_err| {
                std.log.err("failed to reopen database after restore failure: {s}", .{@errorName(reopen_err)});
                return reopen_err;
            };
            return error.RocksDBRestoreBackup;
        }

        try self.reopenDatabase();
        return target_backup_id;
    }

    fn addOrUpdateSubscriber(self: *KvService, peer: *rpc.peer.Peer, conn: *rpc.connection.Connection, notifier: KvClientNotifier.Client) !void {
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

            // Build owned copies of the relevant (filtered) changes.
            const owned_changes = self.buildOwnedKeysChanged(changes, watched_keys) catch |err| {
                std.log.warn("failed to build notification: {s}", .{@errorName(err)});
                continue;
            };

            // Push to subscriber's queue (thread-safe via spinlock).
            while (!subscriber.pending_mu.tryLock()) {}
            defer subscriber.pending_mu.unlock();
            subscriber.pending.append(self.allocator, .{
                .keys_changed = .{ .changes = owned_changes },
            }) catch |err| {
                std.log.warn("failed to queue notification: {s}", .{@errorName(err)});
                for (owned_changes) |c| c.deinit(self.allocator);
                self.allocator.free(owned_changes);
                continue;
            };

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
            defer subscriber.pending_mu.unlock();
            subscriber.pending.append(self.allocator, .{
                .state_reset = .{
                    .restored_backup_id = restored_backup_id,
                    .next_version = next_version,
                },
            }) catch |err| {
                std.log.warn("failed to queue state reset notification: {s}", .{@errorName(err)});
                continue;
            };

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
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    switch (response) {
        .exception => |ex| {
            std.log.debug("client notifier returned exception: {s}", .{ex.reason});
        },
        else => {},
    }
}

fn onWakeStateResetReturn(
    _: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvClientNotifier.StateResetRequired.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    switch (response) {
        .exception => |ex| {
            std.log.debug("client reset notifier returned exception: {s}", .{ex.reason});
        },
        else => {},
    }
}

/// Called on the connection's own thread when woken by another thread.
/// Drains the subscriber's pending notification queue and sends each
/// notification via the Peer (which is thread-affine to this thread).
fn onConnectionWake(conn: *rpc.connection.Connection) void {
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
    var notifier = sub.notifier;
    svc.mu.unlock();

    // Process each queued notification on this connection's thread.
    defer svc.allocator.free(pending);
    std.log.info("WAKE: draining {d} pending notification(s)", .{pending.len});
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
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const key = try params.getKey();
    std.log.info("GET \"{s}\"", .{key});

    const db_key = try makeUserKey(svc.allocator, key);
    defer svc.allocator.free(db_key);

    var err_data: ?rocksdb.Data = null;
    defer if (err_data) |data| data.deinit();

    const maybe_encoded = svc.db.get(null, db_key, &err_data) catch |err| {
        logRocksError("get", err, err_data);
        return err;
    };

    if (maybe_encoded) |encoded| {
        defer encoded.deinit();

        const record = try decodeRecord(encoded.data);
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
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const ops = try params.getOps();
    const op_count: u32 = ops.len();
    std.log.info("WRITE BATCH ops={d}", .{op_count});

    if (op_count == 0) {
        _ = try results.initResults(0);
        try results.setApplied(0);
        try results.setNextVersion(svc.next_version);
        return;
    }

    var outcomes = try svc.allocator.alloc(BatchOutcome, op_count);
    defer svc.allocator.free(outcomes);

    var temp_buffers = std.ArrayListUnmanaged([]u8){};
    defer {
        for (temp_buffers.items) |buf| {
            svc.allocator.free(buf);
        }
        temp_buffers.deinit(svc.allocator);
    }

    var notify_changes = std.ArrayListUnmanaged(NotifyChange){};
    defer notify_changes.deinit(svc.allocator);

    var batch = rocksdb.WriteBatch.init();
    defer batch.deinit();

    var next_version = svc.next_version;
    var batch_put_version: ?u64 = null;

    for (0..op_count) |idx| {
        const op = try ops.get(@intCast(idx));
        const key = try op.getKey();
        const which = try op.which();

        const db_key = try makeUserKey(svc.allocator, key);
        try temp_buffers.append(svc.allocator, db_key);

        switch (which) {
            .put => {
                const value = try op.getPut();

                const assigned_version = if (batch_put_version) |existing|
                    existing
                else blk: {
                    if (next_version == std.math.maxInt(u64)) return error.VersionOverflow;
                    const new_version = next_version;
                    next_version += 1;
                    batch_put_version = new_version;
                    break :blk new_version;
                };

                const encoded_record = try encodeRecord(svc.allocator, assigned_version, value);
                try temp_buffers.append(svc.allocator, encoded_record);

                batch.put(svc.default_cf, db_key, encoded_record);
                outcomes[idx] = .{ .put = .{ .version = assigned_version } };
                try notify_changes.append(svc.allocator, .{
                    .put = .{
                        .key = key,
                        .value = value,
                        .version = assigned_version,
                    },
                });
            },
            .delete => {
                var err_data: ?rocksdb.Data = null;
                defer if (err_data) |data| data.deinit();

                var found = false;
                const maybe_existing = svc.db.get(null, db_key, &err_data) catch |err| {
                    logRocksError("get (batch delete preflight)", err, err_data);
                    return err;
                };

                if (maybe_existing) |existing| {
                    existing.deinit();
                    found = true;
                }

                batch.delete(svc.default_cf, db_key);
                outcomes[idx] = .{ .delete = .{ .found = found } };
                try notify_changes.append(svc.allocator, .{
                    .delete = .{
                        .key = key,
                        .found = found,
                    },
                });
            },
        }
    }

    if (next_version != svc.next_version) {
        var next_version_bytes = encodeVersion(next_version);
        batch.put(svc.default_cf, next_version_meta_key, next_version_bytes[0..]);
    }

    var write_err: ?rocksdb.Data = null;
    defer if (write_err) |data| data.deinit();

    svc.db.write(batch, &write_err) catch |err| {
        logRocksError("write batch", err, write_err);
        return err;
    };

    svc.next_version = next_version;

    var out_results = try results.initResults(op_count);
    for (0..op_count) |idx| {
        const op = try ops.get(@intCast(idx));
        const key = try op.getKey();

        var out = try out_results.get(@intCast(idx));
        try out.setKey(key);

        switch (outcomes[idx]) {
            .put => |put| {
                const value = try op.getPut();
                var out_entry = try out.initPut();
                try out_entry.setKey(key);
                try out_entry.setValue(value);
                try out_entry.setVersion(put.version);
            },
            .delete => |del| {
                try out.setDelete(del.found);
            },
        }
    }

    try results.setApplied(op_count);
    try results.setNextVersion(svc.next_version);

    svc.notifySubscribers(caller_peer, notify_changes.items);
}

fn handleList(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: KvStore.List.Params.Reader,
    results: *KvStore.List.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const prefix = try params.getPrefix();
    const limit = try params.getLimit();
    std.log.info("LIST prefix=\"{s}\" limit={d}", .{ prefix, limit });

    if (limit == 0) {
        _ = try results.initEntries(0);
        return;
    }

    const prefixed_prefix = try makeUserKey(svc.allocator, prefix);
    defer svc.allocator.free(prefixed_prefix);

    // Pass 1: count matching entries.
    var count: u32 = 0;
    {
        var iter = svc.db.iterator(null, .forward, prefixed_prefix);
        defer iter.deinit();

        var iter_err: ?rocksdb.Data = null;
        defer if (iter_err) |data| data.deinit();

        while (count < limit) {
            const maybe_entry = iter.next(&iter_err) catch |err| {
                logRocksError("iterator next (count)", err, iter_err);
                return err;
            };

            const entry = maybe_entry orelse break;
            const stored_key = entry[0].data;

            if (!std.mem.startsWith(u8, stored_key, prefixed_prefix)) break;

            _ = try decodeRecord(entry[1].data);
            count += 1;
        }
    }

    var entries = try results.initEntries(count);

    // Pass 2: materialize result list.
    var idx: u32 = 0;
    var iter = svc.db.iterator(null, .forward, prefixed_prefix);
    defer iter.deinit();

    var iter_err: ?rocksdb.Data = null;
    defer if (iter_err) |data| data.deinit();

    while (idx < count) {
        const maybe_entry = iter.next(&iter_err) catch |err| {
            logRocksError("iterator next (fill)", err, iter_err);
            return err;
        };

        const entry = maybe_entry orelse break;
        const stored_key = entry[0].data;

        if (!std.mem.startsWith(u8, stored_key, prefixed_prefix)) break;

        const logical_key = stored_key[user_key_prefix.len..];
        const record = try decodeRecord(entry[1].data);

        var out = try entries.get(idx);
        try out.setKey(logical_key);
        try out.setValue(record.value);
        try out.setVersion(record.version);

        idx += 1;
    }
}

fn handleSubscribe(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: KvStore.Subscribe.Params.Reader,
    _: *KvStore.Subscribe.Results.Builder,
    caps: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const notifier = try params.resolveNotifier(peer, caps);
    const conn = peer.getAttachedConnection(*rpc.connection.Connection) orelse return error.NoPeerConnection;
    try svc.addOrUpdateSubscriber(peer, conn, notifier);
    std.log.info("SUBSCRIBE cap={d}", .{notifier.cap_id});
}

fn handleSetWatchedKeys(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    params: KvStore.SetWatchedKeys.Params.Reader,
    _: *KvStore.SetWatchedKeys.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const watched_keys = try params.getKeys();
    const count = try svc.setSubscriberWatchedKeys(peer, watched_keys);
    std.log.info("SET WATCHED KEYS count={d}", .{count});
}

fn writeBackupInfo(builder: *BackupInfo.Builder, record: BackupRecord) !void {
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
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const flush_before_backup = try params.getFlushBeforeBackup();
    const outcome = try svc.createBackup(flush_before_backup);

    var backup = try results.initBackup();
    try writeBackupInfo(&backup, outcome.backup);
    try results.setBackupCount(outcome.backup_count);
}

fn handleListBackups(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    _: KvStore.ListBackups.Params.Reader,
    results: *KvStore.ListBackups.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const records = try svc.listBackups();
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
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const svc: *KvService = @ptrCast(@alignCast(ctx_ptr));
    while (!svc.mu.tryLock()) {}
    defer svc.mu.unlock();
    const backup_id = try params.getBackupId();
    const keep_log_files = try params.getKeepLogFiles();

    const restored_backup_id = try svc.restoreFromBackup(backup_id, keep_log_files);
    try results.setRestoredBackupId(restored_backup_id);
    try results.setNextVersion(svc.next_version);
    svc.notifyStateResetSubscribers(caller_peer, restored_backup_id, svc.next_version);
}

// ---------------------------------------------------------------------------
// Peer lifecycle
// ---------------------------------------------------------------------------

fn onPeerError(peer: *rpc.peer.Peer, err: anyerror) void {
    const last_tag: []const u8 = if (peer.last_inbound_tag) |tag| @tagName(tag) else "none";
    std.log.err("peer error: {s} (last_inbound_tag={s})", .{ @errorName(err), last_tag });
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}

fn onPeerClose(peer: *rpc.peer.Peer) void {
    if (g_service) |svc| {
        while (!svc.mu.tryLock()) {}
        defer svc.mu.unlock();
        svc.removeSubscriber(peer);
    }

    std.log.info("client disconnected", .{});
}

// ---------------------------------------------------------------------------
// Listener (WorkerPool)
// ---------------------------------------------------------------------------

fn onAccept(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, conn: *rpc.connection.Connection, _: u32) anyerror!rpc.worker_pool.WorkerPool.AcceptDecision {
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

    peer.start(onPeerError, onPeerClose);
    std.log.info("client connected", .{});
    return .accept;
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

const CliArgs = struct {
    host: []const u8 = "0.0.0.0",
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
        \\Usage: kvstore-server [--host 0.0.0.0] [--port 9000] [--db-path kvstore-data] [--backup-dir kvstore-backups] [--quiet]
        \\  --quiet             suppress debug/info logs
        \\
    , .{});
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn parseIp4Address(host: []const u8, port: u16) !std.Io.net.IpAddress {
    var bytes: [4]u8 = undefined;
    var byte_idx: usize = 0;
    var iter = std.mem.splitScalar(u8, host, '.');
    while (iter.next()) |octet| {
        if (byte_idx >= 4) return error.InvalidAddress;
        bytes[byte_idx] = std.fmt.parseInt(u8, octet, 10) catch return error.InvalidAddress;
        byte_idx += 1;
    }
    if (byte_idx != 4) return error.InvalidAddress;
    return .{ .ip4 = .{ .bytes = bytes, .port = port } };
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
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

    var svc = try KvService.init(allocator, args.db_path, args.backup_dir);
    defer svc.deinit();
    svc.bind();
    g_service = &svc;
    defer g_service = null;

    const address = try parseIp4Address(args.host, args.port);

    var pool = try rpc.worker_pool.WorkerPool.init(
        allocator,
        io,
        address,
        &svc,
        onAccept,
        .{ .concurrency = 4 },
    );
    defer pool.deinit();

    if (!server_is_quiet) {
        std.debug.print("READY on {s}:{d} (db: {s}, backup: {s}, next_version={d})\n", .{
            args.host,
            args.port,
            args.db_path,
            args.backup_dir,
            svc.next_version,
        });
    }

    try pool.run();
}
