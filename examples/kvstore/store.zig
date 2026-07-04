//! A small, self-contained, pure-Zig persistent key/value backend for the
//! kvstore example. It replaces the vendored RocksDB binding so the example
//! stays a focused demonstration of Cap'n Proto RPC with no external C/C++
//! dependency, no network fetch, and nothing to bit-rot against Zig master.
//!
//! Design: an in-memory index (`key -> {version, value}`) fronted by an
//! on-disk append-only write-ahead log (`<data_dir>/wal.log`). Every mutation
//! is appended to the log before the map is updated; on `open()` the log is
//! replayed to rebuild the map. Backups are atomic snapshots of the log file
//! under `<backup_dir>/backup-<id>.wal`; restore swaps a snapshot back in and
//! replays it.
//!
//! Concurrency: the store performs NO internal locking. The kvstore server
//! serializes every access behind `KvService.mu`, so a single-threaded view is
//! guaranteed by the caller — which is also why the now-removed RocksDB
//! binding's `std.Thread.RwLock` (gone from this Zig master) is not missed.
//!
//! All filesystem access goes through `std.Io`, matching the rest of the RPC
//! runtime, and uses positional reads/writes so no seek state is kept.

const std = @import("std");

const Io = std.Io;
const Allocator = std.mem.Allocator;

pub const Store = struct {
    /// A value read back out of the store. `value` borrows the store's own
    /// buffer and is valid only until the next mutation (callers copy it into
    /// the RPC response while holding the service lock).
    pub const Record = struct {
        version: u64,
        value: []const u8,
    };

    /// One mutation in a batch. `key`/`value` are borrowed from the caller
    /// (typically the inbound Cap'n Proto message) for the duration of the
    /// `applyBatch` call.
    pub const BatchOp = union(enum) {
        put: struct { key: []const u8, value: []const u8 },
        delete: []const u8,
    };

    /// The result of applying the batch op at the same index.
    pub const BatchOutcome = union(enum) {
        put: struct { version: u64 },
        delete: struct { found: bool },
    };

    /// One entry from a prefix `list`. Slices borrow the store's buffers and
    /// are valid until the next mutation; the returned slice itself is owned by
    /// the caller.
    pub const ListEntry = struct {
        key: []const u8,
        version: u64,
        value: []const u8,
    };

    /// Metadata for one on-disk backup snapshot.
    pub const BackupRecord = struct {
        backup_id: u32,
        timestamp: i64,
        size: u64,
        num_files: u32,
    };

    pub const BackupCreated = struct {
        record: BackupRecord,
        count: u32,
    };

    const Value = struct {
        version: u64,
        bytes: []u8,
    };

    const wal_name = "wal.log";
    const backup_prefix = "backup-";
    const backup_suffix = ".wal";

    allocator: Allocator,
    io: Io,
    /// Open handle to the data directory; owns `wal`.
    data_dir: Io.Dir,
    /// Path to the backup directory (opened on demand).
    backup_dir_path: []u8,
    map: std.StringHashMapUnmanaged(Value),
    next_version: u64,
    /// Append-only log, open for the life of the store.
    wal: Io.File,
    /// Byte length of `wal`; the append cursor.
    wal_len: u64,

    pub fn open(
        allocator: Allocator,
        io: Io,
        data_dir_path: []const u8,
        backup_dir_path: []const u8,
    ) !Store {
        var cwd = Io.Dir.cwd();
        try cwd.createDirPath(io, data_dir_path);
        var data_dir = try cwd.openDir(io, data_dir_path, .{});
        errdefer data_dir.close(io);

        const wal = try data_dir.createFile(io, wal_name, .{ .read = true, .truncate = false });
        errdefer wal.close(io);

        const owned_backup_dir = try allocator.dupe(u8, backup_dir_path);
        errdefer allocator.free(owned_backup_dir);

        var self = Store{
            .allocator = allocator,
            .io = io,
            .data_dir = data_dir,
            .backup_dir_path = owned_backup_dir,
            .map = .empty,
            .next_version = 1,
            .wal = wal,
            .wal_len = 0,
        };
        try self.replay();
        return self;
    }

    pub fn deinit(self: *Store) void {
        self.clearMap();
        self.map.deinit(self.allocator);
        self.allocator.free(self.backup_dir_path);
        self.wal.close(self.io);
        self.data_dir.close(self.io);
    }

    pub fn nextVersion(self: *const Store) u64 {
        return self.next_version;
    }

    pub fn get(self: *const Store, key: []const u8) ?Record {
        if (self.map.get(key)) |v| return .{ .version = v.version, .value = v.bytes };
        return null;
    }

    /// Apply a batch of puts/deletes atomically with respect to the log: the
    /// whole batch is serialized to the WAL in one positional write before the
    /// in-memory map is touched. All puts in a batch share a single version
    /// (mirroring the original RocksDB-backed semantics); deletes consume none.
    /// `outcomes` must be the same length as `ops`.
    pub fn applyBatch(self: *Store, ops: []const BatchOp, outcomes: []BatchOutcome) !void {
        std.debug.assert(ops.len == outcomes.len);

        // Pass 1: assign versions and compute delete-found against pre-batch
        // state (a delete sees the map as it was before this batch).
        var batch_version: ?u64 = null;
        var put_count: usize = 0;
        for (ops, outcomes) |op, *oc| {
            switch (op) {
                .put => {
                    put_count += 1;
                    const v = batch_version orelse blk: {
                        if (self.next_version == std.math.maxInt(u64)) return error.VersionOverflow;
                        batch_version = self.next_version;
                        break :blk self.next_version;
                    };
                    oc.* = .{ .put = .{ .version = v } };
                },
                .delete => |key| oc.* = .{ .delete = .{ .found = self.map.contains(key) } },
            }
        }

        // Pass 2: serialize the batch into WAL bytes.
        var wal_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer wal_buf.deinit(self.allocator);
        for (ops, outcomes) |op, oc| {
            switch (op) {
                .put => |p| {
                    try wal_buf.append(self.allocator, tag_put);
                    try appendInt(&wal_buf, self.allocator, u64, oc.put.version);
                    try appendInt(&wal_buf, self.allocator, u32, @intCast(p.key.len));
                    try wal_buf.appendSlice(self.allocator, p.key);
                    try appendInt(&wal_buf, self.allocator, u32, @intCast(p.value.len));
                    try wal_buf.appendSlice(self.allocator, p.value);
                },
                .delete => |key| {
                    try wal_buf.append(self.allocator, tag_delete);
                    try appendInt(&wal_buf, self.allocator, u32, @intCast(key.len));
                    try wal_buf.appendSlice(self.allocator, key);
                },
            }
        }

        // Pass 3: make it durable, then apply to the in-memory map.
        try self.appendWal(wal_buf.items);
        try self.map.ensureUnusedCapacity(self.allocator, @intCast(put_count));
        for (ops, outcomes) |op, oc| {
            switch (op) {
                .put => |p| try self.upsert(p.key, oc.put.version, p.value),
                .delete => |key| _ = self.remove(key),
            }
        }
        if (batch_version) |v| self.next_version = v + 1;
    }

    /// Return up to `limit` entries whose key starts with `prefix`, sorted by
    /// key. The returned slice is owned by the caller (free with `allocator`);
    /// the key/value slices inside borrow the store and are valid only until
    /// the next mutation.
    pub fn list(self: *const Store, allocator: Allocator, prefix: []const u8, limit: u32) ![]ListEntry {
        var out: std.ArrayListUnmanaged(ListEntry) = .empty;
        errdefer out.deinit(allocator);

        var it = self.map.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            if (!std.mem.startsWith(u8, key, prefix)) continue;
            try out.append(allocator, .{
                .key = key,
                .version = entry.value_ptr.version,
                .value = entry.value_ptr.bytes,
            });
        }

        std.mem.sort(ListEntry, out.items, {}, lessThanByKey);
        if (out.items.len > limit) out.shrinkRetainingCapacity(limit);
        return out.toOwnedSlice(allocator);
    }

    /// Snapshot the current log into `<backup_dir>/backup-<id>.wal` and return
    /// its metadata plus the total number of backups that now exist.
    pub fn createBackup(self: *Store) !BackupCreated {
        try self.wal.sync(self.io);

        var backup_dir = try self.openBackupDir();
        defer backup_dir.close(self.io);

        const ids = try self.collectBackupIds(backup_dir);
        defer self.allocator.free(ids);

        const next_id: u32 = if (ids.len == 0) 1 else ids[ids.len - 1] + 1;

        var name_buf: [64]u8 = undefined;
        const name = try backupName(&name_buf, next_id);
        try self.data_dir.copyFile(wal_name, backup_dir, name, self.io, .{ .replace = true });

        const st = try backup_dir.statFile(self.io, name, .{});
        return .{
            .record = .{
                .backup_id = next_id,
                .timestamp = st.mtime.toSeconds(),
                .size = st.size,
                .num_files = 1,
            },
            .count = @intCast(ids.len + 1),
        };
    }

    /// List all backups (sorted by id). Caller owns the returned slice.
    pub fn listBackups(self: *Store, allocator: Allocator) ![]BackupRecord {
        var backup_dir = try self.openBackupDir();
        defer backup_dir.close(self.io);

        const ids = try self.collectBackupIds(backup_dir);
        defer self.allocator.free(ids);

        const records = try allocator.alloc(BackupRecord, ids.len);
        errdefer allocator.free(records);

        var name_buf: [64]u8 = undefined;
        for (ids, records) |id, *record| {
            const name = try backupName(&name_buf, id);
            const st = try backup_dir.statFile(self.io, name, .{});
            record.* = .{
                .backup_id = id,
                .timestamp = st.mtime.toSeconds(),
                .size = st.size,
                .num_files = 1,
            };
        }
        return records;
    }

    /// Restore from backup `requested_id` (0 = latest), swapping its snapshot
    /// in as the live log and replaying it. Returns the id actually restored.
    pub fn restoreFromBackup(self: *Store, requested_id: u32, keep_log_files: bool) !u32 {
        // `keep_log_files` is a RocksDB-specific knob with no analogue here.
        _ = keep_log_files;

        var backup_dir = try self.openBackupDir();
        defer backup_dir.close(self.io);

        const ids = try self.collectBackupIds(backup_dir);
        defer self.allocator.free(ids);
        if (ids.len == 0) return error.NoBackupsAvailable;

        var target_id: u32 = requested_id;
        if (target_id == 0) {
            target_id = ids[ids.len - 1];
        } else if (std.mem.indexOfScalar(u32, ids, target_id) == null) {
            return error.BackupNotFound;
        }

        var name_buf: [64]u8 = undefined;
        const name = try backupName(&name_buf, target_id);
        try backup_dir.copyFile(name, self.data_dir, wal_name, self.io, .{ .replace = true });

        // Re-point the live log at the restored file and rebuild the index.
        self.wal.close(self.io);
        self.wal = try self.data_dir.createFile(self.io, wal_name, .{ .read = true, .truncate = false });
        try self.replay();
        return target_id;
    }

    // -- internals ----------------------------------------------------------

    const tag_put: u8 = 1;
    const tag_delete: u8 = 2;

    fn replay(self: *Store) !void {
        self.clearMap();
        self.next_version = 1;

        const len = try self.wal.length(self.io);
        self.wal_len = len;
        if (len == 0) return;

        const buf = try self.allocator.alloc(u8, @intCast(len));
        defer self.allocator.free(buf);
        const n = try self.wal.readPositionalAll(self.io, buf, 0);
        const data = buf[0..n];

        var off: usize = 0;
        var max_version: u64 = 0;
        while (off < data.len) {
            const tag = data[off];
            off += 1;
            switch (tag) {
                tag_put => {
                    const version = try readIntAt(data, &off, u64);
                    const key = try readSliceAt(data, &off);
                    const value = try readSliceAt(data, &off);
                    try self.upsert(key, version, value);
                    if (version > max_version) max_version = version;
                },
                tag_delete => {
                    const key = try readSliceAt(data, &off);
                    _ = self.remove(key);
                },
                else => return error.CorruptWal,
            }
        }
        self.next_version = max_version + 1;
    }

    fn appendWal(self: *Store, bytes: []const u8) !void {
        try self.wal.writePositionalAll(self.io, bytes, self.wal_len);
        self.wal_len += bytes.len;
    }

    fn upsert(self: *Store, key: []const u8, version: u64, value: []const u8) !void {
        if (self.map.getPtr(key)) |vp| {
            const new_value = try self.allocator.dupe(u8, value);
            self.allocator.free(vp.bytes);
            vp.bytes = new_value;
            vp.version = version;
            return;
        }
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);
        try self.map.put(self.allocator, owned_key, .{ .version = version, .bytes = owned_value });
    }

    fn remove(self: *Store, key: []const u8) bool {
        if (self.map.fetchRemove(key)) |kv| {
            self.allocator.free(kv.key);
            self.allocator.free(kv.value.bytes);
            return true;
        }
        return false;
    }

    fn clearMap(self: *Store) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.bytes);
        }
        self.map.clearRetainingCapacity();
    }

    fn openBackupDir(self: *Store) !Io.Dir {
        var cwd = Io.Dir.cwd();
        try cwd.createDirPath(self.io, self.backup_dir_path);
        return cwd.openDir(self.io, self.backup_dir_path, .{});
    }

    /// Collect the ids of every `backup-<id>.wal` in `backup_dir`, ascending.
    fn collectBackupIds(self: *Store, backup_dir: Io.Dir) ![]u32 {
        var ids: std.ArrayListUnmanaged(u32) = .empty;
        errdefer ids.deinit(self.allocator);

        var it = backup_dir.iterate();
        while (try it.next(self.io)) |entry| {
            if (entry.kind != .file) continue;
            const id = parseBackupId(entry.name) orelse continue;
            try ids.append(self.allocator, id);
        }
        std.mem.sort(u32, ids.items, {}, std.sort.asc(u32));
        return ids.toOwnedSlice(self.allocator);
    }
};

fn lessThanByKey(_: void, a: Store.ListEntry, b: Store.ListEntry) bool {
    return std.mem.lessThan(u8, a.key, b.key);
}

fn backupName(buf: []u8, id: u32) ![]const u8 {
    return std.fmt.bufPrint(buf, Store.backup_prefix ++ "{d}" ++ Store.backup_suffix, .{id});
}

fn parseBackupId(name: []const u8) ?u32 {
    if (!std.mem.startsWith(u8, name, Store.backup_prefix)) return null;
    if (!std.mem.endsWith(u8, name, Store.backup_suffix)) return null;
    const digits = name[Store.backup_prefix.len .. name.len - Store.backup_suffix.len];
    if (digits.len == 0) return null;
    return std.fmt.parseInt(u32, digits, 10) catch null;
}

fn appendInt(buf: *std.ArrayListUnmanaged(u8), allocator: Allocator, comptime T: type, value: T) !void {
    var tmp: [@divExact(@typeInfo(T).int.bits, 8)]u8 = undefined;
    std.mem.writeInt(T, &tmp, value, .little);
    try buf.appendSlice(allocator, &tmp);
}

fn readIntAt(data: []const u8, off: *usize, comptime T: type) !T {
    const n = @divExact(@typeInfo(T).int.bits, 8);
    if (off.* + n > data.len) return error.CorruptWal;
    const value = std.mem.readInt(T, data[off.*..][0..n], .little);
    off.* += n;
    return value;
}

fn readSliceAt(data: []const u8, off: *usize) ![]const u8 {
    const len = try readIntAt(data, off, u32);
    if (off.* + len > data.len) return error.CorruptWal;
    const slice = data[off.* .. off.* + len];
    off.* += len;
    return slice;
}

test "store: put/get/list/persist/backup/restore" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var data_buf: [128]u8 = undefined;
    const data_path = try std.fmt.bufPrint(&data_buf, ".zig-cache/tmp/{s}/data", .{tmp.sub_path});
    var backup_buf: [128]u8 = undefined;
    const backup_path = try std.fmt.bufPrint(&backup_buf, ".zig-cache/tmp/{s}/backups", .{tmp.sub_path});

    {
        var store = try Store.open(gpa, io, data_path, backup_path);
        defer store.deinit();

        var outcomes: [3]Store.BatchOutcome = undefined;
        try store.applyBatch(&.{
            .{ .put = .{ .key = "alpha", .value = "one" } },
            .{ .put = .{ .key = "beta", .value = "two" } },
            .{ .delete = "ghost" },
        }, &outcomes);
        // All puts in a batch share one version; deletes consume none.
        try std.testing.expectEqual(@as(u64, 1), outcomes[0].put.version);
        try std.testing.expectEqual(@as(u64, 1), outcomes[1].put.version);
        try std.testing.expectEqual(false, outcomes[2].delete.found);
        try std.testing.expectEqual(@as(u64, 2), store.nextVersion());
        try std.testing.expectEqualStrings("one", store.get("alpha").?.value);
        try std.testing.expect(store.get("ghost") == null);

        // A second batch bumps the shared version again.
        var oc2: [1]Store.BatchOutcome = undefined;
        try store.applyBatch(&.{.{ .put = .{ .key = "alpha", .value = "ONE" } }}, &oc2);
        try std.testing.expectEqual(@as(u64, 2), oc2[0].put.version);
        try std.testing.expectEqualStrings("ONE", store.get("alpha").?.value);
        try std.testing.expectEqual(@as(u64, 3), store.nextVersion());

        const created = try store.createBackup();
        try std.testing.expectEqual(@as(u32, 1), created.record.backup_id);
        try std.testing.expectEqual(@as(u32, 1), created.count);

        // Mutate after the backup, then verify a prefix listing is sorted.
        var oc3: [1]Store.BatchOutcome = undefined;
        try store.applyBatch(&.{.{ .delete = "beta" }}, &oc3);
        try std.testing.expectEqual(true, oc3[0].delete.found);

        const listed = try store.list(gpa, "", 10);
        defer gpa.free(listed);
        try std.testing.expectEqual(@as(usize, 1), listed.len);
        try std.testing.expectEqualStrings("alpha", listed[0].key);

        // Restore the latest backup: beta returns, alpha stays at "ONE".
        const restored = try store.restoreFromBackup(0, false);
        try std.testing.expectEqual(@as(u32, 1), restored);
        try std.testing.expectEqualStrings("ONE", store.get("alpha").?.value);
        try std.testing.expectEqualStrings("two", store.get("beta").?.value);
        try std.testing.expectEqual(@as(u64, 3), store.nextVersion());
    }

    // Reopen: replaying the on-disk log must reconstruct the post-restore state.
    {
        var store = try Store.open(gpa, io, data_path, backup_path);
        defer store.deinit();
        try std.testing.expectEqualStrings("ONE", store.get("alpha").?.value);
        try std.testing.expectEqualStrings("two", store.get("beta").?.value);
        try std.testing.expectEqual(@as(u64, 3), store.nextVersion());

        const backups = try store.listBackups(gpa);
        defer gpa.free(backups);
        try std.testing.expectEqual(@as(usize, 1), backups.len);
        try std.testing.expectEqual(@as(u32, 1), backups[0].backup_id);
    }
}
