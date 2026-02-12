const std = @import("std");
const capnpc = @import("capnpc-zig");
const zz = @import("zigzag");
const kvstore = @import("gen/kvstore.zig");

const xev = capnpc.xev;
const rpc = capnpc.rpc;
const KvStore = kvstore.KvStore;
const KvClientNotifier = kvstore.KvClientNotifier;

const Allocator = std.mem.Allocator;

const max_log_lines: usize = 12;
const default_list_limit: u32 = 25;
const max_history_items: usize = 200;

const command_candidates = [_][]const u8{
    "help",
    "ls",
    "list",
    "get",
    "put",
    "del",
    "delete",
    "backup",
    "backups",
    "restore",
    "open",
    "batch",
    "quit",
    "exit",
};

const batch_op_candidates = [_][]const u8{
    "put",
    "puthex",
    "del",
    "delete",
};

const Mode = enum {
    repl,
    browser,
};

const BatchOpOwned = union(enum) {
    put: struct {
        key: []u8,
        value: []u8,
    },
    delete: struct {
        key: []u8,
    },
};

const PendingRequest = union(enum) {
    get: struct {
        key: []u8,
    },
    list: struct {
        prefix: []u8,
        limit: u32,
    },
    subscribe: void,
    set_watched_keys: void,
    create_backup: struct {
        flush_before_backup: bool,
    },
    list_backups: void,
    restore_from_backup: struct {
        backup_id: u32, // 0 => latest backup
        keep_log_files: bool,
    },
    write_batch: struct {
        ops: []BatchOpOwned,
    },
};

const BrowserEntry = struct {
    key: []u8,
    version: u64,
    version_text: []u8,
    preview: []u8,
};

const PendingRemoteReset = struct {
    restored_backup_id: u32,
    next_version: u64,
};

const CliArgs = struct {
    host: []u8 = undefined,
    port: u16 = 9000,
};

var g_cli_args: ?CliArgs = null;
var g_model: ?*Model = null;

const Model = struct {
    allocator: Allocator = undefined,

    runtime: ?rpc.runtime.Runtime = null,
    socket: ?xev.TCP = null,
    connect_completion: xev.Completion = .{},
    peer: ?*rpc.peer.Peer = null,
    conn: ?*rpc.connection.Connection = null,
    client: ?KvStore.Client = null,
    notifier_server: KvClientNotifier.Server = undefined,

    connected: bool = false,
    subscribed_for_notifications: bool = false,
    pending_rpc: bool = false,
    pending_request: ?PendingRequest = null,
    pending_sync_watched_keys: bool = false,
    pending_remote_reset: ?PendingRemoteReset = null,

    mode: Mode = .repl,
    input: zz.TextInput = undefined,
    table: zz.Table(3) = undefined,

    browser_entries: std.ArrayListUnmanaged(BrowserEntry) = .{},
    browse_prefix: []u8 = undefined,
    browse_limit: u32 = default_list_limit,

    logs: std.ArrayListUnmanaged([]u8) = .{},
    status_text: ?[]u8 = null,

    history: std.ArrayListUnmanaged([]u8) = .{},
    history_cursor: ?usize = null,
    history_draft: ?[]u8 = null,

    inspector_key: ?[]u8 = null,
    inspector_version: ?u64 = null,
    inspector_value: ?[]u8 = null,

    want_quit: bool = false,

    pub const Msg = union(enum) {
        key: zz.KeyEvent,
        tick: zz.msg.Tick,
    };

    pub fn init(self: *Model, ctx: *zz.Context) zz.Cmd(Msg) {
        self.* = .{};
        self.allocator = ctx.persistent_allocator;

        self.input = zz.TextInput.init(self.allocator);
        self.input.setPrompt("repl> ");
        self.input.setPlaceholder("help | ls [prefix] [limit] | get <k> | put/del/batch ... | backup/backups/restore ...");
        self.input.focus();

        self.table = zz.Table(3).init(self.allocator);
        self.table.setHeaders(.{ "Key", "Version", "Preview" });
        self.table.show_row_borders = false;
        self.table.visible_rows = 12;
        self.table.blur();

        self.notifier_server = .{
            .ctx = self,
            .vtable = .{
                .keysChanged = onNotifierKeysChanged,
                .stateResetRequired = onNotifierStateResetRequired,
            },
        };

        self.browse_prefix = self.allocator.dupe(u8, "") catch {
            self.setStatus("failed to allocate browse prefix");
            return .none;
        };

        g_model = self;

        self.logFmt("client starting", .{});
        self.setStatus("initializing runtime");
        self.startConnection();

        return zz.Cmd(Msg).everyMs(16);
    }

    pub fn deinit(self: *Model) void {
        self.clearPendingRequest();

        if (self.peer) |peer| {
            peer.deinit();
            self.allocator.destroy(peer);
            self.peer = null;
        }

        if (self.conn) |conn| {
            conn.deinit();
            self.allocator.destroy(conn);
            self.conn = null;
        }

        if (self.runtime) |*runtime| {
            runtime.deinit();
            self.runtime = null;
        }

        self.clearBrowserEntries();
        self.browser_entries.deinit(self.allocator);

        for (self.logs.items) |line| {
            self.allocator.free(line);
        }
        self.logs.deinit(self.allocator);
        for (self.history.items) |line| {
            self.allocator.free(line);
        }
        self.history.deinit(self.allocator);

        if (self.status_text) |text| self.allocator.free(text);
        if (self.history_draft) |draft| self.allocator.free(draft);

        self.allocator.free(self.browse_prefix);
        self.clearInspector();

        self.table.deinit();
        self.input.deinit();

        if (g_model == self) g_model = null;
    }

    pub fn update(self: *Model, msg: Msg, _: *zz.Context) zz.Cmd(Msg) {
        switch (msg) {
            .tick => {
                self.pumpRuntime();
                if (self.want_quit) return .quit;
                return .none;
            },
            .key => |key| {
                if (self.handleKey(key)) |cmd| {
                    return cmd;
                }
                return .none;
            },
        }
    }

    pub fn view(self: *const Model, ctx: *const zz.Context) []const u8 {
        const allocator = ctx.allocator;

        var out = std.array_list.Managed(u8).init(allocator);
        const writer = out.writer();

        writer.writeAll("KVStore REPL + Browser\n") catch {};
        writer.print(
            "Connection: {s} | Mode: {s} | Pending RPC: {s}\n",
            .{
                if (self.connected) "connected" else "disconnected",
                if (self.mode == .repl) "repl" else "browser",
                if (self.pending_rpc) "yes" else "no",
            },
        ) catch {};
        writer.print("Status: {s}\n", .{self.status_text orelse ""}) catch {};
        writer.writeAll("Keys: Tab autocomplete (REPL)/switch mode (empty) | Up/Down history | Enter execute/open | Ctrl+C quit\n\n") catch {};

        writer.writeAll("Browser\n") catch {};
        const table_view = self.table.view(allocator) catch "[table render failed]";
        writer.writeAll(table_view) catch {};
        writer.writeAll("\n\n") catch {};

        writer.writeAll("Value Inspector\n") catch {};
        const inspector = self.renderInspector(allocator) catch "[inspector render failed]";
        writer.writeAll(inspector) catch {};
        writer.writeAll("\n\n") catch {};

        writer.writeAll("Recent Log\n") catch {};
        const start_idx = if (self.logs.items.len > max_log_lines)
            self.logs.items.len - max_log_lines
        else
            0;
        for (self.logs.items[start_idx..]) |line| {
            writer.print("- {s}\n", .{line}) catch {};
        }

        writer.writeAll("\nCommand\n") catch {};
        const input_view = self.input.view(allocator) catch "[input render failed]";
        writer.writeAll(input_view) catch {};

        return out.toOwnedSlice() catch "render failed";
    }

    fn renderInspector(self: *const Model, allocator: Allocator) ![]u8 {
        var out = std.array_list.Managed(u8).init(allocator);
        errdefer out.deinit();
        const writer = out.writer();

        if (self.inspector_key == null or self.inspector_value == null or self.inspector_version == null) {
            try writer.writeAll("No value loaded. Use `get <key>` or `open` from browser.");
            return out.toOwnedSlice();
        }

        const key = self.inspector_key.?;
        const value = self.inspector_value.?;
        const version = self.inspector_version.?;

        try writer.print("Key: {s}\n", .{key});
        try writer.print("Version: {d}\n", .{version});
        try writer.print("Bytes: {d}\n", .{value.len});

        if (isPrintableAscii(value)) {
            const shown = @min(value.len, 120);
            if (value.len > shown) {
                try writer.print("Text: \"{s}...\"\n", .{value[0..shown]});
            } else {
                try writer.print("Text: \"{s}\"\n", .{value});
            }
        } else {
            try writer.writeAll("Text: <binary>\n");
        }

        const shown_hex = @min(value.len, 48);
        const hex = try allocHexLower(allocator, value[0..shown_hex]);
        defer allocator.free(hex);
        if (value.len > shown_hex) {
            try writer.print("Hex: 0x{s}...\n", .{hex});
        } else {
            try writer.print("Hex: 0x{s}\n", .{hex});
        }

        return out.toOwnedSlice();
    }

    fn handleKey(self: *Model, key: zz.KeyEvent) ?zz.Cmd(Msg) {
        if (key.modifiers.ctrl) {
            switch (key.key) {
                .char => |c| if (c == 'l') {
                    self.clearLogs();
                    self.setStatus("logs cleared");
                    return .none;
                },
                else => {},
            }
        }

        if (self.mode == .repl) {
            switch (key.key) {
                .tab => {
                    self.applyAutocomplete();
                    return .none;
                },
                .up => {
                    self.historyPrev();
                    return .none;
                },
                .down => {
                    self.historyNext();
                    return .none;
                },
                .enter => {
                    self.submitInput();
                    return .none;
                },
                else => {},
            }

            if (self.history_cursor != null and keyMutatesInput(key)) {
                self.history_cursor = null;
                self.clearHistoryDraft();
            }
            self.input.handleKey(key);
            return .none;
        }

        switch (key.key) {
            .tab => {
                self.toggleMode();
                return .none;
            },
            .enter => {
                self.openSelectedEntry();
                return .none;
            },
            .char => |c| if (!key.modifiers.any() and c == 'q') {
                self.want_quit = true;
                return .quit;
            } else {
                self.table.handleKey(key);
            },
            else => self.table.handleKey(key),
        }

        return .none;
    }

    fn toggleMode(self: *Model) void {
        switch (self.mode) {
            .repl => {
                self.mode = .browser;
                self.input.blur();
                self.table.focus();
                self.setStatus("browser mode");
            },
            .browser => {
                self.mode = .repl;
                self.table.blur();
                self.input.focus();
                self.setStatus("repl mode");
            },
        }
    }

    fn openSelectedEntry(self: *Model) void {
        const idx = self.table.selectedRow();
        if (idx >= self.browser_entries.items.len) {
            self.setStatus("no selected entry");
            return;
        }
        const key = self.browser_entries.items[idx].key;
        self.issueGet(key);
    }

    fn submitInput(self: *Model) void {
        const raw = self.input.getValue();
        const line = std.mem.trim(u8, raw, " \t\r\n");
        if (line.len == 0) return;

        self.pushHistory(line);
        self.logFmt("> {s}", .{line});
        self.executeCommand(line);
        _ = self.input.setValue("") catch {};
    }

    fn pushHistory(self: *Model, line: []const u8) void {
        if (line.len == 0) return;

        if (self.history.items.len > 0) {
            const last = self.history.items[self.history.items.len - 1];
            if (std.mem.eql(u8, last, line)) {
                self.history_cursor = null;
                self.clearHistoryDraft();
                return;
            }
        }

        const owned = self.allocator.dupe(u8, line) catch {
            self.setStatus("history: out of memory");
            return;
        };
        self.history.append(self.allocator, owned) catch {
            self.allocator.free(owned);
            self.setStatus("history: out of memory");
            return;
        };

        if (self.history.items.len > max_history_items) {
            const removed = self.history.orderedRemove(0);
            self.allocator.free(removed);
        }

        self.history_cursor = null;
        self.clearHistoryDraft();
    }

    fn clearHistoryDraft(self: *Model) void {
        if (self.history_draft) |draft| {
            self.allocator.free(draft);
            self.history_draft = null;
        }
    }

    fn historyPrev(self: *Model) void {
        if (self.history.items.len == 0) return;

        if (self.history_cursor == null) {
            self.clearHistoryDraft();
            self.history_draft = self.allocator.dupe(u8, self.input.getValue()) catch null;
            self.history_cursor = self.history.items.len - 1;
        } else if (self.history_cursor.? > 0) {
            self.history_cursor = self.history_cursor.? - 1;
        }

        const idx = self.history_cursor orelse return;
        _ = self.input.setValue(self.history.items[idx]) catch {};
    }

    fn historyNext(self: *Model) void {
        const current = self.history_cursor orelse return;

        if (current + 1 < self.history.items.len) {
            self.history_cursor = current + 1;
            _ = self.input.setValue(self.history.items[self.history_cursor.?]) catch {};
            return;
        }

        self.history_cursor = null;
        if (self.history_draft) |draft| {
            _ = self.input.setValue(draft) catch {};
        } else {
            _ = self.input.setValue("") catch {};
        }
        self.clearHistoryDraft();
    }

    fn applyAutocomplete(self: *Model) void {
        const raw = self.input.getValue();
        const trimmed_right = std.mem.trimRight(u8, raw, " \t");
        if (trimmed_right.len == 0) {
            self.toggleMode();
            return;
        }

        var token_count: usize = 0;
        var first_token: ?[]const u8 = null;
        var token_rest = trimmed_right;
        while (nextToken(&token_rest)) |tok| {
            if (first_token == null) first_token = tok;
            token_count += 1;
        }
        if (token_count == 0) return;

        const trailing_space = trimmed_right.len != raw.len;
        const completing_index = if (trailing_space) token_count else token_count - 1;
        const token_start = if (trailing_space) raw.len else findLastTokenStart(raw);
        const prefix = raw[token_start..];
        const base = raw[0..token_start];

        var matches = std.ArrayListUnmanaged([]const u8){};
        defer matches.deinit(self.allocator);

        if (completing_index == 0) {
            for (command_candidates) |candidate| {
                if (startsWithIgnoreCaseAscii(candidate, prefix)) {
                    matches.append(self.allocator, candidate) catch {
                        self.setStatus("autocomplete: out of memory");
                        return;
                    };
                }
            }
        } else {
            const cmd = first_token orelse return;

            if (std.ascii.eqlIgnoreCase(cmd, "batch") and
                std.mem.indexOfScalar(u8, raw, ';') == null)
            {
                for (batch_op_candidates) |candidate| {
                    if (startsWithIgnoreCaseAscii(candidate, prefix)) {
                        matches.append(self.allocator, candidate) catch {
                            self.setStatus("autocomplete: out of memory");
                            return;
                        };
                    }
                }
            }

            if ((std.ascii.eqlIgnoreCase(cmd, "get") or
                std.ascii.eqlIgnoreCase(cmd, "put") or
                std.ascii.eqlIgnoreCase(cmd, "del") or
                std.ascii.eqlIgnoreCase(cmd, "delete") or
                std.ascii.eqlIgnoreCase(cmd, "ls") or
                std.ascii.eqlIgnoreCase(cmd, "list")) and
                completing_index == 1)
            {
                for (self.browser_entries.items) |entry| {
                    if (!std.mem.startsWith(u8, entry.key, prefix)) continue;

                    var already_present = false;
                    for (matches.items) |existing| {
                        if (std.mem.eql(u8, existing, entry.key)) {
                            already_present = true;
                            break;
                        }
                    }
                    if (already_present) continue;

                    matches.append(self.allocator, entry.key) catch {
                        self.setStatus("autocomplete: out of memory");
                        return;
                    };
                }
            }
        }

        if (matches.items.len == 0) {
            self.setStatus("autocomplete: no matches");
            return;
        }

        const replacement = blk: {
            if (matches.items.len == 1) break :blk matches.items[0];

            const lcp_len = longestCommonPrefixLen(matches.items);
            if (lcp_len <= prefix.len) {
                self.setStatusFmt("autocomplete: {d} matches", .{matches.items.len});
                return;
            }
            break :blk matches.items[0][0..lcp_len];
        };

        var updated = std.array_list.Managed(u8).init(self.allocator);
        defer updated.deinit();

        updated.appendSlice(base) catch {
            self.setStatus("autocomplete: out of memory");
            return;
        };
        updated.appendSlice(replacement) catch {
            self.setStatus("autocomplete: out of memory");
            return;
        };
        if (matches.items.len == 1) {
            updated.append(' ') catch {
                self.setStatus("autocomplete: out of memory");
                return;
            };
        }

        self.input.setValue(updated.items) catch {
            self.setStatus("autocomplete: out of memory");
            return;
        };
        self.history_cursor = null;
        self.clearHistoryDraft();
    }

    fn executeCommand(self: *Model, line: []const u8) void {
        var rest = line;
        const cmd = nextToken(&rest) orelse return;

        if (std.ascii.eqlIgnoreCase(cmd, "help")) {
            self.logFmt("commands: help, ls [prefix] [limit], get <key>, put <key> <value>, del <key>, batch <ops>", .{});
            self.logFmt("          backup [noflush], backups, restore <latest|id> [keep-logs], open [idx], quit", .{});
            self.logFmt("batch ops: put <k> <v>; del <k>; puthex <k> <hex>", .{});
            self.setStatus("help printed");
            return;
        }

        if (std.ascii.eqlIgnoreCase(cmd, "quit") or std.ascii.eqlIgnoreCase(cmd, "exit")) {
            self.want_quit = true;
            return;
        }

        if (std.ascii.eqlIgnoreCase(cmd, "open")) {
            const idx_text = nextToken(&rest);
            const idx = if (idx_text) |text|
                std.fmt.parseInt(usize, text, 10) catch {
                    self.setStatus("open: invalid index");
                    return;
                }
            else
                self.table.selectedRow();

            if (idx >= self.browser_entries.items.len) {
                self.setStatus("open: index out of range");
                return;
            }

            self.issueGet(self.browser_entries.items[idx].key);
            return;
        }

        if (std.ascii.eqlIgnoreCase(cmd, "ls") or std.ascii.eqlIgnoreCase(cmd, "list")) {
            const prefix = nextToken(&rest) orelse "";
            const limit = if (nextToken(&rest)) |limit_text|
                std.fmt.parseInt(u32, limit_text, 10) catch {
                    self.setStatus("ls: invalid limit");
                    return;
                }
            else
                self.browse_limit;

            self.setBrowseDefaults(prefix, limit) catch {
                self.setStatus("ls: failed to store browse defaults");
                return;
            };
            self.issueList(self.browse_prefix, self.browse_limit);
            return;
        }

        if (std.ascii.eqlIgnoreCase(cmd, "get")) {
            const key = nextToken(&rest) orelse {
                self.setStatus("get: missing key");
                return;
            };
            self.issueGet(key);
            return;
        }

        if (std.ascii.eqlIgnoreCase(cmd, "put")) {
            const key = nextToken(&rest) orelse {
                self.setStatus("put: missing key");
                return;
            };
            const value = std.mem.trimLeft(u8, rest, " \t");
            if (value.len == 0) {
                self.setStatus("put: missing value");
                return;
            }

            const ops = self.allocator.alloc(BatchOpOwned, 1) catch {
                self.setStatus("put: out of memory");
                return;
            };
            ops[0] = .{ .put = .{
                .key = self.allocator.dupe(u8, key) catch {
                    self.allocator.free(ops);
                    self.setStatus("put: out of memory");
                    return;
                },
                .value = self.allocator.dupe(u8, value) catch {
                    self.allocator.free(ops[0].put.key);
                    self.allocator.free(ops);
                    self.setStatus("put: out of memory");
                    return;
                },
            } };

            self.issueWriteBatchOwned(ops);
            return;
        }

        if (std.ascii.eqlIgnoreCase(cmd, "del") or std.ascii.eqlIgnoreCase(cmd, "delete")) {
            const key = nextToken(&rest) orelse {
                self.setStatus("del: missing key");
                return;
            };

            const ops = self.allocator.alloc(BatchOpOwned, 1) catch {
                self.setStatus("del: out of memory");
                return;
            };
            ops[0] = .{ .delete = .{
                .key = self.allocator.dupe(u8, key) catch {
                    self.allocator.free(ops);
                    self.setStatus("del: out of memory");
                    return;
                },
            } };

            self.issueWriteBatchOwned(ops);
            return;
        }

        if (std.ascii.eqlIgnoreCase(cmd, "backup")) {
            var flush_before_backup = true;
            if (nextToken(&rest)) |flag| {
                if (std.ascii.eqlIgnoreCase(flag, "noflush")) {
                    flush_before_backup = false;
                } else {
                    self.setStatus("backup: expected optional `noflush`");
                    return;
                }
            }

            self.issueCreateBackup(flush_before_backup);
            return;
        }

        if (std.ascii.eqlIgnoreCase(cmd, "backups")) {
            self.issueListBackups();
            return;
        }

        if (std.ascii.eqlIgnoreCase(cmd, "restore")) {
            const target = nextToken(&rest) orelse {
                self.setStatus("restore: expected `latest` or backup id");
                return;
            };

            const backup_id: u32 = if (std.ascii.eqlIgnoreCase(target, "latest"))
                0
            else
                std.fmt.parseInt(u32, target, 10) catch {
                    self.setStatus("restore: invalid backup id");
                    return;
                };

            var keep_log_files = false;
            if (nextToken(&rest)) |flag| {
                if (std.ascii.eqlIgnoreCase(flag, "keep-logs")) {
                    keep_log_files = true;
                } else {
                    self.setStatus("restore: expected optional `keep-logs`");
                    return;
                }
            }

            self.issueRestoreFromBackup(backup_id, keep_log_files);
            return;
        }

        if (std.ascii.eqlIgnoreCase(cmd, "batch")) {
            const batch_spec = std.mem.trimLeft(u8, rest, " \t");
            if (batch_spec.len == 0) {
                self.setStatus("batch: expected operations");
                return;
            }

            const ops = self.parseBatchSpec(batch_spec) catch |err| {
                self.setStatusFmt("batch parse failed: {s}", .{@errorName(err)});
                return;
            };

            self.issueWriteBatchOwned(ops);
            return;
        }

        self.setStatusFmt("unknown command: {s}", .{cmd});
    }

    fn parseBatchSpec(self: *Model, spec: []const u8) ![]BatchOpOwned {
        var ops = std.ArrayListUnmanaged(BatchOpOwned){};
        errdefer {
            for (ops.items) |op| self.freeBatchOp(op);
            ops.deinit(self.allocator);
        }

        var segments = std.mem.splitScalar(u8, spec, ';');
        while (segments.next()) |raw_segment| {
            const segment = std.mem.trim(u8, raw_segment, " \t\r\n");
            if (segment.len == 0) continue;

            var rest = segment;
            const op_name = nextToken(&rest) orelse continue;

            if (std.ascii.eqlIgnoreCase(op_name, "put")) {
                const key = nextToken(&rest) orelse return error.BatchMissingKey;
                const value = std.mem.trimLeft(u8, rest, " \t");
                if (value.len == 0) return error.BatchMissingValue;

                try ops.append(self.allocator, .{ .put = .{
                    .key = try self.allocator.dupe(u8, key),
                    .value = try self.allocator.dupe(u8, value),
                } });
                continue;
            }

            if (std.ascii.eqlIgnoreCase(op_name, "puthex")) {
                const key = nextToken(&rest) orelse return error.BatchMissingKey;
                const hex = std.mem.trimLeft(u8, rest, " \t");
                if (hex.len == 0) return error.BatchMissingValue;

                try ops.append(self.allocator, .{ .put = .{
                    .key = try self.allocator.dupe(u8, key),
                    .value = try parseHexBytes(self.allocator, hex),
                } });
                continue;
            }

            if (std.ascii.eqlIgnoreCase(op_name, "del") or std.ascii.eqlIgnoreCase(op_name, "delete")) {
                const key = nextToken(&rest) orelse return error.BatchMissingKey;
                try ops.append(self.allocator, .{ .delete = .{
                    .key = try self.allocator.dupe(u8, key),
                } });
                continue;
            }

            return error.BatchUnknownOp;
        }

        if (ops.items.len == 0) return error.EmptyBatch;
        return try ops.toOwnedSlice(self.allocator);
    }

    fn setBrowseDefaults(self: *Model, prefix: []const u8, limit: u32) !void {
        self.allocator.free(self.browse_prefix);
        self.browse_prefix = try self.allocator.dupe(u8, prefix);
        self.browse_limit = limit;
    }

    fn startConnection(self: *Model) void {
        const args = g_cli_args orelse {
            self.setStatus("missing startup args");
            return;
        };

        self.runtime = rpc.runtime.Runtime.init(self.allocator) catch |err| {
            self.setStatusFmt("runtime init failed: {s}", .{@errorName(err)});
            return;
        };

        const address = std.net.Address.parseIp4(args.host, args.port) catch |err| {
            self.setStatusFmt("invalid address: {s}", .{@errorName(err)});
            return;
        };

        self.socket = xev.TCP.init(address) catch |err| {
            self.setStatusFmt("socket init failed: {s}", .{@errorName(err)});
            return;
        };

        self.socket.?.connect(&self.runtime.?.loop, &self.connect_completion, address, Model, self, onConnect);
        self.setStatusFmt("connecting to {s}:{d}", .{ args.host, args.port });
    }

    fn pumpRuntime(self: *Model) void {
        if (self.runtime) |*runtime| {
            runtime.run(.no_wait) catch |err| {
                self.setStatusFmt("runtime pump failed: {s}", .{@errorName(err)});
            };
        }
    }

    fn issueGet(self: *Model, key: []const u8) void {
        if (!self.ensureCanSend("get")) return;

        const pending = PendingRequest{ .get = .{
            .key = self.allocator.dupe(u8, key) catch {
                self.setStatus("get: out of memory");
                return;
            },
        } };

        self.pending_request = pending;
        self.pending_rpc = true;

        var client = self.client orelse {
            self.completePending();
            self.setStatus("get: no client");
            return;
        };

        _ = client.callGet(self, buildPendingGet, onGetReturn) catch |err| {
            self.completePending();
            self.setStatusFmt("get send failed: {s}", .{@errorName(err)});
            return;
        };

        self.setStatusFmt("GET {s}", .{key});
    }

    fn issueList(self: *Model, prefix: []const u8, limit: u32) void {
        if (!self.ensureCanSend("list")) return;

        const pending = PendingRequest{ .list = .{
            .prefix = self.allocator.dupe(u8, prefix) catch {
                self.setStatus("list: out of memory");
                return;
            },
            .limit = limit,
        } };

        self.pending_request = pending;
        self.pending_rpc = true;

        var client = self.client orelse {
            self.completePending();
            self.setStatus("list: no client");
            return;
        };

        _ = client.callList(self, buildPendingList, onListReturn) catch |err| {
            self.completePending();
            self.setStatusFmt("list send failed: {s}", .{@errorName(err)});
            return;
        };

        self.setStatusFmt("LIST prefix=\"{s}\" limit={d}", .{ prefix, limit });
    }

    fn issueSubscribe(self: *Model) void {
        if (!self.ensureCanSend("subscribe")) return;

        self.pending_request = .{ .subscribe = {} };
        self.pending_rpc = true;

        var client = self.client orelse {
            self.completePending();
            self.setStatus("subscribe: no client");
            return;
        };

        _ = client.callSubscribe(self, buildPendingSubscribe, onSubscribeReturn) catch |err| {
            self.completePending();
            self.setStatusFmt("subscribe send failed: {s}", .{@errorName(err)});
            self.issueList(self.browse_prefix, self.browse_limit);
            return;
        };

        self.setStatus("subscribing to key change notifications");
    }

    fn issueSetWatchedKeys(self: *Model) void {
        if (!self.connected or self.client == null or self.pending_rpc) return;
        if (!self.subscribed_for_notifications) return;

        self.pending_request = .{ .set_watched_keys = {} };
        self.pending_rpc = true;

        var client = self.client orelse {
            self.completePending();
            return;
        };

        _ = client.callSetWatchedKeys(self, buildPendingSetWatchedKeys, onSetWatchedKeysReturn) catch |err| {
            self.completePending();
            self.setStatusFmt("setWatchedKeys send failed: {s}", .{@errorName(err)});
            return;
        };
    }

    fn issueCreateBackup(self: *Model, flush_before_backup: bool) void {
        if (!self.ensureCanSend("createBackup")) return;

        self.pending_request = .{ .create_backup = .{
            .flush_before_backup = flush_before_backup,
        } };
        self.pending_rpc = true;

        var client = self.client orelse {
            self.completePending();
            self.setStatus("createBackup: no client");
            return;
        };

        _ = client.callCreateBackup(self, buildPendingCreateBackup, onCreateBackupReturn) catch |err| {
            self.completePending();
            self.setStatusFmt("createBackup send failed: {s}", .{@errorName(err)});
            return;
        };

        self.setStatusFmt("CREATE BACKUP flush={}", .{flush_before_backup});
    }

    fn issueListBackups(self: *Model) void {
        if (!self.ensureCanSend("listBackups")) return;

        self.pending_request = .{ .list_backups = {} };
        self.pending_rpc = true;

        var client = self.client orelse {
            self.completePending();
            self.setStatus("listBackups: no client");
            return;
        };

        _ = client.callListBackups(self, null, onListBackupsReturn) catch |err| {
            self.completePending();
            self.setStatusFmt("listBackups send failed: {s}", .{@errorName(err)});
            return;
        };

        self.setStatus("LIST BACKUPS");
    }

    fn issueRestoreFromBackup(self: *Model, backup_id: u32, keep_log_files: bool) void {
        if (!self.ensureCanSend("restoreFromBackup")) return;

        self.pending_request = .{ .restore_from_backup = .{
            .backup_id = backup_id,
            .keep_log_files = keep_log_files,
        } };
        self.pending_rpc = true;

        var client = self.client orelse {
            self.completePending();
            self.setStatus("restoreFromBackup: no client");
            return;
        };

        _ = client.callRestoreFromBackup(self, buildPendingRestoreFromBackup, onRestoreFromBackupReturn) catch |err| {
            self.completePending();
            self.setStatusFmt("restoreFromBackup send failed: {s}", .{@errorName(err)});
            return;
        };

        self.setStatusFmt("RESTORE backupId={d} keepLogFiles={}", .{ backup_id, keep_log_files });
    }

    fn issueWriteBatchOwned(self: *Model, ops: []BatchOpOwned) void {
        if (!self.ensureCanSend("writeBatch")) {
            for (ops) |op| self.freeBatchOp(op);
            self.allocator.free(ops);
            return;
        }

        self.pending_request = .{ .write_batch = .{ .ops = ops } };
        self.pending_rpc = true;

        var client = self.client orelse {
            self.completePending();
            self.setStatus("writeBatch: no client");
            return;
        };

        _ = client.callWriteBatch(self, buildPendingWriteBatch, onWriteBatchReturn) catch |err| {
            self.completePending();
            self.setStatusFmt("writeBatch send failed: {s}", .{@errorName(err)});
            return;
        };

        self.setStatusFmt("WRITE BATCH ops={d}", .{ops.len});
    }

    fn ensureCanSend(self: *Model, name: []const u8) bool {
        if (!self.connected or self.client == null) {
            self.setStatusFmt("{s}: not connected", .{name});
            return false;
        }
        if (self.pending_rpc) {
            self.setStatus("another RPC is in flight");
            return false;
        }
        return true;
    }

    fn clearPendingRequest(self: *Model) void {
        if (self.pending_request) |pending| {
            switch (pending) {
                .get => |req| {
                    self.allocator.free(req.key);
                },
                .list => |req| {
                    self.allocator.free(req.prefix);
                },
                .subscribe => {},
                .set_watched_keys => {},
                .create_backup => {},
                .list_backups => {},
                .restore_from_backup => {},
                .write_batch => |req| {
                    for (req.ops) |op| self.freeBatchOp(op);
                    self.allocator.free(req.ops);
                },
            }
            self.pending_request = null;
        }
    }

    fn completePending(self: *Model) void {
        self.pending_rpc = false;
        self.clearPendingRequest();
        self.maybeRunPendingRemoteReset();
        self.maybeRunPendingWatchedKeysSync();
    }

    fn maybeRunPendingRemoteReset(self: *Model) void {
        const pending = self.pending_remote_reset orelse return;
        if (self.pending_rpc) return;

        if (!self.connected or self.client == null) {
            self.pending_remote_reset = null;
            return;
        }

        self.pending_remote_reset = null;
        self.applyRemoteRestoreReset(pending.restored_backup_id, pending.next_version);
    }

    fn maybeRunPendingWatchedKeysSync(self: *Model) void {
        if (!self.pending_sync_watched_keys) return;
        if (self.pending_rpc) return;

        if (!self.connected or self.client == null or !self.subscribed_for_notifications) {
            self.pending_sync_watched_keys = false;
            return;
        }

        self.pending_sync_watched_keys = false;
        self.issueSetWatchedKeys();
    }

    fn freeBatchOp(self: *Model, op: BatchOpOwned) void {
        switch (op) {
            .put => |put| {
                self.allocator.free(put.key);
                self.allocator.free(put.value);
            },
            .delete => |del| {
                self.allocator.free(del.key);
            },
        }
    }

    fn clearBrowserEntries(self: *Model) void {
        for (self.browser_entries.items) |entry| {
            self.allocator.free(entry.key);
            self.allocator.free(entry.version_text);
            self.allocator.free(entry.preview);
        }
        self.browser_entries.clearRetainingCapacity();
        self.table.clearRows();
    }

    fn applyRemoteRestoreReset(self: *Model, restored_backup_id: u32, next_version: u64) void {
        self.clearBrowserEntries();
        self.clearInspector();
        self.pending_sync_watched_keys = false;

        self.logFmt("REMOTE RESTORE -> backup id={d}, nextVersion={d}", .{ restored_backup_id, next_version });

        if (!self.connected or self.client == null) {
            self.setStatusFmt("remote restore id={d}; disconnected", .{restored_backup_id});
            return;
        }

        if (self.pending_rpc) {
            self.pending_remote_reset = .{
                .restored_backup_id = restored_backup_id,
                .next_version = next_version,
            };
            self.setStatusFmt("remote restore id={d}; waiting for RPC", .{restored_backup_id});
            return;
        }

        self.setStatusFmt("remote restore id={d}; reloading", .{restored_backup_id});
        self.issueList(self.browse_prefix, self.browse_limit);
    }

    fn findBrowserEntryIndex(self: *const Model, key: []const u8) ?usize {
        for (self.browser_entries.items, 0..) |entry, idx| {
            if (std.mem.eql(u8, entry.key, key)) return idx;
        }
        return null;
    }

    fn selectedBrowserKeyDup(self: *Model) ?[]u8 {
        if (self.browser_entries.items.len == 0) return null;
        const idx = self.table.selectedRow();
        if (idx >= self.browser_entries.items.len) return null;
        return self.allocator.dupe(u8, self.browser_entries.items[idx].key) catch null;
    }

    fn rebuildBrowserTableFromEntries(self: *Model, selected_key: ?[]const u8) !void {
        self.table.clearRows();
        for (self.browser_entries.items) |entry| {
            try self.table.addRow(.{ entry.key, entry.version_text, entry.preview });
        }

        if (self.browser_entries.items.len == 0) {
            self.table.cursor_row = 0;
            self.table.y_offset = 0;
            return;
        }

        var target_idx: usize = @min(self.table.cursor_row, self.browser_entries.items.len - 1);
        if (selected_key) |selected| {
            if (self.findBrowserEntryIndex(selected)) |idx| {
                target_idx = idx;
            }
        }

        self.table.cursor_row = target_idx;
        self.table.y_offset = 0;
    }

    fn updateBrowserEntryValue(self: *Model, idx: usize, version: u64, value: []const u8) !void {
        const version_text = try std.fmt.allocPrint(self.allocator, "{d}", .{version});
        errdefer self.allocator.free(version_text);
        const preview = try formatValuePreview(self.allocator, value, 32);
        errdefer self.allocator.free(preview);

        self.allocator.free(self.browser_entries.items[idx].version_text);
        self.allocator.free(self.browser_entries.items[idx].preview);

        self.browser_entries.items[idx].version = version;
        self.browser_entries.items[idx].version_text = version_text;
        self.browser_entries.items[idx].preview = preview;
    }

    fn removeBrowserEntryAt(self: *Model, idx: usize) void {
        const removed = self.browser_entries.orderedRemove(idx);
        self.allocator.free(removed.key);
        self.allocator.free(removed.version_text);
        self.allocator.free(removed.preview);
    }

    fn makeBrowserEntry(self: *Model, key: []const u8, version: u64, value: []const u8) !BrowserEntry {
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        const version_text = try std.fmt.allocPrint(self.allocator, "{d}", .{version});
        errdefer self.allocator.free(version_text);
        const preview = try formatValuePreview(self.allocator, value, 32);
        errdefer self.allocator.free(preview);

        return .{
            .key = owned_key,
            .version = version,
            .version_text = version_text,
            .preview = preview,
        };
    }

    fn tryInsertVisibleBrowserEntry(self: *Model, key: []const u8, version: u64, value: []const u8) !bool {
        if (!std.mem.startsWith(u8, key, self.browse_prefix)) return false;
        const limit: usize = @intCast(self.browse_limit);
        if (limit == 0) return false;

        var insert_idx: usize = self.browser_entries.items.len;
        for (self.browser_entries.items, 0..) |entry, idx| {
            if (std.mem.order(u8, key, entry.key) == .lt) {
                insert_idx = idx;
                break;
            }
        }

        if (self.browser_entries.items.len >= limit and insert_idx >= self.browser_entries.items.len) {
            return false;
        }

        const browser_entry = try self.makeBrowserEntry(key, version, value);
        errdefer {
            self.allocator.free(browser_entry.key);
            self.allocator.free(browser_entry.version_text);
            self.allocator.free(browser_entry.preview);
        }

        try self.browser_entries.insert(self.allocator, insert_idx, browser_entry);

        if (self.browser_entries.items.len > limit) {
            const dropped = self.browser_entries.pop().?;
            self.allocator.free(dropped.key);
            self.allocator.free(dropped.version_text);
            self.allocator.free(dropped.preview);
        }

        return true;
    }

    fn applyWriteOpResultsToBrowser(self: *Model, changes: anytype) !usize {
        if (changes.len() == 0) return 0;

        const selected_key = self.selectedBrowserKeyDup();
        defer if (selected_key) |key| self.allocator.free(key);

        var applied_count: usize = 0;

        for (0..changes.len()) |idx_usize| {
            const idx: u32 = @intCast(idx_usize);
            const change = changes.get(idx) catch continue;
            const key = change.getKey() catch continue;
            const which = change.which() catch continue;

            switch (which) {
                .put => {
                    const put = change.getPut() catch continue;
                    const value = put.getValue() catch continue;
                    const version = put.getVersion() catch continue;

                    if (self.findBrowserEntryIndex(key)) |entry_idx| {
                        self.updateBrowserEntryValue(entry_idx, version, value) catch continue;
                        applied_count += 1;
                    } else {
                        const inserted = self.tryInsertVisibleBrowserEntry(key, version, value) catch continue;
                        if (inserted) applied_count += 1;
                    }

                    if (self.inspector_key) |inspector_key| {
                        if (std.mem.eql(u8, inspector_key, key)) {
                            self.setInspector(key, version, value);
                        }
                    }
                },
                .delete => {
                    _ = change.getDelete() catch false;
                    const entry_idx = self.findBrowserEntryIndex(key) orelse continue;
                    self.removeBrowserEntryAt(entry_idx);
                    applied_count += 1;

                    if (self.inspector_key) |inspector_key| {
                        if (std.mem.eql(u8, inspector_key, key)) {
                            self.clearInspector();
                        }
                    }
                },
            }
        }

        if (applied_count == 0) return 0;

        try self.rebuildBrowserTableFromEntries(if (selected_key) |key| key else null);

        if (self.subscribed_for_notifications) {
            self.pending_sync_watched_keys = true;
            self.maybeRunPendingWatchedKeysSync();
        }

        return applied_count;
    }

    fn reloadBrowserFromList(self: *Model, entries: kvstore.ListResults.Reader) !void {
        self.clearBrowserEntries();

        const values = try entries.getEntries();
        const count = values.len();

        for (0..count) |idx_usize| {
            const idx: u32 = @intCast(idx_usize);
            const entry = try values.get(idx);
            const key = try entry.getKey();
            const value = try entry.getValue();
            const version = try entry.getVersion();

            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);

            const version_text = try std.fmt.allocPrint(self.allocator, "{d}", .{version});
            errdefer self.allocator.free(version_text);

            const preview = try formatValuePreview(self.allocator, value, 32);
            errdefer self.allocator.free(preview);

            const browser_entry = BrowserEntry{
                .key = owned_key,
                .version = version,
                .version_text = version_text,
                .preview = preview,
            };

            try self.browser_entries.append(self.allocator, browser_entry);
        }

        try self.rebuildBrowserTableFromEntries(null);
    }

    fn setInspector(self: *Model, key: []const u8, version: u64, value: []const u8) void {
        self.clearInspector();

        const owned_key = self.allocator.dupe(u8, key) catch {
            self.setStatus("inspector: out of memory");
            return;
        };
        const owned_value = self.allocator.dupe(u8, value) catch {
            self.allocator.free(owned_key);
            self.setStatus("inspector: out of memory");
            return;
        };

        self.inspector_key = owned_key;
        self.inspector_value = owned_value;
        self.inspector_version = version;
    }

    fn clearInspector(self: *Model) void {
        if (self.inspector_key) |key| {
            self.allocator.free(key);
            self.inspector_key = null;
        }
        if (self.inspector_value) |value| {
            self.allocator.free(value);
            self.inspector_value = null;
        }
        self.inspector_version = null;
    }

    fn setStatus(self: *Model, text: []const u8) void {
        const owned = self.allocator.dupe(u8, text) catch return;
        if (self.status_text) |old| self.allocator.free(old);
        self.status_text = owned;
    }

    fn setStatusFmt(self: *Model, comptime fmt: []const u8, args: anytype) void {
        const owned = std.fmt.allocPrint(self.allocator, fmt, args) catch return;
        if (self.status_text) |old| self.allocator.free(old);
        self.status_text = owned;
    }

    fn logFmt(self: *Model, comptime fmt: []const u8, args: anytype) void {
        const line = std.fmt.allocPrint(self.allocator, fmt, args) catch return;
        self.logs.append(self.allocator, line) catch {
            self.allocator.free(line);
            return;
        };

        while (self.logs.items.len > max_log_lines * 2) {
            const removed = self.logs.orderedRemove(0);
            self.allocator.free(removed);
        }
    }

    fn clearLogs(self: *Model) void {
        for (self.logs.items) |line| self.allocator.free(line);
        self.logs.clearRetainingCapacity();
    }
};

fn nextToken(rest: *[]const u8) ?[]const u8 {
    const trimmed = std.mem.trimLeft(u8, rest.*, " \t");
    if (trimmed.len == 0) {
        rest.* = trimmed;
        return null;
    }

    var end: usize = 0;
    while (end < trimmed.len and trimmed[end] != ' ' and trimmed[end] != '\t') : (end += 1) {}

    rest.* = trimmed[end..];
    return trimmed[0..end];
}

fn startsWithIgnoreCaseAscii(haystack: []const u8, prefix: []const u8) bool {
    if (prefix.len > haystack.len) return false;
    for (prefix, 0..) |p, idx| {
        if (std.ascii.toLower(p) != std.ascii.toLower(haystack[idx])) return false;
    }
    return true;
}

fn findLastTokenStart(text: []const u8) usize {
    if (text.len == 0) return 0;

    var idx: usize = text.len;
    while (idx > 0) : (idx -= 1) {
        const ch = text[idx - 1];
        if (ch == ' ' or ch == '\t') return idx;
    }
    return 0;
}

fn longestCommonPrefixLen(items: []const []const u8) usize {
    if (items.len == 0) return 0;

    var limit = items[0].len;
    for (items[1..]) |item| {
        limit = @min(limit, item.len);
    }

    var idx: usize = 0;
    while (idx < limit) : (idx += 1) {
        const b = items[0][idx];
        for (items[1..]) |item| {
            if (item[idx] != b) return idx;
        }
    }
    return limit;
}

fn keyMutatesInput(key: zz.KeyEvent) bool {
    if (key.modifiers.alt or key.modifiers.super) return false;

    if (key.modifiers.ctrl) {
        return switch (key.key) {
            .char => |c| c == 'k' or c == 'u' or c == 'w',
            else => false,
        };
    }

    return switch (key.key) {
        .char, .backspace, .delete, .paste => true,
        else => false,
    };
}

fn parseHexBytes(allocator: Allocator, text: []const u8) ![]u8 {
    const hex = if (std.mem.startsWith(u8, text, "0x") or std.mem.startsWith(u8, text, "0X"))
        text[2..]
    else
        text;

    if (hex.len == 0) return error.EmptyHex;
    if (hex.len % 2 != 0) return error.InvalidHexLength;

    const out = try allocator.alloc(u8, hex.len / 2);
    errdefer allocator.free(out);

    for (0..out.len) |idx| {
        const start = idx * 2;
        out[idx] = try std.fmt.parseInt(u8, hex[start .. start + 2], 16);
    }

    return out;
}

fn allocHexLower(allocator: Allocator, bytes: []const u8) ![]u8 {
    const lut = "0123456789abcdef";
    const out = try allocator.alloc(u8, bytes.len * 2);
    for (bytes, 0..) |b, idx| {
        out[idx * 2] = lut[b >> 4];
        out[idx * 2 + 1] = lut[b & 0x0f];
    }
    return out;
}

fn isPrintableAscii(bytes: []const u8) bool {
    for (bytes) |b| {
        if (b < 32 or b > 126) return false;
    }
    return true;
}

fn formatValuePreview(allocator: Allocator, value: []const u8, max_len: usize) ![]u8 {
    if (isPrintableAscii(value)) {
        const shown = @min(value.len, max_len);
        if (value.len > shown) {
            return std.fmt.allocPrint(allocator, "\"{s}...\" ({d} bytes)", .{ value[0..shown], value.len });
        }
        return std.fmt.allocPrint(allocator, "\"{s}\" ({d} bytes)", .{ value, value.len });
    }

    const shown = @min(value.len, max_len / 2);
    const hex = try allocHexLower(allocator, value[0..shown]);
    defer allocator.free(hex);

    if (value.len > shown) {
        return std.fmt.allocPrint(allocator, "0x{s}... ({d} bytes)", .{ hex, value.len });
    }
    return std.fmt.allocPrint(allocator, "0x{s} ({d} bytes)", .{ hex, value.len });
}

fn buildPendingGet(ctx_ptr: *anyopaque, params: *KvStore.Get.Params.Builder) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));
    const pending = model.pending_request orelse return error.MissingPendingRequest;
    const req = switch (pending) {
        .get => |get_req| get_req,
        else => return error.UnexpectedPendingRequest,
    };
    try params.setKey(req.key);
}

fn onGetReturn(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.Get.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));
    defer model.completePending();

    switch (response) {
        .results => |results| {
            const found = results.getFound() catch |err| {
                model.setStatusFmt("get decode failed: {s}", .{@errorName(err)});
                return;
            };

            if (!found) {
                model.clearInspector();
                model.logFmt("GET -> not found", .{});
                model.setStatus("get: key not found");
                return;
            }

            const entry = results.getEntry() catch |err| {
                model.setStatusFmt("get decode failed: {s}", .{@errorName(err)});
                return;
            };

            const key = entry.getKey() catch |err| {
                model.setStatusFmt("get decode failed: {s}", .{@errorName(err)});
                return;
            };
            const value = entry.getValue() catch |err| {
                model.setStatusFmt("get decode failed: {s}", .{@errorName(err)});
                return;
            };
            const version = entry.getVersion() catch |err| {
                model.setStatusFmt("get decode failed: {s}", .{@errorName(err)});
                return;
            };

            model.setInspector(key, version, value);
            model.logFmt("GET \"{s}\" -> version {d}", .{ key, version });
            model.setStatusFmt("get: found \"{s}\"", .{key});
        },
        .exception => |ex| {
            model.setStatusFmt("get exception: {s}", .{ex.reason});
        },
        .canceled => {
            model.setStatus("get canceled");
        },
        else => {
            model.setStatus("get unexpected response");
        },
    }
}

fn buildPendingList(ctx_ptr: *anyopaque, params: *KvStore.List.Params.Builder) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));
    const pending = model.pending_request orelse return error.MissingPendingRequest;
    const req = switch (pending) {
        .list => |list_req| list_req,
        else => return error.UnexpectedPendingRequest,
    };

    try params.setPrefix(req.prefix);
    try params.setLimit(req.limit);
}

fn onListReturn(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.List.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));
    defer model.completePending();

    switch (response) {
        .results => |results| {
            model.reloadBrowserFromList(results) catch |err| {
                model.setStatusFmt("list decode failed: {s}", .{@errorName(err)});
                return;
            };
            if (model.subscribed_for_notifications) {
                model.pending_sync_watched_keys = true;
            }
            model.logFmt("LIST -> {d} entries", .{model.browser_entries.items.len});
            model.setStatusFmt("list loaded {d} entries", .{model.browser_entries.items.len});
        },
        .exception => |ex| {
            model.setStatusFmt("list exception: {s}", .{ex.reason});
        },
        .canceled => {
            model.setStatus("list canceled");
        },
        else => {
            model.setStatus("list unexpected response");
        },
    }
}

fn buildPendingSubscribe(ctx_ptr: *anyopaque, params: *KvStore.Subscribe.Params.Builder) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));
    const pending = model.pending_request orelse return error.MissingPendingRequest;
    switch (pending) {
        .subscribe => {},
        else => return error.UnexpectedPendingRequest,
    }

    const peer = model.peer orelse return error.MissingPeer;
    try params.setNotifierServer(peer, &model.notifier_server);
}

fn onSubscribeReturn(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.Subscribe.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));

    switch (response) {
        .results => {
            model.subscribed_for_notifications = true;
            model.logFmt("SUBSCRIBE -> ok", .{});
            model.setStatus("subscribed to key changes");
        },
        .exception => |ex| {
            model.subscribed_for_notifications = false;
            model.setStatusFmt("subscribe exception: {s}", .{ex.reason});
        },
        .canceled => {
            model.subscribed_for_notifications = false;
            model.setStatus("subscribe canceled");
        },
        else => {
            model.subscribed_for_notifications = false;
            model.setStatus("subscribe unexpected response");
        },
    }

    model.completePending();
    model.issueList(model.browse_prefix, model.browse_limit);
}

fn buildPendingSetWatchedKeys(
    ctx_ptr: *anyopaque,
    params: *KvStore.SetWatchedKeys.Params.Builder,
) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));
    const pending = model.pending_request orelse return error.MissingPendingRequest;
    switch (pending) {
        .set_watched_keys => {},
        else => return error.UnexpectedPendingRequest,
    }

    var out_keys = try params.initKeys(@intCast(model.browser_entries.items.len));
    for (model.browser_entries.items, 0..) |entry, idx| {
        try out_keys.set(@intCast(idx), entry.key);
    }
}

fn onSetWatchedKeysReturn(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.SetWatchedKeys.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));
    defer model.completePending();

    switch (response) {
        .results => {},
        .exception => |ex| {
            model.setStatusFmt("setWatchedKeys exception: {s}", .{ex.reason});
        },
        .canceled => {
            model.setStatus("setWatchedKeys canceled");
        },
        else => {
            model.setStatus("setWatchedKeys unexpected response");
        },
    }
}

fn buildPendingCreateBackup(
    ctx_ptr: *anyopaque,
    params: *KvStore.CreateBackup.Params.Builder,
) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));
    const pending = model.pending_request orelse return error.MissingPendingRequest;
    const req = switch (pending) {
        .create_backup => |create_backup_req| create_backup_req,
        else => return error.UnexpectedPendingRequest,
    };

    try params.setFlushBeforeBackup(req.flush_before_backup);
}

fn onCreateBackupReturn(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.CreateBackup.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));
    defer model.completePending();

    switch (response) {
        .results => |results| {
            const backup = results.getBackup() catch |err| {
                model.setStatusFmt("createBackup decode failed: {s}", .{@errorName(err)});
                return;
            };
            const backup_id = backup.getBackupId() catch |err| {
                model.setStatusFmt("createBackup decode failed: {s}", .{@errorName(err)});
                return;
            };
            const timestamp = backup.getTimestamp() catch |err| {
                model.setStatusFmt("createBackup decode failed: {s}", .{@errorName(err)});
                return;
            };
            const size = backup.getSize() catch |err| {
                model.setStatusFmt("createBackup decode failed: {s}", .{@errorName(err)});
                return;
            };
            const num_files = backup.getNumFiles() catch |err| {
                model.setStatusFmt("createBackup decode failed: {s}", .{@errorName(err)});
                return;
            };
            const backup_count = results.getBackupCount() catch |err| {
                model.setStatusFmt("createBackup decode failed: {s}", .{@errorName(err)});
                return;
            };

            model.logFmt(
                "BACKUP id={d} ts={d} size={d} files={d} count={d}",
                .{ backup_id, timestamp, size, num_files, backup_count },
            );
            model.setStatusFmt("backup created id={d} count={d}", .{ backup_id, backup_count });
        },
        .exception => |ex| {
            model.setStatusFmt("createBackup exception: {s}", .{ex.reason});
        },
        .canceled => {
            model.setStatus("createBackup canceled");
        },
        else => {
            model.setStatus("createBackup unexpected response");
        },
    }
}

fn onListBackupsReturn(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.ListBackups.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));
    defer model.completePending();

    switch (response) {
        .results => |results| {
            const backups = results.getBackups() catch |err| {
                model.setStatusFmt("listBackups decode failed: {s}", .{@errorName(err)});
                return;
            };

            model.logFmt("BACKUPS -> {d} backup(s)", .{backups.len()});
            for (0..backups.len()) |idx_usize| {
                const idx: u32 = @intCast(idx_usize);
                const backup = backups.get(idx) catch continue;
                const backup_id = backup.getBackupId() catch continue;
                const timestamp = backup.getTimestamp() catch continue;
                const size = backup.getSize() catch continue;
                const num_files = backup.getNumFiles() catch continue;
                model.logFmt(
                    "  id={d} ts={d} size={d} files={d}",
                    .{ backup_id, timestamp, size, num_files },
                );
            }
            model.setStatusFmt("listed {d} backup(s)", .{backups.len()});
        },
        .exception => |ex| {
            model.setStatusFmt("listBackups exception: {s}", .{ex.reason});
        },
        .canceled => {
            model.setStatus("listBackups canceled");
        },
        else => {
            model.setStatus("listBackups unexpected response");
        },
    }
}

fn buildPendingRestoreFromBackup(
    ctx_ptr: *anyopaque,
    params: *KvStore.RestoreFromBackup.Params.Builder,
) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));
    const pending = model.pending_request orelse return error.MissingPendingRequest;
    const req = switch (pending) {
        .restore_from_backup => |restore_req| restore_req,
        else => return error.UnexpectedPendingRequest,
    };

    try params.setBackupId(req.backup_id);
    try params.setKeepLogFiles(req.keep_log_files);
}

fn onRestoreFromBackupReturn(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.RestoreFromBackup.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));

    switch (response) {
        .results => |results| {
            const restored_backup_id = results.getRestoredBackupId() catch |err| {
                model.completePending();
                model.setStatusFmt("restoreFromBackup decode failed: {s}", .{@errorName(err)});
                return;
            };
            const next_version = results.getNextVersion() catch |err| {
                model.completePending();
                model.setStatusFmt("restoreFromBackup decode failed: {s}", .{@errorName(err)});
                return;
            };

            model.completePending();
            model.clearInspector();
            model.logFmt("RESTORE -> backup id={d}, nextVersion={d}", .{ restored_backup_id, next_version });
            model.setStatusFmt("restored backup id={d}", .{restored_backup_id});
            model.issueList(model.browse_prefix, model.browse_limit);
        },
        .exception => |ex| {
            model.completePending();
            model.setStatusFmt("restoreFromBackup exception: {s}", .{ex.reason});
        },
        .canceled => {
            model.completePending();
            model.setStatus("restoreFromBackup canceled");
        },
        else => {
            model.completePending();
            model.setStatus("restoreFromBackup unexpected response");
        },
    }
}

fn buildPendingWriteBatch(ctx_ptr: *anyopaque, params: *KvStore.WriteBatch.Params.Builder) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));
    const pending = model.pending_request orelse return error.MissingPendingRequest;
    const req = switch (pending) {
        .write_batch => |write_req| write_req,
        else => return error.UnexpectedPendingRequest,
    };

    var ops_builder = try params.initOps(@intCast(req.ops.len));
    for (req.ops, 0..) |op, idx| {
        var out = try ops_builder.get(@intCast(idx));
        switch (op) {
            .put => |put| {
                try out.setKey(put.key);
                try out.setPut(put.value);
            },
            .delete => |del| {
                try out.setKey(del.key);
                try out.setDelete({});
            },
        }
    }
}

fn onWriteBatchReturn(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.WriteBatch.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));

    switch (response) {
        .results => |results| {
            const applied = results.getApplied() catch |err| {
                model.completePending();
                model.setStatusFmt("writeBatch decode failed: {s}", .{@errorName(err)});
                return;
            };
            const next_version = results.getNextVersion() catch |err| {
                model.completePending();
                model.setStatusFmt("writeBatch decode failed: {s}", .{@errorName(err)});
                return;
            };
            const out_results = results.getResults() catch |err| {
                model.completePending();
                model.setStatusFmt("writeBatch decode failed: {s}", .{@errorName(err)});
                return;
            };

            for (0..out_results.len()) |idx_usize| {
                const idx: u32 = @intCast(idx_usize);
                const op = out_results.get(idx) catch continue;
                const key = op.getKey() catch continue;
                const which = op.which() catch continue;

                switch (which) {
                    .put => {
                        const entry = op.getPut() catch continue;
                        const version = entry.getVersion() catch continue;
                        model.logFmt("BATCH PUT \"{s}\" -> version {d}", .{ key, version });
                    },
                    .delete => {
                        const found = op.getDelete() catch false;
                        model.logFmt("BATCH DELETE \"{s}\" -> found={}", .{ key, found });
                    },
                }
            }

            const updated_visible = model.applyWriteOpResultsToBrowser(out_results) catch |err| {
                model.completePending();
                model.setStatusFmt("writeBatch apply failed: {s}", .{@errorName(err)});
                return;
            };

            model.completePending();
            model.setStatusFmt("writeBatch applied={d} nextVersion={d} visibleUpdated={d}", .{
                applied,
                next_version,
                updated_visible,
            });
        },
        .exception => |ex| {
            model.completePending();
            model.setStatusFmt("writeBatch exception: {s}", .{ex.reason});
        },
        .canceled => {
            model.completePending();
            model.setStatus("writeBatch canceled");
        },
        else => {
            model.completePending();
            model.setStatus("writeBatch unexpected response");
        },
    }
}

fn onBootstrap(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: KvStore.BootstrapResponse,
) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));

    switch (response) {
        .client => |client| {
            model.client = client;
            model.connected = true;
            model.subscribed_for_notifications = false;
            model.setStatus("connected");
            model.logFmt("bootstrap complete", .{});
            model.issueSubscribe();
        },
        .exception => |ex| {
            model.setStatusFmt("bootstrap exception: {s}", .{ex.reason});
        },
        else => {
            model.setStatus("bootstrap unexpected response");
        },
    }
}

fn onNotifierKeysChanged(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: KvClientNotifier.KeysChanged.Params.Reader,
    _: *KvClientNotifier.KeysChanged.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));
    if (!model.connected) return;
    const changes = try params.getChanges();
    const applied_count = model.applyWriteOpResultsToBrowser(changes) catch |err| {
        model.setStatusFmt("notify apply failed: {s}", .{@errorName(err)});
        return;
    };
    if (applied_count == 0) return;

    model.logFmt("NOTIFY -> applied {d} update(s)", .{applied_count});
    model.setStatusFmt("applied {d} remote update(s)", .{applied_count});
}

fn onNotifierStateResetRequired(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: KvClientNotifier.StateResetRequired.Params.Reader,
    _: *KvClientNotifier.StateResetRequired.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const model: *Model = @ptrCast(@alignCast(ctx_ptr));
    if (!model.connected) return;

    const restored_backup_id = try params.getRestoredBackupId();
    const next_version = try params.getNextVersion();

    if (model.pending_rpc) {
        model.pending_remote_reset = .{
            .restored_backup_id = restored_backup_id,
            .next_version = next_version,
        };
        model.logFmt(
            "REMOTE RESTORE -> backup id={d}, nextVersion={d} (queued)",
            .{ restored_backup_id, next_version },
        );
        model.setStatusFmt("remote restore id={d}; waiting for RPC", .{restored_backup_id});
        return;
    }

    model.applyRemoteRestoreReset(restored_backup_id, next_version);
}

fn onPeerError(peer: *rpc.peer.Peer, err: anyerror) void {
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
    if (g_model) |model| {
        model.connected = false;
        model.client = null;
        model.subscribed_for_notifications = false;
        model.pending_sync_watched_keys = false;
        model.pending_remote_reset = null;
        model.setStatusFmt("peer error: {s}", .{@errorName(err)});
    }
}

fn onPeerClose(peer: *rpc.peer.Peer) void {
    const allocator = peer.allocator;
    const conn = peer.takeAttachedConnection(*rpc.connection.Connection);

    peer.deinit();
    allocator.destroy(peer);

    if (conn) |attached| {
        attached.deinit();
        allocator.destroy(attached);
    }

    if (g_model) |model| {
        model.peer = null;
        model.conn = null;
        model.client = null;
        model.connected = false;
        model.subscribed_for_notifications = false;
        model.pending_sync_watched_keys = false;
        model.pending_remote_reset = null;
        model.completePending();
        model.setStatus("peer closed");
    }
}

fn onConnect(
    ctx: ?*Model,
    loop: *xev.Loop,
    _: *xev.Completion,
    socket: xev.TCP,
    res: xev.ConnectError!void,
) xev.CallbackAction {
    const model = ctx orelse return .disarm;

    if (res) |_| {
        const conn = model.allocator.create(rpc.connection.Connection) catch {
            model.setStatus("connect: out of memory for connection");
            return .disarm;
        };

        conn.* = rpc.connection.Connection.init(model.allocator, loop, socket, .{}) catch |err| {
            model.allocator.destroy(conn);
            model.setStatusFmt("connect: connection init failed: {s}", .{@errorName(err)});
            return .disarm;
        };

        const peer = model.allocator.create(rpc.peer.Peer) catch {
            conn.deinit();
            model.allocator.destroy(conn);
            model.setStatus("connect: out of memory for peer");
            return .disarm;
        };

        peer.* = rpc.peer.Peer.init(model.allocator, conn);

        model.conn = conn;
        model.peer = peer;

        peer.start(onPeerError, onPeerClose);

        _ = KvStore.Client.fromBootstrap(peer, model, onBootstrap) catch |err| {
            model.setStatusFmt("connect: bootstrap call failed: {s}", .{@errorName(err)});
        };
    } else |err| {
        model.setStatusFmt("connect failed: {s}", .{@errorName(err)});
    }

    return .disarm;
}

fn parseArgs(allocator: Allocator) !CliArgs {
    var out = CliArgs{};
    var host_text: []const u8 = "127.0.0.1";

    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    var idx: usize = 1;
    while (idx < argv.len) : (idx += 1) {
        const arg = argv[idx];
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            return error.HelpRequested;
        }
        if (std.mem.eql(u8, arg, "--host")) {
            idx += 1;
            if (idx >= argv.len) return error.MissingArgValue;
            host_text = argv[idx];
            continue;
        }
        if (std.mem.eql(u8, arg, "--port")) {
            idx += 1;
            if (idx >= argv.len) return error.MissingArgValue;
            out.port = try std.fmt.parseInt(u16, argv[idx], 10);
            continue;
        }
    }

    out.host = try allocator.dupe(u8, host_text);
    return out;
}

fn usage() void {
    std.debug.print(
        \\Usage: kvstore-client [--host 127.0.0.1] [--port 9000]
        \\
    , .{});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = parseArgs(allocator) catch |err| switch (err) {
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

    g_cli_args = args;
    defer g_cli_args = null;

    var program = try zz.Program(Model).initWithOptions(allocator, .{
        .alt_screen = true,
        .cursor = true,
        .title = "KVStore REPL",
    });
    defer program.deinit();

    try program.run();
}
