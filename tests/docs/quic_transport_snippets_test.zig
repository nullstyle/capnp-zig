const std = @import("std");
const capnpc = @import("capnpc-zig");

const quic = capnpc.rpc.transport.quic;
const rpc_events = capnpc.rpc.events;

const Net = std.Io.net;

const server_cert_pem = "test certificate fixture";
const server_key_pem = "test private key fixture";

fn loopbackAddr(port: u16) Net.IpAddress {
    return .{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port,
    } };
}

fn initServerOptions(
    listen_addr: Net.IpAddress,
    observer: ?rpc_events.Observer,
) quic.ServerOptions {
    return .{
        .listen_addr = listen_addr,
        .tls_cert_pem = server_cert_pem,
        .tls_key_pem = server_key_pem,
        .mode = .native,
        .native = .{
            .inline_frame_threshold = 64 * 1024,
            .max_control_frame_bytes = quic.default_native_max_control_frame_bytes,
            .max_pending_data_streams = 16,
            .max_pending_data_bytes = quic.default_native_max_pending_data_bytes,
        },
        .observer = observer,
    };
}

fn initClientOptions(
    server_addr: Net.IpAddress,
    observer: ?rpc_events.Observer,
) quic.ClientOptions {
    return .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .mode = .native,
        .native = .{
            .inline_frame_threshold = 64 * 1024,
            .max_control_frame_bytes = quic.default_native_max_control_frame_bytes,
            .max_pending_data_streams = 16,
            .max_pending_data_bytes = quic.default_native_max_pending_data_bytes,
        },
        .observer = observer,
    };
}

test "quic transport guide native mode snippets use the public options surface" {
    comptime {
        if (!quic.enabled) @compileError("QUIC transport snippets require -Dquic=true");
        _ = quic.Connection;
        _ = quic.Server;
        _ = quic.ServerSession;
        _ = quic.Listener;
        _ = quic.NativeOptions;
        _ = quic.TransportMode;
    }

    var event_count: usize = 0;
    const ObserverCtx = struct {
        fn onEvent(ctx: *anyopaque, _: rpc_events.Event) void {
            const count: *usize = @ptrCast(@alignCast(ctx));
            count.* += 1;
        }
    };
    const observer = rpc_events.Observer.init(&event_count, ObserverCtx.onEvent);

    const server_options = initServerOptions(loopbackAddr(7000), observer);
    const client_options = initClientOptions(loopbackAddr(7000), observer);

    try std.testing.expectEqual(quic.TransportMode.native, server_options.mode);
    try std.testing.expectEqual(quic.TransportMode.native, client_options.mode);
    try std.testing.expectEqualStrings(quic.alpn, server_options.alpn_protocols[0]);
    try std.testing.expectEqualStrings(quic.alpn, client_options.alpn_protocols[0]);
    try std.testing.expect(server_options.native.max_control_frame_bytes >= server_options.native.inline_frame_threshold);
    try std.testing.expect(client_options.native.max_control_frame_bytes >= client_options.native.inline_frame_threshold);
    try std.testing.expect(server_options.observer != null);
    try std.testing.expect(client_options.observer != null);

    const server_config = try quic.serverConfigFromOptions(std.testing.allocator, server_options);
    try std.testing.expectEqual(quic.compatibility_max_concurrent_sessions, server_config.max_concurrent_connections);
    try std.testing.expectEqualStrings(quic.alpn, server_config.alpn_protocols[0]);
}

test "quic transport guide server fanout and hardening snippets avoid network setup" {
    const retry_key: quic.ServerRetryTokenKey = @splat(0x33);
    const new_token_key: quic.ServerNewTokenKey = @splat(0x44);
    const reset_key: quic.StatelessResetKey = @splat(0x55);

    const fanout_options = quic.ServerOptions{
        .listen_addr = loopbackAddr(7001),
        .tls_cert_pem = server_cert_pem,
        .tls_key_pem = server_key_pem,
        .max_concurrent_connections = 4,
        .mode = .native,
        .native = .{},
    };

    try std.testing.expectEqual(@as(u32, 4), fanout_options.max_concurrent_connections);
    try std.testing.expectEqual(quic.TransportMode.native, fanout_options.mode);

    const hardened_options = quic.withProductionServerHardening(fanout_options, .{
        .retry_token_key = retry_key,
        .stateless_reset_key = reset_key,
        .new_token_key = new_token_key,
    });

    try std.testing.expectEqual(retry_key, hardened_options.retry_token_key.?);
    try std.testing.expectEqual(reset_key, hardened_options.stateless_reset_key.?);
    try std.testing.expectEqual(new_token_key, hardened_options.new_token_key.?);
    // Production hardening pins every bandwidth/flood ceiling to an EXPLICIT
    // cap rather than `.default`, because `.default` for the listener and
    // bandwidth limiters resolves to "off" (the right ceiling is
    // deployment-specific). `.resolve(0)` therefore has to yield a real cap.
    try std.testing.expect(hardened_options.initial_source_rate_limit.resolve(0).? > 0);
    try std.testing.expect(hardened_options.listener_datagram_rate_limit.resolve(0).? > 0);
    try std.testing.expect(hardened_options.listener_byte_rate_limit.resolve(0).? > 0);
    try std.testing.expect(hardened_options.source_byte_rate_limit.resolve(0).? > 0);
    try std.testing.expect(hardened_options.early_data == .disabled);
    try std.testing.expect(!hardened_options.reveal_close_reason_on_wire);

    const server_config = try quic.serverConfigFromOptions(std.testing.allocator, hardened_options);
    try std.testing.expectEqual(@as(u32, 4), server_config.max_concurrent_connections);
    try std.testing.expectEqual(retry_key, server_config.retry_token_key.?);
    try std.testing.expectEqual(new_token_key, server_config.new_token_key.?);
}

// docs/quic-transport.md, "Production Defaults": the stateless-reset key
// recipe, verbatim.

/// Owner read/write only, where the platform has POSIX modes.
const key_file_permissions: std.Io.File.Permissions =
    if (@hasDecl(std.Io.File.Permissions, "fromMode")) .fromMode(0o600) else .default_file;

/// Load this server's stateless-reset key, creating it on the first start.
/// Every later start, including a restart after a crash, reads back the
/// same 32 bytes.
fn loadOrCreateResetKey(io: std.Io, dir: std.Io.Dir, sub_path: []const u8) !quic.StatelessResetKey {
    if (try readResetKey(io, dir, sub_path)) |key| return key;

    var key: quic.StatelessResetKey = undefined;
    try io.randomSecure(&key);
    // Write a temporary file, then link it into place: a crash cannot leave
    // a short key file, and when two first starts race, the loser reads
    // the winner's key.
    var file = try dir.createFileAtomic(io, sub_path, .{ .permissions = key_file_permissions });
    defer file.deinit(io);
    try file.file.writeStreamingAll(io, &key);
    try file.file.sync(io);
    file.link(io) catch |err| switch (err) {
        error.PathAlreadyExists => return (try readResetKey(io, dir, sub_path)) orelse
            error.InvalidStatelessResetKeyFile,
        else => |e| return e,
    };
    // `sync` above made the bytes durable, not the new name. Sync the
    // directory that holds the name, or a power loss right after the first
    // start can drop the file, and the next start mints a new key. Name it
    // from `sub_path`: on Linux, `file.dir` can be `dir` itself even when
    // `sub_path` has directories.
    try syncDir(io, dir, std.fs.path.dirname(sub_path) orelse ".");
    return key;
}

/// Flush the entries of the directory at `dir_path` to disk. Opened as a
/// file because a `Dir` handle may be path-only (O_PATH on Linux), which
/// cannot be synced. Windows has no directory sync; NTFS journals the
/// entry itself.
fn syncDir(io: std.Io, dir: std.Io.Dir, dir_path: []const u8) !void {
    if (@import("builtin").os.tag == .windows) return;
    const handle = try dir.openFile(io, dir_path, .{});
    defer handle.close(io);
    try handle.sync(io);
}

fn readResetKey(io: std.Io, dir: std.Io.Dir, sub_path: []const u8) !?quic.StatelessResetKey {
    var key: quic.StatelessResetKey = undefined;
    var buf: [key.len + 1]u8 = undefined;
    const bytes = dir.readFile(io, sub_path, &buf) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => |e| return e,
    };
    if (bytes.len != key.len) return error.InvalidStatelessResetKeyFile;
    @memcpy(&key, bytes);
    return key;
}

test "quic transport guide stateless-reset key recipe returns one key across restarts" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // First start: no file yet, so the recipe mints and persists a key.
    const first = try loadOrCreateResetKey(io, tmp.dir, "stateless-reset.key");
    // A restart reads the SAME bytes back: that is what lets the restarted
    // server answer the old process's connections with valid resets.
    const restarted = try loadOrCreateResetKey(io, tmp.dir, "stateless-reset.key");
    try std.testing.expectEqualSlices(u8, &first, &restarted);
    var on_disk: [64]u8 = undefined;
    try std.testing.expectEqualSlices(u8, &first, try tmp.dir.readFile(io, "stateless-reset.key", &on_disk));

    // A fresh deployment mints a different key.
    const other = try loadOrCreateResetKey(io, tmp.dir, "other-server.key");
    try std.testing.expect(!std.mem.eql(u8, &first, &other));

    // A damaged file is an error, never a silently regenerated key.
    try tmp.dir.writeFile(io, .{ .sub_path = "short.key", .data = first[0..16] });
    try std.testing.expectError(error.InvalidStatelessResetKeyFile, loadOrCreateResetKey(io, tmp.dir, "short.key"));

    const options = quic.withProductionServerHardening(.{
        .listen_addr = loopbackAddr(7002),
        .tls_cert_pem = server_cert_pem,
        .tls_key_pem = server_key_pem,
    }, .{
        .retry_token_key = @splat(0x33),
        .stateless_reset_key = restarted,
    });
    try std.testing.expectEqualSlices(u8, &first, &options.stateless_reset_key.?);
}

/// `std.testing.io` with `fileSync` wrapped to record the inode of the last
/// file or directory it synced. Nothing on disk shows which directory a
/// sync flushed, so a test reads it here.
const SyncRecorder = struct {
    var vtable: std.Io.VTable = undefined;
    var last_synced: ?std.Io.File.INode = null;

    fn io() std.Io {
        vtable = std.testing.io.vtable.*;
        vtable.fileSync = fileSync;
        last_synced = null;
        return .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    }

    fn fileSync(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
        try std.testing.io.vtable.fileSync(userdata, file);
        last_synced = (file.stat(std.testing.io) catch return error.Unexpected).inode;
    }
};

test "quic transport guide stateless-reset key recipe syncs the directory that holds the key" {
    // Windows has no directory sync; the recipe skips it there.
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // The recipe's last sync is the directory, after the key file's own.
    const root_inode = (try tmp.dir.stat(io)).inode;
    _ = try loadOrCreateResetKey(SyncRecorder.io(), tmp.dir, "stateless-reset.key");
    try std.testing.expectEqual(root_inode, SyncRecorder.last_synced.?);

    // Under a subdirectory the name lives in `keys/`, so `keys/` is the
    // directory to sync. On Linux, `createFileAtomic` links through
    // O_TMPFILE and returns `file.dir == tmp.dir`, so a recipe that synced
    // `file.dir` fails here. On macOS `file.dir` is `keys/`, so only a Linux
    // run (CI's test-docs-snippets-quic) catches that regression.
    try tmp.dir.createDir(io, "keys", .default_dir);
    var keys_dir = try tmp.dir.openDir(io, "keys", .{});
    defer keys_dir.close(io);
    const keys_inode = (try keys_dir.stat(io)).inode;
    const nested = try loadOrCreateResetKey(SyncRecorder.io(), tmp.dir, "keys/stateless-reset.key");
    try std.testing.expectEqual(keys_inode, SyncRecorder.last_synced.?);
    try std.testing.expectEqualSlices(u8, &nested, &(try loadOrCreateResetKey(io, tmp.dir, "keys/stateless-reset.key")));
}

test "quic transport guide one-call session snippets use the public session surface" {
    comptime {
        if (!quic.enabled) @compileError("QUIC transport snippets require -Dquic=true");
        // Every name the "One-call sessions" section and its example use.
        _ = &quic.connect;
        _ = &quic.serve;
        _ = &quic.ClientSession.fromPeer;
        _ = &quic.ClientSession.run;
        _ = &quic.ClientSession.close;
        _ = &quic.ClientSession.requestStop;
        _ = &quic.ClientSession.closeCause;
        _ = &quic.ClientSession.deinit;
        _ = &quic.PeerServer.getAddress;
        _ = &quic.PeerServer.run;
        _ = &quic.PeerServer.requestStop;
        _ = &quic.PeerServer.deinit;
        _ = &quic.PeerServer.Session.fromPeer;
        _ = &quic.PeerServer.Session.close;
        _ = &quic.PeerServer.Session.closeCause;
        _ = &quic.Server.setOnSessionAccepted;
        _ = &quic.Server.runWithAfterStep;
    }

    const Hooks = struct {
        fn onAccept(_: ?*anyopaque, _: *quic.PeerServer.Session) anyerror!void {}
    };

    const connect_options = quic.ConnectOptions{
        .conn = .{
            .remote_addr = loopbackAddr(7002),
            .server_name = "localhost",
            .ca_pem = server_cert_pem,
        },
    };
    const serve_options = quic.ServeOptions{ .on_accept = Hooks.onAccept };

    // The guide promises the TCP sessions' secure defaults on both sides.
    try std.testing.expectEqual(@as(?u64, 30_000), connect_options.default_call_timeout_ms);
    try std.testing.expectEqual(@as(?u64, 5_000), connect_options.shutdown_drain_timeout_ms);
    try std.testing.expectEqual(@as(?u64, 30_000), connect_options.join_timeout_ms);
    try std.testing.expect(!connect_options.conn.insecure_skip_verify);
    try std.testing.expectEqual(@as(?u64, 30_000), serve_options.default_call_timeout_ms);
    try std.testing.expectEqual(@as(?u64, 5_000), serve_options.shutdown_drain_timeout_ms);
    try std.testing.expectEqual(@as(?u64, 30_000), serve_options.join_timeout_ms);
}

// docs/quic-transport.md, "Session-ticket key": wiring a persisted ticket
// key into the hardened preset, verbatim.

/// The hardened preset for a warm-restore server that persists its
/// session-ticket key. Load every key from its own file on every start.
/// `ticket_key` must stay alive until `Server.init` returns; the server keeps
/// no pointer to it after that.
fn warmRestartOptions(
    base_options: quic.ServerOptions,
    retry_key: quic.ServerRetryTokenKey,
    reset_key: quic.StatelessResetKey,
    new_token_key: quic.ServerNewTokenKey,
    ticket_key: *const quic.SessionTicketKey,
) quic.ServerOptions {
    return quic.withProductionServerHardening(base_options, .{
        .retry_token_key = retry_key,
        .stateless_reset_key = reset_key,
        // Persist it with the ticket key: a returning client with a valid
        // NEW_TOKEN skips the Retry and its round trip.
        .new_token_key = new_token_key,
        .early_data = .restore_only,
        .session_ticket_key = ticket_key,
    });
}

test "quic transport guide session-ticket key wiring" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const state_dir = tmp.dir;
    const base_options: quic.ServerOptions = .{
        .listen_addr = loopbackAddr(7003),
        .tls_cert_pem = server_cert_pem,
        .tls_key_pem = server_key_pem,
    };
    const retry_key: quic.ServerRetryTokenKey = @splat(0x33);
    const reset_key: quic.StatelessResetKey = @splat(0x34);
    const new_token_key: quic.ServerNewTokenKey = @splat(0x35);
    // A 48-byte key file, owner read/write only (`key_file_permissions`).
    const on_disk: quic.SessionTicketKey = @splat(0x36);
    try state_dir.writeFile(io, .{
        .sub_path = "session-ticket.key",
        .data = &on_disk,
        .flags = .{ .permissions = key_file_permissions },
    });

    var ticket_key = try quic.loadTicketKeyFile(io, state_dir, "session-ticket.key");
    defer std.crypto.secureZero(u8, &ticket_key);
    const options = warmRestartOptions(base_options, retry_key, reset_key, new_token_key, &ticket_key);

    try std.testing.expectEqualSlices(u8, &on_disk, options.session_ticket_key.?);
    try std.testing.expectEqual(new_token_key, options.new_token_key.?);
    try std.testing.expect(options.early_data == .without_replay_protection);
    try std.testing.expectEqual(quic.early_dispatch.Mode.restore_only, options.early_dispatch);
    // quic-zig's config carries a copy of the key, by value: zero it once
    // the quic-zig server is built (`Listener.init` does).
    var config = try quic.serverConfigFromOptions(std.testing.allocator, options);
    defer if (config.session_ticket_key) |*copy| std.crypto.secureZero(u8, copy);
    try std.testing.expectEqualSlices(u8, &on_disk, &config.session_ticket_key.?);
}
