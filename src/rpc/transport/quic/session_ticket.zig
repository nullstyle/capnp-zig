//! Persisted BoringSSL session-ticket key for the QUIC server (Experimental).
//!
//! Design and security trade-off: "Session-ticket key" in
//! docs/quic-transport.md. quic-zig v0.25.0 has no ticket-key config field,
//! so `Listener.init` installs the key on the TLS context that quic-zig built
//! (`quic_zig.Server.tls_ctx`), right after `quic_zig.Server.init` and before
//! the first datagram is fed: BoringSSL's ticket-key setter takes no lock.
//! The calls go through `boringssl.raw` of the boringssl module that quic-zig
//! exports (build/helpers.zig `addQuicLibImports`), so `tls_ctx.inner` and
//! the setter's `SSL_CTX` are one type.
//!
//! Coupling: this reaches quic-zig's `Server.tls_ctx` field. A rename fails
//! to compile. A change in meaning is caught by the ticket-key tests in
//! tests/rpc/transport/quic/ (the `.accepted` resumption after a restart).
//! A TLS-context reload (`quic_zig.Server.replaceTlsContext`) builds a
//! context without the key; capnp-zig never reloads, and the docs tell a
//! caller who does to install the key again.

const std = @import("std");
const builtin = @import("builtin");
const boringssl = @import("boringssl");
const quic_zig = @import("quic");

const quic_options = @import("options.zig");

const raw = boringssl.raw;
const SessionTicketKey = quic_options.SessionTicketKey;

pub const InstallError = error{
    /// BoringSSL refused the key, or reading it back did not return the
    /// installed bytes.
    SessionTicketKeyInstallFailed,
};

/// Install `key` (when set) and the ticket lifetime (when set) on the TLS
/// context of a freshly initialized `server`. Call it before the server
/// handles its first datagram. Reads the key back and compares it, so a
/// change in what quic-zig's `tls_ctx` points at fails here, not silently.
pub fn install(
    server: *quic_zig.Server,
    key: ?*const SessionTicketKey,
    lifetime_s: ?u32,
) InstallError!void {
    const ctx = server.tls_ctx.inner;
    if (key) |k| {
        if (raw.zbssl_SSL_CTX_set_tlsext_ticket_keys(ctx, k, k.len) != 1) {
            return error.SessionTicketKeyInstallFailed;
        }
        var back: SessionTicketKey = undefined;
        defer std.crypto.secureZero(u8, &back);
        if (raw.zbssl_SSL_CTX_get_tlsext_ticket_keys(ctx, &back, back.len) != 1) {
            return error.SessionTicketKeyInstallFailed;
        }
        if (!std.crypto.timing_safe.eql(SessionTicketKey, back, k.*)) {
            return error.SessionTicketKeyInstallFailed;
        }
    }
    if (lifetime_s) |seconds| raw.zbssl_SSL_CTX_set_session_psk_dhe_timeout(ctx, seconds);
}

pub const LoadTicketKeyFileError = std.Io.File.OpenError ||
    std.Io.File.StatError ||
    std.Io.File.ReadPositionalError ||
    error{
        /// The file is not exactly 48 bytes, or all 48 bytes are zero.
        /// A damaged key file is an error, never a silently regenerated
        /// key.
        InvalidSessionTicketKeyFile,
        /// POSIX only: the group or others have any permission on the
        /// file. Use mode 0600 (or 0400), as for an SSH private key.
        SessionTicketKeyFilePermissions,
    };

/// Read a persisted session-ticket key: a file of exactly 48 bytes.
///
/// On POSIX it refuses a file that the group or others may access (any of
/// the mode bits 0o077), because the key decrypts recorded 0-RTT data. On
/// Windows there are no mode bits to check: give the file an ACL that grants
/// access to the service account only.
///
/// Create the file once from a CSPRNG, write it atomically (a temporary file,
/// then rename), and load it on every start; the "Production Defaults" reset
/// key recipe in docs/quic-transport.md works with the key type changed to
/// `SessionTicketKey`. The caller owns the returned bytes: pass a pointer as
/// `ServerOptions.session_ticket_key`, and zero them after `Listener.init`.
pub fn loadTicketKeyFile(
    io: std.Io,
    dir: std.Io.Dir,
    sub_path: []const u8,
) LoadTicketKeyFileError!SessionTicketKey {
    const file = try dir.openFile(io, sub_path, .{});
    defer file.close(io);

    if (comptime @hasDecl(std.Io.File.Permissions, "toMode")) {
        const stat = try file.stat(io);
        if (stat.permissions.toMode() & 0o077 != 0) return error.SessionTicketKeyFilePermissions;
    }

    var key: SessionTicketKey = undefined;
    // One spare byte tells a 48-byte file from a longer one.
    var buf: [key.len + 1]u8 = undefined;
    defer std.crypto.secureZero(u8, &buf);
    const len = try file.readPositionalAll(io, &buf, 0);
    if (len != key.len) return error.InvalidSessionTicketKeyFile;
    if (std.mem.allEqual(u8, buf[0..key.len], 0)) return error.InvalidSessionTicketKeyFile;
    @memcpy(&key, buf[0..key.len]);
    return key;
}

pub const testing = if (builtin.is_test) struct {
    /// The lifetime, in seconds, of the TLS session inside a quic-zig
    /// resumption envelope (what a client captures through
    /// `new_session_callback`). A TLS 1.3 client caps it at the lifetime the
    /// server advertised with the ticket, so it shows the server's
    /// `session_ticket_lifetime_s`.
    pub fn ticketLifetimeSeconds(envelope: []const u8) !u32 {
        const decoded = try quic_zig.tls.resumption_state.decode(envelope);
        var ctx = try boringssl.tls.Context.initClient(.{ .verify = .none });
        defer ctx.deinit();
        const session = raw.zbssl_SSL_SESSION_from_bytes(
            decoded.session_ticket.ptr,
            decoded.session_ticket.len,
            ctx.inner,
        ) orelse return error.InvalidResumptionState;
        defer raw.zbssl_SSL_SESSION_free(session);
        return raw.zbssl_SSL_SESSION_get_timeout(session);
    }
} else struct {};
