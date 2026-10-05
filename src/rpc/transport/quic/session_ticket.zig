//! Persisted session-ticket key file for the QUIC server (Experimental).
//!
//! Design and security trade-off: "Session-ticket key" in
//! docs/quic-transport.md. The key reaches quic-zig through its
//! `Server.Config.session_ticket_key` (`serverConfigFromOptions`), and
//! quic-zig installs it on every TLS context it builds. This file only reads
//! the key from disk, with the checks that a secret of that rank needs.

const std = @import("std");

const quic_options = @import("options.zig");

const SessionTicketKey = quic_options.SessionTicketKey;

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
