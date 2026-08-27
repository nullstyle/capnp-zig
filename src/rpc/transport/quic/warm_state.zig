//! Warm sturdy-ref envelope codec: `{session ticket, NEW_TOKEN}` persisted
//! TOGETHER as the warm half of a sturdy ref. quic-zig deliberately delivers
//! the two through separate channels, and only together do they buy the full
//! one-round-trip resume: the ticket carries the TLS session (0-RTT), the
//! NEW_TOKEN carries address validation. An application persists the encoded
//! envelope beside its opaque sturdy-ref bytes and seeds a fresh
//! `WarmRedialClient` with it after a process restart.
//!
//! Wire shape (all little-endian, no padding):
//!   u16 version | u32 ticket_len | ticket bytes | u32 token_len | token bytes
//!
//! Dependency-free by design, so the default (non-QUIC) build exports it too
//! and the coverage-guided fuzzer exercises `decode` as an untrusted-input
//! surface.

const std = @import("std");

pub const version: u16 = 1;

pub const WarmState = struct {
    /// Borrowed views into the decoded buffer — copy before it goes away.
    ticket: []const u8,
    token: []const u8,
};

pub const DecodeError = error{InvalidWarmState};

pub fn encode(allocator: std.mem.Allocator, ticket: []const u8, token: []const u8) ![]u8 {
    const total = 2 + 4 + ticket.len + 4 + token.len;
    const out = try allocator.alloc(u8, total);
    var offset: usize = 0;
    std.mem.writeInt(u16, out[offset..][0..2], version, .little);
    offset += 2;
    std.mem.writeInt(u32, out[offset..][0..4], @intCast(ticket.len), .little);
    offset += 4;
    @memcpy(out[offset..][0..ticket.len], ticket);
    offset += ticket.len;
    std.mem.writeInt(u32, out[offset..][0..4], @intCast(token.len), .little);
    offset += 4;
    @memcpy(out[offset..][0..token.len], token);
    return out;
}

/// Strict decode: unknown version, truncation, or trailing bytes all fail.
/// The returned views BORROW `bytes`.
pub fn decode(bytes: []const u8) DecodeError!WarmState {
    if (bytes.len < 2 + 4) return error.InvalidWarmState;
    var offset: usize = 0;
    const got_version = std.mem.readInt(u16, bytes[offset..][0..2], .little);
    if (got_version != version) return error.InvalidWarmState;
    offset += 2;
    const ticket_len = std.mem.readInt(u32, bytes[offset..][0..4], .little);
    offset += 4;
    if (bytes.len - offset < ticket_len) return error.InvalidWarmState;
    const ticket = bytes[offset..][0..ticket_len];
    offset += ticket_len;
    if (bytes.len - offset < 4) return error.InvalidWarmState;
    const token_len = std.mem.readInt(u32, bytes[offset..][0..4], .little);
    offset += 4;
    if (bytes.len - offset != token_len) return error.InvalidWarmState;
    const token = bytes[offset..][0..token_len];
    return .{ .ticket = ticket, .token = token };
}
