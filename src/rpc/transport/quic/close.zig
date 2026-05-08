const std = @import("std");

/// QUIC application close codes reserved for Cap'n Proto RPC over QUIC.
///
/// QUIC application error codes live in the application namespace, distinct
/// from RFC 9000 transport errors. Zero remains normal close; non-zero values
/// use a compact "CNP" prefix plus a small discriminator.
pub const ApplicationCloseCode = enum(u64) {
    normal = 0,
    frame_error = 0x434e_5001,
    protocol_error = 0x434e_5002,
    internal_error = 0x434e_5003,
    peer_callback_failure = 0x434e_5004,
};

pub const max_wire_reason_bytes: usize = 96;

pub const Status = struct {
    code: ApplicationCloseCode,
    err: ?anyerror = null,
    reason_truncated: bool = false,
    reason_reveals_detail: bool = false,

    pub fn codeValue(self: Status) u64 {
        return @intFromEnum(self.code);
    }
};

pub const PreparedReason = struct {
    len: usize = 0,
    truncated: bool = false,
    reveals_detail: bool = false,
};

pub fn codeValue(code: ApplicationCloseCode) u64 {
    return @intFromEnum(code);
}

pub fn publicReason(code: ApplicationCloseCode) []const u8 {
    return switch (code) {
        .normal => "",
        .frame_error => "rpc frame error",
        .protocol_error => "rpc protocol error",
        .internal_error => "rpc transport error",
        .peer_callback_failure => "rpc callback error",
    };
}

pub fn codeForFrameError(err: anyerror) ApplicationCloseCode {
    return switch (err) {
        error.InvalidFrame, error.FrameTooLarge => .frame_error,
        error.OutOfMemory => .internal_error,
        else => .protocol_error,
    };
}

pub fn codeForStepError(err: anyerror) ApplicationCloseCode {
    return switch (err) {
        error.InvalidFrame, error.FrameTooLarge => .frame_error,
        error.OutOfMemory => .internal_error,
        else => .internal_error,
    };
}

pub fn sanitizeReason(dest: []u8, raw: []const u8) PreparedReason {
    var out = PreparedReason{};
    appendSanitized(dest, &out, raw);
    return out;
}

pub fn prepareWireReason(
    dest: []u8,
    code: ApplicationCloseCode,
    err: ?anyerror,
    reveal_detail: bool,
) PreparedReason {
    const capped_dest = dest[0..@min(dest.len, max_wire_reason_bytes)];
    var out = PreparedReason{};

    appendSanitized(capped_dest, &out, publicReason(code));
    if (reveal_detail) {
        if (err) |actual_err| {
            out.reveals_detail = true;
            appendSanitized(capped_dest, &out, ": ");
            appendSanitized(capped_dest, &out, @errorName(actual_err));
        }
    }

    return out;
}

fn appendSanitized(dest: []u8, out: *PreparedReason, raw: []const u8) void {
    for (raw) |byte| {
        if (out.len >= dest.len) {
            out.truncated = true;
            return;
        }
        dest[out.len] = sanitizeByte(byte);
        out.len += 1;
    }
}

fn sanitizeByte(byte: u8) u8 {
    return if (byte >= 0x20 and byte <= 0x7e) byte else '?';
}

test "QUIC application close codes are stable" {
    try std.testing.expectEqual(@as(u64, 0), codeValue(.normal));
    try std.testing.expectEqual(@as(u64, 0x434e_5001), codeValue(.frame_error));
    try std.testing.expectEqual(@as(u64, 0x434e_5002), codeValue(.protocol_error));
    try std.testing.expectEqual(@as(u64, 0x434e_5003), codeValue(.internal_error));
    try std.testing.expectEqual(@as(u64, 0x434e_5004), codeValue(.peer_callback_failure));
}

test "QUIC close reasons are sanitized and truncated" {
    var buf: [8]u8 = undefined;
    const prepared = sanitizeReason(&buf, "ab\ncd\txyz");

    try std.testing.expectEqual(@as(usize, 8), prepared.len);
    try std.testing.expect(prepared.truncated);
    try std.testing.expectEqualStrings("ab?cd?xy", buf[0..prepared.len]);
}

test "QUIC close reason preparation hides details by default" {
    var buf: [max_wire_reason_bytes]u8 = undefined;
    const prepared = prepareWireReason(&buf, .frame_error, error.InvalidFrame, false);

    try std.testing.expectEqualStrings("rpc frame error", buf[0..prepared.len]);
    try std.testing.expect(!prepared.truncated);
    try std.testing.expect(!prepared.reveals_detail);
}

test "QUIC close reason preparation reveals sanitized details when configured" {
    var buf: [max_wire_reason_bytes]u8 = undefined;
    const prepared = prepareWireReason(&buf, .frame_error, error.InvalidFrame, true);

    try std.testing.expectEqualStrings("rpc frame error: InvalidFrame", buf[0..prepared.len]);
    try std.testing.expect(!prepared.truncated);
    try std.testing.expect(prepared.reveals_detail);
}
