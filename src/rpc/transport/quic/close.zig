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

/// Tracks the first terminal close status and its sanitized wire reason.
///
/// `record` is safe to call from any thread: the QUIC run loop records the
/// terminal status while flushing the wire close frame, while a cross-thread
/// `requestClose` may record a normal close concurrently. A `claimed` latch
/// gives the first recorder exclusive ownership of `reason_buf`/`reason_len`,
/// and `published` is released only after those fields are fully written.
/// Readers of `status`/`reason` acquire `published`, so they observe either no
/// status at all or a completely written snapshot — never a torn reason.
pub const State = struct {
    reveal_detail_on_wire: bool = false,
    claimed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    published: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    status_value: Status = undefined,
    reason_buf: [max_wire_reason_bytes]u8 = undefined,
    reason_len: usize = 0,

    pub fn init(reveal_detail_on_wire: bool) State {
        return .{
            .reveal_detail_on_wire = reveal_detail_on_wire,
        };
    }

    pub fn status(self: *const State) ?Status {
        if (!self.published.load(.acquire)) return null;
        return self.status_value;
    }

    pub fn reason(self: *const State) []const u8 {
        if (!self.published.load(.acquire)) return &.{};
        return self.reason_buf[0..self.reason_len];
    }

    pub fn record(
        self: *State,
        code: ApplicationCloseCode,
        err: ?anyerror,
    ) void {
        // First recorder wins the claim and owns the buffer writes; concurrent
        // recorders return immediately, leaving the first terminal status
        // intact.
        if (self.claimed.swap(true, .acq_rel)) return;
        const prepared = prepareWireReason(
            &self.reason_buf,
            code,
            err,
            self.reveal_detail_on_wire,
        );
        self.reason_len = prepared.len;
        self.status_value = .{
            .code = code,
            .err = err,
            .reason_truncated = prepared.truncated,
            .reason_reveals_detail = prepared.reveals_detail,
        };
        // Publish last: a reader that observes `published` via acquire sees the
        // fully written status and reason above.
        self.published.store(true, .release);
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

test "QUIC close state records only the first status" {
    var state = State.init(true);

    state.record(.frame_error, error.InvalidFrame);
    state.record(.internal_error, error.OutOfMemory);

    const status_value = state.status().?;
    try std.testing.expectEqual(ApplicationCloseCode.frame_error, status_value.code);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), status_value.err);
    try std.testing.expect(status_value.reason_reveals_detail);
    try std.testing.expectEqualStrings("rpc frame error: InvalidFrame", state.reason());
}
