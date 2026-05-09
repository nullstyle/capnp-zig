const quic_close = @import("close.zig");

pub const Kind = enum {
    frame_error,
    callback_error,
    internal_error,
};

pub const Policy = struct {
    code: quic_close.ApplicationCloseCode,
    reset_inbound: bool = false,
    clear_message_callback: bool = false,
    clear_error_callback: bool = false,
    notify_error_callback: bool = true,
};

pub fn policy(kind: Kind, err: anyerror) Policy {
    return switch (kind) {
        .frame_error => .{
            .code = quic_close.codeForFrameError(err),
            .reset_inbound = true,
            .clear_message_callback = true,
            .clear_error_callback = true,
        },
        .callback_error => .{
            .code = .peer_callback_failure,
            .clear_message_callback = true,
            .clear_error_callback = true,
        },
        .internal_error => .{
            .code = quic_close.codeForStepError(err),
        },
    };
}
