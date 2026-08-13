const std = @import("std");
const quic_zig = @import("quic");

const events = @import("../../events.zig");
const framing = @import("../../wire/framing.zig");
const length_framer = @import("length_framer.zig");
const native_framer = @import("native_framer.zig");

const Net = std.Io.net;

/// Native QUIC ALPN for Cap'n Proto RPC over quic_zig.
pub const alpn = "capnp-rpc/1";

/// The first client-initiated bidirectional stream carries the baseline RPC
/// byte stream. This keeps the initial QUIC transport isomorphic to the TCP
/// transport above the framing boundary while reserving additional streams for
/// later QUIC-native payload modes.
pub const baseline_stream_id: u64 = 0;

pub const default_udp_rx_buffer_size: usize = 64 * 1024;
pub const default_udp_tx_buffer_size: usize = 1500;
pub const default_stream_read_buffer_size: usize = 64 * 1024;
pub const default_max_message_bytes: usize = framing.Framer.max_frame_words * 8;
pub const default_max_outbound_queue_items: usize = 1024;
pub const default_max_outbound_queue_bytes: usize = default_max_message_bytes + length_framer.length_prefix_bytes;
pub const default_native_inline_frame_threshold: usize = 64 * 1024;
pub const default_native_max_control_frame_bytes: usize = native_framer.rpc_header_bytes + default_native_inline_frame_threshold;
pub const default_native_max_pending_data_streams: usize = 16;
pub const default_native_max_pending_data_bytes: usize = default_max_message_bytes;
/// Default wall-clock budget for completing an announced data-stream payload.
/// Bounds slow-loris peers that announce a data stream then drip or withhold
/// bytes: the completion window is (re)armed on each byte-delivering read, so a
/// stream that makes no progress for this long is aborted. 30s matches the
/// default QUIC idle-timeout order of magnitude while still bounding the stall.
pub const default_native_data_stream_completion_deadline_us: u64 = 30 * 1_000_000;
pub const default_quic_local_cid_len: u8 = 8;
pub const default_quic_source_rate_window_us: u64 = 1_000_000;
pub const default_quic_source_rate_table_capacity: u32 = 4096;
/// The library-recommended per-source VN cap, mirrored from quic-zig's
/// `Server.Config.default_vn_source_rate_cap`. Kept as a named constant
/// because it is part of capnp-zig's documented posture, not just a
/// pass-through.
pub const default_quic_vn_source_rate_cap: u64 = quic_zig.Server.Config.default_vn_source_rate_cap;
pub const default_quic_retry_token_lifetime_us: u64 = 10_000_000;
pub const default_quic_retry_state_table_capacity: u32 = 4096;
pub const default_quic_new_token_lifetime_us: u64 = 24 * 3600 * 1_000_000;
pub const default_quic_max_connection_memory: u64 = quic_zig.conn.state.default_max_connection_memory;
pub const default_quic_listener_rate_window_us: u64 = 1_000_000;
/// Recommended per-source log-event cap
/// (quic-zig `Server.Config.default_log_source_rate_cap`).
pub const default_quic_log_source_rate_cap: u64 = quic_zig.Server.Config.default_log_source_rate_cap;

/// Recommended per-source Initial-flood cap
/// (quic-zig `Server.Config.default_initial_source_rate_cap`). capnp-zig had
/// no constant for this before v0.9.0 and defaulted the knob to `null`, which
/// under quic-zig >= 0.3.0 meant "explicitly disable this DoS mitigation".
pub const default_quic_initial_source_rate_cap: u64 = quic_zig.Server.Config.default_initial_source_rate_cap;

/// Three-state rate/quota control, re-exported from quic-zig so callers do not
/// have to reach into the dependency: `.default` (the library recommendation),
/// `.disabled` (opt out), `.{ .limit = n }` (explicit cap).
///
/// This replaced `?u32`/`?u64` knobs in v0.9.0. The optional shape could not
/// distinguish "unset" from "deliberately disable a DoS mitigation", and
/// capnp-zig shipped exactly that confusion: `max_initials_per_source_per_window`
/// defaulted to `null`, silently turning the Initial-flood limiter OFF for
/// every server that did not override it.
pub const RateLimit = quic_zig.Server.RateLimit;

/// 0-RTT posture, re-exported from quic-zig: `.disabled`,
/// `.{ .with_anti_replay = &tracker }`, or `.without_replay_protection`.
/// Replaces the old `enable_0rtt` + `early_data_anti_replay` pair, where
/// `true` with a forgotten tracker was a valid config that shipped
/// replay-exposed 0-RTT.
pub const EarlyData = quic_zig.Server.EarlyData;

/// Single-session compatibility capacity used by `Connection.initServer`.
pub const compatibility_max_concurrent_sessions: u32 = 1;

/// The fanout server delegates the actual live-slot cap to
/// `ServerOptions.max_concurrent_connections`, so the public static limit is the
/// full u32 range after rejecting zero.
pub const supported_max_concurrent_sessions: u32 = std.math.maxInt(u32);

pub const TransportMode = enum {
    baseline,
    native,
};

pub const NativeOptions = struct {
    /// Frames at or below this size stay in ordered control-stream envelopes.
    /// Larger frames use one-shot peer-initiated unidirectional data streams.
    inline_frame_threshold: usize = default_native_inline_frame_threshold,
    /// Maximum native control-envelope payload size, excluding the 4-byte
    /// length prefix. Must fit the largest inline frame selected by
    /// `inline_frame_threshold`.
    max_control_frame_bytes: usize = default_native_max_control_frame_bytes,
    /// Maximum number of queued outbound data-stream RPC frames before
    /// backpressure rejects `sendFrame`.
    max_pending_data_streams: usize = default_native_max_pending_data_streams,
    /// Maximum queued outbound data-stream payload bytes before backpressure
    /// rejects `sendFrame`. The inbound side applies the same per-frame budget.
    max_pending_data_bytes: usize = default_native_max_pending_data_bytes,
    /// Wall-clock budget (microseconds) for a peer to finish delivering an
    /// announced inbound data-stream payload. The window is re-armed whenever a
    /// read delivers bytes, so it bounds a stall — not the total transfer time.
    /// `null` disables the deadline (unbounded, matching the pre-deadline
    /// behavior). Guards against slow-loris peers that announce a large data
    /// stream then drip or withhold bytes to pin the session open.
    data_stream_completion_deadline_us: ?u64 = default_native_data_stream_completion_deadline_us,
};

pub const NativeConfigError = error{
    NativeControlFrameLimitTooSmall,
    NativePendingDataStreamLimitRequired,
    NativePendingDataByteLimitRequired,
    NativeInlineFrameExceedsControlFrameLimit,
    NativeControlFrameLimitExceedsWireLimit,
    NativeDataStreamDeadlineRequired,
};

pub const ServerQlogCallback = quic_zig.QlogCallback;
pub const ServerLogEvent = quic_zig.Server.LogEvent;
pub const ServerLogCallback = quic_zig.Server.LogCallback;
pub const ServerRetryTokenKey = quic_zig.conn.RetryTokenKey;
pub const ServerNewTokenKey = quic_zig.conn.NewTokenKey;
pub const ServerAntiReplayTracker = quic_zig.tls.AntiReplayTracker;

pub fn defaultTransportParams() quic_zig.tls.TransportParams {
    return .{
        .max_idle_timeout_ms = 30_000,
        .initial_max_data = 16 * 1024 * 1024,
        .initial_max_stream_data_bidi_local = 1 << 20,
        .initial_max_stream_data_bidi_remote = 1 << 20,
        .initial_max_stream_data_uni = 1 << 20,
        .initial_max_streams_bidi = 16,
        .initial_max_streams_uni = 4,
        .active_connection_id_limit = 4,
    };
}

pub const ClientOptions = struct {
    /// UDP local bind address. When null, an ephemeral unspecified address is
    /// chosen with the same address family as `remote_addr`.
    local_addr: ?Net.IpAddress = null,
    remote_addr: Net.IpAddress,
    server_name: []const u8,
    alpn_protocols: []const []const u8 = &.{alpn},
    transport_params: quic_zig.tls.TransportParams = defaultTransportParams(),
    receive_timeout: std.Io.Duration = std.Io.Duration.fromMilliseconds(5),
    udp_rx_buffer_size: usize = default_udp_rx_buffer_size,
    udp_tx_buffer_size: usize = default_udp_tx_buffer_size,
    stream_read_buffer_size: usize = default_stream_read_buffer_size,
    max_message_bytes: usize = default_max_message_bytes,
    max_outbound_queue_items: usize = default_max_outbound_queue_items,
    max_outbound_queue_bytes: usize = default_max_outbound_queue_bytes,
    mode: TransportMode = .baseline,
    native: NativeOptions = .{},
    ca_pem: ?[]const u8 = null,
    /// Skip server certificate verification. Off by default; enable only for
    /// tests or controlled interop with self-signed peers.
    insecure_skip_verify: bool = false,
    observer: ?events.Observer = null,

    /// Congestion-control selection, forwarded verbatim to quic-zig.
    ///
    /// These exist because the defaults changed underneath us: quic-zig
    /// v0.11.0 flipped CUBIC, pacing and HyStart++ to default-on. Upstream
    /// documents a one-line opt-out per flip, but that lever is only real for
    /// a capnp-zig consumer if this transport forwards it — before these
    /// fields it did not, so the documented escape hatch was unreachable from
    /// here. They also make the behaviour A/B-testable, which is what
    /// `bench-quic` uses to show its bulk mode actually observes the pacer.
    ///
    /// Defaults deliberately mirror upstream's rather than pinning the old
    /// behaviour: this transport follows its backend's defaults, and pinning
    /// silently would hide the very change these fields exist to expose.
    congestion_control: quic_zig.CongestionAlgorithm = .cubic,
    enable_pacing: bool = true,
    enable_hystart: bool = true,
};

pub const ServerOptions = struct {
    listen_addr: Net.IpAddress,
    tls_cert_pem: []const u8,
    tls_key_pem: []const u8,
    alpn_protocols: []const []const u8 = &.{alpn},
    transport_params: quic_zig.tls.TransportParams = defaultTransportParams(),
    max_concurrent_connections: u32 = 1,
    local_cid_len: u8 = default_quic_local_cid_len,
    qlog_callback: ?ServerQlogCallback = null,
    qlog_user_data: ?*anyopaque = null,
    log_callback: ?ServerLogCallback = null,
    log_user_data: ?*anyopaque = null,
    /// Per-source Initial-flood limiter. `.default` applies quic-zig's
    /// recommended cap (32/window) — note this is a BEHAVIOUR CHANGE from the
    /// pre-v0.9.0 `?u32 = null` default, which disabled the limiter outright.
    initial_source_rate_limit: RateLimit = .default,
    source_rate_window_us: u64 = default_quic_source_rate_window_us,
    source_rate_table_capacity: u32 = default_quic_source_rate_table_capacity,
    vn_source_rate_limit: RateLimit = .default,
    retry_token_key: ?ServerRetryTokenKey = null,
    retry_token_lifetime_us: u64 = default_quic_retry_token_lifetime_us,
    retry_state_table_capacity: u32 = default_quic_retry_state_table_capacity,
    new_token_key: ?ServerNewTokenKey = null,
    new_token_lifetime_us: u64 = default_quic_new_token_lifetime_us,
    early_data: EarlyData = .disabled,
    reveal_close_reason_on_wire: bool = false,
    max_connection_memory: u64 = default_quic_max_connection_memory,
    /// Listener-wide and per-source bandwidth ceilings. quic-zig's `.default`
    /// for these three is "off" — the right ceiling is deployment-specific —
    /// so this is not a behaviour change from the old `null`.
    listener_datagram_rate_limit: RateLimit = .default,
    listener_byte_rate_limit: RateLimit = .default,
    listener_rate_window_us: u64 = default_quic_listener_rate_window_us,
    source_byte_rate_limit: RateLimit = .default,
    log_source_rate_limit: RateLimit = .default,
    receive_timeout: std.Io.Duration = std.Io.Duration.fromMilliseconds(5),
    udp_rx_buffer_size: usize = default_udp_rx_buffer_size,
    udp_tx_buffer_size: usize = default_udp_tx_buffer_size,
    stream_read_buffer_size: usize = default_stream_read_buffer_size,
    max_message_bytes: usize = default_max_message_bytes,
    max_outbound_queue_items: usize = default_max_outbound_queue_items,
    max_outbound_queue_bytes: usize = default_max_outbound_queue_bytes,
    mode: TransportMode = .baseline,
    native: NativeOptions = .{},
    observer: ?events.Observer = null,
};

pub const ServerProductionHardening = struct {
    retry_token_key: ServerRetryTokenKey,
    new_token_key: ?ServerNewTokenKey = null,
    initial_source_rate_limit: RateLimit = .{ .limit = default_quic_initial_source_rate_cap },
    vn_source_rate_limit: RateLimit = .default,
    listener_datagram_rate_limit: RateLimit = .{ .limit = 100_000 },
    listener_byte_rate_limit: RateLimit = .{ .limit = 128 * 1024 * 1024 },
    source_byte_rate_limit: RateLimit = .{ .limit = 16 * 1024 * 1024 },
    max_connection_memory: u64 = default_quic_max_connection_memory,
    log_source_rate_limit: RateLimit = .default,
};

pub fn withProductionServerHardening(
    options: ServerOptions,
    hardening: ServerProductionHardening,
) ServerOptions {
    var out = options;
    out.retry_token_key = hardening.retry_token_key;
    out.new_token_key = hardening.new_token_key;
    out.initial_source_rate_limit = hardening.initial_source_rate_limit;
    out.vn_source_rate_limit = hardening.vn_source_rate_limit;
    out.listener_datagram_rate_limit = hardening.listener_datagram_rate_limit;
    out.listener_byte_rate_limit = hardening.listener_byte_rate_limit;
    out.source_byte_rate_limit = hardening.source_byte_rate_limit;
    out.max_connection_memory = hardening.max_connection_memory;
    out.log_source_rate_limit = hardening.log_source_rate_limit;
    out.early_data = .disabled;
    out.reveal_close_reason_on_wire = false;
    return out;
}

pub fn serverConfigFromOptions(
    allocator: std.mem.Allocator,
    options: ServerOptions,
) !quic_zig.Server.Config {
    try validateServerOptions(options);
    return .{
        .allocator = allocator,
        .tls_cert_pem = options.tls_cert_pem,
        .tls_key_pem = options.tls_key_pem,
        .alpn_protocols = options.alpn_protocols,
        .transport_params = options.transport_params,
        .max_concurrent_connections = options.max_concurrent_connections,
        .local_cid_len = options.local_cid_len,
        .qlog_callback = options.qlog_callback,
        .qlog_user_data = options.qlog_user_data,
        .log_callback = options.log_callback,
        .log_user_data = options.log_user_data,
        .initial_source_rate_limit = options.initial_source_rate_limit,
        .source_rate_window_us = options.source_rate_window_us,
        .source_rate_table_capacity = options.source_rate_table_capacity,
        .vn_source_rate_limit = options.vn_source_rate_limit,
        .retry_token_key = options.retry_token_key,
        .retry_token_lifetime_us = options.retry_token_lifetime_us,
        .retry_state_table_capacity = options.retry_state_table_capacity,
        .new_token_key = options.new_token_key,
        .new_token_lifetime_us = options.new_token_lifetime_us,
        .early_data = options.early_data,
        .reveal_close_reason_on_wire = options.reveal_close_reason_on_wire,
        .max_connection_memory = options.max_connection_memory,
        .listener_datagram_rate_limit = options.listener_datagram_rate_limit,
        .listener_byte_rate_limit = options.listener_byte_rate_limit,
        .listener_rate_window_us = options.listener_rate_window_us,
        .source_byte_rate_limit = options.source_byte_rate_limit,
        .log_source_rate_limit = options.log_source_rate_limit,
    };
}

fn validateServerOptions(options: ServerOptions) !void {
    if (options.alpn_protocols.len == 0) return error.InvalidConfig;
    if (options.tls_cert_pem.len == 0 or options.tls_key_pem.len == 0) return error.InvalidConfig;
    if (options.max_concurrent_connections == 0) return error.InvalidConfig;
    if (options.local_cid_len == 0 or options.local_cid_len > 20) return error.InvalidConfig;
    if (options.udp_rx_buffer_size == 0 or
        options.udp_tx_buffer_size == 0 or
        options.stream_read_buffer_size == 0 or
        options.max_message_bytes == 0 or
        options.max_outbound_queue_items == 0 or
        options.max_outbound_queue_bytes == 0 or
        options.max_connection_memory == 0)
    {
        return error.InvalidConfig;
    }
    if (options.source_rate_window_us == 0 or options.source_rate_table_capacity == 0) {
        return error.InvalidConfig;
    }
    if (options.listener_rate_window_us == 0) return error.InvalidConfig;
    // quic-zig rejects `.{ .limit = 0 }` in `Server.init`; fail here too so a
    // bad cap is caught at the capnp-zig boundary, as it was before v0.9.0.
    inline for (.{
        options.initial_source_rate_limit,
        options.vn_source_rate_limit,
        options.listener_datagram_rate_limit,
        options.listener_byte_rate_limit,
        options.source_byte_rate_limit,
        options.log_source_rate_limit,
    }) |rl| {
        switch (rl) {
            .limit => |cap| if (cap == 0) return error.InvalidConfig,
            .default, .disabled => {},
        }
    }
    if (options.retry_token_key != null) {
        if (options.retry_token_lifetime_us == 0 or options.retry_state_table_capacity == 0) {
            return error.InvalidConfig;
        }
    }
    if (options.new_token_key != null and options.new_token_lifetime_us == 0) return error.InvalidConfig;
    try validateNativeOptions(options.mode, options.native, options.max_message_bytes);
}

pub fn validateClientOptions(options: ClientOptions) !void {
    if (options.alpn_protocols.len == 0) return error.InvalidConfig;
    if (options.udp_rx_buffer_size == 0 or
        options.udp_tx_buffer_size == 0 or
        options.stream_read_buffer_size == 0 or
        options.max_message_bytes == 0 or
        options.max_outbound_queue_items == 0 or
        options.max_outbound_queue_bytes == 0)
    {
        return error.InvalidConfig;
    }
    try validateNativeOptions(options.mode, options.native, options.max_message_bytes);
}

fn validateNativeOptions(
    mode: TransportMode,
    native: NativeOptions,
    max_message_bytes: usize,
) !void {
    if (mode == .baseline) return;
    if (native.max_control_frame_bytes < native_framer.common_header_bytes) return error.NativeControlFrameLimitTooSmall;
    if (native.max_pending_data_streams == 0) return error.NativePendingDataStreamLimitRequired;
    if (native.max_pending_data_bytes == 0) return error.NativePendingDataByteLimitRequired;

    const inline_payload_limit = @min(native.inline_frame_threshold, max_message_bytes);
    if (inline_payload_limit > 0) {
        const required_control = std.math.add(usize, native_framer.rpc_header_bytes, inline_payload_limit) catch return error.NativeInlineFrameExceedsControlFrameLimit;
        if (required_control > native.max_control_frame_bytes) return error.NativeInlineFrameExceedsControlFrameLimit;
    }
    if (native.max_control_frame_bytes > std.math.maxInt(u32)) return error.NativeControlFrameLimitExceedsWireLimit;
    if (native.data_stream_completion_deadline_us) |deadline| {
        if (deadline == 0) return error.NativeDataStreamDeadlineRequired;
    }
}
