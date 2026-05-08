const std = @import("std");
const nullq = @import("nullq");

const framing = @import("../../wire/framing.zig");
const length_framer = @import("length_framer.zig");

const Net = std.Io.net;

/// Native QUIC ALPN for Cap'n Proto RPC over nullq.
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
pub const default_quic_local_cid_len: u8 = 8;
pub const default_quic_source_rate_window_us: u64 = 1_000_000;
pub const default_quic_source_rate_table_capacity: u32 = 4096;
pub const default_quic_max_vn_per_source_per_window: ?u32 = 8;
pub const default_quic_retry_token_lifetime_us: u64 = 10_000_000;
pub const default_quic_retry_state_table_capacity: u32 = 4096;
pub const default_quic_new_token_lifetime_us: u64 = 24 * 3600 * 1_000_000;
pub const default_quic_max_connection_memory: u64 = nullq.conn.state.default_max_connection_memory;
pub const default_quic_listener_rate_window_us: u64 = 1_000_000;
pub const default_quic_max_log_events_per_source_per_window: ?u32 = 16;

/// Current capnp-zig QUIC RPC server fanout. The listener/session split makes
/// the next step visible, but peer dispatch still binds one active QUIC session
/// to one server transport today.
pub const supported_max_concurrent_sessions: u32 = 1;

pub const ServerQlogCallback = nullq.QlogCallback;
pub const ServerLogEvent = nullq.Server.LogEvent;
pub const ServerLogCallback = nullq.Server.LogCallback;
pub const ServerRetryTokenKey = nullq.conn.RetryTokenKey;
pub const ServerNewTokenKey = nullq.conn.NewTokenKey;
pub const ServerAntiReplayTracker = nullq.tls.AntiReplayTracker;

pub fn defaultTransportParams() nullq.tls.TransportParams {
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
    transport_params: nullq.tls.TransportParams = defaultTransportParams(),
    receive_timeout: std.Io.Duration = std.Io.Duration.fromMilliseconds(5),
    udp_rx_buffer_size: usize = default_udp_rx_buffer_size,
    udp_tx_buffer_size: usize = default_udp_tx_buffer_size,
    stream_read_buffer_size: usize = default_stream_read_buffer_size,
    max_message_bytes: usize = default_max_message_bytes,
    max_outbound_queue_items: usize = default_max_outbound_queue_items,
    max_outbound_queue_bytes: usize = default_max_outbound_queue_bytes,
    ca_pem: ?[]const u8 = null,
};

pub const ServerOptions = struct {
    listen_addr: Net.IpAddress,
    tls_cert_pem: []const u8,
    tls_key_pem: []const u8,
    alpn_protocols: []const []const u8 = &.{alpn},
    transport_params: nullq.tls.TransportParams = defaultTransportParams(),
    max_concurrent_connections: u32 = 1,
    local_cid_len: u8 = default_quic_local_cid_len,
    qlog_callback: ?ServerQlogCallback = null,
    qlog_user_data: ?*anyopaque = null,
    log_callback: ?ServerLogCallback = null,
    log_user_data: ?*anyopaque = null,
    max_initials_per_source_per_window: ?u32 = null,
    source_rate_window_us: u64 = default_quic_source_rate_window_us,
    source_rate_table_capacity: u32 = default_quic_source_rate_table_capacity,
    max_vn_per_source_per_window: ?u32 = default_quic_max_vn_per_source_per_window,
    retry_token_key: ?ServerRetryTokenKey = null,
    retry_token_lifetime_us: u64 = default_quic_retry_token_lifetime_us,
    retry_state_table_capacity: u32 = default_quic_retry_state_table_capacity,
    new_token_key: ?ServerNewTokenKey = null,
    new_token_lifetime_us: u64 = default_quic_new_token_lifetime_us,
    enable_0rtt: bool = false,
    early_data_anti_replay: ?*ServerAntiReplayTracker = null,
    reveal_close_reason_on_wire: bool = false,
    max_connection_memory: u64 = default_quic_max_connection_memory,
    max_datagrams_per_window: ?u32 = null,
    max_bytes_per_window: ?u64 = null,
    listener_rate_window_us: u64 = default_quic_listener_rate_window_us,
    max_bytes_per_source_per_second: ?u64 = null,
    max_log_events_per_source_per_window: ?u32 = default_quic_max_log_events_per_source_per_window,
    receive_timeout: std.Io.Duration = std.Io.Duration.fromMilliseconds(5),
    udp_rx_buffer_size: usize = default_udp_rx_buffer_size,
    udp_tx_buffer_size: usize = default_udp_tx_buffer_size,
    stream_read_buffer_size: usize = default_stream_read_buffer_size,
    max_message_bytes: usize = default_max_message_bytes,
    max_outbound_queue_items: usize = default_max_outbound_queue_items,
    max_outbound_queue_bytes: usize = default_max_outbound_queue_bytes,
};

pub const ServerProductionHardening = struct {
    retry_token_key: ServerRetryTokenKey,
    new_token_key: ?ServerNewTokenKey = null,
    max_initials_per_source_per_window: u32 = 32,
    max_vn_per_source_per_window: ?u32 = default_quic_max_vn_per_source_per_window,
    max_datagrams_per_window: u32 = 100_000,
    max_bytes_per_window: u64 = 128 * 1024 * 1024,
    max_bytes_per_source_per_second: u64 = 16 * 1024 * 1024,
    max_connection_memory: u64 = default_quic_max_connection_memory,
    max_log_events_per_source_per_window: ?u32 = default_quic_max_log_events_per_source_per_window,
};

pub fn withProductionServerHardening(
    options: ServerOptions,
    hardening: ServerProductionHardening,
) ServerOptions {
    var out = options;
    out.retry_token_key = hardening.retry_token_key;
    out.new_token_key = hardening.new_token_key;
    out.max_initials_per_source_per_window = hardening.max_initials_per_source_per_window;
    out.max_vn_per_source_per_window = hardening.max_vn_per_source_per_window;
    out.max_datagrams_per_window = hardening.max_datagrams_per_window;
    out.max_bytes_per_window = hardening.max_bytes_per_window;
    out.max_bytes_per_source_per_second = hardening.max_bytes_per_source_per_second;
    out.max_connection_memory = hardening.max_connection_memory;
    out.max_log_events_per_source_per_window = hardening.max_log_events_per_source_per_window;
    out.enable_0rtt = false;
    out.early_data_anti_replay = null;
    out.reveal_close_reason_on_wire = false;
    return out;
}

pub fn nullqServerConfigFromOptions(
    allocator: std.mem.Allocator,
    options: ServerOptions,
) !nullq.Server.Config {
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
        .max_initials_per_source_per_window = options.max_initials_per_source_per_window,
        .source_rate_window_us = options.source_rate_window_us,
        .source_rate_table_capacity = options.source_rate_table_capacity,
        .max_vn_per_source_per_window = options.max_vn_per_source_per_window,
        .retry_token_key = options.retry_token_key,
        .retry_token_lifetime_us = options.retry_token_lifetime_us,
        .retry_state_table_capacity = options.retry_state_table_capacity,
        .new_token_key = options.new_token_key,
        .new_token_lifetime_us = options.new_token_lifetime_us,
        .enable_0rtt = options.enable_0rtt,
        .early_data_anti_replay = options.early_data_anti_replay,
        .reveal_close_reason_on_wire = options.reveal_close_reason_on_wire,
        .max_connection_memory = options.max_connection_memory,
        .max_datagrams_per_window = options.max_datagrams_per_window,
        .max_bytes_per_window = options.max_bytes_per_window,
        .listener_rate_window_us = options.listener_rate_window_us,
        .max_bytes_per_source_per_second = options.max_bytes_per_source_per_second,
        .max_log_events_per_source_per_window = options.max_log_events_per_source_per_window,
    };
}

fn validateServerOptions(options: ServerOptions) !void {
    if (options.alpn_protocols.len == 0) return error.InvalidConfig;
    if (options.tls_cert_pem.len == 0 or options.tls_key_pem.len == 0) return error.InvalidConfig;
    if (options.max_concurrent_connections != supported_max_concurrent_sessions) return error.InvalidConfig;
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
    if (options.max_initials_per_source_per_window) |cap| if (cap == 0) return error.InvalidConfig;
    if (options.max_vn_per_source_per_window) |cap| if (cap == 0) return error.InvalidConfig;
    if (options.retry_token_key != null) {
        if (options.retry_token_lifetime_us == 0 or options.retry_state_table_capacity == 0) {
            return error.InvalidConfig;
        }
    }
    if (options.new_token_key != null and options.new_token_lifetime_us == 0) return error.InvalidConfig;
    if (options.max_datagrams_per_window) |cap| if (cap == 0) return error.InvalidConfig;
    if (options.max_bytes_per_window) |cap| if (cap == 0) return error.InvalidConfig;
    if (options.max_bytes_per_source_per_second) |cap| if (cap == 0) return error.InvalidConfig;
    if (options.max_log_events_per_source_per_window) |cap| if (cap == 0) return error.InvalidConfig;
}
