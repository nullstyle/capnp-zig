const adapter = @import("nullq_adapter.zig");
const conn = @import("connection.zig");
const framer = @import("length_framer.zig");
const options = @import("options.zig");

pub const Connection = conn.Connection;
pub const LengthDelimitedFramer = framer.LengthDelimitedFramer;
pub const length_prefix_bytes = framer.length_prefix_bytes;

pub const alpn = options.alpn;
pub const baseline_stream_id = options.baseline_stream_id;
pub const default_udp_rx_buffer_size = options.default_udp_rx_buffer_size;
pub const default_udp_tx_buffer_size = options.default_udp_tx_buffer_size;
pub const default_stream_read_buffer_size = options.default_stream_read_buffer_size;
pub const default_max_message_bytes = options.default_max_message_bytes;
pub const default_max_outbound_queue_items = options.default_max_outbound_queue_items;
pub const default_max_outbound_queue_bytes = options.default_max_outbound_queue_bytes;
pub const default_quic_local_cid_len = options.default_quic_local_cid_len;
pub const default_quic_source_rate_window_us = options.default_quic_source_rate_window_us;
pub const default_quic_source_rate_table_capacity = options.default_quic_source_rate_table_capacity;
pub const default_quic_max_vn_per_source_per_window = options.default_quic_max_vn_per_source_per_window;
pub const default_quic_retry_token_lifetime_us = options.default_quic_retry_token_lifetime_us;
pub const default_quic_retry_state_table_capacity = options.default_quic_retry_state_table_capacity;
pub const default_quic_new_token_lifetime_us = options.default_quic_new_token_lifetime_us;
pub const default_quic_max_connection_memory = options.default_quic_max_connection_memory;
pub const default_quic_listener_rate_window_us = options.default_quic_listener_rate_window_us;
pub const default_quic_max_log_events_per_source_per_window = options.default_quic_max_log_events_per_source_per_window;

pub const ServerQlogCallback = options.ServerQlogCallback;
pub const ServerLogEvent = options.ServerLogEvent;
pub const ServerLogCallback = options.ServerLogCallback;
pub const ServerRetryTokenKey = options.ServerRetryTokenKey;
pub const ServerNewTokenKey = options.ServerNewTokenKey;
pub const ServerAntiReplayTracker = options.ServerAntiReplayTracker;

pub const ClientOptions = options.ClientOptions;
pub const ServerOptions = options.ServerOptions;
pub const ServerProductionHardening = options.ServerProductionHardening;
pub const defaultTransportParams = options.defaultTransportParams;
pub const withProductionServerHardening = options.withProductionServerHardening;
pub const nullqServerConfigFromOptions = options.nullqServerConfigFromOptions;

pub const defaultClientBindAddress = adapter.defaultClientBindAddress;
pub const ipAddressToPathAddress = adapter.ipAddressToPathAddress;
pub const pathAddressToIpAddress = adapter.pathAddressToIpAddress;
