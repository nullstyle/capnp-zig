//! QUIC transport surface for Cap'n Proto RPC.
//!
//! There are two server-side shapes in this package:
//!
//! * `Connection.initServer` is the compatibility path. It presents one
//!   accepted QUIC session as the same peer-facing `Connection` abstraction used
//!   by clients.
//! * `Server` is the fanout path. It owns one listener, accepts multiple QUIC
//!   sessions up to `ServerOptions.max_concurrent_connections`, and lets callers
//!   drive each `ServerSession` independently.
//! * `Listener` plus `Session`/`AcceptedSession` is the lower-level boundary for
//!   tests and bespoke embedding.

const builtin = @import("builtin");
const adapter = @import("quic_zig_adapter.zig");
pub const close = @import("close.zig");
const conn = @import("connection.zig");
const connection_testing = @import("connection_testing.zig");
pub const endpoint = @import("endpoint.zig");
const framer = @import("length_framer.zig");
const native_framer = @import("native_framer.zig");
pub const listener = @import("listener.zig");
const options = @import("options.zig");
const scheduler_mod = @import("scheduler.zig");
const server_mod = @import("server.zig");
pub const session = @import("session.zig");

pub const enabled = true;
/// Compatibility transport for a single vat-to-vat QUIC RPC session.
pub const Connection = conn.Connection;
pub const ApplicationCloseCode = close.ApplicationCloseCode;
pub const CloseStatus = close.Status;
/// Borrowed server-side session selected by a listener.
pub const AcceptedSession = session.AcceptedSession;
/// One-session driver used by the server `Connection` compatibility path.
pub const AcceptedSessionDriver = session.AcceptedSessionDriver;
pub const ClientEndpoint = endpoint.ClientEndpoint;
pub const Endpoint = endpoint.Endpoint;
pub const EndpointDriver = endpoint.EndpointDriver;
/// Server listener/fanout root. Owns the UDP socket and `quic_zig.Server`.
pub const Listener = listener.Listener;
pub const Role = endpoint.Role;
/// Endpoint facade used by `Connection.initServer` to attach one accepted
/// listener session to peer-facing callbacks.
pub const ServerEndpoint = endpoint.ServerEndpoint;
/// Borrowed handle for one accepted server-side QUIC session.
pub const Session = session.Session;
pub const LengthDelimitedFramer = framer.LengthDelimitedFramer;
pub const NativeControlFramer = native_framer.ControlFramer;
pub const NativeControlFrame = native_framer.ControlFrame;
/// Multi-session server/fanout root. Owns the UDP socket and per-session RPC
/// transport drivers.
pub const Server = server_mod.Server;
/// Per-accepted-session RPC transport owned by `Server`.
pub const ServerSession = server_mod.ServerSession;
pub const ServerReceiveResult = server_mod.ReceiveResult;
pub const scheduler = scheduler_mod;
pub const StepMode = scheduler_mod.StepMode;
pub const StepResult = scheduler_mod.StepResult;
pub const length_prefix_bytes = framer.length_prefix_bytes;
pub const native = native_framer;
pub const testing = if (builtin.is_test) struct {
    pub const ConnectionAccess = connection_testing.Access(conn.Connection);
} else struct {};

pub const alpn = options.alpn;
pub const baseline_stream_id = options.baseline_stream_id;
pub const default_udp_rx_buffer_size = options.default_udp_rx_buffer_size;
pub const default_udp_tx_buffer_size = options.default_udp_tx_buffer_size;
pub const default_stream_read_buffer_size = options.default_stream_read_buffer_size;
pub const default_max_message_bytes = options.default_max_message_bytes;
pub const default_max_outbound_queue_items = options.default_max_outbound_queue_items;
pub const default_max_outbound_queue_bytes = options.default_max_outbound_queue_bytes;
pub const default_native_inline_frame_threshold = options.default_native_inline_frame_threshold;
pub const default_native_max_control_frame_bytes = options.default_native_max_control_frame_bytes;
pub const default_native_max_pending_data_streams = options.default_native_max_pending_data_streams;
pub const default_native_max_pending_data_bytes = options.default_native_max_pending_data_bytes;
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
pub const compatibility_max_concurrent_sessions = options.compatibility_max_concurrent_sessions;
pub const supported_max_concurrent_sessions = options.supported_max_concurrent_sessions;

pub const ServerQlogCallback = options.ServerQlogCallback;
pub const ServerLogEvent = options.ServerLogEvent;
pub const ServerLogCallback = options.ServerLogCallback;
pub const ServerRetryTokenKey = options.ServerRetryTokenKey;
pub const ServerNewTokenKey = options.ServerNewTokenKey;
pub const ServerAntiReplayTracker = options.ServerAntiReplayTracker;

pub const ClientOptions = options.ClientOptions;
pub const ServerOptions = options.ServerOptions;
pub const TransportMode = options.TransportMode;
pub const NativeOptions = options.NativeOptions;
pub const NativeConfigError = options.NativeConfigError;
pub const ServerProductionHardening = options.ServerProductionHardening;
pub const defaultTransportParams = options.defaultTransportParams;
pub const withProductionServerHardening = options.withProductionServerHardening;
pub const serverConfigFromOptions = options.serverConfigFromOptions;

pub const defaultClientBindAddress = adapter.defaultClientBindAddress;
pub const ipAddressToPathAddress = adapter.ipAddressToPathAddress;
pub const pathAddressToIpAddress = adapter.pathAddressToIpAddress;
