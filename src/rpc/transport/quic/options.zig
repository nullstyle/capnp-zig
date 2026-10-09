const std = @import("std");
const quic_zig = @import("quic");

const events = @import("../../events.zig");
const framing = @import("../../wire/framing.zig");
const length_framer = @import("length_framer.zig");
const early_dispatch_mod = @import("early_dispatch.zig");
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
/// Kernel receive buffer (`SO_RCVBUF`) this transport asks for on the UDP
/// socket it binds: quic-zig's recommendation for a QUIC endpoint, 4 MiB.
/// A burst that overflows the kernel queue is silent loss, and native mode
/// then collapses (see `defaultTransportParams`): on Linux the OS default
/// (208 KiB) lost a uni window of 8 at 50 ms RTT, and the request is what
/// lets that default window hold. Best effort: on Linux the request tries
/// `SO_RCVBUFFORCE` first (needs `CAP_NET_ADMIN`); otherwise it is capped at
/// `net.core.rmem_max` (stock 208 KiB, which the kernel doubles to 416 KiB).
/// Raise `net.core.rmem_max` and `wmem_max` on a Linux server to get the
/// full 4 MiB. macOS honors it up to `kern.ipc.maxsockbuf`. Windows keeps
/// its default, because quic-zig's helper does not support Windows sockets
/// yet.
pub const default_udp_socket_recv_buffer_bytes: usize = quic_zig.transport.socket_opts.default_server_recv_buffer_bytes;
/// Kernel send buffer (`SO_SNDBUF`) this transport asks for on its UDP
/// socket, best effort like `default_udp_socket_recv_buffer_bytes`.
pub const default_udp_socket_send_buffer_bytes: usize = quic_zig.transport.socket_opts.default_server_send_buffer_bytes;
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
/// What may execute inside the 0-RTT replay-hold window; see
/// `early_dispatch.zig`.
pub const EarlyDispatchMode = @import("early_dispatch.zig").Mode;

/// Single-session compatibility capacity used by `Connection.initServer`.
pub const compatibility_max_concurrent_sessions: u32 = 1;

/// Minimum interval between `on_tick` (peer deadline sweep) invocations on
/// the loop/step cadence. Floors the sweep so a datagram-hot loop does not
/// pay it per packet, while staying far below any realistic call deadline.
pub const min_tick_interval_us: u64 = 1_000;

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
/// Event payload for `ServerLogCallback`. Documented additive upstream —
/// always keep an `else` arm when switching on it.
pub const ServerLogEvent = quic_zig.Server.LogEvent;
pub const ServerLogCallback = quic_zig.Server.LogCallback;
pub const StatelessResetKey = quic_zig.conn.stateless_reset.Key;
pub const ServerRetryTokenKey = quic_zig.conn.RetryTokenKey;
pub const ServerNewTokenKey = quic_zig.conn.NewTokenKey;
pub const ServerAntiReplayTracker = quic_zig.tls.AntiReplayTracker;

/// A persisted session-ticket key, quic-zig's `SessionTicketKey`: a 16-byte
/// key name (sent in clear at the front of every ticket), a 16-byte
/// HMAC-SHA256 key and a 16-byte AES-128 key, in that order (BoringSSL's
/// layout). Generate all 48 bytes from a CSPRNG and keep them in a file of
/// their own (`loadTicketKeyFile`). See `ServerOptions.session_ticket_key`
/// and "Session-ticket key" in docs/quic-transport.md for what a stolen key
/// costs.
pub const SessionTicketKey = quic_zig.SessionTicketKey;

/// Upper bound of `ServerOptions.session_ticket_lifetime_s` and
/// `ClientOptions.session_ticket_lifetime_s`: 2 days, BoringSSL's default
/// TLS 1.3 ticket lifetime (`SSL_DEFAULT_SESSION_PSK_DHE_TIMEOUT`). quic-zig
/// accepts up to 7 days on both sides, but a BoringSSL client (every quic-zig
/// client, so every capnp-zig client) keeps a ticket for 2 days unless its
/// own limit is raised: a longer server lifetime buys those clients nothing,
/// and it keeps a ticket that a stolen key opens valid for longer. Both
/// options only shorten the default.
pub const max_session_ticket_lifetime_s: u32 = 2 * 24 * 60 * 60;

/// Ticket-capture callback for warm restore. Fires once quic-zig has a
/// complete, ready-to-persist resumption envelope for the connection; the
/// bytes are BORROWED for the duration of the call and must be copied.
/// Matches quic-zig's `Client.Config.new_session_callback` shape.
pub const NewSessionCallback = *const fn (user_data: ?*anyopaque, resumption_state: []const u8) void;
/// NEW_TOKEN capture callback, forwarded verbatim to quic-zig. A warm
/// sturdy ref should persist the NEW_TOKEN alongside the resumption
/// envelope — they are separate channels upstream, and only together do
/// they buy the full one-round-trip (address-validated) resume.
pub const NewTokenCallback = quic_zig.conn.NewTokenCallback;
/// 0-RTT outcome snapshot (`not_offered` / `accepted` / `rejected`),
/// readable from the underlying quic connection after the handshake.
pub const EarlyDataStatus = quic_zig.EarlyDataStatus;

/// Stream windows (quic v0.24.0 and later): `initial_max_streams_bidi` /
/// `_uni` is how many streams of that type the PEER may have open AT ONCE.
/// An id comes back once its stream is fully closed (for a one-shot native
/// data stream, about one round trip after it opens). There is no lifetime
/// cap.
///
/// - `initial_max_streams_uni = 8`. Native mode sends every frame above
///   `inline_frame_threshold` on its own one-shot uni stream, so this window
///   is how many large frames can be in flight per direction: about
///   `8 / RTT` frames per second. (The native control stream is the
///   client's bidirectional stream 0; it holds no uni slot.)
///
///   The window also sets the burst that the receiver's kernel UDP queue
///   must hold. An overflow there is silent loss, and loss collapses native
///   bulk throughput 10-30x. So the default is the largest window that held
///   on a stock Linux server: unprivileged, where the transport's 4 MiB
///   `SO_RCVBUF` request (`default_udp_socket_recv_buffer_bytes`) is capped
///   at `net.core.rmem_max` and gets 416 KiB. Measured with `bench-quic
///   --transport native --mode bulk --inflight 64` (64 KiB frames,
///   ReleaseSafe quic, a loopback delay relay for the RTT; Linux 7.0 in a
///   container with pinned sysctls, where the bench reports each socket's
///   kernel drops), MB/s per run:
///
///       kernel receive buffer      RTT    uni 4     uni 8       uni 16
///       Linux default, 208-224 KiB 20 ms  7.8-9.3   11.6-14.2   1.0-1.5
///                                  50 ms  3.8-3.9   1.2-7.6     0.5
///       Linux, request capped:     20 ms  9.3       17.4-18.0   1.3-22.3
///         416 KiB                  50 ms  3.8-3.9   7.5-7.7     0.8-2.9 [1]
///       Linux, 4 MiB granted       20 ms  9.2-9.4   17.9-18.4   32.4-34.7
///                                  50 ms  3.8       7.5-7.7     3.5-10.2
///       macOS, 4 MiB granted       20 ms  10.1      18.9-19.1   26.9-30.9
///                                  50 ms  3.9       7.8-7.9     15.7
///
///   [1] One more run failed after 473 of its 600 calls. With the capped
///   buffer, 12 also collapsed at 20 ms (1.2-6.9). Where the host grants
///   the 4 MiB (macOS; Linux with `net.core.rmem_max` and `wmem_max`
///   raised, or with `CAP_NET_ADMIN`), a window of 16 nearly doubles bulk
///   throughput, so raise it there. Windows above 16 were no faster even
///   with 4 MiB and no kernel drops (20 ms: 32 -> 6.5-9.8, 64 -> 9.7-11.3);
///   that second limit is not explained. On plain loopback the window does
///   not matter. (quic v0.19.0 DOUBLED the old value 4 as streams ended, so
///   its effective window was about 20.)
/// - `initial_max_streams_bidi = 16`. Both modes use exactly one
///   bidirectional stream (the client's stream 0), and every other peer
///   bidirectional stream is refused (`peer_streams.zig`), so this only
///   bounds how many streams a misbehaving peer can hold open at once.
///   Measured: bidi 16 vs 100 makes no difference to baseline call rate
///   or native throughput.
pub fn defaultTransportParams() quic_zig.tls.TransportParams {
    return .{
        .max_idle_timeout_ms = 30_000,
        .initial_max_data = 16 * 1024 * 1024,
        .initial_max_stream_data_bidi_local = 1 << 20,
        .initial_max_stream_data_bidi_remote = 1 << 20,
        .initial_max_stream_data_uni = 1 << 20,
        .initial_max_streams_bidi = 16,
        .initial_max_streams_uni = 8,
        .active_connection_id_limit = 4,
    };
}

/// `params` as an endpoint with a memory budget of `max_connection_memory`
/// announces them: the connection window (`initial_max_data`) at most half
/// of the budget. `serverConfigFromOptions` applies it with
/// `ServerOptions.max_connection_memory`. Nothing changes at the defaults: a
/// 16 MiB window and a 32 MiB budget. A client needs no clamp: it keeps
/// quic-zig's 32 MiB budget, and quic-zig refuses a window above 16 MiB
/// (endpoint_factory.zig checks the two values at comptime).
///
/// One budget holds what a connection writes and what its peer sends. Since
/// quic-zig v0.38.0 a stream write stops short of the receive side's share:
/// the connection window as announced, or quic-zig's window cap (16 MiB or
/// half of the budget, whichever is smaller) if that is larger. A window
/// larger than half of the budget leaves the writer less than half, and a
/// window as large as the budget leaves it nothing: every write returns zero
/// and the connection stalls with no error. With at most half announced, the
/// writer keeps at least half of the budget, and an honest peer never meets
/// EXCESSIVE_LOAD.
///
/// The stream windows (`initial_max_stream_data_*`) stay as given: quic-zig's
/// share counts the connection window only, which bounds what all streams
/// together may send.
pub fn transportParamsWithinBudget(
    params: quic_zig.tls.TransportParams,
    max_connection_memory: u64,
) quic_zig.tls.TransportParams {
    var out = params;
    out.initial_max_data = @min(params.initial_max_data, max_connection_memory / 2);
    return out;
}

pub const ClientOptions = struct {
    /// UDP local bind address. When null, an ephemeral unspecified address is
    /// chosen with the same address family as `remote_addr`. A NEW_TOKEN is
    /// valid only from the address and port that received it, so a server
    /// with Retry on skips the Retry only for a dial from that port;
    /// `WarmRedialClient` reuses each generation's port for its next dial.
    local_addr: ?Net.IpAddress = null,
    remote_addr: Net.IpAddress,
    server_name: []const u8,
    alpn_protocols: []const []const u8 = &.{alpn},
    /// quic-zig refuses a connection window (`initial_max_data`) above
    /// 16 MiB with `error.InvalidValue`. That is half of a client's memory
    /// budget (quic-zig's default, 32 MiB), so a client needs no clamp; see
    /// `transportParamsWithinBudget`.
    transport_params: quic_zig.tls.TransportParams = defaultTransportParams(),
    receive_timeout: std.Io.Duration = std.Io.Duration.fromMilliseconds(5),
    /// Give up on a dial whose handshake has not completed within this
    /// window: the connection aborts locally with the certified cause
    /// `DisconnectCause.handshake_timeout`. Without it, a dial whose
    /// every Initial is silently dropped (server table full, path black
    /// hole) waits FOREVER — no QUIC timer fires on a connection that
    /// never completes its handshake and stops sending. Null disables
    /// the guard (deliberate opt-out, not an unset default).
    handshake_timeout_ms: ?u64 = 30_000,
    udp_rx_buffer_size: usize = default_udp_rx_buffer_size,
    udp_tx_buffer_size: usize = default_udp_tx_buffer_size,
    /// Kernel `SO_RCVBUF` to request for this transport's UDP socket, best
    /// effort; null keeps the OS default. See
    /// `default_udp_socket_recv_buffer_bytes`.
    udp_socket_recv_buffer_bytes: ?usize = default_udp_socket_recv_buffer_bytes,
    /// Kernel `SO_SNDBUF` to request for this transport's UDP socket, best
    /// effort; null keeps the OS default.
    udp_socket_send_buffer_bytes: ?usize = default_udp_socket_send_buffer_bytes,
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
    /// Followed through quic-zig v0.16.0's default flip to BBRv3 (their
    /// fairness-battery-gated change); `.cubic` remains the one-line
    /// rollback at this layer, mirrored server-side by
    /// `ServerOptions.congestion_control`.
    congestion_control: quic_zig.CongestionAlgorithm = .bbr,
    enable_pacing: bool = true,
    enable_hystart: bool = true,

    /// Warm restore (durable-caps ladder, prototype #2). A resumption
    /// envelope previously captured via `new_session_callback`. When set,
    /// the dial resumes the TLS session, enables 0-RTT, and this
    /// transport opens its RPC stream BEFORE the handshake completes, so
    /// frames enqueued before the loop starts ride early data. Safe by
    /// quic-zig contract: on 0-RTT rejection the staged bytes are
    /// requeued verbatim at 1-RTT, so a stale ticket costs a round trip,
    /// never data. Restore-style calls sent this way must be idempotent
    /// — 0-RTT is replayable by design. The bytes are read during
    /// connect and need not outlive it.
    resumption_state: ?[]const u8 = null,
    /// Capture the resumption envelope for a later warm dial. Borrowed
    /// bytes — copy them in the callback.
    new_session_callback: ?NewSessionCallback = null,
    new_session_user_data: ?*anyopaque = null,
    /// The longest this client keeps a session ticket, in seconds
    /// (Experimental; quic-zig's `Client.Config.session_ticket_lifetime_s`).
    /// The client keeps each ticket for the smaller of this value and the
    /// lifetime the server gave it, and does not offer an older ticket: the
    /// dial then takes a full handshake. Null keeps BoringSSL's 2 days. Valid
    /// range 1..`max_session_ticket_lifetime_s` (2 days, the most a
    /// capnp-zig server issues), so the option only shortens; anything else
    /// is `error.InvalidConfig`.
    session_ticket_lifetime_s: ?u32 = null,
    /// Address-validation NEW_TOKEN from a prior connection to this
    /// server, presented on the first Initial so the resume skips Retry.
    new_token: ?[]const u8 = null,
    /// Capture NEW_TOKENs as the server issues them; persist alongside
    /// the resumption envelope.
    new_token_callback: ?NewTokenCallback = null,
    new_token_user_data: ?*anyopaque = null,

    /// Dictated initial DCID (durable-caps ladder, rendezvous dials). When
    /// set, these exact bytes ride the very first Initial instead of
    /// quic-zig's random mint, letting a server that handed them out
    /// out-of-band (a provision ticket) recognize and route the dial from
    /// its first datagram. Must be 8..20 bytes, and MUST be minted from a
    /// CSPRNG — Initial packet-protection keys derive from this value, and
    /// quic-zig validates only the length. The value is plaintext on the
    /// wire: routing, never authorization. A server Retry replaces it on
    /// the wire, so provision-routing servers must not Retry these dials.
    /// The bytes are copied during connect and need not outlive the call.
    initial_dcid: ?[]const u8 = null,
};

pub const ServerOptions = struct {
    listen_addr: Net.IpAddress,
    tls_cert_pem: []const u8,
    tls_key_pem: []const u8,
    alpn_protocols: []const []const u8 = &.{alpn},
    /// Forwarded verbatim to quic-zig. NEVER hand-set the
    /// `stateless_reset_token` member: the accept path only OVERWRITES it
    /// when `stateless_reset_key` is set, so a hand-set value advertises
    /// one FIXED token to every peer this server accepts — any peer that
    /// completed a handshake could then reset any other peer's
    /// connection, and the emitter (which derives tokens from the key)
    /// could never honor it anyway. Set `stateless_reset_key` instead and
    /// let quic-zig derive per-CID tokens — §18.2's token belongs to the
    /// HANDSHAKE CID, which differs per connection, so a value living in
    /// per-server config cannot be correct for more than one peer by
    /// construction. As of the pinned quic v0.16.1 this is ENFORCED:
    /// `Server.init` refuses the keyless combination with
    /// `error.InvalidConfig` (a hand-set token alongside a real key stays
    /// accepted — the accept path overwrites it per connection, so it is
    /// merely inert). Pins at v0.16.0 and earlier caught nothing here.
    ///
    /// The server announces at most half of `max_connection_memory` as its
    /// connection window (`initial_max_data`); see
    /// `transportParamsWithinBudget`.
    transport_params: quic_zig.tls.TransportParams = defaultTransportParams(),
    max_concurrent_connections: u32 = 1,
    local_cid_len: u8 = default_quic_local_cid_len,
    qlog_callback: ?ServerQlogCallback = null,
    qlog_user_data: ?*anyopaque = null,
    log_callback: ?ServerLogCallback = null,
    log_user_data: ?*anyopaque = null,
    /// Stateless-reset emitter key (RFC 9000 §10.3). When set, quic-zig
    /// derives a reset token per issued CID, advertises the §18.2
    /// transport-param token for the handshake CID (which is what lets
    /// CLIENTS prove a crash-restart via `DisconnectCause
    /// .stateless_reset`), and answers unroutable short-header datagrams
    /// with resets (counted by `Server.statelessResetsSent`). PERSIST
    /// this key across restarts — a fresh key invalidates every
    /// previously issued token, and connections that survived the
    /// restart lose their death certificate. Null (no resets) is the
    /// default only for raw options; `withProductionServerHardening`
    /// requires a key.
    stateless_reset_key: ?quic_zig.conn.stateless_reset.Key = null,
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
    /// The clock that NEW_TOKEN times are stamped and checked with, in
    /// microseconds (Experimental; quic-zig's
    /// `Server.Config.new_token_clock`). Null (the default) uses the
    /// listener's clock (`Listener.nowUs`), which starts at the wall clock
    /// and goes on across a restart, so a persisted `new_token_key` already
    /// lets returning clients skip the Retry after a restart. Set it only
    /// for a clock of your own that every process agrees on. quic-zig's
    /// `unixWallClockUs` compiles from v0.30.1 (at the v0.29.0 pin it did
    /// not, with Zig 0.17.0).
    new_token_clock: ?*const fn () u64 = null,
    /// How far a NEW_TOKEN's times may be off the clock when the server
    /// checks it, in microseconds (Experimental; quic-zig's
    /// `Server.Config.new_token_max_clock_skew_us`): a token from the future
    /// by at most this much is taken, and one past its expiry by at most this
    /// much too. Default 0. A few seconds absorb a wall clock that stepped
    /// back between the process that issued the token and the one that
    /// checks it (see `Listener.nowUs`).
    new_token_max_clock_skew_us: u64 = 0,
    early_data: EarlyData = .disabled,
    /// What may EXECUTE off frames that arrived in 0-RTT early data before
    /// the handshake completes (meaningful only with
    /// `early_data = .without_replay_protection`; `.with_anti_replay`
    /// dispatches immediately — the tracker guarantees single use).
    /// `.hold_until_handshake` (default) buffers everything;
    /// `.restore_only` executes the idempotent prefix (Bootstrap +
    /// Restorer calls) early so a warm restore answers without waiting for
    /// the handshake. Baseline mode only; native mode always holds. The
    /// hardened preset sets this together with `early_data` (see
    /// `ProductionEarlyData`).
    early_dispatch: early_dispatch_mod.Mode = .hold_until_handshake,
    /// Opt-in persisted session-ticket key (Experimental). Null (the
    /// default) keeps BoringSSL's per-process random key, so a restarted
    /// server cannot decrypt the tickets its predecessor issued and the
    /// first redial after a crash-restart takes a full handshake. With the
    /// same key on every start, that redial resumes, and with `early_data`
    /// enabled BoringSSL accepts its 0-RTT data.
    ///
    /// `serverConfigFromOptions` copies the key into quic-zig's
    /// `Server.Config.session_ticket_key`, and quic-zig installs it on every
    /// TLS context it builds, also on a `.pem` reload
    /// (`replaceTlsContext`). `Listener.init` (and so `Server.init`, `serve`
    /// and `Connection.initServer`) zeroes its own copy once quic-zig's
    /// server is built and keeps no pointer to yours: you may zero your
    /// copy when `init` returns. A thief who copies the key can decrypt recorded
    /// 0-RTT data (sturdy refs included) and impersonate the server to
    /// resuming clients until their tickets expire, but cannot read 1-RTT
    /// traffic. Read "Session-ticket key" in docs/quic-transport.md before
    /// setting it.
    ///
    /// Refused with `error.InvalidConfig`: an all-zero key, and a key
    /// together with `early_data = .with_anti_replay` (the tracker is
    /// per-process memory, and a persisted key lets a pre-crash flight
    /// replay after the restart). With Retry on (`retry_token_key`) also
    /// persist `new_token_key`: a returning client with a valid NEW_TOKEN
    /// skips the Retry, and one without pays a round trip for it (its
    /// 0-RTT restore still runs before the handshake completes).
    session_ticket_key: ?*const SessionTicketKey = null,
    /// Lifetime, in seconds, of the TLS 1.3 session tickets this server
    /// issues (quic-zig's `Server.Config.session_ticket_lifetime_s`). Null
    /// keeps BoringSSL's 2 days. Valid range 1..`max_session_ticket_lifetime_s`
    /// (2 days, the most a capnp-zig client keeps a ticket): the option only
    /// shortens the window in which a stolen `session_ticket_key` lets a
    /// thief impersonate the server.
    session_ticket_lifetime_s: ?u32 = null,
    /// The ticket key before `session_ticket_key` (Experimental; quic-zig's
    /// `Server.Config.previous_session_ticket_key`). Set it when a process
    /// starts less than one ticket lifetime after a key change: the server
    /// still OPENS the tickets that this key sealed (0-RTT too), and seals
    /// new tickets under `session_ticket_key`. Null (the default): the
    /// server has one key. A running server changes its key with
    /// `Server.rotateSessionTicketKey` instead.
    ///
    /// The same secret as `session_ticket_key`, with the same handling:
    /// `serverConfigFromOptions` copies it by value, `Listener.init` zeroes
    /// that copy once quic-zig's server is built, and quic-zig clears its
    /// own copy when the key's time is over and at `deinit`. Nothing keeps
    /// your pointer. See "Rotation" in docs/quic-transport.md.
    ///
    /// Refused with `error.InvalidConfig`: a previous key with no
    /// `session_ticket_key`, an all-zero key, and a key with the same name
    /// (first 16 bytes) as `session_ticket_key` (a ticket names its key by
    /// those bytes alone).
    previous_session_ticket_key: ?*const SessionTicketKey = null,
    /// When `previous_session_ticket_key` stops opening tickets, in
    /// microseconds on the listener's clock (`Listener.nowUs`: microseconds
    /// since the Unix epoch, and it goes on across a restart). Give the time
    /// of the key change plus one ticket lifetime, and the old key ends when
    /// it would have ended in the process before. Null (the default): one
    /// ticket lifetime (`session_ticket_lifetime_s`, or 2 days) after the
    /// first datagram or tick. Ignored without a previous key.
    previous_session_ticket_key_until_us: ?u64 = null,
    /// Sweep out sessions whose handshake has not completed within this
    /// window (certified cause `DisconnectCause.handshake_timeout`).
    /// Half-open connections are otherwise IMMORTAL — no QUIC timer
    /// fires on a connection that never finishes its handshake and goes
    /// quiet — so under churn, loss, or attack they accumulate until
    /// `max_concurrent_connections` pins and the server silently refuses
    /// every new dial (the QUIC analog of a SYN flood; measured in the
    /// soak: the whole table `.open`, hundreds of silent `table_full`
    /// drops). Null disables the guard (deliberate opt-out).
    handshake_timeout_ms: ?u64 = 10_000,
    /// Congestion-control selection for accepted connections, forwarded
    /// verbatim to quic-zig. Mirrors `ClientOptions.congestion_control`
    /// (same follow-upstream default policy — BBRv3 since quic-zig
    /// v0.16.0; `.cubic` is the rollback). This field previously did not
    /// exist, which silently split posture: servers followed upstream's
    /// default while clients used this transport's own field default.
    congestion_control: quic_zig.CongestionAlgorithm = .bbr,
    reveal_close_reason_on_wire: bool = false,
    /// What one connection may hold (quic-zig's `max_connection_memory`):
    /// send buffers, receive buffers, CRYPTO and DATAGRAM data together. A
    /// write past it returns short (back-pressure). quic-zig keeps the
    /// peer's share out of the writes, the connection window, so the server
    /// announces at most half of the budget as its connection window
    /// (`transport_params.initial_max_data`; `transportParamsWithinBudget`).
    /// Below twice the configured window (32 MiB with the default 16 MiB
    /// window) the budget therefore sets the announced window, which is part
    /// of the 0-RTT context (changing the budget across a restart refuses
    /// 0-RTT on older tickets) and bounds native mode's largest data-stream
    /// frame (docs/quic-transport.md, "Current Limits").
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
    /// Kernel `SO_RCVBUF` to request for the listening UDP socket, best
    /// effort; null keeps the OS default. See
    /// `default_udp_socket_recv_buffer_bytes`.
    udp_socket_recv_buffer_bytes: ?usize = default_udp_socket_recv_buffer_bytes,
    /// Kernel `SO_SNDBUF` to request for the listening UDP socket, best
    /// effort; null keeps the OS default.
    udp_socket_send_buffer_bytes: ?usize = default_udp_socket_send_buffer_bytes,
    stream_read_buffer_size: usize = default_stream_read_buffer_size,
    max_message_bytes: usize = default_max_message_bytes,
    max_outbound_queue_items: usize = default_max_outbound_queue_items,
    max_outbound_queue_bytes: usize = default_max_outbound_queue_bytes,
    mode: TransportMode = .baseline,
    native: NativeOptions = .{},
    observer: ?events.Observer = null,
};

/// 0-RTT posture of the hardened preset (`ServerProductionHardening
/// .early_data`). Each value sets BOTH `ServerOptions.early_data` and
/// `ServerOptions.early_dispatch`, so the preset can never pair replayable
/// early data with immediate dispatch of arbitrary calls.
pub const ProductionEarlyData = enum {
    /// Refuse 0-RTT (the default). A resumed client's staged frames are
    /// requeued at 1-RTT: a stale or refused ticket costs one round trip,
    /// never data. Sets `early_data = .disabled` and `early_dispatch =
    /// .hold_until_handshake`.
    disabled,
    /// Opt in to warm restore. Accept 0-RTT WITHOUT a replay tracker
    /// (`EarlyData.without_replay_protection`), and execute only the
    /// idempotent restore prefix (Bootstrap frames and Restorer calls)
    /// before the handshake completes (`EarlyDispatchMode.restore_only`).
    /// Every other early frame, and everything behind it, waits for the
    /// handshake, which a replayed first flight can never complete. A replay
    /// can therefore re-run only the restore itself, so the application's
    /// Restorer MUST be idempotent (the vat restore convention already
    /// requires this). Native mode holds every early frame until the
    /// handshake.
    restore_only,
};

pub const ServerProductionHardening = struct {
    retry_token_key: ServerRetryTokenKey,
    /// Stateless-reset key (RFC 9000 §10.3). Required: without it the
    /// server never sends a stateless reset, so after a crash-restart its
    /// clients cannot prove the old connection is gone. They see
    /// `DisconnectCause.idle_timeout` instead of `.stateless_reset`, and
    /// `WarmRedialClient` (which redials on `.stateless_reset` by default)
    /// never heals them. Generate it once from a CSPRNG and PERSIST it: a
    /// restarted server must hold the same bytes, because a new key
    /// invalidates every token the old process issued. Keep it secret: anyone
    /// who has it can reset this server's connections. Share one key between
    /// instances only when the load balancer routes by connection ID (RFC
    /// 9000 §21.11): under address-hash routing, a sibling that receives a
    /// live connection's packets sends a valid reset and kills it. Otherwise
    /// give each instance its own persisted key. See "Production Defaults"
    /// in docs/quic-transport.md for a recipe and the sharing rules.
    stateless_reset_key: StatelessResetKey,
    new_token_key: ?ServerNewTokenKey = null,
    /// 0-RTT posture. `.disabled` by default; `.restore_only` is the
    /// explicit warm-restore opt-in. See `ProductionEarlyData`.
    early_data: ProductionEarlyData = .disabled,
    /// Opt-in persisted session-ticket key; see
    /// `ServerOptions.session_ticket_key`. Null by default: the preset
    /// never sets one. The preset always sets `retry_token_key`, so persist
    /// `new_token_key` with the key: without a valid NEW_TOKEN every
    /// restarted client pays a Retry (one round trip) before its 0-RTT
    /// restore runs.
    session_ticket_key: ?*const SessionTicketKey = null,
    /// The ticket key before `session_ticket_key`, and when it stops opening
    /// tickets; see `ServerOptions.previous_session_ticket_key`. Null by
    /// default. The preset sets both with `session_ticket_key`, so the key
    /// pair always comes from one place.
    previous_session_ticket_key: ?*const SessionTicketKey = null,
    previous_session_ticket_key_until_us: ?u64 = null,
    initial_source_rate_limit: RateLimit = .{ .limit = default_quic_initial_source_rate_cap },
    vn_source_rate_limit: RateLimit = .default,
    listener_datagram_rate_limit: RateLimit = .{ .limit = 100_000 },
    listener_byte_rate_limit: RateLimit = .{ .limit = 128 * 1024 * 1024 },
    source_byte_rate_limit: RateLimit = .{ .limit = 16 * 1024 * 1024 },
    max_connection_memory: u64 = default_quic_max_connection_memory,
    log_source_rate_limit: RateLimit = .default,
};

/// Apply the production preset to `options`. Every field the preset names
/// OVERRIDES the base value, including `stateless_reset_key`,
/// `session_ticket_key` and the previous ticket key, the 0-RTT pair
/// (`early_data` + `early_dispatch`, from
/// `ServerProductionHardening.early_data`), and
/// `reveal_close_reason_on_wire` (always false).
pub fn withProductionServerHardening(
    options: ServerOptions,
    hardening: ServerProductionHardening,
) ServerOptions {
    var out = options;
    out.retry_token_key = hardening.retry_token_key;
    out.stateless_reset_key = hardening.stateless_reset_key;
    out.new_token_key = hardening.new_token_key;
    out.session_ticket_key = hardening.session_ticket_key;
    out.previous_session_ticket_key = hardening.previous_session_ticket_key;
    out.previous_session_ticket_key_until_us = hardening.previous_session_ticket_key_until_us;
    out.initial_source_rate_limit = hardening.initial_source_rate_limit;
    out.vn_source_rate_limit = hardening.vn_source_rate_limit;
    out.listener_datagram_rate_limit = hardening.listener_datagram_rate_limit;
    out.listener_byte_rate_limit = hardening.listener_byte_rate_limit;
    out.source_byte_rate_limit = hardening.source_byte_rate_limit;
    out.max_connection_memory = hardening.max_connection_memory;
    out.log_source_rate_limit = hardening.log_source_rate_limit;
    switch (hardening.early_data) {
        .disabled => {
            out.early_data = .disabled;
            out.early_dispatch = .hold_until_handshake;
        },
        .restore_only => {
            out.early_data = .without_replay_protection;
            out.early_dispatch = .restore_only;
        },
    }
    out.reveal_close_reason_on_wire = false;
    return out;
}

/// Build the quic-zig server config for `options`, the one `Listener.init`
/// hands to `quic_zig.Server.init`.
///
/// With `session_ticket_key` (or `previous_session_ticket_key`) set, the
/// returned config holds a COPY of each key, by value
/// (`Server.Config.session_ticket_key`, `previous_session_ticket_key`):
/// secrets. Zero them (`zeroServerConfigSecrets`) once
/// `quic_zig.Server.init` has returned, as `Listener.init` does, and keep
/// the config out of logs.
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
        .transport_params = transportParamsWithinBudget(options.transport_params, options.max_connection_memory),
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
        .stateless_reset_key = options.stateless_reset_key,
        .retry_token_lifetime_us = options.retry_token_lifetime_us,
        .retry_state_table_capacity = options.retry_state_table_capacity,
        .new_token_key = options.new_token_key,
        .new_token_lifetime_us = options.new_token_lifetime_us,
        .new_token_clock = options.new_token_clock,
        .new_token_max_clock_skew_us = options.new_token_max_clock_skew_us,
        .early_data = options.early_data,
        .early_data_application_context = earlyDataApplicationContext(options.mode, options.early_dispatch),
        .session_ticket_key = if (options.session_ticket_key) |key| key.* else null,
        .session_ticket_lifetime_s = options.session_ticket_lifetime_s,
        .previous_session_ticket_key = if (options.previous_session_ticket_key) |key| key.* else null,
        .previous_session_ticket_key_until_us = options.previous_session_ticket_key_until_us,
        .congestion_control = options.congestion_control,
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
    if (zeroSocketBuffer(options.udp_socket_recv_buffer_bytes) or
        zeroSocketBuffer(options.udp_socket_send_buffer_bytes)) return error.InvalidConfig;
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
    // quic-zig's `Server.init` refuses both of these too; checking here keeps
    // the refusal at the capnp-zig boundary, where the reason is documented.
    if (options.session_ticket_key) |key| {
        // A zeroed buffer is a missing key, never a secret.
        if (std.mem.allEqual(u8, key, 0)) return error.InvalidConfig;
        // The replay tracker lives in this process's memory. A persisted
        // key lets a flight recorded before a crash resume after the
        // restart, where the empty tracker calls it fresh and every early
        // call runs twice (docs/quic-transport.md).
        if (std.meta.activeTag(options.early_data) == .with_anti_replay) return error.InvalidConfig;
        // A key with Retry on and no `new_token_key` is allowed: every
        // returning client then pays a Retry, and its 0-RTT data still
        // arrives before the handshake completes (quic-zig v0.27.0 sends
        // it again after the Retry). The trade-off is documented in
        // "Session-ticket key", docs/quic-transport.md.
    }
    if (options.session_ticket_lifetime_s) |lifetime_s| {
        if (lifetime_s == 0 or lifetime_s > max_session_ticket_lifetime_s) return error.InvalidConfig;
    }
    // The three refusals of quic-zig's `Server.init` for a previous key.
    if (options.previous_session_ticket_key) |previous| {
        // Nothing to keep the old key beside.
        const current = options.session_ticket_key orelse return error.InvalidConfig;
        if (std.mem.allEqual(u8, previous, 0)) return error.InvalidConfig;
        // A ticket names its key by the first 16 bytes alone, so two keys
        // with one name cannot both open tickets.
        const name_len = quic_zig.tls.session_ticket.name_len;
        if (std.mem.eql(u8, previous[0..name_len], current[0..name_len])) return error.InvalidConfig;
    }
    try validateNativeOptions(options.mode, options.native, options.max_message_bytes);
}

/// Zero the session-ticket keys that `serverConfigFromOptions` copied into
/// `config` (`session_ticket_key`, `previous_session_ticket_key`). Call it
/// once `quic_zig.Server.init` has returned, on every path: quic-zig keeps
/// copies of its own. `Listener.init` does this for you. Experimental.
pub fn zeroServerConfigSecrets(config: *quic_zig.Server.Config) void {
    if (config.session_ticket_key) |*key| std.crypto.secureZero(u8, key);
    if (config.previous_session_ticket_key) |*key| std.crypto.secureZero(u8, key);
}

/// The application half of the RFC 9001 §4.6.1 0-RTT context that this
/// server binds into its session tickets (quic-zig
/// `Server.Config.early_data_application_context`). quic-zig hashes it
/// together with the primary ALPN (`alpn_protocols[0]`) and the
/// replay-relevant transport parameters, and BoringSSL also refuses early
/// data when the negotiated ALPN differs from the ticket's. This string adds
/// the two settings that decide how capnp-zig reads and runs early frames:
/// the transport mode (baseline and native frame the early bytes
/// differently) and `early_dispatch` (what may run before the handshake). A
/// server restarted with either one changed still resumes the session, but
/// BoringSSL refuses its 0-RTT data, so a persisted `session_ticket_key`
/// never carries early data across such a change. Static strings: quic-zig
/// borrows the slice for the server's lifetime.
fn earlyDataApplicationContext(mode: TransportMode, dispatch: early_dispatch_mod.Mode) []const u8 {
    return switch (mode) {
        inline else => |m| switch (dispatch) {
            inline else => |d| "capnp-zig rpc 0-rtt v1; mode=" ++ @tagName(m) ++ "; early_dispatch=" ++ @tagName(d),
        },
    };
}

pub fn validateClientOptions(options: ClientOptions) !void {
    if (options.alpn_protocols.len == 0) return error.InvalidConfig;
    if (options.initial_dcid) |dcid| {
        if (dcid.len < 8 or dcid.len > 20) return error.InvalidConfig;
    }
    if (options.udp_rx_buffer_size == 0 or
        options.udp_tx_buffer_size == 0 or
        options.stream_read_buffer_size == 0 or
        options.max_message_bytes == 0 or
        options.max_outbound_queue_items == 0 or
        options.max_outbound_queue_bytes == 0)
    {
        return error.InvalidConfig;
    }
    if (zeroSocketBuffer(options.udp_socket_recv_buffer_bytes) or
        zeroSocketBuffer(options.udp_socket_send_buffer_bytes)) return error.InvalidConfig;
    if (options.session_ticket_lifetime_s) |lifetime_s| {
        if (lifetime_s == 0 or lifetime_s > max_session_ticket_lifetime_s) return error.InvalidConfig;
    }
    try validateNativeOptions(options.mode, options.native, options.max_message_bytes);
}

/// A kernel socket buffer request of zero bytes is a configuration error
/// (null, not zero, keeps the OS default).
fn zeroSocketBuffer(bytes: ?usize) bool {
    return if (bytes) |b| b == 0 else false;
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
