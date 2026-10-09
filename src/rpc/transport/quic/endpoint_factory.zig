const std = @import("std");
const quic_zig = @import("quic");

const endpoint_mod = @import("endpoint.zig");
const listener_mod = @import("listener.zig");
const quic_options = @import("options.zig");
const quic_zig_adapter = @import("quic_zig_adapter.zig");

const Net = std.Io.net;

const ClientOptions = quic_options.ClientOptions;
const Endpoint = endpoint_mod.Endpoint;
const Role = endpoint_mod.Role;
const ServerOptions = quic_options.ServerOptions;

pub const CreatedEndpoint = struct {
    role: Role,
    endpoint: Endpoint,

    pub fn deinit(self: *CreatedEndpoint, io: std.Io) void {
        self.endpoint.driver(io).deinit();
        self.* = undefined;
    }
};

/// A client's memory budget: quic-zig's default, 32 MiB. `ClientOptions` has
/// no knob for it.
const client_max_connection_memory = quic_options.default_quic_max_connection_memory;

comptime {
    // A connection window larger than half of the budget takes from what the
    // connection may write (quic-zig v0.38.0), and the server clamps its
    // window for that (`transportParamsWithinBudget`). A client needs no
    // clamp: quic-zig refuses a window above
    // `max_initial_connection_receive_window` (16 MiB) with
    // `error.InvalidValue`, and that is half of the client's budget. If either
    // value moves, clamp the client's window as the server does. The transport
    // test "quic client's quic-zig connection gets the client budget and
    // announces at most half of it" reads both back from the connection.
    std.debug.assert(quic_zig.conn.state.max_initial_connection_receive_window <= client_max_connection_memory / 2);
}

pub fn initClient(
    allocator: std.mem.Allocator,
    io: std.Io,
    options: ClientOptions,
) !CreatedEndpoint {
    const local_addr = options.local_addr orelse quic_zig_adapter.defaultClientBindAddress(options.remote_addr);
    const socket = try Net.IpAddress.bind(&local_addr, io, .{
        .mode = .dgram,
        .protocol = .udp,
    });
    errdefer socket.close(io);
    quic_zig_adapter.requestUdpSocketBuffers(socket.handle, options.udp_socket_recv_buffer_bytes, options.udp_socket_send_buffer_bytes);

    var client = try quic_zig.Client.connect(.{
        .allocator = allocator,
        .server_name = options.server_name,
        .alpn_protocols = options.alpn_protocols,
        // At most half of `client_max_connection_memory` by construction
        // (see the comptime check above `initClient`); a larger window is
        // refused.
        .transport_params = options.transport_params,
        .max_connection_memory = client_max_connection_memory,
        .ca_pem = options.ca_pem,
        .insecure_skip_verify = options.insecure_skip_verify,
        // Forwarded so the documented per-flip opt-outs are reachable from
        // this transport; see ClientOptions for why that was not true before.
        .congestion_control = options.congestion_control,
        .enable_pacing = options.enable_pacing,
        .enable_hystart = options.enable_hystart,
        // Warm-restore surface (see ClientOptions): resumption enables
        // 0-RTT inside quic-zig; the capture callbacks hand tickets and
        // NEW_TOKENs back to the embedder for the next dial.
        .resumption_state = options.resumption_state,
        .new_session_callback = options.new_session_callback,
        .new_session_user_data = options.new_session_user_data,
        .session_ticket_lifetime_s = options.session_ticket_lifetime_s,
        .new_token = options.new_token,
        .new_token_callback = options.new_token_callback,
        .new_token_user_data = options.new_token_user_data,
        // Rendezvous surface (see ClientOptions.initial_dcid): dictated
        // bytes replace quic-zig's random mint on the first Initial.
        .initial_dcid = options.initial_dcid,
    });
    errdefer client.deinit();

    return .{
        .role = .client,
        .endpoint = .{ .client = .{
            .socket = socket,
            .transport = client,
            .remote_addr = options.remote_addr,
            .start_timestamp = std.Io.Timestamp.now(io, .awake),
            .receive_timeout = options.receive_timeout,
        } },
    };
}

pub fn initServer(
    allocator: std.mem.Allocator,
    io: std.Io,
    options: ServerOptions,
) !CreatedEndpoint {
    var listener = try listener_mod.Listener.init(allocator, io, options);
    errdefer listener.deinit();

    return .{
        .role = .server,
        .endpoint = .{ .server = .{ .listener = listener } },
    };
}
