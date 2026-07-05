const std = @import("std");
const quic_zig = @import("quic_zig");

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

    var client = try quic_zig.Client.connect(.{
        .allocator = allocator,
        .server_name = options.server_name,
        .alpn_protocols = options.alpn_protocols,
        .transport_params = options.transport_params,
        .ca_pem = options.ca_pem,
        .insecure_skip_verify = options.insecure_skip_verify,
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
