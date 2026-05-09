const std = @import("std");
const builtin = @import("builtin");

const baseline_engine = @import("baseline_engine.zig");
const close_controller = @import("close_controller.zig");
const endpoint_factory = @import("endpoint_factory.zig");
const endpoint_mod = @import("endpoint.zig");
const native_engine = @import("native_engine.zig");
const quic_options = @import("options.zig");
const wake_mod = @import("wake.zig");

const BaselineEngine = baseline_engine.BaselineEngine;
const ClientOptions = quic_options.ClientOptions;
const CloseController = close_controller.Controller;
const Endpoint = endpoint_mod.Endpoint;
const EndpointRuntime = endpoint_mod.Runtime;
const NativeEngine = native_engine.NativeEngine;
const NativeOptions = quic_options.NativeOptions;
const Role = endpoint_mod.Role;
const ServerOptions = quic_options.ServerOptions;
const TransportMode = quic_options.TransportMode;

/// Shared allocation/configuration for client connections and the server
/// compatibility connection.
///
/// Server fanout should keep listener/session ownership outside `Connection`;
/// this initializer intentionally builds only the one-session transport object.
pub const Config = struct {
    udp_rx_buffer_size: usize,
    udp_tx_buffer_size: usize,
    stream_read_buffer_size: usize,
    max_message_bytes: usize,
    max_outbound_queue_items: usize,
    max_outbound_queue_bytes: usize,
    mode: TransportMode,
    native_options: NativeOptions,
    reveal_close_reason_on_wire: bool = false,

    pub fn fromClient(options: quic_options.ClientOptions) Config {
        return .{
            .udp_rx_buffer_size = options.udp_rx_buffer_size,
            .udp_tx_buffer_size = options.udp_tx_buffer_size,
            .stream_read_buffer_size = options.stream_read_buffer_size,
            .max_message_bytes = options.max_message_bytes,
            .max_outbound_queue_items = options.max_outbound_queue_items,
            .max_outbound_queue_bytes = options.max_outbound_queue_bytes,
            .mode = options.mode,
            .native_options = options.native,
        };
    }

    pub fn fromServer(options: quic_options.ServerOptions) Config {
        return .{
            .udp_rx_buffer_size = options.udp_rx_buffer_size,
            .udp_tx_buffer_size = options.udp_tx_buffer_size,
            .stream_read_buffer_size = options.stream_read_buffer_size,
            .max_message_bytes = options.max_message_bytes,
            .max_outbound_queue_items = options.max_outbound_queue_items,
            .max_outbound_queue_bytes = options.max_outbound_queue_bytes,
            .mode = options.mode,
            .native_options = options.native,
            .reveal_close_reason_on_wire = options.reveal_close_reason_on_wire,
        };
    }
};

pub fn initClient(
    comptime Connection: type,
    allocator: std.mem.Allocator,
    io: std.Io,
    options: ClientOptions,
) !Connection {
    try quic_options.validateClientOptions(options);

    var created = try endpoint_factory.initClient(allocator, io, options);
    errdefer created.deinit(io);

    return try init(
        Connection,
        allocator,
        io,
        created.role,
        created.endpoint,
        Config.fromClient(options),
    );
}

pub fn initServer(
    comptime Connection: type,
    allocator: std.mem.Allocator,
    io: std.Io,
    options: ServerOptions,
) !Connection {
    // Compatibility path: bind a listener, then expose the first accepted
    // server session through the peer-facing Connection callbacks.
    var created = try endpoint_factory.initServer(allocator, io, options);
    errdefer created.deinit(io);

    return try init(
        Connection,
        allocator,
        io,
        created.role,
        created.endpoint,
        Config.fromServer(options),
    );
}

pub fn init(
    comptime Connection: type,
    allocator: std.mem.Allocator,
    io: std.Io,
    role: Role,
    endpoint: Endpoint,
    config: Config,
) !Connection {
    const udp_rx_buf = try allocator.alloc(u8, config.udp_rx_buffer_size);
    errdefer allocator.free(udp_rx_buf);
    const udp_tx_buf = try allocator.alloc(u8, config.udp_tx_buffer_size);
    errdefer allocator.free(udp_tx_buf);
    const stream_read_buf = try allocator.alloc(u8, config.stream_read_buffer_size);
    errdefer allocator.free(stream_read_buf);

    return .{
        .allocator = allocator,
        .role = role,
        .endpoint = EndpointRuntime.init(endpoint, io),
        .udp_rx_buf = udp_rx_buf,
        .udp_tx_buf = udp_tx_buf,
        .stream_read_buf = stream_read_buf,
        .max_message_bytes = config.max_message_bytes,
        .mode = config.mode,
        .baseline = BaselineEngine.init(
            allocator,
            config.max_message_bytes,
            config.max_outbound_queue_items,
            config.max_outbound_queue_bytes,
        ),
        .native = NativeEngine.init(
            allocator,
            role,
            config.max_message_bytes,
            config.max_outbound_queue_items,
            config.max_outbound_queue_bytes,
            config.native_options,
        ),
        .wake_state = wake_mod.Handle.init(),
        .close_controller = CloseController.init(config.reveal_close_reason_on_wire),
        .owner_thread_id = ownerThreadId(),
    };
}

fn ownerThreadId() ?std.Thread.Id {
    if (comptime builtin.target.os.tag == .freestanding) return null;
    return std.Thread.getCurrentId();
}
