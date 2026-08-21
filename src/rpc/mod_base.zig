const builtin = @import("builtin");
const quic_disabled = @import("./transport/quic_disabled.zig");

pub const wire = struct {
    pub const framing = @import("./wire/framing.zig");
    pub const protocol = @import("./wire/protocol.zig");
};

pub const caps = struct {
    pub const table = @import("./caps/table.zig");
    pub const cap_pointer = @import("./caps/cap_pointer.zig");
    pub const descriptors = @import("./caps/descriptors.zig");
    pub const lifecycle = @import("./caps/lifecycle.zig");
    pub const inbound = @import("./caps/inbound.zig");
    pub const outbound = @import("./caps/outbound.zig");
};

pub const promises = struct {
    pub const pipeline = @import("./promises/pipeline.zig");
    pub const promised_answer = @import("./promises/promised_answer.zig");
};

pub const events = @import("./events.zig");
pub const time = @import("./time.zig");
pub const peer = @import("./peer/mod.zig");

pub const vat = struct {
    pub const network = @import("./vat/network.zig");
    pub const quic_network = @import("./vat/quic_network.zig");
    pub const join = @import("./vat/join.zig");
    pub const provisions = @import("./vat/provisions.zig");
    pub const host = @import("./vat/host.zig");
};

pub fn Transport(comptime quic_impl: type, comptime include_tcp: bool) type {
    return struct {
        pub const binding = @import("./transport/binding.zig");
        pub const stream_state = @import("./transport/stream_state.zig");
        pub const quic = quic_impl;

        pub const tcp = if (include_tcp) struct {
            pub const stream = @import("./transport/tcp/stream_transport.zig");
            pub const connection = @import("./transport/tcp/connection.zig");
            pub const runtime = @import("./transport/tcp/runtime.zig");
            pub const client = @import("./transport/tcp/client.zig");
            pub const server = @import("./transport/tcp/server.zig");

            pub const Transport = stream.Transport;
            pub const Connection = connection.Connection;
            pub const ClientSession = client.ClientSession;
            pub const ConnectOptions = client.ConnectOptions;
            pub const connect = client.ClientSession.connect;
            pub const connectHost = client.ClientSession.connectHost;
            pub const ServerSession = server.ServerSession;
            pub const ServeOptions = server.ServeOptions;
            pub const Runtime = runtime.Runtime;
            pub const Listener = runtime.Listener;
            pub const SockAddrStorage = runtime.SockAddrStorage;
            pub const SocketFd = stream.SocketFd;
            pub const closeFd = runtime.closeFd;
            pub const createListenSocket = runtime.createListenSocket;
            pub const createLoopbackSocketPair = runtime.createLoopbackSocketPair;
            pub const ipAddressToSockaddr = runtime.ipAddressToSockaddr;
        } else struct {};
    };
}

pub const transport = Transport(quic_disabled, true);

pub fn Integration(comptime include_worker_pool: bool) type {
    return struct {
        pub const host_peer = @import("./integration/host_peer.zig");
        pub const HostPeer = host_peer.HostPeer;

        pub const worker_pool = if (include_worker_pool) @import("./integration/worker_pool.zig") else struct {};
        pub const WorkerPool = if (include_worker_pool) worker_pool.WorkerPool else void;
    };
}

pub const integration = Integration(true);

pub const generated = struct {
    pub const rpc = @import("./gen/capnp/rpc.zig");
    pub const persistent = @import("./gen/capnp/persistent.zig");
};
/// Deliberately unstable RPC test-support facade. Gated behind
/// `builtin.is_test` so the ~104 Internal helper decls it re-exports are
/// absent from the frozen consumer surface (`src/lib.zig`) and the generated
/// `docs/api-snapshot.txt`, and unreachable from application code. Test builds
/// (`is_test == true`) see the full facade; the RPC test suites reach it via
/// `capnpc-zig`.rpc.testing unchanged.
pub const testing = if (builtin.is_test) @import("./testing.zig") else struct {};

/// Force a namespace subtree through semantic analysis so its `test` blocks
/// join the compilation. Zig analyses container declarations lazily, and a
/// file that is never analysed contributes no tests — so a re-export alone
/// leaves everything behind it invisible to `zig build test`.
///
/// `skip_quic` exists because `transport.quic` is two different things: the
/// `quic_disabled` stub in a default build, where every declaration is a
/// deliberate `@compileError` telling you to pass `-Dquic=true` (referencing
/// it would turn that diagnostic into a build failure for everyone), and the
/// real module under `-Dquic=true`, where it must be walked or the QUIC
/// transport's own tests never run. Pass true from the default root, false
/// from the QUIC root.
///
/// The depth cap stops a self-referential declaration (`pub const Self =
/// @This()`) recursing forever; it only needs to cover
/// mod -> namespace -> file -> nested namespace.
pub fn refAllRecursive(comptime T: type, comptime depth: u8, comptime skip_quic: bool) void {
    if (depth == 0) return;
    inline for (comptime @import("std").meta.declarations(T)) |decl_name| {
        if (skip_quic and comptime @import("std").mem.eql(u8, decl_name, "quic")) continue;
        const field = @field(T, decl_name);
        if (@TypeOf(field) == type) {
            switch (@typeInfo(field)) {
                .@"struct", .@"union", .@"enum", .@"opaque" => refAllRecursive(field, depth - 1, skip_quic),
                else => {},
            }
        }
        _ = &field;
    }
}
