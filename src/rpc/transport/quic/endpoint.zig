const nullq = @import("nullq");

const session = @import("session.zig");

pub const Role = enum { client, server };

pub const Endpoint = union(Role) {
    client: nullq.Client,
    server: ServerEndpoint,
};

/// Compatibility endpoint used by `Connection.initServer`.
///
/// It still owns a `nullq.Server`, but the accepted slot is represented as a
/// session tracker so the listener-owned-server and peer-attached-session
/// responsibilities are no longer collapsed into one anonymous field.
pub const ServerEndpoint = struct {
    server: nullq.Server,
    session: session.SessionTracker = .{},
};
