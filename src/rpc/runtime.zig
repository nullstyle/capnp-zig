const std = @import("std");
const xev = @import("xev");
const Connection = @import("connection.zig").Connection;

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    loop: xev.Loop,

    pub fn init(allocator: std.mem.Allocator) !Runtime {
        const loop = try xev.Loop.init(.{});
        return .{
            .allocator = allocator,
            .loop = loop,
        };
    }

    pub fn deinit(self: *Runtime) void {
        self.loop.deinit();
    }

    pub fn run(self: *Runtime, mode: xev.RunMode) !void {
        try self.loop.run(mode);
    }
};

pub const Listener = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    socket: xev.TCP,
    accept_completion: xev.Completion = .{},
    close_completion: xev.Completion = .{},
    on_accept: *const fn (listener: *Listener, conn: *Connection) void,
    conn_options: Connection.Options,

    pub fn init(
        allocator: std.mem.Allocator,
        loop: *xev.Loop,
        addr: std.net.Address,
        on_accept: *const fn (listener: *Listener, conn: *Connection) void,
        conn_options: Connection.Options,
    ) !Listener {
        var socket = try xev.TCP.init(addr);
        try socket.bind(addr);
        try socket.listen(128);

        return .{
            .allocator = allocator,
            .loop = loop,
            .socket = socket,
            .on_accept = on_accept,
            .conn_options = conn_options,
        };
    }

    pub fn start(self: *Listener) void {
        self.queueAccept();
    }

    pub fn close(self: *Listener) void {
        self.socket.close(self.loop, &self.close_completion, Listener, self, Listener.onClosed);
    }

    fn queueAccept(self: *Listener) void {
        self.socket.accept(self.loop, &self.accept_completion, Listener, self, Listener.onAccept);
    }

    fn onAccept(
        self: ?*Listener,
        _: *xev.Loop,
        _: *xev.Completion,
        res: xev.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        const listener = self.?;
        const socket = res catch {
            return .disarm;
        };

        const conn_ptr = listener.allocator.create(Connection) catch {
            return .disarm;
        };

        conn_ptr.* = Connection.init(
            listener.allocator,
            listener.loop,
            socket,
            listener.conn_options,
        ) catch {
            listener.allocator.destroy(conn_ptr);
            return .disarm;
        };

        listener.on_accept(listener, conn_ptr);
        listener.queueAccept();
        return .disarm;
    }

    fn onClosed(
        _: ?*Listener,
        _: *xev.Loop,
        _: *xev.Completion,
        _: xev.TCP,
        _: xev.CloseError!void,
    ) xev.CallbackAction {
        return .disarm;
    }
};
