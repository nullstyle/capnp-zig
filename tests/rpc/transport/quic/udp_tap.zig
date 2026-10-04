//! A UDP relay between a QUIC client and its server that can add datagrams
//! of its own: an on-path observer that can also send.
//!
//! The client dials the tap's address instead of the server's. The tap
//! forwards every datagram unchanged, so the RPC connection through it is a
//! real one, and on request it sends one extra datagram to either end:
//!
//! * `forged_short_header`: the first byte and the connection ID of the last
//!   1-RTT packet it relayed in that direction, then 3 more bytes. With the
//!   8-byte connection IDs both ends use, that is 12 bytes. It is what
//!   anyone who saw one packet of the connection can send.
//! * `half_of_shortest`: a copy of the shortest 1-RTT datagram it relayed in
//!   that direction, cut to half its length.
//!
//! With `corrupt_first_server_datagram`, the first datagram from the server
//! (its handshake flight) reaches the client twice: first a copy with the
//! byte at `corrupt_offset` changed, then the real one.
//!
//! Every datagram the tap sends to the client comes from the address the
//! client dialed, and every one it sends to the server comes from the address
//! the server knows the client by. So the transport's own receive path takes
//! them: for the client that is the check that a datagram comes from the
//! server address, which a raw socket of the test could not pass. Nothing in
//! `src/` needs a seam for this.
//!
//! The tap is driven by its own thread (`run`). The test thread only sets
//! the atomics below and reads the counters; the socket and the recorded
//! datagrams are the tap thread's alone.

const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const quic = capnpc.rpc.transport.quic;
const Net = std.Io.net;

pub const Direction = enum(u1) { to_server = 0, to_client = 1 };

pub const Injection = enum(u8) { none = 0, forged_short_header, half_of_shortest };

/// QUIC v1 allows connection IDs of at most 20 bytes (RFC 9000 §17.2).
const max_cid_len = 20;
/// The forged datagram's bytes after the connection ID.
const forged_tail = [_]u8{ 0xa5, 0x5a, 0xc3 };
/// Room for every datagram a loopback QUIC endpoint sends.
const rx_buffer_size = 64 * 1024;
/// Longest datagram kept as a `half_of_shortest` candidate.
const max_kept_datagram = 1500;
/// How long the tap waits for a datagram before it looks for work again.
const poll_ms = 2;

const Kept = struct {
    bytes: [max_kept_datagram]u8 = undefined,
    len: usize = 0,
};

pub const UdpTap = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    socket: Net.Socket,
    server_addr: Net.IpAddress,
    rx_buf: []u8,
    udp_receive: quic.testing.UdpReceiveBridge = .{},

    /// Set before `run` starts; read only by the tap thread.
    corrupt_first_server_datagram: bool = false,
    corrupt_offset: usize = 5,
    corrupt_xor: u8 = 0xff,

    // Test thread -> tap thread.
    stop: std.atomic.Value(bool) = .init(false),
    pending: [2]std.atomic.Value(u8) = .{ .init(0), .init(0) },

    // Tap thread -> test thread.
    /// Extra datagrams sent, of any kind.
    injected: std.atomic.Value(usize) = .init(0),
    /// Requests the tap could not serve (nothing recorded yet to build from).
    unserved: std.atomic.Value(usize) = .init(0),
    /// Receive or send errors other than transient ICMP feedback.
    socket_errors: std.atomic.Value(usize) = .init(0),
    forwarded: [2]std.atomic.Value(usize) = .{ .init(0), .init(0) },
    /// Length of the last extra datagram, and of the real one it was built
    /// from (0 for a forgery): evidence for the test's report.
    last_injected_len: std.atomic.Value(usize) = .init(0),
    last_source_len: std.atomic.Value(usize) = .init(0),
    /// Length of the shortest 1-RTT datagram relayed in each direction so
    /// far (0 until there is one): what `half_of_shortest` would cut.
    shortest_len: [2]std.atomic.Value(usize) = .{ .init(0), .init(0) },

    // Tap thread only.
    client_addr: ?Net.IpAddress = null,
    /// Connection ID lengths, read from the server's first long header: its
    /// Destination Connection ID is the client's, its Source the server's.
    /// A 1-RTT packet to the client carries the client's, and one to the
    /// server the server's.
    cid_len: [2]?usize = .{ null, null },
    /// First byte + connection ID of the last 1-RTT packet in each direction.
    last_short_prefix: [2][1 + max_cid_len]u8 = undefined,
    last_short_prefix_len: [2]usize = .{ 0, 0 },
    shortest: [2]Kept = .{ .{}, .{} },
    /// The forged datagram is built here, never in `rx_buf`: a Windows
    /// receive can still own that buffer after its wait timed out.
    forge_buf: [1 + max_cid_len + forged_tail.len]u8 = undefined,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, server_addr: Net.IpAddress) !UdpTap {
        const bind_addr: Net.IpAddress = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
        const socket = try Net.IpAddress.bind(&bind_addr, io, .{ .mode = .dgram, .protocol = .udp });
        errdefer socket.close(io);
        const rx_buf = try allocator.alloc(u8, rx_buffer_size);
        return .{
            .allocator = allocator,
            .io = io,
            .socket = socket,
            .server_addr = server_addr,
            .rx_buf = rx_buf,
        };
    }

    /// Only after the `run` thread has been joined (or was never started).
    pub fn deinit(self: *UdpTap) void {
        // The Windows receive borrows the socket and the buffer; reap it first.
        self.udp_receive.cancel(self.io);
        self.socket.close(self.io);
        self.allocator.free(self.rx_buf);
    }

    /// The address the client dials.
    pub fn address(self: *const UdpTap) Net.IpAddress {
        return self.socket.address;
    }

    pub fn run(self: *UdpTap) void {
        while (!self.stop.load(.acquire)) {
            self.serveRequests();
            self.relayOne() catch |err| {
                if (!quic.testing.isTransientPeerFault(err)) {
                    _ = self.socket_errors.fetchAdd(1, .acq_rel);
                    std.Thread.yield() catch {};
                }
            };
        }
    }

    /// Ask the tap thread for one extra datagram toward `direction`.
    pub fn request(self: *UdpTap, direction: Direction, injection: Injection) void {
        self.pending[@backingInt(direction)].store(@backingInt(injection), .release);
    }

    fn serveRequests(self: *UdpTap) void {
        for ([_]Direction{ .to_server, .to_client }) |direction| {
            const raw = self.pending[@backingInt(direction)].swap(0, .acq_rel);
            if (raw == 0) continue;
            const injection: Injection = @fromBackingInt(@intCast(raw));
            const built = self.build(direction, injection) orelse {
                _ = self.unserved.fetchAdd(1, .acq_rel);
                continue;
            };
            self.sendTo(direction, built.bytes) catch |err| {
                if (!quic.testing.isTransientPeerFault(err)) _ = self.socket_errors.fetchAdd(1, .acq_rel);
                continue;
            };
            self.last_injected_len.store(built.bytes.len, .release);
            self.last_source_len.store(built.source_len, .release);
            _ = self.injected.fetchAdd(1, .acq_rel);
        }
    }

    const Built = struct { bytes: []const u8, source_len: usize };

    fn build(self: *UdpTap, direction: Direction, injection: Injection) ?Built {
        const d = @backingInt(direction);
        switch (injection) {
            .none => return null,
            .forged_short_header => {
                const prefix_len = self.last_short_prefix_len[d];
                if (prefix_len == 0) return null;
                const out = self.forge_buf[0 .. prefix_len + forged_tail.len];
                @memcpy(out[0..prefix_len], self.last_short_prefix[d][0..prefix_len]);
                @memcpy(out[prefix_len..], &forged_tail);
                return .{ .bytes = out, .source_len = 0 };
            },
            .half_of_shortest => {
                const kept = &self.shortest[d];
                if (kept.len < 2) return null;
                return .{ .bytes = kept.bytes[0 .. kept.len / 2], .source_len = kept.len };
            },
        }
    }

    fn relayOne(self: *UdpTap) !void {
        const wait = std.Io.Duration.fromMilliseconds(poll_ms);
        if (comptime builtin.target.os.tag == .windows) {
            const received = try self.udp_receive.receive(self.io, self.socket, self.rx_buf, wait);
            switch (received) {
                .timeout, .wake, .truncated => {},
                .datagram => |msg| try self.relay(msg.data, msg.from),
            }
        } else {
            const msg = quic.testing.nonWindowsReceive(&self.socket, self.io, self.rx_buf, wait) catch |err| switch (err) {
                error.Timeout => return,
                else => return err,
            };
            if (msg.flags.trunc) return;
            try self.relay(msg.data, msg.from);
        }
    }

    fn relay(self: *UdpTap, data: []const u8, from: Net.IpAddress) !void {
        const direction: Direction = if (Net.IpAddress.eql(&from, &self.server_addr)) .to_client else blk: {
            // The first datagram not from the server is the client's dial.
            if (self.client_addr == null) self.client_addr = from;
            if (!Net.IpAddress.eql(&from, &self.client_addr.?)) return;
            break :blk .to_server;
        };
        if (direction == .to_client and self.client_addr == null) return;
        self.observe(direction, data);

        if (direction == .to_client and self.corrupt_first_server_datagram) {
            self.corrupt_first_server_datagram = false;
            if (data.len > self.corrupt_offset) {
                var copy: [max_kept_datagram]u8 = undefined;
                if (data.len <= copy.len) {
                    @memcpy(copy[0..data.len], data);
                    copy[self.corrupt_offset] ^= self.corrupt_xor;
                    try self.sendTo(.to_client, copy[0..data.len]);
                    self.last_injected_len.store(data.len, .release);
                    self.last_source_len.store(data.len, .release);
                    _ = self.injected.fetchAdd(1, .acq_rel);
                }
            }
        }

        try self.sendTo(direction, data);
        _ = self.forwarded[@backingInt(direction)].fetchAdd(1, .acq_rel);
    }

    /// Learn the connection ID lengths from the server's first long header,
    /// then keep what the injections are built from.
    fn observe(self: *UdpTap, direction: Direction, data: []const u8) void {
        const d = @backingInt(direction);
        if (data.len == 0) return;
        if ((data[0] & 0x80) != 0) {
            if (direction == .to_client and self.cid_len[d] == null) {
                // Long header: flags, version (4), DCID length, DCID, SCID
                // length, SCID (RFC 9000 §17.2).
                if (data.len < 6) return;
                const dcid_len: usize = data[5];
                if (dcid_len > max_cid_len or data.len < 7 + dcid_len) return;
                const scid_len: usize = data[6 + dcid_len];
                if (scid_len > max_cid_len) return;
                self.cid_len[@backingInt(Direction.to_client)] = dcid_len;
                self.cid_len[@backingInt(Direction.to_server)] = scid_len;
            }
            return;
        }
        const cid_len = self.cid_len[d] orelse return;
        const prefix_len = 1 + cid_len;
        if (data.len < prefix_len) return;
        @memcpy(self.last_short_prefix[d][0..prefix_len], data[0..prefix_len]);
        self.last_short_prefix_len[d] = prefix_len;
        const kept = &self.shortest[d];
        if (data.len <= kept.bytes.len and (kept.len == 0 or data.len < kept.len)) {
            @memcpy(kept.bytes[0..data.len], data);
            kept.len = data.len;
            self.shortest_len[d].store(data.len, .release);
        }
    }

    fn sendTo(self: *UdpTap, direction: Direction, bytes: []const u8) !void {
        const dest = switch (direction) {
            .to_server => self.server_addr,
            .to_client => self.client_addr orelse return,
        };
        try self.socket.send(self.io, &dest, bytes);
    }
};
