//! Effect queue for one `capnp_conn` (plan §4 "Effects", §4.1 "Ownership").
//!
//! The core never calls into the host. Every Zig callback the Peer runs
//! (transport send/close, question callbacks, export handlers, deinit hooks,
//! the observer) only appends an `Effect` here; the host pulls them with
//! `next` and releases each with `commit`.
//!
//! Rules:
//! - One effect in flight. A second `next` before `commit` is `error.Busy`.
//! - Every effect lives in its own heap `Node`, and every payload slice the
//!   node owns is its own allocation, so a borrowed payload keeps a stable
//!   address until its `commit` even while more effects are queued.
//! - Terminal effects whose producer cannot fail (a question's `deinit_ctx`,
//!   an export's `deinit_ctx`) use a `Node` reserved when the question or
//!   export was created, so they never need memory at the moment they fire.

const std = @import("std");

/// One capability slot in a host payload's `caps[]` (D5). A capability
/// pointer in a standalone host message stores an index into this array.
pub const CapKind = enum(u8) {
    /// A null capability.
    none = 0,
    /// An id the remote exported to us (one wire reference per entry).
    import = 1,
    /// One of our own exports (from `exportCap` / `setBootstrap` /
    /// `promiseExport`).
    @"export" = 2,
    /// A promised answer (pipelining): `id` is one of this side's questions
    /// that has not returned yet, `ops` the pipeline path into its results.
    promised = 3,
};

/// `extern`: this is also the C ABI's `capnp_cap` (abi.zig passes host
/// `caps[]` arrays and effect cap tables through without copying).
pub const Cap = extern struct {
    kind: CapKind,
    id: u32 = 0,
    /// PROMISED only: pointer-field indices from the question's results
    /// struct to the capability (empty: the results root is the capability).
    /// Borrowed for the duration of the call that passes it.
    ops: ?[*]const u16 = null,
    nops: u16 = 0,

    pub fn opsSlice(self: Cap) []const u16 {
        const p = self.ops orelse return &.{};
        return p[0..self.nops];
    }
};

pub const ReturnKind = enum(u8) {
    results = 0,
    exception = 1,
    canceled = 2,
    /// The question ended because the local connection is gone (transport
    /// closed, shutdown drain, teardown) or through `deinit_ctx` only.
    disconnected = 3,
};

pub const Return = struct {
    qid: u32,
    /// CANCELED: the host cancelled the question (`cancel`); the exception
    /// the Peer synthesized rides along in `exception_type`/`reason`.
    kind: ReturnKind,
    /// RESULTS: standalone message whose root is the results struct; its cap
    /// pointers index `caps`.
    msg: []const u8 = &.{},
    /// Import caps here are owned by the host until it calls `release`.
    caps: []const Cap = &.{},
    /// EXCEPTION / DISCONNECTED: the `Exception.Type` ordinal.
    exception_type: u16 = 0,
    reason: []const u8 = "",
};

pub const InboundCall = struct {
    answer_id: u32,
    export_id: u32,
    host_tag: u64,
    interface_id: u64,
    method_id: u16,
    /// Standalone message whose root is the params struct.
    msg: []const u8,
    /// Import caps here are owned by the host until it calls `release`.
    caps: []const Cap,
};

pub const ExportDropped = struct {
    export_id: u32,
    host_tag: u64,
};

pub const Event = struct {
    /// `@intFromEnum` of the `events.Event` tag.
    tag: u8,
    /// `@errorName` of the event's error, if it has one. Static storage.
    err_name: []const u8 = "",
};

pub const Kind = enum(u8) {
    out_frame = 0,
    close_requested = 1,
    @"return" = 2,
    inbound_call = 3,
    export_dropped = 4,
    event = 5,
};

pub const Effect = union(Kind) {
    out_frame: []const u8,
    close_requested: void,
    @"return": Return,
    inbound_call: InboundCall,
    export_dropped: ExportDropped,
    event: Event,
};

/// One queued effect and the allocations it owns.
pub const Node = struct {
    next: ?*Node = null,
    effect: Effect = .close_requested,
    /// Owned payload allocations, freed by `destroy`. Static strings (error
    /// names, fixed reasons) are never recorded here.
    owned_bytes: ?[]const u8 = null,
    owned_caps: ?[]const Cap = null,
    owned_reason: ?[]const u8 = null,

    pub fn create(allocator: std.mem.Allocator) error{OutOfMemory}!*Node {
        const node = try allocator.create(Node);
        node.* = .{};
        return node;
    }

    /// Free the payload allocations but keep the node (for reuse as a
    /// fallback effect after a partial fill failed).
    pub fn freePayload(self: *Node, allocator: std.mem.Allocator) void {
        if (self.owned_bytes) |b| allocator.free(b);
        if (self.owned_caps) |c| allocator.free(c);
        if (self.owned_reason) |r| allocator.free(r);
        self.owned_bytes = null;
        self.owned_caps = null;
        self.owned_reason = null;
    }

    pub fn destroy(self: *Node, allocator: std.mem.Allocator) void {
        self.freePayload(allocator);
        allocator.destroy(self);
    }
};

/// FIFO of effect nodes plus the single in-flight slot.
pub const Queue = struct {
    head: ?*Node = null,
    tail: ?*Node = null,
    len: usize = 0,
    in_flight: ?*Node = null,

    pub fn push(self: *Queue, node: *Node) void {
        node.next = null;
        if (self.tail) |t| {
            t.next = node;
        } else {
            self.head = node;
        }
        self.tail = node;
        self.len += 1;
    }

    /// Borrow the oldest effect. Null when the queue is empty. The pointer
    /// stays valid until `commit`.
    pub fn next(self: *Queue) error{Busy}!?*const Effect {
        if (self.in_flight != null) return error.Busy;
        const node = self.head orelse return null;
        self.head = node.next;
        if (self.head == null) self.tail = null;
        self.len -= 1;
        node.next = null;
        self.in_flight = node;
        return &node.effect;
    }

    /// Release the in-flight effect. A commit with nothing in flight is a
    /// no-op (the C ABI fuzz lane calls it out of order on purpose).
    pub fn commit(self: *Queue, allocator: std.mem.Allocator) void {
        const node = self.in_flight orelse return;
        self.in_flight = null;
        node.destroy(allocator);
    }

    /// Nodes pushed after `mark` (the `tail` captured earlier), oldest first.
    /// A null mark means "from the head".
    pub fn iteratorSince(self: *const Queue, mark: ?*Node) Iterator {
        return .{ .cur = if (mark) |m| m.next else self.head };
    }

    pub const Iterator = struct {
        cur: ?*Node,
        pub fn next(it: *Iterator) ?*Node {
            const n = it.cur orelse return null;
            it.cur = n.next;
            return n;
        }
    };

    pub fn deinit(self: *Queue, allocator: std.mem.Allocator) void {
        self.commit(allocator);
        var cur = self.head;
        while (cur) |n| {
            cur = n.next;
            n.destroy(allocator);
        }
        self.* = .{};
    }
};

test "queue: one effect in flight, FIFO, commit frees" {
    const a = std.testing.allocator;
    var q: Queue = .{};
    defer q.deinit(a);

    const n1 = try Node.create(a);
    n1.effect = .{ .export_dropped = .{ .export_id = 1, .host_tag = 10 } };
    const n2 = try Node.create(a);
    const bytes = try a.dupe(u8, "frame");
    n2.effect = .{ .out_frame = bytes };
    n2.owned_bytes = bytes;
    q.push(n1);
    q.push(n2);

    const e1 = (try q.next()).?;
    try std.testing.expectEqual(@as(u32, 1), e1.export_dropped.export_id);
    try std.testing.expectError(error.Busy, q.next());
    q.commit(a);
    const e2 = (try q.next()).?;
    try std.testing.expectEqualStrings("frame", e2.out_frame);
    q.commit(a);
    q.commit(a); // no-op
    try std.testing.expect((try q.next()) == null);
}
