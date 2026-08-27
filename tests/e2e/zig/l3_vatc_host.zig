//! Zig two-peer VatC host for the cross-impl L3 harness.
//!
//! Binds one TCP port, accepts EXACTLY TWO connections from the C++ driver
//! (which plays recipient VatA and introducer VatB), enrolls both accepted
//! peers into one `rpc.peer.Vat`, and pumps both sockets in a single
//! poll-driven loop — the role-reversal of tools/e2e_l3_cpp.zig's
//! TcpPeer/Pump machinery (accept instead of connect; manual pump, no
//! ServerSession/WorkerPool).
//!
//! Each connection's bootstrap is a returner service whose call returns
//! Carol (the schema's `Number` capability, answering 42), so Carol is a
//! non-bootstrap export holding a real Return-carried wire ref once the
//! introducer fetches her. Connection order is NOT assumed: either accepted
//! connection may end up introducer- or recipient-facing, so both get the
//! identical setup.
//!
//! Assertion transport: TAP on stdout (`READY <port>` first, `ok N - ...`
//! lines, `1..N` plan at exit). The whole run sits under a DebugAllocator;
//! leak-freedom is itself a TAP assertion.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const io_backend_options = @import("io_backend_options");

const rpc = capnpc.rpc;
const HostPeer = rpc.integration.HostPeer;
const Peer = rpc.peer.Peer;
const Vat = rpc.peer.Vat;
const InboundCapTable = rpc.caps.table.InboundCapTable;
const protocol = rpc.wire.protocol;
const Framer = rpc.wire.framing.Framer;
const l3 = @import("l3_l4_interop");

const Allocator = std.mem.Allocator;

/// Overall wall-clock budget after `READY` — the runner's per-case timeout
/// is 20s, so fail loudly (with TAP evidence) before it kills us.
const overall_deadline_ms: i64 = 15_000;
const poll_slice_ms: i32 = 50;
const default_port: u16 = 4707;
const default_seed: u64 = 0x6c33_7661_7463_5eed;

/// `park-expiry` only. Short enough that the driver's wait costs the lane
/// almost nothing, long enough that it cannot fire between the Accept arriving
/// and the driver's pipelined Call landing behind it.
const park_expiry_ttl_ms: u64 = 300;
const park_fairness_ttl_ms: u64 = 3_000;

const Scenario = enum {
    happy,
    embargo,
    unknown_token,
    disconnect,
    /// The FAILED-answer directions of the broken-pipeline rule, cross-impl.
    ///
    /// `unknown-token` proves an unmatched completion token PARKS. This
    /// scenario takes the same parked Accept and lets the L9 TTL evict it,
    /// which answers that question with an exception ("parked accept expired").
    /// Two calls must then be answered with a COPY of that exception, and they
    /// travel different code paths:
    ///
    ///   * one the driver pipelined on the question BEFORE it failed — queued
    ///     against the pending answer, so it is settled by the drain;
    ///   * one that arrives AFTER the exception Return was built — no pending
    ///     answer left to queue against, so it is settled from the recorded
    ///     `failed_answers` entry.
    ///
    /// The second is the one that had only unit coverage, and a conformant C++
    /// client does not normally emit it: once it has PROCESSED the exception
    /// Return its promise is broken and it fails such calls locally, with no
    /// frame. The driver gets one deterministically by queueing it in the same
    /// turn as the Accept that triggers the eviction and never turning its
    /// event loop in between — frame order on one connection does the rest.
    /// Before the fix in v0.7.0 that call queued forever and the driver hung.
    ///
    /// The eviction is driven by that SECOND Accept, not by a background
    /// timer. Expiry is checked at every inbound-frame boundary, so busy
    /// traffic cannot suppress reclamation; this cell happens to use a later
    /// Accept while park-fairness uses an ordinary call.
    park_expiry,
    /// The ORDER-INDEPENDENT rendezvous, other half: Accept BEFORE Provide.
    ///
    /// `unknown-token` proves an Accept naming no provision PARKS. This proves
    /// the park is then ADOPTED and served when the Provide it names arrives —
    /// rpc.h:483-492, "the two calls can happen in any order".
    ///
    /// The driver rewrites A's completion token to `+1`, which is precisely the
    /// token B's NEXT introduction registers, so the Accept necessarily lands
    /// before its Provide. That is arithmetic, not timing, so no delay hook is
    /// needed — which is what made this cell cheap once the token-rewrite modes
    /// were made explicit.
    park_adopt,
    /// One recipient peer fills its one-entry test quota with an unmatched
    /// Accept. A second park on that peer is refused, while the sibling peer
    /// completes a legitimate reverse-direction handoff. Ordinary traffic
    /// after the short TTL sweeps the attacker's first park.
    park_fairness,
    // The C++ driver provides a still-PIPELINED (promisedAnswer-target) cap
    // that ultimately re-resolves to a cap this host only IMPORTS (a cap the
    // introducer itself hosts). With the receiverHosted lift the host SERVES
    // these Accepts: the import is kept alive by a handoff pin whose
    // deferred-Release accounting withholds outbound Releases across the
    // handoff window, and the accepted cap proxies back to the introducer's
    // own capability. These scenarios previously asserted the fail-closed
    // refusal (`CrossPeerReceiverHostedTargetUnsupported`); this is the
    // rewrite that came due when the lift landed.
    //   pipelined-provide       one-hop echo: the promisedAnswer target
    //                           resolves at Provide time -> stored
    //                           .local{receiverHosted} -> served through the
    //                           Provide-time import pin (lift site 1).
    //   pipelined-provide-chain chained echo-of-pipelined: Provide-time
    //                           resolution yields a chained receiverAnswer ->
    //                           stored .promised ops -> serve-time owner-side
    //                           re-resolution -> .imported -> served through
    //                           the serve-time import pin (lift site 2).
    pipelined_provide,
    pipelined_provide_chain,

    fn parse(text: []const u8) ?Scenario {
        if (std.mem.eql(u8, text, "happy")) return .happy;
        if (std.mem.eql(u8, text, "embargo")) return .embargo;
        if (std.mem.eql(u8, text, "unknown-token")) return .unknown_token;
        if (std.mem.eql(u8, text, "disconnect")) return .disconnect;
        if (std.mem.eql(u8, text, "park-expiry")) return .park_expiry;
        if (std.mem.eql(u8, text, "park-adopt")) return .park_adopt;
        if (std.mem.eql(u8, text, "park-fairness")) return .park_fairness;
        if (std.mem.eql(u8, text, "pipelined-provide")) return .pipelined_provide;
        if (std.mem.eql(u8, text, "pipelined-provide-chain")) return .pipelined_provide_chain;
        return null;
    }

    fn name(self: Scenario) []const u8 {
        return switch (self) {
            .happy => "happy",
            .embargo => "embargo",
            .unknown_token => "unknown-token",
            .disconnect => "disconnect",
            .park_expiry => "park-expiry",
            .park_adopt => "park-adopt",
            .park_fairness => "park-fairness",
            .pipelined_provide => "pipelined-provide",
            .pipelined_provide_chain => "pipelined-provide-chain",
        };
    }

    fn isPipelinedProvide(self: Scenario) bool {
        return self == .pipelined_provide or self == .pipelined_provide_chain;
    }
};

const CliArgs = struct {
    host: []const u8 = "0.0.0.0",
    port: u16 = default_port,
    seed: u64 = default_seed,
    scenario: Scenario = .happy,
};

fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
    return @ptrCast(@alignCast(ctx));
}

fn milliTimestamp(io: std.Io) i64 {
    return std.Io.Timestamp.now(io, .awake).toMilliseconds();
}

/// TAP lines go to REAL stdout (std.debug.print is stderr); no allocation so
/// the final lines can print after the DebugAllocator's leak verdict.
fn writeStdout(io: std.Io, text: []const u8) void {
    std.Io.File.stdout().writeStreamingAll(io, text) catch {};
}

const Tap = struct {
    io: std.Io,
    test_num: usize = 0,
    failures: usize = 0,

    fn print(self: *Tap, comptime fmt: []const u8, args: anytype) void {
        var buf: [512]u8 = undefined;
        const text = std.fmt.bufPrint(&buf, fmt, args) catch return;
        writeStdout(self.io, text);
    }

    fn ok(self: *Tap, pass: bool, desc: []const u8) void {
        self.test_num += 1;
        if (pass) {
            self.print("ok {d} - {s}\n", .{ self.test_num, desc });
        } else {
            self.print("not ok {d} - {s}\n", .{ self.test_num, desc });
            self.failures += 1;
        }
    }

    fn diag(self: *Tap, comptime fmt: []const u8, args: anytype) void {
        self.print("# " ++ fmt ++ "\n", args);
    }

    fn plan(self: *Tap) void {
        self.print("1..{d}\n", .{self.test_num});
    }
};

fn usage() void {
    std.debug.print(
        \\Usage: e2e-l3-vatc-host [--host 0.0.0.0] [--port 4707] [--seed N] [--scenario happy|embargo|unknown-token|disconnect|park-expiry|park-adopt|park-fairness|pipelined-provide|pipelined-provide-chain]
        \\
    , .{});
}

fn parseArgs(allocator: Allocator, args: std.process.Args) !CliArgs {
    var out = CliArgs{};
    var host_text: []const u8 = out.host;

    var args_iter = try std.process.Args.Iterator.initAllocator(args, allocator);
    defer args_iter.deinit();
    _ = args_iter.skip();
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            return error.HelpRequested;
        }
        if (std.mem.eql(u8, arg, "--trace")) {
            trace_enabled = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--host")) {
            host_text = args_iter.next() orelse return error.MissingArgValue;
            continue;
        }
        if (std.mem.eql(u8, arg, "--port")) {
            const port_str = args_iter.next() orelse return error.MissingArgValue;
            out.port = try std.fmt.parseInt(u16, port_str, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--seed")) {
            const seed_str = args_iter.next() orelse return error.MissingArgValue;
            out.seed = try std.fmt.parseInt(u64, seed_str, 0);
            continue;
        }
        if (std.mem.eql(u8, arg, "--scenario")) {
            const scenario_str = args_iter.next() orelse return error.MissingArgValue;
            out.scenario = Scenario.parse(scenario_str) orelse return error.InvalidOption;
            continue;
        }
        return error.InvalidOption;
    }

    out.host = try allocator.dupe(u8, host_text);
    return out;
}

/// Deterministic 32-byte CSPRNG seed from the CLI's u64 (reproducibility;
/// the host never mints tokens, this only feeds the vat's embargo-id RNG).
fn seedBytes(seed: u64) [32]u8 {
    var out: [32]u8 = undefined;
    var word: u64 = seed;
    inline for (0..4) |i| {
        std.mem.writeInt(u64, out[i * 8 ..][0..8], word, .little);
        word +%= 0x9e37_79b9_7f4a_7c15;
    }
    return out;
}

// -- Carol: the provided Number capability (counts calls, answers 42) --------

const CarolService = struct {
    calls: u32 = 0,

    fn getNumber(
        ctx: *anyopaque,
        _: *Peer,
        _: l3.Number.GetNumberParams.Reader,
        results: *l3.Number.GetNumberResults.Builder,
        _: *const InboundCapTable,
    ) anyerror!void {
        const self: *CarolService = castCtx(*CarolService, ctx);
        self.calls += 1;
        try results.setN(42);
    }
};

/// Bootstrap service returning a pre-existing export id in its results
/// (grantCapVia pattern from rpc_three_party_handoff_vatc_test.zig). Accepts
/// ANY (interface, method) so the driver's raw-call convention — the existing
/// lane uses Number.interface_id / method 0 — always lands.
const CapReturner = struct {
    export_id: u32,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const InboundCapTable,
    ) anyerror!void {
        const self: *CapReturner = castCtx(*CapReturner, ctx_ptr);
        const ReturnCtx = struct {
            id: u32,
            fn build(bctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const bself: *const @This() = castCtx(*const @This(), bctx);
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                try any.setCapability(.{ .id = bself.id });
            }
        };
        var ret_ctx = ReturnCtx{ .id = self.export_id };
        try peer.sendReturnResults(call.question_id, &ret_ctx, ReturnCtx.build);
    }
};

/// Bootstrap for the pipelined-provide scenarios: ECHOES the caller's single
/// param capability back in the Return, preserving its inbound origin: an import echoes as
/// receiverHosted, an own export as senderHosted, and a promised-answer param
/// (a cap the caller pipelined on one of its own outstanding questions) as a
/// freshly-noted receiverAnswer. This is the natural "return the cap you gave
/// me" server shape; it is what makes the provided pipeline re-resolve to a
/// RECEIVER-hosted cap on the owner leg.
const EchoReturner = struct {
    fn onCall(
        _: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        inbound: *const InboundCapTable,
    ) anyerror!void {
        const descriptors = rpc.caps.table.descriptors;
        const cap = try call.params.content.getCapability();
        const entry = try inbound.get(cap.id);
        // KEEP the echoed capability past this Return: these scenarios go on to
        // serve a `Provide` whose target is this very import, so the reference
        // has to outlive the answer. Retaining is the application half of the
        // contract; the runtime supplies the wire half — `Peer` sees that the
        // params granted import refs and stamps `releaseParamCaps = false` on
        // the Return, so the caller keeps its export alive until this host's own
        // Release. (No `setReleaseParamCaps` override here: an answer that says
        // `true` while the callee also releases would spend the caller's export
        // twice, which is exactly what the C++ reference rejects with "Tried to
        // release invalid export ID".)
        const mutable_inbound: *InboundCapTable = @constCast(inbound);
        try mutable_inbound.retainCapability(cap);
        const mapped: struct { origin: u4, id: u32 } = switch (entry) {
            .none => return error.EchoParamMissing,
            .imported => |c| .{ .origin = descriptors.originCodeForTag(.receiverHosted), .id = c.id },
            .exported => |c| .{ .origin = descriptors.originCodeForTag(.senderHosted), .id = c.id },
            .promised => |promised| .{
                .origin = descriptors.originCodeForTag(.receiverAnswer),
                .id = try peer.caps.noteReceiverAnswer(promised),
            },
        };
        const ReturnCtx = struct {
            origin: u4,
            id: u32,
            fn build(bctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const bself: *const @This() = castCtx(*const @This(), bctx);
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                try any.setCapabilityOriginTagged(bself.origin, bself.id);
            }
        };
        var ret_ctx = ReturnCtx{ .origin = mapped.origin, .id = mapped.id };
        try peer.sendReturnResults(call.question_id, &ret_ctx, ReturnCtx.build);
    }
};

// -- Wire tracing (diagnostics only; enabled with --trace) -------------------

var trace_enabled: bool = false;

fn traceFrame(allocator: Allocator, dir: []const u8, idx: usize, frame: []const u8) void {
    if (!trace_enabled) return;
    var decoded = protocol.DecodedMessage.init(allocator, frame) catch |err| {
        std.debug.print("[conn{d}] {s} <undecodable {d}B: {s}>\n", .{ idx, dir, frame.len, @errorName(err) });
        return;
    };
    defer decoded.deinit();
    switch (decoded.tag) {
        .call => {
            const c = decoded.asCall() catch return;
            std.debug.print("[conn{d}] {s} call qid={d} iface=0x{x} m={d} target={s}/{?d} srt={s}\n", .{
                idx,                    dir,                   c.question_id,                   c.interface_id, c.method_id,
                @tagName(c.target.tag), c.target.imported_cap, @tagName(c.send_results_to.tag),
            });
        },
        .@"return" => {
            const r = decoded.asReturn() catch return;
            std.debug.print("[conn{d}] {s} return aid={d} which={s}\n", .{ idx, dir, r.answer_id, @tagName(r.tag) });
        },
        .finish => {
            const f = decoded.asFinish() catch return;
            std.debug.print("[conn{d}] {s} finish qid={d}\n", .{ idx, dir, f.question_id });
        },
        .bootstrap => {
            const b = decoded.asBootstrap() catch return;
            std.debug.print("[conn{d}] {s} bootstrap qid={d}\n", .{ idx, dir, b.question_id });
        },
        .provide => {
            const p = decoded.asProvide() catch return;
            std.debug.print("[conn{d}] {s} provide qid={d} target={s}/{?d}\n", .{
                idx, dir, p.question_id, @tagName(p.target.tag), p.target.imported_cap,
            });
        },
        .accept => {
            const a = decoded.asAccept() catch return;
            std.debug.print("[conn{d}] {s} accept qid={d} embargo_bytes={?d}\n", .{
                idx, dir, a.question_id, if (a.embargo) |e| e.len else null,
            });
        },
        .disembargo => {
            const d = decoded.asDisembargo() catch return;
            std.debug.print("[conn{d}] {s} disembargo target={s}/{?d} pa_qid={?d} ctx={s} eid={?d} accept_bytes={?d}\n", .{
                idx,
                dir,
                @tagName(d.target.tag),
                d.target.imported_cap,
                if (d.target.promised_answer) |pa| pa.question_id else null,
                @tagName(d.context_tag),
                d.embargo_id,
                if (d.accept) |acc| acc.len else null,
            });
        },
        .resolve => {
            const r = decoded.asResolve() catch return;
            std.debug.print("[conn{d}] {s} resolve pid={d} which={s}\n", .{ idx, dir, r.promise_id, @tagName(r.tag) });
        },
        .release => {
            const r = decoded.asRelease() catch return;
            std.debug.print("[conn{d}] {s} release id={d} n={d}\n", .{ idx, dir, r.id, r.reference_count });
        },
        else => std.debug.print("[conn{d}] {s} {s}\n", .{ idx, dir, @tagName(decoded.tag) }),
    }
}

/// Wire-level record of what Provide target form actually ARRIVED, decoded
/// straight off the frame — independent of any internal re-classification.
/// Shared across connections.
const WireProbes = struct {
    provide_promised_answer: bool = false,
    provide_imported_cap: bool = false,
    /// Outbound `Return`s carrying the parked-accept TTL exception.
    ///
    /// `park-expiry` needs TWO of them on one connection: the Return that
    /// answers the Accept question the TTL evicted, and the Return that answers
    /// the Call the driver had already PIPELINED on that question. One alone
    /// means the answer failed but the pipelined call was silently dropped —
    /// precisely the regression this cell exists to catch, and the shape that
    /// hangs a real client rather than failing it.
    expired_accept_returns: usize = 0,

    /// Outbound counterpart to `note`. Frames are counted as they leave the
    /// host, so this witnesses what the driver will actually receive rather
    /// than what the host's internal state happens to say.
    fn noteOutbound(self: *WireProbes, allocator: Allocator, frame: []const u8) void {
        var decoded = protocol.DecodedMessage.init(allocator, frame) catch return;
        defer decoded.deinit();
        if (decoded.tag != .@"return") return;
        const ret = decoded.asReturn() catch return;
        const ex = ret.exception orelse return;
        if (std.mem.indexOf(u8, ex.reason, "parked accept expired") != null) {
            self.expired_accept_returns += 1;
        }
    }

    fn note(self: *WireProbes, allocator: Allocator, frame: []const u8) void {
        var decoded = protocol.DecodedMessage.init(allocator, frame) catch return;
        defer decoded.deinit();
        if (decoded.tag != .provide) return;
        const p = decoded.asProvide() catch return;
        switch (p.target.tag) {
            .promisedAnswer => self.provide_promised_answer = true,
            .importedCap => self.provide_imported_cap = true,
        }
    }
};

// -- One accepted connection: socket + HostPeer + Framer ---------------------

const HostConn = struct {
    allocator: Allocator,
    index: usize = 0,
    /// Inbound frames dispatched on this connection so far — the role signal
    /// `drainOrder` uses to name the recipient leg without assuming an index.
    frames_in: usize = 0,
    socket: rpc.transport.tcp.SocketFd,
    host: HostPeer,
    framer: Framer,
    /// Pumping stopped: EOF, reset, or write failure. The fd itself is
    /// closed exactly once, in deinit.
    closed: bool = false,
    fd_open: bool = true,
    probe_ctx: ?*anyopaque = null,
    probe_fn: ?*const fn (ctx: *anyopaque) void = null,
    /// Wire-shape recorder (Provide target form as it arrived).
    wire_probes: ?*WireProbes = null,

    fn initAccepted(allocator: Allocator, index: usize, socket: rpc.transport.tcp.SocketFd) HostConn {
        return .{
            .allocator = allocator,
            .index = index,
            .socket = socket,
            .host = HostPeer.init(allocator),
            .framer = Framer.init(allocator),
        };
    }

    fn deinit(self: *HostConn, io: std.Io) void {
        if (self.fd_open) {
            rpc.transport.tcp.closeFd(io, self.socket);
            self.fd_open = false;
        }
        self.framer.deinit();
        self.host.deinit();
    }

    /// Terminal transport notification for this manual socket harness. The
    /// production bound-transport path does this internally; raw EOF/reset and
    /// write failure have to report it explicitly. HostPeer and this guard are
    /// both idempotent, so later deinit cannot detach holder state twice.
    fn markTransportClosed(self: *HostConn) void {
        if (self.closed) return;
        self.closed = true;
        self.host.notifyTransportClosed();
    }

    fn flush(self: *HostConn, io: std.Io) !bool {
        if (self.closed) return false;
        var progressed = false;
        while (self.host.popOutgoingFrame()) |frame| {
            defer self.host.freeFrame(frame);
            traceFrame(self.allocator, "-->", self.index, frame);
            if (self.wire_probes) |wp| wp.noteOutbound(self.allocator, frame);
            writeAll(io, self.socket, frame) catch {
                // Abrupt driver disconnect (EPIPE/reset) is a tolerated end
                // state in every scenario; the drain asserts decide pass/fail.
                self.markTransportClosed();
                return true;
            };
            progressed = true;
        }
        return progressed;
    }

    /// One raw read per poll readiness; frames are pushed through the peer
    /// and the probe hook runs AFTER EVERY FRAME so transient states that
    /// drain within one read batch (e.g. queued embargoed accepts) are still
    /// observed.
    fn readReady(self: *HostConn) !bool {
        var buf: [16 * 1024]u8 = undefined;
        while (true) {
            const rc = std.posix.system.read(self.socket.handle, &buf, buf.len);
            switch (std.posix.errno(rc)) {
                .SUCCESS => {
                    if (rc == 0) {
                        self.markTransportClosed();
                        return true;
                    }
                    const n: usize = @intCast(rc);
                    try self.framer.push(buf[0..n]);
                    while (try self.framer.popFrame()) |frame| {
                        defer self.allocator.free(frame);
                        traceFrame(self.allocator, "<--", self.index, frame);
                        if (self.wire_probes) |wp| wp.note(self.allocator, frame);
                        self.frames_in += 1;
                        try self.host.pushFrame(frame);
                        if (self.probe_fn) |hook| hook(self.probe_ctx.?);
                    }
                    return true;
                },
                .INTR => continue,
                .AGAIN => return false,
                // Connection reset et al: the abrupt-disconnect end state.
                else => {
                    self.markTransportClosed();
                    return true;
                },
            }
        }
    }
};

/// Which accepted socket to drain first when poll reports both ready.
///
/// The two driver legs are INDEPENDENT sockets written in the same kj event
/// loop turn, so the host — not the wire — decides which of two equally legal
/// interleavings it observes. That choice selects which internal branch the
/// provision store takes for an embargoed Accept:
///
///   - introducer socket first: the `context.accept` Disembargo is dispatched
///     before the Accept, so the embargo is already released when the Accept
///     lands and it is served straight through;
///   - recipient socket first: the Accept lands with its embargo unreleased and
///     must be QUEUED in the provision store until the Disembargo arrives.
///
/// Both are correct, but only the second exercises the queue-and-release path,
/// so the `embargo` scenario pins it. The preference is derived from ROLE, not
/// connection index (the contract forbids assuming which accepted socket is
/// which): the introducer must bootstrap the host and fetch Carol before it can
/// send a Provide, so it has always dispatched strictly more frames than the
/// recipient, which stays silent until its Accept. Fewest-frames-first
/// therefore names the recipient leg deterministically, whichever socket it is.
fn drainOrder(conns: []HostConn, quiet_first: bool) [2]usize {
    if (!quiet_first or conns.len < 2) return .{ 0, 1 };
    return if (conns[1].frames_in < conns[0].frames_in) .{ 1, 0 } else .{ 0, 1 };
}

fn writeAll(io: std.Io, socket: rpc.transport.tcp.SocketFd, bytes: []const u8) !void {
    const pattern: []const u8 = &.{};
    const data: [1][]const u8 = .{pattern};
    var offset: usize = 0;
    while (offset < bytes.len) {
        const n = blk: {
            // See rpc.transport.tcp.stream's ioWrite: zig moved socket
            // writes from the vtable onto Operation.net_write around
            // 0.17.0-dev.1786.
            if (comptime @hasField(std.Io.Operation, "net_write")) {
                const result = io.operate(.{ .net_write = .{
                    .socket_handle = socket.handle,
                    .header = bytes[offset..],
                    .data = &data,
                    .splat = 0,
                } }) catch return error.WriteFailed;
                break :blk result.net_write catch return error.WriteFailed;
            }
            break :blk io.vtable.netWrite(io.userdata, socket.handle, bytes[offset..], &data, 0) catch
                return error.WriteFailed;
        };
        if (n == 0) return error.BrokenPipe;
        offset += n;
    }
}

// -- Probes: sampled after every inbound frame -------------------------------

const Probes = struct {
    provide_seen: bool = false,
    proxy_seen: bool = false,
    queued_seen: bool = false,
    parked_seen: bool = false,
    max_peer_parked_peak: usize = 0,
    /// Which STORED form the provide target took after the host's
    /// Provide-time resolution — `.local{receiverHosted}` routes the serve to
    /// the direct fail-closed gate (site 1), `.promised` routes it to the
    /// serve-time owner-side re-resolution (site 2).
    stored_local_receiver_hosted: bool = false,
    stored_promised: bool = false,
};

const Sampler = struct {
    vat: *Vat,
    conns: *[2]HostConn,
    inited: *usize,
    probes: *Probes,

    fn hook(ctx: *anyopaque) void {
        castCtx(*Sampler, ctx).sampleNow();
    }

    fn sampleNow(self: *Sampler) void {
        if (self.vat.index.by_key.count() > 0) self.probes.provide_seen = true;
        if (self.vat.index.queued_accept_count > 0) self.probes.queued_seen = true;
        if (self.vat.index.parked_accept_count > 0) self.probes.parked_seen = true;
        for (self.conns[0..self.inited.*]) |*conn| {
            const peer = &conn.host.peer;
            self.probes.max_peer_parked_peak = @max(self.probes.max_peer_parked_peak, peer.parked_accept_count);
            if (peer.provisions_by_question.count() > 0) self.probes.provide_seen = true;
            if (peer.cross_peer_proxy_links.items.len > 0) self.probes.proxy_seen = true;
            // Record the STORED provide-target form (site discriminator).
            var pit = peer.provides_by_question.valueIterator();
            while (pit.next()) |entry| {
                switch (entry.target) {
                    .local => |local| {
                        const rh = rpc.caps.table.descriptors.originCodeForTag(.receiverHosted);
                        if (local.origin_code == rh) self.probes.stored_local_receiver_hosted = true;
                    },
                    .promised => self.probes.stored_promised = true,
                }
            }
        }
    }
};

/// True when every piece of transient handoff state is gone from both peers
/// and the vat index (mirrors tools/e2e_l3_cpp.zig's expectTransientDrain,
/// widened to the VatC hosting surfaces).
///
/// On failure the individual non-zero counters are named on TAP diagnostics
/// (`# residue ...`) so the assertion reports WHICH bookkeeping survived
/// rather than a bare boolean.
///
/// NOT included here: `cross_peer_proxy_links`. That list is not transient
/// handoff bookkeeping — it is the SOURCE-side back-link for a proxy export
/// that a completed handoff deliberately left LIVE on the sibling peer. It
/// drains when the recipient releases its import (proxy-export destruction ->
/// `CrossPeerProxyContext.deinit` -> `deregisterCrossPeerProxy`) or at peer
/// teardown (`neutralizeCrossPeerProxiesOnSourcePeer`), never at the mere
/// completion of the handoff. `assertCrossPeerProxyLifecycle` covers it.
fn drainedTransient(vat: *Vat, conns: []HostConn, tap: *Tap, label: []const u8) bool {
    var clean = true;

    for (conns, 0..) |*conn, i| {
        const peer = &conn.host.peer;
        const counters = [_]struct { name: []const u8, n: usize }{
            .{ .name = "provides_by_question", .n = peer.provides_by_question.count() },
            .{ .name = "provisions_by_question", .n = peer.provisions_by_question.count() },
            .{ .name = "cross_peer_pending_accepts", .n = peer.cross_peer_pending_accepts.count() },
            .{ .name = "outbound_provides", .n = peer.outbound_provides.count() },
            .{ .name = "pending_third_party_awaits", .n = peer.pending_third_party_awaits.count() },
            .{ .name = "pending_accepts_by_embargo", .n = peer.pending_accepts_by_embargo.count() },
            .{ .name = "pending_accept_embargo_by_question", .n = peer.pending_accept_embargo_by_question.count() },
        };
        for (counters) |c| {
            if (c.n == 0) continue;
            clean = false;
            tap.diag("residue [{s}] peer{d}.{s} = {d}", .{ label, i, c.name, c.n });
        }
    }

    const index_counters = [_]struct { name: []const u8, n: usize }{
        .{ .name = "by_key", .n = vat.index.by_key.count() },
        .{ .name = "provision_count", .n = vat.index.provision_count },
        .{ .name = "queued_accept_count", .n = vat.index.queued_accept_count },
        .{ .name = "parked_accept_count", .n = vat.index.parked_accept_count },
    };
    for (index_counters) |c| {
        if (c.n == 0) continue;
        clean = false;
        tap.diag("residue [{s}] index.{s} = {d}", .{ label, c.name, c.n });
    }

    return clean;
}

fn liveProxyLinks(conns: []HostConn) usize {
    var total: usize = 0;
    for (conns) |*conn| total += conn.host.peer.cross_peer_proxy_links.items.len;
    return total;
}

/// `park-adopt`: the park must have been CONSUMED by adoption, not left
/// outstanding.
///
/// This is the assertion that separates this scenario from a park which merely
/// remained outstanding until transport-close cleanup. Everything else about
/// the end state is the ordinary drained shape.
fn parkWasAdopted(vat: *Vat, conns: []HostConn, tap: *Tap) bool {
    var ok = true;
    if (vat.index.parked_accept_count != 0) {
        ok = false;
        tap.diag("park-adopt: index.parked_accept_count = {d}, want 0 (the park must have been ADOPTED)", .{
            vat.index.parked_accept_count,
        });
    }
    for (conns, 0..) |*conn, i| {
        const pending = conn.host.peer.cross_peer_pending_accepts.count();
        if (pending != 0) {
            ok = false;
            tap.diag("park-adopt: peer{d}.cross_peer_pending_accepts = {d}, want 0", .{ i, pending });
        }
    }
    return ok;
}

/// Holder-side parks and embargo queues are transport-scoped. The manual
/// harness samples their live state before EOF, then calls
/// `HostPeer.notifyTransportClosed()` and uses this checkpoint to prove the
/// close path detached them. Owner provisions are intentionally excluded:
/// they remain valid across provider transport close until Finish or peer
/// teardown.
fn closedHolderStateClean(vat: *Vat, conns: []HostConn, tap: *Tap, label: []const u8) bool {
    var clean = true;
    for (conns, 0..) |*conn, i| {
        const peer = &conn.host.peer;
        const counters = [_]struct { name: []const u8, n: usize }{
            .{ .name = "cross_peer_pending_accepts", .n = peer.cross_peer_pending_accepts.count() },
            .{ .name = "pending_accepts_by_embargo", .n = peer.pending_accepts_by_embargo.count() },
            .{ .name = "pending_accept_embargo_by_question", .n = peer.pending_accept_embargo_by_question.count() },
        };
        for (counters) |c| {
            if (c.n == 0) continue;
            clean = false;
            tap.diag("residue [{s}] peer{d}.{s} = {d} after transport close", .{ label, i, c.name, c.n });
        }
    }
    const counters = [_]struct { name: []const u8, n: usize }{
        .{ .name = "parked_accept_count", .n = vat.index.parked_accept_count },
        .{ .name = "parked_accept_bytes", .n = vat.index.parked_accept_bytes },
        .{ .name = "queued_accept_count", .n = vat.index.queued_accept_count },
        .{ .name = "queued_accept_bytes", .n = vat.index.queued_accept_bytes },
    };
    for (counters) |c| {
        if (c.n == 0) continue;
        clean = false;
        tap.diag("residue [{s}] index.{s} = {d} after transport close", .{ label, c.name, c.n });
    }
    return clean;
}

/// Prove the cross-peer proxy's LIFECYCLE end, which the transient-drain check
/// deliberately does not cover.
///
/// Two terminal paths exist and this asserts whichever the scenario drove:
///   - RELEASE: the recipient dropped its import, so a wire `Release` destroyed
///     the proxy export and swept the source-side back-link already. Nothing is
///     left at the checkpoint. (`happy` drives this: the driver drops the cap
///     and turns the kj loop so the `Release` actually reaches us.)
///   - TEARDOWN: the driver exited with no RPC ceremony at all (`disconnect`
///     `_exit(0)`s by design; C++ also queues no `Release` when its event loop
///     simply stops). The link is then legitimately still live at the
///     checkpoint — the peers have NOT been deinited yet, and a TCP EOF alone
///     is not an RPC-level release. Tear the proxy-OWNING peer down here and
///     require the SOURCE peer's back-link to be swept, which is the real
///     claim: an abrupt disconnect does not strand handoff linkage.
///
/// Returns after possibly deiniting one connection; `deinited` is updated so
/// the caller's teardown does not double-free.
fn assertCrossPeerProxyLifecycle(
    io: std.Io,
    conns: []HostConn,
    deinited: []bool,
    tap: *Tap,
    label: []const u8,
) void {
    const outstanding = liveProxyLinks(conns);
    if (outstanding == 0) {
        tap.ok(true, "cross-peer proxy exports were destroyed by the recipient's Release");
        return;
    }

    // Locate one live link and the connection index of the peer that OWNS the
    // proxy export (the link's `owner_peer`); the link itself lives on the
    // source peer.
    var source_idx: usize = 0;
    var owner_idx: ?usize = null;
    outer: for (conns, 0..) |*conn, i| {
        for (conn.host.peer.cross_peer_proxy_links.items) |link| {
            for (conns, 0..) |*other, j| {
                if (&other.host.peer == link.owner_peer) {
                    source_idx = i;
                    owner_idx = j;
                    break :outer;
                }
            }
        }
    }

    const owner = owner_idx orelse {
        tap.diag("residue [{s}] proxy link owner_peer is not one of our connections", .{label});
        tap.ok(false, "an outstanding cross-peer proxy link names a known peer");
        return;
    };

    tap.diag(
        "[{s}] {d} cross-peer proxy link(s) live at checkpoint (no RPC-level Release arrived); sweeping via peer{d} teardown",
        .{ label, outstanding, owner },
    );

    conns[owner].deinit(io);
    deinited[owner] = true;

    const remaining = conns[source_idx].host.peer.cross_peer_proxy_links.items.len;
    if (remaining != 0) {
        tap.diag("residue [{s}] peer{d}.cross_peer_proxy_links = {d} AFTER owner teardown", .{ label, source_idx, remaining });
    }
    tap.ok(remaining == 0, "peer teardown swept the source-side cross-peer proxy back-links");
}

fn drainQueuedOutgoing(io: std.Io, conns: []HostConn) !void {
    var spins: usize = 0;
    while (true) {
        var round = false;
        for (conns) |*conn| {
            const progressed = try conn.flush(io);
            round = progressed or round;
        }
        if (!round) break;
        spins += 1;
        if (spins > 1024) return error.PumpSpinLimit;
    }
}

fn boundPort(listener: *const rpc.transport.tcp.Listener, fallback: u16) u16 {
    const resolved = switch (listener.getAddress()) {
        .ip4 => |a| a.port,
        .ip6 => |a| a.port,
    };
    return if (resolved != 0) resolved else fallback;
}

fn runHost(allocator: Allocator, io: std.Io, args: CliArgs, tap: *Tap) !void {
    tap.diag("scenario: {s}", .{args.scenario.name()});

    // One vat; deterministic embargo-id entropy from the CLI seed.
    //
    // Vat's secure default gets monotonic time from the by-value Io option.
    // Timed scenarios override only the duration: park-expiry retains its
    // deliberately short regression bound, while park-fairness also shrinks
    // the per-peer quota so one C++ connection can exhaust its own share
    // without consuming the sibling's.
    var vat_options: Vat.Options = .{
        .seed = seedBytes(args.seed),
        .io = io,
    };
    switch (args.scenario) {
        .park_expiry => vat_options.limits.park_ttl_ms = park_expiry_ttl_ms,
        .park_fairness => {
            vat_options.limits.park_ttl_ms = park_fairness_ttl_ms;
            vat_options.limits.max_parked_accepts_per_peer = 1;
            vat_options.limits.max_parked_accept_bytes_per_peer = 64;
        },
        else => {},
    }
    var vat = try Vat.init(allocator, vat_options);
    defer vat.deinit();

    const bind_addr = try std.Io.net.IpAddress.parse(args.host, args.port);
    var listener = try rpc.transport.tcp.Listener.init(allocator, io, bind_addr, .{});
    defer listener.close();

    const port = boundPort(&listener, args.port);
    tap.print("READY {d}\n", .{port});

    const deadline = milliTimestamp(io) + overall_deadline_ms;

    // See `drainOrder`: only the `embargo` scenario cares which of two legal
    // socket interleavings it observes, and only it pins the recipient-first
    // order so the queue-and-release branch is the one under test.
    const quiet_socket_first = args.scenario == .embargo;

    // Carol is one service; each connection exports her separately (per-peer
    // export id spaces) through one shared call counter.
    var carol = CarolService{};
    var carol_server = l3.Number.Server{
        .ctx = &carol,
        .vtable = .{ .getNumber = CarolService.getNumber },
    };
    var returners: [2]CapReturner = undefined;

    var conns: [2]HostConn = undefined;
    var inited: usize = 0;
    // `assertCrossPeerProxyLifecycle` may tear one connection down early to
    // prove the teardown sweep; the scope-exit loop must not double-free it.
    var deinited: [2]bool = .{ false, false };
    defer {
        var i: usize = 0;
        while (i < inited) : (i += 1) if (!deinited[i]) conns[i].deinit(io);
    }

    var probes = Probes{};
    var wire_probes = WireProbes{};
    var sampler = Sampler{
        .vat = &vat,
        .conns = &conns,
        .inited = &inited,
        .probes = &probes,
    };

    // Unified accept+pump loop. The first connection is pumped WHILE waiting
    // for the second accept: a driver that dials its recipient leg lazily
    // (e.g. from connectToIntroduced) must not deadlock against the host.
    while (true) {
        if (milliTimestamp(io) > deadline) {
            tap.ok(false, "host finished before the overall deadline");
            return error.HostDeadlineExceeded;
        }
        if (inited == 2 and conns[0].closed and conns[1].closed) break;

        try drainQueuedOutgoing(io, conns[0..inited]);

        var fds: [3]std.posix.pollfd = undefined;
        var nfds: usize = 0;
        const listen_slot: ?usize = if (inited < 2) blk: {
            fds[nfds] = .{
                .fd = listener.listenHandle().handle,
                .events = std.posix.POLL.IN,
                .revents = 0,
            };
            nfds += 1;
            break :blk 0;
        } else null;
        var conn_slots: [2]?usize = .{ null, null };
        for (conns[0..inited], 0..) |*conn, i| {
            if (conn.closed) continue;
            fds[nfds] = .{
                .fd = conn.socket.handle,
                .events = std.posix.POLL.IN,
                .revents = 0,
            };
            conn_slots[i] = nfds;
            nfds += 1;
        }
        if (nfds == 0) {
            // Nothing left to wait on and the exit condition did not hold:
            // fewer than two connections ever arrived and all are closed.
            tap.ok(false, "driver kept its connections alive until handoff finished");
            return error.DriverDisconnectedEarly;
        }

        while (true) {
            const rc = std.posix.system.poll(&fds, @intCast(nfds), poll_slice_ms);
            switch (std.posix.errno(rc)) {
                .SUCCESS => break,
                .INTR => continue,
                else => return error.PollFailed,
            }
        }

        if (listen_slot) |slot| {
            if (fds[slot].revents != 0) {
                const socket = try listener.acceptFd();
                conns[inited] = HostConn.initAccepted(allocator, inited, socket);
                const conn = &conns[inited];
                inited += 1;
                // Enroll BEFORE any frame of this connection is pumped:
                // attachProvisionIndex refuses pre-existing handoff state.
                try vat.enroll(&conn.host.peer);
                const carol_export_id = try l3.Number.exportServer(&conn.host.peer, &carol_server);
                returners[inited - 1] = .{ .export_id = carol_export_id };
                _ = try conn.host.peer.setBootstrap(.{
                    .ctx = &returners[inited - 1],
                    // The pipelined-provide scenarios swap the fixed-cap
                    // returner for the param-echo returner (ctx unused).
                    .on_call = if (args.scenario.isPipelinedProvide()) EchoReturner.onCall else CapReturner.onCall,
                });
                // `park-expiry` asserts on the exception TEXT, and a HostPeer
                // rewrites the reason of every outbound exception Return to a
                // generic string by default. That default is the right one for
                // a real host, but it would make "the pipelined call got a
                // COPY of the answer's exception" indistinguishable from "it
                // got some fresh generic error" — which is the entire
                // distinction this cell exists to draw.
                if (args.scenario == .park_expiry) {
                    conn.host.setErrorDisclosurePolicy(.{ .reveal_details = true });
                }
                conn.probe_ctx = &sampler;
                conn.probe_fn = Sampler.hook;
                conn.wire_probes = &wire_probes;
                conn.host.start(null, null, null);
                tap.diag("accepted connection {d}", .{inited});
            }
        }

        var progressed = false;
        for (drainOrder(conns[0..inited], quiet_socket_first)) |i| {
            if (i >= inited) continue;
            const conn = &conns[i];
            const slot = conn_slots[i] orelse continue;
            if (fds[slot].revents == 0) continue;
            const p = try conn.readReady();
            progressed = p or progressed;
        }
        if (progressed) try drainQueuedOutgoing(io, conns[0..inited]);
    }

    tap.ok(inited == 2, "accepted exactly two driver connections");

    // Both connections are closed: scripted checkpoint for the host's own
    // per-scenario assertions.
    switch (args.scenario) {
        .happy => {
            tap.ok(probes.provide_seen, "a Provide registered a provision in the vat index");
            tap.ok(probes.proxy_seen, "the Accept was served cross-peer (proxy export minted on the sibling)");
            tap.ok(carol.calls >= 1, "Carol answered at least one getNumber call");
            tap.ok(drainedTransient(&vat, conns[0..inited], tap, "happy"), "all transient handoff state drained");
            // The driver performs the full RPC-level release ceremony here, so
            // `happy` must take the RELEASE path — zero links at the checkpoint,
            // asserted STRICTLY rather than through
            // `assertCrossPeerProxyLifecycle`, whose teardown fallback would let
            // a silently-missing `Release` still pass. Real cross-impl proof
            // that a C++ recipient's `Release` tears the hosted proxy down.
            const links = liveProxyLinks(conns[0..inited]);
            if (links != 0) tap.diag("residue [happy] cross_peer_proxy_links = {d} after the driver's Release ceremony", .{links});
            tap.ok(links == 0, "the recipient's Release destroyed the cross-peer proxy export");
        },
        .embargo => {
            tap.ok(probes.provide_seen, "a Provide registered a provision in the vat index");
            tap.ok(probes.queued_seen, "an embargoed Accept was queued in the provision store");
            tap.ok(probes.proxy_seen, "the Accept was served cross-peer (proxy export minted on the sibling)");
            tap.ok(carol.calls >= 2, "Carol answered the pipelined and the direct call");
            tap.ok(drainedTransient(&vat, conns[0..inited], tap, "embargo"), "all transient handoff state drained");
        },
        .unknown_token => {
            // An Accept whose ThirdPartyCompletion matches no Provide PARKS —
            // it is not an error. The rendezvous is order-independent by spec
            // (vendored rpc.h:483-492: "The two calls can happen in any order;
            // `completeThirdParty()` will wait for a corresponding
            // `awaitThirdParty()` if it hasn't happened already"), and the
            // reference C++ does exactly this: TestVat::completeThirdParty
            // `findOrCreate`s an exchange whose fulfiller is never invoked for
            // an unmatched token (rpc-test.c++:534-537 + :639-643), so the
            // Accept's promise hangs forever. A vat-attached capnp-zig host
            // therefore CANNOT answer "unknown provision" for a well-formed
            // but unmatched token — that string only survives on the
            // no-index (two-party) path and for a missing/undecodable
            // provision field. See the "unmatched completion token" note in
            // the harness CONTRACT.
            tap.ok(probes.provide_seen, "the Provide registered a provision in the vat index");
            tap.ok(probes.parked_seen, "the Accept with an unmatched completion token parked (order-independent rendezvous)");
            tap.ok(!probes.proxy_seen, "no provision was served for the corrupted token (no proxy export minted)");
            tap.ok(carol.calls >= 1, "the parked Accept did not wedge the peer: an ordinary call still reached Carol");
            // The park was sampled while the transport was live. Both sockets
            // have since reached EOF, so the manual harness's exactly-once
            // close notification must detach the holder-side reservation even
            // though its unmatched token could never be adopted.
            tap.ok(
                closedHolderStateClean(&vat, conns[0..inited], tap, "unknown-token"),
                "transport close detached the unmatched parked Accept and refunded its gauges",
            );
        },
        .park_expiry => {
            // Same setup as `unknown-token` (an unmatched token, so the Accept
            // parks), but this host runs with a clock and `park_ttl_ms`, and
            // the driver sends a SECOND Accept once the TTL has elapsed. The
            // sweep on that second Accept evicts the first.
            tap.ok(probes.provide_seen, "a Provide registered a provision in the vat index");
            tap.ok(probes.parked_seen, "the Accept with an unmatched completion token parked");
            tap.ok(!probes.proxy_seen, "no provision was ever served (no proxy export minted)");

            // THE CELL. THREE Returns must carry "parked accept expired", one
            // per arm of the broken-pipeline rule:
            //   1. the evicted Accept question itself;
            //   2. the Call the driver pipelined on it BEFORE it failed, which
            //      was queued and must be drained with the answer's exception;
            //   3. the Call that arrives AFTER the exception Return was built,
            //      which must be answered from the recorded `failed_answers`
            //      entry rather than queued against an answer that is gone.
            //
            // The two call arms are genuinely different code paths, and this
            // count is what tells them apart: ablating the recorded-exception
            // lookup leaves arm 2 green and drops this to 2, while the driver's
            // late call hangs until its bound. A missing Return here is a
            // client that waits forever.
            tap.diag("expired-accept Returns observed: {d}", .{wire_probes.expired_accept_returns});
            tap.ok(
                wire_probes.expired_accept_returns >= 1,
                "the parked-accept TTL evicted the Accept with an exception Return",
            );
            tap.ok(
                wire_probes.expired_accept_returns >= 2,
                "the call pipelined on that answer BEFORE it failed was drained with the same exception",
            );
            tap.ok(
                wire_probes.expired_accept_returns >= 3,
                "the call arriving AFTER it failed got a copy of that exception too",
            );

            // Liveness: evicting a parked accept must not wedge the peer that
            // owned it, and must not disturb the introducer leg.
            tap.ok(carol.calls >= 1, "an ordinary call still reached Carol after the eviction");

            // The second Accept parks after driving the first eviction. EOF on
            // its transport must detach that remaining holder record.
            tap.ok(
                closedHolderStateClean(&vat, conns[0..inited], tap, "park-expiry"),
                "transport close detached the remaining park after expiry",
            );
        },
        .park_adopt => {
            // The other direction of the order-independent rendezvous. The
            // driver's Accept named the token its NEXT introduction registers,
            // so it necessarily arrived BEFORE that Provide and had to park;
            // when the Provide landed the park must have been ADOPTED and
            // served.
            tap.ok(probes.provide_seen, "a Provide registered a provision in the vat index");

            // Both halves are required, and the pair is the point: parking
            // without serving is `unknown-token`, and serving without parking
            // means the Accept was never early and the cell proved nothing
            // about ordering.
            tap.ok(probes.parked_seen, "the Accept arrived first and PARKED (no provision yet)");
            tap.ok(
                probes.proxy_seen,
                "the later Provide ADOPTED that parked Accept and served it cross-peer",
            );

            tap.ok(carol.calls >= 1, "the adopted Accept reached Carol");
            tap.ok(
                parkWasAdopted(&vat, conns[0..inited], tap),
                "the park was CONSUMED by adoption, not left outstanding",
            );
            // The adopted handoff then behaves like any served one: the
            // driver's Release retires the proxy export and everything drains.
            // An adopted capability must not have a second-class lifecycle.
            tap.ok(
                drainedTransient(&vat, conns[0..inited], tap, "park-adopt"),
                "all transient handoff state drained after the driver's Release",
            );
        },
        .park_fairness => {
            tap.ok(probes.provide_seen, "attacker and sibling registered provisions in the vat index");
            tap.ok(probes.parked_seen, "the attacker's first unmatched Accept parked");
            tap.ok(probes.max_peer_parked_peak == 1, "the one-entry per-peer quota prevented a second park on either peer");
            tap.ok(probes.proxy_seen, "the sibling peer completed a legitimate cross-peer handoff");
            tap.ok(carol.calls >= 1, "the sibling's accepted cap remained callable under attacker pressure");
            tap.ok(
                closedHolderStateClean(&vat, conns[0..inited], tap, "park-fairness"),
                "expiry and transport close drained every holder-side reservation",
            );

            // Active owner provisions deliberately survive transport close.
            // Peer teardown is their terminal cleanup path; run it here so the
            // scenario proves the entire vat returns to zero, not merely that
            // its per-peer quota recovered.
            for (conns[0..inited], 0..) |*conn, i| {
                conn.deinit(io);
                deinited[i] = true;
            }
            tap.ok(
                vat.index.by_key.count() == 0 and vat.index.provision_count == 0 and
                    vat.index.parked_accept_count == 0 and vat.index.queued_accept_count == 0,
                "peer teardown drained all remaining vat provision state",
            );
        },
        // The C++ driver introduced a still-unresolved PIPELINED cap that
        // re-resolves to a cap this host only imports. Assert the wire shape
        // (the Provide target arrived as promisedAnswer — C++ did not
        // shorten), the STORED form the Provide-time resolution took (which
        // pins WHICH lift site served the Accept), that the serve really
        // happened cross-peer, that the accepted cap reached the DRIVER's
        // capability (host Carol untouched), and that the driver's release
        // ceremony drained everything. The 43-vs-42 result value is asserted
        // driver-side.
        .pipelined_provide, .pipelined_provide_chain => {
            tap.ok(probes.provide_seen, "a Provide registered a provision in the vat index");
            tap.ok(wire_probes.provide_promised_answer, "the Provide target ARRIVED as promisedAnswer (C++ did not shorten)");
            tap.ok(!wire_probes.provide_imported_cap, "no importedCap-target Provide was observed");
            if (args.scenario == .pipelined_provide) {
                tap.ok(probes.stored_local_receiver_hosted, "Provide-time resolution stored .local{receiverHosted} (lift site 1)");
                tap.ok(!probes.stored_promised, "no .promised target was stored in the one-hop shape");
            } else {
                tap.ok(probes.stored_promised, "Provide-time resolution stored .promised ops (serve-time re-resolution, lift site 2)");
                tap.ok(!probes.stored_local_receiver_hosted, "no .local{receiverHosted} target was stored in the chained shape");
            }
            tap.ok(probes.proxy_seen, "the Accept was SERVED cross-peer (proxy export minted on the sibling)");
            tap.ok(carol.calls == 0, "host Carol was never invoked (the accepted cap reached the driver's own capability)");
            tap.ok(drainedTransient(&vat, conns[0..inited], tap, args.scenario.name()), "all transient handoff state drained");
            // The driver performs the full release ceremony, so the RELEASE
            // path must have destroyed the proxy — asserted strictly, like
            // `happy`.
            const links = liveProxyLinks(conns[0..inited]);
            if (links != 0) tap.diag("residue [{s}] cross_peer_proxy_links = {d} after the driver's Release ceremony", .{ args.scenario.name(), links });
            tap.ok(links == 0, "the recipient's Release destroyed the cross-peer proxy export");
        },
        .disconnect => {
            tap.ok(drainedTransient(&vat, conns[0..inited], tap, "disconnect"), "transient handoff state drained after abrupt disconnect");
            // The driver `_exit(0)`s with no ceremony, so the completed
            // handoff's proxy export is still legitimately live here — TCP EOF
            // is not an RPC release. What must hold is that peer teardown
            // sweeps the source-side back-link rather than stranding it.
            assertCrossPeerProxyLifecycle(io, conns[0..inited], &deinited, tap, "disconnect");
        },
    }
}

pub fn main(init: std.process.Init) !void {
    const backend_kind = capnpc.io_backend.parseKind(io_backend_options.kind) orelse {
        std.debug.print("invalid -Dio-backend selector: {s}\n", .{io_backend_options.kind});
        return error.InvalidIoBackend;
    };
    var backend = try capnpc.io_backend.Backend.init(backend_kind, init.gpa, init.io);
    defer backend.deinit();
    const io = backend.io();

    const args = parseArgs(init.gpa, init.minimal.args) catch |err| switch (err) {
        error.HelpRequested => {
            usage();
            return;
        },
        error.InvalidOption,
        error.InvalidCharacter,
        error.Overflow,
        error.MissingArgValue,
        => {
            usage();
            return err;
        },
        else => return err,
    };
    defer init.gpa.free(args.host);

    var tap = Tap{ .io = io };

    // Everything the harness allocates runs under the DebugAllocator; the
    // leak verdict below is itself part of the TAP plan (so it must be
    // collected AFTER runHost's state has fully deinited).
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    runHost(gpa.allocator(), io, args, &tap) catch |err| {
        tap.diag("host run failed: {s}", .{@errorName(err)});
        tap.ok(false, "host run completes without harness error");
    };
    tap.ok(gpa.deinit() == .ok, "no leaks under the DebugAllocator");

    tap.plan();
    if (tap.failures > 0) return error.TestFailed;
}
