//! RPC soak harness: sustained concurrent bootstrap+call traffic over real
//! loopback TCP, with optional chaos (abrupt mid-flight disconnects) and
//! deadline-cancellation sessions.
//!
//! A WorkerPool server exports an echo bootstrap capability; W client worker
//! threads run connect → bootstrap → N calls → close sessions in a loop until
//! the configured duration elapses. Every chaos-eligible session tears the
//! connection down with a call still in flight; every deadline session runs
//! with a ~1ms call deadline so cancellation races real server Returns.
//!
//! Beyond counts, the harness reports:
//!   * p50 / p99 / max call latency across all successful round-trips, and
//!   * a periodic live-heap memory-growth curve (allocated − freed bytes,
//!     sampled on a background thread), with a PROGRAMMATIC flat-memory pass
//!     criterion — steady-state live heap must not trend upward beyond
//!     allocator noise, not merely be leak-free at the end.
//!
//! Concurrency scales via --workers (≥100 supported) and per-session
//! outstanding-question depth via --inflight (high-in-flight mode).
//!
//! Exit code is nonzero when invariants fail: zero successful calls, an
//! unexpected exception reason, mid-session transport errors past their
//! bound, unexplained setup failures past the same tolerance, a TCP server
//! under test that stopped serving before shutdown, a client-side
//! allocation leak, or a steady-state live-heap growth beyond the
//! configured threshold (and, with --rss-gate enforce, RSS growth).
//!
//! Failure accounting is split by phase:
//!   * SETUP failures happen before a session carries RPC traffic (TCP
//!     connect or Connection.init; QUIC client init). They are classified
//!     (refused, port_exhaustion, resources, timeout, ambiguous, other) and
//!     then resolved by `assessSetup`:
//!       - Reported, not gated: port exhaustion and resource limits, which
//!         are properties of the host or its load. Port exhaustion is
//!         printed with the run offset where it began, and every reported
//!         class also gets a ::warning:: annotation under GitHub Actions.
//!       - Gated past the tolerance below, all together: `other`, because
//!         an unexplained dial failure is a defect until shown otherwise;
//!         and refused and timeout, because the server under test is
//!         in-process and should accept every dial (if it stops, earlier
//!         traffic must not carry the run). Under QUIC
//!         --abrupt-death-every-ms the server is down by design between
//!         incarnations, so there refused and timeout are reported instead.
//!       - `ambiguous`: a Windows connect-stage error.Unexpected. std 0.17's
//!         netConnectIpWindows returns it for several distinct failures
//!         (unmapped bind statuses including AddressInUse, a failed
//!         SO_REUSE_UNICASTPORT, and every AFD_CONNECT status but refused
//!         and insufficient resources), and a ReleaseSafe build does not
//!         print the NTSTATUS. It counts as port exhaustion only with that
//!         shape (`ambiguousIsPortExhaustion`): it began after at least 100
//!         successful dials, and from then on at least half of all dial
//!         attempts failed this way. Otherwise it gates with `other`. A host
//!         that was already port-exhausted when the run began therefore
//!         fails the run too: it exercised almost nothing.
//!     The TCP server's WorkerPool.run must also last until shutdown is
//!     requested; returning earlier fails the run.
//!   * MID-SESSION transport errors (anything that fails after the
//!     connection is up) must satisfy
//!         transport_errors <= chaos_closes + death_allowance + tolerance
//!     chaos_closes: every chaos session ends with exactly one mid-session
//!     error by construction (after it rips its own connection, its next
//!     pump fails). death_allowance (QUIC --abrupt-death-every-ms only):
//!     deaths x churn workers, the most live sessions abrupt deaths can
//!     kill. tolerance: --transport-error-tolerance N, default
//!     max(8, sessions / 500) (0.2%). Before this bound existed, a Windows
//!     64-worker lane passed with 22130 transport errors against 1110 chaos
//!     closes: its last ~60% was dials failing on port exhaustion.
//!
//! Memory has two instruments sharing one steady-state trend check
//! (`assessMemory`): the live Zig heap (counting allocator; enforcing) and
//! the process resident set (RSS: Linux /proc/self/statm, macOS
//! task_info, Windows GetProcessMemoryInfo). RSS sees what the heap counter
//! cannot — BoringSSL's C heap under QUIC, page-level growth — so it is
//! the instrument that can see a C-side leak. It lands REPORT-ONLY
//! (`--rss-gate report`, the default: prints its verdict, never fails the
//! run); `--rss-gate enforce` makes it gate. The harness's own telemetry
//! is bounded (fixed-size latency histograms, preallocated sample series),
//! so neither instrument grows with the call count.
//!
//! Usage: zig build soak -- [--seconds N] [--workers N] [--calls N]
//!                          [--inflight K] [--mem-sample-ms N]
//!                          [--mem-growth-pct P] [--no-chaos] [--no-deadlines]
//!                          [--transport tcp|quic]  (quic needs -Dquic=true)
//!                          [--cc default|cubic|bbr|newreno]  (quic only)
//!                          [--abrupt-death-every-ms N]  (quic only: kill+restart
//!                           the server with no close ceremony every N ms)
//!                          [--heal-workers K]  (quic only: K workers run
//!                           persistent WarmRedialClients that auto-heal
//!                           across abrupt deaths instead of churn sessions)
//!                          [--nagle]  (tcp: leave Nagle on in the client, the
//!                           pre-TCP_NODELAY behaviour, for A/B latency runs)
//!                          [--transport-error-tolerance N]
//!                          [--rss-gate report|enforce] [--rss-growth-pct P]
//!                          [--rss-floor-mib N]
//!                          [--alloc-traces]  (DebugAllocator allocation stack
//!                           traces for leak reports; off by default because
//!                           the tracer itself grows RSS, see SoakGpa)
//!   Ablation hooks (prove the gates have teeth; never use in a real lane):
//!                          [--inject-transport-error-every N]  (count one
//!                           synthetic mid-session transport error on every
//!                           Nth session of each worker)
//!                          [--inject-rss-growth-kib-per-s N]  (touch N KiB/s
//!                           of page memory outside the Zig heap: RSS grows,
//!                           the heap counter does not)

const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const rpc = capnpc.rpc;
const protocol = rpc.wire.protocol;
const cap_table = rpc.caps.table;
const Peer = rpc.peer.Peer;
const Connection = rpc.transport.tcp.Connection;
const quic = rpc.transport.quic;
const WorkerPool = rpc.integration.worker_pool.WorkerPool;
const net = std.Io.net;

const Transport = enum { tcp, quic };

/// Congestion-control selection for the QUIC soak (--cc). A local enum so
/// the TCP-only build never references the quic module's types; mapped to
/// quic-zig's CongestionAlgorithm at the guarded call sites. `default`
/// leaves both sides on the transport's own defaults.
const CcChoice = enum { default, cubic, bbr, newreno };

const Config = struct {
    seconds: u64 = 2,
    workers: u32 = 4,
    // Transport under soak. `quic` requires a -Dquic=true build; the QUIC
    // variant exists so per-connection footprint and steady-state behavior
    // are measurable BEFORE/AFTER a quic-zig pin bump (a rig that cannot
    // observe a change reports "no change").
    transport: Transport = .tcp,
    // QUIC congestion-control override for A/B runs (ignored over tcp).
    cc: CcChoice = .default,
    calls_per_session: u32 = 25,
    // High-in-flight mode: max outstanding questions per session. 1 keeps the
    // classic sequential behavior; larger values keep many calls in flight.
    inflight: u32 = 1,
    chaos: bool = true,
    deadlines: bool = true,
    // Abrupt-death chaos (QUIC only): every N ms the harness KILLS the
    // server with no close ceremony and restarts it on the same port with
    // the same stateless_reset_key — the crash-restart shape the death
    // certificate exists for. Clients' next datagrams draw stateless
    // resets, so sessions close `.stateless_reset` instead of timing out.
    abrupt_death_every_ms: ?u64 = null,
    // Healing workers (QUIC only): the first K workers each run ONE
    // persistent WarmRedialClient for the whole run — restore-backed echo
    // traffic that auto-heals across abrupt server deaths — instead of the
    // churn session loop. Pair with --abrupt-death-every-ms for the
    // churn-scale self-healing proof.
    heal_workers: u32 = 0,
    // Memory-curve sampling interval and the steady-state growth ceiling.
    mem_sample_ms: u64 = 100,
    mem_growth_pct: f64 = 25.0,
    // TCP client sockets disable Nagle like every other client in the tree
    // (ClientSession, the e2e drivers). Without it, a Finish followed by the
    // next small Call write waits on the server's delayed ACK: the ~42 ms
    // p99 floor every Linux soak lane showed. `--nagle` restores the old
    // behaviour for A/B runs.
    nodelay: bool = true,
    // Absolute override for the mid-session transport-error tolerance;
    // null means the documented default, max(8, sessions / 500).
    transport_error_tolerance: ?usize = null,
    // RSS gate: report-only by default (verdict printed, never fails).
    rss_gate: RssGate = .report,
    rss_growth_pct: f64 = 25.0,
    // Absolute floor under which RSS growth is never a finding: allocator
    // caches, thread stacks and page-granular arenas settle in MiB steps,
    // not bytes, so the heap gate's 256 KiB floor would be noise here.
    rss_floor_mib: u64 = 16,
    // DebugAllocator allocation stack traces (see SoakGpa).
    alloc_traces: bool = false,
    // Ablation hooks; see the file header.
    inject_transport_error_every: ?u64 = null,
    inject_rss_growth_kib_per_s: ?u64 = null,
};

const RssGate = enum { report, enforce };

/// Counters are `usize`, not `u64`, so this harness cross-compiles for
/// 32-bit targets: `@atomicLoad`/`@atomicRmw` reject operands wider than the
/// pointer width, and `zig build check-compile -Dtarget=x86-linux-gnu` builds
/// this file. `usize` is u64 on every machine the soak actually runs on, so
/// the counters are unchanged there; the 32-bit build is compile-only rot
/// detection, never an execution target.
const Totals = struct {
    sessions: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    calls_ok: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    calls_cancelled: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    chaos_closes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    // MID-SESSION failures only (see the file header); setup failures are
    // counted, by class, in `setup_failures`.
    transport_errors: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    setup_failures: [setup_class_count]std.atomic.Value(usize) = @splat(std.atomic.Value(usize).init(0)),
    // Run offset (ms) of the first port-exhaustion setup failure; maxInt
    // means none. usize, not u64, for the same 32-bit reason as above.
    first_port_exhaustion_ms: std.atomic.Value(usize) = std.atomic.Value(usize).init(std.math.maxInt(usize)),
    // Sessions whose dial (connect + init) succeeded, and the AmbiguousShape
    // inputs: the onset of the first ambiguous failure (maxInt: none) and
    // how many dials had succeeded by then (written once, by the worker
    // that set the onset; read after join).
    dials_ok: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    ambiguous_onset_ms: std.atomic.Value(usize) = std.atomic.Value(usize).init(std.math.maxInt(usize)),
    dials_ok_at_ambiguous_onset: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    injected_transport_errors: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    // Written once by main before any worker spawns; read-only afterwards.
    start_ns: i64 = 0,
    expected_disconnects: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    // Disconnect-reason Returns on non-chaos sessions: legitimate at high peer
    // counts (idle-timeout drops under contention), not a correctness failure.
    contention_disconnects: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    unexpected_exceptions: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    // Terminal transport close cause per session (the death certificate):
    // indexed by causeSlot(rpc.events.DisconnectCause). TCP sessions all
    // land in .unknown today; QUIC splits local/peer/idle/reset/error.
    disconnects_by_cause: [cause_count]std.atomic.Value(usize) = @splat(std.atomic.Value(usize).init(0)),
};

const cause_count = std.enums.values(rpc.events.DisconnectCause).len;

comptime {
    // `disconnects_by_cause` is indexed by a cause's backing integer and
    // printed by declaration position, so the named causes must stay dense
    // from zero for the two to agree.
    for (std.enums.values(rpc.events.DisconnectCause), 0..) |cause, i| {
        if (@backingInt(cause) != i) @compileError("DisconnectCause values must stay dense from 0");
    }
}

/// The `disconnects_by_cause` slot for `cause`. `DisconnectCause` is
/// non-exhaustive, so a cause this build does not name is counted as
/// `.unknown` instead of indexing past the array.
fn causeSlot(cause: rpc.events.DisconnectCause) usize {
    const i: usize = @backingInt(cause);
    return if (i < cause_count) i else @backingInt(rpc.events.DisconnectCause.unknown);
}

// -- Setup-failure classification --------------------------------------------

/// Where in session setup a dial failed. Only a `connect`-stage failure can
/// be port exhaustion; `init` covers Connection/QUIC-client construction.
const SetupStage = enum { connect, init };

const SetupClass = enum {
    /// Nothing accepted the dial. The server under test is in-process, so
    /// this gates unless the server is down by design (abrupt deaths).
    refused,
    /// The host ran out of ephemeral ports (TIME_WAIT pile-up). Reported.
    port_exhaustion,
    /// Memory, fd or other OS resource limits. Reported.
    resources,
    /// Gates like `refused`.
    timeout,
    /// A Windows connect-stage error.Unexpected. Resolved at the end of the
    /// run by its shape (ambiguousIsPortExhaustion): port exhaustion, or
    /// unexplained.
    ambiguous,
    /// Anything unexplained: gated, because it may be a defect.
    other,
};

const setup_class_count = std.enums.values(SetupClass).len;

/// Classify a setup (pre-traffic) failure. `os_tag` is a parameter rather
/// than read from `builtin` so the Windows arm is unit-testable anywhere.
fn classifySetupFailure(err: anyerror, stage: SetupStage, os_tag: std.Target.Os.Tag) SetupClass {
    return switch (err) {
        error.ConnectionRefused => .refused,
        error.AddressInUse, error.AddressUnavailable => .port_exhaustion,
        error.SystemResources,
        error.ProcessFdQuotaExceeded,
        error.SystemFdQuotaExceeded,
        error.OutOfMemory,
        => .resources,
        error.Timeout, error.ConnectionTimedOut => .timeout,
        // std 0.17's netConnectIpWindows returns Unexpected for several
        // distinct failures: the ephemeral bind's AddressInUse and its other
        // unmapped NTSTATUSes, a failed SO_REUSE_UNICASTPORT, and every
        // AFD_CONNECT status except refused and insufficient resources. With
        // SO_REUSE_UNICASTPORT the port is only chosen at AFD_CONNECT, so
        // exhaustion most likely surfaces there, but a ReleaseSafe build does
        // not print the status. The nightly 64-worker lane logged 21020 of
        // them from ~+8 s; that one had the exhaustion shape. A regression in
        // the same path would not, so the shape decides, not the error.
        error.Unexpected => if (os_tag == .windows and stage == .connect) .ambiguous else .other,
        else => .other,
    };
}

/// How the ambiguous (Windows connect-stage Unexpected) failures fell
/// across the run. Successful dials are counted at the first ambiguous
/// failure (the onset), so `dials_ok_before` + `dials_ok_after` is every
/// successful dial of the run.
const AmbiguousShape = struct {
    failures: usize = 0,
    dials_ok_before: usize = 0,
    dials_ok_after: usize = 0,
};

/// The dial path must have worked this many times before an onset can be
/// exhaustion. A broken path (a std AFD regression) fails from its first
/// dial; ephemeral ports run out only after many of them were used.
const exhaustion_min_dials_before: usize = 100;

/// Port exhaustion has one shape: the dial path demonstrably works, then
/// the host runs out of ports and stays out. Windows holds a closed port in
/// TIME_WAIT for 120 s, so from the onset nearly every dial fails, with a
/// trickle of successes as old TIME_WAITs expire. "Stays out" is: at least
/// half of all dial attempts from the onset on failed this way. A path
/// broken from the start fails the first test; an intermittent defect, which
/// fails a fraction of dials throughout, fails the second.
fn ambiguousIsPortExhaustion(shape: AmbiguousShape) bool {
    if (shape.failures == 0) return false;
    if (shape.dials_ok_before < exhaustion_min_dials_before) return false;
    return shape.failures >= shape.dials_ok_after;
}

const SetupVerdict = struct {
    /// Counted against the tolerance: `other`, ambiguous failures without
    /// the exhaustion shape, and refused/timeout while the server under
    /// test should be accepting.
    gated: usize,
    /// Reported: port exhaustion, plus ambiguous failures with its shape.
    port_exhaustion: usize,
    /// Reported: resource limits, plus refused/timeout while the server is
    /// down by design.
    reported: usize,
    ambiguous_is_exhaustion: bool,
    ok: bool,
};

/// Resolve the setup-failure classes into gated and reported totals.
/// `server_down_by_design` is true only under QUIC --abrupt-death-every-ms,
/// where dials into a restart gap are expected to be refused or time out.
/// Everywhere else the server under test is an in-process listener that
/// should accept every dial: refused or timed-out dials mean it stopped
/// serving, which is a failure of the code under test, not host noise.
fn assessSetup(
    counts: [setup_class_count]usize,
    ambiguous: AmbiguousShape,
    server_down_by_design: bool,
    tolerance: usize,
) SetupVerdict {
    const at = struct {
        fn n(c: [setup_class_count]usize, class: SetupClass) usize {
            return c[@backingInt(class)];
        }
    }.n;
    const ambiguous_is_exhaustion = ambiguousIsPortExhaustion(ambiguous);
    const ambiguous_count = at(counts, .ambiguous);
    const not_accepted = at(counts, .refused) +| at(counts, .timeout);
    var gated = at(counts, .other);
    var port_exhaustion = at(counts, .port_exhaustion);
    var reported = at(counts, .resources);
    if (ambiguous_is_exhaustion) port_exhaustion +|= ambiguous_count else gated +|= ambiguous_count;
    if (server_down_by_design) reported +|= not_accepted else gated +|= not_accepted;
    return .{
        .gated = gated,
        .port_exhaustion = port_exhaustion,
        .reported = reported,
        .ambiguous_is_exhaustion = ambiguous_is_exhaustion,
        .ok = gated <= tolerance,
    };
}

test "classifySetupFailure: connect-stage errors map to their classes" {
    try std.testing.expectEqual(SetupClass.refused, classifySetupFailure(error.ConnectionRefused, .connect, .linux));
    try std.testing.expectEqual(SetupClass.port_exhaustion, classifySetupFailure(error.AddressUnavailable, .connect, .linux));
    try std.testing.expectEqual(SetupClass.port_exhaustion, classifySetupFailure(error.AddressInUse, .connect, .macos));
    try std.testing.expectEqual(SetupClass.resources, classifySetupFailure(error.SystemResources, .connect, .linux));
    try std.testing.expectEqual(SetupClass.timeout, classifySetupFailure(error.Timeout, .connect, .linux));
    try std.testing.expectEqual(SetupClass.other, classifySetupFailure(error.BrokenPipe, .connect, .linux));
}

test "classifySetupFailure: a Windows connect-stage Unexpected is ambiguous, never exhaustion outright" {
    // std 0.17 folds several distinct AFD failures into it; only its shape
    // over the run can make it port exhaustion (ambiguousIsPortExhaustion).
    try std.testing.expectEqual(SetupClass.ambiguous, classifySetupFailure(error.Unexpected, .connect, .windows));
    // The same error from Connection.init, or on POSIX, stays unexplained.
    try std.testing.expectEqual(SetupClass.other, classifySetupFailure(error.Unexpected, .init, .windows));
    try std.testing.expectEqual(SetupClass.other, classifySetupFailure(error.Unexpected, .connect, .linux));
}

test "ambiguousIsPortExhaustion: the 2026-10-03 Windows 64-worker shape is exhaustion" {
    // 5517 sessions, then 21020 Unexpected dials from ~+8 s to the end of a
    // 20 s run. The log does not record how the sessions split around the
    // onset; the memory curve puts nearly all of them before it.
    try std.testing.expect(ambiguousIsPortExhaustion(.{ .failures = 21020, .dials_ok_before = 5000, .dials_ok_after = 517 }));
    // Exhaustion that throttles a longer run (old TIME_WAITs expiring) is
    // still exhaustion while failures are at least half the attempts.
    try std.testing.expect(ambiguousIsPortExhaustion(.{ .failures = 8000, .dials_ok_before = 16000, .dials_ok_after = 8000 }));
}

test "ambiguousIsPortExhaustion: a dial path that never worked is not exhaustion" {
    // Broken from the first dial (a std AFD regression, or a host that was
    // already exhausted when the run began: either way nothing was tested).
    try std.testing.expect(!ambiguousIsPortExhaustion(.{ .failures = 30000, .dials_ok_before = 0, .dials_ok_after = 0 }));
    try std.testing.expect(!ambiguousIsPortExhaustion(.{ .failures = 30000, .dials_ok_before = exhaustion_min_dials_before - 1, .dials_ok_after = 0 }));
    try std.testing.expect(ambiguousIsPortExhaustion(.{ .failures = 30000, .dials_ok_before = exhaustion_min_dials_before, .dials_ok_after = 0 }));
}

test "ambiguousIsPortExhaustion: intermittent failures are not exhaustion" {
    // A tenth of the dials fail, from early on to the end.
    try std.testing.expect(!ambiguousIsPortExhaustion(.{ .failures = 1500, .dials_ok_before = 200, .dials_ok_after = 13500 }));
    // One past even is already not sustained.
    try std.testing.expect(!ambiguousIsPortExhaustion(.{ .failures = 999, .dials_ok_before = 200, .dials_ok_after = 1000 }));
    try std.testing.expect(!ambiguousIsPortExhaustion(.{ .failures = 0, .dials_ok_before = 200, .dials_ok_after = 0 }));
}

fn setupCounts(pairs: []const struct { SetupClass, usize }) [setup_class_count]usize {
    var counts: [setup_class_count]usize = @splat(0);
    for (pairs) |p| counts[@backingInt(p[0])] = p[1];
    return counts;
}

test "assessSetup: refused and timeout gate while the server under test should be up" {
    // The in-process server stopped accepting: every later dial is refused.
    const refused = setupCounts(&.{.{ .refused, 9 }});
    const v = assessSetup(refused, .{}, false, 8);
    try std.testing.expect(!v.ok);
    try std.testing.expectEqual(@as(usize, 9), v.gated);
    // Refused and timeout add up with `other` against one tolerance.
    try std.testing.expect(!assessSetup(setupCounts(&.{ .{ .refused, 3 }, .{ .timeout, 3 }, .{ .other, 3 } }), .{}, false, 8).ok);
    try std.testing.expect(assessSetup(setupCounts(&.{ .{ .refused, 4 }, .{ .timeout, 4 } }), .{}, false, 8).ok);
}

test "assessSetup: refused and timeout are reported while the server is down by design" {
    // QUIC --abrupt-death-every-ms: dials into the restart gap fail.
    const v = assessSetup(setupCounts(&.{ .{ .refused, 400 }, .{ .timeout, 50 } }), .{}, true, 8);
    try std.testing.expect(v.ok);
    try std.testing.expectEqual(@as(usize, 0), v.gated);
    try std.testing.expectEqual(@as(usize, 450), v.reported);
    // `other` still gates in that mode.
    try std.testing.expect(!assessSetup(setupCounts(&.{.{ .other, 9 }}), .{}, true, 8).ok);
}

test "assessSetup: ambiguous Windows dials gate unless they have the exhaustion shape" {
    const counts = setupCounts(&.{.{ .ambiguous, 21020 }});
    const exhausted = assessSetup(counts, .{ .failures = 21020, .dials_ok_before = 5000, .dials_ok_after = 517 }, false, 11);
    try std.testing.expect(exhausted.ok);
    try std.testing.expect(exhausted.ambiguous_is_exhaustion);
    try std.testing.expectEqual(@as(usize, 21020), exhausted.port_exhaustion);
    const broken = assessSetup(counts, .{ .failures = 21020, .dials_ok_before = 0, .dials_ok_after = 0 }, false, 11);
    try std.testing.expect(!broken.ok);
    try std.testing.expect(!broken.ambiguous_is_exhaustion);
    try std.testing.expectEqual(@as(usize, 21020), broken.gated);
    try std.testing.expectEqual(@as(usize, 0), broken.port_exhaustion);
}

test "assessSetup: port exhaustion and resource limits are reported, not gated" {
    const v = assessSetup(setupCounts(&.{ .{ .port_exhaustion, 5000 }, .{ .resources, 20 } }), .{}, false, 8);
    try std.testing.expect(v.ok);
    try std.testing.expectEqual(@as(usize, 5000), v.port_exhaustion);
    try std.testing.expectEqual(@as(usize, 20), v.reported);
}

// -- Transport-error bound ---------------------------------------------------

/// Default slack on top of the structural allowances: max(8, 0.2% of
/// sessions). Every CI TCP lane before this gate landed showed exactly
/// transport_errors == chaos_closes, so the floor absorbs rare contention
/// without hiding a per-session failure mode (which scales with sessions
/// far past 0.2%).
fn defaultTransportTolerance(sessions: usize) usize {
    return @max(8, sessions / 500);
}

const TransportVerdict = struct {
    ok: bool,
    allowed: usize,
};

/// transport_errors <= chaos_closes + death_allowance + tolerance. See the
/// file header for why each term exists.
fn assessTransport(transport_errors: usize, chaos_closes: usize, death_allowance: usize, tolerance: usize) TransportVerdict {
    const allowed = chaos_closes +| death_allowance +| tolerance;
    return .{ .ok = transport_errors <= allowed, .allowed = allowed };
}

test "assessTransport: chaos-only errors pass" {
    // The steady CI shape: one post-rip pump failure per chaos session.
    const v = assessTransport(1117, 1117, 0, defaultTransportTolerance(5533));
    try std.testing.expect(v.ok);
}

test "assessTransport: errors past chaos + tolerance fail" {
    // The pre-gate Windows 64-worker shape: 22130 errors, 1110 chaos closes.
    const v = assessTransport(22130, 1110, 0, defaultTransportTolerance(5517));
    try std.testing.expect(!v.ok);
    try std.testing.expectEqual(@as(usize, 1110 + 11), v.allowed);
    // One past the bound is already red.
    try std.testing.expect(!assessTransport(1110 + 8 + 1, 1110, 0, 8).ok);
    try std.testing.expect(assessTransport(1110 + 8, 1110, 0, 8).ok);
}

test "assessTransport: abrupt deaths widen the bound by their allowance only" {
    // Nightly QUIC heal soak: 299 errors, 243 chaos, 10 deaths x 8 churn.
    try std.testing.expect(assessTransport(299, 243, 10 * 8, defaultTransportTolerance(1285)).ok);
    try std.testing.expect(!assessTransport(299, 243, 0, defaultTransportTolerance(1285)).ok);
}

fn nowNs(io: std.Io) i64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

fn nowNsU(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

// -- Thread-safe live-heap counting allocator --------------------------------
//
// Wraps the backing allocator and tracks live bytes (allocated − freed) so a
// background sampler can watch the heap over time. Counters are atomic so
// worker threads and the sampler never tear a read. This is the "is memory
// flat?" signal: steady churn holds live_bytes roughly constant; a leak or
// unbounded growth makes it climb.

const CountingAllocator = struct {
    backing: std.mem.Allocator,
    allocated: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    freed: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    const vtable = std.mem.Allocator.VTable{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    fn init(backing: std.mem.Allocator) CountingAllocator {
        return .{ .backing = backing };
    }

    fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// Live bytes = allocated − freed. Saturating so a transient read that
    /// observes freed slightly ahead of allocated cannot underflow.
    fn liveBytes(self: *const CountingAllocator) usize {
        return self.allocated.load(.monotonic) -| self.freed.load(.monotonic);
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        _ = self.allocated.fetchAdd(len, .monotonic);
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ok = self.backing.rawResize(memory, alignment, new_len, ret_addr);
        if (ok) {
            if (new_len > memory.len) {
                _ = self.allocated.fetchAdd(new_len - memory.len, .monotonic);
            } else {
                _ = self.freed.fetchAdd(memory.len - new_len, .monotonic);
            }
        }
        return ok;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        if (new_len > memory.len) {
            _ = self.allocated.fetchAdd(new_len - memory.len, .monotonic);
        } else {
            _ = self.freed.fetchAdd(memory.len - new_len, .monotonic);
        }
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        _ = self.freed.fetchAdd(memory.len, .monotonic);
        self.backing.rawFree(memory, alignment, ret_addr);
    }
};

// -- Latency accounting ------------------------------------------------------
//
// Each worker owns its own fixed-size latency histogram (no cross-thread
// locking on the hot path, no allocation per call). Histograms are merged
// after all workers join.
//
// Fixed size is load-bearing for BOTH memory instruments. The previous
// design appended 8 bytes per successful call to a growable list. Counted
// by the heap gate, that turned the steady-state check into a
// calls-per-run gate — exactly how the Windows soak lane failed every
// nightly from its first run (2026-08-12): Windows completed 2-4x the calls
// of the Nagle-capped Linux client in the same 120 s, and the "growth" was
// ~6 B/call of the harness's own samples. Moving the list to an uncounted
// allocator fixed the heap gate, but the list still lived in the process's
// resident set, so the RSS gate would have inherited the same false trend.

/// Log-linear histogram (HDR-style): values below 128 ns get exact buckets;
/// above that, each power-of-two range is split into 64 equal sub-buckets,
/// so a reported percentile is within 1/128 (<0.8%) of the true sample
/// value (the bucket midpoint, never above the observed max). Max is exact.
/// ~30 KiB per worker, allocated and touched before sampling starts.
const LatencyHist = struct {
    const sub_bits = 7;
    const half: usize = 1 << (sub_bits - 1);
    const bucket_count: usize = half * (64 - sub_bits + 2);

    buckets: [bucket_count]u64 = @splat(0),
    count: u64 = 0,
    max: u64 = 0,

    fn indexOf(v: u64) usize {
        if (v < 2 * half) return @intCast(v);
        const msb: u7 = 63 - @as(u7, @clz(v));
        const shift: u6 = @intCast(msb + 1 - sub_bits);
        return half * @as(usize, shift) + @as(usize, @intCast(v >> shift));
    }

    fn lowerBound(index: usize) u64 {
        if (index < 2 * half) return index;
        const shift: u6 = @intCast(index / half - 1);
        return @as(u64, index - half * @as(usize, shift)) << shift;
    }

    fn width(index: usize) u64 {
        if (index < 2 * half) return 1;
        const shift: u6 = @intCast(index / half - 1);
        return @as(u64, 1) << shift;
    }

    fn record(self: *LatencyHist, ns: u64) void {
        self.buckets[indexOf(ns)] += 1;
        self.count += 1;
        self.max = @max(self.max, ns);
    }

    fn merge(self: *LatencyHist, other: *const LatencyHist) void {
        for (&self.buckets, other.buckets) |*a, b| a.* += b;
        self.count += other.count;
        self.max = @max(self.max, other.max);
    }

    /// Nearest-rank percentile over the recorded samples (same rank rule as
    /// the sorted-list implementation this replaced).
    fn percentile(self: *const LatencyHist, pct: f64) u64 {
        if (self.count == 0) return 0;
        const rank: u64 = @intFromFloat(@round(pct / 100.0 * @as(f64, @floatFromInt(self.count - 1))));
        var cumulative: u64 = 0;
        for (self.buckets, 0..) |c, i| {
            cumulative += c;
            if (cumulative > rank) return @min(lowerBound(i) + (width(i) - 1) / 2, self.max);
        }
        return self.max;
    }
};

test "LatencyHist: buckets are contiguous and cover u64" {
    try std.testing.expectEqual(@as(usize, 0), LatencyHist.indexOf(0));
    try std.testing.expectEqual(@as(usize, 127), LatencyHist.indexOf(127));
    try std.testing.expectEqual(@as(usize, 128), LatencyHist.indexOf(128));
    try std.testing.expectEqual(LatencyHist.bucket_count - 1, LatencyHist.indexOf(std.math.maxInt(u64)));
    // Every bucket's lower bound maps back to that bucket, and the next
    // bucket starts exactly where this one ends.
    var i: usize = 0;
    while (i + 1 < LatencyHist.bucket_count) : (i += 1) {
        try std.testing.expectEqual(i, LatencyHist.indexOf(LatencyHist.lowerBound(i)));
        try std.testing.expectEqual(LatencyHist.lowerBound(i + 1), LatencyHist.lowerBound(i) + LatencyHist.width(i));
    }
}

test "LatencyHist: percentiles track exact values within bucket precision" {
    var h: LatencyHist = .{};
    // 1 us .. 100 ms in 1 us steps: nearest-rank p50 is 50_001_000 ns and
    // p99 is 99_000_000 ns.
    var v: u64 = 1;
    while (v <= 100_000) : (v += 1) h.record(v * 1000);
    try std.testing.expectEqual(@as(u64, 100_000), h.count);
    try std.testing.expectEqual(@as(u64, 100_000_000), h.max);
    const p50 = @as(f64, @floatFromInt(h.percentile(50)));
    const p99 = @as(f64, @floatFromInt(h.percentile(99)));
    try std.testing.expect(@abs(p50 - 50_000_000) / 50_000_000 < 0.01);
    try std.testing.expect(@abs(p99 - 99_000_000) / 99_000_000 < 0.01);
    // Small values are exact, and an empty histogram reports zeros.
    var small: LatencyHist = .{};
    for ([_]u64{ 3, 5, 7 }) |s| small.record(s);
    try std.testing.expectEqual(@as(u64, 5), small.percentile(50));
    const empty: LatencyHist = .{};
    try std.testing.expectEqual(@as(u64, 0), empty.percentile(99));
}

test "LatencyHist: merge sums counts and keeps the larger max" {
    var a: LatencyHist = .{};
    var b: LatencyHist = .{};
    a.record(42_000_000);
    b.record(1_000);
    b.record(2_000);
    a.merge(&b);
    try std.testing.expectEqual(@as(u64, 3), a.count);
    try std.testing.expectEqual(@as(u64, 42_000_000), a.max);
    try std.testing.expectEqual(@as(u64, 42_000_000), a.percentile(100));
}

// -- Server ------------------------------------------------------------------

const EchoServer = struct {
    /// Calls to this method sleep server-side before answering, so client
    /// deadlines (1ms) expire and the cancellation path runs against a
    /// real server that still sends its (late) Return.
    pub const slow_method_id: u16 = 2;
    pub const slow_method_delay_ms: u64 = 10;

    // Stable non-null anchor for the bootstrap export ctx (the handler
    // ignores it, but the call orchestration requires a non-null ctx).
    var ctx_anchor: u8 = 0;
    var server_io: ?std.Io = null;

    fn onCall(
        _: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        if (call.method_id == slow_method_id) {
            if (server_io) |io| sleepMs(io, slow_method_delay_ms);
        }
        try peer.sendReturnEmptyStruct(call.question_id);
    }

    fn onAccept(
        _: *anyopaque,
        peer: *Peer,
        _: *Connection,
        _: u32,
    ) anyerror!WorkerPool.AcceptDecision {
        _ = try peer.setBootstrap(.{ .ctx = @ptrCast(&ctx_anchor), .on_call = EchoServer.onCall });
        peer.start(null, null, null);
        return .accept;
    }

    /// The vat-level restore convention the healing workers ride: any peer
    /// presenting the fixed soak sturdy ref gets a fresh echo export. The
    /// re-export-per-restore shape is idempotent at the app layer, which is
    /// what a 0-RTT-riding restore requires.
    const sturdy_ref = "sturdy:soak-echo/1";

    fn onRestore(_: *anyopaque, peer: *Peer, ref: []const u8) anyerror!rpc.peer.RestoreOutcome {
        if (!std.mem.eql(u8, ref, sturdy_ref)) return .unknown;
        return .{ .existing = try peer.addExport(.{ .ctx = @ptrCast(&ctx_anchor), .on_call = onCall }) };
    }
};

/// Runs the TCP server under test (WorkerPool.run) on its own thread and
/// records whether it stopped serving before main asked it to. If run()
/// returns early (a spawn failure, or accept loops that quit), later dials
/// are refused or sit in the backlog unserved, while sessions that finished
/// earlier still count as traffic: without this flag the run could pass.
const PoolRunner = struct {
    pool: *WorkerPool,
    io: std.Io,
    stop_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    // Written by the pool thread, read by main after join.
    run_error: ?anyerror = null,
    exited_early: bool = false,
    exit_ns: i64 = 0,

    fn threadMain(self: *PoolRunner) void {
        self.pool.run() catch |err| {
            self.run_error = err;
            std.debug.print("soak: worker pool run failed: {}\n", .{err});
        };
        if (!self.stop_requested.load(.acquire)) {
            self.exited_early = true;
            self.exit_ns = nowNs(self.io);
            std.debug.print("soak: worker pool stopped serving before shutdown was requested\n", .{});
        }
    }

    /// Whether the server under test served the whole run.
    fn servedWholeRun(self: *const PoolRunner) bool {
        return self.run_error == null and !self.exited_early;
    }
};

// -- Client session ----------------------------------------------------------

const SessionMode = enum { normal, chaos, deadline };

fn SessionOf(comptime ConnT: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        io: std.Io,
        conn: *ConnT,
        peer: *Peer,
        totals: *Totals,
        latency: *LatencyHist,
        mode: SessionMode,
        calls_target: u32,
        // Max outstanding questions for this session. Deadline/chaos sessions run
        // sequentially (inflight 1) so their timing semantics stay exact.
        max_inflight: u32,

        issued: u32 = 0,
        completed: u32 = 0,
        bootstrap_import_id: ?u32 = null,
        chaos_close_initiated: bool = false,
        failed: bool = false,

        // Per-outstanding-question send timestamps, keyed by (index % max_inflight).
        // Sized to MAX_INFLIGHT_SLOTS; --inflight is clamped to it.
        send_ts: [MAX_INFLIGHT_SLOTS]u64 = @splat(0),

        const MAX_INFLIGHT_SLOTS = 256;

        fn onBootstrapReturn(
            ctx: *anyopaque,
            peer: *Peer,
            ret: protocol.Return,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            if (ret.tag != .results) {
                self.noteException(ret);
                self.conn.close();
                return;
            }
            const payload = ret.results orelse return error.MissingPayload;
            const cap = try payload.content.getCapability();
            const resolved = try caps.resolveCapability(cap);
            switch (resolved) {
                .imported => |imported| self.bootstrap_import_id = imported.id,
                else => return error.UnexpectedResolvedCapability,
            }
            try self.pump(peer);
        }

        fn buildEmptyCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            _ = try call.initCapTableTyped(0);
        }

        /// Issue calls up to the inflight ceiling / remaining budget. Chaos
        /// sessions never pump past the half-way rip point.
        fn pump(self: *Self, peer: *Peer) !void {
            const target = self.bootstrap_import_id orelse return error.MissingBootstrapImport;
            // Deadline sessions call the slow server method so the 1ms call
            // deadline expires before the (10ms-delayed) Return arrives.
            const method_id: u16 = if (self.mode == .deadline) EchoServer.slow_method_id else 1;
            while (self.issued < self.calls_target and (self.issued - self.completed) < self.max_inflight) {
                const slot = self.issued % self.max_inflight;
                self.send_ts[slot] = nowNsU(self.io);
                _ = try peer.sendCallResolved(
                    .{ .imported = .{ .id = target } },
                    0x5050_5050,
                    method_id,
                    self,
                    buildEmptyCall,
                    onCallReturn,
                );
                self.issued += 1;
            }
        }

        fn onCallReturn(
            ctx: *anyopaque,
            peer: *Peer,
            ret: protocol.Return,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const done_index = self.completed;
            switch (ret.tag) {
                .results => {
                    _ = self.totals.calls_ok.fetchAdd(1, .monotonic);
                    // Returns are answered FIFO on a single connection, so the
                    // completion index matches issue order for the slot key.
                    const slot = done_index % self.max_inflight;
                    const latency = nowNsU(self.io) -| self.send_ts[slot];
                    self.latency.record(latency);
                },
                .exception => self.noteException(ret),
                else => {},
            }
            self.completed += 1;

            if (self.completed >= self.calls_target) {
                self.conn.close();
                return;
            }

            // Chaos: leave one call in flight and rip the connection down. The
            // peer's close contract fails that in-flight question with a
            // synthetic "disconnected" exception Return, which noteException
            // must treat as the expected outcome for this session.
            if (self.mode == .chaos and self.completed == self.calls_target / 2) {
                self.chaos_close_initiated = true;
                self.sendOne(peer) catch {};
                _ = self.totals.chaos_closes.fetchAdd(1, .monotonic);
                self.conn.close();
                return;
            }

            self.pump(peer) catch |err| {
                if (err == error.PeerShuttingDown) return;
                self.failed = true;
                self.conn.close();
            };
        }

        /// Issue exactly one extra call (used by chaos to leave a question in
        /// flight); ignores the inflight ceiling on purpose.
        fn sendOne(self: *Self, peer: *Peer) !void {
            const target = self.bootstrap_import_id orelse return error.MissingBootstrapImport;
            const method_id: u16 = if (self.mode == .deadline) EchoServer.slow_method_id else 1;
            _ = try peer.sendCallResolved(
                .{ .imported = .{ .id = target } },
                0x5050_5050,
                method_id,
                self,
                buildEmptyCall,
                onCallReturn,
            );
            self.issued += 1;
        }

        fn noteException(self: *Self, ret: protocol.Return) void {
            const reason = if (ret.exception) |ex| ex.reason else "";
            if (std.mem.eql(u8, reason, "deadline exceeded")) {
                _ = self.totals.calls_cancelled.fetchAdd(1, .monotonic);
            } else if (std.mem.eql(u8, reason, rpc.peer.disconnected_reason)) {
                // The peer's close contract fails every in-flight question with a
                // synthetic "disconnected" Return. This is the expected outcome
                // when a chaos session rips its own connection down, and also a
                // legitimate outcome at high peer counts where a starved event
                // loop trips its own idle timeout and drops the connection. Keep
                // the chaos-specific tally exact; count the rest as contention.
                if (self.chaos_close_initiated) {
                    _ = self.totals.expected_disconnects.fetchAdd(1, .monotonic);
                } else {
                    _ = self.totals.contention_disconnects.fetchAdd(1, .monotonic);
                }
            } else {
                _ = self.totals.unexpected_exceptions.fetchAdd(1, .monotonic);
                std.debug.print("soak: unexpected exception reason: '{s}'\n", .{reason});
            }
        }

        fn onPeerError(_: ?*anyopaque, _: *Peer, _: anyerror) void {}
        fn onPeerClose(_: ?*anyopaque, _: *Peer) void {}
    };
}

const Session = SessionOf(Connection);

fn dialTcp(allocator: std.mem.Allocator, io: std.Io, address: net.IpAddress, cfg: *const Config, stage: *SetupStage) anyerror!Connection {
    var addr = address;
    stage.* = .connect;
    const stream = try net.IpAddress.connect(&addr, io, .{ .mode = .stream });
    stage.* = .init;
    const tcp_runtime = rpc.transport.tcp.runtime;
    // Connection.init takes ownership of the socket only on success.
    errdefer tcp_runtime.closeFd(io, .{ .handle = stream.socket.handle });
    // Best-effort and a no-op on Windows, where std's AFD sockets expose no
    // TCP_NODELAY path yet (see setTcpNoDelay; Windows NODELAY via AFD is
    // deferred work). The server side already sets it on every accept.
    if (cfg.nodelay) tcp_runtime.setTcpNoDelay(.{ .handle = stream.socket.handle });
    return try Connection.init(allocator, io, .{ .handle = stream.socket.handle }, .{
        .tick_interval_ms = 5,
        // Generous idle timeout: under high peer counts a session's event
        // loop can be starved for a while; a tight timeout would trip it
        // and drop otherwise-healthy connections. Deadline cancellation
        // uses the per-call timeout, not this, so keeping it loose is safe.
        .idle_timeout_ms = 15_000,
    });
}

fn dialQuic(allocator: std.mem.Allocator, io: std.Io, address: net.IpAddress, cfg: *const Config, stage: *SetupStage) anyerror!quic.Connection {
    // Client init binds the UDP socket and starts the handshake; nothing
    // here is a connect in the TCP sense.
    stage.* = .init;
    var options = quic.ClientOptions{
        .remote_addr = address,
        .server_name = "localhost",
        // Loopback soak against the checked-in self-signed fixture cert.
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
        // Loopback handshakes complete in milliseconds; a dial that gets
        // silently dropped should fail fast and retry, not park a worker.
        .handshake_timeout_ms = 2_000,
    };
    applyCc(&options.congestion_control, cfg.cc);
    return try quic.Connection.initClient(allocator, io, options);
}

/// Map the soak's local `--cc` choice onto quic-zig's enum; `.default`
/// leaves the transport's own default in place.
fn applyCc(field: anytype, choice: CcChoice) void {
    switch (choice) {
        .default => {},
        .cubic => field.* = .cubic,
        .bbr => field.* = .bbr,
        .newreno => field.* = .new_reno,
    }
}

/// Log at most this many setup failures per class, process-wide; the
/// end-of-run summary carries the full classified counts. (The Windows
/// 64-worker lane once printed 21020 identical lines.)
const setup_failure_log_limit = 3;

fn WorkerOf(
    comptime ConnT: type,
    comptime dialFn: fn (std.mem.Allocator, std.Io, net.IpAddress, *const Config, *SetupStage) anyerror!ConnT,
) type {
    return struct {
        const Self = @This();
        const SessionT = SessionOf(ConnT);

        allocator: std.mem.Allocator,
        io: std.Io,
        address: net.IpAddress,
        cfg: *const Config,
        totals: *Totals,
        latency: *LatencyHist,
        stop_at_ns: i64,
        index: u32,

        fn main(self: Self) void {
            var session_index: u64 = 0;
            while (nowNs(self.io) < self.stop_at_ns) : (session_index += 1) {
                // runSession absorbs setup failures itself; an error here
                // happened after the connection was up.
                self.runSession(session_index) catch |err| {
                    _ = self.totals.transport_errors.fetchAdd(1, .monotonic);
                    std.debug.print("soak: worker {} mid-session error: {}\n", .{ self.index, err });
                    sleepMs(self.io, 5);
                };
            }
        }

        fn noteSetupFailure(self: Self, err: anyerror, stage: SetupStage) void {
            const class = classifySetupFailure(err, stage, builtin.os.tag);
            const n = self.totals.setup_failures[@backingInt(class)].fetchAdd(1, .monotonic) + 1;
            const offset_ns = nowNs(self.io) - self.totals.start_ns;
            const offset_ms: usize = @intCast(@max(0, @divTrunc(offset_ns, std.time.ns_per_ms)));
            switch (class) {
                .port_exhaustion => _ = self.totals.first_port_exhaustion_ms.fetchMin(offset_ms, .monotonic),
                .ambiguous => {
                    // The first one fixes the onset and the successful-dial
                    // count before it (see AmbiguousShape).
                    const none = std.math.maxInt(usize);
                    if (self.totals.ambiguous_onset_ms.cmpxchgStrong(none, offset_ms, .acq_rel, .monotonic) == null) {
                        self.totals.dials_ok_at_ambiguous_onset.store(self.totals.dials_ok.load(.monotonic), .monotonic);
                    }
                },
                else => {},
            }
            if (n <= setup_failure_log_limit) {
                std.debug.print("soak: worker {} setup failure ({s}, {s} stage): {}\n", .{ self.index, @tagName(class), @tagName(stage), err });
            }
        }

        fn pickMode(self: Self, session_index: u64) SessionMode {
            if (self.cfg.chaos and session_index % 5 == 1) return .chaos;
            if (self.cfg.deadlines and session_index % 4 == 2) return .deadline;
            return .normal;
        }

        fn runSession(self: Self, session_index: u64) !void {
            const conn = try self.allocator.create(ConnT);
            errdefer self.allocator.destroy(conn);
            var stage: SetupStage = .connect;
            conn.* = dialFn(self.allocator, self.io, self.address, self.cfg, &stage) catch |err| {
                // Setup failure: counted by class, never as a mid-session
                // transport error. Returning normally skips the errdefer.
                self.allocator.destroy(conn);
                self.noteSetupFailure(err, stage);
                sleepMs(self.io, 5);
                return;
            };
            _ = self.totals.dials_ok.fetchAdd(1, .monotonic);

            const peer = try self.allocator.create(Peer);
            errdefer self.allocator.destroy(peer);
            peer.* = Peer.init(self.allocator, conn);

            const mode = self.pickMode(session_index);
            peer.setClockIo(self.io);
            if (mode == .deadline) {
                peer.setTimeouts(.{ .default_call_timeout_ms = 1 });
            }

            // Only normal sessions run high-in-flight; chaos/deadline keep exact
            // sequential timing so their invariants hold. Clamp to the slot cap.
            const inflight: u32 = switch (mode) {
                .normal => @min(@max(self.cfg.inflight, 1), SessionT.MAX_INFLIGHT_SLOTS),
                else => 1,
            };

            var session = SessionT{
                .allocator = self.allocator,
                .io = self.io,
                .conn = conn,
                .peer = peer,
                .totals = self.totals,
                .latency = self.latency,
                .mode = mode,
                // Each deadline-session call burns a 10ms server sleep; keep
                // those sessions short so they don't starve the pool workers.
                .calls_target = if (mode == .deadline)
                    @min(self.cfg.calls_per_session, 6)
                else
                    self.cfg.calls_per_session,
                .max_inflight = inflight,
            };

            peer.start(null, SessionT.onPeerError, SessionT.onPeerClose);
            _ = try peer.sendBootstrap(&session, SessionT.onBootstrapReturn);

            conn.run();

            _ = self.totals.sessions.fetchAdd(1, .monotonic);
            const cause = peer.lastDisconnectCause();
            _ = self.totals.disconnects_by_cause[causeSlot(cause)].fetchAdd(1, .monotonic);
            if (session.failed) {
                _ = self.totals.transport_errors.fetchAdd(1, .monotonic);
            }
            if (self.cfg.inject_transport_error_every) |every| {
                // Ablation hook: a synthetic mid-session error on every Nth
                // session of this worker, so the bound can be shown red.
                if (session_index % every == every - 1) {
                    _ = self.totals.transport_errors.fetchAdd(1, .monotonic);
                    _ = self.totals.injected_transport_errors.fetchAdd(1, .monotonic);
                }
            }

            _ = peer.takeAttachedConnection(*ConnT);
            peer.deinit();
            self.allocator.destroy(peer);
            conn.deinit();
            self.allocator.destroy(conn);
        }
    };
}

const Worker = WorkerOf(Connection, dialTcp);
const QuicWorker = if (quic.enabled) WorkerOf(quic.Connection, dialQuic) else void;

// -- Healing worker (QUIC only) ----------------------------------------------
//
// One persistent WarmRedialClient per healing worker: restore-backed echo
// traffic that chains a new call on every Return (a reset only reaches a
// client that SENDS) and auto-heals across abrupt server deaths. The app
// state mirrors the heal e2e's shape.

const HealApp = if (quic.enabled) struct {
    echo_ok: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    gave_up: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    // Run-thread-only within a generation.
    last_cap: ?cap_table.ResolvedCap = null,
    outcome: ?quic.WarmRedialClient.Outcome = null,

    fn onRebind(ctx: ?*anyopaque, peer: *Peer, cap: cap_table.ResolvedCap) void {
        const self: *HealApp = @ptrCast(@alignCast(ctx.?));
        self.last_cap = cap;
        self.sendEcho(peer, cap);
    }

    fn sendEcho(self: *HealApp, peer: *Peer, cap: cap_table.ResolvedCap) void {
        _ = peer.sendCallResolved(cap, 0, 1, self, null, onEchoReturn) catch {};
    }

    fn onEchoReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        _ = caps;
        const self: *HealApp = @ptrCast(@alignCast(ctx));
        if (ret.tag != .results) return; // the death's synthetic Return ends this chain
        _ = self.echo_ok.fetchAdd(1, .monotonic);
        self.sendEcho(peer, self.last_cap orelse return);
    }

    fn onGiveUp(ctx: ?*anyopaque, cause: rpc.events.DisconnectCause) void {
        const self: *HealApp = @ptrCast(@alignCast(ctx.?));
        _ = cause;
        self.gave_up.store(true, .release);
    }

    fn threadMain(self: *HealApp, client: *quic.WarmRedialClient) void {
        self.outcome = client.run() catch null;
    }
} else void;

// -- QUIC server harness -----------------------------------------------------
//
// TCP's WorkerPool equivalent for the fanout QUIC server: ONE thread owns the
// whole server (accept, session servicing, reaping) plus the Peer lifecycle
// for every accepted session, because the Server's session-list accessors are
// loop-thread-only by contract. New sessions get an EchoServer bootstrap Peer
// bound on the pass after acceptance (the engines dispatch frames that
// buffered before binding); Peers whose session id has disappeared (the
// server reaps sessions once closed) are torn down on the same thread.

const soak_quic_cert_pem = @embedFile("soak_certs/loopback_cert.pem");
const soak_quic_key_pem = @embedFile("soak_certs/loopback_key.pem");

const QuicServerHarness = struct {
    allocator: std.mem.Allocator = undefined,
    server: if (quic.enabled) ?*quic.Server else ?void = null,
    thread: ?std.Thread = null,
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    bound: if (quic.enabled) std.AutoHashMap(u64, *Peer) else void = undefined,
    // Reset-emitter field data (the churn signals upstream's stability case
    // for the reset surfaces asks for). unroutable_* count LogEvent
    // .unroutable_dcid; resets_sent snapshots the server counter at drain.
    // The harness always sets a reset key, so resets_sent==0 here means no
    // stale-CID traffic arrived — NOT a keyless-config artifact. The
    // unroutable_* pair is the signal that holds either way.
    unroutable_seen: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    unroutable_reset_queued: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    resets_sent: u64 = 0,
    // Abrupt-death mode state (loop-thread except where noted).
    io: std.Io = undefined,
    workers_hint: u32 = 0,
    cc: CcChoice = .default,
    death_every_ms: ?u64 = null,
    // Concrete bound address captured after the first bind; every restart
    // re-binds this exact port so surviving clients keep sending into it.
    listen_addr: if (quic.enabled) net.IpAddress else void = undefined,
    deaths: u64 = 0,
    // True while the Server value at `server` is deinitialized (a rebind
    // race exhausted its retries): shutdown must not deinit it again.
    server_destroyed: bool = false,
    // Accumulated per-FeedOutcome datagram counts across incarnations
    // (loop-thread; snapshotted like resets_sent).
    feed_counts: if (quic.enabled) [quic.Server.feed_outcome_count]u64 else void =
        if (quic.enabled) @splat(0) else {},
    rebind_failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn makeOptions(self: *QuicServerHarness, listen_addr: net.IpAddress) quic.ServerOptions {
        if (comptime !quic.enabled) unreachable;
        var server_options = quic.ServerOptions{
            .listen_addr = listen_addr,
            .tls_cert_pem = soak_quic_cert_pem,
            .tls_key_pem = soak_quic_key_pem,
            .receive_timeout = std.Io.Duration.fromMilliseconds(5),
            // Each worker holds one LIVE session, but closing sessions
            // linger in the table through their drain period before reap —
            // and the batched receive path raised churn enough that x2
            // pinned the table at its cap (measured: sessions gauge parked
            // at the cap with hundreds of silent table_full drops, and an
            // unlucky dial whose every Initial hit a full window hung its
            // worker forever). x4+16 absorbs the measured draining backlog
            // with headroom; the feed-outcome report keeps table_full
            // visible if churn ever outruns it again.
            .max_concurrent_connections = self.workers_hint * 4 + 16,
            // Fixed soak key: turns churned-away connection state into
            // observable stateless resets instead of silent timeouts.
            // MUST be byte-identical across abrupt-death incarnations, or
            // surviving clients lose the certificate and stall to idle
            // timeout instead.
            .stateless_reset_key = @as(quic.StatelessResetKey, @splat(0x51)),
            // Sweep half-opens fast: abandoned dials must release their
            // slots well inside the run window.
            .handshake_timeout_ms = 2_000,
            .log_callback = QuicServerHarness.onServerLog,
            .log_user_data = self,
        };
        applyCc(&server_options.congestion_control, self.cc);
        return server_options;
    }

    fn start(self: *QuicServerHarness, allocator: std.mem.Allocator, io: std.Io, workers: u32, cc: CcChoice, death_every_ms: ?u64) !net.IpAddress {
        if (comptime !quic.enabled) unreachable;
        self.allocator = allocator;
        self.io = io;
        self.workers_hint = workers;
        self.cc = cc;
        self.death_every_ms = death_every_ms;
        self.bound = std.AutoHashMap(u64, *Peer).init(allocator);
        const server = try allocator.create(quic.Server);
        errdefer allocator.destroy(server);
        server.* = try quic.Server.init(allocator, io, self.makeOptions(.{ .ip4 = .loopback(0) }));
        self.server = server;
        self.listen_addr = server.getAddress();
        self.thread = try std.Thread.spawn(.{}, QuicServerHarness.threadMain, .{self});
        return self.listen_addr;
    }

    fn shutdown(self: *QuicServerHarness) void {
        if (comptime !quic.enabled) unreachable;
        self.stop.store(true, .release);
        // In abrupt-death mode the loop thread may be mid-kill (the Server
        // value transiently deinitialized), so a cross-thread wake() would
        // race a use-after-free. stepOnce's 5ms receive timeout bounds the
        // un-woken shutdown latency instead.
        if (self.death_every_ms == null) {
            if (self.server) |server| server.wake();
        }
        if (self.thread) |t| t.join();
        if (self.server) |server| {
            if (!self.server_destroyed) server.deinit();
            self.allocator.destroy(server);
        }
        self.bound.deinit();
    }

    fn onServerLog(user_data: ?*anyopaque, ev: quic.ServerLogEvent) void {
        if (comptime !quic.enabled) unreachable;
        const self: *QuicServerHarness = @ptrCast(@alignCast(user_data.?));
        switch (ev) {
            .unroutable_dcid => |info| {
                _ = self.unroutable_seen.fetchAdd(1, .monotonic);
                if (info.reset_queued) _ = self.unroutable_reset_queued.fetchAdd(1, .monotonic);
            },
            // LogEvent is additive upstream; never switch exhaustively.
            else => {},
        }
    }

    /// Crash the server the way a real crash does: snapshot counters, deinit
    /// with NO close ceremony (sans-IO teardown guarantees nothing reaches
    /// the wire), destroy the bound peers, then restart on the SAME port
    /// with the SAME reset key. Loop-thread only.
    fn executeAbruptDeath(self: *QuicServerHarness) void {
        if (comptime !quic.enabled) unreachable;
        const server = self.server.?;
        // The counters live on the Server value; accumulate before the kill
        // or the incarnation's numbers vanish from the report.
        self.resets_sent += server.statelessResetsSent();
        for (server.feedOutcomeCounts(), 0..) |c, i| self.feed_counts[i] += c;
        // Order is load-bearing: deinit fires each live session's close
        // callback into its still-alive bound Peer; only then is it safe to
        // detach and destroy the peers (the ServerSessions are gone).
        server.deinit();
        var it = self.bound.valueIterator();
        while (it.next()) |peer_ptr| {
            const peer = peer_ptr.*;
            _ = peer.takeAttachedConnection(*quic.ServerSession);
            peer.deinit();
            self.allocator.destroy(peer);
        }
        // Clear the whole map: a fresh server can reissue overlapping
        // session ids, and stale entries would alias them.
        self.bound.clearRetainingCapacity();
        // Bounded rebind retry: the freed port transiently returns to the
        // kernel pool (same shape as the crash-restart e2e's 20x5ms loop).
        var attempt: u32 = 0;
        while (true) : (attempt += 1) {
            server.* = quic.Server.init(self.allocator, self.io, self.makeOptions(self.listen_addr)) catch |err| {
                if (attempt >= 20) {
                    std.debug.print("soak: abrupt-death rebind failed after {} attempts: {}\n", .{ attempt + 1, err });
                    self.server_destroyed = true;
                    self.rebind_failed.store(true, .release);
                    self.stop.store(true, .release);
                    return;
                }
                sleepMs(self.io, 5);
                continue;
            };
            break;
        }
        self.deaths += 1;
    }

    fn threadMain(self: *QuicServerHarness) void {
        if (comptime !quic.enabled) unreachable;
        const server = self.server.?;
        var next_death_ns: u64 = if (self.death_every_ms) |ms| nowNsU(self.io) + ms * std.time.ns_per_ms else 0;
        while (!self.stop.load(.acquire)) {
            _ = server.stepOnce(.wait) catch |err| {
                std.debug.print("soak: quic server step failed: {}\n", .{err});
            };
            self.bindNewSessions();
            self.sweepDeadPeers();
            if (self.death_every_ms) |ms| {
                if (nowNsU(self.io) >= next_death_ns) {
                    self.executeAbruptDeath();
                    if (self.server_destroyed) return;
                    next_death_ns = nowNsU(self.io) + ms * std.time.ns_per_ms;
                }
            }
        }
        // Drain: close every live session, keep stepping until the server
        // reaps them (bounded), then tear down every remaining Peer before
        // the server itself is deinitialized.
        server.requestClose();
        var budget: u32 = 400;
        while (budget > 0 and server.sessionCount() > 0) : (budget -= 1) {
            _ = server.stepOnce(.wait) catch {};
            self.sweepDeadPeers();
        }
        var it = self.bound.valueIterator();
        while (it.next()) |peer_ptr| {
            const peer = peer_ptr.*;
            _ = peer.takeAttachedConnection(*quic.ServerSession);
            peer.deinit();
            self.allocator.destroy(peer);
        }
        self.bound.clearRetainingCapacity();
        // Loop-thread-only accessors; snapshot before the loop thread exits.
        // ACCUMULATE: abrupt-death incarnations already banked their counts.
        self.resets_sent += server.statelessResetsSent();
        for (server.feedOutcomeCounts(), 0..) |c, i| self.feed_counts[i] += c;
    }

    fn bindNewSessions(self: *QuicServerHarness) void {
        if (comptime !quic.enabled) unreachable;
        const server = self.server.?;
        var i: usize = 0;
        while (i < server.sessionCount()) : (i += 1) {
            const sess = server.sessionAt(i) orelse continue;
            if (self.bound.contains(sess.id)) continue;
            const peer = self.allocator.create(Peer) catch continue;
            peer.* = Peer.init(self.allocator, sess);
            _ = peer.setBootstrap(.{
                .ctx = @ptrCast(&EchoServer.ctx_anchor),
                .on_call = EchoServer.onCall,
            }) catch {
                _ = peer.takeAttachedConnection(*quic.ServerSession);
                peer.deinit();
                self.allocator.destroy(peer);
                continue;
            };
            peer.setRestorer(@ptrCast(&EchoServer.ctx_anchor), EchoServer.onRestore) catch {
                _ = peer.takeAttachedConnection(*quic.ServerSession);
                peer.deinit();
                self.allocator.destroy(peer);
                continue;
            };
            peer.start(null, null, null);
            self.bound.put(sess.id, peer) catch {
                _ = peer.takeAttachedConnection(*quic.ServerSession);
                peer.deinit();
                self.allocator.destroy(peer);
            };
        }
    }

    fn sweepDeadPeers(self: *QuicServerHarness) void {
        if (comptime !quic.enabled) unreachable;
        const server = self.server.?;
        // Bounded batch per pass; stragglers get the next pass.
        var dead: [64]u64 = undefined;
        var n: usize = 0;
        var it = self.bound.keyIterator();
        while (it.next()) |id_ptr| {
            if (server.sessionById(id_ptr.*) != null) continue;
            if (n == dead.len) break;
            dead[n] = id_ptr.*;
            n += 1;
        }
        for (dead[0..n]) |id| {
            const entry = self.bound.fetchRemove(id) orelse continue;
            const peer = entry.value;
            // The server already destroyed the session at reap time (the
            // Peer's close callback fired then); detach so peer teardown
            // never touches the dead pointer.
            _ = peer.takeAttachedConnection(*quic.ServerSession);
            peer.deinit();
            self.allocator.destroy(peer);
        }
    }
};

// -- Memory sampler ----------------------------------------------------------

const MemSampler = struct {
    counter: *const CountingAllocator,
    io: std.Io,
    interval_ms: u64,
    stop_at_ns: i64,
    stop_flag: *std.atomic.Value(bool),
    // Sample series (live heap bytes; process RSS bytes) written by the
    // sampler, read after join. Both are preallocated for the whole run so
    // appending never moves them mid-run.
    samples: *std.ArrayList(u64),
    rss_samples: *std.ArrayList(u64),
    samples_allocator: std.mem.Allocator,
    rss_injector: ?*RssInjector,

    fn main(self: MemSampler) void {
        while (!self.stop_flag.load(.acquire) and nowNs(self.io) < self.stop_at_ns) {
            if (self.rss_injector) |injector| injector.advance(self.io);
            const live = self.counter.liveBytes();
            self.samples.append(self.samples_allocator, live) catch {};
            if (processRssBytes(self.io)) |rss| {
                self.rss_samples.append(self.samples_allocator, rss) catch {};
            }
            sleepMs(self.io, self.interval_ms);
        }
    }
};

/// Samples the sampler can take in a run, with headroom: it runs from
/// before the workers start until a second past their stop.
fn expectedSampleCount(seconds: u64, interval_ms: u64) usize {
    return @intCast(((seconds + 2) * std.time.ms_per_s) / interval_ms + 16);
}

// -- Process resident set ----------------------------------------------------

/// Current resident set size of this process in bytes, or null where the
/// platform has no reading (the caller reports that as UNAVAILABLE rather
/// than passing silently). This is the instrument that sees memory the
/// counting allocator cannot: C allocations (BoringSSL under QUIC),
/// allocator caches, and page-level growth.
fn processRssBytes(io: std.Io) ?u64 {
    switch (builtin.os.tag) {
        .linux => {
            // statm: size resident shared text lib data dt, in pages.
            var buf: [256]u8 = undefined;
            const text = std.Io.Dir.cwd().readFile(io, "/proc/self/statm", &buf) catch return null;
            var fields = std.mem.tokenizeAny(u8, text, " \n");
            _ = fields.next() orelse return null;
            const resident_pages = std.fmt.parseUnsigned(u64, fields.next() orelse return null, 10) catch return null;
            return resident_pages * std.heap.pageSize();
        },
        .windows => {
            var counters: WinProcessMemoryCounters = undefined;
            counters.cb = @sizeOf(WinProcessMemoryCounters);
            if (K32GetProcessMemoryInfo(std.os.windows.GetCurrentProcess(), &counters, counters.cb) == 0) return null;
            return counters.WorkingSetSize;
        },
        else => {
            if (comptime builtin.os.tag.isDarwin()) {
                var info: std.c.mach_task_basic_info = undefined;
                var count: std.c.mach_msg_type_number_t = std.c.MACH.TASK.BASIC.INFO_COUNT;
                const kr = std.c.task_info(std.c.mach_task_self(), std.c.MACH.TASK.BASIC.INFO, @ptrCast(&info), &count);
                if (kr != 0) return null;
                return info.resident_size;
            }
            return null;
        },
    }
}

/// psapi's PROCESS_MEMORY_COUNTERS. GetProcessMemoryInfo is exported from
/// kernel32 as K32GetProcessMemoryInfo since Windows 7, so no psapi link.
const WinProcessMemoryCounters = extern struct {
    cb: u32,
    PageFaultCount: u32,
    PeakWorkingSetSize: usize,
    WorkingSetSize: usize,
    QuotaPeakPagedPoolUsage: usize,
    QuotaPagedPoolUsage: usize,
    QuotaPeakNonPagedPoolUsage: usize,
    QuotaNonPagedPoolUsage: usize,
    PagefileUsage: usize,
    PeakPagefileUsage: usize,
};

extern "kernel32" fn K32GetProcessMemoryInfo(
    process: std.os.windows.HANDLE,
    counters: *WinProcessMemoryCounters,
    cb: u32,
) callconv(.winapi) c_int;

test "processRssBytes: reads a plausible resident set on supported hosts" {
    const supported = switch (builtin.os.tag) {
        .linux, .windows => true,
        else => builtin.os.tag.isDarwin(),
    };
    const rss = processRssBytes(std.testing.io);
    if (!supported) return error.SkipZigTest;
    // A running test binary is resident: more than a page, less than 64 GiB.
    try std.testing.expect(rss != null);
    try std.testing.expect(rss.? > std.heap.pageSize());
    try std.testing.expect(rss.? < 64 * 1024 * 1024 * 1024);
}

/// Ablation hook (--inject-rss-growth-kib-per-s): grows the resident set at
/// a fixed rate WITHOUT any allocator call the heap gate can see. One page
/// allocator reservation covers the whole run up front (untouched pages are
/// not resident); each sampler tick touches the next slice. Freed after the
/// verdicts, so the terminal leak check is unaffected.
const RssInjector = struct {
    region: []u8,
    touched: usize = 0,
    start_ns: i64,
    bytes_per_s: u64,

    fn init(seconds: u64, kib_per_s: u64, start_ns: i64) !RssInjector {
        const bytes_per_s = kib_per_s * 1024;
        const len: usize = @intCast(@max(bytes_per_s * (seconds + 2), 1));
        // rawAlloc, not alloc: Allocator.alloc fills the slice with the
        // `undefined` pattern in safe builds, which would touch (and make
        // resident) the whole region before the run starts.
        const ptr = std.heap.page_allocator.rawAlloc(len, .fromByteUnits(std.heap.pageSize()), @returnAddress()) orelse
            return error.OutOfMemory;
        return .{
            .region = ptr[0..len],
            .start_ns = start_ns,
            .bytes_per_s = bytes_per_s,
        };
    }

    fn advance(self: *RssInjector, io: std.Io) void {
        const elapsed_ns: u64 = @intCast(@max(0, nowNs(io) - self.start_ns));
        const want: u64 = @min(self.region.len, (elapsed_ns / std.time.ns_per_ms) * self.bytes_per_s / std.time.ms_per_s);
        const target: usize = @intCast(want);
        if (target <= self.touched) return;
        @memset(self.region[self.touched..target], 0xA5);
        self.touched = target;
    }

    fn deinit(self: *RssInjector) void {
        std.heap.page_allocator.rawFree(self.region, .fromByteUnits(std.heap.pageSize()), @returnAddress());
    }
};

/// Programmatic flat-memory check: split the steady-state window (samples
/// after an initial warmup ramp) into a head and tail quarter and compare
/// their means. Growth beyond the configured percentage (with an absolute
/// floor so small series do not trip on noise) fails the run. This is a
/// slope check over the whole run, not a final leak snapshot. Shared by the
/// heap gate and the RSS gate; only the thresholds differ.
const MemVerdict = struct {
    ok: bool,
    head_mean: f64,
    tail_mean: f64,
    growth_pct: f64,
    peak: u64,
    warmup_samples: usize,
};

fn meanOf(slice: []const u64) f64 {
    if (slice.len == 0) return 0;
    var sum: u128 = 0;
    for (slice) |v| sum += v;
    return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(slice.len));
}

/// Heap gate floor: ignore live-heap growth under 256 KiB. That is
/// churn/fragmentation noise, not a leak, and small enough not to matter
/// over a bounded soak.
const heap_abs_floor_bytes: f64 = 256 * 1024;

fn assessMemory(samples: []const u64, growth_pct: f64, abs_floor_bytes: f64) MemVerdict {
    var peak: u64 = 0;
    for (samples) |v| peak = @max(peak, v);

    // Drop the first ~20% as warmup ramp (connection pools, arenas priming).
    const warmup = samples.len / 5;
    const steady = samples[@min(warmup, samples.len)..];
    if (steady.len < 4) {
        // Not enough steady-state signal to judge a slope; treat as pass but
        // report what we have. (Short runs still get the final leak check.)
        return .{
            .ok = true,
            .head_mean = meanOf(steady),
            .tail_mean = meanOf(steady),
            .growth_pct = 0,
            .peak = peak,
            .warmup_samples = warmup,
        };
    }

    const q = steady.len / 4;
    const head = steady[0..@max(q, 1)];
    const tail = steady[steady.len - @max(q, 1) ..];
    const head_mean = meanOf(head);
    const tail_mean = meanOf(tail);

    const growth = tail_mean - head_mean;
    const rel_pct = if (head_mean > 0) (growth / head_mean) * 100.0 else 0;

    // Absolute floor first: growth under it is never a finding, however
    // large relative to a small series.
    const ok = (growth <= abs_floor_bytes) or (rel_pct <= growth_pct);

    return .{
        .ok = ok,
        .head_mean = head_mean,
        .tail_mean = tail_mean,
        .growth_pct = rel_pct,
        .peak = peak,
        .warmup_samples = warmup,
    };
}

test "assessMemory: a flat steady-state series passes" {
    // ~1 MiB heap wobbling within a few percent — allocator noise, not a leak.
    var series: [40]u64 = undefined;
    for (&series, 0..) |*v, i| v.* = 1_000_000 + (i % 5) * 8_000;
    const verdict = assessMemory(&series, 25.0, heap_abs_floor_bytes);
    try std.testing.expect(verdict.ok);
}

test "assessMemory: a monotonically growing series above the floor fails" {
    // Steady-state climb from ~1 MiB to ~5 MiB — a genuine leak/growth trend
    // well past the 256 KiB absolute floor and the 25% relative threshold.
    var series: [40]u64 = undefined;
    for (&series, 0..) |*v, i| v.* = 1_000_000 + i * 100_000;
    const verdict = assessMemory(&series, 25.0, heap_abs_floor_bytes);
    try std.testing.expect(!verdict.ok);
    try std.testing.expect(verdict.growth_pct > 25.0);
}

test "assessMemory: sub-floor growth on a tiny heap is tolerated" {
    // Grows 100% relatively, but only a handful of KiB — under the absolute
    // floor, so churn on a tiny heap must not trip the gate.
    var series: [40]u64 = undefined;
    for (&series, 0..) |*v, i| v.* = 4_096 + i * 128;
    const verdict = assessMemory(&series, 25.0, heap_abs_floor_bytes);
    try std.testing.expect(verdict.ok);
}

test "assessMemory: RSS thresholds see a steady C-side climb the heap never shows" {
    const floor: f64 = 16 * 1024 * 1024;
    // 60 MiB resident, climbing ~1 MiB per sample: the shape of a C heap
    // leaking under handshake churn. Far past the 16 MiB floor and 25%.
    var leaking: [100]u64 = undefined;
    for (&leaking, 0..) |*v, i| v.* = 60 * 1024 * 1024 + i * 1024 * 1024;
    try std.testing.expect(!assessMemory(&leaking, 25.0, floor).ok);
    // The same baseline wobbling by a few MiB (allocator caches settling)
    // stays under the RSS floor even though it would trip the heap floor.
    var settling: [100]u64 = undefined;
    for (&settling, 0..) |*v, i| v.* = 60 * 1024 * 1024 + (i % 7) * 512 * 1024 + i * 32 * 1024;
    try std.testing.expect(assessMemory(&settling, 25.0, floor).ok);
    try std.testing.expect(!assessMemory(&settling, 0.0, heap_abs_floor_bytes).ok);
}

// -- Helpers -----------------------------------------------------------------

/// A finding that is classified and reported but does not fail the run: a
/// `soak: WARN` line, plus a `::warning::` annotation under GitHub Actions
/// so a passing lane cannot hide it in its log.
fn warn(annotate: bool, comptime fmt: []const u8, args: anytype) void {
    std.debug.print("soak: WARN — " ++ fmt ++ "\n", args);
    if (annotate) std.debug.print("::warning title=soak::" ++ fmt ++ "\n", args);
}

fn sleepMs(io: std.Io, ms: u64) void {
    const duration: std.Io.Clock.Duration = .{
        .raw = .{ .nanoseconds = @as(i96, @intCast(ms)) * std.time.ns_per_ms },
        .clock = .awake,
    };
    duration.sleep(io) catch {};
}

fn parseArgs(allocator: std.mem.Allocator, args: std.process.Args) !Config {
    var cfg = Config{};
    var iter = try std.process.Args.Iterator.initAllocator(args, allocator);
    defer iter.deinit();
    _ = iter.skip();
    while (iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--seconds")) {
            cfg.seconds = try std.fmt.parseUnsigned(u64, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--workers")) {
            cfg.workers = try std.fmt.parseUnsigned(u32, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--calls")) {
            cfg.calls_per_session = try std.fmt.parseUnsigned(u32, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--inflight")) {
            cfg.inflight = try std.fmt.parseUnsigned(u32, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--mem-sample-ms")) {
            cfg.mem_sample_ms = try std.fmt.parseUnsigned(u64, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--mem-growth-pct")) {
            cfg.mem_growth_pct = try std.fmt.parseFloat(f64, iter.next() orelse return error.InvalidArgument);
        } else if (std.mem.eql(u8, arg, "--abrupt-death-every-ms")) {
            cfg.abrupt_death_every_ms = try std.fmt.parseUnsigned(u64, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--heal-workers")) {
            cfg.heal_workers = try std.fmt.parseUnsigned(u32, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--no-chaos")) {
            cfg.chaos = false;
        } else if (std.mem.eql(u8, arg, "--no-deadlines")) {
            cfg.deadlines = false;
        } else if (std.mem.eql(u8, arg, "--transport")) {
            const value = iter.next() orelse return error.InvalidArgument;
            cfg.transport = std.meta.stringToEnum(Transport, value) orelse
                return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--cc")) {
            const value = iter.next() orelse return error.InvalidArgument;
            cfg.cc = std.meta.stringToEnum(CcChoice, value) orelse
                return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--nagle")) {
            cfg.nodelay = false;
        } else if (std.mem.eql(u8, arg, "--alloc-traces")) {
            // Consumed by main before parsing (it picks the allocator type).
            cfg.alloc_traces = true;
        } else if (std.mem.eql(u8, arg, "--transport-error-tolerance")) {
            cfg.transport_error_tolerance = try std.fmt.parseUnsigned(usize, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--rss-gate")) {
            const value = iter.next() orelse return error.InvalidArgument;
            cfg.rss_gate = std.meta.stringToEnum(RssGate, value) orelse
                return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--rss-growth-pct")) {
            cfg.rss_growth_pct = try std.fmt.parseFloat(f64, iter.next() orelse return error.InvalidArgument);
        } else if (std.mem.eql(u8, arg, "--rss-floor-mib")) {
            cfg.rss_floor_mib = try std.fmt.parseUnsigned(u64, iter.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--inject-transport-error-every")) {
            cfg.inject_transport_error_every = try std.fmt.parseUnsigned(u64, iter.next() orelse return error.InvalidArgument, 10);
            if (cfg.inject_transport_error_every.? == 0) return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--inject-rss-growth-kib-per-s")) {
            cfg.inject_rss_growth_kib_per_s = try std.fmt.parseUnsigned(u64, iter.next() orelse return error.InvalidArgument, 10);
        } else {
            std.debug.print("soak: unknown argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.workers == 0 or cfg.calls_per_session == 0 or cfg.inflight == 0) return error.InvalidArgument;
    if (cfg.transport == .quic and !quic.enabled) {
        std.debug.print("soak: --transport quic requires a -Dquic=true build\n", .{});
        return error.InvalidArgument;
    }
    if (cfg.mem_sample_ms == 0) return error.InvalidArgument;
    if (cfg.abrupt_death_every_ms) |ms| {
        // TCP sessions all close `.unknown` and carry no reset-token proof,
        // so the mode would measure nothing there.
        if (cfg.transport != .quic) {
            std.debug.print("soak: --abrupt-death-every-ms requires --transport quic\n", .{});
            return error.InvalidArgument;
        }
        if (ms == 0) return error.InvalidArgument;
    }
    if (cfg.heal_workers > 0) {
        if (cfg.transport != .quic) {
            std.debug.print("soak: --heal-workers requires --transport quic\n", .{});
            return error.InvalidArgument;
        }
        if (cfg.heal_workers > cfg.workers) {
            std.debug.print("soak: --heal-workers must not exceed --workers\n", .{});
            return error.InvalidArgument;
        }
    }
    return cfg;
}

/// The soak's DebugAllocator. Allocation stack traces are OFF by default,
/// even in Debug, where std would capture 6 frames per allocation and per
/// free. Measured on 0.17.0 (TCP, 8 workers, live heap flat at ~840 KB in
/// every case): with traces on, process RSS climbed linearly — ~4.7 MB/s on
/// Linux (6.6 MB -> 147 MB in 30 s) and ~11 MB/s on macOS (4.7 MB -> 267 MB
/// in 20 s) — and macOS Debug call latency rose from p50 0.33 ms to 6 ms.
/// With traces off, RSS held flat at ~9.8 MB. That growth belongs to the
/// tracer, not the code under test, and it would make every Debug lane's
/// RSS verdict a false FAIL. The leak check at exit is unaffected; pass
/// `--alloc-traces` to get allocation sites in a leak report (and expect
/// the RSS verdict to be meaningless while they are on).
fn SoakGpa(comptime alloc_traces: bool) type {
    return std.heap.DebugAllocator(.{
        .thread_safe = true,
        .stack_trace_frames = if (alloc_traces) traced_alloc_frames else 0,
    });
}

const traced_alloc_frames: usize = if (std.debug.sys_can_stack_trace) 6 else 0;

pub fn main(init: std.process.Init) !void {
    // The allocator type is comptime, so peek for its one flag before
    // anything is allocated from it.
    if (wantsAllocTraces(init)) return run(SoakGpa(true), init);
    return run(SoakGpa(false), init);
}

fn wantsAllocTraces(init: std.process.Init) bool {
    var iter = std.process.Args.Iterator.initAllocator(init.minimal.args, init.arena.allocator()) catch return false;
    defer iter.deinit();
    while (iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--alloc-traces")) return true;
    }
    return false;
}

fn run(comptime Gpa: type, init: std.process.Init) !void {
    var gpa: Gpa = .init;
    var counter = CountingAllocator.init(gpa.allocator());
    const allocator = counter.allocator();
    // Harness telemetry (latency histograms, the memory curves themselves)
    // goes straight to the DebugAllocator, BYPASSING the counter: the
    // steady-state memory gate must measure the system under test, not
    // the instrument. Still leak-checked by the terminal gpa.deinit. All
    // of it is sized up front (see LatencyHist), so it is flat in RSS too.
    const telemetry_allocator = gpa.allocator();
    const io = init.io;

    const cfg = try parseArgs(allocator, init.minimal.args);
    // Under GitHub Actions, report-only findings are also emitted as
    // `::warning::` annotations so they surface on the run summary instead
    // of hiding in a passing step's log.
    const annotate = if (init.environ_map.get("GITHUB_ACTIONS")) |v| std.mem.eql(u8, v, "true") else false;

    var totals = Totals{};
    EchoServer.server_io = io;

    // Per-worker latency histograms (merged after join). Initializing them
    // here touches every page before the sampler takes its first reading.
    const latency_hists = try telemetry_allocator.alloc(LatencyHist, cfg.workers);
    for (latency_hists) |*h| h.* = .{};

    var pool: WorkerPool = undefined;
    var pool_runner: PoolRunner = undefined;
    var pool_thread: std.Thread = undefined;
    var quic_srv = QuicServerHarness{};
    var address: net.IpAddress = undefined;
    switch (cfg.transport) {
        .tcp => {
            pool = try WorkerPool.init(
                allocator,
                io,
                .{ .ip4 = .loopback(0) },
                @ptrCast(&totals),
                EchoServer.onAccept,
                .{ .concurrency = @max(2, cfg.workers / 2) },
            );
            // `listen()` publishes the kernel-selected ephemeral port in its
            // portable address value. Reading that value avoids POSIX
            // `getsockname` and keeps the real soak path buildable under
            // Windows' native socket handle type.
            const port = pool.server.socket.address.getPort();
            address = .{ .ip4 = .loopback(port) };
            pool_runner = .{ .pool = &pool, .io = io };
            pool_thread = try std.Thread.spawn(.{}, PoolRunner.threadMain, .{&pool_runner});
            std.debug.print(
                "soak: tcp server listening on port {} (workers {}, inflight {}, pool concurrency {})\n",
                .{ port, cfg.workers, cfg.inflight, @max(2, cfg.workers / 2) },
            );
        },
        .quic => {
            // parseArgs already rejected .quic on a non-QUIC build, so this
            // branch is only reachable when the harness compiles for real.
            if (comptime quic.enabled) {
                address = try quic_srv.start(allocator, io, cfg.workers, cfg.cc, cfg.abrupt_death_every_ms);
                std.debug.print(
                    "soak: quic server listening on port {} (workers {}, inflight {})\n",
                    .{ address.getPort(), cfg.workers, cfg.inflight },
                );
            } else unreachable;
        },
    }

    totals.start_ns = nowNs(io);
    const stop_at_ns = totals.start_ns + @as(i64, @intCast(cfg.seconds)) * std.time.ns_per_s;

    // Memory sampler thread: watches live heap and process RSS for the run
    // duration. Both series are sized for the whole run before it starts.
    const sample_capacity = expectedSampleCount(cfg.seconds, cfg.mem_sample_ms);
    var mem_samples: std.ArrayList(u64) = .empty;
    defer mem_samples.deinit(telemetry_allocator);
    try mem_samples.ensureTotalCapacity(telemetry_allocator, sample_capacity);
    var rss_samples: std.ArrayList(u64) = .empty;
    defer rss_samples.deinit(telemetry_allocator);
    try rss_samples.ensureTotalCapacity(telemetry_allocator, sample_capacity);
    var rss_injector: ?RssInjector = if (cfg.inject_rss_growth_kib_per_s) |kib|
        try RssInjector.init(cfg.seconds, kib, totals.start_ns)
    else
        null;
    defer if (rss_injector) |*injector| injector.deinit();
    var mem_stop = std.atomic.Value(bool).init(false);
    const mem_thread = try std.Thread.spawn(.{}, MemSampler.main, .{MemSampler{
        .counter = &counter,
        .io = io,
        .interval_ms = cfg.mem_sample_ms,
        .stop_at_ns = stop_at_ns + std.time.ns_per_s, // sample a bit past worker stop
        .stop_flag = &mem_stop,
        .samples = &mem_samples,
        .rss_samples = &rss_samples,
        .samples_allocator = telemetry_allocator,
        .rss_injector = if (rss_injector) |*injector| injector else null,
    }});

    // Healing clients (QUIC only; parseArgs enforced the pairing): stable
    // storage created before their threads spawn, stopped by this thread
    // once the run window ends.
    const heal_count: u32 = cfg.heal_workers;
    var heal_apps: if (quic.enabled) []HealApp else void =
        if (comptime quic.enabled) try allocator.alloc(HealApp, heal_count) else {};
    var heal_clients: if (quic.enabled) []quic.WarmRedialClient else void =
        if (comptime quic.enabled) try allocator.alloc(quic.WarmRedialClient, heal_count) else {};
    if (comptime quic.enabled) {
        for (heal_apps) |*app| app.* = .{};
        for (heal_clients, 0..) |*client, i| {
            var base = quic.ClientOptions{
                .remote_addr = address,
                .server_name = "localhost",
                .insecure_skip_verify = true,
                .receive_timeout = std.Io.Duration.fromMilliseconds(5),
            };
            applyCc(&base.congestion_control, cfg.cc);
            client.* = try quic.WarmRedialClient.init(
                allocator,
                io,
                base,
                EchoServer.sturdy_ref,
                // Effectively unbounded within the run; the run window is
                // the real budget.
                .{ .max_redials = std.math.maxInt(u32), .backoff_ms = 25 },
                &heal_apps[i],
                HealApp.onRebind,
                HealApp.onGiveUp,
            );
        }
    }

    const worker_threads = try allocator.alloc(std.Thread, cfg.workers);
    for (worker_threads, 0..) |*t, i| {
        if (comptime quic.enabled) {
            if (cfg.transport == .quic and i < heal_count) {
                t.* = try std.Thread.spawn(.{}, HealApp.threadMain, .{ &heal_apps[i], &heal_clients[i] });
                continue;
            }
        }
        t.* = switch (cfg.transport) {
            .tcp => try std.Thread.spawn(.{}, Worker.main, .{Worker{
                .allocator = allocator,
                .io = io,
                .address = address,
                .cfg = &cfg,
                .totals = &totals,
                .latency = &latency_hists[i],
                .stop_at_ns = stop_at_ns,
                .index = @intCast(i),
            }}),
            .quic => if (comptime quic.enabled) try std.Thread.spawn(.{}, QuicWorker.main, .{QuicWorker{
                .allocator = allocator,
                .io = io,
                .address = address,
                .cfg = &cfg,
                .totals = &totals,
                .latency = &latency_hists[i],
                .stop_at_ns = stop_at_ns,
                .index = @intCast(i),
            }}) else unreachable,
        };
    }
    // Churn workers self-terminate at the window's end; healing clients run
    // until told to stop. Join churn first, then stop and join the healers
    // (when every worker heals, wait out the window explicitly).
    for (worker_threads[heal_count..]) |t| t.join();
    if (heal_count == cfg.workers) {
        while (nowNs(io) < stop_at_ns) sleepMs(io, 50);
    }
    var heal_rebinds: usize = 0;
    var heal_redials: usize = 0;
    var heal_give_ups: usize = 0;
    var heal_echo: usize = 0;
    var heal_min_rebinds: usize = std.math.maxInt(usize);
    if (comptime quic.enabled) {
        for (heal_clients) |*client| client.requestStop();
        for (worker_threads[0..heal_count]) |t| t.join();
        for (heal_apps, heal_clients) |*app, *client| {
            if (app.outcome) |o| {
                heal_rebinds += o.rebinds;
                heal_redials += o.redials;
                heal_min_rebinds = @min(heal_min_rebinds, o.rebinds);
            } else {
                heal_min_rebinds = 0;
            }
            if (app.gave_up.load(.acquire)) heal_give_ups += 1;
            heal_echo += app.echo_ok.load(.acquire);
            client.deinit();
        }
        allocator.free(heal_clients);
        allocator.free(heal_apps);
    }
    allocator.free(worker_threads);
    std.debug.print("soak: workers joined, draining server\n", .{});

    mem_stop.store(true, .release);
    mem_thread.join();

    switch (cfg.transport) {
        .tcp => {
            pool_runner.stop_requested.store(true, .release);
            pool.shutdownGraceful(2_000);
            std.debug.print("soak: pool drained, joining pool thread\n", .{});
            pool_thread.join();
            pool.deinit();
            std.debug.print("soak: pool deinit complete\n", .{});
        },
        .quic => {
            if (comptime quic.enabled) {
                quic_srv.shutdown();
                std.debug.print("soak: quic server drained\n", .{});
                std.debug.print(
                    "soak-quic: resets_sent={} unroutable_dcid={} (reset_queued={}) abrupt_deaths={}\n",
                    .{ quic_srv.resets_sent, quic_srv.unroutable_seen.load(.acquire), quic_srv.unroutable_reset_queued.load(.acquire), quic_srv.deaths },
                );
                std.debug.print("soak-quic: feed outcomes:", .{});
                inline for (comptime std.enums.values(quic.listener.FeedOutcome), 0..) |tag, i| {
                    std.debug.print(" {s}={}", .{ @tagName(tag), quic_srv.feed_counts[i] });
                }
                std.debug.print("\n", .{});
                if (cfg.heal_workers > 0) {
                    std.debug.print(
                        "soak-heal: clients={} rebinds={} redials={} give_ups={} echo_ok={} min_rebinds={}\n",
                        .{ cfg.heal_workers, heal_rebinds, heal_redials, heal_give_ups, heal_echo, heal_min_rebinds },
                    );
                }
                // Client handshakes started: one per churn session (setup
                // failures never got that far), plus each healing client's
                // first dial and redials. The per-handshake C-side cost is
                // what the RSS gate exists to watch across a quic bump.
                const handshakes = totals.sessions.load(.acquire) + heal_redials + cfg.heal_workers;
                std.debug.print("soak-quic: client handshakes~{} ({d:.1}/s)\n", .{
                    handshakes,
                    @as(f64, @floatFromInt(handshakes)) / @as(f64, @floatFromInt(@max(cfg.seconds, 1))),
                });
            } else unreachable;
        },
    }

    // -- Merge latency histograms & compute percentiles -------------------
    var lat: LatencyHist = .{};
    for (latency_hists) |*h| lat.merge(h);
    telemetry_allocator.free(latency_hists);

    const sessions = totals.sessions.load(.acquire);
    const ok = totals.calls_ok.load(.acquire);
    const cancelled = totals.calls_cancelled.load(.acquire);
    const chaos_closes = totals.chaos_closes.load(.acquire);
    const transport_errors = totals.transport_errors.load(.acquire);
    const expected_disconnects = totals.expected_disconnects.load(.acquire);
    const contention_disconnects = totals.contention_disconnects.load(.acquire);
    const unexpected = totals.unexpected_exceptions.load(.acquire);
    var setup_counts: [setup_class_count]usize = undefined;
    var setup_total: usize = 0;
    for (&setup_counts, &totals.setup_failures) |*c, *a| {
        c.* = a.load(.acquire);
        setup_total += c.*;
    }

    std.debug.print(
        "soak: sessions={} calls_ok={} cancelled={} chaos_closes={} transport_errors={} expected_disconnects={} contention_disconnects={} unexpected_exceptions={}\n",
        .{ sessions, ok, cancelled, chaos_closes, transport_errors, expected_disconnects, contention_disconnects, unexpected },
    );
    std.debug.print("soak: setup failures={} (", .{setup_total});
    inline for (comptime std.enums.values(SetupClass), 0..) |class, i| {
        std.debug.print("{s}{s}={}", .{ if (i == 0) "" else " ", @tagName(class), setup_counts[i] });
    }
    std.debug.print(")\n", .{});
    std.debug.print("soak: session close causes:", .{});
    inline for (comptime std.enums.values(rpc.events.DisconnectCause), 0..) |cause, i| {
        std.debug.print(" {s}={}", .{ @tagName(cause), totals.disconnects_by_cause[i].load(.acquire) });
    }
    std.debug.print("\n", .{});
    std.debug.print(
        "soak: latency p50={}ns p99={}ns max={}ns (samples={}, client TCP_NODELAY={s})\n",
        .{
            lat.percentile(50.0),
            lat.percentile(99.0),
            lat.max,
            lat.count,
            if (cfg.transport != .tcp) "n/a" else if (!cfg.nodelay) "off (--nagle)" else if (builtin.os.tag == .windows) "unavailable (std AFD)" else "on",
        },
    );

    // -- Memory-growth curves + flat assessments ---------------------------
    const verdict = assessMemory(mem_samples.items, cfg.mem_growth_pct, heap_abs_floor_bytes);
    std.debug.print("soak: memory curve (live bytes, {} samples @ {}ms):\n", .{ mem_samples.items.len, cfg.mem_sample_ms });
    printMemCurve(mem_samples.items);
    std.debug.print(
        "soak: memory steady-state head_mean={d:.0}B tail_mean={d:.0}B growth={d:.2}% peak={}B threshold={d:.1}%\n",
        .{ verdict.head_mean, verdict.tail_mean, verdict.growth_pct, verdict.peak, cfg.mem_growth_pct },
    );

    const rss_floor_bytes: f64 = @floatFromInt(cfg.rss_floor_mib * 1024 * 1024);
    const rss_available = rss_samples.items.len > 0;
    const rss_verdict = assessMemory(rss_samples.items, cfg.rss_growth_pct, rss_floor_bytes);
    const rss_mode = @tagName(cfg.rss_gate);
    if (rss_available) {
        std.debug.print("soak: rss curve (resident bytes, {} samples @ {}ms):\n", .{ rss_samples.items.len, cfg.mem_sample_ms });
        printMemCurve(rss_samples.items);
        std.debug.print(
            "soak: rss steady-state head_mean={d:.0}B tail_mean={d:.0}B growth={d:.2}% delta={d:.0}B peak={}B threshold={d:.1}% floor={}MiB\n",
            .{ rss_verdict.head_mean, rss_verdict.tail_mean, rss_verdict.growth_pct, rss_verdict.tail_mean - rss_verdict.head_mean, rss_verdict.peak, cfg.rss_growth_pct, cfg.rss_floor_mib },
        );
        std.debug.print("soak: rss verdict: {s} ({s}{s})\n", .{
            if (rss_verdict.ok) "PASS" else "FAIL",
            rss_mode,
            if (cfg.alloc_traces) "; --alloc-traces on: RSS includes the tracer's own growth" else "",
        });
    } else {
        std.debug.print("soak: rss verdict: UNAVAILABLE on {s} ({s})\n", .{ @tagName(builtin.os.tag), rss_mode });
    }
    if (rss_injector) |*injector| {
        std.debug.print("soak: ablation: injected {} bytes of RSS growth outside the Zig heap\n", .{injector.touched});
    }

    var failed = false;
    if (sessions == 0 or ok == 0) {
        std.debug.print("soak: FAIL — no successful traffic\n", .{});
        failed = true;
    }
    if (cfg.chaos and chaos_closes == 0) {
        std.debug.print("soak: FAIL — chaos mode produced no transport closes\n", .{});
        failed = true;
    }
    if (unexpected != 0) {
        std.debug.print("soak: FAIL — unexpected exception reasons observed\n", .{});
        failed = true;
    }
    if (expected_disconnects > chaos_closes) {
        std.debug.print("soak: FAIL — more disconnected exceptions than chaos closes\n", .{});
        failed = true;
    }
    if (cfg.deadlines and cancelled == 0) {
        std.debug.print("soak: FAIL — deadline sessions produced no cancellations\n", .{});
        failed = true;
    }
    if (cfg.abrupt_death_every_ms != null) {
        if (comptime quic.enabled) {
            const reset_closes = totals.disconnects_by_cause[causeSlot(.stateless_reset)].load(.acquire);
            if (quic_srv.rebind_failed.load(.acquire)) {
                std.debug.print("soak: FAIL — abrupt-death rebind race exhausted its retries\n", .{});
                failed = true;
            }
            if (quic_srv.deaths == 0) {
                std.debug.print("soak: FAIL — abrupt-death mode executed no deaths\n", .{});
                failed = true;
            }
            if (quic_srv.deaths > 0 and reset_closes == 0) {
                std.debug.print("soak: FAIL — abrupt deaths produced no stateless_reset-certified session closes\n", .{});
                failed = true;
            }
            if (quic_srv.deaths > 0 and quic_srv.resets_sent == 0 and quic_srv.unroutable_reset_queued.load(.acquire) == 0) {
                std.debug.print("soak: FAIL — abrupt deaths produced no reset traffic at the server\n", .{});
                failed = true;
            }
        } else unreachable;
    }
    if (cfg.heal_workers > 0) {
        if (comptime quic.enabled) {
            if (heal_give_ups != 0) {
                std.debug.print("soak: FAIL — {} healing client(s) gave up\n", .{heal_give_ups});
                failed = true;
            }
            if (heal_echo == 0) {
                std.debug.print("soak: FAIL — healing clients completed no echo round trips\n", .{});
                failed = true;
            }
            if (cfg.abrupt_death_every_ms != null and quic_srv.deaths > 0 and heal_min_rebinds < 2) {
                std.debug.print("soak: FAIL — a healing client never healed across an abrupt death (min_rebinds={})\n", .{heal_min_rebinds});
                failed = true;
            }
        } else unreachable;
    }
    // Mid-session transport errors: bounded (see the file header).
    const churn_workers: usize = cfg.workers - cfg.heal_workers;
    const deaths: usize = @intCast(quic_srv.deaths);
    const death_allowance = deaths * churn_workers;
    const tolerance = cfg.transport_error_tolerance orelse defaultTransportTolerance(sessions);
    const transport_verdict = assessTransport(transport_errors, chaos_closes, death_allowance, tolerance);
    std.debug.print(
        "soak: transport-error bound: transport_errors={} allowed={} (chaos_closes={} + death_allowance={} + tolerance={}) -> {s}\n",
        .{ transport_errors, transport_verdict.allowed, chaos_closes, death_allowance, tolerance, if (transport_verdict.ok) "ok" else "EXCEEDED" },
    );
    const injected = totals.injected_transport_errors.load(.acquire);
    if (injected > 0) {
        std.debug.print("soak: ablation: {} of those transport errors were injected (--inject-transport-error-every)\n", .{injected});
    }
    if (!transport_verdict.ok) {
        std.debug.print(
            "soak: FAIL — {} mid-session transport errors exceed the bound of {}\n",
            .{ transport_errors, transport_verdict.allowed },
        );
        failed = true;
    }
    // The TCP server under test must serve the whole run.
    if (cfg.transport == .tcp and !pool_runner.servedWholeRun()) {
        if (pool_runner.run_error) |err| {
            std.debug.print("soak: FAIL — the server under test (WorkerPool.run) failed: {}\n", .{err});
        } else {
            const exit_ms = @divTrunc(pool_runner.exit_ns - totals.start_ns, std.time.ns_per_ms);
            std.debug.print(
                "soak: FAIL — the server under test (WorkerPool.run) stopped serving at +{d:.1}s of a {}s run, before shutdown was requested\n",
                .{ @as(f64, @floatFromInt(exit_ms)) / 1000.0, cfg.seconds },
            );
        }
        failed = true;
    }
    // Setup failures: classified, then resolved into gated and reported
    // totals (see assessSetup and the file header).
    const dials_ok = totals.dials_ok.load(.acquire);
    const dials_ok_at_onset = totals.dials_ok_at_ambiguous_onset.load(.acquire);
    const ambiguous_shape: AmbiguousShape = .{
        .failures = setup_counts[@backingInt(SetupClass.ambiguous)],
        .dials_ok_before = dials_ok_at_onset,
        .dials_ok_after = dials_ok -| dials_ok_at_onset,
    };
    const server_down_by_design = cfg.abrupt_death_every_ms != null;
    const setup_verdict = assessSetup(setup_counts, ambiguous_shape, server_down_by_design, tolerance);
    const ambiguous_onset_ms = totals.ambiguous_onset_ms.load(.acquire);
    if (ambiguous_shape.failures > 0) {
        std.debug.print(
            "soak: windows connect-stage error.Unexpected: {} dials failed from +{d:.1}s, after {} successful dials (port exhaustion needs >= {}); {} dials succeeded since -> {s}\n",
            .{
                ambiguous_shape.failures,
                @as(f64, @floatFromInt(ambiguous_onset_ms)) / 1000.0,
                ambiguous_shape.dials_ok_before,
                exhaustion_min_dials_before,
                ambiguous_shape.dials_ok_after,
                if (setup_verdict.ambiguous_is_exhaustion) "port-exhaustion shape" else "NOT the port-exhaustion shape: unexplained",
            },
        );
    }
    if (!setup_verdict.ok) {
        std.debug.print(
            "soak: FAIL — {} unexplained setup failures exceed the tolerance of {} (other={} ambiguous={} refused={} timeout={}; refused and timeout count because the server under test should be accepting{s})\n",
            .{
                setup_verdict.gated,
                tolerance,
                setup_counts[@backingInt(SetupClass.other)],
                if (setup_verdict.ambiguous_is_exhaustion) 0 else ambiguous_shape.failures,
                if (server_down_by_design) 0 else setup_counts[@backingInt(SetupClass.refused)],
                if (server_down_by_design) 0 else setup_counts[@backingInt(SetupClass.timeout)],
                if (ambiguous_shape.failures > 0 and !setup_verdict.ambiguous_is_exhaustion) "; a host already port-exhausted when the run began also lands here, and exercised nothing" else "",
            },
        );
        failed = true;
    } else if (setup_verdict.gated > 0) {
        warn(annotate, "{} unexplained setup failures, within the tolerance of {}; see the setup failures line", .{ setup_verdict.gated, tolerance });
    }
    if (setup_verdict.port_exhaustion > 0) {
        var first_ms = totals.first_port_exhaustion_ms.load(.acquire);
        if (setup_verdict.ambiguous_is_exhaustion) first_ms = @min(first_ms, ambiguous_onset_ms);
        // Host-side, not a defect in the code under test: reported loudly,
        // with where in the run it began, but not gated.
        warn(
            annotate,
            "host ephemeral-port exhaustion: {} dials failed from +{d:.1}s of a {}s run; traffic after that point was not exercised",
            .{ setup_verdict.port_exhaustion, @as(f64, @floatFromInt(first_ms)) / 1000.0, cfg.seconds },
        );
    }
    if (setup_verdict.reported > 0) {
        warn(
            annotate,
            "{} reported setup failures (resource limits{s}); see the setup failures line",
            .{ setup_verdict.reported, if (server_down_by_design) ", and dials refused or timed out while the server was down by design" else "" },
        );
    }
    if (!verdict.ok) {
        std.debug.print(
            "soak: FAIL — steady-state live heap grew {d:.2}% (> {d:.1}% threshold)\n",
            .{ verdict.growth_pct, cfg.mem_growth_pct },
        );
        failed = true;
    }
    switch (cfg.rss_gate) {
        .enforce => {
            if (!rss_available) {
                // An enforcing gate cannot pass on a reading it never took.
                std.debug.print("soak: FAIL — --rss-gate enforce, but RSS is unavailable on this platform\n", .{});
                failed = true;
            } else if (!rss_verdict.ok) {
                std.debug.print(
                    "soak: FAIL — steady-state RSS grew {d:.2}% (> {d:.1}% threshold, delta {d:.0}B > {}MiB floor)\n",
                    .{ rss_verdict.growth_pct, cfg.rss_growth_pct, rss_verdict.tail_mean - rss_verdict.head_mean, cfg.rss_floor_mib },
                );
                failed = true;
            }
        },
        .report => {
            if (rss_available and !rss_verdict.ok) {
                warn(
                    annotate,
                    "report-only RSS gate would FAIL: steady-state RSS grew {d:.2}% (> {d:.1}%, delta {d:.0}B > {}MiB floor); pass --rss-gate enforce to gate",
                    .{ rss_verdict.growth_pct, cfg.rss_growth_pct, rss_verdict.tail_mean - rss_verdict.head_mean, cfg.rss_floor_mib },
                );
            }
        },
    }
    // Everything above must be freed before this final leak check. The
    // sample series are freed by their `defer`s after this scope, so they
    // are not yet freed here — free them explicitly first so the
    // DebugAllocator sees a clean slate.
    mem_samples.deinit(telemetry_allocator);
    mem_samples = .empty;
    rss_samples.deinit(telemetry_allocator);
    rss_samples = .empty;
    if (gpa.deinit() != .ok) {
        std.debug.print("soak: FAIL — client-side allocation leaks detected\n", .{});
        failed = true;
    }
    if (failed) return error.SoakFailed;
    std.debug.print("soak: PASS\n", .{});
}

/// Print a compact ASCII sparkline of the live-heap series so the curve is
/// visible at a glance without a plotting tool. Downsamples to <=60 columns.
fn printMemCurve(samples: []const u64) void {
    if (samples.len == 0) {
        std.debug.print("  (no samples)\n", .{});
        return;
    }
    var lo: u64 = std.math.maxInt(u64);
    var hi: u64 = 0;
    for (samples) |v| {
        lo = @min(lo, v);
        hi = @max(hi, v);
    }
    const bars = [_][]const u8{ "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█" };
    const span: f64 = if (hi > lo) @floatFromInt(hi - lo) else 1.0;

    const max_cols: usize = 60;
    const cols = @min(samples.len, max_cols);
    var buf: [max_cols * 3]u8 = undefined;
    var w: usize = 0;
    var c: usize = 0;
    while (c < cols) : (c += 1) {
        // Map column c to a source sample (nearest).
        const src = if (cols == 1) 0 else (c * (samples.len - 1)) / (cols - 1);
        const norm = (@as(f64, @floatFromInt(samples[src] -| lo)) / span);
        var level: usize = @intFromFloat(@round(norm * @as(f64, @floatFromInt(bars.len - 1))));
        level = @min(level, bars.len - 1);
        const bar = bars[level];
        @memcpy(buf[w .. w + bar.len], bar);
        w += bar.len;
    }
    std.debug.print("  [{s}]  lo={}B hi={}B\n", .{ buf[0..w], lo, hi });
}
