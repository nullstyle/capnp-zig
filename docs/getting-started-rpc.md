# Getting Started: RPC with capnpc-zig

This guide walks you through defining a Cap'n Proto RPC interface and building a working client-server application in Zig. It assumes you've read the [serialization guide](getting-started-serialization.md).

Every Zig snippet in this guide is compile-gated: `tests/docs/rpc_getting_started_snippets_test.zig` compiles them against the real library and the real generated modules (`zig build test-docs-snippets`), and the quickstart halves are additionally run against each other over a loopback socket. If a snippet here drifts from the shipped API, that build step fails.

> **Status:** The RPC runtime is in production hardening (phase 7). The API documented here is the current supported surface; breaking changes are recorded in the [migration guide](rpc-migration-guide.md).

## Prerequisites

- **Zig 0.17-dev** on `PATH` (`mise.toml` manages helper tools only)
- **Cap'n Proto compiler** (`capnp`) — for schema compilation
- **capnpc-zig** — built from this repo (`zig build`)

## 1. Define Your Interface

The repo's ping-pong example (`examples/pingpong.capnp`) is the minimal shape:

```capnp
@0xa2f1c9c38d1c7b14;

interface PingPong {
  ping @0 (count :UInt32) -> (count :UInt32);
}
```

Each method has:
- An **ordinal** (`@0`) — the method ID on the wire
- **Parameters** — compiled into a `Params` struct
- **Results** — compiled into a `Results` struct

## 2. Generate Zig Code

```bash
capnp compile -o ./zig-out/bin/capnpc-zig pingpong.capnp
```

To run codegen from your own `build.zig` and import the generated module, follow [build-integration.md](build-integration.md) — this guide assumes the generated file is importable as `@import("pingpong")`.

The generated `pingpong.zig` (checked in at `examples/pingpong.zig`) contains, per interface:

- **`PingPong.Client`** — a small by-value handle (`peer` + capability id) with one `callX` method per interface method, plus `fromBootstrap` to obtain the remote's root capability and `release()` to drop the import ref this client owns.
- **`PingPong.Server` + `PingPong.VTable`** — implement methods locally; one handler function pointer per method.
- **`PingPong.Ping.Response`** — a union over every possible `Return`, with an `unwrap()` method that collapses it to `rpc.peer.CallError!Results.Reader`.
- **`PingPong.BootstrapResponse`** — the same shape for bootstrap, unwrapping to a `Client`.
- **`PingPong.PipelinedClient`** — call methods on a *promised* capability before its `Return` arrives (see [Typed pipelining](#typed-pipelining)).
- **`PingPong.setBootstrap(peer, server)` / `PingPong.exportServer(peer, server)`** — register a server as the connection's root capability, or export it as an additional capability.

## 3. The Server Half

A server implements the vtable — one function per interface method. Handlers read `params`, write `results`, and run synchronously on the connection's thread when an inbound call arrives; returning an error sends an RPC exception back to the caller:

```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");
const pingpong = @import("pingpong");

const rpc = capnpc.rpc;
const PingPong = pingpong.PingPong;

fn handlePing(
    _: *anyopaque,
    _: *rpc.peer.Peer,
    params: PingPong.Ping.Params.Reader,
    results: *PingPong.Ping.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const value = try params.getCount();
    try results.setCount(value + 1);
}
```

The server side mirrors `ClientSession` with `ServerSession`: accept one
connection off a `Listener`, set the bootstrap capability, and serve it until
the client disconnects. `ServerSession` owns the `Connection` + `Peer` and the
teardown ordering, exactly like `ClientSession`.

```zig
/// Accept one connection and serve it until the client disconnects.
fn serveOne(allocator: std.mem.Allocator, listener: *rpc.transport.tcp.Listener, server: *PingPong.Server) void {
    var session = rpc.transport.tcp.ServerSession.accept(allocator, listener, .{}) catch return;
    defer session.deinit();
    // Set the bootstrap before run(): the peer is not started until run().
    if (PingPong.setBootstrap(&session.peer, server)) |_| {
        session.run(); // starts the peer, blocks until the connection closes
    } else |_| {}
}
```

`ServerSession` is strictly one connection per session; for a concurrent server,
spawn one `serveOne` per accepted connection, or keep using `Listener` +
`WorkerPool` for a managed pool.

Bind the listener and hand it a server value:

```zig
fn startServer(allocator: std.mem.Allocator, io: std.Io) !void {
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 7001);
    var listener = try rpc.transport.tcp.Listener.init(allocator, io, address, .{});
    defer listener.close();

    var server = PingPong.Server{
        .ctx = undefined, // passed as the first argument to every handler
        .vtable = .{ .ping = handlePing },
    };

    serveOne(allocator, &listener, &server);
}
```

`io` is a `std.Io` instance. In a `main(init: std.process.Init)` entry point the simplest source is the process-provided one, via this repo's backend selector:

```zig
fn setupBackend(init: std.process.Init) !capnpc.io_backend.Backend {
    // .process_init reuses init.io; .threaded / .evented construct their own.
    return capnpc.io_backend.Backend.init(.process_init, init.gpa, init.io);
}
```

then `const io = backend.io();` (and `defer backend.deinit();`). See `examples/rpc_pingpong.zig` for the complete program.

## 4. The Client Half

The client side is one call: `rpc.transport.tcp.connect` returns a heap-owned `*ClientSession` that bundles the `Connection` and `Peer` and encapsulates the whole teardown ordering.

```zig
const ClientState = struct {
    result: ?u32 = null,
    err: ?anyerror = null,
};

fn runClient(
    allocator: std.mem.Allocator,
    io: std.Io,
    address: std.Io.net.IpAddress,
    state: *ClientState,
) !void {
    const session = try rpc.transport.tcp.connect(allocator, io, address, .{});
    defer session.deinit(); // legal here: runs after session.run() returns

    // Request the remote's root capability. Calls are legal immediately —
    // outbound frames enqueue; nothing reads the socket until run().
    _ = try PingPong.Client.fromBootstrap(&session.peer, state, onBootstrap);

    session.run(); // blocks; every callback fires on this thread

    if (state.err) |err| return err;
}
```

The bootstrap callback unwraps the response into a typed `Client` and issues the first call. Inside generated callbacks you get a `*rpc.peer.Peer`; `ClientSession.fromPeer` recovers the owning session:

```zig
fn onBootstrap(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: PingPong.BootstrapResponse,
) anyerror!void {
    const state: *ClientState = @ptrCast(@alignCast(ctx_ptr));
    const client = response.unwrap() catch |err| {
        state.err = err;
        rpc.transport.tcp.ClientSession.fromPeer(peer).close();
        return;
    };
    _ = try client.callPing(state, buildPing, onPingReturn);
}
```

## 5. Making Calls

Each call takes a context pointer and two callbacks: a **build function** that populates the parameters before the message is sent, and a **return callback** that fires when the `Return` arrives. `callX` returns the question id (useful for [per-call deadlines](#timeouts) and [cancellation](#cancellation)); the context must stay alive until the return callback has fired.

```zig
fn buildPing(_: *anyopaque, params: *PingPong.Ping.Params.Builder) anyerror!void {
    try params.setCount(41);
}

fn onPingReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: PingPong.Ping.Response,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const state: *ClientState = @ptrCast(@alignCast(ctx_ptr));
    const session = rpc.transport.tcp.ClientSession.fromPeer(peer);
    defer session.close(); // graceful close; idempotent; legal from callbacks

    const results = response.unwrap() catch |err| {
        state.err = err;
        return;
    };
    state.result = try results.getCount();
}
```

### `unwrap()` and `rpc.peer.CallError`

`Response.unwrap()` collapses the six-arm `Response` union into `rpc.peer.CallError!Results.Reader` (`BootstrapResponse.unwrap()` does the same, yielding a `Client`). The mapping:

| `Return` received | `unwrap()` yields | Notes |
|---|---|---|
| results | `Results.Reader` | Success. |
| exception, reason == `rpc.peer.disconnected_reason` (`"disconnected"`) | `error.Disconnected` | Locally synthesized: transport closed / peer shut down / peer deinit. |
| exception, reason == `rpc.peer.shutdown_reason` (`"peer shutting down"`) | `error.Disconnected` | Locally synthesized when the graceful-shutdown drain bound expires. |
| exception, reason == `rpc.peer.deadline_reason` (`"deadline exceeded"`) | `error.CallTimedOut` | Locally synthesized by deadline expiry. |
| exception, any other reason | `error.RemoteException` | The reason string stays available on the union arm. |
| canceled | `error.Canceled` | The remote confirmed a cancellation we initiated. |
| resultsSentElsewhere / takeFromOtherQuestion / acceptFromThirdParty | `error.UnexpectedReturn` | Return arms the plain-call path never initiates. |

When you need the remote's exception reason, it is still on the union arm after `unwrap()` fails:

```zig
fn describeFailure(response: PingPong.Ping.Response) ?u32 {
    const results = response.unwrap() catch |err| {
        if (err == error.RemoteException) {
            std.log.warn("remote exception: {s}", .{response.exception.reason});
        }
        return null;
    };
    return results.getCount() catch null;
}
```

## 6. Capability Lifecycle

A successful `BootstrapResponse.unwrap()` (and every generated `resolveX` helper on results that carry an interface) **retains** an import ref that the returned `Client` owns. Release it when you are done with the capability:

```zig
const App = struct {
    service: ?PingPong.Client = null,
};

fn dropService(app: *App) void {
    if (app.service) |client| {
        client.release(); // balances the bootstrap/resolve retain; at most once
        app.service = null;
    }
}
```

- `release()` is best-effort and must be called **at most once** per owned `Client`.
- Forgetting it is not a local memory leak: peer teardown releases all imports as a backstop. What it does cost you is server-side resources — the remote keeps the export (and whatever it pins) alive until you release it or the connection closes.
- Releasing one capability does not disturb others on the same connection; `tests/e2e/zig/main_client.zig` releases a sub-capability mid-session and keeps calling the service it came from.

Interfaces can also be passed as method parameters or returned from methods; the runtime exports/imports them automatically. This is the foundation of Cap'n Proto's object-capability model.

## 7. Typed Pipelining

When a method returns a capability, the generated client offers `callXPipelined`, which returns a pipeline handle whose getters produce `PipelinedClient`s — typed clients that target the *promised* result of a question whose `Return` has not arrived yet. Dependent calls hit the wire back-to-back and the server answers them in E-order, saving a round trip per hop.

From the e2e matchmaking schema (`tests/e2e/schemas/matchmaking.capnp`):

```capnp
findMatch @2 (player :PlayerInfo, mode :GameMode) -> (controller :MatchController, matchId :MatchId);
```

```zig
fn findAndReady(app: *MatchApp, service: matchmaking.MatchmakingService.Client) !void {
    // Send findMatch, then immediately call methods on the promised
    // controller — before findMatch's Return has resolved.
    const pipeline = try service.callFindMatchPipelined(app, buildFindMatch, onFindMatchReturn);
    const promised = pipeline.getController();
    _ = try promised.callSignalReady(app, buildSignalReady, onSignalReadyReturn);
    _ = try promised.callGetInfo(app, null, onMatchInfoReturn);
}
```

Each pipelined call has its own return callback and question id, exactly like a direct call. Once `findMatch`'s own `Return` arrives, resolve the now-concrete capability with the generated `resolveController(peer, caps)` helper and call it directly from then on — see the matchmaking scenario in `tests/e2e/zig/main_client.zig` for the full flow.

## 8. Timeouts

`ConnectOptions.default_call_timeout_ms` stamps a deadline on every outbound question at send time. It is **on by default: 30 seconds**. When it is enabled and you did not configure your own connection tick, `connect()` wires a 100ms tick automatically so deadlines actually fire. Pass `null` to disable both.

```zig
fn connectWithTimeouts(
    allocator: std.mem.Allocator,
    io: std.Io,
    address: std.Io.net.IpAddress,
) !*rpc.transport.tcp.ClientSession {
    return rpc.transport.tcp.connect(allocator, io, address, .{
        // Deadline stamped on every call (default 30_000; null disables).
        .default_call_timeout_ms = 10_000,
        // Graceful close(): force-cancel still-outstanding questions after
        // this drain bound (default 5_000).
        .shutdown_drain_timeout_ms = 2_000,
    });
}
```

A timed-out question is cancelled with `rpc.peer.deadline_reason`, so its callback fires with an exception `Return` and `unwrap()` yields `error.CallTimedOut`.

### Per-call override

`callX` returns the question id; override (or set) the deadline for that one call with `Peer.setQuestionDeadline`:

```zig
fn pingWithShortDeadline(session: *rpc.transport.tcp.ClientSession, state: *ClientState, client: PingPong.Client) !void {
    const qid = try client.callPing(state, buildPing, onPingReturn);
    try session.peer.setQuestionDeadline(qid, 250); // this call only: 250ms
}
```

`Peer.clearQuestionDeadline(qid)` removes a deadline. Both are session-thread-only, like every peer entry point.

## 9. Cancellation

Cancel an outstanding call with `Peer.cancelQuestion`. The local callback is delivered an exception `Return` carrying your reason immediately, and a `Finish` is sent so the remote can stop working; a late `Return` from the remote is absorbed silently.

```zig
fn abandonPing(session: *rpc.transport.tcp.ClientSession, qid: u32) !void {
    try session.peer.cancelQuestion(qid, "user clicked cancel");
}
```

Note the `unwrap()` mapping: your custom reason string arrives as `error.RemoteException` (with the reason on the union arm). `error.Canceled` is reserved for the case where the *remote* answers an earlier cancellation with `Return.canceled`.

### Terminal callbacks are guaranteed

You never have to time out on your own callback. Every outstanding question's `on_return` is guaranteed to fire **exactly once**, even when the answer can no longer arrive: transport close (EOF, error, or explicit close) and direct `Peer.deinit` both deliver a synthetic exception `Return` with `rpc.peer.disconnected_reason` (so `unwrap()` yields `error.Disconnected`) before `on_close` runs, while the peer is still intact. See "Terminal question failure on disconnect" in [rpc_runtime_design.md](rpc_runtime_design.md) for the precise contract.

## 10. Shutdown and the Threading Contract

The session is **thread-affine**: `connect`, calls, `run()`, `close()`, and `deinit()` all belong to one thread, and every callback fires inside `run()` on that thread (debug builds assert this). The two supported patterns are running the whole lifecycle on your own thread, or `connect` on thread A then `run()` on thread B — `run()` re-adopts affinity on entry, which is safe because nothing else may touch the session in between.

```zig
/// Graceful stop, from the session thread (e.g. inside any callback):
fn stopGracefully(session: *rpc.transport.tcp.ClientSession) void {
    session.close(); // idempotent; drains, then run() unwinds
}

/// Abort from ANY other thread — the single thread-safe entry point.
/// No graceful drain; in-flight questions resolve as Disconnected.
fn stopFromElsewhere(session: *rpc.transport.tcp.ClientSession) void {
    session.requestStop();
}
```

Lifecycle rules:

- **`run()`** blocks until the transport has closed and `on_close` has fired.
- **`close()`** — graceful, idempotent, session-thread only (callbacks run on the session thread, so calling it from a callback is legal).
- **`requestStop()`** — the only cross-thread call on the whole session.
- **`deinit()`** — legal **only after `run()` has returned** (or if it was never called). It encapsulates the one blessed teardown ordering.
- **`on_error` / `on_close`** (from `ConnectOptions`) fire on the `run()` thread. `on_close` must **not** call `deinit()` — it runs *inside* `run()`. Their `ctx` must outlive the session:

```zig
fn onSessionError(ctx: ?*anyopaque, _: *rpc.transport.tcp.ClientSession, err: anyerror) void {
    const state: *ClientState = @ptrCast(@alignCast(ctx.?));
    if (state.err == null) state.err = err;
}

fn onSessionClose(_: ?*anyopaque, _: *rpc.transport.tcp.ClientSession) void {
    // Last callback before run() returns. Observe, don't deinit.
}

fn connectWithCallbacks(
    allocator: std.mem.Allocator,
    io: std.Io,
    address: std.Io.net.IpAddress,
    state: *ClientState,
) !*rpc.transport.tcp.ClientSession {
    return rpc.transport.tcp.connect(allocator, io, address, .{
        .ctx = state,
        .on_error = onSessionError,
        .on_close = onSessionClose,
    });
}
```

The same contract applies to `ServerSession`: `run()` blocks on the owner
thread, `requestStop()` is the thread-safe entry point, and `deinit()` is legal
only after `run()` returns.

## 11. Troubleshooting

| Mistake | What actually happens |
|---|---|
| `session.deinit()` before `run()` has returned (e.g. from `on_close`, or from another thread while `run()` blocks) | Use-after-free: `run()` is still reading the connection you just freed. Typically a crash inside the read loop, or a DebugAllocator panic. `deinit()` only after `run()` returns; from callbacks use `close()`. |
| Calling session/peer methods (calls, `close()`, `setQuestionDeadline`, ...) from another thread | Debug builds panic on the thread-affinity assertion; release builds are a data race. `requestStop()` is the only thread-safe call. |
| Forgetting `client.release()` | No local leak — peer teardown releases all imports as a backstop — but the server keeps the export and everything it pins alive until the connection closes. |
| Calling `release()` twice on the same owned `Client` | Over-releases the import ref; the remote may drop the export while other holders still use it. Release at most once, then null out your handle. |
| Passing a stack `ctx` that dies before the return callback fires | Use-after-free when the callback dereferences it. The ctx must outlive the call (and `ConnectOptions.ctx` must outlive the session). |
| Expecting timeouts after `default_call_timeout_ms = null` with no explicit `conn.tick_interval_ms` | Nothing sweeps deadlines — `setQuestionDeadline` stamps a deadline no tick ever checks, so calls never time out. Keep the default, or configure a tick. |
| Waiting for a `Return` after the connection dropped | Does not hang: every outstanding question gets the synthetic Disconnected terminal exactly once ([see above](#terminal-callbacks-are-guaranteed)). If you observe a hang, the callback ordering bug is in application state, not a missing `Return`. |

## Running the Examples

The repo includes a complete ping-pong RPC example (both halves of this guide in one file):

```bash
zig build example-rpc
```

See `examples/rpc_pingpong.zig` for the source and `examples/pingpong.capnp` for the schema. For richer scenarios — sub-capabilities, mid-session release, and typed pipelining against reference peers — read `tests/e2e/zig/main_client.zig`.
