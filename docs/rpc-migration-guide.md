# RPC Public Surface Migration Guide

Updated: 2026-07-04

The RPC facade now exposes domain-shaped modules instead of the old flat
compatibility aliases. This was a breaking cleanup in the 0.17-dev development
line: update imports and generated-code integrations to use the canonical
`capnpc.rpc.<domain>` paths directly.

## Public Domains

The supported RPC entry points are grouped by domain:

| Module | Use for |
|---|---|
| `rpc.wire` | RPC frame encoding and generated `rpc.capnp` protocol accessors. |
| `rpc.caps` | Capability descriptors, import/export tables, and payload remapping. |
| `rpc.promises` | Promised-answer transforms and pipelined-call state. |
| `rpc.events` | Redacted observer events shared by transports and peer dispatch. |
| `rpc.transport` | Binding contracts, stream state, TCP transport, and optional QUIC transport. |
| `rpc.peer` | Peer state, bootstrap, calls, returns, forwarding, and lifecycle dispatch. |
| `rpc.integration` | Host-facing adapters such as `HostPeer` and `WorkerPool`. |
| `rpc.generated` | Generated bindings for Cap'n Proto's standard RPC schemas. |
| `rpc.testing` | Test-only white-box helpers; not a stable application API. |

## Removed Alias Mapping

Replace old top-level aliases with the canonical domain path:

| Old name | New name |
|---|---|
| `rpc.protocol` | `rpc.wire.protocol` |
| `rpc.framing` | `rpc.wire.framing` |
| `rpc.cap_table` | `rpc.caps.table` |
| `rpc.promise_pipeline` | `rpc.promises.pipeline` |
| `rpc.connection` | `rpc.transport.tcp.connection` module, or `rpc.transport.tcp.Connection` type |
| `rpc.runtime` | `rpc.transport.tcp.runtime` module, or `rpc.transport.tcp.Runtime` type |
| `rpc.transport_binding` | `rpc.transport.binding` |
| `rpc.host_peer` | `rpc.integration.host_peer` module, or `rpc.integration.HostPeer` type |
| `rpc.worker_pool` | `rpc.integration.worker_pool` module, or `rpc.integration.WorkerPool` type |
| `rpc._internal` | `rpc.testing` for in-tree tests only; no stable production replacement. |

The transport namespace also exposes `rpc.transport.tcp.Listener`,
`rpc.transport.tcp.Transport`, `rpc.transport.stream_state`, and, when built
with QUIC support, `rpc.transport.quic`.

## Mechanical Update Examples

Import modules through their domains:

```zig
const rpc = capnpc.rpc;
const protocol = rpc.wire.protocol;
const cap_table = rpc.caps.table;
const Peer = rpc.peer.Peer;
const Connection = rpc.transport.tcp.Connection;
const HostPeer = rpc.integration.host_peer.HostPeer;
```

For generated RPC code, prefer the same public paths in signatures and helper
calls:

```zig
fn onCall(
    peer: *rpc.peer.Peer,
    call: rpc.wire.protocol.Call,
    caps: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    _ = peer;
    _ = call;
    _ = caps;
}
```

## Notes For Consumers

- Do not reintroduce local shims for removed aliases in new code; use the domain
  paths so future public API changes remain explicit.
- `rpc.testing` is deliberately unstable and intended for repository tests.
  Application code should avoid depending on it.
- TCP remains available through `rpc.transport.tcp`. QUIC remains opt-in through
  `rpc.transport.quic` when the build enables QUIC support.

## 2026-07 Breaking Changes (Session Lifecycle + Typed Responses)

Six breaks landed together; each is a mechanical update. The rewritten
[getting-started guide](getting-started-rpc.md) documents the new surface.

### `Peer.start` carries a callback context

Callbacks take a leading `ctx: ?*anyopaque`; no more globals to reach state.

```zig
// Before
peer.start(onPeerError, onPeerClose); // fn onPeerError(peer: *Peer, err: anyerror) void
// After
peer.start(&my_state, onPeerError, onPeerClose); // fn onPeerError(ctx: ?*anyopaque, peer: *Peer, err: anyerror) void
```

### Generated client call methods take by-value receivers

`Client.callX`, `Client.callXPipelined`, and `PipelinedClient.callX` all take
`self` by value (they never mutated the handle).

```zig
// Before
var c = client; // ritual copy to satisfy `self: *Client`
_ = try c.callPing(ctx, buildPing, onPingReturn);
// After
_ = try client.callPing(ctx, buildPing, onPingReturn);
```

### `Response.unwrap()` and `rpc.peer.CallError`

Generated `Response` and `BootstrapResponse` unions gained `unwrap()`, which
collapses the six arms to `rpc.peer.CallError!Results.Reader` (or `!Client`).
Locally synthesized reasons map to `error.Disconnected` / `error.CallTimedOut`.

```zig
// Before
switch (response) { .results => |r| ..., .exception => |ex| ..., else => ... }
// After
const results = try response.unwrap(); // CallError: RemoteException, Disconnected, CallTimedOut, Canceled, UnexpectedReturn
```

### `Client.release()`

Bootstrap returns (and generated `resolveX` helpers) retain an import ref the
returned `Client` owns; `release()` balances it. Call at most once per owned
client; peer teardown remains the backstop.

```zig
// Before: nothing balanced the bootstrap retain (import held until teardown)
// After
client.release();
```

### `ClientSession` replaces hand-rolled client wiring

`rpc.transport.tcp.connect` / `connectHost` return a heap-owned session that
bundles `Connection` + `Peer`, wires `peer.start`, and owns the one blessed
run()/close()/deinit() teardown ordering (`deinit()` only after `run()`
returns; `requestStop()` is the only cross-thread entry point).

```zig
// Before: create socket + Connection.init + Peer.init + start + run + 4-step manual teardown
// After
const session = try rpc.transport.tcp.connect(gpa, io, address, .{});
defer session.deinit();
// ... fromBootstrap(&session.peer, ...) ... session.run();
```

### Serde export symbols are parent-qualified

Manifest type names and C export symbols mirror the emitted type path, so
same-simple-name nested types no longer collide at link time; residual
collisions fail generation with `error.DuplicateSerdeExportSymbol`.

```text
Before: {"type_name":"Detail","to_json_export":"capnp_rpc_detail_to_json"}
After:  {"type_name":"Exception.Detail","to_json_export":"capnp_rpc_exception_detail_to_json"}
```
