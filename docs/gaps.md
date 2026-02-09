# Zig Cap'n Proto (capnpc-zig) Gap Analysis for Arena

**Date:** 2026-02-09
**Scope:** Full audit of capnpc-zig against the Arena benchmark schema requirements
**Audited version:** commit on main branch, 353+ tests passing

---

## 1. Executive Summary

### What Works

- **Wire format** (encode/decode, packed, far pointers, multi-segment): production-quality, heavily tested
- **Code generation** (structs, enums, unions, groups, interfaces, constants, defaults, annotations, all list types): complete
- **RPC bootstrap/call/return/finish/release**: functional end-to-end over TCP
- **Capability export and passing in return values**: works (proven by e2e tests with ChatRoom/TradeSession patterns)
- **Data fields** (read/write `[]const u8`): fully supported in codegen readers and builders
- **List(Interface)**: `CapabilityListReader`/`CapabilityListBuilder` wrappers are generated
- **Interface fields as parameters/results**: generated `setXxxCapability(cap)` and `readCapability()` work
- **Promise resolution, three-party handoff, disembargo**: implemented and tested
- **libxev TCP transport**: functional on macOS (kqueue) and Linux (io_uring)
- **Promise pipelining client API**: `PipelinedClient` + `callXxxPipelined()` + `sendCallPromisedWithOps()` generated (GAP-1 resolved)
- **Typed capability parameter passing**: `setXxxServer(peer, server)` + `setXxxClient(client)` helpers on builders (GAP-3 resolved)
- **Typed capability resolution**: `resolveXxx(peer, caps)` on readers returns typed Client (GAP-5 resolved)
- **Typed List(Interface) resolution**: `resolveXxx(index, peer, caps)` per-element resolution (GAP-6 resolved)
- **Graceful shutdown**: `peer.shutdown(callback)` with drain semantics (GAP-7 resolved)
- **Deferred handler returns**: `DeferredHandler` + `ReturnSender` with `sendResults()` / `sendException()` (GAP-8 resolved)
- **Basic streaming**: `StreamHandler` type, streaming dispatch, auto-ack Return (GAP-2 partially resolved)

### Remaining Gaps

- **Streaming flow control (GAP-2)**: Basic fire-and-forget streaming works (client calls, server handles, auto-acks). True Cap'n Proto streaming with server-side backpressure and client-side flow control is not yet implemented.
- **Verbose callback-driven API (GAP-4)**: Zig 0.15 removed async/await. The `DeferredHandler` pattern (GAP-8) provides the escape hatch for async handler scenarios.
- **Interface inheritance (GAP-9)**: Full `extends` support with `handleCallDirect`, ancestor VTable fields, and `(interface_id, method_id)` dispatch.
- **Single-threaded event loop (GAP-10)**: Architecture choice. All RPC operations run on a single libxev thread.

### Overall Readiness

**All 7 Arena scenarios can be implemented.** 3 scenarios are trivially ready (ping, echo, transfer). 3 more are fully supported with generated typed helpers (getChain, getFanout, collaborate). The stream scenario has basic support (fire-and-forget); full flow-control streaming is the only remaining significant feature gap.

---

## 2. Scenario-by-Scenario Assessment

### 2.1 `ping` -- Ready

```capnp
ping @0 () -> ();
```

**Status: READY**

Zero-parameter, zero-result RPC. The `PingPong` example demonstrates this exact pattern.

**Gaps: None**

### 2.2 `echo` -- Ready

```capnp
echo @1 (payload :Data) -> (payload :Data);
```

**Status: READY**

Data fields fully supported. The KvStore example demonstrates reading and writing Data fields over RPC.

**Gaps: None**

### 2.3 `getChain` -- Ready

```capnp
getChain @2 () -> (link :ChainLink);

interface ChainLink {
  next @0 () -> (link :ChainLink);
  resolve @1 () -> (value :UInt64);
}
```

**Status: READY**

Previously degraded due to missing promise pipelining and manual capability resolution. Now resolved:
- **GAP-1 (resolved)**: `PipelinedClient` + `callNextPipelined()` enables O(1) pipelining instead of O(n) round-trips
- **GAP-5 (resolved)**: `resolveLink(peer, caps)` on Results.Reader returns a typed `ChainLink.Client`

**Gaps: None**

### 2.4 `getFanout` -- Ready

```capnp
getFanout @3 (width :UInt32) -> (workers :List(Worker));

interface Worker {
  compute @0 (input :Data) -> (output :Data);
}
```

**Status: READY**

Previously required manual plumbing for capability list resolution. Now resolved:
- **GAP-5 (resolved)**: Typed capability resolution on readers
- **GAP-6 (resolved)**: `resolveWorkers(index, peer, caps)` per-element typed resolution for List(Interface)

**Gaps: None**

### 2.5 `transfer` -- Ready

```capnp
transfer @4 (size :UInt64) -> (payload :Data);
```

**Status: READY**

Straightforward UInt64 + Data fields.

**Gaps: None**

### 2.6 `collaborate` -- Ready

```capnp
collaborate @5 (peer :Collaborator) -> (result :Data);

interface Collaborator {
  offer @0 (data :Data) -> (accepted :Bool);
}
```

**Status: READY**

Previously the most difficult scenario due to synchronous handler limitation and manual capability passing. Now resolved:
- **GAP-3 (resolved)**: `setCollaboratorServer(peer, server)` / `setCollaboratorClient(client)` on builders
- **GAP-5 (resolved)**: `resolveCollaborator(peer, caps)` on readers returns typed Client
- **GAP-8 (resolved)**: `DeferredHandler` + `ReturnSender` enables async sub-calls before sending results

**Gaps: None**

### 2.7 `stream` -- Basic Support

```capnp
stream @6 (count :UInt32, size :UInt32) -> (received :UInt32);
```

**Status: BASIC SUPPORT**

Basic fire-and-forget streaming is implemented:
- Schema: `isStreaming()` detects streaming methods
- Codegen: `StreamHandler` type, streaming dispatch, auto-ack Return
- Runtime: `sendReturnEmptyStruct()`, `suppress_auto_finish` flag

What is NOT implemented:
- Server-side backpressure signals to client
- Client-side repeated-call API with flow control
- Streaming finish/done semantics per Cap'n Proto spec

**Gaps:**
- GAP-2: Flow-control streaming (moderate -- basic pattern works)

---

## 3. Detailed Gap List

### GAP-1: Promise Pipelining Client API -- RESOLVED

**Status:** Resolved in commit `73284b9`

The codegen now generates `PipelinedClient` types with `callXxxPipelined()` methods and `sendCallPromisedWithOps()` for each interface. Clients can pipeline calls on unresolved capabilities, achieving O(1) round-trips for chain patterns.

---

### GAP-2: Streaming Support -- PARTIALLY RESOLVED

**Status:** Basic streaming supported, flow control not yet implemented

**What works:**
- Schema detection of `stream` methods via `isStreaming()`
- `StreamHandler` type generated by codegen with streaming dispatch
- Auto-ack Return for streaming calls (`sendReturnEmptyStruct`)
- `is_streaming` and `suppress_auto_finish` flags propagated through runtime

**What remains:**
- Server-side backpressure: no mechanism for the server to signal the client to slow down
- Client-side flow control: no API for repeated calls with automatic pacing
- Finish/done semantics: streaming completion protocol not implemented

The basic fire-and-forget streaming pattern works for Arena benchmarks where the client sends N messages and the server processes them without backpressure.

---

### GAP-3: Typed Capability Parameter Passing -- RESOLVED

**Status:** Resolved in commit `73284b9`

The codegen now generates `setXxxServer(peer, server)` and `setXxxClient(client)` typed helpers on builders for interface parameters. Users no longer need to manually export servers and pass raw capability IDs.

---

### GAP-4: Verbose Callback-Driven API -- MITIGATED

**Status:** Mitigated by DeferredHandler (GAP-8)

Zig 0.15.2 removed async/await, so coroutine-based solutions are not available. The `DeferredHandler` + `ReturnSender` pattern (GAP-8) provides the escape hatch for scenarios requiring async sub-calls within a handler. The callback-driven API remains verbose for complex bidirectional patterns but is functionally complete.

---

### GAP-5: Typed Capability Resolution from InboundCapTable -- RESOLVED

**Status:** Resolved in commit `73284b9`

The codegen now generates `resolveXxx(peer, caps)` methods on result readers that return typed Client instances. Users no longer need to manually resolve capabilities through the InboundCapTable.

---

### GAP-6: Typed List-of-Capability Resolution -- RESOLVED

**Status:** Resolved in commit `73284b9`

The codegen now generates `resolveXxx(index, peer, caps)` per-element typed resolution methods for `List(Interface)` fields, returning typed Client instances.

---

### GAP-7: Graceful Shutdown API -- RESOLVED

**Status:** Resolved in commit `73284b9`

`peer.shutdown(callback)` with drain semantics is now available. The shutdown API handles draining in-flight calls before closing the transport.

---

### GAP-8: Deferred Handler Returns -- RESOLVED

**Status:** Resolved in commit `73284b9`

The codegen now generates `DeferredHandler` types with a `ReturnSender` that provides `sendResults()` and `sendException()` methods. Handlers can make async sub-calls and send results at any later point.

---

### GAP-9: Interface Inheritance -- RESOLVED

**Status:** Resolved. Full `extends` support in codegen.

The codegen now handles interface inheritance (`extends`):
- `superclasses` field parsed from schema wire format (pointer index 4 on Node)
- Ancestor chain collected recursively with diamond-inheritance deduplication
- `handleCallDirect` on every method struct enables type-safe cross-interface dispatch
- `onCall` dispatches on `(interface_id, method_id)` for inherited methods
- VTable includes inherited method handler fields from all ancestor interfaces
- Client and PipelinedClient expose inherited call methods using parent interface types
- Works for transitive inheritance (e.g. `TestExtends2 extends(TestExtends) extends(TestInterface)`)

---

### GAP-10: Single-Threaded Event Loop -- ARCHITECTURE CHOICE

**Status:** By design. Not a correctness issue.

All RPC operations run on a single-threaded libxev event loop. CPU-bound handlers block the loop. For Arena benchmarks involving computation, all workers share a single thread. This is a performance characteristic, not a bug.

---

## 4. Summary Table

| Scenario | Status | Blocking Gaps | Notes |
|----------|--------|--------------|-------|
| ping | READY | None | Trivial |
| echo | READY | None | Trivial |
| getChain | READY | None | Pipelining + typed resolution available |
| getFanout | READY | None | Typed list resolution available |
| transfer | READY | None | Trivial |
| collaborate | READY | None | DeferredHandler + typed cap helpers |
| stream | BASIC | GAP-2 (flow control) | Fire-and-forget works |

| GAP | Status | Severity |
|-----|--------|----------|
| GAP-1: Promise pipelining | RESOLVED | -- |
| GAP-2: Streaming | PARTIAL | Moderate |
| GAP-3: Typed cap params | RESOLVED | -- |
| GAP-4: Verbose callbacks | MITIGATED | Low |
| GAP-5: Typed cap resolution | RESOLVED | -- |
| GAP-6: Typed list resolution | RESOLVED | -- |
| GAP-7: Graceful shutdown | RESOLVED | -- |
| GAP-8: Deferred handlers | RESOLVED | -- |
| GAP-9: Interface inheritance | RESOLVED | -- |
| GAP-10: Single-threaded | ARCHITECTURE | Trivial |

**Bottom line:** 6 of 7 Arena scenarios are fully ready. The stream scenario has basic support; full flow-control streaming is the only remaining significant work item.

---

## Appendix: Key File Locations

| Component | Path |
|-----------|------|
| Code generator (interfaces) | `src/capnpc-zig/generator.zig` |
| Code generator (struct fields) | `src/capnpc-zig/struct_gen.zig` |
| Type mapping | `src/capnpc-zig/types.zig` |
| RPC peer (call/return/cap lifecycle) | `src/rpc/peer.zig` |
| Call sending | `src/rpc/peer/call/peer_call_sender.zig` |
| Return dispatch | `src/rpc/peer/return/peer_return_dispatch.zig` |
| Cap table encoding | `src/rpc/cap_table.zig` |
| Payload cap remapping | `src/rpc/payload_remap.zig` |
| Protocol types | `src/rpc/protocol.zig` |
| Runtime/listener | `src/rpc/runtime.zig` |
| Connection/framing | `src/rpc/connection.zig`, `src/rpc/framing.zig` |
| Transport (libxev) | `src/rpc/transport_xev.zig` |
| Ping-pong example | `examples/rpc_pingpong.zig` |
| KvStore example (full client/server) | `examples/kvstore/` |
| E2E server (capability passing) | `tests/e2e/zig/main_server.zig` |
| RPC runtime design | `docs/rpc_runtime_design.md` |
| API contracts | `docs/api_contracts.md` |
