# RPC Refactor Plan: Domain Layout Then Protocol Split

This is a planning note for the large-scale RPC refactor discussed in the
hardening session. It is intentionally not a design-complete spec. Its job is
to preserve the agreed direction across long sessions and context compaction.

## Current Status

Option 1 source rehome was committed as `50daa66` (`rpc: organize runtime by
domain`):

- RPC implementation files have moved from `level0`/`level1`/`level2`/`level3`
  into domain folders under `wire/`, `caps/`, `promises/`, `transport/`,
  `transport/tcp/`, `transport/quic/`, and `peer/`.
- `src/rpc/mod.zig` and `src/rpc/mod_core.zig` preserve compatibility exports.
- QUIC has been split into `transport/quic/mod.zig`, `connection.zig`,
  `options.zig`, `length_framer.zig`, `outbound_queue.zig`, and
  `quic_zig_adapter.zig`.
- RPC tests have moved from `tests/rpc/level*` into domain folders while the
  cumulative `test-rpc-level*` build step names remain for compatibility.
- `zig build check --summary all`, `zig build test --summary all`,
  `zig build test-e2e-security --summary all`, and
  `zig build hardening --summary all` have passed during the refactor.
- Option 3 Tranche 7 has started locally. The first pass keeps `peer/mod.zig`
  as the public facade while extracting state vocabulary, named peer error
  groups, and the peer-facing transport callback facade into
  `src/rpc/peer/state.zig`, `src/rpc/peer/errors.zig`, and
  `src/rpc/peer/transport.zig`.
- Option 3 Tranche 8 has started with protocol-family facades:
  `dispatch.zig`, `bootstrap.zig`, `finish.zig`, `resolve.zig`, and
  `disembargo.zig`. These currently wrap existing implementation helpers so the
  semantic paths exist before code is moved behind them.
- Option 3 Tranche 10 has split capability lifecycle code into
  `caps/descriptors.zig`, `caps/lifecycle.zig`, `caps/inbound.zig`, and
  `caps/outbound.zig`, with `caps/table.zig` preserved as the compatibility
  facade for `rpc.cap_table`.
- QUIC is now an explicit build-module choice. The default `capnpc-zig` module
  keeps quic-zig/BoringSSL out of serialization and TCP-only builds and exposes a
  disabled `rpc.quic` facade for QUIC-dependency-free framing helpers. Build with
  `-Dquic=true` to select `src/lib_quic.zig`, import quic-zig, and expose the
  native QUIC transport implementation.
- Option 3 Tranche 11 has split the promise/pipeline helpers under the existing
  `src/rpc/promises/` domain into `promised_answer.zig`, `pending_calls.zig`,
  `return_routing.zig`, and `return_send.zig`, while keeping the older
  promise/return helper filenames as compatibility facades.

## Goals

- Replace the `level0`, `level1`, `level2`, `level3` implementation layout with
  names that match how contributors search for the code.
- Make the QUIC transport easy to find and evolve.
- Preserve current behavior and public API during the first wave.
- Keep generated code working while internals move.
- Move toward idiomatic Zig 0.17-dev structure: explicit module surfaces,
  isolated optional dependencies, and clearer `std.Io` boundaries.
- After the tree is navigable, split the core RPC peer by protocol concepts.

## Non-Goals For The First Wave

- Do not redesign Cap'n Proto RPC semantics.
- Do not change wire formats.
- Do not change QUIC behavior while moving files.
- Do not remove public compatibility exports immediately.
- Do not make a broad generic transport abstraction for TCP, QUIC, host peers,
  and future stream transports.

## Important Existing Boundary

Preserve the narrow peer-to-transport seam.

Today, TCP and QUIC both deliver complete RPC frames to `Peer` through the
transport binding path. That is the right boundary:

- Peer owns RPC state and protocol handling.
- Transports own bytes, sockets, UDP/QUIC stepping, writer queues, and lifecycle.
- The shared contract is ordered complete RPC frames plus start/send/close
  behavior.

This boundary should become easier to see, not more abstract.

## Public Names To Preserve

Keep these names exported from `src/rpc/mod.zig` during Option 1:

- `rpc.framing`
- `rpc.protocol`
- `rpc.cap_table`
- `rpc.promise_pipeline`
- `rpc.cap_pointer`
- `rpc.transport_binding`
- `rpc.transport`
- `rpc.connection`
- `rpc.runtime`
- `rpc.quic`
- `rpc.peer`
- `rpc.stream_state`
- `rpc.host_peer`
- `rpc.worker_pool`

Generated code and tests currently rely on several of these names. If any name
is retired later, do it only after a separate compatibility decision.

## Option 1: Domain-First Rehome

Option 1 is mostly mechanical. It should move files by domain, update imports,
and keep compatibility facades. Avoid semantic rewrites in the same tranche.

Target shape:

```text
src/rpc/
  mod.zig
  mod_core.zig
  wire/
    framing.zig
    protocol.zig
  caps/
    table.zig
    cap_pointer.zig
    payload_remap.zig
  promises/
    pipeline.zig
    promised_answer_copy.zig
    peer_promises.zig
    return_send_helpers.zig
  transport/
    binding.zig
    stream_state.zig
    tcp/
      connection.zig
      stream_transport.zig
      runtime.zig
    quic/
      mod.zig
      connection.zig
      options.zig
      length_framer.zig
      outbound_queue.zig
      quic_zig_adapter.zig
  peer/
    mod.zig
    ...
  integration/
    host_peer.zig
    worker_pool.zig
```

### Tranche 1: Wire And Cap Modules

Move:

```text
src/rpc/level0/framing.zig       -> src/rpc/wire/framing.zig
src/rpc/level0/protocol.zig      -> src/rpc/wire/protocol.zig
src/rpc/level0/cap_table.zig     -> src/rpc/caps/table.zig
src/rpc/level3/payload_remap.zig -> src/rpc/caps/payload_remap.zig
src/rpc/common/cap_pointer.zig   -> src/rpc/caps/cap_pointer.zig
```

Keep compatibility exports in `src/rpc/mod.zig`:

```zig
pub const framing = @import("wire/framing.zig");
pub const protocol = @import("wire/protocol.zig");
pub const cap_table = @import("caps/table.zig");
pub const cap_pointer = @import("caps/cap_pointer.zig");
```

Update internal imports, tests, docs, and hardening allowlists after the move.

### Tranche 2: Promise And Pipeline Helpers

Move:

```text
src/rpc/common/promise_pipeline.zig         -> src/rpc/promises/pipeline.zig
src/rpc/common/promised_answer_copy.zig     -> src/rpc/promises/promised_answer_copy.zig
src/rpc/level1/peer_promises.zig            -> src/rpc/promises/peer_promises.zig
src/rpc/level1/peer_return_send_helpers.zig -> src/rpc/promises/return_send_helpers.zig
```

Keep aliases from `mod.zig` and update all internal imports. This tranche should
not change promise semantics.

### Tranche 3: Transport Rehome

Move the peer-facing binding:

```text
src/rpc/common/transport_binding.zig -> src/rpc/transport/binding.zig
src/rpc/level2/stream_state.zig      -> src/rpc/transport/stream_state.zig
```

Move TCP-specific files:

```text
src/rpc/level2/transport.zig  -> src/rpc/transport/tcp/stream_transport.zig
src/rpc/level2/connection.zig -> src/rpc/transport/tcp/connection.zig
src/rpc/level2/runtime.zig    -> src/rpc/transport/tcp/runtime.zig
```

Move QUIC without splitting behavior yet:

```text
src/rpc/level2/quic_transport.zig -> src/rpc/transport/quic/connection.zig
```

At this point, keep old public names as facades:

```zig
pub const transport_binding = @import("transport/binding.zig");
pub const stream_state = @import("transport/stream_state.zig");
pub const transport = @import("transport/tcp/stream_transport.zig");
pub const connection = @import("transport/tcp/connection.zig");
pub const runtime = @import("transport/tcp/runtime.zig");
pub const quic = @import("transport/quic/connection.zig");
```

Do not generalize `WorkerPool` in this tranche. It is currently TCP-shaped.

### Tranche 4: Peer Rehome

Move:

```text
src/rpc/level3/peer.zig   -> src/rpc/peer/mod.zig
src/rpc/level3/peer/*     -> src/rpc/peer/*
```

Keep:

```zig
pub const peer = @import("peer/mod.zig");
```

This will likely be the noisiest import-update tranche. Keep it mechanical.

### Tranche 5: Integration Rehome

Review the existing `src/rpc/integration` folder after the transport move.

Likely keep:

```text
src/rpc/integration/host_peer.zig
src/rpc/integration/worker_pool.zig
```

But document that `worker_pool.zig` is TCP/server oriented. If it is renamed,
use a compatibility alias first.

### Tranche 6: Split QUIC Internals

Only after the path move is green, split the large QUIC file:

```text
src/rpc/transport/quic/mod.zig
src/rpc/transport/quic/connection.zig
src/rpc/transport/quic/options.zig
src/rpc/transport/quic/length_framer.zig
src/rpc/transport/quic/outbound_queue.zig
src/rpc/transport/quic/quic_zig_adapter.zig
```

Suggested responsibilities:

- `mod.zig`: public QUIC facade.
- `connection.zig`: connection lifecycle, attach-to-peer behavior, run loop.
- `options.zig`: public options, limits, hardening knobs, defaults.
- `length_framer.zig`: baseline stream length-delimited RPC frames.
- `outbound_queue.zig`: queued outbound frame storage and accounting.
- `quic_zig_adapter.zig`: quic-zig-specific config mapping, error mapping, path helpers.

Do not introduce QUIC-native multistream semantics here. This split should keep
the current baseline stream behavior.

### Option 1 Stabilization Gate

Run:

```bash
zig fmt src/ tests/
zig build check --summary all
zig build test --summary all
```

Also update:

- `docs/rpc_runtime_design.md`
- `docs/architecture.md`
- hardening docs and security matrix path references
- `src/rpc/llms.txt`, or replace it with human-facing docs
- tests that encode level folder names
- any hardening gates that scan exact paths

## Option 3: Protocol-Semantic Peer/Core Split

Option 3 starts after Option 1 has landed and stabilized. This wave is semantic,
so do it in smaller, reviewable tranches. Keep `peer/mod.zig` as the public
facade throughout.

Target shape:

```text
src/rpc/
  wire/
    messages.zig
    stream_framing.zig
  caps/
    table.zig
    descriptors.zig
    inbound.zig
    outbound.zig
    lifecycle.zig
    payload_remap.zig
  pipeline/
    promised_answer.zig
    pending_calls.zig
    return_routing.zig
    return_send.zig
  peer/
    mod.zig
    state.zig
    errors.zig
    transport.zig
    bootstrap.zig
    dispatch.zig
    call.zig
    return.zig
    finish.zig
    resolve.zig
    disembargo.zig
    provide_accept_join.zig
    third_party.zig
  transport/
    tcp/
    quic/
  integration/
```

### Tranche 7: Extract Peer State

Split `peer/mod.zig` into:

```text
src/rpc/peer/mod.zig
src/rpc/peer/state.zig
src/rpc/peer/errors.zig
src/rpc/peer/transport.zig
```

Move state structs, maps, counters, lifecycle flags, and transport callback glue
out of the public `Peer` implementation where possible.

This tranche should not change dispatch behavior.

First local pass:

- `PeerLimits`, small state record types, `Question`, pending third-party await
  records, and thread-affinity helpers moved behind `peer/state.zig`.
- Peer error names are grouped in `peer/errors.zig`; the current public peer
  entry points still return inferred error unions.
- The peer-to-transport binding aliases now flow through `peer/transport.zig`,
  which also facades the peer transport callback/state helpers.
- `rpc.peer.PeerLimits` and the public transport aliases remain compatibility
  re-exports.

### Tranche 8: Extract Peer Dispatch

Create:

```text
src/rpc/peer/dispatch.zig
src/rpc/peer/bootstrap.zig
src/rpc/peer/call.zig
src/rpc/peer/return.zig
src/rpc/peer/finish.zig
src/rpc/peer/resolve.zig
src/rpc/peer/disembargo.zig
```

Each module should own one protocol message family. Prefer passing a narrow
context object instead of letting every helper reach through the whole `Peer`.

First local pass:

- Added facade modules for dispatch, bootstrap/abort/unimplemented, finish,
  resolve, and disembargo.
- `peer/mod.zig` now routes those message families through the new facade
  names, while `peer_control.zig` remains as the compatibility implementation
  holder for the still-unsplit helpers.
- Bootstrap, abort, and unimplemented-message logic has moved into
  `peer/bootstrap.zig`; `peer_control.zig` re-exports those names for existing
  tests and compatibility.
- Finish cleanup and resolved-answer frame cleanup have moved into
  `peer/finish.zig`; `peer_control.zig` re-exports those names for existing
  tests and compatibility.

### Tranche 9: Split Provide/Accept/Join And Third-Party Helpers

Create or normalize:

```text
src/rpc/peer/provide_accept_join.zig
src/rpc/peer/third_party.zig
```

Keep the current public behavior and routing semantics. This tranche is about
searchability and ownership.

First local pass:

- Added `peer/provide_accept_join.zig` and `peer/third_party.zig` as semantic
  facades for the Provide/Accept/Join and third-party helper families.
- `peer/mod.zig` now routes provide/accept/join, third-party payload capture,
  sendResultsTo-thirdParty, and pending third-party return helpers through the
  semantic facades.
- `peer_control.zig` keeps compatibility re-exports for existing tests/imports
  while the moved helper implementations live in the semantic modules.

### Tranche 10: Split Capability Lifecycle

Break `caps/table.zig` into narrower pieces:

```text
src/rpc/caps/table.zig
src/rpc/caps/descriptors.zig
src/rpc/caps/inbound.zig
src/rpc/caps/outbound.zig
src/rpc/caps/lifecycle.zig
src/rpc/caps/payload_remap.zig
```

Keep `caps/table.zig` as the compatibility facade for names such as
`InboundCapTable`. Generated code should keep working through `rpc.cap_table`.

### Tranche 11: Split Pipeline And Return Routing

Move promised answer and return-routing logic into:

```text
src/rpc/promises/promised_answer.zig
src/rpc/promises/pending_calls.zig
src/rpc/promises/return_routing.zig
src/rpc/promises/return_send.zig
```

The aim is to make pipelining and return handling searchable by protocol
concept, not by historic implementation level. The repository has standardized
on `promises/` as the domain folder, so `pipeline.zig`, `peer_promises.zig`,
`promised_answer_copy.zig`, and `return_send_helpers.zig` remain as
compatibility facades instead of forcing a disruptive directory rename.

### Tranche 12: Narrow Internal API

After semantic modules exist, revisit `rpc._internal`.

Options:

- Remove it from the public module surface.
- Replace it with a deliberately unstable `rpc.testing` support surface.
- Keep an explicit `rpc.internal` only if docs say it is not stable.

This should be a separate compatibility decision, not a side effect of moving
files.

Decision (2026-05-09): use `rpc.testing` as the deliberately unstable
test-support facade. The public `rpc._internal` name remains only as a
deprecated, narrow compatibility alias to that facade; in-tree tests should not
import `_internal` directly.

## Zig 0.17-Dev Modernization Work To Weave In

Do these alongside or shortly after Option 1, not in the middle of delicate peer
semantic changes:

- Decide whether the project still supports Zig 0.16 or formally moves to
  Zig 0.17-dev/master.
- Update `minimum_zig_version`, `mise.toml`, and docs together if moving.
- Make module surfaces explicit:
  - core serialization/codegen
  - RPC without QUIC by default
  - QUIC as an opt-in transport surface via `-Dquic=true`
- Avoid unconditional quic-zig/BoringSSL instantiation for serialization-only users.
- Revisit `std.Io.Evented` now that local Zig master exposes it on macOS/aarch64.
- Isolate POSIX wake pipes, polling, and socket options inside `transport/tcp`.
- Modernize examples/tools around `std.process.Init`, `init.gpa`, `init.io`, and
  current `std.Io` APIs.

## Quality Gates

Use these at the end of each tranche:

```bash
zig fmt src/ tests/
zig build check --summary all
zig build test --summary all
```

For transport or hardening-sensitive tranches, also run the focused suites and
gates that exist in the build graph:

```bash
zig build test-rpc --summary all
zig build test-e2e-security --summary all
zig build hardening --summary all
```

If maintaining Zig 0.16 compatibility while developing on master, also run the
same gates under the pinned toolchain before landing a tranche.

## Implementation Notes For Future Sessions

- Start Option 1 with file moves and import updates only.
- Avoid behavior changes until the domain tree is green.
- Keep compatibility aliases in `src/rpc/mod.zig` and `src/rpc/mod_core.zig`.
- Update docs and path-based hardening tests in the same tranche as the path
  move that affects them.
- Treat QUIC split as a second step after moving it under `transport/quic`.
- Do not start Option 3 until Option 1 is committed, pushed, and stable.
- In Option 3, extract state first, then dispatch families, then caps and
  pipeline internals.
