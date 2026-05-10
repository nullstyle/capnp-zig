# Release And Migration Notes: 2026-05-10

These notes summarize the current RPC polish, hardening, and refactor state for
the Zig 0.17-dev development line. They are not a tagged release announcement;
they are a handoff note for downstream users moving across the recent RPC
surface cleanup.

## What Changed

- RPC public APIs are now domain-shaped under `capnpc.rpc`: `wire`, `caps`,
  `promises`, `events`, `transport`, `peer`, `integration`, `generated`, and
  `testing`.
- TCP transport remains under `rpc.transport.tcp`. Optional QUIC transport is
  under `rpc.transport.quic` and is still selected with `-Dquic=true`.
- QUIC has a conservative baseline mode and an explicit native mode. The native
  mode keeps standard `rpc.capnp` messages while routing large frames over
  QUIC-native data streams.
- QUIC server fanout is exposed through `rpc.transport.quic.Server` and
  per-session `ServerSession` drivers. The single-session
  `Connection.initServer()` path remains the compatibility entry point.
- RPC observer events are exposed as `rpc.events` with redacted payloads for
  connection lifecycle, frame movement, backpressure, resource rejection,
  protocol errors, and close handling.
- `capnpc.io_backend` centralizes `std.Io` backend selection. The Evented
  selector is covered by `zig build -Dio-backend=evented check` /
  `just check-evented` on targets where Zig exposes `std.Io.Evented`.
- Documentation gates now include `zig build docs-smoke`,
  `zig build test-docs-snippets`, and the QUIC snippet variant
  `zig build -Dquic=true test-docs-snippets-quic`.

## Migration Notes

Update application and generated-code integration points to import the domain
modules directly. The old flat RPC compatibility aliases have been removed:

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
| `rpc._internal` | `rpc.testing` for in-tree tests only; no stable production replacement |

Do not add local shims for these aliases in new code. The canonical mapping
lives in [`rpc-migration-guide.md`](rpc-migration-guide.md).

## Local History Anchors

| Commit/tranche | Release note |
|---|---|
| `50daa66` `rpc: organize runtime by domain` | Moved the RPC implementation into domain folders and introduced the domain-shaped facade. |
| `e28cddf` `rpc: normalize public surface` | Removed old flat aliases from the public facade and made canonical domain paths the migration target. |
| `20cf031` `build: rename RPC test steps by domain` | Made focused RPC suites visible as `test-rpc-wire`, `test-rpc-caps`, `test-rpc-promises`, `test-rpc-transport`, `test-rpc-peer`, `test-rpc-integration`, and `test-rpc-quic`. |
| `52a4561` QUIC dependency rename | Completed the dependency naming cleanup used by the QUIC docs and build surface. |
| `477434d` `quic: add native control and data stream mode` | Added the opt-in native QUIC wire shape while preserving standard RPC payloads above transport framing. |
| `f1f225b` `quic: add multi-session server fanout` | Added the `rpc.transport.quic.Server` / `ServerSession` fanout API. |
| `b35b612` `io: enable evented backend selection` | Added the explicit Evented backend selector and no-link check path. |
| `0459344` `rpc: add redacted event observer` | Made redacted, transport-general RPC events visible through `rpc.events`. |
| `bc3f60b` `tools: add docs examples smoke gate` | Added the docs/examples public API smoke gate. |
| `a47fa89` `docs: add migration and snippet smoke gates` | Added migration-guide coverage and snippet fixture compilation. |
| `f9e257c` / `393c3ea` release-polish gates | Expanded CI/docs release polish checks around the surfaced APIs. |

## Still Outstanding

- `rpc.transport.quic.Server` is poll-driven and does not yet provide a
  high-level accept-event abstraction.
- QUIC native mode carries complete RPC frames only; application-level streaming
  params/results are still future work.
- Higher-level generated-client pipelining ergonomics and packed RPC streams
  remain open design items in the RPC runtime plan.
