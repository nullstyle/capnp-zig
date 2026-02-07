# capnp-zig Additions Needed For capnp-deno (Post-Checkpoint)

Updated: 2026-02-07 Evaluated submodule commit: `vendor/capnp-zig@8f83a84`

## Why this update exists

`capnp-zig` moved significantly since the last report (large RPC refactor,
decomposition, and safety/test hardening), so this document focuses only on what
is still required upstream for `capnp-deno` runtime and codegen goals.

## What changed in `8f83a84` that matters to us

- RPC internals were heavily decomposed (`peer_*` orchestration modules), which
  makes future host-ABI surface work easier to implement safely.
- Safety and test depth improved (reader/message overflow hardening and broader
  RPC coverage).
- `HostPeer` and detached peer pump patterns remain available and stable.
- **WASM host ABI remained effectively v1/minimal**
  (`src/wasm/capnp_host_abi.zig` still exports only basic alloc/error/peer
  push-pop APIs plus example serde).

## Current gap matrix

1. ABI negotiation + feature flags: **missing**
2. Atomic error-take API (instance-safe host correlation): **missing**
3. Outbound queue introspection + host-configurable limits: **missing**
4. Server callback bridge ABI (host handles inbound calls): **missing**
5. Structured schema/serde manifest export: **missing**
6. Lifecycle helper exports (`finish`/`release` wrappers): **missing**

Notes:

- `capnp-deno` now has local feature-detection shims (`src/abi.ts`) ready to
  consume these when they land.
- The checkpoint did not introduce the requested ABI symbols in
  `src/wasm/capnp_host_abi.zig` or `docs/wasm_host_abi.md`.

## Required upstream additions (updated priority)

## P0-A: Host callback bridge ABI (highest impact)

Target files:

- `src/wasm/capnp_host_abi.zig`
- `src/rpc/host_peer.zig`
- `src/rpc/peer_call_orchestration.zig`
- `src/rpc/peer_return_orchestration.zig`
- `tests/rpc_host_peer_test.zig`

Add exports:

- `capnp_peer_pop_host_call(...)`
- `capnp_peer_respond_host_call_results(...)`
- `capnp_peer_respond_host_call_exception(...)`

Reason:

- Removes TS-side protocol duplication for server dispatch and capability
  lifecycles.
- Leverages new modular peer orchestration introduced in this checkpoint.

## P0-B: ABI negotiation + error ownership

Target files:

- `src/wasm/capnp_host_abi.zig`
- `docs/wasm_host_abi.md`
- `tests/wasm_host_abi_test.zig` (new)

Add exports:

- `capnp_wasm_abi_min_version()`
- `capnp_wasm_abi_max_version()`
- `capnp_wasm_feature_flags_lo()`
- `capnp_wasm_feature_flags_hi()`
- `capnp_error_take(out_code_ptr, out_msg_ptr_ptr, out_msg_len_ptr)`

Reason:

- Enables deterministic host-side feature negotiation and safer error
  correlation under concurrent host workflows.

## P0-C: Queue/backpressure and limits introspection

Target files:

- `src/wasm/capnp_host_abi.zig`
- `src/rpc/host_peer.zig`
- `tests/rpc_host_peer_test.zig`

Add exports:

- `capnp_peer_outbound_count(peer)`
- `capnp_peer_outbound_bytes(peer)`
- `capnp_peer_has_uncommitted_pop(peer)`
- `capnp_peer_set_limits(peer, ...)`
- `capnp_peer_get_limits(peer, ...)`

Reason:

- Required for production backpressure and DoS guardrails without recompiling
  wasm.

## P1-A: Schema/serde manifest export

Target files:

- `src/wasm/capnp_host_abi.zig`
- `src/capnpc-zig/generator.zig`
- `docs/wasm_host_abi.md`
- `tests/codegen_generated_runtime_test.zig`

Add export:

- `capnp_schema_manifest_json(out_ptr_ptr, out_len_ptr)`

Reason:

- Replaces export-name heuristics with deterministic generated metadata.

## P1-B: Lifecycle helper ABI

Target files:

- `src/wasm/capnp_host_abi.zig`
- `src/rpc/peer.zig`
- `tests/rpc_peer_test.zig`

Add exports:

- `capnp_peer_send_finish(peer, question_id, release_result_caps, require_early_cancellation)`
- `capnp_peer_send_release(peer, cap_id, reference_count)`

Reason:

- Keeps connection-scoped lifecycle semantics in Zig core and simplifies host
  wrappers.

## Acceptance criteria before next submodule bump

1. New exports documented in `docs/wasm_host_abi.md` with ownership/lifetime
   rules.
2. New symbol tests added (positive + malformed/partial export combinations).
3. `zig build test-rpc --summary all` passes.
4. `just test` passes.
5. `zig build gen-rpc-fixtures` remains deterministic for `capnp-deno`.

## Rollout plan with capnp-deno

1. Land P0-B first (already shimmed on host side).
2. Land P0-C for host-side resource policy parity.
3. Land P0-A to unlock first-class server callbacks without TS protocol
   duplication.
4. Land P1-A and P1-B to remove remaining heuristics/boilerplate.

Observability-specific ABI additions remain deferred until feature-complete RPC
parity.
