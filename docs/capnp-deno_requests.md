# capnp-zig Additions Needed For capnp-deno (Post-24f9197)

Updated: 2026-02-07 Evaluated submodule commit: `vendor/capnp-zig@24f9197`

## Status summary

Compared to the prior checkpoint, the major wasm-host ABI requests are now
landed:

1. ABI min/max + feature flags exports: landed
2. `capnp_error_take(...)`: landed
3. Outbound queue counters + limits helpers: landed
4. Host call bridge exports: `capnp_peer_pop_host_call`,
   `capnp_peer_respond_host_call_results`,
   `capnp_peer_respond_host_call_exception`: landed
5. Lifecycle helper exports: `capnp_peer_send_finish`,
   `capnp_peer_send_release`: landed
6. Schema manifest export: `capnp_schema_manifest_json(...)`: landed

`capnp-deno` real-wasm integration now passes against this revision after
adapting tests to the new host-bridge default behavior.

## New integration findings from this bump

## P0: RPC fixture generator does not match wasm-host defaults

Observed:

- `src/wasm/capnp_host_abi.zig` now enables host call bridge by default via
  `enableHostCallBridge()`.
- `tools/gen_rpc_fixtures.zig` still builds fixtures from a plain `HostPeer`
  that does not enable that bridge.
- Result: generated fixtures and actual wasm behavior diverge for bootstrap and
  bootstrap-cap call flows.

Required upstream change:

1. Update `tools/gen_rpc_fixtures.zig` to generate fixtures through the same
   runtime configuration used by `capnp_host_abi.zig` (bridge enabled), or add a
   dedicated `--wasm-host-mode` fixture path and make that the output consumed
   by `capnp-deno`.
2. Add a Zig test that roundtrips fixture generation against wasm-host behavior
   to prevent drift.

## P0: Host call frame ownership cannot be released from wasm ABI

Observed:

- `capnp_peer_pop_host_call(...)` returns frame pointer/len.
- `HostPeer` requires `freeHostCallFrame(...)` ownership release.
- No exported wasm function currently frees popped host-call frame buffers.

Risk:

- Long-running hosts that bridge many inbound calls can leak memory in wasm.

Required upstream change:

1. Add `capnp_peer_pop_host_call_commit(peer)` (commit/consume pattern), or
   `capnp_peer_free_host_call_frame(peer, frame_ptr, frame_len)`.
2. Document ownership semantics in `docs/wasm_host_abi.md`.
3. Add regression coverage for repeated pop/free cycles.

## P1: Bootstrap stub helper should expose deterministic identity semantics

Observed:

- Default bridge bootstrap occupies export `0`.
- `capnp_peer_set_bootstrap_stub(...)` installs stub later, which becomes a new
  export id (`1` in current behavior).
- The helper currently returns `u32` success/failure only, not the export id.

Impact:

- Hosts/tests cannot deterministically target the installed stub without an
  extra bootstrap handshake.

Recommended upstream change:

1. Evolve helper to either:
   - return installed export id, or
   - replace existing bootstrap export in-place and keep stable id semantics.
2. Document the behavior explicitly.

## Recommended upstream patch order

1. Fixture generator parity with wasm-host defaults.
2. Host-call frame release export + ownership docs.
3. Bootstrap-stub identity semantics cleanup.

## capnp-deno work that can proceed now

1. Add first-class TS wrappers for host-call bridge exports in `src/abi.ts`.
2. Build `RpcServerBridge` wiring directly on wasm host-call queue/response
   APIs.
3. Replace remaining fixture-byte assertions with semantic decode assertions
   where behavior is intentionally dynamic.
