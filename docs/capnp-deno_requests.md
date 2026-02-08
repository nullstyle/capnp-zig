# capnp-zig WASM ABI Spec: Host-Call Raw Return Frame

Updated: 2026-02-08\
Status: Proposed (request from `capnp-deno`)

## Problem

`capnp-deno` can generate/handle full Cap'n Proto RPC `Return` messages, but the
current wasm host-call response ABI only exposes:

- `capnp_peer_respond_host_call_results(peer, question_id, payload_ptr, payload_len)`
- `capnp_peer_respond_host_call_exception(peer, question_id, reason_ptr, reason_len)`

This cannot express:

- non-empty `Return.results.capTable`
- non-default `releaseParamCaps` / `noFinishNeeded` flags
- future advanced `Return` forms without adding new per-field ABI exports

## Required ABI Addition

Add this export:

```c
u32 capnp_peer_respond_host_call_return_frame(
  u32 peer,
  u32 return_frame_ptr,
  u32 return_frame_len
);
```

## Capability/Version Signaling

- Add feature-flag bit `8` = `HOST_CALL_RETURN_FRAME`.
- Keep existing exports behavior unchanged.
- Backward compatibility: hosts can continue using
  `capnp_peer_respond_host_call_results` when bit `8` is absent.

## Normative Behavior

On `capnp_peer_respond_host_call_return_frame(...)`:

1. Validate `peer` handle exists.
2. Validate `(ptr,len)` is readable (`len > 0`; `ptr != 0` unless `len == 0`).
3. Parse bytes as one RPC message frame.
4. Require root message tag = `Return`.
5. Require `Return.answerId` maps to an active pending host-call question.
6. If valid, consume that pending host-call question and enqueue/send this
   return exactly as if produced internally.
7. Return `1` on success.

Failure behavior:

- return `0`
- set ABI error state
- do not mutate pending-host-call state on parse/validation failure

## Error Mapping

- invalid pointers/lengths: `ERROR_INVALID_ARG`
- unknown peer: `ERROR_UNKNOWN_PEER`
- malformed frame, non-Return frame, unknown/stale answerId, invalid return
  semantics: `ERROR_HOST_CALL`

Error text should be diagnostic (for example:
`"host-call return frame is not Return"`).

## Memory Ownership

- Input frame memory is host-owned; wasm must not retain borrowed pointers after
  return.
- wasm may copy/parse internally during call.

## Compatibility Contract

- If this export exists, `capnp-deno` will prefer it for host-call results.
- If absent, `capnp-deno` falls back to legacy results/exception exports.
- If both old and new exports exist, both must remain valid.

## Conformance Tests (Required)

Add wasm ABI tests for:

1. Export presence + feature flag bit `8`.
2. Accept valid `Return.results` with:
   - non-empty cap table
   - `releaseParamCaps=false`
   - `noFinishNeeded=true`
3. Accept valid `Return.exception`.
4. Reject non-`Return` frames.
5. Reject malformed/truncated frames.
6. Reject unknown/stale `answerId`.
7. Verify no pending-host-call consumption on invalid frame.

## Optional Follow-up (Not Required For capnp-deno Unblock)

A convenience typed helper export can be added later:

```c
u32 capnp_peer_respond_host_call_results_ex(...);
```

But the raw return-frame export above is sufficient for production use and
future-proofs host-call response semantics.
