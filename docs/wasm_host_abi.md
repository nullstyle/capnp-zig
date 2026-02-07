# WASM Host ABI (Language-Neutral)

Updated: 2026-02-07
Status: Draft v1
Source implementation: `src/wasm/capnp_host_abi.zig`

## Purpose

This document defines a low-level WebAssembly ABI for driving Cap'n Proto
message/RPC logic from any host language/runtime.

Examples of compatible hosts:
- Deno
- Node.js
- Bun
- Rust (`wasmtime`, `wasmer`, `wasmi`)
- Go (`wazero`)
- C/C++ hosts

The ABI is intentionally minimal:
- all exported functions are synchronous,
- all parameters/returns are numeric scalars (`u32`),
- host owns async transport/event loop behavior.

## ABI Versioning

```c
u32 capnp_wasm_abi_version();
u32 capnp_wasm_abi_min_version();
u32 capnp_wasm_abi_max_version();
u32 capnp_wasm_feature_flags_lo();
u32 capnp_wasm_feature_flags_hi();
```

- Current value: `1`.
- Minimum compatible value: `1`.
- Maximum compatible value: `1`.
- Host must validate this at startup.
- If version mismatches, host should fail fast.

Feature flags are encoded as a 64-bit bitset split into low/high `u32` words:

- bit `0`: ABI min/max negotiation exports are present.
- bit `1`: `capnp_error_take(...)` is present.
- bit `2`: outbound queue introspection + set/get limits exports are present.
- bit `3`: host-call bridge pop/respond exports are present.
- bit `4`: lifecycle helper exports (`capnp_peer_send_finish/release`) are present.
- bit `5`: schema manifest export (`capnp_schema_manifest_json`) is present.
- bit `6`: host-call frame release export
  (`capnp_peer_free_host_call_frame`) is present.
- bit `7`: bootstrap-stub identity export
  (`capnp_peer_set_bootstrap_stub_with_id`) is present.

## Types and Conventions

- Integer type: all ABI scalars are unsigned 32-bit (`u32`).
- Boolean return convention:
  - `1` means success/true
  - `0` means false/no-value or failure (check error API when failure is possible)
- Pointer convention:
  - pointers are offsets into wasm linear memory
  - `0` is null
- Length convention:
  - lengths are bytes
  - `(ptr=0, len=0)` is valid for empty buffers

## Memory API

```c
u32 capnp_alloc(u32 len);
void capnp_free(u32 ptr, u32 len);
void capnp_buf_free(u32 ptr, u32 len);
```

Semantics:
- `capnp_alloc(len)` returns a pointer to at least `len` bytes.
- `capnp_alloc(0)` is allowed and returns a non-zero pointer suitable for later
  `capnp_free(ptr, 0)`.
- `capnp_alloc` returns `0` on failure and sets error state.
- `capnp_free`/`capnp_buf_free` are no-ops for `ptr == 0`.
- `capnp_buf_free` is an alias of `capnp_free`.

Ownership:
- Host must free any buffer allocated by `capnp_alloc`.
- For output buffers returned by ABI functions, host must free using
  `capnp_buf_free` (or `capnp_free` if needed).

## Error API

```c
u32 capnp_last_error_code();
u32 capnp_last_error_ptr();
u32 capnp_last_error_len();
void capnp_clear_error();
u32 capnp_error_take(u32 out_code_ptr, u32 out_msg_ptr_ptr, u32 out_msg_len_ptr);
```

Semantics:
- Error state is process-global per wasm instance.
- Most mutating calls clear previous error state on entry.
- On failure, `capnp_last_error_code() != 0`.
- `capnp_last_error_ptr/len` identify a UTF-8 error message in wasm memory.
- `capnp_error_take(...)` snapshots and clears current error state atomically:
  - returns `1` if an error was present (and writes `code/msg_ptr/msg_len`)
  - returns `0` if no error was present (and writes `0/0/0`)
  - validates all output pointers; on invalid args, returns `0` and sets
    `ERROR_INVALID_ARG`

Current code values (implementation detail, may expand):
- `1`: alloc error
- `2`: invalid argument
- `3`: unknown peer handle
- `4`: peer create failure
- `5`: peer push failure
- `6`: peer pop failure
- `7`: serde encode failure
- `8`: serde decode failure
- `9`: bootstrap config failure
- `10`: host-call bridge failure
- `11`: peer lifecycle/control send failure

## RPC Peer API

```c
u32 capnp_peer_new();
void capnp_peer_free(u32 peer);
u32 capnp_peer_push_frame(u32 peer, u32 frame_ptr, u32 frame_len);
u32 capnp_peer_pop_out_frame(u32 peer, u32 out_ptr_ptr, u32 out_len_ptr);
void capnp_peer_pop_commit(u32 peer);
u32 capnp_peer_set_bootstrap_stub(u32 peer); // optional/test hook
u32 capnp_peer_set_bootstrap_stub_with_id(u32 peer, u32 out_export_id_ptr); // optional/test hook
u32 capnp_peer_outbound_count(u32 peer);
u32 capnp_peer_outbound_bytes(u32 peer);
u32 capnp_peer_has_uncommitted_pop(u32 peer);
u32 capnp_peer_set_limits(u32 peer, u32 outbound_count_limit, u32 outbound_bytes_limit);
u32 capnp_peer_get_limits(u32 peer, u32 out_count_limit_ptr, u32 out_bytes_limit_ptr);
u32 capnp_peer_pop_host_call(
  u32 peer,
  u32 out_question_id_ptr,
  u32 out_interface_id_ptr,
  u32 out_method_id_ptr,
  u32 out_frame_ptr_ptr,
  u32 out_frame_len_ptr
);
u32 capnp_peer_free_host_call_frame(u32 peer, u32 frame_ptr, u32 frame_len);
u32 capnp_peer_respond_host_call_results(u32 peer, u32 question_id, u32 payload_ptr, u32 payload_len);
u32 capnp_peer_respond_host_call_exception(u32 peer, u32 question_id, u32 reason_ptr, u32 reason_len);
u32 capnp_peer_send_finish(
  u32 peer,
  u32 question_id,
  u32 release_result_caps,
  u32 require_early_cancellation
);
u32 capnp_peer_send_release(u32 peer, u32 cap_id, u32 reference_count);
u32 capnp_schema_manifest_json(u32 out_ptr_ptr, u32 out_len_ptr);
```

### `capnp_peer_new`
- Returns non-zero opaque peer handle on success.
- Returns `0` on failure and sets error state.

### `capnp_peer_free`
- Idempotent-style behavior for unknown handles (no failure return).
- Releases peer resources.

### `capnp_peer_push_frame`
- Input is one complete Cap'n Proto RPC frame.
- Returns `1` on success.
- Returns `0` on error and sets error state.

### `capnp_peer_pop_out_frame`
- Polls one outbound frame generated by peer state transitions.
- `out_ptr_ptr` and `out_len_ptr` are pointers to writable `u32` cells in wasm
  memory.
- Returns `1` when a frame is available and writes `(ptr,len)`.
- Returns `0` when queue is empty and writes `(0,0)`.
- Returns `0` on invalid args/failure and sets error state.

Borrow rule:
- Returned outbound frame bytes are borrowed.
- Host should copy bytes before making more ABI calls.
- Host should call `capnp_peer_pop_commit(peer)` after copy to release/advance.

### `capnp_peer_pop_commit`
- Commits/release last popped outbound frame for that peer.
- Safe to call even if no frame is currently borrowed.

### `capnp_peer_set_bootstrap_stub`
- Optional hook primarily for integration tests.
- Installs a default bootstrap export that returns an exception.
- Returns `1` on success, `0` on error.
- If called repeatedly on the same peer, the initially installed stub is
  retained.
- For deterministic export identity, prefer
  `capnp_peer_set_bootstrap_stub_with_id`.
- Production hosts typically do not need this.

### `capnp_peer_set_bootstrap_stub_with_id`
- Optional hook primarily for integration tests.
- Installs (or reuses) the default bootstrap-stub export and writes its
  installed export id to `out_export_id_ptr`.
- Returns `1` on success; `0` on invalid args/config failure.
- Repeated calls on the same peer return the same export id.

### `capnp_peer_outbound_count` / `capnp_peer_outbound_bytes`
- Return current queued outbound frame count/bytes for the peer.
- Return `0` and set error on unknown handle.

### `capnp_peer_has_uncommitted_pop`
- Returns `1` if a frame was popped by `capnp_peer_pop_out_frame` and not yet
  committed by `capnp_peer_pop_commit`.
- Returns `0` otherwise.

### `capnp_peer_set_limits` / `capnp_peer_get_limits`
- Configure/read outbound queue limits.
- `outbound_count_limit == 0` means unlimited count.
- `outbound_bytes_limit == 0` means unlimited bytes.
- Limits apply to newly captured outbound frames.

### `capnp_peer_pop_host_call`
- Polls one inbound RPC `Call` that was routed to the host callback bridge.
- Returns `1` and writes:
  - question id (`u32`)
  - interface id (`u64`)
  - method id (`u16`)
  - owned call frame pointer/length (`ptr,len`)
- Returns `0` with zeroed outputs when queue is empty.
- Host owns returned `frame` buffer and must release it with
  `capnp_peer_free_host_call_frame`.

### `capnp_peer_free_host_call_frame`
- Releases a frame previously returned by `capnp_peer_pop_host_call`.
- Returns `1` on success; `0` on invalid args/unknown peer.
- Passing `frame_len == 0` is a no-op success.

### `capnp_peer_respond_host_call_results`
- Sends a `Return.results` for a queued host call question.
- `payload_ptr/len` points to a Cap'n Proto message whose root is the return
  AnyPointer payload.
- Returns `1` on success; `0` on error.

### `capnp_peer_respond_host_call_exception`
- Sends a `Return.exception` for a queued host call question.
- `reason_ptr/len` is UTF-8 reason text.
- Returns `1` on success; `0` on error.

### `capnp_peer_send_finish`
- Sends a `Finish` control message from host to remote peer.
- `release_result_caps` and `require_early_cancellation` are boolean-like `u32`
  flags (`0` or `1` only).
- Returns `1` on success; `0` on validation/send failure.

### `capnp_peer_send_release`
- Sends a `Release` control message from host to remote peer.
- `cap_id` is the imported capability id and `reference_count` is release amount.
- Returns `1` on success; `0` on send failure.

### `capnp_schema_manifest_json`
- Returns deterministic generated schema/serde metadata as UTF-8 JSON bytes.
- On success, writes `(ptr,len)` and returns `1`.
- Host owns returned buffer and must free with `capnp_buf_free`/`capnp_free`.

## Required Host Pump Behavior

After every successful `capnp_peer_push_frame`, host must drain outbound frames:

1. Call `capnp_peer_pop_out_frame` in a loop.
2. If return is `1`, copy the `(ptr,len)` bytes, then call
   `capnp_peer_pop_commit`.
3. If return is `0` and `(ptr,len) == (0,0)`, outbound queue is empty.
4. Preserve frame ordering when sending to transport.

One inbound frame may produce multiple outbound frames.

## Serde API Pattern (Schema-Specific)

Generated exports should follow:

```c
u32 capnp_<schema>_<type>_to_json(
  u32 frame_ptr,
  u32 frame_len,
  u32 out_json_ptr_ptr,
  u32 out_json_len_ptr
);

u32 capnp_<schema>_<type>_from_json(
  u32 json_ptr,
  u32 json_len,
  u32 out_frame_ptr_ptr,
  u32 out_frame_len_ptr
);
```

Current live example exports:
- `capnp_example_person_to_json`
- `capnp_example_person_from_json`

Semantics:
- Return `1` on success and write output `(ptr,len)`.
- Return `0` on error and set error state.
- Host frees successful output with `capnp_buf_free`/`capnp_free`.

## Minimal Host Call Pattern

For a typical inbound RPC frame:

1. Allocate wasm input buffer with `capnp_alloc`.
2. Copy inbound bytes into wasm memory.
3. Call `capnp_peer_push_frame`.
4. Loop `capnp_peer_pop_out_frame` and commit each popped frame.
5. Free temporary input buffer.

For any ABI call returning failure:

1. Read `capnp_last_error_code`.
2. Read message bytes from `capnp_last_error_ptr/len`.
3. Map to host-native error type.

## Non-Goals

- Defining host-language bindings in this document.
- Defining transport APIs (TCP/WebSocket/IPC).
- Defining async callback semantics from wasm to host.

Those are host-layer concerns built on top of this ABI.
