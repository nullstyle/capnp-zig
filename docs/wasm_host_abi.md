# WASM Host ABI (Language-Neutral)

Updated: 2026-09-02
Status: Draft v1
Source implementation: `src/wasm/capnp_host_abi.zig`

## Purpose

This document defines a low-level WebAssembly ABI for driving Cap'n Proto
message/RPC logic from any host language/runtime.

"Language-neutral" describes the **host language**, not a universal ABI shared
by unrelated wasm modules. The `capnp_peer_*` exports, polled host-call bridge,
schema/serde exports, error codes, and feature-bit assignments belong to
capnp-zig. Other projects can reuse the memory, scalar, error, and ownership
conventions under their own ABI name and version. The import-bearing variant
below is design guidance for those projects, not an implemented capnp feature.

Examples of compatible hosts:

- Deno
- Node.js
- Bun
- Rust (`wasmtime`, `wasmer`, `wasmi`)
- Go (`wazero`)
- C/C++ hosts

The ABI is intentionally minimal:

- all exported functions are synchronous,
- all parameters and non-void returns are numeric scalars (`u32`),
- host owns async transport/event loop behavior.

The shipped wasm module has no imports. Hosts serialize calls into each
instance and dispatch host work only after the active export returns.

## Build Recipe

Use the Zig version pinned in `mise.toml`:

```sh
mise install
mise exec -- zig build wasm-host --summary all
```

The artifact is `zig-out/bin/capnp_wasm_host.wasm`; `wasm-deno` is a compatibility
alias for the same build step. With the default top-level Debug configuration,
the wasm module uses **ReleaseSmall**. An explicit top-level release mode is
inherited. Override only the wasm module with `-Dwasm-optimize`:

```sh
mise exec -- zig build wasm-host -Dwasm-optimize=ReleaseSmall
mise exec -- zig build wasm-host -Dwasm-optimize=Debug
mise exec -- zig build wasm-host -Dwasm-optimize=ReleaseSafe
```

ReleaseSmall is recommended for distribution. Debug and ReleaseSafe are useful
when diagnosing failures; measure size using the same compiler and optimization
mode before comparing artifacts. The wasm override does not change native
library, tool, or test optimization.

For downstream build scripts, `build/modules.zig` uses a `wasm32-freestanding`
target, `.entry = .disabled`, `.rdynamic = true`, and `.export_memory = true`,
with 4 MiB initial memory and 64 MiB maximum memory. The core, generated schema,
and wasm root modules all use the wasm target and selected wasm optimization.
Memory may grow up to the maximum; hosts must reacquire views of linear memory
after a wasm call that can allocate (including a nested allocator call).

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

Feature flags are encoded as a 64-bit bitset split into low/high `u32` words.
These bit meanings are local to the capnp ABI. Reusing the bitset convention
does not reserve these bits in another project's ABI or imply compatibility.

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
- bit `8`: raw Return-frame response export
  (`capnp_peer_respond_host_call_return_frame`) is present.
- bit `9`: host-call parameter capabilities are retained until the response
  settles them according to `releaseParamCaps`, rather than released when the
  call is queued for the host.

The v1 discovery exports retain their `capnp_wasm_` names for compatibility;
the other exports use `capnp_`. Hosts must use the names listed here. New ABIs
should choose one project prefix consistently (for example, `<project>_alloc`
and `<project>_abi_version`). Renaming existing symbols requires a compatibility
plan, not a documentation-only change.

## Types and Conventions

- Integer type: all ABI scalars are unsigned 32-bit (`u32`).
- To carry a `u64` as scalars in a new ABI, use `(lo, hi)` words, with
  `value = lo | (u64(hi) << 32)`; use the same order for feature flags. Values
  stored in linear memory are little-endian. Scalar widths do not imply that
  every output cell has width four: the capnp host-call interface id occupies
  eight bytes and its method id occupies two.
- Boolean return convention:
  - `1` means success/true
  - `0` means false/no-value or failure (check error API when failure is possible)
- Pointer convention:
  - pointers are offsets into wasm linear memory
  - `0` is null
- Length convention:
  - lengths are bytes
  - `(ptr=0, len=0)` is valid for empty buffers
- Inputs are borrowed only for the duration of the export: any data needed
  later is copied or decoded into module-owned storage before return. Calling
  an export does not transfer ownership of its input allocation.

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
- For a non-null free, pass the original allocation base and the **exact
  requested length** (or the exact returned length for an owned output).
  Interior pointers, double frees, and rounded or otherwise mismatched lengths
  set error `12` (`ERROR_INVALID_FREE`). A length mismatch on a live allocation
  leaves it live; retry with its correct base and length.
- Memory exports clear prior error state on entry in capnp v1, including a
  null free. Capture an error before allocating scratch storage or cleaning up.
  Do not copy this policy into re-entrant memory exports; see Re-entrancy.

### Allocation Tracking and Pointer Validation

On wasm32, non-empty input ranges and writable output cells must fit wholly
within the requested extent of a live tracked allocation. Allocate host input
and scratch storage with `capnp_alloc`; an interior subrange is valid for input
or output cells, but not for freeing. Merely being within linear memory is
insufficient: static data, stack addresses, outbound borrows, and peer-owned
host-call frames are not in this allocation table. Copy such bytes into a
`capnp_alloc` buffer before passing them as export inputs.

Validation rejects null non-empty ranges, address arithmetic overflow, and
untracked or overlong ranges. Output cells are checked for their full byte
width; use distinct, non-overlapping cells. Ordinary pointer validation failures
set error `2` (`ERROR_INVALID_ARG`). Empty input slices require no dereference;
individual exports may still reject an empty payload.

The per-instance tracked-allocation budgets are **1,024 allocations** and
**32 MiB** of recorded allocation sizes. They cover `capnp_alloc` and owned
schema/serde output buffers. `capnp_alloc(0)` consumes one allocation and one
byte of budget, but exposes a zero-byte usable extent and is freed with length
zero. These are not total heap limits: allocator overhead, peers, queued frames,
and host-call frames are separate. The default peer limit is 128; each peer's
outbound queue defaults to 1,024 frames and 1 MiB, configurable below.

Host-call frames have their own per-peer identity/length table and release
export. Native test/library builds use native-width pointers and disable strict
range validation by default; they are not the wasm32 ABI. Allocation identity
and exact-length free checks still apply.

### Ownership Table

Every buffer-producing export's documentation must state **OWNED** or
**BORROWED**, its lifetime, and its release operation. Names alone do not imply
ownership: both pop exports below produce frames with different lifetimes.

| Producer | Ownership and lifetime | Host action |
|---|---|---|
| `capnp_alloc` | OWNED until freed or shutdown | `capnp_free(ptr, requested_len)` |
| `capnp_peer_pop_out_frame` | BORROWED until that peer's pop commit or destruction, or shutdown | Copy, then `capnp_peer_pop_commit(peer)`; never `buf_free` |
| `capnp_peer_pop_host_call` | OWNED by host for release, tied to the originating peer's lifetime | `capnp_peer_free_host_call_frame(peer, ptr, len)`; never `buf_free` |
| `capnp_schema_manifest_json` | OWNED until freed or shutdown | `capnp_buf_free(ptr, len)` |
| `capnp_example_person_to_json` / `capnp_example_person_from_json` | OWNED until freed or shutdown | `capnp_buf_free(ptr, len)` |
| `capnp_last_error_ptr` with `capnp_last_error_len` | BORROWED shared error storage, overwritten by the next error | Copy immediately; never free |
| `capnp_error_take` message output | BORROWED shared error storage, overwritten by the next error | Copy immediately after take; never free |

For new exports, use `_pop_*` plus `_pop_commit` for a two-phase borrow,
`_buf_free` for ordinary owned outputs, and an explicit owner-specific release
name for outputs with separate lifetime tracking. Always document exceptions
and include OWNED/BORROWED in the source doc comment. Scalar outputs written
into host scratch cells do not transfer ownership of the scratch allocation.

## Error API

```c
u32 capnp_last_error_code();
u32 capnp_last_error_ptr();
u32 capnp_last_error_len();
void capnp_clear_error();
u32 capnp_error_take(u32 out_code_ptr, u32 out_msg_ptr_ptr, u32 out_msg_len_ptr);
```

Semantics:

- One error slot is shared by all calls within a wasm instance.
- Memory, peer (including introspection), schema/serde, and shutdown calls clear
  previous error state on entry. Version/feature queries and `last_error_*`
  getters leave it intact. `capnp_clear_error` explicitly clears it.
- On failure, `capnp_last_error_code() != 0`.
- Error state is sticky until an export clears or replaces it. Inspect it
  immediately, including after void-returning operations such as `capnp_free`.
- `capnp_last_error_ptr/len` identify BORROWED diagnostic bytes in wasm memory.
- `capnp_error_take(...)` snapshots and clears current error state atomically:
  - returns `1` if an error was present (and writes `code/msg_ptr/msg_len`)
  - returns `0` if no error was present (and writes `0/0/0`)
  - validates all output pointers; on invalid args, returns `0` and sets
    `ERROR_INVALID_ARG`
  - returns a BORROWED message pointer, not an allocated copy; clearing the
    code/length does not erase the bytes, but the next error can overwrite them

Allocate error scratch cells **before** the operation that may fail. One
`capnp_alloc(12)` scratch buffer can hold the three wasm32 `u32` outputs: pass
`scratch`, `scratch + 4`, and `scratch + 8`. The three-argument signature still
validates each cell separately; it does not require three allocations. Copy the
message into host memory before cleanup or further calls. "Atomic" describes
the snapshot-and-clear operation, not a durable message copy or a re-entrancy
guarantee.

Error messages use a fixed 1 KiB buffer, are byte-truncated to that limit, and
have no required NUL terminator. Messages containing known path, source-location,
stack-trace, or control-character markers are replaced with `internal error`.
This is a diagnostic scrub, not a general-purpose sanitizer. Decode defensively
if truncation splits UTF-8, and use error codes rather than matching text.

Current code values (implementation detail, may expand):

- `0`: no error
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
- `12`: invalid free (unknown allocation or exact-length mismatch)

## Re-entrancy

The capnp v1 module has **no synchronous wasm-to-host callbacks** and therefore
no supported export subset to invoke from an import frame. Calls into an
instance must be serialized. Its host-call bridge queues work for the host to
poll after an export returns; it does not invoke host code mid-transition.
Finish copying and committing a borrowed outbound frame before dispatching host
callbacks or calling code that may re-enter the module.

An ABI that adds synchronous imports must explicitly define its re-entrant-safe
subset. A useful minimal contract permits only `alloc`, `free`, and a documented
`buf_free` alias from inside an import. State-machine operations, pop/commit,
destruction, shutdown, and error take/clear remain forbidden while an outer
transition is active. Reject prohibited re-entry before clearing errors or
touching state; allocator access must itself be safe at that call site. Copying
capnp's native non-recursive mutex would not make nested calls safe.

In that variant, successful memory exports **must not clear a pending outer
error**. Memory failures must remain observable without overwriting an earlier
driver fault; keep a separate per-transition fault latch or explicitly preserve
the first fault. Clear the outer error/fault state once, on entry to the outer
operation. Disabling clear-on-entry alone does not prevent a later error from
replacing a pending fault.

A borrowed popped output must not stay live across a callback that may re-enter
the module: copy and commit it first. This differs from an input deliberately
borrowed by a synchronous import, whose lifetime and allowed nested calls must
be specified as part of that import's contract.

## Import-Bearing Variant (Guidance for Other ABIs)

Some state machines need an immediate driver decision while processing input
and cannot suspend to service a polled bridge. Synchronous imports can reuse
the scalar and ownership conventions above under a separate ABI contract:

1. Pass byte slices and output-cell addresses to a named import. Import input
   slices are BORROWED for that call only; the host must not retain or mutate
   them. The module keeps the underlying state stable across allowed allocator
   re-entry. Copy host-side views before nested allocation, or reacquire them
   afterwards if linear memory grows.
2. To return bytes, the host calls the module's `alloc` from the import, fills
   that OWNED allocation, writes its pointer and exact length, and transfers it
   to the module when the import returns successfully. Use `(0,0)` for empty.
   **After the import returns**, the module validates the result, copies/decodes
   it into its own storage, and frees the temporary allocation before the outer
   export returns, including on decode failure. The host must not free the
   result before the module can read it. On failed imports, the host releases
   any allocation it has not transferred.
3. State which checks apply in each direction. Host-to-export arguments and
   imported result allocations can use the tracked-allocation table. Addresses
   the module lends to an import may be shadow-stack output cells or interior
   engine slices, which are not tracked host allocations. Validate their linear
   memory bounds and declared widths/lifetimes; permit writes only to the exact
   output cells the module supplied. Do not weaken export validation to admit
   arbitrary pointers just because imports have a different provenance.
4. Define an explicit import success/fault return and a fault latch that survives
   nested memory calls. A trap/host exception needs its own defined failure
   handling; it is not automatically translated into an ABI error. Prefer to
   propagate a driver fault as outer `0` plus a nonzero error code, and document
   whether earlier state changes/effects remain committed. If the outer export
   can return `1` with a driver fault pending, hosts **must inspect the fault
   channel after every outer operation**, including successful returns. The
   failure-only checking rule for capnp v1 is insufficient for that variant.

Use `(lo, hi)` for scalar `u64` inputs/results and feature words. A new ABI may
also use one fixed-layout output record (for example, `u32[3]` for an error
snapshot); define byte widths, offsets, endianness, ownership, and validation
for the whole record. These are choices for that ABI, not changes to the
existing capnp v1 signatures.

## RPC Peer API (Capnp-Specific)

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
u32 capnp_peer_respond_host_call_return_frame(u32 peer, u32 return_frame_ptr, u32 return_frame_len);
u32 capnp_peer_send_finish(
  u32 peer,
  u32 question_id,
  u32 release_result_caps,
  u32 require_early_cancellation
);
u32 capnp_peer_send_release(u32 peer, u32 cap_id, u32 reference_count);
u32 capnp_schema_manifest_json(u32 out_ptr_ptr, u32 out_len_ptr);
void capnp_shutdown();
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

- Returned outbound frame bytes are BORROWED until commit, peer destruction,
  or shutdown. Do not free them with `capnp_buf_free`.
- Copy bytes and call `capnp_peer_pop_commit(peer)` before dispatching callbacks
  or making further state-changing calls.
- A second pop before commit returns `0` with error `6` (`ERROR_PEER_POP`),
  leaving the existing borrow intact. It does not advance or silently commit.
  `capnp_peer_has_uncommitted_pop` exposes this state. Other ABIs may choose an
  implicit-commit policy, but must document it and its invalidation point.

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
  - OWNED call frame pointer/length (`ptr,len`)
- The output cells occupy 4, 8, 2, 4, and 4 bytes respectively on wasm32.
  `out_interface_id_ptr` points to a little-endian `u64`, not a `u32` cell;
  `out_method_id_ptr` points to a little-endian `u16`.
- Returns `0` with zeroed outputs when queue is empty.
- Host owns returned `frame` buffer and must release it with
  `capnp_peer_free_host_call_frame` using the originating peer and exact length.
  Replying does not free this buffer. Destroying the peer or shutting down
  reclaims it and invalidates any host reference.

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

### `capnp_peer_respond_host_call_return_frame`
- Accepts a complete encoded RPC `Return` frame. Results preserve the supplied
  payload, capability table, and flags; `answerId` selects the pending host call.
- Exceptions are rebuilt with the default reason `host call failed`, empty
  trace, and type `0`, preserving `answerId`, `releaseParamCaps`, and
  `noFinishNeeded`.
- For this host-built Return, `releaseParamCaps == 0` makes the host responsible
  for later parameter-capability Releases via `capnp_peer_send_release`.
  `releaseParamCaps == 1` already releases those references; do not also send
  separate Releases for them.
- The input is borrowed for the export call only, like other frame inputs.
- Returns `1` on success; `0` for empty/invalid input, non-Return frames,
  invalid Return semantics, unknown/stale answer ids, or send failure.

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
- Output is OWNED; free with `capnp_buf_free`/`capnp_free`.

### `capnp_shutdown`
- Destroys every peer, reclaims all tracked allocations and outstanding frames,
  and clears error state. No previously returned allocation or peer handle may
  be used afterwards.

## Required Host Pump Behavior (Capnp-Specific)

After every successful `capnp_peer_push_frame`, host must drain outbound frames:

1. Call `capnp_peer_pop_out_frame` in a loop.
2. If return is `1`, copy the `(ptr,len)` bytes, then call
   `capnp_peer_pop_commit`.
3. If return is `0`, check the error code before making another call. Only
   error code `0` with `(ptr,len) == (0,0)` means the queue is empty; outputs
   must not be consumed on failure.
4. Preserve frame ordering when sending to transport.

One inbound frame may produce multiple outbound frames.

Dispatch queued host calls via `capnp_peer_pop_host_call` only after the current
export returns and any outbound borrow is committed. Copy or consume each
owned call frame, release it with its peer-specific release export, and respond
using a host-call response export. As with outbound pops, a host-call pop that
returns `0` means empty only when the error code is also `0`. Responses and
lifecycle sends can produce more outbound frames; drain those too. This is the
polled bridge, not the synchronous-import variant described above.

## Serde API Pattern (Capnp- and Schema-Specific)

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
- Output is OWNED; host frees it with `capnp_buf_free`/`capnp_free`.

Per-type serde exports are one integration pattern, not a requirement for
downstreams. An alternative is a fixed set of push/pop exports carrying
Cap'n Proto-encoded input/effect frames from a host schema. That keeps the
function count independent of payload types and lets encoded frames serve as
conformance fixtures. Specify framing, schema evolution, and ownership in that
ABI; compare artifact sizes under matched build settings rather than assuming
either pattern is always smaller.

## Minimal Host Call Pattern

For a typical inbound RPC frame:

1. Allocate reusable output/error scratch cells before starting operations.
2. Allocate the wasm input buffer with `capnp_alloc` and copy inbound bytes.
3. Call `capnp_peer_push_frame`; on failure, copy the error immediately.
4. After a successful push, loop `capnp_peer_pop_out_frame`, checking for errors
   and copying/committing each popped frame before proceeding.
5. Service any queued host calls and drain frames produced by responses.
6. Free temporary input storage with its exact original length; check the error
   code after the void-returning free.

For any ABI call returning failure:

1. Read `capnp_last_error_code`.
2. Read message bytes from `capnp_last_error_ptr/len`.
3. Map to host-native error type.

Alternatively, use `capnp_error_take` with the preallocated scratch cells and
copy its borrowed message immediately. Preserve the original error in host
memory before cleanup, because memory exports clear it. A failed operation
does not imply rollback of already-applied state changes or queued frames.

## Non-Goals

- Defining host-language bindings in this document.
- Defining transport APIs (TCP/WebSocket/IPC).
- Defining async callback semantics from wasm to host.

Those are host-layer concerns built on top of this ABI.
