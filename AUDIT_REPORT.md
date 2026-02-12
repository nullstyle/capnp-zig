# capnpc-zig Security & Correctness Audit Report (Round 3)

**Date:** 2026-02-11
**Scope:** Full codebase — serialization, codegen, RPC (levels 0-3), WASM ABI
**Method:** 8 parallel assessment tracks covering serialization, RPC L0-1, RPC L2, RPC L3 peer, codegen, WASM ABI, error handling, test quality, and memory lifecycle
**Prior rounds:** Round 1 fixed 23 findings (3 Critical, 8 High, 12 Medium). Round 2 fixed 45 findings (2 High, 14 Medium, 22 Low, 7 Test Gaps). All 619 tests pass.

---

## Summary

| Severity | Count |
|----------|-------|
| Critical | 0 |
| High | 1 |
| Medium | 10 |
| Low | 24 |
| Test Gaps | 9 |

---

## High Severity (1)

### H1: Use-after-free in `Connection.handleRead` after `on_error` callback

**File:** `src/rpc/level2/connection.zig`, lines 150-156 and 165-171

**Description:** When `framer.push()` or `framer.popFrame()` fails, the code calls `self.on_error.?(self, err)` and then accesses `self.framer.reset()`, `self.on_message = null`, and `self.on_error = null`. The `on_error` callback receives the `*Connection` and may call `conn.deinit()` or `allocator.destroy(conn)`, making subsequent `self` dereferences use-after-free. The message-handler error path (line 185-188) correctly captures the callback into a local before calling it, but the framing error paths were not given the same treatment.

A related variant exists in `onTransportClose` (lines 192-201) where `on_close` is checked after `on_error` may have destroyed the connection.

**Fix:** Capture `self.framer`, `self.on_message`, `self.on_error` into locals before the callback, operate on locals afterward. Or restructure so `framer.reset()` and nulling happen before the error callback:

```zig
if (push_result) |_| {} else |err| {
    log.debug("framer push failed: {}", .{err});
    self.framer.reset();
    const cb = self.on_error.?;
    self.on_message = null;
    self.on_error = null;
    cb(self, err);
    return;
}
```

Same pattern for lines 165-171. For `onTransportClose`, capture the close callback into a local before calling the error callback.

---

## Medium Severity (10)

### M1: `debug.assert` bounds check in `U8ListBuilder.setAll` compiled out in release

**File:** `src/serialization/message/list_builders.zig`, line 62

**Description:** Uses `std.debug.assert(self.elements_offset + data.len <= segment.items.len)` which is compiled out in `ReleaseFast`/`ReleaseSmall`, allowing OOB write. The function already returns `!void`.

**Fix:** Replace with `try bounds.checkBoundsMut(segment.items, self.elements_offset, data.len)`. The `bounds` module is already imported.

### M2: `recordResolvedAnswer` destroys old entry before fallible `put`

**File:** `src/rpc/level1/peer_promises.zig`, lines 62-65

**Description:** Unconditionally removes and frees the existing resolved answer, then attempts `put` which can fail with OOM. On failure, old entry is destroyed and new entry is never stored — question_id permanently loses its resolved answer.

**Fix:** Use `getOrPut` (single allocation):
```zig
const entry = try resolved_answers.getOrPut(question_id);
if (entry.found_existing) allocator.free(entry.value_ptr.frame);
entry.value_ptr.* = .{ .frame = frame };
```

### M3: `InboundCapTable.clone()` doesn't track cloned imports in CapTable

**File:** `src/rpc/level0/cap_table.zig`, lines 248-257

**Description:** `clone()` duplicates the entries array including `.imported` entries but does not call `table.noteImport()`. Safe today (only caller's deinit doesn't release), but API is a trap for future callers.

**Fix:** Document the invariant on `clone()` that the clone must NOT be passed to `releaseInboundCaps`, or accept a `*CapTable` parameter and call `noteImport` for each cloned import.

### M4: `Listener.close()` lacks idempotency guard

**File:** `src/rpc/level2/runtime.zig`, lines 179-181

**Description:** Unlike `Transport.close()` which has `if (self.close_requested) return;`, `Listener.close()` unconditionally submits an async close. Double-call causes double-close on the socket fd.

**Fix:** Add `close_requested: bool = false` field and guard:
```zig
pub fn close(self: *Listener) void {
    if (self.close_requested) return;
    self.close_requested = true;
    self.socket.close(...);
}
```

### M5: `StreamState.handleReturn` underflows `in_flight` u32

**File:** `src/rpc/level2/stream_state.zig`, line 25

**Description:** `self.in_flight -= 1` without checking if already 0. Debug-mode panic or silent wrap to `0xFFFF_FFFF` in release.

**Fix:** Add `std.debug.assert(self.in_flight > 0)` or `if (self.in_flight == 0) @panic(...)` before the decrement.

### M6: Duplicate question ID check misses pending promises

**File:** `src/rpc/level3/peer.zig`, lines 1863-1870

**Description:** Inbound call duplicate question ID check verifies `resolved_answers`, `send_results_to_yourself`, and `send_results_to_third_party`, but not `pending_promises` or `pending_export_promises`. A call targeting a still-pending promised answer could pass the duplicate check.

**Fix:** Add checks for `pending_promises` and `pending_export_promises`, or maintain a `pending_answers` set keyed by inbound question_id.

### M7: Inbound caps leaked if `queue_promise_export_call` / `queue_promised_call` fails

**File:** `src/rpc/level3/peer/call/peer_call_orchestration.zig`, lines 168-224 and 256-305

**Description:** `inbound_caps` is initialized, then passed to `queue_promise_export_call` via `try`. If it fails, `inbound_caps` is never deinited because the `defer inbound_caps.deinit()` on line 205 has not been reached. Same for `queue_promised_call` and `queue_export_promise` paths.

**Fix:** Use ownership tracking:
```zig
var inbound_caps = try InboundCapsType.init(...);
var inbound_caps_owned = true;
defer if (inbound_caps_owned) inbound_caps.deinit();
// ... in queue path:
try queue_promise_export_call(peer, export_id, frame, inbound_caps);
inbound_caps_owned = false;
return;
```

### M8: `_deferred` suffix on keyword-escaped VTable field names produces invalid Zig

**File:** `src/capnpc-zig/generator.zig`, lines 727-728, 928, 961, 968

**Description:** When a method name is a Zig keyword (e.g., `type`), `escapeZigKeyword` produces `@"type"`, then `{s}_deferred` produces `@"type"_deferred` which is invalid Zig syntax.

**Fix:** Apply `_deferred` suffix BEFORE keyword escaping:
```zig
const deferred_field = try std.fmt.allocPrint(allocator, "{s}_deferred", .{method_field});
const escaped_deferred = types.escapeZigKeyword(deferred_field);
```

### M9: `PeerState.init` error path leaks Peer hash maps

**File:** `src/wasm/capnp_host_abi.zig`, lines 109-120 and 409-412

**Description:** If `enableHostCallBridge()` fails (OOM), control returns to `capnp_peer_new` which calls `allocator.destroy(state)` — freeing the raw memory but NOT calling `state.host.deinit()` or `state.host.peer.deinit()`, leaking ~20 hash maps.

**Fix:** Add errdefer inside `PeerState.init`:
```zig
fn init(self: *PeerState) !void {
    self.outgoing_fba = std.heap.FixedBufferAllocator.init(&self.outgoing_storage);
    self.host = HostPeer.initWithOutgoingAllocator(allocator, self.outgoing_fba.allocator());
    errdefer self.host.deinit();
    self.host.peer.disableThreadAffinity();
    self.host.start(null, null);
    try self.host.enableHostCallBridge();
    // ...
}
```

### M10: `respondHostCallResults`/`respondHostCallException` don't validate question_id

**File:** `src/rpc/integration/host_peer.zig`, lines 147-174

**Description:** `respondHostCallReturnFrame` validates `pending_host_call_questions.contains(answer_id)` before sending, but `respondHostCallResults` and `respondHostCallException` do not. Allows responding to unknown question IDs (protocol violation) or double-responding.

**Fix:** Add `if (!self.pending_host_call_questions.contains(question_id)) return error.UnknownQuestion;` to both functions.

---

## Low Severity (24)

### L1: Memory leak in `parseInterfaceNode` error path
**File:** `src/serialization/request_reader.zig:268-271`
Sequential allocs without errdefer in `error.InvalidPointer` handler.

### L2: No errdefer for kind-specific nodes in `parseNode`
**File:** `src/serialization/request_reader.zig:156-163`
Currently unexploitable (no failable code after switch), but fragile.

### L3: `resolveListPointer` does not bounds-check list content
**File:** `src/serialization/message.zig:523-542`
All current callers check, but function is unsafe-by-default for future callers.

### L4: `createSegmentWithCapacity` panics on segment count overflow
**File:** `src/serialization/message.zig:1646`
`@intCast` panic if segments > u32 max (practically unreachable).

### L5: `@intCast` panics in `toBytes`/`writeTo` on large segments
**File:** `src/serialization/message.zig:2113,2133,2166,2172`
Multiple unchecked casts to u32 for segment count and size.

### L6: `estimateUnpackedSize` silently accepts truncated regular tags
**File:** `src/serialization/message.zig:183-185`
Overestimates size but `unpackPacked` correctly returns error. Wasted allocation only.

### L7: `encodePayloadCaps` partial encoding failure leaves cap table inconsistent
**File:** `src/rpc/level0/cap_table.zig:498-522`
Documented as known limitation. Receiver-answer entries preserved on mid-loop error.

### L8: Empty ArrayList left in map on `queuePendingCall` append failure
**File:** `src/rpc/level1/peer_promises.zig:21-25`
Harmless (cleaned up on map deinit) but could confuse `contains` checks.

### L9: Missing bounds checks on struct/inline-composite pointer reads in `collectCapsFromPointer`
**File:** `src/rpc/level0/cap_table.zig:407-415,438`
List branch checks bounds but struct and inline-composite branches do not.

### L10: `makeCapabilityPointer` has unnecessary error union return type
**File:** `src/rpc/level0/cap_table.zig:332-334`
Function body cannot fail; all callers use `try` unnecessarily.

### L11: `waitStreaming` silently overwrites prior drain callback
**File:** `src/rpc/level2/stream_state.zig:38-45`
Second caller's callback never fires. Add `debug.assert(self.on_drain == null)`.

### L12: `queueWrite` allows writes to a closing transport
**File:** `src/rpc/level2/transport_xev.zig:180`
Wasted allocation + error churn. Check `close_requested` before allocating WriteOp.

### L13: `Connection.deinit` bypasses transport socket close
**File:** `src/rpc/level2/connection.zig:101-103`
`abandonPendingWrites` zeros pending before `deinit` calls `drainPendingWrites`, so socket never closes through normal path if `close()` wasn't called first.

### L14: `onTransportClose` accesses `conn.on_close` after `on_error` may have destroyed connection
**File:** `src/rpc/level2/connection.zig:192-201`
Variant of H1. Capture close callback into local before calling error callback.

### L15: `completeShutdown` could panic if `transport_ctx` is null
**File:** `src/rpc/level3/peer.zig:745-758`
`.?` unwrap on `transport_ctx` when `transport_close` is set. Use `orelse return`.

### L16: Bootstrap export ref count not decremented on send failure
**File:** `src/rpc/level3/peer/peer_control.zig:93-119`
`note_export_ref` incremented before `send_frame`; no errdefer to decrement on failure.

### L17: Release message sends original count rather than actual refs decremented
**File:** `src/rpc/level3/peer/peer_cap_lifecycle.zig:5-24`
If import had fewer refs than requested count, Release message over-counts.

### L18: senderLoopback disembargo echo does not validate target existence
**File:** `src/rpc/level3/peer/peer_control.zig:384-418`
Validates payload non-null but discards values; doesn't check target is known.

### L19: `dataByteOffset` unchecked multiplication can panic on malicious schemas
**File:** `src/capnpc-zig/struct_gen.zig:1472-1482`
Inconsistent with `discriminantByteOffset` which uses `std.math.mul`. Use checked arithmetic.

### L20: Memory leak of `literal` in `generateConst` on writer error
**File:** `src/capnpc-zig/generator.zig:1180-1182`
If `writer.print` fails, `literal` is never freed. Use `defer` instead of manual free.

### L21: Pipeline type name uses unescaped identifier
**File:** `src/capnpc-zig/generator.zig:1118`
Theoretical only (PascalCase never matches Zig keywords currently).

### L22: `_` (discard identifier) not handled in keyword escaping
**File:** `src/capnpc-zig/types.zig:5-24`
All-separator input produces `_` which is invalid as a binding name.

### L23: WASM `asSlice` no `ptr + len` overflow check on wasm32
**File:** `src/wasm/capnp_host_abi.zig:194-199`
WASM runtime traps instead of graceful error on address-space wrap.

### L24: `capnp_peer_free_host_call_frame` has no double-free protection
**File:** `src/wasm/capnp_host_abi.zig:830-854`
No tracking of outstanding host-call frames (unlike outgoing frame `last_popped` guard).

---

## Test Coverage Gaps (9)

### T1 (High): Peer shutdown callback/drain lifecycle
**Area:** `src/rpc/level3/peer.zig:734-758`
No tests verify: callback fires when questions drain, transport close on completion, idempotency, immediate callback with zero outstanding questions.

### T2 (High): `clone_any_pointer` recursion limit and list cloning
**Area:** `src/serialization/message/clone_any_pointer.zig`
No tests for: `RecursionLimitExceeded` error, inline composite list cloning, pointer list cloning, capability pointer cloning, null pointer, invalid pointer type.

### T3 (Medium): `discriminantByteOffset` overflow edge cases
**Area:** `src/capnpc-zig/struct_gen.zig:51-53`
No tests exercise the `std.math.mul` overflow check.

### T4 (Medium): Direct `unpackPacked`/`estimateUnpackedSize` unit tests
**Area:** `src/serialization/message.zig`
Zero-tag runs, literal-tag runs, truncated input, estimation accuracy not directly tested.

### T5 (Medium): Schema validation RecursionGuard
**Area:** `src/serialization/schema_validation.zig`
No tests for cycle detection, depth limit, `enterViaPointer` reset, validation options.

### T6 (Medium): `importPathFromCapnpName` edge cases
**Area:** `src/capnpc-zig/generator.zig:1323-1330`
No tests for: name without leading slash, name without `.capnp` extension, empty/minimal names.

### T7 (Low): Peer `on_error` callback integration
**Area:** `src/rpc/level3/peer.zig:316`
No peer-level tests verify `on_error` fires on transport error or that null `on_error` is safe.

### T8 (Low): WASM ABI double-init/reinit safety
**Area:** `src/wasm/capnp_host_abi.zig`
No tests for double-init or deinit-then-reinit lifecycle.

### T9 (Low): `readBoolStrict` not tested
**Area:** `src/serialization/message.zig:1134`
Safety-critical strict variant used by protocol layer has no direct tests.

---

## Notes for Implementation

- H1 and L14 are the same root cause (callback-may-destroy-self pattern in connection.zig). Fix H1 comprehensively to also cover L14.
- M1 is confirmed by both the serialization and error-handling assessment agents independently.
- M7 (inbound caps leak) was initially flagged as a false positive by the memory lifecycle agent but confirmed as real by the RPC L3 agent — the `queuePendingCall` errdefer covers the *inner* function but not the *caller's* error path before the defer on line 205 is reached.
- Many Low findings are defense-in-depth improvements (debug asserts, documentation, practically-unreachable overflow checks). Prioritize M and H fixes.
