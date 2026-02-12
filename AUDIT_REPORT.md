# capnpc-zig Security & Correctness Audit Report

**Date:** 2026-02-11
**Scope:** Full codebase — serialization, codegen, RPC (levels 0–3), WASM ABI
**Method:** 9 parallel assessment tracks covering security, correctness, error handling, memory lifecycle, and test coverage

---

## Summary

| Severity | Count |
|----------|-------|
| High | 2 |
| Medium | 14 |
| Low | 22 |
| Test Coverage Gaps | 7 |
| **Total** | **45** |

---

## High Severity (2)

### H1: Import path injection in generated code

**File:** `src/capnpc-zig/generator.zig:131`

**Description:** The import path derived from the schema's `.capnp` filename is inserted into generated `@import("...")` statements using `{s}` format without escaping. Zig's `{s}` format inserts bytes verbatim. A malicious schema with an import name containing `"`, `\`, or newline characters could inject arbitrary Zig code into the generated output.

```zig
try writer.print("const {s} = @import(\"{s}\");\n", .{ mod_name, import_path });
```

The `import_path` comes from `importPathFromCapnpName(imp.name)`, which strips a leading `/` and replaces `.capnp` with `.zig`, but performs no character validation or escaping.

**Trigger:** A crafted `CodeGeneratorRequest` with an import name like `foo\");\nconst evil = @import("evil.zig`.

**Fix:** Use Zig string escaping for the import path, e.g. `std.zig.fmtString(import_path)`.

---

### H2: Thread affinity panic on native multi-threaded WASM ABI usage

**File:** `src/wasm/capnp_host_abi.zig` (contract at lines 27–41), `src/rpc/level3/peer.zig:345-354` (assertion)

**Description:** The ABI documentation states: "Callers on native targets may therefore invoke any exported `capnp_*` function from any thread." However, the underlying `Peer` records its creating thread ID in `initDetached()` and every method calls `assertThreadAffinity()`, which panics in Debug builds if the calling thread differs from the creating thread. The global mutex serializes access but does not change the thread affinity expectation.

**Trigger:** On native (non-WASM) targets in Debug builds: Thread A calls `capnp_peer_new()`, Thread B later calls `capnp_peer_push_frame()` on the same peer. The mutex is acquired, but `assertThreadAffinity()` panics.

**Fix:** After creating the `Peer` in `PeerState.init`, set the peer's `owner_thread_id` to `null` so `assertThreadAffinity` is a no-op (the mutex provides synchronization). Or change documentation to require single-thread usage.

---

## Medium Severity (14)

### M1: Signed integer overflow in offset-to-byte calculations on 32-bit targets

**File:** `src/serialization/message.zig` — lines 443, 504, 559, 771, 817

**Description:** The expression `@as(isize, offset_words) * 8` can overflow on 32-bit targets. `decodeOffsetWords` returns `i32` in range `[-2^29, 2^29 - 1]`. Maximum positive value `536870911 * 8 = 4294967288` overflows `i32` (max `2147483647`). In debug/safe builds this panics; in release-fast it's undefined behavior.

**Trigger:** A crafted Cap'n Proto message with a struct/list pointer whose 30-bit offset >= `2^28`, parsed on a 32-bit target (WASM32, ARM32).

**Fix:** Widen to `i64` before multiplying:
```zig
const signed = @as(i64, @intCast(pointer_pos)) + 8 + @as(i64, offset_words) * 8;
if (signed < 0 or signed > std.math.maxInt(usize)) return error.OutOfBounds;
```

---

### M2: Validation rejects valid double-far inline-composite lists (Layout B)

**File:** `src/serialization/message.zig:807-808`

**Description:** `validateListPointer` rejects inline-composite lists (element_size == 7) when `content_override != null`:
```zig
if (element_size == 7 and content_override != null) {
    return error.InvalidInlineCompositePointer;
}
```
This path is reached for double-far pointer tag words with element_size 7 — the standard "Layout B" used by the reference C++ implementation. `resolveInlineCompositeList` correctly handles this case (lines 640–672), so `Message.validate()` rejects messages that readers can successfully parse.

**Trigger:** A multi-segment message with an inline-composite list whose pointer uses a double-far pointer with Layout B encoding.

**Fix:** Handle `element_size == 7 and content_override != null` by parsing the tag word at the content offset and validating, mirroring the Layout B path in `resolveInlineCompositeList`.

---

### M3: Silent OOM in `structTypeName`, `enumTypeName`, `interfaceTypeName`

**File:** `src/capnpc-zig/struct_gen.zig:1296-1312`

**Description:** Three type name resolution functions use `catch null` on `qualifiedTypeName`, which can return `std.mem.Allocator.Error` (OOM). OOM silently degrades generated code (falls back to generic types like `message.StructReader` instead of specific types).

```zig
fn structTypeName(self: *StructGenerator, id: schema.Id) ?[]const u8 {
    const node = self.getNode(id) orelse return null;
    if (node.kind != .@"struct") return null;
    return self.qualifiedTypeName(node, id) catch null;  // OOM swallowed
}
```

**Fix:** Change return to `!?[]const u8` and propagate the error with `try`.

---

### M4: Silent OOM in `Generator.structTypeName`

**File:** `src/capnpc-zig/generator.zig:1737`

**Description:** Same pattern as M3 in the `Generator` struct's own `structTypeName`:
```zig
return self.allocTypeDeclName(node) catch null;
```

**Fix:** Change return to `!?[]const u8` and propagate the error.

---

### M5: Missing errdefer for `copy` in `sendReturnResults`

**File:** `src/rpc/level3/peer.zig:1137-1139`

**Description:** A copy of the return frame is allocated, then passed to `recordResolvedAnswer`. If `recordResolvedAnswer` fails (OOM during `resolved_answers.put`), the `copy` is leaked.

```zig
const copy = try self.allocator.alloc(u8, bytes.len);
std.mem.copyForwards(u8, copy, bytes);
try self.recordResolvedAnswer(answer_id, copy);  // if fails, copy leaks
```

**Fix:** Add `errdefer self.allocator.free(copy);` between the alloc and `recordResolvedAnswer`.

---

### M6: Missing errdefer for `copy` in `sendPrebuiltReturnFrame`

**File:** `src/rpc/level3/peer.zig:1149-1151`

**Description:** Same pattern as M5:
```zig
const copy = try self.allocator.alloc(u8, frame.len);
std.mem.copyForwards(u8, copy, frame);
try self.recordResolvedAnswer(ret.answer_id, copy);  // if fails, copy leaks
```

**Fix:** Add `errdefer self.allocator.free(copy);` between the alloc and `recordResolvedAnswer`.

---

### M7: Missing errdefer in `queueEmbargoedAccept` existing-key branch

**File:** `src/rpc/level3/peer/peer_embargo_accepts.zig:20-28`

**Description:** In the existing-key branch, `append` to the pending list succeeds but the subsequent `put` into `pending_accept_embargo_by_question` can fail with OOM. The appended entry is orphaned — `Finish` for the question won't find it, and it lingers until embargo release.

**Fix:** Add errdefer to pop the last list element if `put` fails:
```zig
try entry.value_ptr.append(allocator, .{...});
errdefer _ = entry.value_ptr.pop();
try pending_accept_embargo_by_question.put(answer_id, @constCast(entry.key_ptr.*));
```

---

### M8: Socket fd leak in `Listener.init` on bind/listen failure

**File:** `src/rpc/level2/runtime.zig:116-118`

**Description:** `Listener.init` calls `xev.TCP.init(addr)` which creates a socket fd. If the subsequent `socket.bind(addr)` or `socket.listen(128)` fails, the socket fd is never closed.

**Fix:** Add `errdefer std.posix.close(socketFd(socket));` after `TCP.init`.

---

### M9: Import ref count leak on partial `InboundCapTable.init` failure

**File:** `src/rpc/level0/cap_table.zig:193-198`

**Description:** When `InboundCapTable.init` processes cap descriptors in a loop, `resolveDescriptor` calls `table.noteImport(id)` for each. If processing fails on entry N, the `noteImport` calls for entries 0..N-1 are NOT rolled back. The CapTable has permanently inflated import ref counts — subsequent `Release` messages won't fully release them.

**Trigger:** A remote peer sends a message with a cap table where some descriptors are valid but a later one is malformed.

**Fix:** Add an errdefer before the loop that iterates already-processed entries and calls `table.releaseImport` for any resolved as `.imported`.

---

### M10: Unchecked `@intCast` of frame.len to u32 in WASM ABI

**File:** `src/wasm/capnp_host_abi.zig:504, 772`

**Description:** `const frame_len_u32: u32 = @intCast(frame.len)` with no prior bounds check. On wasm32, `usize` is `u32` so it's safe. On 64-bit native targets (test harness), frames exceeding 4GB would cause a runtime trap.

**Fix:** Add `if (frame.len > std.math.maxInt(u32)) { setError(...); return 0; }` before the cast.

---

### M11: Unaligned pointer access in WASM ABI write helpers

**File:** `src/wasm/capnp_host_abi.zig:195-217`

**Description:** `writeU32`, `writeU16`, `writeU64`, `writeAbiPtr` use `@ptrFromInt` to create typed pointers (`*u32`, `*u16`, etc.) from arbitrary `AbiPtr` values. If the host passes a misaligned address, dereferencing is undefined behavior.

**Fix:** Use `*align(1) u32` etc:
```zig
const out: *align(1) u32 = @ptrFromInt(@as(usize, @intCast(ptr)));
```

---

### M12: Auto-free of previous frame in `capnp_peer_pop_out_frame` creates dangling pointer

**File:** `src/wasm/capnp_host_abi.zig:480-484`

**Description:** When `capnp_peer_pop_out_frame` is called, `last_popped` from a previous call is immediately freed before popping the next frame. The host may still hold a pointer to that memory. The doc comment says the frame must be committed first, but there's no enforcement.

**Fix:** Return an error if `last_popped` is non-null instead of silently freeing.

---

### M13: Dead errdefer in `capnp_schema_manifest_json`

**File:** `src/wasm/capnp_host_abi.zig:995`

**Description:** `errdefer allocator.free(copy)` in a function returning `u32` (not `!u32`). The errdefer never fires. No actual leak because the catch block manually frees, but the dead code is misleading.

**Fix:** Remove the dead errdefer.

---

### M14: Unchecked `@intCast` to u16 in schema validation

**File:** `src/serialization/schema_validation.zig:262-263`

**Description:** `@as(u16, @intCast(max_data_word + 1))` — `max_data_word` is computed from `byte_offset / 8` where `byte_offset` comes from field offset in the schema. A malformed schema with very large field offsets would overflow u16.

**Fix:** Add validation that the computed word index fits in u16, returning `error.InvalidSchema` otherwise.

---

## Low Severity (22)

### L1: Missing `assertThreadAffinity` on `Connection.deinit`

**File:** `src/rpc/level2/connection.zig:80`

Modifies state (nulls callbacks, clears transport handlers) without asserting thread affinity. Inconsistent with `Peer.deinit` and `Runtime.deinit`.

**Fix:** Add `self.assertThreadAffinity();` as the first line.

---

### L2: Missing `assertThreadAffinity` on `Connection.start`

**File:** `src/rpc/level2/connection.zig:91`

Sets callbacks and initiates transport I/O without asserting thread affinity.

**Fix:** Add `self.assertThreadAffinity();` as the first line.

---

### L3: No null re-check on callbacks between `handleRead` loop iterations

**File:** `src/rpc/level2/connection.zig:141-158`

`on_message` / `on_error` are checked at the top, but inside the `while(true)` loop, a successful `on_message` callback could set them to null. Next iteration's `.?` unwrap would panic.

**Fix:** Re-check for null at the top of the while loop.

---

### L4: Asymmetric error handling in `handleRead`

**File:** `src/rpc/level2/connection.zig:154-157`

Framing errors reset the framer and null callbacks; message handler errors do not. If the handler error is caused by corrupt data, subsequent buffered frames from the same read will still be delivered.

**Fix:** Design decision — document current behavior or add framer reset on message error.

---

### L5: Missing `assertThreadAffinity` on `Connection.isClosing`

**File:** `src/rpc/level2/connection.zig:119`

Reads transport state without asserting thread affinity. The flags are set without synchronization, so reading from a non-owner thread is a data race.

**Fix:** Add `self.assertThreadAffinity();`.

---

### L6: No `deinit`/synchronous cleanup path for `Listener`

**File:** `src/rpc/level2/runtime.zig` — `Listener` struct

`Listener` has `close()` (async) but no `deinit` for synchronous cleanup. If the event loop exits before close completion fires, the socket fd leaks.

**Fix:** Add a `deinit` method that synchronously closes the socket fd, or document the requirement.

---

### L7: `Connection.deinit` does not destroy the heap-allocated Connection itself

**File:** `src/rpc/level2/connection.zig:80-89`

`deinit` cleans up internal state but doesn't free the `Connection` object. Callers must remember both `deinit()` and `allocator.destroy(conn)`.

**Fix:** Document the two-step cleanup requirement, or provide a `destroy` method.

---

### L8: Missing bounds check in `U8ListBuilder.setAll`

**File:** `src/serialization/message/list_builders.zig:59-64`

Checks `data.len == self.element_count` but does not validate `elements_offset + data.len` is within segment bounds before slicing. Under normal builder operation this is always consistent, but a builder bug could cause OOB access.

**Fix:** Add explicit bounds check before slicing.

---

### L9: `encodeOffsetWords` does not validate offset fits in 30 bits

**File:** `src/serialization/message.zig:20-26`

Accepts any `i32` but Cap'n Proto pointer offsets are 30-bit signed. Values outside range produce corrupt pointers when shifted by `makeStructPointer`/`makeListPointer`.

**Fix:** Add range check: `if (offset_words < -(1 << 29) or offset_words >= (1 << 29)) return error.OffsetOutOfRange;`

---

### L10: `discriminant_offset * 2` can overflow u32 with malicious schema

**File:** `src/capnpc-zig/struct_gen.zig:117, 175, 518, 1533`

`discriminant_offset` is u32 from wire format. `* 2` overflow panics in safe mode. Valid schemas are bounded by `data_word_count * 4` (u16*4), but codegen doesn't validate.

**Fix:** Use overflow-checked arithmetic or validate constraint.

---

### L11: Empty identifier from all-separator names produces invalid Zig

**File:** `src/capnpc-zig/types.zig:34-53`

`normalizeIdentifier` on input like `"___"` produces empty string, leading to invalid generated code.

**Fix:** Return error on empty result.

---

### L12: `@constCast` in `lookupTypePrefix` is sound but fragile

**File:** `src/capnpc-zig/generator.zig:1410-1414`

The callback casts `*const anyopaque` to `*Generator` via `@constCast`. Currently sound but fragile.

**Fix:** Change callback signature to `?*anyopaque` (mutable).

---

### L13: `catch continue` silently skips interface fields on OOM

**File:** `src/capnpc-zig/generator.zig:1272`

```zig
const iface_name = self.qualifiedTypeName(iface_id) catch continue;
```

Silently skips interface fields if OOM occurs, omitting pipeline types from generated code.

**Fix:** Propagate error with `try`.

---

### L14: Recursive `parseType` without depth limit

**File:** `src/serialization/request_reader.zig:545-577`

Deeply nested `List(List(List(...)))` types could cause stack overflow. No recursion depth limit.

**Fix:** Add depth counter parameter, return error at depth > 64.

---

### L15: `queuePendingCall` leaks `InboundCapTable` on OOM

**File:** `src/rpc/level1/peer_promises.zig:5-23`

`inbound_caps` is passed by value. If frame copy or list append fails, the passed-in cap table's backing arrays leak.

**Fix:** Add `errdefer inbound_caps.deinit();` at the top of `queuePendingCall`.

---

### L16: `releaseAllImports` may access freed transport context

**File:** `src/rpc/level3/peer.zig:595-617`

In `peer.deinit()`, `releaseAllImports()` calls `sendFrame` which may access `transport_ctx`. If the connection was destroyed before peer deinit, this is use-after-free. Errors are caught and logged.

**Fix:** Check `transport_send != null` before iterating imports, or document that `detachTransport` must be called first.

---

### L17: Missing bounds validation in `collectCapsFromPointer` for pointer lists

**File:** `src/rpc/level0/cap_table.zig:410-417`

Reads raw bytes from segments at computed offsets without bounds checking. Operates on locally-built messages, so practical risk is low.

**Fix:** Add bounds check: `if (pos + 8 > segment.len) return error.OutOfBounds;`

---

### L18: `encodePayloadCaps` removes receiver_answers before caller confirms send

**File:** `src/rpc/level0/cap_table.zig:498-504`

Receiver-answer entries are permanently removed after encoding. If the caller encounters an error after encoding but before sending, the entries are lost.

**Fix:** Defer cleanup to caller after send confirmation, or document as known limitation.

---

### L19: Transport `deinit` does not cancel pending read completions

**File:** `src/rpc/level2/transport_xev.zig:105-113`

After `deinit`, a pending xev read completion may fire with a dangling pointer to the transport.

**Fix:** Close the socket before freeing the transport, or document that the event loop must be drained before `deinit`.

---

### L20: `capnp_error_take` partial-write can lose original error

**File:** `src/wasm/capnp_host_abi.zig:370-381`

Sequential writes to output locations — if first succeeds but second fails, the original error is overwritten with a pointer error.

**Fix:** Validate all output pointers before performing any writes.

---

### L21: `capnp_peer_set_bootstrap_stub` silently replaces host call bridge

**File:** `src/wasm/capnp_host_abi.zig:543-564`

Replaces the host call bridge bootstrap without notification. The old export entry leaks and `host_bridge_enabled` remains true.

**Fix:** Document behavior, or error/warn when replacing active host call bridge.

---

### L22: Inconsistent keyword escaping for generated method names

**File:** `src/capnpc-zig/generator.zig:772`

Method names converted to PascalCase via `identToZigTypeName` don't apply keyword escaping. Currently safe because all Zig keywords are lowercase, but inconsistent with enum variant escaping.

**Fix:** Apply `escapeZigKeyword` for consistency and future-proofing.

---

## Low Severity — Investigation Items (5)

These are `@intCast` casts that are practically safe due to domain constraints but lack explicit guards:

| # | File | Line | Description |
|---|------|------|-------------|
| I1 | `cap_table.zig` | 63-65 | `totalEntries()` casts 3 HashMap counts to u32 and adds; bounded by u32 ID space |
| I2 | `protocol.zig` | 684, 686 | `@intCast(ops.len)` for PromisedAnswerOps; practically always tiny |
| I3 | `message.zig` | 1605 | `@intCast(segments.items.len)` to u32; segments rarely exceed handful |
| I4 | `message.zig` | 2092, 2131 | `@intCast(segment.items.len / 8)` to u32; would need 32 GiB segment |
| I5 | `schema_validation.zig` | 262-263 | `@intCast(max_data_word + 1)` to u16; valid schemas can't exceed |

---

## Test Coverage Gaps (7)

### T1: `InboundCapTable.clone()` has zero test coverage (Critical)

No test calls `.clone()` on an `InboundCapTable`. The method has an errdefer pattern and creates deep copies — both aspects need direct testing. A subtle bug (shallow copy, wrong errdefer) would cause use-after-free in the RPC layer.

---

### T2: No adversarial/malformed packed format fuzzing (Critical)

The fuzz test at `message_test.zig:1057` feeds random bytes to `Message.init()` (raw format) only. No equivalent test for `Message.initPacked()`. The packed decoder's tag-byte processing could have edge cases that only surface with adversarial input.

---

### T3: `bounds.zig` overflow-safe functions have no direct unit tests (High)

`checkBounds`, `checkBoundsMut`, `checkOffset`, `checkOffsetMut`, `checkListContentBounds` are only tested indirectly. Boundary values (`maxInt(u32)`, zero-length segments) are never exercised.

---

### T4: `resolveInlineCompositeList` far/double-far pointer paths untested (High)

Multi-segment messages with struct lists spanning segments require far pointer resolution. The double-far Layout B path in `resolveInlineCompositeList` has no test coverage.

---

### T5: Double-attach panic guards untested (Medium)

`attachConnection` and `attachTransport` panic guards (added in previous round) have no test verifying the panic fires on double-attach.

---

### T6: `TextListReader.getStrict()` and `PointerListReader.getTextStrict()` untested (Medium)

UTF-8 validation on individual list elements is never tested. Distinct code path from `readTextStrict` on struct fields.

---

### T7: `createConnection` errdefer path untested (Low)

The errdefer in `Listener.createConnection` (added in previous round) is never exercised. A FailingAllocator test would verify no leak on `Connection.init()` failure.

---

## Previously Fixed (Round 1)

The following issues were identified and fixed in the previous assessment round. All 511 tests pass:

| ID | Severity | Fix |
|---|---|---|
| C1 | Critical | ForwardCallContext UAF — clone InboundCapTable by value |
| C2 | Critical | WASM dead errdefer — moved cleanup to catch block |
| C3 | Critical | WASM pointer truncation — use ptrToAbi helper |
| H2 | High | catch unreachable confirmed correct — added comments |
| H3 | High | Silent catch in generator — propagated errors |
| H5 | High | errdefer gap in OutboundCapTable.indexFor — added errdefer |
| H6 | High | Embargo key duplication — single owner, borrower pattern |
| H7 | High | 13 DecodedMessage methods changed to `*const` |
| H8 | High | Connection init errdefer — extracted createConnection helper |
| M1 | Medium | Thread affinity for Connection — added assertions |
| M2 | Medium | Double-attach panic guards added |
| M4 | Medium | Silent catch in releaseAllImports replaced with log.debug |
| M5 | Medium | getStructList bug — use resolveInlineCompositeList |
| M10-12 | Medium | Dead code removal, bounds overflow protection |
