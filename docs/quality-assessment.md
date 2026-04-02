# capnp-zig Quality & Developer Experience Assessment

*Generated 2026-04-02*

## Overall Verdict: **B+ / Production-Ready with Caveats**

The library is architecturally sound, well-tested (703 tests, zero skips), and idiomatic Zig. The serialization and codegen layers are production-ready. The RPC runtime is feature-complete but has testing gaps around concurrency and failure recovery. Developer experience is strong for maintainers but has friction points for new users.

---

## 1. Architecture (8/10)

**Strengths:**
- Clean four-layer design (wire format → schema → codegen → RPC) with proper downward-only dependencies
- Zero external dependencies — pure Zig + POSIX
- Transport-agnostic peer design (TCP, WASM, test mocks)
- Two-tier API: `lib.zig` (full) vs `lib_core.zig` (no POSIX) for embedded/WASM

**Issues:**
- **Peer module complexity** — `src/rpc/level3/peer/` has 26 interconnected files totaling ~10K LOC. The `Peer` struct imports all of them, creating a coupling hotspot. Consider factoring into `PeerQuestionState`, `PeerCapLifecycle`, `PeerDispatcher`, `PeerTransport` sub-components.
- **List reader/builder duplication** — ~500 LOC of mirrored patterns between `list_readers.zig` and `list_builders.zig`. Shared validation logic could be extracted.
- **Protocol.zig hand-written wrappers** — Manual convenience wrappers duplicate generated code from `rpc.capnp`. Could be auto-generated.

---

## 2. Error Handling & Safety (9/10)

**Strengths:**
- Excellent overflow protection — all pointer arithmetic uses `std.math.add/mul` with explicit catch
- Comprehensive bounds checking with dedicated `bounds.zig` module
- Proper `errdefer` usage throughout for leak prevention
- Framing layer validates all untrusted input (segment counts, sizes, traversal limits)
- Schema validation has recursion guards and cycle detection

**Issues:**
- ~~**`Message.validate()` is easy to skip**~~ — **FIXED**: `Message.init()` now validates by default. `initUnvalidated()` is available as an explicit opt-out for trusted data or custom validation flows.
- **Error messages lack context** — `error.InvalidPointer` doesn't say which pointer or offset. `error.OutOfBounds` doesn't say what bounds. Makes production debugging difficult.
- **`catch unreachable` in protocol.zig** (~15 instances) — justified by comments but fragile if codegen changes. Consider explicit error propagation.
- **Silent defaults in readers** — `readU32()` returns 0 on out-of-bounds (Cap'n Proto spec compliance for schema evolution), but this hides bugs. Strict variants exist but require opt-in.

---

## 3. Testing (B+)

**439 tests across 36 files.** Strong coverage of happy paths, good negative testing.

**Strengths:**
- Excellent wire format and protocol testing (spec-compliant)
- Deterministic PRNG-based fuzz testing for all primitive types
- Interop with Go/Python reference implementations
- RPC Level 3 has 126 tests covering complex multi-party flows

**Critical Gaps:**

| Gap | Risk | Effort |
|-----|------|--------|
| No concurrent multi-call RPC tests | Race conditions in question IDs, cap table | ~1 week |
| No connection failure/recovery tests | State corruption, orphaned promises | ~3 days |
| No promise timeout/cancellation races | Deadlocks, resource leaks | ~3 days |
| No sustained load/stress tests | DoS vulnerabilities unknown | ~2 days |
| RPC Level 1 has only 7 tests | Promise pipeline bugs | ~1 week |
| No malformed frame fuzzing | Protocol crash potential | ~2 days |

---

## 4. Public API Ergonomics (7/10)

**Strengths:**
- Builder/Reader duality is intuitive and idiomatic
- Zero-copy deserialization is elegant
- Generated code follows clear patterns

**Friction Points:**
- **Magic numbers in `allocateStruct(data_words, pointer_words)`** — not self-documenting. Generated code abstracts this but raw API is error-prone.
- **RPC callback signatures are complex** — 5-parameter function pointers with `*anyopaque` context. No ergonomic helper to construct handlers.
- **`reader` module name is misleading** — sounds like deserialization but provides streaming I/O (`SliceReader`, `Reader.readMessage`). Would be clearer as `streaming` or `io`.
- ~~**No `initValidated()` helper**~~ — **FIXED**: `Message.init()` now validates by default; `initUnvalidated()` is the opt-out.
- **Union discrimination errors are opaque** — `error.InvalidEnumValue` doesn't say what the discriminant was.

---

## 5. Documentation (Mixed)

**Strong:**
- Excellent getting-started guides for serialization and RPC (`docs/getting-started-*.md`)
- Good architecture docs (`docs/architecture.md`, `docs/rpc_runtime_design.md`)
- Build integration clearly documented
- Working examples in `examples/` and `tests/e2e/`

**Weak:**
- ~~**No doc comments on public APIs**~~ — **FIXED**: All core public types and methods now have `///` doc comments (`Message`, `StructReader`, `StructBuilder`, `MessageBuilder`, `Peer`, list builders/readers).
- **No error handling guide** — errors are scattered, no central reference
- **No troubleshooting/pitfalls guide** — reader lifetime issues, validation requirements, union semantics not documented
- **No message validation/security guide** — critical for untrusted input
- **Examples not linked from README** — `examples/rpc_pingpong.zig` is excellent but undiscoverable

---

## 6. Build System & Tooling (7/10)

**Strengths:**
- Comprehensive Justfile with clear recipes
- Multi-platform CI (Ubuntu, macOS, Windows)
- Benchmark infrastructure with regression checking
- `mise` for tool management

**Issues:**
- **`build.zig` boilerplate** — 895 lines with ~400 lines of repetitive test/module setup. Helper functions could reduce by 40%.
- **`mise.toml` uses `zig = "master"`** — fragile; should pin to `0.16` or a specific dev version for reproducibility
- **No pre-commit hooks** — format errors caught only in CI (slow feedback)
- **No watch mode** — no `just watch` for continuous testing during development
- **CI missing Cap'n Proto on macOS/Windows** — interop tests only fully work on Linux
- **No binary releases** — users must build from source
- **No git version tags** — impossible to pin dependency versions

---

## Top 10 Recommended Changes

### Immediate (High Impact, Low Effort)

1. ~~**Add `Message.initValidated()` helper**~~ — **DONE**: `Message.init()` now validates by default. `initUnvalidated()` is the explicit opt-out for trusted data.

2. ~~**Add doc comments to core public types**~~ — **DONE**: Added `///` doc comments to all public methods on `Message`, `StructReader`, `StructBuilder`, `PointerListBuilder`, `AnyPointerBuilder`, `MessageBuilder`, list builders/readers, and `Peer` (transport lifecycle, RPC operations, capability management). Also documented the `rpc` export in `lib.zig`.

3. **Pin Zig version in `mise.toml`**~~ — **SKIPPED**: Change `zig = "master"` to a specific version. Prevents random CI breakage.

### Short-term (High Impact, Medium Effort)

4. ~~**Add concurrent RPC test suite**~~ — **DONE**: Added 12 tests in `tests/rpc/level3/rpc_concurrent_calls_test.zig` covering: multiple inbound calls to same export, deferred returns in reverse order, outbound calls with in-order/reverse/interleaved returns, mixed results and exceptions, bidirectional interleaved calls, calls to multiple distinct exports, question ID uniqueness, question ID reclamation, stress tests (128 inbound, 64 outbound), and shutdown during pending calls.

5. ~~**Add connection failure/recovery tests**~~ — **DONE**: Added 8 tests in `tests/rpc/level2/rpc_connection_failure_test.zig` covering: deinit with pending outbound/inbound calls (leak detection), shutdown draining callbacks, shutdown+deinit with partial returns, detach transport while calls pending, sendCall/sendBootstrap rejected after shutdown, handleFrame delivers returns post-shutdown.

6. ~~**Create error handling guide**~~ — **DONE**: Created `docs/error-handling.md` with sections for message deserialization errors, validation errors, struct reader errors, builder errors, RPC protocol errors, schema validation errors, and best practices with code examples.

7. **Refactor `build.zig` with helper functions** — Extract `addTestStep()`, `addBenchmark()`, `addLibModule()`. Reduce from 895 to ~500 lines.

### Medium-term (Medium Impact, Higher Effort)

8. **Improve error context** — Wrap key errors with offset/field/segment information. `error.InvalidPointer` includes which pointer at what offset. Makes production debugging tractable.

9. **Create troubleshooting guide** (`docs/troubleshooting.md`) — Reader lifetimes (use-after-free risk), union discriminant semantics, capability ownership, validation requirements. Each with wrong/right code examples.

10. **Factor Peer into sub-components** — Split the 26-file `peer/` directory into cohesive sub-types (`PeerQuestionState`, `PeerCapLifecycle`, `PeerDispatcher`). Reduces coupling and improves testability of the most complex module.

---

## What's Not Broken

The report intentionally surfaces improvement opportunities, but it's worth noting: the core serialization is rock-solid with excellent overflow protection, the codegen produces clean output, the RPC protocol implementation is comprehensive (including three-party handoff, promise pipelining, and streaming), and the test suite is well above average for a project of this size. The recommendations above are about going from good to great.
