# Cap'n Proto Parity Roadmap

## Goal
Deliver a production-ready, fully spec-compliant Cap'n Proto implementation in Zig that is a peer to the reference implementation, with robust interoperability and tests.

## Phase 1: Wire-Format Correctness (Foundation)
- Fix signed pointer offset handling (including negative offsets).
- Correct list decoding (element sizes, inline composite detection, bit lists).
- Add far pointers and inter-segment reference support.
- Unify reader logic to avoid divergent behavior (`message.zig` vs `reader.zig`).
- Tests: negative offsets, list edge cases, multi-segment messages, far pointers.

## Phase 2: Complete Message Builder
- Multi-segment allocation and far pointer emission.
- Lists (primitive, pointer, struct), data blobs, and nested structs.
- Packed message support (optional but required for parity).

## Phase 3: CodeGeneratorRequest Parsing
- Implement real parsing of `CodeGeneratorRequest` from stdin.
- Populate nodes, requested files, and capnp version data.

## Phase 4: Production Codegen
- Generate Reader/Builder code that uses the real message layer.
- Support unions, lists, enums, nested structs, default values, and data fields.

## Phase 5: Interop & Parity Testing
- Differential tests against the reference `capnp` compiler/runtime.
- Official Cap'n Proto test corpus where possible.
- Performance and memory regression benchmarks.

## Definition of Done
- All core features are implemented and tested.
- Interop tests pass for real-world schemas and round-trip serialization.
- Public APIs are stable and documented.
