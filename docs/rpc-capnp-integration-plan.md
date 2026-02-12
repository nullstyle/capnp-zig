# RPC `rpc.capnp` Integration Plan

## Scope

- Canonical RPC schemas live in `src/rpc/capnp/`:
  - `src/rpc/capnp/rpc.capnp`
  - `src/rpc/capnp/persistent.capnp`
  - `src/rpc/capnp/c++.capnp`
- Generated Zig bindings live in `src/rpc/gen/capnp/`.
- `capnp` is a required development dependency for RPC/codegen work. There is no fallback mode.

## Current Baseline

1. Source-of-truth workflow exists.
- Sync: `just --justfile src/rpc/justfile sync-rpc` (alias: `sync`)
- Verify: `just --justfile src/rpc/justfile check-rpc` (alias: `check`)
- Generate: `just --justfile src/rpc/justfile gen-rpc` (alias: `gen`)

2. `CodeGeneratorRequest` parsing now handles `Field.discriminantValue` correctly.
- `src/serialization/request_reader.zig` now XORs with the schema default (`0xffff`) when parsing this field.
- Regression coverage added in `tests/serialization/integration_test.zig`.

3. Generator reliability for RPC schemas improved.
- Group reader/builder wrappers now use `@This()` to avoid nested-type ambiguity.
- Group field setters now write union discriminants when the group struct defines a union.
- Void getters now consume `self` to avoid unused-parameter compile errors.

4. Level-0 protocol now uses generated schema tags directly.
- `src/rpc/level0/protocol.zig` derives these enums from `src/rpc/gen/capnp/rpc.zig`:
  - `MessageTag`
  - `ReturnTag`
  - `MessageTargetTag`
  - `CapDescriptorTag`
  - `ResolveTag`
  - `PromisedAnswerOpTag`
  - `SendResultsToTag`
  - `DisembargoContextTag`
- `MessageTag`, `ReturnTag`, `MessageTargetTag`, `CapDescriptorTag`, `ResolveTag`, `PromisedAnswerOpTag`, `SendResultsToTag`, and `DisembargoContextTag` are direct aliases to generated `WhichTag` types.
- RPC runtime/tests now use generated enum member naming (`camelCase`, `@"return"`), removing hand-maintained tag mapping blocks.

5. Tag-parity checks are now test-gated.
- `tests/rpc/level0/rpc_protocol_test.zig` asserts protocol tag enums stay aligned with generated RPC bindings.
- `tests/rpc/level0/rpc_protocol_test.zig` now also asserts key field parity between manual and generated decode paths for `Call`, `Return`, `Resolve`, and `Disembargo`.
- `tests/serialization/codegen_union_group_test.zig` asserts group unions emit `WhichTag`/`which()` and discriminant writes.

6. Generated-reader decode adoption has started.
- `SendResultsTo.fromReader` now uses generated `Call.SendResultsTo.Reader`.
- `MessageTarget.fromReader` now uses generated `MessageTarget.Reader`.
- `Return.fromReader` now uses generated `Return.Reader`.
- `Resolve.fromReader` now uses generated `Resolve.Reader`.
- This routes call-sites (`Call`, `Disembargo`, `Provide`, `Join`, `Return`, `Resolve`) through generated discriminant/field access for those substructures.

7. Generated-builder encode adoption has started.
- `MessageBuilder.buildBootstrap`, `buildAbort`, `buildRelease`, `buildFinish`, `buildResolveCap`, `buildResolveException`, `buildDisembargoSenderLoopback`, `buildDisembargoReceiverLoopback`, `buildDisembargoAccept`, `buildProvide`, `buildAccept`, `buildThirdPartyAnswer`, and `buildJoin` now use generated builders.
- `MessageBuilder.beginCall` and `beginReturn` now initialize through generated `Message.Builder` + generated `Call/Return` builders.
- `MessageBuilder.buildUnimplementedFromAnyPointer` now uses generated top-level message initialization (`initUnimplemented`) before cloning the nested original pointer.
- `CallBuilder` now uses generated builders for target encoding, `sendResultsTo.thirdParty`, and typed payload/cap-table initialization paths.
- `ReturnBuilder` now uses generated builders for release/no-finish bits, exception/takeFromOtherQuestion/awaitFromThirdParty encoding, and typed payload/cap-table initialization paths.
- Resolve/capability payloads now write via generated `CapDescriptor.Builder` (including receiver-answer and third-party-hosted variants).
- `CapDescriptor.attached_fd` now uses the correct byte offset in protocol encode/decode so it no longer aliases the `id` field.
- Runtime forwarding/cap-remap paths and generated RPC service stubs now build params/results via typed payload APIs (`payloadTyped` + `initContent` + `initCapTableTyped`) instead of low-level `payloadBuilder`/`initResultsStruct`/`setEmptyCapTable` helpers.

## Known Gaps

- `src/rpc/level0/protocol.zig` is still the primary hand-written reader/builder implementation for wire layout and message construction.
- Some cap-table/payload traversal logic remains intentionally low-level because capability remapping requires direct pointer-word inspection/rewrite.

## Migration Plan

1. Keep tag-source migration locked in.
- Keep generated group `WhichTag`/`which()` coverage for wrapper unions.
- Treat regressions in generated group union tags as codegen failures (test-gated).

2. Add parity tests between manual protocol and generated bindings.
- For `Call`, `Return`, `Resolve`, and `Disembargo`, extend from tag parity into key field offsets/slots used by hand-written readers/builders.
- Keep failures explicit with struct/field names and expected vs actual values.

3. Introduce generated-reader usage incrementally.
- Start in non-hot paths (validation/inspection).
- Move to builder/read paths only after parity and behavior are demonstrated in existing RPC level-0/level-3 suites.

4. Keep schema lifecycle as a gate.
- CI should run schema sync check plus tests that exercise generated RPC bindings and protocol parity.

## Immediate Next Milestone

1. Continue replacing manual field-by-field wire encode/decode paths in level-0 protocol with generated wrappers where parity coverage already exists.
2. Keep cap-table/payload traversal low-level code constrained to pointer-rewrite responsibilities and avoid reintroducing generic compatibility helpers.
