# Security Regression Matrix

This matrix maps the hardening work from the May 7, 2026 audit to concrete
regression gates. It is intentionally organized by vulnerability class rather
than by module so future advisories can be triaged against the same checklist.

Status values:

- `Covered`: a named test or gate exercises the class directly.
- `Partial`: deterministic coverage exists, but a broader fuzz/e2e/security
  gate is still useful.
- `Pending`: the audit identified the class, but no direct regression gate has
  landed yet.

| Class | Primary Surface | Regression Gate | Status |
|---|---|---|---|
| Malformed segment tables, far pointers, and pointer traversal | `src/serialization/message.zig` | `zig build test-message`, `zig build test-resource-budgets` | Covered |
| Packed decode malformed input and expansion behavior | `src/serialization/message/packing.zig` | `tests/serialization/message_test.zig`, `tests/serialization/serialization_fuzz_test.zig` | Covered |
| Zero-width or low-word-count structures bypassing resource budgets | Serialization validation and request reader | `tests/serialization/message_test.zig`, `tests/serialization/schema_validation_test.zig` | Covered |
| CodeGeneratorRequest size and output expansion | `src/main.zig`, `src/capnpc-zig/generator.zig` | `src/main.zig` tests, `src/capnpc-zig/generator.zig` budget tests, `zig build test-resource-budgets` | Covered |
| Generated identifier collisions | `src/capnpc-zig/types.zig`, `src/capnpc-zig/generator.zig` | `src/capnpc-zig/generator.zig` duplicate-name tests, `zig build test-codegen` | Covered |
| Output path traversal or symlink escape during codegen | `src/main.zig` | `createOutputFileInDir` traversal and symlink tests | Covered |
| RPC message tag and payload pointer malformation | `src/rpc/wire/protocol.zig` | `zig build test-rpc-level0`, `tests/rpc/wire/rpc_protocol_test.zig` | Covered |
| RPC cap-table descriptor confusion or invalid hosted IDs | `src/rpc/caps/table.zig`, `src/rpc/peer/mod.zig` | `zig build test-rpc-level0`, `zig build test-rpc-level3` | Covered |
| Promised-answer transform length and path validation | `src/rpc/promises`, `src/rpc/peer` | `tests/rpc/promises/rpc_promised_answer_transform_test.zig`, level3 peer tests | Covered |
| Duplicate inbound/outbound questions and release over-counts | `src/rpc/peer/mod.zig` | `tests/rpc/peer/rpc_release_and_failure_test.zig`, `tests/rpc/peer/rpc_peer_test.zig` | Covered |
| Peer pending-map resource exhaustion | `src/rpc/peer/mod.zig` | `zig build test-rpc-level3`, `zig build test-resource-budgets`, `zig build test-oom` | Covered |
| Return/resolve/embargo failure atomicity under OOM or callback failure | `src/rpc/peer/**` | `tests/rpc/peer/rpc_peer_from_peer_zig_test.zig`, `tests/rpc/peer/rpc_release_and_failure_test.zig` | Covered |
| TCP transport frame budget, close, and callback cleanup behavior | `src/rpc/transport/tcp/connection.zig`, `src/rpc/transport/tcp/stream_transport.zig` | `tests/rpc/transport/tcp/rpc_connection_failure_test.zig`, `tests/rpc/peer/rpc_peer_cleanup_test.zig` | Covered |
| QUIC receive/send budget and production hardening controls | `src/rpc/transport/quic/connection.zig` | `tests/rpc/transport/quic/rpc_quic_transport_test.zig`, `zig build -Dquic=true test-rpc-quic`, `zig build -Dquic=true test-resource-budgets` | Covered |
| HostPeer queue budgets and default external exception/abort disclosure | `src/rpc/integration/host_peer.zig` | `tests/rpc/integration/rpc_host_peer_test.zig` | Covered |
| WASM ABI invalid pointers, double frees, output-slot validation, and diagnostic disclosure | `src/wasm/capnp_host_abi.zig` | `tests/wasm_host_abi_test.zig`, `zig build test-wasm-host` | Covered |
| WASM example serde malformed frames and oversized JSON/text | `src/wasm/capnp_host_abi.zig` | `tests/wasm_host_abi_test.zig` example serde tests | Covered |
| Unsafe-pattern drift in input-facing source paths | `src/serialization`, `src/rpc`, `src/wasm` | `zig build hardening` | Covered |
| Resource-budget and OOM regression drift | Serialization, RPC, WASM, codegen | `zig build test-resource-budgets`, `zig build test-oom` | Covered |
| Deterministic fuzz/smoke gate across decode, RPC, QUIC, and peer state | Cross-cutting | `zig build test-fuzz-smoke` | Covered |
| ReleaseSafe hardening coverage | Test/e2e build policy | `zig build test-release-safe`, `release-safe-tests` CI job | Covered |
| Malformed interop/security e2e with raw frame clients | E2E RPC interop | `zig build test-e2e-security`, `tests/rpc/transport/rpc_raw_frame_security_test.zig` | Covered |
| KV example public defaults and sensitive logging | `examples/kvstore` | server defaults/quota unit tests; `cd examples/kvstore && zig build test` | Covered |
| Disclosure scan for banners, build IDs, source paths, stack traces, and verbose close reasons | CI/build policy | `zig build hardening` disclosure scan | Covered |

When adding a new hardening regression, update the `Regression Gate` column to
name the exact command or test file. When an external advisory maps to one of
these classes, add a note to the relevant row rather than creating a one-off
tracking document.
