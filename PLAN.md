# Implementation Plan (Current)

Updated: 2026-02-06

1. Parse `CodeGeneratorRequest` (nodes, fields, types, basic values) and wire into codegen entrypoint. **Done**
2. Expand codegen for constants/defaults/annotations (typed values, pointer defaults, annotation uses) with tests. **Done**
3. Integrate official Cap’n Proto test corpus / `capnp-test` fixtures and interop harness. **Done**
4. Add schema-driven validation and canonicalization APIs. **Done**
5. Extend benchmarks (packed/unpacked, list-heavy, far-pointer focus). **Done**
6. Implement Cap'n Proto RPC runtime + codegen (capability pointers, bootstrap, connection, interfaces) and interop harness. **Done**

Next focus (production hardening, not core parity):
- Expand external matrix interop/e2e coverage (Go + C++ containers) and keep it as a CI hard gate.
- Add long-haul RPC soak/perf regressions and explicit failure-injection scenarios.
- Continue API ergonomics and docs cleanup as features stabilize.

Detailed production parity checklist: `docs/production_parity_checklist.md`
