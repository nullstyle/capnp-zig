# Implementation Plan (Current)

Updated: 2026-02-05

1. Parse `CodeGeneratorRequest` (nodes, fields, types, basic values) and wire into codegen entrypoint. **Done**
2. Expand codegen for constants/defaults/annotations (typed values, pointer defaults, annotation uses) with tests. **Done**
3. Integrate official Cap’n Proto test corpus / `capnp-test` fixtures and interop harness. **In progress**
4. Add schema-driven validation and canonicalization APIs. **Pending**
5. Extend benchmarks (packed/unpacked, list-heavy, far-pointer focus). **Pending**
