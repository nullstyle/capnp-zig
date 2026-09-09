# CI portability and release confidence

The schema compiler is now checksum-pinned to Cap'n Proto 1.5.0 through
`tools/capnp-toolchain.json`. The same source bootstrap serves Linux, macOS,
and Windows. Compiler 1.0.2 omits metadata retained by lossless binary reflection,
so it cannot reproduce the committed generated descriptors. The version checks
reject a mismatched compiler before regeneration or package validation; they do
not normalize away schema fields to make a golden pass.

The Linux reflection oracle uses Clang 18 with GCC 14's C++23 standard library.
GCC 13 lacks `<print>`, and GCC 14.2 crashes while optimizing the pinned reference
HTTP implementation. The Clang/library pair built the pristine reference and
replayed all 32 reflection mutation cases in Ubuntu 24.04 on x86_64. Separately,
the 1.5.0 compiler reproduced the committed enum-evolution golden on Linux and
macOS. Windows runtime verification is a hosted CI gate.

Allocator-failure tests now inject errors independently of allocator resize
optimizations. Registry sweeps use a no-resize backing allocator, and RPC L17
sweeps arm injection after fixture setup. They retain budget boundaries,
rollback checks, and proof that an allocation failure was actually injected.
The corrected sweeps passed repeated x86_64 Linux runs, macOS, and the registry
WASI target; L17 also cross-compiles for Windows.

The hardening gate now scans reflection alongside serialization, RPC, and the
generator. Input-dependent optional payloads fail with existing error variants.
Compile-time generic misuse receives a named compile error. Remaining typed
registry, callback, and rollback unwraps have explicit ownership and state
invariants in the one-for-one allowlist. Three Experimental generator helper
error sets widen; the Stable snapshot remains unchanged.

The existing KVStore service now accepts the strict Text-list reader emitted by
the generator. Its build and tests run in the documentation CI job so generated
reader changes cannot silently leave this service uncompilable.

Local validation before the first CI push:

- Full Debug suite: 199 build steps and 1,828 tests passed.
- Code generation, reflection, hardening, and Stable API checks passed.
- Every committed generated artifact reproduced without changes.
- Package preflight passed for clean default, core, and QUIC consumers in Debug
  and ReleaseSafe, including the packaged generator and unchanged-worktree check.
- KVStore build and four service/storage tests passed in ReleaseSafe.

These local results do not count as sustained nightly confidence. Release
readiness still requires seven consecutive successful scheduled Nightly runs
on the release candidate's code, with positive per-target fuzz activity and
successful soak, forward-compatibility, and extended gates. A manual run can
expose failures early but cannot substitute for seven daily cycles. Keep run
URLs and measured fuzz receipts with the eventual release evidence.
