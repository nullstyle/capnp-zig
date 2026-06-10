# Security Policy

capnpc-zig parses untrusted input by design — wire messages, packed streams,
and RPC frames from remote peers. Security reports are taken seriously.

## Reporting a vulnerability

Please report vulnerabilities privately via
[GitHub private vulnerability reporting](https://github.com/nullstyle/capnp-zig/security/advisories/new)
on this repository. If that is unavailable to you, email `nullstyle@gmail.com`
with `[capnp-zig security]` in the subject.

Please include a reproduction (input bytes or a failing test) where possible.
You can expect an acknowledgment within a week. Please allow a fix to land
before public disclosure.

## Supported versions

This project is pre-1.0; only the `main` branch is supported. There are no
backported security fixes to older commits or tags.

## What counts as a vulnerability

In scope:

- Memory-safety escapes reachable from untrusted bytes: out-of-bounds reads
  or writes, type confusion through pointer validation, use-after-free in
  message or RPC frame handling.
- Resource-exhaustion amplification that bypasses the documented limits
  (`Message` validation options, `PeerLimits`, transport queue bounds,
  framer caps) — e.g. an input whose processing cost or memory footprint is
  unbounded despite limits being configured.
- RPC protocol-state corruption triggerable by a remote peer (capability
  table confusion, refcount desynchronization that frees live capabilities).

Out of scope (file ordinary issues instead):

- Resource exhaustion when limits were left at permissive values or
  validation was explicitly skipped (`Message.initUnvalidated` is documented
  as trusted-input-only).
- Crashes in the compiler plugin (`capnpc-zig`) on malicious *schemas* — the
  schema compiler runs on developer machines with developer-provided input;
  hardening it is tracked as ordinary robustness work.
- Denial of service against the QUIC transport while it is marked
  experimental in [docs/stability.md](docs/stability.md).

## Hardening posture

The repository maintains a security regression matrix
([docs/security-regression-matrix.md](docs/security-regression-matrix.md)),
deterministic fuzz/OOM/resource-budget gates in CI, coverage-guided fuzz
targets (`zig build test-fuzz --fuzz`), and a nightly soak harness. New
parser or protocol code is expected to come with bounds-checked accessors
and regression coverage.
