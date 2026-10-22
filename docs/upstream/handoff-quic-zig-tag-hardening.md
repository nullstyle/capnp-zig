# HANDOFF — quic-zig: tag a release that contains `021591e`

> **Status (2026-10-21): open, not sent yet.** Owner decision D2 = B in
> [sprint-plan-2026-10-21.md](../sprint-plan-2026-10-21.md): capnp-zig
> v0.29.0 stays on quic-zig v0.38.0, and the owner sends this request now.
> capnp-zig moves to the new tag in its own release (v0.29.1 or v0.30.0),
> after the tag exists. capnp-zig first decided to ask for this tag on
> 2026-10-16 (owner decision D1 = B in
> [sprint-plan-2026-10-15.md](../sprint-plan-2026-10-15.md)). The text below
> is the request.

For the agent that works on nullstyle/quic-zig. It is self-contained. The
facts come from the quic-zig checkout on this machine
(`/Users/nullstyle/prj/zig/quic-zig`), read on 2026-10-21. This is a
document for the owner to relay, not a filed issue.

## The ask

Tag a release at `b0d7d78` or later. It must contain `021591e`. Give it:

1. A CHANGELOG entry for the hardening in `021591e` and for the integrator
   effects of `b0d7d78`.
2. A DOWNSTREAM-NOTE entry: a row in "Released change history" in
   `docs/DOWNSTREAM_INTEGRATION.md`, with the integrator effect and the
   required action. Move "Current pin and build options" to the new tag.
3. The release evidence that v0.42.2 has: the gates at the exact tag
   commit, a fresh-cache URL consumer, and the package hash.

## Why capnp-zig asks

- `021591e` (2026-10-13, "fix: harden loss recovery, OOM
  transactionality, multipath routing, and DoS surfaces") is on `main`, but
  in no tag. `git tag --contains 021591e` prints nothing. The newest tag is
  v0.42.2 (`2aa5bae`, 2026-10-10). `main` is at `b0d7d78` (2026-10-13), on
  this machine and on GitHub (`git ls-remote origin`, 2026-10-21).
- capnp-zig pins only tags: each capnp-zig release pins one quic-zig tag by
  its package hash. capnp-zig pins v0.38.0 today (`build.zig.zon`).
- One change in `021591e` is on a path that a peer reaches before the
  application knows who it is: the duplicate-ID scan in
  `TransportParams.decode`.
  - Before `021591e`, `decode` called `hasParameterId` on all the bytes
    before each parameter (`src/tls/transport_params.zig:388` and `:698`
    at `021591e^`). The scan was O(N^2) in the number of parameters.
  - The peer picks N. quic-zig decodes the peer's parameters when the TLS
    handshake completes (`pumpTlsInbox`,
    `src/Connection/recv_data_handlers.zig:249` and `:257`, at `b0d7d78`),
    before the embedder sees the connection. Without client certificates,
    the server has not authenticated the client at that point.
  - `021591e` uses a bitset for the known IDs and a list for the unknown
    IDs, and it caps the number of parameters. The doc comment of the cap
    says that it stops "quadratic CPU work during the handshake"
    (`transport_params.zig:150-153`).
- Nobody measured a DoS. capnp-zig did not measure the CPU cost of a large
  parameter blob. The commit adds a test for the cap ("decode caps total
  number of transport parameters") and no benchmark. capnp-zig does not
  claim a DoS. Its `SECURITY.md` (`:44-45`) does not cover denial of
  service against its QUIC transport while that transport is Experimental.
- Other parts of `021591e` touch paths that capnp-zig's QUIC lanes run:
  0-RTT with persisted session tickets (the `AntiReplayTracker` change),
  the Retry preset (the multi-packet Initial Retry SCID change), and a soak
  under memory pressure (the out-of-memory changes).

## What the notes should say

Please put these facts in the CHANGELOG entry, so that a downstream can
move without reading the diff:

1. **The parameter cap.** The commit message says
   `max_total_parameters = 128`. The code at `b0d7d78` caps at 64
   (`max_transport_parameters`, `transport_params.zig:154`, checked at
   `:389`), and the unknown-ID list has 64 entries (`:385`). Say which
   number you intend. Say what a peer that sends more parameters gets: the
   code returns `Error.InvalidValue`. Give the transport error code that
   the peer sees.
2. **New and changed errors.** For example, `Server.init` now checks
   `accepted_versions.len` (`src/Server.zig:902-903` at `b0d7d78`).
3. **Behavior that an embedder can see.** The 0-RTT anti-replay change
   (`AntiReplayTracker` clamps non-monotonic timestamps), the path routing
   (`activePath()` in place of `primaryPath()`), and the `b0d7d78` changes
   (the `SendStream.in_flight` ring buffer, and the RTT and ACK timing).
4. **The upgrade path.** Can a downstream on v0.38.0 move straight to the
   new tag? Which release notes must it read? capnp-zig crosses v0.39.0 to
   v0.42.2 in the same move.
5. **The toolchain.** The Zig version that the tag needs, and whether the
   stock Zig 0.17.0 Linux build passes (the v0.42.1 rejection). capnp-zig
   pins tagged Zig 0.17.0.

## What capnp-zig does after the tag

capnp-zig moves in its own release (v0.29.1 or v0.30.0), not in v0.29.0.
v0.29.0 already changes the RPC answer paths, and a QUIC change in the
same release would make a regression hard to place. The move is
capnp-zig's work:

- the pin in `build.zig.zon`;
- the `-Dquic=true` API snapshot gates (`check-api-quic` and
  `check-api-experimental-quic`);
- the QUIC soak lanes and `e2e-quic`;
- the http3-zig pair, which is v0.5.7 with quic-zig v0.38.0 today.

## Not checked

- capnp-zig did not build or test `021591e` or `b0d7d78`. Nothing above
  says that the tag fixes a capnp-zig defect.
- Nobody checked whether a move from v0.38.0 to the new tag lets capnp-zig
  delete a workaround.
