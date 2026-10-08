# Framing conformance fixtures

`framing_fixtures.json` pins the observable behavior of capnp-zig's RPC
frame reassembler (`rpc.wire.framing.Framer`): byte streams in, frames or
errors out.

These are meant to be **vendored**. Copy the JSON into your own test suite
and run it against your reader — whether that is our `Framer` reused
directly, or an independent implementation of the same length-prefixed
frame shape. capnp-zig executes the same file in
`tests/rpc/wire/framing_fixtures_test.zig`, so the published bytes cannot
drift away from the implementation without a red build here first.

## Downloading a pinned corpus

The fixtures were first published in **v0.16.0**. Vendor the
[JSON](https://raw.githubusercontent.com/nullstyle/capnp-zig/v0.16.0/tests/fixtures/framing/framing_fixtures.json)
and its [format description](https://github.com/nullstyle/capnp-zig/blob/v0.16.0/tests/fixtures/framing/README.md)
from that tag; the [reference runner](https://github.com/nullstyle/capnp-zig/blob/v0.16.0/tests/rpc/wire/framing_fixtures_test.zig)
shows how capnp-zig executes the same cases. Pin the revision when vendoring so
an upstream change cannot silently replace your conformance corpus.

The SHA-256 of the v0.16.0 JSON is:

```text
53a4a2f3026ae959d1b01761d6893cb77514345200849571065a2ebd0231caae
```

Download from the tagged repository, not the Zig dependency cache:
`build.zig.zon` deliberately excludes development-only `tests/` from the
published Zig package. Adding capnp-zig as a Zig dependency therefore does not
install these fixtures.

## Running them

Each case carries `chunks` (hex byte strings). Feed them to your reader in
order; after each chunk, pop frames until the reader reports "no complete
frame yet". Then compare against `expect`:

- `frames` — the frames that must pop, in order, across the whole
  sequence (hex).
- `error` — the error that must be raised, or `null`. Names are
  `InvalidFrame` (malformed header) and `FrameTooLarge` (a declared or
  buffered size past a limit).
- `error_on` — `"push"` when the error comes from feeding bytes (the
  buffered-bytes ceiling), `"pop"` when it comes from parsing.
- `options.max_buffered_bytes` — when present, construct the reader with
  this ceiling instead of the default.

`constants` records the limits the fixtures were generated against. The
runner asserts them against the live implementation: if they ever change,
every vendored copy is stale and must be regenerated.

## Coverage

Reassembly (single push, byte-at-a-time, split across chunk boundaries,
two frames in one push), incompleteness (truncated frame and sub-header
prefix both yield nothing, not an error), and every rejection path:
segment count at the limit (accepted) and over it, segment-count
overflow, total words past `max_frame_words`, and a buffered-bytes
breach at push time.
