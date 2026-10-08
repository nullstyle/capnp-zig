# Fuzz seeds

- `framing_fixtures.json`, `framing_fixtures.README.md`: copied verbatim from
  capnp-zig at tag `v0.21.0` (`tests/fixtures/framing/`), which is not part of
  the hash-pinned package (plan §1.1). Re-copy when the pin moves:

  ```sh
  git -C ../capnp-zig show v0.21.0:tests/fixtures/framing/framing_fixtures.json > core/fuzz/seeds/framing_fixtures.json
  ```

`core/src/fuzz_abi.zig` (`zig build fuzz-abi -- --seconds N`) pushes every
chunk of every case as-is and in mutated forms, besides the frames two live
connections produce for each other.
