# Design note: one build graph, two library roots

Status: investigated 2026-10-07 for v0.23.0. No code change. The fix that
would remove the problem is not additive, so it needs an owner decision
(see [Decision](#decision)).

## The problem

capnp-zig exports two modules over one `src/` tree:

- `capnpc-zig`, rooted at `src/lib.zig` (`src/lib_quic.zig` with
  `-Dquic=true`): serialization, codegen and RPC.
- `capnpc-zig-core`, rooted at `src/lib_core.zig`: serialization and codegen,
  with a narrower `rpc` (`src/rpc/mod_core.zig`).

Both roots import the same files with relative `@import`s. Zig lets a file
belong to one module per compilation, so one compilation that imports both
modules fails. A scratch consumer reproduces it on tagged Zig 0.17.0 at
`72d6d7f`: a library module bound to `capnpc-zig-core` (the generated
`examples/addressbook.zig` plus a helper that builds a `Person`) and an
executable that also imports `capnpc-zig`:

```
src/serialization/message.zig:1:1: error: file exists in modules 'capnpc-zig' and 'capnpc-zig0'
src/serialization/message.zig:1:1: note: files must belong to only one module
src/lib.zig:8:29: note: file is imported here by the root of module 'capnpc-zig'
src/lib_core.zig:4:29: note: file is imported here by the root of module 'capnpc-zig0'
```

Who hits it: a library that binds `capnpc-zig-core` so it stays safe for
wasm32, used by a native program that also needs RPC. slcp-zig works around
it twice (`build.zig:80-87` and `:227-229`). [Troubleshooting](../troubleshooting.md#one-module-root-per-binary)
documents the error. The same class covers two dependency instances with
different `quic` or `fd-passing` options.

## What works today

Bind the library to whichever module the program uses. Outside `rpc`, both
roots import the same files, and the full root adds only `io_backend` (the
parity tests in `src/lib_core.zig` fail when core lacks a name full has). So
code written against core, generated code included, compiles against full.
In the repro above, binding the library and the generated module to
`capnpc-zig` instead of `capnpc-zig-core` compiles.

A library that must serve both kinds of program creates two instances of its
own module, one per capnp-zig module. slcp-zig does this: `slcp-core` binds
`capnpc-zig-core` for its wasm graphs, and `slcp_core_native` binds the full
module for its native graph. The two instances never meet in one
compilation, so their distinct types do not matter.

## Options for capnp-zig

**A. Document the pattern.** Zero risk. Done for v0.23.0: the
troubleshooting entry links here.

**B. Split the tree into inner modules that own disjoint files, with thin
facades.** An inner serialization module owns `src/serialization`,
`src/reflection`, `src/capnpc-zig` and `src/codegen_abi.zig`; an inner RPC
module owns `src/rpc`. `capnpc-zig` and `capnpc-zig-core` become facades that
only re-export. A graph can then import both facades and gets one copy of
every type, so a core-bound library and a full-bound program share
`message.MessageBuilder`. Costs:

- 41 relative imports in 38 files under `src/rpc` that reach
  `../serialization`, `../reflection` or `../capnpc-zig` become module
  imports. `src/rpc/mod.zig` and `src/rpc/mod_core.zig` must split along the
  same line, and the `-Dquic` root swap must become an option of the inner
  RPC module.
- The `capnp_build_options` module must stay one module per dependency
  instance.
- Consumers that wire modules by hand (`zig build-lib -Mcapnpc-zig=...`,
  which troubleshooting.md documents) need new `--dep` and `-M` flags. That
  is a Breaking change for them, even though no API line changes.
- Snapshots: the API renderer prints types by their path below the module's
  root directory (`serialization.message.Message`). Inner roots kept directly
  in `src/` should leave every line of `docs/api-snapshot.txt` byte-identical.
  This is assumed, not yet proven; `zig build check-api` decides it.
- Gate: a `mixed` consumer in `zig build package-preflight` (the repro
  above), green, with the snapshot files unchanged.

**C. Export `capnpc-zig-core` as the full module on hosted targets.** A
throwaway prototype (one line in `build/modules.zig`, reverted) made the
repro build: the compile became identical to the workaround's. It is not
recommended:

- It changes what `capnpc-zig-core` means: its `rpc` becomes the full RPC
  surface, `io_backend` appears, and transport files enter the graph (left
  unanalyzed unless referenced). The documented promise that core pulls no
  TCP or QUIC transport into the build no longer holds.
- The meaning differs by target: wasm32-freestanding keeps the narrow root.
  Code that compiles natively against "core" can then fail on wasm32.
- A consumer that passes `.quic = true` gets the QUIC root through core too.

## Decision

Recommended: A now (done), B later as its own change, after the owner
accepts the Breaking change for hand-wired consumers and picks the release.
Questions for the owner:

1. Is the hand-wired `-M` path part of the contract that B would break, or
   can B ship in a minor release with an upgrade note?
2. Should B wait for the 1.0 charter, so the module layout freezes once?
