# Repository schema toolchain

Repository development tooling uses the Cap'n Proto 2.0-dev compiler from the
compiler-only archive published by [capnpc-wasm](https://github.com/nullstyle/capnpc-wasm/releases)
(`capnp-wasm-tools` 0.1.0-rc.3 or newer). `tools/capnp-toolchain.json`
identifies the archive, source commit, manifest, compiler module and standard
schemas by hash. Wasmtime is pinned in `mise.toml`; the archive's launcher also
accepts a newer patch release of the same series (48.0.x).

The work is split at the archive boundary:

- `tools/capnp_tool.py` is the consumer side. It owns the pin, downloads the
  archive, checks its digest, extracts it with path checks, and verifies the
  installed package against the pinned manifest digest on every invocation.
- The archive's portable launcher, `package/bin/capnp-wasm.py` (capnpc-wasm's
  launcher contract), does the rest. It runs Wasmtime directly on Linux,
  macOS and Windows, translates caller paths, and publishes generator output
  only after success. `capnp_tool.py` runs it with
  `CAPNP_WASM_EXPECT_MANIFEST_SHA256` set to the pinned manifest digest, so the
  launcher refuses a package that changed after the check.

## Commands

Run these from the repository root:

```sh
mise run bootstrap:capnp
mise run check:capnp
mise exec -- just gen
mise exec -- just check-generated
mise exec -- just package-preflight
```

For a compiler command, arguments after `--` follow the reference CLI:

```sh
mise exec -- uv run --no-project --python 3.13 tools/capnp_tool.py compiler -- \
  compile -o- tests/test_schemas/example.capnp > request.bin
```

The driver preserves binary stdin/stdout and command failure status. Installed
package corruption, a missing Wasmtime runtime, or a compiler failure is an
error; there is no fallback to a native `capnp` found on PATH. Launcher
failures use the launcher's exit statuses (64 usage, 66 missing input, 69 or 78
Wasmtime, 73 output conflict, 74 package verification, 134 trap); a failure of
the pin, download or package check exits 1.

## Compiler and generator boundaries

The compiler emits an unpacked `CodeGeneratorRequest`. The host then runs the
Zig generator as a separate process. `just gen` builds that generator from the
current checkout; package preflight builds it from the manifest-filtered release
package and checks its generated output. The compiler-only archive supplies no
Zig generator or application runtime.

`generate --plugin PATH --output DIR -- SCHEMA_ARGS...` implements this
pipeline through the launcher's `generate` mode. Its schema arguments omit
`compile` and `-o`; the launcher supplies `compile -o-`. Repeated
`--plugin-arg` options carry generator flags. Compilation and generation
complete in temporary storage before successful files are published to the
requested directory; a symlink, a directory where a file belongs, or a
read-only file in the output fails the run before anything moves.

Ordinary library and application builds continue to use checked-in bindings.
They require no compiler download or Wasmtime installation. The distributed
native `capnpc-zig` retains its standard plugin interface.

## Paths and imports

The launcher exposes a single common input root. KJ opens input files relative
to one root descriptor, so additional WASI mount points cannot supply imports.
It translates schema paths, include paths and source prefixes together, adding
a caller-directory source prefix when necessary to preserve relative generated
filenames. Explicit ancestor prefixes retain their meaning. Paths with spaces
are passed as individual arguments, without shell interpolation.

On Windows, explicit schema and include inputs must share a volume; copy a
multi-volume input set into one workspace before invoking the compiler. When
the installed package lies outside that root (on any OS), the launcher copies
its small standard-schema tree into a temporary `.capnp-wasm-include.*`
directory in the current directory for the run.

Bundled standard schemas supplement the caller's explicit include paths.
`--no-standard-import` disables that supplement. Serialization tests retain
their explicit vendored include tree. The packaged-streaming test deliberately
uses only `-Isrc/rpc` with standard imports disabled: it must fail if the package
omits a required RPC annotation schema.

## Updating the pin

After capnpc-wasm publishes a tools release, fill the pin from the release's own
assets, then compare the printed archive and manifest digests with the
release's row in capnpc-wasm's `docs/releases.md` (published releases):

```sh
mise exec -- uv run --no-project --python 3.13 tools/update_capnp_toolchain.py tools capnp-wasm-tools-v0.1.0-rc.3
mise run bootstrap:capnp
mise exec -- uv run --no-project --python 3.13 -m unittest discover -s tools -p test_capnp_tool.py
mise exec -- just check-generated
```

The script checks the manifest asset and the archive against the release's
`SHA256SUMS` and derives the other fields from the manifest. For a draft
release, download the three assets with `gh release download <tag> --dir DIR`
and pass `--assets DIR`.

## Compiler upgrade and reference tests

The previous repository compiler was 1.5.0. The pinned 2.0-dev compiler emits
richer schema-node source ranges and source information. Lossless reflection
retains the original node fields, so this upgrade intentionally changes
embedded descriptor bytes. Review these changes without dropping metadata to
force equality. Generated field APIs and runtime wire behavior have separate
tests; a metadata-only difference does not imply a wire-format change.

Native C++ reflection, generic RPC and streaming checks retain a compiler and
C++ generator matched to their Cap'n Proto/KJ libraries. Docker reference peers
also retain their language generators and runtimes. Those libraries provide
independent decoding and live RPC behavior that a synchronous WASM command
does not implement.

CI exercises the portable driver and required compiler-dependent tests on
Linux, macOS and Windows. Generation drift, Stable API checks, native/WASI
reflection, package preflight and native C++ interoperability cover the
boundaries above.

The upgrade review found changes in 17 generated descriptor bundles. Clearing
only `Node.startByte` and `Node.endByte`, then canonicalizing both versions with
the reference compiler, produced identical bytes in every case. Generated code
outside those bundles and the Stable API snapshot were unchanged. The Go
example regenerated without changes using its vendored generator.

Local migration validation passed 17 driver tests, 53 focused Zig caller tests
without skips, and the full Debug graph (199 build steps, 1,924 tests). Package
preflight passed default, core and QUIC consumers in Debug and ReleaseSafe,
including the extracted native plugin and unchanged-worktree check. Formatting,
hardening, documentation and API checks also passed. Windows runtime acceptance
is provided by the hosted three-OS matrix.

Moving from `capnp-wasm-tools` 0.1.0-rc.2 to rc.3, the first archive with the
portable launcher, changes no committed request, binding, golden or API
snapshot: with a candidate built from the rc.3 source, `just check-generated`
passed unchanged, and the bundled standard schemas have the same digest as in
rc.2. Run it again after filling the pin from the published release.
