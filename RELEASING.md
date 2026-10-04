# Releasing capnpc-zig

This is the checklist for cutting a tagged release. Follow it top to bottom.

## Why this file exists

`v0.4.0` was tagged three minutes *before* its own push CI went red: the static
hardening gate failed on all three operating systems and the benchmark
regression gate failed with it. Both were repaired twelve days later, on
`main`, unreleased — so for those twelve days the only version anyone could
`zig fetch` was strictly the least trustworthy commit on the branch.

The same cut bumped `build.zig.zon` and inserted one CHANGELOG heading, then
stopped. Every consumer-facing document went on advertising `v0.3.0`, including
both copy-pasteable install snippets, for two consecutive releases.

Neither failure was a judgement call — the ceremony simply was not written
down. It is now, and the parts that can be mechanized are.

## Semver classification

The project is pre-1.0 and follows [semver](https://semver.org/). Decide the
bump **before** editing anything.

| Change | Bump |
|---|---|
| Any change to a declaration in `docs/api-snapshot.txt` (the frozen Stable surface) | **minor** |
| Any change to the *shape of generated code*: `docs/generated-shape.txt` (frozen) or `docs/generated-shape-experimental.txt` | **minor** |
| Breaking change to an Experimental surface (`docs/api-snapshot-experimental.txt`, `docs/api-snapshot-experimental-quic.txt`) | **minor** |
| New functionality: additive declarations on ANY tier (Stable or Experimental) | **minor** |
| Bug fixes, docs, internal refactors | **patch** |

`just check-release-drift <prev-tag> [X.Y.Z]` applies this table to the five
snapshot files. It diffs them against the previous release tag, and it fails
when:

- a Stable file (`docs/api-snapshot.txt` or `docs/generated-shape.txt`)
  removes or changes a line, and the release's CHANGELOG section has no
  Stable `### Breaking` entry;
- any of the five files changed, and the bump is a patch.

The hook reads each top-level bullet under `### Breaking` as one entry. An
entry whose bold title says `Experimental` (`(Experimental)`,
`(Experimental behavior)`) is an Experimental entry; every other entry is a
Stable entry. Only the title counts, so a Breaking section that holds only
Experimental entries does not declare a Stable break.

It warns when an experimental file loses lines and no Breaking entry is
tagged Experimental, and when an entry has no Migration paragraph. When
Stable lines go and a Stable entry exists, it lists the lines in a NOTE: it
cannot tell which entry covers which line, so check that by hand. A file that
did not exist at the previous tag counts as all additions.
`release-preflight` and `release-tag` both run it, against the newest `v*`
tag; the rules are in `tools/release_drift.zig`.

Generated code used to be the row that was easy to get wrong. `zig build
check-api` snapshots *library* declarations only, so a change to what the
plugin **emits** passed it green. `da60cb6` (group-typed union member getters
becoming fallible) is the worked example: a compile break for every downstream
consumer with a group inside a union. Now `zig build check-generated-shape`
(the CI Hardening job, on all three OSes) renders the generated code for a
committed corpus of schemas into `docs/generated-shape.txt` and
`docs/generated-shape-experimental.txt`, so a change like that moves a frozen
line. [What is frozen in generated code](docs/generated-api.md#what-is-frozen-in-generated-code)
lists the Stable families. Generated signatures spell the runtime's error
sets, so a runtime error-set change moves generated-shape lines too.

The gate sees signatures, names and wire constants, not behavior. Still read
the diff to `tests/golden/` and run `just check-generated`. That recipe
regenerates the RPC and e2e bindings, addressbook, ping-pong, kvstore, the
WASM binding, the checked-in V1/V2 schema-evolution fixtures and the
generated-shape corpus; all of their diffs are part of the review surface.

A minor bump with any breaking content needs a `### Breaking` heading in the
CHANGELOG with a **Migration** paragraph in each entry. Do not file breaking
changes under `### Fixed`. Put `(Experimental)` in the bold title of an entry
that breaks only Experimental surface. When one change breaks both tiers,
write two entries, one for each tier.

## 1. Preconditions

```bash
git switch main && git pull --rebase
git status --short          # must be empty
```

- [ ] The tree is clean and `main` is up to date with `origin/main`.
- [ ] The `[Unreleased]` CHANGELOG section covers **every** commit since the
      last tag. Check with `git log --oneline <last-tag>..HEAD` and reconcile
      one line at a time.
- [ ] The bump is classified per the table above.
- [ ] `mise run bootstrap:capnp` and `mise run check:capnp` verify the pinned
      WASM compiler package and Wasmtime runtime. Review compiler-provided
      reflection metadata whenever the compiler pin changes.
- [ ] `just check-generated` is clean; no committed consumer binding was made
      stale by a generator change.
- [ ] `just package-preflight` passes. This is the manifest-filtered package
      gate, not a working-tree/path-dependency build: it archives and re-fetches
      the `.paths` result, runs the documented pinned-plugin codegen consumer
      (`dep.artifact("capnpc-zig")`, `tests/package_consumer/codegen`),
      exercises default/core/QUIC consumers in Debug and ReleaseSafe, runs the
      packaged plugin, checks lazy QUIC fetching, and leaves the checkout
      unchanged.
- [ ] Every API a downstream uses is on the package consumers' forced list
      (`tests/package_consumer/src/`: `common.zig` for every root, plus
      `default.zig`, `core.zig` and `quic.zig`). Zig analyzes lazily, so the
      consumers build even when an API they never reference no longer
      compiles. v0.18.0 shipped a TCP `Transport.read` that did not compile on
      tagged Zig 0.17.0 for this reason. When a downstream starts using an API
      that is not listed, add it before you tag: `_ = &Type.function;` with a
      comment that names the downstream. Forcing a function does not force the
      methods of the value it returns. For example, `readU32List` is forced but
      `U32ListReader.get` is not, so list each method that the downstream
      calls on a returned value. A generic function needs a call with concrete
      arguments inside a never-called function that the root takes with `&`,
      as `forceSerializationGenerics` in `common.zig` and `forceQuicGenerics`
      in `quic.zig` do.

## 2. The commit's CI must already be green

This is the rule `v0.4.0` broke. The tag must land on a commit whose push CI has
**already concluded successfully** — not one that is queued, not one that is
running.

```bash
gh run list --branch main --limit 5
gh api "repos/:owner/:repo/actions/runs?head_sha=$(git rev-parse HEAD)" \
  --jq '.workflow_runs[] | "\(.name) \(.conclusion) \(.html_url)"'
```

- [ ] Every job on the target commit reports `success`. If any job is red or
      missing, fix it and re-run this step against the new HEAD — never tag
      through a red gate.

Run the heavy local gates too; they cover lanes hosted CI does not:

```bash
just release-preflight X.Y.Z
```

`release-preflight` first runs `check-release-drift` against the newest `v*`
tag with the version you pass, so a bump that under-declares the snapshot
drift fails before the long gates start. Without a version it reads
`build.zig.zon`; before the version sweep that is still the previous
release's version, so the hook only checks `[Unreleased]` and prints the bump
the drift needs.

`release-preflight` includes `package-preflight`. Do not use
`--skip-quic` here; that switch exists only for constrained local diagnosis.
The preflight confines its source snapshot, consumer projects, install trees,
and Zig caches to its disposable workspace, so a globally cached dependency or
an unfiltered checkout cannot make a broken package look healthy.

## 3. Version sweep

Bump the manifest first, then let the gate find the rest:

```bash
$EDITOR build.zig.zon        # .version = "X.Y.Z"
zig build docs-smoke         # fails, listing every doc still on the old version
```

`tools/docs_examples_smoke.zig` (`version_needles` / `version_pin_markers`)
asserts the manifest version appears in each consumer-facing location and that
no `zig fetch` pin names a different one. Work through its failures until it
passes:

- [ ] `build.zig.zon` — `.version`
- [ ] `README.md` — status banner and install pin
- [ ] `docs/build-integration.md` — install pin, `.url`, `.hash` example
- [ ] `docs/supported-surface.md` — title, opening sentence, pinning advice,
      "Known limitations" heading
- [ ] `docs/stability.md` — "The current version is …"
- [ ] `CHANGELOG.md` — dated section and link-footer entry (below)
- [ ] Every caveat that says a documented feature is not released yet. Each
      one carries an `unreleased-after` HTML comment naming the previous
      version, and docs-smoke lists every one the bump left behind. The
      feature now ships: delete the caveat and its marker, or rewrite it as a
      plain "needs vX.Y.Z or later" note.
- [ ] `src/codegen_abi.zig` — if this release raised `version`, `release` must
      name this release: every generated file quotes it in its skew error.
      (Not a gate: it changes only when the codegen ABI does.)

If you add a new consumer-facing version stamp, add it to `version_needles` in
the same commit — otherwise the next cut will miss it exactly the way this one
missed the others. If you document a feature before it ships, mark the caveat
with `unreleased_marker` (see `tools/docs_examples_smoke.zig`) for the same
reason.

## 4. CHANGELOG

- [ ] Rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` and open a fresh
      empty `## [Unreleased]` above it.
- [ ] Move any breaking entries into a `### Breaking` heading, each with a
      **Migration** paragraph. Tag an Experimental-only entry with
      `(Experimental)` in its bold title (see Semver classification).
- [ ] Update the link footer: repoint `[Unreleased]` at
      `compare/vX.Y.Z...HEAD` and add `[X.Y.Z]: …compare/<prev>...vX.Y.Z`.

## 5. Land, verify, tag

```bash
git commit -am "release: cut vX.Y.Z"
git push
gh run watch                       # the release commit must go green too
```

- [ ] The release commit's own CI is green before the tag is created.

```bash
just release-tag X.Y.Z "<one-line theme>"
```

`release-tag` refuses a dirty tree, a `build.zig.zon` that does not say
X.Y.Z, a failing `check-release-drift` (the CHANGELOG now has its
`## [X.Y.Z]` section, so the hook reads that one), a failing docs-smoke, and a
commit without a green CI run. Then it creates and pushes the annotated tag,
the same as:

```bash
git tag -a vX.Y.Z -m "vX.Y.Z — <one-line theme>"
git push origin vX.Y.Z
```

## 6. Post-tag

- [ ] **Validate the published archive with a real fetch.** The hermetic
      `package-preflight` has already proven a locally generated filtered
      archive; this step separately proves the exact hosted artifact. `.paths` in
      `build.zig.zon` controls what ships, and its breakage is invisible from a
      working tree — a path dependency and a local checkout both hide it.

      ```bash
      cd "$(mktemp -d)" && zig init
      zig fetch --save "git+https://github.com/nullstyle/capnp-zig.git#vX.Y.Z"
      ```

      **A fetch is not enough — BUILD against it, in all THREE consumer
      configurations.** `zig fetch` only downloads and hashes; it compiles
      nothing, so it cannot see a declaration missing from a library root.
      v0.8.0 was tagged with `canonical` exported from `src/lib.zig` but not
      `src/lib_core.zig` — the module the docs tell serialization-only
      consumers to import — and the fetch passed while a three-line consumer
      importing `capnpc-zig-core` did not compile. The SAME export was also
      missing from `src/lib_quic.zig`, found a release later.

      The three configurations, each a root that can diverge independently:

      1. `dep.module("capnpc-zig")` — `src/lib.zig`
      2. `dep.module("capnpc-zig-core")` — `src/lib_core.zig`
      3. `b.dependency("capnpc_zig", .{ ..., .quic = true })` then
         `dep.module("capnpc-zig")` — `src/lib_quic.zig`

      In each, call something *new in this release* — the version bump is the
      whole reason the release exists, so exercise the thing it added.

- [ ] Record the resulting hash in `docs/build-integration.md`, replacing the
      `capnpc_zig-X.Y.Z-...` placeholder with the real value. That turns the
      install snippet into a self-verifying artifact. Then prove the record
      with `just verify-release-hash X.Y.Z` — it re-fetches the published tag
      in a clean consumer and asserts the docs carry the exact hosted hash
      (the stale-digest bug shipped two releases while this was manual).

- [ ] Create the GitHub Release from the CHANGELOG section, so the tag has a
      rendered notes page and watchers get a notification:

      ```bash
      gh release create vX.Y.Z --title "vX.Y.Z" --notes-file <(...)
      ```

- [ ] Announce the supersession if this release corrects a bad tag: say plainly
      in the CHANGELOG prose which version it replaces and why.

## Cadence

Do not let `main` drift far past the tag. The failure mode is specific: fixes
land on `main`, the fetchable version stays stale, and the only version
consumers can reach becomes the *worst* one on the branch. Cut a release
whenever `[Unreleased]` accumulates a `### Fixed` entry that touches a Stable
tier.
