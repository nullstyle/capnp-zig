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
| Any change to the *shape of generated code* — accessor signatures, fallibility, emitted type names | **minor** |
| Breaking change to an Experimental surface (`docs/api-snapshot-experimental.txt`) | **minor** |
| Bug fixes, additive Stable declarations, docs, internal refactors | **patch** |

The generated-code row is the one that is easy to get wrong: `zig build
check-api` snapshots *library* declarations only, so a change to what the
plugin **emits** passes the freeze gate green. `da60cb6` (group-typed union
member getters becoming fallible) is the worked example — it is a compile break
for every downstream consumer with a group inside a union, and the gate could
not see it. Until the freeze gate covers generated shape, classify codegen
output changes by reading the diff to `tests/golden/` and running
`just check-generated`. That gate regenerates the RPC and e2e bindings,
addressbook, ping-pong, kvstore, the WASM binding, and the checked-in V1/V2
schema-evolution fixtures; all of their diffs are part of the review surface.

A minor bump with any breaking content needs a `### Breaking` heading in the
CHANGELOG with a **Migration** paragraph. Do not file breaking changes under
`### Fixed`.

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
- [ ] `just check-generated` is clean; no committed consumer binding was made
      stale by a generator change.

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
just release-preflight
```

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

If you add a new consumer-facing version stamp, add it to `version_needles` in
the same commit — otherwise the next cut will miss it exactly the way this one
missed the others.

## 4. CHANGELOG

- [ ] Rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` and open a fresh
      empty `## [Unreleased]` above it.
- [ ] Move any breaking entries into a `### Breaking` heading with a
      **Migration** paragraph.
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
git tag -a vX.Y.Z -m "vX.Y.Z — <one-line theme>"
git push origin vX.Y.Z
```

## 6. Post-tag

- [ ] **Validate the published archive with a real fetch.** `.paths` in
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
      install snippet into a self-verifying artifact.

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
