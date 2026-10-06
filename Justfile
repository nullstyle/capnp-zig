# Windows: recipes assume a POSIX shell. Git Bash (installed with Git for
# Windows) provides `sh`; pinning it here makes `just` use it explicitly
# instead of silently falling back to cmd/powershell semantics.
set windows-shell := ["sh", "-cu"]

# Pinned Windows Maker can inherit a sibling runner's output pipes. Full-suite
# recipes warm their compile graph first; every test recipe serializes Maker
# jobs on Windows. This does not change concurrency inside a test executable.
test_jobs := if os() == "windows" { "-j1" } else { "" }
# Windows has no std.Io.Evented, so the evented canary cross-checks the Linux
# (Uring) backend there. It is a compile-only object, so no Linux libc or
# runner is needed, and the canary keeps its teeth on every host.
evented_canary_target := if os() == "windows" { "-Dtarget=x86_64-linux" } else { "" }
capnp_tool := justfile_directory() + "/tools/capnp_tool.py"

# Build the plugin
build:
    zig build

# Build in release mode
release:
    zig build -Doptimize=ReleaseSafe

# Build optional QUIC-enabled targets
build-quic:
    zig build -Dquic=true --summary all

# Build WASM host target
wasm-build:
    zig build wasm-host --summary all

# Run tests
test:
    @if [ "{{ os() }}" = windows ]; then zig build test-compile --summary all; fi
    zig build {{ test_jobs }} test --summary all

# Run the RPC ping-pong example
example:
    zig build example-rpc

# Run the RPC ping-pong example over QUIC (quic.serve + quic.connect)
example-quic:
    zig build -Dquic=true example-rpc-quic

# Run the RPC ping-pong example over a Unix-domain socket (Linux, macOS)
example-unix:
    zig build example-rpc-unix

# Run the fd-passing example over a Unix-domain socket (Linux, macOS)
example-fd:
    zig build example-rpc-fd

# Run static hardening gates
hardening:
    zig build hardening

# Validate the manifest-filtered package through clean-room default, core, and
# QUIC consumers without publishing anything.
package-preflight:
    zig build package-preflight --summary all

# Run serialization-focused tests (message/codegen/schema/interop)
test-serialization:
    zig build {{ test_jobs }} test-serialization --summary all

# Run all RPC tests
test-rpc:
    zig build {{ test_jobs }} test-rpc --summary all

# Run the seven focused Level-3 handoff suites
test-rpc-l3:
    zig build {{ test_jobs }} test-rpc-l3 --summary all

# Run resource-budget regression tests
test-resource-budgets:
    zig build {{ test_jobs }} test-resource-budgets --summary all

# Run OOM/failing-allocator regression tests
test-oom:
    zig build {{ test_jobs }} test-oom --summary all

# Run deterministic hardening fuzz/smoke coverage
test-fuzz-smoke:
    zig build {{ test_jobs }} test-fuzz-smoke --summary all

# Run documentation/example smoke coverage
docs-smoke:
    zig build docs-smoke --summary all

# Compile documentation snippet fixtures
test-docs-snippets:
    zig build {{ test_jobs }} test-docs-snippets --summary all

# Compile optional QUIC documentation snippet fixtures
test-docs-snippets-quic:
    zig build {{ test_jobs }} -Dquic=true test-docs-snippets-quic --summary all

# Run key hardening gates under ReleaseSafe
test-release-safe:
    zig build {{ test_jobs }} test-release-safe --summary all

# Run the FULL suite under ReleaseSafe — the mode CI's per-OS job uses.
#
# Not the same thing as `test-release-safe`, which is a focused subset. This
# lane exists because it has now caught two defects nothing else could see: an
# OOM harness that reported a deterministic function as nondeterministic, and a
# dangling `ctx` pointer that segfaults on amd64 while a still-mapped stack page
# hides it on arm64. Neither Debug, ReleaseFast, nor the subset showed either.
# It is in `release-preflight` for that reason: locally green in every other
# mode is not evidence.
test-release-safe-full:
    @if [ "{{ os() }}" = windows ]; then zig build test-compile -Doptimize=ReleaseSafe --summary all; fi
    zig build {{ test_jobs }} test -Doptimize=ReleaseSafe --summary all

# Run teardown-heavy RPC plus schema-fidelity suites under ReleaseFast. This is a MEMORY-SAFETY lane,
# not a performance one: ReleaseFast is the only mode that leaves a freed
# pointer intact, so a use-after-free reached from a destructor shows up here
# and nowhere else.
test-release-fast:
    @if [ "{{ os() }}" = windows ]; then zig build test-release-fast-compile --summary all; fi
    zig build {{ test_jobs }} test-release-fast --summary all

# Run executable brand fidelity and vendored upstream schema closure tests
test-schema-fidelity:
    zig build {{ test_jobs }} test-schema-fidelity --summary all

# Run raw-frame RPC security e2e tests
test-e2e-security:
    zig build {{ test_jobs }} test-e2e-security --summary all

# Run RPC wire framing/protocol tests
test-rpc-wire:
    zig build {{ test_jobs }} test-rpc-wire --summary all

# Run RPC capability table tests
test-rpc-caps:
    zig build {{ test_jobs }} test-rpc-caps --summary all

# Run RPC promise/pipelining tests
test-rpc-promises:
    zig build {{ test_jobs }} test-rpc-promises --summary all

# Run RPC TCP/Unix/raw-frame transport tests
test-rpc-transport:
    zig build {{ test_jobs }} test-rpc-transport --summary all

# Run the AF_UNIX transport suites (regressions, fd drain, lingering close, listen/connect)
test-rpc-unix:
    zig build {{ test_jobs }} test-rpc-unix --summary all

# Run RPC peer semantics tests
test-rpc-peer:
    zig build {{ test_jobs }} test-rpc-peer --summary all

# Run focused Experimental L4 Join lease/lifecycle tests
test-rpc-l4:
    zig build {{ test_jobs }} test-rpc-l4 --summary all

# Run RPC integration tests
test-rpc-integration:
    zig build {{ test_jobs }} test-rpc-integration --summary all

# Run optional QUIC RPC transport tests
test-rpc-quic:
    zig build {{ test_jobs }} -Dquic=true test-rpc-quic --summary all

# Run the native QUIC suites through the build graph's executable evidence
# contract. The host scanner enforces four registered roots, per-root source
# floors, and a repository-wide ban on QUIC SkipZigTest paths.
test-rpc-quic-evidence optimize="Debug":
    @if [ "{{ os() }}" = windows ]; then zig build -Dquic=true -Doptimize={{ optimize }} test-rpc-quic-evidence-compile --summary all; fi
    zig build {{ test_jobs }} -Dquic=true -Doptimize={{ optimize }} test-rpc-quic-evidence --summary all

# Run benchmark regression checks
bench-check:
    zig build -Doptimize=ReleaseFast bench-check

# Run QUIC benchmark regression checks (capnp code ReleaseFast; quic-zig and
# BoringSSL build ReleaseSafe, the only release mode quic-zig offers)
bench-check-quic:
    zig build -Dquic=true -Doptimize=ReleaseFast bench-check-quic

# Run the RPC soak harness (chaos + deadline sessions over loopback TCP)
soak seconds="5" workers="4":
    zig build soak -- --seconds {{ seconds }} --workers {{ workers }}

# Build e2e reference images
e2e-build:
    just --justfile tests/e2e/Justfile build

# Run Zig interoperability e2e gate
e2e:
    just --justfile tests/e2e/Justfile test-zig

# Run e2e using the native Zig runner (no Deno dependency)
e2e-zig:
    just --justfile tests/e2e/Justfile test-zig

# Run the no-docker self-interop e2e (zig client vs zig server, all OSes)
e2e-self:
    zig build e2e-self --summary all

# The same self-interop e2e over a Unix-domain socket (Linux and macOS)
e2e-self-unix:
    zig build e2e-self-unix --summary all

# Run the Zig <-> C++ e2e over Unix-domain sockets (docker; both peers in the cpp-rpc container)
e2e-unix:
    just --justfile tests/e2e/Justfile test-unix

# Run e2e without rebuilding docker images
e2e-skip-build:
    just --justfile tests/e2e/Justfile test-skip-build

# Run the C++-first L3 handoff / L4 recon e2e lane
e2e-l3-cpp:
    just --justfile tests/e2e/Justfile test-l3-cpp

# Run the cross-impl L3 HOSTING lane: the C++ reference drives the recipient
# and introducer roles against a capnp-zig two-peer VatC host
e2e-l3-vatc:
    just --justfile tests/e2e/Justfile test-l3-vatc

# Run the Go L3 handoff recon/source-blocker gate
e2e-l3-go:
    just --justfile tests/e2e/Justfile test-l3-go

# Run the Experimental Zig L4 Join runtime expansion gate
e2e-l4-zig:
    just --justfile tests/e2e/Justfile test-l4-zig

# Run e2e harness without requiring Zig hooks (scaffolding mode)
e2e-scaffold:
    just --justfile tests/e2e/Justfile test-scaffold

# Run optional QUIC gates used by CI
ci-quic:
    just build-quic
    just check-quic
    just test-rpc-quic-evidence Debug
    just test-rpc-quic-evidence ReleaseSafe
    just test-docs-snippets-quic
    just test-quic-full
    zig build -Dquic=true check-api-quic

# The FULL suite against the QUIC library ROOT. `-Dquic=true` swaps the root to
# src/lib_quic.zig, and the targeted lanes above never compile the non-QUIC
# suites against it -- that gap hid a missing `canonical` export on the QUIC
# root through an entire release. Mirrors the CI job of the same shape.
test-quic-full:
    @if [ "{{ os() }}" = windows ]; then zig build -Dquic=true test-compile --summary all; fi
    zig build {{ test_jobs }} -Dquic=true test --summary all

# Compatibility alias for the older non-vacuity recipe name. The evidence gate
# now also asserts the four-root inventory and bans skipped tests.
check-quic-not-noop:
    just test-rpc-quic-evidence Debug

# CI gate (format, compile, docs, tests, QUIC, and interop e2e)
ci:
    just fmt-check
    just check
    just check-evented
    just check-selector
    just check-ios
    zig build check-fd-passing-off-symbols --summary all
    just test-fd-passing-off
    zig build hardening
    zig build check-api
    zig build api-closure
    zig build check-generated-shape
    zig build {{ test_jobs }} test-fuzz-smoke --summary all
    zig build {{ test_jobs }} test-resource-budgets --summary all
    zig build {{ test_jobs }} test-oom --summary all
    zig build {{ test_jobs }} test-e2e-security --summary all
    zig build {{ test_jobs }} test-docs-snippets --summary all
    zig build docs-smoke --summary all
    zig build {{ test_jobs }} test-release-safe --summary all
    just test-release-fast
    just ci-quic
    just src/rpc/check-rpc
    just check-generated
    just package-preflight
    just test
    just e2e-self
    @if [ "{{ os() }}" != windows ]; then just e2e-self-unix; fi
    just e2e-zig
    just e2e-unix
    just e2e-l3-vatc
    zig build example-rpc
    @if [ "{{ os() }}" != windows ]; then zig build example-rpc-unix; fi
    @if [ "{{ os() }}" != windows ]; then zig build example-rpc-fd; fi

# Regenerate committed bindings with the pinned WASM compiler and this checkout's
# native plugin. Compiler inputs and generator outputs use separate processes.
gen:
    uv run --no-project --python 3.13 "{{ capnp_tool }}" verify
    zig build
    cd src/rpc && just gen-rpc
    cd tests/e2e/schemas && uv run --no-project --python 3.13 "{{ capnp_tool }}" generate --plugin "{{justfile_directory()}}/zig-out/bin/capnpc-zig" --output "{{justfile_directory()}}/tests/e2e/zig/generated" -- game_types.capnp bootstrap.capnp game_world.capnp inventory.capnp chat.capnp matchmaking.capnp resolve_disembargo.capnp l3_l4_interop.capnp
    uv run --no-project --python 3.13 "{{ capnp_tool }}" generate --plugin "{{justfile_directory()}}/zig-out/bin/capnpc-zig" --output "{{justfile_directory()}}" -- examples/addressbook.capnp examples/pingpong.capnp
    cd examples/kvstore && uv run --no-project --python 3.13 "{{ capnp_tool }}" generate --plugin "{{justfile_directory()}}/zig-out/bin/capnpc-zig" --output "{{justfile_directory()}}/examples/kvstore/gen" -- kvstore.capnp
    mkdir -p zig-out/check-generated/tests/test_schemas
    # The package-preflight codegen consumer reads this request on stdin, so its
    # clean-room build needs no schema compiler (docs/build-integration.md).
    uv run --no-project --python 3.13 "{{ capnp_tool }}" compiler -- compile -o- --src-prefix=tests/package_consumer/codegen/schema tests/package_consumer/codegen/schema/addressbook.capnp > zig-out/check-generated/addressbook.request.bin
    cp zig-out/check-generated/addressbook.request.bin tests/package_consumer/codegen/schema/addressbook.request.bin
    uv run --no-project --python 3.13 "{{ capnp_tool }}" generate --plugin "{{justfile_directory()}}/zig-out/bin/capnpc-zig" --output "{{justfile_directory()}}/zig-out/check-generated" -- tests/test_schemas/example.capnp
    cp zig-out/check-generated/tests/test_schemas/example.zig src/wasm/generated/example.zig
    mkdir -p tests/serialization/generated
    # These revisions share a file ID, so capnp must compile them in separate
    # requests even though their generated modules are checked together.
    uv run --no-project --python 3.13 "{{ capnp_tool }}" generate --plugin "{{justfile_directory()}}/zig-out/bin/capnpc-zig" --output "{{justfile_directory()}}/zig-out/check-generated" -- tests/test_schemas/enum_evolution_v1.capnp
    cp zig-out/check-generated/tests/test_schemas/enum_evolution_v1.zig tests/serialization/generated/schema_evolution_v1.zig
    uv run --no-project --python 3.13 "{{ capnp_tool }}" generate --plugin "{{justfile_directory()}}/zig-out/bin/capnpc-zig" --output "{{justfile_directory()}}/zig-out/check-generated" -- tests/test_schemas/enum_evolution_v2.capnp
    cp zig-out/check-generated/tests/test_schemas/enum_evolution_v2.zig tests/serialization/generated/schema_evolution_v2.zig
    uv run --no-project --python 3.13 "{{ capnp_tool }}" generate --plugin "{{justfile_directory()}}/zig-out/bin/capnpc-zig" --output "{{justfile_directory()}}/zig-out/check-generated" -- tests/test_schemas/nested_lists_runtime.capnp
    cp zig-out/check-generated/tests/test_schemas/nested_lists_runtime.zig tests/serialization/generated/nested_lists_runtime.zig
    just gen-shape-requests
    CAPNPC_ZIG_UPDATE_GOLDENS=1 zig build {{ test_jobs }} test-codegen
    zig build api-snapshot

# Write the generated-shape corpus: one CodeGeneratorRequest per row of
# `requests` in build/generated_shape.zig (keep the two lists in step; the
# gate fails on a request it does not know). `zig build generated-shape`
# runs the plugin on them, so it needs no schema compiler. This recipe does
# not touch docs/generated-shape*.txt; only `zig build generated-shape` does.
gen-shape-requests:
    #!/usr/bin/env bash
    set -euo pipefail
    out=tests/generated_shape/requests
    mkdir -p "$out"
    rm -f "$out"/*.request.bin
    req() {
      local name="$1" prefix="$2"
      shift 2
      uv run --no-project --python 3.13 "{{ capnp_tool }}" compiler -- compile -o- "--src-prefix=$prefix" "$@" > "$out/$name.request.bin"
    }
    t=tests/test_schemas
    req addressbook examples examples/addressbook.capnp
    req kvstore examples/kvstore examples/kvstore/kvstore.capnp
    req pingpong examples examples/pingpong.capnp
    req persistent src/rpc/capnp src/rpc/capnp/persistent.capnp
    req rpc_inherited_paths "$t" "$t/rpc_inherited_paths.capnp" "$t/rpc_inherited_external.capnp"
    req generic_rpc "$t" "$t/generic_rpc.capnp" "$t/generic_rpc_external.capnp"
    req runtime_guard_names "$t" "$t/runtime_guard_names.capnp" "$t/runtime_abi.capnp"
    req brand_cross_file "$t" "$t/brand_cross_file.capnp" "$t/brand_imported.capnp"
    for schema in inherited_method_collision streaming rpc_pipeline_paths nested_interfaces \
      enum_evolution_v1 union_member_guard_runtime defaults nested_collisions \
      nested_interface_collisions zig_field_names edge_codegen brand_application_edge_cases \
      brand_list_specialization brand_pointer_fidelity generic_collections generic_recursive \
      rpc_nested annotations; do
      req "$schema" "$t" "$t/$schema.capnp"
    done

# Every path `gen` writes plugin output to. The output is committed exactly as
# the plugin wrote it, with no formatting pass.
generated_paths := "src/rpc/gen tests/e2e/zig/generated tests/golden examples/addressbook.zig examples/pingpong.zig examples/kvstore/gen/kvstore.zig src/wasm/generated/example.zig tests/serialization/generated"

# Fail if regeneration changes any committed binding or the Stable API surface,
# or if the plugin's raw output is not zig fmt clean.
check-generated: gen
    # A consumer that runs the pinned plugin gets these exact bytes, so the
    # generator itself must emit zig fmt's layout (src/capnpc-zig/layout.zig).
    zig fmt --check {{ generated_paths }} || { echo "ERROR: capnpc-zig output above is not zig fmt clean — fix the emitter in src/capnpc-zig/, not the generated file"; exit 1; }
    # Of the five surface snapshots, only the Stable docs/api-snapshot.txt is
    # in this diff (`gen` rewrites it, and the experimental one, through
    # `zig build api-snapshot`). The others are not stale-by-design; each has
    # its own strict CI gate:
    #   docs/api-snapshot-experimental.txt       `zig build check-api-experimental`
    #   docs/api-snapshot-experimental-quic.txt  `zig build -Dquic=true check-api-experimental-quic`
    #   docs/generated-shape*.txt                `zig build check-generated-shape`
    # The experimental API snapshots render the same on Linux and macOS
    # (stored thread ids are widened to u64), not on Windows. `gen` writes
    # the generated-shape corpus (tests/generated_shape/requests, diffed
    # below) but not the shape files: only `zig build generated-shape` does.
    git diff --exit-code -- {{ generated_paths }} tests/package_consumer/codegen/schema/addressbook.request.bin tests/generated_shape/requests docs/api-snapshot.txt || { echo "ERROR: committed generated artifacts are stale — run 'just check-generated' locally and commit the result"; exit 1; }

# Assert the Zig on PATH is the one mise.toml pins — the same check
# .github/actions/setup-zig makes, so a local gate proves the same thing CI's
# does. Deliberately compares PATH against `mise current zig` rather than
# holding a copy of the version: mise.toml stays the single specifier.
#
# This exists because the mismatch is easy to have and invisible without it:
# zvm installs its shim at ~/.zvm/bin/zig, which shadows mise's on PATH, so
# `just release-preflight` can gate a release on a toolchain CI never runs.
# `mise exec -- zig ...` (or putting `$(mise where zig)/bin` first) is the fix.
check-toolchain:
    #!/usr/bin/env bash
    set -euo pipefail
    want="$(mise current zig)"
    have="$(zig version)"
    if [ -z "$want" ]; then
      echo "ERROR: mise.toml pins no zig version ('mise current zig' returned empty)"; exit 1
    fi
    if [ "$want" != "$have" ]; then
      echo "ERROR: mise.toml pins ${want} but PATH resolves ${have}"
      echo "       run: PATH=\"\$(mise where zig)/bin:\$PATH\" just <recipe>"
      exit 1
    fi
    echo "zig ${have} matches the mise.toml pin"

# Hold a release to the semver table in RELEASING.md: diff the five surface
# snapshots (docs/api-snapshot.txt and docs/generated-shape.txt, both Stable
# and frozen, plus the three experimental files) against PREV_TAG. Fails when
# a Stable file removes or changes a line and the release's CHANGELOG section
# has no Stable `### Breaking` entry (one whose bold title is not tagged
# Experimental), and when any of the five changed under a patch bump. Warns
# when an experimental file loses lines with no Breaking (Experimental) entry,
# and when a Breaking entry has no Migration paragraph. VERSION defaults to
# build.zig.zon; while that still equals PREV_TAG's version, it checks
# `[Unreleased]` and prints the bump the drift needs. tools/release_drift.zig
# has the rules.
# Usage: just check-release-drift v0.19.1 [0.20.0]
check-release-drift PREV_TAG VERSION="":
    zig build release-drift -- --prev "{{ PREV_TAG }}" {{ if VERSION == "" { "" } else { "--version " + VERSION } }}

# Complete local release preflight, including heavier CI build/regression jobs.
# Pass the version you are about to cut, so the drift hook checks the bump
# before the version sweep: `just release-preflight 0.20.0`.
release-preflight VERSION="":
    just check-toolchain
    just check-release-drift "$(git describe --tags --abbrev=0 --match 'v[0-9]*')" {{ VERSION }}
    just ci
    just test-release-safe-full
    just wasm-build
    just bench-check
    just release

# Alias for the complete local release preflight
preflight VERSION="": (release-preflight VERSION)

# Create and push an annotated release tag — but refuse when the commit being
# tagged does not already have a green CI run. v0.4.0 was tagged three minutes
# before its own push run went red on four jobs; this recipe is the preventive
# half of that lesson (.github/workflows/release.yml is the detective half).
# It also refuses a VERSION that under-declares the surface drift since the
# last release tag (`check-release-drift`), before anything is tagged.
# Usage: just release-tag 0.5.0 "one-line theme"
release-tag VERSION THEME="":
    just check-toolchain
    test -z "$(git status --porcelain)" || { echo "ERROR: worktree is dirty — commit or stash first"; exit 1; }
    test "$(cat build.zig.zon | sed -n 's/.*\.version = "\(.*\)".*/\1/p')" = "{{VERSION}}" || { echo "ERROR: build.zig.zon version does not match {{VERSION}} — run the RELEASING.md sweep first"; exit 1; }
    just check-release-drift "$(git describe --tags --abbrev=0 --match 'v[0-9]*' --exclude 'v{{VERSION}}')" {{VERSION}}
    zig build docs-smoke
    gh api "repos/:owner/:repo/actions/runs?head_sha=$(git rev-parse HEAD)" --jq '[.workflow_runs[] | select(.name == "CI")] | if length == 0 then "NO_RUN" elif all(.conclusion == "success") then "GREEN" else "RED" end' | grep -qx GREEN || { echo "ERROR: HEAD has no green CI run — push and wait for CI before tagging (see RELEASING.md step 2)"; exit 1; }
    git tag -a "v{{VERSION}}" -m "$(test -n "{{THEME}}" && echo "v{{VERSION}} — {{THEME}}" || echo "v{{VERSION}}")"
    git push origin "v{{VERSION}}"
    @echo "Tagged v{{VERSION}}. Now do RELEASING.md step 6: real zig fetch, record the hash (just verify-release-hash {{VERSION}}), create the GitHub Release."

# Post-tag: fetch the published tag into a clean consumer and assert the hash
# recorded in docs/build-integration.md matches the hosted artifact. Prints
# the real hash either way, so on first run (placeholder still in the docs)
# the value to record is on screen. The stale-digest bug shipped two releases
# because this comparison was a manual step. Usage: just verify-release-hash 0.12.0
verify-release-hash VERSION:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    cd "$tmp" && zig init >/dev/null 2>&1
    zig fetch --save "git+https://github.com/nullstyle/capnp-zig.git#v{{VERSION}}" >/dev/null
    hash="$(sed -n 's/.*\.hash = "\(capnpc_zig-[^"]*\)".*/\1/p' build.zig.zon | head -1)"
    test -n "$hash" || { echo "ERROR: no capnpc_zig hash found after fetch"; exit 1; }
    echo "published v{{VERSION}} hash: $hash"
    grep -qF "$hash" "{{justfile_directory()}}/docs/build-integration.md" || { echo "ERROR: docs/build-integration.md does not carry this hash — record it (RELEASING.md step 6)"; exit 1; }
    echo "docs/build-integration.md matches the published artifact"

# List CI workflow jobs as seen by `act`
act-list:
    act -l

# Run local CI-equivalent jobs with `act` (single runner profile, sequential)

# Excludes benchmark regression job by default since host/container timing is not comparable to CI baseline.
act-ci event="pull_request":
    act {{ event }} --matrix os:ubuntu-latest -j fmt-check
    act {{ event }} --matrix os:ubuntu-latest -j test
    act {{ event }} --matrix os:ubuntu-latest -j evented-check
    act {{ event }} --matrix os:ubuntu-latest -j quic-transport
    act {{ event }} --matrix os:ubuntu-latest -j docs-smoke
    act {{ event }} --matrix os:ubuntu-latest -j hardening
    act {{ event }} --matrix os:ubuntu-latest -j e2e-zig
    act {{ event }} --matrix os:ubuntu-latest -j wasm-build
    act {{ event }} --matrix os:ubuntu-latest -j release-build
    act {{ event }} --matrix os:ubuntu-latest -j release-safe-tests

# Run a single CI job locally with `act` (example: `just act-ci-job test`)
act-ci-job job event="pull_request" matrix="os:ubuntu-latest":
    act {{ event }} --matrix {{ matrix }} -j {{ job }}

# Run benchmark regression check locally under `act` (optional; often noisy on laptops/containers)
act-bench event="pull_request":
    act {{ event }} --matrix os:ubuntu-latest -j bench-check

# Install to a local bin path (defaults to ~/.local/bin)
install dest="${HOME}/.local/bin": release
    mkdir -p "{{ dest }}"
    cp zig-out/bin/capnpc-zig "{{ dest }}/capnpc-zig"

# Install to the first writable directory in PATH
install-path: release
    @set -eu
    @for dir in $(printf '%s' "$PATH" | tr ':' ' '); do \
        if [ -n "$dir" ] && [ -d "$dir" ] && [ -w "$dir" ]; then \
            cp zig-out/bin/capnpc-zig "$dir/capnpc-zig"; \
            echo "Installed capnpc-zig to $dir/capnpc-zig"; \
            exit 0; \
        fi; \
    done; \
    echo "No writable directory found in PATH. Use 'just install <dest>' instead."; \
    exit 1

# Clean build artifacts
clean:
    rm -rf zig-out .zig-cache

# Format code. Generated bindings are included: the plugin emits zig fmt's
# layout, so formatting them is a no-op (`check-generated` enforces that).
# Only kvstore's third-party package trees are excluded.
fmt:
    zig fmt --exclude examples/kvstore/zig-pkg --exclude examples/kvstore/vendor src/ tests/ bench/ tools/ examples/

# Check formatting with the same paths CI uses
fmt-check:
    zig fmt --check --exclude examples/kvstore/zig-pkg --exclude examples/kvstore/vendor src/ tests/ bench/ tools/ examples/

# Check for errors without building
check:
    zig build check

# Expected-fail canary: green only while std.Io.Evented fails to compile at the
# pinned Zig with the known std defect (src/io_backend.zig keeps
# evented_available = false). Red means re-check that flag. Linux and Darwin
# check their native backend; Windows cross-checks Linux (see
# evented_canary_target), so `just ci` runs it on every host.
# `-Dio-backend=evented check` compiles nothing evented, so it is not a gate.
check-evented:
    zig build check-evented-canary {{ evented_canary_target }} --summary all

# capnpc-zig-core as static libraries for iOS, both iOS simulators, and macOS
# with fd passing off. Static libraries never link, so no Apple SDK is needed.
check-ios:
    zig build check-ios --summary all

# The full suite with fd passing, the fd closer and the AF_UNIX transport
# compiled out (`-Dfd-passing=false`). It changes the build on Linux and macOS.
test-fd-passing-off:
    zig build -Dfd-passing=false {{ test_jobs }} test --summary all

# Execute the RPC e2e over an explicitly selected Io backend. This is the lane
# with teeth: `-Dio-backend` is a []const u8 compared at RUNTIME by
# io_backend.parseKind, so `check` alone analyses all three arms in every
# configuration and a compile check proves nothing about selection. `.threaded`
# is the only selector that can carry RPC today -- std.Io.Evented has no working
# socket vtable upstream at the pinned toolchain (docs/stability.md).
check-selector:
    zig build -Dio-backend=threaded e2e-self --summary all

# Check optional QUIC-enabled build graph
check-quic:
    zig build -Dquic=true check --summary all

# Generate API documentation
docs:
    zig build docs
