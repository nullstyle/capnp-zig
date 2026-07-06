# Windows: recipes assume a POSIX shell. Git Bash (installed with Git for
# Windows) provides `sh`; pinning it here makes `just` use it explicitly
# instead of silently falling back to cmd/powershell semantics.
set windows-shell := ["sh", "-cu"]

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
    zig build test --summary all

# Run the RPC ping-pong example
example:
    zig build example-rpc

# Run static hardening gates
hardening:
    zig build hardening

# Run serialization-focused tests (message/codegen/schema/interop)
test-serialization:
    zig build test-serialization --summary all

# Run all RPC tests
test-rpc:
    zig build test-rpc --summary all

# Run resource-budget regression tests
test-resource-budgets:
    zig build test-resource-budgets --summary all

# Run OOM/failing-allocator regression tests
test-oom:
    zig build test-oom --summary all

# Run deterministic hardening fuzz/smoke coverage
test-fuzz-smoke:
    zig build test-fuzz-smoke --summary all

# Run documentation/example smoke coverage
docs-smoke:
    zig build docs-smoke --summary all

# Compile documentation snippet fixtures
test-docs-snippets:
    zig build test-docs-snippets --summary all

# Compile optional QUIC documentation snippet fixtures
test-docs-snippets-quic:
    zig build -Dquic=true test-docs-snippets-quic --summary all

# Run key hardening gates under ReleaseSafe
test-release-safe:
    zig build test-release-safe --summary all

# Run raw-frame RPC security e2e tests
test-e2e-security:
    zig build test-e2e-security --summary all

# Run RPC wire framing/protocol tests
test-rpc-wire:
    zig build test-rpc-wire --summary all

# Run RPC capability table tests
test-rpc-caps:
    zig build test-rpc-caps --summary all

# Run RPC promise/pipelining tests
test-rpc-promises:
    zig build test-rpc-promises --summary all

# Run RPC TCP/raw-frame transport tests
test-rpc-transport:
    zig build test-rpc-transport --summary all

# Run RPC peer semantics tests
test-rpc-peer:
    zig build test-rpc-peer --summary all

# Run RPC integration tests
test-rpc-integration:
    zig build test-rpc-integration --summary all

# Run optional QUIC RPC transport tests
test-rpc-quic:
    zig build -Dquic=true test-rpc-quic --summary all

# Run benchmark regression checks
bench-check:
    zig build -Doptimize=ReleaseFast bench-check

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

# Run e2e without rebuilding docker images
e2e-skip-build:
    just --justfile tests/e2e/Justfile test-skip-build

# Run the C++-first L3 handoff / L4 recon e2e lane
e2e-l3-cpp:
    just --justfile tests/e2e/Justfile test-l3-cpp

# Run e2e harness without requiring Zig hooks (scaffolding mode)
e2e-scaffold:
    just --justfile tests/e2e/Justfile test-scaffold

# Run optional QUIC gates used by CI
ci-quic:
    just build-quic
    just check-quic
    just test-rpc-quic
    just test-docs-snippets-quic

# CI gate (format, compile, docs, tests, QUIC, and interop e2e)
ci:
    just fmt-check
    just check
    just check-evented
    zig build hardening
    zig build check-api
    zig build test-fuzz-smoke --summary all
    zig build test-resource-budgets --summary all
    zig build test-oom --summary all
    zig build test-e2e-security --summary all
    zig build test-docs-snippets --summary all
    zig build docs-smoke --summary all
    zig build test-release-safe --summary all
    just ci-quic
    just src/rpc/check-rpc
    just check-generated
    zig build test --summary all
    just e2e-self
    just e2e-zig
    zig build example-rpc

# Regenerate every committed generated artifact and fail if any drifted from
# the current generator/public API. Guards against the recurring "changed the
# generator/API but forgot to regenerate the checked-in files" class that has
# turned CI red more than once. Needs the `capnp` CLI.
check-generated:
    zig build
    cd src/rpc && just gen-rpc
    cd tests/e2e/schemas && capnp compile -o{{justfile_directory()}}/zig-out/bin/capnpc-zig:{{justfile_directory()}}/tests/e2e/zig/generated game_types.capnp bootstrap.capnp game_world.capnp inventory.capnp chat.capnp matchmaking.capnp resolve_disembargo.capnp l3_l4_interop.capnp
    zig build api-snapshot
    just fmt
    git diff --exit-code -- src/rpc/gen tests/e2e/zig/generated docs/api-snapshot.txt docs/api-snapshot-experimental.txt || { echo "ERROR: committed generated artifacts are stale — run 'just check-generated' locally and commit the result"; exit 1; }

# Complete local release preflight, including heavier CI build/regression jobs
release-preflight:
    just ci
    just wasm-build
    just bench-check
    just release

# Alias for the complete local release preflight
preflight: release-preflight

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

# Format code
fmt:
    zig fmt --exclude examples/kvstore/gen --exclude examples/kvstore/zig-pkg --exclude examples/kvstore/vendor --exclude tests/e2e/zig/generated --exclude tests/golden --exclude src/rpc/gen src/ tests/ bench/ tools/ examples/

# Check formatting with the same paths CI uses
fmt-check:
    zig fmt --check --exclude examples/kvstore/gen --exclude examples/kvstore/zig-pkg --exclude examples/kvstore/vendor --exclude tests/e2e/zig/generated --exclude tests/golden --exclude src/rpc/gen src/ tests/ bench/ tools/ examples/

# Check for errors without building
check:
    zig build check

# Check RPC entry points against the explicit Evented Io backend where supported
check-evented:
    zig build -Dio-backend=evented check --summary all

# Check optional QUIC-enabled build graph
check-quic:
    zig build -Dquic=true check --summary all

# Generate API documentation
docs:
    zig build docs
