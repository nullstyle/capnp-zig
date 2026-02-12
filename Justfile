# Build the plugin
build:
    zig build

# Build in release mode
release:
    zig build -Doptimize=ReleaseSafe

# Run tests
test:
    zig build test --summary all

# Run serialization-focused tests (message/codegen/schema/interop)
test-serialization:
    zig build test-serialization --summary all

# Run all RPC tests
test-rpc:
    zig build test-rpc --summary all

# Run Cap'n Proto RPC level 0 tests (framing/protocol/cap-table)
test-rpc-level0:
    zig build test-rpc-level0 --summary all

# Run Cap'n Proto RPC level 1 tests (promises/pipelining)
test-rpc-level1:
    zig build test-rpc-level1 --summary all

# Run Cap'n Proto RPC level 2 tests (runtime plumbing)
test-rpc-level2:
    zig build test-rpc-level2 --summary all

# Run Cap'n Proto RPC level 3+ tests (advanced peer semantics)
test-rpc-level3:
    zig build test-rpc-level3 --summary all

# Build e2e reference images
e2e-build:
    just --justfile tests/e2e/Justfile build

# Run Zig interoperability e2e gate
e2e:
    just --justfile tests/e2e/Justfile test

# Run e2e without rebuilding docker images
e2e-skip-build:
    just --justfile tests/e2e/Justfile test-skip-build

# Run e2e harness without requiring Zig hooks (scaffolding mode)
e2e-scaffold:
    just --justfile tests/e2e/Justfile test-scaffold

# CI gate (unit + interop e2e)
ci:
    just src/rpc/check-rpc
    zig build test --summary all
    just e2e

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
    zig fmt --exclude tests/golden src/ tests/ bench/ tools/ examples/

# Run example schema compilation
example: build
    capnp compile -o ./zig-out/bin/capnpc-zig tests/test_schemas/example.capnp

# Run KVStore example server
example-kvstore-server port="9000" db_path="kvstore-data":
    cd examples/kvstore && zig build server -- --port {{ port }} --db-path {{ db_path }}

# Run KVStore example client
example-kvstore-client port="9000":
    cd examples/kvstore && zig build client -- --port {{ port }}

# Check for errors without building
check:
    zig build check

# Generate API documentation
docs:
    zig build docs
