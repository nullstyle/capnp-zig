# Build the plugin
build:
    zig build

# Build in release mode
release:
    zig build -Doptimize=ReleaseSafe

# Run tests
test:
    zig build test --summary all

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
    zig build test --summary all
    just e2e

# Install to local bin
install: release
    mkdir -p ~/.local/bin
    cp zig-out/bin/capnpc-zig ~/.local/bin/

# Clean build artifacts
clean:
    rm -rf zig-out .zig-cache

# Format code
fmt:
    zig fmt --exclude tests/golden src/ tests/ bench/ tools/

# Run example schema compilation
example: build
    capnp compile -o ./zig-out/bin/capnpc-zig tests/test_schemas/example.capnp

# Check for errors without building
check:
    zig build check

# Generate API documentation
docs:
    zig build docs
