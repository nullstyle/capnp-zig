# Build the plugin
build:
    zig build

# Build in release mode
release:
    zig build -Doptimize=ReleaseSafe

# Run tests
test:
    zig build test --summary all

# Install to local bin
install: release
    mkdir -p ~/.local/bin
    cp zig-out/bin/capnpc-zig ~/.local/bin/

# Clean build artifacts
clean:
    rm -rf zig-out .zig-cache

# Format code
fmt:
    zig fmt src/ tests/

# Run example schema compilation
example: build
    capnp compile -o ./zig-out/bin/capnpc-zig tests/test_schemas/example.capnp

# Check for errors without building
check:
    zig build check

