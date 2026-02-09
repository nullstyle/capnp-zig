# capnpc-zig

**WARNING: This code was extensively vibed;  It's only for me for now, use at your own risk**

A pure Zig implementation of a Cap'n Proto compiler plugin for Zig 0.15.2. Generates idiomatic Zig code from Cap'n Proto schema files with full serialization and deserialization support.

## Features

- **Pure Zig Implementation**: No C++ dependencies, written entirely in Zig 0.15.2
- **Full Serialization Support**: Complete Cap'n Proto wire format implementation
- **Zero-Copy Deserialization**: Readers work directly with message bytes
- **Builder Pattern**: Ergonomic API for constructing messages
- **Comprehensive Tests**: Extensive message/codegen/RPC/interop coverage
- **Type Safe**: Leverages Zig's compile-time type system

## Installation

### Prerequisites

- Zig 0.15.2
- Cap'n Proto compiler (`capnp`) - optional, for schema compilation
- `mise` (recommended, for environment management)
- `just` (recommended, for task automation)

### Building from Source

```bash
# Using just (recommended)
just build

# Or using zig directly
zig build

# Run tests
just test
# or
zig build test
```

## Usage

### As a Library

Add `capnpc-zig` to your project and use the message serialization API directly:

```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");
const message = capnpc.message;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create a message builder
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    // Allocate a struct with 1 data word and 2 pointer words
    var struct_builder = try builder.allocateStruct(1, 2);
    
    // Write primitive fields
    struct_builder.writeU32(0, 42);
    struct_builder.writeU32(4, 100);
    
    // Write text fields
    try struct_builder.writeText(0, "Hello");
    try struct_builder.writeText(1, "World");

    // Serialize to bytes
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    // Deserialize
    var msg = try message.Message.init(allocator, bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    
    // Read fields
    const value1 = root.readU32(0); // 42
    const value2 = root.readU32(4); // 100
    const text1 = try root.readText(0); // "Hello"
    const text2 = try root.readText(1); // "World"
}
```

### Generated Code Example

For a Cap'n Proto schema like:

```capnp
@0x9eb32e19f86ee174;

struct Person {
  name @0 :Text;
  age @1 :UInt32;
  email @2 :Text;
}
```

The generated Zig code provides:

```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create a Person
    var msg_builder = capnpc.message.MessageBuilder.init(allocator);
    defer msg_builder.deinit();
    
    var person_builder = try Person.Builder.init(&msg_builder);
    try person_builder.setName("Alice");
    person_builder.setAge(30);
    try person_builder.setEmail("alice@example.com");
    
    // Serialize
    const bytes = try msg_builder.toBytes();
    defer allocator.free(bytes);
    
    // Deserialize
    var msg = try capnpc.message.Message.init(allocator, bytes);
    defer msg.deinit();
    
    const person_reader = try Person.Reader.init(&msg);
    
    // Access fields
    const name = try person_reader.getName();
    const age = person_reader.getAge();
    const email = try person_reader.getEmail();
}
```

## Project Structure

```
capnpc-zig/
├── src/
│   ├── main.zig              # Plugin entry point
│   ├── lib.zig               # Library exports
│   ├── message.zig           # Core serialization/deserialization
│   ├── schema.zig            # Schema structures
│   ├── reader.zig            # Cap'n Proto reader utilities
│   └── capnpc-zig/
│       ├── generator.zig     # Code generator
│       ├── types.zig         # Type utilities
│       └── struct_gen.zig    # Struct generation
├── tests/
│   ├── message_test.zig      # Message serialization/validation tests
│   ├── codegen_test.zig      # Code generation tests
│   └── integration_test.zig  # Integration tests
├── build.zig                 # Zig build configuration
├── Justfile                  # Task automation
└── .mise.toml                # Environment configuration
```

## API Reference

### Message Module

#### `MessageBuilder`

Creates Cap'n Proto messages.

- `init(allocator: Allocator) MessageBuilder` - Create a new message builder
- `deinit()` - Free all resources
- `allocateStruct(data_words: u16, pointer_words: u16) !StructBuilder` - Allocate a struct
- `toBytes() ![]const u8` - Serialize to Cap'n Proto wire format

#### `Message`

Reads Cap'n Proto messages.

- `init(allocator: Allocator, data: []const u8) !Message` - Parse a message
- `deinit()` - Free resources
- `getRootStruct() !StructReader` - Get the root struct

#### `StructBuilder`

Builds struct data.

- `writeU8/U16/U32/U64(offset: usize, value: T)` - Write integer fields
- `writeBool(byte_offset: usize, bit_offset: u3, value: bool)` - Write boolean fields
- `writeText(pointer_index: usize, text: []const u8) !void` - Write text fields

#### `StructReader`

Reads struct data.

- `readU8/U16/U32/U64(offset: usize) T` - Read integer fields
- `readBool(byte_offset: usize, bit_offset: u3) bool` - Read boolean fields
- `readText(pointer_index: usize) ![]const u8` - Read text fields

## Testing

The project includes comprehensive tests:

```bash
# Run all tests
just test

# Run specific test suites
zig build test-message      # Message tests
zig build test-codegen      # Codegen tests
zig build test-rpc          # RPC tests
just e2e                    # Cross-language interop harness
```

### Test Coverage

- Message wire-format encode/decode, pointer resolution, limits, and malformed/fuzz inputs
- Codegen generation/compile/runtime behavior across schema features, including schema-evolution compatibility checks
- RPC protocol, framing, cap-table encoding, peer runtime semantics, and transport failure-path behavior
- Interop validation against reference stacks via the e2e harness

## Development

### Available Commands

```bash
# Build
just build

# Run tests
just test

# Format code
just fmt

# Clean build artifacts
just clean

# Check for compilation errors
just check

# Generate API docs into zig-out/docs
just docs
```

### Adding New Features

1. Write tests first in `tests/`
2. Implement feature in `src/`
3. Run `just test` to verify
4. Format with `just fmt`

## Implementation Status

Implemented today:
- Full Cap'n Proto message wire format (including packed/unpacked and far pointers)
- Schema-driven code generation via `capnpc-zig`
- RPC protocol/runtime surface with dedicated RPC test suites
- Schema-evolution runtime coverage and expanded transport failure-path tests
- Local benchmark and interop gates (`zig build bench-check`, `just e2e`)

Roadmap and parity tracking live in:
- `ROADMAP.md`
- `docs/production_parity_checklist.md`

## Architecture

The implementation follows a layered architecture:

1. **Wire Format Layer** (`message.zig`): Handles Cap'n Proto binary format
2. **Schema Layer** (`schema.zig`): Represents Cap'n Proto schema structures
3. **Code Generation Layer** (`capnpc-zig/`): Transforms schemas to Zig code
4. **Plugin Layer** (`main.zig`): Integrates with Cap'n Proto compiler

## Performance

The implementation prioritizes:

- **Zero-copy reads**: Readers work directly on message bytes
- **Minimal allocations**: Only allocate for owned data (text, lists)
- **Compile-time safety**: Leverage Zig's type system
- **Inline-friendly**: Small functions suitable for inlining

## Contributing

Contributions are welcome! Please ensure:

- Code is formatted with `zig fmt`
- All tests pass (`zig build test`)
- New features include tests
- Documentation is updated

## License

MIT License

## Acknowledgments

- Cap'n Proto project for the excellent serialization format
- Zig community for the amazing language and tooling
- Existing Cap'n Proto implementations for reference

## Support

For issues, questions, or contributions, please open an issue or pull request.
