# Getting Started: Serialization with capnpc-zig

This guide walks you through defining a Cap'n Proto schema and using the generated Zig code to serialize and deserialize messages.

## Prerequisites

- **Zig 0.15.2** (use `mise install` if you have mise)
- **Cap'n Proto compiler** (`capnp`) — install via your package manager (e.g. `brew install capnp`, `apt install capnproto`)
- **capnpc-zig** — built from this repo (`zig build`)

## 1. Define Your Schema

Create a file called `addressbook.capnp`:

```capnp
@0x9eb32e19f86ee174;

struct Person {
  name @0 :Text;
  age  @1 :UInt32;
  email @2 :Text;
  phones @3 :List(PhoneNumber);

  struct PhoneNumber {
    number @0 :Text;
    type @1 :PhoneType;
  }

  enum PhoneType {
    mobile @0;
    home @1;
    work @2;
  }
}

struct AddressBook {
  people @0 :List(Person);
}
```

Key points:
- The `@0x...` line is a unique file ID — generate one with `capnp id`
- Fields have ordinals (`@0`, `@1`, ...) that define their position in the binary layout
- Structs, enums, and lists compose naturally

## 2. Generate Zig Code

Run the Cap'n Proto compiler with capnpc-zig as the output plugin:

```bash
capnp compile -o ./zig-out/bin/capnpc-zig addressbook.capnp
```

This produces `addressbook.capnp.zig` (or similar, depending on the schema filename). The generated file contains `Reader` and `Builder` types for each struct, plus Zig enums for each Cap'n Proto enum.
Codegen is quiet by default to keep build output clean.

For a canonical `build.zig` automation pattern (codegen step + generated module wiring), see [build-integration.md](build-integration.md).

## 3. Add capnpc-zig as a Dependency

In your project's `build.zig.zon`, add capnpc-zig:

```zig
.dependencies = .{
    .capnpc_zig = .{
        .path = "../capnpc-zig",  // or use .url + .hash for remote
    },
},
```

In your `build.zig`, import the module:

```zig
const capnpc_dep = b.dependency("capnpc_zig", .{
    .target = target,
    .optimize = optimize,
});

// Use the core module (no RPC/xev dependency)
exe.root_module.addImport("capnpc-zig", capnpc_dep.module("capnpc-zig-core"));
```

## 4. Build a Message

Every generated struct has a `Builder` type for writing and a `Reader` type for reading. Here's how to build a `Person` message:

```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");
const message = capnpc.message;
const addressbook = @import("addressbook");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // 1. Create a MessageBuilder
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    // 2. Initialize the root struct
    var person = try addressbook.Person.Builder.init(&builder);

    // 3. Set fields
    try person.setName("Alice Smith");
    try person.setAge(30);
    try person.setEmail("alice@example.com");

    // 4. Serialize to bytes
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    // `bytes` now contains the framed Cap'n Proto message
    std.debug.print("Serialized {d} bytes\n", .{bytes.len});
}
```

### How field setters work

- **Primitives** (`setAge`) write directly into the struct's data section — no allocation
- **Text/Data** (`setName`, `setEmail`) allocate space in the message segment and write a pointer
- All setters return `!void` — text/data setters can fail on allocation; primitive setters are infallible but return `!void` for API consistency

## 5. Deserialize and Read

Reading is zero-copy — the `Reader` accesses bytes directly from the message buffer:

```zig
// 1. Parse the framed message
var msg = try message.Message.init(allocator, bytes);
defer msg.deinit();

// 2. Get a typed Reader for the root struct
const person = try addressbook.Person.Reader.init(&msg);

// 3. Read fields
const name = try person.getName();   // []const u8, points into msg bytes
const age = try person.getAge();     // u32
const email = try person.getEmail(); // []const u8

std.debug.print("{s}, age {d}, {s}\n", .{ name, age, email });
```

### Important: Reader lifetimes

The slices returned by `getName()` and `getEmail()` point directly into the `Message`'s backing memory. Keep the `Message` alive as long as you need the data.

## 6. Enums

Cap'n Proto enums generate standard Zig enums backed by `u16`:

**Schema:**
```capnp
enum PhoneType {
  mobile @0;
  home @1;
  work @2;
}
```

**Generated:**
```zig
pub const PhoneType = enum(u16) {
    Mobile = 0,
    Home = 1,
    Work = 2,
};
```

**Usage:**
```zig
// Writing
try phone.setType(.Mobile);

// Reading
const phone_type = try phone.getType();  // returns PhoneType enum
switch (phone_type) {
    .Mobile => std.debug.print("mobile\n", .{}),
    .Home => std.debug.print("home\n", .{}),
    .Work => std.debug.print("work\n", .{}),
}
```

## 7. Lists

### Primitive lists

```zig
// Writing — init the list with a count, then set each element
var scores = try builder.initScores(3);  // List(UInt32), 3 elements
try scores.set(0, 100);
try scores.set(1, 95);
try scores.set(2, 87);

// Reading
const scores = try reader.getScores();
const len = scores.len();
for (0..len) |i| {
    const score = try scores.get(@intCast(i));
    std.debug.print("score: {d}\n", .{score});
}
```

### Struct lists

```zig
// Writing — init returns a typed StructListBuilder
var phones = try person.initPhones(2);
var phone0 = try phones.get(0);
try phone0.setNumber("555-1234");
try phone0.setType(.Mobile);
var phone1 = try phones.get(1);
try phone1.setNumber("555-5678");
try phone1.setType(.Work);

// Reading
const phones = try person.getPhones();
for (0..phones.len()) |i| {
    const phone = try phones.get(@intCast(i));
    const number = try phone.getNumber();
    const phone_type = try phone.getType();
    std.debug.print("{s} ({any})\n", .{ number, phone_type });
}
```

### Text lists

```zig
// Writing
var tags = try builder.initTags(2);
try tags.set(0, "zig");
try tags.set(1, "capnproto");

// Reading
const tags = try reader.getTags();
const tag = try tags.get(0);  // []const u8
```

## 8. Nested Structs

```zig
// Writing — initAddress allocates a nested struct in the message
var address = try person.initAddress();
try address.setStreet("123 Main St");
try address.setCity("Springfield");
try address.setZipCode(62704);

// Reading
const address = try person.getAddress();
const street = try address.getStreet();
```

## 9. Unions

Cap'n Proto unions use a discriminant field to track which variant is active.

**Schema:**
```capnp
struct Shape {
  color @0 :Color;

  union {
    circle @1 :Float64;       # radius
    rectangle :group {
      width @2 :Float32;
      height @3 :Float32;
    }
  }
}
```

**Generated types:**
```zig
pub const Shape = struct {
    pub const WhichTag = enum(u16) {
        circle = 0,
        rectangle = 1,
    };

    pub const Rectangle = struct {
        pub const Reader = struct { ... };
        pub const Builder = struct { ... };
    };

    pub const Reader = struct {
        pub fn which(self: Reader) !WhichTag { ... }
        pub fn getColor(self: Reader) !Color { ... }
        pub fn getCircle(self: Reader) !f64 { ... }
        pub fn getRectangle(self: Reader) Rectangle.Reader { ... }
    };

    pub const Builder = struct {
        pub fn setColor(self: *Builder, value: Color) !void { ... }
        pub fn setCircle(self: *Builder, value: f64) !void { ... }    // sets discriminant
        pub fn initRectangle(self: *Builder) Rectangle.Builder { ... } // sets discriminant
    };
};
```

**Usage:**
```zig
// Writing a circle
var shape = try Shape.Builder.init(&builder);
try shape.setColor(.Red);
try shape.setCircle(5.0);  // automatically sets discriminant to .circle

// Writing a rectangle (group variant)
var shape = try Shape.Builder.init(&builder);
try shape.setColor(.Blue);
var rect = shape.initRectangle();  // sets discriminant to .rectangle
try rect.setWidth(10.0);
try rect.setHeight(20.0);

// Reading — always check which() first
const shape = try Shape.Reader.init(&msg);
switch (try shape.which()) {
    .circle => {
        const radius = try shape.getCircle();
        std.debug.print("circle r={d}\n", .{radius});
    },
    .rectangle => {
        const rect = shape.getRectangle();
        const w = try rect.getWidth();
        const h = try rect.getHeight();
        std.debug.print("rect {d}x{d}\n", .{ w, h });
    },
}
```

### Union Default-Arm Semantics

Generated unions follow the Cap'n Proto discriminant value directly:

- A newly initialized struct starts with discriminant `0`, so the first union arm is active by default.
- A message that never explicitly set a union arm may still report that first arm at read time.
- Always branch on `which()` before calling arm-specific getters.
- Always call `setXxx()` or `initXxx()` before writing fields for a non-default arm.

This avoids subtle bugs where application code assumes a union arm was explicitly set when it was only the implicit zero/default discriminant.

## 10. Packed Encoding

Cap'n Proto supports a packed encoding that compresses zero bytes, which is common in sparse messages:

```zig
// Serialize to packed format
const packed_bytes = try builder.toPackedBytes();
defer allocator.free(packed_bytes);

// Deserialize from packed format
var msg = try message.Message.initPacked(allocator, packed_bytes);
defer msg.deinit();
```

Packed encoding is useful when sending messages over the network or storing them on disk. Typical compression ratios are 2-4x for sparse messages.

## 11. Schema Evolution

Cap'n Proto is designed for safe schema evolution. You can:

- **Add new fields** to the end of a struct (with new ordinals)
- **Read old messages** with new code — new fields return their default value (0, false, "")
- **Read new messages** with old code — unknown fields are silently ignored

This works because readers return type defaults for any field that falls outside the struct's data section. No versioning metadata is needed.

## Quick Reference

| Schema Type | Zig Read Type | Zig Write Method |
|---|---|---|
| `Bool` | `bool` | `setBoolField(bool)` |
| `UInt8..UInt64` | `u8..u64` | `setField(u32)` |
| `Int8..Int64` | `i8..i64` | `setField(i32)` |
| `Float32/Float64` | `f32/f64` | `setField(f32)` |
| `Text` | `[]const u8` | `setField([]const u8)` |
| `Data` | `[]const u8` | `setField([]const u8)` |
| `List(T)` | typed list reader | `initField(count)` |
| `struct` | `StructName.Reader` | `initField()` |
| `enum` | `EnumName` | `setField(.Variant)` |
| `union` | check `which()` | `setVariant()`/`initVariant()` |
