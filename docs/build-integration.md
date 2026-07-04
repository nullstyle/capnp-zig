# Build Integration: `capnp compile` + Generated Zig Modules

This is a canonical `build.zig` wiring pattern for:

1. Running Cap'n Proto codegen during the build.
2. Importing the generated `.zig` module into your app target.

## Prerequisites

- `capnp` installed.
- `capnpc-zig` installed on `PATH` (for example: `just install-path` in this repo).

## Add `capnpc-zig` as a dependency

Fetch a tagged release into your `build.zig.zon` (`zig fetch --save` computes
and records the `.hash` for you):

```sh
zig fetch --save git+https://github.com/nullstyle/capnp-zig.git#v0.2.0
```

That adds an entry like this to your `build.zig.zon` (the hash is filled in by
the command above — do not hand-write it):

```zig
.dependencies = .{
    .capnpc_zig = .{
        .url = "git+https://github.com/nullstyle/capnp-zig.git#v0.2.0",
        .hash = "capnpc_zig-0.2.0-...",
    },
},
```

The package exposes two library modules — pick one:

- `capnpc-zig` — the full surface (serialization + codegen + RPC runtime).
- `capnpc-zig-core` — serialization + codegen only, with no RPC/transport in
  the module graph. Use this when you only read/write messages.

## Example Layout

```text
your-project/
  build.zig
  schema/addressbook.capnp
  src/main.zig
```

## Canonical `build.zig` Example

```zig
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const capnpc_dep = b.dependency("capnpc_zig", .{
        .target = target,
        .optimize = optimize,
    });
    const capnpc_core = capnpc_dep.module("capnpc-zig-core");

    const exe = b.addExecutable(.{
        .name = "app",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = capnpc_core },
            },
        }),
    });

    const codegen = b.addSystemCommand(&.{
        "capnpc",
        "-ozig:gen",
        "schema/addressbook.capnp",
    });

    const addressbook = b.createModule(.{
        .root_source_file = b.path("schema/addressbook.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "capnpc-zig", .module = capnpc_core },
        },
    });

    exe.root_module.addImport("addressbook", addressbook);

    // Ensure generated code exists before Zig compile/import.
    exe.step.dependOn(&codegen.step);

    b.installArtifact(exe);
}
```

## Notes

- The generated file path follows the schema path (`schema/addressbook.capnp` -> `schema/addressbook.zig`).
- `capnpc-zig` is quiet by default; generated logs are only emitted when verbose mode is enabled (`capnpc-zig --verbose` when invoking the plugin directly).
- If your plugin is not on `PATH`, replace `"capnpc-zig"` in the `-o` argument with an absolute executable path.
