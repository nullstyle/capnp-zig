# Build Integration: `capnp compile` + Generated Zig Modules

This is a canonical `build.zig` wiring pattern for:

1. Running Cap'n Proto codegen during the build.
2. Importing the generated `.zig` module into your app target.

## Prerequisites

- `capnp` installed.
- `capnpc-zig` installed on `PATH` (for example: `just install-path` in this repo).

On Windows, the upstream prebuilt compiler archive contains `capnp.exe` but not
the standard schema tree. If your schema imports `/capnp/*.capnp`, install or
check out that tree too and pass it to `capnp` with `-I<path-to-capnproto>/c++/src`.
The capnpc-zig repository's own tests use the vendored upstream tree at
`vendor/ext/capnproto/c++/src`; that development dependency is intentionally
not part of the filtered consumer package.

## Add `capnpc-zig` as a dependency

Fetch a tagged release into your `build.zig.zon` (`zig fetch --save` computes
and records the `.hash` for you):

```sh
zig fetch --save git+https://github.com/nullstyle/capnp-zig.git#v0.18.0
```

That adds an entry like this to your `build.zig.zon` (the hash is filled in by
the command above — do not hand-write it):

```zig
.dependencies = .{
    .capnpc_zig = .{
        .url = "git+https://github.com/nullstyle/capnp-zig.git#v0.18.0",
        .hash = "capnpc_zig-0.18.0-nUduFTdRNwBzlJgTt6x9lUwRGmpWzSVXrMSE-xFj_dND",
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
        .root_source_file = b.path("gen/schema/addressbook.zig"),
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

## Reflection metadata and runtime versions

The unreleased plugin emits `CAPNP_SCHEMA_REQUEST` and per-type `capnpSchema`
references by default. Use a generator and runtime from the same revision;
released runtimes without `capnpc.reflection` cannot compile that output. Both
`capnpc-zig` and `capnpc-zig-core` expose the reflection API under the generated
module's existing `@import("capnpc-zig")` binding.

When invoking the plugin directly, pass `--no-reflection` to keep the previous
metadata-free output. To pass options explicitly, use the standard binary
request boundary:

```sh
mkdir -p gen
capnp compile -o- schema/addressbook.capnp | (cd gen && capnpc-zig --no-reflection)
```

The colon in `capnp compile -ozig:gen` selects an output directory, not plugin
options. `--no-manifest` controls only the legacy JSON export-name manifest;
the two options are independent. Full and compact API profiles both retain
reflection when enabled. Shape sharing does not alias distinct schema IDs in
reflection-enabled output.

For in-process generation, call `try generator.setSchemaRequest(bytes)` with
the original unpacked compiler request corresponding to `Generator.init`'s
Nodes. The setter owns its encoded metadata; the input bytes may be released
after it returns. Programmatic generators that omit the setter retain their
existing output. See [reflection.md](reflection.md) for runtime use.

## Package integrity preflight (maintainers)

`zig build package-preflight --summary all` (or `just package-preflight`)
tests what the manifest actually exposes, without publishing anything. It:

- snapshots tracked and non-ignored untracked source into an isolated workspace;
- lets Zig apply `build.zig.zon`'s `.paths` filter, rejects material content
  outside the six allowed roots (`build.zig`, `build.zig.zon`, `build`,
  `src`, `README.md`, and `LICENSE`), removes only the empty excluded parent
  directories Zig's local fetch leaves behind, then archives and re-fetches
  that exact surface instead of using a path dependency;
- builds and runs clean-room default, core, and opt-in QUIC consumers in Debug
  and ReleaseSafe with isolated local/global caches;
- proves default/core consumers do not fetch the lazy QUIC dependency and the
  QUIC consumer does;
- builds the packaged compiler plugin, runs it on a checked schema, and compares
  normalized output with the checked-in generated artifact; and
- verifies the checkout status is byte-for-byte unchanged before returning.

The gate requires `git`, `tar`, Zig, and `capnp`. For a local environment that
cannot fetch/build QUIC, pass `-- --skip-quic`; the full CI gate does not skip
it. `-- --keep-temp` retains the otherwise deleted isolated workspace for
diagnosis.

## Notes

- `-ozig:gen` writes generated code under the `gen/` output directory,
  preserving the schema's path: `schema/addressbook.capnp` ->
  `gen/schema/addressbook.zig`.
- `capnpc-zig` is quiet by default; generated logs are only emitted when verbose mode is enabled (`capnpc-zig --verbose` when invoking the plugin directly).
- If your plugin is not on `PATH`, replace `"capnpc-zig"` in the `-o` argument with an absolute executable path.
