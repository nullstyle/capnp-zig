# Native C ABI (Experimental)

`native` is a sans-IO Cap'n Proto RPC connection with a C ABI, for C and
Swift hosts that own their sockets. capnp-swift built it as its core and
moved it here (capnp-swift handoff H7), so it now changes in one place, with
capnp-zig's own tests. All three library roots export it as `native`; it
needs only `capnpc-zig-core`.

- `native.conn` is the Zig API: a `Conn` is a detached `rpc.peer.Peer`, a
  framer and an effect queue.
- `native.abi` is the C ABI over it: `export fn capnp_*`, declared by
  `src/native/include/capnp_core.h`. The header is the reference for every
  function, struct, constant and error code.

Everything here is Experimental: the module, its Zig API and the C ABI may
change in any release. Pin an exact version. The ABI has its own version,
`CAPNP_CORE_ABI_VERSION` (now 1), which a host checks at connect.

## The model

The core never touches a socket and never calls the host, except through the
panic hook. The host:

- pushes received bytes in (`capnp_conn_push_bytes`), in either framing:
  the standard segment-table stream (TCP, Unix sockets, TLS) or the QUIC
  baseline's little-endian u32 length prefix (`CAPNP_FRAMING_U32_LE`;
  `capnp_core_quic_alpn()` returns the frozen ALPN);
- drives time (`capnp_conn_tick`) and reports the end of its transport
  (`capnp_conn_transport_closed`);
- after every call, drains effects (`capnp_conn_next_effect` /
  `capnp_conn_commit_effect`): frames to send, a close request, one RETURN
  per question, inbound calls to answer, dropped exports, and observer
  events. One effect is in flight at a time, and its payload is borrowed
  until its commit.

Params and results cross the ABI as standalone messages whose capability
pointers index a `caps[]` table of imports, exports, promised answers and
null caps. One caller at a time per connection, from any thread.

## Using it from a C or Swift host

Build a static library whose root module references `native.abi`. The
`capnp_*` symbols are emitted only in a compilation that references it, so
plain capnp-zig users get no exported symbols. This is the `build.zig` of the
consumer `zig build package-preflight` builds and runs from the packaged
archive:

<!-- verbatim: tests/package_consumer/build.zig -->
```zig
// The host owns every socket: build capnp-zig without fd passing, the fd
// closer threads and the AF_UNIX transport.
const capnp = b.dependency("capnpc_zig", .{
    .target = target,
    .optimize = optimize,
    .@"fd-passing" = false,
});
// The library root references `native.abi`, so the `capnp_*` symbols
// land in the archive.
const core = b.addLibrary(.{
    .name = "capnp_core",
    .linkage = .static,
    .root_module = b.createModule(.{
        .root_source_file = b.path("src/native_lib.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{
            .{ .name = "capnpc-zig-core", .module = capnp.module("capnpc-zig-core") },
        },
    }),
});
// The program that links the archive brings its own compiler-rt.
core.bundle_compiler_rt = false;
// The header ships in the package, next to the module.
core.installHeader(capnp.path("src/native/include/capnp_core.h"), "capnp_core.h");
b.installArtifact(core);
```

The header's path in the package is `src/native/include/capnp_core.h`
(`capnp.path(...)` above). The library root:

<!-- verbatim-file: tests/package_consumer/src/native_lib.zig -->
```zig
//! Root of the native consumer's static library (docs/native-abi.md): what a
//! C or Swift host's own library root needs to ship capnp-zig's C ABI.

const std = @import("std");
const capnp = @import("capnpc-zig-core");

// Referencing `native.abi` is what emits the `capnp_*` symbols.
comptime {
    _ = capnp.native.abi;
}

/// The allocator behind every connection.
pub const capnp_core_allocator: std.mem.Allocator = std.heap.c_allocator;

/// What `capnp_core_version()` reports: your version and the capnp-zig
/// package you pin.
pub const capnp_core_version_string: [:0]const u8 = "core 0.0.0 / capnp-zig package-preflight / native-consumer";
```

abi.zig reads two declarations from the root module of the compilation
(`@import("root")`). Both are optional, except that a library which does
not link libc must declare `capnp_core_allocator`:

| Declaration | Type | Without it |
|---|---|---|
| `capnp_core_allocator` | `std.mem.Allocator` | in a test build, a counting wrapper around `std.testing.allocator`; else the C allocator when libc is linked; else a compile error in a library, and `std.heap.page_allocator` (at least one page per allocation) in an executable |
| `capnp_core_version_string` | `[:0]const u8` | `"core unknown / capnp-zig unknown / unknown"` |

`capnp_core_debug_trap` (a test hook) traps in `src/native/abi.zig` on the
line marked `CAPNP_CORE_DEBUG_TRAP_LINE`, so crash tooling can check that a
Zig frame symbolicates. In a library built from the package, the debug info
names that file under the package directory, which changes with every pin
(`zig-pkg/capnpc_zig-<version>-<hash>/src/native/abi.zig`): derive the
directory from the pinned package, or match the suffix
`src/native/abi.zig:<line>`.

A library for iOS also needs the std overrides that
[build-integration.md](build-integration.md#ios-the-core-as-a-static-library-experimental)
lists. Zig code can skip the C ABI and use `native.conn.Conn` directly.

## Gates

- `zig build test` runs the shim's own tests in the `capnpc-zig-core` test
  root: `src/native/conn_test.zig` (two connections back to back, the
  capability round trip, disconnect and OOM paths, malformed input, both
  framings), the tests in `abi.zig` (a counting allocator must reach 0 live
  bytes after every export ran), and the header gate,
  `src/native/abi_header_test.zig`: every `pub export fn capnp_*` has a
  prototype in the header with the same calling convention, arity, scalar
  widths and struct layouts, every prototype is exported, and the `CAPNP_*`
  constants match.
- `zig build test-native-abi` (also in `test`) calls the ABI through the
  translated header, linked from a host static library
  (`tests/native/abi_lib_root.zig`); it never imports abi.zig.
- `zig build fuzz-native-abi -- --seconds N [--seed S]` drives the ABI with
  random operation sequences and checks its invariants (one RETURN per
  question, well-formed payloads, 0 live bytes after free). A seeded
  3-second run is part of `test-fuzz-smoke`; Nightly runs it for 10 minutes
  in ReleaseSafe.
- `zig build check-ios` compiles the ABI into the iOS, simulator and macOS
  (fd passing off) libraries; `check-fd-passing-off-symbols` reads the macOS
  one. `zig build package-preflight` builds the consumer above, with a C
  program that includes the header, and runs it.
- `zig build hardening` scans `src/native/`. An ABI change shows in review
  twice: in the header and in `docs/api-snapshot-experimental.txt`, where
  every export and C struct field has a line.

## Provenance

Moved from capnp-swift at `1aceb01` (its `core/src` and
`core/include/capnp_core.h`). capnp-swift keeps its Apple glue: the library
root with the panic hook and std overrides (`apple_root.zig`), the Clang
module map and the XCFramework build.
