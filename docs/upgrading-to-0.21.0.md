# Upgrading to capnp-zig v0.21.0

This guide is for projects that depend on capnp-zig. v0.21.0 bundles these
changes:

- quic-zig v0.28.1 -> v0.30.1: about 91 KB per QUIC connection instead of
  1.09 MB, better loss and probe-timeout handling, and new session-ticket
  options.
- A memory-safety fix for persistent exports (Experimental persistence).
- The core builds for iOS. A new build option, `-Dfd-passing`, can compile
  fd passing out.
- capnp-zig and http3-zig v0.5.4 can now link in one program with one quic
  module.

The Zig toolchain does not change: it stays at tagged 0.17.0. No Stable API
line changes. Two entries are Breaking, and each one has a **Migration**
paragraph in the `0.21.0` section of [CHANGELOG.md](../CHANGELOG.md). That
section is the authoritative list of changes.

## Who should upgrade

- **You use persistence** (`Peer.setPersistentExport` or `setRestorer`)
  together with `Peer.addExportWithDeinit`. Through v0.20.0 the export's
  `deinit_ctx` got an internal pointer, not your ctx. A `deinit_ctx` that
  frees its ctx caused a double free, and your ctx leaked. v0.21.0 gives
  `deinit_ctx` your ctx, exactly once.
- **You run QUIC.** quic-zig v0.29.0 and v0.30.1 cut the memory per
  connection by about 12x, a client completes its handshake through packet
  loss, and a late ACK no longer cuts the congestion window.
- **You build for iOS, or you want no fd closer threads.** See
  `-Dfd-passing` below.

This release has no security fix. v0.20.0 remains the release that fixes
RPC over AF_UNIX sockets.

## The coordinated set

| Component | Version | Pin |
|---|---|---|
| Zig | `0.17.0` (tagged; no change since v0.19.0) | `mise.toml`: `zig = "0.17.0"`; `build.zig.zon`: `.minimum_zig_version = "0.17.0"` |
| capnp-zig | `v0.21.0` (tag at `3490a77`) | `capnpc_zig-0.21.0-nUduFa7PRwBrWc9CyzM8u052LMqBhHzvA52R1s407iCg` |
| quic-zig | `v0.30.1` (tag at `ccf6ae2`; not v0.30.0) | `quic-0.30.1-DnSYvVnOOwBfpDxHQWI41E6YxM1RqqkCuven5bK46jXR` |
| boringssl-zig | `0.6.7` (`ff30fe99`), through quic; no change since v0.25.0 | none (quic pins it) |
| http3-zig (optional) | `v0.5.4` (tag at `22b821f`) | `http3_zig-0.5.4-ayZ03AMwEwCFw0VQCEBd1LP1JBUQtDKT1USKYn_OJACT` |
| qmsg (optional) | `v0.8.1` (tag at `9dae417`) | `qmsg-0.8.1-g3pJMUk4FQAEo6BMkaXjAAYUHuOV96OIBKAC1D_rRdTt` |

### One quic module per process

Zig shares one quic module between packages only when every package pins
the same quic-zig release (same URL and hash) and passes the same option
map:

```zig
const quic_dep = b.dependency("quic", .{
    .target = target,
    .release = optimize != .debug,
    .@"sanitize-c" = @as([]const u8, "trap"),
});
```

capnp-zig v0.21.0, http3-zig v0.5.4 and qmsg v0.8.1 all do this, on
quic-zig v0.30.1. An app that uses capnp-zig with `.quic = true` and
http3-zig v0.5.4 builds with one quic module and one BoringSSL, in Debug
and ReleaseSafe (measured on 2026-10-06). http3-zig v0.5.3 pins quic-zig
v0.29.0, so it does not share the module with capnp-zig v0.21.0, and
v0.5.2 and before build their own quic module. The qmesh-zig and nest
`main` branches pin v0.30.1, with no release yet.

The packages that use quic move to a new quic-zig release together, and
they release after all of them agree. A security fix may release first.

## Checklist for every consumer

1. **Bump the pin.**

   ```sh
   zig fetch --save git+https://github.com/nullstyle/capnp-zig.git#v0.21.0
   ```

   If your own `build.zig` also depends on quic-zig, move it to v0.30.1
   with the option map above.

2. **A raw compiler command needs `capnp_build_options`.** This applies
   only if you pass capnp-zig's modules to `zig build-lib`, `zig build-exe`
   or `zig test` by hand (`-Mcapnpc-zig=.../src/lib.zig`), for Linux or
   macOS. `zig build` and `b.dependency` users change nothing. Add
   `--dep capnp_build_options` and `-Mcapnp_build_options=<file>`, where the
   file holds `pub const fd_passing: bool = true;`
   ([troubleshooting](troubleshooting.md#no-module-named-capnp_build_options)).

3. **Mac Catalyst and DriverKit no longer get fd passing or
   `rpc.transport.unix`.** They now behave like Windows: `unix.listen`
   returns `error.UnixSocketsUnsupported`. No such user is known. If you
   are one, stay on v0.20.0 and tell us.

4. **QUIC in Debug builds: hand a server to another thread only at a
   quiescent point.** quic-zig asserts, in Debug builds, that one thread
   runs `feed`, `tick` and `rotateSessionTicketKey`. To move a server-side
   QUIC `Connection` or `Listener` to another thread, stop the old thread
   first (join it), then call `Connection.adoptOwnerThread` or
   `Listener.adoptLoopThread` on the new thread before its first step. A
   key rotation before `run` may run on any thread, but it must end before
   `run` starts. Release builds do not check.

## New options you may want

- **`-Dfd-passing=false`** (Experimental). It compiles fd passing, the fd
  closer threads and the fd budget out. The AF_UNIX transport then returns
  `error.UnixSocketsUnsupported`, and a TCP transport refuses an AF_UNIX
  socket. A consumer passes it as
  `b.dependency("capnpc_zig", .{ ..., .@"fd-passing" = false })`. Use it
  when your application owns its sockets, for example on Apple platforms
  ([build-integration.md](build-integration.md)).
- **iOS builds.** The core (`capnpc-zig-core`) now compiles for
  `aarch64-ios`, `aarch64-ios-simulator` and `x86_64-ios-simulator`.
  `zig build check-ios` proves it on every push. Your root needs the Zig
  0.17.0 workaround for std's iOS panic path: a trap panic handler, a
  no-op `logFn`, and `std_options_debug_io = std.Io.failing`
  (`tests/apple/apple_check_root.zig` shows it).
- **QUIC session tickets** (Experimental):
  `ServerOptions.previous_session_ticket_key` lets a restarted server open
  the tickets of the key before. `new_token_clock` and
  `new_token_max_clock_skew_us` time NEW_TOKEN tokens.
  `ClientOptions.session_ticket_lifetime_s` limits how long a client keeps
  a ticket. See "Rotation" and "Retry and NEW_TOKEN" in
  [quic-transport.md](quic-transport.md).

## What is new

Read the `0.21.0` section of [CHANGELOG.md](../CHANGELOG.md) for the full
list.
