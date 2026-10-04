# Upgrading to capnp-zig v0.19.0

This guide is for projects that depend on capnp-zig. v0.19.0 is one release
that bundles three moves:

- the tagged Zig 0.17.0 toolchain;
- quic-zig v0.19.0 -> v0.24.1 (no lifetime stream cap; stream limits are a
  window);
- the sprint in [sprint-plan-2026-10-03.md](sprint-plan-2026-10-03.md):
  named builder error sets, the pinnable plugin, the codegen skew guard,
  QUIC sessions at TCP parity, and liveness fixes.

Move all three at once. The authoritative list of changes is the `0.19.0`
section of [CHANGELOG.md](../CHANGELOG.md). Every break is under
`### Breaking`, each with a **Migration** paragraph, including reflection by
default and Text validation in regenerated bindings. The codegen skew guard is
described under `### Added`. The checklist below covers all of them.

## The coordinated set

| Component | Version | Pin |
|---|---|---|
| Zig | `0.17.0` (tagged) | `mise.toml`: `zig = "0.17.0"`; `build.zig.zon`: `.minimum_zig_version = "0.17.0"` |
| capnp-zig | `v0.19.0` | `capnpc_zig-0.19.0-<hash recorded after the tag>` |
| quic-zig | `v0.24.1` (tag at `4564995`) | `quic-0.24.1-DnSYvRqMNADDwuJG9qlOxfaaUIGacBiERjfZ5DINt3RH` |
| boringssl-zig | `0.6.7` (`ff30fe99`), through quic | none (quic pins it) |

capnp-zig pin (let `zig fetch --save` write the hash):

```sh
zig fetch --save git+https://github.com/nullstyle/capnp-zig.git#v0.19.0
```

```zig
.capnpc_zig = .{
    .url = "git+https://github.com/nullstyle/capnp-zig.git#v0.19.0",
    .hash = "capnpc_zig-0.19.0-<hash recorded after the tag>",
},
```

`zig fetch --save` can record the URL with the resolved commit
(`...git?ref=v0.19.0#<commit>`). That is the same pin. The tarball form
(`https://github.com/nullstyle/capnp-zig/archive/refs/tags/v0.19.0.tar.gz`)
also works. Both forms gave the same hash for v0.18.0.

quic pin, for builds that also depend on quic directly:

```zig
.quic = .{
    .url = "https://github.com/nullstyle/quic-zig/archive/refs/tags/v0.24.1.tar.gz",
    .hash = "quic-0.24.1-DnSYvRqMNADDwuJG9qlOxfaaUIGacBiERjfZ5DINt3RH",
},
```

Stay on quic `v0.24.1`. capnp-zig v0.19.0 pins it and is tested only
against it.

### The one-quic-module rule

A binary links exactly one `quic` module. Zig shares a dependency module only
when every parent pins the same tarball and passes the same option map.
Otherwise Zig 0.17.0 fails with `file exists in modules 'quic' and 'quic0'`.
An absent `release` and `.release = false` are different maps.

capnp-zig v0.19.0 passes this map to quic for its `capnpc-zig` module
(`build/modules.zig`):

```zig
.{
    .target = target,
    .release = optimize != .debug,
    .@"sanitize-c" = @as([]const u8, "trap"),
}
```

capnp-zig does not pass `.optimize` to quic. quic-zig before v0.24.1 had no
`optimize` option. Zig reported `invalid option: "optimize"` and went on, so
quic and BoringSSL built in Debug inside every ReleaseSafe build. v0.24.1
accepts `optimize`, but a parent that passes it still makes a different map.

capnp-zig computes `release` from its own `optimize`. So forward your
`optimize` to capnp-zig, and to every other package that depends on quic.
Then every parent computes the same `release`.

```zig
pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const capnp_dep = b.dependency("capnpc_zig", .{
        .target = target,
        .optimize = optimize, // capnp-zig derives quic's `release` from this
        .quic = true,
    });
    // Only if you also import quic yourself:
    const quic_dep = try b.dependencyLazy("quic", .{
        .target = target,
        .release = optimize != .debug,
        .@"sanitize-c" = @as([]const u8, "trap"),
    });
    // ...
}
```

Do not import the `capnpc-zig-release-safe` module next to `capnpc-zig` in
a Debug build with `.quic = true`. That module always passes
`.release = true` to quic, so the build gets two quic modules.

To check, run `zig build --verbose -Doptimize=ReleaseSafe`. Zig prints one
compile command per artifact. Each command must show exactly one `-Mquic=`,
with `-Osafe` in front of it, and no `-Mquic0=`.

## Checklist for every consumer

Do the items that apply. Each item names the change and what to do.

1. **Zig 0.17.0.** Set `zig = "0.17.0"` in `mise.toml` and
   `.minimum_zig_version = "0.17.0"` in `build.zig.zon`. capnp-zig v0.19.0
   declares that floor, and CI runs only tagged 0.17.0. quic v0.24.x declares
   the same floor, so it refuses every 0.17.0-dev build. Dev builds such as
   `dev.1683` are gone from ziglang.org. If you use zvm, run
   `mise exec -- zig ...`.
   Your own code may also break on the std changes:
   - `std.Io.net.Stream.read` does not compile once referenced. It
     destructures the new `ReadResult` struct as a tuple.
   - The `net_read` operation now returns a `ReadResult`; read `.data_len`.
     Submit `net_read` through `io.operate` or `io.operateTimeout` yourself.
     See `netReadLen` in `src/rpc/transport/tcp/stream_transport.zig`.
   - std's network error sets gained `ConnectionTimedOut`.
   - `std.mem.copyForwards` is deprecated in its doc comment. It still
     compiles with no warning. Replace it with `@memmove`. Do not use
     `@memcpy` where the slices can overlap, such as in-place queue
     compaction (`items[head..]` into `items[0..len]`): `@memcpy` on
     overlapping slices panics in safe builds.
2. **Bump the pin** to `v0.19.0` (above). If you build QUIC, bump quic to
   `v0.24.1` in the same commit and follow the one-quic-module rule.
3. **Regenerate bindings with the plugin from your pin.** Never use a
   `capnpc-zig` binary from `PATH`. The canonical recipe is in
   [build-integration.md](build-integration.md): build
   `b.dependency("capnpc_zig", .{ .target = b.graph.host, .optimize = .ReleaseSafe }).artifact("capnpc-zig")`,
   feed the `CodeGeneratorRequest` on stdin, and pass
   `--output-dir=` through `addPrefixedOutputDirectoryArg`. v0.19.0 is the
   first release with `--output-dir=`. Expect a large diff:
   - Output embeds reflection metadata by default (`CAPNP_SCHEMA_REQUEST`,
     `capnpSchema`), and it needs the matching runtime. Pass
     `--no-reflection` to keep metadata-free output.
   - Output is `zig fmt` clean as written. You can drop a fmt exemption for
     generated files. Do not reformat them.
   - With `--output-dir=`, the plugin ignores `CAPNPC_ZIG_*` environment
     options. Pass options as arguments.
   - Generated Text getters now validate UTF-8 and the NUL terminator.
     Generated Text-list readers use `message.StrictTextListReader`.
     Malformed text is an error when read. Schemas with no Text fields see
     no change.
4. **Codegen skew guard.** Every generated file checks
   `capnpc.codegen_abi` (`version` 1, `release` `"0.19.0"`). A file from the
   v0.19.0 plugin fails against an older runtime with one
   `capnpc-zig version skew` error. Fix: move the runtime and the plugin
   together. See [troubleshooting.md](troubleshooting.md).
5. **`DisconnectCause` is non-exhaustive** (`enum(u8) { ..., _ }`). A
   `switch` must have a `_ =>` arm (treat it as `.unknown`) or an `else`
   arm. Use `std.enums.tagName`, not `@tagName`, for a cause you did not
   construct. Bound any array index taken from `@backingInt(cause)`.
   `std.enums.EnumArray`, `EnumSet` and `EnumMap` keyed by `DisconnectCause`
   now span all 256 values; iterate `std.enums.values(DisconnectCause)`
   instead.
6. **`Event` gains `.cancel_failure`.** A `switch` over `rpc.events.Event`
   that lists every variant no longer compiles. Add an `else => {}` arm.
   `Event` is a tagged union, so later variants will break such switches
   again.
7. **Named builder error sets.** Generated mutators that allocate return
   exactly `message.BuildError`; copy setters return `message.CopyError`.
   Twenty-four functions narrowed from `anyerror`, and thirty-one
   `rpc.wire.protocol` and `message` builders widened (the CHANGELOG lists
   them). Code that calls them with `try` or `catch` needs no change. Fix an
   exhaustive error `switch` by switching over `BuildError`/`CopyError`. Fix
   a narrowed function stored in an `anyerror` function pointer with a
   wrapper. If you snapshot your own API through `@typeInfo`, expect the
   rendered error sets to change.
8. **`Framer.buffer` changed meaning.** `buffer.items[0..consumed]` was
   already returned; only `buffer.items[consumed..]` is unread. Code that
   uses only `push`, `popFrame`, `bufferedBytes` and `reset` is unaffected.
   Code that edits `buffer` directly must also set `consumed`. The QUIC
   `LengthDelimitedFramer` and `NativeControlFramer` changed the same way.
9. **`WorkerPool` reaps silent and idle connections.** New
   `WorkerPool.Config.first_frame_timeout_ms` (default 10 s) and
   `idle_timeout_ms` (default 5 min). Raise them or set them to `null` if
   your clients stay quiet longer, or if your server speaks first. You can
   also set `conn.first_frame_timeout_ms` per connection in `on_accept`. Raw
   `Connection`, `ClientSession` and `ServerSession.accept` have no new
   deadline.
10. **Hardened QUIC preset needs a key.** `ServerProductionHardening`
    requires `stateless_reset_key`. Generate 32 bytes once from a CSPRNG,
    persist them, and load the same bytes on every start. Use one key per
    instance unless your load balancer routes by connection ID; a shared key
    under address-hash routing causes false `.stateless_reset` certificates.
    0-RTT is now the explicit `.early_data = .restore_only` opt-in. It also
    needs a `new_token_key`, and your Restorer must be idempotent. The preset
    now sets `early_dispatch` from `.early_data`, so a base `early_dispatch`
    no longer survives it. See "Production Defaults" in
    [quic-transport.md](quic-transport.md).
11. **`WarmRedialClient` budget counts consecutive failures.** A generation
    that stays healthy for `Policy.min_healthy_ms` (default 10 s) resets
    `redials`. Read `total_redials` for a lifetime count. For the old
    lifetime cap, set `.min_healthy_ms = std.math.maxInt(u64)`.
12. **QUIC stream windows and socket buffers.**
    - Defaults are `initial_max_streams_bidi = 16` and `_uni = 8` (was 4).
      They are now windows of streams open at once.
    - `ClientOptions`/`ServerOptions` ask for 4 MiB UDP buffers
      (`udp_socket_recv_buffer_bytes`, `udp_socket_send_buffer_bytes`).
      `null` keeps the OS default; `0` is `error.InvalidConfig`.
    - On Linux servers, raise `net.core.rmem_max`/`wmem_max` to 4 MiB or
      more.
    - `EmbeddedSession` runs on your socket, so size that socket yourself
      (quic-zig `transport.applyServerTuning`).
    - QUIC transports now refuse peer-opened streams that the protocol never
      uses (STOP_SENDING plus RESET_STREAM). Conforming peers never open
      them.
13. **If you drive quic yourself**, read quic-zig's `EMBEDDING.md`, section
    "Stream limits are a window", and its CHANGELOG `0.24.0` entry:
    - `max_streams_per_connection` is gone.
    - `StreamLimitExceeded` is always temporary: pump, then retry.
    - Finish or reset every peer stream. An unanswered stream holds its place
      in the window for the life of the connection.
    - Refuse a stream with STOP_SENDING and RESET_STREAM.
    - Open local streams in id order. More than 64 runs of skipped ids is
      `TooManySkippedStreamIds`.
    - Reserve a stream id only when you send. An id you reserve and then drop
      unopened is a stream the peer counts as open for good.

To find the call sites in one pass:

```sh
grep -rn --include='*.zig' -e 'DisconnectCause' -e 'events.Event' -e 'Framer' \
  -e 'WorkerPool' -e 'withProductionServerHardening' -e 'WarmRedialClient' \
  -e 'transport_params' -e 'copyForwards' -e 'net_read' -e 'stream.read(' \
  -e 'capnpc-zig-release-safe' .
```

Also read your `build.zig` for every `b.dependency("capnpc_zig", ...)` and
`b.dependency("quic", ...)`, and check the option maps against the
one-quic-module rule.
