# Upgrading to capnp-zig v0.20.0

> **Release candidate.** v0.20.0 is not tagged yet, and the newest tag is
> v0.19.1. This guide describes the release candidate on `main`. Until the
> tag exists, keep the v0.19.1 pin.

This guide is for projects that depend on capnp-zig. v0.20.0 bundles these
changes:

- Cap'n Proto RPC over Unix-domain sockets (Linux and macOS), with fd
  passing as an opt-in.
- A security fix for every RPC connection on an AF_UNIX socket.
- quic-zig v0.25.0 -> v0.27.0: session tickets that live through a
  restart, and 0-RTT data that still goes early behind a Retry.
- A session-ticket key for QUIC servers, with rotation.
- A freeze gate for the shape of generated code.

The Zig toolchain does not change: it stays at tagged 0.17.0. No Stable line
changes. Every break is in an Experimental surface, and each one has a
**Migration** paragraph in the `0.20.0` section of
[CHANGELOG.md](../CHANGELOG.md). That section is the authoritative list of
changes. The checklist below covers every break.

## Who must upgrade

**Upgrade if you run capnp-zig RPC over an AF_UNIX stream socket.** A peer
can attach open files to the bytes it sends on such a socket (SCM_RIGHTS).
Through v0.19.1, the stream transport read with a plain `read`:

- On macOS, the process kept every attached fd, and nothing closed it. A
  peer could fill the fd table of the whole process.
- On Linux, the reader thread closed the attached fds. The final close of a
  lingering socket blocked that thread for the linger time, and every frame
  behind it waited too.
- On both, a teardown or a listener `close` could block in the same way.

You are exposed if you use one of these on an AF_UNIX socket:
`tcp.Connection`, `tcp.Transport` (also when you drive it directly),
`tcp.Listener.initFd` with `accept`, `acceptFd` or `close`,
`tcp.ServerSession.accept`, or `tcp.ClientSession`. An attacker needs only
two things: the right to connect to your socket (or to be your peer), and
one `sendmsg`. TCP and QUIC connections are not affected. No v0.19.x
release has this fix, so take v0.20.0.

After the upgrade, raise the soft `RLIMIT_NOFILE` of your process to 1024
or more, before the first AF_UNIX connection. One message can carry 254
fds, and the macOS default soft limit is 256. The library never changes a
process limit (threat-table row 41 in
[rpc-unix-sockets.md](rpc-unix-sockets.md#threat-table)).

QUIC users get no new security fix in this release. v0.19.1 already moved
to quic-zig v0.25.0, which fixes the unauthenticated-datagram fault.
quic-zig v0.26.0 and v0.27.0 have no security fix.

## The coordinated set

| Component | Version | Pin |
|---|---|---|
| Zig | `0.17.0` (tagged; no change since v0.19.0) | `mise.toml`: `zig = "0.17.0"`; `build.zig.zon`: `.minimum_zig_version = "0.17.0"` |
| capnp-zig | `v0.20.0` | `capnpc_zig-0.20.0-...` (`zig fetch --save` writes it; [build-integration.md](build-integration.md) records it at the tag) |
| quic-zig | `v0.27.0` (tag at `9d2ab6e`) | `quic-0.27.0-DnSYvdkEOQDHm_pJQOp6Vo47gQ1Cm7-0c2sgVV0M6Xht` |
| boringssl-zig | `0.6.7` (`ff30fe99`), through quic; no change since v0.25.0 | none (quic pins it) |

capnp-zig pin (let `zig fetch --save` write the hash):

```sh
zig fetch --save git+https://github.com/nullstyle/capnp-zig.git#v0.20.0
```

quic pin, for builds that also depend on quic directly:

```zig
.quic = .{
    .url = "https://github.com/nullstyle/quic-zig/archive/refs/tags/v0.27.0.tar.gz",
    .hash = "quic-0.27.0-DnSYvdkEOQDHm_pJQOp6Vo47gQ1Cm7-0c2sgVV0M6Xht",
},
```

Use quic `v0.27.0`. capnp-zig v0.20.0 pins it and is tested only against
it. Never pin a quic tag older than v0.25.0: each one has the
unauthenticated-datagram fault.

### One quic module per process

A binary links exactly one `quic` module. Zig shares a dependency module
only when every parent pins the same tarball and passes the same option
map. Otherwise the build makes two quic modules, each with its own
BoringSSL, and Zig 0.17.0 can fail with `file exists in modules 'quic' and
'quic0'`.

So, in the same commit, pin every package that depends on quic-zig at a
release that pins quic v0.27.0: capnp-zig, qmsg, nest, qmesh-zig,
http3-zig, and your own build. Before you pin a release of such a package,
read its `build.zig.zon`. Its quic pin must be v0.27.0.

On 2026-10-05, no tag of http3-zig, qmsg or qmesh-zig pins quic v0.27.0.
Their `main` branches pin it, but their newest tags pin older versions:

| Package | Newest tag | quic pin of that tag | quic pin of `main` |
|---|---|---|---|
| http3-zig | `v0.5.1` | `v0.26.0` | `v0.27.0` |
| qmsg | `v0.7.0` | `v0.21.0` | `v0.27.0` |
| qmesh-zig | `0.2.1` | `v0.21.0` | `v0.27.0` |

Do not pin one of these tags next to capnp-zig v0.20.0 with `-Dquic=true`.
The build then makes two quic modules. Pin a release of the package that
pins quic v0.27.0, or wait for one.

The option map does not change. capnp-zig passes this map to quic
(`build/modules.zig`):

```zig
.{
    .target = target,
    .release = optimize != .debug,
    .@"sanitize-c" = @as([]const u8, "trap"),
}
```

Forward your `optimize` to capnp-zig, and to every other package that
depends on quic, so that every parent computes the same `release`.
[upgrading-to-0.19.0.md](upgrading-to-0.19.0.md) ("The one-quic-module
rule") explains the map and gives a full `build.zig` example.

To check, run `zig build --verbose -Doptimize=ReleaseSafe`. Each compile
command must show exactly one `-Mquic=`, with `-Osafe` in front of it, and
no `-Mquic0=`.

## Checklist for every consumer

Do the items that apply. Each item names the change and what to do.

1. **Bump the pins.** Pin capnp-zig `v0.20.0` (above). If you build QUIC,
   pin quic `v0.27.0` in the same commit, and follow the one-quic-module
   rule.
2. **Generated code.** The codegen ABI does not change (`version` 1), so
   bindings from the v0.19.x plugin still compile against the v0.20.0
   runtime. Regenerate them with the plugin from your pin all the same
   ([build-integration.md](build-integration.md)). The only change in
   generated code is the name of one capture: a schema that declares a
   file-level `flag` (for example `annotation flag`) now compiles.
3. **`events.Source` and `events.Resource` are non-exhaustive (Breaking,
   Experimental).** Both are now `enum(u8) { ..., _ }`, with the new values
   `Source.unix` and `Resource.attached_fds`.
   - A `switch` on either enum needs an `else` (or `_`) arm.
   - Use `std.enums.tagName`, not `@tagName`, for a value that you did not
     construct. It returns `null` for an unnamed value.
   - `std.enums.EnumArray`, `EnumSet` and `EnumMap` keyed by either enum
     now span all 256 backing values. Iterate `std.enums.values(...)`
     instead.
4. **AF_UNIX connections report `events.Source.unix`.** Their connection,
   frame, backpressure, pressure and close events said `.tcp` before. If
   your metrics or logs key on `.tcp` for a Unix connection, add `.unix`.
   TCP connections still report `.tcp`.
5. **AF_UNIX connections close the fds a peer attaches (drain mode).** This
   is the security fix. A process-wide closer thread closes every received
   fd, so your code never sees one.
   - The process fd budget bounds the closer's queue of received fds
     (`rpc.transport.unix.fd_io.budget`, `RLIMIT_NOFILE / 4` by default).
     The queue fills when a close blocks, for example on a lingering socket
     that a peer sent. While it is full, an AF_UNIX connection that
     receives data closes with `error.SystemResources`, after a
     `.resource_rejection` event (`resource = .attached_fds`).
   - An AF_UNIX listener accepts nothing while the closer's socket lane is
     full. Each wait emits a `.backpressure` event
     (`err = error.SocketCloseQueueFull`). The error sets do not change.
   - If you want fds to arrive, turn on fd passing on both ends (see "What
     is new").
6. **A QUIC client refused during its handshake ends with `.peer_close`
   (Breaking, Experimental behavior).** A server that refuses a session in
   its accept hook (`Server.setOnSessionAccepted`, or
   `ServeOptions.on_accept`) closes it before the handshake completes.
   Through v0.19.1 the client waited for its own handshake timeout and
   closed with `.handshake_timeout`. Now `run()` returns within a round
   trip with `DisconnectCause.peer_close`, and outstanding questions settle
   as `disconnected`.
   - Code that read `.handshake_timeout` as "the server refused me" must
     also handle `.peer_close`. `WarmRedialClient` redials on neither.
   - Keep the handshake timeout. quic-zig does not send a lost
     CONNECTION_CLOSE again, so a refusal whose close is lost still ends
     at the timeout.
7. **`quic.Listener.nowUs` counts from the Unix epoch (Breaking,
   Experimental).** It counted from `Listener.init`. The clock now starts
   at the wall clock in `init` and advances on the monotonic clock, so a
   restarted server accepts its predecessor's NEW_TOKENs. New field:
   `Listener.clock_origin_us`.
   - If you read `nowUs()` as an uptime, subtract your first reading (or
     `clock_origin_us`).
   - Code that only feeds the value back to quic-zig, or takes
     differences, needs no change.
   - An embedded-mode host that feeds its own clock to quic-zig needs the
     same property, or NEW_TOKENs do not survive a restart.
8. **QUIC tokens are 114 bytes (were 96).** A NEW_TOKEN that a v0.19.x
   client persisted (for example in a `WarmRedialClient.exportWarmState`
   envelope) reads as malformed at a v0.27.0 server. The server treats it
   as no token. That client's next dial gets one Retry (one round trip; its
   0-RTT restore still runs early) and a new token. Nothing closes. If your
   code holds a token in a `[96]u8`, use quic-zig's type or
   `max_token_len`.
9. **QUIC handshake datagrams are at most 1200 bytes.** A client Initial
   and the first datagram of a server are exactly 1200 bytes. A test that
   counts or measures handshake datagrams can change. capnp-zig's receive
   buffers are larger.
10. **QUIC servers bind the transport mode and `early_dispatch` into their
    0-RTT context.** If a server restarts with a different mode or
    `early_dispatch`, a returning client still resumes its session, but the
    server refuses that session's 0-RTT data. An embedder that builds its
    own quic-zig server from `serverConfigFromOptions` gets the same
    context.
11. **`WarmRedialClient` redials from the port of the generation before
    it.** A NEW_TOKEN is valid only from the address and port that earned
    it. So each generation after the first binds the previous generation's
    local UDP port. A `base.local_addr` that names a port is used as it
    is.
    - When the old port is taken, the dial falls back to an ephemeral port
      and gets a Retry. The new counter
      `Outcome.port_fallback_generations` counts these dials ("What is
      new" lists the new counters).
    - Behind a NAT, the server sees the NAT's port. The redial skips the
      Retry only when the NAT maps the reused local port to the same
      external port.
12. **If you drive quic-zig yourself**, read its CHANGELOG entries `0.26.0`
    and `0.27.0`.
    - `requestKeyUpdate` returns `error.KeyUpdateBlocked` until the
      handshake is confirmed.
    - `Connection.setRememberedPeerTransportParams` must carry the two
      stream counts. A resumed client opens at most that many streams
      before its handshake, and one more gives `StreamLimitExceeded`.
      Treat it as transient.
    - capnp-zig calls neither function.

To find the call sites in one pass:

```sh
grep -rn --include='*.zig' -e 'events.Source' -e 'events.Resource' \
  -e '@tagName' -e 'handshake_timeout' -e 'nowUs' -e 'WarmRedialClient' \
  -e 'exportWarmState' -e 'requestKeyUpdate' \
  -e 'setRememberedPeerTransportParams' -e 'initFd' .
```

Also read your `build.zig` for every `b.dependency("capnpc_zig", ...)` and
`b.dependency("quic", ...)`, and check the option maps against the
one-quic-module rule.

## What is new

All of it is Experimental and additive. None of it changes
`docs/api-snapshot.txt`.

- **RPC over Unix-domain sockets (Linux and macOS).**
  `rpc.transport.unix.listen` returns a `tcp.Listener`, so
  `ServerSession.accept` serves it unchanged. `rpc.transport.unix.connect`
  returns a `*tcp.ClientSession`. `listen` holds a `<path>.lock` file, sets
  the socket file to mode 0600 before it accepts, and refuses paths that do
  not fit `sun_path`. Keep the socket in a private (0700) directory.
  `tcp.Listener.unixPath()` gives the path, and `WorkerPool.initListener`
  serves a Unix listener. Other targets get
  `error.UnixSocketsUnsupported`. Run `zig build example-rpc-unix`, and
  read [rpc-unix-sockets.md](rpc-unix-sockets.md) before you ship.
- **Fd passing (opt-in, Linux and macOS).** A capability can carry a file
  descriptor, as in C++. Set `rpc.transport.unix.FdPassing` on both
  `unix.ListenOptions` and `unix.ConnectOptions`. `Peer.setExportFd`
  attaches a borrowed fd to an export, and `Peer.importFd` lends the fd of
  an import until the import is released. Per-message and per-connection
  caps and the process fd budget bound it. It works with the C++ reference
  on Linux (`zig build test-rpc-fd-cpp`, in CI). **On macOS, do not send
  fds to a C++ peer:** kj can give them to the next frame and drop them.
  Run `zig build example-rpc-fd`, and read "Fd passing" and the threat
  table in [rpc-unix-sockets.md](rpc-unix-sockets.md) first.
- **A freeze gate for generated code.** `docs/generated-shape.txt` records
  the Stable families of generated code: Reader/Builder accessors, enums
  and constants with their values (`interface_id`, method `ordinal`),
  `Client` calls, `Server` and `VTable` fields, `Response` with `unwrap`,
  and the callback typedefs. CI renders it from a committed corpus of
  schemas, and a release that changes a Stable line needs a Stable
  `### Breaking` entry. [generated-api.md](generated-api.md#what-is-frozen-in-generated-code)
  lists what is frozen.
- **A QUIC session-ticket key (opt-in).** With
  `ServerOptions.session_ticket_key` set to the same 48 bytes on every
  start, a crash-restarted server opens its predecessor's tickets. A
  `WarmRedialClient` heal then restores in 0-RTT, behind a Retry too.
  `quic.loadTicketKeyFile` reads the key file. On POSIX it refuses a file
  with any group or other permission bit. `session_ticket_lifetime_s`
  shortens the
  ticket lifetime (1 s to 2 days). `Server.rotateSessionTicketKey` and
  `Listener.rotateSessionTicketKey` change the key with no restart and no
  lost ticket; call them on the loop thread. Persist `new_token_key` with
  the key, so that a returning client also skips the Retry. A stolen key
  file opens recorded 0-RTT data and lets the thief impersonate the server
  to resuming clients. Read "Session-ticket key" in
  [quic-transport.md](quic-transport.md#session-ticket-key) before you set
  it.
- **Three new `WarmRedialClient` counters.** `WarmRedialClient.Outcome` has
  three new fields, and `WarmRedialClient` has fields with the same names.
  Each one starts at 0, so a struct literal of `Outcome` without them still
  compiles.
  - `zero_rtt_generations` counts the dials whose early data the server
    accepted, with or without a Retry. The restore of such a dial ran
    before the handshake completed.
  - `retried_generations` counts the dials that got a Retry. Each one cost
    one more round trip.
  - `port_fallback_generations` counts the dials that fell back to an
    ephemeral port, because the port of the generation before was taken.

  A dial in `zero_rtt_generations` and in `retried_generations` restored in
  0-RTT behind a Retry. A dial in `zero_rtt_generations` and not in
  `retried_generations` also skipped the Retry.
