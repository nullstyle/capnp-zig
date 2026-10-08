# Upgrading to capnp-zig v0.22.0

This guide is for projects that depend on capnp-zig. v0.22.0 bundles these
changes:

- quic-zig v0.30.1 -> v0.32.0. A dead QUIC peer is detected one idle
  timeout after the last unanswered send. Through v0.21.0 it took two to
  three idle timeouts.
- Seven fixes in the Experimental QUIC code for connection close and
  teardown, three of them use-after-free risks.
- Level-3 three-party handoff origination over the WASM host ABI (feature
  bit `10`, Experimental).
- capnp-zig and http3-zig v0.5.5 link in one program with one quic module.

The Zig toolchain does not change: it stays at tagged 0.17.0. No API line
changes in any snapshot, and there is no Breaking entry. The `0.22.0`
section of [CHANGELOG.md](../CHANGELOG.md) is the authoritative list of
changes.

## Who should upgrade

- **You run RPC over QUIC.** Take v0.22.0 for the dead-peer fix: with
  capnp-zig v0.21.0 (quic-zig v0.30.1), a peer that died held its
  connection, and delayed a `WarmRedialClient` heal, for two to three idle
  timeouts. A spoofed or unopened datagram could also restart the idle
  timer, in every earlier QUIC release.
- **You embed QUIC with `EmbeddedSession`.** A seat could call `on_error`
  after `on_close`. If your `on_close` frees the Peer (the pattern the Peer
  docs describe), that was a use-after-free. In native mode a client could
  force the timing. v0.22.0 makes the close callback the seat's last call
  into the host.
- **You call `WarmRedialClient.requestStop` from another thread.** It could
  write into a connection that the run thread had already torn down.

This release has no security fix in the sense of a released advisory. The
use-after-free fixes above need an Experimental surface and a specific host
pattern.

## The coordinated set

| Component | Version | Pin |
|---|---|---|
| Zig | `0.17.0` (tagged; no change since v0.19.0) | `mise.toml`: `zig = "0.17.0"`; `build.zig.zon`: `.minimum_zig_version = "0.17.0"` |
| capnp-zig | `v0.22.0` | `capnpc_zig-0.22.0-...` (recorded here after the tag) |
| quic-zig | `v0.32.0` (tag at `ffdb251`) | `quic-0.32.0-DnSYvcGOPADCEZMispvPbD9RznRtyIGq77zHIgmS8iVl` |
| boringssl-zig | `0.6.7` (`ff30fe99`), through quic; no change since v0.25.0 | none (quic pins it) |
| http3-zig (optional) | `v0.5.5` (tag at `380ead3`) | `http3_zig-0.5.5-ayZ03DI5EwD2bajKDfR009PM7jlRI3lzSnrmT2PlWgS4` |

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

capnp-zig v0.22.0 and http3-zig v0.5.5 both pin quic-zig v0.32.0 with this
map. An app that uses capnp-zig with `.quic = true` and http3-zig v0.5.5
builds with one quic module and one BoringSSL, in Debug and ReleaseSafe
(measured on 2026-10-07). http3-zig v0.5.4 pins quic-zig v0.30.1, so it
pairs with capnp-zig v0.21.0 only.

Only packages that link into one program must share a quic-zig pin.
capnp-zig and http3-zig move together. The other quic packages (qmsg,
qmesh-zig, nest, mruby-quic) move on their own schedule; qmsg v0.8.2 also
pins quic-zig v0.32.0. A security fix in quic-zig moves every package at
once.

## Checklist for every consumer

1. **Bump the pin.**

   ```sh
   zig fetch --save git+https://github.com/nullstyle/capnp-zig.git#v0.22.0
   ```

   If your own `build.zig` also depends on quic-zig, move it to v0.32.0
   with the option map above.

2. **Idle timeouts have a floor.** quic-zig now keeps the idle timeout at
   least three probe timeouts long. Before the first RTT sample that is
   about 3 s; on a LAN it is tens of milliseconds, on a WAN path several
   hundred milliseconds or more. A test that sets a very small idle timeout
   and expects a fast close may see the floor.

3. **`EmbeddedSession` hosts:** after the close callback, the seat makes no
   more calls into the host, drops stream data, and returns at once from
   `service`. `destroy` is the host's last call on a seat. A `destroy` from
   inside a seat callback now finishes before that seat call returns.
   `requestClose` now sends a real QUIC close with the normal code.

## What is new

Read the `0.22.0` section of [CHANGELOG.md](../CHANGELOG.md) for the full
list.
