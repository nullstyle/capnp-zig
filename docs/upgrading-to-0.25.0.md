# Upgrading to capnp-zig v0.25.0

This guide is for projects that depend on capnp-zig. v0.25.0 is a QUIC
release, paired with http3-zig v0.5.7. It bundles these changes:

- A fix for a QUIC client that never ended. When a client's connection
  closed with frames still queued, its `Connection.run` kept running, its
  close callback never ran, and a `Peer` on it never settled its pending
  calls. This happened after the server's close, after an idle timeout,
  and in the native large-frame stall. The bug dates from the first QUIC
  transport.
- quic-zig moves from v0.37.2 to v0.38.0. Its memory budget keeps a
  receive reserve, so an honest peer's stream data no longer ends a
  connection with EXCESSIVE_LOAD while the connection window is no larger
  than the budget. A window above half of the budget leaves the writes
  less than half.
- capnp-zig's half-budget write cap (v0.24.0) is gone. So is the
  four-times budget rule from the corrected 0.24.0 notes.
- A server now announces at most half of its `max_connection_memory` as
  its connection window.

The Zig toolchain does not change: it stays at tagged 0.17.0.
Serialization, codegen and TCP RPC do not change. No Stable API line
changes, no Experimental snapshot line moves, and there is no Breaking
entry. The `0.25.0` section of [CHANGELOG.md](../CHANGELOG.md) is the
authoritative list of changes.

## Who should upgrade

- **Every QUIC client.** On v0.24.0 and older, a client whose server shut
  down while the client still had frames queued never ended, not even at
  its idle timeout. Now it ends at once with `DisconnectCause.peer_close`,
  and its pending calls fail as disconnected.
- **QUIC servers.** A session whose connection closes with frames still
  queued now gets its close callback in the step that sees the close.
  Through v0.24.0 it waited for the end of quic-zig's draining period,
  unless the server had closed the session itself. The server no longer
  steps without waiting while a session drains.
- **Projects that also link http3-zig.** Move to http3-zig v0.5.7 in the
  same change. It pins the same quic-zig, so the program has one quic
  module.

Projects that do not build with `-Dquic=true` get no behavior change. This
release has no security advisory.

## The coordinated set

| Component | Version | Pin |
|---|---|---|
| Zig | `0.17.0` (tagged; no change since v0.19.0) | `mise.toml`: `zig = "0.17.0"`; `build.zig.zon`: `.minimum_zig_version = "0.17.0"` |
| capnp-zig | `v0.25.0` (tag at `6598228`) | `capnpc_zig-0.25.0-nUduFVhHTgA6JaxDmRnzuDI978oyDeAuKOxrlJ8VI9fW` |
| quic-zig | `v0.38.0` (tag at `77be067`) | `quic-0.38.0-DnSYve_hPwBp5d2c8QUoZiNWi-rHT6QFR4pK2DueXEAC` |
| http3-zig (optional) | `v0.5.7` (tag at `e9867fc`) | `http3_zig-0.5.7-ayZ03AxnEwCS-38QeydGjOHQ1sAQEWT8xWjf82hAOPBw` |

capnp-zig v0.25.0 and http3-zig v0.5.7 link into one program with one quic
module and one BoringSSL (measured at the v0.5.7 tag, Debug and
ReleaseSafe). http3-zig v0.5.6 pins quic-zig v0.37.2 and pairs with
capnp-zig v0.24.0.

## Checklist for every consumer

1. **Bump the pin.**

   ```sh
   zig fetch --save git+https://github.com/nullstyle/capnp-zig.git#v0.25.0
   ```

   Codegen does not change (codegen ABI 2), so files from the 0.23.0 and
   0.24.0 plugins compile against this runtime.

2. **Move the other quic pins in the same change.** A build that also
   depends on quic-zig or http3-zig must pin quic-zig v0.38.0 (http3-zig
   v0.5.7) with the same options as capnp-zig: `.target`,
   `.release = optimize != .debug`, `.@"sanitize-c" = "trap"`. Otherwise
   the build makes two quic modules, each with its own BoringSSL.

3. **A server with `max_connection_memory` below twice its configured
   connection window (32 MiB with the default 16 MiB window) announces a
   smaller window.** capnp-zig now announces `initial_max_data`
   of at most half of the budget: a 4 MiB budget announces 2 MiB, a
   256 KiB budget 128 KiB. On quic-zig v0.38.0 a larger window would leave
   the server less than half of the budget to write, and a window as large
   as the budget would leave it nothing. Nothing changes at the defaults
   (32 MiB budget, 16 MiB window). If you lowered the budget, check two
   more things:
   - Native mode: with the default stream windows, a server budget below
     2 MiB lowers the largest data-stream frame the server receives to
     about 1 MiB plus half of the budget. A larger frame stalls.
   - 0-RTT: the announced window is part of quic-zig's 0-RTT context. After
     the upgrade (or after any budget change across a restart), tickets
     issued before the change resume without 0-RTT for one ticket lifetime.
     A staged frame then goes at 1-RTT.
   To keep a larger window, raise the budget to at least twice the window
   you want.

4. **You can drop the four-times budget rule.** The tagged v0.24.0 guide
   said a budget of at least twice the announced window was enough. Its
   corrected text (docs/upgrading-to-0.24.0.md, step 4) says only a server
   budget of four times the larger of the window and 16 MiB (64 MiB with
   the defaults) rules out EXCESSIVE_LOAD from an honest peer. On quic-zig
   v0.38.0 a budget as large as the window keeps room for the peer's
   stream data, and a budget of twice the window also leaves half of it
   for writes. The defaults are enough.

5. **An embedder that builds its own quic-zig config** keeps the connection
   window at most half of the budget itself ("Embedder rules" in
   docs/quic-transport.md). `serverConfigFromOptions` does it for you.

6. **Native frames over about 2 MiB still stall with the default
   windows.** That limit is unchanged. Both sides now end at the idle
   timeout, and a `Peer` settles the call as disconnected. Use baseline
   mode for such frames, or raise the receiver's
   `transport_params.initial_max_stream_data_uni` and connection window
   above the largest frame (on a server, keep the budget at least twice
   the connection window). quic-zig refuses a window above 16 MiB with
   `error.InvalidValue`, so send a frame of 16 MiB or more in baseline
   mode, though `max_message_bytes` allows 64 MiB ("Current Limits" in
   docs/quic-transport.md).

## What is new

Read the `0.25.0` section of [CHANGELOG.md](../CHANGELOG.md) for the full
list, with the tests behind each fix.
