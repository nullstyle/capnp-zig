# HANDOFF — zig fork change branch: `net.Stream.read` destructures `ReadResult`

Paste this into a session working on the nullstyle zig fork. Self-contained.
It joins the fork's change-branch list (see the other
`handoff-zig-fork-*.md` files beside this one). Suggested branch:
`fix/stream-read-readresult`.

## The defect

At tagged **0.17.0**, `std.Io.net.Stream.read` does not compile. Any program
that references it fails, whether or not it ever calls it.

File: `lib/std/Io/net.zig`, `Stream.read` (lines 1285-1291 at 0.17.0):

```zig
pub fn read(s: *const Stream, io: Io, data: [][]u8) Reader.Error!usize {
    const rc, _ = try (try io.operate(.{ .net_read = .{
        .socket_handle = s.socket.handle,
        .data = data,
    } })).net_read;
    return rc;
}
```

The function destructures the `net_read` result as a two-element tuple. But
`Io.Operation.NetRead.Result` (`lib/std/Io.zig:447`) is now
`Error!net.Stream.ReadResult`, and `ReadResult` (`net.zig:1272`) is a struct:

```zig
pub const ReadResult = struct {
    data_len: usize,
    control_len: usize = 0,
    control_truncated: bool = false,
};
```

A struct cannot be destructured, so analysis fails. The sibling
`Stream.readWithControl` (`net.zig:1297`) was updated for the new shape;
`read` was not.

The fork's `master` and `feat/evented-dispatch` (checked read-only on
2026-10-03) predate this change: there `NetRead.Result` is still
`Error!usize` and `read` returns it directly. The defect arrives when the fork
rebases onto 0.17.0.

## Evidence

capnp-zig works around it: `src/rpc/transport/tcp/stream_transport.zig`
`ioReadVec` submits the `net_read` operation itself instead of calling
`Stream.read`, and `netReadLen` reads `.data_len`. The TCP wake-channel drain
in `src/rpc/transport/tcp/connection.zig` does the same.

## Minimal repro

No dependencies; capnp-zig is not involved:

```zig
const std = @import("std");

test "Stream.read compiles" {
    // Referencing the wrapper is enough to analyze it; it is never called.
    _ = &std.Io.net.Stream.read;
}
```

```
$ zig test stream_read_repro.zig
lib/std/Io/net.zig:1286:23: error: type 'Io.net.Stream.ReadResult' cannot be destructured
        const rc, _ = try (try io.operate(.{ .net_read = .{
                      ^~~
lib/std/Io/net.zig:1286:21: note: result destructured here
```

## The fix

Read the byte count from the struct:

```zig
pub fn read(s: *const Stream, io: Io, data: [][]u8) Reader.Error!usize {
    const result = try (try io.operate(.{ .net_read = .{
        .socket_handle = s.socket.handle,
        .data = data,
    } })).net_read;
    return result.data_len;
}
```

No control buffer is passed, so `control_len` is always 0 and
`control_truncated` is always false here; dropping them loses nothing.

Then search `lib/std` for other `, _ = ` destructures of a `net_read` result
and for callers that still treat it as `usize`.

## Verification

1. The repro compiles.
2. A real round trip through `Stream.read` on `std.testing.io`. This test
   passes against a 0.17.0 std with only the fix above applied (verified
   2026-10-03 on macOS, Darwin 27, aarch64), and fails to compile on stock
   0.17.0:

   ```zig
   const std = @import("std");
   const net = std.Io.net;

   test "Stream.read returns the byte count of a real loopback read" {
       const io = std.testing.io;
       const any_port = try net.IpAddress.parseLiteral("127.0.0.1:0");
       var server = try any_port.listen(io, .{});
       defer server.deinit(io);

       const client = try server.socket.address.connect(io, .{ .mode = .stream });
       defer client.close(io);
       const accepted = try server.accept(io);
       defer accepted.close(io);

       var wbuf: [16]u8 = undefined;
       var w = client.writer(io, &wbuf);
       try w.interface.writeAll("hello");
       try w.interface.flush();

       var rbuf: [16]u8 = undefined;
       var bufs: [1][]u8 = .{&rbuf};
       const n = try accepted.read(io, &bufs);
       try std.testing.expectEqual(@as(usize, 5), n);
       try std.testing.expectEqualStrings("hello", rbuf[0..n]);
   }
   ```

3. Add that test (or an equivalent) to the std `net` tests so the wrapper is
   analyzed on every std test run. The defect shipped because nothing in std
   references `Stream.read`.

## Bookkeeping

Record in the fork's change-branch list: "`net.Stream.read` must read
`ReadResult.data_len`; at 0.17.0 it destructures the struct and does not
compile."
