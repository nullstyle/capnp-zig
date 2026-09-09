# Generated streaming RPC

Cap'n Proto `-> stream` methods expose a generated `StreamClient` and an optional
`DeferredStreamHandler`. These additions are Experimental. The ordinary
callback-based `Client` remains available.

A `StreamClient` starts with a window of **64 calls and 1 MiB of encoded frame
bytes**. Configure `stream.max_in_flight` and `stream.max_in_flight_bytes` before
sending. Zero means unlimited for that dimension. The call count is reserved
before constructing the request; bytes are reserved after capability encoding
and before transport send. A failed send, exception, cancellation, disconnect,
or peer teardown releases each reservation once. Synchronous Returns are safe,
including a transport reporting an error after delivering the Return.

The byte count includes Cap'n Proto segment framing and capability descriptors.
It measures outstanding encoded requests, not all temporary allocation used to
construct a request. A request larger than a nonzero byte window is rejected
with `StreamCallTooLarge`, even when the stream is idle. It is never admitted as
an oversized exception to the limit.

## Waiting for capacity

`StreamInFlightLimitExceeded` and `StreamByteLimitExceeded` mean the call was not
sent. Register a one-shot callback with `whenStreamingReady(encoded_bytes, ctx,
callback)` and retry from that callback. For a call-count rejection, use zero
bytes. For a byte rejection, `stream.last_rejected_bytes` reports the exact size
of the rejected frame. An oversized request must be reduced or the configured
window increased.

Readiness is an opportunity to retry; it does not reserve capacity. Only one
readiness waiter and one drain waiter may be pending. A second registration is
reported to the new callback as `StreamReadyAlreadyPending` or
`StreamDrainAlreadyPending`, respectively. `waitStreaming(ctx, callback)` fires
when all streaming reservations have settled. Both callbacks receive the cached
terminal stream error when one exists.

The first failed streaming Return is cached as `StreamingCallFailed`.
Subsequent streaming calls and ordinary calls through that `StreamClient` return
that error. A transport error returned directly from a send is also returned to
the initiating caller; already-delivered acknowledgements still settle once.

## Deferred application work

For a method such as `doStreamI`, set the generated VTable's
`doStreamI_deferred` field to a `DoStreamI.DeferredStreamHandler`. The original
`doStreamI` synchronous handler field remains required and is used when the
optional deferred handler is null. The deferred handler receives a
`DoStreamI.StreamReturnSender`:

```zig
fn receive(
    ctx: *anyopaque,
    _: *rpc.peer.Peer,
    params: TestStreaming.DoStreamI.Params.Reader,
    _: *const rpc.caps.table.InboundCapTable,
    sender: TestStreaming.DoStreamI.StreamReturnSender,
) anyerror!void {
    const state: *State = @ptrCast(@alignCast(ctx));
    state.value = try params.getI();
    state.pending = sender;
}
```

After application work finishes, call `try state.pending.?.send()`. Use
`sendException(reason)` to fail the stream. Returning from the deferred handler
does not acknowledge the call. A stale, duplicate, or canceled sender returns
`StreamingCallClosed`; its opaque token also prevents it from acknowledging a
new call that reuses the same wire question ID.

The peer delays later calls to the same generated server until the deferred
streaming acknowledgement commits. This includes an ordinary final method used
as a barrier. Retained queued frames own their parameter capability references;
those references survive until dispatch or cancellation and are released on
teardown. A failed deferred call fails the queued calls for that server.

Inbound retained work is bounded across streaming servers on each peer by
`peer.streaming.limits.max_calls` and `max_bytes`, defaulting to **64 calls and
1 MiB**. Active calls and queued calls both count; the registry's
`outstanding_calls` and `outstanding_bytes` expose the totals. These limits also
bound the number of retained stream identities, including failed streams.
Setting either limit to zero explicitly removes that bound. A request that
cannot fit receives an exception without being retained in the queue.

## Lifetimes

Keep a `StreamClient` at a stable address while calls or waiters are outstanding;
its generated call contexts borrow its stream state. Keep the generated server
and its application context alive for the peer/export lifetime. Release an
owned imported `Client` once after use; constructing a `StreamClient` does not
acquire a second import reference.

Deferred parameters, slices, and the inbound capability table are borrowed only
for the handler invocation. Copy data needed by later work and explicitly
retain capabilities that later work will invoke, following the ordinary RPC
ownership contract. The sender can be copied as a token, but does not keep its
peer alive. Complete or discard deferred work before destroying the peer, and
never use a sender through a destroyed peer pointer.

The regression consumers in
[`rpc_stream_consumer.zig`](../tests/serialization/support/rpc_stream_consumer.zig)
and the [C++ interop driver](../tests/serialization/support/rpc_stream_cpp.cpp)
exercise full and compact generation, byte/call pressure, delayed handlers,
barriers, queued capabilities, cancellation, allocation failure, and stale
acknowledgements. Run `zig build test-codegen-rpc-paths test-codegen-streaming-cpp`; the latter requires native C++ Cap'n Proto libraries
and runs on Linux/macOS. The generated lifecycle fuzz target is
`zig build test-fuzz-generated-rpc --fuzz=10000 -j1`.
