@0xa1b2c3d4e5f60007;

using Go = import "/go.capnp";
$Go.package("resolve_disembargo");
$Go.import("e2e-rpc-test/internal/resolve_disembargo");

# Reflected-capability resolve/disembargo scenario.
#
# Exercises the Cap'n Proto embargo handshake end to end across implementations:
#   - cap-in-params (the caller hands the reflector one of its OWN capabilities),
#   - a promise capability exported before its target is known, resolved later
#     to the caller-hosted capability (a "reflected" resolution: the promise
#     resolves to a target reached by a different path than the promise itself),
#   - the resulting senderLoopback -> receiverLoopback Disembargo round-trip that
#     flushes in-flight pipelined calls before the resolved cap is used directly.
#
# See docs/supported-surface.md for the conformance notes this scenario backs.

interface CallSequence {
  # A caller-hosted callee whose invocations are ordered by a monotonic counter,
  # so pipelined-before-direct ordering across an embargo is observable. Returns
  # the counter value at the time of the call (0 for the first invocation).
  getNumber @0 () -> (n :UInt32);
}

interface Reflector {
  # The caller passes its own CallSequence as `target`. The reflector imports and
  # retains it, exports an unresolved PROMISE capability, and returns it as
  # `promise` — but does NOT resolve it yet. The caller pipelines getNumber calls
  # on `promise` (which park at the reflector), then calls `resolveNow` to make
  # the reflector resolve the promise to `target`. That reflected resolution
  # forwards the parked calls back to `target` and triggers the caller's
  # senderLoopback -> receiverLoopback Disembargo before it issues a direct call.
  reflect @0 (target :CallSequence) -> (promise :CallSequence);

  # Resolve the promise returned by the most recent reflect() to that call's
  # target. Split from reflect() so the caller can guarantee its pipelined calls
  # are parked on the still-unresolved promise before resolution fires.
  resolveNow @1 () -> ();

  # cap-in-params (SERVER-invokes-caller-cap direction). The caller hands the
  # reflector one of its OWN CallSequence capabilities as `cb`. The reflector
  # INVOKES it itself — cb.getNumber() — rather than reflecting it back, and
  # returns the observed counter value as `observed`. The caller asserts the
  # server-observed value matches what its own CallSequence returned, proving a
  # distinct client-supplied cap was invoked server-side.
  invokeCap @2 (cb :CallSequence) -> (observed :UInt32);

  # disconnect-mid-call. The reflector's handler closes its OWN transport, so the
  # caller's outstanding call to disconnectNow() returns a DISCONNECT-class error
  # instead of a normal result. The caller must issue this LAST (the connection
  # dies once the handler fires) and assert it specifically observes a
  # disconnect-class error, not a blanket failure.
  disconnectNow @3 () -> (unused :UInt32);
}
