@0xa1b2c3d4e5f6000a;

# Cancellation scenarios: the caller sends Finish before the callee answers.
#
# Two e2e scenarios share this schema; the server is the same CancelHost for
# both, and the client picks the flow:
#
#   cancel_id_reuse   The client calls slow(), drops it (Finish), then calls
#                     fast() several times, one at a time. The server answers
#                     slow() with Return{canceled} only after the Finish, so
#                     the client can then reuse slow()'s question id. Each
#                     fast() must get its own `n` back, and one of them must
#                     arrive on slow()'s reused id (`answerId`).
#   cancel_pipelined  The client calls getSub(), sends bar() on the Sub that
#                     getSub() will return, then drops getSub() (Finish). The
#                     server holds getSub()'s answer past that Finish. bar()
#                     is pipelined on the answer, so the callee must not
#                     cancel it: bar() must return its results.
#
# A server for these scenarios holds the answers to slow() and getSub(), and
# answers a Finish for a call it has not answered with Return{canceled}.
# cancel_id_reuse does not test a late answer to slow(): the server never
# sends one.

interface Sub {
  # Always returns `value = 4242`.
  bar @0 () -> (value :UInt32);
}

interface CancelHost {
  # Never answers on its own. When the caller finishes the call first, the
  # server answers it with Return{canceled}.
  slow @0 () -> (marker :UInt32);

  # Return `n` at once. `answerId` is the question id the call arrived on.
  # `canceled` counts the calls this connection answered with
  # Return{canceled} so far, and `lastCanceledId` is the answer id of the
  # last one (0 when `canceled` is 0).
  fast @1 (n :UInt32) -> (n :UInt32, answerId :UInt32, canceled :UInt32, lastCanceledId :UInt32);

  # Return a new Sub. The server holds this answer for at least 500 ms
  # first, so a Finish that the caller sends right after its call arrives
  # before the answer. It answers only after that Finish: with no Finish
  # within 5 s, it answers with an exception, because the scenario then
  # tests nothing.
  getSub @2 () -> (sub :Sub);
}
