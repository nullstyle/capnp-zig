# RPC Promises

Promise and pipelining primitives shared by the peer runtime:

- `promised_answer.zig`: owned promised-answer state, transform copying, and
  transform traversal utilities.
- `pending_calls.zig`: pending call queues and replay after promise resolution.
- `return_routing.zig`: `sendResultsToYourself` / `sendResultsToThirdParty`
  routing state helpers.
- `return_send.zig`: return frame delivery helpers and outbound return cap-ref
  tracking.
- `pipeline.zig`, `peer_promises.zig`, `promised_answer_copy.zig`, and
  `return_send_helpers.zig`: compatibility facades for older internal imports.
