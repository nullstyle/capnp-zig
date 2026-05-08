# RPC Promises

Promise and pipelining primitives shared by the peer runtime:

- `peer_promises.zig`: pending call queues and replay after promise resolution.
- `return_send_helpers.zig`: return routing helpers used by pipelined/forwarded returns.
- `pipeline.zig`: owned promised-answer state and transform traversal utilities.
- `promised_answer_copy.zig`: deep-copy helper for promised-answer descriptors.
