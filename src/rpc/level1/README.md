# RPC Level 1

Standalone Cap'n Proto RPC level-1 modules (promise and pipelining primitives):

- `promised_answer_copy.zig`: cloned promised-answer op slices from reader-backed transforms.
- `promise_pipeline.zig`: owned promised-answer state + transform traversal resolver.
- `peer_promises.zig`: pending call queues and replay after promise resolution.
- `peer_return_send_helpers.zig`: return routing helpers used by pipelined/forwarded returns.
