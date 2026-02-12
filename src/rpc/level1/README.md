# RPC Level 1

Standalone Cap'n Proto RPC level-1 modules (promise and pipelining primitives):

- `peer_promises.zig`: pending call queues and replay after promise resolution.
- `peer_return_send_helpers.zig`: return routing helpers used by pipelined/forwarded returns.

Shared promised-answer utilities live in `src/rpc/common/`.
