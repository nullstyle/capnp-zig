@0xa1b2c3d4e5f60009;

# Capability pass-back and pipelined-params scenarios.
#
# Two e2e scenarios share this schema; the server is the same TokenHost for
# both, and the client picks the flow:
#
#   pass_back         The client passes back a Token it imported from the
#                     server (a `receiverHosted` descriptor) in check()'s
#                     params, and the server must receive its OWN Token. The
#                     client also hands the server one of its own Tokens in
#                     echo(), and must receive its own Token back in the
#                     results. A capnp-zig client exports Tokens of its own
#                     first, so its export ids collide with the import ids it
#                     passes back.
#   pipelined_params  The client passes the `token` result of an unanswered
#                     mint() call as check()'s param (a `receiverAnswer`
#                     descriptor). The server must resolve it to its own Token.
#                     The same pipelined into check() from mintFail() must
#                     fail with mintFail()'s exception.

interface Token {
  # The tag the Token was minted (or built) with. The host counts every call
  # that reaches one of its own Tokens, so check() can tell whether a call
  # reached the host's own object.
  tag @0 () -> (tag :UInt32);
}

interface TokenHost {
  # Mint a fresh host-owned Token carrying `tag`.
  mint @0 (tag :UInt32) -> (token :Token);

  # Call token.tag() and report what came back, and whether that call reached
  # one of the host's own Tokens (`local`).
  check @1 (token :Token) -> (tag :UInt32, local :Bool);

  # Return `token` unchanged.
  echo @2 (token :Token) -> (token :Token);

  # Always fail with an exception whose reason contains
  # "deliberate mint failure"; a capability pipelined on `token` is broken.
  mintFail @3 (tag :UInt32) -> (token :Token);
}
