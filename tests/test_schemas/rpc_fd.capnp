@0xba87f4531b1f434c;
# Fd passing between capnp-zig and the C++ reference (sprint item 15).
# The shape of `TestMoreStuff.writeToFd` in the reference's test.capnp, so
# the ported `rpc-twoparty-test.c++` cases run without generating all of it.

interface FdCap {
  # A capability that carries a file descriptor (`CapDescriptor.attachedFd`).
  # It has no methods: the fd that travels with it is the point.
}

interface FdTest {
  writeToFd @0 (fill :List(UInt8), fdCap1 :FdCap, fdCap2 :FdCap)
            -> (fdCap3 :FdCap, secondFdPresent :Bool);
  # `fdCap1` and `fdCap2` each carry a pipe's write end. The server writes
  # "foo" to the first and "bar" to the second, if it got one, and reports
  # whether it did. It also makes a pipe, writes "baz" to it, closes the
  # write end and returns the read end on `fdCap3`. `fill` pads the message,
  # so the fds ride on messages that take many reads.
}
