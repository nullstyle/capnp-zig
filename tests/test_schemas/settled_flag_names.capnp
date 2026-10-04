@0xd3a9c4e81f2b7650;

# A file-scope declaration named `flag` (here an annotation) once collided
# with the `|flag|` capture in generated call-return code, so any schema with
# both a `flag` and an interface produced a binding that did not compile
# ("capture shadows declaration of 'flag'"). `push` covers the streaming
# call-return and context-teardown paths, which used the same capture.

using import "/capnp/stream.capnp".StreamResult;

annotation flag(*) :Bool;

interface Pinger $flag(true) {
  ping @0 () -> (count :UInt32);
  push @1 (n :UInt32) -> stream;
}
