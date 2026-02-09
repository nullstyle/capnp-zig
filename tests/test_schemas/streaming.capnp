@0xc1d4e5f6a7b8c9d0;

using import "/capnp/stream.capnp".StreamResult;

interface TestStreaming {
  doStreamI @0 (i :UInt32) -> stream;
  doStreamJ @1 (j :UInt32) -> stream;
  finishStream @2 () -> (totalI :UInt32, totalJ :UInt32);
}
