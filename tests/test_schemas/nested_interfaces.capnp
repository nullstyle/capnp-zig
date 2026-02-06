@0x9a3d06b5c4b2a9f1;

interface Outer {
  interface Inner {
    ping @0 (value :UInt32) -> (value :UInt32);
  }

  getInner @0 () -> (inner :Inner);
}
