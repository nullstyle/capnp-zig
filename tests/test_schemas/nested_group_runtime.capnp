@0xd4c9b8f2e1a30567;

# BUG 1 fixture: a group nested inside a union group (Circle inside U), plus a
# plain nested group (Inner inside Outer). Both nested groups were previously
# dropped entirely by codegen.

struct Shape {
  u :union {
    circle :group {
      radius @0 :Float64;
      filled @1 :Bool;
    }
    square @2 :Float64;
  }
  outer :group {
    label @3 :Text;
    inner :group {
      value @4 :UInt32;
    }
  }
}
