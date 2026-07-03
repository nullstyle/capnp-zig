@0xb2c3d4e5f6a70891;

# BUG 3 fixture: reading a union member that is not currently selected must
# return error.WrongUnionMember instead of reinterpreting another variant's bits.

struct ShapeGuard {
  union {
    circle @0 :Float64;
    square @1 :Float64;
    empty @2 :Void;
  }
  area @3 :Float64;
}
