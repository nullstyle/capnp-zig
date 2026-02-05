@0x9d3cfb9d6a82b3b1;

struct Point {
  x @0 :UInt32;
  y @1 :UInt32;
}

struct Widget {
  id @0 :UInt32;
  name @1 :Text;
  points @2 :List(Point);
  tags @3 :List(Text);
  bytes @4 :Data;
  u16s @5 :List(UInt16);
  u32s @6 :List(UInt32);
  u64s @7 :List(UInt64);
  bools @8 :List(Bool);
  f32s @9 :List(Float32);
  f64s @10 :List(Float64);
  u16Lists @11 :List(List(UInt16));
}
