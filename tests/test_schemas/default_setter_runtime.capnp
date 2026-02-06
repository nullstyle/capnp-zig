@0xad706ce7fcecd192;

enum Color {
  red @0;
  green @1;
  blue @2;
}

struct DefaultSetterDemo {
  flag @0 :Bool = true;
  count @1 :UInt16 = 0x1234;
  delta @2 :Int32 = -17;
  ratio @3 :Float32 = 1.25;
  scale @4 :Float64 = -2.5;
  color @5 :Color = green;
}
