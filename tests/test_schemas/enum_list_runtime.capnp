@0x8f63df8f53ff9830;

enum Color {
  red @0;
  green @1;
  blue @2;
}

struct EnumListDemo {
  colors @0 :List(Color);
}
