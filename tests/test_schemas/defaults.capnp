@0xd4bca4f77c7b0b7f;

struct Inner {
  id @0 :UInt32;
  label @1 :Text;
}

enum Color {
  red @0;
  green @1;
  blue @2;
}

struct Widget {
  id @0 :UInt32 = 123;
  flag @1 :Bool = true;
  name @2 :Text = "widget";
  data @3 :Data = 0x"010203";
  nums @4 :List(UInt16) = [1, 2, 3];
  inner @5 :Inner = (id = 42, label = "inner");
  color @6 :Color = green;
}

const magicNumber :UInt32 = 987;
const magicData :Data = 0x"0a0b0c";
const magicList :List(UInt16) = [7, 8, 9];
const magicInner :Inner = (id = 7, label = "const");
const magicColor :Color = blue;
