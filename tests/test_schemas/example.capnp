@0x9eb32e19f86ee174;

struct Person {
  name @0 :Text;
  age @1 :UInt32;
  email @2 :Text;
}

struct Address {
  street @0 :Text;
  city @1 :Text;
  zipCode @2 :UInt32;
}

enum Color {
  red @0;
  green @1;
  blue @2;
}
