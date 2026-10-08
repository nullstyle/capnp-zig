@0x8184fd32ded3382c;

# Field shapes for docs/getting-started-serialization.md that the address
# book does not have.

struct Profile {
  scores @0 :List(UInt32);
  tags @1 :List(Text);
  colors @2 :List(Color);
  matrix @3 :List(List(UInt16));
  address @4 :Address;
  values @5 :AnyList;
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

struct Shape {
  color @0 :Color;

  union {
    circle @1 :Float64; # radius
    rectangle :group {
      width @2 :Float32;
      height @3 :Float32;
    }
  }
}
