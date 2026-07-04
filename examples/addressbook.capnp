@0xf02316ceb4253eb9;

# Schema for the standalone serialization example (examples/serialization_demo.zig).
# Kept deliberately small while still exercising the interesting wire-format
# paths: nested structs, an enum, lists (of struct), an (unnamed) union, and
# both Text and Data fields.

struct AddressBook {
  people @0 :List(Person);
}

struct Person {
  id @0 :UInt32;
  name @1 :Text;
  email @2 :Text;
  phones @3 :List(PhoneNumber);
  # Raw bytes — e.g. a tiny avatar thumbnail. Exercises the Data path.
  avatar @4 :Data;

  # Exactly one employment status is active at a time (unnamed union).
  union {
    unemployed @5 :Void;
    employer @6 :Text;
    school @7 :Text;
    selfEmployed @8 :Void;
  }

  struct PhoneNumber {
    number @0 :Text;
    type @1 :PhoneType;
  }

  enum PhoneType {
    mobile @0;
    home @1;
    work @2;
  }
}
