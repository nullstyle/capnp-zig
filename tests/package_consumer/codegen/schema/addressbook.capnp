@0xc9b7d1173c087ea8;

# Schema for the package-preflight codegen consumer. Its compiled
# CodeGeneratorRequest is checked in next to it (addressbook.request.bin), so
# the clean-room consumer needs no schema compiler: `just gen` refreshes it
# with the pinned compiler and `just check-generated` fails on drift.

struct AddressBook {
  people @0 :List(Person);
}

struct Person {
  id @0 :UInt32;
  name @1 :Text;
  email @2 :Text;
  phones @3 :List(PhoneNumber);
  avatar @4 :Data;

  union {
    unemployed @5 :Void;
    employer @6 :Text;
    school @7 :Text;
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
