@0xe8f4c3a2b197d065;

# Version 2 of the checked-in schema-evolution fixture. Existing declarations
# intentionally retain v1's explicit IDs and field ordinals.

enum Status @0xbeb8f82eb34b9733 {
  pending @0;
  ready @1;
  future @2;
}

struct Empty @0x91d8e76572d56f19 {}

struct Child @0xd89dc40de9055f70 {
  value @0 :UInt32;
}

interface Service @0xe2cb7f14f734a5b9 {
  ping @0 () -> ();
}

struct Evolution @0xa72c5f995baa981d {
  status @0 :Status = ready;
  statuses @1 :List(Status);

  text @2 :Text;
  data @3 :Data;
  child @4 :Child;
  children @5 :List(Child);
  any @6 :AnyPointer;
  service @7 :Service;
  defaultText @8 :Text = "fallback";
  empty @9 :Empty;

  union {
    none @10 :Void;
    label @11 :Text;
    details :group {
      note @12 :Text;
      state @13 :Status = ready;
    }
    futureLabel @14 :Text;
  }

  # This slot does not exist in a v1 allocation. A v2 reader over v1 bytes
  # must report it absent rather than confusing the default value with presence.
  addedText @15 :Text;
}
