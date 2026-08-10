@0xe8f4c3a2b197d065;

# Version 1 of the checked-in schema-evolution fixture. Keep this file's IDs,
# existing field ordinals, and declaration order synchronized with v2: the two
# generated modules intentionally describe different revisions of the same
# wire protocol.

enum Status @0xbeb8f82eb34b9733 {
  pending @0;
  ready @1;
}

struct Empty @0x91d8e76572d56f19 {}

struct Child @0xd89dc40de9055f70 {
  value @0 :UInt32;
}

interface Service @0xe2cb7f14f734a5b9 {
  ping @0 () -> ();
}

struct Evolution @0xa72c5f995baa981d {
  # Non-zero enum defaults prove that raw ordinal access applies the same XOR
  # transformation as the typed accessors.
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
  }
}
