@0x9cc05c61e24a6ec1;

struct Scalars {
  nothing @0 :Void;
  boolean @1 :Bool = true;
  signed8 @2 :Int8 = -12;
  signed16 @3 :Int16 = -1234;
  signed32 @4 :Int32 = -123456;
  signed64 @5 :Int64 = -123456789012345;
  unsigned8 @6 :UInt8 = 240;
  unsigned16 @7 :UInt16 = 60000;
  unsigned32 @8 :UInt32 = 4000000000;
  unsigned64 @9 :UInt64 = 18000000000000000000;
  real32 @10 :Float32 = 1.25;
  real64 @11 :Float64 = -2.5;
  booleans @12 :List(Bool);
  signed @13 :List(Int16);
  reals @14 :List(Float64);
  texts @15 :List(Text);
  blobs @16 :List(Data);
  nested @17 :List(List(UInt32));
  choices @18 :List(Choice);
  service @19 :Lookup;
  enum Choice { first @0; second @1; }
}

interface Lookup {
  find @0 (name :Text) -> (value :Scalars);
}

struct Evolution {
  records @0 :List(Entry);
  marker @1 :Text;
  nested @2 :List(List(Entry));
  single @3 :Entry;
  struct Entry {
    value @0 :UInt64;
    extra @1 :UInt64;
    label @2 :Text;
    note @3 :Text;
  }
}
