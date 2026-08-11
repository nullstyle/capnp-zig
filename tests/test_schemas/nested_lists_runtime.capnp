@0xe7f2c0b9d4116a35;

enum Shade {
  red @0;
  green @1;
}

interface Service {}

struct Child {
  value @0 :UInt16;
}

struct NestedListDemo {
  numbers @0 :List(List(UInt16));
  deepText @1 :List(List(List(Text)));
  records @2 :List(List(Child));
  dataRows @3 :List(List(Data));
  enumRows @4 :List(List(Shade));
  serviceRows @5 :List(List(Service));
  voidRows @6 :List(List(Void));

  union {
    choiceRows @7 :List(List(UInt32));
    none @8 :Void;
  }

  grouped :group {
    groupRows @9 :List(List(Bool));
  }

  defaultRows @10 :List(List(UInt16)) = [[4, 5], []];
  i8Rows @11 :List(List(Int8));
  u8Rows @12 :List(List(UInt8));
  i16Rows @13 :List(List(Int16));
  i32Rows @14 :List(List(Int32));
  i64Rows @15 :List(List(Int64));
  u64Rows @16 :List(List(UInt64));
  f32Rows @17 :List(List(Float32));
  f64Rows @18 :List(List(Float64));
}
