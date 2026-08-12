@0xbad6c6ad74a132e1;

struct Box(T) {
  annotation branded(struct) :T;
  value @0 :T;
}

struct Annotated $Box(Text).branded("scoped") {}

struct Child {
  value @0 :UInt32;
}

enum Color {
  red @0;
  green @1;
  blue @2;
}

struct Outer(T) {
  struct Inner(U) {
    outer @0 :T;
    inner @1 :U;
  }
}

struct Envelope(T) {
  struct Inner {
    inherited @0 :T;
  }
  inner @0 :Inner;
}

interface Service {
  ping @0 () -> ();
}

struct Fidelity {
  any @0 :AnyPointer;
  anyStruct @1 :AnyStruct;
  anyList @2 :AnyList;
  capability @3 :Capability;

  # Deliberately matches the target type name after Zig capitalization. The
  # generated Brands.Box wrapper must still refer to the file-level Box type.
  box @4 :Box(Text);
  childBox @5 :Box(Child);
  listBox @6 :Box(List(UInt32));
  unboundBox @7 :Box;

  union {
    unionStruct @8 :AnyStruct;
    unionList @9 :AnyList;
  }

  grouped :group {
    groupList @10 :AnyList;
    groupBox @11 :Box(Text);
    groupDefaultBox @23 :Box(Child) = (value = (value = 91));
  }


  defaultBox @12 :Box(Child) = (value = (value = 77));
  lexicalBox @13 :Outer(Text).Inner(Data);
  textListBox @14 :Box(List(Text));
  enumListBox @15 :Box(List(Color));
  structListBox @16 :Box(List(Child));
  deepListBox @17 :Box(List(List(UInt16)));
  serviceBox @18 :Box(Service);
  nestedBox @19 :Box(Box(Text));
  inheritedEnvelope @20 :Envelope(Text);
  dataListBox @21 :Box(List(Data));
  serviceListBox @22 :Box(List(Service));
  nestedStructListBox @24 :Box(List(Box(Text)));
  deepNestedStructListBox @25 :Box(List(List(Box(Text))));
  inheritedStructListBox @26 :Box(List(Outer(Text).Inner(Data)));
}

interface GenericMethods {
  call @0 [T] (value :T) -> (echo :T);
}

interface GenericBase(T) {}

interface BrandedDerived extends(GenericBase(Text)) {}

interface OuterGeneric(T) {
  call @0 (value :T) -> (echo :T);
}
