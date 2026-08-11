@0xbad6c6ad74a132e1;

struct Box(T) {
  annotation branded(struct) :T;
  value @0 :T;
}

struct Annotated $Box(Text).branded("scoped") {}

struct Child {
  value @0 :UInt32;
}

struct Outer(T) {
  struct Inner(U) {
    outer @0 :T;
    inner @1 :U;
  }
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
  }


  defaultBox @12 :Box(Child) = (value = (value = 77));
  lexicalBox @13 :Outer(Text).Inner(Data);
}

interface GenericMethods {
  call @0 [T] (value :T) -> (echo :T);
}

interface GenericBase(T) {}

interface BrandedDerived extends(GenericBase(Text)) {}

interface OuterGeneric(T) {
  call @0 (value :T) -> (echo :T);
}
