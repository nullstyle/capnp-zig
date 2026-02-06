@0xcd7f79c9d3e64921;

interface Service {}

struct Child {
  value @0 :UInt16;
}

struct ListWrapperDemo {
  children @0 :List(Child);
  dataItems @1 :List(Data);
  services @2 :List(Service);
}
