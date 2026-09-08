@0x95951f9becc17ea9;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("feature_helper_names");
using Go = import "/go.capnp";
$Go.package("helpernames");
$Go.import("capnpc-wasm/features/helpernames");

# These legal schema names also name generated Zig helper declarations.
struct WhichTag {
  union {
    none @0 :Void;
    number @1 :UInt32;
  }
}

struct EnumOrdinals {
  value @0 :State;
  enum State {
    idle @0;
    active @1;
  }
}

struct NestedLists {
  values @0 :List(List(UInt32));
}

struct PointerKinds {
  value @0 :AnyStruct;
}

struct Payload {
  number @0 :UInt32;
  text @1 :Text;
}

struct GroupViews {
  value @0 :State;
  enum State {
    zero @0;
    one @1;
  }
  enumOrdinals :group {
    number @1 :UInt32;
  }
}
