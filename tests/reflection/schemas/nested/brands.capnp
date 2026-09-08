@0xe371410cfa48dc65;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("feature_brands");
using Go = import "/go.capnp";
$Go.package("nested");
$Go.import("capnpc-wasm/features/nested");
using Shared = import "../shared/common.capnp";

struct Box(T) {
  value @0 :T;
  struct Pair(U) {
    first @0 :T;
    second @1 :U;
  }
}

using TextBox = Box(Text);

struct Pointers {
  any @0 :AnyPointer;
  structure @1 :AnyStruct;
  list @2 :AnyList;
}

const textList :List(Text) = ["first", "second"];
const typedPointers :Pointers = (
  any = Shared.sample,
  structure = Shared.sample,
  list = .textList
);

struct Brands $Shared.note("nested generic brands and AnyPointer values") {
  text @0 :TextBox = (value = "branded 🦀");
  nested @1 :Box(Box(Shared.Record)) = (value = (value = Shared.sample));
  pair @2 :Box(Text).Pair(Data) = (first = "outer", second = 0x"00 80 ff");
  unbound @3 :Box;
  pointers @4 :Pointers = .typedPointers;
}
