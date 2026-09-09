@0xbbd5e2fd41f0175c;

struct Box(T) {
  value @0 :T;
  next @1 :Box(T);
}

interface Service(T) {
  echo @0 (value :T) -> (value :T);
  getBox @1 () -> (box :Box(T));
}

interface Factory {
  getService @0 () -> (box :Box(Service(Text)));
  identity @1 [T] (value :T) -> (value :T);
}

using External = import "generic_rpc_external.capnp";
interface TextChild extends(External.Middle(Text)) {}
interface DataChild extends(External.Middle(Data)) {}
interface Left extends(External.Parent(Text)) {}
interface Right extends(External.Parent(Text)) {}
interface Diamond extends(Left, Right) {}
struct Defaults {
  union {
    choice @0 :Box(Text) = (value = "typed default");
    plain @1 :Text = "literal default";
  }
}
interface ConflictText extends(External.Parent(Text)) {}
interface ConflictData extends(External.Parent(Data)) {}
interface Conflicting extends(ConflictText, ConflictData) {}
interface NamedMethods {
  identity @0 [U] Box(U) -> Box(U);
}

struct Constraints(T) {
  union {
    record @0 :AnyStruct;
    other @1 :T;
  }
}

interface PlainService { echo @0 (value :Text) -> (value :Text); }
struct PlainHolder { capability @0 :PlainService; }
interface HolderFactory { get @0 () -> (box :Box(PlainHolder)); }
