@0xaf116dde00565943;
interface Leaf { ping @0 () -> (); }
interface First { ping @0 () -> (value :UInt32, service :Leaf); }
interface Second { ping @0 () -> (value :UInt32, service :Leaf); }
interface Both extends(First, Second) {}
interface Own extends(First, Second) { ping @0 () -> (value :UInt32); }
interface Left extends(First) {}
interface Right extends(First) {}
interface Diamond extends(Left, Right) {}
struct A { interface B @0xca1fd246754ef5bd { ping @0 () -> (); } }
interface AB @0xd6c9ac3ac16b809b { ping @0 () -> (); }
interface Fallback extends(A.B, AB) {}
using External = import "rpc_inherited_external.capnp";
interface Imported extends(First, External.First) {}

# The inherited qualified name also reserves its generated WithOptions family.
interface CompanionBase @0xcdcde10962f5eb1f {
  ping @0 () -> (service :Leaf);
}
interface Companions extends(CompanionBase) {
  ping @0 ();
  pingFromCompanionBaseWithOptions @1 ();
}

interface FamilyOnly extends(CompanionBase) {
  pingWithOptions @0 ();
}
interface CompanionBaseWithOptions @0xbeb4094e5bcb2ad7 {
  ping @0 () -> (service :Leaf);
}
interface FamilyNames extends(CompanionBase, CompanionBaseWithOptions) {}
