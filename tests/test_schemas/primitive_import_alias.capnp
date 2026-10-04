@0xd8e4a1c7b2f05e93;

# Uses types from type.capnp, whose import alias is the Zig primitive name
# `type`. Typed applications (`Holder.Apply`, `Service.Apply`) anchor those
# references at the generated file namespace, so the generator must spell the
# alias `_capnp_file.type`, not `_capnp_file.@"type"`, to stay zig fmt clean.

using T = import "type.capnp";

struct Holder {
  thing @0 :T.Thing;
  things @1 :List(T.Thing);
  colors @2 :List(T.Color);
  box @3 :T.Box(T.Thing);
  inner @4 :T.Thing.Inner;
  svc @5 :T.Svc;
}

interface Service extends(T.Svc) {
  direct @0 T.Thing -> T.Thing;
}
