@0xcd6311052775fb75;

annotation note @0x8bcc6b3c927c67dd (struct, field, enum, enumerant, interface, method) :Text;
annotation flag @0xbaa0e38ccb5ab775 (field) :Bool;

struct Person $note("type") {
  id @0 :UInt32 $note("id") $flag(true);
}

enum Color $note("color") {
  red @0 $note("red");
  green @1;
}

interface Service $note("svc") {
  ping @0 () -> () $note("ping");
}
