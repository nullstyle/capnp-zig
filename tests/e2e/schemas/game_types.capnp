@0xa1b2c3d4e5f60001;

# Shared types for the game-dev e2e RPC test suite.
# These types are imported by the service schemas.

struct PlayerId {
  id @0 :UInt64;
}

struct Position {
  x @0 :Float32;
  y @1 :Float32;
  z @2 :Float32;
}

struct Vector3 {
  x @0 :Float32;
  y @1 :Float32;
  z @2 :Float32;
}

enum Faction {
  neutral @0;
  alliance @1;
  horde @2;
  pirates @3;
}

enum Rarity {
  common @0;
  uncommon @1;
  rare @2;
  epic @3;
  legendary @4;
}

struct ItemId {
  id @0 :UInt64;
}

struct Item {
  id @0 :ItemId;
  name @1 :Text;
  rarity @2 :Rarity;
  level @3 :UInt16;
  stackSize @4 :UInt32 = 1;
  attributes @5 :List(Attribute);
}

struct Attribute {
  name @0 :Text;
  value @1 :Int32;
}

struct Timestamp {
  unixMillis @0 :Int64;
}

struct PlayerInfo {
  id @0 :PlayerId;
  name @1 :Text;
  faction @2 :Faction;
  level @3 :UInt16;
}

enum StatusCode {
  ok @0;
  notFound @1;
  permissionDenied @2;
  alreadyExists @3;
  invalidArgument @4;
  resourceExhausted @5;
}
