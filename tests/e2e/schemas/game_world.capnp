@0xa1b2c3d4e5f60002;

using import "game_types.capnp".PlayerId;
using import "game_types.capnp".Position;
using import "game_types.capnp".Vector3;
using import "game_types.capnp".Faction;
using import "game_types.capnp".StatusCode;

# GameWorld service: entity management (spawn, move, query).
# Exercises: basic CRUD RPCs, structs, lists, enums, unions.

struct EntityId {
  id @0 :UInt64;
}

enum EntityKind {
  player @0;
  npc @1;
  monster @2;
  projectile @3;
}

struct Entity {
  id @0 :EntityId;
  kind @1 :EntityKind;
  name @2 :Text;
  position @3 :Position;
  health @4 :Int32;
  maxHealth @5 :Int32;
  faction @6 :Faction;
  alive @7 :Bool = true;
}

struct SpawnRequest {
  kind @0 :EntityKind;
  name @1 :Text;
  position @2 :Position;
  faction @3 :Faction;
  maxHealth @4 :Int32 = 100;
}

struct AreaQuery {
  center @0 :Position;
  radius @1 :Float32;
  filter :union {
    all @2 :Void;
    byKind @3 :EntityKind;
    byFaction @4 :Faction;
  }
}

interface GameWorld {
  # Spawn a new entity in the world and return it.
  spawnEntity @0 (request :SpawnRequest) -> (entity :Entity, status :StatusCode);

  # Remove an entity from the world.
  despawnEntity @1 (id :EntityId) -> (status :StatusCode);

  # Get a single entity by ID.
  getEntity @2 (id :EntityId) -> (entity :Entity, status :StatusCode);

  # Move an entity to a new position.
  moveEntity @3 (id :EntityId, newPosition :Position) -> (entity :Entity, status :StatusCode);

  # Deal damage to an entity. Returns updated entity (may be dead).
  damageEntity @4 (id :EntityId, amount :Int32) -> (entity :Entity, killed :Bool, status :StatusCode);

  # Query entities within an area.
  queryArea @5 (query :AreaQuery) -> (entities :List(Entity), count :UInt32);
}
