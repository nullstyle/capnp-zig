@0xa1b2c3d4e5f60789;

# BUG 2 fixture: an interface that redeclares a method whose name collides with
# an inherited method. This is legal Cap'n Proto (method names are scoped
# per-interface), but the generator emits `callPing`/VTable fields for both the
# own and the inherited method into the same Zig namespaces. The plugin must
# reject this with a clean error.DuplicateGeneratedName rather than emitting
# uncompilable Zig.

interface Base {
  ping @0 () -> ();
}

interface Derived extends(Base) {
  ping @0 () -> ();
}
