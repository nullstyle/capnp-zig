@0xd3a1c0ffeecafe27;

# Regression schema for the union-group stale-data bug (BUG 4). The union's
# scalar variants (`blob`, `scalar`) and the `dims` group share the same
# data/pointer storage. Selecting `dims` after a scalar variant was set must
# zero the group's own data words and null its pointer slots so its fields read
# back as defaults, not stale bits from the previously-selected variant.
struct Shape {
  name @0 :Text;
  union {
    blob @1 :UInt64;
    scalar @2 :Float64;
    dims :group {
      width @3 :Float32;
      height @4 :Float32;
      label @5 :Text;
      note @6 :Text;
    }
  }
}
