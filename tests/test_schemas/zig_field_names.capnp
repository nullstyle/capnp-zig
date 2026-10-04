@0xd2a7c4b19e3f5a61;

# Schema names that are Zig primitives (void, bool, type, u8) or keywords
# (error), used where generated code reads them as field names: union tags and
# their enum literals, VTable fields, and the typed ServerAdapter's vtable
# literal. zig fmt unquotes a primitive name in those positions but keeps a
# keyword quoted, so the generator must do the same for its output to stay fmt
# clean. (Schema enumerants are capitalized, so Kind only feeds the method.)

enum Kind {
  void @0;
  type @1;
  bool @2;
  error @3;
}

struct Slot {
  kind @0 :Kind;
  union {
    void @1 :Void;
    bool @2 :Bool;
    u8 @3 :UInt8;
    error @4 :Text;
  }
}

interface Probe {
  void @0 () -> ();
  type @1 (kind :Kind) -> (kind :Kind);
}
