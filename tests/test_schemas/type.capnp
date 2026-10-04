@0xc5b0e9f2a4d17e31;

# Imported by primitive_import_alias.capnp. The file name gives the importing
# binding a file-scope alias named after a Zig primitive, declared as
# `pub const @"type"`. Behind the `_capnp_file.` anchor that alias is a field
# access, where zig fmt unquotes it to `_capnp_file.type`.

struct Thing {
  x @0 :UInt8;

  struct Inner {
    y @0 :UInt16;
  }
}

enum Color {
  red @0;
  green @1;
}

struct Box(T) {
  value @0 :T;
}

interface Svc {
  ping @0 (thing :Thing) -> (thing :Thing);
}
