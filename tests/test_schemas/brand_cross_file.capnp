@0xabfefeb049054398;

using Imported = import "brand_imported.capnp";

struct Root {
  foreignBox @0 :Imported.Box(List(Imported.Child));
}
