@0x969663cf3bda0b98;

# File-scope declarations named like identifiers a generated file's header
# could introduce. Zig rejects a local that shadows a container-level
# declaration, so the codegen-ABI guard that resolves `capnpc` must not
# declare locals a schema can name. This file yields `pub const runtime`
# (a constant) and `pub const runtime_abi` (the import alias below).

using Abi = import "runtime_abi.capnp";

const runtime :UInt32 = 7;

struct Holder {
  thing @0 :Abi.Thing;
}
