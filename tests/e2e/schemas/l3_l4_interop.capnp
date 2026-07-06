@0xa1b2c3d4e5f60008;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("e2e::l3l4");

# Test-only Level-3/Level-4 interop schema.
#
# The token structs are intentionally tiny and explicit so the Zig harness and
# the C++ reference harness can agree on the AnyPointer payloads used in
# Provide/Accept without depending on a production VatNetwork policy.

struct VatId {
  host @0 :Text;
}

struct ThirdPartyToContact {
  host @0 :Text;
  token @1 :UInt64;
  sentBy @2 :Text;
}

struct ThirdPartyToAwait {
  token @0 :UInt64;
}

struct ThirdPartyCompletion {
  token @0 :UInt64;
}

struct JoinKeyPart {
  joinId @0 :UInt32;
  partCount @1 :UInt16;
  partNum @2 :UInt16;
}

struct JoinResult {
  joinId @0 :UInt32;
  succeeded @1 :Bool;
  cap @2 :Capability;
}

interface Number {
  getNumber @0 () -> (n :UInt32);
}
