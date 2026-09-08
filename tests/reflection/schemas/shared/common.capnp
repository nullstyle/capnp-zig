@0xe270d888fb3c3218;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("feature_test");
using Go = import "/go.capnp";
$Go.package("shared");
$Go.import("capnpc-wasm/features/shared");

annotation note(file, struct, field, const) :Text;
$note("shared declarations reached through relative imports");

struct Record $note("struct annotation") {
  label @0 :Text = "default 🦀";
  mode @1 :Mode = archived;
  enum Mode {
    active @0;
    archived @1;
  }
}

const sample :Record = (label = "constant é", mode = active);
