@0x9892b7577e71da4b;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("feature_test");
using Go = import "/go.capnp";
$Go.package("features");
$Go.import("capnpc-wasm/features");
using Shared = import "shared/common.capnp";

const rawBytes :Data = embed "assets/bytes.bin";
const textFile :Text = embed "assets/unicode.txt";
const unsignedMaximum :UInt64 = 0xffffffffffffffff;
const signedMinimum :Int64 = -0x8000000000000000;
const signedMaximum :Int64 = 0x7fffffffffffffff;

struct Values $Shared.note("binary embeds, integer limits, groups, and defaults") {
  raw @0 :Data = .rawBytes;
  text @1 :Text = .textFile;
  inlineData @2 :Data = 0x"00 7f 80 ff 00";
  inlineText @3 :Text = "NUL:\x00; café 🦀\n";
  high @4 :UInt64 = .unsignedMaximum;
  low @5 :Int64 = .signedMinimum;
  highSigned @6 :Int64 = .signedMaximum;
  numbers @7 :List(UInt64) = [0, 0x8000000000000000, 0xffffffffffffffff];
  record @8 :Shared.Record = Shared.sample;
  records @9 :List(Shared.Record) = [(label = "first"), (mode = active)];
  details :group {
    enabled @10 :Bool = true;
    mode @11 :Shared.Record.Mode = archived;
  }
  union {
    none @12 :Void;
    selected :group {
      name @13 :Text;
      payload @14 :Data;
    }
  }
  tagged @15 :Text = "tagged" $Shared.note("field annotation");
}
