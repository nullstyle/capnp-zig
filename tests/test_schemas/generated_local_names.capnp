@0x89dc9fe13f41fe66;

# Declarations named like the locals, parameters and captures that generated
# code declares. Zig rejects a local that shadows a declaration of any
# enclosing container, and a schema decides some of those declaration names:
# its constants and annotations (at file scope, or nested in a struct or an
# interface), its type names, and the aliases of the files it imports. A file
# with `const ctx` beside any interface did not compile ("function parameter
# shadows declaration of 'ctx'").

using import "/capnp/stream.capnp".StreamResult;
using Imported = import "user_ctx.capnp";

const any :UInt32 = 1;
const bindings :UInt32 = 2;
const build :UInt32 = 3;
const builder :UInt32 = 4;
const c :UInt32 = 5;
const call :UInt32 = 6;
const callback :UInt32 = 7;
const cap :UInt32 = 8;
const caps :UInt32 = 9;
const client :UInt32 = 10;
const ctx :UInt32 = 11;
const dctx :UInt32 = 12;
const dead :UInt32 = 13;
const destination :UInt32 = 14;
const err :UInt32 = 15;
const ex :UInt32 = 16;
const handler :UInt32 = 17;
const handlers :UInt32 = 18;
const i :UInt32 = 19;
const imported :UInt32 = 20;
const index :UInt32 = 21;
const inner :UInt32 = 22;
const list :UInt32 = 23;
const msg :UInt32 = 24;
const ops :UInt32 = 25;
const options :UInt32 = 26;
const ordinal :UInt32 = 27;
const params :UInt32 = 28;
const path :UInt32 = 29;
const payload :UInt32 = 30;
const pointer :UInt32 = 31;
const qid :UInt32 = 32;
const r :UInt32 = 33;
const raw :UInt32 = 34;
const reader :UInt32 = 35;
const reason :UInt32 = 36;
const reservation :UInt32 = 37;
const resolved :UInt32 = 38;
const response :UInt32 = 39;
const result :UInt32 = 40;
const results :UInt32 = 41;
const root :UInt32 = 42;
const self :UInt32 = 43;
const sender :UInt32 = 44;
const server :UInt32 = 45;
const source :UInt32 = 46;
const storage :UInt32 = 47;
const stored :UInt32 = 48;
const token :UInt32 = 49;
const value :UInt32 = 50;

annotation peer(*) :Bool;
annotation ret(*) :Bool;
annotation settled(*) :Bool;

# Type names that match the generated type-valued locals.
struct Adapter {
  value @0 :UInt32;
}

struct Ancestor {
  value @0 :UInt32;
}

enum Color {
  red @0;
  green @1;
}

struct Box(T) {
  value @0 :T;
}

struct Holder $peer(true) {
  # Holder.Builder takes a `builder` parameter.
  const builder :UInt32 = 100;

  count @0 :UInt32 = 7;
  name @1 :Text = "holder";
  bytes @2 :Data;
  items @3 :List(UInt32);
  things @4 :List(Imported.Thing);
  thing @5 :Imported.Thing;
  color @6 :Color;
  pinger @7 :Pinger;
  box @8 :Box(Text);
  union {
    none @9 :Void;
    some @10 :UInt32;
  }
  info :group {
    tag @11 :Text;
  }
  boxes @12 :List(Box(Text));
  defaulted @13 :Box(Text) = (value = "typed default");
}

interface Base(T) {
  get @0 () -> (value :T);
}

interface Pinger extends(Base(Text)) $ret(true) {
  # Generated call-return code declares a `results` local.
  const results :UInt32 = 200;

  ping @0 (n :UInt32) -> (count :UInt32, holder :Holder);
  push @1 (n :UInt32) -> stream;
  child @2 () -> (pinger :Pinger);
  identity @3 [U] (value :U) -> (value :U);
}

struct Quiet $settled(true) {
  # No generated local in Quiet's scope is named `ctx`, so this one keeps its
  # name while the file-scope `ctx` cannot.
  const ctx :UInt32 = 300;
}
