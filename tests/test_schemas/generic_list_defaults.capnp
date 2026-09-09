@0xeb449ce886d0aeab;
struct Box(T) { value @0 :T; }
struct Root {
  boxes @0 :List(Box(Text)) = [(value = "seed")];
  union {
    none @1 :Void;
    choices @2 :List(Box(Text));
  }
}
