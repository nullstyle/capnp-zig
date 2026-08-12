@0xd7bbfd88b7e6de93;

struct Box(T) {
  value @0 :T;
}

struct Root {
  direct @0 :Box(Text);
  nested @1 :Box(List(Box(Text)));
  deep @2 :Box(List(List(Box(Text))));
}
