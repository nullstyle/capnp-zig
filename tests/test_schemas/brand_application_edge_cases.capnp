@0xb91098d224cb9ccd;

struct Box(T) {
  value @0 :T;
}

enum Color {
  red @0;
  green @1;
}

struct Child {
  value @0 :Text;
}

struct ChildHolder {
  child @0 :Child;
}

struct Collision(A, B) {
  foo @0 :A;
  fooElement @1 :B;
}

struct Defaults(T) {
  child @0 :Box(Child) = (value = (value = "nested-default"));
  grouped :group {
    groupChild @1 :Box(Child) = (value = (value = "group-default"));
  }
}

struct Grouped(T) {
  grouped :group {
    box @0 :Box(T);
  }
  union {
    choiceGroup :group {
      choice @1 :Box(T);
    }
    other @2 :Void;
  }
}

struct Root {
  box @0 :Box(Text);
  color @1 :Box(List(Color));
  child @2 :Box(List(Child));
  collision @3 :Collision(List(Box(Text)), Box(Text));
  defaults @4 :Defaults(Data);
  grouped @5 :Grouped(Text);
}
