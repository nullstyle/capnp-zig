@0xcbed42fd50dcaa72;
struct Link(T) {
  value @0 :T;
  next @1 :Link(T);
  children @2 :List(Link(T));
}
struct Root { head @0 :Link(Text); }
