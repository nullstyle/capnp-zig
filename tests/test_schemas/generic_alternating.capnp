@0x934c15e7c63f92ae;
struct Alternating(A, B) {
  value @0 :A;
  next @1 :Alternating(B, A);
}
struct Root { head @0 :Alternating(Text, Data); }
