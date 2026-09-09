@0xffcbec81b13c9413;
interface Service { ping @0 () -> (); }
struct Holder {
  ignored @0 :Text;
  service @1 :Service;
  next @2 :Holder;
  details :group { nested @3 :Service; }
  union {
    absent @4 :Void;
    unsafe @5 :Service;
    selected :group { target @6 :Service; }
  }
}
interface Factory {
  direct @0 () -> (service :Service);
  nested @1 () -> (padding :Text, holder :Holder);
}
