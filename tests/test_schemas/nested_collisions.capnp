@0xf100000000000001;

# Same-simple-name nested types across two parents, and same-named methods
# across two interfaces. Legal Cap'n Proto; before parent-qualified codegen the
# generator aborted with error.DuplicateGeneratedName.

struct Outer1 {
    x @0 :Inner;
    struct Inner {
        a @0 :UInt32;
    }
    enum Color {
        red @0;
        green @1;
    }
}

struct Outer2 {
    y @0 :Inner;
    struct Inner {
        b @0 :Text;
    }
    enum Color {
        blue @0;
    }
}

interface Svc1 {
    doIt @0 () -> (result :Text);
}

interface Svc2 {
    doIt @0 () -> (result :UInt32);
}
