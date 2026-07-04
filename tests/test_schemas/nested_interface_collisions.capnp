@0xf100000000000002;

# Same-simple-name nested INTERFACES across two parent interfaces. Legal Cap'n
# Proto. Before parent-qualified nested-interface codegen these both emitted a
# bare `Handle` at file scope, colliding (DuplicateGeneratedName / ambiguous
# generated symbols). Each nested interface also carries its own nested types
# and a method returning the sibling, to exercise self-reference qualification.

interface Alpha {
    interface Handle {
        struct Token {
            id @0 :UInt64;
        }
        ping @0 (token :Token) -> (ok :Bool);
    }
    open @0 () -> (handle :Handle);
}

interface Beta {
    interface Handle {
        struct Token {
            name @0 :Text;
        }
        pong @0 (token :Token) -> (count :UInt32);
    }
    open @0 () -> (handle :Handle);
}
