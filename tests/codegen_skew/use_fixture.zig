//! Compile root for the plugin/runtime skew check (`test-codegen-skew`).
//!
//! It imports a freshly generated binding as `fixture` while `capnpc-zig` is a
//! stub runtime, and forces the binding's `Builder` layout, which needs
//! `message.StructBuilder` and therefore the guarded `capnpc` binding. The
//! build step expects exactly one compile error: the guard's message.

const fixture = @import("fixture");

comptime {
    _ = @sizeOf(fixture.Person.Builder);
}
