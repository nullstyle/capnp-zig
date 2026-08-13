const base = @import("./mod_base.zig");

pub const wire = base.wire;
pub const caps = base.caps;
pub const promises = base.promises;
pub const events = base.events;
pub const time = base.time;
pub const transport = base.transport;
pub const peer = base.peer;
pub const vat = base.vat;
pub const integration = base.integration;
pub const generated = base.generated;
pub const testing = base.testing;

// Force the whole RPC subtree through semantic analysis so its `test` blocks
// are part of the compilation.
//
// Without this, `zig build test` compiled ZERO of the 179 `test` blocks under
// `src/rpc`. The re-exports above hand back namespace *structs*
// (`mod_base.wire` and friends); Zig analyses a container's declarations
// lazily, so nothing ever forced the files behind them to be analysed, and a
// file that is never analysed contributes no tests. Proven with a canary: a
// `test { try std.testing.expect(false); }` added to `src/rpc/peer/mod.zig`
// left `zig build test` exiting 0 without ever mentioning it.
//
// `std.testing.refAllDecls` is not enough on its own -- it stops at those
// namespace structs, and the recursive variant it used to pair with no longer
// exists in std. Hence the bounded walk below. The depth cap is what keeps a
// self-referential declaration (`pub const Self = @This()`) from recursing
// forever; it only needs to cover mod -> namespace -> file -> nested namespace.
test {
    base.refAllRecursive(@This(), 4, true);
}
