//! Root of a library that ships the native C ABI without libc and without a
//! `capnp_core_allocator`. abi.zig must refuse it at compile time: the only
//! allocator left would be `std.heap.page_allocator`, which maps at least a
//! page per allocation. `zig build test-native-abi` builds it and expects
//! exactly that compile error (build/native.zig).

const core = @import("capnpc-zig-core");

comptime {
    _ = core.native.abi;
}
