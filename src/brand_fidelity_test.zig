//! Test root for shared brand eligibility/accounting internals.
//!
//! Keeping this root under `src/` lets the internal module import the schema
//! resolver without exposing either implementation detail through `lib.zig`.
comptime {
    _ = @import("capnpc-zig/brand_fidelity.zig");
}
