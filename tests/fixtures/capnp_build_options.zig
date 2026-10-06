//! The `capnp_build_options` module for compiler commands that pass
//! capnp-zig's modules by hand (`zig test -Mcapnpc-zig=src/lib.zig ...`):
//! the tests that compile generated code at run time, and the scripts under
//! `tools/`. `zig build` generates its own copy from `-Dfd-passing`; these
//! commands test code generation, not fd passing, so they keep the default.
pub const fd_passing: bool = true;
