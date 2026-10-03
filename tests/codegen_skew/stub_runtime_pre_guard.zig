//! Stub capnpc-zig runtime from before the codegen ABI guard existed (0.18.0
//! and earlier): it has no `codegen_abi` declaration at all. The generated
//! guard must treat that as ABI 0 rather than fail on the missing member.

pub const message = struct {};
pub const schema = struct {};
