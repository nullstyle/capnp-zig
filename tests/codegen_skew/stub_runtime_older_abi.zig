//! Stub capnpc-zig runtime that reports an older codegen ABI than the plugin
//! emits. It carries nothing else: a generated file must stop at its guard
//! before it reaches `message`, `schema` or any other runtime declaration.

pub const codegen_abi = struct {
    pub const version: u32 = 0;
    pub const oldest_supported: u32 = 0;
    pub const release: []const u8 = "0.0.0";
};
