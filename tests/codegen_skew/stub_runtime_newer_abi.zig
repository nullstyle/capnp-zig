//! Stub capnpc-zig runtime that has dropped support for the codegen ABI the
//! plugin emits today. The generated guard must ask for regeneration with the
//! runtime's own release instead of failing inside the file.

pub const codegen_abi = struct {
    pub const version: u32 = 1000;
    pub const oldest_supported: u32 = 999;
    pub const release: []const u8 = "99.0.0";
};
