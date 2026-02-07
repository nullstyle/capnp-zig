// Library exports for capnpc-zig
pub const message = @import("message.zig");
pub const schema = @import("schema.zig");
pub const reader = @import("reader.zig");
pub const codegen = @import("capnpc-zig/generator.zig");
pub const request = @import("request_reader.zig");
pub const schema_validation = @import("schema_validation.zig");
pub const rpc = @import("rpc/mod.zig");

test {
    @import("std").testing.refAllDecls(@This());
    _ = @import("rpc/connection.zig");
    _ = @import("rpc/transport_xev.zig");
}
