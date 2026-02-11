// Core library exports without the xev transport/runtime surface.
pub const message = @import("serialization/message.zig");
pub const schema = @import("serialization/schema.zig");
pub const reader = @import("serialization/reader.zig");
pub const codegen = @import("capnpc-zig/generator.zig");
pub const request = @import("serialization/request_reader.zig");
pub const schema_validation = @import("serialization/schema_validation.zig");
pub const rpc = @import("rpc/mod_core.zig");

test {
    @import("std").testing.refAllDecls(@This());
}
