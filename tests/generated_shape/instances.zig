//! Generic instances for the generated-shape walk (tools/generated_shape.zig).
//!
//! A generic schema type renders as `Apply(comptime bindings: anytype)`, whose
//! members exist only once it is instantiated, so the walker cannot see them
//! on its own. Each declaration here instantiates one, and the walker renders
//! its members under `<profile>.instances.<name>`. Instance lines are
//! Experimental: the Stable families of the gate cover the generated
//! declarations themselves.
//!
//! Each profile namespace may only use that profile's corpus modules
//! (`shape-<profile>-<request>`), so a type name maps back to one profile.
//!
//! Bindings are struct codecs over another instance. A generic type declares
//! members that need more than a `Text` or `Data` binding has: the walker
//! resolves every signature, and `Pipeline.getValue` returns `T.Pipeline`,
//! which `capnp.generic.Text` does not declare. A struct codec over an
//! instance declares everything (`Reader`, `Builder`, `Pipeline`, `init`).

const capnp = @import("capnpc-zig");
const Text = capnp.generic.Text;

pub const full = struct {
    const pingpong = @import("shape-full-pingpong").pingpong;
    const generic_rpc = @import("shape-full-generic_rpc").generic_rpc;
    const generic_collections = @import("shape-full-generic_collections").generic_collections;

    /// `Box(Text)`, used only as a binding (its own `Pipeline` would not
    /// resolve), wrapped as a struct codec: `value` and `next` are pointers.
    const BoxOfText = capnp.generic.Struct(generic_rpc.Box.Apply(.{ .T = Text }), 0, 2);

    /// A non-generic interface's `Apply`.
    pub const pingpong_PingPong = pingpong.PingPong.Apply(.{});
    /// A generic struct, bound to a struct.
    pub const generic_rpc_Box_BoxOfText = generic_rpc.Box.Apply(.{ .T = BoxOfText });
    /// A generic interface, bound to a struct.
    pub const generic_rpc_Service_BoxOfText = generic_rpc.Service.Apply(.{ .T = BoxOfText });
    /// A generic method (`identity @1 [T]`) inside a non-generic interface.
    pub const generic_rpc_Factory = generic_rpc.Factory.Apply(.{});
    pub const generic_rpc_Factory_Identity_BoxOfText = generic_rpc.Factory.Apply(.{}).Identity.Apply(.{ .T = BoxOfText });
    /// A generic struct from another schema.
    pub const generic_collections_Box_BoxOfText = generic_collections.Box.Apply(.{ .T = BoxOfText });
};

pub const compact = struct {
    const generic_rpc = @import("shape-compact-generic_rpc").generic_rpc;
    const BoxOfText = capnp.generic.Struct(generic_rpc.Box.Apply(.{ .T = Text }), 0, 2);

    pub const generic_rpc_Service_BoxOfText = generic_rpc.Service.Apply(.{ .T = BoxOfText });
};
