const base = @import("./mod_base.zig");

pub const wire = base.wire;
pub const caps = base.caps;
pub const promises = base.promises;
pub const events = base.events;
pub const time = base.time;
pub const transport = base.Transport(@import("./transport/quic/mod.zig"), true);
pub const peer = base.peer;
// Must mirror mod_base's namespace list: a QUIC-enabled build is the same RPC
// runtime with a different transport, so omitting a namespace here silently
// removes it from `-Dquic=true` consumers only. `vat` was missing, which made
// the Experimental L3/L4 addressing seams unreachable in QUIC builds.
pub const vat = base.vat;
pub const integration = base.integration;
pub const generated = base.generated;
pub const testing = base.testing;
