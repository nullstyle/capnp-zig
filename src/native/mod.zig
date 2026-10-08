//! Native (Experimental): a sans-IO RPC connection and its C ABI, moved here
//! from capnp-swift's core (capnp-swift handoff H7).
//!
//! A `conn.Conn` is a detached `rpc.peer.Peer` plus a framer and an effect
//! queue: the host owns the socket, pushes received bytes in, drives time,
//! and drains effects (outbound frames, returns, inbound calls, dropped
//! exports, events). The core never calls the host.
//!
//! `abi` is the C ABI over it (`export fn capnp_*`), declared by
//! `include/capnp_core.h` in this directory. Its symbols are emitted only in
//! a compilation that references `native.abi`: a C or Swift host's library
//! root does `comptime { _ = capnp.native.abi; }`. Plain capnp-zig users get
//! no exported symbols. See docs/native-abi.md.

/// The sans-IO connection (Zig API).
pub const conn = @import("conn.zig");
/// The effect queue and the host-facing cap/effect types.
pub const effects = @import("effects.zig");
/// Capability remapping between host payloads and RPC payloads.
pub const cap_remap = @import("cap_remap.zig");
/// The C ABI (`capnp_core.h`). Referencing it emits the `capnp_*` symbols.
pub const abi = @import("abi.zig");
