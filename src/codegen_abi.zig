//! The contract between generated code and the capnpc-zig runtime it imports.
//!
//! Every file the plugin generates resolves its `capnpc` binding through a
//! comptime check against these declarations. When the plugin that generated a
//! file is newer than the runtime a consumer pins (or the runtime has dropped
//! support for an old generated shape), compilation stops with one readable
//! error that names the required version, instead of many errors deep inside
//! the generated file.
//!
//! The check uses `@hasDecl`, which only sees `pub` declarations, so these
//! must stay `pub` and stay reachable as `capnpc.codegen_abi` from every
//! library root (`lib.zig`, `lib_core.zig`, `lib_quic.zig`).
//!
//! Bump `version` and set `release` whenever generated code starts depending
//! on a runtime declaration that an older runtime lacks. Raise
//! `oldest_supported` only when the runtime stops compiling an older generated
//! shape.

/// The generated-code ABI this runtime implements. The plugin built from the
/// same tree stamps this value into every file it generates.
pub const version: u32 = 1;

/// The oldest generated-code ABI this runtime still compiles.
pub const oldest_supported: u32 = 1;

/// The first capnpc-zig release whose runtime implements `version`.
pub const release: []const u8 = "0.19.0";
