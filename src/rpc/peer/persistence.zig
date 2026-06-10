//! Sturdy-ref persistence helpers (Cap'n Proto RPC level 2).
//!
//! `Persistent.save()` is an ordinary interface call defined by the vendored
//! `persistent.capnp`; the `SturdyRef` and `Owner` type parameters are
//! realm-defined. This module pins down the capnp-zig realm conventions and
//! provides the pure payload builders/parsers used by `Peer`:
//!
//! * A sturdy ref is an opaque byte string supplied by the application. On
//!   the wire it travels as a `Data` pointer in the `SaveResults.sturdyRef`
//!   `AnyPointer` slot.
//! * Restore is vat-specific per the spec (the old `Restore` message is
//!   obsolete). capnp-zig serves restore as a call on the bootstrap
//!   capability using the vat-level convention below:
//!
//! ```capnp
//! @0xeb0d831668c521e8;
//! interface Restorer @0xac47e3f6453b50f3 {
//!   restore @0 (sturdyRef :Data) -> (cap :Capability);
//! }
//! ```
//!
//! Peer-facing orchestration (persistent-export registry, restorer hook,
//! `sendSave`/`sendRestore`) lives in `peer/mod.zig`.

const protocol = @import("../wire/protocol.zig");
const message = @import("../../serialization/message.zig");

/// `Persistent` interface ID from the vendored `persistent.capnp`.
pub const persistent_interface_id: u64 = 0xc8cb212fcd9f5691;
/// `Persistent.save` method ordinal.
pub const save_method_id: u16 = 0;

/// capnp-zig vat-level `Restorer` convention interface ID (see module doc).
pub const restorer_interface_id: u64 = 0xac47e3f6453b50f3;
/// `Restorer.restore` method ordinal.
pub const restore_method_id: u16 = 0;

/// What a restorer hook resolved a sturdy ref to.
pub fn RestoreOutcome(comptime ExportType: type) type {
    return union(enum) {
        /// The sturdy ref is not recognized; the caller receives an
        /// "unknown sturdy ref" exception Return.
        unknown,
        /// Re-expose an already-registered export (shared across restores).
        existing: u32,
        /// Register a fresh export hosting the restored capability.
        host: ExportType,
    };
}

/// Read `SaveParams.sealFor` from an inbound save call payload. Returns
/// null when the params struct or the field is absent — sealing is
/// realm-defined and optional, so a missing owner is not an error.
pub fn readSealFor(params: protocol.Payload) ?message.AnyPointerReader {
    if (params.content.isNull()) return null;
    const params_struct = params.content.getStruct() catch return null;
    const seal_for = params_struct.readAnyPointer(0) catch return null;
    if (seal_for.isNull()) return null;
    return seal_for;
}

/// Read the sturdy-ref bytes from an inbound restore call payload
/// (`restore(sturdyRef :Data)` per the vat convention).
pub fn readSturdyRefParam(params: protocol.Payload) ![]const u8 {
    if (params.content.isNull()) return error.MissingSturdyRef;
    const params_struct = params.content.getStruct() catch return error.MalformedSturdyRef;
    const field = params_struct.readAnyPointer(0) catch return error.MissingSturdyRef;
    if (field.isNull()) return error.MissingSturdyRef;
    return field.getData() catch return error.MalformedSturdyRef;
}

/// Read the sturdy-ref bytes from a `SaveResults` payload. The bytes are
/// borrowed from the underlying frame.
pub fn readSturdyRefResult(payload: protocol.Payload) ![]const u8 {
    if (payload.content.isNull()) return error.MissingSturdyRef;
    const results_struct = payload.content.getStruct() catch return error.MalformedSturdyRef;
    const field = results_struct.readAnyPointer(0) catch return error.MissingSturdyRef;
    if (field.isNull()) return error.MissingSturdyRef;
    return field.getData() catch return error.MalformedSturdyRef;
}

/// Write empty `SaveParams` (sealFor = null) into an outbound save call.
pub fn writeEmptySaveParams(call: *protocol.CallBuilder) !void {
    var payload = try call.payloadTyped();
    const any = try payload.initContent();
    _ = try any.initStruct(0, 1);
}

/// Write restore params (`sturdyRef` as a `Data` pointer) into an outbound
/// restore call. The bytes are copied into the frame immediately.
pub fn writeRestoreParams(call: *protocol.CallBuilder, sturdy_ref: []const u8) !void {
    var payload = try call.payloadTyped();
    const any = try payload.initContent();
    const params = try any.initStruct(0, 1);
    const field = try params.getAnyPointer(0);
    try field.setData(sturdy_ref);
}

/// `ReturnBuildFn` context carrying the sturdy-ref payload for a save Return.
pub const SturdyRefReturnContext = struct {
    sturdy_ref: []const u8,
};

/// Build `SaveResults { sturdyRef = Data }` into a Return under
/// `Peer.sendReturnResults` (the cap table is encoded by the caller).
pub fn buildSaveResultsReturn(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
    const ctx: *const SturdyRefReturnContext = @ptrCast(@alignCast(ctx_ptr));
    var payload = try ret.payloadTyped();
    const any = try payload.initContent();
    const results = try any.initStruct(0, 1);
    const field = try results.getAnyPointer(0);
    try field.setData(ctx.sturdy_ref);
}

/// `ReturnBuildFn` context carrying the capability for a restore Return.
pub const CapabilityReturnContext = struct {
    cap_id: u32,
};

/// Build `(cap :Capability)` results pointing at a local export. The raw
/// export ID written here is rewritten into a cap-table index (with a
/// `senderHosted` descriptor) by `Peer.sendReturnResults`.
pub fn buildCapabilityReturn(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
    const ctx: *const CapabilityReturnContext = @ptrCast(@alignCast(ctx_ptr));
    var payload = try ret.payloadTyped();
    const any = try payload.initContent();
    const results = try any.initStruct(0, 1);
    const field = try results.getAnyPointer(0);
    try field.setCapability(.{ .id = ctx.cap_id });
}
