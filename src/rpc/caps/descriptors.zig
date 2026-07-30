const protocol = @import("../wire/protocol.zig");
const promised_answer = @import("../promises/promised_answer.zig");

/// An exported (local) capability referenced by ID.
pub const ExportCap = struct {
    id: u32,
};

/// An imported (remote) capability referenced by ID.
pub const ImportCap = struct {
    id: u32,
};

/// A capability reference resolved from a cap table entry to its
/// logical location: local export, remote import, promise pipeline, or absent.
pub const ResolvedCap = union(enum) {
    none,
    exported: ExportCap,
    imported: ImportCap,
    promised: protocol.PromisedAnswer,
};

/// A heap-owned copy of a `PromisedAnswer` (question ID + transform ops).
pub const OwnedPromisedAnswer = promised_answer.OwnedPromisedAnswer;

/// Map a capability's known `CapDescriptorTag` to the compact 4-bit origin code
/// carried through an in-builder origin-tagged capability pointer (see
/// `capability_remap.makeOriginTaggedCapabilityPointer`). Only the four
/// referable variants are ever tagged; the enum ordinals (senderHosted=1,
/// senderPromise=2, receiverHosted=3, receiverAnswer=4) fit in a `u4`.
pub fn originCodeForTag(tag: protocol.CapDescriptorTag) u4 {
    return @intCast(@backingInt(tag));
}

/// Recover the `CapDescriptorTag` from an origin code produced by
/// `originCodeForTag`. Rejects codes that do not name a referable capability
/// variant so a corrupt/hostile intermediate pointer fails closed rather than
/// resolving to an unexpected id space.
pub fn tagForOriginCode(code: u4) !protocol.CapDescriptorTag {
    return switch (code) {
        1 => .senderHosted,
        2 => .senderPromise,
        3 => .receiverHosted,
        4 => .receiverAnswer,
        else => error.InvalidCapabilityOrigin,
    };
}

/// Walk a promised-answer transform path through a results payload to find
/// the referenced capability.
pub fn resolvePromisedAnswer(
    payload: protocol.Payload,
    transform: protocol.PromisedAnswerTransform,
) !ResolvedCap {
    const resolved = try promised_answer.resolvePromisedAnswer(payload, transform);
    return switch (resolved) {
        .none => .none,
        .exported_id => |id| .{ .exported = .{ .id = id } },
        .imported_id => |id| .{ .imported = .{ .id = id } },
        .promised => |promised| .{ .promised = promised },
    };
}
