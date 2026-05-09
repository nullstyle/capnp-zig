const protocol = @import("../wire/protocol.zig");
const promise_pipeline = @import("../promises/pipeline.zig");

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
pub const OwnedPromisedAnswer = promise_pipeline.OwnedPromisedAnswer;

/// Walk a promised-answer transform path through a results payload to find
/// the referenced capability.
pub fn resolvePromisedAnswer(
    payload: protocol.Payload,
    transform: protocol.PromisedAnswerTransform,
) !ResolvedCap {
    const resolved = try promise_pipeline.resolvePromisedAnswer(payload, transform);
    return switch (resolved) {
        .none => .none,
        .exported_id => |id| .{ .exported = .{ .id = id } },
        .imported_id => |id| .{ .imported = .{ .id = id } },
        .promised => |promised| .{ .promised = promised },
    };
}
