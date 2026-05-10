const std = @import("std");
const message = @import("../../serialization/message.zig");
const protocol = @import("../wire/protocol.zig");
const descriptors = @import("descriptors.zig");
const lifecycle = @import("lifecycle.zig");

pub const CapTable = lifecycle.CapTable;
pub const ResolvedCap = descriptors.ResolvedCap;
pub const max_table_size = lifecycle.max_table_size;

/// Resolved capability table for an inbound Call or Return payload.
///
/// Created by decoding the cap descriptors from the wire message and
/// resolving each against the connection's `CapTable`. Tracks which entries
/// have been retained (referenced) so unused imports can be released.
pub const InboundCapTable = struct {
    allocator: std.mem.Allocator,
    entries: []ResolvedCap,
    retained: []bool,

    pub fn init(
        allocator: std.mem.Allocator,
        list_opt: ?message.StructListReader,
        table: *CapTable,
    ) !InboundCapTable {
        const list = list_opt orelse {
            const entries = try allocator.alloc(ResolvedCap, 0);
            errdefer allocator.free(entries);
            const retained = try allocator.alloc(bool, 0);
            return .{
                .allocator = allocator,
                .entries = entries,
                .retained = retained,
            };
        };

        const count = list.len();
        if (count > max_table_size) return error.CapTableFull;
        var entries = try allocator.alloc(ResolvedCap, count);
        errdefer allocator.free(entries);
        const retained = try allocator.alloc(bool, count);
        errdefer allocator.free(retained);
        @memset(retained, false);
        var processed: u32 = 0;
        errdefer {
            // Roll back noteImport calls for already-processed entries.
            for (entries[0..processed]) |entry| {
                if (entry == .imported) {
                    _ = table.releaseImport(entry.imported.id);
                }
            }
        }
        while (processed < count) : (processed += 1) {
            const reader = try list.get(processed);
            const descriptor = try protocol.CapDescriptor.fromReader(reader);
            entries[processed] = try resolveDescriptor(table, descriptor);
        }

        return .{
            .allocator = allocator,
            .entries = entries,
            .retained = retained,
        };
    }

    pub fn deinit(self: *InboundCapTable) void {
        self.allocator.free(self.entries);
        self.allocator.free(self.retained);
    }

    pub fn len(self: *const InboundCapTable) u32 {
        return @as(u32, @intCast(self.entries.len));
    }

    pub fn get(self: *const InboundCapTable, index: u32) !ResolvedCap {
        if (index >= self.entries.len) return error.CapabilityIndexOutOfBounds;
        return self.entries[index];
    }

    pub fn retainIndex(self: *InboundCapTable, index: u32) !void {
        if (index >= self.entries.len) return error.CapabilityIndexOutOfBounds;
        self.retained[index] = true;
    }

    pub fn retainCapability(self: *InboundCapTable, cap: message.Capability) !void {
        try self.retainIndex(cap.id);
    }

    pub fn isRetained(self: *const InboundCapTable, index: u32) bool {
        if (index >= self.retained.len) return false;
        return self.retained[index];
    }

    pub fn resolveCapability(self: *const InboundCapTable, cap: message.Capability) !ResolvedCap {
        return self.get(cap.id);
    }

    /// Create an independent deep copy that owns its own slices.
    ///
    /// This clone only duplicates the local slices; it does not increment
    /// import reference counts in any `CapTable`. Callers must not pass the
    /// clone to `releaseInboundCaps` unless they have separately balanced
    /// import ownership.
    pub fn clone(self: *const InboundCapTable) !InboundCapTable {
        const entries = try self.allocator.dupe(ResolvedCap, self.entries);
        errdefer self.allocator.free(entries);
        const retained = try self.allocator.dupe(bool, self.retained);
        return .{
            .allocator = self.allocator,
            .entries = entries,
            .retained = retained,
        };
    }
};

fn resolveDescriptor(table: *CapTable, descriptor: protocol.CapDescriptor) !ResolvedCap {
    return switch (descriptor.tag) {
        .none => .none,
        .senderHosted, .senderPromise => {
            const id = descriptor.id orelse return error.MissingCapDescriptorId;
            try table.noteImport(id);
            return .{ .imported = .{ .id = id } };
        },
        .receiverHosted => {
            const id = descriptor.id orelse return error.MissingCapDescriptorId;
            if (!table.hasExport(id)) return error.UnknownExport;
            return .{ .exported = .{ .id = id } };
        },
        .receiverAnswer => {
            const promised = descriptor.promised_answer orelse return error.MissingPromisedAnswer;
            return .{ .promised = promised };
        },
        .thirdPartyHosted => {
            const third = descriptor.third_party orelse return error.MissingThirdPartyCapDescriptor;
            try table.noteImport(third.vine_id);
            return .{ .imported = .{ .id = third.vine_id } };
        },
    };
}

/// Resolve a cap descriptor from the wire format into a `ResolvedCap`,
/// noting any new imports in the cap table.
pub fn resolveCapDescriptor(table: *CapTable, descriptor: protocol.CapDescriptor) !ResolvedCap {
    return resolveDescriptor(table, descriptor);
}
