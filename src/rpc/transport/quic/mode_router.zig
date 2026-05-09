const std = @import("std");
const quic_zig = @import("quic_zig");

const baseline_engine = @import("baseline_engine.zig");
const engine_owner = @import("engine_owner.zig");
const endpoint_mod = @import("endpoint.zig");
const native_engine = @import("native_engine.zig");
const quic_options = @import("options.zig");

const BaselineEngine = baseline_engine.BaselineEngine;
const EngineOwner = engine_owner.Owner;
const NativeEngine = native_engine.NativeEngine;
const Role = endpoint_mod.Role;
const TransportMode = quic_options.TransportMode;

pub const Router = struct {
    mode: TransportMode,
    baseline: *BaselineEngine,
    native: *NativeEngine,

    pub fn enqueue(
        self: Router,
        allocator: std.mem.Allocator,
        frame: []const u8,
    ) !void {
        switch (self.mode) {
            .baseline => try self.baseline.enqueue(allocator, frame),
            .native => try self.native.enqueue(allocator, frame),
        }
    }

    pub fn outboundEmpty(self: Router) bool {
        return switch (self.mode) {
            .baseline => self.baseline.outboundEmpty(),
            .native => self.native.outboundEmpty(),
        };
    }

    pub fn hasImmediateWork(
        self: Router,
        role: Role,
        conn: *quic_zig.Connection,
    ) bool {
        return switch (self.mode) {
            .baseline => self.baseline.hasImmediateWork(role, conn),
            .native => self.native.hasImmediateWork(role, conn),
        };
    }

    pub fn service(
        self: Router,
        owner: EngineOwner,
        conn: *quic_zig.Connection,
    ) !void {
        switch (self.mode) {
            .baseline => try self.baseline.service(owner.baseline(), conn),
            .native => try self.native.service(owner.native(), conn),
        }
    }
};
