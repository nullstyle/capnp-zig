//! Internal backing-memory budget, including arena capacity and headers.
const std = @import("std");
const Self = @This();

parent: std.mem.Allocator,
limit: usize,
used: usize = 0,
denied: bool = false,

pub fn allocator(self: *Self) std.mem.Allocator {
    return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
}

fn allowed(self: *Self, old_len: usize, new_len: usize) bool {
    self.denied = false;
    if (new_len > old_len and new_len - old_len > self.limit - self.used) {
        self.denied = true;
        return false;
    }
    return true;
}

fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
    const self: *Self = @ptrCast(@alignCast(ctx));
    if (!self.allowed(0, len)) return null;
    const ptr = self.parent.rawAlloc(len, alignment, ra) orelse return null;
    self.used += len;
    return ptr;
}

fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
    const self: *Self = @ptrCast(@alignCast(ctx));
    if (!self.allowed(memory.len, new_len)) return false;
    if (!self.parent.rawResize(memory, alignment, new_len, ra)) return false;
    self.used = self.used - memory.len + new_len;
    return true;
}

fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
    const self: *Self = @ptrCast(@alignCast(ctx));
    if (!self.allowed(memory.len, new_len)) return null;
    const ptr = self.parent.rawRemap(memory, alignment, new_len, ra) orelse return null;
    self.used = self.used - memory.len + new_len;
    return ptr;
}

fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
    const self: *Self = @ptrCast(@alignCast(ctx));
    self.parent.rawFree(memory, alignment, ra);
    self.used -= memory.len;
}
