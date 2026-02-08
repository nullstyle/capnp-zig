const std = @import("std");

pub const Snapshot = struct {
    alloc_calls: usize,
    resize_calls: usize,
    remap_calls: usize,
    free_calls: usize,
    allocated_bytes: usize,
    freed_bytes: usize,
};

pub const CountingAllocator = struct {
    backing: std.mem.Allocator,
    alloc_calls: usize = 0,
    resize_calls: usize = 0,
    remap_calls: usize = 0,
    free_calls: usize = 0,
    allocated_bytes: usize = 0,
    freed_bytes: usize = 0,

    const vtable = std.mem.Allocator.VTable{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    pub fn init(backing: std.mem.Allocator) CountingAllocator {
        return .{
            .backing = backing,
        };
    }

    pub fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn snapshot(self: *const CountingAllocator) Snapshot {
        return .{
            .alloc_calls = self.alloc_calls,
            .resize_calls = self.resize_calls,
            .remap_calls = self.remap_calls,
            .free_calls = self.free_calls,
            .allocated_bytes = self.allocated_bytes,
            .freed_bytes = self.freed_bytes,
        };
    }

    pub fn deltaSince(self: *const CountingAllocator, before: Snapshot) Snapshot {
        const after = self.snapshot();
        return .{
            .alloc_calls = after.alloc_calls - before.alloc_calls,
            .resize_calls = after.resize_calls - before.resize_calls,
            .remap_calls = after.remap_calls - before.remap_calls,
            .free_calls = after.free_calls - before.free_calls,
            .allocated_bytes = after.allocated_bytes - before.allocated_bytes,
            .freed_bytes = after.freed_bytes - before.freed_bytes,
        };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.alloc_calls += 1;
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.allocated_bytes +|= len;
        return ptr;
    }

    fn resize(
        ctx: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
    ) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.resize_calls += 1;
        const resized = self.backing.rawResize(memory, alignment, new_len, ret_addr);
        if (resized) {
            if (new_len > memory.len) {
                self.allocated_bytes +|= new_len - memory.len;
            } else {
                self.freed_bytes +|= memory.len - new_len;
            }
        }
        return resized;
    }

    fn remap(
        ctx: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
    ) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.remap_calls += 1;
        const remapped = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        if (new_len > memory.len) {
            self.allocated_bytes +|= new_len - memory.len;
        } else {
            self.freed_bytes +|= memory.len - new_len;
        }
        return remapped;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.free_calls += 1;
        self.freed_bytes +|= memory.len;
        self.backing.rawFree(memory, alignment, ret_addr);
    }
};
