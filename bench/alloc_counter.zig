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

/// `CountingAllocator` for an allocator used from several threads at once.
///
/// The RPC benchmarks need it: a TCP connection frees written frames on its
/// writer thread, and the server side of each benchmark runs on its own
/// thread, so plain `+=` counters would race (and under-count). Counters
/// are monotonic atomics: each one is exact, but a snapshot taken while
/// another thread is mid-call is not a consistent cut across counters. The
/// benchmarks only divide window totals by thousands of calls, where a
/// boundary straddle is far below the gate tolerance.
///
/// Give each side its own instance (client, server) so the two threads do
/// not contend for one cache line on every allocation.
pub const SharedCountingAllocator = struct {
    backing: std.mem.Allocator,
    alloc_calls: std.atomic.Value(usize) = .init(0),
    resize_calls: std.atomic.Value(usize) = .init(0),
    remap_calls: std.atomic.Value(usize) = .init(0),
    free_calls: std.atomic.Value(usize) = .init(0),
    allocated_bytes: std.atomic.Value(usize) = .init(0),
    freed_bytes: std.atomic.Value(usize) = .init(0),

    const vtable = std.mem.Allocator.VTable{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    pub fn init(backing: std.mem.Allocator) SharedCountingAllocator {
        return .{ .backing = backing };
    }

    pub fn allocator(self: *SharedCountingAllocator) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn snapshot(self: *const SharedCountingAllocator) Snapshot {
        return .{
            .alloc_calls = self.alloc_calls.load(.monotonic),
            .resize_calls = self.resize_calls.load(.monotonic),
            .remap_calls = self.remap_calls.load(.monotonic),
            .free_calls = self.free_calls.load(.monotonic),
            .allocated_bytes = self.allocated_bytes.load(.monotonic),
            .freed_bytes = self.freed_bytes.load(.monotonic),
        };
    }

    fn bump(counter: *std.atomic.Value(usize), n: usize) void {
        _ = counter.fetchAdd(n, .monotonic);
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *SharedCountingAllocator = @ptrCast(@alignCast(ctx));
        bump(&self.alloc_calls, 1);
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        bump(&self.allocated_bytes, len);
        return ptr;
    }

    fn resize(
        ctx: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
    ) bool {
        const self: *SharedCountingAllocator = @ptrCast(@alignCast(ctx));
        bump(&self.resize_calls, 1);
        const resized = self.backing.rawResize(memory, alignment, new_len, ret_addr);
        if (resized) {
            if (new_len > memory.len) {
                bump(&self.allocated_bytes, new_len - memory.len);
            } else {
                bump(&self.freed_bytes, memory.len - new_len);
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
        const self: *SharedCountingAllocator = @ptrCast(@alignCast(ctx));
        bump(&self.remap_calls, 1);
        const remapped = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        if (new_len > memory.len) {
            bump(&self.allocated_bytes, new_len - memory.len);
        } else {
            bump(&self.freed_bytes, memory.len - new_len);
        }
        return remapped;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *SharedCountingAllocator = @ptrCast(@alignCast(ctx));
        bump(&self.free_calls, 1);
        bump(&self.freed_bytes, memory.len);
        self.backing.rawFree(memory, alignment, ret_addr);
    }
};

/// Allocation activity over a measured window, per unit of work. Shared by
/// the RPC benchmarks so `alloc_count_per_call` / `alloc_bytes_per_call`
/// mean the same thing in every JSON line that carries them.
pub const WindowAllocs = struct {
    /// Allocation calls in the window (`alloc` only; a successful in-place
    /// growth is a `resize`/`remap`, counted in bytes but not here).
    alloc_count: usize = 0,
    /// Bytes handed out in the window: fresh allocations plus in-place
    /// growth. Frees are not subtracted; this is churn, not residency.
    alloc_bytes: usize = 0,

    pub fn between(before: Snapshot, after: Snapshot) WindowAllocs {
        return .{
            .alloc_count = after.alloc_calls - before.alloc_calls,
            .alloc_bytes = after.allocated_bytes - before.allocated_bytes,
        };
    }

    pub fn plus(a: WindowAllocs, b: WindowAllocs) WindowAllocs {
        return .{
            .alloc_count = a.alloc_count + b.alloc_count,
            .alloc_bytes = a.alloc_bytes + b.alloc_bytes,
        };
    }

    pub fn countPer(self: WindowAllocs, units: usize) f64 {
        if (units == 0) return 0;
        return @as(f64, @floatFromInt(self.alloc_count)) / @as(f64, @floatFromInt(units));
    }

    pub fn bytesPer(self: WindowAllocs, units: usize) f64 {
        if (units == 0) return 0;
        return @as(f64, @floatFromInt(self.alloc_bytes)) / @as(f64, @floatFromInt(units));
    }
};
