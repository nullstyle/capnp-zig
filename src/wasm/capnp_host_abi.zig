const std = @import("std");
const builtin = @import("builtin");
const core = @import("capnpc-zig-core");
const generated_example = @import("generated/example.zig");

/// On freestanding targets (WASM), provide a no-op log function since there
/// is no stderr. On native targets, use the default log implementation.
pub const std_options: std.Options = .{
    .logFn = if (builtin.target.os.tag == .freestanding) noopLog else std.log.defaultLog,
};

fn noopLog(
    comptime _: std.log.Level,
    comptime _: @Type(.enum_literal),
    comptime _: []const u8,
    _: anytype,
) void {}

const log = std.log.scoped(.wasm_abi);

const HostPeer = core.rpc.host_peer.HostPeer;
const message = core.message;
const Peer = core.rpc.peer.Peer;
const protocol = core.rpc.protocol;
const cap_table = core.rpc.cap_table;

pub const AbiPtr = if (builtin.target.cpu.arch == .wasm32) u32 else usize;

// Thread-safety contract
//
// WASM linear memory is single-threaded: each WASM module instance executes on
// a single thread and has its own isolated linear memory, so concurrent host
// calls within a single instance cannot occur. On wasm32 targets, no
// synchronization is needed and none is provided.
//
// When compiled for native targets (used by the test harness and by hosts that
// embed this module as a native library), a mutex guards all mutable global
// state (`peers`, `next_peer_id`, and the error-reporting variables) so that
// concurrent calls from different host threads do not corrupt shared state.
//
// Callers on native targets may therefore invoke any exported `capnp_*`
// function from any thread. The mutex is coarse-grained: every exported
// function acquires it on entry and releases it on return.
const GlobalMutex = if (builtin.target.cpu.arch == .wasm32)
    // SAFETY: WASM linear memory is single-threaded; concurrent host calls
    // within a single module instance are undefined behavior per the WASM
    // spec. No synchronization is required.
    struct {
        pub fn lock(_: *@This()) void {}
        pub fn unlock(_: *@This()) void {}
    }
else
    std.Thread.Mutex;

var global_mutex: GlobalMutex = .{};
const allocator: std.mem.Allocator = if (builtin.target.cpu.arch == .wasm32)
    std.heap.wasm_allocator
else
    std.heap.page_allocator;
const ABI_VERSION: u32 = 1;
const ABI_MIN_VERSION: u32 = 1;
const ABI_MAX_VERSION: u32 = 1;
const FEATURE_ABI_RANGE: u64 = 1 << 0;
const FEATURE_ERROR_TAKE: u64 = 1 << 1;
const FEATURE_PEER_LIMITS: u64 = 1 << 2;
const FEATURE_HOST_CALL_BRIDGE: u64 = 1 << 3;
const FEATURE_LIFECYCLE_HELPERS: u64 = 1 << 4;
const FEATURE_SCHEMA_MANIFEST: u64 = 1 << 5;
const FEATURE_HOST_CALL_FRAME_RELEASE: u64 = 1 << 6;
const FEATURE_BOOTSTRAP_STUB_IDENTITY: u64 = 1 << 7;
const FEATURE_HOST_CALL_RETURN_FRAME: u64 = 1 << 8;
const ABI_FEATURE_FLAGS: u64 = FEATURE_ABI_RANGE |
    FEATURE_ERROR_TAKE |
    FEATURE_PEER_LIMITS |
    FEATURE_HOST_CALL_BRIDGE |
    FEATURE_LIFECYCLE_HELPERS |
    FEATURE_SCHEMA_MANIFEST |
    FEATURE_HOST_CALL_FRAME_RELEASE |
    FEATURE_BOOTSTRAP_STUB_IDENTITY |
    FEATURE_HOST_CALL_RETURN_FRAME;

const ERROR_ALLOC: u32 = 1;
const ERROR_INVALID_ARG: u32 = 2;
const ERROR_UNKNOWN_PEER: u32 = 3;
const ERROR_PEER_CREATE: u32 = 4;
const ERROR_PEER_PUSH: u32 = 5;
const ERROR_PEER_POP: u32 = 6;
const ERROR_SERDE_ENCODE: u32 = 7;
const ERROR_SERDE_DECODE: u32 = 8;
const ERROR_BOOTSTRAP_CONFIG: u32 = 9;
const ERROR_HOST_CALL: u32 = 10;
const ERROR_PEER_CONTROL: u32 = 11;

const PersonJson = struct {
    name: []const u8,
    age: u32,
    email: []const u8,
};

const PeerState = struct {
    const OUTGOING_SCRATCH_BYTES: usize = 1024 * 1024;

    outgoing_storage: [OUTGOING_SCRATCH_BYTES]u8 = undefined,
    outgoing_fba: std.heap.FixedBufferAllocator = undefined,
    host: HostPeer = undefined,
    last_popped: ?[]u8 = null,
    outstanding_host_call_frames: std.AutoHashMap(usize, u32) = undefined,
    bootstrap_stub_export_id: ?u32 = null,

    fn init(self: *PeerState) !void {
        self.outstanding_host_call_frames = std.AutoHashMap(usize, u32).init(allocator);
        self.outgoing_fba = std.heap.FixedBufferAllocator.init(&self.outgoing_storage);
        self.host = HostPeer.initWithOutgoingAllocator(allocator, self.outgoing_fba.allocator());
        errdefer self.host.deinit();
        errdefer self.outstanding_host_call_frames.deinit();
        // The WASM ABI's global mutex serializes all access, so thread
        // affinity checking is redundant. Disable it so that peers created
        // on one thread can be used from another without a debug panic.
        self.host.peer.disableThreadAffinity();
        self.host.start(null, null);
        try self.host.enableHostCallBridge();
        self.last_popped = null;
        self.bootstrap_stub_export_id = null;
    }

    fn deinit(self: *PeerState) void {
        if (self.last_popped) |frame| {
            self.host.freeFrame(frame);
            self.last_popped = null;
        }
        var outstanding_it = self.outstanding_host_call_frames.iterator();
        while (outstanding_it.next()) |entry| {
            const frame_ptr: [*]u8 = @ptrFromInt(entry.key_ptr.*);
            self.host.freeHostCallFrame(frame_ptr[0..entry.value_ptr.*]);
        }
        self.outstanding_host_call_frames.deinit();
        self.host.deinit();
    }

    fn resetScratchIfIdle(self: *PeerState) void {
        if (self.last_popped != null) return;
        if (self.host.pendingOutgoingCount() != 0) return;
        self.outgoing_fba.reset();
    }
};

const BootstrapStubHandler = struct {
    fn onCall(
        ctx: *anyopaque,
        called_peer: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        _ = ctx;
        _ = inbound_caps;
        try called_peer.sendReturnException(call.question_id, "bootstrap stub");
    }
};

var bootstrap_stub_ctx: u8 = 0;

var peers = std.AutoHashMapUnmanaged(u32, *PeerState){};
var next_peer_id: u32 = 1;

var last_error_code: u32 = 0;
var last_error_len: u32 = 0;
var last_error_buf: [1024]u8 = [_]u8{0} ** 1024;

// Note: clearErrorState, setError, and getPeerState are internal helpers.
// They do NOT acquire the mutex themselves; callers must hold global_mutex.

fn clearErrorState() void {
    last_error_code = 0;
    last_error_len = 0;
}

fn setError(code: u32, msg: []const u8) void {
    const n = @min(msg.len, last_error_buf.len);
    if (n > 0) {
        std.mem.copyForwards(u8, last_error_buf[0..n], msg[0..n]);
    }
    last_error_code = code;
    last_error_len = @intCast(n);
}

fn getPeerState(handle: u32) ?*PeerState {
    return peers.get(handle);
}

fn setHostCallReturnFrameError(err: anyerror) void {
    switch (err) {
        error.HostCallReturnNotReturn => setError(ERROR_HOST_CALL, "host-call return frame is not Return"),
        error.InvalidReturnSemantics => setError(ERROR_HOST_CALL, "host-call return frame has invalid Return semantics"),
        error.UnknownQuestion => setError(ERROR_HOST_CALL, "host-call return frame answerId is not pending"),
        else => setError(ERROR_HOST_CALL, @errorName(err)),
    }
}

fn ptrToAbi(ptr: usize) !AbiPtr {
    if (ptr > std.math.maxInt(AbiPtr)) return error.PointerOverflow;
    return @intCast(ptr);
}

fn asSlice(ptr: AbiPtr, len: u32) ![]const u8 {
    if (len == 0) return &.{};
    if (ptr == 0) return error.NullPointer;
    const start: usize = @intCast(ptr);
    const len_usize: usize = @intCast(len);
    const end = std.math.add(usize, start, len_usize) catch return error.LengthOverflow;
    if (end < start) return error.LengthOverflow;
    const p: [*]const u8 = @ptrFromInt(start);
    return p[0..len_usize];
}

fn writeU32(ptr: AbiPtr, value: u32) !void {
    if (ptr == 0) return error.NullPointer;
    const out: *align(1) u32 = @ptrFromInt(@as(usize, @intCast(ptr)));
    out.* = value;
}

fn writeU16(ptr: AbiPtr, value: u16) !void {
    if (ptr == 0) return error.NullPointer;
    const out: *align(1) u16 = @ptrFromInt(@as(usize, @intCast(ptr)));
    out.* = value;
}

fn writeU64(ptr: AbiPtr, value: u64) !void {
    if (ptr == 0) return error.NullPointer;
    const out: *align(1) u64 = @ptrFromInt(@as(usize, @intCast(ptr)));
    out.* = value;
}

fn writeAbiPtr(ptr: AbiPtr, value: AbiPtr) !void {
    if (ptr == 0) return error.NullPointer;
    const out: *align(1) AbiPtr = @ptrFromInt(@as(usize, @intCast(ptr)));
    out.* = value;
}

fn allocatePeerId() ?u32 {
    var attempts: usize = 0;
    while (attempts < std.math.maxInt(u32)) : (attempts += 1) {
        const candidate = next_peer_id;
        next_peer_id +%= 1;
        if (candidate == 0) continue;
        if (!peers.contains(candidate)) return candidate;
    }
    return null;
}

fn writeOutBuffer(out_ptr_ptr: AbiPtr, out_len_ptr: AbiPtr, bytes: []const u8) !void {
    if (out_ptr_ptr == 0 or out_len_ptr == 0) return error.NullPointer;
    if (bytes.len > std.math.maxInt(u32)) return error.LengthOverflow;
    const out_ptr: AbiPtr = if (bytes.len == 0) 0 else try ptrToAbi(@intFromPtr(bytes.ptr));
    try writeAbiPtr(out_ptr_ptr, out_ptr);
    try writeU32(out_len_ptr, @intCast(bytes.len));
}

fn clampU32(value: usize) u32 {
    return @intCast(@min(value, std.math.maxInt(u32)));
}

fn schemaManifestJsonBytes() []const u8 {
    if (@hasDecl(generated_example, "capnpSchemaManifestJson")) {
        return generated_example.capnpSchemaManifestJson();
    }
    if (@hasDecl(generated_example, "CAPNP_SCHEMA_MANIFEST_JSON")) {
        return generated_example.CAPNP_SCHEMA_MANIFEST_JSON;
    }
    return "{\"schema\":\"\",\"module\":\"\",\"serde\":[]}";
}

pub export fn capnp_wasm_abi_version() u32 {
    return ABI_VERSION;
}

/// Allocate `len` bytes from the module allocator.
/// Thread-safe on native targets (mutex-protected).
/// On wasm32, single-threaded by WASM spec.
pub export fn capnp_alloc(len: u32) AbiPtr {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();
    const size: usize = if (len == 0) 1 else @as(usize, len);
    const mem = allocator.alloc(u8, size) catch {
        setError(ERROR_ALLOC, "capnp_alloc out of memory");
        return 0;
    };
    return ptrToAbi(@intFromPtr(mem.ptr)) catch {
        allocator.free(mem);
        setError(ERROR_ALLOC, "capnp_alloc pointer overflow");
        return 0;
    };
}

/// Free a previously allocated buffer.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_free(ptr: AbiPtr, len: u32) void {
    global_mutex.lock();
    defer global_mutex.unlock();

    if (ptr == 0) return;
    const size: usize = if (len == 0) 1 else @as(usize, len);
    const mem: [*]u8 = @ptrFromInt(@as(usize, @intCast(ptr)));
    allocator.free(mem[0..size]);
}

/// Free a buffer returned by an ABI function (alias for capnp_free).
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_buf_free(ptr: AbiPtr, len: u32) void {
    // capnp_free acquires the mutex internally.
    capnp_free(ptr, len);
}

/// Return the current error code, or 0 if no error is pending.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_last_error_code() u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    return last_error_code;
}

/// Return a pointer to the error message buffer.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_last_error_ptr() AbiPtr {
    global_mutex.lock();
    defer global_mutex.unlock();

    return ptrToAbi(@intFromPtr(&last_error_buf)) catch 0;
}

/// Return the length of the current error message, or 0 if no error is pending.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_last_error_len() u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    return last_error_len;
}

/// Clear the current error state.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_clear_error() void {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();
}

pub export fn capnp_wasm_abi_min_version() u32 {
    return ABI_MIN_VERSION;
}

pub export fn capnp_wasm_abi_max_version() u32 {
    return ABI_MAX_VERSION;
}

pub export fn capnp_wasm_feature_flags_lo() u32 {
    return @truncate(ABI_FEATURE_FLAGS);
}

pub export fn capnp_wasm_feature_flags_hi() u32 {
    return @truncate(ABI_FEATURE_FLAGS >> 32);
}

/// Atomically drain and clear the current error state, writing the error
/// code, message pointer, and message length to the provided output locations.
/// Returns 1 if an error was present, 0 otherwise.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_error_take(out_code_ptr: AbiPtr, out_msg_ptr_ptr: AbiPtr, out_msg_len_ptr: AbiPtr) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    // Validate ALL output pointers before performing any writes, so that a
    // failure on the second or third pointer cannot overwrite the original
    // error state after the first pointer has already been written.
    if (out_code_ptr == 0 or out_msg_ptr_ptr == 0 or out_msg_len_ptr == 0) {
        setError(ERROR_INVALID_ARG, "output pointer is null");
        return 0;
    }

    // Verify each pointer is usable by checking alignment/validity upfront.
    // writeU32 and writeAbiPtr only fail on null, which is already checked
    // above. Collect all values before writing anything.
    const code = last_error_code;
    const len = last_error_len;
    const msg_ptr: AbiPtr = if (code == 0 or len == 0)
        0
    else
        ptrToAbi(@intFromPtr(&last_error_buf)) catch {
            setError(ERROR_INVALID_ARG, "error pointer overflow");
            return 0;
        };

    // All validation passed — perform writes. These cannot fail because
    // null pointers were rejected above.
    writeU32(out_code_ptr, code) catch unreachable;
    writeAbiPtr(out_msg_ptr_ptr, msg_ptr) catch unreachable;
    writeU32(out_msg_len_ptr, len) catch unreachable;

    clearErrorState();
    return if (code == 0) 0 else 1;
}

/// Create a new peer instance and return its handle, or 0 on error.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_new() u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const id = allocatePeerId() orelse {
        setError(ERROR_PEER_CREATE, "no available peer ids");
        return 0;
    };

    const state = allocator.create(PeerState) catch {
        setError(ERROR_PEER_CREATE, "peer allocation failed");
        return 0;
    };
    state.init() catch |err| {
        allocator.destroy(state);
        setError(ERROR_PEER_CREATE, @errorName(err));
        return 0;
    };

    peers.put(allocator, id, state) catch {
        state.deinit();
        allocator.destroy(state);
        setError(ERROR_PEER_CREATE, "peer map insert failed");
        return 0;
    };

    return id;
}

/// Destroy a peer and free its resources.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_free(peer: u32) void {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const removed = peers.fetchRemove(peer) orelse return;
    var state = removed.value;
    state.deinit();
    allocator.destroy(state);
}

/// Push an inbound frame to the peer for processing.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_push_frame(peer: u32, frame_ptr: AbiPtr, frame_len: u32) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    const frame = asSlice(frame_ptr, frame_len) catch {
        setError(ERROR_INVALID_ARG, "invalid frame pointer");
        return 0;
    };

    state.host.pushFrame(frame) catch |err| {
        setError(ERROR_PEER_PUSH, @errorName(err));
        return 0;
    };

    return 1;
}

/// Pop the next outbound frame from the peer. Returns 1 if a frame was
/// available, 0 otherwise. The frame must be committed or freed before
/// popping again.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_pop_out_frame(peer: u32, out_ptr_ptr: AbiPtr, out_len_ptr: AbiPtr) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    if (out_ptr_ptr == 0 or out_len_ptr == 0) {
        setError(ERROR_INVALID_ARG, "output pointer is null");
        return 0;
    }

    if (state.last_popped != null) {
        setError(ERROR_PEER_POP, "previous frame not committed; call capnp_peer_pop_commit first");
        return 0;
    }

    const frame = state.host.popOutgoingFrame() orelse {
        writeAbiPtr(out_ptr_ptr, 0) catch {
            setError(ERROR_INVALID_ARG, "invalid out_ptr_ptr");
            return 0;
        };
        writeU32(out_len_ptr, 0) catch {
            setError(ERROR_INVALID_ARG, "invalid out_len_ptr");
            return 0;
        };
        state.resetScratchIfIdle();
        return 0;
    };

    const frame_ptr = ptrToAbi(@intFromPtr(frame.ptr)) catch {
        state.host.freeFrame(frame);
        setError(ERROR_PEER_POP, "frame pointer overflow");
        return 0;
    };
    if (frame.len > std.math.maxInt(u32)) {
        state.host.freeFrame(frame);
        setError(ERROR_PEER_POP, "frame too large for ABI");
        return 0;
    }
    const frame_len_u32: u32 = @intCast(frame.len);

    writeAbiPtr(out_ptr_ptr, frame_ptr) catch {
        state.host.freeFrame(frame);
        setError(ERROR_INVALID_ARG, "invalid out_ptr_ptr");
        return 0;
    };
    writeU32(out_len_ptr, frame_len_u32) catch {
        state.host.freeFrame(frame);
        setError(ERROR_INVALID_ARG, "invalid out_len_ptr");
        return 0;
    };

    state.last_popped = frame;
    return 1;
}

/// Commit the last popped frame, releasing its memory.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_pop_commit(peer: u32) void {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return;
    };

    if (state.last_popped) |frame| {
        state.host.freeFrame(frame);
        state.last_popped = null;
        state.resetScratchIfIdle();
    }
}

/// Install the default bootstrap stub on the peer.
///
/// This replaces any previously configured bootstrap capability (including the
/// host call bridge bootstrap set during peer initialization). The old export
/// entry is not reclaimed. Callers should only call this once per peer; repeated
/// calls are idempotent (they return the same export ID without replacing the
/// bootstrap again).
///
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_set_bootstrap_stub(peer: u32) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    if (state.bootstrap_stub_export_id != null) return 1;

    if (state.host.host_bridge_enabled) {
        log.warn("peer {d}: replacing host call bridge bootstrap with bootstrap stub", .{peer});
    }

    const export_id = state.host.peer.setBootstrap(.{
        .ctx = &bootstrap_stub_ctx,
        .on_call = BootstrapStubHandler.onCall,
    }) catch |err| {
        setError(ERROR_BOOTSTRAP_CONFIG, @errorName(err));
        return 0;
    };
    state.bootstrap_stub_export_id = export_id;
    return 1;
}

/// Install the bootstrap stub and write the resulting export ID to the
/// output pointer. Idempotent: repeated calls return the same ID.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_set_bootstrap_stub_with_id(
    peer: u32,
    out_export_id_ptr: AbiPtr,
) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    if (out_export_id_ptr == 0) {
        setError(ERROR_INVALID_ARG, "output pointer is null");
        return 0;
    }

    if (state.bootstrap_stub_export_id == null) {
        const export_id = state.host.peer.setBootstrap(.{
            .ctx = &bootstrap_stub_ctx,
            .on_call = BootstrapStubHandler.onCall,
        }) catch |err| {
            setError(ERROR_BOOTSTRAP_CONFIG, @errorName(err));
            return 0;
        };
        state.bootstrap_stub_export_id = export_id;
    }

    writeU32(out_export_id_ptr, state.bootstrap_stub_export_id.?) catch {
        setError(ERROR_INVALID_ARG, "invalid out_export_id_ptr");
        return 0;
    };

    return 1;
}

/// Return the number of pending outbound frames for the peer.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_outbound_count(peer: u32) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };
    return clampU32(state.host.pendingOutgoingCount());
}

/// Return the total byte count of pending outbound frames for the peer.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_outbound_bytes(peer: u32) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };
    return clampU32(state.host.pendingOutgoingBytes());
}

/// Return 1 if the peer has an uncommitted popped frame, 0 otherwise.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_has_uncommitted_pop(peer: u32) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };
    return if (state.last_popped != null) 1 else 0;
}

/// Set outbound queue limits for the peer.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_set_limits(
    peer: u32,
    outbound_count_limit: u32,
    outbound_bytes_limit: u32,
) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    state.host.setLimits(.{
        .outbound_count_limit = outbound_count_limit,
        .outbound_bytes_limit = outbound_bytes_limit,
    });
    return 1;
}

/// Read the current outbound queue limits for the peer.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_get_limits(
    peer: u32,
    out_count_limit_ptr: AbiPtr,
    out_bytes_limit_ptr: AbiPtr,
) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    if (out_count_limit_ptr == 0 or out_bytes_limit_ptr == 0) {
        setError(ERROR_INVALID_ARG, "output pointer is null");
        return 0;
    }

    const limits = state.host.getLimits();
    writeU32(out_count_limit_ptr, clampU32(limits.outbound_count_limit)) catch {
        setError(ERROR_INVALID_ARG, "invalid out_count_limit_ptr");
        return 0;
    };
    writeU32(out_bytes_limit_ptr, clampU32(limits.outbound_bytes_limit)) catch {
        setError(ERROR_INVALID_ARG, "invalid out_bytes_limit_ptr");
        return 0;
    };

    return 1;
}

/// Pop the next inbound host call from the peer. Returns 1 if a call was
/// available, 0 otherwise.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_pop_host_call(
    peer: u32,
    out_question_id_ptr: AbiPtr,
    out_interface_id_ptr: AbiPtr,
    out_method_id_ptr: AbiPtr,
    out_frame_ptr_ptr: AbiPtr,
    out_frame_len_ptr: AbiPtr,
) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    if (out_question_id_ptr == 0 or
        out_interface_id_ptr == 0 or
        out_method_id_ptr == 0 or
        out_frame_ptr_ptr == 0 or
        out_frame_len_ptr == 0)
    {
        setError(ERROR_INVALID_ARG, "output pointer is null");
        return 0;
    }

    const call = state.host.popHostCall() orelse {
        writeU32(out_question_id_ptr, 0) catch {
            setError(ERROR_INVALID_ARG, "invalid out_question_id_ptr");
            return 0;
        };
        writeU64(out_interface_id_ptr, 0) catch {
            setError(ERROR_INVALID_ARG, "invalid out_interface_id_ptr");
            return 0;
        };
        writeU16(out_method_id_ptr, 0) catch {
            setError(ERROR_INVALID_ARG, "invalid out_method_id_ptr");
            return 0;
        };
        writeAbiPtr(out_frame_ptr_ptr, 0) catch {
            setError(ERROR_INVALID_ARG, "invalid out_frame_ptr_ptr");
            return 0;
        };
        writeU32(out_frame_len_ptr, 0) catch {
            setError(ERROR_INVALID_ARG, "invalid out_frame_len_ptr");
            return 0;
        };
        return 0;
    };

    const frame_ptr: AbiPtr = if (call.frame.len == 0) 0 else ptrToAbi(@intFromPtr(call.frame.ptr)) catch {
        state.host.freeHostCallFrame(call.frame);
        setError(ERROR_HOST_CALL, "host call frame pointer overflow");
        return 0;
    };
    if (call.frame.len > std.math.maxInt(u32)) {
        state.host.freeHostCallFrame(call.frame);
        setError(ERROR_HOST_CALL, "frame too large for ABI");
        return 0;
    }
    const frame_len: u32 = @intCast(call.frame.len);
    if (frame_len != 0) {
        const frame_addr = @intFromPtr(call.frame.ptr);
        const slot = state.outstanding_host_call_frames.getOrPut(frame_addr) catch {
            state.host.freeHostCallFrame(call.frame);
            setError(ERROR_HOST_CALL, "host call frame tracking failed");
            return 0;
        };
        if (slot.found_existing) {
            state.host.freeHostCallFrame(call.frame);
            setError(ERROR_HOST_CALL, "host call frame tracking collision");
            return 0;
        }
        slot.value_ptr.* = frame_len;
    }

    writeU32(out_question_id_ptr, call.question_id) catch {
        if (frame_len != 0) {
            _ = state.outstanding_host_call_frames.remove(@intFromPtr(call.frame.ptr));
        }
        state.host.freeHostCallFrame(call.frame);
        setError(ERROR_INVALID_ARG, "invalid out_question_id_ptr");
        return 0;
    };
    writeU64(out_interface_id_ptr, call.interface_id) catch {
        if (frame_len != 0) {
            _ = state.outstanding_host_call_frames.remove(@intFromPtr(call.frame.ptr));
        }
        state.host.freeHostCallFrame(call.frame);
        setError(ERROR_INVALID_ARG, "invalid out_interface_id_ptr");
        return 0;
    };
    writeU16(out_method_id_ptr, call.method_id) catch {
        if (frame_len != 0) {
            _ = state.outstanding_host_call_frames.remove(@intFromPtr(call.frame.ptr));
        }
        state.host.freeHostCallFrame(call.frame);
        setError(ERROR_INVALID_ARG, "invalid out_method_id_ptr");
        return 0;
    };
    writeAbiPtr(out_frame_ptr_ptr, frame_ptr) catch {
        if (frame_len != 0) {
            _ = state.outstanding_host_call_frames.remove(@intFromPtr(call.frame.ptr));
        }
        state.host.freeHostCallFrame(call.frame);
        setError(ERROR_INVALID_ARG, "invalid out_frame_ptr_ptr");
        return 0;
    };
    writeU32(out_frame_len_ptr, frame_len) catch {
        if (frame_len != 0) {
            _ = state.outstanding_host_call_frames.remove(@intFromPtr(call.frame.ptr));
        }
        state.host.freeHostCallFrame(call.frame);
        setError(ERROR_INVALID_ARG, "invalid out_frame_len_ptr");
        return 0;
    };

    return 1;
}

/// Free a host call frame previously obtained via capnp_peer_pop_host_call.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_free_host_call_frame(
    peer: u32,
    frame_ptr: AbiPtr,
    frame_len: u32,
) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    if (frame_len == 0) return 1;
    if (frame_ptr == 0) {
        setError(ERROR_INVALID_ARG, "invalid host call frame pointer");
        return 0;
    }

    const frame_addr: usize = @intCast(frame_ptr);
    const tracked = state.outstanding_host_call_frames.fetchRemove(frame_addr) orelse {
        setError(ERROR_INVALID_ARG, "unknown host call frame");
        return 0;
    };
    if (tracked.value != frame_len) {
        state.outstanding_host_call_frames.put(frame_addr, tracked.value) catch {};
        setError(ERROR_INVALID_ARG, "host call frame length mismatch");
        return 0;
    }

    const frame_ptr_bytes: [*]u8 = @ptrFromInt(@as(usize, @intCast(frame_ptr)));
    state.host.freeHostCallFrame(frame_ptr_bytes[0..@as(usize, frame_len)]);
    return 1;
}

/// Respond to a host call with results.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_respond_host_call_results(
    peer: u32,
    question_id: u32,
    payload_ptr: AbiPtr,
    payload_len: u32,
) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    const payload = asSlice(payload_ptr, payload_len) catch {
        setError(ERROR_INVALID_ARG, "invalid payload pointer");
        return 0;
    };

    state.host.respondHostCallResults(question_id, payload) catch |err| {
        setError(ERROR_HOST_CALL, @errorName(err));
        return 0;
    };
    return 1;
}

/// Respond to a host call with an exception.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_respond_host_call_exception(
    peer: u32,
    question_id: u32,
    reason_ptr: AbiPtr,
    reason_len: u32,
) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    const reason = asSlice(reason_ptr, reason_len) catch {
        setError(ERROR_INVALID_ARG, "invalid reason pointer");
        return 0;
    };

    state.host.respondHostCallException(question_id, reason) catch |err| {
        setError(ERROR_HOST_CALL, @errorName(err));
        return 0;
    };
    return 1;
}

/// Respond to a host call with a raw Return frame.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_respond_host_call_return_frame(
    peer: u32,
    return_frame_ptr: AbiPtr,
    return_frame_len: u32,
) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    if (return_frame_len == 0) {
        setError(ERROR_INVALID_ARG, "invalid return frame length");
        return 0;
    }

    const return_frame = asSlice(return_frame_ptr, return_frame_len) catch {
        setError(ERROR_INVALID_ARG, "invalid return frame pointer");
        return 0;
    };

    state.host.respondHostCallReturnFrame(return_frame) catch |err| {
        setHostCallReturnFrameError(err);
        return 0;
    };
    return 1;
}

/// Send a Finish control message for the given question.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_send_finish(
    peer: u32,
    question_id: u32,
    release_result_caps: u32,
    require_early_cancellation: u32,
) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    if ((release_result_caps != 0 and release_result_caps != 1) or
        (require_early_cancellation != 0 and require_early_cancellation != 1))
    {
        setError(ERROR_INVALID_ARG, "bool flag must be 0 or 1");
        return 0;
    }

    state.host.peer.sendFinishForHost(
        question_id,
        release_result_caps != 0,
        require_early_cancellation != 0,
    ) catch |err| {
        setError(ERROR_PEER_CONTROL, @errorName(err));
        return 0;
    };
    return 1;
}

/// Send a Release control message for the given capability.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_peer_send_release(peer: u32, cap_id: u32, reference_count: u32) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    state.host.peer.sendReleaseForHost(cap_id, reference_count) catch |err| {
        setError(ERROR_PEER_CONTROL, @errorName(err));
        return 0;
    };
    return 1;
}

/// Return the schema manifest as a JSON-encoded, heap-allocated buffer.
/// The caller must free the buffer via capnp_buf_free.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_schema_manifest_json(out_ptr_ptr: AbiPtr, out_len_ptr: AbiPtr) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const manifest = schemaManifestJsonBytes();
    const copy = allocator.alloc(u8, manifest.len) catch {
        setError(ERROR_ALLOC, "schema manifest allocation failed");
        return 0;
    };
    std.mem.copyForwards(u8, copy, manifest);

    writeOutBuffer(out_ptr_ptr, out_len_ptr, copy) catch |err| {
        allocator.free(copy);
        setError(ERROR_INVALID_ARG, @errorName(err));
        return 0;
    };
    return 1;
}

/// Decode a Person Cap'n Proto frame and return its JSON representation.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_example_person_to_json(
    frame_ptr: AbiPtr,
    frame_len: u32,
    out_json_ptr_ptr: AbiPtr,
    out_json_len_ptr: AbiPtr,
) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const frame = asSlice(frame_ptr, frame_len) catch {
        setError(ERROR_INVALID_ARG, "invalid frame pointer");
        return 0;
    };

    var msg = message.Message.init(allocator, frame) catch |err| {
        setError(ERROR_SERDE_DECODE, @errorName(err));
        return 0;
    };
    defer msg.deinit();

    const reader = generated_example.Person.Reader.init(&msg) catch |err| {
        setError(ERROR_SERDE_DECODE, @errorName(err));
        return 0;
    };

    const name = reader.getName() catch |err| {
        setError(ERROR_SERDE_DECODE, @errorName(err));
        return 0;
    };
    const age = reader.getAge() catch |err| {
        setError(ERROR_SERDE_DECODE, @errorName(err));
        return 0;
    };
    const email = reader.getEmail() catch |err| {
        setError(ERROR_SERDE_DECODE, @errorName(err));
        return 0;
    };

    const json_bytes = std.json.Stringify.valueAlloc(allocator, PersonJson{
        .name = name,
        .age = age,
        .email = email,
    }, .{}) catch |err| {
        setError(ERROR_SERDE_DECODE, @errorName(err));
        return 0;
    };

    writeOutBuffer(out_json_ptr_ptr, out_json_len_ptr, json_bytes) catch |err| {
        allocator.free(json_bytes);
        setError(ERROR_INVALID_ARG, @errorName(err));
        return 0;
    };

    return 1;
}

/// Encode a Person from JSON into a Cap'n Proto frame.
/// Thread-safe on native targets (mutex-protected).
pub export fn capnp_example_person_from_json(
    json_ptr: AbiPtr,
    json_len: u32,
    out_frame_ptr_ptr: AbiPtr,
    out_frame_len_ptr: AbiPtr,
) u32 {
    global_mutex.lock();
    defer global_mutex.unlock();

    clearErrorState();

    const json_bytes = asSlice(json_ptr, json_len) catch {
        setError(ERROR_INVALID_ARG, "invalid json pointer");
        return 0;
    };

    var parsed = std.json.parseFromSlice(PersonJson, allocator, json_bytes, .{}) catch |err| {
        setError(ERROR_SERDE_ENCODE, @errorName(err));
        return 0;
    };
    defer parsed.deinit();

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var person = generated_example.Person.Builder.init(&builder) catch |err| {
        setError(ERROR_SERDE_ENCODE, @errorName(err));
        return 0;
    };

    person.setName(parsed.value.name) catch |err| {
        setError(ERROR_SERDE_ENCODE, @errorName(err));
        return 0;
    };
    person.setAge(parsed.value.age) catch |err| {
        setError(ERROR_SERDE_ENCODE, @errorName(err));
        return 0;
    };
    person.setEmail(parsed.value.email) catch |err| {
        setError(ERROR_SERDE_ENCODE, @errorName(err));
        return 0;
    };

    const frame = builder.toBytes() catch |err| {
        setError(ERROR_SERDE_ENCODE, @errorName(err));
        return 0;
    };

    writeOutBuffer(out_frame_ptr_ptr, out_frame_len_ptr, frame) catch |err| {
        allocator.free(frame);
        setError(ERROR_INVALID_ARG, @errorName(err));
        return 0;
    };

    return 1;
}
