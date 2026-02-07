const std = @import("std");
const core = @import("capnpc-zig-core");
const generated_example = @import("capnp-wasm-example-schema");

const HostPeer = core.rpc.host_peer.HostPeer;
const message = core.message;
const Peer = core.rpc.peer.Peer;
const protocol = core.rpc.protocol;
const cap_table = core.rpc.cap_table;

const allocator = std.heap.wasm_allocator;
const ABI_VERSION: u32 = 1;

const ERROR_ALLOC: u32 = 1;
const ERROR_INVALID_ARG: u32 = 2;
const ERROR_UNKNOWN_PEER: u32 = 3;
const ERROR_PEER_CREATE: u32 = 4;
const ERROR_PEER_PUSH: u32 = 5;
const ERROR_PEER_POP: u32 = 6;
const ERROR_SERDE_ENCODE: u32 = 7;
const ERROR_SERDE_DECODE: u32 = 8;
const ERROR_BOOTSTRAP_CONFIG: u32 = 9;

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
    bootstrap_stub_export_id: ?u32 = null,

    fn init(self: *PeerState) void {
        self.outgoing_fba = std.heap.FixedBufferAllocator.init(&self.outgoing_storage);
        self.host = HostPeer.initWithOutgoingAllocator(allocator, self.outgoing_fba.allocator());
        self.host.start(null, null);
        self.last_popped = null;
        self.bootstrap_stub_export_id = null;
    }

    fn deinit(self: *PeerState) void {
        if (self.last_popped) |frame| {
            self.host.freeFrame(frame);
            self.last_popped = null;
        }
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

fn ptrToU32(ptr: usize) !u32 {
    if (ptr > std.math.maxInt(u32)) return error.PointerOverflow;
    return @intCast(ptr);
}

fn asSlice(ptr: u32, len: u32) ![]const u8 {
    if (len == 0) return &.{};
    if (ptr == 0) return error.NullPointer;
    const p: [*]const u8 = @ptrFromInt(@as(usize, ptr));
    return p[0..@as(usize, len)];
}

fn writeU32(ptr: u32, value: u32) !void {
    if (ptr == 0) return error.NullPointer;
    const out: *u32 = @ptrFromInt(@as(usize, ptr));
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

fn writeOutBuffer(out_ptr_ptr: u32, out_len_ptr: u32, bytes: []const u8) !void {
    if (out_ptr_ptr == 0 or out_len_ptr == 0) return error.NullPointer;
    if (bytes.len > std.math.maxInt(u32)) return error.LengthOverflow;
    const out_ptr: u32 = if (bytes.len == 0) 0 else try ptrToU32(@intFromPtr(bytes.ptr));
    try writeU32(out_ptr_ptr, out_ptr);
    try writeU32(out_len_ptr, @intCast(bytes.len));
}

pub export fn capnp_wasm_abi_version() u32 {
    return ABI_VERSION;
}

pub export fn capnp_alloc(len: u32) u32 {
    clearErrorState();
    const size: usize = if (len == 0) 1 else @as(usize, len);
    const mem = allocator.alloc(u8, size) catch {
        setError(ERROR_ALLOC, "capnp_alloc out of memory");
        return 0;
    };
    return ptrToU32(@intFromPtr(mem.ptr)) catch {
        allocator.free(mem);
        setError(ERROR_ALLOC, "capnp_alloc pointer overflow");
        return 0;
    };
}

pub export fn capnp_free(ptr: u32, len: u32) void {
    if (ptr == 0) return;
    const size: usize = if (len == 0) 1 else @as(usize, len);
    const mem: [*]u8 = @ptrFromInt(@as(usize, ptr));
    allocator.free(mem[0..size]);
}

pub export fn capnp_buf_free(ptr: u32, len: u32) void {
    capnp_free(ptr, len);
}

pub export fn capnp_last_error_code() u32 {
    return last_error_code;
}

pub export fn capnp_last_error_ptr() u32 {
    return @intCast(@intFromPtr(&last_error_buf));
}

pub export fn capnp_last_error_len() u32 {
    return last_error_len;
}

pub export fn capnp_clear_error() void {
    clearErrorState();
}

pub export fn capnp_peer_new() u32 {
    clearErrorState();

    const id = allocatePeerId() orelse {
        setError(ERROR_PEER_CREATE, "no available peer ids");
        return 0;
    };

    const state = allocator.create(PeerState) catch {
        setError(ERROR_PEER_CREATE, "peer allocation failed");
        return 0;
    };
    state.init();
    errdefer {
        state.deinit();
        allocator.destroy(state);
    }

    peers.put(allocator, id, state) catch {
        setError(ERROR_PEER_CREATE, "peer map insert failed");
        return 0;
    };

    return id;
}

pub export fn capnp_peer_free(peer: u32) void {
    clearErrorState();

    const removed = peers.fetchRemove(peer) orelse return;
    var state = removed.value;
    state.deinit();
    allocator.destroy(state);
}

pub export fn capnp_peer_push_frame(peer: u32, frame_ptr: u32, frame_len: u32) u32 {
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

pub export fn capnp_peer_pop_out_frame(peer: u32, out_ptr_ptr: u32, out_len_ptr: u32) u32 {
    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    if (out_ptr_ptr == 0 or out_len_ptr == 0) {
        setError(ERROR_INVALID_ARG, "output pointer is null");
        return 0;
    }

    if (state.last_popped) |frame| {
        state.host.freeFrame(frame);
        state.last_popped = null;
        state.resetScratchIfIdle();
    }

    const frame = state.host.popOutgoingFrame() orelse {
        writeU32(out_ptr_ptr, 0) catch {
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

    const frame_ptr = ptrToU32(@intFromPtr(frame.ptr)) catch {
        state.host.freeFrame(frame);
        setError(ERROR_PEER_POP, "frame pointer overflow");
        return 0;
    };
    const frame_len_u32: u32 = @intCast(frame.len);

    writeU32(out_ptr_ptr, frame_ptr) catch {
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

pub export fn capnp_peer_pop_commit(peer: u32) void {
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

pub export fn capnp_peer_set_bootstrap_stub(peer: u32) u32 {
    clearErrorState();

    const state = getPeerState(peer) orelse {
        setError(ERROR_UNKNOWN_PEER, "unknown peer handle");
        return 0;
    };

    if (state.bootstrap_stub_export_id != null) return 1;

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

pub export fn capnp_example_person_to_json(
    frame_ptr: u32,
    frame_len: u32,
    out_json_ptr_ptr: u32,
    out_json_len_ptr: u32,
) u32 {
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

pub export fn capnp_example_person_from_json(
    json_ptr: u32,
    json_len: u32,
    out_frame_ptr_ptr: u32,
    out_frame_len_ptr: u32,
) u32 {
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
