const std = @import("std");
const capnpc = @import("capnpc-zig");
const compare = @import("support/capnp_compare.zig");
const xev = @import("xev");

const protocol = capnpc.rpc.protocol;
const cap_table = capnpc.rpc.cap_table;
const peer_mod = capnpc.rpc.peer;
const connection_mod = capnpc.rpc.connection;
const request_reader = capnpc.request;
const schema = capnpc.schema;

const ParamLayout = struct {
    data_words: u16,
    pointer_words: u16,
    a_offset: usize,
    b_offset: usize,
};

const ResultLayout = struct {
    data_words: u16,
    pointer_words: u16,
    field_offset: usize,
    field2_offset: usize,
};

const CapabilityLayout = struct {
    data_words: u16,
    pointer_words: u16,
    pointer_index: usize,
};

const SchemaInfo = struct {
    arith_interface_id: u64,
    callback_interface_id: u64,
    multiply_id: u16,
    divide_id: u16,
    loop_promise_id: u16,
    callback_ping_id: u16,
    multiply_params: ParamLayout,
    multiply_results: ResultLayout,
    divide_params: ParamLayout,
    divide_results: ResultLayout,
    loop_promise_params: CapabilityLayout,
    loop_promise_results: CapabilityLayout,
    callback_ping_results: ResultLayout,
};

const InteropBackend = enum {
    go,
    cpp,
};

const State = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    peer: ?*peer_mod.Peer = null,
    conn: ?*connection_mod.Connection = null,
    schema: SchemaInfo,
    a: i64,
    b: i64,
    done: bool = false,
    err: ?anyerror = null,
};

const CallContext = struct {
    state: *State,
    cap_id: u32,
    kind: Kind,
};

const Kind = enum {
    multiply,
    divide,
};

var g_state: ?*State = null;

const PromiseState = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    peer: ?*peer_mod.Peer = null,
    conn: ?*connection_mod.Connection = null,
    schema: SchemaInfo,
    callback_value: i64,
    done: bool = false,
    err: ?anyerror = null,
};

const LoopPromiseContext = struct {
    state: *PromiseState,
    arith_cap_id: u32,
    callback_export_id: u32,
};

const LoopPingContext = struct {
    state: *PromiseState,
};

var g_promise_state: ?*PromiseState = null;

const PromiseRaceState = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    peer: ?*peer_mod.Peer = null,
    conn: ?*connection_mod.Connection = null,
    schema: SchemaInfo,
    callback_contexts: [2]?*PromiseRaceCallbackCtx = .{ null, null },
    expected_values: [2]i64 = .{ 0, 0 },
    completed_pings: u8 = 0,
    done: bool = false,
    err: ?anyerror = null,
};

const PromiseRaceCallbackCtx = struct {
    state: *PromiseRaceState,
    value: i64,
};

const PromiseRaceLoopContext = struct {
    state: *PromiseRaceState,
    callback_export_id: u32,
    expected_value: i64,
};

const PromiseRacePingContext = struct {
    state: *PromiseRaceState,
    promised_cap_id: u32,
    expected_value: i64,
};

var g_promise_race_state: ?*PromiseRaceState = null;

const ThirdPartyState = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    peer: ?*peer_mod.Peer = null,
    conn: ?*connection_mod.Connection = null,
    schema: SchemaInfo,
    a: i64,
    b: i64,
    done: bool = false,
    err: ?anyerror = null,
};

const ThirdPartyCallContext = struct {
    state: *ThirdPartyState,
    cap_id: u32,
};

var g_third_party_state: ?*ThirdPartyState = null;

const ExceptionFollowupState = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    peer: ?*peer_mod.Peer = null,
    conn: ?*connection_mod.Connection = null,
    schema: SchemaInfo,
    done: bool = false,
    err: ?anyerror = null,
};

const ExceptionFollowupCallContext = struct {
    state: *ExceptionFollowupState,
    cap_id: u32,
};

var g_exception_followup_state: ?*ExceptionFollowupState = null;

const FinishEdgeState = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    peer: ?*peer_mod.Peer = null,
    conn: ?*connection_mod.Connection = null,
    schema: SchemaInfo,
    done: bool = false,
    err: ?anyerror = null,
    canceled_return_seen: bool = false,
    pending_canceled_ctx: ?*FinishEdgeCallContext = null,
};

const FinishEdgeCallContext = struct {
    state: *FinishEdgeState,
    cap_id: u32,
};

var g_finish_edge_state: ?*FinishEdgeState = null;

fn runCommand(allocator: std.mem.Allocator, argv: []const []const u8, cwd: ?[]const u8) !std.process.Child.RunResult {
    return std.process.Child.run(.{
        .allocator = allocator,
        .argv = argv,
        .cwd = cwd,
        .max_output_bytes = 10 * 1024 * 1024,
    });
}

fn resolveCapnpGoStd(allocator: std.mem.Allocator) ![]u8 {
    const env_path = std.process.getEnvVarOwned(allocator, "CAPNP_GO_STD") catch |err| switch (err) {
        error.EnvironmentVariableNotFound => null,
        else => return err,
    };
    if (env_path) |raw| {
        defer allocator.free(raw);
        return std.fs.cwd().realpathAlloc(allocator, raw) catch error.InvalidCapnpGoStdPath;
    }

    // Default to the vendored Go runtime path for CI/local reproducibility.
    return std.fs.cwd().realpathAlloc(allocator, "vendor/ext/go-capnp/std") catch error.MissingCapnpGoStd;
}

fn resolveInteropBackend(allocator: std.mem.Allocator) !InteropBackend {
    const raw = std.process.getEnvVarOwned(allocator, "CAPNP_INTEROP_BACKEND") catch |err| switch (err) {
        error.EnvironmentVariableNotFound => return .go,
        else => return err,
    };
    defer allocator.free(raw);

    if (std.mem.eql(u8, raw, "go")) return .go;
    if (std.mem.eql(u8, raw, "cpp")) return .cpp;
    return error.InvalidInteropBackend;
}

fn buildGoInteropServer(allocator: std.mem.Allocator) !void {
    try std.fs.cwd().makePath("tests/interop_rpc/.zig-cache");

    const argv = &[_][]const u8{
        "go",
        "build",
        "-mod=vendor",
        "-o",
        ".zig-cache/arith-server-test",
        "./cmd/arith-server",
    };
    const result = runCommand(allocator, argv, "tests/interop_rpc") catch |err| switch (err) {
        error.FileNotFound => return error.GoToolMissing,
        else => return err,
    };
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    if (!(result.term == .Exited and result.term.Exited == 0)) {
        if (result.stderr.len > 0) std.log.err("go build stderr:\n{s}", .{result.stderr});
        return error.GoBuildFailed;
    }
}

fn buildCppInteropServer(allocator: std.mem.Allocator) !void {
    try std.fs.cwd().makePath("tests/interop_rpc/.zig-cache");
    try std.fs.cwd().makePath("tests/interop_rpc/.zig-cache/cpp-gen");

    const capnp_go_std = try resolveCapnpGoStd(allocator);
    defer allocator.free(capnp_go_std);

    const schema_argv = &[_][]const u8{
        "capnp",
        "compile",
        "-I",
        capnp_go_std,
        "-oc++:.zig-cache/cpp-gen",
        "arith/arith.capnp",
    };
    const schema_result = runCommand(allocator, schema_argv, "tests/interop_rpc") catch |err| switch (err) {
        error.FileNotFound => return error.CapnpToolMissing,
        else => return err,
    };
    defer allocator.free(schema_result.stdout);
    defer allocator.free(schema_result.stderr);

    if (!(schema_result.term == .Exited and schema_result.term.Exited == 0)) {
        if (schema_result.stderr.len > 0) std.log.err("capnp compile (c++) stderr:\n{s}", .{schema_result.stderr});
        return error.CapnpCompileFailed;
    }

    const cxx_env = std.process.getEnvVarOwned(allocator, "CXX") catch |err| switch (err) {
        error.EnvironmentVariableNotFound => null,
        else => return err,
    };
    defer if (cxx_env) |owned| allocator.free(owned);

    const cxx = if (cxx_env) |owned| owned else "c++";
    const build_argv = [_][]const u8{
        cxx,
        "-std=c++20",
        "-O2",
        "-Wall",
        "-Wextra",
        "-I.",
        "-I.zig-cache/cpp-gen",
        "-o",
        ".zig-cache/arith-server-cpp-test",
        "cpp/arith_server.cpp",
        ".zig-cache/cpp-gen/arith/arith.capnp.c++",
        "-lcapnp-rpc",
        "-lcapnp",
        "-lkj-async",
        "-lkj",
        "-pthread",
    };

    const build_result = runCommand(allocator, build_argv[0..], "tests/interop_rpc") catch |err| switch (err) {
        error.FileNotFound => return error.CppToolMissing,
        else => return err,
    };
    defer allocator.free(build_result.stdout);
    defer allocator.free(build_result.stderr);

    if (!(build_result.term == .Exited and build_result.term.Exited == 0)) {
        if (build_result.stderr.len > 0) std.log.err("c++ build stderr:\n{s}", .{build_result.stderr});
        return error.CppBuildFailed;
    }
}

fn spawnGoInteropServer(allocator: std.mem.Allocator, addr_string: []const u8) !std.process.Child {
    try buildGoInteropServer(allocator);

    var child = std.process.Child.init(&[_][]const u8{
        "./.zig-cache/arith-server-test",
        "-addr",
        addr_string,
    }, allocator);
    child.cwd = "tests/interop_rpc";
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Ignore;
    child.spawn() catch |err| switch (err) {
        error.FileNotFound => return error.InteropServerBinaryMissing,
        else => return err,
    };
    return child;
}

fn spawnCppInteropServer(allocator: std.mem.Allocator, addr_string: []const u8) !std.process.Child {
    try buildCppInteropServer(allocator);

    var child = std.process.Child.init(&[_][]const u8{
        "./.zig-cache/arith-server-cpp-test",
        "-addr",
        addr_string,
    }, allocator);
    child.cwd = "tests/interop_rpc";
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Ignore;
    child.spawn() catch |err| switch (err) {
        error.FileNotFound => return error.InteropServerBinaryMissing,
        else => return err,
    };
    return child;
}

fn spawnInteropServer(
    allocator: std.mem.Allocator,
    addr_string: []const u8,
    backend: InteropBackend,
) !std.process.Child {
    return switch (backend) {
        .go => spawnGoInteropServer(allocator, addr_string),
        .cpp => spawnCppInteropServer(allocator, addr_string),
    };
}

fn waitForServerReady(addr: std.net.Address, timeout_ms: i64) !void {
    const start = std.time.milliTimestamp();
    while (true) {
        const stream = std.net.tcpConnectToAddress(addr) catch |err| switch (err) {
            error.ConnectionRefused,
            error.ConnectionResetByPeer,
            error.ConnectionTimedOut,
            error.NetworkUnreachable,
            => {
                if (std.time.milliTimestamp() - start > timeout_ms) return error.Timeout;
                std.Thread.sleep(20 * std.time.ns_per_ms);
                continue;
            },
            else => return err,
        };
        stream.close();
        return;
    }
}

fn loadSchemaInfo(
    allocator: std.mem.Allocator,
    capnp_go_std: []const u8,
    capnp_path: []const u8,
    cwd: ?[]const u8,
) !SchemaInfo {
    const argv = &[_][]const u8{
        "capnp",
        "compile",
        "-I",
        capnp_go_std,
        "-o-",
        capnp_path,
    };
    const result = runCommand(allocator, argv, cwd) catch |err| switch (err) {
        error.FileNotFound => return error.CapnpToolMissing,
        else => return err,
    };
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    if (!(result.term == .Exited and result.term.Exited == 0)) return error.CapnpCompileFailed;

    const request = try request_reader.parseCodeGeneratorRequest(allocator, result.stdout);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    const arith_iface = findInterfaceBySuffix(request.nodes, "Arith") orelse return error.InterfaceNotFound;
    const callback_iface = findInterfaceBySuffix(request.nodes, "Callback") orelse return error.InterfaceNotFound;
    const arith_interface_id = arith_iface.id;
    const callback_interface_id = callback_iface.id;
    const arith_info = arith_iface.interface_node orelse return error.InvalidInterfaceNode;
    const callback_info = callback_iface.interface_node orelse return error.InvalidInterfaceNode;

    var multiply_id: u16 = 0;
    var divide_id: u16 = 0;
    var loop_promise_id: u16 = 0;
    var callback_ping_id: u16 = 0;
    var multiply_params: ParamLayout = undefined;
    var multiply_results: ResultLayout = undefined;
    var divide_params: ParamLayout = undefined;
    var divide_results: ResultLayout = undefined;
    var loop_promise_params: CapabilityLayout = undefined;
    var loop_promise_results: CapabilityLayout = undefined;
    var callback_ping_results: ResultLayout = undefined;

    for (arith_info.methods) |method| {
        if (std.mem.eql(u8, method.name, "multiply")) {
            multiply_id = method.code_order;
            multiply_params = try extractParams(request.nodes, method.param_struct_type);
            multiply_results = try extractResults(request.nodes, method.result_struct_type, false);
        } else if (std.mem.eql(u8, method.name, "divide")) {
            divide_id = method.code_order;
            divide_params = try extractParams(request.nodes, method.param_struct_type);
            divide_results = try extractResults(request.nodes, method.result_struct_type, true);
        } else if (std.mem.eql(u8, method.name, "loopPromise")) {
            loop_promise_id = method.code_order;
            loop_promise_params = try extractCapabilityLayout(request.nodes, method.param_struct_type, "cb");
            loop_promise_results = try extractCapabilityLayout(request.nodes, method.result_struct_type, "resolvedCb");
        }
    }

    for (callback_info.methods) |method| {
        if (std.mem.eql(u8, method.name, "ping")) {
            callback_ping_id = method.code_order;
            callback_ping_results = try extractSingleResult(request.nodes, method.result_struct_type, "value");
        }
    }

    return .{
        .arith_interface_id = arith_interface_id,
        .callback_interface_id = callback_interface_id,
        .multiply_id = multiply_id,
        .divide_id = divide_id,
        .loop_promise_id = loop_promise_id,
        .callback_ping_id = callback_ping_id,
        .multiply_params = multiply_params,
        .multiply_results = multiply_results,
        .divide_params = divide_params,
        .divide_results = divide_results,
        .loop_promise_params = loop_promise_params,
        .loop_promise_results = loop_promise_results,
        .callback_ping_results = callback_ping_results,
    };
}

fn findInterfaceBySuffix(nodes: []schema.Node, suffix: []const u8) ?*const schema.Node {
    for (nodes) |*node| {
        if (node.kind != .interface) continue;
        if (std.mem.endsWith(u8, node.display_name, suffix)) return node;
    }
    return null;
}

fn fieldOffsetInt64(node: *const schema.Node, name: []const u8) !usize {
    const info = node.struct_node orelse return error.InvalidStructNode;
    for (info.fields) |field| {
        const slot = field.slot orelse continue;
        if (!std.mem.eql(u8, field.name, name)) continue;
        return @as(usize, slot.offset) * 8;
    }
    return error.FieldNotFound;
}

fn fieldPointerIndex(node: *const schema.Node, name: []const u8) !usize {
    const info = node.struct_node orelse return error.InvalidStructNode;
    for (info.fields) |field| {
        const slot = field.slot orelse continue;
        if (!std.mem.eql(u8, field.name, name)) continue;
        return @as(usize, slot.offset);
    }
    return error.FieldNotFound;
}

fn extractParams(nodes: []schema.Node, id: schema.Id) !ParamLayout {
    const node = compare.findNodeById(nodes, id) orelse return error.InvalidSchema;
    const info = node.struct_node orelse return error.InvalidStructNode;
    return .{
        .data_words = info.data_word_count,
        .pointer_words = info.pointer_count,
        .a_offset = try fieldOffsetInt64(node, "a"),
        .b_offset = try fieldOffsetInt64(node, "b"),
    };
}

fn extractResults(nodes: []schema.Node, id: schema.Id, has_two: bool) !ResultLayout {
    const node = compare.findNodeById(nodes, id) orelse return error.InvalidSchema;
    const info = node.struct_node orelse return error.InvalidStructNode;
    const field_offset = if (has_two) try fieldOffsetInt64(node, "quotient") else try fieldOffsetInt64(node, "product");
    const field2_offset = if (has_two) try fieldOffsetInt64(node, "remainder") else 0;
    return .{
        .data_words = info.data_word_count,
        .pointer_words = info.pointer_count,
        .field_offset = field_offset,
        .field2_offset = field2_offset,
    };
}

fn extractSingleResult(nodes: []schema.Node, id: schema.Id, name: []const u8) !ResultLayout {
    const node = compare.findNodeById(nodes, id) orelse return error.InvalidSchema;
    const info = node.struct_node orelse return error.InvalidStructNode;
    return .{
        .data_words = info.data_word_count,
        .pointer_words = info.pointer_count,
        .field_offset = try fieldOffsetInt64(node, name),
        .field2_offset = 0,
    };
}

fn extractCapabilityLayout(nodes: []schema.Node, id: schema.Id, name: []const u8) !CapabilityLayout {
    const node = compare.findNodeById(nodes, id) orelse return error.InvalidSchema;
    const info = node.struct_node orelse return error.InvalidStructNode;
    return .{
        .data_words = info.data_word_count,
        .pointer_words = info.pointer_count,
        .pointer_index = try fieldPointerIndex(node, name),
    };
}

fn onPeerError(peer: *peer_mod.Peer, err: anyerror) void {
    _ = peer;
    if (g_state) |state| {
        state.err = err;
        state.done = true;
    }
}

fn onPeerClose(peer: *peer_mod.Peer) void {
    _ = peer;
    if (g_state) |state| {
        state.done = true;
    }
}

fn onPromisePeerError(peer: *peer_mod.Peer, err: anyerror) void {
    _ = peer;
    if (g_promise_state) |state| {
        state.err = err;
        state.done = true;
    }
}

fn onPromisePeerClose(peer: *peer_mod.Peer) void {
    _ = peer;
    if (g_promise_state) |state| {
        state.done = true;
    }
}

fn onPromiseRacePeerError(peer: *peer_mod.Peer, err: anyerror) void {
    _ = peer;
    if (g_promise_race_state) |state| {
        state.err = err;
        state.done = true;
    }
}

fn onPromiseRacePeerClose(peer: *peer_mod.Peer) void {
    _ = peer;
    if (g_promise_race_state) |state| {
        state.done = true;
    }
}

fn onThirdPartyPeerError(peer: *peer_mod.Peer, err: anyerror) void {
    _ = peer;
    if (g_third_party_state) |state| {
        state.err = err;
        state.done = true;
    }
}

fn onThirdPartyPeerClose(peer: *peer_mod.Peer) void {
    _ = peer;
    if (g_third_party_state) |state| {
        state.done = true;
    }
}

fn onExceptionFollowupPeerError(peer: *peer_mod.Peer, err: anyerror) void {
    _ = peer;
    if (g_exception_followup_state) |state| {
        state.err = err;
        state.done = true;
    }
}

fn onExceptionFollowupPeerClose(peer: *peer_mod.Peer) void {
    _ = peer;
    if (g_exception_followup_state) |state| {
        state.done = true;
    }
}

fn onFinishEdgePeerError(peer: *peer_mod.Peer, err: anyerror) void {
    _ = peer;
    if (g_finish_edge_state) |state| {
        state.err = err;
        state.done = true;
    }
}

fn onFinishEdgePeerClose(peer: *peer_mod.Peer) void {
    _ = peer;
    if (g_finish_edge_state) |state| {
        state.done = true;
    }
}

fn expectExceptionContains(ret: protocol.Return, needle: []const u8) !void {
    if (ret.tag != .exception) return error.UnexpectedReturn;
    const ex = ret.exception orelse return error.MissingException;
    if (std.mem.indexOf(u8, ex.reason, needle) == null) return error.UnexpectedExceptionReason;
}

fn buildLoopPromise(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
    const ctx: *LoopPromiseContext = @ptrCast(@alignCast(ctx_ptr));
    const state = ctx.state;
    const layout = state.schema.loop_promise_params;
    var params_builder = try call.initParamsStruct(layout.data_words, layout.pointer_words);
    const cb_ptr = try params_builder.getAnyPointer(layout.pointer_index);
    try cb_ptr.setCapability(.{ .id = ctx.callback_export_id });
}

fn buildEmptyCallParams(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
    _ = ctx_ptr;
    _ = try call.initParamsStruct(0, 0);
}

fn buildCallbackPingReturn(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
    const state: *PromiseState = @ptrCast(@alignCast(ctx_ptr));
    const layout = state.schema.callback_ping_results;
    var results = try ret.initResultsStruct(layout.data_words, layout.pointer_words);
    results.writeU64(layout.field_offset, @bitCast(state.callback_value));
}

fn onCallbackPingCall(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    call: protocol.Call,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    _ = caps;
    const state: *PromiseState = @ptrCast(@alignCast(ctx_ptr));
    if (call.interface_id != state.schema.callback_interface_id or call.method_id != state.schema.callback_ping_id) {
        try peer.sendReturnException(call.question_id, "unexpected callback method");
        return;
    }
    try peer.sendReturnResults(call.question_id, state, buildCallbackPingReturn);
}

fn onLoopPingReturn(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    _ = caps;
    const ctx: *LoopPingContext = @ptrCast(@alignCast(ctx_ptr));
    defer peer.allocator.destroy(ctx);

    const state = ctx.state;
    if (ret.tag != .results) return error.UnexpectedReturn;
    const payload = ret.results orelse return error.MissingReturnPayload;
    const reader = try payload.content.getStruct();
    const value = @as(i64, @bitCast(reader.readU64(state.schema.callback_ping_results.field_offset)));
    if (value != state.callback_value) return error.UnexpectedProduct;
    state.done = true;
}

fn onLoopPromiseReturn(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    const ctx: *LoopPromiseContext = @ptrCast(@alignCast(ctx_ptr));
    defer peer.allocator.destroy(ctx);

    if (ret.tag != .results) return error.UnexpectedReturn;
    const payload = ret.results orelse return error.MissingReturnPayload;
    const result_struct = try payload.content.getStruct();
    const cap_ptr = try result_struct.readAnyPointer(ctx.state.schema.loop_promise_results.pointer_index);
    const cap = try cap_ptr.getCapability();
    const resolved = try caps.resolveCapability(cap);
    const promised_cap_id = switch (resolved) {
        .imported => |imported| imported.id,
        else => return error.UnexpectedBootstrapCapability,
    };

    const ping_ctx = try peer.allocator.create(LoopPingContext);
    errdefer peer.allocator.destroy(ping_ctx);
    ping_ctx.* = .{ .state = ctx.state };
    _ = try peer.sendCall(
        promised_cap_id,
        ctx.state.schema.callback_interface_id,
        ctx.state.schema.callback_ping_id,
        ping_ctx,
        buildEmptyCallParams,
        onLoopPingReturn,
    );
}

fn onPromiseBootstrapReturn(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    const state: *PromiseState = @ptrCast(@alignCast(ctx_ptr));
    if (ret.tag != .results) return error.UnexpectedBootstrapReturn;
    const payload = ret.results orelse return error.MissingReturnPayload;
    const cap = try payload.content.getCapability();
    const resolved = try caps.resolveCapability(cap);
    const arith_cap_id = switch (resolved) {
        .imported => |imported| imported.id,
        else => return error.UnexpectedBootstrapCapability,
    };

    const callback_export_id = try peer.addExport(.{
        .ctx = state,
        .on_call = onCallbackPingCall,
    });

    const loop_ctx = try peer.allocator.create(LoopPromiseContext);
    errdefer peer.allocator.destroy(loop_ctx);
    loop_ctx.* = .{
        .state = state,
        .arith_cap_id = arith_cap_id,
        .callback_export_id = callback_export_id,
    };

    _ = try peer.sendCall(
        arith_cap_id,
        state.schema.arith_interface_id,
        state.schema.loop_promise_id,
        loop_ctx,
        buildLoopPromise,
        onLoopPromiseReturn,
    );
}

fn buildPromiseRaceLoopPromise(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
    const ctx: *PromiseRaceLoopContext = @ptrCast(@alignCast(ctx_ptr));
    const layout = ctx.state.schema.loop_promise_params;
    var params_builder = try call.initParamsStruct(layout.data_words, layout.pointer_words);
    const cb_ptr = try params_builder.getAnyPointer(layout.pointer_index);
    try cb_ptr.setCapability(.{ .id = ctx.callback_export_id });
}

fn buildPromiseRaceCallbackPingReturn(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
    const ctx: *PromiseRaceCallbackCtx = @ptrCast(@alignCast(ctx_ptr));
    const layout = ctx.state.schema.callback_ping_results;
    var results = try ret.initResultsStruct(layout.data_words, layout.pointer_words);
    results.writeU64(layout.field_offset, @bitCast(ctx.value));
}

fn onPromiseRaceCallbackPingCall(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    call: protocol.Call,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    _ = caps;
    const ctx: *PromiseRaceCallbackCtx = @ptrCast(@alignCast(ctx_ptr));
    const state = ctx.state;
    if (call.interface_id != state.schema.callback_interface_id or call.method_id != state.schema.callback_ping_id) {
        try peer.sendReturnException(call.question_id, "unexpected callback method");
        return;
    }
    try peer.sendReturnResults(call.question_id, ctx, buildPromiseRaceCallbackPingReturn);
}

fn onPromiseRacePingReturn(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    _ = caps;
    const ctx: *PromiseRacePingContext = @ptrCast(@alignCast(ctx_ptr));
    defer peer.allocator.destroy(ctx);

    if (ret.tag != .results) return error.UnexpectedReturn;
    const payload = ret.results orelse return error.MissingReturnPayload;
    const reader = try payload.content.getStruct();
    const value = @as(i64, @bitCast(reader.readU64(ctx.state.schema.callback_ping_results.field_offset)));
    if (value != ctx.expected_value) return error.UnexpectedProduct;

    try peer.releaseImport(ctx.promised_cap_id, 1);
    ctx.state.completed_pings += 1;
    if (ctx.state.completed_pings == @as(u8, @intCast(ctx.state.expected_values.len))) {
        ctx.state.done = true;
    }
}

fn onPromiseRaceLoopPromiseReturn(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    const ctx: *PromiseRaceLoopContext = @ptrCast(@alignCast(ctx_ptr));
    defer peer.allocator.destroy(ctx);

    if (ret.tag != .results) return error.UnexpectedReturn;
    const payload = ret.results orelse return error.MissingReturnPayload;
    const result_struct = try payload.content.getStruct();
    const cap_ptr = try result_struct.readAnyPointer(ctx.state.schema.loop_promise_results.pointer_index);
    const cap = try cap_ptr.getCapability();
    const resolved = try caps.resolveCapability(cap);
    const promised_cap_id = switch (resolved) {
        .imported => |imported| imported.id,
        else => return error.UnexpectedBootstrapCapability,
    };

    const ping_ctx = try peer.allocator.create(PromiseRacePingContext);
    errdefer peer.allocator.destroy(ping_ctx);
    ping_ctx.* = .{
        .state = ctx.state,
        .promised_cap_id = promised_cap_id,
        .expected_value = ctx.expected_value,
    };
    _ = try peer.sendCall(
        promised_cap_id,
        ctx.state.schema.callback_interface_id,
        ctx.state.schema.callback_ping_id,
        ping_ctx,
        buildEmptyCallParams,
        onPromiseRacePingReturn,
    );
}

fn onPromiseRaceBootstrapReturn(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    const state: *PromiseRaceState = @ptrCast(@alignCast(ctx_ptr));
    if (ret.tag != .results) return error.UnexpectedBootstrapReturn;
    const payload = ret.results orelse return error.MissingReturnPayload;
    const cap = try payload.content.getCapability();
    const resolved = try caps.resolveCapability(cap);
    const arith_cap_id = switch (resolved) {
        .imported => |imported| imported.id,
        else => return error.UnexpectedBootstrapCapability,
    };

    var idx: usize = 0;
    while (idx < state.expected_values.len) : (idx += 1) {
        const callback_ctx = state.callback_contexts[idx] orelse return error.MissingCallbackContext;
        const callback_export_id = try peer.addExport(.{
            .ctx = callback_ctx,
            .on_call = onPromiseRaceCallbackPingCall,
        });

        const loop_ctx = try peer.allocator.create(PromiseRaceLoopContext);
        errdefer peer.allocator.destroy(loop_ctx);
        loop_ctx.* = .{
            .state = state,
            .callback_export_id = callback_export_id,
            .expected_value = state.expected_values[idx],
        };
        _ = try peer.sendCall(
            arith_cap_id,
            state.schema.arith_interface_id,
            state.schema.loop_promise_id,
            loop_ctx,
            buildPromiseRaceLoopPromise,
            onPromiseRaceLoopPromiseReturn,
        );
    }
}

fn buildMultiplyThirdParty(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
    const ctx: *ThirdPartyCallContext = @ptrCast(@alignCast(ctx_ptr));
    const state = ctx.state;
    try call.setSendResultsToThirdPartyNull();
    var params_builder = try call.initParamsStruct(state.schema.multiply_params.data_words, state.schema.multiply_params.pointer_words);
    params_builder.writeU64(state.schema.multiply_params.a_offset, @bitCast(state.a));
    params_builder.writeU64(state.schema.multiply_params.b_offset, @bitCast(state.b));
}

fn onThirdPartyCallReturn(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    _ = caps;
    const ctx: *ThirdPartyCallContext = @ptrCast(@alignCast(ctx_ptr));
    defer peer.allocator.destroy(ctx);

    try expectExceptionContains(ret, "unimplemented");
    try peer.releaseImport(ctx.cap_id, 1);
    ctx.state.done = true;
}

fn onThirdPartyBootstrapReturn(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    const state: *ThirdPartyState = @ptrCast(@alignCast(ctx_ptr));
    if (ret.tag != .results) return error.UnexpectedBootstrapReturn;
    const payload = ret.results orelse return error.MissingReturnPayload;
    const cap = try payload.content.getCapability();
    try @constCast(caps).retainCapability(cap);
    const resolved = try caps.resolveCapability(cap);
    const cap_id = switch (resolved) {
        .imported => |imported| imported.id,
        else => return error.UnexpectedBootstrapCapability,
    };

    const call_ctx = try peer.allocator.create(ThirdPartyCallContext);
    call_ctx.* = .{
        .state = state,
        .cap_id = cap_id,
    };
    _ = try peer.sendCall(
        cap_id,
        state.schema.arith_interface_id,
        state.schema.multiply_id,
        call_ctx,
        buildMultiplyThirdParty,
        onThirdPartyCallReturn,
    );
}

fn buildDivideByZero(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
    const ctx: *ExceptionFollowupCallContext = @ptrCast(@alignCast(ctx_ptr));
    const state = ctx.state;
    var params_builder = try call.initParamsStruct(state.schema.divide_params.data_words, state.schema.divide_params.pointer_words);
    params_builder.writeU64(state.schema.divide_params.a_offset, @bitCast(@as(i64, 42)));
    params_builder.writeU64(state.schema.divide_params.b_offset, @bitCast(@as(i64, 0)));
}

fn buildMultiplyAfterException(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
    const ctx: *ExceptionFollowupCallContext = @ptrCast(@alignCast(ctx_ptr));
    const state = ctx.state;
    var params_builder = try call.initParamsStruct(state.schema.multiply_params.data_words, state.schema.multiply_params.pointer_words);
    params_builder.writeU64(state.schema.multiply_params.a_offset, @bitCast(@as(i64, 9)));
    params_builder.writeU64(state.schema.multiply_params.b_offset, @bitCast(@as(i64, 7)));
}

fn onExceptionMultiplyReturn(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    _ = caps;
    const ctx: *ExceptionFollowupCallContext = @ptrCast(@alignCast(ctx_ptr));
    defer peer.allocator.destroy(ctx);

    if (ret.tag != .results) return error.UnexpectedReturn;
    const payload = ret.results orelse return error.MissingReturnPayload;
    const reader = try payload.content.getStruct();
    const product = @as(i64, @bitCast(reader.readU64(ctx.state.schema.multiply_results.field_offset)));
    if (product != 63) return error.UnexpectedProduct;

    try peer.releaseImport(ctx.cap_id, 1);
    ctx.state.done = true;
}

fn onExceptionDivideReturn(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    _ = caps;
    const ctx: *ExceptionFollowupCallContext = @ptrCast(@alignCast(ctx_ptr));
    const state = ctx.state;
    defer peer.allocator.destroy(ctx);

    try expectExceptionContains(ret, "divide by zero");

    const multiply_ctx = try peer.allocator.create(ExceptionFollowupCallContext);
    multiply_ctx.* = .{
        .state = state,
        .cap_id = ctx.cap_id,
    };
    _ = try peer.sendCall(
        ctx.cap_id,
        state.schema.arith_interface_id,
        state.schema.multiply_id,
        multiply_ctx,
        buildMultiplyAfterException,
        onExceptionMultiplyReturn,
    );
}

fn onExceptionBootstrapReturn(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    const state: *ExceptionFollowupState = @ptrCast(@alignCast(ctx_ptr));
    if (ret.tag != .results) return error.UnexpectedBootstrapReturn;
    const payload = ret.results orelse return error.MissingReturnPayload;
    const cap = try payload.content.getCapability();
    try @constCast(caps).retainCapability(cap);
    const resolved = try caps.resolveCapability(cap);
    const cap_id = switch (resolved) {
        .imported => |imported| imported.id,
        else => return error.UnexpectedBootstrapCapability,
    };

    const divide_ctx = try peer.allocator.create(ExceptionFollowupCallContext);
    divide_ctx.* = .{
        .state = state,
        .cap_id = cap_id,
    };
    _ = try peer.sendCall(
        cap_id,
        state.schema.arith_interface_id,
        state.schema.divide_id,
        divide_ctx,
        buildDivideByZero,
        onExceptionDivideReturn,
    );
}

fn buildFinishEdgeMultiply(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
    const ctx: *FinishEdgeCallContext = @ptrCast(@alignCast(ctx_ptr));
    const state = ctx.state;
    var params_builder = try call.initParamsStruct(state.schema.multiply_params.data_words, state.schema.multiply_params.pointer_words);
    params_builder.writeU64(state.schema.multiply_params.a_offset, @bitCast(@as(i64, 13)));
    params_builder.writeU64(state.schema.multiply_params.b_offset, @bitCast(@as(i64, 11)));
}

fn buildFinishEdgeDivide(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
    const ctx: *FinishEdgeCallContext = @ptrCast(@alignCast(ctx_ptr));
    const state = ctx.state;
    var params_builder = try call.initParamsStruct(state.schema.divide_params.data_words, state.schema.divide_params.pointer_words);
    params_builder.writeU64(state.schema.divide_params.a_offset, @bitCast(@as(i64, 42)));
    params_builder.writeU64(state.schema.divide_params.b_offset, @bitCast(@as(i64, 8)));
}

fn onFinishEdgeCanceledCallReturn(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    _ = caps;
    const ctx: *FinishEdgeCallContext = @ptrCast(@alignCast(ctx_ptr));
    defer {
        if (ctx.state.pending_canceled_ctx) |pending| {
            if (pending == ctx) {
                ctx.state.pending_canceled_ctx = null;
                peer.allocator.destroy(ctx);
            }
        }
    }
    // This return races with the outbound Finish we send immediately after the call.
    ctx.state.canceled_return_seen = ret.tag == .results or ret.tag == .canceled or ret.tag == .exception;
}

fn onFinishEdgeDivideReturn(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    _ = caps;
    const ctx: *FinishEdgeCallContext = @ptrCast(@alignCast(ctx_ptr));
    defer peer.allocator.destroy(ctx);

    if (ret.tag != .results) return error.UnexpectedReturn;
    const payload = ret.results orelse return error.MissingReturnPayload;
    const reader = try payload.content.getStruct();
    const quotient = @as(i64, @bitCast(reader.readU64(ctx.state.schema.divide_results.field_offset)));
    const remainder = @as(i64, @bitCast(reader.readU64(ctx.state.schema.divide_results.field2_offset)));
    if (quotient != 5 or remainder != 2) return error.UnexpectedQuotient;

    try peer.releaseImport(ctx.cap_id, 1);
    ctx.state.done = true;
}

fn onFinishEdgeBootstrapReturn(
    ctx_ptr: *anyopaque,
    peer: *peer_mod.Peer,
    ret: protocol.Return,
    caps: *const cap_table.InboundCapTable,
) anyerror!void {
    const state: *FinishEdgeState = @ptrCast(@alignCast(ctx_ptr));
    if (ret.tag != .results) return error.UnexpectedBootstrapReturn;
    const payload = ret.results orelse return error.MissingReturnPayload;
    const cap = try payload.content.getCapability();
    try @constCast(caps).retainCapability(cap);
    const resolved = try caps.resolveCapability(cap);
    const cap_id = switch (resolved) {
        .imported => |imported| imported.id,
        else => return error.UnexpectedBootstrapCapability,
    };

    const canceled_ctx = try peer.allocator.create(FinishEdgeCallContext);
    errdefer peer.allocator.destroy(canceled_ctx);
    canceled_ctx.* = .{
        .state = state,
        .cap_id = cap_id,
    };
    state.pending_canceled_ctx = canceled_ctx;
    const canceled_question_id = try peer.sendCall(
        cap_id,
        state.schema.arith_interface_id,
        state.schema.multiply_id,
        canceled_ctx,
        buildFinishEdgeMultiply,
        onFinishEdgeCanceledCallReturn,
    );

    var finish_builder = protocol.MessageBuilder.init(peer.allocator);
    defer finish_builder.deinit();
    try finish_builder.buildFinish(canceled_question_id, false, false);
    const finish_frame = try finish_builder.finish();
    defer peer.allocator.free(finish_frame);
    try peer.conn.sendFrame(finish_frame);

    const divide_ctx = try peer.allocator.create(FinishEdgeCallContext);
    divide_ctx.* = .{
        .state = state,
        .cap_id = cap_id,
    };
    _ = try peer.sendCall(
        cap_id,
        state.schema.arith_interface_id,
        state.schema.divide_id,
        divide_ctx,
        buildFinishEdgeDivide,
        onFinishEdgeDivideReturn,
    );
}

fn buildMultiply(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
    const ctx: *CallContext = @ptrCast(@alignCast(ctx_ptr));
    const state = ctx.state;
    var params_builder = try call.initParamsStruct(state.schema.multiply_params.data_words, state.schema.multiply_params.pointer_words);
    params_builder.writeU64(state.schema.multiply_params.a_offset, @bitCast(state.a));
    params_builder.writeU64(state.schema.multiply_params.b_offset, @bitCast(state.b));
}

fn buildDivide(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
    const ctx: *CallContext = @ptrCast(@alignCast(ctx_ptr));
    const state = ctx.state;
    var params_builder = try call.initParamsStruct(state.schema.divide_params.data_words, state.schema.divide_params.pointer_words);
    params_builder.writeU64(state.schema.divide_params.a_offset, @bitCast(state.a));
    params_builder.writeU64(state.schema.divide_params.b_offset, @bitCast(state.b));
}

fn onMultiplyReturn(ctx_ptr: *anyopaque, peer: *peer_mod.Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
    const ctx: *CallContext = @ptrCast(@alignCast(ctx_ptr));
    defer peer.allocator.destroy(ctx);

    const state = ctx.state;
    if (ret.tag != .results) return error.UnexpectedReturn;
    const payload = ret.results orelse return error.MissingReturnPayload;
    const reader = try payload.content.getStruct();
    const product = @as(i64, @bitCast(reader.readU64(state.schema.multiply_results.field_offset)));
    const expected = state.a * state.b;
    if (product != expected) return error.UnexpectedProduct;

    const divide_ctx = try peer.allocator.create(CallContext);
    divide_ctx.* = .{ .state = state, .cap_id = ctx.cap_id, .kind = .divide };
    _ = try peer.sendCall(ctx.cap_id, state.schema.arith_interface_id, state.schema.divide_id, divide_ctx, buildDivide, onDivideReturn);
}

fn onDivideReturn(ctx_ptr: *anyopaque, peer: *peer_mod.Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
    const ctx: *CallContext = @ptrCast(@alignCast(ctx_ptr));
    defer peer.allocator.destroy(ctx);

    const state = ctx.state;
    if (ret.tag != .results) return error.UnexpectedReturn;
    const payload = ret.results orelse return error.MissingReturnPayload;
    const reader = try payload.content.getStruct();
    const quotient = @as(i64, @bitCast(reader.readU64(state.schema.divide_results.field_offset)));
    const remainder = @as(i64, @bitCast(reader.readU64(state.schema.divide_results.field2_offset)));
    const expected_q = @divTrunc(state.a, state.b);
    const expected_r = @mod(state.a, state.b);
    if (quotient != expected_q or remainder != expected_r) return error.UnexpectedQuotient;

    try peer.releaseImport(ctx.cap_id, 1);
    state.done = true;
}

fn onBootstrapReturn(ctx_ptr: *anyopaque, peer: *peer_mod.Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
    const state: *State = @ptrCast(@alignCast(ctx_ptr));
    if (ret.tag != .results) return error.UnexpectedBootstrapReturn;
    const payload = ret.results orelse return error.MissingReturnPayload;
    const cap = try payload.content.getCapability();
    try @constCast(caps).retainCapability(cap);
    const resolved = try caps.resolveCapability(cap);
    const cap_id = switch (resolved) {
        .imported => |imported| imported.id,
        else => return error.UnexpectedBootstrapCapability,
    };

    const ctx = try peer.allocator.create(CallContext);
    ctx.* = .{ .state = state, .cap_id = cap_id, .kind = .multiply };
    _ = try peer.sendCall(cap_id, state.schema.arith_interface_id, state.schema.multiply_id, ctx, buildMultiply, onMultiplyReturn);
}

test "RPC interop with reference server" {
    const allocator = std.testing.allocator;
    const backend = try resolveInteropBackend(allocator);

    const capnp_go_std = try resolveCapnpGoStd(allocator);
    defer allocator.free(capnp_go_std);

    const schema_info = try loadSchemaInfo(allocator, capnp_go_std, "arith/arith.capnp", "tests/interop_rpc");

    // Reserve an ephemeral port.
    const addr = try std.net.Address.parseIp4("127.0.0.1", 0);
    var listener = try std.net.Address.listen(addr, .{});
    const port = listener.listen_address.getPort();
    listener.deinit();

    const addr_string = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_string);

    const target_addr = try std.net.Address.parseIp4("127.0.0.1", port);

    var child = try spawnInteropServer(allocator, addr_string, backend);
    defer {
        _ = child.kill() catch {};
        _ = child.wait() catch {};
    }

    try waitForServerReady(target_addr, 5000);

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var state = State{
        .allocator = allocator,
        .loop = &loop,
        .schema = schema_info,
        .a = 42,
        .b = 8,
    };
    g_state = &state;
    defer g_state = null;

    var socket = try xev.TCP.init(target_addr);
    var connect_completion: xev.Completion = .{};

    const ConnectCtx = struct { state: *State };
    var connect_ctx = ConnectCtx{ .state = &state };

    socket.connect(&loop, &connect_completion, target_addr, ConnectCtx, &connect_ctx, struct {
        fn onConnect(
            ctx: ?*ConnectCtx,
            loop_ptr: *xev.Loop,
            _: *xev.Completion,
            s: xev.TCP,
            res: xev.ConnectError!void,
        ) xev.CallbackAction {
            const connect_state = ctx.?.state;
            if (res) |_| {
                const conn_ptr = connect_state.allocator.create(connection_mod.Connection) catch {
                    connect_state.err = error.OutOfMemory;
                    connect_state.done = true;
                    return .disarm;
                };
                conn_ptr.* = connection_mod.Connection.init(connect_state.allocator, loop_ptr, s, .{}) catch |err| {
                    connect_state.allocator.destroy(conn_ptr);
                    connect_state.err = err;
                    connect_state.done = true;
                    return .disarm;
                };

                const peer_ptr = connect_state.allocator.create(peer_mod.Peer) catch {
                    connect_state.err = error.OutOfMemory;
                    connect_state.done = true;
                    return .disarm;
                };
                peer_ptr.* = peer_mod.Peer.init(connect_state.allocator, conn_ptr);
                connect_state.conn = conn_ptr;
                connect_state.peer = peer_ptr;

                peer_ptr.start(onPeerError, onPeerClose);
                _ = peer_ptr.sendBootstrap(connect_state, onBootstrapReturn) catch |err| {
                    connect_state.err = err;
                    connect_state.done = true;
                };
            } else |err| {
                connect_state.err = err;
                connect_state.done = true;
            }
            return .disarm;
        }
    }.onConnect);

    const start_time = std.time.milliTimestamp();
    while (!state.done) {
        try loop.run(.once);
        if (std.time.milliTimestamp() - start_time > 2000) {
            return error.Timeout;
        }
    }

    if (state.err) |err| {
        std.log.err("RPC interop state error: {s}", .{@errorName(err)});
        if (err == error.RemoteAbort) {
            if (state.peer) |peer| {
                if (peer.getLastInboundTag()) |tag| {
                    std.log.err("RPC interop last inbound tag: {s}", .{@tagName(tag)});
                }
                if (peer.getLastRemoteAbortReason()) |reason| {
                    std.log.err("RPC interop remote abort reason: {s}", .{reason});
                }
            }
        }
        return err;
    }

    if (state.peer) |peer| {
        peer.deinit();
        allocator.destroy(peer);
    }
    if (state.conn) |conn| {
        conn.deinit();
        allocator.destroy(conn);
    }
}

test "RPC interop senderPromise resolve disembargo with Go server" {
    const allocator = std.testing.allocator;
    const backend = try resolveInteropBackend(allocator);
    if (backend == .cpp) return error.SkipZigTest;

    const capnp_go_std = try resolveCapnpGoStd(allocator);
    defer allocator.free(capnp_go_std);

    const schema_info = try loadSchemaInfo(allocator, capnp_go_std, "arith/arith.capnp", "tests/interop_rpc");

    const addr = try std.net.Address.parseIp4("127.0.0.1", 0);
    var listener = try std.net.Address.listen(addr, .{});
    const port = listener.listen_address.getPort();
    listener.deinit();

    const addr_string = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_string);

    const target_addr = try std.net.Address.parseIp4("127.0.0.1", port);

    var child = try spawnInteropServer(allocator, addr_string, backend);
    defer {
        _ = child.kill() catch {};
        _ = child.wait() catch {};
    }

    try waitForServerReady(target_addr, 5000);

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var state = PromiseState{
        .allocator = allocator,
        .loop = &loop,
        .schema = schema_info,
        .callback_value = 41,
    };
    g_promise_state = &state;
    defer g_promise_state = null;

    var socket = try xev.TCP.init(target_addr);
    var connect_completion: xev.Completion = .{};

    const ConnectCtx = struct { state: *PromiseState };
    var connect_ctx = ConnectCtx{ .state = &state };

    socket.connect(&loop, &connect_completion, target_addr, ConnectCtx, &connect_ctx, struct {
        fn onConnect(
            ctx: ?*ConnectCtx,
            loop_ptr: *xev.Loop,
            _: *xev.Completion,
            s: xev.TCP,
            res: xev.ConnectError!void,
        ) xev.CallbackAction {
            const connect_state = ctx.?.state;
            if (res) |_| {
                const conn_ptr = connect_state.allocator.create(connection_mod.Connection) catch {
                    connect_state.err = error.OutOfMemory;
                    connect_state.done = true;
                    return .disarm;
                };
                conn_ptr.* = connection_mod.Connection.init(connect_state.allocator, loop_ptr, s, .{}) catch |err| {
                    connect_state.allocator.destroy(conn_ptr);
                    connect_state.err = err;
                    connect_state.done = true;
                    return .disarm;
                };

                const peer_ptr = connect_state.allocator.create(peer_mod.Peer) catch {
                    connect_state.err = error.OutOfMemory;
                    connect_state.done = true;
                    return .disarm;
                };
                peer_ptr.* = peer_mod.Peer.init(connect_state.allocator, conn_ptr);
                connect_state.conn = conn_ptr;
                connect_state.peer = peer_ptr;

                peer_ptr.start(onPromisePeerError, onPromisePeerClose);
                _ = peer_ptr.sendBootstrap(connect_state, onPromiseBootstrapReturn) catch |err| {
                    connect_state.err = err;
                    connect_state.done = true;
                };
            } else |err| {
                connect_state.err = err;
                connect_state.done = true;
            }
            return .disarm;
        }
    }.onConnect);

    const start_time = std.time.milliTimestamp();
    while (!state.done) {
        try loop.run(.once);
        if (std.time.milliTimestamp() - start_time > 2500) {
            return error.Timeout;
        }
    }

    if (state.err) |err| {
        std.log.err("RPC senderPromise interop state error: {s}", .{@errorName(err)});
        if (err == error.RemoteAbort) {
            if (state.peer) |peer| {
                if (peer.getLastInboundTag()) |tag| {
                    std.log.err("RPC senderPromise last inbound tag: {s}", .{@tagName(tag)});
                }
                if (peer.getLastRemoteAbortReason()) |reason| {
                    std.log.err("RPC senderPromise remote abort reason: {s}", .{reason});
                }
            }
        }
        return err;
    }

    if (state.peer) |peer| {
        peer.deinit();
        allocator.destroy(peer);
    }
    if (state.conn) |conn| {
        conn.deinit();
        allocator.destroy(conn);
    }
}

test "RPC interop loopPromise race with Go server" {
    const allocator = std.testing.allocator;
    const backend = try resolveInteropBackend(allocator);
    if (backend == .cpp) return error.SkipZigTest;

    const capnp_go_std = try resolveCapnpGoStd(allocator);
    defer allocator.free(capnp_go_std);

    const schema_info = try loadSchemaInfo(allocator, capnp_go_std, "arith/arith.capnp", "tests/interop_rpc");

    const addr = try std.net.Address.parseIp4("127.0.0.1", 0);
    var listener = try std.net.Address.listen(addr, .{});
    const port = listener.listen_address.getPort();
    listener.deinit();

    const addr_string = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_string);

    const target_addr = try std.net.Address.parseIp4("127.0.0.1", port);
    var child = try spawnInteropServer(allocator, addr_string, backend);
    defer {
        _ = child.kill() catch {};
        _ = child.wait() catch {};
    }

    try waitForServerReady(target_addr, 5000);

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var state = PromiseRaceState{
        .allocator = allocator,
        .loop = &loop,
        .schema = schema_info,
        .expected_values = .{ 41, 7 },
    };
    var callback_ctx0 = PromiseRaceCallbackCtx{ .state = &state, .value = 41 };
    var callback_ctx1 = PromiseRaceCallbackCtx{ .state = &state, .value = 7 };
    state.callback_contexts = .{ &callback_ctx0, &callback_ctx1 };

    g_promise_race_state = &state;
    defer g_promise_race_state = null;

    var socket = try xev.TCP.init(target_addr);
    var connect_completion: xev.Completion = .{};

    const ConnectCtx = struct { state: *PromiseRaceState };
    var connect_ctx = ConnectCtx{ .state = &state };

    socket.connect(&loop, &connect_completion, target_addr, ConnectCtx, &connect_ctx, struct {
        fn onConnect(
            ctx: ?*ConnectCtx,
            loop_ptr: *xev.Loop,
            _: *xev.Completion,
            s: xev.TCP,
            res: xev.ConnectError!void,
        ) xev.CallbackAction {
            const connect_state = ctx.?.state;
            if (res) |_| {
                const conn_ptr = connect_state.allocator.create(connection_mod.Connection) catch {
                    connect_state.err = error.OutOfMemory;
                    connect_state.done = true;
                    return .disarm;
                };
                conn_ptr.* = connection_mod.Connection.init(connect_state.allocator, loop_ptr, s, .{}) catch |err| {
                    connect_state.allocator.destroy(conn_ptr);
                    connect_state.err = err;
                    connect_state.done = true;
                    return .disarm;
                };

                const peer_ptr = connect_state.allocator.create(peer_mod.Peer) catch {
                    connect_state.err = error.OutOfMemory;
                    connect_state.done = true;
                    return .disarm;
                };
                peer_ptr.* = peer_mod.Peer.init(connect_state.allocator, conn_ptr);
                connect_state.conn = conn_ptr;
                connect_state.peer = peer_ptr;

                peer_ptr.start(onPromiseRacePeerError, onPromiseRacePeerClose);
                _ = peer_ptr.sendBootstrap(connect_state, onPromiseRaceBootstrapReturn) catch |err| {
                    connect_state.err = err;
                    connect_state.done = true;
                };
            } else |err| {
                connect_state.err = err;
                connect_state.done = true;
            }
            return .disarm;
        }
    }.onConnect);

    const start_time = std.time.milliTimestamp();
    while (!state.done) {
        try loop.run(.once);
        if (std.time.milliTimestamp() - start_time > 3500) return error.Timeout;
    }

    if (state.err) |err| {
        std.log.err("RPC loopPromise race interop state error: {s}", .{@errorName(err)});
        if (err == error.RemoteAbort) {
            if (state.peer) |peer| {
                if (peer.getLastInboundTag()) |tag| {
                    std.log.err("RPC loopPromise race last inbound tag: {s}", .{@tagName(tag)});
                }
                if (peer.getLastRemoteAbortReason()) |reason| {
                    std.log.err("RPC loopPromise race remote abort reason: {s}", .{reason});
                }
            }
        }
        return err;
    }

    if (state.completed_pings != 2) return error.UnexpectedProduct;

    if (state.peer) |peer| {
        peer.deinit();
        allocator.destroy(peer);
    }
    if (state.conn) |conn| {
        conn.deinit();
        allocator.destroy(conn);
    }
}

test "RPC interop third-party sendResultsTo compatibility with Go server" {
    const allocator = std.testing.allocator;
    const backend = try resolveInteropBackend(allocator);
    if (backend == .cpp) return error.SkipZigTest;

    const capnp_go_std = try resolveCapnpGoStd(allocator);
    defer allocator.free(capnp_go_std);

    const schema_info = try loadSchemaInfo(allocator, capnp_go_std, "arith/arith.capnp", "tests/interop_rpc");

    const addr = try std.net.Address.parseIp4("127.0.0.1", 0);
    var listener = try std.net.Address.listen(addr, .{});
    const port = listener.listen_address.getPort();
    listener.deinit();

    const addr_string = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_string);

    const target_addr = try std.net.Address.parseIp4("127.0.0.1", port);
    var child = try spawnInteropServer(allocator, addr_string, backend);
    defer {
        _ = child.kill() catch {};
        _ = child.wait() catch {};
    }

    try waitForServerReady(target_addr, 5000);

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var state = ThirdPartyState{
        .allocator = allocator,
        .loop = &loop,
        .schema = schema_info,
        .a = 6,
        .b = 7,
    };
    g_third_party_state = &state;
    defer g_third_party_state = null;

    var socket = try xev.TCP.init(target_addr);
    var connect_completion: xev.Completion = .{};

    const ConnectCtx = struct { state: *ThirdPartyState };
    var connect_ctx = ConnectCtx{ .state = &state };

    socket.connect(&loop, &connect_completion, target_addr, ConnectCtx, &connect_ctx, struct {
        fn onConnect(
            ctx: ?*ConnectCtx,
            loop_ptr: *xev.Loop,
            _: *xev.Completion,
            s: xev.TCP,
            res: xev.ConnectError!void,
        ) xev.CallbackAction {
            const connect_state = ctx.?.state;
            if (res) |_| {
                const conn_ptr = connect_state.allocator.create(connection_mod.Connection) catch {
                    connect_state.err = error.OutOfMemory;
                    connect_state.done = true;
                    return .disarm;
                };
                conn_ptr.* = connection_mod.Connection.init(connect_state.allocator, loop_ptr, s, .{}) catch |err| {
                    connect_state.allocator.destroy(conn_ptr);
                    connect_state.err = err;
                    connect_state.done = true;
                    return .disarm;
                };

                const peer_ptr = connect_state.allocator.create(peer_mod.Peer) catch {
                    connect_state.err = error.OutOfMemory;
                    connect_state.done = true;
                    return .disarm;
                };
                peer_ptr.* = peer_mod.Peer.init(connect_state.allocator, conn_ptr);
                connect_state.conn = conn_ptr;
                connect_state.peer = peer_ptr;

                peer_ptr.start(onThirdPartyPeerError, onThirdPartyPeerClose);
                _ = peer_ptr.sendBootstrap(connect_state, onThirdPartyBootstrapReturn) catch |err| {
                    connect_state.err = err;
                    connect_state.done = true;
                };
            } else |err| {
                connect_state.err = err;
                connect_state.done = true;
            }
            return .disarm;
        }
    }.onConnect);

    const start_time = std.time.milliTimestamp();
    while (!state.done) {
        try loop.run(.once);
        if (std.time.milliTimestamp() - start_time > 2500) return error.Timeout;
    }

    if (state.err) |err| {
        std.log.err("RPC third-party interop state error: {s}", .{@errorName(err)});
        if (err == error.RemoteAbort) {
            if (state.peer) |peer| {
                if (peer.getLastInboundTag()) |tag| {
                    std.log.err("RPC third-party last inbound tag: {s}", .{@tagName(tag)});
                }
                if (peer.getLastRemoteAbortReason()) |reason| {
                    std.log.err("RPC third-party remote abort reason: {s}", .{reason});
                }
            }
        }
        return err;
    }

    if (state.peer) |peer| {
        peer.deinit();
        allocator.destroy(peer);
    }
    if (state.conn) |conn| {
        conn.deinit();
        allocator.destroy(conn);
    }
}

test "RPC interop exception path remains healthy for follow-up call" {
    const allocator = std.testing.allocator;
    const backend = try resolveInteropBackend(allocator);

    const capnp_go_std = try resolveCapnpGoStd(allocator);
    defer allocator.free(capnp_go_std);

    const schema_info = try loadSchemaInfo(allocator, capnp_go_std, "arith/arith.capnp", "tests/interop_rpc");

    const addr = try std.net.Address.parseIp4("127.0.0.1", 0);
    var listener = try std.net.Address.listen(addr, .{});
    const port = listener.listen_address.getPort();
    listener.deinit();

    const addr_string = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_string);

    const target_addr = try std.net.Address.parseIp4("127.0.0.1", port);
    var child = try spawnInteropServer(allocator, addr_string, backend);
    defer {
        _ = child.kill() catch {};
        _ = child.wait() catch {};
    }

    try waitForServerReady(target_addr, 5000);

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var state = ExceptionFollowupState{
        .allocator = allocator,
        .loop = &loop,
        .schema = schema_info,
    };
    g_exception_followup_state = &state;
    defer g_exception_followup_state = null;

    var socket = try xev.TCP.init(target_addr);
    var connect_completion: xev.Completion = .{};

    const ConnectCtx = struct { state: *ExceptionFollowupState };
    var connect_ctx = ConnectCtx{ .state = &state };

    socket.connect(&loop, &connect_completion, target_addr, ConnectCtx, &connect_ctx, struct {
        fn onConnect(
            ctx: ?*ConnectCtx,
            loop_ptr: *xev.Loop,
            _: *xev.Completion,
            s: xev.TCP,
            res: xev.ConnectError!void,
        ) xev.CallbackAction {
            const connect_state = ctx.?.state;
            if (res) |_| {
                const conn_ptr = connect_state.allocator.create(connection_mod.Connection) catch {
                    connect_state.err = error.OutOfMemory;
                    connect_state.done = true;
                    return .disarm;
                };
                conn_ptr.* = connection_mod.Connection.init(connect_state.allocator, loop_ptr, s, .{}) catch |err| {
                    connect_state.allocator.destroy(conn_ptr);
                    connect_state.err = err;
                    connect_state.done = true;
                    return .disarm;
                };

                const peer_ptr = connect_state.allocator.create(peer_mod.Peer) catch {
                    connect_state.err = error.OutOfMemory;
                    connect_state.done = true;
                    return .disarm;
                };
                peer_ptr.* = peer_mod.Peer.init(connect_state.allocator, conn_ptr);
                connect_state.conn = conn_ptr;
                connect_state.peer = peer_ptr;

                peer_ptr.start(onExceptionFollowupPeerError, onExceptionFollowupPeerClose);
                _ = peer_ptr.sendBootstrap(connect_state, onExceptionBootstrapReturn) catch |err| {
                    connect_state.err = err;
                    connect_state.done = true;
                };
            } else |err| {
                connect_state.err = err;
                connect_state.done = true;
            }
            return .disarm;
        }
    }.onConnect);

    const start_time = std.time.milliTimestamp();
    while (!state.done) {
        try loop.run(.once);
        if (std.time.milliTimestamp() - start_time > 2500) return error.Timeout;
    }

    if (state.err) |err| {
        std.log.err("RPC exception-followup interop state error: {s}", .{@errorName(err)});
        if (err == error.RemoteAbort) {
            if (state.peer) |peer| {
                if (peer.getLastInboundTag()) |tag| {
                    std.log.err("RPC exception-followup last inbound tag: {s}", .{@tagName(tag)});
                }
                if (peer.getLastRemoteAbortReason()) |reason| {
                    std.log.err("RPC exception-followup remote abort reason: {s}", .{reason});
                }
            }
        }
        return err;
    }

    if (state.peer) |peer| {
        peer.deinit();
        allocator.destroy(peer);
    }
    if (state.conn) |conn| {
        conn.deinit();
        allocator.destroy(conn);
    }
}

test "RPC interop early finish on one question does not break follow-up calls" {
    const allocator = std.testing.allocator;
    const backend = try resolveInteropBackend(allocator);

    const capnp_go_std = try resolveCapnpGoStd(allocator);
    defer allocator.free(capnp_go_std);

    const schema_info = try loadSchemaInfo(allocator, capnp_go_std, "arith/arith.capnp", "tests/interop_rpc");

    const addr = try std.net.Address.parseIp4("127.0.0.1", 0);
    var listener = try std.net.Address.listen(addr, .{});
    const port = listener.listen_address.getPort();
    listener.deinit();

    const addr_string = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_string);

    const target_addr = try std.net.Address.parseIp4("127.0.0.1", port);
    var child = try spawnInteropServer(allocator, addr_string, backend);
    defer {
        _ = child.kill() catch {};
        _ = child.wait() catch {};
    }

    try waitForServerReady(target_addr, 5000);

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var state = FinishEdgeState{
        .allocator = allocator,
        .loop = &loop,
        .schema = schema_info,
    };
    g_finish_edge_state = &state;
    defer g_finish_edge_state = null;

    var socket = try xev.TCP.init(target_addr);
    var connect_completion: xev.Completion = .{};

    const ConnectCtx = struct { state: *FinishEdgeState };
    var connect_ctx = ConnectCtx{ .state = &state };

    socket.connect(&loop, &connect_completion, target_addr, ConnectCtx, &connect_ctx, struct {
        fn onConnect(
            ctx: ?*ConnectCtx,
            loop_ptr: *xev.Loop,
            _: *xev.Completion,
            s: xev.TCP,
            res: xev.ConnectError!void,
        ) xev.CallbackAction {
            const connect_state = ctx.?.state;
            if (res) |_| {
                const conn_ptr = connect_state.allocator.create(connection_mod.Connection) catch {
                    connect_state.err = error.OutOfMemory;
                    connect_state.done = true;
                    return .disarm;
                };
                conn_ptr.* = connection_mod.Connection.init(connect_state.allocator, loop_ptr, s, .{}) catch |err| {
                    connect_state.allocator.destroy(conn_ptr);
                    connect_state.err = err;
                    connect_state.done = true;
                    return .disarm;
                };

                const peer_ptr = connect_state.allocator.create(peer_mod.Peer) catch {
                    connect_state.err = error.OutOfMemory;
                    connect_state.done = true;
                    return .disarm;
                };
                peer_ptr.* = peer_mod.Peer.init(connect_state.allocator, conn_ptr);
                connect_state.conn = conn_ptr;
                connect_state.peer = peer_ptr;

                peer_ptr.start(onFinishEdgePeerError, onFinishEdgePeerClose);
                _ = peer_ptr.sendBootstrap(connect_state, onFinishEdgeBootstrapReturn) catch |err| {
                    connect_state.err = err;
                    connect_state.done = true;
                };
            } else |err| {
                connect_state.err = err;
                connect_state.done = true;
            }
            return .disarm;
        }
    }.onConnect);

    const start_time = std.time.milliTimestamp();
    while (!state.done) {
        try loop.run(.once);
        if (std.time.milliTimestamp() - start_time > 3000) return error.Timeout;
    }

    if (state.err) |err| {
        std.log.err("RPC finish-edge interop state error: {s}", .{@errorName(err)});
        if (err == error.RemoteAbort) {
            if (state.peer) |peer| {
                if (peer.getLastInboundTag()) |tag| {
                    std.log.err("RPC finish-edge last inbound tag: {s}", .{@tagName(tag)});
                }
                if (peer.getLastRemoteAbortReason()) |reason| {
                    std.log.err("RPC finish-edge remote abort reason: {s}", .{reason});
                }
            }
        }
        return err;
    }

    if (state.pending_canceled_ctx) |pending| {
        if (state.peer) |peer| {
            peer.allocator.destroy(pending);
        }
        state.pending_canceled_ctx = null;
    }

    if (state.peer) |peer| {
        peer.deinit();
        allocator.destroy(peer);
    }
    if (state.conn) |conn| {
        conn.deinit();
        allocator.destroy(conn);
    }
}
