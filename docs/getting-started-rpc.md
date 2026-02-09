# Getting Started: RPC with capnpc-zig

This guide walks you through defining a Cap'n Proto RPC interface and building a working client-server application in Zig. It assumes you've read the [serialization guide](getting-started-serialization.md).

> **Status:** The RPC runtime is experimental. The API may change.

## Prerequisites

- **Zig 0.15.2** (use `mise install` if you have mise)
- **Cap'n Proto compiler** (`capnp`) — for schema compilation
- **capnpc-zig** — built from this repo (`zig build`)

## 1. Define Your Interface

Create `calculator.capnp`:

```capnp
@0xb6a52c8e4b3d7f01;

interface Calculator {
  add @0 (a :Int32, b :Int32) -> (result :Int32);
  multiply @1 (a :Int32, b :Int32) -> (result :Int32);
}
```

Each method has:
- An **ordinal** (`@0`, `@1`) — the method ID on the wire
- **Parameters** — compiled into a `Params` struct
- **Results** — compiled into a `Results` struct

## 2. Generate Zig Code

```bash
capnp compile -o ./zig-out/bin/capnpc-zig calculator.capnp
```

This generates `calculator.zig` containing:

```zig
pub const Calculator = struct {
    pub const interface_id: u64 = 0x...;

    // Method enum
    pub const Method = enum(u16) { Add = 0, Multiply = 1 };

    // Per-method types (for each method: Add, Multiply)
    pub const Add = struct {
        pub const Params = AddParams;
        pub const Results = AddResults;
        pub const BuildFn = *const fn (ctx: *anyopaque, params: *Params.Builder) anyerror!void;
        pub const Handler = *const fn (ctx: *anyopaque, peer: *rpc.peer.Peer, params: Params.Reader, results: *Results.Builder, caps: *const rpc.cap_table.InboundCapTable) anyerror!void;
        pub const Response = union(enum) {
            results: Results.Reader,
            exception: rpc.protocol.Exception,
            canceled,
            // ...
        };
        pub const Callback = *const fn (ctx: *anyopaque, peer: *rpc.peer.Peer, response: Response, caps: *const rpc.cap_table.InboundCapTable) anyerror!void;
    };

    // Client — for making outbound calls
    pub const Client = struct {
        pub fn callAdd(self: *Client, ctx: *anyopaque, build: ?Add.BuildFn, on_return: Add.Callback) !u32 { ... }
        pub fn callMultiply(self: *Client, ctx: *anyopaque, build: ?Multiply.BuildFn, on_return: Multiply.Callback) !u32 { ... }
        pub fn fromBootstrap(peer: *rpc.peer.Peer, ctx: *anyopaque, callback: BootstrapCallback) !u32 { ... }
    };

    // Server — for handling inbound calls
    pub const Server = struct {
        ctx: *anyopaque,
        vtable: VTable,
    };

    pub const VTable = struct {
        add: Add.Handler,
        multiply: Multiply.Handler,
    };

    pub fn setBootstrap(peer: *rpc.peer.Peer, server: *Server) !u32 { ... }
    pub fn exportServer(peer: *rpc.peer.Peer, server: *Server) !u32 { ... }
};

// Auto-generated param/result structs
pub const AddParams = struct {
    pub const Reader = struct { pub fn getA(self: Reader) !i32 { ... } ... };
    pub const Builder = struct { pub fn setA(self: *Builder, value: i32) !void { ... } ... };
};
pub const AddResults = struct { ... };
// ...
```

The key generated pieces are:
- **`Client`** — call remote methods; each method gets a `callMethodName()` function
- **`Server` + `VTable`** — implement methods locally; fill in one handler per method
- **`BootstrapResponse`** — union returned when you first connect and request the remote's root capability

## 3. Implement the Server

A server implements the VTable — one function per interface method:

```zig
const calculator = @import("calculator");
const Calculator = calculator.Calculator;

fn handleAdd(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: Calculator.Add.Params.Reader,
    results: *Calculator.Add.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    _ = ctx_ptr;
    const a = try params.getA();
    const b = try params.getB();
    try results.setResult(a + b);
}

fn handleMultiply(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    params: Calculator.Multiply.Params.Reader,
    results: *Calculator.Multiply.Results.Builder,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    _ = ctx_ptr;
    const a = try params.getA();
    const b = try params.getB();
    try results.setResult(a * b);
}
```

Each handler:
- Reads parameters from `params` (a `Reader`)
- Writes results into `results` (a `*Builder`)
- Is called synchronously by the RPC runtime when a matching inbound call arrives
- Can return an error, which the runtime converts to an RPC exception sent back to the caller

## 4. Set Up the RPC Runtime

The RPC runtime uses [libxev](https://github.com/mitchellh/libxev) for async I/O. Everything runs on a single-threaded event loop.

```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");
const xev = @import("xev");

const rpc = capnpc.rpc;
const calculator = @import("calculator");
const Calculator = calculator.Calculator;

var g_state: ?*State = null;

const State = struct {
    allocator: std.mem.Allocator,
    done: bool = false,
    err: ?anyerror = null,
    server_peer: ?*rpc.peer.Peer = null,
    client_peer: ?*rpc.peer.Peer = null,
};

fn onPeerError(peer: *rpc.peer.Peer, err: anyerror) void {
    _ = peer;
    if (g_state) |state| {
        state.err = err;
        state.done = true;
    }
}

fn onPeerClose(peer: *rpc.peer.Peer) void {
    _ = peer;
    if (g_state) |state| state.done = true;
}
```

### Start a listener (server side)

```zig
const ServerCtx = struct {
    listener: rpc.runtime.Listener,
    state: *State,
    server: Calculator.Server,
};

fn onAccept(listener: *rpc.runtime.Listener, conn: *rpc.connection.Connection) void {
    const server_ctx: *ServerCtx = @fieldParentPtr("listener", listener);
    const state = server_ctx.state;

    const peer = state.allocator.create(rpc.peer.Peer) catch {
        state.err = error.OutOfMemory;
        state.done = true;
        return;
    };
    peer.* = rpc.peer.Peer.init(state.allocator, conn);
    state.server_peer = peer;

    // Register the Calculator as the bootstrap capability
    _ = Calculator.setBootstrap(peer, &server_ctx.server) catch |err| {
        state.err = err;
        state.done = true;
        return;
    };

    peer.start(onPeerError, onPeerClose);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var runtime = try rpc.runtime.Runtime.init(allocator);
    defer runtime.deinit();

    var state = State{ .allocator = allocator };
    g_state = &state;

    const addr = try std.net.Address.parseIp4("127.0.0.1", 7001);

    var server_ctx = ServerCtx{
        .state = &state,
        .server = Calculator.Server{
            .ctx = &state,
            .vtable = .{
                .add = handleAdd,
                .multiply = handleMultiply,
            },
        },
        .listener = try rpc.runtime.Listener.init(
            allocator, &runtime.loop, addr, onAccept, .{},
        ),
    };
    server_ctx.listener.start();

    // ... (connect client, then run event loop) ...

    while (!state.done) {
        try runtime.loop.run(.once);
    }
}
```

## 5. Connect a Client

The client connects via TCP, then requests the **bootstrap capability** — the root interface the server offers:

```zig
fn onBootstrap(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: Calculator.BootstrapResponse,
) anyerror!void {
    const state: *State = @ptrCast(@alignCast(ctx_ptr));
    switch (response) {
        .client => |client| {
            // We now have a Calculator.Client — make a call
            var c = client;
            const call_ctx = try peer.allocator.create(CallCtx);
            call_ctx.* = .{ .state = state };
            _ = try c.callAdd(call_ctx, buildAdd, onAddReturn);
        },
        .exception => return error.BootstrapFailed,
        else => return error.UnexpectedResponse,
    }
}
```

Initiate the connection (inside an xev connect callback):

```zig
const conn = try allocator.create(rpc.connection.Connection);
conn.* = try rpc.connection.Connection.init(allocator, loop, socket, .{});

const peer = try allocator.create(rpc.peer.Peer);
peer.* = rpc.peer.Peer.init(allocator, conn);

peer.start(onPeerError, onPeerClose);

// Request the bootstrap capability
_ = try Calculator.Client.fromBootstrap(peer, &state, onBootstrap);
```

## 6. Make RPC Calls

Each call has two callbacks:

1. **Build function** — populates the parameters before the message is sent
2. **Return callback** — handles the response when it arrives

```zig
const CallCtx = struct { state: *State };

// Called by the runtime to build the outgoing parameters
fn buildAdd(ctx_ptr: *anyopaque, params: *Calculator.Add.Params.Builder) anyerror!void {
    _ = ctx_ptr;
    try params.setA(40);
    try params.setB(2);
}

// Called when the server's response arrives
fn onAddReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: Calculator.Add.Response,
    _: *const rpc.cap_table.InboundCapTable,
) anyerror!void {
    const ctx: *CallCtx = @ptrCast(@alignCast(ctx_ptr));
    defer peer.allocator.destroy(ctx);

    switch (response) {
        .results => |results| {
            const result = try results.getResult();
            std.debug.print("40 + 2 = {d}\n", .{result});  // "40 + 2 = 42"
        },
        .exception => |ex| {
            std.debug.print("RPC error: {s}\n", .{ex.reason});
        },
        else => return error.UnexpectedResponse,
    }

    // Done — close the connection
    ctx.state.done = true;
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}
```

Then trigger the call from the bootstrap handler:

```zig
var client = calculator_client;  // from bootstrap response
_ = try client.callAdd(call_ctx, buildAdd, onAddReturn);
```

## 7. Complete Flow

Here's the sequence of events:

```
Server                          Client
  |                               |
  |  <-- TCP connect ------------ |
  |                               |
  |  <-- Bootstrap request ------ |
  |  --- Bootstrap response ---->  |  (contains Calculator capability)
  |                               |
  |  <-- Call: add(40, 2) ------- |
  |  --- Return: result=42 ----> |
  |                               |
  |  <-- connection close ------  |
```

## 8. Cleanup

Always clean up peers and connections when done:

```zig
if (state.client_peer) |peer| {
    peer.deinit();
    allocator.destroy(peer);
}
if (state.client_conn) |conn| {
    conn.deinit();
    allocator.destroy(conn);
}
// Same for server peer and conn
```

## Key Concepts

### Bootstrap Capability

When a client connects, it requests the server's **bootstrap capability** — a single root interface that the server exports. This is the entry point for all subsequent RPC calls. Set it with `MyInterface.setBootstrap(peer, &server)`.

### Callback Architecture

The RPC runtime is fully async and callback-driven:
- No async/await or coroutines
- Build functions populate parameters synchronously before send
- Return callbacks fire on the event loop thread when responses arrive
- Context pointers (`*anyopaque`) carry state between callbacks

### Single-Threaded

All operations on a Peer must happen on the event loop thread. The runtime asserts this in debug builds. Do not share Peers across threads.

### VTable Pattern

Server implementations use a vtable — a struct of function pointers, one per method:

```zig
const server = Calculator.Server{
    .ctx = &my_app_state,          // passed as first arg to handlers
    .vtable = .{
        .add = handleAdd,
        .multiply = handleMultiply,
    },
};
```

### Error Handling

- If a handler returns an error, the runtime sends an RPC exception to the caller
- Clients see the exception as the `.exception` variant in the `Response` union
- Transport errors are reported via the `onPeerError` callback

### Passing Capabilities

Interfaces can be passed as method parameters or return values. When you return an interface from a method, the runtime automatically exports it and the remote peer gets a `Client` for it. This is the foundation of Cap'n Proto's object-capability security model.

## Running the Example

The repo includes a complete ping-pong RPC example:

```bash
zig build example-rpc
```

See `examples/rpc_pingpong.zig` for the full source and `examples/pingpong.capnp` for the schema.
