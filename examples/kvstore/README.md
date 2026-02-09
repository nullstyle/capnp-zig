# KVStore Example

A complete Cap'n Proto RPC example with a key-value store server and client running as separate processes.

## What it demonstrates

- 4-method RPC interface (get, set, delete, list)
- Text, Data, nested structs, and List types
- Stateful server with in-memory storage
- Separate client/server processes over TCP
- Callback-driven async RPC with chained operations

## Schema

See `kvstore.capnp` for the interface definition. The generated Zig code is checked in as `kvstore.zig`.

## Running

All commands should be run from this directory (`examples/kvstore/`).

### Option 1: Two terminals

Terminal 1 (server):
```bash
zig build server -- --port 9000
```

Terminal 2 (client):
```bash
zig build client -- --port 9000
```

### Option 2: Procfile

```bash
overmind start
# or
foreman start
```

### Option 3: just recipes (from repo root)

```bash
just example-kvstore-server      # default port 9000
just example-kvstore-client 9001 # custom port
```

## Expected output

Server:
```
READY on 0.0.0.0:9000
info: client connected
info: SET "hello" (5 bytes) -> version 1 (new)
info: SET "count" (4 bytes) -> version 2 (new)
info: GET "hello"
info: SET "hello" (7 bytes) -> version 3 (update)
info: LIST prefix="" limit=10
info: DELETE "count"
info: GET "count"
info: client disconnected
```

Client:
```
Connected to KvStore server

1. SET "hello" = "world" -> version 1
2. SET "count" = [0,0,0,42] -> version 2
3. GET "hello" -> found=true, value="world"
4. SET "hello" = "updated" -> version 3
5. LIST "" limit=10 -> 2 entries:
     [0] "hello" = 7 bytes, version 3
     [1] "count" = 4 bytes, version 2
6. DELETE "count" -> found=true
7. GET "count" -> found=false

All operations completed successfully!
```

## Regenerating the schema

If you modify `kvstore.capnp`, regenerate the Zig code from the repo root:

```bash
zig build && capnp compile -o ./zig-out/bin/capnpc-zig examples/kvstore/kvstore.capnp
mv kvstore.capnp.zig examples/kvstore/kvstore.zig
```
