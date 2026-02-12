# KVStore Example

**WARNING: this shit was entirely vibed into existence.  pretty cool it actually works though**

Cap'n Proto RPC key/value service backed by RocksDB, with an interactive Zig TUI client.

This example is intentionally close to a real service shape:

- Persistent storage (RocksDB)
- Batch writes (`writeBatch`) with per-batch version assignment
- Prefix listing for browser-style reads
- Server-driven client notifications
- Backup/restore operations through the RPC protocol

## What You Get

- `server.zig`: TCP RPC server that hosts `KvStore`
- `client.zig`: interactive browser + REPL client built with `zigzag`
- `kvstore.capnp`: protocol schema
- `gen/kvstore.zig`: generated Zig bindings

## Protocol Summary

Schema: `examples/kvstore/kvstore.capnp`

`KvStore` methods:

- `get(key)` -> `(entry, found)`
- `writeBatch(ops)` -> `(results, applied, nextVersion)`
- `list(prefix, limit)` -> `(entries)`
- `subscribe(notifier)` -> `()`
- `setWatchedKeys(keys)` -> `()`
- `createBackup(flushBeforeBackup)` -> `(backup, backupCount)`
- `listBackups()` -> `(backups)`
- `restoreFromBackup(backupId, keepLogFiles)` -> `(restoredBackupId, nextVersion)`

`KvClientNotifier` methods:

- `keysChanged(changes)`: keyed change notifications for watched keys
- `stateResetRequired(restoredBackupId, nextVersion)`: sent when another client restores a backup

## Semantics That Matter

### Versioning

- Versions are persisted in RocksDB metadata.
- A `writeBatch` that contains at least one `put` consumes exactly one new version.
- All `put` operations in that batch share the same assigned version.
- Delete-only batches do not consume a new version.

### Notifications

- Clients subscribe once, then keep their watched key set current with `setWatchedKeys`.
- The included TUI client automatically watches keys currently visible in the browser table.
- `keysChanged` is delivered only to other clients (not the writer) and only when changed keys intersect each client's watched set.
- `restoreFromBackup` triggers `stateResetRequired` to all other subscribed clients (not filtered by watched keys). Clients should clear local state and reload.

## Build And Run

Run from `examples/kvstore/`.

### Common tasks

```bash
just gen
just build
just test
```

### Start server

```bash
just server
# custom:
just server host=0.0.0.0 port=9000 db_path=kvstore-data backup_dir=kvstore-backups
```

Server flags:

- `--host` (default `0.0.0.0`)
- `--port` (default `9000`)
- `--db-path` (default `kvstore-data`)
- `--backup-dir` (default `kvstore-backups`)

### Start client

```bash
just client
# custom:
just client host=127.0.0.1 port=9000
```

Client flags:

- `--host` (default `127.0.0.1`)
- `--port` (default `9000`)

## Client UI

Two working areas:

- Browser table: key, version, preview
- REPL input: command execution + history + autocomplete

Useful keys:

- `Tab`: autocomplete (REPL) or mode switch when appropriate
- `Enter`: run command (REPL) or open selected key (browser)
- `Up` / `Down`: REPL history
- `Ctrl+L`: clear log panel
- `Ctrl+C`: quit
- `q`: quit from browser mode

## REPL Commands

- `help`
- `ls [prefix] [limit]` (alias: `list`)
- `get <key>`
- `put <key> <value>`
- `del <key>` (alias: `delete`)
- `batch <ops>`
- `backup [noflush]`
- `backups`
- `restore <latest|backupId> [keep-logs]`
- `open [index]`
- `quit` (alias: `exit`)

## Batch Syntax

Batch operations are semicolon-separated.

Supported ops:

- `put <key> <value>`
- `puthex <key> <hex>` (optional `0x` prefix)
- `del <key>` (alias `delete`)

Examples:

```text
batch put user:1 alice; put user:2 bob; del user:3
batch puthex blob:1 000102ff; del old:key
```

## Backup And Restore Workflow

Create and inspect backups:

```text
backup
backups
```

Restore latest:

```text
restore latest
```

Restore a specific backup and keep log files:

```text
restore 12 keep-logs
```

After restore:

- Restoring client receives normal RPC response with `(restoredBackupId, nextVersion)`.
- Other connected subscribed clients receive `stateResetRequired` and should reload their view/state.

## Regeneration

If you change `kvstore.capnp`, regenerate bindings:

```bash
just gen
```
