# RPC Persistence Guide

RPC Level 2 persistence is Experimental in capnp-zig. Pin an exact commit or
release before using it, and expect the vat-level restore convention to evolve
before it becomes Stable.

Persistence lets an application turn a live capability into an opaque sturdy-ref
byte string, disconnect, reconnect later, and restore a fresh live capability.
capnp-zig deliberately does not define the meaning of the bytes: the application
owns the realm, storage, authorization, and revocation policy.

## Server Flow

1. Export the capability normally with `Peer.addExport()` or by returning it from
   a bootstrap method.
2. Call `Peer.setPersistentExport(export_id, ctx, on_save)` on that export.
   `on_save` returns app-defined sturdy-ref bytes allocated from
   `peer.allocator`; the peer embeds and frees them.
3. Install a restore hook on the bootstrap export with
   `Peer.setRestorer(ctx, on_restore)`.
4. In `on_restore`, return `.unknown`, `.existing = export_id`, or
   `.host = Export` for a fresh export.

Save and restore are served through the normal inbound call path. A persistent
export still forwards every non-`Persistent.save()` interface to its original
handler, and the restorer hook lives only on the current bootstrap export.

## Client Flow

1. Connect and call `Peer.sendBootstrap()` to import the remote bootstrap cap.
2. Call `Peer.sendSave(import_id, ctx, callback)` on an imported capability.
   The callback receives sturdy-ref bytes borrowed from the Return frame; copy
   them before the callback returns.
3. After reconnecting, call `Peer.sendBootstrap()` again.
4. Call `Peer.sendRestore(bootstrap_import_id, sturdy_ref, ctx, callback)`.
   A successful restore callback receives a retained capability. Release it with
   `Peer.releaseImport()` when the application is done.

`Peer.sendSave()` is the empty-`sealFor` convenience. If an application needs a
sealed sturdy ref, use the generated `rpc.generated.persistent.Persistent`
client and build `SaveParams.sealFor` explicitly. `sealFor` is realm-defined and
is passed through to the save handler as a raw `AnyPointerReader`.

## Limits And Evidence

`PeerLimits.max_persistent_exports` bounds the number of exports with persistence
hooks, and `PeerLimits.max_sturdy_ref_bytes` bounds a single sturdy-ref payload
on both save output and restore input. Pressure and rejection events are emitted
through the normal RPC observer, and `Peer.stats()` reports
`persistent_exports`, `saves_served`, and `restores_served`.

The current Experimental evidence covers:

- save/restore reconnect and resave over in-process HostPeer transport;
- malformed restore params and malformed Save/Restore results;
- Return send failures, including rollback of freshly hosted restore exports;
- callback failure after `sendRestore` retained an import;
- independent clearing of save and restorer hooks;
- allocator rollback for `setPersistentExport`, `setRestorer`, `sendSave`, and
  `sendRestore`.

L2 is still not a cross-implementation portability promise. The wire-level
`Persistent.save()` interface is standard, but the restore convention and
sturdy-ref bytes are vat/realm-specific.
