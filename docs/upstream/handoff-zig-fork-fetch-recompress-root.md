# HANDOFF — zig fork change branch: standalone `zig fetch` caches a mis-rooted tarball

Paste this into a session working on the nullstyle zig fork. Self-contained.
It joins the fork's change-branch list (see the other
`handoff-zig-fork-*.md` files beside this one). Suggested branch:
`fix/fetch-recompress-root`.

## The defect

At tagged **0.17.0**, a standalone `zig fetch <url>` (run outside a project, so
there is no project-local `zig-pkg/`) writes a recompressed tarball to the
global cache (`<global cache>/p/<hash>.tar.gz`) with the package one directory
too deep. The next `zig build` that needs that package loads the cached
tarball, finds no `build.zig.zon` at its root, hashes it as a nameless package,
and fails:

```
build.zig.zon:8:21: error: hash mismatch: manifest declares capnpc_zig-0.18.0-nUduFTdRNwBzlJgTt6x9lUwRGmpWzSVXrMSE-xFj_dND but the fetched package has N-V-__8AADdRNwBwMSFYBflRn85ej_SF0GhjIrwOallQrgFW
```

The declared hash is correct; `zig fetch` itself printed it. The cache entry
is what is wrong, and it stays wrong until someone deletes it. capnp-zig has
hit this on every new quic pin it primed with `zig fetch` (the "N-V cache
poisoning" trap in its notes).

File: `lib/compiler/Maker/Fetch.zig`, `runResource`. The archive is unpacked
into a temporary directory, and `unpack_result.root_dir` names the single
top-level directory that tarballs from GitHub and similar hosts wrap the
package in (`capnp-zig-0.18.0/`). The manifest and hash are computed on
`tmp/<dir>/<root_dir>` (line 747), and `package_sub_path` is that same path
(line 770). Then:

```zig
if (job_queue.local_storage) |ls| {
    f.package_root = try ls.pkg_root.join(arena, computed_package_hash.toSlice());
    renameTmpIntoCache(io, package_sub_path, f.package_root) ...
} else {
    f.package_root = tmp_directory_path;   // line 792: drops root_dir
}
...
job_queue.group.async(io, JobQueue.recompress, .{ job_queue, computed_package_hash, f.package_root }); // line 799
```

With local storage (any `zig build`, or `zig fetch --save` inside a project)
the root is right. Without it, `package_root` is the temporary directory
itself, so `recompress` archives `<hash>/capnp-zig-0.18.0/build.zig.zon`
instead of `<hash>/build.zig.zon`. Reading it back strips only `<hash>/`.

A second symptom of the same line: the cleanup at line 804 calls `deleteDir`
on that non-empty temporary directory, so every standalone fetch prints
`warning(fetch): failed to delete temporary directory ...: DirNotEmpty` and
leaks the directory under `<global cache>/tmp/`.

## Minimal repro

Any tarball with a top-level wrapper directory. With a fresh cache:

```sh
export ZIG_GLOBAL_CACHE_DIR="$(mktemp -d)"
zig fetch https://github.com/nullstyle/capnp-zig/archive/refs/tags/v0.18.0.tar.gz
# prints the correct hash, plus the DirNotEmpty warning
tar -tzf "$ZIG_GLOBAL_CACHE_DIR"/p/capnpc_zig-0.18.0-*.tar.gz | head -1
# capnpc_zig-0.18.0-.../capnp-zig-0.18.0/LICENSE   <- one level too deep
```

Then, in a project whose `build.zig.zon` declares that URL and hash, run
`zig build` with the same `ZIG_GLOBAL_CACHE_DIR`: it fails with the
`hash mismatch ... N-V-__8AA...` error above and leaves an
`N-V-__8AA...` directory in `zig-pkg/`.

## The fix

Use the stripped root in the no-local-storage branch:

```zig
} else {
    f.package_root = package_sub_path;
}
```

and make the cleanup at line 804 remove the whole temporary tree once
`recompress` has finished with it (today `recompress` runs as an async task
over that tree, so deleting it before the task completes would race). Keep
the warning for a real failure.

## Verification

Verified 2026-10-03 on macOS (Darwin 27, aarch64) with a copy of the 0.17.0
`lib/` with only the one-line `package_sub_path` change, selected through
`ZIG_LIB_DIR`:

1. The repro's tarball lists `capnpc_zig-0.18.0-.../LICENSE` (correct root).
2. `zig build` in the consumer project then succeeds from that cache.
3. Stock 0.17.0, same steps: the nested tarball and the `N-V` hash mismatch.
   It is deterministic (line 792 is not a race), which matches capnp-zig's
   record of hitting it on every pin it primed this way.
4. Stock 0.17.0 with a project-local fetch (`zig fetch --save` inside a
   project, or `zig build`) writes a correctly rooted tarball, which isolates
   the defect to the no-local-storage branch.

## Bookkeeping

Record in the fork's change-branch list: "standalone `zig fetch` must
recompress from the stripped package root; at 0.17.0 it caches a mis-rooted
tarball that later builds reject with an `N-V-` hash mismatch."
