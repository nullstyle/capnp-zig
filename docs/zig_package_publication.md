# Zig Package Publication Notes

This repository now uses a URL+hash dependency for `libxev` in `build.zig.zon`.

Previous local-development form (replaced):

```zon
.libxev = .{
    .path = "vendor/ext/libxev",
},
```

The URL+hash form is suitable for external `zig fetch` consumers.

## Release-ready publication steps

1. Choose a stable `libxev` upstream reference (tag or commit).
2. Ensure `build.zig.zon` uses a URL-based dependency:
   - `.url = "..."` (archive URL)
   - `.hash = "..."` (content hash produced by `zig fetch --save`)
3. Verify downstream consumption from a clean external project:
   - `zig fetch --save <this-package-url>`
   - `zig build` in downstream consumer
4. Keep `vendor/ext/libxev` only if needed for local/offline workflows; otherwise remove to avoid dual-source drift.

## Current status

- Package version updated to semantic versioning (`0.1.0`).
- URL+hash dependency migration is complete for `libxev` (pinned to commit `42d4ead52667e03619dcd5b1a3ca8ef7d5dd24ed`).
- Remaining optional cleanup: remove or retain `vendor/ext/libxev` depending on offline/local workflow preference.
