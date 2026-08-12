const std = @import("std");
const build_impl = @import("./build/build_impl.zig");

// Thin driver: the whole build graph lives in build/build_impl.zig (B-series
// decomposition). Step names, registration order, and option handling are the
// build's public ABI — 46 Justfile recipes and CI address them — so the impl
// preserves them exactly; `zig build -l` (and the -Dquic=true variant) must
// stay byte-identical across build-file changes.
//
// NOTE: build/ must stay in build.zig.zon `.paths` (the whitelist) or the
// packaged tarball cannot build — gated by `zig build package-preflight`.
pub fn build(b: *std.Build) !void {
    return build_impl.buildImpl(b);
}
