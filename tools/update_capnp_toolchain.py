#!/usr/bin/env python3
"""Fill a capnpc-wasm pin from a published release's own assets.

    uv run --no-project --python 3.13 tools/update_capnp_toolchain.py tools capnp-wasm-tools-v0.1.0-rc.3
    uv run --no-project --python 3.13 tools/update_capnp_toolchain.py generator capnpc-wasm-v0.1.0-rc.6

`tools` rewrites tools/capnp-toolchain.json (the compiler archive that
tools/capnp_tool.py installs); `generator` rewrites
tools/capnpc-wasm-generator.json (the full SDK archive whose capnpc-zig.wasm
the drift check runs). The script downloads SHA256SUMS, the manifest asset,
and the archive of the release, checks the manifest and the archive against
SHA256SUMS, and derives every other field from the manifest. With --assets DIR
it reads those three files from DIR instead (for a draft release, download
them with `gh release download <tag> --dir DIR`).

Compare the archive and manifest digests it prints with the release's row in
capnpc-wasm's docs/releases.md (published releases) before committing: that
row is the channel independent of the release page.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
REPOSITORY = "https://github.com/nullstyle/capnpc-wasm"
FLAVORS = {
    "tools": ("capnp-wasm-tools-v", "capnp-wasm-tools-", ROOT / "tools/capnp-toolchain.json"),
    "generator": ("capnpc-wasm-v", "capnpc-wasm-", ROOT / "tools/capnpc-wasm-generator.json"),
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def fetch(url):
    with urllib.request.urlopen(url, timeout=120) as response:
        return response.read()


def read_asset(name, base_url, assets):
    if assets is not None:
        return (assets / name).read_bytes()
    return fetch(base_url + name)


def parse_sums(text):
    sums = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        match = re.fullmatch(r"([0-9a-f]{64}) [ *](\S.*)", line)
        if not match:
            raise ValueError("malformed SHA256SUMS line: " + line)
        sums[match.group(2)] = match.group(1)
    return sums


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("kind", choices=sorted(FLAVORS))
    parser.add_argument("tag", help="the release tag, for example capnp-wasm-tools-v0.1.0-rc.3")
    parser.add_argument("--assets", type=Path, help="read SHA256SUMS, the manifest, and the archive from DIR")
    args = parser.parse_args(argv)
    tag_prefix, stem_prefix, pin_path = FLAVORS[args.kind]
    if not args.tag.startswith(tag_prefix):
        parser.error("a %s pin needs a %s<version> tag" % (args.kind, tag_prefix))
    version = args.tag[len(tag_prefix):]
    stem = stem_prefix + version
    base_url = "%s/releases/download/%s/" % (REPOSITORY, args.tag)

    sums = parse_sums(read_asset("SHA256SUMS", base_url, args.assets).decode("utf-8"))
    for name in (stem + ".tgz", stem + ".manifest.json"):
        if name not in sums:
            raise ValueError("SHA256SUMS does not list " + name)
    raw_manifest = read_asset(stem + ".manifest.json", base_url, args.assets)
    if digest(raw_manifest) != sums[stem + ".manifest.json"]:
        raise ValueError("the manifest asset does not match SHA256SUMS")
    archive = read_asset(stem + ".tgz", base_url, args.assets)
    if digest(archive) != sums[stem + ".tgz"]:
        raise ValueError("the archive does not match SHA256SUMS")
    manifest = json.loads(raw_manifest)
    if manifest.get("version") != version or manifest["source"]["dirty"]:
        raise ValueError("the manifest names version %s (dirty: %s), not a clean %s"
                         % (manifest.get("version"), manifest["source"]["dirty"], version))
    files = {entry["path"]: entry["sha256"] for entry in manifest["files"]}
    if "bin/capnp-wasm.py" not in files:
        raise ValueError("the archive has no bin/capnp-wasm.py; pin a release from 0.1.0-rc.3 (tools) on")
    pin = {
        "format": 1,
        "url": base_url + stem + ".tgz",
        "sha256": sums[stem + ".tgz"],
        "manifest_sha256": sums[stem + ".manifest.json"],
        "source_commit": manifest["source"]["commit"],
    }
    if args.kind == "tools":
        pin["compiler_sha256"] = files["wasm/capnp.wasm"]
        pin["include_sha256"] = digest("".join(
            files[name] + "  " + name + "\n" for name in sorted(files) if name.startswith("include/")).encode())
    else:
        pin["generator_sha256"] = files["wasm/capnpc-zig.wasm"]
        pin["capnp_zig"] = manifest["references"]["ref/capnp-zig"]
    pin_path.write_text(json.dumps(pin, indent=2) + "\n")
    print("wrote %s for %s" % (pin_path.relative_to(ROOT), args.tag))
    print("  archive %s  %s" % (pin["sha256"], stem + ".tgz"))
    print("  manifest %s  %s" % (pin["manifest_sha256"], stem + ".manifest.json"))
    print("  producer commit %s" % pin["source_commit"])
    print("Compare these with the published releases row in capnpc-wasm's docs/releases.md.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError) as error:
        print("update_capnp_toolchain: " + str(error), file=sys.stderr)
        sys.exit(1)
