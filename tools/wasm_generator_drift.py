#!/usr/bin/env python3
"""Drift signal: does the released capnpc-zig.wasm generate what this checkout does?

capnpc-wasm ships capnpc-zig.wasm, built from the capnp-zig revision its
release pins (`capnp_zig` in tools/capnpc-wasm-generator.json). Consumers that
generate with that module get that revision's output, not this checkout's.
This check installs the pinned full SDK archive, runs its capnpc-zig.wasm under
the packaged launcher on every committed CodeGeneratorRequest, runs this
checkout's native plugin on the same requests, and compares the files byte for
byte. A difference means a generator change here that no capnpc-wasm release
carries yet; it is a signal to cut one, not a defect in this checkout.

    uv run --no-project --python 3.13 tools/wasm_generator_drift.py --plugin zig-out/bin/capnpc-zig

Exit status: 0 when every request generates identical files, 1 on a difference
or an error. --archive installs a local archive with the pinned digest.
"""

import argparse
import difflib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))
import capnp_tool as tool  # noqa: E402

PIN = ROOT / "tools/capnpc-wasm-generator.json"
MODULE = "wasm/capnpc-zig.wasm"
REQUESTS = [
    "tests/package_consumer/codegen/schema/addressbook.request.bin",
    "tests/docs/schema/guide.request.bin",
]
REQUEST_DIRECTORY = "tests/generated_shape/requests"


def generator_pin():
    pin = tool.read_json(PIN)
    if pin.get("format") != 1:
        raise ValueError("unsupported generator pin format")
    unfilled = [field for field, value in pin.items() if isinstance(value, str) and "FILL-IN" in value]
    if unfilled:
        raise ValueError("tools/capnpc-wasm-generator.json is not filled in (" + ", ".join(unfilled) +
                         "); run tools/update_capnp_toolchain.py generator <tag> after the release is published")
    for field in ("sha256", "manifest_sha256", "generator_sha256"):
        if not re.fullmatch(r"[0-9a-f]{64}", pin[field]):
            raise ValueError("invalid generator pin digest: " + field)
    for field in ("source_commit", "capnp_zig"):
        if not re.fullmatch(r"[0-9a-f]{40}", pin[field]):
            raise ValueError("invalid generator pin revision: " + field)
    return pin


def verify(package, pin):
    files = tool.inventory(package)
    raw = files.get("manifest.json", b"")
    if tool.digest(raw) != pin["manifest_sha256"]:
        raise ValueError("generator package manifest digest mismatch")
    manifest = json.loads(raw)
    if manifest["source"]["commit"] != pin["source_commit"] or manifest["source"]["dirty"]:
        raise ValueError("generator package source identity mismatch")
    if manifest.get("references", {}).get("ref/capnp-zig") != pin["capnp_zig"]:
        raise ValueError("generator package names another capnp-zig revision")
    expected = {"manifest.json"}
    for entry in manifest["files"]:
        name = str(tool.safe_path(entry["path"]))
        expected.add(name)
        data = files.get(name)
        if data is None or len(data) != entry["bytes"] or tool.digest(data) != entry["sha256"]:
            raise ValueError("generator package integrity mismatch: " + name)
    if set(files) != expected:
        raise ValueError("generator package inventory mismatch")
    if tool.digest(files[MODULE]) != pin["generator_sha256"]:
        raise ValueError("capnpc-zig.wasm digest mismatch")
    if tool.LAUNCHER not in files:
        raise ValueError("generator package has no " + tool.LAUNCHER)
    return package


def install(pin, archive_override=None):
    destination = tool.cache_root(ROOT) / "generator" / pin["sha256"] / "package"
    if destination.exists():
        return verify(destination, pin)
    destination.parent.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="install-", dir=destination.parent.parent) as temp:
        temp = Path(temp)
        archive = temp / "sdk.tgz"
        if archive_override:
            shutil.copyfile(archive_override, archive)
        else:
            with urllib.request.urlopen(pin["url"], timeout=120) as response, archive.open("wb") as output:
                shutil.copyfileobj(response, output)
        if tool.digest(archive.read_bytes()) != pin["sha256"]:
            raise ValueError("generator archive digest mismatch")
        unpacked = temp / "unpacked"
        unpacked.mkdir()
        tool.extract_archive(archive, unpacked)
        verify(unpacked / "package", pin)
        destination.parent.mkdir(exist_ok=True)
        (unpacked / "package").rename(destination)
    return destination


def tree(root):
    return {path.relative_to(root).as_posix(): path.read_bytes()
            for path in sorted(root.rglob("*")) if path.is_file()}


def run_wasm(package, pin, request, output):
    output.mkdir(parents=True)
    environment = dict(os.environ, CAPNP_WASM_EXPECT_MANIFEST_SHA256=pin["manifest_sha256"])
    result = subprocess.run([sys.executable, str(package / tool.LAUNCHER), "generator", "--module",
                             str(package / MODULE), "--output", str(output), "--"],
                            input=request, env=environment, capture_output=True)
    if result.returncode:
        raise ValueError("capnpc-zig.wasm failed (%d): %s" % (result.returncode, result.stderr.decode()))


def run_native(plugin, request, output):
    result = subprocess.run([str(plugin), "--output-dir=" + str(output)], input=request, capture_output=True)
    if result.returncode:
        raise ValueError("native capnpc-zig failed (%d): %s" % (result.returncode, result.stderr.decode()))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--plugin", type=Path, default=ROOT / "zig-out/bin/capnpc-zig",
                        help="this checkout's native plugin (default: zig-out/bin/capnpc-zig)")
    parser.add_argument("--archive", type=Path, help="install a local archive with the pinned digest")
    args = parser.parse_args(argv)
    plugin = args.plugin.resolve()
    if os.name == "nt" and not plugin.exists() and plugin.suffix.lower() != ".exe":
        plugin = plugin.with_name(plugin.name + ".exe")
    if not plugin.is_file():
        raise ValueError("native plugin not found: %s (run zig build first)" % plugin)
    pin = generator_pin()
    package = install(pin, args.archive)
    requests = [ROOT / name for name in REQUESTS] + sorted((ROOT / REQUEST_DIRECTORY).glob("*.request.bin"))
    report = []
    with tempfile.TemporaryDirectory(prefix="wasm generator drift ") as temp:
        temp = Path(temp)
        for request_path in requests:
            name = request_path.relative_to(ROOT).as_posix()[:-len(".request.bin")]
            request = request_path.read_bytes()
            key = name.replace("/", "__")
            run_wasm(package, pin, request, temp / "wasm" / key)
            run_native(plugin, request, temp / "native" / key)
            released, current = tree(temp / "wasm" / key), tree(temp / "native" / key)
            changed = sorted(path for path in set(released) | set(current) if released.get(path) != current.get(path))
            for path in changed:
                before = released.get(path, b"").decode("utf-8", "replace").splitlines()
                after = current.get(path, b"").decode("utf-8", "replace").splitlines()
                diff = list(difflib.unified_diff(before, after, "released/" + path, "checkout/" + path,
                                                 lineterm="", n=1))
                report.append((name, path, len(diff), diff[:40]))
    label = "%s (capnp-zig %s)" % (pin["url"].rsplit("/", 2)[-2], pin["capnp_zig"][:12])
    if not report:
        print("capnpc-zig.wasm from %s matches this checkout's plugin on %d requests" % (label, len(requests)))
        return 0
    lines = ["capnpc-zig.wasm from %s differs from this checkout's plugin:" % label]
    for name, path, size, diff in report:
        lines.append("  %s: %s (%d diff lines)" % (name, path, size))
    lines.append("A capnpc-wasm release that carries this generator change is needed before Wasm users see it.")
    print("\n".join(lines))
    for name, path, size, diff in report[:3]:
        print("\n".join(diff))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as stream:
            stream.write("### Released Wasm generator drift\n\n" + "\n".join(lines) + "\n")
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print("wasm_generator_drift: " + str(error), file=sys.stderr)
        sys.exit(1)
