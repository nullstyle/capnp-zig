#!/usr/bin/env python3
"""Pinned capnpc-wasm tools archive: install, verify, and delegate to its launcher.

Run with Python 3.13; commands resolve caller paths from the current directory.
This file owns only the consumer side of the toolchain: the pin in
tools/capnp-toolchain.json, the download, the safe extraction, and the check of
the installed package against the pinned manifest digest on every invocation.
Compiling and generating are the packaged portable launcher's job
(package/bin/capnp-wasm.py, capnpc-wasm's launcher contract): its `capnp` mode
translates caller paths, its `generate` mode runs a native plugin with
transactional output, and it runs Wasmtime on Linux, macOS, and Windows. No
native compiler is selected.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
LAUNCHER = "bin/capnp-wasm.py"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    return json.loads(path.read_text())


def safe_path(name):
    if not isinstance(name, str) or not name or "\\" in name:
        raise ValueError("invalid relative path: " + repr(name))
    if name.startswith("/") or any(p in ("", ".", "..") for p in name.split("/")):
        raise ValueError("invalid relative path: " + repr(name))
    for part in name.split("/"):
        if (re.search(r'[<>:"|?*\x00-\x1f]', part) or part.endswith((".", " ")) or
                re.fullmatch(r"(?i:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])", part.split(".")[0])):
            raise ValueError("non-portable relative path: " + repr(name))
    return PurePosixPath(name)


def inventory(root):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or path.is_junction():
            raise ValueError("symlink in tool inputs: " + str(path))
        if path.is_file():
            name = str(safe_path(path.relative_to(root).as_posix()))
            result[name] = path.read_bytes()
        elif not path.is_dir():
            raise ValueError("unsupported tool input: " + str(path))
    return result


def includes_digest(files):
    return digest("".join(digest(data) + "  " + name + "\n"
                          for name, data in sorted(files.items())
                          if name.startswith("include/")).encode())


def extract_archive(archive, destination):
    """Extract regular files/directories only; validate the entire archive first."""
    with tarfile.open(archive, "r:*") as tar:
        members = tar.getmembers()
        seen = {}
        for member in members:
            name = member.name.rstrip("/")
            safe_path(name)
            key = name.casefold()
            if key in seen or not (member.isfile() or member.isdir()):
                raise ValueError("unsafe or duplicate archive member: " + name)
            seen[key] = member
        for name in seen:
            for parent in PurePosixPath(name).parents:
                if str(parent) in seen and not seen[str(parent)].isdir():
                    raise ValueError("archive file used as a parent directory: " + name)
        for member in members:
            target = destination.joinpath(*safe_path(member.name.rstrip("/")).parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(member) as source, target.open("xb") as output:
                    shutil.copyfileobj(source, output)


def verify_package(package, pin):
    if package.is_symlink() or package.is_junction():
        raise ValueError("tool package must not be a symlink")
    files = inventory(package)
    raw_manifest = files.get("manifest.json", b"")
    if digest(raw_manifest) != pin["manifest_sha256"]:
        raise ValueError("tool package manifest digest mismatch")
    manifest = json.loads(raw_manifest)
    if manifest["source"]["commit"] != pin["source_commit"] or manifest["source"]["dirty"]:
        raise ValueError("tool package source identity mismatch")
    expected = {"manifest.json"}
    for entry in manifest["files"]:
        name = str(safe_path(entry["path"]))
        if name in expected:
            raise ValueError("duplicate manifest entry: " + name)
        expected.add(name)
        data = files.get(name)
        if data is None or len(data) != entry["bytes"] or digest(data) != entry["sha256"]:
            raise ValueError("tool package integrity mismatch: " + name)
    if set(files) != expected:
        raise ValueError("tool package inventory mismatch")
    if digest(files["wasm/capnp.wasm"]) != pin["compiler_sha256"]:
        raise ValueError("compiler digest mismatch")
    if includes_digest(files) != pin["include_sha256"]:
        raise ValueError("standard includes digest mismatch")
    if LAUNCHER not in files:
        raise ValueError("tool package has no " + LAUNCHER + "; pin capnp-wasm-tools 0.1.0-rc.3 or newer")
    return package


def cache_root(root):
    return Path(os.environ.get("CAPNP_WASM_CACHE", root / ".zig-cache/capnp-wasm")).resolve()


def tool_pin(root):
    pin = read_json(root / "tools/capnp-toolchain.json")
    if pin.get("format") != 1:
        raise ValueError("unsupported compiler lock format")
    unfilled = [field for field, value in pin.items() if isinstance(value, str) and "FILL-IN" in value]
    if unfilled:
        raise ValueError("tools/capnp-toolchain.json is not filled in (" + ", ".join(unfilled) +
                         "); run tools/update_capnp_toolchain.py after the release is published")
    for field in ("sha256", "manifest_sha256", "compiler_sha256", "include_sha256"):
        if not re.fullmatch(r"[0-9a-f]{64}", pin[field]):
            raise ValueError("invalid compiler lock digest: " + field)
    if not re.fullmatch(r"[0-9a-f]{40}", pin["source_commit"]):
        raise ValueError("invalid compiler lock source commit")
    return pin


def package_path(root, pin):
    return cache_root(root) / "artifacts" / pin["sha256"] / "package"


def installed_package(root):
    pin = tool_pin(root)
    package = package_path(root, pin)
    if not package.exists():
        raise ValueError("WASM compiler is not installed; run this script's bootstrap command "
                         "(mise run bootstrap:capnp in a capnp-zig checkout)")
    return verify_package(package, pin)


def launch(package, pin, args, **kwargs):
    """Run the verified package's portable launcher; return its exit status.

    The package was checked against the pinned manifest digest just before,
    and the launcher checks it again against the same digest, so a package
    changed after bootstrap never runs.
    """
    environment = dict(os.environ, CAPNP_WASM_EXPECT_MANIFEST_SHA256=pin["manifest_sha256"])
    command = [sys.executable, str(package / LAUNCHER), *args]
    return subprocess.run(command, env=environment, **kwargs).returncode


def compiler(package, pin, args, **kwargs):
    if not args:
        raise ValueError("missing compiler arguments after --")
    return launch(package, pin, ["capnp", "--", *args], **kwargs)


def bootstrap(root, archive_override=None):
    pin = tool_pin(root)
    destination = package_path(root, pin)
    destination.parent.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        verify_package(destination, pin)
    else:
        with tempfile.TemporaryDirectory(prefix="install-", dir=destination.parent.parent) as temp:
            temp = Path(temp)
            archive = temp / "tools.tgz"
            if archive_override:
                shutil.copyfile(archive_override, archive)
            else:
                with urllib.request.urlopen(pin["url"], timeout=60) as response, archive.open("wb") as output:
                    shutil.copyfileobj(response, output)
            if digest(archive.read_bytes()) != pin["sha256"]:
                raise ValueError("tool archive digest mismatch")
            unpacked = temp / "unpacked"
            unpacked.mkdir()
            extract_archive(archive, unpacked)
            verify_package(unpacked / "package", pin)
            if set(p.name for p in unpacked.iterdir()) != {"package"}:
                raise ValueError("unexpected tool archive root")
            destination.parent.mkdir(exist_ok=True)
            (unpacked / "package").rename(destination)
    result = compiler(destination, pin, ["--version"], stdout=sys.stderr)
    if result:
        return result
    print("capnp-wasm: verified " + pin["source_commit"] + " (" + pin["sha256"] + ")", file=sys.stderr)
    return 0


def generate(package, pin, plugin, output, plugin_args, compiler_args):
    if not compiler_args:
        raise ValueError("missing schema compiler arguments after --")
    options = compiler_args[:compiler_args.index("--")] if "--" in compiler_args else compiler_args
    if (options and options[0] == "compile") or any(
            arg == "--output" or arg.startswith(("-o", "--output=")) for arg in options):
        raise ValueError("generate arguments must omit compile and -o/--output")
    plugin = Path(os.path.abspath(plugin))
    if os.name == "nt" and not plugin.exists() and plugin.suffix.lower() != ".exe":
        plugin = plugin.with_name(plugin.name + ".exe")
    if not plugin.is_file():
        raise ValueError("native generator does not exist: " + str(plugin))
    arguments = ["generate", "--plugin", str(plugin), "--output", os.path.abspath(output)]
    for value in plugin_args:
        arguments.append("--plugin-arg=" + value)
    return launch(package, pin, [*arguments, "--", *compiler_args])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    install = commands.add_parser("bootstrap", help="download and verify the pinned compiler package")
    install.add_argument("--archive", type=Path, help="use a local archive with the same pinned digest")
    commands.add_parser("verify", help="verify the installed package and Wasmtime version")
    compile_parser = commands.add_parser("compiler", help="run compiler arguments after --")
    compile_parser.add_argument("args", nargs=argparse.REMAINDER)
    generator = commands.add_parser("generate", help="compile a request and run the specified native plugin")
    generator.add_argument("--plugin", required=True)
    generator.add_argument("--output", required=True)
    generator.add_argument("--plugin-arg", action="append", default=[],
                           help="use --plugin-arg=--flag for an argument beginning with -")
    generator.add_argument("args", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.command == "bootstrap":
        return bootstrap(ROOT, args.archive)
    pin = tool_pin(ROOT)
    package = installed_package(ROOT)
    if args.command == "verify":
        status = launch(package, pin, ["verify", "--expect-manifest-sha256", pin["manifest_sha256"]])
        return status or compiler(package, pin, ["--version"])
    if not args.args or args.args[0] != "--":
        parser.error(args.command + " requires -- before its arguments")
    if args.command == "generate":
        return generate(package, pin, args.plugin, args.output, args.plugin_arg, args.args[1:])
    return compiler(package, pin, args.args[1:])


def cli(argv=None):
    try:
        status = main(argv)
    except (ValueError, OSError, subprocess.CalledProcessError, tarfile.TarError) as error:
        print("capnp-wasm: " + str(error), file=sys.stderr)
        status = 1
    return 128 - status if status < 0 else status


if __name__ == "__main__":
    sys.exit(cli())
