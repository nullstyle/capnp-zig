#!/usr/bin/env python3
"""Pinned WASM compiler and current-source native plugin orchestration.

Run with Python 3.13; commands resolve caller paths from the current directory.
The package is verified on every invocation; no native compiler is selected.
"""

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
import ntpath
import posixpath
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request

ROOT = Path(__file__).resolve().parent.parent


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
    return package


def cache_root(root):
    return Path(os.environ.get("CAPNP_WASM_CACHE", root / ".zig-cache/capnp-wasm")).resolve()


def tool_pin(root):
    pin = read_json(root / "tools/capnp-toolchain.json")
    if pin.get("format") != 1:
        raise ValueError("unsupported compiler lock format")
    for field in ("sha256", "manifest_sha256", "compiler_sha256", "include_sha256"):
        if not re.fullmatch(r"[0-9a-f]{64}", pin[field]):
            raise ValueError("invalid compiler lock digest: " + field)
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


def runtime(package):
    expected = (package / "runtime/wasmtime-version").read_text().strip()
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", expected):
        raise ValueError("invalid packaged Wasmtime version")
    executable = os.environ.get("CAPNP_WASM_WASMTIME", "wasmtime")
    result = subprocess.run([executable, "--version"], capture_output=True, check=True)
    actual = result.stdout.decode().strip()
    if actual != "wasmtime " + expected and not actual.startswith("wasmtime " + expected + " "):
        raise ValueError(f"expected Wasmtime {expected}, got: {actual}")
    return executable


def compiler_operation(args):
    for index, arg in enumerate(args):
        if arg == "--":
            break
        if not arg.startswith("-"):
            if arg in ("compile", "encode", "decode", "eval", "convert", "id"):
                return index, arg
            break
    return None, None


def compiler_path_arguments(args):
    """Locate filenames without interpreting constant expressions or format names."""
    operation_index, operation = compiler_operation(args)
    if operation_index is None:
        return []
    paths = []
    position = 0
    options = True
    index = operation_index + 1
    while index < len(args):
        arg = args[index]
        if options and arg == "--":
            options = False
        elif options and arg in ("-I", "--import-path", "--src-prefix"):
            index += 1
            if index < len(args):
                paths.append((index, "", True))
        elif options and arg.startswith(("--import-path=", "--src-prefix=")):
            paths.append((index, arg.split("=", 1)[0] + "=", True))
        elif options and arg.startswith("-I"):
            paths.append((index, "-I", True))
        elif options and arg in ("-o", "--output", "--segment-size"):
            index += 1
        elif options and arg.startswith("-"):
            pass
        else:
            if (operation == "compile" or
                    operation in ("encode", "decode", "eval") and position == 0 or
                    operation == "convert" and position == 1):
                paths.append((index, "", False))
            position += 1
        index += 1
    return paths


def compiler_paths(args, cwd, windows=None):
    """Return one native root, caller directory within it, and translated argv.

    All filenames, -I paths and --src-prefix paths share the same translation.
    Preview 1 libc starts at /, regardless of Wasmtime's Preview 2 cwd option.
    A fallback source prefix preserves the caller-relative requested filenames.
    KJ opens through its root directory fd, so independent WASI preopens cannot
    supply additional trees. Explicit inputs must live on the caller's volume.
    """
    windows = os.name == "nt" if windows is None else windows
    native = ntpath if windows else posixpath
    cwd = native.normpath(str(cwd))
    roots = [cwd]
    paths = []
    result = list(args)
    for index, prefix, directory in compiler_path_arguments(args):
        value = args[index][len(prefix):]
        if not value:
            continue  # Keep the compiler's own invalid-option diagnostic.
        if windows and ntpath.splitdrive(value)[0] and not ntpath.isabs(value):
            raise ValueError("drive-relative paths are ambiguous; use an absolute path: " + value)
        absolute = native.normpath(native.join(cwd, value))
        roots.append(absolute if directory else native.dirname(absolute))
        paths.append((index, prefix, value, absolute))
    try:
        root = native.commonpath(roots)
    except ValueError as error:
        raise ValueError("explicit schema/include paths must be on the working directory's volume; "
                         "copy those inputs to that volume first") from error
    if "::" in root:
        raise ValueError("filesystem path cannot contain ::")

    def guest(path):
        relative = native.relpath(path, root)
        return "/" if relative == "." else "/" + relative.replace("\\", "/")

    for index, prefix, value, absolute in paths:
        result[index] = prefix + guest(absolute)
    if compiler_operation(args)[1] == "compile" and paths:
        # Explicit ancestor prefixes intentionally override the compiler's cwd
        # fallback. Descendant prefixes still win over an added cwd prefix.
        prefixes = [absolute for index, prefix, value, absolute in paths
                    if prefix == "--src-prefix=" or args[index - 1] == "--src-prefix"]
        covered = any(native.normcase(native.commonpath([cwd, prefix])) == native.normcase(prefix)
                      for prefix in prefixes)
        if not covered:
            end_options = result.index("--") if "--" in result else len(result)
            result.insert(end_options, "--src-prefix=" + guest(cwd))
    return root, guest(cwd), result


def compiler(package, args, **kwargs):
    if not args:
        raise ValueError("missing compiler arguments after --")
    cwd = Path.cwd().resolve()
    executable = runtime(package)
    args = list(args)
    with ExitStack() as cleanup:
        end_options = args.index("--") if "--" in args else len(args)
        options = args[:end_options]
        if (compiler_operation(args)[1] in ("compile", "encode", "decode", "eval", "convert") and
                "--no-standard-import" not in options and
                not any(arg in ("--version", "--help") for arg in options)):
            includes = package / "include"
            if os.name == "nt" and includes.drive.lower() != cwd.drive.lower():
                staging = Path(cleanup.enter_context(tempfile.TemporaryDirectory(prefix=".capnp-includes-", dir=cwd)))
                includes = Path(shutil.copytree(includes, staging / "include"))
            # Explicit imports take precedence. Never search incidental /usr
            # includes inside the shared root. --no-standard-import is exact:
            # when the caller supplies it, no bundled include path is added.
            args[end_options:end_options] = ["--no-standard-import", "-I" + str(includes)]
        root, _, args = compiler_paths(args, cwd)
        command = [executable, "run", "-W", "exceptions=y", "-S", "cwd=/",
                   "--dir", root + "::/", str(package / "wasm/capnp.wasm"), *args]
        return subprocess.run(command, **kwargs).returncode


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
    result = compiler(destination, ["--version"], stdout=sys.stderr)
    if result:
        return result
    print("capnp-wasm: verified " + pin["source_commit"] + " (" + pin["sha256"] + ")", file=sys.stderr)
    return 0


def publish(staged, output):
    """Publish only regular generated files, after validating all destinations."""
    files = inventory(staged)
    output = Path(os.path.abspath(output))
    for name in files:
        destination = output.joinpath(*safe_path(name).parts)
        for parent in (destination, *destination.parents):
            if parent.is_symlink() or parent.is_junction():
                raise ValueError("generated output cannot follow a symlink: " + str(parent))
        if destination.exists() and not destination.is_file():
            raise ValueError("generated file would replace a directory: " + str(destination))
        if any(parent.exists() and not parent.is_dir() for parent in destination.parents):
            raise ValueError("generated output parent is not a directory: " + str(destination))
    for name, data in files.items():
        destination = output.joinpath(*safe_path(name).parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=".capnp-output-", dir=destination.parent)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(data)
            os.replace(temporary, destination)
        finally:
            Path(temporary).unlink(missing_ok=True)


def generate(package, plugin, output, plugin_args, compiler_args):
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
    output = Path(os.path.abspath(output))
    # Spool requests as binary data. No pipe can deadlock while the compiler
    # reports errors, and generators never run after an unsuccessful compile.
    with tempfile.TemporaryFile() as request:
        status = compiler(package, ["compile", "-o-", *compiler_args], stdout=request)
        if status:
            return status
        request.seek(0)
        with tempfile.TemporaryDirectory(prefix="capnp generate ") as temporary:
            staged = Path(temporary)
            result = subprocess.run([str(plugin), *plugin_args], cwd=staged, stdin=request)
            if result.returncode:
                return result.returncode
            publish(staged, output)
    return 0


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
    package = installed_package(ROOT)
    if args.command == "verify":
        return compiler(package, ["--version"])
    if not args.args or args.args[0] != "--":
        parser.error(args.command + " requires -- before its arguments")
    if args.command == "generate":
        return generate(package, args.plugin, args.output, args.plugin_arg, args.args[1:])
    return compiler(package, args.args[1:])


def cli(argv=None):
    try:
        status = main(argv)
    except (ValueError, OSError, subprocess.CalledProcessError, tarfile.TarError) as error:
        print("capnp-wasm: " + str(error), file=sys.stderr)
        status = 1
    return 128 - status if status < 0 else status


if __name__ == "__main__":
    sys.exit(cli())
