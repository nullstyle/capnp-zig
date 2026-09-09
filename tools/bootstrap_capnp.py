#!/usr/bin/env python3
"""Build the checksum-pinned schema compiler without modifying vendor material."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
PIN = json.loads((ROOT / "tools/capnp-toolchain.json").read_text())
CACHE = ROOT / ".zig-cache/capnp-toolchain"
INSTALL = CACHE / "install"


def run(*args):
    subprocess.run([str(arg) for arg in args], check=True)


def verify(executable):
    actual = subprocess.check_output([str(executable), "--version"], text=True).strip()
    expected = "Cap'n Proto version " + PIN["version"]
    if actual != expected:
        raise SystemExit(
            f"Expected {expected}; PATH provides {actual}. "
            "Run mise run bootstrap:capnp, then use mise exec -- <command>."
        )
    print(actual, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true", help="Assert the PATH compiler matches the pin")
    args = parser.parse_args()
    if args.verify:
        verify("capnp")
        return

    CACHE.mkdir(parents=True, exist_ok=True)
    archive = CACHE / "source.tar.gz"
    if not archive.exists() or hashlib.sha256(archive.read_bytes()).hexdigest() != PIN["sha256"]:
        temporary = archive.with_suffix(".download")
        urllib.request.urlretrieve(PIN["url"], temporary)
        actual = hashlib.sha256(temporary.read_bytes()).hexdigest()
        if actual != PIN["sha256"]:
            temporary.unlink()
            raise SystemExit(f"Cap'n Proto source checksum mismatch: {actual}")
        temporary.replace(archive)

    source = CACHE / ("capnproto-c++-" + PIN["version"])
    source_stamp = CACHE / "source.sha256"
    if not source.exists() or not source_stamp.exists() or source_stamp.read_text() != PIN["sha256"]:
        shutil.rmtree(source, ignore_errors=True)
        shutil.rmtree(CACHE / "build", ignore_errors=True)
        shutil.rmtree(INSTALL, ignore_errors=True)
        with tarfile.open(archive) as contents:
            contents.extractall(CACHE, filter="data")
        source_stamp.write_text(PIN["sha256"])

    build = CACHE / "build"
    run(
        "cmake", "-S", source, "-B", build,
        "-DCMAKE_BUILD_TYPE=Release", "-DBUILD_TESTING=OFF",
        "-DWITH_OPENSSL=OFF", "-DWITH_ZLIB=OFF",
        "-DCMAKE_INSTALL_LIBDIR=lib", f"-DCMAKE_INSTALL_PREFIX={INSTALL}",
    )
    run("cmake", "--build", build, "--config", "Release", "--parallel", "4")
    run("cmake", "--install", build, "--config", "Release")
    executable = INSTALL / "bin" / ("capnp.exe" if os.name == "nt" else "capnp")
    verify(executable)
    # setup-capnp shares this bootstrap with local mise tasks; no second pin.
    if "GITHUB_PATH" in os.environ:
        with open(os.environ["GITHUB_PATH"], "a", encoding="utf-8") as output:
            output.write(str(INSTALL / "bin") + "\n")
    print(f"Compiler installed in {INSTALL / 'bin'}", flush=True)


if __name__ == "__main__":
    main()
