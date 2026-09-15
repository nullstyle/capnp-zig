"""Portable compiler acceptance tests; the real pinned module is required."""

from pathlib import Path
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

import capnp_tool as tool

ROOT = Path(__file__).resolve().parent.parent
DRIVER = ROOT / "tools/capnp_tool.py"


class IntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="capnp integrity ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def package(self):
        package = self.root / "package"
        contents = {"wasm/capnp.wasm": b"\x00asm\x01\x00\x00\x00",
                    "include/capnp/schema.capnp": b"@0xabc;\n",
                    "runtime/wasmtime-version": b"48.0.1\n"}
        for name, data in contents.items():
            path = package / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        manifest = {"format": 1, "source": {"commit": "a" * 40, "dirty": False},
                    "files": [{"path": name, "bytes": len(data), "sha256": tool.digest(data)}
                              for name, data in contents.items()]}
        raw = json.dumps(manifest).encode()
        (package / "manifest.json").write_bytes(raw)
        pin = {"manifest_sha256": tool.digest(raw), "source_commit": "a" * 40,
               "compiler_sha256": tool.digest(contents["wasm/capnp.wasm"]),
               "include_sha256": tool.includes_digest(contents)}
        return package, pin

    def test_verified_package_rejects_tampering_missing_and_extra_files(self):
        package, pin = self.package()
        self.assertEqual(tool.verify_package(package, pin), package)
        path = package / "wasm/capnp.wasm"
        original = path.read_bytes()
        path.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "integrity"):
            tool.verify_package(package, pin)
        path.unlink()
        with self.assertRaisesRegex(ValueError, "integrity"):
            tool.verify_package(package, pin)
        path.write_bytes(original)
        (package / "extra").write_bytes(b"unexpected")
        with self.assertRaisesRegex(ValueError, "inventory"):
            tool.verify_package(package, pin)

    def test_manifest_replacement_cannot_bless_modified_compiler(self):
        package, pin = self.package()
        manifest = tool.read_json(package / "manifest.json")
        (package / "wasm/capnp.wasm").write_bytes(b"forged")
        manifest["files"][0].update(bytes=6, sha256=tool.digest(b"forged"))
        (package / "manifest.json").write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "manifest digest"):
            tool.verify_package(package, pin)

    def test_package_source_compiler_and_include_pins_are_enforced(self):
        package, pin = self.package()
        for field in ("source_commit", "compiler_sha256", "include_sha256"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                tool.verify_package(package, dict(pin, **{field: "0" * len(pin[field])}))

    def test_unsafe_archives_are_rejected_before_extraction(self):
        for name, kind in [("../escape", tarfile.REGTYPE), ("/absolute", tarfile.REGTYPE),
                           ("package/link", tarfile.SYMTYPE), ("package/link", tarfile.LNKTYPE),
                           ("C:/escape", tarfile.REGTYPE), ("package/file:stream", tarfile.REGTYPE),
                           ("package/CON", tarfile.REGTYPE), ("package/trailing.", tarfile.REGTYPE)]:
            with self.subTest(name=name, kind=kind):
                archive = self.root / "bad.tgz"
                with tarfile.open(archive, "w:gz") as tar:
                    good = tarfile.TarInfo("package/good")
                    good.size = 2
                    tar.addfile(good, io.BytesIO(b"ok"))
                    bad = tarfile.TarInfo(name)
                    bad.type = kind
                    bad.linkname = "../escape"
                    tar.addfile(bad)
                out = self.root / "unpacked"
                with self.assertRaises(ValueError):
                    tool.extract_archive(archive, out)
                self.assertFalse(out.exists())

    def test_archive_aliases_and_file_parent_collisions_are_rejected_before_extraction(self):
        for names in (("package/file", "package/file"), ("package/File", "package/file"),
                      ("package/parent", "package/parent/child")):
            with self.subTest(names=names):
                archive = self.root / "bad.tgz"
                with tarfile.open(archive, "w:gz") as tar:
                    for name in names:
                        tar.addfile(tarfile.TarInfo(name))
                out = self.root / "unpacked"
                with self.assertRaises(ValueError):
                    tool.extract_archive(archive, out)
                self.assertFalse(out.exists())


class CompilerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="capnp tool test ")
        self.addCleanup(self.temp.cleanup)
        self.cwd = Path(self.temp.name).resolve()

    def cli(self, *args, input=None, cwd=None, env=None):
        return subprocess.run([sys.executable, str(DRIVER), *args], cwd=self.cwd if cwd is None else cwd,
                              input=input, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              env=env, timeout=30)

    def test_pinned_compiler_runs_from_unrelated_directory(self):
        result = self.cli("compiler", "--", "--version")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, b"Cap'n Proto version 2.0-dev\n")

    def test_standard_imports_are_available_only_when_permitted(self):
        (self.cwd / "schema with spaces.capnp").write_text(
            '@0xf3d4f49d221cd00f;\nusing Cxx = import "/capnp/c++.capnp";\n'
            '$Cxx.namespace("fixture");\nstruct Example { value @0 :UInt32; }\n')
        args = ("compile", "-o-", "schema with spaces.capnp")
        result = self.cli("compiler", "--", *args)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertGreater(len(result.stdout), 8)
        repeated = self.cli("compiler", "--", *args)
        self.assertEqual(repeated.returncode, 0, repeated.stderr)
        self.assertEqual(result.stdout, repeated.stdout)
        isolated = self.cli("compiler", "--", *args, "--no-standard-import")
        self.assertNotEqual(isolated.returncode, 0)
        self.assertIn(b"Import failed", isolated.stderr)
        verbose = self.cli("compiler", "--", "--verbose", *args)
        self.assertEqual(verbose.returncode, 0, verbose.stderr)
        self.assertEqual(verbose.stdout, result.stdout)

    def test_fallback_prefix_matches_direct_module_and_relocated_package(self):
        (self.cwd / "schema.capnp").write_text(
            '@0xf3d4f49d221cd00f; using Cxx = import "/capnp/c++.capnp";\n'
            '$Cxx.namespace("fixture"); struct Example { value @0 :UInt32; }\n')
        package = tool.installed_package(ROOT)
        shutil.copytree(package / "include", self.cwd / "fixture includes")
        expected = subprocess.run(
            [tool.runtime(package), "run", "-W", "exceptions=y", "--dir", str(self.cwd) + "::/",
             str(package / "wasm/capnp.wasm"), "compile", "--no-standard-import",
             "-Ifixture includes", "-o-", "schema.capnp"], capture_output=True, timeout=30)
        self.assertEqual(expected.returncode, 0, expected.stderr)
        ordinary = self.cli("compiler", "--", "compile", "-o-", "schema.capnp")
        self.assertEqual(ordinary.returncode, 0, ordinary.stderr)
        self.assertEqual(ordinary.stdout, expected.stdout)
        cache = self.cwd / "relocated cache with spaces"
        pin = tool.tool_pin(ROOT)
        shutil.copytree(package, cache / "artifacts" / pin["sha256"] / "package")
        relocated = self.cli("compiler", "--", "compile", "-o-", "schema.capnp",
                             env=dict(os.environ, CAPNP_WASM_CACHE=str(cache)))
        self.assertEqual(relocated.returncode, 0, relocated.stderr)
        self.assertEqual(relocated.stdout, expected.stdout)

    def test_absolute_paths_nested_cwd_and_prefixes_preserve_request_bytes(self):
        schemas = self.cwd / "schema directory"
        schemas.mkdir()
        nested = schemas / "nested"
        nested.mkdir()
        (schemas / "shared.capnp").write_text('@0xfa8d2093c0ff2ca1; struct Shared { value @0 :UInt32; }')
        source = nested / "source with spaces.capnp"
        source.write_text('@0xf3d4f49d221cd00f;\nusing S = import "../shared.capnp";\n'
                          'using Cxx = import "/capnp/c++.capnp";\n$Cxx.namespace("fixture");\n'
                          'struct Example { value @0 :S.Shared; }\n')
        relative = self.cli("compiler", "--", "compile", "-o-", "--src-prefix=schema directory",
                            "schema directory/nested/source with spaces.capnp")
        absolute = self.cli("compiler", "--", "compile", "-o-", "--src-prefix", str(schemas), str(source))
        ancestor = self.cli("compiler", "--", "compile", "-o-", "--src-prefix=..", source.name, cwd=nested)
        for result in (relative, absolute, ancestor):
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, relative.stdout)

    def test_explicit_include_precedes_bundled_include_and_binary_streams_round_trip(self):
        includes = self.cwd / "custom includes" / "capnp"
        includes.mkdir(parents=True)
        # Deliberately shadow a standard file with a schema the bundle lacks.
        (includes / "c++.capnp").write_text('@0xdc82e6c1e795fb53; const marker :Text = "custom";')
        source = self.cwd / "value.capnp"
        source.write_text('@0xf3d4f49d221cd00f;\nusing Cxx = import "/capnp/c++.capnp";\n'
                          'const chosen :Text = Cxx.marker;\nstruct Value { data @0 :Data; count @1 :UInt32; }')
        common = ("-I", str(includes.parent))
        evaluated = self.cli("compiler", "--", "eval", *common, "-otext", str(source), "chosen")
        self.assertEqual(evaluated.returncode, 0, evaluated.stderr)
        self.assertEqual(evaluated.stdout.strip(), b'"custom"')
        encoded = self.cli("compiler", "--", "encode", *common, str(source), "Value",
                           input=b'(data = 0x"000aff7f0d", count = 16909060)\n')
        self.assertEqual(encoded.returncode, 0, encoded.stderr)
        decoded = self.cli("compiler", "--", "decode", *common, str(source), "Value", input=encoded.stdout)
        self.assertEqual(decoded.returncode, 0, decoded.stderr)
        self.assertIn(b'\\000\\n\\377\\177\\r', decoded.stdout)
        canonical = self.cli("compiler", "--", "convert", "binary:canonical", *common,
                             str(source), "Value", input=encoded.stdout)
        self.assertEqual(canonical.returncode, 0, canonical.stderr)
        self.assertEqual(canonical.stdout, encoded.stdout[8:])

    def test_generator_failure_leaves_existing_output_and_propagates_exit(self):
        (self.cwd / "a.capnp").write_text('@0xf3d4f49d221cd00f; struct A { value @0 :UInt32; }')
        plugin = self.cwd / "fixture plugin.py"
        plugin.write_text('import pathlib,sys\n'
                          'data=sys.stdin.buffer.read()\n'
                          'assert data and data[0:4] == bytes(4)\n'
                          'pathlib.Path("generated.bin").write_bytes(data)\n'
                          'sys.exit(int(sys.argv[1]))\n')
        output = self.cwd / "output with spaces"
        output.mkdir()
        (output / "generated.bin").write_bytes(b"existing")
        args = ("generate", "--plugin", sys.executable, "--output", str(output),
                "--plugin-arg", str(plugin), "--plugin-arg", "37", "--", "a.capnp")
        failed = self.cli(*args)
        self.assertEqual(failed.returncode, 37, failed.stderr)
        self.assertEqual((output / "generated.bin").read_bytes(), b"existing")
        succeeded = self.cli(*(tuple("0" if arg == "37" else arg for arg in args)))
        self.assertEqual(succeeded.returncode, 0, succeeded.stderr)
        expected = self.cli("compiler", "--", "compile", "-o-", "a.capnp")
        self.assertEqual(expected.returncode, 0, expected.stderr)
        self.assertEqual((output / "generated.bin").read_bytes(), expected.stdout)
        (self.cwd / "a.capnp").write_text("invalid schema")
        invalid = self.cli(*args)
        self.assertEqual(invalid.returncode, 1, invalid.stderr)
        self.assertEqual((output / "generated.bin").read_bytes(), expected.stdout)

    def test_missing_or_wrong_runtime_fails_without_native_compiler_fallback(self):
        for executable in (sys.executable, str(self.cwd / "missing wasmtime")):
            with self.subTest(executable=executable):
                result = self.cli("compiler", "--", "--version",
                                  env=dict(os.environ, CAPNP_WASM_WASMTIME=executable))
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, b"")
                self.assertIn(b"capnp-wasm:", result.stderr)

    def test_bootstrap_and_cached_compiler_reject_tampering(self):
        cache = self.cwd / "isolated cache"
        env = dict(os.environ, CAPNP_WASM_CACHE=str(cache))
        archive = self.cwd / "tampered.tgz"
        archive.write_bytes(b"not the pinned archive")
        rejected = self.cli("bootstrap", "--archive", str(archive), env=env)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn(b"archive digest mismatch", rejected.stderr)
        self.assertFalse(any(cache.rglob("package")))
        pin = tool.tool_pin(ROOT)
        package = cache / "artifacts" / pin["sha256"] / "package"
        shutil.copytree(tool.installed_package(ROOT), package)
        (package / "wasm/capnp.wasm").write_bytes(b"tampered module")
        rejected = self.cli("compiler", "--", "--version", env=env)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn(b"integrity mismatch", rejected.stderr)
        self.assertEqual(rejected.stdout, b"")

    def test_compile_error_preserves_guest_exit_and_diagnostic(self):
        (self.cwd / "broken.capnp").write_text("invalid schema")
        result = self.cli("compiler", "--", "compile", "--no-standard-import", "-o-", "broken.capnp")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, b"")
        self.assertIn(b"broken.capnp:", result.stderr)
        self.assertIn(b"error:", result.stderr)

    def test_generator_flags_are_opaque_and_conflicting_compiler_flags_are_rejected(self):
        (self.cwd / "a.capnp").write_text('@0xf3d4f49d221cd00f; struct A { value @0 :UInt32; }')
        plugin = self.cwd / "flag plugin.py"
        plugin.write_text('import pathlib,sys\n'
                          'assert sys.argv[1:] == ["--expected flag", "literal;$value"]\n'
                          'pathlib.Path("result").write_bytes(sys.stdin.buffer.read())\n')
        output = self.cwd / "flag output"
        args = ("generate", "--plugin", sys.executable, "--output", str(output),
                "--plugin-arg", str(plugin), "--plugin-arg=--expected flag",
                "--plugin-arg=literal;$value", "--")
        valid = self.cli(*args, "a.capnp")
        self.assertEqual(valid.returncode, 0, valid.stderr)
        original = (output / "result").read_bytes()
        for options in (("compile", "a.capnp"), ("-o-", "a.capnp")):
            rejected = self.cli(*args, *options)
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn(b"must omit compile", rejected.stderr)
            self.assertEqual((output / "result").read_bytes(), original)
        (self.cwd / "a.capnp").rename(self.cwd / "-leading.capnp")
        dash_file = self.cli(*args, "--", "-leading.capnp")
        self.assertEqual(dash_file.returncode, 0, dash_file.stderr)
        compiled = self.cli("compiler", "--", "compile", "-o-", "--", "-leading.capnp")
        self.assertEqual(compiled.returncode, 0, compiled.stderr)
        self.assertEqual((output / "result").read_bytes(), compiled.stdout)


class WindowsPathTests(unittest.TestCase):
    def test_drive_paths_and_separators_share_one_translation(self):
        root, cwd, args = tool.compiler_paths(
            ["compile", "-o-", "-IC:\\shared includes", "--src-prefix", "C:\\work tree\\schemas",
             "schemas\\nested\\file.capnp"], "C:\\work tree", windows=True)
        self.assertEqual(root, "C:\\")
        self.assertEqual(cwd, "/work tree")
        self.assertEqual(args, ["compile", "-o-", "-I/shared includes", "--src-prefix",
                                "/work tree/schemas", "/work tree/schemas/nested/file.capnp",
                                "--src-prefix=/work tree"])

    def test_explicit_cross_volume_and_drive_relative_inputs_are_rejected(self):
        for filename in ("D:\\other\\file.capnp", "C:ambiguous.capnp"):
            with self.subTest(filename=filename), self.assertRaises(ValueError):
                tool.compiler_paths(["compile", "-o-", filename], "C:\\work", windows=True)


if __name__ == "__main__":
    unittest.main()
