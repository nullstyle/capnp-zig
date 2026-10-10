// Portable compiler acceptance tests; the real pinned package is required.
//
// tools/capnp_tool.ts installs and verifies the pinned capnpc-wasm tools
// archive and delegates compiling and generating to its launcher
// (package/bin/capnp-wasm.ts). These tests run that path end to end; the path
// translation tests call the installed launcher's own translation.
//
//   deno test --allow-all --no-config tools/capnp_tool_test.ts

import * as tool from "./capnp_tool.ts";

const DRIVER = `${tool.ROOT}/tools/capnp_tool.ts`;
const text = new TextDecoder();
const bytes = new TextEncoder();

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function assertEquals(actual: unknown, expected: unknown, label: string) {
  const [a, b] = [JSON.stringify(actual), JSON.stringify(expected)];
  assert(a === b, `${label}: expected ${b}, got ${a}`);
}

async function assertRejects(run: () => Promise<unknown>, pattern?: RegExp) {
  try {
    await run();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    assert(!pattern || pattern.test(message), `unexpected error: ${message}`);
    return;
  }
  throw new Error(`expected an error${pattern ? ` matching ${pattern}` : ""}`);
}

async function exists(path: string): Promise<boolean> {
  try {
    await Deno.lstat(path);
    return true;
  } catch {
    return false;
  }
}

async function copyTree(source: string, destination: string) {
  await Deno.mkdir(destination, { recursive: true });
  for await (const entry of Deno.readDir(source)) {
    const from = `${source}/${entry.name}`;
    const to = `${destination}/${entry.name}`;
    if (entry.isDirectory) await copyTree(from, to);
    else await Deno.copyFile(from, to);
  }
}

async function scratch(prefix: string, fn: (root: string) => Promise<void>) {
  const root = await Deno.realPath(await Deno.makeTempDir({ prefix }));
  try {
    await fn(root);
  } finally {
    await Deno.remove(root, { recursive: true }).catch(() => {});
  }
}

// ---------------------------------------------------------------------------
// Integrity of the package and the archive.

async function fixturePackage(root: string) {
  const pkg = `${root}/package`;
  const contents = new Map<string, Uint8Array>([
    ["wasm/capnp.wasm", new Uint8Array([0, 0x61, 0x73, 0x6d, 1, 0, 0, 0])],
    ["include/capnp/schema.capnp", bytes.encode("@0xabc;\n")],
    ["bin/capnp-wasm.ts", bytes.encode("// launcher\n")],
    ["runtime/wasmtime-version", bytes.encode("48.0.1\n")],
  ]);
  const files = [];
  for (const [name, data] of contents) {
    const path = `${pkg}/${name}`;
    await Deno.mkdir(path.slice(0, path.lastIndexOf("/")), { recursive: true });
    await Deno.writeFile(path, data);
    files.push({
      path: name,
      bytes: data.length,
      sha256: await tool.digest(data),
    });
  }
  const raw = bytes.encode(
    JSON.stringify({
      format: 1,
      source: { commit: "a".repeat(40), dirty: false },
      files,
    }),
  );
  await Deno.writeFile(`${pkg}/manifest.json`, raw);
  const pin: tool.Pin = {
    url: "",
    sha256: "",
    manifest_sha256: await tool.digest(raw),
    source_commit: "a".repeat(40),
    compiler_sha256: await tool.digest(contents.get("wasm/capnp.wasm")!),
    include_sha256: await tool.includesDigest(contents),
  };
  return { pkg, pin };
}

Deno.test("a verified package rejects tampering, missing, and extra files", async () => {
  await scratch("capnp integrity ", async (root) => {
    const { pkg, pin } = await fixturePackage(root);
    assertEquals(await tool.verifyPackage(pkg, pin), pkg, "verified package");
    const path = `${pkg}/wasm/capnp.wasm`;
    const original = await Deno.readFile(path);
    await Deno.writeTextFile(path, "changed");
    await assertRejects(() => tool.verifyPackage(pkg, pin), /integrity/);
    await Deno.remove(path);
    await assertRejects(() => tool.verifyPackage(pkg, pin), /integrity/);
    await Deno.writeFile(path, original);
    await Deno.writeTextFile(`${pkg}/extra`, "unexpected");
    await assertRejects(() => tool.verifyPackage(pkg, pin), /inventory/);
  });
});

Deno.test("a replaced manifest cannot bless a modified compiler", async () => {
  await scratch("capnp integrity ", async (root) => {
    const { pkg, pin } = await fixturePackage(root);
    const manifest = JSON.parse(
      await Deno.readTextFile(`${pkg}/manifest.json`),
    );
    await Deno.writeTextFile(`${pkg}/wasm/capnp.wasm`, "forged");
    Object.assign(manifest.files[0], {
      bytes: 6,
      sha256: await tool.digest(bytes.encode("forged")),
    });
    await Deno.writeTextFile(`${pkg}/manifest.json`, JSON.stringify(manifest));
    await assertRejects(() => tool.verifyPackage(pkg, pin), /manifest digest/);
  });
});

Deno.test("the source, compiler, and include pins are enforced", async () => {
  await scratch("capnp integrity ", async (root) => {
    const { pkg, pin } = await fixturePackage(root);
    for (
      const field of [
        "source_commit",
        "compiler_sha256",
        "include_sha256",
      ] as const
    ) {
      await assertRejects(() =>
        tool.verifyPackage(pkg, {
          ...pin,
          [field]: "0".repeat(pin[field].length),
        })
      );
    }
  });
});

/** A gzip-compressed ustar archive of the given members. */
async function tarball(
  members: { name: string; type?: string; data?: string; link?: string }[],
) {
  const chunks: Uint8Array[] = [];
  for (const member of members) {
    const data = bytes.encode(member.data ?? "");
    const header = new Uint8Array(512);
    header.set(bytes.encode(member.name));
    const field = (offset: number, width: number, value: number) =>
      header.set(
        bytes.encode(value.toString(8).padStart(width - 1, "0") + "\0"),
        offset,
      );
    field(100, 8, 0o644);
    field(108, 8, 0);
    field(116, 8, 0);
    field(124, 12, data.length);
    field(136, 12, 0);
    header.fill(32, 148, 156);
    header[156] = (member.type ?? "0").charCodeAt(0);
    if (member.link) header.set(bytes.encode(member.link), 157);
    header.set(bytes.encode("ustar\0" + "00"), 257);
    field(148, 7, header.reduce((sum, byte) => sum + byte, 0));
    header[155] = 32;
    chunks.push(header, data, new Uint8Array((512 - data.length % 512) % 512));
  }
  chunks.push(new Uint8Array(1024));
  return new Uint8Array(
    await new Response(
      new Blob(chunks.map((chunk) => new Uint8Array(chunk))).stream()
        .pipeThrough(new CompressionStream("gzip")),
    ).arrayBuffer(),
  );
}

Deno.test("unsafe archives are rejected before extraction", async () => {
  await scratch("capnp archive ", async (root) => {
    const cases: [string, string][] = [
      ["../escape", "0"],
      ["/absolute", "0"],
      ["package/link", "2"],
      ["package/link", "1"],
      ["C:/escape", "0"],
      ["package/file:stream", "0"],
      ["package/CON", "0"],
      ["package/trailing.", "0"],
    ];
    for (const [name, type] of cases) {
      const archive = await tarball([
        { name: "package/good", data: "ok" },
        { name, type, link: "../escape" },
      ]);
      const out = `${root}/unpacked`;
      await assertRejects(() => tool.extractArchive(archive, out));
      assert(!await exists(out), `${name} (${type}) created ${out}`);
    }
  });
});

Deno.test("archive aliases and file-parent collisions are rejected before extraction", async () => {
  await scratch("capnp archive ", async (root) => {
    for (
      const names of [
        ["package/file", "package/file"],
        ["package/File", "package/file"],
        ["package/parent", "package/parent/child"],
      ]
    ) {
      const archive = await tarball(names.map((name) => ({ name })));
      const out = `${root}/unpacked`;
      await assertRejects(() => tool.extractArchive(archive, out));
      assert(!await exists(out), `${names} created ${out}`);
    }
  });
});

// ---------------------------------------------------------------------------
// The installed package, end to end.

interface Result {
  code: number;
  stdout: Uint8Array;
  stderr: string;
}

async function cli(
  args: string[],
  options: { cwd: string; input?: Uint8Array; env?: Record<string, string> },
): Promise<Result> {
  const child = new Deno.Command(Deno.execPath(), {
    args: ["run", "--allow-all", "--no-config", DRIVER, ...args],
    cwd: options.cwd,
    env: options.env,
    stdin: options.input ? "piped" : "null",
    stdout: "piped",
    stderr: "piped",
    signal: AbortSignal.timeout(60_000),
  }).spawn();
  if (options.input) {
    const writer = child.stdin.getWriter();
    await writer.write(options.input).catch(() => {});
    await writer.close().catch(() => {});
  }
  const output = await child.output();
  return {
    code: output.code,
    stdout: output.stdout,
    stderr: text.decode(output.stderr),
  };
}

function same(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && a.every((byte, index) => byte === b[index]);
}

function compilerTest(name: string, fn: (cwd: string) => Promise<void>) {
  Deno.test(name, () => scratch("capnp tool test ", fn));
}

compilerTest(
  "the pinned compiler runs from an unrelated directory",
  async (cwd) => {
    const result = await cli(["compiler", "--", "--version"], { cwd });
    assertEquals(result.code, 0, result.stderr);
    assertEquals(
      text.decode(result.stdout),
      "Cap'n Proto version 2.0-dev\n",
      "version",
    );
  },
);

compilerTest(
  "standard imports are available only when permitted",
  async (cwd) => {
    await Deno.writeTextFile(
      `${cwd}/schema with spaces.capnp`,
      '@0xf3d4f49d221cd00f;\nusing Cxx = import "/capnp/c++.capnp";\n$Cxx.namespace("fixture");\nstruct Example { value @0 :UInt32; }\n',
    );
    const args = ["compile", "-o-", "schema with spaces.capnp"];
    const result = await cli(["compiler", "--", ...args], { cwd });
    assertEquals(result.code, 0, result.stderr);
    assert(result.stdout.length > 8, "empty request");
    const repeated = await cli(["compiler", "--", ...args], { cwd });
    assert(
      repeated.code === 0 && same(repeated.stdout, result.stdout),
      "repeated compile differs",
    );
    const isolated = await cli([
      "compiler",
      "--",
      ...args,
      "--no-standard-import",
    ], { cwd });
    assert(
      isolated.code !== 0 && isolated.stderr.includes("Import failed"),
      isolated.stderr,
    );
    const verbose = await cli(["compiler", "--", "--verbose", ...args], {
      cwd,
    });
    assert(
      verbose.code === 0 && same(verbose.stdout, result.stdout),
      verbose.stderr,
    );
  },
);

compilerTest(
  "the fallback prefix matches the direct module and a relocated package",
  async (cwd) => {
    await Deno.writeTextFile(
      `${cwd}/schema.capnp`,
      '@0xf3d4f49d221cd00f; using Cxx = import "/capnp/c++.capnp";\n$Cxx.namespace("fixture"); struct Example { value @0 :UInt32; }\n',
    );
    const pkg = await tool.installedPackage(tool.ROOT);
    await copyTree(`${pkg}/include`, `${cwd}/fixture includes`);
    const expected = await new Deno.Command(
      Deno.env.get("CAPNP_WASM_WASMTIME") ?? "wasmtime",
      {
        args: [
          "run",
          "-W",
          "exceptions=y",
          "--dir",
          `${cwd}::/`,
          `${pkg}/wasm/capnp.wasm`,
          "compile",
          "--no-standard-import",
          "-Ifixture includes",
          "-o-",
          "schema.capnp",
        ],
        stdout: "piped",
        stderr: "piped",
      },
    ).output();
    assertEquals(expected.code, 0, text.decode(expected.stderr));
    const ordinary = await cli([
      "compiler",
      "--",
      "compile",
      "-o-",
      "schema.capnp",
    ], { cwd });
    assert(
      ordinary.code === 0 && same(ordinary.stdout, expected.stdout),
      ordinary.stderr,
    );
    const cache = `${cwd}/relocated cache with spaces`;
    const pin = await tool.toolPin(tool.ROOT);
    await copyTree(pkg, `${cache}/artifacts/${pin.sha256}/package`);
    const relocated = await cli([
      "compiler",
      "--",
      "compile",
      "-o-",
      "schema.capnp",
    ], {
      cwd,
      env: { CAPNP_WASM_CACHE: cache },
    });
    assert(
      relocated.code === 0 && same(relocated.stdout, expected.stdout),
      relocated.stderr,
    );
  },
);

compilerTest(
  "absolute paths, a nested cwd, and prefixes preserve request bytes",
  async (cwd) => {
    const schemas = `${cwd}/schema directory`;
    const nested = `${schemas}/nested`;
    await Deno.mkdir(nested, { recursive: true });
    await Deno.writeTextFile(
      `${schemas}/shared.capnp`,
      "@0xfa8d2093c0ff2ca1; struct Shared { value @0 :UInt32; }",
    );
    const source = `${nested}/source with spaces.capnp`;
    await Deno.writeTextFile(
      source,
      '@0xf3d4f49d221cd00f;\nusing S = import "../shared.capnp";\nusing Cxx = import "/capnp/c++.capnp";\n$Cxx.namespace("fixture");\nstruct Example { value @0 :S.Shared; }\n',
    );
    const relative = await cli([
      "compiler",
      "--",
      "compile",
      "-o-",
      "--src-prefix=schema directory",
      "schema directory/nested/source with spaces.capnp",
    ], { cwd });
    const absolute = await cli([
      "compiler",
      "--",
      "compile",
      "-o-",
      "--src-prefix",
      schemas,
      source,
    ], { cwd });
    const ancestor = await cli([
      "compiler",
      "--",
      "compile",
      "-o-",
      "--src-prefix=..",
      "source with spaces.capnp",
    ], {
      cwd: nested,
    });
    for (const result of [relative, absolute, ancestor]) {
      assert(
        result.code === 0 && same(result.stdout, relative.stdout),
        result.stderr,
      );
    }
  },
);

compilerTest(
  "an explicit include precedes the bundled one and binary streams round-trip",
  async (cwd) => {
    const includes = `${cwd}/custom includes/capnp`;
    await Deno.mkdir(includes, { recursive: true });
    // Deliberately shadow a standard file with a schema the bundle lacks.
    await Deno.writeTextFile(
      `${includes}/c++.capnp`,
      '@0xdc82e6c1e795fb53; const marker :Text = "custom";',
    );
    const source = `${cwd}/value.capnp`;
    await Deno.writeTextFile(
      source,
      '@0xf3d4f49d221cd00f;\nusing Cxx = import "/capnp/c++.capnp";\nconst chosen :Text = Cxx.marker;\nstruct Value { data @0 :Data; count @1 :UInt32; }',
    );
    const common = ["-I", `${cwd}/custom includes`];
    const evaluated = await cli([
      "compiler",
      "--",
      "eval",
      ...common,
      "-otext",
      source,
      "chosen",
    ], { cwd });
    assert(evaluated.code === 0, evaluated.stderr);
    assertEquals(text.decode(evaluated.stdout).trim(), '"custom"', "eval");
    const encoded = await cli([
      "compiler",
      "--",
      "encode",
      ...common,
      source,
      "Value",
    ], {
      cwd,
      input: bytes.encode('(data = 0x"000aff7f0d", count = 16909060)\n'),
    });
    assert(encoded.code === 0, encoded.stderr);
    const decoded = await cli([
      "compiler",
      "--",
      "decode",
      ...common,
      source,
      "Value",
    ], {
      cwd,
      input: encoded.stdout,
    });
    assert(
      decoded.code === 0 &&
        text.decode(decoded.stdout).includes("\\000\\n\\377\\177\\r"),
      decoded.stderr,
    );
    const canonical = await cli([
      "compiler",
      "--",
      "convert",
      "binary:canonical",
      ...common,
      source,
      "Value",
    ], {
      cwd,
      input: encoded.stdout,
    });
    assert(
      canonical.code === 0 &&
        same(canonical.stdout, encoded.stdout.subarray(8)),
      canonical.stderr,
    );
  },
);

/** A native generator: this Deno running a fixture script. */
async function fixturePlugin(
  cwd: string,
  name: string,
  source: string,
): Promise<string[]> {
  const script = `${cwd}/${name}`;
  await Deno.writeTextFile(script, source);
  return [
    "--plugin",
    Deno.execPath(),
    "--plugin-arg",
    "run",
    "--plugin-arg=--allow-all",
    "--plugin-arg=--no-config",
    "--plugin-arg",
    script,
  ];
}

compilerTest(
  "a generator failure leaves existing output and propagates its exit",
  async (cwd) => {
    await Deno.writeTextFile(
      `${cwd}/a.capnp`,
      "@0xf3d4f49d221cd00f; struct A { value @0 :UInt32; }",
    );
    const plugin = await fixturePlugin(
      cwd,
      "fixture plugin.ts",
      `const data = new Uint8Array(await new Response(Deno.stdin.readable).arrayBuffer());
if (data.length < 4 || data.subarray(0, 4).some((byte) => byte !== 0)) Deno.exit(99);
await Deno.writeFile("generated.bin", data);
Deno.exit(Number(Deno.args[0]));
`,
    );
    const output = `${cwd}/output with spaces`;
    await Deno.mkdir(output);
    await Deno.writeTextFile(`${output}/generated.bin`, "existing");
    const args = (
      code: string,
    ) => [
      "generate",
      ...plugin,
      "--plugin-arg",
      code,
      "--output",
      output,
      "--",
      "a.capnp",
    ];
    const failed = await cli(args("37"), { cwd });
    assertEquals(failed.code, 37, failed.stderr);
    assertEquals(
      await Deno.readTextFile(`${output}/generated.bin`),
      "existing",
      "output after a failure",
    );
    const succeeded = await cli(args("0"), { cwd });
    assertEquals(succeeded.code, 0, succeeded.stderr);
    const expected = await cli(
      ["compiler", "--", "compile", "-o-", "a.capnp"],
      { cwd },
    );
    assert(expected.code === 0, expected.stderr);
    assert(
      same(await Deno.readFile(`${output}/generated.bin`), expected.stdout),
      "generated request differs",
    );
    await Deno.writeTextFile(`${cwd}/a.capnp`, "invalid schema");
    const invalid = await cli(args("0"), { cwd });
    assertEquals(invalid.code, 1, invalid.stderr);
    assert(
      same(await Deno.readFile(`${output}/generated.bin`), expected.stdout),
      "output after a failed compile",
    );
  },
);

compilerTest(
  "a missing or wrong runtime fails without a native compiler fallback",
  async (cwd) => {
    for (const executable of [Deno.execPath(), `${cwd}/missing wasmtime`]) {
      const result = await cli(["compiler", "--", "--version"], {
        cwd,
        env: { CAPNP_WASM_WASMTIME: executable },
      });
      assert(result.code !== 0, `${executable} was accepted`);
      assertEquals(result.stdout.length, 0, "stdout");
      assert(result.stderr.includes("capnp-wasm:"), result.stderr);
    }
  },
);

compilerTest(
  "bootstrap and the cached compiler reject tampering",
  async (cwd) => {
    const cache = `${cwd}/isolated cache`;
    const env = { CAPNP_WASM_CACHE: cache };
    await Deno.writeTextFile(`${cwd}/tampered.tgz`, "not the pinned archive");
    const rejected = await cli([
      "bootstrap",
      "--archive",
      `${cwd}/tampered.tgz`,
    ], { cwd, env });
    assert(
      rejected.code !== 0 &&
        rejected.stderr.includes("archive digest mismatch"),
      rejected.stderr,
    );
    const pin = await tool.toolPin(tool.ROOT);
    const pkg = `${cache}/artifacts/${pin.sha256}/package`;
    assert(!await exists(pkg), "a rejected archive was installed");
    await copyTree(await tool.installedPackage(tool.ROOT), pkg);
    await Deno.writeTextFile(`${pkg}/wasm/capnp.wasm`, "tampered module");
    const tampered = await cli(["compiler", "--", "--version"], { cwd, env });
    assert(
      tampered.code !== 0 && tampered.stderr.includes("integrity mismatch"),
      tampered.stderr,
    );
    assertEquals(tampered.stdout.length, 0, "stdout");
  },
);

compilerTest(
  "a compile error keeps the guest's exit and diagnostic",
  async (cwd) => {
    await Deno.writeTextFile(`${cwd}/broken.capnp`, "invalid schema");
    const result = await cli([
      "compiler",
      "--",
      "compile",
      "--no-standard-import",
      "-o-",
      "broken.capnp",
    ], { cwd });
    assertEquals(result.code, 1, result.stderr);
    assertEquals(result.stdout.length, 0, "stdout");
    assert(
      result.stderr.includes("broken.capnp:") &&
        result.stderr.includes("error:"),
      result.stderr,
    );
  },
);

compilerTest(
  "generator flags are opaque and conflicting compiler flags are rejected",
  async (cwd) => {
    await Deno.writeTextFile(
      `${cwd}/a.capnp`,
      "@0xf3d4f49d221cd00f; struct A { value @0 :UInt32; }",
    );
    const plugin = await fixturePlugin(
      cwd,
      "flag plugin.ts",
      `if (JSON.stringify(Deno.args) !== JSON.stringify(["--expected flag", "literal;$value"])) Deno.exit(98);
await Deno.writeFile("result", new Uint8Array(await new Response(Deno.stdin.readable).arrayBuffer()));
`,
    );
    const output = `${cwd}/flag output`;
    const args = [
      "generate",
      ...plugin,
      "--plugin-arg=--expected flag",
      "--plugin-arg=literal;$value",
      "--output",
      output,
      "--",
    ];
    const valid = await cli([...args, "a.capnp"], { cwd });
    assertEquals(valid.code, 0, valid.stderr);
    const original = await Deno.readFile(`${output}/result`);
    for (const options of [["compile", "a.capnp"], ["-o-", "a.capnp"]]) {
      const rejected = await cli([...args, ...options], { cwd });
      assert(
        rejected.code !== 0 && rejected.stderr.includes("must omit compile"),
        rejected.stderr,
      );
      assert(
        same(await Deno.readFile(`${output}/result`), original),
        "output changed",
      );
    }
    await Deno.rename(`${cwd}/a.capnp`, `${cwd}/-leading.capnp`);
    const dashFile = await cli([...args, "--", "-leading.capnp"], { cwd });
    assertEquals(dashFile.code, 0, dashFile.stderr);
    const compiled = await cli([
      "compiler",
      "--",
      "compile",
      "-o-",
      "--",
      "-leading.capnp",
    ], { cwd });
    assertEquals(compiled.code, 0, compiled.stderr);
    assert(
      same(await Deno.readFile(`${output}/result`), compiled.stdout),
      "result differs from the request",
    );
  },
);

// ---------------------------------------------------------------------------
// The installed launcher's translation of Windows paths (capnp mode).

async function installedLauncher() {
  const pkg = await tool.installedPackage(tool.ROOT);
  const path = `${pkg}/${tool.LAUNCHER}`.replaceAll("\\", "/");
  return await import(
    new URL(`file://${path.startsWith("/") ? "" : "/"}${encodeURI(path)}`).href
  );
}

Deno.test("Windows drive paths and separators share one translation", async () => {
  const launcher = await installedLauncher();
  const { root, guestCwd, args } = launcher.translatePaths(
    [
      "compile",
      "-o-",
      "-IC:\\shared includes",
      "--src-prefix",
      "C:\\work tree\\schemas",
      "schemas\\nested\\file.capnp",
    ],
    "C:\\work tree",
    undefined,
    true,
  );
  assertEquals(root, "C:\\", "root");
  assertEquals(guestCwd, "/work tree", "guest cwd");
  assertEquals(args, [
    "compile",
    "-o-",
    "-I/shared includes",
    "--src-prefix",
    "/work tree/schemas",
    "/work tree/schemas/nested/file.capnp",
    "--src-prefix=/work tree",
  ], "arguments");
});

Deno.test("explicit cross-volume and drive-relative inputs are rejected", async () => {
  const launcher = await installedLauncher();
  for (const filename of ["D:\\other\\file.capnp", "C:ambiguous.capnp"]) {
    let failure: unknown;
    try {
      launcher.translatePaths(
        ["compile", "-o-", filename],
        "C:\\work",
        undefined,
        true,
      );
    } catch (error) {
      failure = error;
    }
    assert(failure instanceof launcher.Failure, `${filename} was accepted`);
  }
});
