#!/usr/bin/env -S deno run --allow-all --no-config
// Drift signal: does the released capnpc-zig.wasm generate what this checkout
// does?
//
// capnpc-wasm ships capnpc-zig.wasm, built from the capnp-zig revision its
// release pins (`capnp_zig` in tools/capnpc-wasm-generator.json). Consumers
// that generate with that module get that revision's output, not this
// checkout's. This check installs the pinned full SDK archive, runs its
// capnpc-zig.wasm under the packaged launcher on every committed
// CodeGeneratorRequest, runs this checkout's native plugin on the same
// requests, and compares the files byte for byte. A difference means a
// generator change here that no capnpc-wasm release carries yet; it is a
// signal to cut one, not a defect in this checkout.
//
//   deno run --allow-all --no-config tools/wasm_generator_drift.ts --plugin zig-out/bin/capnpc-zig
//
// Exit status: 0 when every request generates identical files, 1 on a
// difference or an error. --archive installs a local archive with the pinned
// digest.

import {
  cacheRoot,
  digest,
  extractArchive,
  inventory,
  LAUNCHER,
  ROOT,
  safePath,
} from "./capnp_tool.ts";

const PIN = `${ROOT}/tools/capnpc-wasm-generator.json`;
const MODULE = "wasm/capnpc-zig.wasm";
const REQUESTS = [
  "tests/package_consumer/codegen/schema/addressbook.request.bin",
  "tests/docs/schema/guide.request.bin",
];
const REQUEST_DIRECTORY = "tests/generated_shape/requests";

interface GeneratorPin {
  format: number;
  url: string;
  sha256: string;
  manifest_sha256: string;
  source_commit: string;
  generator_sha256: string;
  capnp_zig: string;
}

async function generatorPin(): Promise<GeneratorPin> {
  const pin = JSON.parse(await Deno.readTextFile(PIN));
  if (pin.format !== 1) throw new Error("unsupported generator pin format");
  const unfilled = Object.entries(pin).filter(([, value]) =>
    typeof value === "string" && value.includes("FILL-IN")
  ).map(([field]) => field);
  if (unfilled.length > 0) {
    throw new Error(
      `tools/capnpc-wasm-generator.json is not filled in (${
        unfilled.join(", ")
      }); run tools/update_capnp_toolchain.ts generator <tag> after the release is published`,
    );
  }
  for (const field of ["sha256", "manifest_sha256", "generator_sha256"]) {
    if (!/^[0-9a-f]{64}$/.test(pin[field])) {
      throw new Error(`invalid generator pin digest: ${field}`);
    }
  }
  for (const field of ["source_commit", "capnp_zig"]) {
    if (!/^[0-9a-f]{40}$/.test(pin[field])) {
      throw new Error(`invalid generator pin revision: ${field}`);
    }
  }
  return pin;
}

async function verify(pkg: string, pin: GeneratorPin): Promise<string> {
  const files = await inventory(pkg);
  const raw = files.get("manifest.json") ?? new Uint8Array();
  if (await digest(raw) !== pin.manifest_sha256) {
    throw new Error("generator package manifest digest mismatch");
  }
  const manifest = JSON.parse(new TextDecoder().decode(raw));
  if (manifest.source.commit !== pin.source_commit || manifest.source.dirty) {
    throw new Error("generator package source identity mismatch");
  }
  if (manifest.references?.["ref/capnp-zig"] !== pin.capnp_zig) {
    throw new Error("generator package names another capnp-zig revision");
  }
  const expected = new Set(["manifest.json"]);
  for (const entry of manifest.files) {
    const name = safePath(entry.path);
    expected.add(name);
    const data = files.get(name);
    if (
      data === undefined || data.length !== entry.bytes ||
      await digest(data) !== entry.sha256
    ) throw new Error(`generator package integrity mismatch: ${name}`);
  }
  if (
    files.size !== expected.size ||
    [...files.keys()].some((name) => !expected.has(name))
  ) throw new Error("generator package inventory mismatch");
  if (await digest(files.get(MODULE)!) !== pin.generator_sha256) {
    throw new Error("capnpc-zig.wasm digest mismatch");
  }
  if (!files.has(LAUNCHER)) {
    throw new Error(`generator package has no ${LAUNCHER}`);
  }
  return pkg;
}

async function exists(path: string): Promise<boolean> {
  try {
    await Deno.lstat(path);
    return true;
  } catch {
    return false;
  }
}

async function install(
  pin: GeneratorPin,
  archiveOverride?: string,
): Promise<string> {
  const generators = `${cacheRoot(ROOT)}/generator`;
  const destination = `${generators}/${pin.sha256}/package`;
  if (await exists(destination)) return await verify(destination, pin);
  await Deno.mkdir(generators, { recursive: true });
  const temp = await Deno.makeTempDir({ dir: generators, prefix: "install-" });
  try {
    const archive = archiveOverride
      ? await Deno.readFile(archiveOverride)
      : new Uint8Array(
        await (await fetch(pin.url, { signal: AbortSignal.timeout(120_000) }))
          .arrayBuffer(),
      );
    if (await digest(archive) !== pin.sha256) {
      throw new Error("generator archive digest mismatch");
    }
    const unpacked = `${temp}/unpacked`;
    await Deno.mkdir(unpacked);
    await extractArchive(archive, unpacked);
    await verify(`${unpacked}/package`, pin);
    await Deno.mkdir(`${generators}/${pin.sha256}`, { recursive: true });
    await Deno.rename(`${unpacked}/package`, destination);
  } finally {
    await Deno.remove(temp, { recursive: true }).catch(() => {});
  }
  return destination;
}

async function tree(root: string): Promise<Map<string, Uint8Array>> {
  return await exists(root) ? await inventory(root) : new Map();
}

async function run(
  command: string[],
  options: { input: Uint8Array; env?: Record<string, string>; cwd?: string },
  label: string,
) {
  const child = new Deno.Command(command[0], {
    args: command.slice(1),
    env: options.env,
    cwd: options.cwd,
    stdin: "piped",
    stdout: "piped",
    stderr: "piped",
  }).spawn();
  const writer = child.stdin.getWriter();
  await writer.write(options.input).catch(() => {});
  await writer.close().catch(() => {});
  const output = await child.output();
  if (!output.success) {
    throw new Error(
      `${label} failed (${output.code}): ${
        new TextDecoder().decode(output.stderr)
      }`,
    );
  }
}

/** The unified diff hunks' line count and the first lines, for the report. */
function diffLines(path: string, before: string, after: string): string[] {
  const a = before.split("\n");
  const b = after.split("\n");
  const lines = [`--- released/${path}`, `+++ checkout/${path}`];
  const length = Math.max(a.length, b.length);
  for (let index = 0; index < length; index++) {
    if (a[index] === b[index]) continue;
    if (index < a.length) lines.push(`-${a[index]}`);
    if (index < b.length) lines.push(`+${b[index]}`);
  }
  return lines;
}

export async function main(argv: string[]): Promise<number> {
  let plugin = `${ROOT}/zig-out/bin/capnpc-zig`;
  let archive: string | undefined;
  for (let index = 0; index < argv.length; index += 2) {
    if (argv[index] === "--plugin" && argv[index + 1]) plugin = argv[index + 1];
    else if (argv[index] === "--archive" && argv[index + 1]) {
      archive = argv[index + 1];
    } else {
      console.error(
        "usage: wasm_generator_drift.ts [--plugin EXE] [--archive FILE]",
      );
      return 2;
    }
  }
  if (!plugin.startsWith("/") && !/^[A-Za-z]:[\\/]/.test(plugin)) {
    plugin = `${Deno.cwd()}/${plugin}`;
  }
  if (
    Deno.build.os === "windows" && !await exists(plugin) &&
    !plugin.toLowerCase().endsWith(".exe")
  ) plugin += ".exe";
  if (!(await Deno.stat(plugin).catch(() => undefined))?.isFile) {
    throw new Error(`native plugin not found: ${plugin} (run zig build first)`);
  }
  const pin = await generatorPin();
  const pkg = await install(pin, archive);
  const requests = [...REQUESTS];
  for await (const entry of Deno.readDir(`${ROOT}/${REQUEST_DIRECTORY}`)) {
    if (entry.name.endsWith(".request.bin")) {
      requests.push(`${REQUEST_DIRECTORY}/${entry.name}`);
    }
  }
  requests.splice(
    REQUESTS.length,
    Infinity,
    ...requests.slice(REQUESTS.length).sort(),
  );
  const report: [string, string, string[]][] = [];
  const temp = await Deno.makeTempDir({ prefix: "wasm generator drift " });
  try {
    for (const requestPath of requests) {
      const name = requestPath.slice(0, -".request.bin".length);
      const request = await Deno.readFile(`${ROOT}/${requestPath}`);
      const key = name.replaceAll("/", "__");
      const wasmOutput = `${temp}/wasm/${key}`;
      const nativeOutput = `${temp}/native/${key}`;
      await Deno.mkdir(wasmOutput, { recursive: true });
      await run([
        Deno.execPath(),
        "run",
        "--allow-all",
        "--no-config",
        `${pkg}/${LAUNCHER}`,
        "generator",
        "--module",
        `${pkg}/${MODULE}`,
        "--output",
        wasmOutput,
        "--",
      ], {
        input: request,
        env: { CAPNP_WASM_EXPECT_MANIFEST_SHA256: pin.manifest_sha256 },
      }, "capnpc-zig.wasm");
      await run([plugin, `--output-dir=${nativeOutput}`], {
        input: request,
      }, "native capnpc-zig");
      const released = await tree(wasmOutput);
      const current = await tree(nativeOutput);
      const paths = [...new Set([...released.keys(), ...current.keys()])]
        .sort();
      for (const path of paths) {
        const a = released.get(path);
        const b = current.get(path);
        if (a && b && await digest(a) === await digest(b)) continue;
        const decoder = new TextDecoder();
        report.push([
          name,
          path,
          diffLines(
            path,
            a ? decoder.decode(a) : "",
            b ? decoder.decode(b) : "",
          ),
        ]);
      }
    }
  } finally {
    await Deno.remove(temp, { recursive: true }).catch(() => {});
  }
  const label = `${pin.url.split("/").at(-2)} (capnp-zig ${
    pin.capnp_zig.slice(0, 12)
  })`;
  if (report.length === 0) {
    console.log(
      `capnpc-zig.wasm from ${label} matches this checkout's plugin on ${requests.length} requests`,
    );
    return 0;
  }
  const lines = [
    `capnpc-zig.wasm from ${label} differs from this checkout's plugin:`,
  ];
  for (const [name, path, diff] of report) {
    lines.push(`  ${name}: ${path} (${diff.length - 2} changed lines)`);
  }
  lines.push(
    "A capnpc-wasm release that carries this generator change is needed before Wasm users see it.",
  );
  console.log(lines.join("\n"));
  for (const [, , diff] of report.slice(0, 3)) {
    console.log(diff.slice(0, 40).join("\n"));
  }
  const summary = Deno.env.get("GITHUB_STEP_SUMMARY");
  if (summary) {
    await Deno.writeTextFile(
      summary,
      `### Released Wasm generator drift\n\n${lines.join("\n")}\n`,
      { append: true },
    );
  }
  return 1;
}

if (import.meta.main) {
  try {
    Deno.exit(await main(Deno.args));
  } catch (error) {
    console.error(
      `wasm_generator_drift: ${error instanceof Error ? error.message : error}`,
    );
    Deno.exit(1);
  }
}
