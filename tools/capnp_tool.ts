#!/usr/bin/env -S deno run --allow-all --no-config
// capnp_tool.ts: the pinned capnpc-wasm tools archive. Install it, verify it,
// and hand every compile to its launcher.
//
//   deno run --allow-all --no-config tools/capnp_tool.ts bootstrap [--archive FILE]
//   deno run --allow-all --no-config tools/capnp_tool.ts verify
//   deno run --allow-all --no-config tools/capnp_tool.ts compiler -- CAPNP_ARGS...
//   deno run --allow-all --no-config tools/capnp_tool.ts generate --plugin EXE \
//     --output DIR [--plugin-arg ARG]... -- SCHEMA_ARGS...
//
// Commands resolve caller paths from the current directory. This file owns
// only the consumer side of the toolchain: the pin in
// tools/capnp-toolchain.json, the download, the safe extraction, and the check
// of the installed package against the pinned manifest digest on every
// invocation. Compiling and generating are the packaged launcher's job
// (package/bin/capnp-wasm.ts, capnpc-wasm's launcher contract): its capnp mode
// translates caller paths, its generate mode runs a native plugin with
// transactional output, and it runs Wasmtime on Linux, macOS, and Windows. No
// native compiler is selected. Deno 2.4.5 or newer; no imports.

/** The checkout this tool belongs to: the parent of tools/. */
export const ROOT = import.meta.dirname!.replace(/[\\/][^\\/]+$/, "");
export const LAUNCHER = "bin/capnp-wasm.ts";
const WINDOWS = Deno.build.os === "windows";

export interface Pin {
  format?: number;
  url: string;
  sha256: string;
  manifest_sha256: string;
  source_commit: string;
  compiler_sha256: string;
  include_sha256: string;
}

export async function digest(data: Uint8Array): Promise<string> {
  const hash = new Uint8Array(
    await crypto.subtle.digest("SHA-256", new Uint8Array(data)),
  );
  return Array.from(hash, (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
}

/** A relative path that is safe and portable on every host, or an error. */
export function safePath(name: unknown): string {
  if (typeof name !== "string" || name === "" || name.includes("\\")) {
    throw new Error(`invalid relative path: ${JSON.stringify(name)}`);
  }
  const parts = name.split("/");
  if (name.startsWith("/") || parts.some((p) => ["", ".", ".."].includes(p))) {
    throw new Error(`invalid relative path: ${JSON.stringify(name)}`);
  }
  for (const part of parts) {
    if (
      // deno-lint-ignore no-control-regex
      /[<>:"|?*\x00-\x1f]/.test(part) || part.endsWith(".") ||
      part.endsWith(" ") ||
      /^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])$/i.test(part.split(".")[0])
    ) throw new Error(`non-portable relative path: ${JSON.stringify(name)}`);
  }
  return name;
}

/** Every file below root by relative name; links and other entries fail. */
export async function inventory(
  root: string,
): Promise<Map<string, Uint8Array>> {
  const result = new Map<string, Uint8Array>();
  const visit = async (directory: string, prefix: string) => {
    const names: string[] = [];
    for await (const entry of Deno.readDir(directory)) names.push(entry.name);
    for (const name of names.sort()) {
      const path = `${directory}/${name}`;
      const info = await Deno.lstat(path);
      if (info.isSymlink) throw new Error(`symlink in tool inputs: ${path}`);
      if (info.isDirectory) await visit(path, `${prefix}${name}/`);
      else if (info.isFile) {
        result.set(safePath(`${prefix}${name}`), await Deno.readFile(path));
      } else throw new Error(`unsupported tool input: ${path}`);
    }
  };
  await visit(root, "");
  return result;
}

export async function includesDigest(
  files: Map<string, Uint8Array>,
): Promise<string> {
  let listing = "";
  for (const name of [...files.keys()].sort()) {
    if (name.startsWith("include/")) {
      listing += `${await digest(files.get(name)!)}  ${name}\n`;
    }
  }
  return await digest(new TextEncoder().encode(listing));
}

interface Member {
  name: string;
  directory: boolean;
  data: Uint8Array;
}

/** The members of a gzip-compressed ustar archive; refuses what it cannot vouch for. */
async function readArchive(archive: Uint8Array): Promise<Member[]> {
  const tar = new Uint8Array(
    await new Response(
      new Blob([new Uint8Array(archive)]).stream().pipeThrough(
        new DecompressionStream("gzip"),
      ),
    ).arrayBuffer(),
  );
  const text = (bytes: Uint8Array) => {
    const end = bytes.indexOf(0);
    return new TextDecoder("utf-8", { fatal: true }).decode(
      end < 0 ? bytes : bytes.subarray(0, end),
    );
  };
  const octal = (bytes: Uint8Array) => {
    const value = text(bytes).trim();
    if (!/^[0-7]+$/.test(value)) throw new Error("malformed tar header");
    return parseInt(value, 8);
  };
  const members: Member[] = [];
  for (let offset = 0;;) {
    if (offset + 512 > tar.length) throw new Error("truncated tar archive");
    const header = tar.subarray(offset, offset + 512);
    if (header.every((byte) => byte === 0)) break;
    const checksum = header.reduce(
      (sum, byte, index) => sum + (index >= 148 && index < 156 ? 32 : byte),
      0,
    );
    if (checksum !== octal(header.subarray(148, 156))) {
      throw new Error("tar header checksum mismatch");
    }
    const prefix = text(header.subarray(345, 500));
    const base = text(header.subarray(0, 100));
    const name = (prefix ? `${prefix}/${base}` : base).replace(/\/+$/, "");
    const type = String.fromCharCode(header[156]);
    const size = octal(header.subarray(124, 136));
    const start = offset + 512;
    if (start + size > tar.length) throw new Error("truncated tar archive");
    if (!["0", "\0", "5"].includes(type)) {
      throw new Error(`unsafe or duplicate archive member: ${name}`);
    }
    members.push({
      name,
      directory: type === "5",
      data: tar.slice(start, start + size),
    });
    offset = start + Math.ceil(size / 512) * 512;
  }
  return members;
}

/** Extracts regular files and directories only, after checking every member. */
export async function extractArchive(archive: Uint8Array, destination: string) {
  const members = await readArchive(archive);
  const seen = new Map<string, Member>();
  for (const member of members) {
    safePath(member.name);
    const key = member.name.toLowerCase();
    if (seen.has(key)) {
      throw new Error(`unsafe or duplicate archive member: ${member.name}`);
    }
    seen.set(key, member);
  }
  for (const member of members) {
    const parts = member.name.split("/");
    for (let length = 1; length < parts.length; length++) {
      const parent = seen.get(parts.slice(0, length).join("/").toLowerCase());
      if (parent && !parent.directory) {
        throw new Error(
          `archive file used as a parent directory: ${member.name}`,
        );
      }
    }
  }
  for (const member of members) {
    const target = `${destination}/${member.name}`;
    if (member.directory) {
      await Deno.mkdir(target, { recursive: true });
    } else {
      await Deno.mkdir(target.slice(0, target.lastIndexOf("/")), {
        recursive: true,
      });
      await Deno.writeFile(target, member.data, { createNew: true });
    }
  }
}

/** Checks the package against the pin; returns it or throws. */
export async function verifyPackage(
  pkg: string,
  pin: Pin,
): Promise<string> {
  if ((await Deno.lstat(pkg)).isSymlink) {
    throw new Error("tool package must not be a symlink");
  }
  const files = await inventory(pkg);
  const rawManifest = files.get("manifest.json") ?? new Uint8Array();
  if (await digest(rawManifest) !== pin.manifest_sha256) {
    throw new Error("tool package manifest digest mismatch");
  }
  const manifest = JSON.parse(new TextDecoder().decode(rawManifest));
  if (
    manifest.source.commit !== pin.source_commit || manifest.source.dirty
  ) throw new Error("tool package source identity mismatch");
  const expected = new Set(["manifest.json"]);
  for (const entry of manifest.files) {
    const name = safePath(entry.path);
    if (expected.has(name)) {
      throw new Error(`duplicate manifest entry: ${name}`);
    }
    expected.add(name);
    const data = files.get(name);
    if (
      data === undefined || data.length !== entry.bytes ||
      await digest(data) !== entry.sha256
    ) throw new Error(`tool package integrity mismatch: ${name}`);
  }
  if (
    files.size !== expected.size ||
    [...files.keys()].some((name) => !expected.has(name))
  ) throw new Error("tool package inventory mismatch");
  if (await digest(files.get("wasm/capnp.wasm")!) !== pin.compiler_sha256) {
    throw new Error("compiler digest mismatch");
  }
  if (await includesDigest(files) !== pin.include_sha256) {
    throw new Error("standard includes digest mismatch");
  }
  if (!files.has(LAUNCHER)) {
    throw new Error(
      `tool package has no ${LAUNCHER}; pin capnp-wasm-tools 0.1.0-rc.3 or newer`,
    );
  }
  return pkg;
}

function absolute(path: string): string {
  if (path.startsWith("/") || /^[A-Za-z]:[\\/]/.test(path)) return path;
  return `${Deno.cwd()}/${path}`;
}

export function cacheRoot(root: string): string {
  return absolute(
    Deno.env.get("CAPNP_WASM_CACHE") ?? `${root}/.zig-cache/capnp-wasm`,
  );
}

export async function toolPin(root: string): Promise<Pin> {
  const pin = JSON.parse(
    await Deno.readTextFile(`${root}/tools/capnp-toolchain.json`),
  );
  if (pin.format !== 1) throw new Error("unsupported compiler lock format");
  const unfilled = Object.entries(pin).filter(([, value]) =>
    typeof value === "string" && value.includes("FILL-IN")
  ).map(([field]) => field);
  if (unfilled.length > 0) {
    throw new Error(
      `tools/capnp-toolchain.json is not filled in (${
        unfilled.join(", ")
      }); run tools/update_capnp_toolchain.ts after the release is published`,
    );
  }
  for (
    const field of [
      "sha256",
      "manifest_sha256",
      "compiler_sha256",
      "include_sha256",
    ]
  ) {
    if (!/^[0-9a-f]{64}$/.test(pin[field])) {
      throw new Error(`invalid compiler lock digest: ${field}`);
    }
  }
  if (!/^[0-9a-f]{40}$/.test(pin.source_commit)) {
    throw new Error("invalid compiler lock source commit");
  }
  return pin;
}

export function packagePath(root: string, pin: Pin): string {
  return `${cacheRoot(root)}/artifacts/${pin.sha256}/package`;
}

async function exists(path: string): Promise<boolean> {
  try {
    await Deno.lstat(path);
    return true;
  } catch {
    return false;
  }
}

export async function installedPackage(root: string): Promise<string> {
  const pin = await toolPin(root);
  const pkg = packagePath(root, pin);
  if (!await exists(pkg)) {
    throw new Error(
      "WASM compiler is not installed; run this script's bootstrap command " +
        "(mise run bootstrap:capnp in a capnp-zig checkout)",
    );
  }
  return await verifyPackage(pkg, pin);
}

/**
 * Runs the verified package's launcher with this Deno and returns its exit
 * status. The package was checked against the pinned manifest digest just
 * before, and the launcher checks it again against the same digest, so a
 * package changed after bootstrap never runs.
 */
export async function launch(
  pkg: string,
  pin: Pin,
  args: string[],
  options: { stdoutToStderr?: boolean } = {},
): Promise<number> {
  const child = new Deno.Command(Deno.execPath(), {
    args: ["run", "--allow-all", "--no-config", `${pkg}/${LAUNCHER}`, ...args],
    env: { CAPNP_WASM_EXPECT_MANIFEST_SHA256: pin.manifest_sha256 },
    stdin: "inherit",
    stdout: options.stdoutToStderr ? "piped" : "inherit",
    stderr: "inherit",
  }).spawn();
  // The launcher stops its guest and cleans up on these; wait for it.
  const signals: Deno.Signal[] = WINDOWS
    ? ["SIGINT", "SIGBREAK"]
    : ["SIGINT", "SIGTERM", "SIGHUP"];
  const listeners: [Deno.Signal, () => void][] = [];
  for (const signal of signals) {
    const listener = () => {
      try {
        child.kill(signal);
      } catch {
        // The launcher has already exited.
      }
    };
    try {
      Deno.addSignalListener(signal, listener);
      listeners.push([signal, listener]);
    } catch {
      // This platform cannot deliver the signal.
    }
  }
  try {
    if (options.stdoutToStderr) {
      for await (const chunk of child.stdout) {
        for (let offset = 0; offset < chunk.length;) {
          offset += Deno.stderr.writeSync(chunk.subarray(offset));
        }
      }
    }
    return (await child.status).code;
  } finally {
    for (const [signal, listener] of listeners) {
      Deno.removeSignalListener(signal, listener);
    }
  }
}

export async function compiler(
  pkg: string,
  pin: Pin,
  args: string[],
  options: { stdoutToStderr?: boolean } = {},
): Promise<number> {
  if (args.length === 0) {
    throw new Error("missing compiler arguments after --");
  }
  return await launch(pkg, pin, ["capnp", "--", ...args], options);
}

export async function bootstrap(
  root: string,
  archiveOverride?: string,
): Promise<number> {
  const pin = await toolPin(root);
  const destination = packagePath(root, pin);
  const artifacts = destination.slice(
    0,
    destination.lastIndexOf("/", destination.lastIndexOf("/") - 1),
  );
  await Deno.mkdir(artifacts, { recursive: true });
  if (await exists(destination)) {
    await verifyPackage(destination, pin);
  } else {
    const temp = await Deno.makeTempDir({ dir: artifacts, prefix: "install-" });
    try {
      const archive = archiveOverride
        ? await Deno.readFile(archiveOverride)
        : new Uint8Array(
          await (await fetch(pin.url, { signal: AbortSignal.timeout(60_000) }))
            .arrayBuffer(),
        );
      if (await digest(archive) !== pin.sha256) {
        throw new Error("tool archive digest mismatch");
      }
      const unpacked = `${temp}/unpacked`;
      await Deno.mkdir(unpacked);
      await extractArchive(archive, unpacked);
      await verifyPackage(`${unpacked}/package`, pin);
      const roots = [];
      for await (const entry of Deno.readDir(unpacked)) roots.push(entry.name);
      if (roots.length !== 1 || roots[0] !== "package") {
        throw new Error("unexpected tool archive root");
      }
      await Deno.mkdir(destination.slice(0, destination.lastIndexOf("/")), {
        recursive: true,
      });
      await Deno.rename(`${unpacked}/package`, destination);
    } finally {
      await Deno.remove(temp, { recursive: true }).catch(() => {});
    }
  }
  const result = await compiler(destination, pin, ["--version"], {
    stdoutToStderr: true,
  });
  if (result !== 0) return result;
  console.error(`capnp-wasm: verified ${pin.source_commit} (${pin.sha256})`);
  return 0;
}

export async function generate(
  pkg: string,
  pin: Pin,
  plugin: string,
  output: string,
  pluginArgs: string[],
  compilerArgs: string[],
): Promise<number> {
  if (compilerArgs.length === 0) {
    throw new Error("missing schema compiler arguments after --");
  }
  const end = compilerArgs.includes("--")
    ? compilerArgs.indexOf("--")
    : compilerArgs.length;
  const options = compilerArgs.slice(0, end);
  if (
    options[0] === "compile" ||
    options.some((arg) =>
      arg === "--output" || arg.startsWith("-o") || arg.startsWith("--output=")
    )
  ) throw new Error("generate arguments must omit compile and -o/--output");
  let executable = absolute(plugin);
  if (
    WINDOWS && !await exists(executable) &&
    !executable.toLowerCase().endsWith(".exe")
  ) executable += ".exe";
  const info = await Deno.stat(executable).catch(() => undefined);
  if (!info?.isFile) {
    throw new Error(`native generator does not exist: ${executable}`);
  }
  return await launch(pkg, pin, [
    "generate",
    "--plugin",
    executable,
    "--output",
    absolute(output),
    ...pluginArgs.map((value) => `--plugin-arg=${value}`),
    "--",
    ...compilerArgs,
  ]);
}

const USAGE = `usage: capnp_tool.ts bootstrap [--archive FILE]
       capnp_tool.ts verify
       capnp_tool.ts compiler -- CAPNP_ARGS...
       capnp_tool.ts generate --plugin EXE --output DIR [--plugin-arg ARG]... -- SCHEMA_ARGS...
(use --plugin-arg=--flag for an argument beginning with -)`;

class Usage extends Error {}

export async function main(argv: string[]): Promise<number> {
  const [command, ...args] = argv;
  if (command === "bootstrap") {
    if (args.length === 0) return await bootstrap(ROOT);
    if (args.length === 2 && args[0] === "--archive") {
      return await bootstrap(ROOT, args[1]);
    }
    throw new Usage(USAGE);
  }
  if (!["verify", "compiler", "generate"].includes(command)) {
    throw new Usage(USAGE);
  }
  const pin = await toolPin(ROOT);
  const pkg = await installedPackage(ROOT);
  if (command === "verify") {
    if (args.length > 0) throw new Usage(USAGE);
    const status = await launch(pkg, pin, [
      "verify",
      "--expect-manifest-sha256",
      pin.manifest_sha256,
    ]);
    return status || await compiler(pkg, pin, ["--version"]);
  }
  if (command === "compiler") {
    if (args[0] !== "--") {
      throw new Usage(`compiler requires -- before its arguments\n${USAGE}`);
    }
    return await compiler(pkg, pin, args.slice(1));
  }
  let plugin: string | undefined;
  let output: string | undefined;
  const pluginArgs: string[] = [];
  let index = 0;
  for (; index < args.length && args[index] !== "--"; index++) {
    const arg = args[index];
    if (arg.startsWith("--plugin-arg=")) {
      pluginArgs.push(arg.slice("--plugin-arg=".length));
    } else if (
      ["--plugin", "--output", "--plugin-arg"].includes(arg) &&
      index + 1 < args.length
    ) {
      const value = args[++index];
      if (arg === "--plugin") plugin = value;
      else if (arg === "--output") output = value;
      else pluginArgs.push(value);
    } else if (arg.startsWith("--plugin=")) {
      plugin = arg.slice("--plugin=".length);
    } else if (arg.startsWith("--output=")) {
      output = arg.slice("--output=".length);
    } else throw new Usage(USAGE);
  }
  if (plugin === undefined || output === undefined) throw new Usage(USAGE);
  if (args[index] !== "--") {
    throw new Usage(`generate requires -- before its arguments\n${USAGE}`);
  }
  return await generate(
    pkg,
    pin,
    plugin,
    output,
    pluginArgs,
    args.slice(index + 1),
  );
}

export async function cli(argv: string[]): Promise<number> {
  try {
    return await main(argv);
  } catch (error) {
    if (error instanceof Usage) {
      console.error(error.message);
      return 2;
    }
    console.error(
      `capnp-wasm: ${error instanceof Error ? error.message : error}`,
    );
    return 1;
  }
}

if (import.meta.main) Deno.exit(await cli(Deno.args));
