#!/usr/bin/env -S deno run --allow-all --no-config
// Fill tools/capnp-toolchain.json, the pin of the capnpc-wasm tools archive
// that tools/capnp_tool.ts installs, from a published release's own assets.
//
//   deno run --allow-all --no-config tools/update_capnp_toolchain.ts tools capnp-wasm-tools-v0.1.0-rc.3
//
// The script downloads SHA256SUMS, the manifest asset, and the archive of the
// release, checks the manifest and the archive against SHA256SUMS, and derives
// every other field from the manifest. With --assets DIR it reads those three
// files from DIR instead (for a draft release, download them with
// `gh release download <tag> --dir DIR`).
//
// Compare the archive and manifest digests it prints with the release's row in
// capnpc-wasm's docs/releases.md (published releases) before committing: that
// row is the channel independent of the release page.

import { digest, ROOT } from "./capnp_tool.ts";

const REPOSITORY = "https://github.com/nullstyle/capnpc-wasm";
const TAG_PREFIX = "capnp-wasm-tools-v";
const STEM_PREFIX = "capnp-wasm-tools-";
const PIN_PATH = `${ROOT}/tools/capnp-toolchain.json`;
const USAGE = "usage: update_capnp_toolchain.ts tools TAG [--assets DIR]";

export function parseSums(text: string): Map<string, string> {
  const sums = new Map<string, string>();
  for (const line of text.split("\n")) {
    if (line.trim() === "") continue;
    const match = /^([0-9a-f]{64}) [ *](\S.*)$/.exec(line);
    if (!match) throw new Error(`malformed SHA256SUMS line: ${line}`);
    sums.set(match[2], match[1]);
  }
  return sums;
}

async function readAsset(
  name: string,
  baseUrl: string,
  assets?: string,
): Promise<Uint8Array> {
  if (assets !== undefined) return await Deno.readFile(`${assets}/${name}`);
  const response = await fetch(baseUrl + name, {
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`${baseUrl + name}: ${response.status}`);
  return new Uint8Array(await response.arrayBuffer());
}

export async function main(argv: string[]): Promise<number> {
  const [kind, tag, ...rest] = argv;
  const assets = rest[0] === "--assets" && rest.length === 2
    ? rest[1]
    : undefined;
  if (kind !== "tools" || !tag || (rest.length > 0 && assets === undefined)) {
    console.error(USAGE);
    return 2;
  }
  if (!tag.startsWith(TAG_PREFIX)) {
    console.error(`the tools pin needs a ${TAG_PREFIX}<version> tag`);
    return 2;
  }
  const version = tag.slice(TAG_PREFIX.length);
  const stem = STEM_PREFIX + version;
  const baseUrl = `${REPOSITORY}/releases/download/${tag}/`;

  const sums = parseSums(
    new TextDecoder().decode(await readAsset("SHA256SUMS", baseUrl, assets)),
  );
  for (const name of [`${stem}.tgz`, `${stem}.manifest.json`]) {
    if (!sums.has(name)) throw new Error(`SHA256SUMS does not list ${name}`);
  }
  const rawManifest = await readAsset(`${stem}.manifest.json`, baseUrl, assets);
  if (await digest(rawManifest) !== sums.get(`${stem}.manifest.json`)) {
    throw new Error("the manifest asset does not match SHA256SUMS");
  }
  const archive = await readAsset(`${stem}.tgz`, baseUrl, assets);
  if (await digest(archive) !== sums.get(`${stem}.tgz`)) {
    throw new Error("the archive does not match SHA256SUMS");
  }
  const manifest = JSON.parse(new TextDecoder().decode(rawManifest));
  if (manifest.version !== version || manifest.source.dirty) {
    throw new Error(
      `the manifest names version ${manifest.version} (dirty: ${manifest.source.dirty}), not a clean ${version}`,
    );
  }
  const files = new Map<string, string>(
    manifest.files.map((entry: { path: string; sha256: string }) => [
      entry.path,
      entry.sha256,
    ]),
  );
  if (!files.has("bin/capnp-wasm.ts")) {
    throw new Error(
      "the archive has no bin/capnp-wasm.ts; pin a release from 0.1.0-rc.3 (tools) on",
    );
  }
  const pin: Record<string, unknown> = {
    format: 1,
    url: `${baseUrl}${stem}.tgz`,
    sha256: sums.get(`${stem}.tgz`),
    manifest_sha256: sums.get(`${stem}.manifest.json`),
    source_commit: manifest.source.commit,
  };
  pin.compiler_sha256 = files.get("wasm/capnp.wasm");
  pin.include_sha256 = await digest(
    new TextEncoder().encode(
      [...files.keys()].filter((name) => name.startsWith("include/")).sort()
        .map((name) => `${files.get(name)}  ${name}\n`).join(""),
    ),
  );
  await Deno.writeTextFile(PIN_PATH, JSON.stringify(pin, null, 2) + "\n");
  console.log(`wrote ${PIN_PATH.slice(ROOT.length + 1)} for ${tag}`);
  console.log(`  archive ${pin.sha256}  ${stem}.tgz`);
  console.log(`  manifest ${pin.manifest_sha256}  ${stem}.manifest.json`);
  console.log(`  producer commit ${pin.source_commit}`);
  console.log(
    "Compare these with the published releases row in capnpc-wasm's docs/releases.md.",
  );
  return 0;
}

if (import.meta.main) {
  try {
    Deno.exit(await main(Deno.args));
  } catch (error) {
    console.error(
      `update_capnp_toolchain: ${
        error instanceof Error ? error.message : error
      }`,
    );
    Deno.exit(1);
  }
}
