#!/usr/bin/env -S deno run --allow-all
// tools/e2e_runner.ts — Cap'n Proto RPC interoperability e2e test runner.
// Deno rewrite of tools/e2e_runner.zig.

// ---------- constants & types ----------

const BACKENDS = ["cpp", "go", "python", "rust"] as const;
type Backend = (typeof BACKENDS)[number];

const SCHEMAS = ["game_world", "chat", "inventory", "matchmaking"] as const;
type Schema = (typeof SCHEMAS)[number];

type Direction = "both" | "zig-client" | "zig-server";

interface CaseResult {
  key: string;
  status: string;
}

interface TapEval {
  passCount: number;
  failCount: number;
  bailed: boolean;
}

interface Paths {
  repoRoot: string;
  composeFile: string;
  resultsDir: string;
  zigJustfile: string;
  goJustfile: string;
  cppJustfile: string;
  pythonJustfile: string;
  rustJustfile: string;
}

const BACKEND_PORTS: Record<Backend, number> = {
  cpp: 4000,
  go: 4001,
  python: 4002,
  rust: 4003,
};

const SERVER_TIMEOUT_MS = 60_000;
const CASE_TIMEOUT_MS = 20_000;
const DEFAULT_E2E_ZIG_GLOBAL_CACHE_DIR = "./.zig-global-cache";

// ---------- CLI parsing ----------

interface Config {
  buildOnly: boolean;
  skipBuild: boolean;
  allowMissingHooks: boolean;
  verbose: boolean;
  direction: Direction;
  backends: Backend[];
  schemas: Schema[];
}

function printUsage(): void {
  console.error(
    `Usage: deno run --allow-all tools/e2e_runner.ts [OPTIONS]

Options:
  --build-only
  --skip-build
  --direction=both|zig-client|zig-server
  --backend=cpp|go|python|rust (repeatable)
  --schema=game_world|chat|inventory|matchmaking (repeatable)
  --allow-missing-hooks
  --verbose
  --help

Optional hook env vars:
  E2E_ZIG_GLOBAL_CACHE_DIR
  (if set, used to seed ZIG_GLOBAL_CACHE_DIR for zig build/run)

Legacy override hook env vars:
  E2E_ZIG_CLIENT_CMD
  E2E_ZIG_SERVER_CMD
By default the Zig client hook runs via:
  just --justfile tests/e2e/zig/Justfile client-hook ...
By default the Zig server runs via:
  just --justfile tests/e2e/zig/Justfile server-hook ...`,
  );
}

function parseArgs(args: string[]): Config | null {
  const cfg: Config = {
    buildOnly: false,
    skipBuild: false,
    allowMissingHooks: false,
    verbose: false,
    direction: "both",
    backends: [],
    schemas: [],
  };

  for (const arg of args) {
    if (arg === "--help" || arg === "-h") {
      printUsage();
      return null;
    }
    if (arg === "--build-only") {
      cfg.buildOnly = true;
    } else if (arg === "--skip-build") {
      cfg.skipBuild = true;
    } else if (arg === "--allow-missing-hooks") {
      cfg.allowMissingHooks = true;
    } else if (arg === "--verbose") {
      cfg.verbose = true;
    } else if (arg.startsWith("--direction=")) {
      const val = arg.slice("--direction=".length);
      if (!["both", "zig-client", "zig-server"].includes(val)) {
        console.error(`Invalid direction: ${val}`);
        Deno.exit(1);
      }
      cfg.direction = val as Direction;
    } else if (arg.startsWith("--backend=")) {
      const val = arg.slice("--backend=".length);
      if (!(BACKENDS as readonly string[]).includes(val)) {
        console.error(`Invalid backend: ${val}`);
        Deno.exit(1);
      }
      cfg.backends.push(val as Backend);
    } else if (arg.startsWith("--schema=")) {
      const val = arg.slice("--schema=".length);
      if (!(SCHEMAS as readonly string[]).includes(val)) {
        console.error(`Invalid schema: ${val}`);
        Deno.exit(1);
      }
      cfg.schemas.push(val as Schema);
    } else {
      console.error(`Unknown option: ${arg}`);
      Deno.exit(1);
    }
  }

  // Default to all backends/schemas when none specified.
  if (cfg.backends.length === 0) cfg.backends = [...BACKENDS];
  if (cfg.schemas.length === 0) cfg.schemas = [...SCHEMAS];

  return cfg;
}

// ---------- helpers ----------

function backendJustfile(paths: Paths, backend: Backend): string {
  return {
    cpp: paths.cppJustfile,
    go: paths.goJustfile,
    python: paths.pythonJustfile,
    rust: paths.rustJustfile,
  }[backend];
}

function schemaNameForBackend(backend: Backend, schema: Schema): string {
  if (backend === "go" && schema === "game_world") return "gameworld";
  return schema;
}

async function fileExists(path: string): Promise<boolean> {
  try {
    await Deno.stat(path);
    return true;
  } catch {
    return false;
  }
}

async function detectPaths(): Promise<Paths> {
  if (await fileExists("tests/e2e/docker-compose.yml")) {
    return {
      repoRoot: ".",
      composeFile: "tests/e2e/docker-compose.yml",
      resultsDir: "tests/e2e/.results",
      zigJustfile: "tests/e2e/zig/Justfile",
      goJustfile: "tests/e2e/go/Justfile",
      cppJustfile: "tests/e2e/cpp/Justfile",
      pythonJustfile: "tests/e2e/python/Justfile",
      rustJustfile: "tests/e2e/rust/Justfile",
    };
  }

  if (
    (await fileExists("docker-compose.yml")) &&
    (await fileExists("schemas/game_world.capnp"))
  ) {
    return {
      repoRoot: "../..",
      composeFile: "docker-compose.yml",
      resultsDir: ".results",
      zigJustfile: "zig/Justfile",
      goJustfile: "go/Justfile",
      cppJustfile: "cpp/Justfile",
      pythonJustfile: "python/Justfile",
      rustJustfile: "rust/Justfile",
    };
  }

  throw new Error(
    "Cannot detect e2e paths -- run from repo root or tests/e2e/",
  );
}

// ---------- subprocess helpers ----------

interface RunResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

async function run(
  cmd: string[],
  opts: {
    cwd?: string;
    env?: Record<string, string>;
    timeoutMs?: number;
  } = {},
): Promise<RunResult> {
  const command = new Deno.Command(cmd[0], {
    args: cmd.slice(1),
    cwd: opts.cwd,
    env: opts.env,
    stdin: "null",
    stdout: "piped",
    stderr: "piped",
    signal: opts.timeoutMs ? AbortSignal.timeout(opts.timeoutMs) : undefined,
  });

  try {
    const result = await command.output();
    return {
      exitCode: result.code,
      stdout: new TextDecoder().decode(result.stdout),
      stderr: new TextDecoder().decode(result.stderr),
    };
  } catch (e) {
    if (e instanceof DOMException && e.name === "TimeoutError") {
      return { exitCode: 124, stdout: "", stderr: "timeout\n" };
    }
    throw e;
  }
}

async function runShell(
  command: string,
  opts: { env?: Record<string, string>; timeoutMs?: number } = {},
): Promise<RunResult> {
  return run(["sh", "-lc", command], opts);
}

// ---------- TAP parsing ----------

function evalTap(output: string): TapEval {
  let passCount = 0;
  let failCount = 0;
  let bailed = false;

  for (const line of output.split("\n")) {
    if (line.startsWith("Bail out!")) bailed = true;
    else if (line.startsWith("ok ")) passCount++;
    else if (line.startsWith("not ok ")) failCount++;
  }

  return { passCount, failCount, bailed };
}

function statusFromRun(exitCode: number, tap: TapEval): string {
  if (
    exitCode === 0 && tap.failCount === 0 && tap.passCount > 0 && !tap.bailed
  ) {
    return `PASS(${tap.passCount})`;
  }
  if (tap.bailed) {
    return exitCode === 0
      ? "FAIL(BAIL)"
      : `FAIL(exit=${exitCode},BAIL)`;
  }
  if (tap.passCount + tap.failCount === 0) {
    return exitCode === 0
      ? "FAIL(NO_TESTS)"
      : `FAIL(exit=${exitCode},NO_TESTS)`;
  }
  const total = tap.passCount + tap.failCount;
  if (exitCode === 0) {
    return `FAIL(${tap.failCount}/${total})`;
  }
  return `FAIL(exit=${exitCode},${tap.failCount}/${total})`;
}

// ---------- port & docker helpers ----------

async function waitForPort(
  port: number,
  timeoutMs: number,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const conn = await Deno.connect({ hostname: "127.0.0.1", port });
      conn.close();
      return true;
    } catch {
      await new Promise((r) => setTimeout(r, 100));
    }
  }
  return false;
}

async function findFreePort(): Promise<number> {
  const listener = Deno.listen({ hostname: "127.0.0.1", port: 0 });
  const { port } = listener.addr as Deno.NetAddr;
  listener.close();
  return port;
}

async function dockerRmForce(container: string): Promise<void> {
  await run(["docker", "rm", "-f", container]);
}

async function stopRefServers(): Promise<void> {
  for (const backend of BACKENDS) {
    for (const schema of SCHEMAS) {
      await dockerRmForce(`e2e-ref-server-${backend}-${schema}`);
    }
  }
}

async function composeDown(paths: Paths): Promise<void> {
  await run([
    "docker",
    "compose",
    "-f",
    paths.composeFile,
    "down",
    "--remove-orphans",
    "--timeout",
    "5",
  ]);
}

async function writeCombinedOutput(
  path: string,
  stdout: string,
  stderr: string,
): Promise<void> {
  let content = stdout;
  if (stderr) {
    if (content && !content.endsWith("\n")) content += "\n";
    content += stderr;
  }
  await Deno.writeTextFile(path, content);
}

function printCommandFailure(label: string, res: RunResult): void {
  console.error(`${label} failed (exit=${res.exitCode})`);
  if (res.stdout) console.error("stdout:\n" + res.stdout);
  if (res.stderr) console.error("stderr:\n" + res.stderr);
}

// ---------- build ----------

async function buildImages(
  paths: Paths,
  backends: Backend[],
): Promise<void> {
  for (const backend of backends) {
    const justfile = backendJustfile(paths, backend);
    const res = await run(["just", "--justfile", justfile, "docker-build"]);
    if (res.exitCode !== 0) {
      printCommandFailure(`just docker-build (${backend})`, res);
      throw new Error(`docker-build failed for ${backend}`);
    }
  }
}

// ---------- zig-client phase ----------

async function startRefServer(
  paths: Paths,
  backend: Backend,
  schema: Schema,
): Promise<void> {
  const container = `e2e-ref-server-${backend}-${schema}`;
  await dockerRmForce(container);

  const port = BACKEND_PORTS[backend];
  const justfile = backendJustfile(paths, backend);
  const res = await run([
    "just",
    "--justfile",
    justfile,
    "docker-server",
    container,
    "0.0.0.0",
    String(port),
    schemaNameForBackend(backend, schema),
  ]);

  if (res.exitCode !== 0) {
    printCommandFailure(`just docker-server (${backend})`, res);
    throw new Error("ServerStartFailed");
  }

  const ready = await waitForPort(port, SERVER_TIMEOUT_MS);
  if (!ready) {
    await dockerRmForce(container);
    throw new Error("ServerStartTimeout");
  }
}

async function stopRefServer(
  backend: Backend,
  schema: Schema,
): Promise<void> {
  await dockerRmForce(`e2e-ref-server-${backend}-${schema}`);
}

async function runZigClientCase(
  paths: Paths,
  zigClientCmd: string | undefined,
  backend: Backend,
  schema: Schema,
  outputPath: string,
): Promise<RunResult> {
  const port = BACKEND_PORTS[backend];
  let res: RunResult;

  if (zigClientCmd) {
    res = await runShell(zigClientCmd, {
      env: {
        ...Deno.env.toObject(),
        E2E_TARGET_HOST: "127.0.0.1",
        E2E_TARGET_PORT: String(port),
        E2E_SCHEMA: schema,
        E2E_BACKEND: backend,
      },
      timeoutMs: CASE_TIMEOUT_MS,
    });
  } else {
    res = await run(
      [
        "just",
        "--justfile",
        paths.zigJustfile,
        "client-hook",
        "127.0.0.1",
        String(port),
        schema,
        backend,
      ],
      { timeoutMs: CASE_TIMEOUT_MS },
    );
  }

  await writeCombinedOutput(outputPath, res.stdout, res.stderr);
  return res;
}

async function runZigClientPhase(
  paths: Paths,
  cfg: Config,
  zigClientCmd: string | undefined,
  results: CaseResult[],
): Promise<void> {
  console.error("==> Phase: Zig client -> reference server");

  for (const schema of cfg.schemas) {
    for (const backend of cfg.backends) {
      const key = `zig-client:${schema}:${backend}`;
      console.error(`    case ${key}`);

      try {
        await startRefServer(paths, backend, schema);
      } catch (e) {
        results.push({
          key,
          status: `FAIL(server-start:${(e as Error).message})`,
        });
        continue;
      }

      try {
        const outputPath =
          `${paths.resultsDir}/zig_client_${schema}_${backend}.tap`;
        const res = await runZigClientCase(
          paths,
          zigClientCmd,
          backend,
          schema,
          outputPath,
        );
        const combined = res.stdout + "\n" + res.stderr;
        const tap = evalTap(combined);
        const status = statusFromRun(res.exitCode, tap);
        results.push({ key, status });

        if (cfg.verbose && !status.startsWith("PASS")) {
          console.error(`      output:\n${combined}`);
        }
      } finally {
        await stopRefServer(backend, schema);
      }
    }
  }
}

// ---------- zig-server phase ----------

function zigServerEnv(): Record<string, string> {
  const cacheDir = Deno.env.get("E2E_ZIG_GLOBAL_CACHE_DIR") ??
    Deno.env.get("ZIG_GLOBAL_CACHE_DIR") ??
    DEFAULT_E2E_ZIG_GLOBAL_CACHE_DIR;
  return {
    ...Deno.env.toObject(),
    ZIG_GLOBAL_CACHE_DIR: cacheDir,
  };
}

async function prebuildZigServer(paths: Paths): Promise<string> {
  const env = zigServerEnv();
  console.error("    pre-building e2e-zig-server...");
  const res = await run(
    ["zig", "build", "e2e-zig-server-install"],
    { cwd: paths.repoRoot, env },
  );
  if (res.exitCode !== 0) {
    printCommandFailure("zig build e2e-zig-server-install", res);
    throw new Error("e2e-zig-server build failed");
  }
  const binPath = `${paths.repoRoot}/zig-out/bin/e2e-zig-server`;
  return binPath;
}

async function startZigServer(
  paths: Paths,
  zigServerCmd: string | undefined,
  serverBinPath: string | undefined,
  schema: Schema,
  port: number,
  verbose: boolean,
): Promise<Deno.ChildProcess> {
  const env = zigServerEnv();

  let proc: Deno.ChildProcess;

  if (zigServerCmd) {
    env.E2E_BIND_HOST = "0.0.0.0";
    env.E2E_BIND_PORT = String(port);
    env.E2E_SCHEMA = schema;
    proc = new Deno.Command("sh", {
      args: ["-lc", zigServerCmd],
      stdin: "null",
      stdout: verbose ? "inherit" : "null",
      stderr: "piped",
      env,
    }).spawn();
  } else if (serverBinPath) {
    proc = new Deno.Command(serverBinPath, {
      args: ["--host", "0.0.0.0", "--port", String(port), "--schema", schema],
      stdin: "null",
      stdout: verbose ? "inherit" : "null",
      stderr: "piped",
      env,
    }).spawn();
  } else {
    proc = new Deno.Command("just", {
      args: [
        "--justfile",
        paths.zigJustfile,
        "server-hook",
        "0.0.0.0",
        String(port),
        schema,
      ],
      stdin: "null",
      stdout: verbose ? "inherit" : "null",
      stderr: "piped",
      env,
    }).spawn();
  }

  // Wait for server readiness: race port polling against early process exit.
  // The server prints "READY\n" to stderr when listening, but we also detect
  // crashes so we don't sit for 60s on a dead process.
  let processExited = false;
  let exitCode = -1;
  const stderrChunks: Uint8Array[] = [];
  const stderrDone = (async () => {
    for await (const chunk of proc.stderr) {
      stderrChunks.push(chunk);
      if (verbose) {
        await Deno.stderr.write(chunk);
      }
    }
  })();
  proc.status.then((s) => {
    processExited = true;
    exitCode = s.code;
  });

  const deadline = Date.now() + SERVER_TIMEOUT_MS;
  let ready = false;
  while (Date.now() < deadline) {
    if (processExited) break;
    try {
      const conn = await Deno.connect({ hostname: "127.0.0.1", port });
      conn.close();
      ready = true;
      break;
    } catch {
      await new Promise((r) => setTimeout(r, 100));
    }
  }

  if (!ready) {
    try {
      proc.kill();
    } catch { /* already dead */ }
    await stderrDone;
    const stderrText = new TextDecoder().decode(
      concatBytes(stderrChunks),
    );
    if (processExited) {
      console.error(
        `    server exited early (code=${exitCode}). stderr:\n${stderrText}`,
      );
    } else {
      console.error(
        `    server did not become ready within ${SERVER_TIMEOUT_MS}ms`,
      );
      if (stderrText) console.error(`    stderr:\n${stderrText}`);
    }
    throw new Error("ServerStartTimeout");
  }

  return proc;
}

function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((n, c) => n + c.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.length;
  }
  return out;
}

async function runRefClientCase(
  paths: Paths,
  backend: Backend,
  schema: Schema,
  targetPort: number,
  outputPath: string,
): Promise<RunResult> {
  const justfile = backendJustfile(paths, backend);
  const res = await run(
    [
      "just",
      "--justfile",
      justfile,
      "docker-client",
      "host.docker.internal",
      String(targetPort),
      schemaNameForBackend(backend, schema),
    ],
    { timeoutMs: CASE_TIMEOUT_MS },
  );
  await writeCombinedOutput(outputPath, res.stdout, res.stderr);
  return res;
}

async function runZigServerPhase(
  paths: Paths,
  cfg: Config,
  zigServerCmd: string | undefined,
  results: CaseResult[],
): Promise<void> {
  console.error("==> Phase: reference client -> Zig server");

  // Pre-build the server binary once (skip if using override command).
  let serverBinPath: string | undefined;
  if (!zigServerCmd) {
    try {
      serverBinPath = await prebuildZigServer(paths);
    } catch {
      // Fall back to just server-hook per case.
    }
  }

  for (const schema of cfg.schemas) {
    for (const backend of cfg.backends) {
      const key = `zig-server:${schema}:${backend}`;

      let port: number;
      try {
        port = await findFreePort();
      } catch (e) {
        results.push({
          key,
          status: `FAIL(port-reserve:${(e as Error).message})`,
        });
        continue;
      }

      console.error(
        `    starting Zig server for schema=${schema} on port=${port}`,
      );

      let proc: Deno.ChildProcess;
      try {
        proc = await startZigServer(
          paths,
          zigServerCmd,
          serverBinPath,
          schema,
          port,
          cfg.verbose,
        );
      } catch (e) {
        results.push({
          key,
          status: `FAIL(server-start:${(e as Error).message})`,
        });
        continue;
      }

      try {
        console.error(`    case ${key}`);
        const outputPath =
          `${paths.resultsDir}/zig_server_${schema}_${backend}.tap`;
        const res = await runRefClientCase(
          paths,
          backend,
          schema,
          port,
          outputPath,
        );
        const combined = res.stdout + "\n" + res.stderr;
        const tap = evalTap(combined);
        const status = statusFromRun(res.exitCode, tap);
        results.push({ key, status });

        if (cfg.verbose && !status.startsWith("PASS")) {
          console.error(`      output:\n${combined}`);
        }
      } finally {
        try {
          proc.kill();
        } catch { /* already dead */ }
        // Drain piped stderr to avoid resource leaks.
        try {
          for await (const _ of proc.stderr) { /* discard */ }
        } catch { /* ignore */ }
        await proc.status;
      }
    }
  }
}

// ---------- summary ----------

function writeSummary(paths: Paths, results: CaseResult[]): void {
  let passed = 0, failed = 0, skipped = 0;
  for (const r of results) {
    if (r.status.startsWith("PASS")) passed++;
    else if (r.status.startsWith("SKIP")) skipped++;
    else failed++;
  }

  console.error();
  console.error(
    "======================================================================",
  );
  console.error("  Zig Interop E2E Summary");
  console.error(
    "======================================================================",
  );
  console.error();
  console.error(`  Total cases: ${results.length}`);
  console.error(`  Passed:      ${passed}`);
  console.error(`  Failed:      ${failed}`);
  console.error(`  Skipped:     ${skipped}`);
  console.error();
  console.error(`${"case".padEnd(36)} status`);
  console.error(`${"----".padEnd(36)} ------`);
  for (const r of results) {
    console.error(`${r.key.padEnd(36)} ${r.status}`);
  }

  const summaryPath = `${paths.resultsDir}/summary.json`;
  const summaryObj = {
    total: results.length,
    passed,
    failed,
    skipped,
    results: Object.fromEntries(results.map((r) => [r.key, r.status])),
  };
  Deno.writeTextFileSync(
    summaryPath,
    JSON.stringify(summaryObj, null, 2) + "\n",
  );

  if (failed > 0) {
    Deno.exit(1);
  }
}

// ---------- main ----------

async function main(): Promise<void> {
  const cfg = parseArgs(Deno.args);
  if (!cfg) return; // --help was requested

  const paths = await detectPaths();
  await Deno.mkdir(paths.resultsDir, { recursive: true });

  // Best-effort cleanup of old servers from previous runs.
  await stopRefServers();

  try {
    if (!cfg.skipBuild) {
      await buildImages(paths, cfg.backends);
    }

    if (cfg.buildOnly) return;

    const zigClientCmd = Deno.env.get("E2E_ZIG_CLIENT_CMD");
    const zigServerCmd = Deno.env.get("E2E_ZIG_SERVER_CMD");

    const haveDefaultHooks = await fileExists(paths.zigJustfile);
    const haveClientHook = !!zigClientCmd || haveDefaultHooks;
    const haveServerHook = !!zigServerCmd || haveDefaultHooks;

    if (!cfg.allowMissingHooks) {
      if (
        (cfg.direction === "both" || cfg.direction === "zig-client") &&
        !haveClientHook
      ) {
        console.error(
          `Missing Zig client hook (${paths.zigJustfile}) and no E2E_ZIG_CLIENT_CMD override set`,
        );
        Deno.exit(1);
      }
      if (
        (cfg.direction === "both" || cfg.direction === "zig-server") &&
        !haveServerHook
      ) {
        console.error(
          `Missing Zig server hook (${paths.zigJustfile}) and no E2E_ZIG_SERVER_CMD override set`,
        );
        Deno.exit(1);
      }
    }

    const results: CaseResult[] = [];

    // Scaffold missing hooks.
    if (cfg.allowMissingHooks) {
      if (
        (cfg.direction === "both" || cfg.direction === "zig-client") &&
        !haveClientHook
      ) {
        for (const schema of cfg.schemas) {
          for (const backend of cfg.backends) {
            results.push({
              key: `zig-client:${schema}:${backend}`,
              status: "SKIP(missing-zig-client-hook)",
            });
          }
        }
      }
      if (
        (cfg.direction === "both" || cfg.direction === "zig-server") &&
        !haveServerHook
      ) {
        for (const schema of cfg.schemas) {
          for (const backend of cfg.backends) {
            results.push({
              key: `zig-server:${schema}:${backend}`,
              status: "SKIP(missing-zig-server-hook)",
            });
          }
        }
      }
    }

    if (
      (cfg.direction === "both" || cfg.direction === "zig-client") &&
      haveClientHook
    ) {
      await runZigClientPhase(paths, cfg, zigClientCmd, results);
    }

    if (
      (cfg.direction === "both" || cfg.direction === "zig-server") &&
      haveServerHook
    ) {
      await runZigServerPhase(paths, cfg, zigServerCmd, results);
    }

    writeSummary(paths, results);
  } finally {
    await stopRefServers();
    await composeDown(paths);
  }
}

main();
