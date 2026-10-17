// CI gate tools: the Nightly tag gate and the Nightly failure annotations
// it reads.
//
//   deno run --allow-run=gh,git --allow-read --allow-write --allow-env --no-config \
//     tools/ci_gates.ts nightly-gate <sha> [--flakes FILE] [--repo OWNER/REPO]
//       [--today YYYY-MM-DD] [--created-before ISO-TIME] [--record FILE] [--fixture FILE]
//   deno run --allow-read --allow-env --no-config \
//     tools/ci_gates.ts annotate --log-dir DIR [--expect NAME]
//
// nightly-gate (`just check-nightly-gate SHA`; `just release-tag` runs it)
//   Reads the 3 most recent completed Nightly runs whose head_sha is SHA, with
//   the jobs of every attempt and the nightly-failure annotations of each
//   failed job. It passes only when there are 3 runs and each run is green or
//   fails only on failures that an unexpired row of .github/known-flakes.txt
//   covers. The header of that file gives the row format and the matching
//   rules. Exit 0: pass. Exit 1: refused. Exit 2: the gate could not be
//   evaluated (bad arguments, a bad row, an API error). Callers must treat
//   every nonzero exit as a refusal.
//
//   --today and --created-before evaluate the gate at another time: rows
//   expire against --today, and only runs created before --created-before
//   count. --record writes the API responses it read (only the fields this
//   tool reads) to FILE; --fixture reads them from FILE instead of gh.
//   tools/ci_gates_test.ts uses recorded responses, so it needs no network.
//
// annotate (the final `if: failure()` step of each Nightly job)
//   Reads NIGHTLY_STEPS (`${{ toJSON(steps) }}`) and the logs that the steps
//   wrote to DIR/<step id>.log. For each step that failed, it prints one
//   `::error title=nightly-failure::<name>` workflow command per failure:
//     - `error: '<test>' failed` (and the other Zig test-failure lines) gives
//       the test name;
//     - `soak: FAIL — <reason>` gives `<step id>: <reason>`, with each number
//       in the reason written as N, so the name is the same from run to run;
//     - a failed step with neither line gives `step:<step id>`.
//   GitHub keeps at most 10 error annotations per step. When there are more
//   than 9 names, the 10th annotation has the title nightly-failure-overflow,
//   and the gate refuses that job: no row can cover failures it cannot see.
//   GitHub also keeps at most 50 annotations per job. If earlier steps used
//   them all, the job has no nightly-failure annotation, and only a row
//   with no test can cover it.
//
//   --expect NAME prints no workflow command. It exits 1 unless NAME is one
//   of the names. The Nightly soak job uses it on every run, so a green run
//   also proves the log wiring (the step id in GITHUB_ACTION, the tee into
//   RUNNER_TEMP) on Linux and Windows.

export const REQUIRED_RUNS = 3;
export const ROW_MAX_DAYS = 30;
export const MAX_NAMED = 9;
export const NAME_TITLE = "nightly-failure";
export const OVERFLOW_TITLE = "nightly-failure-overflow";
export const NIGHTLY_WORKFLOW = "nightly.yml";
export const DEFAULT_FLAKES = ".github/known-flakes.txt";

// Job conclusions that are not a failure.
const OK_JOB = new Set(["success", "skipped", "neutral"]);

export class GateError extends Error {}

// ---------------------------------------------------------------------------
// Known-flakes rows

export interface FlakeRow {
  line: number;
  job: string;
  run: string;
  owner: string;
  added: string;
  expires: string;
  test: string | null;
}

const DATE = /^\d{4}-\d{2}-\d{2}$/;

function dayNumber(date: string): number {
  if (!DATE.test(date)) return NaN;
  const ms = Date.parse(`${date}T00:00:00Z`);
  if (Number.isNaN(ms)) return NaN;
  // Date.parse accepts 2026-02-30; a round trip rejects it.
  if (new Date(ms).toISOString().slice(0, 10) !== date) return NaN;
  return ms / 86_400_000;
}

export function isDate(date: string): boolean {
  return !Number.isNaN(dayNumber(date));
}

// Row format: job | run id | owner | added | expires | test (optional).
// The test is the last field, so it can hold a `|`.
export function parseKnownFlakes(text: string): FlakeRow[] {
  const rows: FlakeRow[] = [];
  const lines = text.split(/\r?\n/);
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    const trimmed = raw.trim();
    if (trimmed === "" || trimmed.startsWith("#")) continue;
    const where = `known-flakes line ${i + 1}`;
    const parts = raw.split("|");
    if (parts.length < 5) {
      throw new GateError(
        `${where}: expected "job | run id | owner | added | expires | test", got ${
          JSON.stringify(trimmed)
        }`,
      );
    }
    const [job, run, owner, added, expires] = parts.slice(0, 5).map((p) =>
      p.trim()
    );
    const test = parts.length > 5 ? parts.slice(5).join("|").trim() : "";
    if (job === "") throw new GateError(`${where}: the job name is empty`);
    if (!/^[1-9]\d*$/.test(run)) {
      throw new GateError(
        `${where}: the run id ${JSON.stringify(run)} is not a number`,
      );
    }
    if (owner === "") throw new GateError(`${where}: the owner is empty`);
    if (!isDate(added)) {
      throw new GateError(
        `${where}: the added date ${JSON.stringify(added)} is not YYYY-MM-DD`,
      );
    }
    if (!isDate(expires)) {
      throw new GateError(
        `${where}: the expiry date ${
          JSON.stringify(expires)
        } is not YYYY-MM-DD`,
      );
    }
    const days = dayNumber(expires) - dayNumber(added);
    if (days < 0) {
      throw new GateError(
        `${where}: the row expires (${expires}) before it was added (${added})`,
      );
    }
    if (days > ROW_MAX_DAYS) {
      throw new GateError(
        `${where}: the row expires ${days} days after it was added; the limit is ${ROW_MAX_DAYS}`,
      );
    }
    rows.push({
      line: i + 1,
      job,
      run,
      owner,
      added,
      expires,
      test: test === "" ? null : test,
    });
  }
  return rows;
}

export function rowInForce(row: FlakeRow, today: string): boolean {
  const t = dayNumber(today);
  return dayNumber(row.added) <= t && t <= dayNumber(row.expires);
}

// ---------------------------------------------------------------------------
// Failure names from step logs (annotate)

// The Zig build runner's per-test errors (lib/compiler/Maker/Step/Run.zig).
const TEST_LINE =
  /^error: '(.+)' (?:failed:?|failed without output|errored|leaked \d+ allocations:?|logged \d+ errors:?|terminated .*|timed out after .*)$/;
const SOAK_LINE = /^soak: FAIL\s*(?:—|--?)\s*(.+)$/;
const TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z /;
// deno-lint-ignore no-control-regex
const ANSI = /\x1b\[[0-9;]*[A-Za-z]/g;

function cleanLine(line: string): string {
  return line.replace(/\r$/, "").replace(ANSI, "").replace(TIMESTAMP, "");
}

export function soakName(stepId: string, reason: string): string {
  return `${stepId}: ${reason.trim().replace(/\d+(?:\.\d+)?/g, "N")}`;
}

// The names for one failed step. Never empty: a failed step with no test or
// soak line is named `step:<id>`.
export function failureNames(stepId: string, log: string | null): string[] {
  const names: string[] = [];
  for (const raw of (log ?? "").split("\n")) {
    const line = cleanLine(raw);
    const test = TEST_LINE.exec(line);
    if (test) {
      names.push(test[1]);
      continue;
    }
    const soak = SOAK_LINE.exec(line);
    if (soak) names.push(soakName(stepId, soak[1]));
  }
  if (names.length === 0) names.push(`step:${stepId}`);
  return names;
}

export function escapeData(value: string): string {
  return value.replace(/%/g, "%25").replace(/\r/g, "%0D").replace(/\n/g, "%0A");
}

export interface AnnotateResult {
  names: string[];
  lines: string[];
  notes: string[];
}

// stepsJson is `${{ toJSON(steps) }}`: {id: {outcome, conclusion, outputs}}.
export function annotate(
  stepsJson: string,
  readLog: (stepId: string) => string | null,
): AnnotateResult {
  let steps: Record<string, { conclusion?: string }>;
  try {
    steps = JSON.parse(stepsJson);
  } catch (error) {
    throw new GateError(
      `NIGHTLY_STEPS is not JSON: ${
        error instanceof Error ? error.message : error
      }`,
    );
  }
  if (steps === null || typeof steps !== "object" || Array.isArray(steps)) {
    throw new GateError("NIGHTLY_STEPS is not a JSON object");
  }
  const notes: string[] = [];
  const names: string[] = [];
  for (const [id, step] of Object.entries(steps)) {
    if (step?.conclusion !== "failure") continue;
    const log = readLog(id);
    if (log === null) notes.push(`step ${id} failed and wrote no log`);
    for (const name of failureNames(id, log)) {
      if (!names.includes(name)) names.push(name);
    }
  }
  if (names.length === 0) {
    notes.push(
      "no step with an id failed; the tag gate sees this job as failed with no annotation",
    );
  }
  const lines = names.slice(0, MAX_NAMED).map((n) =>
    `::error title=${NAME_TITLE}::${escapeData(n)}`
  );
  if (names.length > MAX_NAMED) {
    lines.push(
      `::error title=${OVERFLOW_TITLE}::${
        names.length - MAX_NAMED
      } more failures are not named here; see the log`,
    );
  }
  return { names, lines, notes };
}

// ---------------------------------------------------------------------------
// Nightly gate

export interface Api {
  get(path: string): Promise<unknown>;
}

export interface JobEvidence {
  id: number;
  name: string;
  conclusion: string | null;
  attempt: number;
  names: string[];
  overflow: boolean;
}

export interface RunEvidence {
  id: number;
  conclusion: string | null;
  createdAt: string;
  event: string;
  attempt: number;
  url: string;
  jobs: JobEvidence[];
}

export interface Evidence {
  sha: string;
  completed: RunEvidence[];
  inProgress: { id: number; status: string; createdAt: string }[];
}

interface RawRun {
  id: number;
  status: string;
  conclusion: string | null;
  created_at: string;
  event: string;
  run_attempt: number;
  html_url: string;
  head_sha: string;
}

interface RawJob {
  id: number;
  name: string;
  conclusion: string | null;
  run_attempt: number;
}

interface RawAnnotation {
  title: string | null;
  message: string | null;
}

export function runsPath(repo: string, sha: string): string {
  return `repos/${repo}/actions/workflows/${NIGHTLY_WORKFLOW}/runs?head_sha=${sha}&per_page=100`;
}

export function jobsPath(repo: string, runId: number): string {
  return `repos/${repo}/actions/runs/${runId}/jobs?filter=all&per_page=100`;
}

export function annotationsPath(repo: string, jobId: number): string {
  return `repos/${repo}/check-runs/${jobId}/annotations?per_page=100`;
}

function field<T>(value: unknown, key: string, what: string): T {
  if (value === null || typeof value !== "object" || !(key in value)) {
    throw new GateError(`unexpected API response for ${what}: no "${key}"`);
  }
  return (value as Record<string, T>)[key];
}

export async function collectEvidence(
  api: Api,
  repo: string,
  sha: string,
  options: { createdBefore?: string } = {},
): Promise<Evidence> {
  const listing = await api.get(runsPath(repo, sha));
  const runs = field<RawRun[]>(listing, "workflow_runs", "the Nightly runs");
  const total = field<number>(listing, "total_count", "the Nightly runs");
  if (total > runs.length) {
    throw new GateError(
      `${total} Nightly runs exist for ${sha}, more than one page; the gate reads only ${runs.length}`,
    );
  }
  const before = options.createdBefore === undefined
    ? Infinity
    : Date.parse(options.createdBefore);
  if (Number.isNaN(before)) {
    throw new GateError(
      `--created-before ${options.createdBefore} is not a time`,
    );
  }
  const mine = runs
    .filter((r) => r.head_sha === sha && Date.parse(r.created_at) < before)
    .sort((a, b) =>
      Date.parse(b.created_at) - Date.parse(a.created_at) || b.id - a.id
    );
  const inProgress = mine
    .filter((r) => r.status !== "completed")
    .map((r) => ({ id: r.id, status: r.status, createdAt: r.created_at }));
  const completed: RunEvidence[] = [];
  for (
    const run of mine.filter((r) => r.status === "completed").slice(
      0,
      REQUIRED_RUNS,
    )
  ) {
    const jobListing = await api.get(jobsPath(repo, run.id));
    const rawJobs = field<RawJob[]>(
      jobListing,
      "jobs",
      `the jobs of run ${run.id}`,
    );
    const jobTotal = field<number>(
      jobListing,
      "total_count",
      `the jobs of run ${run.id}`,
    );
    if (jobTotal > rawJobs.length) {
      throw new GateError(
        `run ${run.id} has ${jobTotal} jobs, more than one page; the gate reads only ${rawJobs.length}`,
      );
    }
    const jobs: JobEvidence[] = [];
    for (const job of rawJobs) {
      const failed = !OK_JOB.has(job.conclusion ?? "");
      let names: string[] = [];
      let overflow = false;
      if (failed) {
        const annotations = await api.get(annotationsPath(repo, job.id));
        if (!Array.isArray(annotations)) {
          throw new GateError(`unexpected annotations for job ${job.id}`);
        }
        const typed = annotations as RawAnnotation[];
        names = typed.filter((a) => a.title === NAME_TITLE).map((a) =>
          a.message ?? ""
        );
        overflow = typed.some((a) => a.title === OVERFLOW_TITLE);
      }
      jobs.push({
        id: job.id,
        name: job.name,
        conclusion: job.conclusion,
        attempt: job.run_attempt,
        names,
        overflow,
      });
    }
    completed.push({
      id: run.id,
      conclusion: run.conclusion,
      createdAt: run.created_at,
      event: run.event,
      attempt: run.run_attempt,
      url: run.html_url,
      jobs,
    });
  }
  return { sha, completed, inProgress };
}

export interface GateResult {
  pass: boolean;
  lines: string[];
}

function describeRow(row: FlakeRow): string {
  return `known-flakes line ${row.line} (expires ${row.expires})`;
}

export function evaluateGate(
  evidence: Evidence,
  rows: FlakeRow[],
  today: string,
): GateResult {
  if (!isDate(today)) throw new GateError(`--today ${today} is not YYYY-MM-DD`);
  const lines: string[] = [];
  const inForce = rows.filter((r) => rowInForce(r, today));
  lines.push(`Nightly gate for ${evidence.sha} (today ${today})`);
  lines.push(
    `known-flakes: ${inForce.length} rows in force, ${
      rows.length - inForce.length
    } not in force`,
  );
  for (const run of evidence.inProgress) {
    lines.push(
      `run ${run.id} (${run.createdAt}): ${run.status}; the gate does not count it`,
    );
  }

  const problems: string[] = [];
  for (const run of evidence.completed) {
    lines.push(
      `run ${run.id} (${run.createdAt}, ${run.event}, attempt ${run.attempt}): ${run.conclusion} ${run.url}`,
    );
    const failing = run.jobs.filter((j) => !OK_JOB.has(j.conclusion ?? ""));
    if (failing.length === 0) {
      if (run.conclusion !== "success") {
        lines.push(`  the run concluded ${run.conclusion} with no failed job`);
        problems.push(
          `run ${run.id} concluded ${run.conclusion} with no failed job`,
        );
      }
      continue;
    }
    for (const job of failing) {
      const label =
        `job "${job.name}" (attempt ${job.attempt}): ${job.conclusion}`;
      const sameJob = rows.filter((r) => r.job === job.name);
      if (job.overflow) {
        lines.push(
          `  ${label}: more failures than annotations; no row can cover it`,
        );
        problems.push(
          `run ${run.id} job "${job.name}" has more failures than annotations`,
        );
        continue;
      }
      const wanted: (string | null)[] = job.names.length > 0
        ? job.names
        : [null];
      if (job.names.length === 0) {
        lines.push(`  ${label}, with no ${NAME_TITLE} annotation`);
      } else lines.push(`  ${label}`);
      for (const test of wanted) {
        const what = test === null ? "the job, with no test" : `test "${test}"`;
        const matches = sameJob.filter((r) => r.test === test);
        const covering = matches.find((r) => rowInForce(r, today));
        if (covering) {
          lines.push(
            `    ${what}: covered by ${
              describeRow(covering)
            }, run ${covering.run}, owner ${covering.owner}`,
          );
          continue;
        }
        const stale = matches.length > 0
          ? `; ${matches.map(describeRow).join(", ")} is not in force`
          : "";
        lines.push(`    ${what}: NOT COVERED${stale}`);
        problems.push(
          `run ${run.id}, job "${job.name}": no row in force covers ${what}`,
        );
      }
    }
  }

  const count = evidence.completed.length;
  if (count < REQUIRED_RUNS) {
    problems.unshift(
      `${count} completed Nightly run${
        count === 1 ? "" : "s"
      } on ${evidence.sha}; the gate needs ${REQUIRED_RUNS}` +
        ` (start them with: gh workflow run Nightly --ref main)`,
    );
  }
  const pass = problems.length === 0;
  if (pass) {
    lines.push(
      `PASS: ${count} Nightly runs, each green or failing only on unexpired known-flakes rows`,
    );
  } else for (const p of problems) lines.push(`REFUSED: ${p}`);
  return { pass, lines };
}

// ---------------------------------------------------------------------------
// CLI

const decoder = new TextDecoder();

async function run(
  cmd: string,
  args: string[],
): Promise<{ code: number; stdout: string; stderr: string }> {
  const out = await new Deno.Command(cmd, {
    args,
    stdout: "piped",
    stderr: "piped",
  }).output();
  return {
    code: out.code,
    stdout: decoder.decode(out.stdout),
    stderr: decoder.decode(out.stderr),
  };
}

class GhApi implements Api {
  async get(path: string): Promise<unknown> {
    const out = await run("gh", [
      "api",
      "-H",
      "Accept: application/vnd.github+json",
      path,
    ]);
    if (out.code !== 0) {
      throw new GateError(
        `gh api ${path} failed: ${out.stderr.trim() || `exit ${out.code}`}`,
      );
    }
    return JSON.parse(out.stdout);
  }
}

function pick(
  value: Record<string, unknown>,
  keys: string[],
): Record<string, unknown> {
  return Object.fromEntries(
    keys.filter((k) => k in value).map((k) => [k, value[k]]),
  );
}

// Keep only the fields this tool reads, so recorded fixtures stay small.
export function trimResponse(path: string, value: unknown): unknown {
  if (path.includes("/annotations")) {
    return (value as Record<string, unknown>[]).map((a) =>
      pick(a, ["annotation_level", "title", "message"])
    );
  }
  const v = value as Record<string, unknown>;
  if (path.includes("/jobs?")) {
    return {
      total_count: v.total_count,
      jobs: (v.jobs as Record<string, unknown>[]).map((j) =>
        pick(j, ["id", "name", "status", "conclusion", "run_attempt"])
      ),
    };
  }
  if (path.includes("/runs?")) {
    const keys = [
      "id",
      "name",
      "head_sha",
      "status",
      "conclusion",
      "created_at",
      "event",
      "run_attempt",
      "html_url",
    ];
    return {
      total_count: v.total_count,
      workflow_runs: (v.workflow_runs as Record<string, unknown>[]).map((r) =>
        pick(r, keys)
      ),
    };
  }
  return value;
}

class RecordingApi implements Api {
  readonly seen: Record<string, unknown> = {};
  constructor(private readonly inner: Api) {}
  async get(path: string): Promise<unknown> {
    const value = trimResponse(path, await this.inner.get(path));
    this.seen[path] = value;
    return value;
  }
}

export class FixtureApi implements Api {
  constructor(private readonly responses: Record<string, unknown>) {}
  get(path: string): Promise<unknown> {
    if (!(path in this.responses)) {
      return Promise.reject(
        new GateError(`the fixture has no response for ${path}`),
      );
    }
    return Promise.resolve(this.responses[path]);
  }
}

function parseFlags(
  args: string[],
  known: string[],
): { positional: string[]; flags: Record<string, string> } {
  const positional: string[] = [];
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (!arg.startsWith("--")) {
      positional.push(arg);
      continue;
    }
    const eq = arg.indexOf("=");
    const name = eq < 0 ? arg.slice(2) : arg.slice(2, eq);
    if (!known.includes(name)) throw new GateError(`unknown option --${name}`);
    const value = eq < 0 ? args[++i] : arg.slice(eq + 1);
    if (value === undefined) throw new GateError(`--${name} needs a value`);
    flags[name] = value;
  }
  return { positional, flags };
}

async function resolveSha(ref: string): Promise<string> {
  if (/^[0-9a-f]{40}$/.test(ref)) return ref;
  const out = await run("git", [
    "rev-parse",
    "--verify",
    "--quiet",
    `${ref}^{commit}`,
  ]);
  const sha = out.stdout.trim();
  if (out.code !== 0 || !/^[0-9a-f]{40}$/.test(sha)) {
    throw new GateError(
      `cannot resolve ${
        JSON.stringify(ref)
      } to a commit in this checkout; pass the full 40-character SHA`,
    );
  }
  return sha;
}

async function nightlyGate(args: string[]): Promise<number> {
  const { positional, flags } = parseFlags(args, [
    "flakes",
    "repo",
    "today",
    "created-before",
    "record",
    "fixture",
  ]);
  if (positional.length !== 1) {
    throw new GateError("usage: nightly-gate <sha> [options]");
  }
  const sha = await resolveSha(positional[0]);
  const repo = flags.repo ?? ":owner/:repo";
  const today = flags.today ?? new Date().toISOString().slice(0, 10);
  const rows = parseKnownFlakes(
    await Deno.readTextFile(flags.flakes ?? DEFAULT_FLAKES),
  );
  let api: Api;
  if (flags.fixture !== undefined) {
    api = new FixtureApi(JSON.parse(await Deno.readTextFile(flags.fixture)));
  } else api = new GhApi();
  const recorder = flags.record !== undefined ? new RecordingApi(api) : null;
  const evidence = await collectEvidence(recorder ?? api, repo, sha, {
    createdBefore: flags["created-before"],
  });
  if (recorder) {
    await Deno.writeTextFile(
      flags.record,
      JSON.stringify(recorder.seen, null, 2) + "\n",
    );
  }
  const result = evaluateGate(evidence, rows, today);
  console.log(result.lines.join("\n"));
  return result.pass ? 0 : 1;
}

function annotateCommand(args: string[]): number {
  const { positional, flags } = parseFlags(args, ["log-dir", "expect"]);
  if (positional.length !== 0 || flags["log-dir"] === undefined) {
    throw new GateError("usage: annotate --log-dir DIR [--expect NAME]");
  }
  const dir = flags["log-dir"];
  const result = annotate(Deno.env.get("NIGHTLY_STEPS") ?? "{}", (id) => {
    try {
      return Deno.readTextFileSync(`${dir}/${id}.log`);
    } catch {
      return null;
    }
  });
  for (const note of result.notes) console.log(`note: ${note}`);
  // --expect checks the log wiring on a green run: it prints no workflow
  // command, so it adds no annotation.
  const expected = flags.expect;
  if (expected !== undefined) {
    const found = result.names.includes(expected);
    console.log(
      found
        ? `wiring check: found "${expected}"`
        : `wiring check: expected "${expected}", got ${
          JSON.stringify(result.names)
        }`,
    );
    return found ? 0 : 1;
  }
  for (const line of result.lines) console.log(line);
  return 0;
}

export async function main(args: string[]): Promise<number> {
  const [command, ...rest] = args;
  try {
    switch (command) {
      case "nightly-gate":
        return await nightlyGate(rest);
      case "annotate":
        return annotateCommand(rest);
      default:
        console.error(
          "usage: ci_gates.ts nightly-gate|annotate ...",
        );
        return 2;
    }
  } catch (error) {
    // Exit 2, never 1: a gate that could not run has not refused for a reason.
    if (
      error instanceof GateError || error instanceof SyntaxError ||
      error instanceof Deno.errors.NotFound
    ) {
      console.error(`error: ${error.message}`);
    } else {
      console.error(error);
    }
    return 2;
  }
}

if (import.meta.main) Deno.exit(await main(Deno.args));
