// CI gate tools: the Nightly tag gate, the Nightly failure annotations it
// reads, the job-timeout check for the workflow files, and the job headroom
// check.
//
//   deno run --allow-run=gh,git --allow-read --allow-write --allow-env --no-config \
//     tools/ci_gates.ts nightly-gate <sha> [--flakes FILE] [--repo OWNER/REPO]
//       [--today YYYY-MM-DD] [--created-before ISO-TIME] [--record FILE] [--fixture FILE]
//   deno run --allow-read --allow-env --no-config \
//     tools/ci_gates.ts annotate --log-dir DIR [--expect NAME]
//   deno run --allow-read --no-config \
//     tools/ci_gates.ts job-timeouts [WORKFLOW.yml...]
//   deno run --allow-run=gh --allow-read --allow-write --no-config \
//     tools/ci_gates.ts job-headroom [--workflow-file FILE] [--branch NAME]
//       [--runs N] [--allow-few-runs JOB_ID]... [--repo OWNER/REPO]
//       [--record FILE] [--fixture FILE]
//
// nightly-gate (`just check-nightly-gate SHA`; `just release-tag` runs it)
//   Reads the 3 most recent completed Nightly runs whose head_sha is SHA, with
//   the jobs of every attempt and the nightly-failure annotations of each
//   failed job. It passes only when there are 3 runs and each run is green or
//   fails only on failures that an unexpired row of .github/known-flakes.txt
//   covers. The header of that file gives the row format and the matching
//   rules. It also lists the older completed runs on SHA that did not
//   succeed; it does not count them. Exit 0: pass. Exit 1: refused. Exit 2: the gate could not be
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
//     - a failed step with neither line gives `step:<step id>`;
//     - a step also gives `step:<step id>` when its log shows a failure that
//       has no test name: the test process crashed or stopped responding, a
//       compile error, or a Build Summary that counts more failed, crashed
//       or timed-out tests than the log names. So a row for one flaky test
//       does not cover a second failure in the same step.
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
//
// job-timeouts (the CI job "CI gate tools")
//   Fails when a job in a workflow file has no job-level `timeout-minutes`,
//   or when its value is not a whole number from 1 to 360. A step-level
//   `timeout-minutes` does not count. The scan reads the block structure by
//   indentation and stops with an error on a layout it does not know, so an
//   unusual file is a failure, not a silent pass. With no file, it checks
//   every .yml and .yaml file in .github/workflows, so a new workflow is
//   checked too.
//
// job-headroom (`just check-job-headroom`; `just release-preflight` runs it)
//   Reads the 10 most recent green runs (--runs, 10 to 100) of a workflow
//   (default .github/workflows/ci.yml) on main (--branch), and the latest
//   attempt of each job in them. A run counts only when its event is push,
//   schedule or workflow_dispatch: a pull request from a fork's own main
//   branch is not a main run. For each job that succeeded, it takes the
//   time from start to end. It prints the peak and its run id for each job
//   name, the job-level timeout-minutes of the job in the workflow file,
//   and the peak as a share of that cap. It refuses when:
//     - a peak is over 75% of its cap;
//     - a job has no usable cap;
//     - there are fewer than 10 green runs;
//     - a job name succeeded in fewer than 10 of the runs, or a job of the
//       file has no job name in them (a new job, or a job whose `name:`
//       changed);
//     - no job in the runs matches a job of the file.
//   Exit 0: pass. Exit 1: refused. Exit 2: it could not evaluate.
//
//   A run's job name comes from the job's `name:` in the workflow file;
//   each `${{ ... }}` in it stands for any text, such as a matrix value, so
//   a matrix job has one job name per leg. The matrix value is part of the
//   name, so a runner label change renames the job. RUNNER_RENAMES maps
//   each old label to its new one (ubuntu-latest to ubuntu-24.04), so a
//   job's history does not start over at the change: without the map, each
//   leg would have too few runs under each name. A job name that matches no
//   job of the file (a job that was removed or renamed) is listed as a note.
//
//   --allow-few-runs JOB_ID (repeatable) passes that job with fewer than 10
//   runs, for example a new job or a matrix leg that was added or removed.
//   The peaks it has are still checked. --record and --fixture work as for
//   nightly-gate.

export const REQUIRED_RUNS = 3;
export const ROW_MAX_DAYS = 30;
export const MAX_NAMED = 9;
export const NAME_TITLE = "nightly-failure";
export const OVERFLOW_TITLE = "nightly-failure-overflow";
export const NIGHTLY_WORKFLOW = "nightly.yml";
export const DEFAULT_FLAKES = ".github/known-flakes.txt";
export const MAX_TIMEOUT_MINUTES = 360;
export const WORKFLOW_DIR = ".github/workflows";

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
// Group 2 is set for the forms that the Build Summary counts as a failed,
// crashed or timed-out test; leaks and logged errors are not counted there.
const TEST_LINE =
  /^error: '(.+)' (?:leaked \d+ allocations:?|logged \d+ errors:?|(failed:?|failed without output|errored|exited with .*|terminated .*|stopped with .*|timed out after .*))$/;
// Failures with no test name: the test process crashed between tests or
// stopped responding (Run.zig step.fail), or a compile error.
const UNNAMED_LINE =
  /^error: (?:unable to write stdin \(.*\); )?test process unexpectedly |^error: test runner failed to respond |^error: test '.+' .*; input saved to |^\S.*:\d+:\d+: error: /;
// "Build Summary: 16/18 steps succeeded (1 failed); 222/223 tests passed
// (1 failed)". The parentheses can also hold "N skipped", "N crashed" and
// "N timed out" (lib/compiler/Maker.zig).
const SUMMARY_LINE = /^Build Summary: .*; \d+\/\d+ tests passed \(([^)]*)\)/;
const SUMMARY_COUNT = /(\d+) (?:failed|crashed|timed out)/g;
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
// soak line is named `step:<id>`. A step that also has a failure with no
// test name gets `step:<id>` after its test names.
export function failureNames(stepId: string, log: string | null): string[] {
  const names: string[] = [];
  let unnamed = false;
  let counted = 0; // test lines of the kinds that the Build Summary counts
  let summarized = 0; // failed + crashed + timed out, from the Build Summary
  for (const raw of (log ?? "").split("\n")) {
    const line = cleanLine(raw);
    const test = TEST_LINE.exec(line);
    if (test) {
      names.push(test[1]);
      if (test[2] !== undefined) counted++;
      continue;
    }
    const soak = SOAK_LINE.exec(line);
    if (soak) {
      names.push(soakName(stepId, soak[1]));
      continue;
    }
    if (UNNAMED_LINE.test(line)) {
      unnamed = true;
      continue;
    }
    const summary = SUMMARY_LINE.exec(line);
    if (summary) {
      for (const m of summary[1].matchAll(SUMMARY_COUNT)) {
        summarized += Number(m[1]);
      }
    }
  }
  if (names.length === 0 || unnamed || summarized > counted) {
    names.push(`step:${stepId}`);
  }
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
  // Completed runs older than the ones the gate reads. The gate does not
  // count them, but it lists the ones that did not succeed.
  older: {
    id: number;
    conclusion: string | null;
    createdAt: string;
    url: string;
  }[];
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
  const done = mine.filter((r) => r.status === "completed");
  const older = done.slice(REQUIRED_RUNS).map((r) => ({
    id: r.id,
    conclusion: r.conclusion,
    createdAt: r.created_at,
    url: r.html_url,
  }));
  const completed: RunEvidence[] = [];
  for (const run of done.slice(0, REQUIRED_RUNS)) {
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
  return { sha, completed, inProgress, older };
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

  for (const run of evidence.older) {
    if (run.conclusion === "success") continue;
    lines.push(
      `run ${run.id} (${run.createdAt}): ${run.conclusion}, older than the ${REQUIRED_RUNS} runs above; the gate does not count it, but read it before you tag ${run.url}`,
    );
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
// Job timeouts

export interface JobTimeout {
  id: string;
  line: number;
  timeout: string | null;
  // The job-level `name:`, or null when the job has none (GitHub then shows
  // the job id).
  name: string | null;
}

const KEY_LINE = /^([A-Za-z0-9_-]+):(?:[ \t]+(.*))?$/;

function stripComment(value: string): string {
  return value.replace(/(^|\s)#.*$/, "").trim();
}

// A YAML scalar on one line: a quoted value keeps a " #" inside the quotes.
// The scan does not read escapes (\" in double quotes, '' in single
// quotes), so it stops on one instead of cutting the value short.
function scalar(value: string, where: string): string {
  const v = value.trim();
  const quote = v[0];
  if (quote === '"' || quote === "'") {
    const end = v.indexOf(quote, 1);
    if (end > 0) {
      const inner = v.slice(1, end);
      if (
        (quote === '"' && inner.includes("\\")) ||
        (quote === "'" && v[end + 1] === "'")
      ) {
        throw new GateError(
          `${where}: a quoted value with an escape; write it without one`,
        );
      }
      return inner;
    }
  }
  return stripComment(v);
}

// Lists each job of a workflow file with its job-level timeout-minutes.
// Throws GateError on a layout the scan does not know.
export function jobTimeouts(text: string, file = "workflow"): JobTimeout[] {
  const lines = text.split(/\r?\n/);
  const content: { n: number; indent: number; body: string }[] = [];
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    if (raw.trim() === "") continue;
    const indentText = /^[ \t]*/.exec(raw)![0];
    if (indentText.includes("\t")) {
      throw new GateError(`${file}:${i + 1}: a tab in the indentation`);
    }
    const body = raw.slice(indentText.length);
    if (body.startsWith("#")) continue;
    content.push({ n: i + 1, indent: indentText.length, body });
  }
  const start = content.findIndex((l) =>
    l.indent === 0 && /^jobs:\s*(#.*)?$/.test(l.body)
  );
  if (start < 0) throw new GateError(`${file}: no top-level "jobs:" block`);
  let end = content.length;
  for (let k = start + 1; k < content.length; k++) {
    if (content[k].indent === 0) {
      end = k;
      break;
    }
  }
  const block = content.slice(start + 1, end);
  if (block.length === 0) {
    throw new GateError(`${file}: the "jobs:" block is empty`);
  }
  const jobIndent = block[0].indent;
  const jobs: JobTimeout[] = [];
  let current: JobTimeout | null = null;
  let propIndent = -1;
  for (const l of block) {
    if (l.indent < jobIndent) {
      throw new GateError(`${file}:${l.n}: a line outside the job layout`);
    }
    if (l.indent === jobIndent) {
      const m = /^([A-Za-z_][A-Za-z0-9_-]*):\s*(#.*)?$/.exec(l.body);
      if (!m) {
        throw new GateError(
          `${file}:${l.n}: expected a job id, got ${JSON.stringify(l.body)}`,
        );
      }
      current = { id: m[1], line: l.n, timeout: null, name: null };
      jobs.push(current);
      propIndent = -1;
      continue;
    }
    if (current === null) {
      throw new GateError(`${file}:${l.n}: a line before the first job`);
    }
    if (propIndent < 0) propIndent = l.indent;
    if (l.indent < propIndent) {
      throw new GateError(
        `${file}:${l.n}: a line between the job and its keys`,
      );
    }
    if (l.indent > propIndent) continue; // steps, matrix, env, run scripts
    const key = KEY_LINE.exec(l.body);
    if (!key) {
      throw new GateError(
        `${file}:${l.n}: expected a job key, got ${JSON.stringify(l.body)}`,
      );
    }
    if (key[1] === "timeout-minutes") {
      current.timeout = stripComment(key[2] ?? "").replace(
        /^(["'])(.*)\1$/,
        "$2",
      );
    }
    if (key[1] === "name") {
      current.name = scalar(key[2] ?? "", `${file}:${l.n}`);
    }
  }
  return jobs;
}

export function timeoutProblems(file: string, jobs: JobTimeout[]): string[] {
  const problems: string[] = [];
  for (const job of jobs) {
    if (job.timeout === null || job.timeout === "") {
      problems.push(
        `${file}:${job.line}: job "${job.id}" has no job-level timeout-minutes`,
      );
      continue;
    }
    const minutes = /^[1-9]\d*$/.test(job.timeout) ? Number(job.timeout) : NaN;
    if (!(minutes >= 1 && minutes <= MAX_TIMEOUT_MINUTES)) {
      problems.push(
        `${file}:${job.line}: job "${job.id}" has timeout-minutes ${
          JSON.stringify(job.timeout)
        }; use a whole number from 1 to ${MAX_TIMEOUT_MINUTES}`,
      );
    }
  }
  return problems;
}

// ---------------------------------------------------------------------------
// Job headroom

export const HEADROOM_RUNS = 10;
export const HEADROOM_MAX_RUNS = 100;
export const HEADROOM_SHARE = 0.75;
export const DEFAULT_HEADROOM_WORKFLOW = `${WORKFLOW_DIR}/ci.yml`;

// Runner labels that a job name used to carry, and the label that replaced
// each one. A matrix job's name embeds its runner label ("Test
// (ubuntu-latest)"), so a label change renames the job. The gate counts a
// run under the old name as a run of the new name, so the history of a job
// does not start over at the change.
export const RUNNER_RENAMES: readonly (readonly [string, string])[] = [
  // GitHub moves ubuntu-latest to Ubuntu 26 from 2026-10-19, so the
  // workflows pin ubuntu-24.04 (v0.29.0).
  ["ubuntu-latest", "ubuntu-24.04"],
];

// The events of a main run. A pull request from a fork's own main branch is
// listed under branch=main too, but it is not a main run.
export const HEADROOM_EVENTS: ReadonlySet<string> = new Set([
  "push",
  "schedule",
  "workflow_dispatch",
]);

export interface HeadroomRun {
  id: number;
  sha: string;
  event: string;
  createdAt: string;
  attempt: number;
}

export interface HeadroomSample {
  run: number;
  job: number;
  name: string;
  seconds: number;
}

export interface HeadroomEvidence {
  workflow: string;
  branch: string;
  runs: HeadroomRun[];
  samples: HeadroomSample[];
}

interface RawTimedJob extends RawJob {
  started_at: string | null;
  completed_at: string | null;
}

export function headroomRunsPath(
  repo: string,
  workflow: string,
  branch: string,
  count: number,
): string {
  return `repos/${repo}/actions/workflows/${workflow}/runs?branch=${
    encodeURIComponent(branch)
  }&status=success&per_page=${count}`;
}

// The latest attempt of each job: a job that a re-run replaced is not a
// healthy run of that job.
export function latestJobsPath(repo: string, runId: number): string {
  return `repos/${repo}/actions/runs/${runId}/jobs?filter=latest&per_page=100`;
}

// Reads the `count` most recent green runs of `workflow` (a file name in
// .github/workflows, such as ci.yml) on `branch`, and the duration of each
// job in them that succeeded. A run from another branch or from an event
// that HEADROOM_EVENTS does not name is dropped, so the gate can then have
// fewer runs than it asked for.
export async function collectHeadroom(
  api: Api,
  repo: string,
  workflow: string,
  branch: string,
  count: number,
): Promise<HeadroomEvidence> {
  if (!(Number.isInteger(count) && count >= 1 && count <= HEADROOM_MAX_RUNS)) {
    throw new GateError(
      `--runs ${count}: use a whole number from 1 to ${HEADROOM_MAX_RUNS}`,
    );
  }
  const listing = await api.get(
    headroomRunsPath(repo, workflow, branch, count),
  );
  const raw = field<(RawRun & { head_branch?: string })[]>(
    listing,
    "workflow_runs",
    `the ${workflow} runs`,
  );
  const green = raw
    .filter((r) =>
      r.status === "completed" && r.conclusion === "success" &&
      (r.head_branch === undefined || r.head_branch === branch) &&
      HEADROOM_EVENTS.has(r.event)
    )
    .sort((a, b) =>
      Date.parse(b.created_at) - Date.parse(a.created_at) || b.id - a.id
    )
    .slice(0, count);
  const runs: HeadroomRun[] = [];
  const samples: HeadroomSample[] = [];
  for (const run of green) {
    runs.push({
      id: run.id,
      sha: run.head_sha,
      event: run.event,
      createdAt: run.created_at,
      attempt: run.run_attempt,
    });
    const jobListing = await api.get(latestJobsPath(repo, run.id));
    const jobs = field<RawTimedJob[]>(
      jobListing,
      "jobs",
      `the jobs of run ${run.id}`,
    );
    const total = field<number>(
      jobListing,
      "total_count",
      `the jobs of run ${run.id}`,
    );
    if (total > jobs.length) {
      throw new GateError(
        `run ${run.id} has ${total} jobs, more than one page; the gate reads only ${jobs.length}`,
      );
    }
    for (const job of jobs) {
      if (job.conclusion !== "success") continue;
      if (job.started_at === null || job.completed_at === null) continue;
      const seconds =
        (Date.parse(job.completed_at) - Date.parse(job.started_at)) / 1000;
      if (!(seconds >= 0)) {
        throw new GateError(
          `job ${job.id} of run ${run.id} has no usable start and end time`,
        );
      }
      samples.push({ run: run.id, job: job.id, name: job.name, seconds });
    }
  }
  return { workflow, branch, runs, samples };
}

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// Replaces each old runner label of RUNNER_RENAMES that stands as a whole
// token of the name: "Test (ubuntu-latest)", but not "ubuntu-latest-arm".
export function normalizeJobName(
  name: string,
  renames: readonly (readonly [string, string])[] = RUNNER_RENAMES,
): string {
  let out = name;
  for (const [from, to] of renames) {
    out = out.replace(
      new RegExp(`(^|[ (,])${escapeRegExp(from)}(?=$|[ ),])`, "g"),
      `$1${to}`,
    );
  }
  return out;
}

// The names GitHub gives the runs of a workflow job. Each `${{ ... }}` in
// the job's `name:` stands for any text, such as a matrix value. A job with
// no `name:` is shown as its id, with the matrix values in parentheses.
export function jobNamePattern(job: JobTimeout): RegExp {
  if (job.name === null || job.name === "") {
    return new RegExp(`^${escapeRegExp(job.id)}(?: \\(.+\\))?$`);
  }
  const parts = job.name.split(/\$\{\{.*?\}\}/);
  return new RegExp(`^${parts.map(escapeRegExp).join(".+?")}$`);
}

function minutes(seconds: number): string {
  return (Math.round(seconds / 6) / 10).toFixed(1);
}

function percent(part: number, whole: number): string {
  return (Math.round((1000 * part) / whole) / 10).toFixed(1);
}

function plural(count: number, word: string): string {
  return `${count} ${word}${count === 1 ? "" : "s"}`;
}

export interface HeadroomOptions {
  // The workflow file named in the output (default: evidence.workflow).
  file?: string;
  // The fewest green runs, and the fewest of them that each job name of the
  // file must have succeeded in (default HEADROOM_RUNS).
  minRuns?: number;
  // The largest share of its cap that a peak may reach (default
  // HEADROOM_SHARE).
  share?: number;
  // Old and new runner labels (default RUNNER_RENAMES).
  renames?: readonly (readonly [string, string])[];
  // Job ids that may have fewer than minRuns runs (--allow-few-runs). Their
  // peaks are still checked.
  allowFewRuns?: readonly string[];
}

export function evaluateHeadroom(
  evidence: HeadroomEvidence,
  jobs: JobTimeout[],
  options: HeadroomOptions = {},
): GateResult {
  const file = options.file ?? evidence.workflow;
  const minRuns = options.minRuns ?? HEADROOM_RUNS;
  const share = options.share ?? HEADROOM_SHARE;
  const renames = options.renames ?? RUNNER_RENAMES;
  const allowed = [...new Set(options.allowFewRuns ?? [])];
  const limit = `${Math.round(share * 100)}%`;
  const lines: string[] = [];
  const problems: string[] = [];
  const runs = evidence.runs;
  lines.push(
    runs.length === 0
      ? `Job headroom for ${file} on ${evidence.branch}: no green run`
      : `Job headroom for ${file} on ${evidence.branch}: ${runs.length} green runs, ${
        runs.at(-1)!.id
      } (${runs.at(-1)!.createdAt}) to ${runs[0].id} (${runs[0].createdAt})`,
  );
  lines.push(
    `A job may run for at most ${limit} of its job-level timeout-minutes. Each job name of a job must have succeeded in at least ${minRuns} of the runs. ${
      renames.map(([from, to]) => `A name with ${from} counts as ${to}.`)
        .join(" ")
    }`.trimEnd(),
  );
  // With too few runs, every job has too few runs: refuse once, for the runs.
  const enoughRuns = runs.length >= minRuns;
  if (!enoughRuns) {
    problems.push(
      `${runs.length} green runs of ${evidence.workflow} on ${evidence.branch}; the gate needs ${minRuns}`,
    );
  }

  const patterns = jobs.map((job) => ({
    job,
    pattern: jobNamePattern(job),
    literal: job.name !== null && !job.name.includes("${{"),
  }));
  interface Group {
    job: JobTimeout;
    name: string;
    peak: HeadroomSample;
    runs: Set<number>;
  }
  const groups = new Map<string, Group>();
  const unmatched = new Map<string, number>();
  for (const sample of evidence.samples) {
    const name = normalizeJobName(sample.name, renames);
    let found = patterns.filter((p) => p.pattern.test(name));
    if (found.length > 1 && found.some((p) => p.literal)) {
      found = found.filter((p) => p.literal);
    }
    if (found.length > 1) {
      throw new GateError(
        `job "${sample.name}" matches ${found.length} jobs of ${file}: ${
          found.map((p) => p.job.id).join(", ")
        }`,
      );
    }
    if (found.length === 0) {
      unmatched.set(name, (unmatched.get(name) ?? 0) + 1);
      continue;
    }
    const group = groups.get(name);
    if (group === undefined) {
      groups.set(name, {
        job: found[0].job,
        name,
        peak: sample,
        runs: new Set([sample.run]),
      });
      continue;
    }
    group.runs.add(sample.run);
    if (sample.seconds > group.peak.seconds) group.peak = sample;
  }

  const ordered = [...groups.values()].sort((a, b) =>
    a.job.line - b.job.line || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0)
  );
  const namesOf = new Map<JobTimeout, Group[]>();
  for (const g of ordered) {
    namesOf.set(g.job, [...(namesOf.get(g.job) ?? []), g]);
  }
  // A job is fully checked when it has a job name and each of its job
  // names succeeded in at least minRuns runs.
  const full = (job: JobTimeout) =>
    (namesOf.get(job) ?? []).length > 0 &&
    namesOf.get(job)!.every((g) => g.runs.size >= minRuns);
  for (const g of ordered) {
    const count = g.runs.size;
    const few = count < minRuns;
    const fewMark = few ? `FEWER THAN ${minRuns} RUNS` : null;
    const cap = g.job.timeout !== null && /^[1-9]\d*$/.test(g.job.timeout)
      ? Number(g.job.timeout)
      : NaN;
    if (!(cap >= 1 && cap <= MAX_TIMEOUT_MINUTES)) {
      lines.push(
        `  ${g.name}: peak ${
          minutes(g.peak.seconds)
        } min (run ${g.peak.run}), no usable cap, ${plural(count, "run")}${
          fewMark ? `: ${fewMark}` : ""
        }`,
      );
      problems.push(
        `${g.name}: job "${g.job.id}" (${file}:${g.job.line}) has no job-level timeout-minutes from 1 to ${MAX_TIMEOUT_MINUTES}`,
      );
      continue;
    }
    const over = g.peak.seconds > share * cap * 60;
    const marks = [over ? `OVER ${limit}` : null, fewMark].filter((m) =>
      m !== null
    );
    lines.push(
      `  ${g.name}: peak ${
        minutes(g.peak.seconds)
      } min (run ${g.peak.run}), cap ${cap} min, ${
        percent(g.peak.seconds, cap * 60)
      }%, ${plural(count, "run")}${
        marks.length > 0 ? `: ${marks.join(", ")}` : ""
      }`,
    );
    if (over) {
      problems.push(
        `${g.name} peaked at ${
          minutes(g.peak.seconds)
        } min in run ${g.peak.run}, ${
          percent(g.peak.seconds, cap * 60)
        }% of its ${cap}-min cap; the limit is ${limit} (${
          minutes(share * cap * 60)
        } min)`,
      );
    }
    if (few && enoughRuns && !allowed.includes(g.job.id)) {
      problems.push(
        `${g.name} succeeded in ${count} of the ${runs.length} green runs; the gate needs ${minRuns} (job "${g.job.id}", ${file}:${g.job.line}). Wait for more green runs, or pass --allow-few-runs ${g.job.id}`,
      );
    }
  }
  for (const [name, count] of unmatched) {
    lines.push(
      `note: job "${name}" matches no job of ${file} (${
        plural(count, "run")
      }); it is not checked`,
    );
  }
  for (const job of jobs) {
    if (namesOf.has(job)) continue;
    if (allowed.includes(job.id)) {
      lines.push(
        `note: job "${job.id}" (${file}:${job.line}) has no successful run in these runs; it passed with --allow-few-runs`,
      );
    } else if (enoughRuns) {
      problems.push(
        `job "${job.id}" (${file}:${job.line}) has no successful run in the ${runs.length} green runs, so its cap is not checked. Wait for ${minRuns} green runs, or pass --allow-few-runs ${job.id}`,
      );
    }
  }
  // An opt-out cannot excuse this one: with no sample, nothing is checked.
  if (runs.length > 0 && groups.size === 0) {
    problems.push(
      `no successful job in the ${runs.length} green runs matches a job of ${file}`,
    );
  }
  const excused: string[] = [];
  for (const id of allowed) {
    const job = jobs.find((j) => j.id === id);
    if (job === undefined) {
      lines.push(`note: --allow-few-runs ${id}: ${file} has no job "${id}"`);
    } else if (full(job)) {
      lines.push(
        `note: --allow-few-runs ${id}: the job ran in at least ${minRuns} green runs; it is checked as usual`,
      );
    } else excused.push(id);
  }

  const pass = problems.length === 0;
  if (pass) {
    const fullJobs = jobs.filter(full).length;
    lines.push(
      `PASS: ${plural(jobs.length, "job")} of ${file} (${
        plural(ordered.length, "job name")
      }) peaked at or below ${limit} of ${
        jobs.length === 1 ? "its cap" : "their caps"
      }; ${fullJobs} of ${jobs.length} ran in at least ${minRuns} of the ${runs.length} green runs${
        excused.length > 0
          ? `; ${excused.length} passed with --allow-few-runs: ${
            excused.join(", ")
          }`
          : ""
      }`,
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
        pick(j, [
          "id",
          "name",
          "status",
          "conclusion",
          "run_attempt",
          "started_at",
          "completed_at",
        ])
      ),
    };
  }
  if (path.includes("/runs?")) {
    const keys = [
      "id",
      "name",
      "head_branch",
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

// An option in `repeated` can be given more than once; its values go to
// `lists`. Any other option keeps its last value.
function parseFlags(
  args: string[],
  known: string[],
  repeated: string[] = [],
): {
  positional: string[];
  flags: Record<string, string>;
  lists: Record<string, string[]>;
} {
  const positional: string[] = [];
  const flags: Record<string, string> = {};
  const lists: Record<string, string[]> = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (!arg.startsWith("--")) {
      positional.push(arg);
      continue;
    }
    const eq = arg.indexOf("=");
    const name = eq < 0 ? arg.slice(2) : arg.slice(2, eq);
    if (!known.includes(name) && !repeated.includes(name)) {
      throw new GateError(`unknown option --${name}`);
    }
    const value = eq < 0 ? args[++i] : arg.slice(eq + 1);
    if (value === undefined) throw new GateError(`--${name} needs a value`);
    if (repeated.includes(name)) (lists[name] ??= []).push(value);
    else flags[name] = value;
  }
  return { positional, flags, lists };
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

async function workflowFiles(): Promise<string[]> {
  const files: string[] = [];
  try {
    for await (const entry of Deno.readDir(WORKFLOW_DIR)) {
      if (entry.isFile && /\.ya?ml$/.test(entry.name)) {
        files.push(`${WORKFLOW_DIR}/${entry.name}`);
      }
    }
  } catch (error) {
    throw new GateError(
      `cannot read ${WORKFLOW_DIR}: ${
        error instanceof Error ? error.message : error
      }`,
    );
  }
  if (files.length === 0) {
    throw new GateError(`${WORKFLOW_DIR} has no .yml or .yaml file`);
  }
  return files.sort();
}

async function jobTimeoutsCommand(args: string[]): Promise<number> {
  if (args.some((a) => a.startsWith("--"))) {
    throw new GateError("usage: job-timeouts [WORKFLOW.yml...]");
  }
  const files = args.length > 0 ? args : await workflowFiles();
  const problems: string[] = [];
  for (const file of files) {
    const jobs = jobTimeouts(await Deno.readTextFile(file), file);
    const found = timeoutProblems(file, jobs);
    problems.push(...found);
    console.log(
      `${file}: ${jobs.length} jobs, ${
        jobs.length - found.length
      } with a job-level timeout-minutes`,
    );
  }
  for (const p of problems) console.log(`error: ${p}`);
  return problems.length === 0 ? 0 : 1;
}

async function jobHeadroomCommand(args: string[]): Promise<number> {
  const { positional, flags, lists } = parseFlags(args, [
    "workflow-file",
    "branch",
    "runs",
    "repo",
    "record",
    "fixture",
  ], ["allow-few-runs"]);
  if (positional.length !== 0) {
    throw new GateError(
      "usage: job-headroom [--workflow-file FILE] [--branch NAME] [--runs N] [--allow-few-runs JOB_ID]... [--repo OWNER/REPO] [--record FILE] [--fixture FILE]",
    );
  }
  const allowFewRuns = lists["allow-few-runs"] ?? [];
  for (const id of allowFewRuns) {
    if (!/^[A-Za-z_][A-Za-z0-9_-]*$/.test(id)) {
      throw new GateError(
        `--allow-few-runs ${
          JSON.stringify(id)
        }: give a job id, such as quic-transport`,
      );
    }
  }
  const file = flags["workflow-file"] ?? DEFAULT_HEADROOM_WORKFLOW;
  const workflow = file.replace(/^.*[\\/]/, "");
  const runsText = flags.runs ?? String(HEADROOM_RUNS);
  const count = /^[1-9]\d*$/.test(runsText) ? Number(runsText) : NaN;
  if (!(count >= HEADROOM_RUNS && count <= HEADROOM_MAX_RUNS)) {
    throw new GateError(
      `--runs ${runsText}: use a whole number from ${HEADROOM_RUNS} to ${HEADROOM_MAX_RUNS}`,
    );
  }
  const jobs = jobTimeouts(await Deno.readTextFile(file), file);
  let api: Api;
  if (flags.fixture !== undefined) {
    api = new FixtureApi(JSON.parse(await Deno.readTextFile(flags.fixture)));
  } else api = new GhApi();
  const recorder = flags.record !== undefined ? new RecordingApi(api) : null;
  const evidence = await collectHeadroom(
    recorder ?? api,
    flags.repo ?? ":owner/:repo",
    workflow,
    flags.branch ?? "main",
    count,
  );
  if (recorder) {
    await Deno.writeTextFile(
      flags.record,
      JSON.stringify(recorder.seen, null, 2) + "\n",
    );
  }
  const result = evaluateHeadroom(evidence, jobs, {
    file,
    minRuns: HEADROOM_RUNS,
    allowFewRuns,
  });
  console.log(result.lines.join("\n"));
  return result.pass ? 0 : 1;
}

export async function main(args: string[]): Promise<number> {
  const [command, ...rest] = args;
  try {
    switch (command) {
      case "nightly-gate":
        return await nightlyGate(rest);
      case "annotate":
        return annotateCommand(rest);
      case "job-timeouts":
        return await jobTimeoutsCommand(rest);
      case "job-headroom":
        return await jobHeadroomCommand(rest);
      default:
        console.error(
          "usage: ci_gates.ts nightly-gate|annotate|job-timeouts|job-headroom ...",
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
