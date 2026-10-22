// Tests for tools/ci_gates.ts. No network: the Nightly gate reads API
// responses recorded with `nightly-gate --record` (tools/ci_gates_fixtures/,
// trimmed to the fields the tool reads), and the failure parser reads real
// excerpts of the two failed Nightly logs of 10-13 and 10-15. The job
// headroom gate reads responses recorded with `job-headroom --record`: the
// 10 green main CI runs up to bdc656f, and the branch CI run 39537367357.
//
//   deno test --allow-all --no-config tools/ci_gates_test.ts

import {
  annotate,
  collectHeadroom,
  evaluateGate,
  evaluateHeadroom,
  type Evidence,
  failureNames,
  FixtureApi,
  type FlakeRow,
  GateError,
  HEADROOM_RUNS,
  type HeadroomEvidence,
  headroomRunsPath,
  jobNamePattern,
  jobTimeouts,
  latestJobsPath,
  MAX_NAMED,
  NAME_TITLE,
  normalizeJobName,
  OVERFLOW_TITLE,
  parseKnownFlakes,
  rowInForce,
  timeoutProblems,
} from "./ci_gates.ts";
import { collectEvidence, escapeData } from "./ci_gates.ts";

const TOOLS = import.meta.dirname!;
const ROOT = TOOLS.replace(/[\\/][^\\/]+$/, "");
const FIXTURES = `${TOOLS}/ci_gates_fixtures`;
const REPO = ":owner/:repo";
const BBF = "bbf91e702cce40935eb98d1793622a34f87bab5f";
const D0C = "d0ca8d6e67dd2b40383dbfefca95444935184189";
const EXTENDED = "Extended gates";
const WIN_SOAK = "RPC soak (chaos + deadlines, windows-latest)";
const PLAN_WINDOW = "2026-10-16T00:00:00Z"; // the 3 runs the sprint plan names
const WARM_REDIAL_TEST =
  "rpc_quic_peer_test.test.WarmRedialClient.requestStop never writes into a generation that ended on its own";

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function assertEquals(actual: unknown, expected: unknown, label: string) {
  const [a, b] = [JSON.stringify(actual), JSON.stringify(expected)];
  assert(a === b, `${label}: expected ${b}, got ${a}`);
}

function assertThrows(run: () => unknown, pattern: RegExp) {
  try {
    run();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    assert(error instanceof GateError, `expected a GateError, got ${message}`);
    assert(pattern.test(message), `unexpected error: ${message}`);
    return;
  }
  throw new Error(`expected an error matching ${pattern}`);
}

function fixture(name: string): Record<string, unknown> {
  return JSON.parse(Deno.readTextFileSync(`${FIXTURES}/${name}`));
}

function rowsText(rows: string[]): string {
  return ["# test rows", ...rows].join("\n") + "\n";
}

async function gate(
  responses: Record<string, unknown>,
  sha: string,
  rows: string[],
  today: string,
  createdBefore?: string,
) {
  const evidence = await collectEvidence(new FixtureApi(responses), REPO, sha, {
    createdBefore,
  });
  return {
    evidence,
    result: evaluateGate(evidence, parseKnownFlakes(rowsText(rows)), today),
  };
}

function runsKey(sha: string): string {
  return `repos/${REPO}/actions/workflows/nightly.yml/runs?head_sha=${sha}&per_page=100`;
}

function jobsKey(run: number): string {
  return `repos/${REPO}/actions/runs/${run}/jobs?filter=all&per_page=100`;
}

function annotationsKey(job: number): string {
  return `repos/${REPO}/check-runs/${job}/annotations?per_page=100`;
}

// The id of the recorded failed Extended gates job of 10-15.
const EXTENDED_JOB_1015 = 116349833595;

function withAnnotations(
  names: string[],
  overflow = false,
): Record<string, unknown> {
  const responses = fixture("bbf91e7.json");
  const current = responses[annotationsKey(EXTENDED_JOB_1015)] as unknown[];
  const extra = names.map((message) => ({
    annotation_level: "failure",
    title: NAME_TITLE,
    message,
  }));
  if (overflow) {
    extra.push({
      annotation_level: "failure",
      title: OVERFLOW_TITLE,
      message: "3 more failures",
    });
  }
  responses[annotationsKey(EXTENDED_JOB_1015)] = [...current, ...extra];
  return responses;
}

// ---------------------------------------------------------------------------
// known-flakes rows

// Only the format: the release procedure adds rows to this file, and a test
// that counts them or reads today's date would turn CI red on that commit.
// parseKnownFlakes also refuses a row that expires more than 30 days after
// it was added.
Deno.test("the committed known-flakes file parses", () => {
  parseKnownFlakes(Deno.readTextFileSync(`${ROOT}/.github/known-flakes.txt`));
});

Deno.test("a row has 5 fields and an optional test that can contain a bar", () => {
  const rows = parseKnownFlakes(rowsText([
    `${EXTENDED} | 38713314813 | nullstyle | 2026-10-15 | 2026-10-29`,
    `${WIN_SOAK} | 38377271133 | nullstyle | 2026-10-13 | 2026-11-12 |   `,
    `${EXTENDED} | 1 | owner | 2026-10-01 | 2026-10-01 | suite.test.a | b`,
  ]));
  assertEquals(
    rows.map((r) => r.test),
    [null, null, "suite.test.a | b"],
    "tests",
  );
  assertEquals(rows.map((r) => r.line), [2, 3, 4], "line numbers");
  assertEquals(rows[1].job, WIN_SOAK, "job with a matrix value");
});

Deno.test("a malformed row is refused with its line number", () => {
  const ok = `${EXTENDED} | 1 | owner | 2026-10-01 | 2026-10-02`;
  const cases: [string, RegExp][] = [
    [`${EXTENDED} | 1 | owner | 2026-10-01`, /line 2: expected/],
    [` | 1 | owner | 2026-10-01 | 2026-10-02`, /line 2: the job name is empty/],
    [
      `${EXTENDED} | 12a | owner | 2026-10-01 | 2026-10-02`,
      /line 2: the run id "12a" is not a number/,
    ],
    [
      `${EXTENDED} | 1 |  | 2026-10-01 | 2026-10-02`,
      /line 2: the owner is empty/,
    ],
    [
      `${EXTENDED} | 1 | owner | 2026-02-30 | 2026-03-02`,
      /line 2: the added date "2026-02-30"/,
    ],
    [
      `${EXTENDED} | 1 | owner | 2026-10-01 | 10/02/2026`,
      /line 2: the expiry date "10\/02\/2026"/,
    ],
    [
      `${EXTENDED} | 1 | owner | 2026-10-02 | 2026-10-01`,
      /line 2: the row expires \(2026-10-01\) before/,
    ],
    [
      `${EXTENDED} | 1 | owner | 2026-10-01 | 2026-11-01`,
      /line 2: the row expires 31 days after it was added; the limit is 30/,
    ],
  ];
  for (const [row, pattern] of cases) {
    assertThrows(() => parseKnownFlakes(rowsText([row, ok])), pattern);
  }
  assertEquals(
    parseKnownFlakes(
      rowsText([`${EXTENDED} | 1 | owner | 2026-10-01 | 2026-10-31`]),
    ).length,
    1,
    "30 days",
  );
});

Deno.test("a row is in force from its added date through its expiry date", () => {
  const [row] = parseKnownFlakes(
    rowsText([`${EXTENDED} | 1 | owner | 2026-10-10 | 2026-10-20`]),
  );
  assertEquals(
    ["2026-10-09", "2026-10-10", "2026-10-20", "2026-10-21"].map((d) =>
      rowInForce(row, d)
    ),
    [false, true, true, false],
    "in force",
  );
});

// ---------------------------------------------------------------------------
// failure names

Deno.test("the 10-15 Extended gates log names the failed test", () => {
  const log = Deno.readTextFileSync(
    `${FIXTURES}/38713314813-extended-gates.log`,
  );
  assertEquals(
    failureNames("quic_transport_tests", log),
    [WARM_REDIAL_TEST],
    "names",
  );
});

Deno.test("the 10-13 Windows soak log names the soak failure with numbers as N", () => {
  const log = Deno.readTextFileSync(`${FIXTURES}/38377271133-soak-windows.log`);
  assertEquals(failureNames("soak_64w", log), [
    "soak_64w: steady-state live heap grew N% (> N% threshold)",
  ], "names");
});

Deno.test("every Zig test-failure line gives the test name", () => {
  const log = [
    "error: 'a.test.fails' failed:",
    "error: 'a.test.quiet' failed without output",
    "error: 'a.test.errs' errored",
    "error: 'a.test.leaks' leaked 2 allocations:",
    "error: 'a.test.logs' logged 1 errors:",
    "error: 'a.test.crashes' terminated with signal ABRT with stderr:",
    "error: 'a.test.exits' exited with code 3 with stderr:",
    "error: 'a.test.exits.quietly' exited with code 3",
    "error: 'a.test.stops' stopped with signal STOP",
    "error: 'a.test.hangs' timed out after 10s",
    "error: 'a.test.it's quoted' failed:",
    "\x1b[31merror: 'a.test.colored' failed:\x1b[0m\r",
    "2026-10-15T09:48:54.4388807Z error: 'a.test.stamped' failed:",
    "error: SoakFailed",
    "failed command: ./test --listen=-",
    "  error: 'indented.test' failed:",
    "soak: WARN — host ephemeral-port exhaustion: 36927 dials failed",
    "soak: FAIL -- 3 healing client(s) gave up",
  ].join("\n");
  assertEquals(failureNames("full_suite", log), [
    "a.test.fails",
    "a.test.quiet",
    "a.test.errs",
    "a.test.leaks",
    "a.test.logs",
    "a.test.crashes",
    "a.test.exits",
    "a.test.exits.quietly",
    "a.test.stops",
    "a.test.hangs",
    "a.test.it's quoted",
    "a.test.colored",
    "a.test.stamped",
    "full_suite: N healing client(s) gave up",
  ], "names");
});

Deno.test("a failure with no test name adds step:<id> after the test names", () => {
  const flaky = "error: 'a.test.flaky' failed:";
  const names = (...lines: string[]) =>
    failureNames("full_suite", [flaky, ...lines].join("\n"));
  const both = ["a.test.flaky", "step:full_suite"];
  assertEquals(
    names("error: test process unexpectedly exited with code 1"),
    both,
    "the test process crashed between tests",
  );
  assertEquals(
    names(
      "error: unable to write stdin (BrokenPipe); test process unexpectedly terminated with signal SEGV",
    ),
    both,
    "the test process closed stdin",
  );
  assertEquals(
    names("error: test runner failed to respond for 30s"),
    both,
    "the test runner hangs",
  );
  assertEquals(
    names(
      "error: test 'a.test.fuzz' terminated with signal ABRT; input saved to '.zig-cache/f/1'",
    ),
    both,
    "a fuzz crash",
  );
  assertEquals(
    names("src/rpc/peer.zig:12:5: error: expected type 'u32', found 'bool'"),
    both,
    "a compile error",
  );
  assertEquals(
    names(
      "Build Summary: 16/18 steps succeeded (1 failed); 221/223 tests passed (2 failed)",
    ),
    both,
    "the summary counts a failed test that the log does not name",
  );
  assertEquals(
    names(
      "error: 'a.test.leaks' leaked 1 allocations:",
      "Build Summary: 16/18 steps succeeded (1 failed); 221/223 tests passed (1 skipped, 1 failed)",
    ),
    ["a.test.flaky", "a.test.leaks"],
    "the summary counts only the named failure; a leak is not counted",
  );
  assertEquals(
    names(
      "error: 'a.test.leaks' leaked 1 allocations:",
      "Build Summary: 16/18 steps succeeded (1 failed); 221/223 tests passed (2 failed)",
    ),
    ["a.test.flaky", "a.test.leaks", "step:full_suite"],
    "a leak line does not stand for a failed test of the summary",
  );
  assertEquals(
    names(
      "Build Summary: 16/18 steps succeeded (1 failed); 220/223 tests passed (1 failed, 1 crashed, 1 timed out)",
    ),
    both,
    "crashed and timed-out tests count",
  );
  assertEquals(
    names(
      "error: 'b.test.crash' exited with code 3 with stderr:",
      "       thread 1 panic: reached unreachable code",
      "       /home/runner/work/x.zig:3:5: 0x10 in crash (x.zig)",
      "error: test process unexpectedly exited with code 3",
    ),
    ["a.test.flaky", "b.test.crash", "step:full_suite"],
    "a crashed test, then the test process",
  );
  assertEquals(
    names(
      "       src/indented.zig:1:2: error: in the stderr of a test",
      "error: process exited with code 1",
      "error: the following build command exited with code 1:",
      "error: SoakFailed",
    ),
    ["a.test.flaky"],
    "the build runner's own exit lines are not failures",
  );
});

Deno.test("a failed step with no test or soak line is named step:<id>", () => {
  assertEquals(
    failureNames("fuzz_native_abi", "seed 0x1234: invariant violated\n"),
    ["step:fuzz_native_abi"],
    "log",
  );
  assertEquals(failureNames("prefetch", null), ["step:prefetch"], "no log");
});

// ---------------------------------------------------------------------------
// annotate

function steps(entries: Record<string, [string, string]>): string {
  return JSON.stringify(
    Object.fromEntries(
      Object.entries(entries).map((
        [id, [outcome, conclusion]],
      ) => [id, { outputs: {}, outcome, conclusion }]),
    ),
  );
}

Deno.test("annotate names the failures of the failed steps only", () => {
  const logs: Record<string, string> = {
    full_suite:
      "error: 'a.test.x' failed:\nerror: 'a.test.y' failed:\nerror: 'a.test.x' failed:\n",
    passed: "error: 'not.counted' failed:\n",
    tolerated: "error: 'continue.on.error' failed:\n",
  };
  const result = annotate(
    steps({
      passed: ["success", "success"],
      tolerated: ["failure", "success"],
      full_suite: ["failure", "failure"],
      no_log: ["failure", "failure"],
    }),
    (id) => logs[id] ?? null,
  );
  assertEquals(result.lines, [
    `::error title=${NAME_TITLE}::a.test.x`,
    `::error title=${NAME_TITLE}::a.test.y`,
    `::error title=${NAME_TITLE}::step:no_log`,
  ], "lines");
  assertEquals(result.notes, ["step no_log failed and wrote no log"], "notes");
});

Deno.test("annotate names at most 9 failures and marks the rest as an overflow", () => {
  const log = Array.from(
    { length: 12 },
    (_, i) => `error: 'a.test.${i}' failed:`,
  ).join("\n");
  const result = annotate(
    steps({ full_suite: ["failure", "failure"] }),
    () => log,
  );
  assertEquals(
    result.lines.length,
    MAX_NAMED + 1,
    "annotation count (GitHub keeps 10 errors per step)",
  );
  assertEquals(
    result.lines[MAX_NAMED - 1],
    `::error title=${NAME_TITLE}::a.test.8`,
    "last name",
  );
  assertEquals(
    result.lines[MAX_NAMED],
    `::error title=${OVERFLOW_TITLE}::3 more failures are not named here; see the log`,
    "overflow",
  );
});

Deno.test("annotate escapes workflow-command data and refuses bad input", () => {
  assertEquals(escapeData("100% a\r\nb"), "100%25 a%0D%0Ab", "escape");
  const result = annotate(steps({ ok: ["success", "success"] }), () => null);
  assertEquals(result.lines, [], "no failed step");
  assert(result.notes[0].startsWith("no step with an id failed"), "note");
  assertThrows(
    () => annotate("{not json", () => null),
    /NIGHTLY_STEPS is not JSON/,
  );
  assertThrows(() => annotate("[]", () => null), /not a JSON object/);
});

// ---------------------------------------------------------------------------
// the Nightly gate on recorded responses

Deno.test("bbf91e7 is refused: run 38713314813 failed in Extended gates", async () => {
  const { evidence, result } = await gate(
    fixture("bbf91e7.json"),
    BBF,
    [],
    "2026-10-17",
  );
  assertEquals(evidence.completed.map((r) => r.id), [
    38880648176,
    38713314813,
    38542668305,
  ], "3 most recent");
  assert(!result.pass, "must refuse");
  assertEquals(
    result.lines.filter((l) => l.startsWith("REFUSED")),
    [`REFUSED: run 38713314813, job "${EXTENDED}": no row in force covers the job, with no test`],
    "refusals",
  );
  // The failed run of 10-13 is older than the 3 runs; the gate lists it.
  assertEquals(
    evidence.older.map((r) => [r.id, r.conclusion]),
    [[38377271133, "failure"], [38231937129, "success"], [
      38128471120,
      "success",
    ]],
    "older runs",
  );
  assert(
    result.lines.some((l) =>
      l.startsWith(
        "run 38377271133 (2026-10-13T",
      ) && l.includes("failure, older than the 3 runs above")
    ),
    result.lines.join("\n"),
  );
});

Deno.test("the plan's window of bbf91e7 is refused, passes with 2 rows in force, and is refused when they expire", async () => {
  const rows = [
    `${EXTENDED} | 38713314813 | nullstyle | 2026-10-15 | 2026-10-29`,
    `${WIN_SOAK} | 38377271133 | nullstyle | 2026-10-13 | 2026-10-27`,
  ];
  const plan = (r: string[], today: string) =>
    gate(fixture("bbf91e7.json"), BBF, r, today, PLAN_WINDOW);

  const none = await plan([], "2026-10-17");
  assertEquals(none.evidence.completed.map((r) => r.id), [
    38713314813,
    38542668305,
    38377271133,
  ], "plan window");
  assert(!none.result.pass, "no rows: refuse");
  assertEquals(
    none.result.lines.filter((l) => l.startsWith("REFUSED")).length,
    2,
    "two refusals",
  );

  const both = await plan(rows, "2026-10-17");
  assert(
    both.result.pass,
    `2 rows in force: pass\n${both.result.lines.join("\n")}`,
  );

  const one = await plan(rows.slice(0, 1), "2026-10-17");
  assert(!one.result.pass, "1 row: refuse");

  const lastDay = await plan(rows, "2026-10-27");
  assert(lastDay.result.pass, "both rows are in force on 10-27");

  const expired = await plan(rows, "2026-10-28");
  assert(!expired.result.pass, "the soak row expired: refuse");
  assert(
    expired.result.lines.some((l) =>
      l.includes("known-flakes line 3 (expires 2026-10-27) is not in force")
    ),
    expired.result.lines.join("\n"),
  );
});

Deno.test("a row covers only its own job", async () => {
  const rows = [
    `RPC soak (chaos + deadlines, ubuntu-24.04) | 38377271133 | nullstyle | 2026-10-13 | 2026-10-27`,
    `${EXTENDED} | 38713314813 | nullstyle | 2026-10-15 | 2026-10-29`,
  ];
  const { result } = await gate(
    fixture("bbf91e7.json"),
    BBF,
    rows,
    "2026-10-17",
    PLAN_WINDOW,
  );
  assert(!result.pass, "the ubuntu row must not cover the windows job");
});

Deno.test("a row that names a test does not cover a failure with no annotation", async () => {
  const rows = [
    `${EXTENDED} | 38713314813 | nullstyle | 2026-10-15 | 2026-10-29 | ${WARM_REDIAL_TEST}`,
  ];
  const { result } = await gate(
    fixture("bbf91e7.json"),
    BBF,
    rows,
    "2026-10-17",
  );
  assert(!result.pass, "must refuse");
});

Deno.test("a SHA with no Nightly run is refused", async () => {
  const { result } = await gate(fixture("d0ca8d6.json"), D0C, [], "2026-10-17");
  assert(!result.pass, "must refuse");
  assert(
    result.lines.some((l) =>
      l.startsWith(`REFUSED: 0 completed Nightly runs on ${D0C}`)
    ),
    result.lines.join("\n"),
  );
});

Deno.test("an annotated failure needs a row for each of its tests", async () => {
  const jobRow =
    `${EXTENDED} | 38713314813 | nullstyle | 2026-10-15 | 2026-10-29`;
  const testRow = (t: string) => `${jobRow} | ${t}`;
  const run = (names: string[], rows: string[], overflow = false) =>
    gate(withAnnotations(names, overflow), BBF, rows, "2026-10-17");

  assert(
    (await run([WARM_REDIAL_TEST], [testRow(WARM_REDIAL_TEST)])).result.pass,
    "the test row covers it",
  );
  assert(
    !(await run([WARM_REDIAL_TEST], [jobRow])).result.pass,
    "a row with no test does not cover a named test",
  );
  assert(
    !(await run([WARM_REDIAL_TEST], [testRow("other.test")])).result.pass,
    "another test's row",
  );
  assert(
    !(await run([WARM_REDIAL_TEST, "step:prefetch"], [
      testRow(WARM_REDIAL_TEST),
    ])).result.pass,
    "one of two names is not covered",
  );
  assert(
    (await run([WARM_REDIAL_TEST, "step:prefetch"], [
      testRow(WARM_REDIAL_TEST),
      testRow("step:prefetch"),
    ])).result.pass,
    "both names covered",
  );
  const overflow = await run([WARM_REDIAL_TEST], [
    testRow(WARM_REDIAL_TEST),
    jobRow,
  ], true);
  assert(!overflow.result.pass, "an overflow is never covered");
});

Deno.test("a re-run does not hide a failed attempt", async () => {
  // Run 38542668305 as if attempt 1 failed in Extended gates and a re-run
  // (attempt 2) went green: the run now says success, attempt 2.
  const responses = fixture("bbf91e7.json");
  const runs = (responses[runsKey(BBF)] as {
    workflow_runs: { id: number; run_attempt: number }[];
  }).workflow_runs;
  runs.find((r) => r.id === 38542668305)!.run_attempt = 2;
  const key = jobsKey(38542668305);
  const listing = responses[key] as {
    total_count: number;
    jobs: Record<string, unknown>[];
  };
  for (const job of listing.jobs) job.run_attempt = 2;
  listing.jobs.push({
    id: 1,
    name: EXTENDED,
    status: "completed",
    conclusion: "failure",
    run_attempt: 1,
  });
  listing.total_count += 1;
  responses[annotationsKey(1)] = [];
  const rows = [
    `${EXTENDED} | 38713314813 | nullstyle | 2026-10-15 | 2026-10-29`,
  ];
  const refused = await gate(responses, BBF, [], "2026-10-17");
  assert(
    refused.result.lines.some((l) =>
      l.startsWith(`REFUSED: run 38542668305, job "${EXTENDED}"`)
    ),
    refused.result.lines.join("\n"),
  );
  assert(
    (await gate(responses, BBF, rows, "2026-10-17")).result.pass,
    "the row covers both runs",
  );
});

Deno.test("a cancelled job fails the gate, also when a re-run went green", async () => {
  // Run 38542668305 as if attempt 1 was cancelled in Extended gates and a
  // re-run (attempt 2) went green.
  const responses = fixture("bbf91e7.json");
  const runs = (responses[runsKey(BBF)] as {
    workflow_runs: { id: number; run_attempt: number }[];
  }).workflow_runs;
  runs.find((r) => r.id === 38542668305)!.run_attempt = 2;
  const listing = responses[jobsKey(38542668305)] as {
    total_count: number;
    jobs: Record<string, unknown>[];
  };
  for (const job of listing.jobs) job.run_attempt = 2;
  listing.jobs.push({
    id: 1,
    name: EXTENDED,
    status: "completed",
    conclusion: "cancelled",
    run_attempt: 1,
  });
  listing.total_count += 1;
  responses[annotationsKey(1)] = [];
  const { result } = await gate(responses, BBF, [], "2026-10-17");
  assert(!result.pass, "must refuse");
  assert(
    result.lines.includes(
      `REFUSED: run 38542668305, job "${EXTENDED}": no row in force covers the job, with no test`,
    ),
    result.lines.join("\n"),
  );
  assert(
    result.lines.some((l) => l.includes("(attempt 1): cancelled")),
    result.lines.join("\n"),
  );
});

Deno.test("runs in progress, other commits and failed runs with no failed job", async () => {
  const responses = fixture("bbf91e7.json");
  const listing = responses[runsKey(BBF)] as {
    total_count: number;
    workflow_runs: Record<string, unknown>[];
  };
  const newest = { ...listing.workflow_runs[0] };
  listing.workflow_runs.unshift(
    {
      ...newest,
      id: 2,
      status: "in_progress",
      conclusion: null,
      created_at: "2026-10-17T09:00:00Z",
    },
    {
      ...newest,
      id: 3,
      head_sha: "0".repeat(40),
      created_at: "2026-10-17T08:00:00Z",
    },
  );
  listing.total_count += 2;
  const rows = [
    `${EXTENDED} | 38713314813 | nullstyle | 2026-10-15 | 2026-10-29`,
  ];
  const { evidence, result } = await gate(responses, BBF, rows, "2026-10-17");
  assertEquals(evidence.completed.map((r) => r.id), [
    38880648176,
    38713314813,
    38542668305,
  ], "completed runs");
  assertEquals(evidence.inProgress.map((r) => r.id), [2], "in progress");
  assert(result.pass, result.lines.join("\n"));

  const runs = listing.workflow_runs as { id: number; conclusion: string }[];
  runs.find((r) => r.id === 38880648176)!.conclusion = "failure";
  const odd = await gate(responses, BBF, rows, "2026-10-17");
  assert(
    odd.result.lines.includes(
      "REFUSED: run 38880648176 concluded failure with no failed job",
    ),
    odd.result.lines.join("\n"),
  );
});

Deno.test("fewer than 3 completed runs are refused, and a second page is an error", async () => {
  const responses = fixture("bbf91e7.json");
  const listing = responses[runsKey(BBF)] as {
    total_count: number;
    workflow_runs: { id: number }[];
  };
  listing.workflow_runs = listing.workflow_runs.filter((r) =>
    r.id === 38880648176 || r.id === 38542668305
  );
  listing.total_count = 2;
  const { result } = await gate(responses, BBF, [], "2026-10-17");
  assert(
    result.lines.some((l) =>
      l.startsWith(`REFUSED: 2 completed Nightly runs on ${BBF}`)
    ),
    result.lines.join("\n"),
  );
  listing.total_count = 101;
  let message = "";
  await gate(responses, BBF, [], "2026-10-17").catch((e) =>
    message = e.message
  );
  assert(
    /more than one page/.test(message),
    `expected a page error, got ${message}`,
  );
});

Deno.test("evaluateGate refuses a bad today", () => {
  const evidence: Evidence = {
    sha: BBF,
    completed: [],
    inProgress: [],
    older: [],
  };
  assertThrows(
    () => evaluateGate(evidence, [] as FlakeRow[], "17-10-2026"),
    /--today 17-10-2026/,
  );
});

// ---------------------------------------------------------------------------
// job timeouts

const WORKFLOW = `name: X
on:
  push:
jobs:
  # a comment
  with-timeout:
    name: A
    runs-on: ubuntu-24.04
    timeout-minutes: 30 # measured
    steps:
      - run: echo
  step-timeout-only:
    runs-on: ubuntu-24.04
    steps:
      - name: s
        timeout-minutes: 35
        run: |
          timeout-minutes: 5
  quoted:
    name: "Q #1 (\${{ matrix.os }})" # a comment
    timeout-minutes: "12"
    steps: []
`;

Deno.test("job-timeouts reads only job-level keys", () => {
  const jobs = jobTimeouts(WORKFLOW, "x.yml");
  assertEquals(jobs.map((j) => [j.id, j.timeout]), [["with-timeout", "30"], [
    "step-timeout-only",
    null,
  ], [
    "quoted",
    "12",
  ]], "jobs");
  assertEquals(
    jobs.map((j) => j.name),
    ["A", null, "Q #1 (${{ matrix.os }})"],
    "job-level names only; a step name is not one",
  );
  assertEquals(timeoutProblems("x.yml", jobs), [
    'x.yml:12: job "step-timeout-only" has no job-level timeout-minutes',
  ], "problems");
});

Deno.test("job-timeouts refuses bad values and unknown layouts", () => {
  const value = (v: string) =>
    timeoutProblems(
      "x.yml",
      jobTimeouts(`jobs:\n  a:\n    timeout-minutes: ${v}\n`),
    );
  assertEquals(value("360").length, 0, "360");
  for (const bad of ["0", "361", "abc", "${{ matrix.t }}", "1.5"]) {
    assertEquals(value(bad).length, 1, bad);
  }
  assertThrows(() => jobTimeouts("on: push\n"), /no top-level "jobs:"/);
  assertThrows(() => jobTimeouts("jobs: {a: {}}\n"), /no top-level "jobs:"/);
  assertThrows(() => jobTimeouts("jobs:\n"), /"jobs:" block is empty/);
  assertThrows(
    () => jobTimeouts("jobs:\n  a:\n\ttimeout-minutes: 3\n"),
    /tab in the indentation/,
  );
  assertThrows(
    () => jobTimeouts("jobs:\n  a: {timeout-minutes: 3}\n"),
    /expected a job id/,
  );
  assertThrows(
    () => jobTimeouts("jobs:\n    a:\n      x: 1\n  b:\n"),
    /outside the job layout/,
  );
  assertThrows(
    () => jobTimeouts("jobs:\n  a:\n      x: 1\n    y: 2\n"),
    /between the job and its keys/,
  );
  assertThrows(
    () => jobTimeouts("jobs:\n  a:\n    - x\n"),
    /expected a job key/,
  );
});

// ---------------------------------------------------------------------------
// job headroom

// The 10 green main CI runs up to bdc656f (2026-10-17 to 10-21), recorded
// with `job-headroom --record`. Their Linux jobs carry ubuntu-latest.
const HEADROOM_MAIN = "headroom-ci-main-bdc656f.json";
// The branch CI run 39537367357 (bdc656f, workflow_dispatch), where QUIC
// targeted transport on Windows took 24.8 min.
const HEADROOM_BRANCH = "headroom-ci-branch-39537367357.json";
const BRANCH_RUN = 39537367357;
const NEWEST_MAIN_RUN = 39541793776;
const QUIC_WIN = "QUIC targeted transport (windows-latest)";

interface TimedJob {
  id: number;
  name: string;
  conclusion: string;
  started_at: string;
  completed_at: string;
}

function mainRunIds(responses: Record<string, unknown>): number[] {
  const listing = responses[headroomRunsPath(REPO, "ci.yml", "main", 10)] as {
    workflow_runs: { id: number }[];
  };
  return listing.workflow_runs.map((r) => r.id);
}

function jobsOf(responses: Record<string, unknown>, run: number): TimedJob[] {
  return (responses[latestJobsPath(REPO, run)] as { jobs: TimedJob[] }).jobs;
}

// A workflow with only the quic-transport job, at the given cap. The other
// jobs of the recorded runs match no job of it, so they are not checked.
function quicWorkflow(minutes: number): string {
  return [
    "jobs:",
    "  quic-transport:",
    "    name: QUIC targeted transport (${{ matrix.os }})",
    "    runs-on: ${{ matrix.os }}",
    `    timeout-minutes: ${minutes}`,
    "",
  ].join("\n");
}

// The recorded main runs, with the QUIC Windows job of the newest one given
// the start and end times of the same job in the branch run (24.8 min).
function withBranchPeak(): Record<string, unknown> {
  const responses = fixture(HEADROOM_MAIN);
  const branch = jobsOf(fixture(HEADROOM_BRANCH), BRANCH_RUN).find((j) =>
    j.name === QUIC_WIN
  )!;
  const target = jobsOf(responses, NEWEST_MAIN_RUN).find((j) =>
    j.name === QUIC_WIN
  )!;
  target.started_at = branch.started_at;
  target.completed_at = branch.completed_at;
  return responses;
}

async function headroom(
  responses: Record<string, unknown>,
  workflow: string,
  count = HEADROOM_RUNS,
) {
  const evidence = await collectHeadroom(
    new FixtureApi(responses),
    REPO,
    "ci.yml",
    "main",
    count,
  );
  return {
    evidence,
    result: evaluateHeadroom(evidence, jobTimeouts(workflow, "ci.yml"), {
      file: "ci.yml",
    }),
  };
}

function refusals(lines: string[]): string[] {
  return lines.filter((l) => l.startsWith("REFUSED"));
}

function lineFor(lines: string[], name: string): string {
  const found = lines.filter((l) => l.startsWith(`  ${name}: `));
  assert(found.length === 1, `one line for ${name}:\n${lines.join("\n")}`);
  return found[0];
}

Deno.test("job-headroom reads the green runs and the succeeded jobs in them", async () => {
  const { evidence } = await headroom(fixture(HEADROOM_MAIN), quicWorkflow(40));
  assertEquals(evidence.runs.length, 10, "runs");
  assertEquals(evidence.runs[0].id, NEWEST_MAIN_RUN, "newest first");
  assertEquals(evidence.runs[9].id, 39070979687, "oldest");
  // 30 jobs per run, all succeeded.
  assertEquals(evidence.samples.length, 300, "samples");
  const quic = evidence.samples.filter((s) => s.name === QUIC_WIN);
  assertEquals(quic.length, 10, "one QUIC Windows job per run");
  assertEquals(
    Math.max(...quic.map((s) => s.seconds)),
    1377,
    "QUIC Windows peak: 22 min 57 s",
  );

  // A job that did not succeed, or never started, is not a sample.
  const responses = fixture(HEADROOM_MAIN);
  const [first, second] = jobsOf(responses, NEWEST_MAIN_RUN);
  first.conclusion = "skipped";
  (second as { started_at: string | null }).started_at = null;
  const fewer = await headroom(responses, quicWorkflow(40));
  assertEquals(fewer.evidence.samples.length, 298, "skipped and unstarted");
});

Deno.test("the recorded main runs pass a 40-minute QUIC cap and are refused at 30", async () => {
  const at40 = await headroom(fixture(HEADROOM_MAIN), quicWorkflow(40));
  assert(at40.result.pass, at40.result.lines.join("\n"));
  assertEquals(
    lineFor(at40.result.lines, QUIC_WIN),
    `  ${QUIC_WIN}: peak 23.0 min (run ${NEWEST_MAIN_RUN}), cap 40 min, 57.4%, 10 runs`,
    "QUIC Windows at 40",
  );
  const at30 = await headroom(fixture(HEADROOM_MAIN), quicWorkflow(30));
  assert(!at30.result.pass, "23.0 of 30 min is 76.5%: refuse");
  assertEquals(refusals(at30.result.lines), [
    `REFUSED: ${QUIC_WIN} peaked at 23.0 min in run ${NEWEST_MAIN_RUN}, 76.5% of its 30-min cap; the limit is 75% (22.5 min)`,
  ], "refusals");
  assert(
    lineFor(at30.result.lines, QUIC_WIN).endsWith(": OVER 75%"),
    at30.result.lines.join("\n"),
  );
});

Deno.test("a 24.8-minute peak is refused at a 30-minute cap and passes at 40", async () => {
  const at30 = await headroom(withBranchPeak(), quicWorkflow(30));
  assert(!at30.result.pass, at30.result.lines.join("\n"));
  assertEquals(refusals(at30.result.lines), [
    `REFUSED: ${QUIC_WIN} peaked at 24.8 min in run ${NEWEST_MAIN_RUN}, 82.5% of its 30-min cap; the limit is 75% (22.5 min)`,
  ], "refusals");
  const at40 = await headroom(withBranchPeak(), quicWorkflow(40));
  assert(at40.result.pass, at40.result.lines.join("\n"));
  assert(
    lineFor(at40.result.lines, QUIC_WIN).includes(
      "peak 24.8 min (run 39541793776), cap 40 min, 61.9%",
    ),
    at40.result.lines.join("\n"),
  );
});

Deno.test("a peak at exactly 75% of the cap passes, and one second more is refused", () => {
  const evidence = (seconds: number): HeadroomEvidence => ({
    workflow: "ci.yml",
    branch: "main",
    runs: Array.from({ length: 10 }, (_, i) => ({
      id: i + 1,
      sha: "0".repeat(40),
      event: "push",
      createdAt: `2026-10-${String(10 + i).padStart(2, "0")}T00:00:00Z`,
      attempt: 1,
    })),
    samples: [{ run: 1, job: 1, name: "J", seconds }],
  });
  const jobs = jobTimeouts(
    "jobs:\n  j:\n    name: J\n    timeout-minutes: 20\n",
  );
  assert(evaluateHeadroom(evidence(900), jobs).pass, "15 of 20 min");
  assert(!evaluateHeadroom(evidence(901), jobs).pass, "15 min 1 s of 20 min");
});

Deno.test("the history of a job carries across the ubuntu-latest to ubuntu-24.04 rename", async () => {
  assertEquals(
    [
      "Test (ubuntu-latest)",
      "RPC soak (chaos + deadlines, ubuntu-latest)",
      "Test (ubuntu-latest-arm)",
      "Test (ubuntu-24.04)",
      "ubuntu-latest",
    ].map((n) => normalizeJobName(n)),
    [
      "Test (ubuntu-24.04)",
      "RPC soak (chaos + deadlines, ubuntu-24.04)",
      "Test (ubuntu-latest-arm)",
      "Test (ubuntu-24.04)",
      "ubuntu-24.04",
    ],
    "names",
  );

  // Half of the runs as if they ran after the rename.
  const responses = fixture(HEADROOM_MAIN);
  for (const run of mainRunIds(responses).slice(0, 5)) {
    for (const job of jobsOf(responses, run)) {
      job.name = job.name.replace("ubuntu-latest", "ubuntu-24.04");
    }
  }
  const ci = await Deno.readTextFile(`${ROOT}/.github/workflows/ci.yml`);
  const { result } = await headroom(responses, ci);
  const text = result.lines.join("\n");
  assert(!text.includes("ubuntu-latest)"), text);
  // The 5 Linux matrix legs of the committed ci.yml (Test, QUIC targeted
  // transport, Fd passing compiled out, Hardening gate, ReleaseSafe
  // hardening tests) each have all 10 runs. A leg that matched no job of
  // ci.yml would add a note line here, which does not end that way.
  const linux = result.lines.filter((l) => l.includes("(ubuntu-24.04)"));
  assertEquals(linux.length, 5, `Linux legs:\n${text}`);
  assert(linux.every((l) => l.endsWith(", 10 runs")), text);
  assert(
    lineFor(result.lines, "Test (ubuntu-24.04)").endsWith(", 10 runs"),
    text,
  );
});

Deno.test("job-headroom lists the jobs it cannot check, refuses too few runs and refuses ambiguous names", async () => {
  const workflow = [
    "jobs:",
    "  quic-transport:",
    "    name: QUIC targeted transport (${{ matrix.os }})",
    "    timeout-minutes: 40",
    "  new-job:",
    "    timeout-minutes: 10",
    "",
  ].join("\n");
  const { result } = await headroom(fixture(HEADROOM_MAIN), workflow);
  assert(result.pass, result.lines.join("\n"));
  assert(
    result.lines.includes(
      'note: job "Test (macos-latest)" matches no job of ci.yml (10 runs); it is not checked',
    ),
    result.lines.join("\n"),
  );
  assert(
    result.lines.includes(
      'note: job "new-job" (ci.yml:5) has no successful run in these runs; it is not checked',
    ),
    result.lines.join("\n"),
  );

  // Fewer green runs than the gate needs.
  const responses = fixture(HEADROOM_MAIN);
  const key = headroomRunsPath(REPO, "ci.yml", "main", 10);
  const listing = responses[key] as { workflow_runs: unknown[] };
  listing.workflow_runs = listing.workflow_runs.slice(0, 9);
  const few = await headroom(responses, quicWorkflow(40));
  assert(!few.result.pass, "9 runs");
  assertEquals(refusals(few.result.lines), [
    "REFUSED: 9 green runs of ci.yml on main; the gate needs 10",
  ], "too few runs");

  // A job with no usable cap.
  const noCap = await headroom(
    fixture(HEADROOM_MAIN),
    "jobs:\n  quic-transport:\n    name: QUIC targeted transport (${{ matrix.os }})\n",
  );
  assert(!noCap.result.pass, "no cap");
  assert(
    refusals(noCap.result.lines).every((l) =>
      l.includes("has no job-level timeout-minutes from 1 to 360")
    ),
    noCap.result.lines.join("\n"),
  );

  // Two templates that match the same name.
  const twice = quicWorkflow(40) +
    "  other:\n    name: QUIC ${{ matrix.x }}\n    timeout-minutes: 40\n";
  let message = "";
  await headroom(fixture(HEADROOM_MAIN), twice).catch((e) =>
    message = e.message
  );
  assert(/matches 2 jobs of ci\.yml/.test(message), `got ${message}`);
});

Deno.test("a job name maps to its job through the name template", () => {
  const [templated, bare, literal] = jobTimeouts(
    [
      "jobs:",
      "  quic-transport:",
      "    name: QUIC targeted transport (${{ matrix.os }})",
      "  bare:",
      "    runs-on: x",
      "  literal:",
      "    name: Format check (fast)",
      "",
    ].join("\n"),
  );
  const t = jobNamePattern(templated);
  assert(t.test("QUIC targeted transport (windows-latest)"), "matrix value");
  assert(!t.test("QUIC targeted transport"), "no matrix value");
  assert(!t.test("xQUIC targeted transport (a)"), "anchored");
  const b = jobNamePattern(bare);
  assert(b.test("bare") && b.test("bare (a, b)"), "a job with no name");
  assert(!b.test("bare2"), "another job id");
  const l = jobNamePattern(literal);
  assert(
    l.test("Format check (fast)") && !l.test("Format check (x)"),
    "literal",
  );
});

// ---------------------------------------------------------------------------
// the command line

async function cli(
  args: string[],
  env: Record<string, string> = {},
  cwd = ROOT,
) {
  const out = await new Deno.Command(Deno.execPath(), {
    args: [
      "run",
      "--allow-all",
      "--no-config",
      `${TOOLS}/ci_gates.ts`,
      ...args,
    ],
    cwd,
    env,
    stdout: "piped",
    stderr: "piped",
  }).output();
  const text = new TextDecoder();
  return {
    code: out.code,
    stdout: text.decode(out.stdout),
    stderr: text.decode(out.stderr),
  };
}

async function scratch(fn: (dir: string) => Promise<void>) {
  const dir = await Deno.makeTempDir({ prefix: "ci_gates_test" });
  try {
    await fn(dir);
  } finally {
    await Deno.remove(dir, { recursive: true });
  }
}

// Every case names its rows file, so no case depends on the rows that are
// committed in .github/known-flakes.txt or on today's date.
Deno.test("nightly-gate exits 1 to refuse, 0 to pass and 2 when it cannot evaluate", () =>
  scratch(async (dir) => {
    const bbf = `${FIXTURES}/bbf91e7.json`;
    const empty = `${dir}/empty-flakes.txt`;
    await Deno.writeTextFile(empty, rowsText([]));
    const goodRows = rowsText([
      `${EXTENDED} | 38713314813 | nullstyle | 2026-10-15 | 2026-10-29`,
      `${WIN_SOAK} | 38377271133 | nullstyle | 2026-10-13 | 2026-10-27`,
    ]);
    const flakes = `${dir}/known-flakes.txt`;
    await Deno.writeTextFile(flakes, goodRows);
    const window = ["--created-before", PLAN_WINDOW, "--fixture", bbf];
    const plan = [...window, "--flakes", flakes];
    const today = ["--today", "2026-10-17"];
    assertEquals(
      (await cli(["nightly-gate", BBF, "--fixture", bbf, "--flakes", empty]))
        .code,
      1,
      "no rows",
    );
    assertEquals(
      (await cli(["nightly-gate", BBF, ...plan, ...today])).code,
      0,
      "rows in force",
    );
    assertEquals(
      (await cli(["nightly-gate", BBF, ...plan, "--today", "2026-10-30"])).code,
      1,
      "rows expired",
    );

    // Without --flakes the gate reads .github/known-flakes.txt in the
    // current directory.
    const repo = `${dir}/repo`;
    await Deno.mkdir(`${repo}/.github`, { recursive: true });
    await Deno.writeTextFile(`${repo}/.github/known-flakes.txt`, goodRows);
    const byDefault = await cli(
      ["nightly-gate", BBF, ...window, ...today],
      {},
      repo,
    );
    assertEquals(byDefault.code, 0, "default rows file");
    assert(
      byDefault.stdout.includes("known-flakes: 2 rows in force"),
      byDefault.stdout,
    );

    assertEquals(
      (await cli([
        "nightly-gate",
        D0C,
        "--fixture",
        `${FIXTURES}/d0ca8d6.json`,
        "--flakes",
        flakes,
        ...today,
      ])).code,
      1,
      "no run",
    );

    const badFlakes = `${dir}/bad-flakes.txt`;
    await Deno.writeTextFile(
      badFlakes,
      rowsText([`${EXTENDED} | x | nullstyle | 2026-10-15 | 2026-10-29`]),
    );
    const bad = await cli([
      "nightly-gate",
      BBF,
      ...window,
      "--flakes",
      badFlakes,
    ]);
    assertEquals(bad.code, 2, "bad row");
    assert(bad.stderr.includes("known-flakes line 2: the run id"), bad.stderr);
    assertEquals(
      (await cli([
        "nightly-gate",
        BBF,
        "--fixture",
        `${dir}/missing.json`,
        "--flakes",
        empty,
      ])).code,
      2,
      "no fixture",
    );
    assertEquals(
      (await cli([
        "nightly-gate",
        BBF,
        "--fixture",
        `${FIXTURES}/d0ca8d6.json`,
        "--flakes",
        empty,
      ])).code,
      2,
      "no response",
    );
    assertEquals(
      (await cli(["nightly-gate", BBF, "--bogus", "1"])).code,
      2,
      "unknown option",
    );
    assertEquals((await cli(["nightly-gate"])).code, 2, "no sha");
  }));

Deno.test("annotate prints the workflow commands for the failed steps", () =>
  scratch(async (dir) => {
    await Deno.copyFile(
      `${FIXTURES}/38377271133-soak-windows.log`,
      `${dir}/soak_64w.log`,
    );
    const out = await cli(["annotate", "--log-dir", dir], {
      NIGHTLY_STEPS: steps({
        widen_ports: ["failure", "success"],
        soak_64w: ["failure", "failure"],
      }),
    });
    assertEquals(out.code, 0, "exit");
    // %25 is the workflow-command escape of "%"; the annotation says "%".
    assertEquals(out.stdout.trim().split("\n"), [
      `::error title=${NAME_TITLE}::soak_64w: steady-state live heap grew N%25 (> N%25 threshold)`,
    ], "stdout");
  }));

Deno.test("annotate --expect checks the wiring and prints no workflow command", () =>
  scratch(async (dir) => {
    await Deno.writeTextFile(
      `${dir}/log_wiring.log`,
      "error: 'ci_gates.wiring' failed:\n",
    );
    const env = {
      NIGHTLY_STEPS: steps({ log_wiring: ["failure", "failure"] }),
    };
    const found = await cli([
      "annotate",
      "--log-dir",
      dir,
      "--expect",
      "ci_gates.wiring",
    ], env);
    assertEquals(found.code, 0, `found:\n${found.stdout}`);
    assert(!found.stdout.includes("::"), found.stdout);
    const wrongId = await cli([
      "annotate",
      "--log-dir",
      dir,
      "--expect",
      "ci_gates.wiring",
    ], {
      NIGHTLY_STEPS: steps({ other_id: ["failure", "failure"] }),
    });
    assertEquals(
      wrongId.code,
      1,
      `a log under another step id:\n${wrongId.stdout}`,
    );
    assert(!wrongId.stdout.includes("::"), wrongId.stdout);
  }));

Deno.test("job-timeouts passes on the workflows and fails when one job loses its timeout", () =>
  scratch(async (dir) => {
    const files = ["ci.yml", "nightly.yml", "release.yml"].map((f) =>
      `.github/workflows/${f}`
    );
    const ok = await cli(["job-timeouts", ...files]);
    assertEquals(ok.code, 0, `committed workflows:\n${ok.stdout}`);

    // With no file it checks every workflow in .github/workflows.
    const all = await cli(["job-timeouts"]);
    assertEquals(all.code, 0, `all workflows:\n${all.stdout}`);
    for (const f of files) {
      assert(all.stdout.includes(`${f}: `), `${f} is checked:\n${all.stdout}`);
    }
    const repo = `${dir}/repo`;
    await Deno.mkdir(`${repo}/.github/workflows`, { recursive: true });
    await Deno.writeTextFile(
      `${repo}/.github/workflows/a.yml`,
      "jobs:\n  a:\n    timeout-minutes: 5\n",
    );
    await Deno.writeTextFile(
      `${repo}/.github/workflows/new.yaml`,
      "jobs:\n  b:\n    runs-on: ubuntu-24.04\n",
    );
    await Deno.writeTextFile(`${repo}/.github/workflows/README`, "not yaml\n");
    const added = await cli(["job-timeouts"], {}, repo);
    assertEquals(added.code, 1, `a new workflow:\n${added.stdout}`);
    assert(
      added.stdout.includes(
        '.github/workflows/new.yaml:2: job "b" has no job-level timeout-minutes',
      ),
      added.stdout,
    );
    await Deno.mkdir(`${dir}/empty/.github/workflows`, { recursive: true });
    assertEquals(
      (await cli(["job-timeouts"], {}, `${dir}/empty`)).code,
      2,
      "no workflow file",
    );

    // The Test job keeps a step-level timeout-minutes, so a count of the
    // key in the file still sees one there. The check must not.
    const ci = await Deno.readTextFile(`${ROOT}/.github/workflows/ci.yml`);
    const testJob = ci.indexOf("\n  test:\n");
    assert(testJob > 0, "ci.yml has a test job");
    const jobLevel = ci.indexOf("\n    timeout-minutes:", testJob);
    const lineEnd = ci.indexOf("\n", jobLevel + 1);
    const ablated = ci.slice(0, jobLevel) + ci.slice(lineEnd);
    assert(
      ablated.slice(testJob).includes("timeout-minutes: 35"),
      "the step-level timeout stays",
    );
    await Deno.writeTextFile(`${dir}/ci.yml`, ablated);
    const red = await cli(["job-timeouts", `${dir}/ci.yml`]);
    assertEquals(red.code, 1, `ablated:\n${red.stdout}`);
    assert(
      red.stdout.includes('job "test" has no job-level timeout-minutes'),
      red.stdout,
    );
  }));

Deno.test("job-headroom exits 1 on a 24.8-minute peak at a 30-minute cap, 0 at 40, and 2 when it cannot evaluate", () =>
  scratch(async (dir) => {
    const responses = `${dir}/responses.json`;
    await Deno.writeTextFile(responses, JSON.stringify(withBranchPeak()));
    const at30 = `${dir}/ci.yml`;
    await Deno.writeTextFile(at30, quicWorkflow(30));
    const at40 = `${dir}/at40/ci.yml`;
    await Deno.mkdir(`${dir}/at40`);
    await Deno.writeTextFile(at40, quicWorkflow(40));
    const args = (file: string) => [
      "job-headroom",
      "--fixture",
      responses,
      "--workflow-file",
      file,
    ];

    const red = await cli(args(at30));
    assertEquals(red.code, 1, `cap 30:\n${red.stdout}${red.stderr}`);
    assert(
      red.stdout.includes(`REFUSED: ${QUIC_WIN} peaked at 24.8 min`),
      red.stdout,
    );
    const green = await cli(args(at40));
    assertEquals(green.code, 0, `cap 40:\n${green.stdout}${green.stderr}`);
    assert(green.stdout.includes("PASS: "), green.stdout);

    // The workflow name in the API path is the file name.
    const other = `${dir}/nightly.yml`;
    await Deno.writeTextFile(other, quicWorkflow(40));
    assertEquals(
      (await cli(args(other))).code,
      2,
      "no response for nightly.yml",
    );
    assertEquals(
      (await cli([...args(at40), "--runs", "9"])).code,
      2,
      "fewer than 10 runs asked for",
    );
    assertEquals(
      (await cli([...args(at40), "--runs", "11"])).code,
      2,
      "no response for 11 runs",
    );
    assertEquals(
      (await cli([...args(at40), "extra"])).code,
      2,
      "a positional argument",
    );
  }));
