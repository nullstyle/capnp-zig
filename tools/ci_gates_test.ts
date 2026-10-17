// Tests for tools/ci_gates.ts. No network: the Nightly gate reads API
// responses recorded with `nightly-gate --record` (tools/ci_gates_fixtures/,
// trimmed to the fields the tool reads), and the failure parser reads real
// excerpts of the two failed Nightly logs of 10-13 and 10-15.
//
//   deno test --allow-all --no-config tools/ci_gates_test.ts

import {
  annotate,
  evaluateGate,
  type Evidence,
  failureNames,
  FixtureApi,
  type FlakeRow,
  GateError,
  MAX_NAMED,
  NAME_TITLE,
  OVERFLOW_TITLE,
  parseKnownFlakes,
  rowInForce,
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

Deno.test("the committed known-flakes file parses and has no rows", () => {
  const rows = parseKnownFlakes(
    Deno.readTextFileSync(`${ROOT}/.github/known-flakes.txt`),
  );
  assertEquals(rows.length, 0, "rows");
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
    "a.test.hangs",
    "a.test.it's quoted",
    "a.test.colored",
    "a.test.stamped",
    "full_suite: N healing client(s) gave up",
  ], "names");
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
    `RPC soak (chaos + deadlines, ubuntu-latest) | 38377271133 | nullstyle | 2026-10-13 | 2026-10-27`,
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
  const evidence: Evidence = { sha: BBF, completed: [], inProgress: [] };
  assertThrows(
    () => evaluateGate(evidence, [] as FlakeRow[], "17-10-2026"),
    /--today 17-10-2026/,
  );
});

// ---------------------------------------------------------------------------
// the command line

async function cli(args: string[], env: Record<string, string> = {}) {
  const out = await new Deno.Command(Deno.execPath(), {
    args: [
      "run",
      "--allow-all",
      "--no-config",
      `${TOOLS}/ci_gates.ts`,
      ...args,
    ],
    cwd: ROOT,
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

Deno.test("nightly-gate exits 1 to refuse, 0 to pass and 2 when it cannot evaluate", () =>
  scratch(async (dir) => {
    const bbf = `${FIXTURES}/bbf91e7.json`;
    const flakes = `${dir}/known-flakes.txt`;
    await Deno.writeTextFile(
      flakes,
      rowsText([
        `${EXTENDED} | 38713314813 | nullstyle | 2026-10-15 | 2026-10-29`,
        `${WIN_SOAK} | 38377271133 | nullstyle | 2026-10-13 | 2026-10-27`,
      ]),
    );
    const plan = [
      "--created-before",
      PLAN_WINDOW,
      "--fixture",
      bbf,
      "--flakes",
      flakes,
    ];
    assertEquals(
      (await cli(["nightly-gate", BBF, "--fixture", bbf])).code,
      1,
      "committed rows",
    );
    assertEquals(
      (await cli(["nightly-gate", BBF, ...plan, "--today", "2026-10-17"])).code,
      0,
      "rows in force",
    );
    assertEquals(
      (await cli(["nightly-gate", BBF, ...plan, "--today", "2026-10-30"])).code,
      1,
      "rows expired",
    );
    assertEquals(
      (await cli([
        "nightly-gate",
        D0C,
        "--fixture",
        `${FIXTURES}/d0ca8d6.json`,
      ])).code,
      1,
      "no run",
    );

    await Deno.writeTextFile(
      flakes,
      rowsText([`${EXTENDED} | x | nullstyle | 2026-10-15 | 2026-10-29`]),
    );
    const bad = await cli(["nightly-gate", BBF, ...plan]);
    assertEquals(bad.code, 2, "bad row");
    assert(bad.stderr.includes("known-flakes line 2: the run id"), bad.stderr);
    assertEquals(
      (await cli(["nightly-gate", BBF, "--fixture", `${dir}/missing.json`]))
        .code,
      2,
      "no fixture",
    );
    assertEquals(
      (await cli([
        "nightly-gate",
        BBF,
        "--fixture",
        `${FIXTURES}/d0ca8d6.json`,
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
