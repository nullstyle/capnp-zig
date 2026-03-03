# Repository Guidelines

## Project Structure & Module Organization
- `src/` holds the Zig library and plugin entry point.
- `src/serialization/` contains wire-format, schema, and reader/validation modules.
- `src/capnpc-zig/` contains codegen utilities and generators.
- `src/rpc/level0`, `src/rpc/level1`, `src/rpc/level2`, and `src/rpc/level3` group RPC runtime modules by Cap'n Proto level.
- `src/rpc/level1/` contains promise and pipelining primitives shared by higher-level peer flows.
- `tests/serialization/` contains serialization-focused suites; `tests/rpc/level0..level3/` contain RPC suites by level.
- `tests/` also contains support assets; fixture schemas live in `tests/test_schemas/`.
- `build.zig` defines build/test steps; `Justfile` wraps common tasks.
- `zig-out/` and `.zig-cache/` are build artifacts.

## Build, Test, and Development Commands
- `just build` builds the project (`zig build`).
- `just release` builds optimized (`zig build -Doptimize=ReleaseSafe`).
- `just test` runs all tests with summary output (`zig build test --summary all`).
- `just test-serialization` runs serialization-focused suites.
- `just test-rpc`, `just test-rpc-level0`, `just test-rpc-level1`, `just test-rpc-level2`, `just test-rpc-level3` run RPC suites by level.
- `zig build test-message`, `zig build test-codegen`, `zig build test-integration`, `zig build test-real-world`, `zig build test-union` run focused suites.
- `zig build test-serialization` runs all non-RPC serialization suites.
- `zig build test-rpc-level0`, `zig build test-rpc-level1`, `zig build test-rpc-level2`, `zig build test-rpc-level3` run cumulative RPC levels.
- `zig build test-capnp-testdata` and `zig build test-capnp-test-vendor` run Cap’n Proto fixture suites.
- `just fmt` formats `src/` and `tests/` (`zig fmt`).
- `just check` compiles without linking (`zig build check`).
- `just example` runs Cap’n Proto compilation using the local plugin (requires `capnp` and `just build`).
- `just install` copies `zig-out/bin/capnpc-zig` to `~/.local/bin/`.
- `zig build bench-ping-pong -- --iters 10000 --payload 1024` runs the ping-pong benchmark.
- `zig build example-rpc` runs the RPC ping-pong example (`examples/rpc_pingpong.zig`).

## Coding Style & Naming Conventions
- Format with `zig fmt`; don’t hand-format.
- Indentation follows Zig defaults (4 spaces, no tabs).
- Types use `UpperCamelCase`, functions/vars use `lowerCamelCase`.
- Files are `snake_case.zig` (examples: `message.zig`, `integration_test.zig`).

## Testing Guidelines
- Tests use Zig’s built-in `test` blocks in `tests/**/*.zig`.
- Name new test files `*_test.zig` and group by area (`tests/serialization` or `tests/rpc/level0..level3`).
- Run `just test` before submitting changes; add targeted tests for new behavior.

## Commit & Pull Request Guidelines
- This repository has no commits yet, so history-based conventions aren't visible.
- Use concise, imperative commit summaries (optionally scoped), e.g. `message: handle empty segments`.
- PRs should include a clear summary, the commands you ran, and any schema samples if codegen behavior changes.

## Tooling & Configuration
- Target Zig `0.16`. `capnp`, `just`, and `mise` are optional but recommended for local workflows.
Use 'bd' for task tracking

<!-- BEGIN BEADS INTEGRATION -->
## Issue Tracking with bd (beads)

**IMPORTANT**: This project uses **bd (beads)** for ALL issue tracking. Do NOT use markdown TODOs, task lists, or other tracking methods.

### Why bd?

- Dependency-aware: Track blockers and relationships between issues
- Git-friendly: Dolt-powered version control with native sync
- Agent-optimized: JSON output, ready work detection, discovered-from links
- Prevents duplicate tracking systems and confusion

### Quick Start

**Check for ready work:**

```bash
bd ready --json
```

**Create new issues:**

```bash
bd create "Issue title" --description="Detailed context" -t bug|feature|task -p 0-4 --json
bd create "Issue title" --description="What this issue is about" -p 1 --deps discovered-from:bd-123 --json
```

**Claim and update:**

```bash
bd update <id> --claim --json
bd update bd-42 --priority 1 --json
```

**Complete work:**

```bash
bd close bd-42 --reason "Completed" --json
```

### Issue Types

- `bug` - Something broken
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature with subtasks
- `chore` - Maintenance (dependencies, tooling)

### Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (default, nice-to-have)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

### Workflow for AI Agents

1. **Check ready work**: `bd ready` shows unblocked issues
2. **Claim your task atomically**: `bd update <id> --claim`
3. **Work on it**: Implement, test, document
4. **Discover new work?** Create linked issue:
   - `bd create "Found bug" --description="Details about what was found" -p 1 --deps discovered-from:<parent-id>`
5. **Complete**: `bd close <id> --reason "Done"`

### Auto-Sync

bd automatically syncs via Dolt:

- Each write auto-commits to Dolt history
- Use `bd dolt push`/`bd dolt pull` for remote sync
- No manual export/import needed!

### Important Rules

- ✅ Use bd for ALL task tracking
- ✅ Always use `--json` flag for programmatic use
- ✅ Link discovered work with `discovered-from` dependencies
- ✅ Check `bd ready` before asking "what should I work on?"
- ❌ Do NOT create markdown TODO lists
- ❌ Do NOT use external issue trackers
- ❌ Do NOT duplicate tracking systems

For more details, see README.md and docs/QUICKSTART.md.

<!-- END BEADS INTEGRATION -->

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
