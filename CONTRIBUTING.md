# Contributing

> **Current state (2026-06-08):** Loom is single-owner local-development. All
> work happens directly on `dev`; the branch is hundreds of commits ahead of
> `origin/dev` and is pushed when batches are ready. CI runs on every push
> and PR (`.github/workflows/ci.yml`: ruff + mypy --strict + unit/contract/
> property + sibling-package tests gate `dev`; integration suite opt-in via
> the `ci:integration` PR label). There are no live PRs yet and no
> issue-tracker workflow with external contributors. The "Future
> contributor workflow" section at the bottom describes the GitHub-flow
> process we'll switch to when external contributors join — it is not yet
> in force.

## Active Workflow (Single-Owner Mode)

### 1. Plan first, code second

Substantial work happens against a written plan under `docs/plans/` (one file
per implementation plan, dated `YYYY-MM-DD-loom-plan-NN-<short-name>.md`).
Plans follow TDD-by-task structure: each task lists its files, writes a
failing test, runs to confirm fail, implements, runs to confirm pass, and
commits. See any plan under `docs/plans/2026-06-08-loom-plan-2[3-7]-*.md` for
the format.

Specs that motivate plans live under `docs/specs/` (one file per design,
dated `YYYY-MM-DD-loom-<topic>-design.md`).

### 2. Direct-to-dev commits

Work commits directly to `dev`. No feature branches, no PRs, no merge
commits. Backup tags (e.g. `pre-organize-backup-YYYY-MM-DD`) are created
before risky operations (history rewrites, large renames).

### 3. Per-task TDD commit granularity

Within a plan, each TDD cycle gets its own commit:

```
feat(loom.driver): add NetworkPolicy enforcement (Plan 2 Task 8)
test(loom.driver): NetworkPolicy enforces allowlist (Plan 2 Task 8)
fix(loom.driver): iptables guard for vanilla images (Plan 2 Task 9)
```

This is verbose by design — it documents the build-test-fix progression for
later audit. Squashing the per-task commits at plan-shipping time is **not**
done; the trail is the point.

### 4. Per-plan shipping cadence

Each plan ships with a trailing two-commit pair:

```
Plan NN final sweep: ruff/mypy cleanup + CHANGELOG
fix(plan-NN): post-ship audit follow-ups
```

The "final sweep" runs the full quality bar (ruff, mypy strict, full test
suite) and adds the plan's entry to `CHANGELOG.md`. The "post-ship audit"
captures any bugs the immediate self-review finds — kept separate so the
audit pass is documented as a deliberate review step, not folded into the
implementation history.

### 5. Tags follow plan boundaries

Each plan tags its tip:

```
loom-foundation-v0.1            (Plan 1)
loom-driver-trajectory-v0.2     (Plan 2)
...
loom-service-v0.22              (Plan 22)
```

Tags are pushed to dev only; we do not use SemVer `vX.Y.Z` tags on `main`
yet. When SemVer releases start, the version source of truth will be
`pyproject.toml [project] version` (root package + any published
sibling packages under `packages/`), bumped manually per release.

## Commit Style

Concise imperative messages. Prefix with `<scope>:` matching the changed
package or surface:

- `feat(loom.driver): add NetworkPolicy enforcement`
- `feat(loom_service): GET /api/v1/usage rollup`
- `feat(web): SPA scaffold + read pages (Plan 21)`
- `fix(plan-19): post-ship audit follow-ups`
- `docs: harbor parity arc design (Plans 23-27)`
- `chore(legacy): archive pre-Loom build files`

Multi-paragraph bodies are encouraged for non-trivial commits. Include a
Co-Authored-By trailer if pair-programming with Claude:

```
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

## Definition Of Done (for a plan)

Before declaring a plan shipped:

- `ruff check src tests` passes
- `mypy --strict` passes across every Loom source package
- `pytest tests/unit tests/contract` passes
- `pytest tests/integration` passes (some tests require Docker — use
  `sg docker -c "pytest tests/integration"` if the shell doesn't have
  docker group access)
- `CHANGELOG.md` has an `[Unreleased]` entry for the plan
- `docs/plans/<plan>.md` task checkboxes are all checked
- A plan tag (`loom-<scope>-v0.X`) is created at the tip
- Memory updates for substantive changes (see "Documentation Hygiene" below)
- A post-ship self-audit (read each touched file once more, look for
  regressions, edge cases, type drift) — separate commit if it finds anything

## Documentation Hygiene

Every ship sweeps these targets — see
`feedback-update-all-needed.md` in owner memory:

1. `CHANGELOG.md` — `[Unreleased]` entry under Added / Changed / Fixed
2. `docs/plans/<plan>.md` — check off completed tasks
3. `README.md` — only if status, tags, or top-level structure changed
4. `NOTICE.md` — only if licensing or attribution changed
5. In-tree cross-references — `git grep <renamed-path-or-symbol>`

## Owner-Local Files

`AGENTS.md` and `MEMORY.md` at the repo root are owner-local context files,
gitignored. They are not part of the project artifact and must never be
committed.

The deeper memory store at
`~/.claude/projects/-home-hongjian-agentic-data-platform/memory/` lives
outside the repo. Memory updates are part of Documentation Hygiene above
but never end up in git.

## Known Gaps (Open Backlog)

Tracked as GitHub issues with `label:gap`:
[gap issues](https://github.com/carinrc/loom/issues?q=is%3Aopen+label%3Agap).
Highlights as of 2026-06-08:

- **GitHub-flow surface dormant** — PR/issue templates decorative,
  CODEOWNERS unused, no `vX.Y.Z` tags on `main`. Catch-all: issue
  #254 — to activate when external contributors join.

Per-arc tracking lives in the arc-tagged epic
([`loom:arc`](https://github.com/carinrc/loom/issues?q=is%3Aopen+label%3A%22loom%3Aarc%22))
with one sub-issue per plan
([`loom:plan`](https://github.com/carinrc/loom/issues?q=is%3Aopen+label%3A%22loom%3Aplan%22)).
Deferred long-horizon items use
[`deferred:v1.5`](https://github.com/carinrc/loom/issues?q=is%3Aopen+label%3A%22deferred%3Av1.5%22).

## Future Contributor Workflow

When Loom opens to external contributors, the workflow switches to
GitHub flow. This section describes the intended target state.

### Branching

- `feature/<short-name>` for product or platform features
- `infra/<short-name>` for deployment and infrastructure work
- `docs/<short-name>` for documentation-only changes
- `fix/<short-name>` for defects
- `research/<short-name>` for exploratory prototypes

Feature branches cut from `dev`. PRs target `dev`. `main` is reserved for
release promotion PRs from `dev`.

### Pull Requests

- Link to a GitHub issue with acceptance criteria
- Keep one concern per PR
- Use the PR template
- Wait for CI green and required review before merge
- Squash-merge to keep `dev` linear; the per-task TDD trail stays on the
  feature branch's history (recoverable via the PR's "commits" view)

### Release Flow

1. After dev validation passes for a planned release, open a release issue
2. Bump `pyproject.toml [project] version` (root + any published
   `packages/<name>/pyproject.toml`) and update `CHANGELOG.md` (move
   `[Unreleased]` items into a dated section)
3. Open a PR from `dev` to `main`
4. After merge, tag the `main` commit `vX.Y.Z` to create the GitHub release
5. Plan-level tags (`loom-*-v0.X`) continue to mark intermediate dev tips
