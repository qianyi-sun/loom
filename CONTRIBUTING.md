# Contributing

> **Current state (2026-06-12):** Loom is public-readiness hardened
> and is operated as an internal-development-first project. Normal
> changes land through PRs into `dev`; `main` remains reserved for
> release promotion from `dev`. `dev` does not currently require human
> review because the project owner is the only active maintainer, but CI
> remains required. External pull requests are not accepted yet. Workflows
> that need publish or deployment secrets must use protected GitHub
> Environments and must not expose secrets to pull request code.

## Active Workflow

### 1. Per-task TDD commit granularity

Each TDD cycle gets its own commit:

```
test(loom.driver): NetworkPolicy enforces allowlist
feat(loom.driver): add NetworkPolicy enforcement
fix(loom.driver): iptables guard for vanilla images
```

The trail documents the build-test-fix progression for later audit.
PR-mode merges squash the per-task commits. The repo forbids
merge commits and rebase merges, so the per-task history stays on the
pre-merge branch and remains recoverable via the PR's "commits" view.

### 2. Commit Style

Concise imperative messages. Prefix with `<scope>:` matching the changed
package or surface:

- `feat(loom.driver): add NetworkPolicy enforcement`
- `feat(loom_service): GET /api/v1/usage rollup`
- `feat(web): SPA scaffold + read pages`
- `fix(loom_drivers.daytona): teardown ordering`
- `docs: update --task id format + --backend fake caveat`
- `chore(ci): bump actions/checkout v4→v5`

Multi-paragraph bodies are encouraged for non-trivial commits — explain
the *why* (not the *what*; diffs show the what). Include a
Co-Authored-By trailer if pair-programming with Claude:

```
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

## Definition Of Done

Before declaring work shipped:

- `ruff check src tests packages` passes
- `mypy --strict` passes
- `pytest tests/unit tests/contract tests/property tests/loom_cli`
  passes (the CI gate runs the same set + sibling packages)
- `pytest tests/integration` passes for Docker-touching changes
  (use `sg docker -c "pytest tests/integration"` if your shell lacks
  the docker group)
- `README.md` / relevant `docs/architecture/*.md` updated if the
  architectural surface changed
- All affected Markdown docs were scanned and updated for code,
  workflow, deployment, runtime, dependency, or contract changes
- **Architecture changes that depend on Docker / k8s / CNI / kernel
  primitive behavior**: add or update a corresponding spike under
  `docs/architecture/<topic>-spikes/` so CI can empirically verify
  the mechanism. See
  [`docs/architecture/cluster-deploy-spikes/README.md`](docs/architecture/cluster-deploy-spikes/README.md)
  for the pattern and "when to add a spike" guidance. The cluster-deploy
  spec went through 11 revs catching mechanisms that didn't compose
  with the underlying primitives — spikes turn that 5-min test into
  a CI gate
- Post-ship self-audit pass (re-read each touched file once more for
  regressions, edge cases, type drift) - separate commit if anything
  surfaces

## Owner-Local Files

`AGENTS.md`, `MEMORY.md`, and `NEW_SESSION_BRIEFING.md` at the repo
root are owner-local context files, gitignored. They are not part of
the project artifact and must never be committed.

## Known Gaps

Tracked as GitHub issues with `label:gap`:
[gap issues](https://github.com/carinrc/loom/issues?q=is%3Aopen+label%3Agap).

Per-arc tracking lives in the arc-tagged epic
([`loom:arc`](https://github.com/carinrc/loom/issues?q=is%3Aopen+label%3A%22loom%3Aarc%22)).
Deferred long-horizon items use
[`deferred:v1.5`](https://github.com/carinrc/loom/issues?q=is%3Aopen+label%3A%22deferred%3Av1.5%22).

## Branching

- `feature/<short-name>` for product or platform features
- `infra/<short-name>` for deployment and infrastructure work
- `docs/<short-name>` for documentation-only changes
- `fix/<short-name>` for defects
- `research/<short-name>` for exploratory prototypes

Feature branches cut from `dev`. PRs target `dev`. `main` is reserved
for release promotion PRs from `dev`.

## Pull Requests

- Link to a GitHub issue with acceptance criteria
- Keep one concern per PR
- Use the PR template
- Wait for CI green before merge; human review is required only on
  branches or environments whose protection rules demand it
- Squash-merge to keep `dev` linear
- Do not add credentials, private endpoints, local environment files, or
  generated run artifacts
- Do not expect publish/deploy secrets to be available in PR workflows
- Do not open external PRs yet; use issues for discussion until the
  external contribution policy is explicitly opened

## Release Flow

1. After dev validation passes for a planned release, open a release
   issue
2. Bump `pyproject.toml [project] version` (root + any published
   `packages/<name>/pyproject.toml`); the GitHub release notes are
   the user-facing changelog (auto-generated from squash-merge PR
   titles between tags)
3. Open a PR from `dev` to `main`
4. After merge, tag the `main` commit `vX.Y.Z` to create the GitHub
   release
