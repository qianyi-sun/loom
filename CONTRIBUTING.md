# Contributing

> **Current state (2026-06-18):** Loom is public-readiness hardened
> and is operated as an issue-scoped GitHub-flow project. Normal
> changes land through PRs into `dev`; `main` remains reserved for
> release promotion from `dev`. `repository-checks` remains the required
> fast CI gate. Normal `dev` PRs use GitHub auto-merge, so GitHub
> squash-merges once required checks and required review state pass.
> External pull requests are accepted for issue-scoped work that follows
> the templates below. Workflows that need publish or deployment secrets
> must use protected GitHub Environments and must not expose secrets to
> pull request code.

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

Deployment environments are separated from branch workflow:
`development` follows `dev` at `dev.yylx.world`, `staging` deploys pinned
`dev` SHAs at `staging.yylx.world`, and `production` follows `main` or
`release-*` tags at `yylx.world`. Production deploys use the protected
GitHub Environment named `production`; normal development jobs must not use
production kubeconfig, database, object-store, provider, SecretStore, or
worker-token secrets.

## Issue Ownership

- Work from an issue with acceptance criteria before opening a PR.
- Comment on the issue before starting substantial work. Maintainers
  mark actively owned issues with a `[WIP] ` title prefix and keep
  assignee + project status current so contributors can avoid duplicate
  work.
- Use `Refs #N` when a PR advances an issue but does not complete the
  whole acceptance scope. Use GitHub auto-close keywords only when the
  PR fully satisfies the issue.

## Pull Requests

- Link to a GitHub issue with acceptance criteria
- Keep one concern per PR
- Use the PR template
- Enable GitHub auto-merge on normal `dev` PRs after opening them.
  GitHub squash-merges when `repository-checks` and any required review
  state pass; do not hand-merge eligible `dev` PRs just because CI is
  green.
- Human review is required only on branches or environments whose
  protection rules demand it, and for external PRs before enabling or
  approving auto-merge
- Squash merge is the only allowed merge method, keeping `dev` linear
- Do not add credentials, private endpoints, local environment files, or
  generated run artifacts
- Do not expect publish/deploy secrets to be available in PR workflows
- External pull requests are accepted for issue-scoped work. If an issue
  is ambiguous, discuss the scope in the issue before implementing.

## Release Flow

1. After dev validation passes for a planned release, open a release
   issue
2. Bump `pyproject.toml [project] version` (root + any published
   `packages/<name>/pyproject.toml`); the GitHub release notes are
   the user-facing changelog (auto-generated from squash-merge PR
   titles between tags)
3. Open a PR from `dev` to `main`
4. Merge the release PR only after the release owner confirms the staging
   evidence; production release PRs do not rely on routine `dev` auto-merge
5. After merge, tag the `main` commit `vX.Y.Z` to create the GitHub
   release
