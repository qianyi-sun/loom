# Contributing

> **Canonical repository:** use
> [`qianyi-sun/loom`](https://github.com/qianyi-sun/loom) for new branches,
> pull requests, and issues.

> Loom is public-readiness hardened and is operated as an issue-scoped
> GitHub-flow project. Normal
> changes land through PRs into `dev`; `main` remains reserved for
> release promotion from `dev`. Every relevant non-draft PR reports the four
> stable validation contexts: `repository-checks`,
> `images-gate`, `cluster-smoke-gate`, and
> `staging-smoke-gate`. The shared planner selects the applicable validation
> work from changed paths, while labels can request additional validation.
> Labels may add validation but cannot remove path-inferred validation. The
> trusted base-branch controller enables GitHub squash auto-merge for every
> non-draft PR, independent of author or reviewer identity. GitHub waits for
> every required gate on that current head SHA before merging.
> Static documentation is a location-and-format allowlist; unknown runtime
> paths fail safe to the full validation set. Manual runs report `*-manual`
> contexts and cannot satisfy protected PR contexts. These four strict,
> GitHub-Actions-app-bound current-head checks are the only merge authority for
> `dev`: it requires no human approval, no CODEOWNER approval, and no
> conversation resolution. CI or release-authority changes select full CI but
> add no human merge gate.
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
- `fix(loom_drivers.modal): teardown ordering`
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
  primitive behavior**: add or update focused automated coverage under the
  owning test lane so CI empirically verifies the mechanism
- Post-ship self-audit pass (re-read each touched file once more for
  regressions, edge cases, type drift) - separate commit if anything
  surfaces

## Owner-Local Files

`AGENTS.md`, `MEMORY.md`, and `NEW_SESSION_BRIEFING.md` at the repo
root are owner-local context files, gitignored. They are not part of
the project artifact and must never be committed.

## Branching

- `feature/<short-name>` for product or platform features
- `infra/<short-name>` for deployment and infrastructure work
- `docs/<short-name>` for documentation-only changes
- `fix/<short-name>` for defects
- `research/<short-name>` for exploratory prototypes

Feature branches cut from `dev`. PRs target `dev`. `main` is reserved
for release promotion PRs from `dev`.

Deployment environments are separated from branch workflow: `development`
uses `https://yylx.world/dev`, `staging` uses
`https://yylx.world/staging`, and `production` follows `main` or
immutable `vX.Y.Z` production release tags at `https://yylx.world/prod`. Do
not use environment-specific yylx frontend subdomains as entrypoints; they are
not provisioned. Production deploys use the protected GitHub
Environment named `production`; normal development jobs must not use production
kubeconfig, database, object-store, provider, SecretStore, or worker-token
secrets.

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
- The four stable gate contexts are `repository-checks`, `images-gate`,
  `cluster-smoke-gate`, and `staging-smoke-gate`. The shared planner selects
  their validation work from changed paths; labels request additional work but
  cannot turn off path-inferred work. A new non-document path without a known
  owner selects every heavy lane until its ownership is declared.
- The trusted base-branch controller enables GitHub squash auto-merge for every
  non-draft `dev` PR, regardless of author or reviewer. It remains queued until
  every required gate is visible and successful on the current head SHA. The
  four strict, app-bound checks are the
  only merge authority: `dev` requires no human approval, no CODEOWNER
  approval, and no conversation resolution. Do not hand-merge an eligible
  `dev` PR just because CI is green.
- Do not assume labels are the only way to select validation. For example,
  relevant image paths select `images-gate` automatically; `ci:images` adds
  multi-arch image validation when the changed paths do not already require it.
  In the checked-in workflow, pull requests, merge groups, and manual
  dispatches build with a read-only token, do not log in to GHCR, and do not
  use a shared publication cache. Ordinary manual dispatch is build-only. The
  `publish` jobs request `packages: write` only for a push to `dev`/`main`, or
  for the exact protected-head reconciliation dispatched by the checked-in
  `trusted-image-release-controller` as `github-actions[bot]`. The controller
  closes GitHub's intentional suppression of workflows caused by an earlier
  workflow `GITHUB_TOKEN`; it selects the range from the nearest successful
  trusted release ancestor and deduplicates active, successful, and failed
  heads. It cannot select PR code or an arbitrary commit. This is not a repository-wide sandbox for
  same-repository writers: branch workflow code still runs from the PR branch.
  Autonomous agents need a fork-only
  execution boundary or an external trusted workflow/App before this can be
  treated as a hard token ceiling.
- `staging-smoke-gate` is a credential-free cluster validation lane. It does not
  request the `ci-aws` environment and does not treat a skipped real-AWS check
  as successful cloud evidence. Real AWS validation is a separate trusted,
  post-merge/release activity and cannot satisfy this protected PR context.
- For `main`, the active `main protected promotion` ruleset accepts only a
  same-repository, current-head `dev` production candidate. Its sole required
  context is the direct `main-promotion-gate` job, which verifies the open PR,
  current `dev` head, successful `release-promotion-gate` run, and unexpired
  evidence artifact all identify the same SHA. The four heavy gates remain the
  protected admission authority for changes entering `dev`.
- Changes to `.github/**`, CI validation-selection policy, and production release
  verification select full CI. Rollout authority, deployment, and release-gate
  changes also select every heavy lane; `cluster-smoke-gate` renders and audits
  both staging and production without credentials. Manual dispatches use
  distinct `*-manual` check names and cannot satisfy branch protection.
- Squash merge is the only allowed merge method, keeping `dev` linear
- Do not add credentials, private endpoints, local environment files, or
  generated run artifacts
- Do not expect publish/deploy secrets to be available in PR workflows
- External pull requests are accepted for issue-scoped work. If an issue
  is ambiguous, discuss the scope in the issue before implementing.

## Release Flow

1. Let changes enter `dev` through its four protected heavy gates, then select
   the exact stable current `dev` SHA.
2. Open a release issue and bump
   `pyproject.toml [project] version` (root + any published
   `packages/<name>/pyproject.toml`); the GitHub release notes are
   the user-facing changelog (auto-generated from squash-merge PR
   titles between tags)
3. Choose the new immutable SemVer prod
   tag (`vX.Y.Z`), deploy that image tag to staging, and
   run `.github/workflows/release-promotion-gate.yml` with the structured
   release evidence manifest. Its `release_owner_approval` and Production
   Environment approval are distinct controls and are not interchangeable with
   CI merge authority.
4. While `dev` still points to that exact candidate, open the same-repository
   `dev` to `main` PR. Dispatch `.github/workflows/main-promotion-gate.yml`
   from `dev` with the candidate SHA, PR number, and successful release-gate
   run ID. If `dev` advances, select and validate the new head instead of
   reusing stale evidence.
5. Enable GitHub's native squash auto-merge after `main-promotion-gate` passes.
   The `main` ruleset requires only that composite gate; direct pushes,
   deletion, force-pushes, and bypasses remain prohibited.
6. Deploy production from `main` with `candidate_sha`, `image_tag`, and
   `release_gate_run_id`. The production deployment workflow rejects missing
   or mismatched gate evidence before it can use production environment
   secrets.
7. Tag the promoted `main` commit with the recorded `vX.Y.Z` tag to create
   the GitHub release. Never force-move or reuse a published prod tag; rollback
   deploys the previous recorded tag or image digest instead.

## Repository Hardening (maintainers)

These settings are part of Loom's public-readiness posture and must stay
on. They are listed here, not in the user-facing README, so maintainers
have one place to audit them.

- **Default `GITHUB_TOKEN` permissions are read-only.** The required
  `repository-checks` workflow inherits read-only token scope; jobs that
  need to write must opt in per-job and per-permission.
- **Publish and deploy workflows use protected GitHub Environments.**
  Secrets for benchmark-bundle publish or infrastructure deploy live in
  protected Environments so they are not available to pull request code.
- **The active `dev protected admission` ruleset** requires the strict,
  GitHub-Actions-app-bound current-head contexts `repository-checks`,
  `images-gate`, `cluster-smoke-gate`, and `staging-smoke-gate`, blocks direct
  pushes, deletion, and force-pushes, and enforces squash-only linear-history
  merges. It has no bypass actors and requires zero human approvals, zero
  CODEOWNER approvals, and zero conversation resolution; these four CI checks
  are its merge authority.
- **The active `main protected promotion` ruleset** requires a pull request,
  and only the GitHub-Actions-app-bound `main-promotion-gate` context. That
  direct job binds the current `dev` head, same-repository PR, successful
  release gate, and evidence artifact to one candidate SHA. The ruleset is
  squash-only and linear, has no bypass actors, and blocks direct pushes,
  deletion, and force-pushes. Its status check is intentionally non-strict;
  candidate freshness is enforced by the composite gate itself.
- **Auto-merge is native and author-neutral.** `allow_auto_merge=true` lets a
  developer or maintainer enable GitHub's squash auto-merge on every eligible
  non-draft PR. The four required checks remain merge authority for `dev`;
  enabling auto-merge grants no bypass. `main` uses the separate protected
  promotion ruleset described above.
  Verify remote rulesets through the GitHub API or repository settings rather
  than treating this checked-in description as live evidence.
- **Selected Actions sources** restricts third-party actions to the repository
  allowlist. Every remote `uses:` reference must also match the full commit SHA
  and upstream version recorded in `config/ci-actions-lock.json`; run
  `uv run --no-sync python scripts/check_ci_action_pins.py` when adding or updating an
  Action.
- **The uv toolchain is content-verified.** Keep the exact uv version and the
  official macOS arm64/Linux x86_64/Linux arm64 archive SHA256 values in
  `config/uv-toolchain.toml`; repository contracts require every setup step to
  use the matching checksum.
- **Secret scanning** is enabled at the repo level.

External pull requests for issue-scoped work go through the same CI
gate; they cannot reach publish/deploy secrets because those live in
protected Environments rather than as repository secrets.
