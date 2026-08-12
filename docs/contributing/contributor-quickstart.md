# Contributor quickstart

For people working on Loom itself, not just running it. End-user
docs live in [`user-guide.md`](../user-guide.md) +
[`operator-runbook.md`](../runbooks/operator-runbook.md).

The canonical public development repository is
[`qianyi-sun/loom`](https://github.com/qianyi-sun/loom):

```bash
git clone https://github.com/qianyi-sun/loom.git
cd loom
```

## Repo layout

```
LICENSE                            # Apache-2.0
src/loom/                          # foundation library (types, errors, models)
src/loom_cli/                      # `loom` CLI entry point
src/loom_drivers/                  # cloud Driver implementations (Daytona, Modal)
src/loom_control_plane/            # FastAPI Control Plane service
src/loom_llm_gateway/              # OpenAI-compatible LLM Gateway service
src/loom_worker/                   # Worker process
src/loom_service/                  # REST surface for SPA / external clients
src/loom_benchmark_tool/           # `loom-benchmark` operator CLI
packages/loom-launcher/            # PyPI-style agent-adapter framework
packages/loom-benchmarks/          # PyPI-style benchmark adapters + bundled catalog
packages/loom-benchmark-terminal-bench-2/  # TB-2 canonical adapter
migrations/                        # Alembic
tests/{unit,contract,integration,system,property,loom_cli,fixtures}/
web/                               # React SPA
deploy/                            # images, Compose, Kubernetes, environment, and fleet config
docs/                              # current user, architecture, integration, and runbook docs
archive/                           # non-current designs, plans, history, and evidence
scripts/                           # operator + test helpers
```

## Components

| Component | Lives in | Talks to |
|---|---|---|
| Foundation library | `src/loom/` | (used by all) |
| `loom` CLI | `src/loom_cli/` | adapters, local disk, provider SDKs |
| Cloud drivers | `src/loom_drivers/` | Daytona, Modal |
| Control Plane | `src/loom_control_plane/` | Postgres, MinIO |
| LLM Gateway | `src/loom_llm_gateway/` | Anthropic / OpenAI / Google, Postgres |
| Worker | `src/loom_worker/` | Control Plane, Gateway, MinIO, Docker |
| Service (REST) | `src/loom_service/` | CP, Gateway, Postgres |
| Web SPA | `web/` (served by `loom-web` k8s pod via nginx) | `loom_service` `/api/v1/*` |
| Benchmark adapters | `packages/loom-benchmarks/` + `packages/loom-benchmark-terminal-bench-2/` | (discovered via entry-points) |
| Agent adapters | `packages/loom-launcher/` | (discovered via `loom_launcher.get_adapter`) |
| Operator CLI | `src/loom_benchmark_tool/` | Postgres, MinIO |

## Dev setup

Service mode requires Docker CLI with the Compose plugin. On macOS, install and
start Docker Desktop first; `docker compose version` should succeed before
running `loom service up --environment local`.

```bash
# One-time — uv 0.11.26 creates .venv/ from the tracked universal lock.
# The workspace lock covers macOS arm64, Linux x86_64, and Linux arm64.
uv python install 3.11
uv sync --locked --all-packages --extra dev --python 3.11
source .venv/bin/activate

# Provider keys + stack bootstrap
cp .env.example .env       # then edit
loom service up --environment local  # docker compose + migrations + token

# Front-end iteration (Vite HMR on :5173, proxies /api → :8090)
cd web && npm install && npm run dev
```

## Frontend recovery contract

The SPA must remain visible and recoverable when runtime config, browser
session loading, the root render tree, or one routed page fails. Read
[`frontend-error-recovery.md`](../architecture/frontend-error-recovery.md)
before changing `web/src/main.tsx`, `web/src/bootstrap/FrontendBootstrap.tsx`,
`web/src/auth/AuthContext.tsx`, the root/route boundaries, or their reporter.

Keep these invariants when adding a new failure path:

- Render fixed, kind-specific safe copy plus a generated `WEB-*` reference;
  never render an API/browser error message or response body.
- Keep raw URLs, query strings, tokens, cookies, CSRF values, stacks, component
  stacks, and raw throwables out of both UI state and reporter payloads.
- Treat only the exact `/api/v1/auth/me` `401` as `signed-out`. Network,
  non-401 HTTP, `204`, malformed, and invalid-schema responses are
  `unavailable`, not a reason to show a login form. A successful response must
  include the declared identity, authorization, membership, and non-empty CSRF
  fields; do not silently default a malformed session into authenticated state.
- Treat an ambiguous response from login completion, password login, invite
  acceptance, or team switching as `unavailable`: clear old authorization,
  CSRF, and query trust, then reconcile through `/api/v1/auth/me`. The server
  may have committed the session mutation before the response was lost.
- Keep auth reads, session-producing mutations, and logout on the shared query
  client's session-operation queue. Root recovery remounts must wait for an old
  operation to settle and then read authoritative `/auth/me`; an older response
  must never reinstall CSRF after an exact unauthorized event.
- Offer Retry only when a fresh in-document attempt can work. A cached
  `React.lazy` rejection must use `retryPolicy="reload-required"` and omit the
  misleading Retry action.
- Preserve route-aware Home (`/dev/`, `/prod/`, or `/`), keyboard focus, a
  single `main#main-content`, and distinct reference/report correlation.
- Keep `BrowserFailureReport` bounded to reference, allowlisted kind, redacted
  pathname, and optional safe same-origin source position. The current reporter
  is an in-memory hook, not a telemetry transport, and does not retain the
  original throwable.

Run the focused recovery tests before the full web suite:

```bash
cd web
npm test -- \
  src/__tests__/AuthContext.test.tsx \
  src/__tests__/bootstrap/FrontendBootstrap.test.tsx \
  src/__tests__/components/BrowserErrorBoundary.test.tsx \
  src/__tests__/components/Layout.test.tsx \
  src/__tests__/components/RecoveryPanel.test.tsx \
  src/__tests__/components/RootErrorBoundary.test.tsx \
  src/__tests__/components/RouteRecoveryBoundary.test.tsx
npm test
npm run typecheck
npm run lint
npm run test:coverage
npm run build
npm run test:e2e -- e2e/recovery.test.ts
LOOM_E2E_ROUTE_PREFIX=/prod npm run test:e2e -- e2e/recovery.test.ts
```

The render-fault scenarios must stay inside the compile-time browser-test
build. A normal `npm run build` verifies that the emitted `dist/` assets do not
contain the recovery fault key or the fixed test-only fault strings.

## Tests

Every PR and merge-group candidate reports four stable validation contexts:
`repository-checks`, `images-gate`, `cluster-smoke-gate`, and
`staging-smoke-gate`. The shared validation planner selects the applicable
work automatically from changed paths. Labels may request additional work, but
they cannot turn off validation inferred from paths. Docs-only PRs take a
bounded location-and-format fast path while the stable gate contexts still
report. Runtime Markdown outside that boundary, executable files in `docs/`,
and unknown non-document paths do not take the fast path; unknown runtime paths
select all heavy lanes until they gain an explicit owner.

`repository-checks` is the fast-tier aggregator: ruff/mypy/static checks, two
root-test shards, and sibling-package tests run in parallel jobs, then it combines their
coverage artifacts, applies the 70% fast-tier gate, and writes the default
fast-tier coverage summary. CI restores only uv's package/download cache and
never restores `.venv` or `.mypy_cache`; PR and merge-group runs cannot save
cache entries. Every job creates a clean environment with `uv sync --locked`,
then uses `uv run --no-sync` so a test command cannot silently resolve a new
environment. The uv executable version and official per-platform archive
SHA256 values are reviewed in `config/uv-toolchain.toml`; every setup step must
verify the matching checksum. Go's setup cache is disabled rather than allowing
PR code to save module or build-cache state. `dev` pushes skip the Python gate
because the squash-merged PR already produced the required context.

Frontend, SPA image/runtime-config, and frontend gate changes additionally
select the required `web-checks` job. `repository-checks` fails when that
selected job fails or is missing. Reproduce the complete frontend gate with a
frozen install:

```bash
cd web
npm ci
npm run typecheck
npm run lint
npm run test:coverage
npm run build
npx --no-install playwright install chromium
npm run test:e2e
```

The Playwright command defaults to the local `/dev` prefix. To exercise the
same generic production-build harness at the production basename without
contacting a live environment, use:

```bash
LOOM_E2E_ROUTE_PREFIX=/prod npm run test:e2e
```

`LOOM_E2E_ORIGIN` may select a different localhost port. Both inputs are
validated and never authorize staging or production access. The harness never
reuses an existing server: if the selected local origin is occupied, stop that
process or select another localhost port so Playwright can build and serve its
own browser-test bundle.

Vitest enforces statements, lines, and functions at 80% and branches at 75%;
only the generated `web/src/api/schema.d.ts` production source is excluded. The
Playwright gate serves the production build at `/dev` by default and supports
the same local contract at `/prod`. It exercises logged-out, user, and admin
routes at 1440x900 and 390x844, reloads deep links, and rejects empty roots,
page errors, unexpected console output, same-origin request failures, failed
browser assets, and script/style MIME mismatches. Axe must report zero serious
or critical violations. Its exact request/response fixtures are local-only and
contain no deployment credentials.

```bash
uv run --no-sync ruff check src tests packages migrations
uv run --no-sync mypy
mapfile -t root_tests < <(uv run --no-sync python scripts/component_ownership.py test-paths --lane tests-root)
uv run --no-sync pytest "${root_tests[@]}" --cov=src --cov=packages --cov-report=term
mapfile -t package_tests < <(uv run --no-sync python scripts/component_ownership.py test-paths --lane tests-packages)
uv run --no-sync pytest "${package_tests[@]}" --cov=src --cov=packages --cov-append --cov-report=term
uv run --no-sync coverage report --fail-under=70
```

Local verification should use Python 3.11, matching the `repository-checks`
job. The repository root `.python-version` pins uv-managed virtualenv creation
to 3.11; if a local `.venv` was created with another interpreter, remove it and
rerun `uv sync --locked --all-packages --extra dev --python 3.11` before
running mypy.

Heavier suites are opt-in:

```bash
# Integration tier — Docker + Postgres + MinIO via testcontainers
pytest tests/integration                 # full
pytest tests/integration -m "not slow"   # exclude @slow tests (Docker driver, e2e)
pytest tests/integration -m slow         # only the heavy ones

# System tier — full docker-compose stack
pytest tests/system -v

# Live Daytona — costs ~$0.01/run
LOOM_RUN_DAYTONA_INTEGRATION=1 DAYTONA_API_KEY=... \
  pytest tests/integration/test_daytona_driver.py -v
```

On GitHub, selected non-Docker integration tests are split into two disjoint,
contiguous ranges of the manifest-owned filename order. Contiguous ordering
preserves the suite's session-scoped Postgres setup/cleanup contract while the
two shards start directly after the planner, in parallel with the fast tier.
The local commands remain serial equivalents so they are easy to reproduce.

Every relevant non-draft PR runs its path-selected validation plan and emits
the four protected contexts. Drafts and unrelated metadata events use only a
`*-filtered` context. No label, author, reviewer, or merge coordinator grants
gate authority; validation labels only add work to the path-inferred plan.

The protected names are owned by the default-branch-trusted authoritative-gate publisher, not
by an individual validation attempt. Once the publisher is present on the PR's
base, CI, image, cluster, and staging workflows report `*-attempt` results. A
relevant same-head event returns the four authoritative checks to `in_progress`
as soon as the publisher observes it; only the newest full generation may finalize them. Superseded
attempt cancellation is ignored, while a failure, timeout, cancellation, or
missing aggregate in the newest generation remains red. The generation marker
captures the PR identity and complete validation-relevant label snapshot; the
publisher binds it to an ordered validation-event epoch. The publisher validates
live state and the exact run attempt before and after a terminal update; if authority
changed during the CheckRun write, the same check is returned to `in_progress`.
Mask-aware event occurrences cover observable add/remove sequences whose final
labels match. Comments and unrelated labels do not replace the epoch or share
the authoritative source concurrency lane. A pending generation keeps one
fail-closed required identity. Once the exact source result is terminal, the
publisher retires that pending identity out of the protected name and creates
the new required CheckRun already completed. The direct terminal creation makes
GitHub evaluate the exact current result without depending on an in-place
pending-to-success transition; an interrupted retirement or creation leaves the
required context missing or failed. Do not use an empty or tree-identical commit
to repair a check rollup.

After changing the publisher, verify its identity transition on a disposable
PR before closing the governance issue. For each protected context, inspect the
exact head's CheckRuns through the GitHub API. The pending CheckRun ID must be
renamed to `authoritative-gate-retired (<context> #<id>)` and completed with a
failure conclusion; a different CheckRun ID must own the original protected
name and already be terminal when created. The publisher also appends a commit
status under the protected name after each fail-closed or terminal ledger write;
that status event makes ruleset evaluation independent of the arbitrary
CheckSuite to which GitHub attaches custom CheckRuns. All four current protected
checks and statuses must come from GitHub Actions app `15368` and complete
successfully before the PR merges through normal squash auto-merge. Keep the
ruleset active with an empty bypass-actor list throughout this acceptance; a
green source workflow or an ordinary non-required CheckRun is not equivalent
evidence.

Same-repository `dev`-to-`main` promotions use this publisher. Branch push
aggregates use `*-push`, so they never duplicate a protected name on a
later promotion PR that points at the same commit.

The `slow` marker is applied at module level on the heaviest 9 test
files (Docker driver lifecycle / exec / io / healthcheck /
network-policy + full trial e2e + Daytona live). CI selects integration for
non-documentation changes. The ownership authority at
`config/component-ownership.toml` inventories every tracked Dockerfile and
Python, Go, or web test path. Schema v2 also declares the allowed CI lanes,
versioned runtime-payload execution policies, immutable container digests,
per-payload minimal fixture cases, and component smoke, scan, and attestation
owners. Validate the whole inventory, inspect the exact isolated
payload plan, or query one path with:

```bash
python3 scripts/component_ownership.py validate
python3 scripts/component_ownership.py execution-plan --lane runtime-payload
python3 scripts/runtime_payload_conformance.py
python3 scripts/component_ownership.py query tests/integration/test_trial_e2e_docker.py
python3 scripts/component_ownership.py test-paths --lane frontend
```

When the authority exposes its frontend-lane query, `web-checks` consumes that
output instead of copying the owned web test patterns into the workflow. Until
that command is available, the job runs the complete Vitest suite. The quality
gate still owns thresholds, build/browser behavior, and the two specialized
route-smoke unit harnesses; component and test ownership remains the manifest's
responsibility.

The validator fails for missing or ambiguous ownership, stale patterns,
undeclared owner names, and a `pytest.mark.docker` module outside the Docker
tier. Runtime payloads are not executed as host pytest: each manifest-owned
file runs in its own read-only, networkless, resource-limited container against
the payload case's minimal synthetic passing-workspace fixtures. The immutable
image is pulled once, then every case runs with further pulls disabled. This
proves verifier conformance only; it is not proof that a real task or trial
succeeded. Planner, staging-start, and release consumers use manifest-derived
inputs. Rollout build, kind-load, and expected-image evidence use one ten-image
matrix generated from the fixed candidate worktree's eight
`rollout_role = "primary"` and two `rollout_role = "auxiliary"` components, then
persisted by the build step. The two sandbox conformance images are intentionally
excluded. Every change under `tests/integration/`
continues to select the Docker tier;
relevant runtime paths do too. `ci:integration` and `ci:integration-docker`
add those tiers when paths do not already require them. The selected smoke
gates cancel superseded PR runs, so a new push to the same PR stops the older
`cluster-smoke`, `staging-smoke`, or `cluster-deploy-spikes` run instead of
building a queue of stale checks.

`images-gate` is separate from the fast tier. Relevant image paths select its
PR validation automatically; `ci:images` adds validation when paths do not
already require it, and `.github/workflows/images.yml` remains manually
dispatchable. Manual runs report `images-gate-manual`, not the protected
`images-gate` context; the same `*-manual` rule applies to all four protected
workflows. The image workflow plans a path-aware matrix so web-only changes
build only the web image, Dockerfile-only changes build the matching component,
and shared Python/runtime changes rebuild the affected Python images. Relevant
pull requests, merge groups, and manual dispatches use the checked-in read-only
build path, do not log in to GHCR, and do not use a publication cache. Manual
dispatch is build-only. Image validation builds AMD64 and ARM64 in separate
jobs on matching native CPUs; the ARM job uses `ubuntu-24.04-arm` instead of
QEMU. Only the checked-in architecture-specific `publish` jobs and their
manifest joiner on a push to `dev` or `main` request job-scoped `packages: write`
authority. The joiner verifies that the final tag contains exactly the AMD64 and
ARM64 members. Same-repository branch workflow code still runs on the read-only PR
path; autonomous-agent hard isolation requires
fork-only execution or an external trusted workflow/App.

`staging-smoke-gate` proves the credential-free kind deployment smoke only. It
never enters `ci-aws`, and a missing or skipped real-AWS run is not represented
as cloud validation. Real AWS evidence belongs to a separately protected,
trusted post-merge/release workflow rather than the required PR context.

## Coverage gates

- **Fast tier:** gated at **70 %** via
  `coverage report --fail-under=70` in CI. Drops below fail
  `repository-checks` for everyone. The same job writes the default fast-tier
  coverage summary to the GitHub Actions step summary.
- **Combined fast + integration:** measured and posted to the GitHub Actions
  step summary only on PRs labelled `ci:integration` or
  `ci:coverage-summary`. It is reported but is not a required threshold.
- `coverage.xml` ships as a workflow artifact for external tools.

To reproduce the protected fast coverage gate locally, run the equivalent
serial form of the two pytest coverage steps, then run the threshold check.
CI runs these pytest commands in parallel and combines their coverage data in
the final `repository-checks` job; local serial runs need `--cov-append` on the
second command:

```bash
rm -f .coverage coverage.xml
uv run --no-sync pytest \
  tests/unit tests/contract tests/property tests/loom_cli tests/ops \
  --cov=src --cov=packages \
  --cov-report=term --cov-report=xml
uv run --no-sync pytest \
  packages/loom-launcher/tests \
  packages/loom-benchmarks/tests \
  packages/loom-benchmark-terminal-bench-2/tests \
  --cov=src --cov=packages --cov-append \
  --cov-report=term --cov-report=xml
uv run --no-sync coverage report --fail-under=70
```

The first pytest command alone is not the fast coverage gate: it measures the
package source directories in `--cov=packages` before the sibling package tests
have appended their coverage, so it can report a lower partial total. The gate
is the final `coverage report` after both pytest commands have completed.

## Workflow

Use issue-scoped PRs into `dev` for normal development; `main` is
release-only and receives promotion PRs from `dev`. See
[`../CONTRIBUTING.md`](../../CONTRIBUTING.md) for issue ownership, commit
style, PR requirements, and the definition of done.

New contributors should start from an open issue or discuss scope in a
new issue before implementing. PRs use
[`.github/PULL_REQUEST_TEMPLATE.md`](../../.github/PULL_REQUEST_TEMPLATE.md)
and must link the issue they advance. Maintainers mark actively owned
issues with a `[WIP] ` title prefix, keep the project status current,
and follow the normal `dev` auto-merge policy.

Every non-draft `dev` PR uses squash auto-merge. The trusted base-branch
controller enables it without checking out PR code or considering author or
reviewer identity. GitHub keeps each candidate queued until `repository-checks`, `images-gate`,
`cluster-smoke-gate`, and `staging-smoke-gate` are visible and successful on
the current head SHA. Those four strict, GitHub-Actions-app-bound checks are
the only merge authority: `dev` requires no human approval, no CODEOWNER
approval, and no conversation resolution. Maintainers should not manually
merge an eligible `dev` PR just because CI is green.

For `main`, the trusted controller enables squash auto-merge only for a
same-repository `dev` promotion after release evidence is attached. GitHub has
no active `main` ruleset, so maintainers must not use a direct push or manual
merge to bypass the checked-in release flow.

Current `dev protected admission` ruleset:

- Squash-only (no rebase merge, no merge commits)
- `required_linear_history: true`
- `repository-checks`, `images-gate`, `cluster-smoke-gate`, and
  `staging-smoke-gate` are the required stable status checks
- `allow_auto_merge: true`; the trusted controller enables it for every
  non-draft PR, and GitHub holds each candidate until the policy above passes
- no bypass actors; repository admins go through the gate too
- no human approval, no CODEOWNER approval, and no conversation resolution

Current `main` promotion boundary:

- the trusted auto-merge controller accepts only a same-repository `dev` ->
  `main` promotion and the checked-in release flow requires the four
  current-head checks plus release evidence;
- GitHub has no active `main` branch ruleset, so maintainers must not bypass the
  controller with a direct push or manual merge.

Secrets and side-effect workflows:
- Pull request workflows use read-only `GITHUB_TOKEN` permissions and
  must not receive publish or deployment secrets.
- PRs from forks or external contributor branches must not depend on
  protected secrets; maintainers can rerun protected workflows from a
  trusted branch when needed.
- The benchmark publishing workflow uses the protected
  `huggingface-publish` environment and should only expose `HF_TOKEN`
  after branch restrictions and maintainer approval pass.
- Deployment or publish workflow changes are public-repository security-boundary
  changes, so the fail-closed planner must select the full CI validation set. A
  platform-admin review may be requested for context, but it is not a `dev`
  merge gate.
