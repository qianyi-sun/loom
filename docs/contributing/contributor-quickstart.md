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
src/loom_drivers/                  # cloud Driver implementations (daytona/)
src/loom_control_plane/            # FastAPI Control Plane service
src/loom_llm_gateway/              # OpenAI-compatible LLM Gateway service
src/loom_worker/                   # Worker process
src/loom_service/                  # REST surface for SPA / external clients
src/loom_benchmark_tool/           # `loom-benchmark` operator CLI
packages/loom-launcher/            # PyPI-style agent-adapter framework
packages/loom-benchmarks/          # PyPI-style benchmark-adapter framework + 23 catalog entries (19 adapter files)
packages/loom-benchmark-terminal-bench-2/  # TB-2 canonical adapter
migrations/                        # Alembic
tests/{unit,contract,integration,system,property,loom_cli,fixtures}/
web/                               # React SPA
deploy/                            # Dockerfile.{control-plane,gateway,worker,service,web}
                                   # + k8s/{postgres,minio,llm-gateway,control-plane,
                                   #        loom-service,web,worker,ingress}.yaml
                                   # + nginx-spa.conf + docker-compose.{dev,test}.yml
docs/                              # index.md → user-guide, architecture/, operator-runbook,
                                   # authoring-a-task, loom-vs-harbor, contributor-quickstart
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
running `loom service up`.

```bash
# One-time — uv creates .venv/ on first sync; activate it after.
# Dependency resolution is intentionally constrained in pyproject.toml
# to stay valid on both local macOS development and Linux x86_64 CI.
uv python install 3.11
uv sync --extra dev --python 3.11
uv pip install -e packages/loom-launcher \
               -e packages/loom-benchmarks \
               -e packages/loom-benchmark-terminal-bench-2
source .venv/bin/activate

# Provider keys + stack bootstrap
cp .env.example .env       # then edit
loom service up            # docker compose + migrations + token

# Front-end iteration (Vite HMR on :5173, proxies /api → :8090)
cd web && npm install && npm run dev
```

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
fast-tier coverage summary. The mypy step uses a GitHub Actions cache for
`.mypy_cache`; a restored cache is only a speed-up, not a replacement for
running `uv run mypy`. `dev` pushes skip the Python gate because the
squash-merged PR already produced the required context.

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
only the generated `src/api/schema.d.ts` production source is excluded. The
Playwright gate serves the production build at `/dev` by default and supports
the same local contract at `/prod`. It exercises logged-out, user, and admin
routes at 1440x900 and 390x844, reloads deep links, and rejects empty roots,
page errors, unexpected console output, same-origin request failures, failed
browser assets, and script/style MIME mismatches. Axe must report zero serious
or critical violations. Its exact request/response fixtures are local-only and
contain no deployment credentials.

```bash
uv run ruff check src tests packages migrations
uv run mypy
uv run pytest tests/unit tests/ops tests/loom_cli tests/contract tests/property
uv run pytest packages/loom-launcher/tests \
              packages/loom-benchmarks/tests \
              packages/loom-benchmark-terminal-bench-2/tests
```

Local verification should use Python 3.11, matching the `repository-checks`
job. The repository root `.python-version` pins uv-managed virtualenv creation
to 3.11; if a local `.venv` was created with another interpreter, remove it and
rerun `uv sync --extra dev --python 3.11` before running mypy.

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

On GitHub, selected non-Docker integration tests are split into two disjoint
filename shards and start directly after the planner, in parallel with the
fast tier. The local commands remain serial equivalents so they are easy to
reproduce.

Every relevant non-draft PR runs its path-selected validation plan and emits
the four protected contexts. Drafts and unrelated metadata events use only a
`*-filtered` context. No label, author, reviewer, or merge coordinator grants
gate authority; validation labels only add work to the path-inferred plan.

The `slow` marker is applied at module level on the heaviest 9 test
files (Docker driver lifecycle / exec / io / healthcheck /
network-policy + full trial e2e + Daytona live). CI selects integration for
non-documentation changes. The phase-one ownership authority at
`config/component-ownership.toml` inventories every tracked Dockerfile and
Python, Go, or web test path. It also declares the allowed CI lanes and
component smoke, scan, and attestation owners. Validate the whole inventory or
query one path with:

```bash
python3 scripts/component_ownership.py validate
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
tier. Planner, workflow, rollout, staging-start, and release consumers still
use their existing checked-in mappings until the remaining #788 migration
slices make the manifest their generated input. During that transition, every
change under `tests/integration/` continues to select the Docker tier;
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
dispatch is build-only. Only the checked-in `publish` job on a push to `dev` or
`main` requests job-scoped `packages: write` authority and publishes multi-arch
images. Same-repository branch workflow code still runs on the read-only PR
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
- **Combined fast + integration:** measured + posted to the GitHub
  Actions step summary (workflow run page) only on PRs labelled
  `ci:integration` or `ci:coverage-summary`. Not yet gated; keep changes
  aligned with the historical archive issue (carinrc#7).
- Baseline at latest dev tip: ~72 % fast, ~85 % combined.
- `coverage.xml` ships as a workflow artifact for external tools.

To reproduce the protected fast coverage gate locally, run the equivalent
serial form of the two pytest coverage steps, then run the threshold check.
CI runs these pytest commands in parallel and combines their coverage data in
the final `repository-checks` job; local serial runs need `--cov-append` on the
second command:

```bash
rm -f .coverage coverage.xml
uv run pytest \
  tests/unit tests/contract tests/property tests/loom_cli tests/ops \
  --cov=src --cov=packages \
  --cov-report=term --cov-report=xml
uv run pytest \
  packages/loom-launcher/tests \
  packages/loom-benchmarks/tests \
  packages/loom-benchmark-terminal-bench-2/tests \
  --cov=src --cov=packages --cov-append \
  --cov-report=term --cov-report=xml
uv run coverage report --fail-under=70
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

`main` accepts only a production release promotion from `dev`. The same trusted
controller enables squash auto-merge after release evidence is attached. The
four current-head gates are the only merge authority; no human or CODEOWNER
approval is required.

Current `dev` branch-protection settings (verified by Task 6):
- Squash-only (no rebase merge, no merge commits)
- `required_linear_history: true`
- `repository-checks`, `images-gate`, `cluster-smoke-gate`, and
  `staging-smoke-gate` are the required stable status checks
- `allow_auto_merge: true`; the trusted controller enables it for every
  non-draft PR, and GitHub holds each candidate until the policy above passes
- `enforce_admins: true` on `dev` - admins go through the gate too
- no human approval, no CODEOWNER approval, and no conversation resolution

Current `main` promotion policy:

- the four strict current-head checks, admin enforcement, and linear history
  remain required;
- human approval, CODEOWNER review, and conversation resolution are not merge
  gates;
- only a same-repository `dev` -> `main` promotion is eligible for the trusted
  auto-merge controller.

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
