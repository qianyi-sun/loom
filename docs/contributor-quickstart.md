# Contributor quickstart

For people working on Loom itself, not just running it. End-user
docs live in [`user-guide.md`](user-guide.md) +
[`operator-runbook.md`](operator-runbook.md).

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
packages/loom-benchmarks/          # PyPI-style benchmark-adapter framework + 13 adapters
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

```bash
# One-time — uv creates .venv/ on first sync; activate it after.
uv sync --extra dev
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

CI gates the fast tier on every push + PR:

```bash
pytest tests/unit tests/contract tests/property tests/loom_cli \
       packages/loom-launcher/tests packages/loom-benchmarks/tests \
       packages/loom-benchmark-terminal-bench-2/tests
       # ~10 s, no external deps
```

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

The `slow` marker is applied at module level on the heaviest 9 test
files (Docker driver lifecycle / exec / io / healthcheck /
network-policy + full trial e2e + Daytona live). CI runs the fast
tier on `ci:integration`-labeled PRs and both tiers on push to
dev/main; see [`#259`](https://github.com/carinrc/loom/issues/259)
for the speedup history.

## Coverage gates

- **Fast tier:** gated at **70 %** via
  `coverage report --fail-under=70` in CI. Drops below fail
  `repository-checks` for everyone.
- **Combined fast + integration:** measured + posted to the GitHub
  Actions step summary (workflow run page) when integration ran
  (i.e., `ci:integration` label or push). Not yet gated — see
  [`#260`](https://github.com/carinrc/loom/issues/260) for the
  staged rollout.
- Baseline at latest dev tip: ~72 % fast, ~85 % combined.
- `coverage.xml` ships as a workflow artifact for external tools.

## Workflow

Use PRs into `dev` for normal development; `main` is release-only and
receives promotion PRs from `dev`. See
[`../CONTRIBUTING.md`](../CONTRIBUTING.md) for commit style, PR
requirements, and the definition of done. Public-readiness work is
tracked at [`#330`](https://github.com/carinrc/loom/issues/330), and
the broader GitHub-flow cleanup remains tracked at
[`#254`](https://github.com/carinrc/loom/issues/254).

Merge mechanics:
- Squash-only (no rebase merge, no merge commits)
- `required_linear_history: true`
- `repository-checks` is the only required status check
- `enforce_admins: true` on `dev` - admins go through the gate too

Secrets and side-effect workflows:
- Pull request workflows use read-only `GITHUB_TOKEN` permissions and
  must not receive publish or deployment secrets.
- The benchmark publishing workflow uses the protected
  `huggingface-publish` environment and should only expose `HF_TOKEN`
  after branch restrictions and maintainer approval pass.
- Deployment or publish workflow changes need platform-admin review
  because they change the public-repository security boundary.
