# Loom

Agent evaluation and training-data generation runtime.

Loom is **benchmark-agnostic**: scalability and compatibility with
arbitrary benchmarks via per-benchmark adapters are the product, not
optimization for any single workflow. 14 benchmark adapters ship today
(HumanEval, SWE-Bench Verified/full/Multimodal, MBPP, LiveCodeBench,
BFCL, GAIA, AIME, OSWorld, WebArena, SkillFlow, SkillLearnBench,
Terminal-Bench-2.0).

There are two ways to use Loom:

- **`loom` CLI (local laptop)** — `pip install` and run benchmarks
  against agents on your machine, no server stack required. The fastest
  way to get a single trial's trajectory + ATIF JSON out the door.
- **Service mode (cluster)** — Control Plane + Workers + LLM Gateway +
  Postgres + MinIO. Multi-team, DRF-scheduled, with the SPA at
  `web/` for browsing trials and managing tokens.

## Quick start — `loom run` (laptop)

```bash
pip install -e . -e packages/loom-launcher -e packages/loom-benchmarks

# Discover what's available.
loom datasets list

# Set an upstream API key (or `export ANTHROPIC_API_KEY=...`).
loom config set token.anthropic sk-ant-xxx

# Run a benchmark against any agent + backend.
loom run \
  --dataset swe-bench-verified \
  --agent claude-code \
  --model anthropic/claude-opus-4-7 \
  --backend docker \
  --concurrency 4 \
  --output-dir ./runs
```

Per-trial outputs land at `./runs/<trial-id>/events.jsonl` (full event
trajectory) + `./runs/<trial-id>/atif.json` (ATIF v1.7 projection).
`loom run --json` switches the per-trial line to a JSON object for
piping to `jq`.

### Targeting a single task

`--task` is `<dataset-slug>/<instance-id>` — the instance-id portion
may itself contain slashes (HumanEval upstream literally uses
`HumanEval/0`, SWE-Bench uses `django__django-12345`, etc.):

```bash
# One HumanEval task, oracle baseline, no real agent run.
loom run --task humaneval/HumanEval/0 --agent oracle --backend fake \
         --output-dir /tmp/loom-smoke --json
```

### Backend choice

- `--backend fake` — wiring smoke; the trial runs end-to-end and
  events.jsonl + atif.json land on disk, but no real solver runs
  inside a sandbox. Use this to verify your install is healthy.
- `--backend docker` — real per-task sandbox via the local Docker
  daemon. Required for actual evaluation runs.
- `--backend daytona` — cloud sandboxes via Daytona (see below).

### Cloud backends (optional)

```bash
# Daytona cloud sandboxes — no local Docker required.
export DAYTONA_API_KEY=...
loom run --backend daytona --dataset bfcl --agent oracle ...
```

`--backend daytona` shells trials to Daytona cloud sandboxes;
compute-seconds + cost land in the `cloud_compute_records` table when
the run is connected to a Loom service (`--server-url`).

### `loom datasets`

```
loom datasets list                  # all sources (installed + available)
loom datasets list --installed      # only adapters in this venv
loom datasets show humaneval        # adapter detail + entry-point
loom datasets install <slug>        # pip-install a registry entry
```

Discovery unions three sources: entry-points (`loom.benchmarks` group),
an in-tree default registry JSON, and an optional Loom service's
`/api/v1/benchmarks` (set `LOOM_SERVER_URL`).

## Quick start — service mode (cluster)

```bash
pip install -e ".[dev]"
docker compose -f deploy/docker-compose.dev.yml up -d
alembic -c migrations/alembic.ini upgrade head
TEAM_TOKEN=$(python scripts/seed_test_data.py)

# loom_service is the user-facing REST surface on :8090 — Control
# Plane sits at :8080 behind it on the internal compose network.
curl -X POST http://localhost:8090/api/v1/trials \
  -H "Authorization: Bearer $TEAM_TOKEN" \
  -d '{"task_id":"hello-world","config":{}}'
```

See `docs/operator-runbook.md` for production deployment + token
rotation + alarm response, and `docs/authoring-a-task.md` for
writing new tasks.

## Architecture (one paragraph)

A FastAPI **Control Plane** owns the trial state machine, DRF
(Dominant Resource Fairness) scheduling, and the trajectory index.
**Workers** poll for trials, run them in-process against a **Driver**
(Docker, Daytona; Modal pending), emit append-only JSONL trajectories
to MinIO, and report state via fenced HTTP PATCH endpoints. An **LLM
Gateway** (LiteLLM-backed) proxies model calls so every trajectory
carries faithful token usage and cost. Postgres + MinIO are the only
stateful services; Gateway, Control Plane, and `loom_service` (REST +
SPA) are stateless.

For laptop use, the **`loom` CLI** reuses `Trial.run()` against a
`LocalDiskObjectStore` + `UpstreamDirectGatewayClient` (provider SDKs
directly), so trajectories + ATIF files are bit-identical to a
service-mode run.

## Status

What's shipped: the full runtime + REST surface + React SPA + CLI +
14 benchmark adapters + 11 agent adapters + Docker and Daytona cloud
drivers. Both modes (CLI on laptop, service on cluster) are
production-ready.

Queue:
- **Modal cloud driver** ([#253](https://github.com/carinrc/loom/issues/253)) — `cloud_compute_records` schema is already generic via the `cloud_provider` column, so ships zero schema work
- **Production topology decisions** ([#134](https://github.com/carinrc/loom/issues/134))
- Several `deferred:v1.5` items (SPA SSE, rate-card catalog, skill objects, weights inference, ops/governance policy, research-team intake)

- **What shipped when:** GitHub releases + `git log` (no separate CHANGELOG)
- **Docs index:** `docs/index.md` (start here)
- **Architecture & contracts:** `docs/architecture/overview.md`
- **Design tradeoffs vs Harbor:** `docs/loom-vs-harbor.md`

## What Loom does

- Runs benchmark tasks against arbitrary agents (Claude Code, custom,
  Oracle baseline, 11 production CLI adapters in `loom-launcher` plus
  a `hello` test-reference adapter).
- Captures full agent trajectories as event-sourced JSONL on MinIO
  (or local disk in CLI mode).
- Projects to **ATIF v1.7** at finalize for downstream tooling.
- Schedules across teams with **DRF** (Dominant Resource Fairness) in
  service mode.
- Tracks LLM-call cost against versioned rate cards via the LLM
  Gateway; tracks cloud-driver compute-seconds + cost in
  `cloud_compute_records`.
- Surfaces both via `/api/v1/usage` (service) or per-trial JSON
  (`loom run --json`).

## Components

| Component | Lives in | Talks to |
|---|---|---|
| Foundation library | `src/loom/` | (used by all) |
| `loom` CLI | `src/loom_cli/` | adapters, local disk, provider SDKs |
| Cloud drivers | `src/loom_drivers/` | Daytona (Modal pending) |
| Control Plane | `src/loom_control_plane/` | Postgres, MinIO |
| LLM Gateway | `src/loom_llm_gateway/` | Anthropic / OpenAI / Google, Postgres |
| Worker | `src/loom_worker/` | Control Plane, Gateway, MinIO, Docker |
| Service (REST + SPA) | `src/loom_service/` + `web/` | CP, Gateway, Postgres |
| Benchmark adapters | `packages/loom-benchmarks/` + `packages/loom-benchmark-terminal-bench-2/` | (discovered via entry-points) |
| Agent adapters | `packages/loom-launcher/` | (discovered via `loom_launcher.get_adapter`) |
| Operator CLI | `src/loom_benchmark_tool/` | Postgres, MinIO |

## Tests

CI gates the fast tier on every push + PR:

```bash
pytest tests/unit tests/contract tests/property tests/loom_cli \
       packages/loom-launcher/tests packages/loom-benchmarks/tests \
       packages/loom-benchmark-terminal-bench-2/tests
       # ~10s, 649+ tests, no external deps
```

Heavier suites are opt-in:

```bash
pytest tests/integration -v         # Docker + Postgres + MinIO via testcontainers
pytest tests/system -v              # full docker-compose stack
LOOM_RUN_DAYTONA_INTEGRATION=1 \
DAYTONA_API_KEY=... \
pytest tests/integration/test_daytona_driver.py -v   # live Daytona, costs ~$0.01
```

Coverage is measured + posted to PRs. The fast tier is gated at
**70 %** (`coverage report --fail-under=70`) — drops below fail
`repository-checks` for everyone. Combined fast+integration coverage
is also computed (when integration ran via the `ci:integration`
label) and posted to the PR comment, but isn't yet gated. Baseline
~72 % fast tier, ~85 % combined. `coverage.xml` ships as a
workflow artifact.

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
                                   # authoring-a-task, loom-vs-harbor
scripts/                           # operator + test helpers
```

## Docs

- `docs/index.md` — navigation entry point (start here)
- `docs/user-guide.md` — `loom` CLI guide for researchers (install,
  `run`, `datasets`, `config`, troubleshooting)
- `docs/architecture/` — focused architecture docs (overview, driver
  protocol, benchmark adapter, agent adapter, trajectory + ATIF,
  CLI mode, service mode)
- `docs/operator-runbook.md` — production deployment + ops
- `docs/authoring-a-task.md` — writing a new benchmark task
- `docs/loom-vs-harbor.md` — design tradeoffs vs. Harbor + current gaps

## Contributing

`CONTRIBUTING.md` — single-owner direct-to-dev workflow (with the
GitHub-flow target state for future contributors documented at the
bottom).
`SECURITY.md` — generic policy, still applicable.
