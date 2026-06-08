# Loom

Agent evaluation and training-data generation runtime.

Loom replaces the `harbor==0.9.0` integration this repo previously
shipped with a tailored, distributed runtime designed for shared use
across our research subteams. The first product workflow targeting
Loom is SkillFlow + SkillLearnBench (terminal-agent skill learning)
under pilot group.

## Status

All seven implementation plans shipped (2026-06-05/06). The runtime
core is runnable; v0.7 is the first tagged release.

- **Tag:** `loom-v0.7-runtime-core`
- **Plan tags:** `loom-foundation-v0.1` (Plan 1), `loom-driver-trajectory-v0.2` (Plan 2), `loom-agent-verifier-trial-v0.3` (Plan 3), `loom-llm-gateway-v0.4` (Plan 4), `loom-control-plane-v0.5` (Plan 5), `loom-worker-v0.6` (Plan 6).
- **CHANGELOG:** `CHANGELOG.md`
- **Design spec:** `docs/specs/2026-06-05-loom-runtime-core-design.md`
- **Implementation plans:** `docs/plans/`
- **Pre-Loom repo content** (read-only reference): `legacy/`

## What Loom does

- Runs benchmark tasks against arbitrary agents (Claude Code, custom, Oracle baseline).
- Captures full agent trajectories as event-sourced JSONL on MinIO.
- Projects to **ATIF v1.7** at finalize for downstream tooling.
- Schedules across teams with **DRF** (Dominant Resource Fairness).
- Tracks costs against versioned rate cards via the LLM Gateway.

## Architecture (one paragraph)

A FastAPI **Control Plane** owns the trial state machine, DRF
scheduling, and the trajectory index. **Workers** poll for trials,
run them in-process against a **Driver** (Docker in v1), emit
append-only JSONL trajectories to MinIO, and report state via fenced
HTTP PATCH endpoints. An **LLM Gateway** (LiteLLM-backed) proxies
model calls so every trajectory carries faithful token usage and
cost. Postgres + MinIO are the only stateful services; the LLM
Gateway and Control Plane are stateless.

## Components

| Component | Lives in | Talks to |
|---|---|---|
| Foundation library | `src/loom/` | (used by all) |
| Control Plane | `src/loom_control_plane/` | Postgres, MinIO |
| LLM Gateway | `src/loom_llm_gateway/` | Anthropic / OpenAI / Together, Postgres |
| Worker | `src/loom_worker/` | Control Plane, Gateway, MinIO, Docker |

## Quick start (local dev)

```bash
pip install -e ".[dev]"
docker compose -f deploy/docker-compose.dev.yml up -d
alembic -c migrations/alembic.ini upgrade head
TEAM_TOKEN=$(python scripts/seed_test_data.py)

curl -X POST http://localhost:8080/trials \
  -H "Authorization: Bearer $TEAM_TOKEN" \
  -d '{"task_id":"hello-world","config":{}}'
```

See `docs/operator-runbook.md` for production deployment + token
rotation + alarm response, and `docs/task-authoring-guide.md` for
writing new tasks.

## Tests

```bash
pytest tests/unit tests/contract           # fast: ~3s, no external deps
pytest tests/property                       # hypothesis-based
sg docker -c "pytest tests/integration"     # docker-gated; ~4 min
pytest tests/system -v                      # full-stack (compose up + down)
```

`tests/system` is excluded from the default `pytest tests/` collection
(see `pyproject.toml`); opt in explicitly.

## Repo layout

```
src/loom/                          # foundation library
src/loom_control_plane/            # FastAPI Control Plane service
src/loom_llm_gateway/              # OpenAI-compatible LLM Gateway service
src/loom_worker/                   # Worker process
migrations/                        # Alembic
tests/{unit,contract,integration,system,property,fixtures}/
docs/{specs,plans}/    # design + roadmap
deploy/                            # docker-compose + k8s manifests
scripts/                           # operator + test helpers
legacy/                            # everything from before Loom — read-only
```

## How to read this repo

1. `docs/specs/2026-06-05-loom-runtime-core-design.md` — the design
2. `CHANGELOG.md` — what shipped and when
3. `docs/operator-runbook.md` — production deploy + ops
4. `docs/task-authoring-guide.md` — how to write a new task

## Contributing

`CONTRIBUTING.md` — generic process, still applicable.
`SECURITY.md` — generic policy, still applicable.
