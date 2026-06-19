# Loom Task Authoring Guide

How to write a new Loom task. Examples under `tests/fixtures/tasks/`
are canonical references — `hello-world/` is the smallest possible
task; `multi-step-3/` shows per-step rewards + `min_reward` gates.

## Directory layout

```
my-task/
├── task.toml                # required: task config (see src/loom/models/task.py:TaskConfig)
├── instruction.md           # required: top-level agent instruction
├── solution/
│   └── solve.sh             # required when [agent].name = "oracle" (chmod +x)
├── environment/
│   └── Dockerfile           # optional: custom image (otherwise docker_image used)
├── steps/                   # optional: per-step overrides
│   └── phase-1/
│       └── instruction.md
└── tests/                   # verifier-mode dependent
    └── test_*.py            # pytest verifier reads these
```

## Minimal `task.toml`

```toml
schema_version = "1"

[task]
id = "my-task"
name = "Short human-readable name"
description = "What the agent must accomplish."

[environment]
os = "linux"
docker_image = "python:3.11-alpine"
# OR build a custom image:
# dockerfile = "environment/Dockerfile"

[agent]
name = "oracle"  # or "litellm", "claude-code"
# [agent.model]
# provider = "anthropic"
# name = "claude-opus-4-7"

[verifier]
name = "pytest"  # or "script", "structured", "llm-judge", "composite"

[[steps]]
name = "main"
artifacts = ["result.txt"]
# min_reward = 0.5  # gates aggregate reward; only on multi-step tasks
```

## Agent choices

| name | When | Notes |
|---|---|---|
| `oracle` | Baseline + dev fixtures | Runs `solution/solve.sh` inside the sandbox. Tasks that set `[agent].name = "oracle"` must include it. |
| `litellm` | Out-of-box LLM agent | Tool-loop over Gateway calls; requires `[agent.model]`. |
| `claude-code` | In-box CLI agent | Runs `claude` inside the container; requires the image to ship the CLI. |

## Verifier choices

| name | What it does | Input |
|---|---|---|
| `pytest` | Runs `tests/test_*.py` in-sandbox, parses JUnit XML | `tests/` dir |
| `script` | Runs an arbitrary script; reads JSON from `LOOM_VERIFIER_OUTPUT` | `verifier/run.sh` |
| `structured` | JSON Schema validation of an artifact | schema in config |
| `llm-judge` | Submits trajectory excerpt + rubric to the gateway | rubric in config |
| `composite` | Aggregates sub-verifiers (MEAN/MIN/MAX/WEIGHTED) | nested verifier list |

## Multi-step tasks

Use multiple `[[steps]]` blocks. The runtime invokes the agent once
per step, threading instructions through `steps/<name>/instruction.md`
when present (falling back to top-level `instruction.md`). Each step
produces its own `VerifierResult`; the trial-level reward is
aggregated per `TrialConfig.step_aggregation` (mean / min / final).

`min_reward` on a step gates the trial: if any gated step's reward
falls below the threshold, the trial halts at that step and the
aggregate is computed from the steps that completed.

## Environments

- **Standard image:** `docker_image = "python:3.11-alpine"`. Fastest
  iteration, smallest image, but anything you need (pytest, etc.)
  must be present at runtime — `pip install` happens inside the
  container per step.
- **Custom image:** `dockerfile = "environment/Dockerfile"`. Loom
  builds the image once at task ingestion and caches it. Use this
  when the runtime overhead of installing dependencies dominates
  trial duration, or when the env has system-level requirements.

## Network policy

```toml
[environment.network_policy]
kind = "allowlist"  # or "public" (default) or "no-network"
allowlist = ["pypi.org", "files.pythonhosted.org"]
```

Workers enforce via iptables inside the sandbox netns. `dynamic_network_policy = true`
in the worker's capabilities is required for allowlist mode.

## Healthcheck

```toml
[environment.healthcheck]
command = "curl -fs http://localhost:8000/health"
retries = 10
interval_sec = 2.0
```

The driver polls until the healthcheck passes (with exponential
backoff up to `retries`). Useful for tasks where the agent must wait
for a service to be ready before solving.

## Per-step instructions

When `steps/<name>/instruction.md` exists, the agent receives THAT
instruction for that step instead of the top-level one. The top-level
`instruction.md` becomes a fallback used by any step without its own.

## Local validation

```bash
# Schema-validate the task config without running it:
python -c "
import tomllib
from loom.models.task import TaskConfig
TaskConfig.model_validate(tomllib.load(open('task.toml', 'rb')))
print('ok')
"

# Smoke-test the Oracle solution against a real Docker driver:
sg docker -c "pytest tests/integration/test_trial_e2e_docker.py -v"
```

## Registering

There are three supported task-registration shapes today:

- Single dev/test fixtures can still be seeded with
  `scripts/seed_test_data.py`.
- Operator-owned folders of `task.toml` bundles can be registered as a
  benchmark through `config/benchmarks.toml` and
  `loom datasets sync-config`.
- Adapter-backed benchmarks should go through
  `loom datasets publish` followed by `loom datasets register`.
  Publish validates each generated `task.toml`, writes a schema v3
  manifest with per-task `task_config`, and register persists that
  config into `tasks.config`.

For a one-off local fixture, use `scripts/seed_test_data.py` as a
template:

```bash
python scripts/seed_test_data.py \
    --db-url postgresql+psycopg://loom:PWD@HOST:5432/loom \
    --task-id my-task \
    --print team
```

The script reads `tests/fixtures/tasks/my-task/task.toml`, computes a
SHA-256 checksum of the TOML, and inserts the row. To register a task
that lives somewhere else than `tests/fixtures/tasks/`, fork the
script — the SQL is short and the schema is at
`src/loom/db/schema.py:Task`.

Stored task rows must keep `config` valid against `TaskConfig`.
`POST /trials` and `POST /api/v1/batches` reject invalid task config
with HTTP 400, and the batch runner excludes legacy invalid rows from
older batches so they do not retry forever. Task ids may contain `/`
segments, for example `humaneval/HumanEval/26`; workers fetch those
through the normal bundle endpoint.

Catalog counts are runnable counts. A task row inserted as an import
placeholder with empty or incomplete `config` is not counted by benchmark
`task_count` or `POST /api/v1/tasks/count`, so the New Batch screen shows
the benchmark as needing publish instead of offering an evaluation run.

For benchmark-level onboarding, prefer the manifest path:

```bash
loom datasets publish my-benchmark --hf-org "$LOOM_HF_ORG"
loom datasets register my-benchmark --hf-org "$LOOM_HF_ORG" \
    --db-url "$LOOM_DB_URL"
```

Legacy manifests without `task_config` remain metadata placeholders.
They must be republished or backfilled before users can launch them
from the batch UI. Folder-first user-owned publishing is tracked by
the benchmark onboarding design; it should reuse the same validate,
publish, register, and smoke primitives.

Once registered, submit a trial:

```bash
curl -X POST https://loom.example.com/trials \
  -H "Authorization: Bearer $TEAM_TOKEN" \
  -d '{"task_id": "my-task", "config": {}}'
```

`POST /admin/tasks` is planned for v1.5 — it'll accept a tarball of the
fixture dir, validate the TaskConfig, and stash the source for the
worker to pull via `bundle["source"]`.

## Gotchas

- **Solve script paths.** `solve.sh` runs with `/workspace` as cwd in
  the sandbox; `tests/` runs the same way. Write tests relative to
  `/workspace/`, not the host fixture dir. For code-completion
  benchmarks that already ship a canonical `solution/solution.py`, a
  no-op `solve.sh` is enough for oracle smoke; the pytest verifier does
  the correctness check.
- **Shared verifier env.** v1 runs the pytest verifier in the same
  container as the agent. The agent's image must therefore ship
  `pytest` (and any test deps) — see `hello-world/environment/Dockerfile`
  for the canonical preinstall.
- **Step IDs.** `name` becomes the step_id in the trajectory; keep it
  short and stable across runs so ATIF projection stays consistent.
- **Determinism.** Loom replays trajectories and re-projects ATIF on
  schema bumps. Tasks that depend on wall-clock time or random seeds
  must surface those values as artifacts so reviewers can audit.
