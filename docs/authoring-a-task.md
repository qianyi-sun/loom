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

### The `oracle_eligible` task tag

The oracle agent requires an in-sandbox `solution/solve.sh`. Adapter-emitted
benchmarks are heterogeneous — SkillLearnBench, for example, ships upstream
`solve.sh` for only 73 of its 100 tasks, and Terminal-Bench-2 mirrors
`solution.sh` / `solution.yaml` only when the upstream instance provides
one. The batch preflight (`src/loom_service/task_compat.py`) filters
`(oracle, task)` pairs before dispatch so the matrix runner never launches a
trial that would deterministically fail with
`AgentError: OracleAgent requires .../solution/solve.sh; not found`.

Adapters that produce oracle-eligible tasks should emit a per-instance
`oracle_eligible` tag with values `"true"` or `"false"`:

```python
BenchmarkInstance(
    instance_id="…",
    task_config={…},
    tags={
        "oracle_eligible": "true" if solve_sh_present else "false",
        # other tags…
    },
)
```

Resolution order in `task_provides_capability("solution_solve_sh", …)`:

1. Task uses the `pytest` verifier → eligible (pytest adapters co-emit
   `solution/solve.sh` next to `solution/_reference.py`).
2. Explicit `oracle_eligible="true"` or `"false"` tag → wins over the
   task-id prefix fallback.
3. Task id matches a known adapter prefix (e.g. `terminal-bench-2/`) whose
   adapter unconditionally emits `solution/solve.sh` → eligible.
4. Otherwise → not eligible; the batch preflight excludes the task and
   surfaces a count + concrete example in the API response.

Hand-authored `task.toml` bundles don't need the tag — the pytest-verifier
rule already covers the common case. Add the tag only when an adapter
produces a mixed slate where some instances lack an upstream `solve.sh`.

## Verifier choices

| name | What it does | Input |
|---|---|---|
| `pytest` | Runs `tests/test_*.py` in-sandbox, parses JUnit XML | `tests/` dir |
| `script` | Runs an arbitrary script; reads `VerifierResult` JSON from `LOOM_VERIFIER_OUTPUT` | `verifier/run.sh` |
| `structured` | JSON Schema validation of an artifact | schema in config |
| `llm-judge` | Submits trajectory excerpt + rubric to the gateway | rubric in config |
| `composite` | Aggregates sub-verifiers (MEAN/MIN/MAX/WEIGHTED) | nested verifier list |

For `pytest`, Loom first ensures `pytest` is importable, then runs the
configured test directory with `--junitxml=/loom/verifier/junit.xml`.
Dependency setup failures are verifier infrastructure failures. A bounded
pytest command timeout is reported as a scored `passed=0.0` outcome with
structured timeout diagnostics, which is appropriate for code benchmarks where
generated solutions may hang.

```toml
[verifier]
name = "pytest"
timeout_sec = 420

[verifier.args]
tests_dir = "/workspace/tests"
install_timeout_sec = 120
pytest_timeout_sec = 240
```

For `script`, set the script path explicitly and write a full
`VerifierResult` JSON object to `LOOM_VERIFIER_OUTPUT`:

```toml
[verifier]
name = "script"

[verifier.args]
script_path = "/workspace/verifier/run.sh"
```

```sh
#!/bin/sh
set -eu
script_dir="$(CDPATH= cd "$(dirname "$0")" && pwd)"
task_dir="${LOOM_TASK_DIR:-$(dirname "$script_dir")}"
answer_file="${LOOM_AGENT_OUTPUT:-$task_dir/final_answer.txt}"
python3 "$task_dir/verifier/check.py" \
  --answer "$answer_file" \
  --output "$LOOM_VERIFIER_OUTPUT"
```

`ScriptVerifier` guarantees `LOOM_VERIFIER_OUTPUT`; task bundles should derive
their own task/artifact paths from the script location, `/workspace`, or safe
defaults. A model's wrong answer should produce a scored verifier result such
as `{"rewards":{"score":0.0},"checks":[...]}` so the trial succeeds as a
platform run while recording model correctness separately. If a script verifier
exits before writing valid JSON, Loom reports a verifier infrastructure failure
and includes return code, stdout/stderr tails, truncation status, duration, the
script path, and the expected output path in `VerifierResult.error.detail`.
Individual `checks[].detail` fields may be objects or simpler JSON values such
as strings when upstream verifier tooling emits legacy diagnostics.

For `litellm` service-mode runs, the platform can write the final model
response into declared step artifacts before verification. Keep benchmark
scoring rules in the verifier, not in the agent. `final_answer.txt` receives
the model's final response with fenced helper code blocks removed so official
or upstream-style extractors such as `Answer: ...`, `Exact Answer: ...`, or
LaTeX `\boxed{...}` still see their expected answer text. A strict
`answer.txt` artifact receives a normalized final answer value for
Harbor-style exact-match tasks. Other artifact paths, including code or JSON
outputs, continue to prefer the first fenced block when the model supplies one.

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
  uploads the Dockerfile with the task bundle, then service-mode workers
  build it from the materialized bundle before the first trial that needs
  it. The built image is cached under a deterministic `loom-task:<hash>`
  tag derived from the task id, checksum, Dockerfile path, and optional
  `docker_build_context`. Use this when the runtime overhead of installing
  dependencies dominates trial duration, or when the env has system-level
  requirements. Keep paths relative to the task bundle; absolute paths and
  `..` traversal are rejected. Put build-only files under `.loom-build/` and
  set `docker_build_context` to that directory when those files should be
  available to Docker but hidden from the agent workspace. `build_timeout_sec`
  controls the per-task image build timeout and defaults to 1200 seconds.
  Loom does not flatten or rewrite bundle directories: if the build context is
  the bundle root and the Dockerfile says `COPY . /app/`, a file stored at
  `environment/setup_repo.sh` lands at `/app/environment/setup_repo.sh`, not
  `/app/setup_repo.sh`. Move files to the declared build-context root, update
  the Dockerfile path, or set `docker_build_context` to a directory whose layout
  matches the Dockerfile.
  Compatibility preflight rejects known platform-breaking patterns before
  expensive fanout, including Dockerfiles that mutate `/etc/resolv.conf`,
  `/etc/nsswitch.conf`, or `/etc/hosts` before the service-mode agent layer is
  installed. Use task-owned setup/runtime steps for DNS/NSS experiments instead
  of breaking agent installation or model connectivity in the base task image.
  Workers also enforce operator-owned build context limits before calling
  Docker: `LOOM_TASK_IMAGE_BUILD_MAX_FILES` defaults to 2000 files and
  `LOOM_TASK_IMAGE_BUILD_MAX_BYTES` defaults to 536870912 bytes. If either
  limit is exceeded, the trial fails during setup with an actionable
  diagnostic instead of starting an unbounded Docker build.
- **CPU architecture:** `cpu_arch = "x86_64"` is the default. Set
  `cpu_arch = "arm64"` for ARM-only images, or `cpu_arch = "any"` only after
  the image, verifier, and expected artifacts have been proven credible on both
  x86_64 and ARM64 workers. Legacy tasks that omit this field are scheduled as
  x86_64-only so newly attached ARM64 worker pools cannot accidentally claim
  architecture-specific images. Catalog-backed adapters may emit
  `cpu_arch = "any"` from explicit benchmark metadata only after that benchmark
  has dual-architecture evidence; do not change the schema default to `any`.
- **Auxiliary services:** add `[[environment.sidecars]]` entries for Docker
  services that must run beside the primary sandbox. Sidecars support
  `docker_image` or `dockerfile`/`docker_build_context`, `command`,
  `environment`, `hostname`, `depends_on`, and `healthcheck`. Docker-backed
  workers start them on the same per-trial network as the main container and
  wait for declared healthchecks through the final Docker probe's timeout window
  before running the agent. Use `[environment].environment` for environment
  variables that belong on the primary sandbox container.
- **Sandbox create options:** `extra_hosts`, `dns`, and `tmpfs` are primary
  sandbox container options, applied when Docker-backed workers create the
  container. Use them only when the task definition depends on custom hostname
  resolution, resolver behavior, or tmpfs-backed paths. Example:
  ```toml
  [environment]
  dns = ["192.0.2.1"]
  tmpfs = ["/root:size=100M,mode=755"]

  [environment.extra_hosts]
  "example.com" = "131.25.18.2"
  ```

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
  `loom datasets sync-config`. For a user-owned benchmark folder, first
  run `loom datasets validate-local <folder>`; it validates every
  `task.toml` and prints the `[[local]]` snippet to add to the registry.
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
the benchmark as blocked instead of offering an evaluation run. Source license
metadata remains visible on benchmark/task rows, but service mode does not use
`team_quotas.license_allowlist` to reduce selectable counts or block submit. A
benchmark with no registered task rows appears as `Needs publish`; a benchmark
with raw legacy rows but no valid stored `TaskConfig` appears as
`Needs republish` with the raw-versus-runnable count.

For first-party adapter-backed benchmarks, prefer the manifest path:

```bash
loom datasets publish my-benchmark --hf-org "$LOOM_HF_ORG"
loom datasets register my-benchmark --hf-org "$LOOM_HF_ORG" \
    --db-url "$LOOM_DB_URL" \
    --mirror-to-object-store \
    --minio-endpoint "$LOOM_MINIO_ENDPOINT" \
    --minio-access-key "$LOOM_MINIO_ACCESS_KEY" \
    --minio-secret-key "$LOOM_MINIO_SECRET_KEY"
loom datasets audit my-benchmark --db-url "$LOOM_DB_URL" \
    --verify-bundles \
    --minio-endpoint "$LOOM_MINIO_ENDPOINT" \
    --minio-access-key "$LOOM_MINIO_ACCESS_KEY" \
    --minio-secret-key "$LOOM_MINIO_SECRET_KEY"
```

Legacy manifests without `task_config` remain metadata placeholders.
They must be republished or backfilled before users can launch them
from the batch UI.

For staging/staging/production, `register --mirror-to-object-store` is the
preferred path for HF-published benchmarks. It keeps Hugging Face as the
publication/provenance source, mirrors each task bundle into internal object
storage, stores `s3://...` runtime sources, and lets workers run without HF
tokens or direct HF egress.

For user-owned local benchmarks that do not need a Python adapter, use the
folder-first registry path instead:

```text
team-evals/
  benchmark.toml
  tasks/
    alpha/
      task.toml
      instruction.md
      solution/
      tests/
```

```bash
loom datasets validate-local "$LOOM_WORKER_FIXTURES_ROOT/team-evals"
loom datasets sync-config \
  --config ./config/benchmarks.toml \
  --fixtures-root "$LOOM_WORKER_FIXTURES_ROOT" \
  --db-url "$LOOM_DB_URL"
loom datasets audit team-evals --db-url "$LOOM_DB_URL"
```

`benchmark.toml` carries benchmark-level metadata. The default
`source_subdir` is `tasks`, so the DB task id is `team-evals/alpha` while the
worker materializes `fixture://team-evals/tasks/alpha`.

For production, publish the same validated folder to object storage instead of
depending on a worker fixture mount:

```bash
loom datasets publish-local ./team-evals \
  --db-url "$LOOM_DB_URL" \
  --minio-endpoint "$LOOM_MINIO_ENDPOINT" \
  --minio-access-key "$LOOM_MINIO_ACCESS_KEY" \
  --minio-secret-key "$LOOM_MINIO_SECRET_KEY" \
  --bucket loom-benchmarks
```

That path registers task sources such as
`s3://loom-benchmarks/team-evals/alpha/`, which the worker already materializes
through the object-store backend.

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

- **Solve script paths.** `solve.sh`, artifact collection, and verifier
  artifact access run under `[environment].workdir`, which defaults to
  `/workspace`. Write tests relative to that sandbox path, not the host
  fixture dir. If an upstream benchmark expects another root, declare it in
  `task.toml`; for example Terminal-Bench-2 uses `workdir = "/app"`. For
  code-completion benchmarks that already ship a canonical
  `solution/solution.py`, a no-op `solve.sh` is enough for oracle smoke; the
  pytest verifier does the correctness check.
- **Shared verifier env.** v1 runs the pytest verifier in the same
  container as the agent. The agent's image must therefore ship
  `pytest` (and any test deps) — see `hello-world/environment/Dockerfile`
  for the canonical preinstall.
- **Step IDs.** `name` becomes the step_id in the trajectory; keep it
  short and stable across runs so ATIF projection stays consistent.
- **Determinism.** Loom replays trajectories and re-projects ATIF on
  schema bumps. Tasks that depend on wall-clock time or random seeds
  must surface those values as artifacts so reviewers can audit.
