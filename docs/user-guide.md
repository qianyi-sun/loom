# User Guide — `loom` CLI

Everything a researcher needs to run LLMs against customizable tasks
from a laptop. Pluggable agents (use one of the 11 shipped harnesses
or write your own); pluggable task adapters (21 ship in the core package,
plus optional Terminal-Bench-2). One
`uv sync`, then `loom run`.

## Install

```bash
uv sync                                          # core deps; creates .venv/
uv pip install -e packages/loom-launcher \
               -e packages/loom-benchmarks \
               -e packages/loom-benchmark-terminal-bench-2
source .venv/bin/activate                        # so `loom` is on PATH
```

The last package is optional (TB-2 adapter). All others ship the core
21-adapter slate.

> Why `uv`? It's what the repo's tests + CI use; it creates the venv
> for you on first sync, sidestepping the PEP 668 "externally managed
> environment" error you get from `pip install` on modern
> Debian / Ubuntu. Install: <https://docs.astral.sh/uv/getting-started/installation/>.
> Prefer plain `pip`? Run `python3 -m venv .venv && source
> .venv/bin/activate` first, then `pip install -e . -e
> packages/loom-launcher -e packages/loom-benchmarks`.

## First run (smoke)

```bash
loom datasets list                # see what's available
loom config show                  # check token state
loom run \
  --task humaneval/HumanEval/0 \
  --agent oracle \
  --backend fake \
  --output-dir /tmp/loom-smoke \
  --json
```

That wires through every layer (config → adapter → task loader →
trial → object store → ATIF projection) without spending any cloud
budget. **Note**: `--backend fake` no-ops every sandbox `exec` — the
trial completes successfully but no real solver runs. Use it to
verify your install; use `--backend docker` or `--backend daytona`
for actual evaluation.

After the smoke succeeds:

```bash
ls /tmp/loom-smoke/<trial-id>/
  events.jsonl       # event-sourced trajectory (one JSON object per line)
  atif.json          # ATIF v1.7 projection
```

## Real evaluation runs

```bash
# Anthropic / OpenAI / Google API key
loom config set token.anthropic sk-ant-...
# Or via env: export ANTHROPIC_API_KEY=sk-ant-...
# Or add ANTHROPIC_API_KEY=sk-ant-... to the nearest project .env.

# Run a benchmark
loom run \
  --dataset swe-bench-verified \
  --agent claude-code \
  --model anthropic/claude-opus-4-7 \
  --backend docker \
  --concurrency 4 \
  --output-dir ./runs
```

On startup, the `loom` CLI walks up from the current directory to the
nearest git root or home directory and loads the first `.env` it finds
without overriding already-exported shell variables.

Per-trial outputs land at `./runs/<trial-id>/{events.jsonl,atif.json}`.
With `--json`, each trial's result also prints as a JSON line on stdout
for piping (e.g. `loom run ... --json | jq '.state'`).

In service mode, team-scoped provider connections are the normal path
for user-hosted OpenAI-compatible endpoints. Register the connection,
refresh or manually add its model ids, then launch from the SPA. The
SPA hides obvious tool/API entries by default and submits
`provider_connection_id` + `provider_model_id` with the trial or batch;
operators can use the raw model view for debugging noisy catalogs.
For complete setup recipes, including hosted third-party APIs and
Slurm/vLLM checkpoint deployment on a GPU cluster, see
[`provider-onboarding.md`](provider-onboarding.md).
Running the local service stack requires Docker CLI with the Compose
plugin; on macOS, install and start Docker Desktop, then verify
`docker compose version` before `loom service up`.

## Web sessions and teams

When Loom is served as a web platform, the SPA uses browser user sessions.
Users join from invite links or request access from Settings. Public beta does
not send automatic email links: an admin reviews requests, chooses a fixed
internal team and role, and manually shares the one-time invite link. If an
operator or local development server gives you a one-time login code, Settings
can also complete that browser sign-in. The service sets an HttpOnly session
cookie. Auth responses return a CSRF token that the SPA keeps in memory; the
browser sends cookies automatically, and mutating requests include the CSRF
header. You should not paste a raw bearer token into the production SPA.

Your current team controls execution, cost attribution, provider credentials,
members, and team API tokens. Roles are enforced by the API:

- `viewer` can read the current team's batches, trials, provider summaries, and
  usage.
- `member` can also submit work for the current team.
- `owner` can manage team API tokens and provider connections.
- `platform_admin` is an operator role with cross-team inspection/admin access.

The app shell always shows the current team and role beside the primary
navigation, so users can confirm which team will own new batches, provider
connections, invites, and API-token actions before they act. After sign-in,
Home is the default landing page. It summarizes team readiness, provider
health, benchmark readiness, active workers, recent batch/trial activity, and
separates user-owned next actions from operator-owned prerequisites. Team
Settings shows the signed-in user, current team, role, team switcher, joined
browser users, and role-aware setup links. Team owners get Team access for
invites and API tokens; platform admins also manage fixed internal teams and
approve pending access requests into a selected team and role. Members and
viewers see only the actions their role allows.

Team access is split into task-focused sections. Platform admins start on
pending requests, can switch to fixed-team maintenance, create/list invites,
manage API tokens, or inspect the audit log. Team owners see only invite and
API-token sections. Invite creation uses the visible team selector instead of a
raw team id field; the raw invite link is still revealed only once and must be
shared manually.

Most web workflows now include contextual quickstarts directly on the page. Use
the copyable snippets in Settings, Team access, Providers, New Batch, Monitor,
Batch Detail, Trial Detail, Run Library, Usage, Rate cards, Tasks, and
Benchmarks when you want the CLI/API equivalent for the page you are viewing.
Home intentionally avoids telling users to run operator import or worker
commands from the browser; those items appear as operator actions when the
service reports that platform prerequisites are missing. The examples use safe
placeholders and `env:`/`file:` secret references so users do not need to switch
back to this guide for the common path.

The CLI uses named team API tokens for service workflows. Create, rotate, or
revoke those tokens from Team access only as a team owner. The one-time reveal
shows CLI setup commands such as `export LOOM_API_TOKEN=loom_api_...` and
`loom auth login --server URL --token env:LOOM_API_TOKEN`. Use
`loom auth whoami` to verify the active server, team, scopes, and token prefix
without printing the raw token. Completed run metadata and safe artifacts are
shared across teams through the Run Library; ordinary batch, trial, trajectory,
ATIF, artifact, cancellation, rerun, and provider routes remain current-team
scoped.

Monitor lists show the owning team for each batch and trial. Ordinary users see
their current team's work; platform admins can use the team filter to inspect
cross-team queues without losing context. Monitor filters are reflected in the
URL so support links can preserve `view`, `state`, `q`, `batch_id`, `team_id`,
`benchmark_id`, `agent`, and `model` while switching between batch and trial
views.

### Public server CLI flow

From a fresh shell, authenticate with a scoped team API token from Team access:

```bash
export LOOM_API_TOKEN=loom_api_...
loom auth login --server https://loom.example.com --token env:LOOM_API_TOKEN
loom auth whoami
```

Provider keys also use indirection so secrets do not appear in shell history:

```bash
export OPENAI_API_KEY=sk-...
loom providers create \
  --name smoke-openai \
  --type openai-compatible \
  --base-url https://api.openai.com/v1 \
  --api-key env:OPENAI_API_KEY
loom providers test smoke-openai
loom providers models smoke-openai --refresh
```

Submit, monitor, inspect usage, and download through public `/api/v1` routes:

```bash
loom eval batch create \
  --name public-cli-smoke \
  --agent litellm \
  --provider smoke-openai \
  --model gpt-4o-mini \
  --benchmark humaneval \
  --n-per-task 1

loom eval batch show <batch-id>
loom eval trial list --state succeeded --limit 5
loom eval usage --start 2026-06-01 --end 2026-06-30
loom eval trial show <trial-id>
loom eval trial download <trial-id> --kind atif --output atif.json
loom eval trial download <trial-id> --kind trajectory --output events.jsonl
loom eval trial download <trial-id> --kind artifact \
  --artifact-key <artifact-key-from-trial-show> \
  --output artifact.bin
```

Auth and permission errors use the same remediation hint across CLI subcommands.
Text and JSON output redact raw bearer tokens, provider keys, internal service
hosts, and signed object-store URLs. `loom eval trial show` prints copyable
download commands instead of MinIO/S3 signed URLs.

## Run Library

Run Library is the org-wide place to inspect completed shared work. Team still
controls execution, cost, provider credentials, members, and API tokens. The
Library only exposes completed metadata and artifacts that passed sharing
checks.

Use the SPA top-level **Run Library** page:

- **My team** shows your team's library rows.
- **All teams** shows your team plus completed org-shared rows from other
  teams.
- Owner-team labels show who ran the original work.
- State, team, and artifact-type filters narrow the table without showing raw
  JSON payloads.

Open a Library row to inspect task selection, agent/model config, trial rollup,
provenance, and artifact groups. Safe shared artifacts expose Download, Copy
URL, and Reuse actions. Blocked artifacts show only a safe reason and cannot be
downloaded or reused by another team.

Clone config and reuse artifact both create new records in your current team and
record `source_provenance`. They do not copy the source team's provider
connection or credentials; choose a provider connection owned by your team when
the source config requires one. The Run Library detail page shows that selector
before cloning provider-backed work.

## Default views and diagnostics

The SPA defaults to readable summaries instead of raw API payloads.
New Batch explains task selection, agent/model combinations, backend,
and advanced trial settings in product terms. Batch Detail shows a
Run plan, Monitor shows planned trials and evaluator score, Run Library shows
org-wide completed shared work, and Trial Detail separates platform outcome
from evaluator reward. Batch Detail and Trial Detail also show owner team,
visibility/share status, and provenance when a run was cloned or reused.
For normal model-backed runs, New Batch starts from the provider connection and
model choice and uses Loom's default model runner internally. Users only open
`Use a specific agent` when they want a non-default runtime such as `oracle` or
another service-mode agent; choosing an agent that does not need a model hides
the provider/model controls.

When a finished batch has transient gateway failures, Batch Detail shows
`Rerun failed cases`. That action creates a linked rerun batch containing
only the failed task/sample/combination coordinates. The original batch
keeps its original trial counts, and Batch Detail also shows the effective
result after successful linked reruns replace those transient failures.

Raw data is still available when you need to debug or reproduce an API
request. Look for `Diagnostics`, `Raw event data`, or explicit
advanced disclosures. Those panels contain internal field names such
as `task_filter`, `trial_config`, trajectory event payloads,
fan-out errors, and rate-card payloads. They are intentionally closed
by default so the normal workflow stays focused on what was launched,
what is running, and what needs attention.

Provider pages use the same model. A connection marked `Ready` means
the last provider test passed. `Needs attention` means the last test
failed and batches using that connection may fail. `Untested` means
the connection has been saved but should be tested before real runs.
Allowed-model summaries distinguish unrestricted discovered models
from explicit allow-lists. Provider tabs are URL-addressable with
`?tab=overview`, `?tab=models`, and `?tab=settings`, so operators can link
directly to the relevant setup or debugging view.

Contextual snippets are not a substitute for diagnostics. If a quickstart
command fails, open the same page's diagnostic panel or detail view, then copy
the relevant `loom eval batch show`, `loom eval trial show`, `loom eval trial
download`, `loom providers test`, or `loom eval usage` command from the page
to reproduce the issue from a shell.

## Cloud sandboxes (Daytona)

```bash
export DAYTONA_API_KEY=...
loom run --backend daytona --dataset bfcl \
  --agent claude-code --model anthropic/claude-opus-4-7 \
  --output-dir ./runs
```

BFCL is a tool-use benchmark. Its task instructions require the agent to
write `agent_output.json` with a `calls` list, or a `turns` list for
multi-turn tasks; it does not use the `oracle` baseline because there is no
preseeded `solution/solve.sh`.

The reasoning and browsing benchmark adapters added for #307 use the same
catalog lifecycle but have different runtime assumptions:

- `gpqa` publishes the full official GPQA Extended set (546 rows) from the
  pinned `idavidrein/gpqa` repository and grades a final A-D answer letter.
- `hendrycks-math` publishes the full 5000-row MATH test split from the pinned
  `HuggingFaceTB/MATH` `all` config and grades the final boxed/exact answer.
- `mmlu-pro` publishes the full 12032-row MMLU-Pro test split and grades a
  final A-J option letter.
- `tau2-bench` publishes the default leaderboard task sets for airline,
  retail, and telecom (278 tasks). The task bundle includes domain assets and
  expects `agent_output.json` containing planned tool actions and user-facing
  messages; the verifier is deterministic, so operators should document any
  future switch to the upstream interactive simulator separately.
- `browsecomp` publishes the full 1266-question BrowseComp release from
  OpenAI simple-evals. It requires network/browsing capability at execution
  time and grades the `Exact Answer:` line deterministically.

Daytona usage rows land in the `cloud_compute_records` table when the
run is connected to a Loom service (`--server-url`). Standalone
laptop runs skip persistence — the trajectories + ATIF still drop on
local disk.

## Local LLMs (vLLM, ollama, llama.cpp, lm-studio)

Three paths, ordered by what most users reach for first.

### Path A: inline — your server is already running

Pass the server URL on the command line. No config, no state.

```bash
loom run --task humaneval/HumanEval/0 \
         --agent claude-code \
         --local-server http://localhost:8000/v1 \
         --model meta-llama/Llama-3.1-8B-Instruct \
         --backend docker
```

With `--local-server` present, `--model` is the upstream model id
verbatim — no `provider/` split, no `local/<name>/` dance. Loom
registers a transient `_inline` provider just for this trial.

Auth flag if the server requires it:

```bash
loom run ... --local-server http://my-vllm.internal/v1 \
             --local-api-key sk-foo
# or
LOOM_LOCAL_API_KEY=sk-foo loom run ...
```

CLI flag > env var > none. Cost defaults to `$0` (no rate-card row
for `local:_inline` by default; add one keyed
`provider="local:_inline"` if you want GPU attribution).

### Path B: managed — Loom starts vLLM for you

If you have weights on disk or a HuggingFace model id, install the
optional vLLM extra and pass the spec directly:

```bash
uv sync --extra vllm        # one-time; vLLM has GPU requirements

# HuggingFace model id
loom run --task humaneval/HumanEval/0 \
         --agent claude-code \
         --model hf:meta-llama/Llama-3.1-8B-Instruct \
         --backend docker

# Local weights directory — pass the path directly
loom run --task humaneval/HumanEval/0 \
         --agent claude-code \
         --model /data/checkpoints/my-llama-3.1-tune/ \
         --backend docker
```

Path detection (no `file:` prefix needed): `--model` starting with
`/`, `~`, `./`, or `../` is treated as a local weights directory.

Loom:
1. Finds a free TCP port (starting at 8234).
2. Starts vLLM as a subprocess (`vllm serve <model> --port ...`).
3. Polls `/v1/models` until healthy.
4. Reads the canonical model id vLLM advertises (so HF ids that get
   shortened by vLLM still resolve correctly).
5. Runs the trial against `local/_auto_vllm/<served-name>`.
6. Tears down vLLM at end-of-process (or on Ctrl-C / SIGTERM).

vLLM tuning (passed through to `vllm serve`):

| Flag | Default | Notes |
|---|---|---|
| `--vllm-port N` | 0 (auto-pick from 8234) | Pin if you need a predictable port |
| `--vllm-host HOST` | `127.0.0.1` (loopback) | Set to `0.0.0.0` to expose on the LAN |
| `--tensor-parallel-size N` | 1 | GPUs across which to shard |
| `--gpu-memory-utilization F` | 0.90 | 0.0–1.0 |
| `--max-model-len N` | model's max | Cap context window |
| `--enforce-eager` | off | Disable CUDA graphs (debug only) |
| `--keep-alive` | off | Leave vLLM running after the trial (handy when iterating against the same model). Only meaningful for single-`--model` runs; multi-model loops always tear down each iteration's server. |

### Path C: persisted — same server, many runs

If you target the same server every day, persist it once and skip
typing the URL:

1. **Start your local server.** Examples:
   ```bash
   vllm serve meta-llama/Llama-3.1-8B-Instruct
   # or
   ollama serve  &&  ollama pull llama3.1
   # or
   ./llama-server -m model.gguf --port 8080
   ```
2. **Register it with Loom** (one-time):
   ```bash
   loom config set local.vllm.base_url http://localhost:8000/v1
   loom config set local.vllm.api_key sk-foo   # optional
   ```
   `vllm` is your chosen name; register several
   (`local.ollama.base_url ...`, `local.lmstudio.base_url ...`)
   and pick one per trial.
3. **Sanity-check it's reachable** (recommended):
   ```bash
   loom models test local/vllm
   # → ✓ vllm reachable at http://localhost:8000/v1
   #     models advertised by /v1/models: 1
   #       • meta-llama/Llama-3.1-8B-Instruct
   ```
4. **Run a trial.** Model spec is `local/<server>/<model_id>` where
   `<model_id>` is what `/v1/models` returns:
   ```bash
   loom run \
     --task humaneval/HumanEval/0 \
     --agent litellm \
     --model local/vllm/meta-llama/Llama-3.1-8B-Instruct \
     --backend docker
   ```

`loom models list` shows everything currently configured (remote
providers + local servers, redacted keys).

**Cost tracking:** local trials default to `$0` (no upstream API
cost). If you want internal cost-accounting against your own GPU
rates, add a row to `~/.config/loom/rate-cards.toml`:

```toml
[[entries]]
provider = "local:vllm"          # "local:" prefix + your server name
model = "meta-llama/Llama-3.1-8B-Instruct"
input_per_mtok = 0.10            # your own $/Mtok numbers
output_per_mtok = 0.30
cache_read_per_mtok = 0.0
cache_write_per_mtok = 0.0
```

Service-mode operators register local providers via env vars:
`LOOM_GW_LOCAL_<NAME>_BASE_URL=http://...` (+ optional
`LOOM_GW_LOCAL_<NAME>_API_KEY`). Route-level Gateway dispatch for
`model=local/...` is the natural follow-up; today, service-mode
clients reach local servers via `loom run` on the agent's host.

## Comparing multiple models

`loom run --model A --model B` runs the same tasks against each
model. Default behavior:

- **Sequential** — load A, run all tasks, unload A, then load B.
  One vLLM in GPU memory at a time. Peak memory = max(A, B).
- **Output bucketing** — trials land under
  `<output-dir>/<model-slug>/<trial-id>/`. The slug is derived from
  the model spec (HF basename, path basename, or registered name).
- **Exit code** — `max` of all per-model exit codes, so any failure
  surfaces.

Opt into multi-GPU parallel:

```bash
loom run ... --model hf:A --model hf:B --parallel-models
```

You're responsible for ensuring enough GPU memory for both. Loom
does not auto-partition `--gpu-memory-utilization` between models.

## `loom serve` reference

Foreground command that launches vLLM and registers it as
`local/<name>`. No daemon, no PID files; closing the terminal
closes the server.

```
loom serve <spec> [--name NAME] [vLLM tuning flags]
```

- `<spec>`: `hf:<org>/<name>` or a path (`/`, `~`, `./`, `../`).
- `--name`: registration name; defaults to a slug of the spec.
- Tuning flags: `--vllm-port`, `--vllm-host`,
  `--gpu-memory-utilization`, `--tensor-parallel-size`,
  `--max-model-len`, `--enforce-eager`.

On startup, writes:

```toml
[local_providers.<name>]
base_url = "http://localhost:<port>/v1"
served_model_name = "<canonical>"
```

to `~/.config/loom/config.toml`. On shutdown (Ctrl-C, SIGTERM, or
vLLM crash), the entry is removed.

## `loom datasets` reference

```
loom datasets list                  # union: installed + catalog + remote
loom datasets list --installed      # only adapters in this venv
loom datasets list --available      # only catalog entries not installed
loom datasets list --remote         # only entries from LOOM_SERVER_URL/api/v1/benchmarks
loom datasets list --json           # machine-readable output

loom datasets show <slug>           # full detail for one adapter
loom datasets install <slug>        # pip-install a catalog entry
loom datasets refresh-catalog      # drop the 24h catalog HTTP cache
loom datasets provision-public-beta-catalog
                                    # copy ready catalog rows + S3 bundles
                                    # into a public-beta/release target
```

Discovery sources, in precedence order (builtin wins on slug
conflict):

1. **builtin** — `pip install`'d packages declaring
   `[project.entry-points."loom.benchmarks"]`
2. **remote** — `GET <LOOM_SERVER_URL>/api/v1/benchmarks` when a Loom
   service is reachable
3. **catalog** — `src/loom_cli/catalog_data/default-catalog.json`
   (override via `--catalog-url` or `LOOM_CATALOG_URL`)

### Operator-registered benchmarks via `config/benchmarks.toml`

Operators can register two non-adapter benchmark shapes without
writing Python:

- **`[[local]]`** — point at a folder of `task.toml` bundles on the
  worker's `fixtures_root`. The folder becomes a benchmark; each
  bundle becomes a task. User-authored folders can include a
  `benchmark.toml` file and keep bundles under `tasks/`; validate them
  with `loom datasets validate-local PATH` before syncing.
- **`[[remap]]`** — reuse an existing adapter's parsing against a
  different upstream (e.g., a HumanEval fork).

The file lives at `<repo>/config/benchmarks.toml` in dev or
`/etc/loom/benchmarks.toml` in production (override with
`$LOOM_BENCHMARKS_CONFIG_PATH`). Both shapes flow through
`loom datasets sync-config` (manual) and `loom service up`
(automatic, dev compose only). See
[`architecture/benchmark-adapter.md`](architecture/benchmark-adapter.md)
under "Operator-facing TOML registry" for the schema and worked
examples.

Minimal user-owned local benchmark flow:

```bash
# Layout: $LOOM_WORKER_FIXTURES_ROOT/team-evals/benchmark.toml and
# $LOOM_WORKER_FIXTURES_ROOT/team-evals/tasks/<task-id>/task.toml
loom datasets validate-local "$LOOM_WORKER_FIXTURES_ROOT/team-evals"

# Copy the printed [[local]] snippet into config/benchmarks.toml, then sync.
loom datasets sync-config \
  --config ./config/benchmarks.toml \
  --fixtures-root "$LOOM_WORKER_FIXTURES_ROOT" \
  --db-url "$LOOM_DB_URL"

loom datasets audit team-evals --db-url "$LOOM_DB_URL"
```

The validated folder path is the same path the worker materializes later.
With `source_subdir = "tasks"`, the task row is named
`team-evals/<task-id>` while its source points at
`fixture://team-evals/tasks/<task-id>`.

Production deployments should publish the same validated folder to object
storage instead of relying on a shared worker fixture mount:

```bash
loom datasets validate-local ./team-evals
loom datasets publish-local ./team-evals \
  --db-url "$LOOM_DB_URL" \
  --minio-endpoint "$LOOM_MINIO_ENDPOINT" \
  --minio-access-key "$LOOM_MINIO_ACCESS_KEY" \
  --minio-secret-key "$LOOM_MINIO_SECRET_KEY" \
  --bucket loom-benchmarks

loom datasets audit team-evals --db-url "$LOOM_DB_URL"
```

`publish-local` uploads each task bundle under
`s3://loom-benchmarks/team-evals/<task-id>/` and registers task rows with those
sources. The worker uses the existing object-store materializer at runtime. If a
task declares `environment.dockerfile`, that Dockerfile and its build context
are part of the uploaded bundle; the worker builds and caches a deterministic
`loom-task:<hash>` image before the first service-mode trial that needs it.
Tasks may narrow the Docker build root with `environment.docker_build_context`,
and build-only contexts under `.loom-build/` are not uploaded into the agent
workspace. Tasks can also declare `environment.sidecars` for Docker services
that must run on the same per-trial network as the primary sandbox. If the
uploaded build context exceeds worker operator limits, the trial fails in setup
with a diagnostic that names `LOOM_TASK_IMAGE_BUILD_MAX_FILES` or
`LOOM_TASK_IMAGE_BUILD_MAX_BYTES`.

For a cheap oracle smoke that does not call a model, omit provider/model flags:

```bash
loom eval batch create \
  --name team-evals-oracle-smoke \
  --agent oracle \
  --benchmark team-evals \
  --n-per-task 1
```

For model-backed agents, include a provider connection and model:

```bash
loom eval batch create \
  --name team-evals-litellm-smoke \
  --agent litellm \
  --provider smoke-openai \
  --model gpt-4o-mini \
  --benchmark team-evals \
  --n-per-task 1
```

### Benchmark readiness audit

Operators can inspect why a benchmark is or is not runnable without
opening the SPA:

```bash
loom datasets audit --all --db-url "$LOOM_DB_URL"
loom datasets audit swe-bench-verified --db-url "$LOOM_DB_URL"
loom datasets audit --all --db-url "$LOOM_DB_URL" --json
python scripts/benchmark_reward_gate.py readiness \
  --server-url "$LOOM_SERVER_URL" \
  --token env:LOOM_API_TOKEN
```

The audit reports raw task rows, valid `TaskConfig` rows, source
schemes, materializer status, readiness state, and blocker reason. A
legacy published manifest with task rows but empty `config` appears as
`blocked` with `manifest_legacy_missing_task_config`; republish or
backfill the benchmark before launching user batches.

The same readiness model powers the service catalog. In New Batch, a benchmark
marked `Ready` is selectable. `Needs publish` means no runnable tasks are
registered yet. `Needs republish` means raw task rows exist but their stored
configs are not valid `TaskConfig` objects. Source license metadata is visible
on catalog/task rows but does not disable benchmarks or block submit. Disabled
rows show the API-provided readiness message and
raw-versus-runnable counts so operators know whether to publish, republish,
or repair a benchmark. `Not supported yet` means the benchmark is intentionally
visible but excluded from the current supported runtime surface; it cannot be
selected until the listed runtime work lands. `Deferred` means the benchmark is
visible for roadmap transparency but intentionally outside the current
supported scope until the listed product or data-access follow-up lands.
Required public benchmarks should move from publish/repair pending states to
`Ready`; hiding a needed benchmark is not a substitute for publishing it.

The hidden `/benchmarks` route remains a power-user diagnostic view rather
than the normal submission path. It includes the same registry readiness states
plus operator-oriented `loom datasets list --remote`, `loom datasets audit`,
and `loom datasets sync-config --dry-run` snippets that use token and database
environment references instead of raw secrets.

After a supported-benchmark acceptance batch finishes, operators can verify the
reward contract from the public API:

```bash
python scripts/benchmark_reward_gate.py batch \
  --server-url "$LOOM_SERVER_URL" \
  --token env:LOOM_API_TOKEN \
  --batch-id "$BATCH_ID"
```

This gate checks that the batch is terminal, fan-out produced the expected
number of trials, and every succeeded trial has a numeric reward. It treats
model-correctness scores such as `0` as valid verifier output, but fails
missing rewards and benchmark-side verifier or environment failures.

For public-beta or release deployments, operators should provision the ready
catalog with `loom datasets provision-public-beta-catalog` before inviting
users. That command copies only runnable benchmark/task rows and their
referenced `s3://` bundles from a known-good source environment into the target
database/object store. It is separate from `scripts/seed_test_data.py`, which is
only for disposable auth/run fixtures.

Batch fan-out still treats deterministic non-license policy/config failures as
terminal defense-in-depth instead of leaving the batch `submitted`; open Batch
Detail or run `loom eval batch show <id>` to inspect `failure_reason`,
`failure_message`, and `fanout_errors`.

If a model gateway returns transient `502`, `503`, `504`, timeout, or
connection-reset failures, Loom retries the agent call within the trial
before marking the trial failed. If retry budget is exhausted, rerun the
remaining failed cases from Batch Detail instead of launching the whole
batch again.

## `loom config` reference

Config persists to `$XDG_CONFIG_HOME/loom/config.toml` (defaults to
`~/.config/loom/config.toml`). Tokens are redacted in `loom config show`.

```
loom config set token.<provider> <key>    # provider in {anthropic, openai, google}
loom config set server_url <url>          # optional Loom service URL
loom config show
```

Env vars override config: `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`,
`GOOGLE_API_KEY`, `LOOM_SERVER_URL`, `LOOM_API_TOKEN`.

## `loom run` reference

```
loom run
  --dataset SLUG          # run all tasks in the dataset
  --task SLUG/INSTANCE    # run one task; instance-id may contain '/'
                          # (e.g. humaneval/HumanEval/0, swe-bench-verified/django__django-12345)
  --split SPLIT           # default "test"
  --agent NAME            # oracle | litellm | <launcher adapter>
  --model PROVIDER/NAME   # e.g. anthropic/claude-opus-4-7
  --backend BACKEND       # docker | fake | daytona
  --concurrency N         # parallel trials (default 1)
  --output-dir DIR        # default ./runs
  --json                  # JSON-line output instead of text
  --server-url URL        # also POST results to <url>/api/v1/cli/results
  --tb2-report PATH       # also write Terminal-Bench-2.0 canonical JSON
```

`--dataset` and `--task` are mutually exclusive (one required).

`--agent`:
- `oracle` — runs `solution/solve.sh` from the task bundle (reference
  baseline, no model call)
- `litellm` — talks to the configured model via LiteLLM dialect routing
- any other name — resolved via `loom_launcher.get_adapter(name)`;
  ships 11 concrete adapters (claude-code, codex, openhands, aider,
  opencode, swe-agent, mini-swe-agent, openhands-sdk, gemini-cli,
  qwen-cli, kimi-cli)

In service mode, the SPA and API only allow launch for agents whose
runtime is ready in the deployed worker/sandbox contract. `GET
/api/v1/agents` includes `service_mode_ready`, `readiness_message`, and
`runtime_contract` metadata. The New batch form marks unavailable
agents as `setup needed`, and submit routes reject bypassed requests
with the same setup message instead of creating a batch that can only
fail in the worker.

Before enabling an external agent runtime, audit the sandbox image that
will run the task:

```bash
loom agents audit-runtime --image python:3.11-slim
loom agents audit-runtime --image my-agent-sandbox:dev --agent opencode --json
```

Operators can build the repo's candidate agent-capable sandbox image for
service-mode smoke testing:

```bash
docker build -f deploy/Dockerfile.agent-sandbox -t loom-agent-sandbox:dev .
loom agents audit-runtime --image loom-agent-sandbox:dev --json
loom agents smoke-runtime --image loom-agent-sandbox:dev --json
```

That image installs the external CLI and Python dependencies declared by
the agent catalog. `audit-runtime` checks dependency presence inside the
named image; `smoke-runtime` then runs the selected agents through a
minimal platform trial with a deterministic provider stub. If the audit
reports `blocked`, the image is still missing a declared runtime
dependency or the adapter contract no longer matches the upstream package.
The `openhands` and `openhands-sdk` adapters intentionally use Loom's
`loom_launcher.openhands_sdk_runner` module because upstream OpenHands SDK
ships a Python library, not a stable one-shot CLI. When these adapters are
installed dynamically on a benchmark task image, Loom creates a dedicated
Python 3.12 venv under `/opt/loom-agents/openhands-sdk` and installs
`loom-launcher` from a pinned repository subdirectory ref; the provider/model you
select is still injected at trial runtime through the gateway environment and
adapter arguments, not baked into the task image.

The audit command exits `0` only when every audited agent is ready for the
named image. It exits `1` when any agent is still `blocked` by missing
dependencies or `gated` by the catalog readiness flag, and `2` for usage
errors such as an unknown agent name. A passing audit proves dependency
presence; close the loop with `smoke-runtime` or a normal trial/batch
smoke before treating the agent as fully supported.

In service mode, workers install the chosen adapter's CLI into the
trial sandbox at spawn time on top of the benchmark's `task_image`.
The first trial of a new `(task_image, agent)` combination takes a
few extra minutes (package installs); subsequent trials hit the
content-addressed cache and start instantly. See
[`docs/operator-runbook.md#trial-cache-per-trial-agent-install`](operator-runbook.md#trial-cache-per-trial-agent-install)
for the operator-side knobs.

## Rate cards

Rate cards live at `~/.config/loom/rate-cards.toml`, seeded on first
run from `src/loom_cli/data/default-rate-cards.toml`. Edit the file
to add or override entries (e.g. for a self-hosted model or a
provider Loom doesn't ship a default for). Cost is computed locally
from these rates plus the token counts returned by the provider SDK.

<a id="pasting-task-ids"></a>

## Pasting task ids

The SPA's New batch form has an "Explicit task ids" subset mode
that accepts a paste box. The parser handles every format users
tend to paste from — notebooks, spreadsheets, chat messages, URLs,
ranges, JSON arrays. All of the following paste cleanly:

- **One per line** —
  ```
  HumanEval/0
  HumanEval/1
  HumanEval/2
  ```
- **Comma / semicolon / pipe / tab / 2+-space separated** —
  `HumanEval/0, HumanEval/1, HumanEval/2`
- **JSON array** (single or double quotes) —
  `["HumanEval/0", "HumanEval/1"]`
- **Python list literal** (trailing commas OK) —
  `['HumanEval/0', 'HumanEval/1',]`
- **Range shorthand** — `HumanEval/0-4` expands to 0..4.
- **Prefix shorthand** — `HumanEval/0,1,2,3` expands to 0..3.
- **Mixed range + list** — `HumanEval/0-2, HumanEval/3, HumanEval/4`.
- **Markdown bullets** — `-`, `*`, `•`, `→`, `>`, numbered `1.` /
  `2.`.
- **Markdown single-column table** — header + separator + rows.
- **CSV with header** — first column wins, sibling columns dropped.
- **Triple-backtick code fences** — `` ``` `` lines are stripped,
  contents kept.
- **`#` comments** — everything after `#` on a line is dropped.
- **URL prefixes** — `/api/v1/tasks/` and `/tasks/` are stripped.

After parsing, the result is sorted + deduplicated. The preview
line below the textarea shows `Parsed N ids` (or a red error
naming the first offending segment).

The "Validate against catalog" button does a single
`GET /api/v1/tasks?task_ids=...` roundtrip and surfaces any
unknown ids inline; submission itself validates server-side, so
the catalog check is purely a UX prefetch.

## Troubleshooting

**`HfUriError: Repository id must be 'namespace/name'`** — older
`loom-benchmarks` releases shipped two HuggingFace adapters
(`humaneval`, `mbpp`) with unnamespaced upstream IDs that newer
`huggingface_hub` versions reject. Upgrade `loom-benchmarks` to a
release that pins `openai/openai_humaneval` and
`google-research-datasets/mbpp`.

**`no tasks selected (dataset='X' task='Y')`** — the `--task` form is
`<dataset-slug>/<instance-id>`. HumanEval instance ids contain a
slash (`HumanEval/0`); SWE-Bench instance ids contain double
underscores (`django__django-12345`). Check the upstream dataset for
the exact form.

**`DAYTONA_API_KEY or DAYTONA_JWT_TOKEN must be set`** — `--backend
daytona` requires credentials. Either `loom config set
token.daytona <key>` is not currently supported (the CLI reads
`DAYTONA_*` env vars directly); export `DAYTONA_API_KEY` before
invoking `loom run`.

**`loom.benchmarks entry-point '<name>' failed to load`** — a
pip-installed adapter package raised at import time. Try `pip
install --force-reinstall <package>` to refresh the entry-points
metadata, or `loom datasets list --installed` to confirm which
adapters are loadable.
