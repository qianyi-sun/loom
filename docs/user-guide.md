# User Guide

Everything a researcher needs to run agents against benchmarks with Loom, from
the CLI or the web app. This guide covers install, quickstarts for the four
common workflows, sandbox backends, model sources (hosted APIs + local LLMs),
web platform workflows, benchmark and dataset management, CLI reference, and
troubleshooting.

Loom's product boundary and architecture are in the
[repo README](../README.md). Provider setup for hosted APIs and self-hosted
GPU-cluster vLLM lives in [`provider-onboarding.md`](integrations/provider-onboarding.md).

> **Cross-repo issue/PR refs:** bare `#N` in this guide may point to the
> pre-2026-06-26 `carinrc/loom` archive tracker (numbering was reset on
> the new canonical repo `qianyi-sun/loom`). See
> [`repo-migration.md`](contributing/repo-migration.md).

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

## Choose your quickstart

Pick the path that matches what you have:

| You have… | Follow |
|---|---|
| No stack at all, just a task + a model key, throwaway run | [Laptop-only `loom run`](#quickstart-laptop-only-loom-run) |
| No account; you want to run the full stack on your own machine | [Run Loom locally](#quickstart-run-loom-locally) |
| An account on a running Loom and prefer a terminal | [Submit from the CLI to a Loom server](#quickstart-submit-from-the-cli-to-a-loom-server) |
| The same account and want clicks | [Submit from the web app](#quickstart-submit-from-the-web-app) |

The web and CLI paths submit into a running service and persist
trajectories/ATIF/usage server-side. `loom service up` runs that same service
stack against local Docker + Postgres + MinIO. `loom run` is a one-shot
in-process trial that writes `events.jsonl` + `atif.json` to a local directory
and exits — no DB, no team, no provider registration.

## Quickstart: Laptop-only `loom run`

For a local one-off experiment without the service stack.

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

Real evaluation runs:

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

Model-backed local run against an already-running OpenAI-compatible server:

```bash
loom run --task humaneval/HumanEval/0 \
  --agent claude-code \
  --local-server http://localhost:8000/v1 \
  --model meta-llama/Llama-3.1-8B-Instruct \
  --backend docker
```

Full local LLM workflows (auto-starting vLLM, comparing multiple models) live
in [Model sources → Local LLMs](#local-llms-vllm-ollama-llamacpp-lm-studio).

## Quickstart: Run Loom Locally

Local service mode is the fastest way to run the full stack for development or
demo preparation.

Prerequisites:

- Python 3.12;
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/);
- Docker CLI with the Compose plugin;
- Docker Desktop running on macOS.

Install (see [Install](#install) above), then start the service stack:

```bash
cp .env.example .env
# Edit .env if you want default provider keys for local testing.
loom service up
```

`loom service up` starts Postgres, MinIO, LLM Gateway, Control Plane,
`loom_service`, Worker, and the React SPA. It runs migrations, seeds local
tokens, and prints endpoint URLs.

Default local URLs:

| What | URL |
|---|---|
| Web app | http://localhost:5173/ |
| API root | http://localhost:8090/ |
| Swagger UI | http://localhost:8090/docs |
| Health | http://localhost:8090/api/v1/health |
| MinIO console | http://localhost:9001/ |

Tear down:

```bash
loom service down      # preserve volumes
loom service down -v   # remove Postgres + MinIO volumes
```

Local compose is for development, not public hosting. Production or shared
deployments use `loom cluster` with Kubernetes, TLS Ingress, protected
environment secrets, and release-promotion evidence. See
[`operator-runbook.md`](runbooks/operator-runbook.md).

Running the local service stack requires Docker CLI with the Compose
plugin; on macOS, install and start Docker Desktop, then verify
`docker compose version` before `loom service up`.

## Quickstart: Submit from the CLI to a Loom Server

Use this path from any machine that can reach the Loom public API.
The CLI machine does not need GPUs, model weights, or direct access to the
benchmark workers; it only needs network access to the Loom service and a team
account.

Install the CLI (see [Install](#install)), then authenticate with your
approved username/password account. The CLI uses the same account as the web
app; if you don't have one yet, follow the request-access flow in
[Web sessions and teams](#web-sessions-and-teams) first.

```bash
export LOOM_PASSWORD=...
loom auth login --server https://yylx.world/dev --username USER --password env:LOOM_PASSWORD
loom auth whoami
```

Register a hosted OpenAI-compatible provider:

```bash
export PROVIDER_API_KEY=sk-...

loom providers create \
  --name smoke-openai \
  --type openai-compatible \
  --base-url https://api.openai.com/v1 \
  --api-key env:PROVIDER_API_KEY

loom providers test smoke-openai
loom providers models smoke-openai --refresh
loom providers models smoke-openai --preflight gpt-4o-mini
```

Provider owners can share a connection with another team without copying or
revealing the API key:

```bash
loom providers share smoke-openai --target-team-id <team-uuid>
loom providers unshare smoke-openai --target-team-id <team-uuid>
```

The target team can list, select, and use the shared provider. Only the owner
team or a platform admin can update, rotate, test, refresh, hide, unhide, or
delete it. During a run, the LLM Gateway uses the owner-side stored secret but
records calls and cost against the consuming team/user for the submitted trial.

Submit, monitor, inspect usage, and download through public `/api/v1` routes:

```bash
loom eval batch create \
  --name-suffix public-cli-smoke \
  --agent litellm \
  --provider smoke-openai \
  --model gpt-4o-mini \
  --benchmark humaneval \
  --n-per-task 1

loom eval batch show <batch-id>
loom eval diagnose batch <batch-id>
loom eval batch debug <batch-id> --format json
loom eval batch rerun-plan <batch-id> --format text
loom resources status
loom eval trial list --state succeeded --limit 5
loom eval usage --start 2026-06-01 --end 2026-06-30 --include-batches
loom eval usage --start 2026-06-01 --end 2026-06-30 \
  --provider-connection-id <provider-id> \
  --model qwen3.6-35b-a3b \
  --benchmark-id skilllearnbench \
  --breakdown-by pricing_mode
loom eval usage --start 2026-06-01 --end 2026-06-30 \
  --pricing-mode failed-upstream
loom eval usage --start 2026-06-01 --end 2026-06-30 \
  --batch-id <main-batch-id> \
  --include-batch-family \
  --include-batches
loom eval trial show <trial-id>
loom eval diagnose trial <trial-id>
loom eval trial debug <trial-id> --format json
loom eval trial download <trial-id> --kind atif --output atif.json
loom eval trial download <trial-id> --kind trajectory --output events.jsonl
loom eval trial download <trial-id> --kind artifact \
  --artifact-key <artifact-key-from-trial-show> \
  --output artifact.bin
```

Trial downloads always go through the service API. The service proxies stored
trajectory and ATIF objects when present; for newer Postgres-backed trials it
can also reconstruct trajectory JSONL and ATIF from durable `trial_events`,
gateway `llm_calls`, and the trial terminal state/result without exposing
MinIO/S3 URLs. Reconstructed downloads use clean trial-wide sequence numbers and
preserve synthetic LLM usage summaries even when the original object copy is
missing. If ATIF projection metadata is missing, the ATIF download returns a
structured conflict while trajectory download remains available for debugging.

For a self-hosted inference service, register the final reachable `/v1` URL the
same way:

```bash
export SELF_HOSTED_API_KEY=...

loom providers create \
  --name lab-vllm \
  --type openai-compatible \
  --base-url https://inference.example.com/v1 \
  --api-key env:SELF_HOSTED_API_KEY

loom providers test lab-vllm
loom providers models lab-vllm --refresh
loom providers models lab-vllm --preflight qwen2.5-coder-7b-instruct
```

If the model server does not expose a useful `/models` catalog, add the model id
manually from the provider page or provider model API before submitting a batch.
Preflight that cached model before launching large batches.

### Delivery bundles for release handoff

For release handoff or customer delivery, create one deterministic bundle for a
finished batch family instead of downloading trial objects one by one:

```bash
loom eval batch delivery-bundle <batch-id> \
  --supplemental-batch-id <linked-rerun-batch-id> \
  --output delivery-bundle.tar.gz
```

Use `--mode raw-harbor` when the handoff needs raw provider and Harbor-style
execution artifacts for training or audit without changing Loom-native schemas:

```bash
loom eval batch delivery-bundle <batch-id> \
  --mode raw-harbor \
  --output raw-harbor-delivery.tar.gz
```

Use `--mode raw-harbor-tb2-v1` when the downstream consumer expects the
versioned TB2/Phase 1 delivery profile:

```bash
loom eval batch delivery-bundle <batch-id> \
  --mode raw-harbor-tb2-v1 \
  --output raw-harbor-tb2-v1-delivery.tar.gz
```

Use `--mode raw-harbor-tb2-v2` when the trial trajectory contains typed
`terminus2_*` events from the Harbor checkpoint bridge. This mode writes
execution history from typed events (not provider-log synthesis), keeps model
input in `model_input_trajectory.json`, and embeds hash-verified native Harbor
artifacts under `native/`:

```bash
loom eval batch delivery-bundle <batch-id> \
  --mode raw-harbor-tb2-v2 \
  --output raw-harbor-tb2-v2-delivery.tar.gz
```

Live staging validation for `raw-harbor-tb2-v2` is still pending; continue
using `raw-harbor-tb2-v1` for provider-log-based exports until typed-event
batches are available.

`delivery-bundle` asks the service to choose the final trial for each
task/sample/combination coordinate across the main batch and any explicit
supplemental rerun batches. Later linked reruns replace earlier failed attempts
only at the same coordinate, so the manifest preserves the lineage from the
selected trial back to its source batch. Before the archive is marked ready, the
service verifies that every selected trajectory and ATIF object can be read
through object storage. The command downloads the archive, verifies the exposed
SHA-256, writes `<archive>.sha256`, and exits non-zero with the service's
structured error if a referenced object is missing or a coordinate still has no
successful final trial. The service builds archives through a bounded spool and
streams object-store bodies into the tar writer; the CLI streams downloads to
disk while hashing chunks. Large raw exports should not require memory
proportional to total archive size.

Each archive contains:

- `manifest.json` with source batch ids, selection rule, trial counts, object
  counts, selected lineage, and storage/checksum evidence for referenced
  objects. The archive SHA-256 is exposed by the API/CLI, artifact metadata,
  and the `.sha256` sidecar rather than self-referentially inside the tarball.
- `summary.json` with the same high-level delivery metadata for automation.
- `ledger/trials.jsonl` and `ledger/trials.csv` for reviewer-friendly trial
  lineage, rewards, object paths, and checksums.
- `checksums/SHA256SUMS` with SHA-256 digests for the archive payload files.
- `trajectories/<task>/<trial-id>.events.jsonl` and
  `atif/<task>/<trial-id>.atif.json` for the selected final trials.

Raw Harbor modes keep those lightweight files and add:

- `provider_logs/manifest.json` plus redacted raw provider request/response
  logs captured on the Loom Gateway path.
- `task_bundles/<task_id>/...` for object-store task bundle inputs when the
  selected task source is available as an `s3://` bundle.
- `agent_runs/<task_id>/<trial_id>/execution_result.json`, `metrics.json`,
  `artifact_manifest.json`, `verifier_output.json`,
  `provider_logs_manifest.json`, and `atif.json`.
- `derived/sft_messages.jsonl`, derived from the redacted provider logs for
  downstream SFT-style pipelines.

The base `raw-harbor` mode preserves the Loom-native event stream as
`agent_runs/<task_id>/<trial_id>/trajectory.jsonl` and leaves assistant payload
schemas unchanged. The `raw-harbor-tb2-v1` profile instead writes
`trajectory.json` reconstructed from provider logs plus
`loom_trajectory.jsonl` as the raw timing/audit spine, and normalizes
Loom/Terminus assistant action keys such as `state_analysis`, `explanation`,
`timeout_sec`, and `is_task_complete` into the TB2-facing `analysis`, `plan`,
`duration`, and `task_complete` schema. The `raw-harbor-tb2-v2` profile
projects execution from typed `terminus2_*` events, writes
`model_input_trajectory.json` and `terminal_transcript.jsonl`, omits
`derived/sft_messages.jsonl`, and includes native Harbor files under
`agent_runs/<task_id>/<trial_id>/native/`.

The same flow is available in the SPA Batch Detail page through the **Delivery
bundle** card. API clients can call
`POST /api/v1/batches/{id}/delivery-export` with optional
`mode` (`lightweight`, `raw-harbor`, `raw-harbor-tb2-v1`, or
`raw-harbor-tb2-v2`) and
`supplemental_batch_ids`, poll
`GET /api/v1/batches/{id}/delivery-export`, and download through the returned
route-aware `/api/v1/batches/.../delivery-export/.../download` URL. On hosted
staging/prod this URL includes the public route prefix, for example
`https://yylx.world/dev/api/v1/...` or `https://yylx.world/prod/api/v1/...`.
Creating a bundle requires submit/admin scope; reading or downloading an
existing bundle only requires normal read access. These routes are team-scoped
and never expose raw MinIO/S3 URLs.

### Cross-team submission (platform admins)

`loom eval batch create` can omit `--name`; the service derives a concise
name and description from the benchmark/subset, combinations, provider/model,
and backend. Use `--name-suffix` only when you need an extra human label after
the generated prefix.

For an agent × provider-model matrix, submit API-shaped combinations directly.
Each combination can carry its own `provider_connection_id` and
`provider_model_id`; batch-level provider fields are only defaults for legacy
single-provider submissions:

```bash
loom eval batch create \
  --name-suffix glm-vs-qwen \
  --benchmark humaneval \
  --combinations-json '[
    {
      "label": "terminus-glm",
      "agent_name": "terminus-2",
      "agent_model": {"provider": "openai", "name": "glm-5.1-thinking", "source": "api"},
      "provider_connection_id": "11111111-1111-4111-8111-111111111111",
      "provider_model_id": "glm-5.1-thinking",
      "n_per_task": 1
    },
    {
      "label": "opencode-qwen",
      "agent_name": "opencode",
      "agent_model": {"provider": "openai", "name": "qwen3.6-35b-a3b", "source": "api"},
      "provider_connection_id": "33333333-3333-4333-8333-333333333333",
      "provider_model_id": "qwen3.6-35b-a3b",
      "n_per_task": 1
    }
  ]'
```

Provider connections are team-scoped. A platform admin using a user-owned API
token can submit on behalf of a different team by passing `--team-id`; this
also scopes provider-name lookup so the selected provider must belong to the
target team:

```bash
loom eval batch create \
  --team-id <target-team-id> \
  --name-suffix admin-provider-smoke \
  --agent litellm \
  --provider smoke-openai \
  --model gpt-4o-mini \
  --benchmark humaneval \
  --n-per-task 1
```

Auth and permission errors use the same remediation hint across CLI subcommands.
Text and JSON output redact raw bearer tokens, provider keys, internal service
hosts, and signed object-store URLs. `loom eval trial show` prints copyable
download commands instead of MinIO/S3 signed URLs.

### Diagnosis and debug evidence

Use `loom eval diagnose batch <batch-id>` or
`loom eval diagnose trial <trial-id>` first when a run failed and a human or
AI agent needs the short answer. The diagnosis output is deterministic and
derived from redacted debug evidence. It summarizes the primary cause, owner
layer, confidence, affected trial count, score reliability, supporting
evidence, reason clusters for batches, and next actions. Pass `--format json`
to retrieve the same schema-versioned `DiagnosisReport` for automation.
If `loom eval batch show <batch-id>` prints
`llm_evidence_status: no_calls_invalid` or a no-call warning, treat the batch
as invalid benchmark evidence. A model-backed terminal trial finished without
persisting gateway call records, so a reward of `0` in that state is not a
clean model-quality score. Trial JSON includes `no_call_reason`,
`no_call_message`, and `no_call_retryable`; batch JSON includes
`no_call_reason_counts`. If the reason is `codex_high_demand_no_call`, Codex
exited before any Gateway request, and the trial must be excluded from clean
parity/request-parameter baselines unless a retry records real calls.

Use `loom eval batch debug <batch-id>` or
`loom eval trial debug <trial-id>` when an API agent needs structured failure
evidence without scraping the web UI. The CLI calls
`GET /api/v1/batches/{id}/debug` and
`GET /api/v1/trials/{trial_id}/debug`. The response includes a stable
`failure.reason_code`, `failure.category`, `failure.attribution`,
`failure.failure_class`, `failure.root_cause`,
`failure.platform_outcome`, `failure.score_outcome`, and
`failure.rerun_recommendation`, plus lifecycle state/timestamps/attempts,
safe backend/provider/model identifiers, token usage summaries,
task/checksum/readiness metadata, verifier reward/error details, scoped
ATIF/trajectory/artifact links, and `next_actions`. These payloads are
team-scoped and redacted the same way as normal detail responses. Required
production failure reasons are typed distinctly: reward `0` with verifier
output is `score_failure`, missing verifier output is `verifier_failure`,
missing trajectory/ATIF or verifier-required artifacts is `artifact_failure`,
provider no-call/timeout is `provider_failure`, and
setup/build/image/preflight failures remain separated between
`platform_failure` and `task_failure`. Missing verifier-required artifacts are
reported as invalid, retryable evidence because the score row cannot be audited
against the verifier's output-file contract.

Use `loom eval batch rerun-plan <batch-id>` or
`GET /api/v1/batches/{id}/rerun-plan` before launching supplemental work. The
plan is deterministic and separates failed coordinates into `auto_safe`,
`operator_approval`, `not_rerunnable`, and `already_covered` buckets. Pass
repeated `--task-id` values, or repeated `task_id` query parameters, to scope
the plan to an explicit task list. By default, the supplemental task id list
contains only auto-safe platform/transient failures. `--include-operator-approval`
adds operator-approved coordinates, but task compatibility failures and reward
`0` score failures remain excluded unless the task or scoring evidence changes
under an explicit operator workflow. `supplemental_task_ids` stays unique for
copy/paste task-list launches; `supplemental_coordinates` preserves every
`task_id`/`sample_idx`/`combination_idx` row when multiple samples or
combinations for the same task need rerun.

## Quickstart: Submit from the Web App

Use this path when working through the public UI.

> **Need an account?** Staging uses admin-approved username/password
> accounts (no email, no automatic mail). On the sign-in page, use the
> **Request account** card: enter a username, pick an existing team, and
> wait for an admin to approve and share the one-time password setup link.
> If the team you need doesn't exist, ask an admin to create it first.
> Full flow: [Web sessions and teams](#web-sessions-and-teams).

1. Open [https://yylx.world/dev](https://yylx.world/dev) or your local Loom URL. First production uses [https://yylx.world/prod](https://yylx.world/prod).
2. Sign in and select the team that owns the run.
3. Open **Providers**.
   - Add a third-party provider connection, or register a self-hosted
     OpenAI-compatible endpoint.
   - Test the connection.
   - Refresh models.
   - Preflight the exact model you plan to use.
4. Open **New batch**.
   - Select one or more benchmarks.
   - Choose the task subset: all, first N, last N, random N, or explicit task
     ids.
   - Pick agent/model combinations.
   - Optionally add a short suffix; Loom generates the batch name and
     description from the selected tasks and combinations.
   - Review the planned trial count and submit.
5. Open **Monitor** to watch batch/trial progress.
6. Open the batch or trial detail page for evaluator reward, platform outcome,
   trajectory, ATIF, and artifacts.
7. Open **Run Library** to find completed shared work and reuse safe artifacts
   or clone run configuration into the current team.

The public web app intentionally hides raw infrastructure details in the common
path. Diagnostic panels and copyable CLI snippets are available on the same
pages when you need to reproduce or debug from a terminal.

For the deep dive on how web sessions, teams, roles, monitor filters, and
diagnostic panels work, see [Web platform workflows](#web-platform-workflows).

## Backends

`loom run --backend` picks the sandbox that hosts the trial:

- **`docker`** — the default for real runs. Uses the local Docker daemon (or
  Docker Desktop on macOS) and per-trial containers.
- **`fake`** — no-op sandbox. `exec` returns success without running anything.
  Verifies the trial pipeline end-to-end without a container.
- **`daytona`** — cloud sandbox for elastic capacity or when Docker isn't
  available on the submit host.

### Cloud sandboxes (Daytona)

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
- `math-500` publishes the 500-problem MATH subset from
  `HuggingFaceH4/MATH-500` and grades the final boxed/exact answer.
- `hendrycks-math` remains available as a full 5000-row MATH test-split adapter
  from the pinned `HuggingFaceTB/MATH` `all` config, but it is outside the
  current v1.0 supported set.
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

## Model sources

### Hosted providers

The full hosted-provider registration workflow, including OpenAI-compatible
endpoints and provider-native APIs, lives in
[`provider-onboarding.md`](integrations/provider-onboarding.md).

Quick recap for `loom run`:

```bash
loom config set token.anthropic sk-ant-...
# Or export ANTHROPIC_API_KEY / OPENAI_API_KEY / GOOGLE_API_KEY.
# Or add ANTHROPIC_API_KEY=... to a project .env at or above the CWD.
loom run --model anthropic/claude-opus-4-7 ...
```

For service-mode `litellm` trials and Codex subprocess trials, API
clients can include `trial_config.request_params` to pin non-sensitive
generation controls such as `temperature`, `top_p`, `seed`, max output
limits, reasoning effort, tool-choice mode, and provider decoding extras.
Loom strips prompt/message payloads, headers, API keys, credentials, and unknown
extras before forwarding the request, then exposes the effective
controls in provider debug evidence. Codex needs an adapter-specific
bridge because its CLI constructs provider requests itself: the worker
passes sanitized per-trial controls to the Codex launcher as
`LOOM_CODEX_SETTINGS_JSON`, and the launcher/Gateway sanitize those
values again before forwarding and auditing them. Other subprocess
agents need their own adapter-specific support.

In service mode, team-scoped provider connections are the normal path
for user-hosted OpenAI-compatible endpoints. Register the connection,
refresh or manually add its model ids, then launch from the SPA. The
v1.0 hosted/public platform does not run model servers for users; it only
stores the team-scoped provider connection and routes calls to the endpoint
the team provides.
SPA hides obvious tool/API entries by default and submits
`provider_connection_id` + `provider_model_id` with the trial or batch;
operators can use the raw model view for debugging noisy catalogs.
When New Batch links you to a provider's Models tab to refresh,
preflight, or manually add a model, use the "Back to New Batch" link on
that page to return to the batch form after the model catalog is ready.

### Local LLMs (vLLM, ollama, llama.cpp, lm-studio)

Three paths for driving `loom run` against a local OpenAI-compatible server,
ordered by what most users reach for first.

#### Path A: inline — your server is already running

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

#### Path B: local CLI helper — start vLLM for this process

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

This path is local `loom run` behavior only. It is not available as hosted
platform inference in v1.0; service-mode users should run their own vLLM or
other OpenAI-compatible endpoint and register it as a provider connection.

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

#### Path C: persisted — same server, many runs

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

### `loom serve` reference

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

### Comparing multiple models

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

## Web platform workflows

The rest of this section documents behavior of the deployed web platform (SPA
+ public API). It applies to a running Loom service; laptop-only `loom run`
skips all of it.

### Web sessions and teams

When Loom is served as a web platform, the SPA uses browser user sessions.
Users sign in with username and password. First-time users request an account
from Settings by entering a username and selecting an existing team. If the
team is missing, contact an admin to create it first. Staging does not
collect email and does not send automatic mail: an admin reviews the request,
approves a team role, and manually shares the one-time password setup link.
Forgot-password follows the same pattern: the user submits a reset request,
the admin approves it, and the user receives a one-time reset link. On deployed
web environments, setup and reset links are generated with the public HTTPS
origin. The service sets an HttpOnly session cookie. Auth responses return a
CSRF token that the SPA keeps in memory; the browser sends cookies
automatically, and mutating requests include the CSRF header. You should not
paste a raw bearer token into the production SPA. The CLI stores the same
session cookie plus CSRF token in the owner-only config file; `loom auth
whoami` persists rotated session/CSRF values returned by the server, and
mutating CLI requests refresh the session
CSRF before retrying a CSRF-specific rejection.

Your current team controls execution, cost attribution, provider credentials,
members, and user-owned API tokens. Roles are enforced by the API:

- `viewer` can read the current team's batches, trials, provider summaries, and
  usage.
- `member` can also submit work for the current team.
- `owner` can manage user-owned API tokens and provider connections.
- `platform_admin` is an operator role with cross-team inspection/admin access.

The app shell always shows the current team and role beside the primary
navigation, so users can confirm which team will own new batches, provider
connections, reset/setup approvals, and API-token actions before they act. After sign-in,
Home is the default landing page. It summarizes team readiness, provider
health, benchmark readiness, active workers, recent batch/trial activity, and
separates user-owned next actions from operator-owned prerequisites. Team
Settings shows the signed-in username, current team, role, team switcher, joined
browser users, and role-aware setup links. Team owners get Team access for
legacy invites and API tokens; platform admins also manage fixed internal
teams, approve pending username account requests, and approve password reset
requests. Members and viewers see only the actions their role allows.

Team access is split into task-focused sections. Platform admins can review
legacy team requests, approve account setup and password reset requests, switch
to fixed-team maintenance, create/list legacy invites, manage API tokens, or
inspect the audit log. Team owners see only invite and API-token sections.
Account setup/reset links are revealed only once and must be shared manually.
The platform-admin Audit tab loads only when opened and provides Previous/Next
controls over the complete timestamp/id-ordered history. Loading marks page
controls unavailable without removing keyboard focus; an error keeps Previous
and Retry available; the terminal page is announced explicitly.

Most web workflows now include contextual quickstarts directly on the page. Use
the copyable snippets in Settings, Team access, Providers, New Batch, Monitor,
Batch Detail, Trial Detail, Run Library, Usage, Rate cards, Tasks, and
Benchmarks when you want the CLI/API equivalent for the page you are viewing.
Home intentionally avoids telling users to run operator import or worker
commands from the browser; those items appear as operator actions when the
service reports that platform prerequisites are missing. The examples use safe
placeholders and `env:`/`file:` secret references so users do not need to switch
back to this guide for the common path.

The CLI uses the same username/password account by default. Use
`loom auth login --server URL --username USER --password env:LOOM_PASSWORD`,
then `loom auth whoami` to verify the active server, user, current team, role,
and scopes. Team owners can still create, rotate, or revoke named API tokens
from Team access for automation; those tokens are user-owned, scoped, and
shown only once on create/rotate. Legacy unowned team tokens are read/compat
credentials only and cannot create batches, direct trials, reruns, clones, or
artifact reuse jobs. Completed run metadata and safe artifacts are shared
across teams through the Run Library; ordinary batch, trial, trajectory, ATIF,
artifact, cancellation, rerun, and provider routes remain current-team scoped.

### Default views and diagnostics

Monitor lists show `username / team` for each batch and trial when the submitter
is known, with legacy team fallback for old rows. Ordinary users see their
current team's work; platform admins can use the team filter to inspect
cross-team queues without losing context. The Monitor health card summarizes
the current URL scope with batch/trial state counters, queued/claimed/running
trial pressure, concurrent task slots, active worker count, worker backends, and
per-resource-pool slot usage before the row table. In the batch view, the `q`
search filter scopes both the table and health card to matching batch identity
text or batch ids, so shared Monitor links keep their counters aligned with the
visible rows. For autoscaled worker pools,
the resource summary also includes desired slots, pending slots, draining
slots, idle-window age, and the last autoscaler decision. The same slot summary
is available from the CLI with `loom resources status` and
`loom resources status --json`. The table's State dropdown is still reflected
in the URL, but the health card keeps all lifecycle counters visible so queue
pressure does not disappear when you filter the rows to failures. Monitor
filters are reflected in the URL so support links can preserve `view`, `state`,
`q`, `batch_id`, `team_id`, `benchmark_id`, `agent`, and `model` while
switching between batch and trial views. Failed trial lists include a Failure
diagnostics summary grouped by platform `failure_reason`, with representative
messages and links to the first affected trial so provider, sandbox, verifier,
and artifact failures can be triaged before opening individual logs.
User-facing web timestamps render in the viewer's local timezone with a short
timezone label, and CLI text summaries use the executing shell's local
timezone. API and `--format json` responses keep canonical timezone-aware ISO
timestamps. Token usage labels use `Input` and `Output` instead of abbreviated
`P`/`C` wording.

New Batch includes a Release review card before submit. Check that it shows the
intended task scope, planned trial count, selected backend worker availability,
provider connection status, and model preflight state before launching a
release-blocking batch. When tag filters are selected, the generated description
preview and stored batch description include the selected tag keys and values.

Usage follows the same team-context model. Ordinary users see usage scoped to
their current team and do not enter raw team ids. Platform admins can leave the
team filter blank for platform-wide usage or choose an internal team by name;
the copyable CLI command still includes the stable `--team-id` value when a
team is selected. For a shared provider, filter by `provider_connection_id` and
use `breakdown_by=team` or `breakdown_by=user` to separate owner-team usage
from each consuming shared team/user. Admin-on-behalf submissions keep the
represented team/user on the batch for product ownership, but usage and billing
are attributed to the real acting admin/user. The represented user/team sees
the run in monitor views and can use normal owner actions such as detail,
debug, rerun, cancel, and artifact download. Admin usage views can request
per-batch drilldown; token-only or self-deployed calls show token totals with
`cost_status=not_applicable` instead of a fabricated dollar amount. Failed
upstream provider attempts show as `pricing_mode=failed-upstream` and
`cost_status=failed_upstream`, so they remain inspectable without being counted
as priced provider usage. Usage views also show `usage_estimate_confidence`:
`high` means the provider returned a complete usage block, while `partial` or
`missing` means token and cost totals are lower confidence because some
provider usage fields were absent.

The SPA defaults to readable summaries instead of raw API payloads.
New Batch explains task selection, agent/model combinations, backend,
and advanced trial settings in product terms. Batch Detail shows a
Run plan, Monitor shows planned trials and evaluator score, Run Library shows
org-wide completed shared work, and Trial Detail separates platform outcome
from evaluator reward. Batch Detail and Trial Detail also show owner team,
visibility/share status, provenance when a run was cloned or reused, and a
Debug evidence card when the API has structured outcome evidence. That card
shows reason code, category, attribution, lifecycle state, model/provider
summary, and suggested next actions in human-readable form; the exact redacted
JSON remains in a collapsed disclosure for API reproduction.
For normal model-backed runs, New Batch starts from the provider connection and
model choice and uses Loom's default model runner internally. Users only open
`Use a specific agent` when they want a non-default runtime such as `oracle` or
another service-mode agent; choosing an agent that does not need a model hides
the provider/model controls.

When a finished batch has transient gateway failures, Batch Detail shows
`Rerun failed cases` and a supplemental rerun recommendation. The
recommendation distinguishes auto-safe platform failures from failures that
need operator approval and from failures that are not rerunnable. Reward `0`
with verifier output is a platform success and a score failure, so Monitor and
Run Library show it separately from platform failure and it is not selected for
automatic supplemental reruns. The action creates a linked rerun batch
containing only selected task/sample/combination coordinates. The original
batch keeps its original trial counts, and Batch Detail also shows the
effective result after successful linked reruns replace those transient
failures. `GET /api/v1/batches/{id}` includes the same `rerun_plan` summary
and `final_trial_selection` map so API agents can preserve main/supplemental
lineage without scraping the UI.
Use `loom eval batch delivery-bundle <original-batch-id>` after the reruns
finish to export that effective result with the original and supplemental
batch lineage recorded in the manifest, ledger, artifact metadata, and Batch
Detail download status.

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

Tabbed views use one keyboard contract across provider details, task-set
details, Team access, and the model-source picker. Press Tab to enter the
selected tab, Left/Right to move and activate, or Home/End to jump to the first
or last available tab. Disabled choices are skipped. Each tab is linked to its
panel for screen readers, and changing tabs does not change the underlying API
request or saved payload semantics.

Contextual snippets are not a substitute for diagnostics. If a quickstart
command fails, open the same page's diagnostic panel or detail view, then copy
the relevant `loom eval batch show`, `loom eval trial show`, `loom eval trial
download`, `loom providers test`, or `loom eval usage` command from the page
to reproduce the issue from a shell.

### Run Library

Run Library is the org-wide place to inspect completed shared work. Team still
controls execution, cost, provider credentials, members, and API tokens. The
Library only exposes completed metadata and artifacts that passed sharing
checks.

Use the SPA top-level **Run Library** page:

- **My team** shows your team's library rows.
- **All teams** shows your team plus completed org-shared rows from other
  teams.
- Owner-team labels show who ran the original work.
- State, team, artifact-type, search, benchmark, agent, model provider/name,
  provider connection, and provider model filters narrow the table without
  parsing display names or showing raw JSON payloads.
- Artifact badges on the list are bounded previews; a `+` means the run has
  more typed artifacts than the list page counted. Open the row or export
  artifact metadata for the complete set.
- Platform admins can use the team filter with internal team names across the
  whole platform; ordinary users see only their joined teams.
- Use **Next** and **Previous** to traverse older and newer pages. Cursor
  history is kept only for the current browser session and never added to the
  URL; the shareable URL continues to contain only scope and filters.
- Changing any scope or filter returns to page one before requesting the new
  selection. Loading guards both page controls with accessible unavailable
  state while keeping them focusable, failed pages offer Retry and preserve
  Previous when available, and an empty or final page explicitly says that the
  end of results was reached. Keyboard focus stays on the control or filter the
  user activated.

Open a Library row to inspect task selection, agent/model config, trial rollup,
debug evidence, provenance, and artifact group previews. For multi-agent/model
batches, the **Combination results** table compares each requested combination's
reward, actual/expected trial count, scored-trial count, success/failure counts,
LLM calls, and token totals. Rows with no materialized trials are separate from
rows that have trials but no scored reward. When shared supplemental reruns
replace failed originals, the table shows the effective result. Safe shared
artifacts expose Download, Copy URL, and Reuse actions. Blocked artifacts show
only a safe reason and cannot be downloaded or reused by another team. Typed
artifact rows also show the artifact type, owner
team, source trial/batch, safety/redaction state, and content-hash prefix.
Large-run detail views use a capped typed artifact preview instead of loading
full legacy trial trajectory indexes or the complete typed artifact inventory.
Use **Export artifact metadata** on the detail page to download safe typed
artifact metadata for that run as JSONL.

Clone config and reuse artifact both create new records in your current team and
record `source_provenance`. They do not copy the source team's provider
connection or credentials; choose a provider connection owned by or shared with
your team when the source config requires one. The Run Library detail page
shows that selector before cloning provider-backed work.

From the CLI, export safe Run Library artifact metadata with:

```bash
loom eval artifact export \
  --scope all \
  --artifact-type metric_table \
  --safety-state safe \
  --format jsonl \
  --output run-library-artifacts.jsonl
```

The export contains redacted metadata and storage pointers only; it does not
copy object bodies or source-team provider credentials.

### Pasting task ids

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

## Benchmarks and datasets

### `loom datasets` reference

```
loom datasets list                  # union: installed + catalog + remote
loom datasets list --installed      # only adapters in this venv
loom datasets list --available      # only catalog entries not installed
loom datasets list --remote         # only entries from LOOM_SERVER_URL/api/v1/benchmarks
loom datasets list --json           # machine-readable output

loom datasets show <slug>           # full detail for one adapter
loom datasets install <slug>        # pip-install a catalog entry
loom datasets refresh-catalog      # drop the 24h catalog HTTP cache
loom datasets provision-catalog
                                    # copy ready benchmark/task rows,
                                    # materialize supported agent rows,
                                    # and copy S3 bundles into a
                                    # staging/release target
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
  --fixtures-root "$LOOM_WORKER_FIXTURES_ROOT"

loom datasets audit team-evals
```

The validated folder path is the same path the worker materializes later.
With `source_subdir = "tasks"`, the task row is named
`team-evals/<task-id>` while its source points at
`fixture://team-evals/tasks/<task-id>`.

Production deployments should publish the same validated folder to object
storage instead of relying on a shared worker fixture mount:

```bash
# Export LOOM_DB_URL and LOOM_MINIO_* in the shell or process environment.
# Do not pass credential values through argv; publish-local reads these env vars.
loom datasets validate-local ./team-evals
loom datasets publish-local ./team-evals --bucket loom-benchmarks

loom datasets audit team-evals
```

`publish-local` uploads each task bundle under
`s3://loom-benchmarks/team-evals/<task-id>/` and registers task rows with those
sources. The worker uses the existing object-store materializer at runtime. If a
task declares `environment.dockerfile`, that Dockerfile and its build context
are part of the uploaded bundle; the worker builds and caches a deterministic
`loom-task:<hash>` image before the first service-mode trial that needs it.
The secret-bearing `publish-local` flags also accept safe references such as
`--db-url env:LOOM_DB_URL`, `--minio-access-key env:LOOM_MINIO_ACCESS_KEY`,
and `--minio-secret-key env:LOOM_MINIO_SECRET_KEY`, but literal credential
values are rejected because argv is visible in process listings.
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
  --name-suffix oracle-smoke \
  --agent oracle \
  --benchmark team-evals \
  --n-per-task 1
```

For model-backed agents, include a provider connection and model:

```bash
loom eval batch create \
  --name-suffix litellm-smoke \
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
# Export LOOM_DB_URL in the shell or process environment.
loom datasets audit --all
loom datasets audit swe-bench-verified
loom datasets audit --all --json
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
supported scope until the listed product or data-access follow-up lands. `Not
in v1.0` means the built-in benchmark is outside the current v1.0 allowlist; it
is visible, disabled, and excluded from supported task counts until a support
issue promotes it into scope.
Required public benchmarks should move from publish/repair pending states to
`Ready`; hiding a needed benchmark is not a substitute for publishing it.

The hidden `/benchmarks` route remains a power-user diagnostic view rather
than the normal submission path. It includes the same registry readiness states
plus operator-oriented `loom datasets list --remote`, `loom datasets audit`,
and `loom datasets sync-config --dry-run` snippets that use token and database
environment references instead of raw secrets.

After the supported-benchmark acceptance batch or batches finish, operators can
verify the v1.0 reward contract from the public API:

```bash
python scripts/benchmark_reward_gate.py sweep \
  --server-url "$LOOM_SERVER_URL" \
  --token env:LOOM_API_TOKEN \
  --batch-id "$BATCH_ID"
```

Repeat `--batch-id` when the acceptance run uses separate batches for separate
benchmarks. This gate checks that each batch is terminal, fan-out produced the
expected number of trials, and every v1.0-supported benchmark has distinct
numeric-reward task coverage equal to `/api/v1/tasks/count`. It treats
model-correctness scores such as `0` as valid verifier output. A later rerun
batch can cover an earlier provider/agent/platform transient failure for the
same task, but missing rewards without rerun coverage, missing allowlist benchmark
coverage, and benchmark-side verifier or environment failures fail the sweep.
For a narrower diagnostic, add repeated `--expected-benchmark BENCHMARK_ID`
arguments.

For staging or release deployments, operators should provision the ready
catalog before inviting users. Use `loom datasets provision-catalog`
when copying runnable benchmark/task rows, materializing supported service-mode
agent rows, and copying referenced `s3://` bundles from a known-good source
environment into the target database/object store. For
HF-published first-party benchmarks in protected environments, use
`loom datasets register --mirror-to-object-store` so runtime rows point at
internal object storage while preserving HF repo/revision/checksum provenance.
Follow either path with `/api/v1/agents` discovery evidence and
`loom datasets audit --all --verify-bundles`. These
commands are separate from `scripts/seed_test_data.py`, which is only for
disposable auth/run fixtures. `loom eval batch create` uses the local agent
catalog when `loom-launcher` is installed, but falls back to the deployed
server's `/api/v1/agents` catalog for service-mode submissions so an operator
checkout can submit `codex` and other server-ready agents without installing
local launcher adapters.

Batch fan-out still treats deterministic non-license policy/config failures as
terminal defense-in-depth instead of leaving the batch `submitted`; open Batch
Detail or run `loom eval batch show <id>` to inspect `failure_reason`,
`failure_message`, and `fanout_errors`.

If a model gateway returns transient `502`, `503`, `504`, timeout, connection
reset, remote-protocol, or provider transport-disconnect failures, Loom retries
the agent call within the trial before marking the trial failed. If retry
budget is exhausted, rerun the remaining failed cases from Batch Detail instead
of launching the whole batch again.

## Reference

### `loom run` reference

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
- `terminus-2` — Harbor Terminus2 embedded in the worker image; tool-use
  terminal loop with typed `terminus2_*` trajectory events and Harbor artifacts
  under `.loom/agent/`. Requires a provider + model; does not use
  `loom-launcher` or per-trial `install_script`. See
  [`architecture/terminus2-runtime.md`](architecture/terminus2-runtime.md).
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
[`operator-runbook.md#trial-cache-per-trial-agent-install`](runbooks/operator-runbook.md#trial-cache-per-trial-agent-install)
for the operator-side knobs.

### `loom config` reference

Config persists to `$XDG_CONFIG_HOME/loom/config.toml` (defaults to
`~/.config/loom/config.toml`). Tokens are redacted in `loom config show`.

```
loom config set token.<provider> <key>    # provider in {anthropic, openai, google}
loom config set server_url <url>          # optional Loom service URL
loom config show
```

Env vars override config: `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`,
`GOOGLE_API_KEY`, `LOOM_SERVER_URL`, `LOOM_API_TOKEN`.

### Rate cards

Rate cards live at `~/.config/loom/rate-cards.toml`, seeded on first
run from `src/loom_cli/data/default-rate-cards.toml`. Edit the file
to add or override entries (e.g. for a self-hosted model or a
provider Loom doesn't ship a default for). Cost is computed locally
from these rates plus the token counts returned by the provider SDK.
For service-mode hosted YibuAPI pricing, an admin can sync the official
catalog with:

```bash
loom admin rate-cards sync-yibuapi
```

Self-deployed provider connections should usually remain `tokens-only`,
which records tokens but leaves dollar cost as not applicable. Hosted YibuAPI
connections that use the synced rate card and `pricing_source=rate-card` with
`rate_card_provider=yibuapi` return per-trial, per-batch, and admin usage
costs; self-deployed/private APIs return token totals and usage confidence
without inventing a dollar amount.
When inspecting a main production batch plus linked supplemental reruns, pass
`batch_id=<main-batch-id>&include_batch_family=true` to `/api/v1/usage` or use
`loom eval usage --batch-id <main-batch-id> --include-batch-family`. Add
`--include-batches` to see the main and rerun child batches that contributed
to the family total.

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
