# Loom

Loom is a team platform for evaluating agents and LLM-powered systems on
benchmark tasks. Researchers use it to launch many trials, watch progress,
inspect failures, compare model/agent choices, and download reproducible
artifacts.

The important product boundary is simple:

- **Loom runs the evaluation platform.** It owns teams, auth, benchmark
  catalogs, job scheduling, sandbox execution, trajectories, metrics, and the
  web/CLI surfaces.
- **Users bring the inference API.** A team can connect a hosted provider such
  as OpenAI, Anthropic, Google, Together, or Fireworks, or expose its own
  OpenAI-compatible inference service from a GPU cluster.
- **v1.0 does not host model inference.** The public/platform deployment does
  not run vLLM or serve checkpoints for users. Any vLLM, Ollama, llama.cpp,
  LM Studio, or hosted API endpoint is operated by the user/team and registered
  as a provider connection.

Public beta: [https://yylx.world](https://yylx.world)

## What Loom Does

Loom turns a model/agent evaluation into a tracked run:

1. A user picks a **provider/model**, **agent**, **benchmark**, and task subset
   from the web app or CLI.
2. Loom creates a batch and fans it out into trials.
3. Workers claim trials, materialize task bundles, run agents in sandboxes, and
   route all model calls through the Loom LLM Gateway.
4. Loom records event-sourced trajectories, ATIF metric documents, verifier
   outcomes, token usage, and artifacts.
5. Users inspect results in Monitor, Trial/Batch detail, Run Library, or through
   the CLI/API.

Out of the box, Loom includes benchmark adapters such as HumanEval, MBPP,
LiveCodeBench, BFCL, BrowseComp, GPQA, MATH-500, full Hendrycks MATH,
MMLU-Pro, AIME, SWE-Bench variants, SkillFlow, and SkillLearnBench. Some
visible catalog rows can be intentionally disabled until their runtime, dataset,
or release-scope requirements are available. Agents are also pluggable; the
shipped catalog includes CLI harnesses for common coding agents plus an
`oracle` baseline.

## Architecture

```mermaid
flowchart LR
    User["Researcher\nBrowser or loom CLI"] --> Service["loom_service\npublic REST API + SPA"]
    Service --> CP["Control Plane\nstate machine + DRF scheduling"]
    CP --> Worker["Workers\nk8s or elastic Slurm"]
    Worker --> Sandbox["Trial sandbox\nagent + task bundle + verifier"]
    Sandbox --> Gateway["LLM Gateway\nprovider facade + usage/cost accounting"]
    Gateway --> Hosted["Third-party inference API\nOpenAI / Anthropic / Google / Together / ..."]
    Gateway --> SelfHosted["User-hosted inference API\nvLLM / Ollama / llama.cpp / LM Studio"]
    CP --> Postgres[("Postgres\nruns, trials, teams, provider refs")]
    Worker --> MinIO[("MinIO/S3\ntrajectories, ATIF, artifacts")]
    Service --> MinIO
```

The Control Plane, Gateway, Service, SPA, and Workers are stateless. Postgres
and MinIO/S3 are the durable state. The same `Trial.run()` orchestrator powers
both laptop CLI mode and service mode, so local and service-mode runs emit the
same trajectory and ATIF shapes.

## Bring Your Own Inference API

Every real model-backed Loom run needs a provider connection owned by the team
submitting the work. Loom encrypts the provider API key, stores only secret
references in the database, and uses the LLM Gateway server-side during runtime.

Submission surface and inference source are independent choices. A user can
submit from the hosted web app or CLI on any machine that can reach Loom, while
the model itself can live behind a third-party API or on the user's own GPU
cluster.

### Option 1: Third-party hosted API

Use this when the model is already exposed by a hosted API provider.

Examples:

- OpenAI-compatible APIs such as OpenAI, Together, Fireworks, or a vLLM
  endpoint managed by the user/team.
- Provider-native APIs such as Anthropic or Google when that provider type is
  enabled.

The team registers:

- provider type;
- API base URL;
- API key by `env:` or browser secret field;
- optional allowed model ids;
- optional rate-card namespace for cost attribution.

Then Loom can test the connection, refresh models, preflight one model, and make
runtime calls through the Gateway.

### Option 2: User-hosted inference API on the user's cluster

Use this when the team owns a checkpoint or Hugging Face model and wants to run
inference on its own GPU cluster.

The user cluster runs an OpenAI-compatible HTTP service, usually vLLM. Loom does
not need to own that cluster and does not need SSH access for normal inference
calls. The only required runtime path is HTTP reachability from the Loom
deployment to the final `/v1` endpoint.

Typical connection patterns:

- public HTTPS endpoint owned by the user;
- bastion TCP forward from a login node to a compute node;
- SSH reverse tunnel;
- VPN, Tailscale, WireGuard, or another approved private route;
- a user-provided stable URL that forwards to the live inference job.

Runtime flow:

```mermaid
sequenceDiagram
    participant U as User GPU cluster
    participant E as Public/tunnel endpoint
    participant G as Loom LLM Gateway
    participant W as Loom worker sandbox

    U->>E: vLLM exposes OpenAI-compatible /v1
    W->>G: Agent sends chat/completions via Loom facade
    G->>E: Gateway calls registered provider URL with team key
    E->>U: Forward to model server
    U-->>G: Model response
    G-->>W: Response + usage recorded in Loom
```

If the endpoint is public on standard `443` or `80`, the default egress policy
usually works. If it is private or uses a non-standard port, an operator must
approve that IP/CIDR and TCP port in the Loom cluster config before connection
tests, model refresh, preflight, and runtime calls can reach it. This keeps
private-network access explicit and reviewable.

For Slurm/vLLM clusters, Loom ships a helper that generates a small deployment
bundle:

```bash
loom inference deploy slurm \
  --model Qwen/Qwen2.5-Coder-7B-Instruct \
  --served-model-name qwen2.5-coder-7b-instruct \
  --provider-name lab-qwen25-coder \
  --partition compute \
  --gres gpu:h100:1 \
  --venv /path/to/vllm-venv \
  --port 8001 \
  --expose user-provided \
  --endpoint-url https://inference.example.com/v1 \
  --output-dir ~/loom-inference/lab-qwen25 \
  --no-submit
```

The generated bundle includes a Slurm script, healthcheck, provider
registration script, and an owner-only API-key file. See
[`docs/provider-onboarding.md#gpu-cluster-checkpoint`](docs/provider-onboarding.md#gpu-cluster-checkpoint)
for the full workflow.

## Quickstart: Submit From the Web App

Use this path when presenting the public UI.

1. Open [https://yylx.world](https://yylx.world) or your local Loom URL.
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
   - Review the planned trial count and submit.
5. Open **Monitor** to watch batch/trial progress.
6. Open the batch or trial detail page for evaluator reward, platform outcome,
   trajectory, ATIF, and artifacts.
7. Open **Run Library** to find completed shared work and reuse safe artifacts
   or clone run configuration into the current team.

The public web app intentionally hides raw infrastructure details in the common
path. Diagnostic panels and copyable CLI snippets are available on the same
pages when you need to reproduce or debug from a terminal.

## Quickstart: Submit From the CLI to Any Loom Server

Use this path from any machine that can reach the Loom public API.
The CLI machine does not need GPUs, model weights, or direct access to the
benchmark workers; it only needs network access to the Loom service and a team
API token.

Install the CLI from a source checkout:

```bash
uv sync
uv pip install -e packages/loom-launcher -e packages/loom-benchmarks
source .venv/bin/activate
loom --help
```

Create a team API token from **Team access**, then authenticate:

```bash
export LOOM_API_TOKEN=loom_api_...
loom auth login --server https://yylx.world --token env:LOOM_API_TOKEN
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

Submit a small model-backed batch:

```bash
loom eval batch create \
  --name cli-smoke-$(date +%s) \
  --task-filter '{"task_ids":["hello-world"]}' \
  --provider smoke-openai \
  --model gpt-4o-mini \
  --agent litellm \
  --n-per-task 1

loom eval batch show <batch-id>
loom eval trial list --state succeeded --limit 5
loom eval trial show <trial-id>
loom eval trial download <trial-id> --kind atif --output atif.json
loom eval trial download <trial-id> --kind trajectory --output events.jsonl
```

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
manually from the provider page or provider model API, then preflight it before
launching large batches.

## Quickstart: Run Loom Locally

Local service mode is the fastest way to run the full stack for development or
demo preparation.

Prerequisites:

- Python 3.12;
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/);
- Docker CLI with the Compose plugin;
- Docker Desktop running on macOS.

Install dependencies:

```bash
uv sync
uv pip install -e packages/loom-launcher -e packages/loom-benchmarks
source .venv/bin/activate
docker compose version
```

Start the service stack:

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
[`docs/operator-runbook.md`](docs/operator-runbook.md).

## Quickstart: Laptop-Only `loom run`

For a local one-off experiment without the service stack, use `loom run`.

```bash
loom datasets list

# Smoke test with no real model provider.
loom run --task humaneval/HumanEval/0 \
  --agent oracle \
  --backend fake \
  --output-dir /tmp/loom-smoke \
  --json

# Model-backed local run against an already-running OpenAI-compatible server.
loom run --task humaneval/HumanEval/0 \
  --agent claude-code \
  --local-server http://localhost:8000/v1 \
  --model meta-llama/Llama-3.1-8B-Instruct \
  --backend docker
```

`loom run` writes `events.jsonl` and `atif.json` under the output directory.
The full local LLM workflow, including auto-starting vLLM and comparing multiple
models, lives in [`docs/user-guide.md#local-llms`](docs/user-guide.md#local-llms).

## Key Concepts

| Concept | Meaning |
|---|---|
| Team | Owns provider credentials, API tokens, submitted runs, cost attribution, and members. |
| Provider connection | Team-scoped inference API configuration. It can point to a hosted provider or a user-hosted endpoint. |
| Model | Concrete model id selected from a provider connection. Refresh discovers models; preflight proves one model can generate. |
| Agent | Harness that drives a model through a task, such as `litellm`, coding-agent CLIs, or `oracle`. |
| Benchmark | Adapter that publishes tasks into Loom's catalog. |
| Batch | A submitted run plan: task filter plus one or more agent/model combinations. |
| Trial | One executable unit from a batch. |
| Trajectory | Event-sourced JSONL record of execution, tool use, model calls, verifier checks, and finalization. |
| ATIF | Structured metric document with rewards, token usage, verifier output, and platform outcome. |

## Where to Read More

- [`docs/index.md`](docs/index.md) - documentation map.
- [`docs/user-guide.md`](docs/user-guide.md) - CLI and user workflows.
- [`docs/provider-onboarding.md`](docs/provider-onboarding.md) - hosted API and
  self-hosted GPU-cluster provider setup.
- [`docs/architecture/overview.md`](docs/architecture/overview.md) - component
  map and execution model.
- [`docs/architecture/service-mode.md`](docs/architecture/service-mode.md) -
  service-mode details.
- [`docs/operator-runbook.md`](docs/operator-runbook.md) - production deploy,
  environment isolation, release gates, and operations.
- [`docs/remote-worker-pool.md`](docs/remote-worker-pool.md) - attaching extra
  Docker/Slurm worker capacity.
- [`docs/authoring-a-task.md`](docs/authoring-a-task.md) - creating a new task
  or benchmark adapter.
- [`docs/loom-vs-harbor.md`](docs/loom-vs-harbor.md) - design tradeoffs and
  gaps versus Harbor.
- [`docs/contributor-quickstart.md`](docs/contributor-quickstart.md) - repo
  layout, tests, and contribution workflow.

## Repository Governance

Loom is licensed under Apache-2.0. Normal development uses PRs into `dev`;
`main` is reserved for release promotion. The required `repository-checks`
workflow runs with read-only default GitHub token permissions. Workflows that
publish benchmark bundles or deploy infrastructure use protected GitHub
Environments so secrets are not available to pull request code.

Repository public-readiness was completed in
[#12](https://github.com/carinrc/loom/issues/12). Maintainers should keep branch
protection, read-only default workflow tokens, selected Actions sources,
protected publish/deploy environments, and secret scanning enabled. External
pull requests are not accepted yet; use issues for discussion until the
contribution policy is opened.
