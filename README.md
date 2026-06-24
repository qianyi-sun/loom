# Loom

A team platform for running LLMs on customizable tasks, tracking
their performance, and saving results for download.

LLMs are first-class — point at any cloud provider (Anthropic,
OpenAI, Google) or any **local** OpenAI-compatible server (vLLM,
ollama, llama.cpp, lm-studio). See
[`docs/user-guide.md#local-llms`](docs/user-guide.md) for the local
workflow. Tasks and agents are **pluggable infrastructure**, not
assumptions:

- **Tasks** — 21 catalog benchmark adapters ship out of the box (HumanEval,
  SWE-Bench Verified/full/Multimodal, MBPP, LiveCodeBench, BFCL,
  BrowseComp, GPQA, Hendrycks MATH, MMLU-Pro, tau2-bench, GAIA,
  AIME, OSWorld, WebArena, SkillFlow, SkillLearnBench), plus the
  optional Terminal-Bench-2.0 package. Catalog presence is separate from
  current runtime support: OSWorld and WebArena remain visible but disabled
  until Loom has the required UI benchmark runtime. Add your own by
  implementing a `BenchmarkAdapter` Protocol — your task isn't locked to
  anyone else's leaderboard.
- **Agents** — 11 pick-up-and-use harnesses for popular CLI agents
  (Claude Code, Codex, OpenHands, Aider, Gemini, Qwen, Kimi, ...)
  plus an `oracle` baseline. Bring your own by implementing
  `AgentAdapter` (a small `build_invocation` + `capture_events`
  Protocol in the `loom-launcher` package).

For every trial, Loom records the full agent execution as
event-sourced JSONL (every LLM call, tool use, env exec, verifier
check), projects an **ATIF v1.7** metric document (per-step rewards,
token usage, cost attribution, verifier outcomes), and stores both
for browsing in the SPA or downloading as artifacts. Multi-team fair
scheduling (DRF), per-`(team, trial, step)` cost attribution, license
allowlists, and a `/api/v1/usage` dashboard make it usable for a
research org, not just one user.

**Batches** — teams launch many trials from the SPA by selecting a
provider/model, agent, backend, benchmark, and task filter. The run
records keep the submitted config snapshot so later catalog changes do
not rewrite historical results.

Two ways to consume:

- **`loom` CLI** — `uv sync` and run benchmarks against agents on
  your laptop. No server stack.
- **Service mode** — Control Plane + Workers + LLM Gateway + Postgres
  + MinIO, with a React SPA at `web/` for launching batches, monitoring
  trials, reviewing admin access, and inspecting per-team usage.
  `loom service up` brings the whole stack up in one command.

---

## Quick start — laptop (`loom run`)

```bash
# Install with uv (recommended — what the repo's tests + CI use). uv
# creates `.venv/` on first run, so no separate `python3 -m venv` step.
# See https://docs.astral.sh/uv/getting-started/installation/ to install uv.
uv sync                                           # core deps
uv pip install -e packages/loom-launcher -e packages/loom-benchmarks
source .venv/bin/activate                         # so `loom` is on PATH

loom datasets list                          # what's available
loom config set token.anthropic sk-ant-xxx  # one-time provider key setup
# Or put provider env vars in the nearest project .env; exported
# shell variables still win over .env values.

# Smoke test: one HumanEval task, oracle baseline, no real agent.
loom run --task humaneval/HumanEval/0 \
         --agent oracle --backend fake \
         --output-dir /tmp/loom-smoke --json

# Real eval against a cloud provider (Anthropic, OpenAI, Google).
loom run --dataset swe-bench-verified \
         --agent claude-code --model anthropic/claude-opus-4-7 \
         --backend docker --concurrency 4 --output-dir ./runs
```

### Running with local LLMs

Three ways to point Loom at a local LLM, ordered by what most users
reach for first.

**A. Already have a server running?** Pass `--local-server` inline.
No config, no state.

```bash
# Your vLLM (or ollama / llama.cpp / lm-studio) is already up at :8000
loom run --task humaneval/HumanEval/0 \
         --agent claude-code \
         --local-server http://localhost:8000/v1\
         --model meta-llama/Llama-3.1-8B-Instruct \
         --backend docker

# With auth (e.g. vLLM started with --api-key, or via env var):
loom run ... --local-server http://my-vllm.internal/v1 \
             --local-api-key sk-foo
# or  LOOM_LOCAL_API_KEY=sk-foo loom run ...
```

**B. Want Loom to start vLLM for you?** Pass weights directly.

```bash
uv sync --extra vllm   # one-time, adds the vLLM dep (GPU required)

# HuggingFace model id — Loom downloads + serves
loom run --task humaneval/HumanEval/0 \
         --agent claude-code \
         --model hf:meta-llama/Llama-3.1-8B-Instruct \
         --backend docker

# Local weights directory — pass the path directly (no `file:` prefix)
loom run --task humaneval/HumanEval/0 \
         --agent claude-code \
         --model /data/checkpoints/my-llama-3.1-tune/ \
         --backend docker
```

Loom finds a free port (starting at 8234), starts vLLM, waits until
`/v1/models` reports healthy, runs the trial, and tears down vLLM at
the end. Ctrl-C cleans up too.

vLLM tuning flags (forwarded to `vllm serve`):
`--tensor-parallel-size N`, `--max-model-len N`,
`--gpu-memory-utilization 0.85`, `--vllm-port 18234`,
`--vllm-host 0.0.0.0` (default `127.0.0.1`; loopback-only by design —
opt in explicitly to expose on the LAN), `--enforce-eager`,
`--keep-alive` (leave server running between iterations; only meaningful for 
single-`--model` runs; multi-model loops always tear down each iteration's server).

**C. Use the same server repeatedly?** Persist it with `loom config`.

```bash
loom config set local.vllm.base_url http://localhost:8000/v1
loom config set local.vllm.api_key sk-foo   # optional

loom run --task humaneval/HumanEval/0 \
         --agent claude-code \
         --model local/vllm/meta-llama/Llama-3.1-8B-Instruct \
         --backend docker
```

Register multiple servers (`local.ollama.base_url ...`,
`local.lmstudio.base_url ...`) and pick one per trial. Local trials
cost $0 by default; add a rate-card row keyed
`provider="local:<server>"` to attribute internal GPU cost.

### Compare N models on the same dataset

Pass `--model` more than once. Loom runs your tasks against each
model in turn, bucketing output under a per-model directory:

```bash
loom run --dataset humaneval \
         --model hf:meta-llama/Llama-3.1-8B-Instruct \
         --model hf:meta-llama/Llama-3.1-70B-Instruct \
         --output-dir runs/compare/
# → runs/compare/llama-3-1-8b-instruct/<trial-id>/
# → runs/compare/llama-3-1-70b-instruct/<trial-id>/
```

Sequential by default: only one vLLM is loaded at a time, so peak
GPU memory = max(A, B), not A + B. Pass `--parallel-models` to launch
all upfront for multi-GPU users.

### Pre-launch a server, share across runs

If you'll target the same server many times, `loom serve` writes
the URL into config on start and removes it on Ctrl-C — no `loom
config set` housekeeping:

```bash
# Terminal 1
loom serve hf:meta-llama/Llama-3.1-8B-Instruct --name llama8b
# → ✓ vLLM ready; registered as local/llama8b; Ctrl-C to stop

# Terminal 2
loom run --dataset humaneval --backend docker \
         --model local/llama8b/meta-llama/Llama-3.1-8B-Instruct
```

Per-trial outputs land at `./runs/<trial-id>/events.jsonl` (full
event trajectory) + `./runs/<trial-id>/atif.json` (ATIF v1.7
projection). Full CLI reference, including service-mode env-var
config (`LOOM_GW_LOCAL_<NAME>_BASE_URL`):
[`docs/user-guide.md`](docs/user-guide.md). Internals
(subprocess lifecycle, model-spec rewrite, cleanup model):
[`docs/architecture/local-llm.md`](docs/architecture/local-llm.md).

---

## Quick start — service mode (`loom service up`)

Service mode requires Docker CLI with the Compose plugin. On macOS, install and
start Docker Desktop first, then verify the prerequisite with:

```bash
docker compose version
```

```bash
cp .env.example .env             # add your provider API keys
loom service up                  # docker compose + migrations + token bootstrap
```

`loom service up` brings up the full stack (Postgres + MinIO + LLM
Gateway + Control Plane + loom_service + Worker + React SPA), ensures
the dev singleton admin secret exists, runs migrations, seeds team/worker
tokens, prints the team bearer + endpoint URLs, and writes the admin token to
the ignored local env file for explicit `reveal-admin` use.

Dev compose is for local development, not public hosting. Published ports bind
to `127.0.0.1` by default through `LOOM_DEV_BIND_ADDR`; use `loom cluster`
with TLS Ingress for public deployments.

URLs printed at the end of `loom service up`:

| What | URL |
|---|---|
| **User panel (SPA)** | http://localhost:5173/ |
| API root (manifest) | http://localhost:8090/ |
| Swagger UI | http://localhost:8090/docs |
| Health (no auth) | http://localhost:8090/api/v1/health |
| MinIO console | http://localhost:9001/ |

Dev artifact, ATIF, and trajectory downloads are proxied through
`loom_service`. Links returned by `GET /api/v1/trials/{id}` stay on the API
host, so browser and host CLI users do not need direct access to the MinIO S3
port. The CLI prints download commands instead of raw signed URLs.

Submit a trial directly via the REST surface:

```bash
export LOOM_API_TOKEN=loom_api_...   # create in Team access → API tokens
loom auth login --server http://localhost:8090 --token env:LOOM_API_TOKEN
loom auth whoami
loom eval usage --start 2026-06-01 --end 2026-06-30
loom eval trial download <trial-id> --kind atif --output atif.json

curl -X POST http://localhost:8090/api/v1/trials \
  -H "Authorization: Bearer $LOOM_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"task_id":"hello-world","config":{"agent_name":"oracle","agent_model":null}}'
```

**Hot reload in dev**: the three uvicorn services (`loom_service`,
`control_plane`, `llm_gateway`) run with `--reload` watching
`/app/src`. Edits under `src/` hot-restart the affected process in
~1 second — no need to re-run `loom service up`. The SPA reloads in
the browser via Vite HMR. Worker is the only one that stays manual:
`docker compose -f deploy/docker-compose.dev.yml restart worker`
after editing worker code.

Tear down: `loom service down` (preserves volumes) or `loom service
down -v` (wipes Postgres + MinIO).

Production deployment: [`docs/operator-runbook.md`](docs/operator-runbook.md).
Provider setup for hosted APIs and Slurm/vLLM self-hosted checkpoints:
[`docs/provider-onboarding.md`](docs/provider-onboarding.md).
To add extra Docker-capable hosts to a shared-dev or staging control
node before full Kubernetes mode, see
[`docs/remote-worker-pool.md`](docs/remote-worker-pool.md). That runbook
includes host inventory, capacity planning, and Slurm submission helpers
for OLDLAB-style pools that should consume every usable worker node near
its stable CPU/RAM/Docker limit. To share the per-trial agent-install
cache across workers via a Docker registry, see
[`docs/operator-runbook.md#trial-cache-per-trial-agent-install`](docs/operator-runbook.md#trial-cache-per-trial-agent-install).

---

## Architecture

A FastAPI **Control Plane** owns the trial state machine, DRF
(Dominant Resource Fairness) scheduling, and the trajectory index.
**Workers** poll for trials, run them in-process against a **Driver**
(Docker, Daytona, Modal — Modal supports GPU passthrough via
`--gpu <TYPE>`), emit append-only JSONL trajectories
to MinIO, and report state via fenced HTTP PATCH endpoints. An **LLM
Gateway** (LiteLLM-backed) proxies model calls so every trajectory
carries faithful token usage and cost. Postgres + MinIO are the only
stateful services; Gateway, Control Plane, `loom_service` (REST), and
the SPA are stateless. The **`loom` CLI** reuses the same
`Trial.run()` against a `LocalDiskObjectStore` +
`UpstreamDirectGatewayClient`, so trajectories + ATIF files are
bit-identical to a service-mode run.

Drill down: [`docs/architecture/overview.md`](docs/architecture/overview.md).

---

## Docs

- [`docs/index.md`](docs/index.md) — navigation entry point
- [`docs/user-guide.md`](docs/user-guide.md) — `loom` CLI reference
- [`docs/architecture/`](docs/architecture/) — focused architecture
  docs (overview, driver protocol, benchmark adapter, agent adapter,
  trajectory + ATIF, CLI mode, service mode)
- [`docs/operator-runbook.md`](docs/operator-runbook.md) — production deployment + ops
- [`docs/authoring-a-task.md`](docs/authoring-a-task.md) — writing a new benchmark task
- [`docs/loom-vs-harbor.md`](docs/loom-vs-harbor.md) — design tradeoffs vs. Harbor + gaps
- [`docs/contributor-quickstart.md`](docs/contributor-quickstart.md)
  — repo layout, tests, coverage gates, dev setup

What shipped when: GitHub releases + `git log` (no separate
CHANGELOG). [`CONTRIBUTING.md`](CONTRIBUTING.md) and
[`SECURITY.md`](SECURITY.md) for the workflow + reporting policy.

---

## Repository governance

Loom is licensed under Apache-2.0. Normal development uses PRs into
`dev`; `main` is reserved for release promotion. The required
`repository-checks` workflow runs with read-only default GitHub token
permissions. Workflows that publish benchmark bundles or deploy
infrastructure use protected GitHub Environments so secrets are not
available to pull request code.

Repository public-readiness was completed in
[#12](https://github.com/carinrc/loom/issues/12). Maintainers should
keep branch protection, read-only default workflow tokens, selected
Actions sources, protected publish/deploy environments, and secret
scanning enabled. External pull requests are not accepted yet; use
issues for discussion until the contribution policy is opened.
