# Loom

A team platform for running LLMs on customizable tasks, tracking
their performance, and saving results for download.

LLMs are first-class — point at any cloud provider (Anthropic,
OpenAI, Google) or any **local** OpenAI-compatible server (vLLM,
ollama, llama.cpp, lm-studio). See
[`docs/user-guide.md#local-llms`](docs/user-guide.md) for the local
workflow. Tasks and agents are **pluggable infrastructure**, not
assumptions:

- **Tasks** — 14 benchmark adapters ship out of the box (HumanEval,
  SWE-Bench Verified/full/Multimodal, MBPP, LiveCodeBench, BFCL,
  GAIA, AIME, OSWorld, WebArena, SkillFlow, SkillLearnBench,
  Terminal-Bench-2.0). Add your own by implementing a
  `BenchmarkAdapter` Protocol — your task isn't locked to anyone
  else's leaderboard.
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

Two ways to consume:

- **`loom` CLI** — `pip install` and run benchmarks against agents on
  your laptop. No server stack.
- **Service mode** — Control Plane + Workers + LLM Gateway + Postgres
  + MinIO, with a React SPA at `web/` for browsing trials, campaigns,
  and per-team usage. `loom service up` brings the whole stack up in
  one command.

---

## Quick start — laptop (`loom run`)

```bash
pip install -e . -e packages/loom-launcher -e packages/loom-benchmarks

loom datasets list                          # what's available
loom config set token.anthropic sk-ant-xxx  # one-time provider key setup

# Smoke test: one HumanEval task, oracle baseline, no real agent.
loom run --task humaneval/HumanEval/0 \
         --agent oracle --backend fake \
         --output-dir /tmp/loom-smoke --json

# Real eval against a cloud provider (Anthropic, OpenAI, Google).
loom run --dataset swe-bench-verified \
         --agent claude-code --model anthropic/claude-opus-4-7 \
         --backend docker --concurrency 4 --output-dir ./runs
```

### Against a locally-served LLM

Loom accepts any OpenAI-compatible local server (vLLM, ollama,
llama.cpp, lm-studio) — point it at your model, register the
server's URL once, run trials against it like any other provider:

```bash
# 1. Start your server. Example: vLLM.
vllm serve meta-llama/Llama-3.1-8B-Instruct
# or:  ollama serve  &&  ollama pull llama3.1
# or:  ./llama-server -m model.gguf --port 8080

# 2. Tell Loom where to find it (one-time per server).
loom config set local.vllm.base_url http://localhost:8000/v1
# optional, only if the server requires auth (e.g. vLLM --api-key):
loom config set local.vllm.api_key sk-foo

# 3. Sanity-check (recommended).
loom models test local/vllm
# → ✓ vllm reachable at http://localhost:8000/v1
#     models advertised by /v1/models: 1
#       • meta-llama/Llama-3.1-8B-Instruct

# 4. Run a trial. Model spec is `local/<server>/<model_id>` —
#    `<server>` is your chosen name; `<model_id>` is what
#    /v1/models returns.
loom run --task humaneval/HumanEval/0 \
         --agent litellm \
         --model local/vllm/meta-llama/Llama-3.1-8B-Instruct \
         --backend docker --output-dir ./runs
```

Register multiple servers (`local.ollama.base_url ...`,
`local.lmstudio.base_url ...`) and pick one per trial. Local trials
cost $0 by default; add a rate-card row keyed
`provider="local:<server>"` to attribute internal GPU cost.

Per-trial outputs land at `./runs/<trial-id>/events.jsonl` (full
event trajectory) + `./runs/<trial-id>/atif.json` (ATIF v1.7
projection). Full CLI reference, including service-mode env-var
config (`LOOM_GW_LOCAL_<NAME>_BASE_URL`):
[`docs/user-guide.md`](docs/user-guide.md).

---

## Quick start — service mode (`loom service up`)

```bash
cp .env.example .env             # add your provider API keys
loom service up                  # docker compose + migrations + token bootstrap
```

`loom service up` brings up the full stack (Postgres + MinIO + LLM
Gateway + Control Plane + loom_service + Worker), runs migrations,
seeds a team token, and prints the bearer + endpoint URLs. Then:

```bash
TEAM_TOKEN=$(loom service up | grep loom_team_ | tr -d ' ')   # or copy from output

# Submit a trial via the user-facing REST surface (loom_service).
curl -X POST http://localhost:8090/api/v1/trials \
  -H "Authorization: Bearer $TEAM_TOKEN" \
  -d '{"task_id":"hello-world","config":{}}'
```

Interactive API docs at `http://localhost:8090/docs` (Swagger UI,
auto-generated by FastAPI). MinIO console at `http://localhost:9001`.

Tear down: `loom service down` (preserves volumes) or `loom service
down -v` (wipes Postgres + MinIO).

Production deployment: [`docs/operator-runbook.md`](docs/operator-runbook.md).

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
