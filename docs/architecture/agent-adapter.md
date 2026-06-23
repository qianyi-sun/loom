# Agent adapter

Loom drives three kinds of agents:

1. **OracleAgent** — runs `solution/solve.sh` from the task bundle.
   Reference baseline; no model call.
2. **LiteLLMAgent** — talks to the configured model via LiteLLM
   dialect routing through the Loom Gateway.
3. **SubprocessAgent + a launcher adapter** — wraps an external CLI
   agent (claude-code, codex, openhands, ...) launched as a
   subprocess inside the sandbox. The 11 shipped CLI adapters live
   in `packages/loom-launcher/`.

This doc focuses on #3 — the extensible surface.

## `loom-launcher` framework

```python
from loom_launcher.adapter import AgentAdapter
from loom_launcher.registry import register_adapter
```

An adapter is a class that knows how to:

- Build the argv to launch the agent CLI inside the sandbox
- Stream events out of the running agent (stdout JSONL, log file,
  HTTP poll, or pty)

```python
@runtime_checkable
class AgentAdapter(Protocol):
    name: str                                  # slug, e.g. "claude-code"
    supports_os: frozenset[str]                # {"linux", ...}
    endpoint_dialect: EndpointDialect          # "openai_chat" | "anthropic" | ...
    api_key_env: str                           # e.g. "ANTHROPIC_API_KEY"
    base_url_env: str
    model_name_template: str                   # "{model_id}" | "openai/{model_id}"
    supports_multi_turn: bool                  # metadata only in v1
    additional_egress: frozenset[str]          # extra hostnames beyond Gateway
    install_script: str | None                 # shell script that installs the
                                               # CLI into the trial sandbox; see
                                               # "Per-trial agent installation"

    def build_invocation(
        self, *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]: ...

    def capture_events(
        self, *,
        exec_handle: ExecHandle,
        step_id: str,
        trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEventLike]: ...
```

Adapters are immutable dataclasses (`frozen=True`) — the registry
holds module-level instances. `TrajectoryEventLike` is any
pydantic-serialisable object with `.model_dump()`; the worker's
`SubprocessAgent._bridge()` adapts back to the canonical
`loom.models.trajectory.TrajectoryEvent` union.

Canonical source: `packages/loom-launcher/loom_launcher/adapter.py`.

## Capture utilities

`loom_launcher.capture` ships four reusable capture helpers — most
adapters compose one of them:

| Helper | When agents emit | Used by |
|---|---|---|
| `stream_stdout_jsonl` | Each event as a JSON line on stdout | claude-code, codex, gemini-cli, mini-swe-agent, opencode, openhands, openhands-sdk, hello |
| `tail_log_file` | Events to a file path, tailed | aider, swe-agent |
| `poll_local_http` | HTTP `/events?since=N` (curl in-sandbox) | reserved for server-mode adapters |
| `tail_pty` | ANSI terminal output (parsed) | qwen-cli, kimi-cli |

## Shipped adapters

`packages/loom-launcher/loom_launcher/adapters/` (11 production +
`hello` test reference):

| Slug | Capture | Notes |
|---|---|---|
| aider | tail_log_file | |
| claude-code | stdout_jsonl | Anthropic's CLI |
| codex | stdout_jsonl | OpenAI Codex CLI |
| gemini-cli | stdout_jsonl | Google Gemini CLI |
| kimi-cli | tail_pty | Moonshot Kimi CLI |
| mini-swe-agent | stdout_jsonl | |
| opencode | stdout_jsonl | |
| openhands | stdout_jsonl | legacy name backed by the SDK runner |
| openhands-sdk | stdout_jsonl | SDK variant |
| qwen-cli | tail_pty | Alibaba Qwen CLI |
| swe-agent | tail_log_file | |
| _hello_ | _stdout_jsonl_ | _Reference adapter shipped with the framework — used as a test fixture / minimal example. Not for production use._ |

All adapters self-register via `register_adapter(...)` at module
import time. `loom_cli/__init__.py` eager-imports
`loom_launcher.adapters.*` so the registry is populated before
`get_adapter()` calls.

## SubprocessAgent (the wiring)

`src/loom/agent/subprocess.py` is the glue: it takes an
`AgentAdapter` + a `Driver` + a `Trial.run()` context and:

1. Mints a per-step JWT (so the launched CLI can call the Loom
   Gateway with bounded scope)
2. Calls `driver.exec_streaming(adapter.build_invocation(...))` to
   launch
3. Drains `adapter.capture_events(...)` into the trajectory writer
4. Waits for the subprocess exit code and surfaces non-zero exits as
   `AgentError`
5. If the step is cancelled or event capture fails before the process
   exits, best-effort kills the streaming exec handle so a timed-out
   adapter does not keep running in the sandbox

The adapter package supplies metadata, argv construction, and capture
logic. CLI installation into the trial sandbox is handled at trial
spawn time via the adapter's `install_script` (see [Per-trial agent
installation](#per-trial-agent-installation)). In service mode the API
catalog exposes the adapter's runtime contract as `runtime_contract`
and `service_mode_ready` metadata from `GET /api/v1/agents`, and submit
routes reject agents whose default service-mode sandbox runtime
contract cannot be satisfied.

Service-mode workers expose two gateway URLs because they live in
different network namespaces:

- `LOOM_WORKER_GATEWAY_URL` is the worker process's URL for
  worker-side clients such as `LiteLLMAgent`.
- `LOOM_WORKER_SUBPROCESS_GATEWAY_URL` is optional and is the
  OpenAI-compatible facade base URL as seen from the trial sandbox
  subprocess. Use it when Docker sandboxes cannot resolve the worker
  pod/container's service DNS. The k8s manifest sets it to
  `http://host.docker.internal:30443/openai/v1`, and `DockerDriver`
  injects `host.docker.internal -> host-gateway` when that hostname is
  used.

In CLI mode the JWT-minting path is no-op'd — `loom_cli/agent_factory.py`
substitutes a `_NoopCPClient` for the SubprocessAgent's CP-client
dependency since there's no Control Plane to record llm_calls to.
The launched CLI talks to the upstream provider directly using the
env-var API key the CLI puts in the sandbox.

## Per-trial agent installation

(See `src/loom_worker/trial_cache.py` and `docs/operator-runbook.md`
for the operator-facing knobs.)

`AgentAdapter.install_script` is a multi-line shell script that
installs the adapter's CLI into the trial sandbox. The worker runs it
inside a layered image built on top of the benchmark's `task_image`
before the trial starts. Each adapter is responsible for installing
exactly the runtime it needs (`apk add` / `apt-get install` + a pinned
`npm install -g pkg@version` / `pip install pkg==version`). Versions
MUST be pinned — CI runs `scripts/check_install_scripts_pinned.py`
which AST-parses every adapter module and rejects floating tags.

### Cache key + sharing

```
cache_key = sha256(task_image_digest + install_script_text)[:32]
layered_tag = loom-trial-cache:<cache_key>
```

The key is **content-addressed**: the same `(task_image, adapter)`
pair across teams and workers always produces the same key. The cache
is shared cluster-wide and across users by construction.

The worker's `resolve_trial_image()` runs this flow:

1. Local hit → return `layered_tag` immediately.
2. Otherwise, claim a cluster-wide builder slot from the Control
   Plane (`POST /api/v1/internal/trial-cache/claim`). Non-claimants
   poll `GET /api/v1/internal/trial-cache/{cache_key}` until the slot
   is released, then try local + registry again.
3. With the slot held, try the optional shared registry
   (`{trial_cache_registry_repo}:<cache_key>`). On hit, pull, tag
   locally, release the slot, done.
4. On miss, synthesize a tiny Dockerfile:
   ```dockerfile
   FROM <task_image>
   COPY install.sh /tmp/install.sh
   RUN bash /tmp/install.sh
   ```
   Build with `loom.trial-cache.created-at` label, push to the registry
   if configured (best-effort), release the slot.

The slot has a TTL (`trial_cache_build_lock_timeout_sec`, default 30
min) refreshed every 60 s by an async heartbeat; if the building
worker crashes, the slot expires and any subsequent claimant takes
over. The CP route uses `INSERT ... ON CONFLICT` against the
`active_trial_cache_builds` table — no Postgres advisory locks (which
would tie up CP connection-pool slots for the whole build).

### Optional shared registry

Set `service_config.trial_cache_registry_repo` to a registry path
your workers can pull from and push to (Docker Hub, GHCR, ECR,
self-hosted — anything `docker pull`/`docker push` can reach). When
set, the first worker to build a `(task_image, adapter)` pair pushes
the layered image, and every other worker (across teams) pulls the
hot layer instead of rebuilding. When unset, each worker builds
locally and caches in its own daemon.

The registry path is treated as untrusted on read: pulled images are
validated by tag (`<cache_key>`) and only used after a successful
local `docker image inspect`. Push failures degrade silently — the
local layered image is still produced and used for the current trial.

### Eviction

Docker labels are immutable post-build, so the worker can't do
classical LRU. It uses a TTL prune (`trial_cache_ttl_hours`, default
168 h = 7 days) filtered by the `loom.trial-cache=true` label, with
age determined by Docker's native image-creation timestamp, plus a
capacity backstop (`trial_cache_min_free_gb`, default 20 GB) that
evicts oldest-by-creation entries until disk frees up. Eviction is
local only — registry retention is the registry operator's concern.



Every displayed service-mode agent entry includes runtime metadata:

- `service_mode_ready`: whether the default service-mode worker and
  trial sandbox contract can run this agent today.
- `readiness_status` / `readiness_message`: user-facing setup state
  and an actionable explanation when the agent is gated.
- `runtime_contract.execution`: built-in agent, in-box CLI, or
  subprocess adapter execution mode.
- `runtime_contract.capture`: trajectory capture helper expected for
  the adapter.
- `runtime_contract.required_executables` and
  `runtime_contract.required_python_modules`: sandbox runtime
  dependencies that must exist before enabling the agent.
- `runtime_contract.endpoint_dialect`, `api_key_env`, `base_url_env`,
  and `model_name_template`: provider-facing contract used to validate
  compatible model/provider choices.

The service-mode catalog marks displayed agents ready after their runtime
contract has both dependency-audit and platform-dev smoke evidence. The
readiness flag is not a substitute for per-image validation: if a task uses
a thinner sandbox image, `loom agents audit-runtime` still reports
`blocked` for missing executables or Python modules before users submit a
doomed batch.

Operators can audit a concrete trial sandbox image before enabling an
agent:

```bash
loom agents audit-runtime --image python:3.11-slim
loom agents audit-runtime --image my-agent-sandbox:dev --agent opencode --json
```

For service-mode smoke work, build the candidate all-agent sandbox image
from this repo and audit that exact image:

```bash
docker build -f deploy/Dockerfile.agent-sandbox -t loom-agent-sandbox:dev .
loom agents audit-runtime --image loom-agent-sandbox:dev --json
loom agents smoke-runtime --image loom-agent-sandbox:dev --json
```

The image provisions Node 22 CLI adapters (`claude`, `codex`, `gemini`,
`kimi`, `opencode`, `qwen`) and Python runtimes for `aider`,
`mini-swe-agent`, `openhands`, `openhands-sdk`, and `swe-agent`. `aider`
and `mini-swe-agent` live in isolated virtual environments with PATH
shims. In the all-agent sandbox image, OpenHands, the Loom-owned OpenHands SDK
runner, and SWE-agent stay importable from the main Python 3.12 runtime. When
OpenHands is installed dynamically on top of a benchmark task image, the
adapter instead creates `/opt/loom-agents/openhands-sdk` with pinned `uv` and
Python 3.12, installs `loom-launcher` from a pinned repository subdirectory ref,
then invokes that venv's interpreter so Python 3.11 task images do not block
`openhands-sdk` resolution. SWE-agent is installed editable from its tagged
source tree so its upstream `config/` layout is present at runtime.
The legacy `openhands` adapter name is SDK-backed as well; the historical
`python -m openhands.server` contract is not used for non-interactive
service-mode trials.

The audit runs dependency probes inside the named Docker image and
reports one row per displayed agent. `blocked` means an executable or
Python module declared by `runtime_contract` is missing. `ready` means the
image satisfies the dependency checks and the catalog permits service-mode
submission for that agent. `smoke-runtime` goes further by executing a
minimal platform trial for each selected agent against a deterministic
provider stub. Neither command pulls images implicitly; build or pull the
target sandbox image first so the checks cover exactly what workers will
run.

Dependency audit findings can also expose adapter drift. The upstream
OpenHands SDK wheels provide `openhands.sdk`, not a stable one-shot CLI;
`openhands` and `openhands-sdk` therefore run Loom's
`loom_launcher.openhands_sdk_runner` module and the agent sandbox probes
both that module and `openhands.sdk`. The selected model is not baked into the
benchmark task image; the worker passes the model spec and gateway environment
to the adapter process at trial runtime.

## Adding a new agent adapter

1. New module: `packages/loom-launcher/loom_launcher/adapters/<name>.py`.
2. Implement `AgentAdapter` (`build_invocation` + `capture_events`).
   Pick the capture helper that matches how your agent emits.
3. Register at module level:
   ```python
   from loom_launcher.registry import register_adapter

   class MyAdapter:
       name = "my-agent"
       def build_invocation(self, ...): ...
       async def capture_events(self, ...): ...

   register_adapter(MyAdapter())
   ```
4. Add `tests/test_adapter_my_agent.py` under the sibling package's
   `tests/` (with `_ScriptedSandbox` + scripted event stream — see
   any existing adapter test).
5. Runs surface automatically — `loom run --agent my-agent` resolves
   via `loom_launcher.get_adapter("my-agent")` once the eager-import
   path in `loom_cli/__init__.py` picks up the new module.

## Common pitfalls

- **Capture helper choice**: if the agent emits structured JSON,
  prefer `stream_stdout_jsonl` (lowest overhead, lowest parsing
  cost). PTY capture is heaviest — only use it for terminal-UI CLIs
  whose output is intended for humans.
- **No nested `bind_trial_context`**. The contextvars helper used
  for log correlation does **not** nest — bind at the outermost
  scope only (typically inside `Trial.run()`). Nesting silently drops
  the inner binding when the outer scope exits.
- **Per-step JWT scope**: minted JWTs are valid for the current
  step only. If your agent runs across step boundaries (e.g. a
  multi-step compose flow), mint a fresh token per step.

## See also

- [overview.md](overview.md)
- [trajectory-and-atif.md](trajectory-and-atif.md) — the
  `TrajectoryEvent` types your `capture_events` yields
- [driver-protocol.md](driver-protocol.md) — `ExecHandle` your
  adapter receives
- `packages/loom-launcher/loom_launcher/adapters/codex.py` — typical
  stdout-JSONL adapter
- `packages/loom-launcher/loom_launcher/adapters/openhands.py` —
  typical HTTP-poll adapter
- `packages/loom-launcher/loom_launcher/adapters/claude_code.py` —
  typical PTY adapter
