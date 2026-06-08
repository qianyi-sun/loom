# Loom — Agent Integrations Design

**Status:** DRAFT — awaiting user review.
**Date:** 2026-06-06
**Owner:** Hongjian + Claude.
**Scope:** Adapters for 11 third-party agent runtimes covering the slate Harbor supported, plus the Gateway + Driver + auth extensions they require. The runtime core itself (v0.7) stays unchanged where possible; this spec is additive.

---

## 1. Goal

Loom v0.7 ships three first-party agent runtimes (Oracle, LiteLLM, ClaudeCode). To match Harbor's surface and serve real research workloads, we need adapters for the eleven agents listed below, with uniform cost attribution, trajectory capture, and image management.

The eleven agents (Harbor parity):

| Agent           | Dialect             | Shape       | Capture mechanism      |
|-----------------|---------------------|-------------|------------------------|
| codex (OpenAI)  | openai_responses    | CLI         | tail_pty (TUI today)   |
| opencode        | openai_chat         | CLI         | stream_stdout_jsonl    |
| aider           | openai_chat         | Python lib  | tail_log_file          |
| openhands       | openai_chat         | Server      | poll_local_http        |
| openhands-sdk   | openai_chat         | Python lib  | stream_stdout_jsonl    |
| swe-agent       | openai_chat         | Python lib  | tail_log_file          |
| mini-swe-agent  | openai_chat         | Python lib  | stream_stdout_jsonl    |
| claude-code     | anthropic           | CLI         | stream_stdout_jsonl    |
| gemini-cli      | gemini              | CLI         | stream_stdout_jsonl    |
| qwen-cli        | openai_chat         | CLI         | tail_pty               |
| kimi-cli        | openai_chat         | CLI         | tail_pty               |

The already-shipped `ClaudeCodeAgent` (in-box) and `LiteLLMAgent` (out-of-box) stay in tree as backwards-compatible runtimes through v1.0; new development uses the `SubprocessAgent` shape from this spec.

## 2. Architecture

Five additive pieces. Each ships independently:

```
PyPI: loom-launcher (NEW)
  ├─ AgentAdapter Protocol            ← contract
  ├─ 11 adapter instances             ← per-agent metadata + capture impl
  └─ 3 capture patterns               ← stream_stdout_jsonl, tail_log_file, poll_local_http

src/loom/agent/
  └─ subprocess.py                    ← SubprocessAgent (generic AgentRuntime)

src/loom/driver/base.py
  └─ exec_streaming() method          ← additive to Driver Protocol

src/loom_llm_gateway/
  ├─ routes/responses.py              ← /v1/responses (OpenAI Responses)
  ├─ routes/messages.py               ← /v1/messages (Anthropic Messages)
  ├─ routes/gemini.py                 ← /v1beta/models/{model}:generateContent
  └─ auth.py                          ← JWT branch added to verify_bearer_token

src/loom_control_plane/routes/
  └─ step_tokens.py                   ← POST /admin/step-tokens
```

The launcher is the only new Python package shipped to PyPI. Workers install it (via the `loom-launcher` PyPI package) to access the adapter registry — they need adapter metadata (env vars, argv, capture pattern) to construct a `SubprocessAgent`. Workers do NOT execute the agent itself; that runs inside the sandbox, where the same `loom-launcher` is installed by the task's Dockerfile and provides the runtime `capture_events` impl. Two consumers, one package.

**Image management:** zero Loom-built images required. Each task's `environment/Dockerfile` installs the agent it uses plus `pip install loom-launcher`. Loom optionally publishes `loom-agent-{popular-name}` base images (claude-code, gemini-cli, aider) as a convenience — task authors can `FROM loom-agent-aider:0.8` to skip the install. Convenience layer, not required.

## 3. AgentAdapter Protocol

`loom_launcher.AgentAdapter` (lives in the PyPI package):

```python
@runtime_checkable
class AgentAdapter(Protocol):
    name: str
    supports_os: frozenset[OS]
    endpoint_dialect: Literal[
        "openai_chat", "openai_responses", "anthropic", "gemini",
    ]
    api_key_env: str                    # e.g. "OPENAI_API_KEY"
    base_url_env: str                   # e.g. "OPENAI_BASE_URL"
    model_name_template: str            # "{model_id}" or "openai/{model_id}"

    # Runtime/policy metadata
    supports_multi_turn: bool           # if False, each step → fresh invocation
    additional_egress: frozenset[str]   # hostnames beyond the Gateway (telemetry,
                                        # auto-update). Unioned into the trial's
                                        # NetworkPolicy allowlist by
                                        # derive_requires_caps.

    def build_invocation(
        self, *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        """Return argv for the agent's CLI/launcher. `env` is mutated in-place
        if the adapter needs to set additional env vars beyond api_key_env
        and base_url_env (which SubprocessAgent sets automatically)."""

    async def capture_events(
        self, *, exec_handle: ExecHandle, step_id: str, trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEvent]:
        """Yield trajectory events for the duration of the run. The
        adapter chooses how — streaming the ExecHandle's stdout, polling
        a local HTTP server inside the sandbox, or tailing a log file.
        Adapter terminates when the agent process exits."""
```

Three reusable capture primitives ship in `loom_launcher.capture`:

- **`stream_stdout_jsonl(handle)`** — yields events from a process that emits one JSON object per line on stdout. Used by claude-code (`--output-format stream-json`), gemini-cli (`--output json`), mini-swe-agent, opencode, openhands-sdk.
- **`tail_log_file(env, path, poll_interval_sec=0.5)`** — polls a file inside the sandbox via `env.read_file()`, yields events as new lines appear, exits when the agent process is gone and no new bytes appear. Used by aider (`.aider.chat.history.md`) and swe-agent (`trajectory.jsonl`).
- **`poll_local_http(handle, port, path="/events")`** — opens a TCP connection through the sandbox's network (loopback inside the container is reachable from worker only via a port forward; in v1 we use `docker exec` curl inside the container instead, since the Worker doesn't have a route into the container's loopback). Used by openhands.

For TUI-only agents (codex, qwen-cli, kimi-cli at time of writing), the adapter uses `tail_pty(handle)` which scrapes a known prompt pattern from the PTY output. This is documented as best-effort; events lose structured fields like `tool_calls` and degrade to `AgentThoughtEvent` only. Adapters mark `degraded=True` in metadata so dashboards can flag these trajectories.

## 4. SubprocessAgent runtime

`loom.agent.subprocess.SubprocessAgent` is a generic `AgentRuntime` impl wrapping any `AgentAdapter`:

```python
@dataclass
class SubprocessAgent:
    adapter: AgentAdapter
    model: ModelSpec
    gateway_url: str
    cp_client: HttpControlPlaneClient   # for minting step JWTs

    name: str = field(init=False)       # = adapter.name
    version: str = "1.0"
    supports_os: frozenset[OS] = field(init=False)

    async def run(self, *, instruction, env, trajectory, mcp,
                  skills_dir, step_id, trial_id):
        # 1. Mint a step-scoped JWT (Section 6).
        step_token = await self.cp_client.mint_step_token(
            trial_id=trial_id, step_id=step_id,
            ttl_sec=int(self.model.timeout_sec or 1800),
        )

        # 2. Build env + argv.
        env_vars = {
            self.adapter.api_key_env: step_token,
            self.adapter.base_url_env: self.gateway_url,
        }
        argv = self.adapter.build_invocation(
            instruction=instruction,
            workdir=env.workdir,
            model=self.model,
            env=env_vars,
        )

        # 3. Streaming exec (Section 5).
        handle = await env.exec_streaming(
            argv, env_vars=env_vars, cwd=env.workdir,
        )

        # 4. Adapter captures + we forward events.
        async for event in self.adapter.capture_events(
            exec_handle=handle, step_id=step_id, trial_id=trial_id,
        ):
            await trajectory.emit(event)

        rc = await handle.wait()
        if rc != 0:
            raise AgentError(
                f"{self.adapter.name} exited rc={rc} on step {step_id}",
            )
```

Per-step semantics (multi-turn deferred):
- v1 ships only single-shot execution: each step gets a fresh `agent.run()`, fresh process, no context carry. This matches how most CLI agents already work.
- The `supports_multi_turn` flag on `AgentAdapter` is metadata about the agent's capability, NOT a runtime switch in v1. SubprocessAgent ignores it. v1.5 adds the persistent-session path: SubprocessAgent keeps the `ExecHandle` alive across steps and exposes `inject_instruction(handle, instruction)` for adapters whose flag is True. Section 8.1's table annotates which adapters' agents support multi-turn so v1.5 work doesn't need re-discovery.

## 5. Driver Protocol extension

Plan 2's `Driver` Protocol grows one additive method. Existing `exec()` stays for short commands; long-running agents use `exec_streaming()`:

```python
@dataclass
class ExecHandle:
    pid: int
    stdout: AsyncIterator[bytes]    # chunked, unbuffered
    stderr: AsyncIterator[bytes]
    async def wait(self) -> int: ...
    async def kill(self) -> None: ...

class Driver(Protocol):
    # ... existing methods unchanged ...

    async def exec_streaming(
        self,
        argv: list[str],
        *,
        env_vars: dict[str, str],
        cwd: PurePosixPath,
        user: str | int | None = None,
    ) -> ExecHandle:
        """Start a process and return immediately. Caller iterates stdout
        / stderr (each yield is whatever chunk size the underlying driver
        produces), then `await handle.wait()` for the exit code. Driver
        buffers nothing — chunks flow through. No 10 MB cap."""
```

`FakeDriver` gains a `scripted_handle_factory` test helper that constructs a fake `ExecHandle` from a scripted (stdout_chunks, stderr_chunks, return_code) tuple.

`DockerDriver` implements `exec_streaming` via `docker.APIClient.exec_create()` + `exec_start(stream=True, demux=True)`, which returns a generator of `(stdout_chunk, stderr_chunk)` tuples — wrapped in two `asyncio.Queue` consumers.

Existing `exec()` callers (every Plan 2/3 site) remain on the buffered path; only `SubprocessAgent` calls `exec_streaming`.

## 6. Gateway multi-dialect endpoints + step JWT

### 6.1 Step-scoped JWT for cost attribution

Native dialects (openai_chat, openai_responses, anthropic, gemini) don't carry Loom's `loom: {team_id, trial_id, step_id}` block. To preserve per-trial cost attribution, the agent's bearer token IS the trial context.

Control Plane adds `POST /admin/step-tokens`:

```
POST /admin/step-tokens
Authorization: Bearer <worker-token>   (scope: "worker:report")
{
  "team_id": "...",
  "trial_id": "...",
  "step_id": "main",
  "ttl_sec": 1800
}
→
{
  "token": "loom_step_eyJ...",          # JWT, HS256
  "expires_at": "2026-06-06T19:42:11Z"
}
```

JWT payload claims:

```json
{
  "iss": "loom-control-plane",
  "sub": "step-session",
  "team_id": "...",
  "trial_id": "...",
  "step_id": "main",
  "exp": 1717693331,
  "scopes": ["llm:call"]
}
```

The signing key is a single HMAC secret in `loom-secrets/step-jwt-signing-key`, shared between Control Plane and Gateway. (Asymmetric RS256 reserved for v1.5 when we have external Gateways.) Workers don't see the key — they receive minted tokens.

### 6.2 Gateway JWT verification

`loom.auth.verify_bearer_token` (shared between Control Plane and Gateway) gains a JWT branch:

```python
async def verify_bearer_token(session, raw_token: str | None) -> AuthContext | None:
    if raw_token is None:
        return None
    if raw_token.startswith("loom_step_"):
        return await _verify_step_jwt(raw_token[len("loom_step_"):])
    # existing DB hash-lookup path for team/worker/admin tokens
    return await _verify_db_token(session, raw_token)
```

`_verify_step_jwt` validates signature + exp, returns an `AuthContext` with `team_id`, `trial_id`, `step_id`, and the synthetic scope `["llm:call"]`. Calls fall through to dialect handlers below.

### 6.3 Dialect endpoints

Three new routes mounted under `loom_llm_gateway/routes/`:

| Endpoint                                          | Body schema                       | Response shape                | LiteLLM call                 |
|---------------------------------------------------|-----------------------------------|-------------------------------|------------------------------|
| `POST /v1/messages`                               | Anthropic Messages body           | Anthropic Messages response   | **Native passthrough** to Anthropic via httpx (LiteLLM's openai-shape round-trip would lose `cache_control`, system blocks, tool_use content blocks). Gateway extracts `usage` from the native response for `llm_calls` writing. |
| `POST /v1/responses`                              | OpenAI Responses body             | OpenAI Responses response     | **Native passthrough** to OpenAI Responses API via httpx (LiteLLM does not yet have a stable Responses adapter). |
| `POST /v1beta/models/{model}:generateContent`     | Gemini generateContent body       | Gemini generateContent response | **Native passthrough** to Gemini via httpx (preserves `functionCall` parts, `cachedContent`, `thoughtsTokenCount`). |

All four endpoints (existing chat + three new) share:
- Auth via `verify_bearer_token` → `AuthContext` (now carries team_id/trial_id/step_id from the JWT branch)
- Rate-card lookup via `request.app.state.rate_card_cache.lookup(provider, model, tier=None, region=None)` — same signature as v0.7
- Cost compute via `compute_cost_usd(rate_card, input_tokens, output_tokens, provider_extras)` — same function, extracts tokens from each dialect's response format
- Cost row written to `llm_calls` (see Section 7)

**Why native passthrough, not LiteLLM normalization.** LiteLLM's chat-completions abstraction maps every provider into an OpenAI-shaped envelope and back. That round-trip is lossy for: Anthropic `cache_control` markers on content blocks; Anthropic system-block separation from user messages; Anthropic `tool_use` / `tool_result` typed content blocks; Gemini multi-part `inlineData` for images; OpenAI Responses' `reasoning` content type. For the chat dialect (Plan 4's existing endpoint) LiteLLM is fine. For native dialects we keep the original shape on the wire and let the agent's SDK speak its language.

A small `loom_llm_gateway.dialect` module owns the four dialect-specific token-extractors and cost rollups:

```python
@dataclass
class DialectAdapter:
    name: str
    extract_tokens: Callable[[dict], TokenUsage]
    extract_tool_calls: Callable[[dict], list[ToolUseEvent]]
    extract_provider_extras: Callable[[dict], dict[str, int]]
```

`TokenUsage` is the existing model from Plan 4 (input_tokens, output_tokens, provider_extras). Each dialect adapter pulls the right counters from its response:

- **anthropic** → `usage.input_tokens`, `usage.output_tokens`, plus `cache_creation_input_tokens`, `cache_read_input_tokens` in provider_extras.
- **gemini** → `usageMetadata.promptTokenCount`, `candidatesTokenCount`, plus `cachedContentTokenCount`, `thoughtsTokenCount`.
- **openai_responses** → `usage.input_tokens`, `output_tokens`, plus `output_tokens_details.reasoning_tokens`.
- **openai_chat** → existing logic in Plan 4 unchanged.

## 7. Trajectory + ATIF projection

### 7.1 Two write paths, deduped

Two independent paths emit trajectory events. The trial's local trajectory
JSONL (the worker's write target — Plan 2's `TrajectoryWriter`) holds the
semantic events; the `llm_calls` table holds the cost/usage rows
attributed to the trial. They are joined at trial finalize, BEFORE the
ATIF projection runs.

1. **Gateway path** (canonical for LLM calls): every dialect endpoint, after `acompletion()` succeeds, inserts a row into `llm_calls`:
   ```sql
   CREATE TABLE llm_calls (
       id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
       team_id       uuid NOT NULL,
       trial_id      uuid NOT NULL,
       step_id       text NOT NULL,
       model         text NOT NULL,
       dialect       text NOT NULL,
       input_tokens  int  NOT NULL,
       output_tokens int  NOT NULL,
       provider_extras jsonb NOT NULL DEFAULT '{}',
       cost_usd      numeric(12,6) NOT NULL,
       rate_card_hash text NOT NULL,
       captured_at   timestamptz NOT NULL DEFAULT now()
   );
   CREATE INDEX llm_calls_step_idx ON llm_calls (trial_id, step_id, captured_at);
   ```
   At trial finalize, the worker calls a new Control Plane endpoint
   `GET /trials/{trial_id}/llm-calls` (added to Plan 9) which returns the
   rows for that trial; the worker projects each row to one
   `LLMCallEvent` and appends them to the local trajectory JSONL
   **before** finalize uploads the JSONL to MinIO + runs the ATIF
   projection. This keeps `project_to_atif()` pure (Plan 2 contract
   preserved) while sourcing LLM cost facts from the canonical Gateway
   table rather than a duplicated agent-side capture.

2. **Agent-side capture path** (semantic events): the adapter's `capture_events()` emits `ToolUseEvent`, `AgentThoughtEvent`, and `EnvExecEvent` based on the agent's stdout / logs / HTTP feed. These cover *what the agent did* (tool calls, reasoning, shell commands) — orthogonal to *what the LLM cost*.

**Dedup rule:** the Gateway path owns `LLMCallEvent` (sourced from
`llm_calls` at finalize); the adapter's `capture_events()` NEVER emits
`LLMCallEvent` even if the agent's output echoes the LLM exchange. If an
adapter's output stream contains assistant text or tool-call structure,
it emits `AgentThoughtEvent` / `ToolUseEvent` instead. The agent-side
path owns those. No overlap.

### 7.2 ATIF projection

Existing `project_to_atif()` is unchanged. It already groups events by step_id; both write paths use the same step_id, so events interleave naturally on the trajectory timeline and project cleanly into ATIF v1.7.

Two new ATIF metadata fields populated from `provider_extras`:
- `llm_total_reasoning_tokens` (sum across all calls, where the dialect exposes it)
- `llm_total_cached_tokens` (sum of cache_read_input_tokens + cachedContentTokenCount)

Adding fields is additive in ATIF v1.7; the projection function `provider_extras` rollup is one new line in `loom.trajectory.atif`.

## 8. Per-adapter inventory

One file per adapter under `loom_launcher/adapters/`. Each is ~60-100 LOC. Schema:

```python
# loom_launcher/adapters/claude_code.py
from loom_launcher import AgentAdapter, capture

@dataclass(frozen=True)
class ClaudeCodeAdapter(AgentAdapter):
    name = "claude-code"
    supports_os = frozenset({"linux"})
    endpoint_dialect = "anthropic"
    api_key_env = "ANTHROPIC_API_KEY"
    base_url_env = "ANTHROPIC_BASE_URL"
    model_name_template = "{model_id}"
    supports_multi_turn = False
    additional_egress = frozenset()   # telemetry + auto-update disabled via env vars

    def build_invocation(self, *, instruction, workdir, model, env):
        # Real claude-code CLI: instruction is the positional `--print` arg;
        # there is no `--workdir` or `--no-update-check` flag. Auto-update +
        # telemetry are disabled via env vars (verified against claude-code
        # release notes / docs at spec write-time; pin via sandbox image).
        env["DISABLE_TELEMETRY"] = "1"
        env["CLAUDE_CODE_AUTO_UPDATE"] = "false"
        return [
            "sh", "-c",
            (
                f"cd {shlex.quote(str(workdir))} && "
                f"claude --output-format stream-json "
                f"--model {shlex.quote(model.name)} "
                f"--print {shlex.quote(instruction)}"
            ),
        ]

    async def capture_events(self, *, exec_handle, step_id, trial_id):
        async for event in capture.stream_stdout_jsonl(exec_handle):
            yield event
```

### 8.1 Per-agent table (concrete decisions)

| Adapter         | api_key_env       | base_url_env             | model_template          | supports_multi_turn | Capture          | additional_egress              | Notes                                       |
|-----------------|-------------------|--------------------------|-------------------------|---------------------|------------------|--------------------------------|---------------------------------------------|
| codex           | OPENAI_API_KEY    | OPENAI_BASE_URL          | {model_id}              | false               | tail_pty         | none                           | Degraded fidelity; switch to JSON when upstream ships it |
| opencode        | OPENAI_API_KEY    | OPENAI_BASE_URL          | openai/{model_id}       | false               | stream_stdout_jsonl | none                       |                                             |
| aider           | OPENAI_API_KEY    | OPENAI_API_BASE          | openai/{model_id}       | true (v1.5)         | tail_log_file    | none (telemetry disabled via env) | `AIDER_NO_TELEMETRY=1` set in build_invocation |
| openhands       | LLM_API_KEY       | LLM_BASE_URL             | openai/{model_id}       | true (v1.5)         | poll_local_http  | none                           | Adapter launches OpenHands in server mode    |
| openhands-sdk   | LLM_API_KEY       | LLM_BASE_URL             | openai/{model_id}       | false               | stream_stdout_jsonl | none                       |                                             |
| swe-agent       | OPENAI_API_KEY    | OPENAI_API_BASE          | openai/{model_id}       | false               | tail_log_file    | none                           | Reads swe-agent's own `trajectory.jsonl` inside the sandbox |
| mini-swe-agent  | OPENAI_API_KEY    | OPENAI_BASE_URL          | openai/{model_id}       | false               | stream_stdout_jsonl | none                       |                                             |
| claude-code     | ANTHROPIC_API_KEY | ANTHROPIC_BASE_URL       | {model_id}              | true (v1.5)         | stream_stdout_jsonl | none                       | Replaces v0.7's `ClaudeCodeAgent` for new tasks |
| gemini-cli      | GOOGLE_API_KEY    | GOOGLE_GEMINI_BASE_URL   | google/{model_id}       | true (v1.5)         | stream_stdout_jsonl | none                       |                                             |
| qwen-cli        | OPENAI_API_KEY    | OPENAI_BASE_URL          | {model_id}              | false               | tail_pty         | none                           | Same degraded fidelity caveat as codex       |
| kimi-cli        | OPENAI_API_KEY    | OPENAI_BASE_URL          | openai/{model_id}       | false               | tail_pty         | none                           |                                             |

## 9. Network policy + egress

`derive_requires_caps` (Plan 5's pure transform from `TaskConfig` → `RequiredCapabilities`) grows one rule: if the task's chosen agent has a `subprocess` runtime (i.e., is an `AgentAdapter`), union `adapter.additional_egress` into the baseline `network_policies` allowlist. This auto-grants telemetry/update endpoints the adapter needs without task authors writing them per-task.

For agents whose telemetry can be disabled via env var (most), the adapter sets the env var in `build_invocation` and `additional_egress = frozenset()`. For agents that hard-require an upstream endpoint (e.g., openhands hitting its own version-check server), the egress is documented in the adapter.

## 10. Worker integration

`loom_worker.main_loop._default_agent_factory` gains a third branch:

```python
def make(task_dir, gateway, model, *, agent_name: str):
    # `agent_name` is read from task_config.agent.name by the caller —
    # task_dir is the materialized fixture root; we don't re-parse the
    # toml inside the factory.
    if agent_name == "oracle":
        return OracleAgent(...)
    if agent_name == "litellm":
        return LiteLLMAgent(...)                  # v0.7 backwards-compat
    if agent_name == "claude-code-inbox":
        return ClaudeCodeAgent(...)               # v0.7 in-box runtime
    # New subprocess-style agents — look up the adapter by name.
    from loom_launcher import get_adapter
    adapter = get_adapter(agent_name)
    if adapter is None:
        raise ConfigError(f"unknown agent {agent_name!r}")
    return SubprocessAgent(
        adapter=adapter, model=model,
        gateway_url=settings.gateway_url,
        cp_client=cp_client,
    )
```

`get_adapter()` is a name → instance registry in `loom_launcher.__init__`. Adapters self-register at import time via a small decorator. Workers `pip install loom-launcher` to access the registry (the launcher CAN be used both inside the sandbox and on the worker — when used on the worker, only the adapter metadata is read; `build_invocation` and `capture_events` produce data structures the worker passes through to `SubprocessAgent`).

## 11. Testing strategy

Three test tiers:

1. **Contract tests** (one per adapter, in the `loom-launcher` repo): hold a fake `ExecHandle` with scripted stdout, assert `capture_events` yields the expected `TrajectoryEvent` sequence. ~150 LOC each. Loom's own `tests/` directory gains a single integration test that exercises the registry round-trip (workers can resolve adapter X by name).
2. **Gateway dialect tests** (one per dialect): hold a fake `acompletion` returning a canned dialect response, assert the route returns the correct dialect shape AND inserts the correct `llm_calls` row. Lives in `tests/integration/test_gateway_{dialect}.py`. Uses the same ASGITransport pattern as Plan 4.
3. **End-to-end smoke** (one per agent, opt-in): spin up the agent's actual sandbox image, run a hello-world task that doesn't require a real LLM (uses an `aider --offline` mode or a mock backend), assert trajectory contains expected event shapes. Lives in `tests/system/test_agent_{name}_smoke.py`. Docker-gated.

Property tests:
- Adapter conformance: every registered adapter must (a) be JSON-serializable, (b) have non-empty `name`, (c) return a non-empty `argv` from `build_invocation` for a stub instruction, (d) declare a known `endpoint_dialect`.
- Gateway dialect round-trip: any canned response in a dialect → `extract_tokens` → `compute_cost_usd` produces a non-negative cost.

## 12. Migration + backwards compatibility

- `LiteLLMAgent` (v0.7) stays in tree and remains selectable via `agent.name = "litellm"`.
- v0.7's `ClaudeCodeAgent` is **renamed** to `agent.name = "claude-code-inbox"` to avoid the name collision with this spec's subprocess-style `claude-code` adapter (the v0.7 runtime is in-box; the new adapter is out-of-box). Migration: any existing `task.toml` with `agent.name = "claude-code"` that was authored against v0.7 must be updated to `"claude-code-inbox"`. Both runtimes ship through v1.0.
- The `tokens` table gains no schema change. Step JWTs are stateless.
- The new `llm_calls` table is additive; existing Gateway state is untouched.
- The `Driver` Protocol grows `exec_streaming()` as an additive method; existing implementations (FakeDriver, DockerDriver) MUST implement it. No callers of the existing `exec()` change.

## 13. Out of scope (explicit)

- **Multi-turn sessions across steps.** Architecture supports it; runtime ships only single-shot v1. v1.5 work.
- **Custom dialect plugins.** The four dialects are baked into the Gateway. A 5th dialect requires a Gateway code change.
- **Asymmetric JWT keys.** v1 uses HS256 with a shared secret. RS256 / external Gateway federation is v1.5.
- **Token leak revocation.** Step JWTs aren't in the DB; the only way to invalidate one before exp is to rotate the signing key. v1 accepts this; step TTLs are short (≤ step_timeout, typically ≤30 min).
- **In-flight cost throttling.** Per-team budget enforcement happens at trial submit (Plan 5's quota check). Per-call mid-trial throttling not implemented; runaway agents are bounded by step_timeout.

## 14. Open questions

None remaining at spec write-time.

## 15. Implementation sequencing

Five plans, dependency-ordered. Each is independently shippable:

1. **Plan 8 — Driver Protocol extension.** Adds `exec_streaming` to `Driver`, `ExecHandle` dataclass, FakeDriver impl, DockerDriver impl, contract + integration tests. ~2 days, no downstream deps.
2. **Plan 9 — Gateway multi-dialect.** Three new routes + JWT verification branch + `llm_calls` table + dialect adapters. Depends on Plan 8 only for testing (workers don't change yet). ~4 days.
3. **Plan 10 — loom-launcher package + AgentAdapter Protocol.** New PyPI package, three capture patterns, registry. No Loom changes — pure new code. ~3 days.
4. **Plan 11 — SubprocessAgent runtime + worker integration.** `loom.agent.subprocess.SubprocessAgent`, `mint_step_token` on HttpControlPlaneClient, factory branch, end-to-end happy path. Depends on Plans 8, 9, 10. ~3 days.
5. **Plan 12 — 11 adapters.** One per agent, parallelizable. Each adapter is ~100 LOC + 1 contract test. ~5 days total if done sequentially; ~2 days with parallel review.

Total: ~17 working days from spec sign-off to all 11 adapters shipping. Plans 8 + 9 + 10 can land independently; the rest stack.
