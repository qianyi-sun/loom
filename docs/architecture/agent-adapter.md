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
| `stream_stdout_jsonl` | Each event as a JSON line on stdout | codex, opencode, aider |
| `tail_log_file` | Events to a file path, tailed | swe-agent, mini-swe-agent |
| `poll_local_http` | HTTP `/events?since=N` (curl in-sandbox) | openhands, openhands-sdk |
| `tail_pty` | ANSI terminal output (parsed) | claude-code, gemini-cli, qwen-cli, kimi-cli |

## Shipped adapters

`packages/loom-launcher/loom_launcher/adapters/` (11 production +
`hello` test reference):

| Slug | Capture | Notes |
|---|---|---|
| aider | stdout_jsonl | |
| claude-code | tail_pty | Anthropic's CLI |
| codex | stdout_jsonl | OpenAI Codex CLI |
| gemini-cli | tail_pty | Google Gemini CLI |
| kimi-cli | tail_pty | Moonshot Kimi CLI |
| mini-swe-agent | tail_log_file | |
| opencode | stdout_jsonl | |
| openhands | poll_local_http | port 9999 |
| openhands-sdk | poll_local_http | port 9999, SDK variant |
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
2. Uploads the agent's binary + setup payload to the sandbox
3. Calls `driver.exec_streaming(adapter.build_invocation(...))` to
   launch
4. Drains `adapter.capture_events(...)` into the trajectory writer
5. On step end, kills the subprocess via the ExecHandle

In CLI mode the JWT-minting path is no-op'd — `loom_cli/agent_factory.py`
substitutes a `_NoopCPClient` for the SubprocessAgent's CP-client
dependency since there's no Control Plane to record llm_calls to.
The launched CLI talks to the upstream provider directly using the
env-var API key the CLI puts in the sandbox.

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
