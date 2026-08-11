# Local-LLM execution path

How `loom run` dispatches to a local OpenAI-compatible server (vLLM,
ollama, llama.cpp, lm-studio), and how the optional local CLI vLLM
launcher fits in.

This document is about laptop/CLI execution. Loom service mode does not
host model inference for users; service-mode teams provide their own hosted or
self-hosted OpenAI-compatible endpoint and register it as a provider
connection.

User-facing reference: [`../user-guide.md#local-llms`](../user-guide.md).

## Three entry points

```
  --local-server URL --model ID         inline: user manages the server
  --model local/<server>/<model_id>     persisted: user managed + registered
  --model hf:<org>/<name>               local CLI helper starts vLLM
  --model /path/ (or ~/path/, ./path/)  local CLI helper starts vLLM
```

All four converge on the same `local` provider dispatch inside
`UpstreamDirectGatewayClient._call_local` — they differ only in
**who owns the server lifecycle** and **how the provider entry is
populated**.

```
   --local-server URL              local/<server>/...
   --model ID                                │
        │                                    │
register cfg.local_providers       cfg.local_providers[server]
   ["_inline"] = LocalProvider                │   (persisted via
   (base_url=URL, api_key=...)                │    `loom config set
        │                                    │     local.<name>...`)
        │                                    │
rewrite model →                              │
   ModelSpec(                                │
     provider="local",                       │
     name="_inline/<ID>")                    │
        ▼                                    │
                                              │
   --model hf:<id>                            │
   --model /path/                             │
        │                                    │
launch_vllm(spec) → VLLMServerInfo            │
        │                                    │
register cfg.local_providers["_auto_vllm"]    │
   = LocalProvider(...)                       │
        │                                    │
rewrite model →                              │
   ModelSpec(                                │
     provider="local",                       │
     name="_auto_vllm/<served>")             │
        ▼                                    ▼
                _call_local(request)
                       │
                       ▼
            POST <base_url>/chat/completions
               (OpenAI-compatible HTTP)
                       │
                       ▼
            GatewayCallResponse  ── token-usage + cost-attribution
                                    computed locally from rate cards
```

## Model-spec parsing

`src/loom_cli/run_cmd.py:_parse_model` recognises four shapes:

| Spec                               | Provider | Notes                                                                 |
|------------------------------------|----------|-----------------------------------------------------------------------|
| `anthropic/claude-opus-4-7`        | `anthropic` | Direct provider SDK                                                |
| `local/vllm/meta-llama/Llama-3.1-8B` | `local` | First path segment is the server name; rest is the model id     |
| `hf:meta-llama/Llama-3.1-8B`       | `hf`     | Triggers local CLI vLLM launch; must contain `/`                     |
| `/data/checkpoints/my-tune/` (also `~/…`, `./…`, `../…`) | `file` | Triggers local CLI vLLM launch; detected by leading filesystem marker |

The `--local-server URL` flag bypasses `_parse_model` entirely:
`--model` is treated as the raw upstream model id, and `_run_async`
synthesises `ModelSpec(provider="local", name="_inline/<id>")` after
registering a transient `LocalProvider` for `_inline`.

For `hf:` / path specs, `_run_async` calls `launch_vllm(...)` *before*
the trial starts, registers a transient local provider under
`_auto_vllm`, then rewrites `model` so the rest of the pipeline sees
`local/_auto_vllm/<served>`. Nothing downstream (gateway client,
trial state machine, ATIF projection) knows the launcher exists.

## Why `_inline` and `_auto_vllm` as the server names

Both are leading-underscore on purpose:

1. `LOCAL_NAME_RE` in `loom_cli.config` rejects names starting with
   `_`, so users can't accidentally collide via `loom config set
   local._inline.base_url ...` or `loom config set
   local._auto_vllm.base_url ...`.
2. Reserved stable names so multiple trials in one `loom run` share
   the same transient provider (the launch / registration is gated
   per invocation — only one per `loom run`).
3. The names appear in `cfg.local_providers` mutated in-process only;
   they are never persisted to `~/.config/loom/config.toml`.

`_inline` is for `--local-server` (no subprocess); `_auto_vllm` is
for the local CLI vLLM helper path (the `loom run` process owns the
subprocess).

## Subprocess lifecycle

`src/loom_cli/vllm_runner.py` owns the vLLM subprocess. Public
surface:

```python
@dataclass(frozen=True)
class VLLMLaunchSpec:
    model: str
    port: int = 0                # 0 → autopick from 8234
    host: str = "127.0.0.1"      # loopback-by-default; opt in to LAN
    gpu_memory_utilization: float = 0.90
    tensor_parallel_size: int = 1
    max_model_len: int | None = None
    enforce_eager: bool = False
    extra_args: tuple[str, ...] = ()
    keep_alive: bool = False     # don't tear down at trial end

@dataclass
class VLLMServerInfo:
    base_url: str                # http://localhost:<port>/v1
    served_model_name: str       # what /v1/models advertises
    pid: int

def launch_vllm(spec: VLLMLaunchSpec) -> VLLMServerInfo: ...
def stop_all() -> None: ...
```

Sequence for one `launch_vllm` call:

1. **Dep check** — `shutil.which("vllm")`. Missing → raise
   `MissingVLLMDependencyError` with a copy-paste `pip install
   loom[vllm]` hint.
2. **Cleanup handlers** — install atexit + SIGTERM exactly once per
   process (see below).
3. **Port** — `spec.port` if pinned, else first free TCP port ≥ 8234
   (probed on 127.0.0.1 to match the bind).
4. **Model path** — for local-path specs, resolve + assert exists;
   for HF ids, pass through.
5. **subprocess.Popen** with `vllm serve <model> --host <host> --port
   <port> --gpu-memory-utilization ... --tensor-parallel-size ...`.
   stdout/stderr inherit the parent's fds (avoids 64KB-pipe-fill
   deadlock).
6. **Health probe** — poll `GET <base_url>/models` until 200 OK,
   timeout 300s. Fail fast if `proc.poll()` returns a non-None code:
   surface "exited prematurely with code N" rather than wait for the
   full timeout.
7. **Canonical model id** — read `data[0].id` from `/v1/models`. vLLM
   sometimes shortens HF org/name into something different; using the
   canonical name ensures the chat-completions request resolves.
8. Return `VLLMServerInfo`.

On any exception in steps 6–7, the partial subprocess is stopped
(graceful TERM → 30s wait → KILL fallback) and removed from
`_LIVE_PROCESSES` before re-raising.

## Cleanup model

Two layers, deliberately partitioned by signal type:

| Trigger             | Mechanism                                   | Notes                                                           |
|---------------------|---------------------------------------------|-----------------------------------------------------------------|
| Normal exit         | `atexit.register(stop_all)`                  | Also fires on `KeyboardInterrupt`-induced exit                  |
| Ctrl-C (SIGINT)     | Python default → `KeyboardInterrupt`         | Propagates through `asyncio.run` to `_run_async`'s `try/finally` |
| SIGTERM             | Custom handler → `stop_all()` then `SIG_DFL` | `atexit` does not fire on SIGTERM, so the custom handler performs cleanup |
| Happy-path teardown | `try/finally` around `asyncio.gather` calls `stop_all()` | Closes the GPU-reclaim window before process exit |
| Crash mid-launch    | `launch_vllm` except path stops + removes from registry  | No orphan if step 6/7 raises                       |

**SIGINT behavior:** the default handler lets `asyncio.run` raise
`KeyboardInterrupt`, so the surrounding `try/finally` also tears down Docker
containers and Daytona sandboxes. A custom SIGINT handler would intercept
Ctrl-C before that shared cleanup path runs.

## Security defaults

vLLM binds `127.0.0.1` by default. Loom is the only client (via the
locally-launched chat-completions HTTP); LAN exposure of model
weights + inference would be a surprise on an untrusted network. Opt
in with `--vllm-host 0.0.0.0` if you actually want LAN access.

## Concurrency

`--concurrency N` runs N parallel trials under one `asyncio.Semaphore`.
The vLLM launch happens *once*, before the gather — all trials share
the same server. `_LIVE_PROCESSES` is module-level and tracks every
launcher process owned by the current CLI process.

## What this is NOT

- **Not a multi-launcher abstraction.** Only vLLM is wired.
  ollama / llama.cpp / lm-studio users go through the manual
  `local/<server>/<model_id>` path.
- **Not service-mode-aware.** Service-mode operators register local
  providers via `LOOM_GW_LOCAL_<NAME>_BASE_URL` env vars, dispatched
  inside the LLM Gateway. The launcher only runs in CLI mode
  (`loom_cli` package); the service-mode Gateway has no equivalent.
- **Not batch scheduler-aware.** The launcher supports only a local subprocess;
  it does not submit vLLM servers to an external batch scheduler.

For comparing N models in one run / pre-launching a server with
`loom serve`, see [`multi-server-local-llm.md`](multi-server-local-llm.md).

## Test strategy

`tests/loom_cli/test_vllm_runner.py` mocks `subprocess.Popen` and
`httpx.get` end-to-end. The vLLM dep itself is never imported during
tests — `shutil.which` is monkey-patched. Coverage targets:

- Dep-check + install hint
- Model-path resolution (HF id passthrough, missing file rejection,
  existing directory acceptance)
- Cmd construction (all flags forwarded)
- Free-port autopick (skips bound ports)
- Health probe: 200-OK path, dead-subprocess fail-fast,
  timeout-and-still-running
- `/v1/models` query: happy path, empty-data error
- Stop-process: graceful → kill fallback, no-op when already exited
- Stop-all: drains `_LIVE_PROCESSES`
- End-to-end launch with mocks
- Startup-failure cleanup (process removed from registry)
