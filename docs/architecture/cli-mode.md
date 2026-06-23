# CLI mode

`loom run` is a **stateless** wrapper around the same `Trial.run()`
orchestrator service mode uses. No Control Plane, no Worker, no
Gateway, no Postgres, no MinIO required.

## What the CLI actually does

```
loom run --task humaneval/HumanEval/0 --agent oracle --backend docker ...
  │
  ▼
src/loom_cli/__main__.main()                    parse argparse
  │
  ▼
src/loom_cli/run_cmd.run() → _run_async()       async dispatch
  │
  ├─► load_tasks(dataset, task_filter, ...)     fetch + convert tasks
  │     │
  │     ├─► fetch_upstream(...)                 git clone / HF download
  │     ├─► adapter.list_instances(...)
  │     └─► adapter.convert_instance(...)       → task.toml + instruction.md + tests/
  │
  ├─► _driver_factory(--backend, cfg)           → Callable[[], Driver]
  │
  ├─► _build_sdk_clients(tokens)                → anthropic / openai / google clients
  │
  └─► for each task in parallel (asyncio.Semaphore):
        │
        ▼
        LocalRunner(trial_id, task_config, task_dir, driver_factory,
                    object_store=LocalDiskObjectStore,
                    upstream_gateway_tokens=...,
                    sdk_clients=...)
          │
          ▼
        runner.run()
          │
          ├─► gateway = UpstreamDirectGatewayClient(...)
          ├─► agent_factory(task_config.agent.name, model, gateway)
          ├─► driver = driver_factory()
          ├─► verifier = PytestVerifier()
          │
          └─► Trial(ctx=TrialContext(
                  trial_id, team_id, task_config, task_dir,
                  driver, agent, verifier,
                  object_store=LocalDiskObjectStore,
                  local_trajectory_path=output_dir/<trial>/events.jsonl,
                  llm_calls_fetcher=None,
              )).run()
                │
                ▼
              TrialResult (state, rewards, ...)
```

Each trial gets its own `trial_id` (fresh UUID4). Per-trial outputs
land at `<output_dir>/<trial_id>/{events.jsonl,atif.json}` plus an
internal MinIO-shaped tree under `<output_dir>/_store/` that
LocalDiskObjectStore writes through to.

## The four dependency wirings

The whole CLI is "service mode minus the network calls" — it swaps
four dependencies:

| Dependency | Service mode | CLI mode |
|---|---|---|
| `ObjectStore` | `MinioObjectStore` (boto3 to MinIO) | `loom_cli.local_object_store.LocalDiskObjectStore` (host filesystem) |
| `LLMGatewayClient` | `loom.agent.HttpLLMGatewayClient` (HTTP to Loom Gateway) | `loom_cli.upstream_gateway.UpstreamDirectGatewayClient` (provider SDKs directly) |
| `state_patch_callback` | `loom_worker.control_plane_client.PatchState` (fenced HTTP to CP) | None — `LocalRunner` simply doesn't pass a state-patch callback into `TrialContext`; `Trial.run()` no-ops on PATCH when the callback is `None` |
| `llm_calls_fetcher` | `loom_worker.HttpControlPlaneClient.get_trial_llm_calls` | None — CLI doesn't centralize llm_calls |

Every other primitive (Driver, Verifier, Trial, TrajectoryWriter,
ATIF projection, capability checks, etc.) is the same code path as
service mode. Trajectory files are bit-identical (modulo UUIDs and
timestamps).

## `LocalDiskObjectStore`

`src/loom_cli/local_object_store.py`. Implements the `ObjectStore`
Protocol against the local filesystem:

```python
store = LocalDiskObjectStore(root=Path("/tmp/loom-runs"))

await store.put_object(bucket="traj", key="t1/events.jsonl", body=b"...")
# writes to /tmp/loom-runs/traj/t1/events.jsonl

await store.get_object(bucket="traj", key="t1/events.jsonl")
# reads back

upload = await store.create_multipart_upload(bucket="b", key="k")
await store.upload_part(upload, part_number=1, body=b"hello ")
await store.upload_part(upload, part_number=2, body=b"world")
uri = await store.complete_multipart_upload(upload)
# /tmp/loom-runs/b/k contains "hello world"
```

Multipart parts buffered in-memory then concatenated at complete time
— scope is CLI (single-process), no need for true streaming.
`presign_put` returns a `file://` URL that callers (curl, etc.)
treat as locally readable. Path traversal in `download_prefix` is
guarded by `_has_traversal` (reused from the production store).

## `UpstreamDirectGatewayClient`

`src/loom_cli/upstream_gateway.py`. Implements `LLMGatewayClient`
against provider SDKs directly:

```python
client = UpstreamDirectGatewayClient(
    anthropic_client=anthropic.AsyncAnthropic(api_key=...),
    openai_client=openai.AsyncOpenAI(api_key=...),
    google_client=genai,  # google.generativeai module
    tokens={"anthropic": "sk-ant-...", "openai": "sk-...", "google": "g-..."},
)
resp = await client.call(GatewayCallRequest(
    model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
    messages=[...],
    system_prompt="...",
))
# resp is the same GatewayCallResponse the service-mode Gateway returns
```

Dispatches on `request.model.provider`:
- `anthropic` → `messages.create(...)`
- `openai` → `chat.completions.create(...)`
- `google` → `generate_content_async(...)`
- `local` → OpenAI-compatible HTTP against
  `cfg.local_providers[server].base_url` (used for user-registered
  servers AND for the transient `_auto_vllm` provider the launcher
  installs — see [`local-llm.md`](local-llm.md))

Cost is computed locally from `~/.config/loom/rate-cards.toml`
(seeded from `src/loom_cli/data/default-rate-cards.toml` on first
run). User edits override defaults. Missing rate-card row → `KeyError`
with a hint to add one.

## Stateless agent factory

`src/loom_cli/agent_factory.py` mirrors
`loom_worker.main_loop._default_agent_factory` but:

- Takes no Control Plane client
- For `SubprocessAgent`, substitutes a `_NoopCPClient` (the launched
  CLI talks to the upstream provider via env-var API key in the
  sandbox; nothing reports back to a Loom Gateway)
- Routes `oracle` → `OracleAgent(task_dir, trial_id)` directly

## Concurrency

`asyncio.Semaphore(max(1, args.concurrency))` bounds parallel trials.
Each `_one(loaded)` coroutine takes a permit, builds a `LocalRunner`,
runs it, releases the permit. Trials are run via `asyncio.gather`.

Per-trial state isolation: each trial has its own `Driver` instance
(via `driver_factory()`), its own `TrajectoryWriter`, its own
working dir under `<output_dir>/<trial_id>/`. They share the
`UpstreamDirectGatewayClient` (provider SDK clients are themselves
safe for concurrent use).

## TB-2 report integration

`--tb2-report PATH` collects every `TrialResult` across all completed
trials and writes
`loom_benchmark_terminal_bench_2.report.to_tb2_report(trials)` to the
given path after `asyncio.gather` resolves. Lazy import — the TB-2
sibling package is optional unless the flag is actually used. Missing
package logs a warning, doesn't crash.

## Public server subcommands

`loom auth`, `loom providers`, and `loom eval` talk to a deployed Loom service
through the public API. They do not require direct access to Control Plane, LLM
Gateway, Postgres, MinIO, or worker routes.

- `loom auth login --server URL --token env:LOOM_API_TOKEN` stores the public
  server URL and scoped team API token in
  `$XDG_CONFIG_HOME/loom/config.toml`. The config directory is owner-only and
  the config file is written with mode `0600` on POSIX systems.
- `loom auth whoami` calls `GET /api/v1/auth/whoami` and prints server, team,
  role/scopes, and token prefix without printing raw token material.
- `loom providers ...` manages team provider connections through
  `/api/v1/provider-connections`; provider keys use `env:VAR`, `file:PATH`, or
  stdin sources rather than literal argv values.
- `loom eval batch ...`, `loom eval trial ...`, and `loom eval usage ...` use
  `/api/v1/batches`, `/api/v1/trials`, and `/api/v1/usage`.
- `loom eval trial download TRIAL_ID --kind atif|trajectory|artifact` downloads
  through service-proxied `/api/v1/trials/...` routes. The CLI does not print
  raw MinIO/S3 signed URLs; `trial show` prints download commands.

All server-talking commands share the same not-logged-in and 401/403 handling:
the message points back to Team access token setup and redacts bearer tokens,
provider keys, signed URLs, and internal service hostnames from server detail
text.

## `loom config` + `loom datasets`

See [../user-guide.md](../user-guide.md) for the command reference.
Internals:

- **Config** — `src/loom_cli/config.py`. XDG-aware loader/writer at
  `$XDG_CONFIG_HOME/loom/config.toml` (default
  `~/.config/loom/config.toml`). `LoomConfig` dataclass with upstream provider
  `tokens`, deployed `server_url`, scoped `auth_token`, and local provider
  entries. `loom config show` redacts token values to `<first2>***<last2>`.
- **Environment** — `src/loom_cli/__main__.py` loads the nearest
  project `.env` on CLI startup, walking upward from CWD and stopping
  at the first git root or home directory. Loading uses
  `override=False`, so exported shell variables take precedence over
  `.env`, and no missing-file message is emitted when no `.env` is
  found.
- **Datasets discovery** — three sources merged with precedence
  `builtin > remote > catalog`:
  - `src/loom_cli/builtin.py` — entry-points loader
  - `src/loom_cli/remote.py` — `GET LOOM_SERVER_URL/api/v1/benchmarks`
  - `src/loom_cli/catalog.py` — JSON fetch + 24h on-disk cache;
    in-tree default at `src/loom_cli/catalog_data/default-catalog.json`
- **Install** — `src/loom_cli/install.py`. Shells out to
  `[sys.executable, "-m", "pip", "install", spec]` after rejecting
  shell metacharacters in spec (policy-only — `subprocess.run` with a
  list doesn't shell-expand).
- **Lifecycle subcommands** (modular-D) — `import`, `publish`,
  `register`, `verify` live in `src/loom_cli/datasets_cmd.py` and
  delegate to `loom_benchmark_tool.{import_cmd, publish_cmd,
  register_cmd, verify_cmd}`'s `run_*` functions. `python -m
  loom_benchmark_tool` keeps working as a deprecation shim.

## Common pitfalls

- **License is metadata, not an execution gate**. CLI mode, service mode, and
  the Control Plane all allow tasks regardless of SPDX value. The legacy
  `team_quotas.license_allowlist` field is retained only as historical
  metadata.
- **No cross-trial coordination**. Each `loom run` invocation is
  independent. If you need rate limiting across many parallel
  invocations, run them inside one `loom run --concurrency N`
  (single-process Semaphore-bounded), not as many separate
  processes.
- **`--server-url` is best-effort POST**, never gates exit. If you
  need guaranteed delivery, use service mode (`POST /api/v1/trials`
  with idempotency_key).

## See also

- [overview.md](overview.md)
- [service-mode.md](service-mode.md) — same `Trial.run()`, different
  wiring
- `src/loom_cli/run_cmd.py` — the orchestrator
- `src/loom_cli/local_runner.py` — the per-trial wrapper
