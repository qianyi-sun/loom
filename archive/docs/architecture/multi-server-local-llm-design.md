# Multi-server local-LLM

> Archived on 2026-08-11. This page contains implementation sketches,
> deferred scope, and delivery-era wording. See the current
> [`multi-server-local-llm.md`](../../../docs/architecture/multi-server-local-llm.md).

> **Status**: shipped. Implementation in `src/loom_cli/serve_cmd.py`
> + `src/loom_cli/run_cmd.py:_run_sequential`/`_run_parallel`. This
> page documents the design decisions and the cross-component flow;
> for the user-facing surface see
> [`docs/user-guide.md#comparing-multiple-models`](../../../docs/user-guide.md#comparing-multiple-models).

## Goals

1. **Compare N models on the same dataset** in one `loom run`
   invocation, with output bucketed by model and ATIF metadata
   tagged for downstream aggregation.
2. **Pre-launch a server once, share across many runs** via a new
   `loom serve` command that auto-registers in user config and
   cleans up on shutdown.
3. **Independent parallel `loom run` invocations** (two terminals,
   two `hf:`/path/`--local-server` runs) work reliably, with the
   port-autopick TOCTOU hole closed.

## Non-goals (deliberately deferred)

- Daemon mode for `loom serve` (no PID files, no `loom serve stop`).
- Auto GPU-memory partitioning under `--parallel-models`.
- Per-task model selection (different tasks in one dataset call
  different models). Not in current user demand; would force a
  task-side opt-in we don't have today.
- Service-mode integration. `loom serve` is CLI-only; the
  service-mode LLM Gateway has its own dispatch path.

## Command surface

### `loom serve <spec> [--name NAME] [vLLM flags...]`

New subcommand. Foreground, blocks until Ctrl-C / SIGTERM.

```
loom serve hf:meta-llama/Llama-3.1-8B-Instruct --name llama8b
→ ✓ vLLM ready
  served_model_name=meta-llama/Llama-3.1-8B-Instruct
  base_url=http://localhost:8234/v1
→ registered as local/llama8b
→ keeping process alive; Ctrl-C to stop
```

Behavior:

- Accepts the same model-spec shapes as `loom run --model …`:
  `hf:<id>`, `/path/`, `~/path/`, `./path/`, `../path/`.
- Calls `launch_vllm(...)` from `vllm_runner.py`.
- Writes config:
  ```toml
  [local_providers.<name>]
  base_url = "http://localhost:<port>/v1"
  served_model_name = "<canonical>"
  ```
  The `served_model_name` field is NEW (config-schema change — see
  below).
- `--name` defaults to a sanitized slug of the model id
  (`meta-llama/Llama-3.1-8B-Instruct` → `llama-3-1-8b-instruct`).
- Forwards all the same vLLM tuning flags as `loom run`
  (`--vllm-port`, `--vllm-host`, `--tensor-parallel-size`,
  `--gpu-memory-utilization`, `--max-model-len`, `--enforce-eager`).
- On shutdown (Ctrl-C, SIGTERM, vLLM crash):
  - Stop the vLLM subprocess via the existing `stop_one(proc)`.
  - **Remove** the `[local_providers.<name>]` entry from config so a stale URL
    doesn't linger.

### `loom run --model M1 --model M2 [--parallel-models] …`

`--model` becomes `action="append"`. Single-model behavior unchanged.

Multi-model (N ≥ 2) semantics:

- **Sequential by default** — Loom launches model A, runs all tasks
  in the dataset against it, stops A, launches B, repeats. Peak GPU
  memory = `max(A, B)`, not `A + B`. The right default for the
  common single-GPU laptop case.
- **`--parallel-models`** — opt-in for multi-GPU users. All vLLMs
  launched upfront, all tasks gather across all models concurrently.
  User assumes responsibility for GPU memory + (optionally)
  `CUDA_VISIBLE_DEVICES`.
- **Output**: with N>1, trials land in
  `<output-dir>/<model-slug>/<trial-id>/`. With N=1, layout is
  unchanged (`<output-dir>/<trial-id>/`).
- **Output-dir identity**: trials land under `<output-dir>/<model-slug>/<trial-id>/` 
  so downstream tools can group by directory name. ATIF JSON itself does not 
  carry a `model_slug` field today — filed as a follow-up if/when downstream 
  consumers need it as a structured tag.

Mutual exclusions:

- `--model hf:X --model /path/` (two managed specs of different
  kinds) — OK, each launches its own vLLM in sequence.
- `--local-server URL --model X --model Y` — OK, both run against
  the same server (URL doesn't change between models).
- `--local-server URL --model hf:X` — REJECTED, matching existing
  single-server behavior.
- `--parallel-models` with a single `--model` — warning + ignore.

## Internals

### `src/loom_cli/vllm_runner.py` (extend, don't rewrite)

Two small additions to the existing module:

```python
def stop_one(proc: subprocess.Popen[bytes]) -> None:
    """Stop one specific process. Like `stop_all` but for the
    sequential-load loop where we want to release one model before
    launching the next."""
    if proc in _LIVE_PROCESSES:
        _LIVE_PROCESSES.remove(proc)
    _stop_process(proc)

def model_slug(spec: str) -> str:
    """Derive a stable, filesystem-safe slug from any model spec
    (hf:/, /path/, or a registered name). Used for output bucketing
    and `loom serve --name` defaults."""
```

`launch_vllm` already supports N concurrent launches via the
`_LIVE_PROCESSES` list. No change to its body.

### `src/loom_cli/serve_cmd.py`

```python
async def serve(args: argparse.Namespace) -> int:
    spec = _parse_serve_model(args.model_spec)  # same shapes as run
    launch_spec = VLLMLaunchSpec(model=spec.name, ...)
    info = launch_vllm(launch_spec)

    name = args.name or model_slug(args.model_spec)
    _write_local_provider_config(name, info.base_url, info.served_model_name)
    try:
        sys.stderr.write(f"→ registered as local/{name}\n")
        sys.stderr.write("→ keeping process alive; Ctrl-C to stop\n")
        await _block_until_shutdown()  # asyncio.Event + signal handler
    finally:
        _delete_local_provider_config(name)
        stop_one(info.proc)
    return 0
```

Reuses the existing signal-handler model from `vllm_runner.py`
(atexit + SIGTERM; SIGINT propagates via `KeyboardInterrupt`).

### `src/loom_cli/run_cmd.py` (extend)

`--model` argparse: `action="append"`, type unchanged. After parse:

```python
model_specs: list[str] = args.model or []
models: list[ModelSpec] = [_parse_model(s) for s in model_specs]

if len(models) <= 1:
    return await _run_async_single(args, models[0] if models else None)
elif args.parallel_models:
    return await _run_async_parallel(args, models)
else:
    return await _run_async_sequential(args, models)
```

`_run_async_single` is today's `_run_async` body, factored out.

`_run_async_sequential`:
```python
exit_codes: list[int] = []
for spec in models:
    sub_output = output_dir / model_slug(spec_to_str(spec))
    info = launch_vllm(...) if spec.provider in ("hf", "file") else None
    register transient provider
    code = await _run_async_single(args, spec, output_root=sub_output)
    if info is not None and not args.keep_alive:
        stop_one(info.proc)
        unregister transient provider
    exit_codes.append(code)
return max(exit_codes)
```

`_run_async_parallel`:
```python
infos = [launch_vllm(spec) for spec in models if needs_launch(spec)]
# Build per-model task lists, gather across all, write outputs into
# <output-dir>/<slug>/<trial-id>/ via per-model LocalDiskObjectStore.
# Stop all at the end.
```

### `src/loom_cli/config.py`

`LocalProvider` carries an optional `served_model_name` field:

```python
@dataclass
class LocalProvider:
    base_url: str
    api_key: str | None = None
    served_model_name: str | None = None  # added for loom serve
```

Used by `loom serve` to persist the canonical name. `loom run` reads
it (where present) so the user can say `--model local/llama8b/...`
and the trailing `<served>` is auto-filled from config when omitted.

Backward-compatible: existing config blocks without
`served_model_name` continue to work.

### Port autopick TOCTOU fix

`_find_free_port` currently does a pre-bind probe; another process
can grab the port between probe and `vllm serve` launch. Fix:

- Drop the explicit probe.
- Launch vLLM with the candidate port; if it exits within 5s with a
  bind-failure stderr signature, retry the next port. Limit to 1000
  retries (same envelope).

This eliminates the probe race AND keeps the rest of `launch_vllm`'s
existing fail-fast on subprocess death.

## Data flow examples

### Compare two models, sequential (default)

```
loom run --dataset humaneval \
         --model hf:meta-llama/Llama-3.1-8B-Instruct \
         --model hf:meta-llama/Llama-3.1-70B-Instruct \
         --output-dir runs/compare/

# Loop iteration 1: 8B
launch_vllm(hf:meta-llama/Llama-3.1-8B-Instruct)
register cfg.local_providers["_auto_vllm"] = LocalProvider(...)
model → local/_auto_vllm/Llama-3.1-8B-Instruct
trial gather → outputs to runs/compare/llama-3-1-8b-instruct/<trial>/
stop_one(8B proc); unregister _auto_vllm

# Loop iteration 2: 70B
launch_vllm(hf:meta-llama/Llama-3.1-70B-Instruct)
register cfg.local_providers["_auto_vllm"] = LocalProvider(...)
model → local/_auto_vllm/Llama-3.1-70B-Instruct
trial gather → outputs to runs/compare/llama-3-1-70b-instruct/<trial>/
stop_one(70B proc); unregister _auto_vllm
```

Peak GPU = `max(8B_mem, 70B_mem)`. Downstream tools can identify which 
model produced each trial by inspecting the output directory name.

### Serve + share

```
# Terminal 1:
loom serve hf:meta-llama/Llama-3.1-8B-Instruct --name llama8b
# Writes [local_providers.llama8b] to config, blocks.

# Terminal 2:
loom run --model local/llama8b/meta-llama/Llama-3.1-8B-Instruct \
         --dataset humaneval

# Loom reads [local_providers.llama8b] from config, dispatches via _call_local.
# No new vLLM launch. Terminal 1's process owns the GPU.

# Terminal 1: Ctrl-C
# → stop_one() + delete [local_providers.llama8b] from config
```

### Mixed managed + persisted

```
# Compare a HuggingFace baseline against your fine-tune that's
# already running:
loom run --dataset humaneval \
         --model local/my-tune/meta-llama/Llama-3.1-8B-Instruct \
         --model hf:meta-llama/Llama-3.1-8B-Instruct

# Iteration 1: local/my-tune (no launch; uses existing server)
# Iteration 2: hf: spec (launches transient vLLM; tears down)
```

## Error handling

| Failure | Behavior |
|---|---|
| `loom serve` model not parseable | exit 2, no config touched |
| `loom serve` vLLM crashes during startup | exit 2, no config written (registration is post-health-check) |
| `loom serve` Ctrl-C during health probe | vLLM stopped, no config written |
| `loom serve` Ctrl-C after registration | vLLM stopped, config entry deleted |
| `loom serve` SIGKILL | atexit doesn't fire; config entry left over. Documented; user can `loom config unset local_providers.<name>.base_url` if they hit it. |
| `loom run` sequential: model A fails | continue to B; final exit = max(A_code, B_code) |
| `loom run` sequential: A succeeds, B fails to launch | A's outputs preserved; B's slot empty; exit 2 |
| `loom run` parallel: one vLLM crashes mid-gather | trials targeting that model fail; others continue; `stop_all()` at end tears the rest down |
| `--parallel-models` with one `--model` | warn on stderr, treat as single-model |

## Testing strategy

### New test files

**`tests/loom_cli/test_serve_cmd.py`** (~10 tests):
- Smoke: `loom serve hf:X` calls `launch_vllm` (mocked), writes
  `[local_providers.<name>]` to config, blocks on `signal.pause` (mocked).
- `--name` default — slug derivation.
- `--name` explicit.
- Ctrl-C path: config entry removed.
- Launch failure: config not written.
- Path spec: `loom serve /tmp/weights/` works the same.
- Argparse rejection of bad spec.

**`tests/loom_cli/test_multi_model_run.py`** (~12 tests):
- Single `--model` (regression): output layout unchanged.
- Two `--model`, sequential: vLLM launched twice, both stopped,
  output bucketed by slug.
- Two `--model`, `--parallel-models`: both launched upfront, both
  stopped at end, output bucketed.
- Sequential: A fails, B still runs.
- Parallel: A's vLLM crashes mid-gather, B's trials still complete.
- Mutual exclusion: `--local-server` + multiple `--model` runs both
  against the same URL.
- `--parallel-models` with one `--model` warns + treats as single.
- ATIF `model_slug` field populated correctly.

**Extend `tests/loom_cli/test_vllm_runner.py`** (~4 tests):
- `stop_one(proc)`: removes from `_LIVE_PROCESSES` + stops.
- `stop_one` no-op on already-stopped.
- `model_slug`: HF id, path, registered name shapes.
- TOCTOU fix: simulate bind failure → retry next port.

### Coverage targets

- All new lines covered ≥ 90% (matches repo gate).
- All branches in `_run_async_sequential` / `_run_async_parallel`
  exercised, including failure paths (A fails / B fails / both fail).

## Migration / compat

- **No CLI-breaking changes.** Existing `loom run --model X` keeps
  exact behavior; `--model` becoming `append` is additive.
- **Config schema bump** (`LocalProvider.served_model_name`) is
  additive; existing config files load unchanged.
- **Output layout**: only changes with N>1 models. Single-model
  layout (`<output-dir>/<trial-id>/`) preserved.
- ATIF: gains one field; consumers that don't read `model_slug`
  ignore it.

## What this is NOT

- Not a model-router. Tasks don't pick their own model; the
  `--model` list is a global product of (tasks × models).
- Not a daemon. `loom serve` blocks; closing the terminal closes
  the server. Filed: daemon mode as a follow-up if real users ask.
- Not service-mode. Workers in service-mode dispatch through the
  LLM Gateway; this design is CLI-only.

## Edge cases resolved at implementation time

Two micro-decisions resolved during design, honored in the
implementation:

1. **`model_slug` collision** — two models that produce the same
   slug (e.g. two different paths both ending in `weights/`) currently 
   write to the same output bucket; the second iteration's outputs overwrite 
   the first's. Detected by inspection but not blocked. If real users hit this, 
   an index-suffix (`weights-1`, `weights-2`) is the planned fix.
2. **`loom serve --name` collision** — if `local/<X>` is already 
   registered in config, `loom serve` rejects with exit 2 and a clear message. 
   Implemented in `serve_cmd.py`.

## See also

- [`local-llm.md`](../../../docs/architecture/local-llm.md) — single-server launcher this
  design builds on
- [`cli-mode.md`](../../../docs/architecture/cli-mode.md) — how `loom run` reuses
  `Trial.run()` statelessly
- [`cost-and-rate-cards.md`](../../../docs/architecture/cost-and-rate-cards.md) —
  `local:<server>` provider naming for cost attribution
