# User Guide — `loom` CLI

Everything a researcher needs to run LLMs against customizable tasks
from a laptop. Pluggable agents (use one of the 11 shipped harnesses
or write your own); pluggable task adapters (14 ship). One
`uv sync`, then `loom run`.

## Install

```bash
uv sync                                          # core deps; creates .venv/
uv pip install -e packages/loom-launcher \
               -e packages/loom-benchmarks \
               -e packages/loom-benchmark-terminal-bench-2
source .venv/bin/activate                        # so `loom` is on PATH
```

The last package is optional (TB-2 adapter). All others ship the core
14-adapter slate.

> Why `uv`? It's what the repo's tests + CI use; it creates the venv
> for you on first sync, sidestepping the PEP 668 "externally managed
> environment" error you get from `pip install` on modern
> Debian / Ubuntu. Install: <https://docs.astral.sh/uv/getting-started/installation/>.
> Prefer plain `pip`? Run `python3 -m venv .venv && source
> .venv/bin/activate` first, then `pip install -e . -e
> packages/loom-launcher -e packages/loom-benchmarks`.

## First run (smoke)

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

## Real evaluation runs

```bash
# Anthropic / OpenAI / Google API key
loom config set token.anthropic sk-ant-...
# Or via env: export ANTHROPIC_API_KEY=sk-ant-...

# Run a benchmark
loom run \
  --dataset swe-bench-verified \
  --agent claude-code \
  --model anthropic/claude-opus-4-7 \
  --backend docker \
  --concurrency 4 \
  --output-dir ./runs
```

Per-trial outputs land at `./runs/<trial-id>/{events.jsonl,atif.json}`.
With `--json`, each trial's result also prints as a JSON line on stdout
for piping (e.g. `loom run ... --json | jq '.state'`).

## Cloud sandboxes (Daytona)

```bash
export DAYTONA_API_KEY=...
loom run --backend daytona --dataset bfcl --agent oracle --output-dir ./runs
```

Daytona usage rows land in the `cloud_compute_records` table when the
run is connected to a Loom service (`--server-url`). Standalone
laptop runs skip persistence — the trajectories + ATIF still drop on
local disk.

## Local LLMs (vLLM, ollama, llama.cpp, lm-studio)

Three paths, ordered by what most users reach for first.

### Path A: inline — your server is already running

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

### Path B: managed — Loom starts vLLM for you

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

### Path C: persisted — same server, many runs

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

## Comparing multiple models

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

## `loom serve` reference

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

## `loom datasets` reference

```
loom datasets list                  # union: installed + catalog + remote
loom datasets list --installed      # only adapters in this venv
loom datasets list --available      # only catalog entries not installed
loom datasets list --remote         # only entries from LOOM_SERVER_URL/api/v1/benchmarks
loom datasets list --json           # machine-readable output

loom datasets show <slug>           # full detail for one adapter
loom datasets install <slug>        # pip-install a catalog entry
loom datasets refresh-catalog      # drop the 24h catalog HTTP cache
```

Discovery sources, in precedence order (builtin wins on slug
conflict):

1. **builtin** — `pip install`'d packages declaring
   `[project.entry-points."loom.benchmarks"]`
2. **remote** — `GET <LOOM_SERVER_URL>/api/v1/benchmarks` when a Loom
   service is reachable
3. **catalog** — `src/loom_cli/catalog_data/default-catalog.json`
   (override via `--catalog-url` or `LOOM_CATALOG_URL`)

## `loom config` reference

Config persists to `$XDG_CONFIG_HOME/loom/config.toml` (defaults to
`~/.config/loom/config.toml`). Tokens are redacted in `loom config show`.

```
loom config set token.<provider> <key>    # provider in {anthropic, openai, google}
loom config set server_url <url>          # optional Loom service URL
loom config show
```

Env vars override config: `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`,
`GOOGLE_API_KEY`, `LOOM_SERVER_URL`, `LOOM_API_TOKEN`.

## `loom run` reference

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
- any other name — resolved via `loom_launcher.get_adapter(name)`;
  ships 11 concrete adapters (claude-code, codex, openhands, aider,
  opencode, swe-agent, mini-swe-agent, openhands-sdk, gemini-cli,
  qwen-cli, kimi-cli)

## Rate cards

Rate cards live at `~/.config/loom/rate-cards.toml`, seeded on first
run from `src/loom_cli/data/default-rate-cards.toml`. Edit the file
to add or override entries (e.g. for a self-hosted model or a
provider Loom doesn't ship a default for). Cost is computed locally
from these rates plus the token counts returned by the provider SDK.

<a id="pasting-task-ids"></a>

## Pasting task ids

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
