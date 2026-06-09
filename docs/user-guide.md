# User Guide — `loom` CLI

Everything a researcher needs to run LLMs against customizable tasks
from a laptop. Pluggable agents (use one of the 11 shipped harnesses
or write your own); pluggable task adapters (14 ship). One
`pip install`, then `loom run`.

## Install

```bash
pip install -e . \
            -e packages/loom-launcher \
            -e packages/loom-benchmarks \
            -e packages/loom-benchmark-terminal-bench-2
```

The last package is optional (TB-2 adapter). All others ship the core
14-adapter slate.

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

## `loom datasets` reference

```
loom datasets list                  # union: installed + registry + remote
loom datasets list --installed      # only adapters in this venv
loom datasets list --available      # only registry entries not installed
loom datasets list --remote         # only entries from LOOM_SERVER_URL/api/v1/benchmarks
loom datasets list --json           # machine-readable output

loom datasets show <slug>           # full detail for one adapter
loom datasets install <slug>        # pip-install a registry entry
loom datasets refresh-registry      # drop the 24h registry HTTP cache
```

Discovery sources, in precedence order (builtin wins on slug
conflict):

1. **builtin** — `pip install`'d packages declaring
   `[project.entry-points."loom.benchmarks"]`
2. **remote** — `GET <LOOM_SERVER_URL>/api/v1/benchmarks` when a Loom
   service is reachable
3. **registry** — `src/loom_cli/registry_data/default-registry.json`
   (override via `--registry-url` or `LOOM_REGISTRY_URL`)

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
