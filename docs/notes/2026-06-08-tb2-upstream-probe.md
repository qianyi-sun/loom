# Terminal-Bench-2.0 Upstream Probe (2026-06-08)

## Dataset distribution

TB-2's official `registry.json` (https://github.com/laude-institute/terminal-bench/blob/main/registry.json)
lists `terminal-bench-core` v0.1.1 as the current pinnable leaderboard target:

- `github_url`: `https://github.com/laude-institute/terminal-bench`
- `branch`: `dataset/terminal-bench-core/v0.1.x`
- `commit_hash`: `91e10457b5410f16c44364da1a34cb6de8c488a5`
- `dataset_path`: `./tasks`
- `terminal_bench_version`: `>=0.2.4`
- 78 task ids in `task_id_subset` (full set used by the leaderboard).

There is NO HuggingFace dataset mirror. Distribution is git-only. We use
`loom_benchmarks.fetch.fetch_upstream` with `UpstreamSource(kind="git", ...)`.
That helper already handles full-SHA pins via `git init && git fetch <sha>`
(see `loom_benchmarks.fetch._looks_like_sha`, line 37–38).

## Pinned revision

`loom_benchmark_terminal_bench_2.upstream.UPSTREAM_REVISION = "91e10457b5410f16c44364da1a34cb6de8c488a5"`

This SHA targets terminal-bench-core v0.1.1, which is the dataset version
the Harbor leaderboard scores against as of 2026-06-08.

## Per-task on-disk layout

Each task directory under `tasks/<slug>/` contains:

| File / dir            | Purpose                                                                    |
| --------------------- | -------------------------------------------------------------------------- |
| `task.yaml`           | Task metadata + agent instruction (see schema below)                       |
| `Dockerfile`          | Per-task client image; usually `FROM ghcr.io/laude-institute/t-bench/...`  |
| `docker-compose.yaml` | Multi-container topology (always at least a `client` service)              |
| `solution.sh`         | Reference ("oracle") solution shell script                                 |
| `run-tests.sh`        | Entrypoint the harness invokes to grade                                    |
| `tests/`              | Pytest files + optional setup helpers (`setup-uv-pytest.sh` etc.)          |

## `task.yaml` schema (verified against `tasks/hello-world/task.yaml` at the pinned SHA)

```yaml
instruction: |-
  <free text shown to the agent>
author_email: <string>
difficulty: easy | medium | hard
category: <string>
tags: [<string>, ...]
parser_name: pytest                  # currently always "pytest" in v0.1.1
max_agent_timeout_sec: <float>       # default 360.0 if absent
max_test_timeout_sec: <float>        # default 60.0 if absent
test_scripts: [<filename>, ...]      # scripts run-tests.sh sources from $TEST_DIR
run_tests_in_same_shell: <bool>
env_name: <string | null>            # docker-compose service name override
```

Optional fields observed in other tasks: `image`, `tags`, `expert_time_min`,
`min_required_tools`. The adapter MUST tolerate unknown top-level keys (forward
compat).

## Judging contract

The harness boots `docker-compose.yaml`, drops the agent into the `client`
container, runs `run-tests.sh` inside it after the agent declares done, and
parses pytest output via `parser_name`. For `parser_name: pytest` the grading
rule is binary: pytest exit code 0 ⇒ resolved; non-zero ⇒ unresolved.

There is no per-test partial credit — `is_resolved` is the single score TB-2
reports. Loom maps this to `rewards = {"resolved": 1.0 or 0.0}` and a single
`CheckResult(name="resolved", passed=...)`.

## TB-2 report JSON (verified against `terminal_bench/harness_models.py`)

Two top-level model classes:

### `TrialResults`

```json
{
  "trial_name": "<task-slug>.<attempt>",
  "task_id": "<task-slug>",
  "task_description": "<instruction text>",
  "is_resolved": true,
  "failure_mode": "none" | "agent_timeout" | "test_timeout"
                | "parser_error" | "context_limit" | "unknown",
  "parser_results": {"<test_name>": "passed" | "failed"},
  "total_input_tokens": 0,
  "total_output_tokens": 0,
  "uuid": "<uuid4>",
  "recording_path": "<relative path to asciinema cast, optional>"
}
```

### `BenchmarkResults`

```json
{
  "results": [<TrialResults>, ...],
  "accuracy": <float>,
  "n_resolved": <int>,
  "n_unresolved": <int>,
  "resolved_ids": ["<task-slug>", ...],
  "unresolved_ids": ["<task-slug>", ...],
  "pass_at_k": {"1": <float>}
}
```

## Protocol gaps detected

The adapter writes the standard `task.toml`, `instruction.md`, `tests/`,
`environment/` layout (verified against `loom_benchmarks/base.py` docstring,
lines 1–13). All fields fit. No additions to `BenchmarkAdapter` Protocol
required. The Plan 25 spec's "compatibility shim" option (§3 deliverable 2)
is therefore a no-op — DO NOT extend `loom_benchmarks/base.py` in this plan.

If any TB-2 task at the pinned SHA requires a docker-compose multi-service
topology (which Loom's single-image model doesn't support), the adapter
emits a warning into `ConvertedTask.warnings` and falls back to using the
`client` service's Dockerfile directly. Task 7 implements this fallback.

## License

TB-2 is Apache-2.0 (LICENSE in repo root). Apache-2.0 is already in the
default `team_quotas.license_allowlist` (per Plan 13). No allowlist change
required.
