# Trajectories & ATIF

Loom emits **two** trajectory artifacts per trial:

1. **`events.jsonl`** — event-sourced append-only JSONL written
   incrementally as the trial runs. Lives at MinIO key
   `<team_id>/<trial_id>/events.jsonl` (service mode) or
   `<output_dir>/<trial_id>/events.jsonl` (CLI mode).
2. **`atif.json`** — ATIF v1.7 projection computed from the
   trajectory at finalize. Lives at MinIO key
   `<team_id>/<trial_id>/atif.json` (or alongside events.jsonl on
   disk in CLI mode).

The two coexist by design: trajectories are write-optimized (append,
no merging) and lossless; ATIF is read-optimized (one document per
trial) and projection-only.

## Event schema

Every event is one JSON object per line. Minimum shape:

```json
{
  "emitted_at": "<ISO8601 UTC>",
  "trial_id": "<uuid>",
  "step_id": "<step_name | __trial__>",
  "seq": <int>,
  "kind": "<event_kind>",
  ...
}
```

Per-trial events have a monotonic `(step_id, seq)` pair. Cross-step
ordering is NOT guaranteed — concurrent steps in multi-step trials can
emit in any interleaving.

### Event kinds (snake_case)

| Kind | Emitted by | Carries |
|---|---|---|
| `trial_start` | Trial.run() | task_id, agent_name, agent_mode |
| `trial_end` | Trial.run() | (terminal marker) |
| `trial_error` | Trial.run() | error_type, message, traceback |
| `trial_cancelled` | Trial.run() | cancellation_requested_at, observed_at |
| `step_start` | step_runner | step_name, instruction_excerpt |
| `step_end` | step_runner | summary, error (if failed) |
| `env_start` / `env_ready` / `env_stop` | Driver | sandbox lifecycle |
| `env_exec` | Driver | command, exit_code, duration_sec |
| `file_upload` / `file_download` | Driver | path, size, sha256 |
| `llm_call` | Gateway / LiteLLMAgent | model, dialect, input_tokens, output_tokens, cost_usd, rate_card_hash, latency_ms |
| `tool_use` | Agent adapter | name, args, result_excerpt |
| `agent_thought` | Agent adapter | reasoning text |
| `verifier_start` / `verifier_end` / `verifier_check` | Verifier | check_name, passed, message |
| `network_policy_change` | Driver | from, to |
| `worker_lost_claim` | Worker | reason (heartbeat timeout, fence-bump) |

Full schema (and the per-event payload fields not shown here):
`src/loom/models/trajectory.py`. New events can be added by appending
kinds — the writer + reader are forward-compatible (unknown kinds
pass through unchanged).

## Writer (`src/loom/trajectory/writer.py`)

```python
writer = TrajectoryWriter(
    trial_id=trial_id,
    local_path=Path("/tmp/events.jsonl"),
    object_store=store,
    bucket="trajectories",
    key=f"{team_id}/{trial_id}/events.jsonl",
)
await writer.write(event)
await writer.finalize()
```

Behavior:

- **Local-first durability** — every event is appended to the local
  file synchronously before any network call. A crash mid-trial
  doesn't lose events.
- **Background flush** — flush triggers at any of: 1 MB pending bytes,
  100 pending events, or 10 seconds since last flush. The flush
  uploads pending bytes as the next multipart part.
- **5 MiB AWS-S3 mid-trial part floor** — multipart parts must be ≥
  5 MiB EXCEPT the last one. The writer buffers smaller parts until
  the threshold is met. Test code passes `min_part_bytes=0` to keep
  small-event tests deterministic.
- **`finalize()`** uploads remaining bytes as the final part and
  closes the multipart upload.

Orphan handling: if a worker dies mid-trial, the local file lives but
nothing is in MinIO. The Worker's `OrphanCleanup` sweep (run at
startup) walks `<tmp>/loom-trial-*` dirs and uploads any
not-finalized trajectories. Failed orphan uploads log + skip; the
local file is preserved.

## Reader (`src/loom/trajectory/reader.py`)

Streaming line-decoded reader. Used by:

- Service `/api/v1/trials/{id}/trajectory` (with cursor pagination)
- ATIF projector
- The SPA Trajectory viewer (load-more-on-scroll)

```python
async for event in reader.stream(bucket="trajectories", key="..."):
    ...
```

Reads via `httpx.AsyncClient.stream("GET", presigned_url)` —
no full-file buffering.

## ATIF v1.7 projection (`src/loom/trajectory/atif.py`)

Pure function: `project_to_atif(events: list[TrajectoryEvent]) ->
dict`. Reads the in-memory event list at finalize, walks once, emits
a single document.

ATIF shape (rough; canonical definition is in the projection code):

```json
{
  "schema_version": "1.7",
  "trial_id": "<uuid>",
  "task_id": "humaneval/HumanEval/0",
  "agent": {"name": "claude-code", "version": "...", "mode": "out-of-box", "model": {...}},
  "started_at": "...",
  "finished_at": "...",
  "state": "succeeded",
  "rewards": {"resolved": 1.0},
  "steps": [
    {
      "step_name": "main",
      "instruction": "...",
      "llm_calls": [
        {"model": "claude-opus-4-7", "input_tokens": ..., "output_tokens": ..., "cost_usd": ...},
        ...
      ],
      "tool_uses": [...],
      "agent_messages": [...],
      "verifier_result": {
        "rewards": {"resolved": 1.0},
        "checks": [{"name": "tb2_run_tests", "passed": true, "message": "exit=0"}]
      },
      "duration_sec": 42.5
    }
  ]
}
```

Service mode enriches the ATIF with `llm_calls` fetched from the
Gateway (via `llm_calls_fetcher` callback) before projection when the
agent did not already write gateway-backed `llm_call` trajectory
events. Synthetic `llm_call` events are billing summaries; full
request/response bodies live in the Gateway database. CLI mode skips
this step (no Gateway).

ATIF is a pure projection — re-running it on the same events.jsonl
produces a byte-identical result modulo `emitted_at` timestamps.

## Service Output Projection

In service mode, a successful Worker finalize also updates the trial row
through the fenced Control Plane `PATCH /trials/{id}/trajectory_index`
path:

- `trials.result` stores the serialized `TrialResult`, including
  `aggregate_reward` for list and batch rollups.
- `trials.trajectory_index` stores `trajectory_uri`, `atif_uri`,
  `atif_schema_version`, and an `artifacts` array containing each
  uploaded artifact's `step_name`, bucket, object key, and size.

`GET /api/v1/trials/{id}` reads this projection to return ready flags
and authenticated service download URLs for ATIF, trajectory, and artifacts.
`loom_service` streams those object bodies from the internal MinIO/S3 endpoint
to the caller, so browser and laptop clients do not need direct object-store
network access. A succeeded trial should not require clients to scan object
storage, guess artifact keys, or open a separate MinIO tunnel.

## Trajectory ↔ ATIF in code

```python
# Worker / CLI:
writer = TrajectoryWriter(...)
async with writer:
    async for event in trial.run():
        await writer.write(event)
    await writer.finalize()

# Finalize step:
events = list(reader.stream(bucket, events_key))
atif = project_to_atif(events, llm_calls=fetched_llm_calls)
await object_store.put_object(bucket, atif_key, json.dumps(atif).encode())
```

## Common pitfalls

- **`bind_trial_context` does not nest.** The contextvars helper for
  log correlation can only have ONE binding active. Bind at the
  outermost scope (e.g. inside `Trial.run()`'s body, not inside
  per-step helpers); nesting silently drops the inner binding when
  the outer scope exits.
- **Don't write to the trajectory from inside `project_to_atif`.**
  Projection is pure; it reads events, returns a dict.
- **5 MiB part floor.** If you set `min_part_bytes=0` in production,
  multipart upload fails after the first <5 MiB flush. Only test code
  should pass 0.

## See also

- [overview.md](overview.md)
- `src/loom/trajectory/writer.py` — the writer
- `src/loom/trajectory/reader.py` — the reader
- `src/loom/trajectory/atif.py` — the projection
- `src/loom/models/trajectory.py` — event dataclasses
