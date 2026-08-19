# Harbor-embedded Terminus-2 runtime

Loom's `terminus-2` agent runs pinned Harbor `Terminus2` **in-process inside
the worker image**, not as a `loom-launcher` subprocess adapter. The worker
routes `agent.name == "terminus-2"` to `LoomTerminus2Runtime`
(`src/loom/agent/terminus2/runtime.py`).

## Pin and worker image

| Constant | Value |
|---|---|
| Harbor compat SHA | `527d50deb63a5d279e8c20593c18a2cbc7f61f9e` |
| Harbor runtime version | `0.18.0` |
| Loom bridge revision | `1.0` |

Harbor is installed only in `deploy/Dockerfile.worker` (not the main Loom
package). Gate 1 evidence:

- `deploy/worker-image.lock` — frozen worker transitive deps after `pip check`
- `deploy/worker-image.wheels.json` — SHA256 for critical wheels
- `scripts/ops/update_worker_image_lock.sh` — regenerate lock/hashes after
  worker dependency changes

Worker provenance is emitted at trial start via `terminus2_runtime_provenance`
events and `loom.agent.terminus2.worker_provenance`.

This runtime identity is independent of benchmark identity. A TB2.1 trial must
record both the Harbor Hub rev-6 package/profile provenance and the Terminus-2
worker/bridge provenance; matching one does not attest the other. The benchmark
worker also verifies the materialized task checksum before agent startup, keeps
private tests/solutions/verifier files out of this agent runtime, and creates a
fresh verifier-only driver after Terminus-2 exits. The public agent workspace
is handed to that driver as a validated archive snapshot so directory layout,
regular-file modes, safe workspace-relative symlinks, and hardlinks remain
score-equivalent. Traversal, absolute/private link targets, devices, FIFOs, and
sockets fail closed. Cancellation waits for verifier-driver teardown before it
propagates, preventing sandbox cleanup from racing network/sidecar teardown.

## Runtime shape

```
Trial.run()
  └─ LoomTerminus2Runtime
       ├─ LoomHarborEnvironment  (Harbor BaseEnvironment → Loom Driver exec/tmux)
       ├─ harbor.agents.terminus_2.Terminus2  (pinned in-process)
       ├─ LLM via Gateway step JWT (openai/{model_id})
       └─ HarborCheckpointBridge
            ├─ reads Harbor trajectory.json + recording.cast under .loom/agent/
            ├─ GatewayCallLedger joins episodes to CP llm_calls
            │    (correlated client_call_id / episode+ordinal; legacy token join)
            └─ appends typed terminus2_* events to Loom trajectory JSONL
```

Harbor artifacts copied into the trial sandbox:

- `.loom/agent/trajectory.json`
- `.loom/agent/recording.cast`

Step runner always includes `.loom/agent/**` in artifact patterns for
`terminus-2` trials.

## Step JWT lifetime

Terminus-2 snapshots one step-scoped JWT when the agent phase starts and does
not rotate it during that phase. Before invoking the runtime, the step runner
therefore sets the JWT TTL to the same effective timeout used by agent
execution, rounded up, plus a 300-second cleanup and clock-skew buffer. The
effective timeout includes task defaults, per-step overrides, the trial
`override_agent_timeout_sec`, and `agent_timeout_multiplier`.

The Control Plane accepts requested step-token TTLs up to 30,000 seconds. A
configuration whose effective timeout plus buffer exceeds that limit fails
token issuance rather than silently minting a token that expires before the
agent deadline.

## Typed trajectory events

The Harbor-embedded runtime emits these events for operators and debuggers:

| Event kind | Purpose |
|---|---|
| `terminus2_runtime_provenance` | Harbor pin, bridge revision, template hashes |
| `terminus2_turn` | Model turn boundary |
| `terminus2_command` | Command issued to the terminal |
| `terminus2_terminal_observation` | Bounded terminal output observation |
| `terminus2_parse_retry` | Parser retry after malformed model output |
| `terminus2_context_boundary` | Context/window boundary marker |
| `terminus2_artifact_ref` | Pointer to Harbor artifact (e.g. `recording.cast`) |
| `terminus2_model_switch_planned` | Durable K1/K2 cuts from the plan (before Harbor runs) |
| `terminus2_model_switch` | Applied student↔teacher cut |
| `terminus2_llm_call_started` / `_completed` / `_failed` | Per-call correlation around the router |
| `terminus2_episode_checkpoint` | Episode snapshot used on worker retry |
| `terminus2_recovery_failed` | Retry refused rather than merging two Harbor runs |

Gateway join failures (missing `cp_client`, ambiguous token match, command
without observation on a full episode) fail closed via `CheckpointBridgeError`
→ trial `agent` phase error.

## Service catalog contract

`GET /api/v1/agents` lists `terminus-2` under builtins (`kind="builtin"`):

- `execution`: `builtin-terminus2-harbor`
- `capture`: `typed_events+harbor_artifacts`
- `needs_model`: true
- Model template: `openai/{model_id}` through the Gateway Chat facade

`terminus-2` does **not** use per-trial `AgentAdapter.install_script` layering.
It requires the worker image built from the current `Dockerfile.worker`.

## ARM64 / GB10

Task bundles that `FROM mictern2/terminus2-full:latest` trigger
`_ensure_terminus_2_arm64_base_if_needed` on ARM64 workers (GB10 pool) before
the task image build. See [service-mode.md](service-mode.md) § Runtime-fallback
base image registry.

## Staging acceptance (Gate 3)

Minimal smoke after a staging rollout that includes the Harbor-embedded worker
image and gateway-ledger bridge:

```bash
loom auth login --server https://yylx.world/dev ...
# User batch creation does not select a required worker pool. Operators collect
# pool-specific coverage with `loom admin batches submit-on-behalf` separately.
loom eval batch create \
  --name-suffix terminus2-gate3-smoke \
  --agent terminus-2 \
  --provider <team-provider> \
  --model <preflight-passing-model> \
  --benchmark loom-smoke \
  --task-filter '{"task_ids":["loom-smoke/gb10-oracle-hello-world"]}' \
  --n-per-task 1
```

Evidence checklist:

- `llm_evidence_status: calls_observed` on the batch/trial
- `terminus2_runtime_provenance` with `harbor_compat_sha` matching the pin
- for a TB2.1 canary, physical profile `terminal-bench-2@tb2.1-r6`, Hub
  metadata/package digest, Loom bundle checksum, verifier identity, and runtime
  provenance are all present as separate fields
- At least one `terminus2_command` + `terminus2_terminal_observation` pair
- LLM rows joined to real `gateway_request_id` / `llm_calls.id` (not synthetic
  `harbor-step-N` ids)
- Harbor artifacts present: `.loom/agent/trajectory.json`, `.loom/agent/recording.cast`
- verifier reward is finite and numeric; `0` is valid, while a missing reward
  is a platform/verifier failure

## Export

| Mode | Status | Source |
|---|---|---|
| `raw-harbor-tb2-v1` | Shipped | Provider logs + reward joins |
| `raw-harbor-tb2-v2` | Shipped; requires typed source events | Checkpoint bridge / typed `terminus2_*` events via `Terminus2TrajectoryMapper` |

`raw-harbor-tb2-v2` is wired in the delivery API and CLI. Use it when the
trial trajectory contains typed `terminus2_*` events and hash-verified native
Harbor artifacts. Its exporter fails closed when that contract is incomplete;
use `raw-harbor-tb2-v1` for provider-log-based source trials.

## Mid-trajectory student/teacher/student switch (#1380)

When `trial_config.multi_model.enabled` is true on a **terminus-2** trial,
Loom constructs a second Harbor `LiteLLM` (teacher) with the same gateway JWT
and replaces `agent._llm` with a Loom `BaseLLM` router **after**
`Terminus2(...)` and **before** `agent.run(...)`. Harbor is not forked and
`_model_name` is not mutated.

v1 schedule (episode-based; later an off-track detector can set K1/K2):

```
episodes 1 .. K1-1  → student  (trial_config.agent_model)
episodes K1 .. K2-1 → teacher  (multi_model.secondary_model)
episodes K2 .. N    → student
```

`K2 = K1 + teacher_episodes` (default teacher_episodes = 2). At least two
role cuts if the run reaches K2.

Second policy `beta_mixture` (`--multi-model-beta`): each Harbor episode
hashes `seed:trial_id:episode` into `[0, 1)` and runs the **teacher if
`draw < beta`**, else student. Episode grain matches Path A (parse retries
stay on the same actor). This is **not** DAgger: the teacher is not queried
for a label on student-driven episodes. `beta = 1` is all teacher, including
episode 1. Emits `terminus2_model_mix_planned` instead of the two K1/K2
planned cuts. Applied `terminus2_model_switch` still fires when consecutive
episodes change role.

Constraints:

- Same BYO `provider_connection_id` for both models.
- Silent switch (no handoff prompt). On each role change the router drops
  `previous_response_id` and sends normalized chat history to the incoming model.
- `switch_episode` (K1) is sampled at batch/trial accept when omitted and
  stored on an immutable `model_switch_plans` row (not only on `trial_config`).
  `return_switch_episode` (K2) is always `K1 + teacher_episodes`.
- Router keys off Harbor `_n_episodes` so parse retries stay on the same role.
- Emits a `terminus2_model_switch` event per applied cut.
- Workers that do not advertise `terminus2_model_switch` cannot claim these trials.

### Worker retry: fail closed (no merged Harbor rerun)

Loom's scheduler already retries a trial by letting a new worker claim it and
run the agent step again. For a **single-model** terminus-2 trial that is a
full restart: Harbor always boots from episode 1, and the new attempt is a
new run of the task.

For **multi-model** terminus-2 the durable object is one Terminus session
(`terminus_agent_executions`) plus the immutable K1/K2 plan. After each
completed episode the worker writes `episode_checkpoints` (episode, active
role, last call ordinal, seq, checksum).

On the next claim the worker **reclaims** that execution (new
`terminus_agent_run_attempt`, same execution and plan):

| Checkpoint | Harbor can resume mid-session? | What we do |
|---|---|---|
| None (never finished episode 1) | N/A | Start Harbor from episode 1 (`fresh`). Same as a normal first run. |
| Present and checksum fails | No | `recovery_failed`. Do not run. |
| Present and checksum verifies | No (pinned Harbor has no Loom-owned resume) | **Also `recovery_failed`.** Do not start Harbor from episode 1. |

Pinned Harbor `Terminus2.run()` always starts at episode 1. Restarting it
while the first attempt's events still sit on the trial would **append a
second student beginning onto a trajectory that already entered teacher**.
That merge is forbidden.

v1 therefore **fails the trial attempt** (`terminus2_recovery_failed`,
`AgentError` whose message starts with `terminus-2 recovery_failed`) instead
of stitching. A later exact replay is a **new trial** that **inherits** the
plan (same K1/K2), not a continuation of the crashed session.

This is intentional, not a missing Harbor hook. True mid-session resume
(same tmux, same episode, same role) would need a Harbor resume path,
which #1380 left out of scope.

Related: `src/loom_control_plane/terminus_recovery.py`,
`src/loom/agent/terminus2/runtime.py`.

## Related code

| Path | Role |
|---|---|
| `src/loom/agent/terminus2/runtime.py` | `LoomTerminus2Runtime` |
| `src/loom/agent/terminus2/model_switch.py` | Two LiteLLM + `BaseLLM` role router (#1380) |
| `src/loom/agent/terminus2/harbor_environment.py` | Driver bridge |
| `src/loom/agent/terminus2/checkpoint_bridge.py` | Harbor JSON → typed events |
| `src/loom/agent/terminus2/gateway_ledger.py` | CP `llm_calls` join |
| `src/loom/agent/terminus2/mapper.py` | TB2 v2 projection |
| `src/loom_service/delivery_export_tb2_v2.py` | TB2 v2 export framework |
| `src/loom_service/multi_model.py` | Batch validation / materialization |
| `src/loom_control_plane/terminus_recovery.py` | Execution reclaim; fail closed if a checkpoint exists |
| `packages/loom-launcher/loom_launcher/terminus_2_runner.py` | Import-stability stub; exits with current runtime guidance |
| `tests/conformance/terminus2/README.md` | Local conformance notes |
| `tests/unit/test_multi_model_switch.py` | Role router + config unit tests |
