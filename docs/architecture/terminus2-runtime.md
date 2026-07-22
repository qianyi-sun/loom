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
            ├─ GatewayCallLedger joins episodes to CP llm_calls by step_id + tokens
            └─ appends typed terminus2_* events to Loom trajectory JSONL
```

Harbor artifacts copied into the trial sandbox:

- `.loom/agent/trajectory.json`
- `.loom/agent/recording.cast`

Step runner always includes `.loom/agent/**` in artifact patterns for
`terminus-2` trials.

## Typed trajectory events

The subprocess-era events (`terminus2_session_ready`, `terminus2_exec_run`,
`loom-terminus-2` tmux recovery) were removed with the Harbor-embedded runtime.
Operators and
debuggers should look for:

| Event kind | Purpose |
|---|---|
| `terminus2_runtime_provenance` | Harbor pin, bridge revision, template hashes |
| `terminus2_turn` | Model turn boundary |
| `terminus2_command` | Command issued to the terminal |
| `terminus2_terminal_observation` | Bounded terminal output observation |
| `terminus2_parse_retry` | Parser retry after malformed model output |
| `terminus2_context_boundary` | Context/window boundary marker |
| `terminus2_artifact_ref` | Pointer to Harbor artifact (e.g. `recording.cast`) |

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
loom eval batch create \
  --name-suffix terminus2-gate3-smoke \
  --agent terminus-2 \
  --provider <team-provider> \
  --model <preflight-passing-model> \
  --benchmark loom-smoke \
  --task-filter '{"task_ids":["loom-smoke/gb10-oracle-hello-world"]}' \
  --n-per-task 1 \
  --required-worker-pool gb10
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
| `raw-harbor-tb2-v2` | Shipped framework (`[Needs validation]` for live staging proof) | Checkpoint bridge / typed `terminus2_*` events via `Terminus2TrajectoryMapper` |

`raw-harbor-tb2-v2` is wired in the delivery API and CLI. Use it when the
trial trajectory contains typed `terminus2_*` events and native Harbor
artifacts. Live staging validation is still pending; use `raw-harbor-tb2-v1`
for provider-log-based exports until typed-event batches are available.

## Related code

| Path | Role |
|---|---|
| `src/loom/agent/terminus2/runtime.py` | `LoomTerminus2Runtime` |
| `src/loom/agent/terminus2/harbor_environment.py` | Driver bridge |
| `src/loom/agent/terminus2/checkpoint_bridge.py` | Harbor JSON → typed events |
| `src/loom/agent/terminus2/gateway_ledger.py` | CP `llm_calls` join |
| `src/loom/agent/terminus2/mapper.py` | TB2 v2 projection |
| `src/loom_service/delivery_export_tb2_v2.py` | TB2 v2 export framework |
| `packages/loom-launcher/loom_launcher/terminus_2_runner.py` | Deprecated stub |
| `tests/conformance/terminus2/README.md` | Local conformance notes |
