# Overview

Loom runs LLMs against customizable tasks through pluggable agent
harnesses, capturing the full execution trace + metrics for browsing
and download. Two execution modes share the same primitives:

- **CLI mode** — `loom run` on a laptop. No server stack. Trajectories
  and ATIF docs land on local disk. Provider SDKs called directly.
  See [cli-mode.md](cli-mode.md).
- **Service mode** — Control Plane + Workers + LLM Gateway + Postgres
  + MinIO. Multi-team with DRF (Dominant Resource Fairness)
  scheduling. SPA at `web/` for browsing trials. See
  [service-mode.md](service-mode.md).

Both modes use the same `Trial.run()` orchestrator and emit
bit-identical event-sourced JSONL trajectories.

## Component map

| Component | Path | Role |
|---|---|---|
| Foundation library | `src/loom/` | Types, errors, models, Driver Protocol, Trial orchestrator, Trajectory writer/reader, ATIF projection, Verifier base |
| `loom` CLI | `src/loom_cli/` | `loom run/config/datasets`; stateless wrapper around `Trial.run()` |
| Cloud drivers | `src/loom_drivers/` | `Driver` Protocol implementations for Modal sandboxes |
| Control Plane | `src/loom_control_plane/` | Trial state machine, DRF claim, trajectory index, signed-URL artifact upload |
| LLM Gateway | `src/loom_llm_gateway/` | LiteLLM-backed provider proxy with rate-card cost compute + per-call attribution |
| Worker | `src/loom_worker/` | Polls Control Plane for trials, runs them locally, emits trajectory to MinIO, reports state via fenced PATCH |
| Service (REST + SPA) | `src/loom_service/` + `web/` | External REST surface (`/api/v1/...`) and React SPA |
| Operator CLI | `src/loom_benchmark_tool/` | `loom-benchmark list/import/verify` for cluster-side adapter management |
| Benchmark adapters | `packages/loom-benchmarks/` (13) + `packages/loom-benchmark-terminal-bench-2/` (1) | One PyPI-style sibling package per adapter family; discovered via `loom.benchmarks` entry-points |
| Agent adapters | `packages/loom-launcher/` (11 production + `hello` test reference) | Subprocess-based CLI agent wrappers (claude-code, codex, openhands, ...) |

Postgres + MinIO are the only stateful services. Control Plane,
Gateway, Worker, Service are all stateless.

## Where Trial.run() lives

`loom.trial.trial.Trial.run()` is the single orchestrator. It takes a
`TrialContext` and walks the trial state machine. Both modes construct
a `TrialContext` and hand it to `Trial.run()`; the only differences
are the wiring of the four dependencies:

| Dependency | CLI mode | Service mode |
|---|---|---|
| `ObjectStore` | `LocalDiskObjectStore` (host filesystem) | `MinioObjectStore` (MinIO via boto3) |
| `LLMGatewayClient` | `UpstreamDirectGatewayClient` (provider SDKs) | `HttpLLMGatewayClient` (HTTP to Loom Gateway) |
| `state_patch_callback` | None (no Control Plane) | `loom_worker.control_plane_client.PatchState` (fenced HTTP) |
| `Driver` | `FakeDriver` / `DockerDriver` / cloud driver | same set (chosen by `--backend` / worker config) |

Trajectories produced by both modes parse identically; ATIF JSON is
the same shape; verifier results are the same type.

## Data flow (one sentence)

A researcher (CLI) or `POST /trials` (service) submits a trial; the
trial loads + converts its task bundle, starts a sandbox, runs the
agent against the instruction, lets the verifier grade the result,
finalizes (uploads final trajectory parts, projects to ATIF), and
returns a `TrialResult`. See [service-mode.md](service-mode.md) for
the full ASCII timing diagram with claim, fencing, and finalize.

## State machine

`loom.models.result.TrialState` defines six terminal/non-terminal
states:

```
queued ──(claim SQL)──► claimed ──► running ──► succeeded
                            │            │           │
                            │            └──────►  failed
                            │
                            └─────────────────►  cancelled
                                   (PATCH from operator,
                                    valid from queued, claimed, or running)
```

`queued → claimed` happens only through the atomic claim SQL (a Worker
takes ownership); all other transitions go through `PATCH
/trials/{id}/state` and are fenced by `worker_id` match. Trial
finalization (uploading the last trajectory part + projecting ATIF) is
a side-effect of reaching `succeeded`/`failed`/`cancelled`, not its
own state.

## What lives where (so you can `cd` straight to it)

- **A Driver Protocol method's contract** —
  `src/loom/driver/base.py` (Protocol + ExecHandle + ExecResult + 10
  MB cap constant)
- **DockerDriver impl** — `src/loom/driver/docker.py`
- **ModalDriver impl** — `src/loom_drivers/modal/driver.py`
- **FakeDriver impl** — `src/loom/driver/fake.py`
- **Trial orchestrator** — `src/loom/trial/trial.py`
- **Trajectory writer** — `src/loom/trajectory/writer.py`
- **ATIF projection** — `src/loom/trajectory/atif.py`
- **Per-step runner** — `src/loom/trial/step_runner.py`
- **Verifier base** — `src/loom/verifier/base.py`
- **Concrete verifiers** — `src/loom/verifier/{pytest_verifier,script_verifier,structured,llm_judge,composite}.py`
- **Control Plane FastAPI app** — `src/loom_control_plane/app.py`
- **Worker main loop** — `src/loom_worker/main_loop.py`
- **LLM Gateway routes** — `src/loom_llm_gateway/routes/`
- **Service REST routes** — `src/loom_service/routes/`
- **CLI argparse** — `src/loom_cli/__main__.py`
- **CLI orchestrator** — `src/loom_cli/run_cmd.py`
- **Database schema** — `src/loom/db/schema.py` (SQLAlchemy models)
- **Migrations** — `migrations/versions/`

## See also

- [driver-protocol.md](driver-protocol.md)
- [benchmark-adapter.md](benchmark-adapter.md)
- [agent-adapter.md](agent-adapter.md)
- [trajectory-and-atif.md](trajectory-and-atif.md)
- [cli-mode.md](cli-mode.md)
- [service-mode.md](service-mode.md)
