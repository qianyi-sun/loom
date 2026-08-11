# Pipeline orchestrator

The standalone `loom-pipeline-orchestrator` controller is disabled by default.
When enabled, it advances durable `PipelineRun` graphs from immutable
`RunGraphSpecV1` records and owns graph projection,
readiness freezing, bounded fan-out, retry decisions, hard-budget accounting,
terminal-cause latching, and final result projection. It does not execute
containers, commit Artifact objects, expose a public API, or create `Batch` or
`Trial` rows.

## Reconciliation and fencing

Controllers claim runs with `FOR UPDATE SKIP LOCKED`. A claim lasts exactly 60
seconds, is renewed every 10 seconds, and increments a durable lease epoch.
Every mutating repository operation verifies controller identity, lease epoch,
and lease expiry. Losing the claim ends that reconciliation pass; stale
controllers cannot continue to write.

Reconciliation is replay-safe:

1. Materialize singleton StageRuns and dependencies from the immutable graph.
2. Project automatic outcome gates and dependency state.
3. Expand a committed, validated fan-out manifest once, atomically creating
   zero, one, or many child StageRuns and their mirrored gate rows.
4. Freeze resolved bindings and exact execution-spec bytes at readiness.
5. Render outside the transaction, then atomically create one Attempt and all
   of its budget reservations. A renderer failure terminalizes the StageRun
   without creating an Attempt.
6. Settle or release reservations, apply the closed retry allowlist, and
   project the run result after the terminal barrier closes.

Attempt creation and downstream execution are separate. This controller stops
at the durable fenced handoff; it does not dispatch the Attempt to a worker or
commit its Artifact objects.

## Hard budgets and cancellation

Budget values are exact integer micro-USD amounts. Run, StageRun, Attempt, and
provider/GPU reservation keys use closed namespaces and digest-bound replay.
Reservation, settlement, and release are transactional. Provider or GPU truth
may report unavoidable overage, which terminalizes the run as
`accounting_violation`; an Artifact cannot be committed after its reservation
is exhausted.

The first terminal cause wins. User cancellation, wall-budget expiry, monetary
budget exhaustion, and accounting violation share one immutable latch. The
same transaction creates cancellation-outbox rows for live Attempts; cleanup
acknowledgements are durable and replay-safe. No retry may be scheduled after a
terminal cause is present.

## Acceptance seams

Acceptance preflight and fault-hold behavior is expressed through strict,
injected protocols. The controller persists authorization, candidate, worker,
Slurm allocation, capability, policy, and epoch snapshots before advancing a
preflight fence. The process does not provide production acceptance adapters
or execute a fixed-candidate acceptance run. It never reads secret values from
graph or result documents.

## BEHAVIOR rollout stage

The repository includes one executable BEHAVIOR stage adapter under
`src/loom/integrations/behavior/stages/rollout.py`. It implements only the
`rollout` stage; the other BEHAVIOR stage values remain contract types rather
than installed adapters. This stage process is separate from the graph
controller above and does not change the controller's disabled deployment
state or its Attempt-handoff boundary.

Invoke the adapter inside its pinned runtime image with:

```bash
python -m loom.integrations.behavior.cli run \
  --request /inputs/stage-request.json \
  --output-dir /outputs
```

The request must contain exactly three singleton bindings in order:
`task_instance` (`behavior_task_instance.v1`), `dataset`
(`behavior_dataset_snapshot.v1`), and `policy`
(`behavior_policy_checkpoint.v1`). Recording is fixed at 30 frames per second
with depth recording disabled. Request, input, recipe, image, execution-spec,
and compatibility digests are revalidated before the attempt workspace can
commit output.

The runtime contract accepts either two RTX 5080 devices on `oldlab`, split
between simulator and VLA roles, or one GB10 device shared by both roles. The
adapter applies the request's uint32 seed in a fixed Python, NumPy, PyTorch,
CUDA, and OmniGibson order. It starts the VLA server first, allows 180
one-second TCP readiness probes, then starts the one-episode simulator in the
same process group. The simulator deadline is 8,100 seconds. Interrupt and
failure cleanup sends `SIGINT`, waits up to 120 seconds, and uses `SIGKILL` only
for children that remain live.

A successful attempt atomically commits one
`behavior_rollout_bundle.v1` artifact and one `loom.stage-result.v1`. The
bundle contains the HDF5 trajectory, BDDL transitions, validated scene projection,
three H.264 camera streams, a fixed H.264 composite, and an optional predicate
catalog. Identity, seed, step count, frame count, media format, byte budget,
and provenance must agree across those files. Partial adapter output and
scratch data are removed on every failure.

`scripts/behavior/run_rollout.sbatch` is a two-argument compatibility shim for
the command above. It does not submit a Slurm job or own fan-out, retries,
input fetching, or upload; those remain surrounding Loom Pipeline authorities.
The adapter itself owns VLA and simulator child-process supervision.
`python -m loom.integrations.behavior.cli validate` provides read-only
canonical request, result, and artifact validation.

## Process and deployment

Run locally against an already migrated database:

```bash
LOOM_PIPELINE_ORCHESTRATOR_DB_URL='postgresql+psycopg://...' \
LOOM_PIPELINE_ORCHESTRATOR_CONTROLLER_ID='controller-1' \
python -m loom_pipeline_orchestrator
```

Configuration uses the `LOOM_PIPELINE_ORCHESTRATOR_` prefix. The picker batch
is capped at 50; lease and renewal periods are fixed at 60 and 10 seconds. The
process serves `/healthz` on port 8092.

`deploy/k8s/pipeline-orchestrator.yaml` and the cluster template intentionally
set `replicas: 0`. The image is buildable and covered by conformance, scan, and
attestation ownership, but it is excluded from the primary rollout set. A merge
or green CI run does not authorize scaling it up, applying migration 0079 to a
live database, or claiming staging/production acceptance.
