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
