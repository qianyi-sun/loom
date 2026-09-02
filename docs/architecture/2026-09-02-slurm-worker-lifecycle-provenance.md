# Slurm worker lifecycle provenance

## Status and scope

This design fixes an ownership collision between GB10 node lifecycle workers
and elastic Slurm workers. Both worker kinds can report the same physical
hostname and pool, but only node lifecycle workers are governed by
`GB10WorkerPoolDesiredState.host_intents`.

The failed staging rollout at mutation epoch 239 demonstrated the collision:
`trt-gb10-1` was intentionally stopped as a node lifecycle host while an
autoscaler-owned Slurm allocation on that host registered a healthy worker.
The lifecycle status path selected the Slurm worker by hostname, and final
convergence rejected it as node-agent drift.

This change does not weaken the release gate, reserve a dedicated builder
host, change Slurm containment, or modify worker scheduling. It establishes
the ownership edge that the existing data model already supports and makes
GB10 lifecycle act only on workers it owns.

## Invariants

1. A hostname is location, not ownership. No exemption may be based only on
   hostname, pool, backend, freshness, or a naming convention.
2. Slurm provenance is an all-or-none group:
   `sandbox_identity`, `candidate_sha`, `slurm_job_id`, and
   `compose_project`.
3. A worker is Slurm-owned only when registration matches one exact active
   `SlurmWorkerJob` on cluster, environment, pool, hostname, candidate,
   compose project, job ID, and concurrency.
4. The worker insert and `SlurmWorkerJob.worker_id` assignment occur in one
   database transaction while the job row is locked.
5. Slurm observations update scheduler state but never establish or replace
   worker ownership.
6. GB10 lifecycle excludes only workers linked to active, internally
   consistent Slurm jobs. Terminal, unlinked, incomplete, or inconsistent
   links remain visible to lifecycle and therefore fail closed.
7. Existing workers that omit Slurm provenance remain valid legacy/node
   workers. No schema migration or backfill guesses ownership.

## Registration contract

Elastic Slurm workers already receive these settings from the controller:

- `LOOM_WORKER_SANDBOX_IDENTITY`
- `LOOM_WORKER_CANDIDATE_SHA`
- `LOOM_WORKER_SLURM_JOB_ID`
- `LOOM_WORKER_COMPOSE_PROJECT`

When `slurm_job_id` is nonempty, the worker passes all four values in
`POST /workers/register`. The client rejects a partial group before making the
request, and the Control Plane independently applies the same all-or-none
rule. Non-Slurm workers omit the group even if they have unrelated deployment
labels.

The Control Plane infers `slurm_cluster_id` with `slurm_cluster_for_pool()`
and locks the single `SlurmWorkerJob` matching all of:

| Worker registration | Slurm job field |
|---|---|
| inferred cluster | `slurm_cluster_id` |
| `sandbox_identity` | `environment` and `sandbox_identity` |
| `pool_name` | `pool_name` |
| `hostname` | `nodelist` |
| `max_concurrent` | `requested_concurrency` |
| `candidate_sha` | `candidate_sha` |
| `slurm_job_id` | `job_id` |
| `compose_project` | `compose_project` |

The job must be `pending` or `running` and have no linked worker. A missing,
terminal, mismatched, or already-linked job returns a conflict without
revealing which provenance field differed. Once validated, the transaction
inserts the `Worker` and sets the existing job's `worker_id` to the new UUID.
The current foreign key and job-ID uniqueness constraints are sufficient, so
no migration is required.

## Ownership classifier

The lifecycle classifier reads `SlurmWorkerJob` joined to `Worker` and admits
only active links whose stored relationship is internally consistent:

- job state is `pending` or `running`;
- all stored job provenance fields are nonempty;
- `environment == sandbox_identity`;
- the job cluster equals `slurm_cluster_for_pool(pool_name)`;
- worker pool, hostname, and concurrency equal the job's pool, nodelist, and
  requested concurrency.

This check defends against stale historical links and privileged/manual data
errors. The registration transaction remains the only supported way to create
a new link. In particular, a Slurm state observation carrying `worker_id`
may confirm the same existing link but cannot create or overwrite it.

## GB10 lifecycle behavior

The classifier is applied at every hostname-based lifecycle boundary:

1. desired `draining` or `stopped` intent does not mutate a proven active
   Slurm worker's drain state;
2. node-report reconciliation does not drain or recover a proven active
   Slurm worker;
3. lifecycle status does not select a proven active Slurm worker as a node
   worker;
4. lifecycle status does not list that worker as unlinked drift.

When the Slurm job becomes terminal or its relationship becomes
inconsistent, the exemption disappears automatically. A still-fresh worker
then appears in the existing node/unlinked inventory and the unchanged
release gate blocks convergence. This is intentional: the Slurm registry,
not elapsed time or hostname, controls the ownership transition.

## Failure and upgrade behavior

- Old worker binary against new Control Plane: registration omits provenance
  and succeeds as before; the worker is not granted a Slurm exemption.
- New worker binary against old Control Plane: the old dictionary-based route
  ignores the additional keys; rollout order still upgrades the Control Plane
  before relying on the classifier.
- Partial provenance: registration fails before worker creation.
- Exact job not found or terminal: registration returns HTTP 409.
- Duplicate registration for one job: the row lock serializes requests; only
  one worker can be linked.
- Reconcile reports another worker ID: the registered link is preserved and
  the conflicting observation is logged.
- Active job turns terminal: lifecycle sees the worker again and existing
  fail-closed release checks apply.

## Verification

Tests must prove:

- the worker main loop and HTTP client emit the complete group and reject a
  partial group;
- exact registration links the worker and job transactionally;
- wrong candidate, host, pool, concurrency, partial group, terminal job, and
  duplicate link are rejected without creating a worker;
- Slurm observations cannot create or replace ownership;
- active exact links are excluded from both lifecycle reconciliation and
  status inventory;
- terminal or internally inconsistent links are not excluded;
- an ordinary fresh worker on a stopped GB10 host is still reported and the
  existing release gate still rejects it.

Live acceptance requires a fresh protected rollout request bound to the
merged candidate. The failed epoch-239 request remains immutable evidence and
must not be resumed after candidate/config drift.
