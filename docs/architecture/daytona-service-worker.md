# Daytona service-mode worker

Daytona is an explicit, opt-in service-worker backend. The default remains
`docker`; setting `LOOM_WORKER_SANDBOX_BACKEND=daytona` starts a small Loom
controller that manages multiple remote Daytona sandboxes. The sandboxes are
not Kubernetes Pods and the controller does not use the Slurm controller,
GB10/OLDLAB autoscaler, Docker socket, Compose, cgroups, or the local trial
image cache.

## Startup contract

A Daytona controller fails before worker registration unless all of these are
true:

- `LOOM_WORKER_CANDIDATE_SHA` is the exact lowercase 40-character commit SHA;
- `DAYTONA_API_KEY` or `DAYTONA_JWT_TOKEN` is available;
- Docker/Slurm-local settings such as cgroup containment, sandbox isolation,
  Compose identity, local vLLM, and per-container limits are disabled;
- the configured Daytona API concurrency, cleanup interval, and sandbox TTL
  are positive.

The worker advertises only `backend=daytona`. Batch backend selection is copied
into the Trial capability snapshot and the claim query requires an exact
backend match, so a Docker worker cannot claim Daytona work and vice versa.

## Immutable execution identity

Each claimed Daytona trial must start from a registry reference of the form
`repository@sha256:<digest>`. Dockerfile tasks consume the scheduler's frozen
`TaskImageExecutionGrantV1`; prebuilt tasks must already name an immutable
digest. Mutable tags, worker-local image builds, sidecars, custom DNS/hosts,
tmpfs, host-mounted family state, private-workspace verifier splitting, and
runtime agent install layers fail closed. Those workloads stay on Docker or
Slurm until their dedicated compatibility work lands.

Before provider creation, the controller persists this tuple in Postgres:

`trial_id / attempt_count / team_id / worker_id / candidate_sha /
provider_scope / artifact_ref / sandbox_name`.

The deterministic sandbox name is the provider idempotency key. On restart or
an ambiguous create response, the controller looks up the stored sandbox ID or
name before creating. It therefore adopts a committed sandbox instead of
creating a duplicate.

## Lifecycle and recovery

Loom owns the hard deadline, configured TTL, cancellation, usage attribution,
and cleanup journal. Daytona creation explicitly sets auto-stop and auto-pause
to `0` and auto-delete to `-1`, preventing provider idle detection from
terminating a background job. The controller bounds and spaces lifecycle API
calls with a process-wide gate.

Successful start and delete transitions are written to `daytona_sandboxes`.
Deletion inserts exactly one `cloud_compute_records` row while holding the
ledger row lock. A periodic reconciler leases cleanup work when a trial is
terminal, reassigned, on another attempt, past its Loom deadline, or already in
`delete_pending`. Cleanup is fenced to a one-way fingerprint of the Daytona API
URL, target, and credential so a controller cannot delete a sandbox from
another provider account or scope. Provider 404 is treated as an idempotent
successful delete; other
errors return the row to `delete_pending` for retry.

## Opt-in configuration

The capability ships disabled. An operator may configure a dedicated
deployment with:

```text
LOOM_WORKER_SANDBOX_BACKEND=daytona
LOOM_WORKER_CANDIDATE_SHA=<exact-git-sha>
DAYTONA_API_KEY=<secret>
LOOM_WORKER_DAYTONA_API_MAX_CONCURRENT=4
LOOM_WORKER_DAYTONA_API_MIN_INTERVAL_SEC=0.25
LOOM_WORKER_DAYTONA_SANDBOX_TTL_SEC=86400
LOOM_WORKER_DAYTONA_CLEANUP_INTERVAL_SEC=30
```

This configuration alone does not provision credentials, deploy a controller,
enable automatic overflow from local pools, or alter the Docker worker.
