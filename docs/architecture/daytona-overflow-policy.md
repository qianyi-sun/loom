# Daytona backend policy and local-first overflow

Issue #425 makes backend admission a persisted scheduling decision. It does not
provision Daytona capacity, deploy a worker, enable credentials, or run the live
pilot tracked by #430.

## Default and authority boundary

An omitted `backend_policy` is always `local_only` with `allowed_backends =
["docker"]`. A batch cannot select Daytona through the legacy `backend` string
alone.

Both explicit Daytona and automatic overflow are accepted only by
`POST /api/v1/admin/batches/on-behalf`. The operator must supply a complete
policy containing:

- ordered allowed backends;
- spillover delay;
- CPU, memory, and disk limits;
- an effective-dated USD price snapshot and source version;
- maximum runtime; and
- a hard maximum cloud cost.

The service computes the retry-aware worst case as:

`trials × max attempts × max runtime × hourly resource price`

Submission fails when that value exceeds the hard cloud budget. The accepted
snapshot, authority actor, and canonical SHA-256 digest are immutable on the
batch and copied to every child trial.

Example explicit policy (rate values are examples only; operators must use the
current approved rate card):

```json
{
  "backend": "daytona",
  "backend_policy": {
    "mode": "explicit",
    "allowed_backends": ["daytona"],
    "spillover_after_queue_seconds": 0,
    "daytona_resources": {"cpu": 2, "memory_gib": 4, "disk_gib": 10},
    "daytona_price_snapshot": {
      "source": "operator-rate-card",
      "version": "YYYY-MM-DD",
      "effective_at": "YYYY-MM-DDT00:00:00Z",
      "currency": "USD",
      "cpu_usd_per_hour": "0.00",
      "memory_gib_usd_per_hour": "0.00",
      "disk_gib_usd_per_hour": "0.00"
    },
    "max_cloud_cost_usd": "1.00",
    "max_runtime_seconds": 1800
  }
}
```

At least one rate must be positive; the zero values above intentionally cannot
be submitted without replacement.

## Compatibility matrix

Daytona admission fails before fan-out when any selected task needs:

- ARM or GPU execution;
- task sidecars;
- `extra_hosts`, custom DNS, or tmpfs;
- host-local `skills_dir` resources;
- an unmaterialized mutable image alias;
- a separate/private verifier or TB2.1 private workspace; or
- host-mounted family-run state.

The response uses `reason=daytona_task_incompatible` and returns stable reason
codes per task. Required local worker-pool coverage trials cannot share Daytona
authority.

## Atomic selection

Overflow trials start without `selected_backend`. The claim transaction keeps
DRF ordering and records exactly one decision:

- a compatible Docker worker may claim immediately, recording
  `local_capacity_available`;
- a Daytona worker must wait for the spillover threshold and may claim only
  when no fresh, active, compatible Docker worker has a free slot, recording
  `spillover_threshold_met`.

The same transaction writes `selected_backend`, reason, decision time, and the
resolved backend capability. Cloud-disabled trials remain pinned to Docker.
Trial list/detail responses expose the policy, digest, decision, compatibility
reasons, and either `spillover_delay_not_met` or
`no_eligible_backend_capacity` while an overflow trial remains unselected.

## Runtime enforcement

The worker validates the claim's persisted policy before sandbox creation.
Daytona receives explicit `Resources(cpu, memory, disk)` values. Task-level
limits may reduce those values but cannot exceed the policy ceiling. The
policy's maximum runtime also bounds the worker watchdog and sandbox cleanup
deadline. Usage accounting uses the persisted resource-price snapshot rather
than the historical placeholder rate.

Daytona organization pool exhaustion and API throttling remain provider
backpressure; they do not activate Loom's Slurm autoscaler. Fleet readiness,
live credentials, deployment, UI controls (#429), and pilot acceptance (#430)
are separate gates.

## Credentials

For local development, `DAYTONA_API_KEY` and `DAYTONA_API_URL` belong only in an
ignored, mode-0600 `.env` or the deployment secret store. Never place them in a
policy snapshot, task definition, issue, PR, log, or committed manifest.
