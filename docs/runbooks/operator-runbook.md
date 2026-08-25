# Loom operator runbook

This is the current operating reference for shared and production Loom
deployments. Component behavior belongs in the
[architecture documentation](../architecture/README.md); local-only setup is
covered by the [local development runbook](local-dev-workflow.md).

## Environment boundaries

Keep each environment's Kubernetes credentials, databases, object buckets,
SecretStore keys, worker tokens, provider connections, deploy credentials, and
GitHub Environment secrets separate.

| Environment | Source | Namespace | Public route | API base |
| --- | --- | --- | --- | --- |
| development | `dev` | `loom-dev` | `https://yylx.world/dev` | `https://yylx.world/dev/api` |
| staging | pinned `dev` SHA | `loom-staging` | `https://yylx.world/staging` | `https://yylx.world/staging/api` |
| production | `main` | `loom-prod` | `https://yylx.world/prod` | `https://yylx.world/prod/api` |

The checked-in profiles are under `deploy/environments/`. Validate their
identity and the deployment workflow before promotion:

```bash
uv run --no-sync python scripts/validate_environment_isolation.py \
  --profiles-dir deploy/environments \
  --workflow .github/workflows/deploy-environment.yml \
  --dry-run-artifact release-evidence/environment-isolation-dry-run.json
```

The generic `.github/workflows/deploy-environment.yml` deployment path owns
development and production only. It rejects staging; validation of the staging
profile here does not grant the hosted workflow staging mutation authority.

Dry-run evidence may contain safe secret references, but never credentials,
bearer tokens, signed URLs, object-store keys, or provider API keys.

## Locked operator environment

Use the repository lock and workspace packages for release commands:

```bash
uv sync --locked --all-packages --extra cluster --extra rollout --extra dev --python 3.11
uv pip check --python .venv/bin/python
```

The rollout extra installs the implementations under
`packages/loom-benchmarks` and `packages/loom-benchmark-terminal-bench-2`.
After syncing, use `uv run --no-sync` so an operational command cannot change
the environment implicitly.

## Shared staging

> **Shared-staging invariant:** only the root-installed rollout authority may
> mutate `loom-staging`. It fresh-fetches its configured remote, admits only
> the configured merged branch head (`refs/heads/dev` for staging), binds a
> request to that immutable candidate, creates the protected backup, and owns
> the complete rollout lifecycle. Unmerged pull-request refs, personal
> checkouts, caller-supplied SHAs, and direct lower-level cluster mutations are
> rejected.

`loom-staging-rollout --env staging start` is the only staging mutation path.
Hosted workflow dispatch cannot substitute for the installed authority's
host-local secrets, shared lock, protected backup, isolated rehearsal, or final
gate evidence.

Use the installed client:

```bash
loom-staging-rollout --env staging preflight
loom-staging-rollout --env staging start --dry-run
loom-staging-rollout --env staging start
loom-staging-rollout --env staging status REQUEST_ID
loom-staging-rollout --env staging logs REQUEST_ID
loom-staging-rollout --env staging logs REQUEST_ID --follow
loom-staging-rollout --env staging resume REQUEST_ID
loom-staging-rollout --env staging cancel REQUEST_ID --reason "bounded operational reason"
loom-staging-rollout --env staging cleanup-incomplete-backup REQUEST_ID
loom-staging-rollout --env staging lifecycle-capacity inventory --artifact-bundle-sha256 DIGEST
loom-staging-rollout --env staging lifecycle-capacity apply --artifact-bundle-sha256 DIGEST --approved-plan-sha256 SHA256
loom-staging-rollout --env staging backup-recovery inventory
loom-staging-rollout --env staging backup-recovery apply --approved-plan-sha256 SHA256
loom-staging-rollout --env staging backup-retention inventory
loom-staging-rollout --env staging backup-retention apply --approved-plan-sha256 SHA256
```

`start` accepts no ref, SHA, image tag, checkout, secret, or passthrough
argument. `--dry-run` validates authority and resolves the current candidate
without mutating staging. `status` and `logs` expose redacted request evidence.
Use the `preflight_artifact_bundle_sha256` value returned by a passing
`preflight` or `start` command as `DIGEST`; use the same value for inventory and
apply.
Use `resume` only for the same immutable request after correcting its failure;
use a merged revert on `dev` and a new request to roll code back.

If a detached backup job is already `backup_verified`, `launch_pending`, or
`launch_running` while rotation still contains its manifest-verified candidate,
do not use incomplete-backup cleanup and do not edit the rotation files. While
the lifecycle is maintenance-idle, inspect `backup-recovery inventory`, approve
the exact printed plan digest, and apply that digest. Then inspect and separately
approve `backup-retention` to retire the prior active payload. Exact replay is
idempotent; any lease, attestation, request, Attempt, or rotation drift is a
hard refusal.

The broker enforces a single environment lifecycle owner. Broker unavailability
does not grant authority for direct mutation. See
[Protected Staging Rollout](../architecture/staging-rollout.md) for the
persistence and refusal contract and [Staging Release Validation](staging-launch.md)
for the current acceptance checks.

## Unprotected and custom clusters

For an operator-authorized custom cluster, inspect before applying:

```bash
uv run --no-sync loom cluster preflight --config cluster-config.toml
uv run --no-sync loom cluster render --config cluster-config.toml > /tmp/loom-rendered.yaml
uv run --no-sync loom cluster audit --config cluster-config.toml
uv run --no-sync loom cluster up --config cluster-config.toml --migrate
uv run --no-sync loom cluster status --config cluster-config.toml
```

Use `loom cluster reconcile --shadow` for a read-only desired-vs-live drift
report. `loom cluster down` preserves PVCs and the namespace unless explicit
volume or namespace deletion flags are supplied. Never use these direct
mutation commands against shared staging.

The checked-in multi-node staging deployment has its own
[k3s procedure](deploy-staging-k3s.md).

## Workload trust boundary

Protected profiles accept only the current internal-trust tuple:

```toml
workload_trust_mode = "internal_trusted"
taskset_transforms_enabled = false
taskset_transform_network_isolated = false
untrusted_workload_isolation = false
```

Invalid profile, manifest, or live environment values fail before transform,
source, verifier, or subprocess content is fetched or run. The protected
namespace is authoritative target evidence, and manual rollout validates the
cluster and namespace identity before evidence collection or disposable local
work. `--skip-preflight` does not bypass the contract. See
[User-Brought TaskSets](../architecture/user-brought-tasksets.md) and
[Sandbox Isolation](../architecture/sandbox-isolation.md).

## Schema migrations

Run Alembic before rolling application pods. The standard deployment and
protected rollout paths do this automatically. For an authorized manual
cluster, render the sanctioned Job with the same image identity used by the
deployment:

```bash
uv run --no-sync loom cluster render-migration \
  --image-tag IMAGE_TAG \
  --namespace NAMESPACE \
  --container-registry REGISTRY > /tmp/loom-migration.yaml
kubectl apply -f /tmp/loom-migration.yaml
kubectl -n NAMESPACE wait --for=condition=complete job/LOOM_MIGRATION_JOB --timeout=10m
```

Inspect the failed Job and its logs before retrying. Database migrations are
forward-only during an image rollback; restoring an older schema requires the
separately approved backup-and-restore procedure.

## Backup and restore

Before a protected release, create database, object-store, and runtime-secret
backups using the environment's backup system. Then record a metadata-only
manifest and verify its completeness and age:

```text
loom cluster backup manifest --help
loom cluster backup check --help
```

Keep backup data and credentials outside repository Markdown, issues, pull
requests, and workflow logs. A restore must target the same environment
identity, start from an independently verified backup, pause new submissions,
restore database and object objects coherently, reapply runtime secrets, run
the current migration Job, and pass health plus representative trial checks
before traffic resumes.

## Production release

Production is promoted from a pinned `dev` candidate; it is not deployed from
an arbitrary branch or tag.

1. Choose the 40-character candidate SHA, built image tag or digest, and a new
   immutable SemVer `prod_tag` such as `v1.2.3`.
2. Deploy the exact candidate to shared staging through the installed rollout
   authority and complete [staging validation](staging-launch.md).
3. Build the structured release manifest required by
   `scripts/ops/release_gate.py`, including image digests, frontend route
   evidence, `prod_staging_isolation`, `raw_delivery_export_status`, rollback
   and recovery data, and `release_owner_approval`.
4. Dispatch `.github/workflows/release-promotion-gate.yml` from `dev` with
   `candidate_sha`, `image_tag`, and the base64-encoded evidence manifest.
5. Open the `dev` to `main` promotion pull request and enable GitHub's native
   squash auto-merge; the protected current-head CI gates are the only merge
   authority.
6. Dispatch `.github/workflows/deploy-environment.yml` from `main` with
   `environment=production`, the same `candidate_sha` and `image_tag`, and the
   successful `release_gate_run_id`.
7. Put the recorded immutable `prod_tag` on the merged `main` commit. Never
   reuse or force-move a published production tag.

Manifest `release_owner_approval` records acceptance of the candidate and
evidence. Production Environment approval releases deployment secrets. They
are distinct controls and are not interchangeable with CI merge authority.

Rollback uses a previously recorded image digest or a merged revert promoted
through the same gate. Restore from the recorded recovery point if the new
schema or data is incompatible; do not force-move a tag.

## Storage retention

Review the configured lifecycle policy before applying it:

```bash
uv run --no-sync loom cluster bootstrap-storage-lifecycle \
  --config config/storage-lifecycle.example.toml \
  --dry-run
uv run --no-sync loom cluster bootstrap-storage-lifecycle \
  --config /secure/path/storage-lifecycle.toml \
  --endpoint https://OBJECT_STORE_ENDPOINT
```

The live apply path supports S3-compatible backends. Re-running the same policy
is idempotent. Confirm rules through the object-store API and monitor bucket
usage after changes. Pure GCS rendering is available in the renderer library
but is not dispatched by this CLI.

## Credential rotation

For worker tokens, overlap old and new credentials until every in-cluster and
remote worker has re-registered with the new token:

```text
loom admin tokens worker rotate --help
loom admin tokens worker revoke --help
```

For provider connections, rotate and then validate the stored credential:

```bash
loom providers rotate-key CONNECTION --api-key env:PROVIDER_API_KEY
loom providers test CONNECTION
loom providers models CONNECTION --refresh
loom providers models CONNECTION --preflight MODEL
```

For the SecretStore master key, deploy the new primary alongside the old
fallback, run `loom admin secret-store rewrap`, verify all rows, restart
services with only the new key, and remove the fallback. Never remove the old
key before rewrap succeeds.

## Production alerts

The Prometheus rules in `deploy/k8s/prometheus-rules.yaml` are the source of
truth. Start with the firing rule, then correlate service health, pod restarts,
recent rollouts, queue/worker inventory, and dependency metrics.

| Area | Alerts | Immediate checks |
| --- | --- | --- |
| scheduling | `LoomNoWorkersActive`, `LoomQueueBacklog`, `LoomTrialsStuckClaimed`, `LoomWorkerReclaimsSpiking`, `LoomRetryExhaustedSpiking`, `LoomClaimLatencyP95High` | Worker inventory, claim latency, capacity limits, trial events |
| Control Plane | `LoomStatePatchTimeouts`, `LoomControlPlaneDown` | `/healthz`, pod logs, Postgres/PgBouncer, rollout state |
| Gateway | `LoomLLMGatewayDown`, `LoomGatewayProviderErrorRate`, `LoomGatewayCostSpike` | Gateway health, provider connection status, upstream errors and usage attribution |
| service/API | `LoomServiceDown`, `LoomServiceHighErrorRate`, `LoomServiceAuthFailureSpike`, `LoomServiceSubmissionRejectSpike` | `/api/v1/health`, ingress, auth audit events, rejection reasons |
| workers | `LoomWorkerProcessDown`, `LoomWorkerHeartbeatFailing`, `LoomWorkerTrialFailureRateHigh`, `LoomWorkerTokenStaleness` | Host/process health, heartbeat age, Docker/Slurm capacity, token generation |
| pooling/listen | `LoomPgbouncerClientWaiting`, `LoomPgbouncerScrapeDown`, `LoomListenWatcherPollFallback` | Pool saturation, exporter health, database reachability and LISTEN fallback |
| object storage | `LoomMinioPVCUsageHigh`, `LoomMinioPVCUsageCritical`, `LoomMinioWriteLatencyHigh`, `LoomMinioRequestErrorRateHigh`, `LoomMinioNodeOffline` | PVC and bucket usage, node/quorum state, write latency, lifecycle rules |
| Pipelines | `LoomPipelineStageQueueStuck`, `LoomPipelineStageDeadlineOverrun`, `LoomPipelineControllerReconcileErrors`, `LoomPipelineForcedCancellation`, `LoomPipelineCheckpointStale`, `LoomPipelineArtifactCommitFailures`, `LoomPipelineGpuAllocatedIdle` | Pipeline list/show/watch, Pipeline panels, scoped controller/worker logs, authority boundary |

Silence an alert only for a bounded maintenance window with an owner and
expiry. Record the sanitized incident timeline outside active reference docs.

### Worker-token staleness

Compare the active token prefixes reported by workers with the intended
generation. Install the new token on every consumer, restart one worker at a
time, confirm clean registration and claims, and only then revoke the old
prefix. An alert during an intentional overlap is expected; an old generation
after the overlap is not.

### MinIO PVC usage

Use `lifecycle-capacity inventory` above for shared staging; it measures the
least-free live MinIO drive on the distributed deployment and inventories the
exact managed buckets without granting write authority. For a single-node
custom cluster, confirm its host-path filesystem and bucket usage directly:

```bash
kubectl -n NAMESPACE exec statefulset/loom-minio -- df -h /data
mc du --recursive ALIAS/BUCKET
```

At the high threshold, identify growth and validate retention. At the critical
threshold, pause large submissions and protect write headroom. Never remove
objects directly unless their retention and trial/artifact references have
been verified.

### Pipeline Stage queue stuck

`loom_pipeline_stage_queue_age_seconds` means the maximum age by closed state and resource class. The alert requires ready/queued/retry_wait above 900 seconds or claimed above 300 seconds for 10 minutes. Use Grafana/Prometheus, `loom pipeline list`, `loom pipeline show RUN_ID`, and `loom pipeline watch RUN_ID`; then read only scoped logs with `kubectl -n NAMESPACE logs deploy/loom-control-plane --since=30m`. Cancel or retry only with submit authority; do not mutate database state or auto-remediate from the alert.

### Pipeline Stage deadline overrun

`loom_pipeline_stage_deadline_overrun_seconds` means time beyond the frozen Stage timeout plus 35-second cleanup grace. Any positive value for 5 minutes is critical. Correlate Grafana/Prometheus with `loom pipeline list/show/watch` and scoped `kubectl -n NAMESPACE logs -l app=loom-worker --since=30m`. Cancellation and worker remediation require the applicable operator authority; the alert grants none.

### Pipeline controller reconcile errors

`loom_pipeline_controller_reconcile_errors_total` counts exhausted operations, not successful transaction retries. Three increases in five minutes sustained for five minutes warn. Inspect its closed reason label, `loom pipeline list/show/watch`, and scoped controller logs. Do not bypass controller invariants, budgets, or durable leases; retry only through the authorized Pipeline API.

### Pipeline forced cancellation

`loom_pipeline_cancel_latency_seconds_count{outcome="forced"}` records positive forced-cleanup acknowledgement. Any increase over 15 minutes fires immediately. Inspect the run and Attempt lifecycle with `loom pipeline show/watch`, the cancellation Grafana panel, and scoped worker logs. Preserve committed Artifacts; follow worker cleanup authority before any remediation.

### Pipeline checkpoint stale

`loom_pipeline_checkpoint_oldest_age_seconds` is the age of the newest commit, or Attempt start before sequence zero, for active checkpoint-enabled Attempts. Above 300 seconds for 10 minutes warns. Use Grafana/Prometheus, `loom pipeline show/watch`, and scoped worker logs. Do not synthesize a checkpoint or enable checkpoint reuse; manual retry creates a full replay only when the API says eligible.

### Pipeline Artifact commit failures

`loom_pipeline_artifact_commit_failures_total` counts one returned operation failure by closed commit kind and reason. Any increase over 10 minutes sustained for five minutes warns. Inspect Grafana/Prometheus, `loom pipeline show/watch`, and scoped control-plane logs. Never retry individual multipart chunks manually, expose object keys, or weaken integrity/quota/fencing checks.

### Pipeline GPU allocated idle

`loom_pipeline_gpu_allocated_idle_seconds` measures a leased local GPU Attempt whose expected process group is absent or cleanup is pending; low utilization is intentionally excluded. More than 300 seconds for 10 minutes is critical. Inspect the closed cluster/reason series, `loom pipeline list/show/watch`, and scoped worker/Slurm logs. Drain or cancel only under worker/cluster authority; do not kill an unidentified process from metric labels.

## Incident handoff

For any failed rollout or service incident:

1. Stop new mutations and preserve the current request, logs, and metrics.
2. Identify the environment, candidate/image identity, first failing check,
   affected teams, and whether durable state may be inconsistent.
3. Prefer read-only `status`, `reconcile --shadow`, health, and inventory
   commands before changing state.
4. Resume an immutable staging request only after its underlying failure is
   corrected; otherwise use a normal new rollout or release rollback.
5. Store redacted incident and release artifacts outside `docs/`; archive a
   stable retrospective only when it has lasting historical value.
