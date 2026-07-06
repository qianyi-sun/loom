# Full/Max-Slot Three-Cluster Canary Runbook

This runbook prepares the unified staging canary for #49/#129 with the
current #188/#193/#190/#271 blocker set. It is a preparation artifact only:
do not submit the canary until the coordinating thread gives an explicit
`GO` for the full/max-slot three-cluster canary.

The canary must prove that a current staging release can execute a
SkillLearnBench full/max-slot workload across all three release-managed pools:

- `oldlab` - x86_64 elastic Slurm workers.
- `k8s-worker` - x86_64 Kubernetes worker Deployment.
- `gb10-arm64` - ARM64 GB10 Slurm-backed workers.

## Hard Gates Before GO

Stop before submitting any workload unless every item in this section is true.

1. The coordinator has posted an explicit `GO` for this canary.
2. The staging anchor is clean at execution time. A clean anchor requires
   the latest `environment-state check` for the chosen rollout to report
   `ok=true` and `drift=[]`, with no active Slurm jobs, GB10 node-agent
   reports, worker env files, worker repo paths from an older `IMAGE_TAG`,
   missing or unexecutable external autoscaler `ExecStart` paths, or recent
   failed external autoscaler service results such as `status=203/EXEC`.
   Historical drift such as stale OLDLAB Slurm job `17972` on
   `staging-ce55a35` is a stop condition until the follow-up check proves
   the replacement state is clean.
3. #190 targeted durability validation has completed on the clean anchor.
   Do not use the full/max-slot canary as the first proof of the #190 S3
   materialization and trajectory/artifact durability fix.
4. #271 rollout/startup stability validation has completed on the clean
   anchor. The rollout evidence must not show control-plane, service, worker,
   Postgres, or DNS dependency crash loops.
5. #188 pool coverage is configured in the canary submission with repeated
   `--required-worker-pool oldlab --required-worker-pool k8s-worker
   --required-worker-pool gb10-arm64`. The selected task slate must include at
   least one task compatible with each required pool's CPU architecture when
   that architecture is known from active workers or autoscaler policy; fanout
   records `required_worker_pool_incompatible` rather than submitting an
   unclaimable coverage trial when it cannot satisfy a pool.
6. #193 coverage expectations are explicit. The current canary covers the
   release-gate observable `claimed_without_started=0` through batch debug
   evidence and `scripts/staging_smoke_gate.py`. It does not replace a
   deliberate #193 fault-injection/reclaim test unless #193 has separately
   landed that diagnostic hook and the coordinator adds it to GO scope.

## Operator Inputs

Set these values in the shell that will run the canary. Use paths from the
clean staging rollout, not from an older rollout.

```bash
export RELEASE_SHA=<clean-anchor-git-sha>
export IMAGE_TAG=staging-<clean-anchor-sha7>
export ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}"
export ROLLOUT_DIR=/data/loom-staging/rollouts/<clean-anchor-dir>
export RUN_ID=full-max-slot-three-cluster-$(date -u +%Y%m%dT%H%M%SZ)
export CANARY_DIR="$ROLLOUT_DIR/$RUN_ID"
export CLUSTER_CONFIG=<path-to-clean-anchor-cluster-config.toml>

export PUBLIC_URL=https://loom.example.com
export CP_URL=http://control-node.lan:18081
export ADMIN_TOKEN_FILE=/secure/path/admin-token
export WORKER_TOKEN_FILE=/secure/path/worker-token
export K8S_NAMESPACE=loom-staging

export TEAM_A_TOKEN=<team-a-user-owned-api-token>
export TEAM_B_TOKEN=<team-b-user-owned-api-token>
export AGENTIC_RL_TEAM_ID=<agentic-rl-team-uuid>
export PROVIDER_CONNECTION_NAME=mz_tn_canada_qianyi
export PROVIDER_MODEL_PROVIDER=yibuapi
export PROVIDER_MODEL_NAME=gpt-4o-mini
export COORDINATOR_APPROVED_28_TASK_FILTER_JSON=<path-or-empty-for-full100>
```

Create the evidence layout only after GO or as a dry preparation step that
does not touch the cluster:

```bash
mkdir -p \
  "$CANARY_DIR/00-anchor" \
  "$CANARY_DIR/01-clean-anchor" \
  "$CANARY_DIR/02-preflight" \
  "$CANARY_DIR/03-190-targeted" \
  "$CANARY_DIR/04-submit" \
  "$CANARY_DIR/05-watch" \
  "$CANARY_DIR/06-terminal" \
  "$CANARY_DIR/07-summary"
```

## Clean Anchor Preflight

These commands are release-convergence gates. `environment-state apply` is
idempotent but mutating; run it only in the authorized pre-canary window. Any
non-zero exit, stale release path, missing pool, service restart, or drift row
is a stop condition.

```bash
git rev-parse HEAD | tee "$CANARY_DIR/00-anchor/local-head.txt"
test "$(git rev-parse HEAD)" = "$RELEASE_SHA"

loom cluster release-manifest \
  --config "$CLUSTER_CONFIG" \
  --environment staging \
  --image-tag "$IMAGE_TAG" \
  --git-sha "$RELEASE_SHA" \
  --environment-state-file deploy/environment-state/staging.toml \
  --env-config-version "$ENV_CONFIG_VERSION" \
  --expected-image-identities-json "$CANARY_DIR/00-anchor/image-identities-$IMAGE_TAG.json" \
  --output "$CANARY_DIR/00-anchor/release-manifest-$IMAGE_TAG.json"

loom cluster minio-storage-preflight \
  --namespace "$K8S_NAMESPACE" \
  --output "$CANARY_DIR/01-clean-anchor/minio-storage-preflight-$IMAGE_TAG.json" \
  --format json \
  | tee "$CANARY_DIR/01-clean-anchor/minio-storage-preflight-$IMAGE_TAG.stdout.json"

loom cluster release-gate \
  --manifest "$CANARY_DIR/00-anchor/release-manifest-$IMAGE_TAG.json" \
  --config "$CLUSTER_CONFIG" \
  --rendered-manifest "$CANARY_DIR/00-anchor/rendered.yaml" \
  --namespace "$K8S_NAMESPACE" \
  --environment staging \
  --minio-storage-preflight "$CANARY_DIR/01-clean-anchor/minio-storage-preflight-$IMAGE_TAG.json" \
  --format json \
  | tee "$CANARY_DIR/01-clean-anchor/release-gate-$IMAGE_TAG.json"

loom cluster status \
  --namespace "$K8S_NAMESPACE" \
  --format json \
  | tee "$CANARY_DIR/01-clean-anchor/cluster-status.json"

LIVE_ADMIN_TOKEN_FINGERPRINT="$(
  kubectl -n "$K8S_NAMESPACE" get secret loom-admin-secret \
    -o jsonpath='{.data.secrets\.toml}' \
    | base64 -d \
    | uv run python -c 'import hashlib,sys,tomllib; token=tomllib.loads(sys.stdin.read())["admin"]["token"]; print(f"sha256:{hashlib.sha256(token.encode()).hexdigest()[:12]} len={len(token)}")'
)"

loom admin environment-state apply \
  --cp-url "$CP_URL" \
  --admin-token "file:$ADMIN_TOKEN_FILE" \
  --expect-admin-token-fingerprint "$LIVE_ADMIN_TOKEN_FINGERPRINT" \
  --environment staging \
  --file deploy/environment-state/staging.toml \
  --var IMAGE_TAG="$IMAGE_TAG" \
  --var ENV_CONFIG_VERSION="$ENV_CONFIG_VERSION" \
  --var GIT_SHA="$RELEASE_SHA" \
  | tee "$CANARY_DIR/01-clean-anchor/environment-state-apply.txt"

loom admin environment-state check \
  --cp-url "$CP_URL" \
  --admin-token "file:$ADMIN_TOKEN_FILE" \
  --expect-admin-token-fingerprint "$LIVE_ADMIN_TOKEN_FINGERPRINT" \
  --environment staging \
  --file deploy/environment-state/staging.toml \
  --var IMAGE_TAG="$IMAGE_TAG" \
  --var ENV_CONFIG_VERSION="$ENV_CONFIG_VERSION" \
  --var GIT_SHA="$RELEASE_SHA" \
  --worker-token "file:$WORKER_TOKEN_FILE" \
  | tee "$CANARY_DIR/01-clean-anchor/environment-state-check.txt"

loom admin slurm-workers status \
  --cp-url "$CP_URL" \
  --admin-token "file:$ADMIN_TOKEN_FILE" \
  --format json \
  | tee "$CANARY_DIR/01-clean-anchor/slurm-workers-status.json"

loom admin gb10-workers status \
  --cp-url "$CP_URL" \
  --admin-token "file:$ADMIN_TOKEN_FILE" \
  --environment production \
  --pool-name gb10-arm64 \
  --release-image-tag "$IMAGE_TAG" \
  --release-env-config-version "$ENV_CONFIG_VERSION" \
  --format json \
  | tee "$CANARY_DIR/01-clean-anchor/gb10-workers-status.json"

loom admin worker-pools autoscaler status \
  --cp-url "$CP_URL" \
  --admin-token "file:$ADMIN_TOKEN_FILE" \
  --format json \
  | tee "$CANARY_DIR/01-clean-anchor/worker-pool-autoscalers-status.json"

loom resources status --json \
  | tee "$CANARY_DIR/01-clean-anchor/resources-status.json"
```

The anchor is not clean if any of these checks show:

- `environment-state check` not returning `ok=true` / `drift=[]`.
- stale OLDLAB or GB10 env/repo paths from an older `IMAGE_TAG`.
- `last_blocked_reason=release_state_drift` on any autoscaler policy.
- zero healthy slots for `oldlab`, `k8s-worker`, or `gb10-arm64`.
- GB10 source provenance missing, dirty, or mismatched with desired `GIT_SHA`.
- current service/control-plane/worker restarts that are not explained by the
  accepted rollout window.

## #190 Targeted Durability Gate

Run this before the canary and store the output in `03-190-targeted/`. This is
not the full/max-slot canary; it is the targeted durability proof that must
already pass before the canary begins. Pass smoke-gate tokens, MinIO
credentials, and explicit secret needles as `env:VAR`, `file:PATH`, or `-`
sources; do not expand raw secret values into argv.

```bash
uv run python scripts/staging_smoke_gate.py \
  --server-url "$PUBLIC_URL" \
  --team-a-token env:TEAM_A_TOKEN \
  --team-b-token env:TEAM_B_TOKEN \
  --catalog-minio-endpoint "$PUBLIC_BETA_MINIO_ENDPOINT" \
  --catalog-minio-access-key env:PUBLIC_BETA_MINIO_ACCESS_KEY \
  --catalog-minio-secret-key env:PUBLIC_BETA_MINIO_SECRET_KEY \
  --object-store-write-check-only \
  --object-store-write-check-bucket trajectories \
  --object-store-write-check-count 64 \
  --object-store-write-check-concurrency 16 \
  --fail-on-skip \
  --markdown-output "$CANARY_DIR/03-190-targeted/object-store-preflight.md" \
  --json-output "$CANARY_DIR/03-190-targeted/object-store-preflight.json"
```

The coordinator should also attach the #190 targeted batch/materialization
evidence in this directory, including batch id, final trial counts, sampled
worker logs, and the absence of repeated `task materialization timed out`,
`trajectory_flush_failed`, or object-store connection failures. If that
evidence does not exist or fails, stop here.

## Auth, Catalog, And Provider Preflight

Run these before canary submission and store them in `02-preflight/`. These
checks do not create trials.

```bash
curl -fsS \
  -H "Authorization: Bearer $TEAM_A_TOKEN" \
  "$PUBLIC_URL/api/v1/auth/whoami" \
  | tee "$CANARY_DIR/02-preflight/whoami-team-a.json"

curl -fsS \
  -H "Authorization: Bearer $TEAM_B_TOKEN" \
  "$PUBLIC_URL/api/v1/auth/whoami" \
  | tee "$CANARY_DIR/02-preflight/whoami-team-b.json"

curl -fsS \
  -H "Authorization: Bearer $TEAM_A_TOKEN" \
  "$PUBLIC_URL/api/v1/models" \
  | tee "$CANARY_DIR/02-preflight/provider-models.json"

uv run python scripts/benchmark_reward_gate.py readiness \
  --server-url "$PUBLIC_URL" \
  --token env:TEAM_A_TOKEN \
  | tee "$CANARY_DIR/02-preflight/catalog-readiness.txt"
```

Stop if either `whoami` response is not a non-admin user-owned API token, if
`provider-models.json` does not contain `$PROVIDER_MODEL_PROVIDER` /
`$PROVIDER_MODEL_NAME`, or if catalog readiness fails.

## Canary Submission Plan

The workload must be selected by the coordinator before GO. Preserve the task
filter as an artifact so later reviewers can recompute the expected count.

Use one of these shapes:

- Full100 release shape: SkillLearnBench full100, `n_per_task=2`, expected
  normal trials `200`, plus three #188 coverage trials.
- Theoretical max-slot shape: 28 coordinator-selected portable tasks,
  `n_per_task=7`, expected normal trials `196`, plus three #188 coverage
  trials. If the target is an exact aggregate-slot count, account for the
  three additional #188 coverage trials when choosing task count and
  `n_per_task`.

Create exactly one task-filter artifact.

Full100 shape:

```bash
printf '{"benchmark_id":"skilllearnbench"}\n' \
  > "$CANARY_DIR/04-submit/task-filter.json"
```

Theoretical max-slot shape:

```bash
cp "$COORDINATOR_APPROVED_28_TASK_FILTER_JSON" \
  "$CANARY_DIR/04-submit/task-filter.json"
```

For either shape, validate and store a pretty copy:

```bash
jq . "$CANARY_DIR/04-submit/task-filter.json" \
  | tee "$CANARY_DIR/04-submit/task-filter.pretty.json"
```

Do not synthesize the 28-task filter during execution. It must be the exact
coordinator-approved portable task list for the clean anchor.

Submit only after explicit GO:

```bash
export N_PER_TASK=<2-for-full100-or-7-for-28-task-max-slot>

loom eval batch create \
  --team-id "$AGENTIC_RL_TEAM_ID" \
  --name-suffix "$RUN_ID" \
  --task-filter @"$CANARY_DIR/04-submit/task-filter.json" \
  --provider "$PROVIDER_CONNECTION_NAME" \
  --model "$PROVIDER_MODEL_NAME" \
  --agent codex \
  --n-per-task "$N_PER_TASK" \
  --backend docker \
  --storage-preflight-evidence "$CANARY_DIR/01-clean-anchor/minio-storage-preflight-$IMAGE_TAG.json" \
  --required-worker-pool oldlab \
  --required-worker-pool k8s-worker \
  --required-worker-pool gb10-arm64 \
  | tee "$CANARY_DIR/04-submit/batch-create.txt"
```

Stop if the storage preflight artifact has `outcome="stop"`. Reclaim MinIO
space or provision backing storage before submission. Only use
`--override-storage-preflight-stop` after an explicit coordinator GO that is
recorded in `$CANARY_DIR/07-summary`; the override means the operator accepts
the object-store capacity risk for this run.

Immediately record the batch id and expected count:

```bash
export CANARY_BATCH_ID=<batch-uuid-from-create-output>

loom eval batch show "$CANARY_BATCH_ID" --format json \
  | tee "$CANARY_DIR/04-submit/batch-created.json"

jq '{id, state, expected_trial_count, required_worker_pools}' \
  "$CANARY_DIR/04-submit/batch-created.json" \
  | tee "$CANARY_DIR/04-submit/batch-created-summary.json"
```

Stop if `required_worker_pools` is not exactly
`["oldlab","k8s-worker","gb10-arm64"]` or if the expected count does not
include the three #188 coverage trials. During the watch loop, also stop if
batch debug or detail output records a `required_worker_pool_incompatible`
fanout error; the task slate is not valid release evidence for all required
pools.

## Watch Loop

Capture progress without mutating state. A five-minute interval is enough
unless the coordinator requests tighter sampling.

```bash
while true; do
  ts="$(date -u +%Y%m%dT%H%M%SZ)"

  loom eval batch show "$CANARY_BATCH_ID" --format json \
    > "$CANARY_DIR/05-watch/batch-$ts.json"

  loom eval batch debug "$CANARY_BATCH_ID" --format json \
    > "$CANARY_DIR/05-watch/batch-debug-$ts.json"

  loom resources status --json \
    > "$CANARY_DIR/05-watch/resources-$ts.json"

  loom admin worker-pools autoscaler status \
    --cp-url "$CP_URL" \
    --admin-token "file:$ADMIN_TOKEN_FILE" \
    --format json \
    > "$CANARY_DIR/05-watch/autoscalers-$ts.json"

  jq '{state, expected_trial_count, succeeded, failed, cancelled, queued, claimed, running}' \
    "$CANARY_DIR/05-watch/batch-$ts.json"

  sleep 300
done
```

The operator may stop the watch loop after the batch reaches a terminal state
or after a stop condition below is hit. Do not cancel the batch unless the
coordinator explicitly decides to cancel; cancellation is a separate mutating
operation and its transcript must be captured.

## Stop Conditions During Canary

Escalate and stop waiting for acceptance evidence if any condition appears.
Only cancel the batch after an explicit operator/coordinator decision.

- `loom-service`, `loom-control-plane`, `loom-worker`, Postgres, MinIO, or
  Gateway pods restart unexpectedly after the clean anchor. `SandboxChanged`,
  DNS resolution failures for `loom-postgres`, or worker failures resolving
  the control-plane service are #271 failures.
- Object-store or materialization signatures recur, including
  `task materialization timed out after 300s`, repeated S3 prefix/object
  download failures, `XMinioStorageFull`, `trajectory_flush_failed`, or ATIF
  projection failures. Treat these as #190 blockers, not benchmark scores.
- `runs.claimed_without_started` becomes nonzero in batch debug evidence or
  `scripts/staging_smoke_gate.py`. Treat this as #193 evidence requiring
  diagnosis/reclaim follow-up. If the row later becomes terminal with
  `retry_exhausted`, inspect the trial `failure_message` for the
  `claimed_without_started_reclaimed` diagnostic and prior worker id.
- Any required pool has no terminal trial after its #188 coverage trial has had
  enough time to be claimed and the other pools are already draining. Use batch
  debug worker-pool evidence before declaring this.
- `worker-pool autoscaler status` reports `release_state_drift`, stale jobs,
  disabled timers, missing env files, or old repo paths.
- The provider/model path fails before worker execution, `llm_evidence_status`
  is invalid for model-backed terminal trials, or all trials fail with the same
  platform-side setup reason.
- The smoke gate emits a secret leak, internal URL leak, signed object-store URL
  leak, or structured timeout/transport failure row.

## Terminal Evidence

After the batch reaches a terminal state, collect these artifacts:

```bash
loom eval batch show "$CANARY_BATCH_ID" --format json \
  | tee "$CANARY_DIR/06-terminal/batch-final.json"

loom eval batch debug "$CANARY_BATCH_ID" --format json \
  | tee "$CANARY_DIR/06-terminal/batch-debug-final.json"

loom eval diagnose batch "$CANARY_BATCH_ID" --format json \
  | tee "$CANARY_DIR/06-terminal/batch-diagnosis-final.json"

uv run python scripts/staging_smoke_gate.py \
  --server-url "$PUBLIC_URL" \
  --team-a-token env:TEAM_A_TOKEN \
  --team-b-token env:TEAM_B_TOKEN \
  --provider-connection-name "$PROVIDER_CONNECTION_NAME" \
  --provider-model-provider "$PROVIDER_MODEL_PROVIDER" \
  --provider-model-name "$PROVIDER_MODEL_NAME" \
  --batch-id "$CANARY_BATCH_ID" \
  --catalog-minio-endpoint "$PUBLIC_BETA_MINIO_ENDPOINT" \
  --catalog-minio-access-key env:PUBLIC_BETA_MINIO_ACCESS_KEY \
  --catalog-minio-secret-key env:PUBLIC_BETA_MINIO_SECRET_KEY \
  --object-store-write-check \
  --object-store-write-check-bucket trajectories \
  --object-store-write-check-count 64 \
  --object-store-write-check-concurrency 16 \
  --k8s-namespace "$K8S_NAMESPACE" \
  --required-worker-pool oldlab \
  --required-worker-pool k8s-worker \
  --required-worker-pool gb10-arm64 \
  --secret-needle env:PUBLIC_BETA_SECRET_NEEDLE \
  --internal-url-needle loom-minio.loom.svc.cluster.local \
  --fail-on-skip \
  --markdown-output "$CANARY_DIR/06-terminal/staging-smoke.md" \
  --json-output "$CANARY_DIR/06-terminal/staging-smoke.json"

uv run python scripts/benchmark_reward_gate.py readiness \
  --server-url "$PUBLIC_URL" \
  --token env:TEAM_A_TOKEN \
  | tee "$CANARY_DIR/06-terminal/benchmark-readiness.txt"

uv run python scripts/benchmark_reward_gate.py sweep \
  --server-url "$PUBLIC_URL" \
  --token env:TEAM_A_TOKEN \
  --batch-id "$CANARY_BATCH_ID" \
  | tee "$CANARY_DIR/06-terminal/benchmark-sweep.txt"
```

Acceptance requires:

- Batch terminal state is acceptable to the coordinator. `cancelled` or
  operator-stopped runs can still be useful debugging evidence but are not
  automatic #49 acceptance.
- `runs.claimed_without_started=0`.
- `runs.worker_pool_coverage` passes for all three pools.
- `batch-debug-final.json` has terminal worker-pool evidence for `oldlab`,
  `k8s-worker`, and `gb10-arm64`.
- Each architecture family has platform-successful terminal trials with
  persisted trajectory and artifact/ATIF evidence.
- The object-store write probe passes at the documented count/concurrency.
- `service.no_oom_restarts` passes.
- Benchmark reward gate passes or identifies only explicitly tracked,
  coordinator-accepted benchmark exclusions.

## Evidence Directory Contract

Keep evidence under the rollout root for the clean anchor:

```text
$CANARY_DIR/
  00-anchor/
    local-head.txt
    release-manifest-$IMAGE_TAG.json
  01-clean-anchor/
    cluster-status.json
    environment-state-apply.txt
    environment-state-check.txt
    slurm-workers-status.json
    gb10-workers-status.json
    worker-pool-autoscalers-status.json
    resources-status.json
  02-preflight/
    whoami-team-a.json
    whoami-team-b.json
    provider-models.json
    catalog-readiness.txt
  03-190-targeted/
    object-store-preflight.md
    object-store-preflight.json
    targeted-durability-summary.md
  04-submit/
    task-filter.json
    task-filter.pretty.json
    batch-create.txt
    batch-created.json
    batch-created-summary.json
  05-watch/
    batch-*.json
    batch-debug-*.json
    resources-*.json
    autoscalers-*.json
  06-terminal/
    batch-final.json
    batch-debug-final.json
    batch-diagnosis-final.json
    staging-smoke.md
    staging-smoke.json
    benchmark-readiness.txt
    benchmark-sweep.txt
  07-summary/
    release-comment.md
    issue-188-comment.md
    issue-190-comment.md
    issue-193-comment.md
    issue-271-comment.md
    issue-49-comment.md
```

Evidence files must not contain raw bearer tokens, provider keys, signed
object-store URLs, or internal service URLs beyond the documented redacted
needles used by the smoke gate. If a command emits sensitive values, redact the
copy before attaching it to GitHub and keep the raw file only in the protected
operator evidence directory.

## GitHub Bookkeeping After The Run

Prepare concise comments from `07-summary/` and post them to:

- #188: terminal required worker-pool coverage result.
- #190: object-store/materialization/trajectory durability result.
- #193: `claimed_without_started` and any reclaim/diagnostic findings.
- #271: rollout/startup stability and restart/DNS/Postgres evidence.
- #49 and #129: mixed architecture capacity/dispatch acceptance summary.
- #286: whether the clean anchor remained converged to the release manifest.

Do not close #188/#190/#193/#271/#49/#129 until their own acceptance criteria
are actually satisfied by the collected evidence.
