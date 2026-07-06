# Staging Launch Gate

This page is the release-owner checklist for Loom's staging. It
pulls together the deployment, onboarding, Run Library, security, and smoke
evidence needed before `dev` can be promoted to `main`.

## Launch Shape

- Loom uses no-email username/password accounts. Public users request a
  username for an existing team, an admin approves the request and manually
  shares a one-time setup link, and the same account works in browser and CLI.
- Team is the boundary for execution, provider credentials, cost attribution,
  members, and API tokens.
- Completed run metadata and safe artifacts are shared org-wide only through
  the Run Library. Normal batch, trial, trajectory, ATIF, artifact, provider,
  cancel, and rerun routes stay owner-team scoped unless the caller is a
  platform admin.
- Clone config and reuse artifact create destination-team records with
  provenance. They never copy source-team provider credentials.
- Quota and rate-limit enforcement are not launch blockers for this staging. Use
  cost alerts, team disable/pause controls, and provider-key rotation as
  operator responses until a separate product policy exists.

## Required Documentation

- Public deployment and private service boundary:
  [`docs/operator-runbook.md`](operator-runbook.md) and
  [`docs/architecture/cluster-deploy.md`](architecture/cluster-deploy.md).
- Browser and CLI onboarding:
  [`docs/user-guide.md#web-sessions-and-teams`](user-guide.md#web-sessions-and-teams)
  and
  [`docs/user-guide.md#public-server-cli-flow`](user-guide.md#public-server-cli-flow).
- Run Library and artifact reuse:
  [`docs/user-guide.md#run-library`](user-guide.md#run-library) and
  [`docs/architecture/run-library.md`](architecture/run-library.md).
- Security model:
  [`SECURITY.md`](../SECURITY.md),
  [`docs/architecture/auth-threat-model.md`](architecture/auth-threat-model.md),
  and
  [`docs/architecture/auth-registration-spec.md`](architecture/auth-registration-spec.md).
- Troubleshooting:
  [`docs/operator-runbook.md#alarm-response-troubleshooting-matrix`](operator-runbook.md#alarm-response-troubleshooting-matrix)
  plus the provider, sharing, and download checks in the staging smoke gate.
- Full/max-slot three-cluster canary preparation:
  [`docs/full-max-slot-canary-runbook.md`](full-max-slot-canary-runbook.md).

## Evidence Required

Attach these to the release issue or release PR:

- Release promotion gate workflow run from
  `.github/workflows/release-promotion-gate.yml`, plus its
  `release-gate-evidence` artifact. Production deploys require the same
  candidate SHA, image tag, and gate run id.
- Environment isolation transcript from
  `python scripts/validate_environment_isolation.py --profiles-dir deploy/environments --workflow .github/workflows/deploy-environment.yml`,
  plus the exact `deploy/environments/staging.cluster.toml` and
  `deploy/environments/production.cluster.toml` inputs used for the candidate.
- `loom cluster audit` output showing TLS ingress, only `/` and `/api/v1`
  public backends or the canonical `/prod`/`/dev` prefixed equivalents, no
  public LLM Gateway, no public Control Plane, and no public object store.
- For first prod, `python scripts/ops/frontend_route_smoke.py --route ...`
  output proving `https://yylx.world/prod` exposes production identity and
  `https://yylx.world/prod/api`, while `https://yylx.world/dev` exposes
  staging identity and `https://yylx.world/dev/api`, with
  no-store runtime config responses.
- For first prod, the release-promotion manifest's `prod_staging_isolation` check
  must embed structured dry-run evidence for state profiles, object storage
  buckets/prefix policy, safe token/provider refs, frontend API bases, worker
  API URLs, worker image/source identities, and staging lease status. The
  production gate fails active staging leases unless `staging_slots=0` or a documented
  override with an approval URL is present. Safe refs such as
  `github-environment:production/...` are expected; raw token/provider/MinIO
  values are not.
- Screenshots or notes for logged-out SPA load, account request, admin account
  approval link, password setup, forgot-password request/reset approval, Team
  Settings, provider setup, SPA batch submission, Monitor progress showing
  `username / team`, and Run Library My team / All teams views.
- CLI transcript for `loom auth login`, `loom auth whoami`,
  `loom providers test`, `loom providers models`, `loom eval batch create`,
  `loom eval batch show`, `loom eval trial show`, and
  `loom eval trial download`.
- Benchmark catalog provisioning transcript showing either
  `loom datasets provision-catalog` with non-zero
  `ready_agents`, non-zero `ready_benchmarks`, non-zero `ready_tasks`, and
  `missing=0`, or
  `loom datasets register <benchmark>` against the published HF manifest with
  `--mirror-to-object-store`, non-zero `registered`, non-zero `mirrored`, zero
  unexpected `legacy_placeholders`, and `loom datasets audit --verify-bundles`
  showing `missing=0`. The protected rollout smoke catalog must also include
  the checked-in GB10 smoke fixture published through
  `loom datasets publish-local deploy/catalog/gb10-smoke`, producing task
  `loom-smoke/gb10-oracle-hello-world` with an internal `s3://` source. Include
  `/api/v1/agents` evidence with at least one ready entry and
  `/api/v1/benchmarks` evidence with at least one runnable entry. For private or
  gated HF manifests, confirm the operator
  context has `HF_TOKEN` and target `LOOM_SVC_MINIO_*` credentials. Workers may
  still receive optional `loom-secrets/huggingface-api-key` as legacy `hf://`
  compatibility, but mirrored release evidence should not depend on worker
  direct HF fetches.
- SkillLearnBench HF mirror/token-boundary evidence JSON saved as
  `$ROLLOUT_DIR/hf-mirror-boundary-evidence-$IMAGE_TAG.json`. It must be
  secret-safe and prove `benchmark_id=skilllearnbench`, runnable rows,
  `requires_caps.cpu_arch=any`, all sampled runtime sources are internal
  `s3://` bundle prefixes, HF upstream kind/locator/revision are retained, the
  service canary reached `started` and a terminal state from the internal
  source, `worker_boundary.hf_token_present=false`, and
  `direct_hf_egress_required=false`. Do not paste raw HF/API/MinIO values into
  the artifact; use paths, counts, prefixes, and redacted references only.
- Environment desired-state transcript showing
  `loom admin environment-state apply` and
  `loom admin environment-state check` against
  `deploy/environment-state/staging.toml` or
  `deploy/environment-state/staging.toml`, with the rollout `IMAGE_TAG` and
  `ENV_CONFIG_VERSION` variables supplied. This must converge worker-pool
  autoscaler policies, GB10 desired state, and any external Slurm autoscaler
  supervisor before Monitor/resource-pool screenshots are used as evidence. For
  staging, the OLDLAB supervisor unit must point at the current rollout
  checkout, include `--pool-name oldlab`, have an executable `ExecStart`
  Python path and existing autoscaler script, have no recent failed service
  result such as `status=203/EXEC`, and have its user timer active. The
  staging profile targets the existing CP desired-state environment
  `production` until GB10 node agents are renamed in a coordinated rollout.
- Benchmark reward acceptance transcript from
  `scripts/benchmark_reward_gate.py`: the readiness gate must pass against the
  user-visible catalog, and the supported-benchmark sweep gate must prove every
  v1.0-supported benchmark has numeric-reward coverage for every currently
  runnable task. A model answer scored `0` is a valid evaluator result; missing
  reward, verifier error, task-image failure, benchmark-side timeout, or missing
  allowlist coverage is not.
- Score-positive canary transcript from
  `scripts/benchmark_reward_gate.py score-positive-canary` before any full
  production benchmark batch that changes the agent, provider, model, runtime,
  worker pool, or task-family mix. This gate is separate from platform success:
  it fails when all scored canary trials have reward `0` or when no trial was
  scored. Preserve both JSON and Markdown evidence with task ids, reward
  distribution, unscored-trial taxonomy, and baseline agent/model/provider
  fields. Proceeding despite a failing gate requires an explicit operator
  override with an issue or PR reference plus rationale; otherwise do not submit
  the full production batch.
- Layer 1 score-credibility transcript from
  `scripts/benchmark_score_alignment_gate.py manifest --manifest
  docs/benchmark-score-alignment.json`, proving that every v1.0-supported
  benchmark has a canonical scoring reference, score semantics, Harbor/upstream
  parity decision, and at least one same-output replay case definition.
- If a remote-worker pool is attached, private tunnel evidence from
  `scripts/ops/worker_service_tunnels.py watchdog-evidence`, `check`, and
  `check-remote` showing the Control Plane, Gateway, optional subprocess
  Gateway facade, and MinIO worker-facing URLs are healthy from the control
  node and from at least one worker-host context. The watchdog evidence must
  show the active timer state, the durable script path, and the resolved
  env-file path without printing env-file contents.
- If OLDLAB elastic workers are enabled, `worker_capacity_smoke` release-gate
  evidence must include the smoke batch id, runtime, failure count, and one
  `oldlab_worker_records` entry per OLDLAB worker with node name, Slurm job id,
  Loom worker id, configured concurrency, and claimed trial count.
- For first prod, `worker_capacity_smoke` / prod-staging isolation evidence must
  also attach the secret-safe output from
  `uv run python scripts/ops/worker_capacity_manifest.py --manifest
  deploy/worker-capacity/prod-first.toml`. The report must show production as
  the default owner of all eligible GB10/OLDLAB slots, staging/dev at zero slots
  unless an explicit bounded borrow is active, and no worker identity, API URL,
  image tag, source commit, compose service, or Kubernetes deployment crossing
  the prod/dev boundary. If a bounded staging lease is active, the evidence must
  include the prod-pressure counts used by `status`; nonzero prod pressure must
  produce `prod_pressure.cause=prod_capacity_pressure`,
  `new_staging_claims_allowed=false`, idle staging slots returned to prod, and running
  staging slots reported as draining rather than as staging rollout failure.
- `scripts/staging_smoke_gate.py` Markdown evidence with `--fail-on-skip`
  and `--allow-mutating-checks` against disposable staging data. The report
  must include the final service restart/OOM row by passing `--k8s-namespace`;
  that row is evaluated after HTTP/API route probes and fails if the service
  restart count increased during the smoke or the final pod state reports
  `OOMKilled`. The `auth.team_a_whoami` / `auth.team_b_whoami` rows must show
  non-admin user-owned API tokens. Do not use platform-admin tokens,
  admin-minted legacy team tokens, or manual SQL token insertion for cross-team
  release evidence.
  When `--batch-id` is provided, the `runs.claimed_without_started` row must
  be `PASS`; a nonzero value means the source run still has orphaned claimed
  work and cannot be used as release evidence. For terminal failures that came
  from a reclaimed pre-start claim, inspect the trial `failure_message` for the
  `claimed_without_started_reclaimed` diagnostic with the prior worker id and
  claim timing.
- For GB10/opencode acceptance batches, preserve debug evidence for every
  timeout or reclaimed trial. A worker hard deadline or CP stale-running reclaim
  must appear as `state=failed`, `failure_reason=agent_timeout`, not generic
  `cancelled`. The debug evidence must show `activity.last_trial_event`,
  `activity.last_llm_call_at`, `worker.heartbeat_age_sec`,
  `agent.timeout.agent_timeout_sec`, and `stale_running.reason` so operators can
  distinguish a silent live worker from a dead-worker retry or explicit
  operator cancellation.
- For IP-address staging hosts, note the hostless Ingress rendering, attach
  evidence that the TLS Secret certificate includes the staging IP as a Subject
  Alternative Name, and verify the ingress controller serves that Secret as its
  default certificate.
- Leak-scan note showing seeded fake secrets and internal service URLs were not
  found in API responses, audit excerpts, or downloaded safe artifacts.
- For the final #49/#129 full/max-slot three-cluster canary, use
  [`docs/full-max-slot-canary-runbook.md`](full-max-slot-canary-runbook.md).
  The canary must wait for a clean staging anchor, completed #190 targeted
  durability validation, and an explicit coordinator `GO`. The batch must use
  repeated `--required-worker-pool` flags for `oldlab` and
  `gb10-arm64`; terminal evidence must include `runs.worker_pool_coverage`
  and `runs.claimed_without_started` from the smoke gate. Staging
  clusters run with `k8s_worker.enabled=false` (#383) so x86_64
  coverage is delivered by the Slurm-managed `oldlab` pool, not by an
  in-cluster `k8s-worker` Deployment.

Generate a rollout release manifest before the protected apply. It is the
machine-readable expected-state anchor for image/render convergence, DB revision
checks, and per-component release evidence:

```bash
loom cluster release-manifest \
  --config "$CLUSTER_CONFIG" \
  --environment staging \
  --image-tag "$IMAGE_TAG" \
  --git-sha "$(git rev-parse HEAD)" \
  --environment-state-file deploy/environment-state/staging.toml \
  --env-config-version "${ENV_CONFIG_VERSION:-$IMAGE_TAG}" \
  --expected-image-identities-json "$ROLLOUT_DIR/image-identities-$IMAGE_TAG.json" \
  --output "$ROLLOUT_DIR/release-manifest-$IMAGE_TAG.json"
```

The image identities JSON is build evidence keyed by Deployment and container,
with `image`, and at least one immutable `repo_digest` or `image_id` per
release-managed container. Before the first release-managed mutation, choose a
shared lock directory that every staging operator on the host uses:

```bash
export LOOM_ROLLOUT_LOCK_DIR=/data/loom-staging/rollout-locks

loom cluster up \
  --config "$CLUSTER_CONFIG" \
  --namespace "$K8S_NAMESPACE" \
  --environment staging \
  --rollout-id "$IMAGE_TAG" \
  --rollout-lock-dir "$LOOM_ROLLOUT_LOCK_DIR" \
  --rollout-lock-evidence "$ROLLOUT_DIR/rollout-mutation-lock-$IMAGE_TAG.json" \
  --recover-sandbox-deadlines \
  --sandbox-deadline-max-pods 4 \
  --timeout 900
```

The lease covers `loom cluster up` for Kubernetes release-managed components and
`loom admin environment-state apply/check` for the external worker desired-state
gate in this first slice. It does not stop manual `kubectl`, Slurm, or GB10 host
commands that bypass Loom tooling; those remain detected by release-gate and
environment-state convergence checks. A second Loom-protected mutation for the
same environment fails with the active owner id and expiry. Use
`--force-rollout-lock` only after preserving evidence that the recorded owner is
stale, such as a dead terminal/session, no active rollout process for the owner
PID/host, and no in-flight rollout issue comment claiming ownership. The force
flag replaces an abandoned durable record; it does not bypass an active process
that still holds the advisory lock.

The sandbox-deadline recovery flags are intentionally narrow. They only act
after the protected preflight/apply path, only when pod events classify the
stall as `node_runtime_sandbox_deadline` (`FailedCreatePodSandBox` or
`FailedKillPod` with `context deadline exceeded`), and only delete the capped
set of classified pods before one readiness retry. If that retry fails, keep
the evidence and inspect kind/containerd/kubelet instead of deleting unrelated
pods or skipping the storage/backup guards.

When staging uses `static-host-path` storage with `k8s_worker.enabled=false`,
the render still includes `persistentvolumeclaim/loom-worker-trajectories`.
The disabled-worker profile removes only in-cluster worker compute/network
resources; the retained trajectory PVC remains part of the protected storage
boundary checked before rollout mutation.

After `loom cluster up` reaches readiness, run the hard convergence gate against
the same saved inputs:

```bash
loom cluster release-gate \
  --manifest "$ROLLOUT_DIR/release-manifest-$IMAGE_TAG.json" \
  --config "$CLUSTER_CONFIG" \
  --rendered-manifest "$ROLLOUT_DIR/rendered.yaml" \
  --environment-state-check "$ROLLOUT_DIR/environment-state-check-$IMAGE_TAG.json" \
  --hf-mirror-boundary-evidence "$ROLLOUT_DIR/hf-mirror-boundary-evidence-$IMAGE_TAG.json" \
  --namespace "$K8S_NAMESPACE" \
  --environment staging \
  --format json \
  > "$ROLLOUT_DIR/release-gate-$IMAGE_TAG.json"

loom cluster release-gate \
  --manifest "$ROLLOUT_DIR/release-manifest-$IMAGE_TAG.json" \
  --config "$CLUSTER_CONFIG" \
  --rendered-manifest "$ROLLOUT_DIR/rendered.yaml" \
  --environment-state-check "$ROLLOUT_DIR/environment-state-check-$IMAGE_TAG.json" \
  --hf-mirror-boundary-evidence "$ROLLOUT_DIR/hf-mirror-boundary-evidence-$IMAGE_TAG.json" \
  --namespace "$K8S_NAMESPACE" \
  --environment staging \
  --format markdown \
  > "$ROLLOUT_DIR/release-gate-$IMAGE_TAG.md"
```

This gate fails on rendered/config hash drift, unverifiable managed image
identity convergence, classified node-runtime sandbox deadline stalls, live DB
Alembic revision mismatch, and missing or failed environment-state convergence
evidence when the release manifest records external-worker desired state.
Generate the environment-state artifact with
`loom admin environment-state check --format json` after the matching
`environment-state apply`; `ok=false` or any non-empty `drift` array keeps the
release-gate artifact red and blocks workload-validation anchors. When the
manifest records GB10 desired state, also pass
`loom admin gb10-workers status --format json` via
`--gb10-workers-status`; stale, missing, unreachable, non-applied, dirty, or
capacity-mismatched active GB10 nodes keep the release gate red. For staging
and production, pass `--hf-mirror-boundary-evidence`; missing evidence,
non-S3 SkillLearnBench runtime sources, missing HF provenance, worker
`HF_TOKEN` presence, direct worker HF egress dependence, or raw secret-looking
values keep the release gate red. For normal runtime image IDs, it compares the
Ready pod `imageID` against the manifest digest or image ID. For kind-loaded
staging images, Kubernetes may report
`docker.io/library/import-YYYY-MM-DD@sha256:...`; in that case the gate accepts
the target-generation pod only when its pod spec and Deployment template image
match the release manifest, and records the kind-import identity plus any stale
`containerStatuses.image` tag as evidence. Managed Deployments intentionally
scaled to zero pass on Deployment-template image convergence and record
zero-replica evidence instead of requiring a Ready pod. The DB probe runs inside
`deploy/loom-control-plane` and reports `env:LOOM_CP_DB_URL` without printing
the underlying connection string.

The JSON release-gate artifact includes `component_evidence`, a concise
machine-readable row set for each release-managed Kubernetes Deployment
container and each external worker surface recorded by the environment-state
manifest section. Each row records expected image/profile, live image or check
artifact, generation or job id when available, readiness/convergence state,
restart or crash detail when the gate observed one, supporting evidence, and
pass/fail. The Markdown form is the pasteable issue-comment table for the same
rows; attach both artifacts to the rollout directory when recording #286/#294
evidence.

Keep this artifact free of raw bearer tokens, provider keys, signed
object-store URLs, internal service URLs, and secret refs; the production
release gate rejects those patterns. The deploy workflow writes the same
artifact, plus `rendered.yaml`, into `rollout-evidence/` before `loom cluster
up` and uploads the directory as a workflow artifact. To have the deploy helper
run this hard gate automatically, set `LOOM_RELEASE_GATE_HARD_CHECKS=true` and
point `LOOM_EXPECTED_IMAGE_IDENTITIES_JSON` at the image identity JSON file.

## Automated Gate

Secret-bearing smoke-gate inputs (`--team-a-token`, `--team-b-token`,
`--catalog-minio-access-key`, `--catalog-minio-secret-key`, and
`--secret-needle`) accept `env:VAR`, `file:PATH`, or `-` sources. Do not expand
raw secret values into argv.

Before submitting staging canaries or supported-benchmark release trials,
run the object-store write gate by itself:

```bash
python scripts/staging_smoke_gate.py \
  --server-url https://loom.example.com \
  --team-a-token env:TEAM_A_TOKEN \
  --team-b-token env:TEAM_B_TOKEN \
  --catalog-minio-endpoint "$STAGING_MINIO_ENDPOINT" \
  --catalog-minio-access-key env:STAGING_MINIO_ACCESS_KEY \
  --catalog-minio-secret-key env:STAGING_MINIO_SECRET_KEY \
  --object-store-write-check-only \
  --object-store-write-check-bucket trajectories \
  --object-store-write-check-count 64 \
  --object-store-write-check-concurrency 16 \
  --fail-on-skip \
  --markdown-output staging-object-store-preflight.md \
  --json-output staging-object-store-preflight.json
```

The preflight must show `object_store.minio_write_probe` as `PASS` before any
trial execution starts. For full100 or remote-worker acceptance, keep the probe
at a nontrivial count/concurrency so the gate exercises object-store connection
pooling before workers start artifact and trajectory uploads.

After browser setup and a completed Team A source run, run:

```bash
python scripts/staging_smoke_gate.py \
  --server-url https://loom.example.com \
  --team-a-token env:TEAM_A_TOKEN \
  --team-b-token env:TEAM_B_TOKEN \
  --provider-connection-name mz_tn_canada_qianyi \
  --provider-model-provider yibuapi \
  --provider-model-name glm5.1-thinking \
  --batch-id "$TEAM_A_BATCH_ID" \
  --trial-id "$TEAM_A_TRIAL_ID" \
  --safe-artifact-key "$SAFE_ARTIFACT_KEY" \
  --blocked-artifact-key "$BLOCKED_ARTIFACT_KEY" \
  --private-trial-id "$PRIVATE_TRIAL_ID" \
  --private-artifact-key "$PRIVATE_ARTIFACT_KEY" \
  --clone-provider-connection-id "$TEAM_B_PROVIDER_CONNECTION_ID" \
  --reuse-provider-connection-id "$TEAM_B_PROVIDER_CONNECTION_ID" \
  --catalog-minio-endpoint "$STAGING_MINIO_ENDPOINT" \
  --catalog-minio-access-key env:STAGING_MINIO_ACCESS_KEY \
  --catalog-minio-secret-key env:STAGING_MINIO_SECRET_KEY \
  --object-store-write-check \
  --object-store-write-check-bucket trajectories \
  --object-store-write-check-count 64 \
  --object-store-write-check-concurrency 16 \
  --k8s-namespace loom-staging \
  --required-worker-pool gb10-arm64 \
  --secret-needle env:STAGING_SECRET_NEEDLE \
  --internal-url-needle loom-minio.loom.svc.cluster.local \
  --allow-mutating-checks \
  --fail-on-skip \
  --markdown-output staging-smoke.md \
  --json-output staging-smoke.json
```

The script checks:

- public health and logged-out SPA reachability;
- Team A and Team B non-admin user-owned API-token auth;
- provider connection and model-discovery surfaces;
- runnable benchmark catalog presence;
- sampled ready benchmark task bundle prefixes in object storage;
- a concurrent MinIO write/delete probe against the runtime trajectory bucket,
  so `XMinioStorageFull`, connection-pool pressure, and other object-store
  write failures are caught before submitting canary or release-trial work;
- `loom-service` pod restart/OOM status when `--k8s-namespace` is provided,
  with a baseline before route probes and a final verdict after route probes;
- terminal trial coverage for required worker pools such as `oldlab` when
  `--required-worker-pool` is provided;
- batch/trial detail and service-proxied ATIF/trajectory downloads;
- trial and batch debug evidence containing last event/LLM activity, worker
  heartbeat freshness, runtime, agent timeout config, and stale-running
  keep/reclaim diagnostics;
- Run Library My team and All teams visibility;
- owner-team label;
- cross-team safe artifact download through Run Library;
- direct owner-team artifact route denial;
- clone config, reuse artifact, and provenance;
- blocked artifact denial;
- private artifact denial;
- cross-team mutation denial;
- structured API timeout and transport-exception failures that still write
  Markdown/JSON evidence;
- seeded fake secret, token-pattern, signed-URL, and internal-URL leaks.

The script intentionally redacts raw tokens, provider-key-like values, seeded
fake secrets, signed object-store URLs, and internal service URLs from its
Markdown and JSON output. A timed-out API endpoint should produce a failed row
with the HTTP method, endpoint, and timeout instead of a Python traceback.

Before submitting a staging canary or supported-benchmark acceptance run,
the `object_store.minio_write_probe` row must pass. If it fails with
`XMinioStorageFull`, reclaim or provision storage for the MinIO-backed
filesystem first; do not start the trial and wait for worker artifact upload to
discover the same failure later. For release/full100 gates, do not reduce the
object count/concurrency below the documented values without recording why the
environment cannot sustain the probe.

For mixed-architecture full100 gates, warm task images before the large batch.
Run a small architecture-targeted canary on every required worker pool, confirm
the shared trial-cache registry or worker-local cache is reused, and treat any
`task_image_build_timeout` / `building Docker image ... exceeded ...s` failure
as a platform setup blocker rather than model-quality evidence.
For Terminus 2 task bundles that inherit from `mictern2/terminus2-full:latest`,
the first GB10/ARM64 canary also creates a worker-local compatibility base
image on the ARM64 Docker daemon before building the task image, because the
upstream tag is amd64-only.
Do not rely on theoretical max-slot saturation to prove worker-pool coverage.
For v1.0's GB10-only staging gate, require `--required-worker-pool gb10-arm64`.
For v1.1/full-cluster mixed-pool evidence, create the release/acceptance batch
with repeated `--required-worker-pool` flags, for example
`--required-worker-pool oldlab --required-worker-pool gb10-arm64`. On clusters
that intentionally host a dedicated k8s worker node pool, add
`--required-worker-pool k8s-worker` as well. The service adds one
pool-pinned coverage trial per required pool while leaving the normal portable
trials portable. When a target pool's CPU architecture is known from active
workers or autoscaler policy, coverage uses a selected task compatible with that
architecture; otherwise fanout records `required_worker_pool_incompatible`
instead of submitting a permanently unclaimable coverage trial. The
`scripts/staging_smoke_gate.py --required-worker-pool` check must later
prove each required pool has terminal batch evidence.

Run the benchmark reward gate after catalog provisioning and again after the
supported-benchmark acceptance batch or batches reach a terminal state:

```bash
python scripts/benchmark_reward_gate.py readiness \
  --server-url https://loom.example.com \
  --token env:TEAM_A_TOKEN

python scripts/benchmark_reward_gate.py sweep \
  --server-url https://loom.example.com \
  --token env:TEAM_A_TOKEN \
  --batch-id "$SUPPORTED_BENCHMARK_ACCEPTANCE_BATCH_ID"
```

Before a full production benchmark batch, first run a small canary with the
same agent, provider/model, runtime, worker-pool, and representative task-family
mix. A canary that only proves provider calls and platform-succeeded trials is
not enough. The score-positive canary gate is mandatory and must show at least
one scored trial with reward greater than `0`:

```bash
python scripts/benchmark_reward_gate.py score-positive-canary \
  --server-url https://loom.example.com \
  --token env:TEAM_A_TOKEN \
  --batch-id "$CANARY_BATCH_ID" \
  --json-output "$EVIDENCE_DIR/score-positive-canary.json" \
  --markdown-output "$EVIDENCE_DIR/score-positive-canary.md"
```

If the command exits nonzero, do not submit the full production batch. The
report records task ids, reward distribution, unscored-trial taxonomy, and
baseline batch fields so the operator can decide whether to fix the
agent/provider/runtime configuration or choose a different accepted path.
Operator override is intentionally explicit and auditable:

```bash
python scripts/benchmark_reward_gate.py score-positive-canary \
  --server-url https://loom.example.com \
  --token env:TEAM_A_TOKEN \
  --batch-id "$CANARY_BATCH_ID" \
  --json-output "$EVIDENCE_DIR/score-positive-canary.json" \
  --markdown-output "$EVIDENCE_DIR/score-positive-canary.md" \
  --override-issue "#445" \
  --override-rationale "Coordinator accepted an alternate score-positive path; keep the failed agent/provider issue open."
```

Use override only when the referenced issue or PR comment records the operator
decision and rationale. Without both fields, the CLI refuses to override.

The readiness command uses the same full benchmark surface as New Batch,
including `include_empty=true` pending rows. It must fail while required
user-facing benchmarks are still `Needs publish` / `Needs republish`; the
release answer is to publish and prove them, not to hide them. Rows marked
`Not supported yet` with `blocker_reason="unsupported_runtime"` are explicit
current-scope exclusions: they stay visible to users, remain unselectable, and
are skipped by this readiness gate until their runtime follow-up is delivered.
Rows marked `Deferred` with `blocker_reason="deferred_support"` use the same
gate behavior for benchmarks that need a tracked product or data-access
follow-up before they can be supported.
Rows marked `Not in v1.0` with `blocker_reason="not_v1_supported"` are built-in
catalog benchmarks outside the current allowlist; they stay visible,
unselectable, excluded from supported task counts, and skipped by this
readiness gate until a support issue promotes them into scope.

The sweep command defaults to the v1.0 benchmark allowlist and calls
`/api/v1/tasks/count` for each benchmark. It groups all trials from the supplied
batch ids by benchmark and distinct task id, then requires every runnable task
to have a succeeded trial with a numeric aggregate reward. Repeat `--batch-id`
for one-batch-per-benchmark validation or task-level reruns; a later numeric
reward for the same task covers provider/agent/platform transient failures from
an earlier batch, while benchmark-side verifier/environment failures still fail
the sweep. Pass `--expected-benchmark` to narrow an intermediate diagnostic
run.

## Remote Worker Tunnel Gate

If the staging uses extra remote workers outside the Kubernetes cluster,
run this private gate after every rollout and before load testing:

```bash
scripts/ops/worker_service_tunnels.py watchdog-evidence \
  --expected-script-path "$PWD/scripts/ops/worker_service_tunnels.py" \
  | tee staging-watchdog-evidence.json

export REMOTE_WORKER_ENV_FILE="$(
  jq -r '.env_file.path' staging-watchdog-evidence.json
)"

scripts/ops/worker_service_tunnels.py check \
  --env-file "$REMOTE_WORKER_ENV_FILE"

scripts/ops/worker_service_tunnels.py check-remote worker-hosts.txt \
  --env-file "$REMOTE_WORKER_ENV_FILE"
```

For Slurm-only worker hosts, use the same generated check script inside each
worker allocation:

```bash
scripts/ops/worker_service_tunnels.py print-check-script \
  --env-file .env.remote-worker \
  | srun --jobid "$REMOTE_WORKER_JOB_ID" --overlap --ntasks=1 bash -s
```

The env file supplies the same worker-facing URLs used by remote workers:

```bash
LOOM_WORKER_CONTROL_PLANE_URL=http://control-node.lan:18081
LOOM_WORKER_GATEWAY_URL=http://control-node.lan:19100
LOOM_WORKER_MINIO_ENDPOINT=http://control-node.lan:19000
# Optional when Docker sandboxes use a different host-gateway facade URL.
# LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:30444/openai/v1
```

The gate exits non-zero if any required tunnel is down. If
`LOOM_WORKER_SUBPROCESS_GATEWAY_URL` is set, the gate also prints and probes
`subprocess-gateway`; `host.docker.internal` is checked through the equivalent
host-side loopback URL. This gate is separate from the public API smoke test:
`https://loom.example.com` can be healthy while the
remote-worker pool is detached.

When the subprocess gateway URL uses a different local port than
`LOOM_WORKER_GATEWAY_URL`, install that port as a managed tunnel instead of an
ad-hoc `kubectl port-forward`:

```bash
scripts/ops/worker_service_tunnels.py install-systemd \
  --namespace loom-staging \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/staging.kubeconfig \
  --subprocess-gateway-local-port 30444
```

For GB10 and OLDLAB staging rollouts, gate the Slurm-managed capacity first
and then gate node-agent convergence only for compose rollout compatibility.
Run `environment-state check` from the Slurm submit/shared-storage host so it
can validate external runner env files, shared worker checkouts, and local
systemd user timers in addition to CP-backed state. Pass the active worker
token through `--worker-token env:...` or `file:...`; the gate emits only
redacted sha256-prefix fingerprints. The Slurm check catches pending/stale
capacity requests, active jobs launched from stale `LOOM_REMOTE_WORKER_*`
paths, stale remote env worker-token fingerprints, inactive OLDLAB autoscaler
timers, unscoped external autoscaler commands that omit `--pool-name oldlab`,
missing or unexecutable external autoscaler `ExecStart` command paths, recent
failed autoscaler service results such as `status=203/EXEC`, and the active
`gb10-arm64`/`oldlab` pool shapes; the node-agent check catches stale
host-local checkouts, local-build fallback using an old tree, and env files
that did not apply even when the pool still has healthy heartbeats.
The external Slurm autoscaler also treats release-state drift as a hard blocked
decision: stale pending/running jobs are not counted as healthy warm capacity,
and `loom admin worker-pools autoscaler status` reports
`last_blocked_reason=release_state_drift` with the affected Slurm job ids in
`last_error`. Do not submit staging canaries until those jobs are replaced
or cancelled and `environment-state check` is clean.
If OLDLAB resource-aware scale-up has no safe allowed node, status reports
`last_blocked_reason=no_safe_slurm_nodes` and
`last_blocked_details.node_exclusions` with per-node reasons such as
`insufficient_memory`, `cpu_load_high`, `unsafe_state`, `active_loom_job`, or
`missing_resource_snapshot`; fix the capacity condition or adjust the allowed
node/resource policy before treating OLDLAB as a validation pool.
`environment-state check --format json` includes these hard blockers under
`autoscaler_blockers`, and `loom cluster release-gate` fails the
environment-state convergence row while the blocker is active.

```bash
LIVE_ADMIN_TOKEN_FINGERPRINT="$(
  kubectl -n loom-staging get secret loom-admin-secret \
    -o jsonpath='{.data.secrets\.toml}' \
    | base64 -d \
    | python3 -c 'import hashlib,sys,tomllib; token=tomllib.loads(sys.stdin.read())["admin"]["token"]; print(f"sha256:{hashlib.sha256(token.encode()).hexdigest()[:12]} len={len(token)}")'
)"

loom admin environment-state apply \
  --cp-url http://control-node.lan:18081 \
  --admin-token file:/secure/path/admin-token \
  --expect-admin-token-fingerprint "$LIVE_ADMIN_TOKEN_FINGERPRINT" \
  --environment staging \
  --file deploy/environment-state/staging.toml \
  --var IMAGE_TAG="$IMAGE_TAG" \
  --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}" \
  --var GIT_SHA="$RELEASE_SHA" \
  --rollout-id "$IMAGE_TAG" \
  --rollout-lock-dir "$LOOM_ROLLOUT_LOCK_DIR" \
  --rollout-lock-evidence "$ROLLOUT_DIR/environment-state-apply-lock-$IMAGE_TAG.json"

loom admin environment-state check \
  --cp-url http://control-node.lan:18081 \
  --admin-token file:/secure/path/admin-token \
  --expect-admin-token-fingerprint "$LIVE_ADMIN_TOKEN_FINGERPRINT" \
  --environment staging \
  --file deploy/environment-state/staging.toml \
  --var IMAGE_TAG="$IMAGE_TAG" \
  --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}" \
  --var GIT_SHA="$RELEASE_SHA" \
  --worker-token file:/secure/path/worker-token \
  --rollout-id "$IMAGE_TAG" \
  --rollout-lock-dir "$LOOM_ROLLOUT_LOCK_DIR" \
  --rollout-lock-evidence "$ROLLOUT_DIR/environment-state-check-lock-$IMAGE_TAG.json" \
  --format json \
  > "$ROLLOUT_DIR/environment-state-check-$IMAGE_TAG.json"

loom admin slurm-workers status \
  --cp-url http://control-node.lan:18081 \
  --admin-token file:/secure/path/admin-token

loom admin gb10-workers status \
  --cp-url http://control-node.lan:18081 \
  --admin-token file:/secure/path/admin-token \
  --environment production \
  --pool-name gb10-arm64 \
  --release-image-tag "$IMAGE_TAG" \
  --release-env-config-version "$ENV_CONFIG_VERSION" \
  --format json \
  > "$ROLLOUT_DIR/gb10-workers-status-$IMAGE_TAG.json"
```

For GB10 node-agent compatibility workers, `gb10-workers status` proves the
non-secret desired image/env-config state and source-checkout provenance. The
profile writes `source_git_commit` from `GIT_SHA`; active nodes must report a
clean git checkout at that commit. Missing provenance, a stale
`compose_project_dir`, a dirty checkout, or a source commit that differs from
desired state is a hard failure even when image/env fields are current. During
worker-token rotation, also run
`loom worker gb10-agent plan/apply --worker-token file:/...` on each GB10 host,
then verify `loom resources status --json` shows fresh `gb10-arm64` active
workers and the Control Plane logs no new `/workers/register` 401s. The
`--worker-token` value is read locally on the host and is never stored in the
Control Plane desired state.

For OLDLAB 1-5, use the committed staged files in
`deploy/worker-pools/oldlab/` as the source of truth for included nodes,
requested Slurm slice, and controller env overrides. OLDLAB 4/5 must not be
counted in production capacity until a real Loom batch records worker
registration, heartbeat, claim/finalize, and artifact evidence from those
nodes.

## Release Decision

The staging launch gate passes only when:

- every required manual evidence item is attached;
- any attached remote-worker pool has passing private tunnel checks from both
  the control node and a worker-host context;
- the ready benchmark catalog has been provisioned with `missing=0`;
- `scripts/staging_smoke_gate.py` exits 0 with `--fail-on-skip`;
- the smoke report's `runs.claimed_without_started` row is `PASS` for the
  source batch used as launch evidence;
- the smoke report's final `service.no_oom_restarts` row is `PASS` after all
  HTTP/API route probes for the deployed `loom-service` pods;
- the smoke report's `runs.worker_pool_coverage` row is `PASS` for any
  release-required worker pool such as `oldlab`; create the source batch with
  matching `loom eval batch create --required-worker-pool ...` flags instead
  of depending on max-slot pressure to assign work to the pool;
- any GB10/opencode timeout or reclaim evidence is explicit
  `agent_timeout` with stale-running/debug-evidence fields, and no batch is left
  indefinitely `running` because a child trial exceeded its watchdog deadline;
- no response, audit excerpt, log excerpt, or safe downloaded artifact contains
  seeded fake secrets or internal URLs;
- unsafe artifacts are blocked and cannot be downloaded by another team;
- clone/reuse provenance points back to the source run or artifact;
- the release issue links the exact commit, staging URL, and evidence files.

If any item fails, keep the release on `dev`, record the failing subsystem from
the smoke report, and open or update the owning issue before retrying.
