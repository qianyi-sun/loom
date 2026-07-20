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
  [`docs/runbooks/operator-runbook.md`](operator-runbook.md) and
  [`docs/architecture/cluster-deploy.md`](../architecture/cluster-deploy.md).
- Independent merged-only staging operation:
  [`docs/architecture/adr/independent-staging-rollout-runner.md`](../architecture/adr/independent-staging-rollout-runner.md)
  and the
  [`loom-staging-rollout` operator interface](operator-runbook.md#independent-staging-operator-interface-803).
- Browser and CLI onboarding:
  [`docs/user-guide.md#web-sessions-and-teams`](../user-guide.md#web-sessions-and-teams)
  and
  [`docs/user-guide.md#public-server-cli-flow`](../user-guide.md#public-server-cli-flow).
- Run Library and artifact reuse:
  [`docs/user-guide.md#run-library`](../user-guide.md#run-library) and
  [`docs/architecture/run-library.md`](../architecture/run-library.md).
- Security model:
  [`SECURITY.md`](../../SECURITY.md),
  [`docs/architecture/auth-threat-model.md`](../architecture/auth-threat-model.md),
  and
  [`docs/architecture/auth-registration-spec.md`](../architecture/auth-registration-spec.md).
- Troubleshooting:
  [`docs/runbooks/operator-runbook.md#alarm-response-troubleshooting-matrix`](operator-runbook.md#alarm-response-troubleshooting-matrix)
  plus the provider, sharing, and download checks in the staging smoke gate.
- Full/max-slot three-cluster canary preparation:
  [`docs/runbooks/full-max-slot-canary-runbook.md`](full-max-slot-canary-runbook.md).

## Evidence Required

Attach these to the release issue or release PR:

- Broker request and attempt evidence showing the authenticated operator,
  freshly fetched `refs/heads/dev`, exact merged SHA, derived image tag,
  immutable backup manifest digest, detached unit, rollout ID, lifecycle-lock
  events, and terminal summary. A pull-request ref, feature branch, tag,
  historical/local SHA, custom tag, personal key/unit, or mutable
  `backups/latest` manifest is not valid staging evidence. For the initial #803
  live acceptance, retain Hongjian's and Devansh's separate `start --dry-run`
  previews, unauthorized-caller rejection, and simultaneous-start rejection;
  both previews must resolve the same current merged SHA when `dev` has not
  moved.
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
  `https://yylx.world/prod/api`, while `https://yylx.world/staging` exposes
  staging identity and `https://yylx.world/staging/api`, with
  no-store runtime config responses.
- `www.yylx.world` must not become a second staging/prod surface. It is only a
  TLS-bound redirect host via `ingress_redirect_hosts = ["www.yylx.world"]`;
  smoke and release evidence should use the canonical bare-domain routes.
- Sanitized `loom-staging-admin-browser-smoke.json` evidence bound to the exact
  build SHA reported by the running service and `https://yylx.world/staging`,
  produced only by broker-owned step 16 of a candidate-bound protected-staging
  rollout after the logged-out route smoke. The ephemeral kind workflow remains
  `runtime_environment = "development"` and only proves that this exchange is
  hidden with a credential-free `404`; it must never pose as protected staging.
  The broker accepts the installed Qianyi-owned `0640` token only through its
  existing `loom-rollout` read-only ACL; symlinked paths, mutable group/other
  authority, owner drift, hard links, and changing metadata fail closed before
  the browser container starts.
  Archive the report with the broker request ID, attempt, envelope digest,
  resolved candidate SHA, and matching running-service build SHA; a PR artifact or manual command
  is not candidate evidence. The rollout evidence must show the correlated
  browser request ID and safe audit event, successful product APIs and visible
  state for all six Admin Access tabs,
  including Arrow/Home/End roving focus and exact ARIA tab-to-panel
  relationships, Audit log, Rate cards, logout/revocation, and final
  `/api/v1/auth/me` `401`. The brokered rollout may source the ephemeral
  singleton admin bearer only through an owner-only (`0600`) file or
  non-interactive stdin; it must upload no trace, screenshot, storage state,
  cookie, or raw secret.
  This staging-only admin evidence does not replace normal-user onboarding,
  team-boundary, or submission evidence.
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
- Benchmark catalog provisioning evidence from `loom cluster rollout` step 11
  (`catalog-provisioning.json`, redacted stdout/stderr logs) showing the
  committed staging profile's HF registration path,
  the rollout-owned mode-`0700` catalog cache root that overrides inherited
  `XDG_CACHE_HOME`/`HF_HOME`/`HF_HUB_CACHE`,
  `loom datasets register <benchmark>` against the published HF manifest with
  `--mirror-to-object-store`, non-zero `registered`, non-zero `mirrored`, zero
  unexpected `legacy_placeholders`, and `loom datasets audit --verify-bundles`
  showing `missing=0`. The protected rollout smoke catalog must also include
  the checked-in GB10 smoke fixture published through
  `loom datasets publish-local deploy/catalog/gb10-smoke`, producing task
  `loom-smoke/gb10-oracle-hello-world` with an internal `s3://` source. Include
  `/api/v1/agents` evidence with at least one ready entry and
  `/api/v1/benchmarks` evidence with at least one runnable entry. For private or
  gated HF manifests, confirm the operator context uses protected
  `staging-catalog-provisioning.env` or `catalog_provisioning.env_sources` for
  `PUBLISHED_SHA`, `HF_TOKEN`, and target `LOOM_SVC_MINIO_*` credentials.
  The committed staging profile also records step-owned Kubernetes
  port-forward evidence for `loom-postgres` and `loom-minio`, so the candidate
  CLI can run from the rollout host without relying on host DNS for cluster
  service names or on hand-started tunnels.
  Use the source-copy `loom datasets provision-catalog` path only when a
  profile also declares protected source catalog/object-store inputs.
  `loom-service` may receive optional `loom-secrets/huggingface-api-key` as
  `HF_TOKEN` for gated catalog mirror provisioning. Workers must not receive
  `HF_TOKEN`; mirrored release evidence must prove worker materialization uses
  internal `s3://` sources.
- SkillLearnBench HF mirror/token-boundary evidence generated by
  `loom datasets hf-boundary-evidence skilllearnbench` and saved as
  `$ROLLOUT_DIR/hf-mirror-boundary-evidence-$IMAGE_TAG.json`. Protected rollout
  step 14 first validates the exact current host-to-worker registration set,
  submits or idempotently recovers a deterministic one-trial SkillLearnBench
  oracle canary bound to that set, waits for success, and passes that exact
  batch id while generating this artifact before release-gate. It never falls
  back to a historical succeeded canary after a worker restart. The artifact must
  be secret-safe and prove `benchmark_id=skilllearnbench`, runnable rows,
  `requires_caps.cpu_arch=any`, all sampled runtime sources are internal
  `s3://` bundle prefixes, runtime HF mirror provenance is retained from task
  tags/source prefixes, the canary reached `started` and a terminal state from
  the internal source, `worker_boundary.hf_token_present=false`, and
  `direct_hf_egress_required=false`. Do not use `benchmarks.upstream_kind` as
  the HF mirror fact; it is adapter/source provenance and may legitimately be
  `git`.
- Broker-generated environment desired-state transcript showing the driver's
  `loom admin environment-state apply` and
  `loom admin environment-state check` against
  `deploy/environment-state/staging.toml`, with the rollout `IMAGE_TAG` and
  `ENV_CONFIG_VERSION` variables supplied. This must converge worker-pool
  autoscaler policies, GB10 desired state, and any external Slurm autoscaler
  supervisor before Monitor/resource-pool screenshots are used as evidence.
  Protected rollout step 11 also materializes the candidate profile to
  `/data/loom-staging/environment-state/staging.toml` and records
  source/target sha256 plus mode evidence, so rerun/resume does not depend on a
  stale physical profile copy. For staging, the rendered
  service/control-plane/gateway pods must have
  `LOOM_ENV=staging`, and `environment-state` evidence must show staging
  worker-pool policies and GB10 desired state keyed as `staging/gb10`.
  Any current-path staging evidence that reports `production/gb10` is
  drift, not a valid migration exception.
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
  docs/score-alignment/manifest.json`, proving that every v1.0-supported
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
  Runtime evidence must also include
  `scripts/ops/prod_pressure_worker_control.py`: the production CP pressure
  snapshot is consumed by staging desired state and `Worker.drain_state`, so a
  fresh claim attempt is fenced before node-agent shutdown convergence. A
  manifest-only status preview is not worker-control evidence.
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
  [`docs/runbooks/full-max-slot-canary-runbook.md`](full-max-slot-canary-runbook.md).
  The canary must wait for a clean staging anchor, completed #190 targeted
  durability validation, and an explicit coordinator `GO`. The batch must use
  repeated `--required-worker-pool` flags for `oldlab` and
  `gb10`; terminal evidence must include `runs.worker_pool_coverage`
  and `runs.claimed_without_started` from the smoke gate. Staging
  clusters run with `k8s_worker.enabled=false` (#383) so x86_64
  coverage is delivered by the Slurm-managed `oldlab` pool, not by an
  in-cluster `k8s-worker` Deployment.

The broker-owned rollout generates the release manifest before protected
apply. It is the machine-readable expected-state anchor for image/render
convergence, DB revision checks, and per-component release evidence. The image
identities JSON is keyed by Deployment and container and contains `image` plus
at least one immutable `repo_digest` or `image_id` per release-managed
container.

Operators do not run `loom cluster release-manifest`, `loom cluster up`,
`environment-state apply`, or `--force-rollout-lock` directly for staging.
Those are candidate-bound driver steps constructed from the immutable broker
envelope. The full-lifecycle lock rejects another broker request, while the
existing per-mutation lease covers Kubernetes apply and external worker
desired-state steps. If a lock appears stale, inspect it through
`loom-staging-rollout status REQUEST_ID`; resume the recorded request only
after the broker proves the prior attempt terminal. Never force or replace the
lease from an operator shell.

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

After protected apply reaches readiness, the same detached driver generates HF
boundary evidence and runs the hard convergence gate against its saved inputs.
Operators attach the resulting JSON and Markdown artifacts from the request's
rollout directory; they do not reconstruct or rerun the release-gate argv from
an ambient checkout.

This gate fails on rendered/config hash drift, unverifiable managed image
identity convergence, classified node-runtime sandbox deadline stalls, live DB
Alembic revision mismatch, and missing or failed environment-state convergence
evidence when the release manifest records external-worker desired state.
The driver generates the environment-state check after its matching apply;
`ok=false` or any non-empty `drift` array keeps the release-gate artifact red
and blocks workload-validation anchors. When the manifest records GB10 desired
state, the driver also supplies its GB10 status artifact; stale, missing,
unreachable, non-applied, dirty, or capacity-mismatched active GB10 nodes keep
the release gate red. For staging and production it also supplies HF mirror
boundary evidence; missing evidence,
non-S3 SkillLearnBench runtime sources, missing HF provenance, worker
`HF_TOKEN` presence, missing/failed GB10 token inspection, direct worker HF
egress dependence, or raw secret-looking values keep the release gate red. For
normal runtime image IDs, it compares the
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
  --provider-model-name glm-5.1-thinking \
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
  --required-worker-pool gb10 \
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
For v1.0's GB10-only staging gate, require `--required-worker-pool gb10`.
For v1.1/full-cluster mixed-pool evidence, create the release/acceptance batch
with repeated `--required-worker-pool` flags, for example
`--required-worker-pool oldlab --required-worker-pool gb10`. On clusters
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

If shared staging uses remote workers outside Kubernetes, the candidate-bound
broker driver runs the watchdog, local tunnel, and remote/Slurm allocation
checks before release-gate and load testing. Operators inspect the resulting
redacted tunnel artifacts through `loom-staging-rollout status REQUEST_ID` and
`loom-staging-rollout logs REQUEST_ID`; they do not rerun the helper from an
interactive checkout or reconstruct its env, kubeconfig, host, or job inputs.

The env file supplies the same worker-facing URLs used by remote workers:

```bash
LOOM_WORKER_CONTROL_PLANE_URL=http://control-node.lan:18081
LOOM_WORKER_GATEWAY_URL=http://control-node.lan:19100
LOOM_WORKER_MINIO_ENDPOINT=http://control-node.lan:19000
# Optional when Docker sandboxes use a different host-gateway facade URL.
# LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:30444/openai/v1
```

The broker-owned gate exits non-zero if any required tunnel is down. If
`LOOM_WORKER_SUBPROCESS_GATEWAY_URL` is set, the gate also prints and probes
`subprocess-gateway`; `host.docker.internal` is checked through the equivalent
host-side loopback URL. This gate is separate from the public API smoke test:
`https://loom.example.com` can be healthy while the
remote-worker pool is detached.

Tunnel installation or repair is root-owned broker maintenance, not a staging
operator gate. Disable admission, prove the active request terminal, repair the
installed durable units from clean merged `dev`, restore admission, and resume
the original request through `loom-staging-rollout resume REQUEST_ID`. Never
substitute an ad-hoc `kubectl port-forward` or interactive
`worker_service_tunnels.py install-systemd` invocation as rollout evidence.

For GB10 and OLDLAB staging rollouts, the detached driver gates Slurm-managed
capacity first and then node-agent convergence for compose compatibility. Its
candidate-owned environment-state step runs from the fixed
submit/shared-storage host so it can validate external runner env files,
shared worker checkouts, local systemd user timers, and CP-backed state. The
broker supplies the protected worker-token source; evidence contains only
redacted sha256-prefix fingerprints. The Slurm check catches pending/stale
capacity requests, active jobs launched from stale `LOOM_REMOTE_WORKER_*`
paths, stale remote env worker-token fingerprints, inactive OLDLAB autoscaler
timers, unscoped external autoscaler commands that omit `--pool-name oldlab`,
missing or unexecutable external autoscaler `ExecStart` command paths, recent
failed autoscaler service results such as `status=203/EXEC`, and the active
`gb10`/`oldlab` pool shapes; the node-agent check catches stale
host-local checkouts, local-build fallback using an old tree, and env files
that did not apply even when the pool still has healthy heartbeats.
The installed runner's private generated directory must already contain a
service-owned mode-`0600` GB10 env template. The host installer bootstraps that
template once from a validated fixed legacy source when the directory is empty;
the rollout step never falls back to an arbitrary path. A missing template or
any symlink, hard link, wrong mode, oversized file, malformed dotenv entry, or
missing required endpoint/credential key fails before environment-state apply.
The same installer creates the external-runner checkout authority at
`/shared_work2/qianyi/.loom-staging-rollout/worker-repos` as
`loom-rollout:sharedwork` mode `2750`, without granting the service write
access to its `qianyi:sharedwork` mode-`2775` parent. Before creating this
authority, apply the checked-in exact-host export allowance on `trt-gb10-2`;
the platform-dev installer installs `shared_work2.mount` and verifies exact
NFS source, mountpoint, NFSv4.2 options, and device identity. A local directory
at `/shared_work2` fails closed. Runtime readiness proves that `loom-rollout`
can write/search the dedicated root and that the `qianyi` Slurm submitter can
read/search but not write it. Before running the requestless Tier 0–2
assessment, the installer publishes only the exact image-tagged direct child
through the fixed `loom-rollout` command, private final-name claim, and
inode-bound publication mode transition. This prepares source only and cannot
install or activate a GB10 unit. Installer `check`, Tier 0, and Step 11 share
the same read-only verifier; Step 11 reuses the same materializer and accepts
the already-matched immutable checkout. Authority symlinks, wrong
owner/group/mode, hardlinks,
special files, or a
non-exact resolved SHA fail closed. Publication is no-replace and immutable:
an exact existing target is accepted only after full index/physical-tree
validation, and drift is never replaced or implicitly cleaned during rollout
or resume. Broker preflight verifies the fixed 14 active nodes can read/search
but not write the root. After publish and before environment-state apply, step
11 streams trusted verifier bytes over protected SSH stdin (never from the
target) and requires all 14 nodes to agree on exact HEAD, clean status,
index/modes, deterministic tracked-file readability, and content identity.
The 13 clients must report the exact NFSv4 source and `trt-gb10-2` the ext4
backend; mount/device/inode values remain sanitized evidence. Private worker
env and token sources stay platform-dev-local mode `0600` files.
The external Slurm autoscaler also treats release-state drift as a fail-closed
decision. This includes a pending/running job whose node is no longer in the
policy's `allowed_nodes`; such jobs are neither healthy warm capacity nor part
of the legal active-node/job caps. A running job safely linked to a Loom worker
uses the normal drain path; once the worker is `draining` and its
claimed/running trials reach zero, the autoscaler cancels the job (or observes
that it already exited) and marks the worker `drained`. Pending or unlinked
drift remains blocked for operator
reconciliation rather than being force-cancelled. During convergence,
`loom admin worker-pools autoscaler status` reports
`last_blocked_reason=release_state_drift` with the affected Slurm job ids in
`last_error`. Do not submit staging canaries until those jobs are replaced or
cancelled and `environment-state check` is clean.
If OLDLAB resource-aware scale-up has no safe allowed node, status reports
`last_blocked_reason=no_safe_slurm_nodes` and
`last_blocked_details.node_exclusions` with per-node reasons such as
`insufficient_memory`, `cpu_load_high`, `unsafe_state`, `active_loom_job`, or
`missing_resource_snapshot`; fix the capacity condition or adjust the allowed
node/resource policy before treating OLDLAB as a validation pool.
The resulting environment-state JSON includes these hard blockers under
`autoscaler_blockers`, and release-gate fails its convergence row while any
blocker is active. Operators inspect the candidate-bound artifacts through
`loom-staging-rollout logs REQUEST_ID` and the rollout evidence path reported
by `status`; they do not read the live admin Secret or invoke apply/check from
an interactive shell.

For GB10 node-agent compatibility workers, `gb10-workers status` proves the
non-secret desired image/env-config state and source-checkout provenance. The
profile writes `source_git_commit` from `GIT_SHA`; active nodes must report a
clean git checkout at that commit. Missing provenance, a stale
`compose_project_dir`, a dirty checkout, or a source commit that differs from
desired state is a hard failure even when image/env fields are current. The same
artifact must also show a linked worker registry row for every active GB10 host:
`worker_id` present, `worker_status=active`, `worker_fresh=true`, and
`worker_backend_names` containing `docker`. This prevents release-gate from
passing nodes that applied env/source state but have no fresh worker available
for `/api/v1/backends` and smoke submission. Rollout release-gate retries direct
`gb10-workers status` release-target mismatches while newly started workers
register and heartbeat; persistent stale worker evidence after the retry window
means the host runtime needs repair. Worker-token rotation is represented in
the committed desired-state inputs and applied by a new merged broker rollout;
do not run per-host `gb10-agent apply` as a staging-launch shortcut. The worker
token is read from the protected service source and is never stored in Control
Plane desired state or operator argv.

Rollout step 12 also proves the boot-time recovery chain rather than only one
successful apply: the deploy user must have `Linger=yes`, the candidate
node-agent service and timer must match the installed units, and the timer must
be enabled and active/waiting. After the rollout session disconnects, wait more
than one timer period, stop one declared-active canary worker, and require the
same candidate image and a fresh linked heartbeat to return within the next
period. A stopped/excluded host, including the current node 7 exception, must
not return. Do not leave a legacy production node-agent timer sharing the
staging tunnel ports or Compose root; fence or isolate it before connectivity
is restored.

For OLDLAB 1-5, use the committed staged files in
`deploy/worker-pools/oldlab/` as the source of truth for included nodes,
requested Slurm slice, and controller env overrides. OLDLAB 4/5 must not be
counted in production capacity until a real Loom batch records worker
registration, heartbeat, claim/finalize, and artifact evidence from those
nodes.

## Release Decision

The staging launch gate passes only when:

- every required manual evidence item is attached;
- the rollout was admitted by `loom-staging-rollout` from the freshly fetched
  merged `dev` head, completed under one full-lifecycle owner, and can be
  inspected by another staging operator without Qianyi's login session,
  checkout, credential handoff, private key, or terminal;
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
