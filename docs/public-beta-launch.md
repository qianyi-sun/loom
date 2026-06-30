# Public Beta Launch Gate

This page is the release-owner checklist for Loom's public beta. It
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
- Quota and rate-limit enforcement are not launch blockers for this beta. Use
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
  public backends, no public LLM Gateway, no public Control Plane, and no public
  object store.
- Screenshots or notes for logged-out SPA load, account request, admin account
  approval link, password setup, forgot-password request/reset approval, Team
  Settings, provider setup, SPA batch submission, Monitor progress showing
  `username / team`, and Run Library My team / All teams views.
- CLI transcript for `loom auth login`, `loom auth whoami`,
  `loom providers test`, `loom providers models`, `loom eval batch create`,
  `loom eval batch show`, `loom eval trial show`, and
  `loom eval trial download`.
- Benchmark catalog provisioning transcript showing either
  `loom datasets provision-public-beta-catalog` with non-zero
  `ready_agents`, non-zero `ready_benchmarks`, non-zero `ready_tasks`, and
  `missing=0`, or
  `loom datasets register <benchmark>` against the published HF manifest with
  `--mirror-to-object-store`, non-zero `registered`, non-zero `mirrored`, zero
  unexpected `legacy_placeholders`, and `loom datasets audit --verify-bundles`
  showing `missing=0`. Include `/api/v1/agents` evidence with at least one
  ready entry and `/api/v1/benchmarks` evidence with at least one runnable
  entry. For private or gated HF manifests, confirm the operator
  context has `HF_TOKEN` and target `LOOM_SVC_MINIO_*` credentials. Workers may
  still receive optional `loom-secrets/huggingface-api-key` as legacy `hf://`
  compatibility, but mirrored release evidence should not depend on worker
  direct HF fetches.
- Environment desired-state transcript showing
  `loom admin environment-state apply` and
  `loom admin environment-state check` against
  `deploy/environment-state/public-beta.toml` or
  `deploy/environment-state/staging.toml`, with the rollout `IMAGE_TAG` and
  `ENV_CONFIG_VERSION` variables supplied. This must converge worker-pool
  autoscaler policies, GB10 desired state, and any external Slurm autoscaler
  supervisor before Monitor/resource-pool screenshots are used as evidence. For
  public-beta, the OLDLAB supervisor unit must point at the current rollout
  checkout, include `--pool-name oldlab`, and have its user timer active. The
  public-beta profile targets the existing CP desired-state environment
  `production` until GB10 node agents are renamed in a coordinated rollout.
- Benchmark reward acceptance transcript from
  `scripts/benchmark_reward_gate.py`: the readiness gate must pass against the
  user-visible catalog, and the supported-benchmark sweep gate must prove every
  v1.0-supported benchmark has numeric-reward coverage for every currently
  runnable task. A model answer scored `0` is a valid evaluator result; missing
  reward, verifier error, task-image failure, benchmark-side timeout, or missing
  allowlist coverage is not.
- Layer 1 score-credibility transcript from
  `scripts/benchmark_score_alignment_gate.py manifest --manifest
  docs/benchmark-score-alignment.json`, proving that every v1.0-supported
  benchmark has a canonical scoring reference, score semantics, Harbor/upstream
  parity decision, and at least one same-output replay case definition.
- If a remote-worker pool is attached, private tunnel evidence from
  `scripts/ops/worker_service_tunnels.py check` and `check-remote` showing the
  Control Plane, Gateway, optional subprocess Gateway facade, and MinIO
  worker-facing URLs are healthy from the control node and from at least one
  worker-host context, plus evidence that
  `loom-remote-worker-tunnel-watchdog.timer` is active on the control node.
- If OLDLAB elastic workers are enabled, `worker_capacity_smoke` release-gate
  evidence must include the smoke batch id, runtime, failure count, and one
  `oldlab_worker_records` entry per OLDLAB worker with node name, Slurm job id,
  Loom worker id, configured concurrency, and claimed trial count.
- `scripts/public_beta_smoke_gate.py` Markdown evidence with `--fail-on-skip`
  and `--allow-mutating-checks` against disposable staging data. When
  `--batch-id` is provided, the `runs.claimed_without_started` row must be
  `PASS`; a nonzero value means the source run still has orphaned claimed work
  and cannot be used as release evidence.
- For IP-address staging hosts, note the hostless Ingress rendering, attach
  evidence that the TLS Secret certificate includes the staging IP as a Subject
  Alternative Name, and verify the ingress controller serves that Secret as its
  default certificate.
- Leak-scan note showing seeded fake secrets and internal service URLs were not
  found in API responses, audit excerpts, or downloaded safe artifacts.

The release gate manifest is the machine-readable index for those items. Keep
it free of raw bearer tokens, provider keys, signed object-store URLs, internal
service URLs, and secret refs; `scripts/ops/release_gate.py` fails the gate if
any of those patterns appear.

## Automated Gate

Before submitting public-beta canaries or supported-benchmark release trials,
run the object-store write gate by itself:

```bash
python scripts/public_beta_smoke_gate.py \
  --server-url https://loom.example.com \
  --team-a-token "$TEAM_A_TOKEN" \
  --team-b-token "$TEAM_B_TOKEN" \
  --catalog-minio-endpoint "$PUBLIC_BETA_MINIO_ENDPOINT" \
  --catalog-minio-access-key "$PUBLIC_BETA_MINIO_ACCESS_KEY" \
  --catalog-minio-secret-key "$PUBLIC_BETA_MINIO_SECRET_KEY" \
  --object-store-write-check-only \
  --object-store-write-check-bucket trajectories \
  --fail-on-skip \
  --markdown-output public-beta-object-store-preflight.md \
  --json-output public-beta-object-store-preflight.json
```

The preflight must show `object_store.minio_write_probe` as `PASS` before any
trial execution starts.

After browser setup and a completed Team A source run, run:

```bash
python scripts/public_beta_smoke_gate.py \
  --server-url https://loom.example.com \
  --team-a-token "$TEAM_A_TOKEN" \
  --team-b-token "$TEAM_B_TOKEN" \
  --provider-connection-name mz_tn_canada_qianyi \
  --provider-model-provider yibuapi \
  --provider-model-name gpt-4o-mini \
  --batch-id "$TEAM_A_BATCH_ID" \
  --trial-id "$TEAM_A_TRIAL_ID" \
  --safe-artifact-key "$SAFE_ARTIFACT_KEY" \
  --blocked-artifact-key "$BLOCKED_ARTIFACT_KEY" \
  --private-trial-id "$PRIVATE_TRIAL_ID" \
  --private-artifact-key "$PRIVATE_ARTIFACT_KEY" \
  --clone-provider-connection-id "$TEAM_B_PROVIDER_CONNECTION_ID" \
  --reuse-provider-connection-id "$TEAM_B_PROVIDER_CONNECTION_ID" \
  --catalog-minio-endpoint "$PUBLIC_BETA_MINIO_ENDPOINT" \
  --catalog-minio-access-key "$PUBLIC_BETA_MINIO_ACCESS_KEY" \
  --catalog-minio-secret-key "$PUBLIC_BETA_MINIO_SECRET_KEY" \
  --object-store-write-check \
  --object-store-write-check-bucket trajectories \
  --secret-needle seeded-public-beta-secret \
  --internal-url-needle loom-minio.loom.svc.cluster.local \
  --allow-mutating-checks \
  --fail-on-skip \
  --markdown-output public-beta-smoke.md \
  --json-output public-beta-smoke.json
```

The script checks:

- public health and logged-out SPA reachability;
- Team A and Team B token auth;
- provider connection and model-discovery surfaces;
- runnable benchmark catalog presence;
- sampled ready benchmark task bundle prefixes in object storage;
- a MinIO write/delete probe against the runtime trajectory bucket, so
  `XMinioStorageFull` and other object-store write failures are caught before
  submitting canary or release-trial work;
- batch/trial detail and service-proxied ATIF/trajectory downloads;
- Run Library My team and All teams visibility;
- owner-team label;
- cross-team safe artifact download through Run Library;
- direct owner-team artifact route denial;
- clone config, reuse artifact, and provenance;
- blocked artifact denial;
- private artifact denial;
- cross-team mutation denial;
- seeded fake secret, token-pattern, signed-URL, and internal-URL leaks.

The script intentionally redacts raw tokens, provider-key-like values, seeded
fake secrets, signed object-store URLs, and internal service URLs from its
Markdown and JSON output.

Before submitting a public-beta canary or supported-benchmark acceptance run,
the `object_store.minio_write_probe` row must pass. If it fails with
`XMinioStorageFull`, reclaim or provision storage for the MinIO-backed
filesystem first; do not start the trial and wait for worker artifact upload to
discover the same failure later.

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

If the public beta uses extra remote workers outside the Kubernetes cluster,
run this private gate after every rollout and before load testing:

```bash
systemctl --user is-active loom-remote-worker-tunnel-watchdog.timer

scripts/ops/worker_service_tunnels.py check \
  --env-file .env.remote-worker

scripts/ops/worker_service_tunnels.py check-remote worker-hosts.txt \
  --env-file .env.remote-worker
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
  --namespace loom-public-beta \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/public-beta.kubeconfig \
  --subprocess-gateway-local-port 30444
```

For GB10 and OLDLAB public-beta rollouts, gate the Slurm-managed capacity first
and then gate node-agent convergence only for compose rollout compatibility.
Run `environment-state check` from the Slurm submit/shared-storage host so it
can validate external runner env files, shared worker checkouts, and local
systemd user timers in addition to CP-backed state. The Slurm check catches
pending/stale capacity requests, active jobs launched from stale
`LOOM_REMOTE_WORKER_*` paths, inactive OLDLAB autoscaler timers, unscoped
external autoscaler commands that omit `--pool-name oldlab`, and the active
`gb10-arm64`/`oldlab` pool shapes; the node-agent check catches stale
host-local checkouts, local-build fallback using an old tree, and env files
that did not apply even when the pool still has healthy heartbeats.

```bash
loom admin environment-state apply \
  --cp-url http://control-node.lan:18081 \
  --admin-token file:/secure/path/admin-token \
  --environment public-beta \
  --file deploy/environment-state/public-beta.toml \
  --var IMAGE_TAG="$IMAGE_TAG" \
  --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}"

loom admin environment-state check \
  --cp-url http://control-node.lan:18081 \
  --admin-token file:/secure/path/admin-token \
  --environment public-beta \
  --file deploy/environment-state/public-beta.toml \
  --var IMAGE_TAG="$IMAGE_TAG" \
  --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}"

loom admin slurm-workers status \
  --cp-url http://control-node.lan:18081 \
  --admin-token file:/secure/path/admin-token

loom admin gb10-workers status \
  --cp-url http://control-node.lan:18081 \
  --admin-token file:/secure/path/admin-token \
  --environment production \
  --pool-name gb10-arm64 \
  --release-image-tag "$IMAGE_TAG" \
  --release-env-config-version "$ENV_CONFIG_VERSION"
```

For OLDLAB 1-5, use the committed staged files in
`deploy/worker-pools/oldlab/` as the source of truth for included nodes,
requested Slurm slice, and controller env overrides. OLDLAB 4/5 must not be
counted in production capacity until a real Loom batch records worker
registration, heartbeat, claim/finalize, and artifact evidence from those
nodes.

## Release Decision

The public beta launch gate passes only when:

- every required manual evidence item is attached;
- any attached remote-worker pool has passing private tunnel checks from both
  the control node and a worker-host context;
- the ready benchmark catalog has been provisioned with `missing=0`;
- `scripts/public_beta_smoke_gate.py` exits 0 with `--fail-on-skip`;
- the smoke report's `runs.claimed_without_started` row is `PASS` for the
  source batch used as launch evidence;
- no response, audit excerpt, log excerpt, or safe downloaded artifact contains
  seeded fake secrets or internal URLs;
- unsafe artifacts are blocked and cannot be downloaded by another team;
- clone/reuse provenance points back to the source run or artifact;
- the release issue links the exact commit, staging URL, and evidence files.

If any item fails, keep the release on `dev`, record the failing subsystem from
the smoke report, and open or update the owning issue before retrying.
