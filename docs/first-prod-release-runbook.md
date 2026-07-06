# First Prod Release Runbook

This is the single operator path for the first `main`-based production
release. It ties together first-prod bootstrap, temporary staging capacity
leases, frontend route checks, the production release gate, rollback
preparation, and emergency staging drain.

The current-doc gap this closes: the repo already had the right helper
scripts, but the operator path was split across the general operator runbook,
remote-worker notes, release-gate tests, frontend route smoke tests, and
worker-capacity docs. A reviewer could not follow one executable, no-secret
dry run and then know exactly which commands become live production actions.

## Safety Labels

| Label | Meaning |
|---|---|
| `DRY-RUN SAFE` | Repo-only or local-file-only. Safe for a reviewer to run without production secrets, live kubeconfig, live database access, or staging/prod workload authority. |
| `READ-ONLY LIVE` | Reads public or already-sanitized live evidence. Does not mutate prod, staging, workers, DB, object storage, or Git tags, but still needs release-owner coordination when it hits canonical prod/dev URLs. |
| `LIVE PROD AUTHORITY REQUIRED` | Needs production GitHub Environment approval, production deploy rights, production kubeconfig, release tag push rights, or authority to change live desired state. Do not run from a docs review or local dry run. |

Never put raw token, provider key, database password, MinIO credential,
signed URL, kubeconfig, or bearer value into shell history, argv evidence,
JSON, Markdown, logs, PRs, or issues. Use secret references such as
`github-environment:production/LOOM_SERVICE_API_TOKEN`, `env:GH_TOKEN`, or
`file:/secure/path/token`. Expected redacted evidence may contain
`<redacted>` or `[REDACTED sha256:<12-hex> len=<N>]`.

## Required Inputs

Set these once at the start of an operator session. The fake values shown here
are safe for the no-secret dry run; replace them only during live release
work.

```bash
export CANDIDATE_SHA="${CANDIDATE_SHA:-0123456789abcdef0123456789abcdef01234567}"
export PROD_RELEASE_SHA="${PROD_RELEASE_SHA:-$CANDIDATE_SHA}"
export STAGING_RELEASE_SHA="${STAGING_RELEASE_SHA:-abcdef0123456789abcdef0123456789abcdef01}"
export IMAGE_TAG="${IMAGE_TAG:-release-0123456789ab}"
export PROD_IMAGE_TAG="${PROD_IMAGE_TAG:-$IMAGE_TAG}"
export STAGING_IMAGE_TAG="${STAGING_IMAGE_TAG:-staging-abcdef0}"
export PROD_TAG="${PROD_TAG:-v1.0.0}"
export ROLLOUT_DIR="${ROLLOUT_DIR:-/tmp/loom-first-prod-dry-run}"
mkdir -p "$ROLLOUT_DIR"
```

Stop condition: `CANDIDATE_SHA` and `PROD_RELEASE_SHA` must be 40-character
lowercase Git SHAs. The live release owner must choose a new immutable
`PROD_TAG`; never reuse or force-move a published tag.

## No-Secret Non-Production Dry Run

This No-secret non-production dry run covers First-prod bootstrap, Staging validation lease, Frontend environment checks, Production release, Rollback preparation, and Emergency staging drain.

This section is the acceptance-testable dry run. It uses only committed repo
files, fake SHAs, synthetic evidence, and local files under `$ROLLOUT_DIR`.
It must not touch live staging, production, DB, object storage, workers, or
secrets.

### 1. First-Prod Bootstrap

`DRY-RUN SAFE`

```bash
git fetch origin dev main --tags
git rev-parse --verify origin/dev
git tag -l "$PROD_TAG"

uv run python scripts/validate_environment_isolation.py \
  --profiles-dir deploy/environments \
  --workflow .github/workflows/deploy-environment.yml \
  --dry-run-artifact "$ROLLOUT_DIR/environment-isolation-dry-run.json"
```

Expected success output:

```text
Environment isolation validation: PASS
```

Expected failure output:

```text
Environment isolation validation: FAIL
- production: service_api_token_ref must start with 'github-environment:production/'
```

Stop condition: any shared namespace, database, bucket, frontend API base,
worker token ref, service token ref, provider ref, or YibuAPI ref blocks the
release. Fix the committed environment profiles instead of substituting a
runtime secret.

### 2. Capacity Status

`DRY-RUN SAFE`

This proves the default first-prod desired state: production owns all eligible
shared physical capacity, staging/dev owns zero borrowed slots, and unreachable
hosts stay out of both pools.

```bash
uv run python scripts/ops/worker_capacity_manifest.py status \
  --manifest deploy/worker-capacity/prod-first.toml \
  --var PROD_IMAGE_TAG="$PROD_IMAGE_TAG" \
  --var PROD_SOURCE_COMMIT="$PROD_RELEASE_SHA" \
  --var STAGING_IMAGE_TAG="$STAGING_IMAGE_TAG" \
  --var STAGING_SOURCE_COMMIT="$STAGING_RELEASE_SHA" \
  --format json \
  --evidence-out "$ROLLOUT_DIR/worker-capacity-prod-first.json"
```

Expected success output:

```json
{
  "status": "pass",
  "operation": "status",
  "new_staging_claims_allowed": false,
  "lease": {"state": "none"},
  "summary": {
    "prod_slots": 180,
    "staging_slots": 0,
    "state_counts": {"eligible": 19, "unreachable": 1}
  }
}
```

Expected failure output:

```json
{
  "status": "fail",
  "drift": [
    {"path": "workers[0].api_url", "expected": "https://yylx.world/prod/api", "actual": "<redacted>"}
  ]
}
```

Stop condition: unresolved placeholders, non-empty `errors`, non-empty
`drift`, `staging_slots` greater than `0`, or `new_staging_claims_allowed=true`
blocks production promotion.

### 3. Staging Validation Lease

Staging leases are temporary. The default prod capacity ownership must be
restored after validation, even when validation fails or is interrupted.
Operational shorthand: default prod capacity ownership must be restored.

Preview the lease first. Without `--apply`, this command writes no file and
does not mutate live state.

`DRY-RUN SAFE`

```bash
uv run python scripts/ops/worker_capacity_manifest.py lease-staging \
  --manifest deploy/worker-capacity/prod-first.toml \
  --var PROD_IMAGE_TAG="$PROD_IMAGE_TAG" \
  --var PROD_SOURCE_COMMIT="$PROD_RELEASE_SHA" \
  --var STAGING_IMAGE_TAG="$STAGING_IMAGE_TAG" \
  --var STAGING_SOURCE_COMMIT="$STAGING_RELEASE_SHA" \
  --reason "staging rollout smoke $STAGING_IMAGE_TAG" \
  --ttl 45m \
  --slots-per-host 1 \
  --max-total-slots 2 \
  --preemptible \
  --format json
```

Expected success output:

```json
{
  "status": "pass",
  "operation": "lease-staging",
  "applied": false,
  "new_staging_claims_allowed": true,
  "lease": {
    "state": "active",
    "ttl_seconds": 2700,
    "preemptible": true
  },
  "summary": {"prod_slots": 178, "staging_slots": 2}
}
```

Write the lease manifest only after the preview is reviewed. This is still
repo/file-only; the helper writes a TOML desired-state file and sanitized JSON
evidence, not live worker state.

`DRY-RUN SAFE`

```bash
export LEASE_MANIFEST="$ROLLOUT_DIR/worker-capacity-staging-lease.toml"

uv run python scripts/ops/worker_capacity_manifest.py lease-staging \
  --manifest deploy/worker-capacity/prod-first.toml \
  --var PROD_IMAGE_TAG="$PROD_IMAGE_TAG" \
  --var PROD_SOURCE_COMMIT="$PROD_RELEASE_SHA" \
  --var STAGING_IMAGE_TAG="$STAGING_IMAGE_TAG" \
  --var STAGING_SOURCE_COMMIT="$STAGING_RELEASE_SHA" \
  --reason "staging rollout smoke $STAGING_IMAGE_TAG" \
  --ttl 45m \
  --slots-per-host 1 \
  --max-total-slots 2 \
  --preemptible \
  --apply \
  --output-manifest "$LEASE_MANIFEST" \
  --evidence-out "$ROLLOUT_DIR/worker-capacity-staging-lease.json"
```

Stop condition: do not run staging validation if the lease output is
`status=fail`, assigns staging more than the approved slots, uses
`--non-preemptible` without an explicit approval URL, or contains secret-like
values.

### 4. Prod-Pressure Status And Emergency Staging Drain

Use `status` whenever production demand appears while a staging lease exists.
Any nonzero production pending count, active count, or capacity shortfall stops
new staging claims and returns idle staging slots to production in the desired-state
artifact.

`DRY-RUN SAFE`

```bash
uv run python scripts/ops/worker_capacity_manifest.py status \
  --manifest "$LEASE_MANIFEST" \
  --prod-pending-count "${PROD_PENDING_COUNT:-3}" \
  --prod-active-count "${PROD_ACTIVE_COUNT:-0}" \
  --prod-capacity-shortfall "${PROD_CAPACITY_SHORTFALL:-1}" \
  --prod-pressure-source "control-plane prod queue summary" \
  --preemptible-grace-period 10m \
  --apply \
  --output-manifest "$ROLLOUT_DIR/worker-capacity-staging-status.toml" \
  --evidence-out "$ROLLOUT_DIR/worker-capacity-staging-status.json"
```

Expected success output:

```json
{
  "status": "pass",
  "operation": "status",
  "new_staging_claims_allowed": false,
  "prod_pressure": {
    "has_pressure": true,
    "cause": "prod_capacity_pressure",
    "prod_pending_count": 3,
    "prod_capacity_shortfall": 1
  },
  "summary": {"prod_slots": 180, "staging_slots": 0}
}
```

Use an explicit manual drain for emergency or operator-initiated pause cases
that are not represented by the prod-pressure counts.

`DRY-RUN SAFE`

```bash
uv run python scripts/ops/worker_capacity_manifest.py drain-staging \
  --manifest "$LEASE_MANIFEST" \
  --reason "prod pressure before release gate" \
  --apply \
  --output-manifest "$ROLLOUT_DIR/worker-capacity-staging-draining.toml" \
  --evidence-out "$ROLLOUT_DIR/worker-capacity-staging-drain.json"
```

Expected success output:

```json
{
  "status": "pass",
  "operation": "drain-staging",
  "new_staging_claims_allowed": false,
  "drain": {
    "idle_leased_slots": 2,
    "running_staging_trials": 0,
    "released_idle_hosts": ["trt-gb10-1", "trt-gb10-2"]
  },
  "summary": {"prod_slots": 180, "staging_slots": 0}
}
```

Stop condition: if `running_staging_trials` is nonzero, record the trial IDs from
the secret-free worker/status artifact, wait for graceful completion or use the
approved retry/cancel path for preemptible staging work. Do not start a prod
release gate while staging has active borrowed slots unless the release owner
adds an explicit override with `approved=true`, a reason, and an HTTPS
evidence URL.

### 5. Staging Release

Run this after staging validation completes, fails, or is cancelled. It is
idempotent and should be rerun until evidence shows staging slots are zero.

`DRY-RUN SAFE`

```bash
uv run python scripts/ops/worker_capacity_manifest.py release-staging \
  --manifest "$LEASE_MANIFEST" \
  --reason "staging smoke complete" \
  --apply \
  --output-manifest "$ROLLOUT_DIR/worker-capacity-staging-released.toml" \
  --evidence-out "$ROLLOUT_DIR/worker-capacity-staging-release.json"
```

Expected success output:

```json
{
  "status": "pass",
  "operation": "release-staging",
  "new_staging_claims_allowed": false,
  "lease": {
    "state": "released",
    "released_at": "2026-07-06T00:20:00Z"
  },
  "summary": {"prod_slots": 180, "staging_slots": 0}
}
```

Stop condition: if release evidence still shows `staging_slots` greater than `0`
or `new_staging_claims_allowed=true`, repeat `release-staging` against the latest
lease manifest or drain status. Do not promote production with an active staging
lease.

### 6. Frontend Environment Checks

For a no-secret local smoke, run this against a non-production static preview
or local ingress that serves both `/prod` and `/dev` runtime config files.

`DRY-RUN SAFE`

```bash
uv run python scripts/ops/frontend_route_smoke.py \
  --route production="${LOCAL_ORIGIN:-http://127.0.0.1:4173}/prod=${LOCAL_ORIGIN:-http://127.0.0.1:4173}/prod/api" \
  --route development="${LOCAL_ORIGIN:-http://127.0.0.1:4173}/dev=${LOCAL_ORIGIN:-http://127.0.0.1:4173}/dev/api" \
  --json
```

For release evidence against canonical public routes:

`READ-ONLY LIVE`

```bash
uv run python scripts/ops/frontend_route_smoke.py \
  --route production=https://yylx.world/prod=https://yylx.world/prod/api \
  --route development=https://yylx.world/dev=https://yylx.world/dev/api \
  --json \
  > "$ROLLOUT_DIR/frontend-route-smoke.json"
```

Expected success output:

```json
{
  "status": "pass",
  "routes": [
    {
      "route_url": "https://yylx.world/prod",
      "expected_environment": "production",
      "expected_api_base": "https://yylx.world/prod/api",
      "status": "pass"
    }
  ]
}
```

Expected failure output:

```json
{
  "status": "fail",
  "routes": [
    {
      "status": "fail",
      "errors": [
        "apiRouteBase must be https://yylx.world/dev/api",
        "runtime config response must be no-store"
      ]
    }
  ]
}
```

Stop condition: any production route resolving `/dev`, any development route
resolving `/prod`, missing `no-store`, missing environment identity, or a
cached stale config blocks production promotion.

### 7. Operator-Free User E2E Evidence Gate

The live normal-user journey for #493 is `LIVE PROD AUTHORITY REQUIRED` and is
not part of a no-secret dry run. It must be run later by a normal scoped user
or user-agent through exposed CLI/API and frontend surfaces only. The
repo-side check below is `DRY-RUN SAFE`: it validates the redacted evidence
package after that journey exists and rejects operator shortcuts or leaks.

The evidence package must use token sources such as
`env:LOOM_PROD_USER_E2E_TOKEN` or `file:/secure/path/token`, never raw bearer
values, signed URLs, database credentials, MinIO credentials, shell traces, or
argv token flags. It must include:

- production environment, route, API base, user role, and token source;
- prod/dev route and API-base separation for `https://yylx.world/prod` and
  `https://yylx.world/dev`;
- `cli_api` pass records for `submit`, `monitor`, `batch_detail`,
  `batch_debug`, `trial_detail`, `trial_debug`, `download_atif`,
  `download_trajectory`, `download_artifact`, `delivery_bundle`, and
  `integrity`;
- frontend pass records for route/API-base loading, navigation, submit/status/
  debug/download buttons, and service-proxied download routes;
- `forbidden_shortcuts: []`.

The required public CLI examples include exact exposed commands, for example
`loom eval batch delivery-bundle <batch-id> --mode raw-harbor-tb2-v1 --output
delivery.tar.gz`; do not substitute operator-only helpers, direct DB reads,
MinIO tools, Kubernetes commands, worker SSH, or hidden admin commands.

`DRY-RUN SAFE`

```bash
uv run python scripts/ops/operator_free_user_e2e_gate.py validate \
  --evidence "$ROLLOUT_DIR/operator-free-user-e2e.json" \
  --output-json "$ROLLOUT_DIR/operator-free-user-e2e-report.json"
```

Expected success output:

```text
Operator-free user E2E gate: PASS
```

Expected JSON evidence snippet:

```json
{
  "status": "pass",
  "issue": 493,
  "environment": {
    "name": "production",
    "route": "https://yylx.world/prod",
    "api_base": "https://yylx.world/prod/api",
    "user_role": "normal_user",
    "token_source": "env:LOOM_PROD_USER_E2E_TOKEN"
  },
  "validated_cli_api_steps": [
    "submit",
    "monitor",
    "batch_detail",
    "batch_debug",
    "trial_detail",
    "trial_debug",
    "download_atif",
    "download_trajectory",
    "download_artifact",
    "delivery_bundle",
    "integrity"
  ]
}
```

Expected failure output:

```text
Operator-free user E2E gate: FAIL
- missing required cli_api step 'trial_debug'
- forbidden shortcut declared at forbidden_shortcuts[0]
- forbidden evidence value at cli_api.submit.evidence
```

Stop condition: do not treat #493 as complete when this repo-side validator
passes against synthetic evidence. Keep #493 open until the authorized live
normal-user prod/dev route validation is run and the same validator accepts the
redacted evidence package without raw secrets, signed URLs, operator-only
surfaces, or prod/dev route confusion.

### 8. Production Release Gate

Create a synthetic no-secret manifest for the dry run. Live releases must use
real staging evidence, real image digests, real backup pointers, real frontend
route smoke evidence, the latest staging capacity evidence, the latest
operator-free user E2E/raw-delivery status, and rollback evidence.

`DRY-RUN SAFE`

```bash
uv run python - <<'PY'
import json
import os
from pathlib import Path

candidate = os.environ["CANDIDATE_SHA"]
image_tag = os.environ["IMAGE_TAG"]
rollout_dir = Path(os.environ["ROLLOUT_DIR"])
rollout_dir.mkdir(parents=True, exist_ok=True)

def digest(component, hex_char):
    return f"ghcr.io/qianyi-sun/{component}@sha256:{hex_char * 64}"

manifest = {
    "schema_version": 1,
    "candidate_sha": candidate,
    "image_tag": image_tag,
    "prod_tag": os.environ["PROD_TAG"],
    "staging_url": "https://staging.yylx.world",
    "image_digests": {
        "loom-control-plane": digest("loom-control-plane", "1"),
        "loom-llm-gateway": digest("loom-llm-gateway", "2"),
        "loom-service": digest("loom-service", "3"),
        "loom-worker": digest("loom-worker", "4"),
        "loom-web": digest("loom-web", "5"),
    },
    "checks": {
        "repository_ci": {"status": "pass", "url": "https://example.invalid/repository-checks"},
        "image_build": {"status": "pass", "url": "https://example.invalid/images"},
        "cluster_render_audit": {
            "status": "pass",
            "url": "https://example.invalid/render",
            "staging_config": "deploy/environments/staging.cluster.toml",
            "production_config": "deploy/environments/production.cluster.toml",
        },
        "migration_dry_run": {
            "status": "pass",
            "url": "https://example.invalid/migration",
            "db_recovery_point": "postgres-backup-20260706T000000Z",
        },
        "public_api_spa_smoke": {
            "status": "pass",
            "url": "https://example.invalid/smoke",
            "batch_id": "batch-release-smoke",
            "trial_id": "trial-release-smoke",
            "artifact_url": "https://staging.yylx.world/api/v1/trials/trial-release-smoke/atif",
            "service_no_oom_restarts_row": "final service.no_oom_restarts row after route probes passed",
        },
        "frontend_route_evidence": {
            "status": "pass",
            "url": "https://example.invalid/frontend-route-smoke",
            "production_route": "https://yylx.world/prod",
            "development_route": "https://yylx.world/dev",
            "production_api_base": "https://yylx.world/prod/api",
            "development_api_base": "https://yylx.world/dev/api",
        },
        "secret_redaction": {"status": "pass", "url": "https://example.invalid/redaction"},
        "provider_smoke": {
            "status": "pass",
            "url": "https://example.invalid/provider",
            "provider_path": "openai-compatible-prod-ref",
        },
        "benchmark_reward_gate": {
            "status": "pass",
            "url": "https://example.invalid/reward",
            "batch_id": "batch-reward-gate",
            "benchmarks": ["humaneval"],
        },
        "score_positive_canary": {
            "status": "pass",
            "url": "https://example.invalid/score-positive",
            "batch_id": "batch-score-positive-canary",
            "scored_trial_count": 7,
            "positive_reward_trial_count": 1,
        },
        "benchmark_score_alignment": {
            "status": "pass",
            "url": "https://example.invalid/score-alignment",
            "manifest": "docs/benchmark-score-alignment.md",
            "benchmarks": ["humaneval"],
        },
        "hf_mirror_token_boundary": {
            "status": "pass",
            "url": "https://example.invalid/hf-mirror-token-boundary",
            "benchmark_id": "skilllearnbench",
            "environment": "staging",
            "runtime_source_scheme": "s3",
            "runtime_source_prefix": "s3://loom-benchmarks/skilllearnbench/",
            "runnable_tasks": 100,
            "internal_s3_sources": 100,
            "total_task_sources": 100,
            "hf_provenance_retained": True,
            "upstream_kind": "huggingface",
            "upstream_locator": "PRHW/SkillLearnBench",
            "upstream_revision": candidate[:12],
            "worker_hf_token_present": False,
            "direct_hf_egress_required": False,
            "secret_safe": True,
        },
        "worker_capacity_smoke": {
            "status": "pass",
            "url": "https://example.invalid/worker-capacity",
            "batch_id": "batch-worker-capacity",
            "k8s_workers": 3,
            "oldlab_workers": 1,
            "runtime_seconds": 120,
            "failures": 0,
            "oldlab_worker_records": [
                {
                    "node_name": "TRT-EAI-OLDLAB-1",
                    "slurm_job_id": "13441",
                    "worker_id": "worker-oldlab-1",
                    "concurrency": 6,
                    "trials_claimed": 4,
                }
            ],
        },
        "prod_staging_isolation": {
            "status": "pass",
            "url": "https://example.invalid/prod-staging-isolation",
            "state_profile_evidence": "release-evidence/prod-staging-state-profile.json",
            "worker_identity_evidence": "release-evidence/prod-staging-worker-identity.json",
            "frontend_api_base_evidence": "release-evidence/prod-staging-api-base.json",
            "state_profiles": {
                "production": {
                    "environment": "production",
                    "github_environment": "production",
                    "namespace": "loom-prod",
                    "database_name": "loom_prod",
                    "provider_connection_namespace": "production",
                    "object_storage": {
                        "task_bucket": "loom-prod-tasks",
                        "trajectories_bucket": "loom-prod-trajectories",
                        "artifacts_bucket": "loom-prod-artifacts",
                    },
                    "secret_refs": {
                        "secret_store_key_ref": "github-environment:production/LOOM_SECRET_STORE_MASTER_KEY",
                        "service_api_token_ref": "github-environment:production/LOOM_SERVICE_API_TOKEN",
                        "worker_token_ref": "github-environment:production/LOOM_WORKER_TOKEN",
                        "provider_secret_ref": "github-environment:production/LOOM_PROVIDER_SECRET_REF",
                        "yibuapi_secret_ref": "github-environment:production/YIBUAPI_API_KEY",
                    },
                },
                "staging": {
                    "environment": "staging",
                    "github_environment": "staging",
                    "namespace": "loom-staging",
                    "database_name": "loom_staging",
                    "provider_connection_namespace": "staging",
                    "object_storage": {
                        "task_bucket": "loom-staging-tasks",
                        "trajectories_bucket": "loom-staging-trajectories",
                        "artifacts_bucket": "loom-staging-artifacts",
                    },
                    "secret_refs": {
                        "secret_store_key_ref": "github-environment:staging/LOOM_SECRET_STORE_MASTER_KEY",
                        "service_api_token_ref": "github-environment:staging/LOOM_SERVICE_API_TOKEN",
                        "worker_token_ref": "github-environment:staging/LOOM_WORKER_TOKEN",
                        "provider_secret_ref": "github-environment:staging/LOOM_PROVIDER_SECRET_REF",
                        "yibuapi_secret_ref": "github-environment:staging/YIBUAPI_API_KEY",
                    },
                },
            },
            "frontend": {
                "production": {
                    "environment": "production",
                    "route": "https://yylx.world/prod",
                    "api_base": "https://yylx.world/prod/api",
                    "environment_label": "Production",
                },
                "staging": {
                    "environment": "staging",
                    "route": "https://yylx.world/dev",
                    "api_base": "https://yylx.world/dev/api",
                    "environment_label": "Staging",
                },
            },
            "workers": {
                "production": {
                    "environment": "production",
                    "api_url": "https://yylx.world/prod/api",
                    "image": f"ghcr.io/qianyi-sun/loom-worker:{image_tag}",
                    "image_digest": digest("loom-worker", "4"),
                    "source_commit": candidate,
                    "k8s_namespace": "loom-prod",
                    "k8s_deployment": "loom-prod-worker",
                },
                "staging": {
                    "environment": "staging",
                    "api_url": "https://yylx.world/dev/api",
                    "image": "ghcr.io/qianyi-sun/loom-worker:staging-abcdef0",
                    "image_digest": digest("loom-worker", "6"),
                    "source_commit": os.environ["STAGING_RELEASE_SHA"],
                    "k8s_namespace": "loom-staging",
                    "k8s_deployment": "loom-staging-worker",
                },
            },
            "staging_capacity": {
                "lease_state": "none",
                "staging_slots": 0,
                "new_staging_claims_allowed": False,
                "override": {"approved": False},
            },
        },
        "raw_delivery_export_status": {
            "status": "pass",
            "url": "https://example.invalid/user-e2e",
            "requirement_status": "required export bundle verified for first-prod representative workflow",
        },
        "rollback_plan": {
            "status": "pass",
            "previous_production_image_digest": digest("loom-service", "a"),
            "rendered_manifest": "s3://loom-release-evidence/prod-rendered-prev.yaml",
            "db_recovery_point": "postgres-backup-20260706T000000Z",
        },
        "release_owner_approval": {
            "status": "pass",
            "owner": "qianyi-sun",
            "url": "https://example.invalid/release-approval",
        },
    },
}

(rollout_dir / "release-gate-input.json").write_text(
    json.dumps(manifest, indent=2, sort_keys=True),
    encoding="utf-8",
)
PY

uv run python scripts/ops/release_gate.py validate \
  --manifest "$ROLLOUT_DIR/release-gate-input.json" \
  --candidate-sha "$CANDIDATE_SHA" \
  --image-tag "$IMAGE_TAG" \
  --output-json "$ROLLOUT_DIR/release-gate-evidence.json" \
  --output-markdown "$ROLLOUT_DIR/release-gate-evidence.md"
```

Expected success output:

```text
Release gate validation: PASS
```

Expected JSON evidence snippet:

```json
{
  "status": "pass",
  "candidate_sha": "0123456789abcdef0123456789abcdef01234567",
  "image_tag": "release-0123456789ab",
  "prod_tag": "v1.0.0"
}
```

Expected failure output:

```text
Release gate validation: FAIL
- forbidden evidence value at checks.prod_staging_isolation.state_profiles.production.operator_note
```

Stop condition: missing required checks, stale candidate/image mismatch, active
staging capacity without override, shared prod/dev state, route mismatch, missing
final `service.no_oom_restarts` smoke evidence after route probes, missing or
failing `hf_mirror_token_boundary` evidence for SkillLearnBench mirrored
`s3://` runtime sources and worker HF token absence, or any forbidden evidence
value blocks promotion.

### 9. Production Release Workflow

The workflow commands below are not part of the no-secret dry run. They are
listed here so the release owner can run the same command shape with real
evidence after the dry run passes.

`LIVE PROD AUTHORITY REQUIRED` for tag pushes and production deploys.
`release-promotion-gate.yml` runs in the `staging` GitHub Environment, but it
gates production and should be treated as release-owner work.

```bash
base64_manifest="$(base64 < "$ROLLOUT_DIR/release-gate-input.json" | tr -d '\n')"

gh workflow run release-promotion-gate.yml \
  --ref dev \
  -f candidate_sha="$CANDIDATE_SHA" \
  -f image_tag="$IMAGE_TAG" \
  -f evidence_manifest_b64="$base64_manifest"
```

Expected success output:

```text
Created workflow_dispatch event for release-promotion-gate.yml at dev
```

Prepare the release PR from `dev` to `main`; attach the release-gate run,
frontend route evidence, worker-capacity evidence, raw-delivery/user-E2E
status, HF mirror/token-boundary evidence, and rollback plan. After merge, tag
the merged `main` commit exactly once:

```bash
git fetch origin main --tags
MAIN_RELEASE_SHA="$(git rev-parse origin/main)"
git tag -a "$PROD_TAG" "$MAIN_RELEASE_SHA" -m "Loom $PROD_TAG"
git push origin "$PROD_TAG"
```

Dry-run the production deploy first. Even with `dry_run=true`, this enters the
production deployment workflow and must go through production approval.

```bash
gh workflow run deploy-environment.yml \
  --ref "$PROD_TAG" \
  -f environment=production \
  -f image_tag="$IMAGE_TAG" \
  -f dry_run=true \
  -f candidate_sha="$CANDIDATE_SHA" \
  -f release_gate_run_id="$RELEASE_GATE_RUN_ID"
```

Then deploy for real:

```bash
gh workflow run deploy-environment.yml \
  --ref "$PROD_TAG" \
  -f environment=production \
  -f image_tag="$IMAGE_TAG" \
  -f dry_run=false \
  -f candidate_sha="$CANDIDATE_SHA" \
  -f release_gate_run_id="$RELEASE_GATE_RUN_ID"
```

The production job runs this preflight before `deploy_environment.sh`:

```bash
LOOM_CANDIDATE_SHA="$CANDIDATE_SHA" \
LOOM_IMAGE_TAG="$IMAGE_TAG" \
LOOM_RELEASE_GATE_RUN_ID="$RELEASE_GATE_RUN_ID" \
GH_TOKEN="$GH_TOKEN" \
bash scripts/ops/verify_production_release_gate.sh
```

Expected failure output when required live inputs are missing:

```text
error: production deploy requires release gate inputs: LOOM_CANDIDATE_SHA LOOM_IMAGE_TAG LOOM_RELEASE_GATE_RUN_ID GH_TOKEN
```

Stop condition: do not bypass this script. If it rejects the candidate SHA,
image tag, artifact download, missing gate evidence, leaked secret, internal
service URL, or non-ancestor production ref, fix evidence or rollback instead
of editing the workflow.

## Rollback Preparation

Create rollback evidence before the live production deploy. This step records
the previous production image/tag and recovery pointers; it must not contain
raw backup contents or credentials.

`DRY-RUN SAFE` with fake values; `LIVE PROD AUTHORITY REQUIRED` when using real
production deploy artifacts or backup pointers.

```bash
export PREVIOUS_PROD_TAG="${PREVIOUS_PROD_TAG:-v0.9.9}"
export PREVIOUS_PROD_IMAGE_TAG="${PREVIOUS_PROD_IMAGE_TAG:-release-prev-good}"
export PREVIOUS_PROD_IMAGE_DIGEST="${PREVIOUS_PROD_IMAGE_DIGEST:-ghcr.io/qianyi-sun/loom-service@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}"
export PREVIOUS_RELEASE_GATE_RUN_ID="${PREVIOUS_RELEASE_GATE_RUN_ID:-1234567890}"

uv run python - <<'PY'
import json
import os
from pathlib import Path

rollout_dir = Path(os.environ["ROLLOUT_DIR"])
rollback = {
    "artifact_type": "first-prod-rollback-prep",
    "previous_prod_tag": os.environ["PREVIOUS_PROD_TAG"],
    "previous_prod_image_tag": os.environ["PREVIOUS_PROD_IMAGE_TAG"],
    "previous_production_image_digest": os.environ["PREVIOUS_PROD_IMAGE_DIGEST"],
    "previous_release_gate_run_id": os.environ["PREVIOUS_RELEASE_GATE_RUN_ID"],
    "db_recovery_point": "postgres-backup-<redacted-pointer>",
    "object_storage_recovery_point": "object-store-snapshot-<redacted-pointer>",
    "rendered_manifest": "release-evidence/prod-rendered-prev.yaml",
    "secret_values": "[REDACTED sha256:<12-hex> len=<N>]",
}
(rollout_dir / "rollback-prep.json").write_text(
    json.dumps(rollback, indent=2, sort_keys=True),
    encoding="utf-8",
)
PY
```

Expected success output:

```json
{
  "artifact_type": "first-prod-rollback-prep",
  "previous_prod_tag": "v0.9.9",
  "db_recovery_point": "postgres-backup-<redacted-pointer>",
  "secret_values": "[REDACTED sha256:<12-hex> len=<N>]"
}
```

For a live image rollback to the previous validated production tag:

`LIVE PROD AUTHORITY REQUIRED`

```bash
gh workflow run deploy-environment.yml \
  --ref "$PREVIOUS_PROD_TAG" \
  -f environment=production \
  -f image_tag="$PREVIOUS_PROD_IMAGE_TAG" \
  -f dry_run=false \
  -f candidate_sha="$PREVIOUS_CANDIDATE_SHA" \
  -f release_gate_run_id="$PREVIOUS_RELEASE_GATE_RUN_ID"
```

For single-component Kubernetes rollback during an approved break-glass window:

`LIVE PROD AUTHORITY REQUIRED`

```bash
kubectl -n loom-prod rollout undo deploy/loom-control-plane
kubectl -n loom-prod rollout undo deploy/loom-llm-gateway
kubectl -n loom-prod rollout undo deploy/loom-service
kubectl -n loom-prod rollout undo deploy/loom-web
kubectl -n loom-prod rollout undo deploy/loom-worker
```

Stop condition: DB downgrades that drop data are not reversible without
restore from snapshot. Do not run `alembic downgrade` or object-store restore
from this runbook alone; require the release owner and the recorded recovery
point.

## Final Live Acceptance Checklist

Before declaring the first production release ready:

- Release gate evidence has `"status": "pass"` and includes real
  `prod_staging_isolation`, `frontend_route_evidence`, `worker_capacity_smoke`,
  `raw_delivery_export_status`, and `rollback_plan` records.
- Staging capacity evidence shows `"staging_slots": 0`,
  `"new_staging_claims_allowed": false`, and no active lease unless the release
  owner approved a documented override.
- Frontend route smoke proves `https://yylx.world/prod` resolves production
  metadata/API base and `https://yylx.world/dev` resolves staging metadata/API
  base.
- Staging smoke evidence includes the final `service.no_oom_restarts` row
  after route probes and security scanning, not only the pre-route baseline.
- The production deploy uses `main` or an immutable `vX.Y.Z` tag, never `dev`.
- Rollback prep records previous image/tag, previous release-gate run, DB
  recovery point, object-storage recovery point, and redacted secret evidence.
- No live prod/staging workload, DB change, worker cancellation, tag push,
  or deploy is run from a no-secret dry-run review.
