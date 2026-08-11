# Staging release validation

Use this checklist after the installed rollout authority has deployed an exact
merged `dev` candidate to shared staging. It defines the current validation
boundary for production promotion; it is not authority to mutate the cluster.

## Candidate and environment identity

Record the rollout request ID, candidate SHA, image tag and digests, rendered
profile digest, migration result, backup manifest, and running build identity.
They must all identify the same candidate.

Confirm the environment boundary:

```bash
uv run --no-sync python scripts/validate_environment_isolation.py \
  --profiles-dir deploy/environments \
  --workflow .github/workflows/deploy-environment.yml \
  --dry-run-artifact release-evidence/environment-isolation-dry-run.json

uv run --no-sync loom cluster audit \
  --config deploy/environments/staging.multinode.cluster.toml
```

The audit must expose only the staged SPA and service API over TLS. The
Control Plane, LLM Gateway, databases, object store, and internal routers must
not become public backends.

Use the installed authority for rollout inspection:

```bash
loom-staging-rollout --env staging status REQUEST_ID
loom-staging-rollout --env staging logs REQUEST_ID
```

Do not run `loom cluster up`, `loom cluster rollout`, direct migration Jobs,
environment-state mutation, tunnel installation, or worker reconfiguration
outside that request.

## Public route and authentication checks

Verify the logged-out SPA, runtime configuration, and health API at
`https://yylx.world/staging`. The runtime configuration response must be
`Cache-Control: no-store` and must point to
`https://yylx.world/staging/api`. Confirm that production remains at
`https://yylx.world/prod` and does not share staging state.

Exercise the supported user flows with disposable staging identities:

- account request, approval, password setup, login, logout, and password reset;
- team membership and team-scoped API-token behavior;
- provider creation/test/model discovery without exposing stored credentials;
- batch submission, progress, trial detail, and allowed artifact downloads;
- Run Library `My team` and `All teams` visibility, owner-team labels, clone
  config, artifact reuse provenance, and cross-team denial behavior.

Use `scripts/ops/frontend_route_smoke.py --help` for the route evidence
collector. Store only sanitized responses and identifiers.

## Repeatable API smoke

Run the current API and Run Library gate using secret-source arguments rather
than literal tokens:

```text
uv run --no-sync python scripts/staging_smoke_gate.py \
  --server-url https://yylx.world/staging \
  --team-a-token file:/secure/path/team-a-token \
  --team-b-token file:/secure/path/team-b-token \
  --provider-connection-name CONNECTION \
  --provider-model-provider PROVIDER \
  --provider-model-name MODEL \
  --batch-id BATCH_ID \
  --trial-id TRIAL_ID \
  --required-worker-pool gb10 \
  --fail-on-skip \
  --json-output release-evidence/staging-smoke.json \
  --markdown-output release-evidence/staging-smoke.md
```

Supply the artifact, catalog, object-store, and Kubernetes diagnostic options
shown by `--help` when those checks are part of the candidate. A release report
must have no failed required check and, when `--fail-on-skip` is used, no
required skip.

The gate covers public health and logged-out SPA behavior, team identities,
providers and model catalog, ready agents and benchmarks, mirrored bundle
objects, object-store write/delete, service restart count, batch/trial state,
worker-pool coverage, owner downloads, cross-team boundaries, Run Library
clone/reuse provenance, mutation denial, and secret/internal-URL scanning.

## Benchmark and worker checks

Before promotion, verify all of the following against the candidate-bound
catalog and current worker inventory:

1. Catalog audit reports runnable tasks with complete internal bundle objects.
2. The representative provider/model preflight succeeds through the Loom
   Gateway.
3. The benchmark reward gate and score-positive canary use fresh batch and
   trial IDs from this rollout.
4. `docs/score-alignment/manifest.json` passes the Layer 1 gate for the current
   catalog contract.
5. HF-mirrored tasks resolve from internal `s3://` objects; workers do not
   receive `HF_TOKEN`.
6. Required Kubernetes and external worker pools are registered, healthy, and
   represented by terminal trial evidence.
7. Production-owned capacity remains available according to the checked-in
   capacity policy; any staging lease is bounded and released after validation.

Useful offline checks:

```bash
uv run --no-sync python scripts/benchmark_score_alignment_gate.py manifest \
  --manifest docs/score-alignment/manifest.json
uv run --no-sync python scripts/ops/worker_capacity_manifest.py status \
  --config deploy/worker-capacity/prod-first.toml
```

## Secret and isolation checks

Scan reports for bearer tokens, API keys, signed URLs, object-store
credentials, internal service URLs, local hostnames, and secret-store values.
Release evidence may contain secret references such as a GitHub Environment
secret name, never the value.

The `prod_staging_isolation` evidence must describe both environments'
namespaces, database names, object buckets, route/API bases, safe credential
references, worker endpoints and image/source identities, and current staging
capacity lease state. Production and staging values must be distinct where the
release gate requires separation.

## Promotion manifest

Validate the structured evidence with the same code used by the workflow:

```bash
uv run --no-sync python scripts/ops/release_gate.py validate \
  --manifest release-gate-input.json \
  --candidate-sha CANDIDATE_SHA \
  --image-tag IMAGE_TAG \
  --output-json release-evidence/release-gate-evidence.json \
  --output-markdown release-evidence/release-gate-evidence.md
```

The manifest currently requires these checks:

- `repository_ci`, `image_build`, `cluster_render_audit`, and
  `migration_dry_run`;
- `public_api_spa_smoke`, `frontend_route_evidence`, `secret_redaction`, and
  `provider_smoke`;
- `benchmark_reward_gate`, `score_positive_canary`,
  `benchmark_score_alignment`, and `hf_mirror_token_boundary`;
- `worker_capacity_smoke`, `prod_staging_isolation`, and
  `raw_delivery_export_status`;
- `rollback_plan` and `release_owner_approval`.

Dispatch the promotion gate only after the local validator passes:

```bash
EVIDENCE_MANIFEST_B64="$(base64 < release-gate-input.json | tr -d '\n')"
gh workflow run release-promotion-gate.yml \
  --ref dev \
  -f candidate_sha=CANDIDATE_SHA \
  -f image_tag=IMAGE_TAG \
  -f evidence_manifest_b64="$EVIDENCE_MANIFEST_B64"
```

Do not promote when candidate identity differs across evidence, a required
check failed or is missing, the redaction scan fails, a backup/recovery point
is absent, worker or storage capacity is unsafe, staging and production state
overlap, or the rollback plan is not executable.

After collecting the final sanitized evidence, release temporary staging
capacity and disposable credentials. Keep generated reports with the release
artifact, not under `docs/`.
