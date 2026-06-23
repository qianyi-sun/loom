# Public Beta Launch Gate

This page is the release-owner checklist for Loom's invite-only public beta. It
pulls together the deployment, onboarding, Run Library, security, and smoke
evidence needed before `dev` can be promoted to `main`.

## Launch Shape

- Loom is invite-only. Public users join through browser invites, then create
  named team API tokens for CLI use.
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
- Invite-only browser and CLI onboarding:
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

- `loom cluster audit` output showing TLS ingress, only `/` and `/api/v1`
  public backends, no public LLM Gateway, no public Control Plane, and no public
  object store.
- Screenshots or notes for logged-out SPA load, invite creation, invite
  acceptance, Team Settings API-token creation, provider setup, SPA batch
  submission, Monitor progress, and Run Library My team / All teams views.
- CLI transcript for `loom auth login`, `loom auth whoami`,
  `loom providers test`, `loom providers models`, `loom eval batch create`,
  `loom eval batch show`, `loom eval trial show`, and
  `loom eval trial download`.
- Benchmark catalog provisioning transcript showing
  `loom datasets provision-public-beta-catalog` with non-zero
  `ready_benchmarks`, non-zero `ready_tasks`, and `missing=0`, plus
  `/api/v1/benchmarks` evidence with at least one runnable entry.
- `scripts/public_beta_smoke_gate.py` Markdown evidence with `--fail-on-skip`
  and `--allow-mutating-checks` against disposable staging data.
- For IP-address staging hosts, note the hostless Ingress rendering, attach
  evidence that the TLS Secret certificate includes the staging IP as a Subject
  Alternative Name, and verify the ingress controller serves that Secret as its
  default certificate.
- Leak-scan note showing seeded fake secrets and internal service URLs were not
  found in API responses, audit excerpts, or downloaded safe artifacts.

## Automated Gate

After browser setup and a completed Team A source run, run:

```bash
python scripts/public_beta_smoke_gate.py \
  --server-url https://loom.example.com \
  --team-a-token "$TEAM_A_TOKEN" \
  --team-b-token "$TEAM_B_TOKEN" \
  --provider-connection-name smoke-openai \
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

## Release Decision

The public beta launch gate passes only when:

- every required manual evidence item is attached;
- the ready benchmark catalog has been provisioned with `missing=0`;
- `scripts/public_beta_smoke_gate.py` exits 0 with `--fail-on-skip`;
- no response, audit excerpt, log excerpt, or safe downloaded artifact contains
  seeded fake secrets or internal URLs;
- unsafe artifacts are blocked and cannot be downloaded by another team;
- clone/reuse provenance points back to the source run or artifact;
- the release issue links the exact commit, staging URL, and evidence files.

If any item fails, keep the release on `dev`, record the failing subsystem from
the smoke report, and open or update the owning issue before retrying.
