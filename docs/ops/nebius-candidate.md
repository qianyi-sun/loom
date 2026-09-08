# Nebius integration candidates

This independent publication lane advances #1798. It builds the six platform
images from one `codex/nebius-main` commit, scans them, pushes directly to Nebius
Registry and signs a digest manifest plus the service-execution runtime profile.
It does not deploy, change `dev`/`main` authority, or prove live acceptance.

## Bootstrap

Publication runs on GitHub-hosted Ubuntu 24.04 AMD64. CI and publication require
no Nebius runner node groups, ARC controller, runner registration credentials or
custom runner image. The workflow installs version-pinned Skopeo from Ubuntu and
starts the pinned rootless BuildKit container on the disposable hosted machine.
Its client is copied from that same immutable image. The daemon's container port
is published only on host loopback; no host network, host workspace mount, or
Docker socket is passed into the builder. Workspaces and builder data disappear
with the job, and an always-run cleanup retains only daemon status/exit code in the artifact before removal.
Raw daemon logs stay job-local; failed publication commands retain the existing
bounded sanitized diagnostic artifact.

Create environment `nebius-integration`, allowing only `codex/nebius-main`.
After the environment and trust configuration are verified, set repository variable
`NEBIUS_RELEASE_ENABLED=true` to enable publication on integration branch pushes.
Configure variables `NEBIUS_REGISTRY_PREFIX` (a Nebius registry repository
prefix), `NEBIUS_IMAGE_ADMISSION_SIGNING_KEY_ID` and
`NEBIUS_IMAGE_ADMISSION_TRUSTED_KEYRING_JSON`. Configure secrets
`NEBIUS_REGISTRY_SERVICE_ACCOUNT_JSON` (a registry-writer service account's
authorized-key credentials JSON, with `subject-credentials` using RS256) and
`NEBIUS_IMAGE_ADMISSION_SIGNING_KEY` (established Ed25519 PEM private key).
The independently provisioned public keyring must also be supplied to deployment
and Control Plane trust configuration. Publication never generates a new trust
root or installs the keyring. Grant the registry identity only its intended
registry write scope. The locked SDK exchanges its authorized key for a fresh
access token at publication time and before each image push. The shared temporary
Docker/Skopeo auth file uses `oauth2accesstoken` and mode 0600; credentials and auth
files live outside the build context and are removed at job exit. A copied access
token is not a supported bootstrap credential. Rotate/revoke the authorized key
through IAM as needed, without editing pipeline code. See the
[Nebius service-account authentication contract](https://docs.nebius.com/grpc-api/auth).

BuildKit is pinned to `v0.33.0-rootless` and its manifest digest in the workflow.
Skopeo is pinned to Ubuntu package `1.13.3+ds1-2ubuntu0.24.04.3`; the explicit
Ubuntu 24.04 runner label keeps that package source stable. Trivy uses the
repository's version/hash-checked installer and controlled scan exceptions;
Python tooling uses `uv sync --locked` with the existing `cluster` extra for the
Nebius SDK. Images are built to an OCI archive, scanned and copied with digest
preservation; registry readback must match the scanned immutable image. Initial
publication builds all six images without an incremental-release controller or
another required CI gate. The resulting platform images and all deployed
platform resources use Nebius; the build job itself uses GitHub infrastructure.

## Publish and verify

The workflow runs on pushes to `codex/nebius-main` once enabled; it also supports
manual dispatch where GitHub exposes that event for the workflow. The
workflow's immutable `github.sha` is the candidate; there is no arbitrary candidate
override. The push trigger permits branch-local bootstrap without putting the
experimental workflow on `dev`. GitHub may suppress follow-on events for commits
created with a workflow's `GITHUB_TOKEN`; use a normal protected branch merge
initiated with the owner's supported GitHub credentials when publication is needed.
Do not substitute an ambient local publish or expand legacy lanes.

The artifact contains `candidate.json`, `runtime-profile.json`, and per-image scan
reports/SBOMs. Partial scan evidence is retained on failure; absence of the signed
candidate means no complete release exists. Partial registry uploads are possible
on failure and must not be deployed. Tag names aid discovery only: deployments
consume the signed `@sha256:` references. Artifact names are
`nebius-candidate-<sha>-<run-id>-<run-attempt>` so retries retain earlier evidence
without upload-name conflicts. GitHub retains the run ID when rerunning a job;
download the exact attempt's artifact. Never resolve tags in an already accepted
candidate or overwrite an evidence bundle.

Verify with the environment-owned keyring, not a key supplied by the artifact:

```sh
uv run --frozen python scripts/ops/nebius_candidate.py verify \
  --candidate candidate.json --runtime-profile runtime-profile.json \
  --trusted-keyring /secure/nebius-image-admission-keyring.json
```

`create --build-record PATH --signing-key PATH --signing-key-id ID
--trusted-keyring PATH --output DIR` is the local packaging interface for an
already collected build record. Signing authority belongs only to the controlled
publisher. The signed manifest binds repository, integration ref, source commit
and tree, workflow/run, all six registry digests and evidence hashes. Profile hash
is SHA256 over UTF-8 `json.dumps(profile, sort_keys=True, separators=(',', ':'))`
plus a newline, with the `sha256:` prefix. Signatures use the existing canonical
document format. Runtime admissions retain the existing non-expiring profile
contract; revoke a compromised signer through the independently managed keyring.
Vulnerability evidence belongs to its exact build and scan time and is not a
claim that the image remains free of subsequently discovered vulnerabilities.
Publication failures retain a bounded `failed-command.json` with operation,
exit status and sanitized diagnostics alongside partial scan evidence.

Required follow-up acceptance: provision the restricted GitHub environment and
publisher IAM/trust bindings, publish an exact merged integration commit, verify
registry readback, and deploy that signed bundle through the independent platform
renderer/deployer. Publication requires no cluster bootstrap.
