# Nebius integration candidates

This independent publication lane advances #1798. It builds the six platform
images from one `codex/nebius-main` commit, scans them, pushes directly to Nebius
Registry and signs a digest manifest plus the service-execution runtime profile.
It does not deploy, change `dev`/`main` authority, or prove live acceptance.

## Bootstrap

Provision a clean, ephemeral Linux AMD64 ARC scale set on Nebius named
`loom-nebius-release`, using the pinned rootless BuildKit sidecar and its matching
`/opt/buildkit/buildctl` client. Build the runner derivative from
`deploy/Dockerfile.nebius-runner`, publish it to Nebius and configure the resulting
immutable digest as `release_runner_image`. It includes version-pinned Skopeo;
runtime jobs need neither sudo nor a privileged Docker daemon. Register this scale set only to this repository, with ephemeral workspaces.
Environment secrets are restricted to the integration branch; the runner itself
has no publication credentials, cloud writer identity or Kubernetes API token.
A runner label is routing, not workflow authorization. No runner fallback
exists. Runner creation/registration and IAM provisioning are separate operator
steps, not performed by this workflow.

Create environment `nebius-integration`, allowing only `codex/nebius-main`.
After the runner and trust configuration are verified, set repository variable
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

BuildKit comes from its verified sidecar image and Skopeo comes from the controlled
runner derivative, pinned to Ubuntu package `1.13.3+ds1-2ubuntu0.24.04.3` (the
[Noble package source](https://launchpad.net/ubuntu/noble/+source/skopeo)). Its package
inventory is baked under `/opt/loom-runner`. Trivy uses the repository's
version/hash-checked installer and controlled scan exceptions; Python tooling uses
the frozen lockfile with the existing `cluster` extra for the Nebius SDK. Images
are built to an OCI archive, scanned and copied with digest preservation; registry
readback must match the scanned immutable image. Initial publication builds all six images once
without another incremental-release controller or required CI gate.

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
consume the signed `@sha256:` references. Reruns build a new artifact/run identity;
never resolve tags in an already accepted candidate or overwrite an evidence bundle.

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

Required follow-up acceptance: provision the Nebius runner and environment/IAM
bindings, publish an exact merged integration commit, verify registry readback,
and deploy that signed bundle through the independent platform renderer/deployer.

The release derivative retains the runner listener and Node 20/24 executables
needed by JavaScript actions. It removes upstream's unused Docker daemon/CLI,
containerd/runc/buildx and bundled npm/corepack distributions. Application package
installation runs inside BuildKit's build stages, not in this release runner.
This narrower tooling avoids inheriting vulnerable dependencies from software
the publication job never uses. The complete remaining image is scanned under
the unchanged critical-vulnerability policy.
