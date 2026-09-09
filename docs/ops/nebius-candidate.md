# Nebius integration candidates

This independent publication lane advances #1798. It builds the six platform
images from one `codex/nebius-main` commit, scans them, pushes directly to Nebius
Registry and records the commit and six immutable image references. It also
produces the existing service-execution runtime profile required by Control Plane.
It does not deploy, change `dev`/`main` authority, or prove live acceptance.

## Bootstrap

Publication runs on GitHub-hosted Ubuntu 24.04 AMD64. CI and publication require
no Nebius runner node groups, ARC controller, runner registration credentials or
custom runner image. The workflow installs version-pinned Skopeo from Ubuntu and
starts the pinned BuildKit container on the disposable hosted machine.
Its client is copied from that same immutable image. The daemon's container port
is published only on host loopback; no host network, host workspace mount, or
Docker socket is passed into the builder. Workspaces and builder data disappear
with the job, and an always-run cleanup retains bounded, sanitized daemon diagnostics before removal.
The standard privileged BuildKit container runs only on that disposable hosted
machine, with no host socket or workspace mount. The rootless daemon exited before
listening in the first GitHub-hosted run; this supported container mode removes
its dependency on host user namespaces.
Failed publication commands use the same bounded diagnostic sanitizer.

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
Docker/Skopeo auth file uses `iam` and mode 0600; credentials and auth
files live outside the build context and are removed at job exit. A copied access
token is not a supported bootstrap credential. Rotate/revoke the authorized key
through IAM as needed, without editing pipeline code. See the
[Nebius service-account authentication contract](https://docs.nebius.com/container-registry/authentication).

BuildKit is pinned to `v0.33.0` and its manifest digest in the workflow.
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
reports/SBOMs. Partial scan evidence is retained on failure; absence of the completed
candidate means no complete release exists. Partial registry uploads are possible
on failure and must not be deployed. Tag names aid discovery only: deployments
consume the published `@sha256:` references. Artifact names are
`nebius-candidate-<sha>-<run-id>-<run-attempt>` so retries retain earlier evidence
without upload-name conflicts. GitHub retains the run ID when rerunning a job;
download the exact attempt's artifact. Never resolve tags in an already accepted
candidate or overwrite an evidence bundle.

`candidate.json` is a plain publication record: repository/ref, commit, workflow
run and six digest references. Renderer and deployer consume those fields without
detached signatures, profile hashes or another reconstruction/verification layer.
Optional format inspection is available with:

```sh
uv run --no-sync python scripts/ops/nebius_candidate.py check-shape \
  --candidate candidate.json
```

The publisher signs only the two image-admission statements already required by
Control Plane for the task/service image and execution-runtime image. Their
existing scan/provenance and runtime-binary fields remain in `runtime-profile.json`;
Control Plane verifies them through its established runtime admission path.
The signer/keyring remains environment-owned configuration. The publisher checks
the scanned OCI digest against the pushed digest once; later deployment steps
reuse the publication record instead of hashing the same files again.

Runtime admissions retain the existing non-expiring profile
contract; revoke a compromised signer through the independently managed keyring.
Vulnerability evidence belongs to its exact build and scan time and is not a
claim that the image remains free of subsequently discovered vulnerabilities.
Publication failures retain a bounded `failed-command.json` with operation,
exit status and sanitized diagnostics alongside partial scan evidence.

Required follow-up acceptance: provision the restricted GitHub environment and
publisher IAM/trust bindings, publish an exact merged integration commit, verify
registry readback, and deploy that publication bundle through the independent platform
renderer/deployer. Publication requires no cluster bootstrap.

The publisher exports one OCI archive per image. It extracts that archive into a temporary OCI layout directory for Trivy vulnerability and SBOM reports, then pushes the original archive with Skopeo. Trivy accepts an OCI layout directory through `--input`; an OCI tar archive is not a Docker-save archive. The temporary layout is removed after scanning. The existing scan policy and scanned-to-published digest check are unchanged.
