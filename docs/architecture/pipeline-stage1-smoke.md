# BEHAVIOR Stage 1 smoke authority

The Stage 1 smoke is an internal, single-run acceptance path for one real
BEHAVIOR rollout. It is not an ordinary Recipe and it is not part of the final
`matrix|soak` acceptance protocol. The public Recipe catalog and public run
submission API deliberately do not expose `behavior-stage1-smoke@1`; attempts
to submit it through those surfaces return `404`.

## Authority boundary

Rendering a candidate is local and read-only. A candidate freezes the Loom
commit, team, environment, backend, policy epoch and config digest, image index
and selected platform child, runtime/resource/renderer/schema digests, three
ordered Artifact inputs, one-episode parameters, budget, preview policy, start
window, and cleanup deadline. Rendering does not create a database row, worker,
policy activation, Slurm job, or PipelineRun.

The only repository route that can create the run is the hidden internal
`POST /api/v1/internal/pipeline-stage1-smoke/execute` endpoint. It requires:

- canonical candidate, authorization, and preflight documents;
- an Ed25519 signature from a configured Stage 1 operator key;
- a fresh one-use nonce and an exact idempotency key;
- an injected preflight authority that independently verifies the candidate,
  worker, image/platform, inputs, cluster, GPU order, capacity, and zero-residue
  observations.

In one transaction the service consumes the authorization and creates exactly
one controller-owned PipelineRun, one budget ledger, one GPU backend selection,
and one scoped policy activation with `desired_slots=1`. Replaying the same
signed request returns the same run; changing any bound input conflicts before
a second mutation.

The hidden evidence and cleanup routes are separately signed. They also require
injected evidence and cleanup authorities. The default application composition
installs none of these three authorities, so execute, evidence, and cleanup all
fail closed with `503` until an approved deployment supplies them.

## Runtime confinement

The graph contains one `rollout` container node, exactly three singleton
bindings (`task_instance`, `dataset`, `policy`), one required
`behavior_rollout_bundle.v1` output, network `none`, no provider, no secret,
and no fallback. Its node image is the immutable OCI index; the resolved
platform child remains a separate frozen field.

Only a controller-created Slurm worker for `behavior-gpu-oldlab` or
`behavior-gpu-gb10` enables the production Pipeline runtime. It advertises the
dedicated `loom-stage1-smoke-worker-v1` feature. The scheduler excludes Trial
work and all non-Stage-1 attempts from such a worker. The claim includes a
server-derived grant whose candidate, authorization, preflight, image,
bindings, renderer, and policy digests are revalidated before any attempt
resource is created.

The worker then uses the claim-bound input materializer, attested GPU preflight,
closed Docker runtime, low-rate live preview publisher, final-output uploader,
heartbeat/control loop, and durable cleanup journal. Completion is accepted
only after the committed output, StageResult, lineage, marker, and verified
files converge in one control-plane transaction. Failure and cancellation
reports require an observed zero-resource cleanup proof.

## Operator CLI

The CLI never accepts a private signing key. The operator signs canonical bytes
outside Loom and supplies the resulting signature file.

```console
loom pipeline stage1-smoke render-candidate \
  --candidate @candidate.json --json

loom pipeline stage1-smoke execute \
  --candidate @candidate.json \
  --authorization @authorization.json \
  --preflight @preflight.json \
  --confirm-candidate-sha sha256:... \
  --idempotency-key stage1-... \
  --signature-key-id operator-key-1 \
  --signature @execute.signature \
  --json

loom pipeline stage1-smoke cleanup \
  --cleanup @cleanup.json \
  --confirm-candidate-sha sha256:... \
  --signature-key-id operator-key-1 \
  --signature @cleanup.signature \
  --json
```

`render-candidate` and all preflight construction are non-mutating. `execute`
is the explicit live mutation boundary. Cleanup is mandatory after every live
outcome and is idempotent.

## Required post-merge gates

Repository CI cannot prove this live acceptance. Before `execute` can be
enabled, a later change must provide a real, hash-locked BEHAVIOR sim image and
rollout backend, publish the exact merged source as a native amd64/arm64 OCI
index, read back both children plus SBOM and attestations, and merge a runtime
contract lock for those exact digests. The same candidate must then be deployed
with production preflight, evidence, and cleanup authority adapters.

Image publication, runtime-lock merge, and deployment still do not authorize a
live run. The owner must separately authorize one exact candidate, backend,
policy epoch, input set, budget, time window, and cleanup deadline. The issue
remains open until that run's output, preview/viewer readback, accounting, and
zero-residue cleanup evidence are accepted.
