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

The hidden internal protocol is intentionally two-phase. First,
`POST /api/v1/internal/pipeline-stage1-smoke/capacity-preflight` consumes a
candidate-bound signature and creates only a durable capacity intent plus one
scoped policy activation with `desired_slots=1`. It creates no PipelineRun.
After the autoscaler has produced the exact worker/allocation/GPU observation,
`POST /api/v1/internal/pipeline-stage1-smoke/execute` consumes a separate fresh
signature and may create the sole run. These routes require:

- canonical candidate, authorization, and preflight documents;
- an Ed25519 signature from a configured Stage 1 operator key;
- a fresh one-use nonce and an exact idempotency key;
- an injected preflight authority that independently verifies the candidate,
  worker, image/platform, inputs, cluster, GPU order, capacity, and zero-residue
  observations.

Execute creates exactly one controller-owned PipelineRun, one budget ledger,
and one GPU backend selection while retaining the capacity activation from the
first phase. Replaying either exact signed request returns the same resource;
changing any bound input conflicts before a second mutation.

The hidden evidence and cleanup routes are separately signed. Cleanup begins by
atomically changing the activation to `draining / desired_slots=0`; only after
the controller has drained all resources can cleanup-final accept independent
zero-residue evidence and disable the activation. A capacity intent that never
reaches execute follows the same signed drain/finalize path and terminates as
`capacity_aborted`, without creating a Run. The default application composition
installs none of the capacity-preflight, execution-preflight, evidence, or
cleanup authorities, so every live phase fails closed with `503` until an
approved deployment supplies them.

Terminal evidence is a closed typed document, not an operator note. It binds
the complete ordered Attempt set, GPU topology, selected platform child, input
view, at least three preview frames and their cadence, the committed Artifact
and upload marker, authenticated viewer/readback proofs, synchronization bound,
and secret-canary result. The SQL authority joins those identities and digests
back to Run, StageRun, Attempt, preview, upload, and Artifact rows; an injected
protected-environment observer must return the identical evidence document.

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

loom pipeline stage1-smoke capacity-preflight \
  --candidate @candidate.json \
  --authorization @authorization.json \
  --confirm-candidate-sha sha256:... \
  --idempotency-key stage1-capacity-... \
  --signature-key-id operator-key-1 \
  --signature @capacity.signature \
  --json

loom pipeline stage1-smoke execute \
  --candidate @candidate.json \
  --authorization @authorization.json \
  --preflight @preflight.json \
  --confirm-candidate-sha sha256:... \
  --idempotency-key stage1-... \
  --signature-key-id operator-key-1 \
  --signature @execute.signature \
  --json

loom pipeline stage1-smoke cleanup-begin \
  --cleanup-begin @cleanup-begin.json \
  --confirm-candidate-sha sha256:... \
  --signature-key-id operator-key-1 \
  --signature @cleanup-begin.signature \
  --json

loom pipeline stage1-smoke cleanup \
  --cleanup @cleanup.json \
  --confirm-candidate-sha sha256:... \
  --signature-key-id operator-key-1 \
  --signature @cleanup.signature \
  --json
```

`inventory`, `prepare-candidate`, `render-candidate`, and observation assembly
are read-only. `capacity-preflight` is the first explicit live mutation and
`execute` is a distinct second live mutation. Cleanup is mandatory after every
capacity activation, including pre-execute failure, and both cleanup phases are
idempotent.

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
