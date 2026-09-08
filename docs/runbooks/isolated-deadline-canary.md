# Isolated deadline fixture (#1857)

This is the first implementation slice of #1857, following the #1858 dispatch
audit. It supplies a deployable fault-provider process, resource renderer and
read-only receipt projection. It does **not** yet supply the protected live
launcher, exclusive-worker preflight, single-trial orchestration or complete
acceptance collector. Keep #1857 and #1748 open. Every fixture evidence response
therefore has `full_canary_passed=false`.

## Protocol and trust

The real Terminus-2 route is `/openai/v1/chat/completions`. After a durable
dispatch receipt commits, this route sends its random UUID as `X-Request-ID`.
The ID is unique per actual Gateway HTTP retry. Caller-supplied trace IDs are
ignored. It contains no trial, team, step, agent-attempt, grant, bearer or prompt.
Other dialects are unchanged by this slice.

The fixture holds a queued request for at most three seconds awaiting operator
approval. It cannot decide which attempt to hold from arrival order or time.
The operator reads that exact receipt in the installed Gateway container via
`python -m loom_llm_gateway.deadline_canary_receipts`, providing the expected
team/trial/step/provider IDs. The reader uses a bounded read-only transaction,
requires an admitted, pending OpenAI model-call receipt with signed attempt and
grant IDs and an unexpired deadline, and emits only a safe projection. Gateway
database credentials remain in the Gateway container.

An independently authenticated operator sends this projection to
`POST /operator/approve`. The provider capability cannot access operator routes,
and the operator capability is not a provider credential. Case B approvals also
require the reader to find exactly one worker timeout record for the first
attempt with `task_stopped=true` and configured timeout 10. Missing/delayed
evidence fails closed; it is not inferred from a newer request.

`POST /operator/arm` binds the submitted trial and step once. Requests may queue
while the operator reads the submission response but cannot execute before arm
and receipt approval. Rebinding, duplicate IDs, foreign identities, expired
grants, another request from attempt 1, and excess requests are rejected. The
ledger retains at most four request records and counts additional rejections.
Provider prompts, headers and credentials are never retained in evidence.

## Provider surface

- `GET /healthz`: readiness; becomes unavailable after close or expiry.
- `GET /v1/models`: authenticated discovery of `loom-deadline-canary`.
- `POST /v1/chat/completions`: non-streaming OpenAI response format.
- Before arm, a direct model preflight without a receipt ID returns a discovery
  completion and is counted separately. After arm, missing IDs are rejected.
- Case A approves one held request. Case B approves one held request, then two
  consecutive replies for the same fresh attempt and deadline. Each reply's
  assistant content is JSON with `analysis`, `plan`, `commands=[]` and
  `task_complete=true`, matching Harbor's two-completion confirmation protocol.
- The hold ends two seconds beyond the signed deadline (bounded to 13 seconds
  after approval). Successful fixture responses contain explicitly synthetic
  token counts; they are not evidence of external provider billing.
- `GET /operator/evidence` remains readable after close/expiry with the operator
  capability. `POST /operator/close` stops further work. A process interruption
  invalidates the run; do not resume an in-memory fixture or infer lost evidence.

## Deployment boundary

`loom_cli.deadline_canary_manifest.render_fixture_resources` renders a unique
ConfigMap, Job, Service and NetworkPolicy in `loom-staging`. It requires an
immutable Gateway image digest. The Job has no retry, no service-account token,
no host mounts, no DB credentials and a read-only root filesystem; its lifetime
is bounded. Two dedicated capabilities are mounted from a run-specific Secret.
Rendering does not prove that the image belongs to an accepted candidate.

The forthcoming protected launcher must verify the completed official rollout,
candidate/image/route readback, Kubernetes ownership and policy enforcement
before applying. The fixture's ingress policy permits only same-namespace
Gateway, egress proxy and service pods; the operator uses a protected local
port-forward. Kubernetes policies are additive: inspect the effective policy,
not merely this rendered document. The provider endpoint must pass ordinary
registration and egress checks. An internal Service requires an already
authorized private-endpoint team; do not change that flag or relax loopback,
link-local, SSRF or DNS-rebinding protections for a test.

Use existing `POST /api/v1/trials` with a stable idempotency key and singular
`required_worker_pool`, not batch coverage placement. Before submission require
the dedicated task checksum/pinned Harbor provenance, exact authorized team and
connection, persisted retry ceiling/config and a genuinely exclusive worker
allocation. Pool pin or momentary idleness is not exclusive ownership. Never
cancel or drain another user's work. The full9 provider/config is not the fault
provider and must remain unchanged.

The launcher must collect fixture evidence before exact UID-bound cleanup and
retain partial evidence on failure. Missing evidence never means success.

## Local verification versus live acceptance

The real TCP/PostgreSQL regression exercises Gateway auth, committed opaque ID,
reader binding checks, a ten-second deadline and two fresh-attempt completions.
Its worker timeout row is explicitly seeded test data, not a real supervisor
observation. It proves the transport/fixture contract only. Unit checks cover
capability separation, malformed/missing IDs, cancellation, replay, expiry,
bounded record count, immutable image requirement and isolated resource shape.

Full acceptance still needs real worker/CP/Harbor execution, exact deployed
images and route, quota/readback, unique terminal event, Trial/result/ATIF
agreement, timeout/drain and separate persistence latency, signed retry/grant
joins, and post-run worker health. See the full manifest in
[Terminus-2 runtime](../architecture/terminus2-runtime.md#ten-second-production-equivalent-deadline-canary-1748).
