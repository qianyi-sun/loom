# Daytona service-mode worker

Daytona is an explicit, opt-in service-worker backend. The default remains
`docker`; setting `LOOM_WORKER_SANDBOX_BACKEND=daytona` starts a small Loom
controller that manages multiple remote Daytona sandboxes. The sandboxes are
not Kubernetes Pods and the controller does not use the Slurm controller,
GB10/OLDLAB autoscaler, Docker socket, Compose, cgroups, or the local trial
image cache.

## Startup contract

A Daytona controller fails before worker registration unless all of these are
true:

- `LOOM_WORKER_CANDIDATE_SHA` is the exact lowercase 40-character commit SHA;
- `DAYTONA_API_KEY` or `DAYTONA_JWT_TOKEN` is available;
- Docker/Slurm-local settings such as cgroup containment, sandbox isolation,
  Compose identity, local vLLM, and per-container limits are disabled;
- the configured Daytona API concurrency, cleanup interval, and sandbox TTL
  are positive.

The worker advertises only `backend=daytona`. Batch backend selection is copied
into the Trial capability snapshot and the claim query requires an exact
backend match, so a Docker worker cannot claim Daytona work and vice versa.

## Immutable execution identity

Each claimed Daytona trial must start from a registry reference of the form
`repository@sha256:<digest>`. Dockerfile tasks consume the scheduler's frozen
`TaskImageExecutionGrantV1`; prebuilt tasks must already name an immutable
digest. Mutable tags, worker-local image builds, sidecars, custom DNS/hosts,
tmpfs, host-mounted family state, private-workspace verifier splitting, and
runtime agent install layers fail closed. Those workloads stay on Docker or
Slurm until their dedicated compatibility work lands.

Before provider creation, the controller persists this tuple in Postgres:

`trial_id / attempt_count / team_id / worker_id / candidate_sha /
provider_scope / artifact_ref / sandbox_name`.

The deterministic sandbox name is the provider idempotency key. On restart or
an ambiguous create response, the controller looks up the stored sandbox ID or
name before creating. It therefore adopts a committed sandbox instead of
creating a duplicate.

## Lifecycle and recovery

Loom owns the hard deadline, configured TTL, cancellation, usage attribution,
and cleanup journal. Daytona creation explicitly sets auto-stop and auto-pause
to `0` and auto-delete to `-1`, preventing provider idle detection from
terminating a background job. The controller bounds and spaces lifecycle API
calls with a process-wide gate.

Successful start and delete transitions are written to `daytona_sandboxes`.
Deletion inserts exactly one `cloud_compute_records` row while holding the
ledger row lock. A periodic reconciler leases cleanup work when a trial is
terminal, reassigned, on another attempt, past its Loom deadline, or already in
`delete_pending`. Cleanup is fenced to a one-way fingerprint of the Daytona API
URL, target, and credential so a controller cannot delete a sandbox from
another provider account or scope. Provider 404 is treated as an idempotent
successful delete; other
errors return the row to `delete_pending` for retry.

## Gateway and secret boundary

Service-mode Daytona trials are admitted with a fail-closed security profile:

- The controller's `DAYTONA_API_KEY`, worker token, Gateway signing key, and
  model-provider credentials remain in Loom services. They are never copied
  from the worker process into sandbox environment variables, files, command
  arguments, or labels.
- Task-supplied environment values that have a secret-bearing name or value
  are rejected before any Daytona API mutation. Custom DNS and `/etc/hosts`
  aliases are rejected as well.
- Built-in agents whose model calls execute in the trusted worker use a
  `no-network` sandbox baseline. Out-of-box subprocess agents receive only a
  step-scoped `loom_step_...` Gateway credential and the reviewed public HTTPS
  Gateway URL.
- The subprocess credential is minted per agent step with the configured
  `LOOM_WORKER_SANDBOX_STEP_JWT_TTL_SEC` (600 seconds by default). It carries
  the team, trial, step, `llm:call`, and provider-connection scope enforced by
  the Gateway. The first slice does not rotate a token during one agent step;
  a call after expiry fails and follows the trial retry policy.

Raw provider keys therefore terminate at the Loom Gateway. The Gateway
decrypts the selected provider connection, performs the upstream call, and
records attribution without returning that credential to Daytona.

## Network policy

The secure profile applies network restrictions in the initial Daytona create
request. A sandbox is never created with public egress and restricted only
after startup.

- Built-in agents default to `no-network`.
- Subprocess agents default to an allowlist containing exactly the configured
  Gateway hostname. Loom passes it through Daytona's native network-layer
  domain firewall so DNS resolution works without opening general egress.
- `public`, arbitrary CIDRs, unreviewed domains, private/internal Gateway URLs,
  custom DNS, and host aliases fail task compatibility admission.
- Step-level network plans may reduce authority to `no-network`, but may not
  expand it beyond the reviewed Gateway hostname.

The domain allowlist is enforced by Daytona's sandbox firewall, not merely by
HTTP proxy environment variables. Organization-tier network restrictions still
take precedence, so a target that cannot apply the reviewed domain policy is
not compatible with this worker profile.

## Hosted, BYOC, and data residency

Hosted Daytona is allowed only for non-confidential workloads whose task
bundle, immutable image, prompts, outputs, and transient workspace may be
processed in the selected Daytona target and region. The first supported class
has no raw secrets, private source credentials, internal network dependencies,
sidecars, custom DNS, host mounts, or regulated/residency-constrained data.

Use local Loom execution, or a separately reviewed Daytona BYOC target, for
proprietary repositories, customer data, export-controlled or regulated data,
tenant-controlled network requirements, and workloads whose residency or
retention contract is not met by hosted Daytona. BYOC changes the infrastructure
owner but does not waive Loom's Gateway-only credential and network policy.
Automatic overflow must remain disabled until the target, region, retention,
logging, deletion, and subprocessors have been approved for the workload.

## Gated live security proof

The fake/unit suite verifies create-time network arguments, raw-secret
rejection, scoped credential admission, no-network, and allowlist denial
without contacting Daytona. A separate opt-in test creates and deletes one
real sandbox, performs one Gateway model call with `max_tokens=1`, proves a
direct provider connection is blocked, then proves `no-network` blocks the
Gateway too:

```text
LOOM_RUN_DAYTONA_GATEWAY_LIVE=1
DAYTONA_API_KEY=<controller-secret>
LOOM_DAYTONA_GATEWAY_LIVE_IMAGE=<immutable-image@sha256:digest>
LOOM_DAYTONA_GATEWAY_LIVE_URL=https://<reviewed-gateway>/openai/v1
LOOM_DAYTONA_GATEWAY_LIVE_MODEL=<provider-model-name>
LOOM_DAYTONA_GATEWAY_LIVE_PROVIDER_CONNECTION_ID=<uuid>
LOOM_DAYTONA_GATEWAY_LIVE_STEP_TOKEN=<fresh-bound-loom_step_token>
uv run pytest -q tests/integration/test_daytona_gateway_security_live.py
```

Run it only from a reviewed operator shell or secret-injection system; do not
place the token on a shared command line or commit it. Before enabling the
flag, record the Daytona target/backend, model/provider connection, one-call
count, zero parallel workers/retries, and expected compute plus model billing.
The test is skipped in normal CI and is not evidence that overflow scheduling
or a production pilot has been enabled.

## Opt-in configuration

The capability ships disabled. An operator may configure a dedicated
deployment with:

```text
LOOM_WORKER_SANDBOX_BACKEND=daytona
LOOM_WORKER_CANDIDATE_SHA=<exact-git-sha>
DAYTONA_API_KEY=<secret>
LOOM_WORKER_DAYTONA_API_MAX_CONCURRENT=4
LOOM_WORKER_DAYTONA_API_MIN_INTERVAL_SEC=0.25
LOOM_WORKER_DAYTONA_SANDBOX_TTL_SEC=86400
LOOM_WORKER_DAYTONA_CLEANUP_INTERVAL_SEC=30
LOOM_WORKER_SUBPROCESS_GATEWAY_URL=https://gateway.example.com/openai/v1
LOOM_WORKER_SANDBOX_STEP_JWT_TTL_SEC=600
```

This configuration alone does not provision credentials, deploy a controller,
enable automatic overflow from local pools, or alter the Docker worker.
